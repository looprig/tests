//go:build integration

package orchestrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// ---- a browser that repairs from the durable journal -----------------------
//
// PooledViewer RECORDS a stream; PooledBrowser APPLIES one, the way a real
// browser must: it holds a position (the highest journal sequence it is
// covered through), applies an enduring record that is the next one, skips one
// it already holds, and REPAIRS by reading Factory's journal route whenever it
// is told to (a session.reset) or cannot know what it missed (every
// (re)subscribe). Every repair is a read of the REAL SessionStore through a
// real Factory's HTTP route, so nothing in the kit answers for the events a
// browser recovers.
//
// It is meant for a DurableTail world, where the product's stream is that
// journal. In any other world the journal holds none of the product's events
// and a repair can only ever learn a number.
//
// # The repair is LAZY, and that is sound rather than convenient
//
// A repair is performed when Settle walks past the record that owes it, which
// may be some time after it was logged. That is safe because of the product's
// order: an event is durable BEFORE it is published, so a journal read made at
// any later moment covers every publication already in the log. A browser that
// repairs late can only learn more, never less.

// PooledBrowserOptions chooses how a browser's transport behaves.
type PooledBrowserOptions struct {
	// Stallable wraps the TCP connection so Stall can stop this browser
	// READING ITS SOCKET -- a genuinely slow consumer at the transport, as
	// opposed to one that reads promptly and is slow in a callback, which the
	// client library's own unbounded callback queue would absorb.
	Stallable bool
	// ReadBufferBytes, when positive, shrinks the socket's kernel receive
	// buffer so a stalled consumer backs up into the server's queue sooner.
	ReadBufferBytes int
}

// PooledBrowserEntry is one thing that happened to a browser, in order.
type PooledBrowserEntry struct {
	// Kind is "E" (enduring publication), "X" (ephemeral publication, Seq is
	// its body's "n"), "R" (session.reset), "T" (journal tip hint), "subscribed", "subscribing", "unsubscribed", "connecting",
	// "disconnected" or "?" (an undecodable record).
	Kind    string
	EventID sessionwire.EventID
	Seq     uint64 // E: journal_seq. R: last_contiguous.
	Tip     uint64 // R, T: journal tip.
	Code    uint32 // transport and subscription state changes.
	Reason  string
	Stray   bool
}

// PooledRepair is one journal read a browser made.
type PooledRepair struct {
	From           uint64
	CoveredThrough uint64
	Events         []PooledCommitted
	Pages          int
}

// PooledBrowser is one browser-shaped ClientLink client watching ONE session.
type PooledBrowser struct {
	Tenant  sessionwire.TenantID
	Session sessionwire.SessionID

	factory *PooledFactory
	client  *centrifugego.Client
	sub     *centrifugego.Subscription
	gate    *stallGate
	accepts int

	mu      sync.Mutex
	log     []PooledBrowserEntry
	closed  bool
	started bool

	// settle state, owned by the test goroutine.
	processed int
	position  uint64
	applied   []PooledCommitted
	repairs   []PooledRepair
	liveSeen  map[uint64]int
}

// OpenPooledBrowser connects a browser to one replica as tenant.
func OpenPooledBrowser(tb TB, ctx context.Context, f *PooledFactory, tenant sessionwire.TenantID, options PooledBrowserOptions) *PooledBrowser {
	tb.Helper()
	b := &PooledBrowser{Tenant: tenant, factory: f, liveSeen: map[uint64]int{}}
	config := centrifugego.Config{
		Token: PooledBearers[tenant],
		Data:  []byte(`{"protocol_version":"1"}`),
		Header: http.Header{
			"Authorization": {"Bearer " + PooledBearers[tenant]},
			"Origin":        {f.BaseURL},
		},
		Name:             "orchestrationtest-pooled-browser",
		HandshakeTimeout: 10 * time.Second,
		LogLevel:         centrifugego.LogLevelNone,
	}
	if options.Stallable || options.ReadBufferBytes > 0 {
		b.gate = &stallGate{wake: make(chan struct{})}
		close(b.gate.wake)
		gate, bufferBytes := b.gate, options.ReadBufferBytes
		config.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			var dialer net.Dialer
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if tcp, ok := conn.(*net.TCPConn); ok && bufferBytes > 0 {
				if err := tcp.SetReadBuffer(bufferBytes); err != nil {
					_ = conn.Close()
					return nil, err
				}
			}
			b.mu.Lock()
			b.accepts++
			b.mu.Unlock()
			return &stallConn{Conn: conn, gate: gate}, nil
		}
	}
	endpoint := "ws" + strings.TrimPrefix(f.BaseURL, "http") + "/v1/realtime"
	client := centrifugego.NewJsonClient(endpoint, config)
	connected := make(chan struct{}, 1)
	client.OnConnected(func(centrifugego.ConnectedEvent) {
		select {
		case connected <- struct{}{}:
		default:
		}
	})
	client.OnConnecting(func(e centrifugego.ConnectingEvent) {
		b.note(PooledBrowserEntry{Kind: "connecting", Code: e.Code, Reason: e.Reason})
	})
	client.OnDisconnected(func(e centrifugego.DisconnectedEvent) {
		b.note(PooledBrowserEntry{Kind: "disconnected", Code: e.Code, Reason: e.Reason})
	})
	b.client = client
	if err := client.Connect(); err != nil {
		client.Close()
		tb.Fatalf("orchestrationtest: the browser for %q could not connect: %v", tenant, err)
		return nil
	}
	tb.Cleanup(b.Close)
	select {
	case <-connected:
	case <-ctx.Done():
		tb.Fatalf("orchestrationtest: the browser for %q did not connect: %v", tenant, ctx.Err())
		return nil
	}
	return b
}

func (b *PooledBrowser) note(entry PooledBrowserEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.log = append(b.log, entry)
}

// Watch subscribes to one session and fixes the browser's starting position:
// resumeFrom is the sequence it already holds everything through -- the OLD
// CURSOR of a browser coming back, or zero for one that holds nothing. It
// returns the subscribe's refusal rather than failing on it.
//
// A subscription the SERVER ends is re-subscribed at once, as a browser's join
// does; the re-subscribe is logged and repaired like any other.
func (b *PooledBrowser) Watch(tb TB, ctx context.Context, s sessionwire.SessionID, resumeFrom uint64) error {
	tb.Helper()
	b.Session, b.position = s, resumeFrom
	sub, err := b.client.NewSubscription(ClientLinkChannel(b.Tenant, s))
	if err != nil {
		tb.Fatalf("orchestrationtest: building a browser subscription: %v", err)
		return nil
	}
	b.sub = sub
	answered := make(chan error, 1)
	sub.OnSubscribed(func(centrifugego.SubscribedEvent) {
		b.note(PooledBrowserEntry{Kind: "subscribed"})
		select {
		case answered <- nil:
		default:
		}
	})
	sub.OnSubscribing(func(e centrifugego.SubscribingEvent) {
		b.note(PooledBrowserEntry{Kind: "subscribing", Code: e.Code, Reason: e.Reason})
	})
	sub.OnUnsubscribed(func(e centrifugego.UnsubscribedEvent) {
		b.note(PooledBrowserEntry{Kind: "unsubscribed", Code: e.Code, Reason: e.Reason})
		b.mu.Lock()
		closed := b.closed
		b.mu.Unlock()
		// 2000-2499 is the server's "unsubscribed, do not come back on your
		// own" band (Factory's CloseSession uses 2000); the library
		// re-subscribes by itself only at 2500 and above.
		if !closed && e.Code >= 2000 && e.Code < 2500 {
			go func() { _ = sub.Subscribe() }()
		}
	})
	sub.OnError(func(e centrifugego.SubscriptionErrorEvent) {
		select {
		case answered <- e.Error:
		default:
		}
	})
	sub.OnPublication(func(e centrifugego.PublicationEvent) {
		b.note(browserEntryOf(e.Data, b.Tenant, s))
	})
	if err := sub.Subscribe(); err != nil {
		tb.Fatalf("orchestrationtest: browser subscribing: %v", err)
		return nil
	}
	select {
	case err := <-answered:
		return err
	case <-ctx.Done():
		tb.Fatalf("orchestrationtest: the browser's subscribe was never answered: %v", ctx.Err())
		return nil
	}
}

// browserEntryOf decodes one session-channel record.
func browserEntryOf(data []byte, tenant sessionwire.TenantID, s sessionwire.SessionID) PooledBrowserEntry {
	recordType, err := sessionwire.SessionRecordTypeOf(data)
	if err != nil {
		return PooledBrowserEntry{Kind: "?", Reason: string(data)}
	}
	switch recordType {
	case sessionwire.SessionRecordTypeEnduringPublication:
		var p sessionwire.EnduringPublication
		if err := p.UnmarshalJSON(data); err != nil {
			return PooledBrowserEntry{Kind: "?", Reason: string(data)}
		}
		return PooledBrowserEntry{Kind: "E", EventID: p.EventID, Seq: p.JournalSeq, Stray: p.TenantID != tenant || p.SessionID != s}
	case sessionwire.SessionRecordTypeSessionReset:
		var r sessionwire.SessionReset
		if err := r.UnmarshalJSON(data); err != nil {
			return PooledBrowserEntry{Kind: "?", Reason: string(data)}
		}
		return PooledBrowserEntry{Kind: "R", Seq: r.LastContiguous, Tip: r.JournalTip, Stray: r.TenantID != tenant || r.SessionID != s}
	case sessionwire.SessionRecordTypeEphemeralPublication:
		var x sessionwire.EphemeralPublication
		if err := x.UnmarshalJSON(data); err != nil {
			return PooledBrowserEntry{Kind: "?", Reason: string(data)}
		}
		// A case numbers its ephemeral bodies {"n":...}; the number is carried
		// in Seq so order and uniqueness can be read off the log.
		var numbered struct {
			N uint64 `json:"n"`
		}
		_ = json.Unmarshal(x.Body, &numbered)
		return PooledBrowserEntry{Kind: "X", Seq: numbered.N, Stray: x.TenantID != tenant || x.SessionID != s}
	case sessionwire.SessionRecordTypeJournalTip:
		var t sessionwire.JournalTip
		if err := t.UnmarshalJSON(data); err != nil {
			return PooledBrowserEntry{Kind: "?", Reason: string(data)}
		}
		return PooledBrowserEntry{Kind: "T", Tip: t.Tip, Stray: t.TenantID != tenant || t.SessionID != s}
	}
	return PooledBrowserEntry{Kind: "?", Reason: string(recordType)}
}

// Stall stops this browser reading its socket. Stallable browsers only.
func (b *PooledBrowser) Stall() { b.gate.stall() }

// Release lets a stalled browser read again.
func (b *PooledBrowser) Release() { b.gate.release() }

// Dials reports how many TCP connections this browser has opened. A reconnect
// is a second dial, so a value above one is the transport closing the link.
// Stallable browsers only.
func (b *PooledBrowser) Dials() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.accepts
}

// Close disconnects this browser. It is idempotent.
func (b *PooledBrowser) Close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	if b.gate != nil {
		b.gate.release()
	}
	b.client.Close()
}

// Log reports everything that has happened to this browser, in order.
func (b *PooledBrowser) Log() []PooledBrowserEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]PooledBrowserEntry(nil), b.log...)
}

// LiveEnduring reports every enduring publication that arrived LIVE, in order.
func (b *PooledBrowser) LiveEnduring() []PooledCommitted {
	var out []PooledCommitted
	for _, entry := range b.Log() {
		if entry.Kind == "E" {
			out = append(out, PooledCommitted{EventID: entry.EventID, JournalSeq: entry.Seq})
		}
	}
	return out
}

// Ephemeral reports the numbers of every ephemeral publication that arrived,
// in arrival order.
func (b *PooledBrowser) Ephemeral() []uint64 {
	var out []uint64
	for _, entry := range b.Log() {
		if entry.Kind == "X" {
			out = append(out, entry.Seq)
		}
	}
	return out
}

// Resets reports every session.reset this browser received.
func (b *PooledBrowser) Resets() []PooledBrowserEntry {
	var out []PooledBrowserEntry
	for _, entry := range b.Log() {
		if entry.Kind == "R" {
			out = append(out, entry)
		}
	}
	return out
}

// Settle applies every entry logged since the last call, repairing as it goes,
// and returns what the browser now holds and the position it holds it through.
//
// It FAILS the case on a SILENT GAP -- an enduring record beyond the next
// sequence that no reset or subscribe announced -- and on a stray record naming
// another session, because either is the failure every realtime case exists to
// catch. It does NOT fail on a live record the browser already holds: a
// browser subscribes before it reads, so an overlap between the two is the
// design, and skipping it is the client's job.
func (b *PooledBrowser) Settle(tb TB, ctx context.Context) ([]PooledCommitted, uint64) {
	tb.Helper()
	log := b.Log()
	for ; b.processed < len(log); b.processed++ {
		entry := log[b.processed]
		if entry.Stray {
			tb.Fatalf("orchestrationtest: the browser for %s/%s received a record naming another session: %+v", b.Tenant, b.Session, entry)
			return nil, 0
		}
		switch entry.Kind {
		case "E":
			b.liveSeen[entry.Seq]++
			switch {
			case entry.Seq <= b.position:
				// Already held: the overlap between a subscribe and its read.
			case b.started && entry.Seq == b.position+1:
				b.applied = append(b.applied, PooledCommitted{EventID: entry.EventID, JournalSeq: entry.Seq})
				b.position = entry.Seq
			default:
				tb.Fatalf("orchestrationtest: SILENT GAP -- the browser for %s/%s holds through %d and received %s at %d with nothing announcing the gap (log %v)",
					b.Tenant, b.Session, b.position, entry.EventID, entry.Seq, log)
				return nil, 0
			}
		case "R":
			b.repair(tb, ctx)
		case "subscribed":
			// Every (re)subscribe is a moment the browser cannot know what it
			// missed, so it reads the journal from where it is.
			b.repair(tb, ctx)
			b.started = true
		}
	}
	return append([]PooledCommitted(nil), b.applied...), b.position
}

// repair reads Factory's journal route from the browser's position to the
// captured tip, following cursors, and applies what it finds.
func (b *PooledBrowser) repair(tb TB, ctx context.Context) {
	tb.Helper()
	read := ReadPooledJournalFrom(tb, ctx, b.factory, b.Tenant, b.Session, b.position+1)
	for _, event := range read.Events {
		if event.JournalSeq <= b.position {
			continue
		}
		b.applied = append(b.applied, event)
		b.position = event.JournalSeq
	}
	if read.CoveredThrough > b.position {
		b.position = read.CoveredThrough
	}
	b.repairs = append(b.repairs, read)
}

// Repairs reports every journal read Settle made, in order.
func (b *PooledBrowser) Repairs() []PooledRepair {
	return append([]PooledRepair(nil), b.repairs...)
}

// LiveDuplicates reports every sequence delivered LIVE more than once. Only
// entries Settle has walked are counted.
func (b *PooledBrowser) LiveDuplicates() map[uint64]int {
	out := map[uint64]int{}
	for seq, count := range b.liveSeen {
		if count > 1 {
			out[seq] = count
		}
	}
	return out
}

// ReadPooledJournalFrom is a browser's durable repair: it reads one session's
// journal through a replica's HTTP route from fromSeq, following next_cursor
// until the store names no continuation, and returns every public event and
// the sequence the reads cover through.
func ReadPooledJournalFrom(tb TB, ctx context.Context, f *PooledFactory, tenant sessionwire.TenantID, s sessionwire.SessionID, fromSeq uint64) PooledRepair {
	tb.Helper()
	out := PooledRepair{From: fromSeq}
	query := url.Values{"from_seq": {strconv.FormatUint(fromSeq, 10)}}
	for {
		status, body := f.Get(tb, ctx, tenant, "/v1/sessions/"+string(s)+"/journal?"+query.Encode())
		if status != http.StatusOK {
			tb.Fatalf("orchestrationtest: the journal read of %s/%s from %d answered %d: %s", tenant, s, fromSeq, status, body)
			return out
		}
		var page sessionwire.JournalPage
		if err := json.Unmarshal(body, &page); err != nil {
			tb.Fatalf("orchestrationtest: the journal page is not a Core JournalPage: %v", err)
			return out
		}
		out.Pages++
		for _, event := range page.Events {
			out.Events = append(out.Events, PooledCommitted{EventID: event.EventID, JournalSeq: event.JournalSeq})
		}
		if page.CoveredThrough > out.CoveredThrough {
			out.CoveredThrough = page.CoveredThrough
		}
		if page.NextCursor == "" {
			return out
		}
		if out.Pages > 10_000 {
			tb.Fatalf("orchestrationtest: the journal read of %s/%s did not terminate", tenant, s)
			return out
		}
		query = url.Values{"cursor": {string(page.NextCursor)}}
	}
}

// ---- a socket that can stop being read -------------------------------------

type stallGate struct {
	mu      sync.Mutex
	stalled bool
	wake    chan struct{}
}

func (g *stallGate) stall() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.stalled {
		g.stalled, g.wake = true, make(chan struct{})
	}
}

func (g *stallGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stalled {
		g.stalled = false
		close(g.wake)
	}
}

func (g *stallGate) wait() {
	g.mu.Lock()
	wake := g.wake
	g.mu.Unlock()
	<-wake
}

// stallConn is a TCP connection whose Read waits while its gate is stalled, so
// the bytes the server sends pile up in the kernel and then in the server's
// own per-connection queue -- which is the slow consumer the transport's
// threshold exists for.
type stallConn struct {
	net.Conn
	gate *stallGate
}

func (c *stallConn) Read(p []byte) (int, error) {
	c.gate.wait()
	return c.Conn.Read(p)
}

// String renders a browser's log compactly for failure messages.
func (e PooledBrowserEntry) String() string {
	switch e.Kind {
	case "E":
		return fmt.Sprintf("E%d", e.Seq)
	case "R":
		return fmt.Sprintf("R%d/%d", e.Seq, e.Tip)
	case "T":
		return fmt.Sprintf("T%d", e.Tip)
	case "X":
		return fmt.Sprintf("X%d", e.Seq)
	default:
		if e.Code != 0 {
			return fmt.Sprintf("%s(%d %s)", e.Kind, e.Code, e.Reason)
		}
		return e.Kind
	}
}
