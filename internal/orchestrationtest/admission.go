//go:build integration

package orchestrationtest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/factory"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/runtimecommand"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/sessionstore"
)

// This file is the kit's support for runbook task I1.2 -- admission, crash and
// two-Factory ordering. Everything in it is either a way to COMPOSE a real
// replica differently (a crash point in its durable command plane, a sweep it
// does not run) or a way to READ real evidence (the runtime's own harness
// journal). Nothing here answers for the thing under test.

// ---- composition -----------------------------------------------------------

// PooledFactoryConfig chooses what one pooled replica is composed with.
//
// Every zero member takes the same default StartPooledFactory uses, so a case
// states only the collaborator its claim is about.
type PooledFactoryConfig struct {
	Replica string
	Logs    io.Writer

	// Commands is the durable command plane. Nil composes the real Store. A
	// non-nil value must still be the real Store underneath -- see
	// StallAfterCommit -- or the case is testing the kit.
	Commands factory.Commands

	// WithoutPendingCommands composes NO pending placement. Such a replica
	// admits and serves reads but never places anything: it is the mutant a
	// recovery case uses to prove its recovery came from a replica's sweep and
	// not from something else in the world.
	WithoutPendingCommands bool

	// ApplyDeadline, when positive, overrides ReconcileLimits.ApplyDeadline,
	// the bound this replica stamps on every command it admits.
	ApplyDeadline time.Duration

	// Pending is the pending-command reader placement sweeps. Nil composes
	// the real Store.
	Pending factory.PendingCommands

	// Directory, when set, wraps the replica's real store directory (latency
	// injection, observation). The wrapped value is what PooledFactory
	// reports as its Directory.
	Directory func(factory.Directory) factory.Directory

	// ServiceToken is the HostLink credential this replica presents. Empty
	// takes PooledServiceToken, which the world's Hosts accept; anything else
	// is refused by every Host.
	ServiceToken string

	// DemandTimeout bounds one subscriber-demand poll. Zero takes Factory's
	// default.
	DemandTimeout time.Duration

	// Interval and ClaimTTL override the sweep cadence and the claim
	// lifetime. Zero takes ReconcileSweepInterval and ReconcileClaimTTL.
	Interval time.Duration
	ClaimTTL time.Duration

	// PerConnectionQueueBytes overrides the ClientLink's per-connection
	// outbound queue budget, in BYTES. Zero takes Factory's default. It is how
	// a case configures the slow-consumer threshold it then measures.
	PerConnectionQueueBytes int

	// WriteTimeout, PingInterval and PongTimeout override the ClientLink's
	// liveness bounds. Zero takes Factory's default. Factory requires
	// WriteTimeout <= PongTimeout < PingInterval. A slow-consumer case raises
	// them so that the QUEUE BUDGET is the only bound its stall can reach.
	WriteTimeout time.Duration
	PingInterval time.Duration
	PongTimeout  time.Duration
}

// StartPooledFactoryWith composes, starts and serves a real pooled Factory
// under cfg.
func StartPooledFactoryWith(tb TB, ctx context.Context, world *PooledWorld, cfg PooledFactoryConfig) *PooledFactory {
	tb.Helper()
	return startPooledFactory(tb, ctx, world, cfg, sessionwire.HostPlacementPooled, nil)
}

// StartPooledHostWith is StartPooledHost with wrap in front of the Host's real
// Routes(). It is how a case puts a wire fault -- ReplyDropper -- between a
// real Factory and a real Host without either knowing.
func StartPooledHostWith(tb TB, ctx context.Context, world *PooledWorld, id sessionwire.HostID, generation uint64, wrap func(http.Handler) http.Handler) *PooledHost {
	tb.Helper()
	return startHostWrapped(tb, ctx, world, id, generation, "", 0, wrap)
}

// StartPooledHostSized is StartPooledHostWith with the Host's advertised
// capacity chosen. Placement skips a Host with no available capacity and ranks
// the rest by free capacity, so a Host of capacity one that already holds a
// session is how a case puts its NEXT session on a different Host without any
// placement seam.
func StartPooledHostSized(tb TB, ctx context.Context, world *PooledWorld, id sessionwire.HostID, generation, capacity uint64, wrap func(http.Handler) http.Handler) *PooledHost {
	tb.Helper()
	return startHostWrapped(tb, ctx, world, id, generation, "", capacity, wrap)
}

// PostRaw is Post without a test handle, for use from racing goroutines: a
// transport failure is returned rather than failing the case from a goroutine
// that is not the test's.
func (f *PooledFactory) PostRaw(ctx context.Context, tenant sessionwire.TenantID, path string, body any) (int, []byte, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, f.BaseURL+path, bytes.NewReader(encoded))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+PooledBearers[tenant])
	request.Header.Set("Content-Type", "application/json")
	response, err := f.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(response.Body)
	return response.StatusCode, answer, err
}

// ---- a crash point between commit and forward ------------------------------

// StallAfterCommit is the REAL Store with one crash point: once armed, the
// next AdmitDispositionCommand commits through the real Store and then never
// returns to Factory. It blocks until the request's own context ends and
// answers that context's error.
//
// That is "the process died after the inbox commit and before the forward",
// reproduced at the only place a caller outside Factory can reach. Everything
// Factory does AFTER admission -- its answer, and deliverAdmitted's wake to the
// owning Host -- runs after this call returns, so none of it runs with a
// successful admission behind it: the handler sees a failure, answers a fault,
// and delivers nothing. The durable record exists and nobody was told.
//
// It observes nothing about the command it did not commit. Every byte of the
// record is the real Store's.
type StallAfterCommit struct {
	*sessionstore.Store

	mu        sync.Mutex
	armed     bool
	committed chan sessionstore.DispositionInboxEntry
}

// NewStallAfterCommit wraps store, disarmed.
func NewStallAfterCommit(store *sessionstore.Store) *StallAfterCommit {
	return &StallAfterCommit{Store: store, committed: make(chan sessionstore.DispositionInboxEntry, 1)}
}

// Arm makes the NEXT admission the one that commits and stalls.
func (s *StallAfterCommit) Arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = true
}

// Committed delivers the entry the armed admission committed, once.
func (s *StallAfterCommit) Committed() <-chan sessionstore.DispositionInboxEntry { return s.committed }

// AdmitDispositionCommand satisfies factory.Commands.
func (s *StallAfterCommit) AdmitDispositionCommand(ctx context.Context, req sessionstore.AdmitDispositionCommandRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	s.mu.Lock()
	armed := s.armed
	s.armed = false
	s.mu.Unlock()
	entry, created, err := s.Store.AdmitDispositionCommand(ctx, req)
	if !armed || err != nil {
		return entry, created, err
	}
	s.committed <- entry
	<-ctx.Done()
	return sessionstore.DispositionInboxEntry{}, false, fmt.Errorf("orchestrationtest: the replica died after the inbox commit: %w", ctx.Err())
}

// ---- the runtime's own evidence --------------------------------------------

// JournalApplication is one application prefix in a harness journal: the
// durable correlation of one public CommandID to one runtime command UUID,
// written BEFORE the command's effect.
type JournalApplication struct {
	Seq         uint64
	Application runtimecommand.Application
}

// JournalDisposition is one disposition frame in a harness journal.
type JournalDisposition struct {
	Seq         uint64
	Disposition runtimecommand.CommandDisposition
}

// CommandEvidence is what one runtime session's harness journal says about the
// commands applied to it, in ledger order. It is read with the harness store's
// own privileged replayer -- the reader Host settles from -- so it is the
// runtime's account of what happened, not anything a Factory or Host reported.
type CommandEvidence struct {
	Applications []JournalApplication
	Dispositions []JournalDisposition

	// effectsBy counts the DURABLE EFFECT events -- event.TurnStarted and
	// event.TurnFoldedInto -- caused by the runtime command id, which is how
	// harness correlates an applied input actually reaching a turn.
	//
	// TurnRejected and InputCancelled are also Enduring Reply events caused
	// by a command (harness's resolution of a queued input resolves to
	// exactly one of TurnStarted/TurnFoldedInto/TurnRejected/InputCancelled),
	// but neither is an EFFECT: a command settled `applied` whose input was
	// then rejected or cancelled must not be counted as "has an effect", so
	// they are tracked separately and EffectsOf deliberately excludes them.
	effectsBy   map[uuid.UUID]int
	rejectedBy  map[uuid.UUID]int
	cancelledBy map[uuid.UUID]int
}

// ApplicationsOf returns every application prefix naming command.
func (e CommandEvidence) ApplicationsOf(command sessionwire.CommandID) []JournalApplication {
	var out []JournalApplication
	for _, app := range e.Applications {
		if string(app.Application.CommandID) == string(command) {
			out = append(out, app)
		}
	}
	return out
}

// DispositionsOf returns every disposition frame naming command.
func (e CommandEvidence) DispositionsOf(command sessionwire.CommandID) []JournalDisposition {
	var out []JournalDisposition
	for _, d := range e.Dispositions {
		if string(d.Disposition.CommandID) == string(command) {
			out = append(out, d)
		}
	}
	return out
}

// EffectsOf counts the DURABLE EFFECT events -- TurnStarted and
// TurnFoldedInto -- caused by one runtime command. It deliberately excludes a
// caused TurnRejected or InputCancelled; see RejectedOf and CancelledOf.
func (e CommandEvidence) EffectsOf(runtimeCommand uuid.UUID) int { return e.effectsBy[runtimeCommand] }

// RejectedOf reports whether the runtime command caused a TurnRejected --
// harness refused the queued input rather than starting or folding a turn.
func (e CommandEvidence) RejectedOf(runtimeCommand uuid.UUID) bool {
	return e.rejectedBy[runtimeCommand] > 0
}

// CancelledOf reports whether the runtime command caused an InputCancelled --
// the queued input left the loop's queue without ever resolving to a turn.
func (e CommandEvidence) CancelledOf(runtimeCommand uuid.UUID) bool {
	return e.cancelledBy[runtimeCommand] > 0
}

// ReadCommandEvidence walks one runtime session's harness journal from the
// beginning.
func ReadCommandEvidence(tb TB, world *PooledWorld, tenant sessionwire.TenantID, id uuid.UUID) CommandEvidence {
	tb.Helper()
	replayer, err := world.Journals[tenant].OpenInternalRecordReplayer(id, harnessstore.ReplayRequest{})
	if err != nil {
		tb.Fatalf("orchestrationtest: opening a record replayer for %s: %v", id, err)
		return CommandEvidence{}
	}
	cursor, err := replayer.Open(context.Background(), journal.ReplayRequest{SessionID: id, From: journal.Beginning()})
	if err != nil {
		tb.Fatalf("orchestrationtest: opening a record cursor for %s: %v", id, err)
		return CommandEvidence{}
	}
	defer func() { _ = cursor.Close() }()
	evidence := CommandEvidence{
		effectsBy:   map[uuid.UUID]int{},
		rejectedBy:  map[uuid.UUID]int{},
		cancelledBy: map[uuid.UUID]int{},
	}
	for {
		record, seq, err := cursor.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return evidence
		}
		if err != nil {
			// FAIL CLOSED: a frame this walk cannot read may be the very
			// application a case is counting.
			tb.Fatalf("orchestrationtest: replaying %s at seq %d: %v", id, seq, err)
			return evidence
		}
		switch typed := record.(type) {
		case journal.CommandApplicationRecord:
			evidence.Applications = append(evidence.Applications, JournalApplication{Seq: seq, Application: typed.Application()})
		case journal.CommandDispositionRecord:
			evidence.Dispositions = append(evidence.Dispositions, JournalDisposition{Seq: seq, Disposition: typed.Disposition()})
		case journal.EventRecord:
			ev := typed.Event()
			cause := ev.EventHeader().Cause.CommandID
			if cause.IsZero() {
				continue
			}
			// A queued input resolves to exactly one of TurnStarted,
			// TurnFoldedInto, TurnRejected or InputCancelled. Only the first
			// two are a durable EFFECT; a rejection or cancellation is
			// tracked separately so a caller can fail loud on it instead of
			// mistaking "no effect yet" for "no effect ever".
			switch ev.(type) {
			case event.TurnStarted, event.TurnFoldedInto:
				evidence.effectsBy[cause]++
			case event.TurnRejected:
				evidence.rejectedBy[cause]++
			case event.InputCancelled:
				evidence.cancelledBy[cause]++
			}
		}
	}
}

// UserTextsInLastRequest returns, in order, the text of every user message in
// the MOST RECENT model request -- the conversation exactly as the model was
// last shown it. A command applied twice shows as its words twice; commands
// applied out of order show out of order.
func (l *PooledLLM) UserTextsInLastRequest() []string {
	requests := l.Requests()
	if len(requests) == 0 {
		return nil
	}
	var texts []string
	for _, message := range requests[len(requests)-1].Messages {
		user, ok := message.(*content.UserMessage)
		if !ok {
			continue
		}
		var parts []string
		for _, block := range user.Blocks {
			if text, ok := block.(*content.TextBlock); ok {
				parts = append(parts, text.Text)
			}
		}
		texts = append(texts, strings.Join(parts, " "))
	}
	return texts
}

// ---- a HostLink that loses its replies -------------------------------------

// ReplyDropper sits in front of a real Host's Handler and DELETES the Host's
// reply to every command delivery -- the RPC whose method is a session's
// channel (Core's HostLinkChannelPrefix) -- while passing every other byte in
// both directions untouched. The Host receives and acts on every delivery; the
// Factory never learns that it did.
//
// Unlike HostLinkTap, which is passive by contract, this is a FAULT, and it is
// a separate type so a passive observation can never be mistaken for one.
//
// It records every delivery it saw and whether it removed the reply, so a case
// asserting "the reply was lost and the command was redelivered" asserts it
// from the wire rather than assuming the fault fired.
type ReplyDropper struct {
	mu         sync.Mutex
	deliveries []ObservedDelivery
	faults     []string
	nextConn   int
}

// ObservedDelivery is one command delivery the dropper saw on the wire.
type ObservedDelivery struct {
	Conn         int
	RPCID        uint32
	Channel      string
	CommandID    sessionwire.CommandID
	ReplyDropped bool
	// At is when the delivery reached the Host's side of the wire.
	At time.Time
}

// NewReplyDropper returns a dropper that has seen nothing.
func NewReplyDropper() *ReplyDropper { return &ReplyDropper{} }

// Deliveries reports every delivery seen, in arrival order.
func (d *ReplyDropper) Deliveries() []ObservedDelivery {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]ObservedDelivery(nil), d.deliveries...)
}

// Faults reports anything the dropper could not parse. A non-empty answer
// means a reply may have passed that should have been dropped.
func (d *ReplyDropper) Faults() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.faults...)
}

// Wrap puts the dropper in front of next.
func (d *ReplyDropper) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		d.nextConn++
		id := d.nextConn
		d.mu.Unlock()
		next.ServeHTTP(&dropperWriter{ResponseWriter: w, dropper: d, conn: id}, r)
	})
}

type dropperWriter struct {
	http.ResponseWriter
	dropper *ReplyDropper
	conn    int
}

func (w *dropperWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	raw, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return raw, rw, err
	}
	conn := &droppingConn{Conn: raw, dropper: w.dropper, id: w.conn, pending: map[uint32]int{}}
	// The server's buffered reader is replaced for HostLinkTap's reason: the
	// upgrader reuses it as the connection's read buffer, and bytes already in
	// it would bypass the parse that decides which replies to drop.
	var reader io.Reader = conn
	if rw != nil && rw.Reader.Buffered() > 0 {
		pending, _ := rw.Reader.Peek(rw.Reader.Buffered())
		pending = append([]byte(nil), pending...)
		conn.observeClient(pending)
		reader = io.MultiReader(bytes.NewReader(pending), conn)
	}
	return conn, bufio.NewReadWriter(bufio.NewReaderSize(reader, 4096), bufio.NewWriterSize(conn, 4096)), nil
}

// droppingConn parses client frames on the way in (to learn which RPC ids are
// deliveries) and rewrites server frames on the way out (to remove their
// replies).
type droppingConn struct {
	net.Conn
	dropper *ReplyDropper
	id      int

	mu       sync.Mutex
	inbound  frameReader
	outbound []byte
	headDone bool
	// pending maps a delivery's RPC id to its index in dropper.deliveries.
	pending map[uint32]int
}

func (c *droppingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.observeClient(p[:n])
	}
	return n, err
}

// observeClient records every delivery RPC in the client's bytes. It runs
// BEFORE the Host sees them, so the Host cannot reply to a delivery this conn
// does not yet know about.
func (c *droppingConn) observeClient(p []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inbound.buf = append(c.inbound.buf, p...)
	for {
		payload, opcode, fin, _, consumed, err := nextFrame(c.inbound.buf)
		if err != nil {
			c.fault("client frame: " + err.Error())
			c.inbound.buf = nil
			return
		}
		if consumed == 0 {
			return
		}
		c.inbound.buf = c.inbound.buf[consumed:]
		if (opcode != 0x1 && opcode != 0x2) || !fin {
			if opcode == 0x0 {
				c.fault("fragmented client message")
			}
			continue
		}
		for _, line := range bytes.Split(payload, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			var command WireCommand
			if err := json.Unmarshal(line, &command); err != nil {
				c.fault("client line: " + err.Error())
				continue
			}
			if command.RPC == nil || !strings.HasPrefix(command.RPC.Method, sessionwire.HostLinkChannelPrefix) {
				continue
			}
			var delivery sessionwire.HostLinkCommandDelivery
			_ = json.Unmarshal(command.RPC.Data, &delivery)
			c.dropper.mu.Lock()
			c.pending[command.ID] = len(c.dropper.deliveries)
			c.dropper.deliveries = append(c.dropper.deliveries, ObservedDelivery{
				Conn: c.id, RPCID: command.ID, Channel: command.RPC.Method, CommandID: delivery.CommandID, At: time.Now(),
			})
			c.dropper.mu.Unlock()
		}
	}
}

func (c *droppingConn) fault(what string) {
	c.dropper.mu.Lock()
	c.dropper.faults = append(c.dropper.faults, what)
	c.dropper.mu.Unlock()
}

// Write buffers the Host's bytes and forwards every COMPLETE frame, rewritten
// without the replies to deliveries. It reports the whole of p written: a
// partial frame is held until the rest of it arrives.
func (c *droppingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.outbound = append(c.outbound, p...)
	var out []byte
	if !c.headDone {
		end := bytes.Index(c.outbound, []byte("\r\n\r\n"))
		if end < 0 {
			return len(p), nil
		}
		out = append(out, c.outbound[:end+4]...)
		c.outbound = c.outbound[end+4:]
		c.headDone = true
	}
	for {
		payload, opcode, fin, rsv, consumed, err := nextFrame(c.outbound)
		if err != nil {
			c.fault("server frame: " + err.Error())
			out = append(out, c.outbound...)
			c.outbound = nil
			break
		}
		if consumed == 0 {
			break
		}
		frame := c.outbound[:consumed]
		c.outbound = c.outbound[consumed:]
		if (opcode != 0x1 && opcode != 0x2) || !fin || rsv != 0 {
			out = append(out, frame...)
			continue
		}
		kept, removed := c.withoutDeliveryReplies(payload)
		if !removed {
			out = append(out, frame...)
			continue
		}
		if len(kept) == 0 {
			continue
		}
		out = append(out, encodeServerFrame(opcode, kept)...)
	}
	if len(out) == 0 {
		return len(p), nil
	}
	if _, err := c.Conn.Write(out); err != nil {
		return 0, err
	}
	return len(p), nil
}

// withoutDeliveryReplies removes every reply line answering a known delivery.
// Called with c.mu held.
func (c *droppingConn) withoutDeliveryReplies(payload []byte) ([]byte, bool) {
	lines := bytes.Split(payload, []byte("\n"))
	kept := make([][]byte, 0, len(lines))
	removed := false
	for _, line := range lines {
		trimmed := bytes.TrimSpace(line)
		var reply struct {
			ID uint32 `json:"id"`
		}
		if len(trimmed) > 0 && json.Unmarshal(trimmed, &reply) == nil && reply.ID != 0 {
			if index, ok := c.pending[reply.ID]; ok {
				delete(c.pending, reply.ID)
				c.dropper.mu.Lock()
				c.dropper.deliveries[index].ReplyDropped = true
				c.dropper.mu.Unlock()
				removed = true
				continue
			}
		}
		kept = append(kept, line)
	}
	joined := bytes.Join(kept, []byte("\n"))
	if len(bytes.TrimSpace(joined)) == 0 {
		return nil, removed
	}
	return joined, removed
}

// encodeServerFrame builds one unmasked, final RFC 6455 frame.
func encodeServerFrame(opcode byte, payload []byte) []byte {
	header := []byte{0x80 | opcode}
	switch n := len(payload); {
	case n < 126:
		header = append(header, byte(n))
	case n <= 0xFFFF:
		header = append(header, 126, 0, 0)
		binary.BigEndian.PutUint16(header[2:], uint16(n))
	default:
		header = append(header, 127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(header[2:], uint64(n))
	}
	return append(header, payload...)
}
