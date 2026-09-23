//go:build integration

package orchestrationtest

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// HostLinkInjector sits in front of a REAL Host's handler and can put extra
// session-channel records on the Host→Factory side of every HostLink, as if
// the Host had published them.
//
// # Why a wire injector, and what it stands for
//
// Two I1.4 rows need records a released Host never sends:
//
//   - an EPHEMERAL publication (case 4). host v0.5.0's department seam carries
//     only EnduringPublication (department.RigSession.SubscribeCommitted), so
//     no composition of it can produce one. Core defines the record and Factory's
//     relay classifies it; the injector is the Host-side producer the released
//     Host does not have yet.
//   - a record Factory's relay REFUSES (case 2). A refused frame is the one
//     HostBinding failure a Host can cause that is not a transport fault: the
//     live-tail plane repairs the session's HostBinding (stop the tail, read
//     the tip, rebind, resume, reset every DeliveryBinding) exactly as it does
//     for a lost link.
//
// It does not touch anything the Host sends. An injected record is a whole
// WebSocket text frame written between two of the Host's own frames -- never
// inside one -- carrying one Centrifuge JSON push for the channel, in the shape
// the Host's own node writes (see Push).
type HostLinkInjector struct {
	mu    sync.Mutex
	conns []*injectingConn
}

// NewHostLinkInjector returns an injector with no connections yet.
func NewHostLinkInjector() *HostLinkInjector { return &HostLinkInjector{} }

// Wrap puts the injector in front of next.
func (i *HostLinkInjector) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&injectingWriter{ResponseWriter: w, injector: i}, r)
	})
}

// Live reports how many HostLinks are open and able to carry an injection.
func (i *HostLinkInjector) Live() int {
	i.mu.Lock()
	conns := append([]*injectingConn(nil), i.conns...)
	i.mu.Unlock()
	live := 0
	for _, c := range conns {
		c.mu.Lock()
		if c.headDone && !c.closed {
			live++
		}
		c.mu.Unlock()
	}
	return live
}

// ErrNoHostLink reports an injection with no live HostLink to carry it.
var ErrNoHostLink = errors.New("orchestrationtest: no live HostLink to inject into")

// Push writes one session-channel record to every live HostLink, on the
// session's HostLink channel, and reports how many links carried it. A link
// with no subscription to that channel -- another tenant's, or another
// replica's that is not watching -- is handed a push for a channel it never
// joined, which the Centrifuge client drops.
func (i *HostLinkInjector) Push(tenant sessionwire.TenantID, s sessionwire.SessionID, record []byte) (int, error) {
	push, err := json.Marshal(map[string]any{
		"push": map[string]any{
			"channel": sessionwire.HostLinkChannel(tenant, s),
			"pub":     map[string]any{"data": json.RawMessage(record)},
		},
	})
	if err != nil {
		return 0, err
	}
	frame := textFrame(push)
	i.mu.Lock()
	conns := append([]*injectingConn(nil), i.conns...)
	i.mu.Unlock()
	carried := 0
	for _, c := range conns {
		if c.inject(frame) {
			carried++
		}
	}
	if carried == 0 {
		var states []string
		for _, c := range conns {
			c.mu.Lock()
			states = append(states, fmt.Sprintf("{head=%v closed=%v why=%q left=%d tail=%d}", c.headDone, c.closed, c.why, c.left, len(c.tail)))
			c.mu.Unlock()
		}
		return 0, fmt.Errorf("%w (links %v)", ErrNoHostLink, states)
	}
	return carried, nil
}

// EphemeralRecord encodes one Core ephemeral publication.
func EphemeralRecord(tenant sessionwire.TenantID, s sessionwire.SessionID, body []byte) ([]byte, error) {
	return sessionwire.EphemeralPublication{TenantID: tenant, SessionID: s, Body: body}.MarshalJSON()
}

// textFrame is one unmasked, unfragmented RFC 6455 text frame, which is what a
// server sends.
func textFrame(payload []byte) []byte {
	var head []byte
	switch n := len(payload); {
	case n < 126:
		head = []byte{0x81, byte(n)}
	case n <= 0xFFFF:
		head = []byte{0x81, 126, 0, 0}
		binary.BigEndian.PutUint16(head[2:], uint16(n))
	default:
		head = make([]byte, 10)
		head[0], head[1] = 0x81, 127
		binary.BigEndian.PutUint64(head[2:], uint64(n))
	}
	return append(head, payload...)
}

type injectingWriter struct {
	http.ResponseWriter
	injector *HostLinkInjector
}

func (w *injectingWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	raw, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return raw, rw, err
	}
	conn := &injectingConn{Conn: raw}
	w.injector.mu.Lock()
	w.injector.conns = append(w.injector.conns, conn)
	w.injector.mu.Unlock()
	// The writer is replaced so every byte the Host writes passes through the
	// frame tracker; the reader is kept, including anything it has buffered.
	var reader *bufio.Reader
	if rw != nil {
		reader = rw.Reader
	} else {
		reader = bufio.NewReader(raw)
	}
	return conn, bufio.NewReadWriter(reader, bufio.NewWriterSize(conn, 4096)), nil
}

// injectingConn tracks RFC 6455 frame boundaries in what the Host writes, so an
// injected frame lands only between two of them.
type injectingConn struct {
	net.Conn

	mu       sync.Mutex
	headDone bool   // the HTTP 101 response head has been written
	tail     []byte // the last bytes of an unfinished head, or a partial frame header
	left     uint64 // payload bytes still owed by the current frame
	pending  [][]byte
	closed   bool
	why      string
}

func (c *injectingConn) atBoundary() bool { return c.headDone && c.left == 0 && len(c.tail) == 0 }

func (c *injectingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, err := c.Conn.Write(p)
	c.advance(p[:n])
	if err != nil {
		c.closed = true
		c.why = fmt.Sprintf("host write: n=%d/%d err=%v", n, len(p), err)
		return n, err
	}
	if c.atBoundary() {
		c.flushLocked()
	}
	return n, nil
}

func (c *injectingConn) Close() error {
	c.mu.Lock()
	c.closed = true
	if c.why == "" {
		c.why = "closed"
	}
	c.mu.Unlock()
	return c.Conn.Close()
}

// inject writes frame now if the stream is between frames, or queues it for
// the next boundary. It reports false for a closed connection.
func (c *injectingConn) inject(frame []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.headDone {
		return false
	}
	c.pending = append(c.pending, frame)
	if c.atBoundary() {
		c.flushLocked()
	}
	return !c.closed
}

func (c *injectingConn) flushLocked() {
	if len(c.pending) == 0 {
		return
	}
	// The Host sets a write deadline before each of ITS writes, so the one in
	// force now may have expired long ago. An injected write carries its own,
	// and clears it after: the Host sets a fresh one before its next write.
	_ = c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	defer func() { _ = c.Conn.SetWriteDeadline(time.Time{}) }()
	for len(c.pending) > 0 {
		if _, err := c.Conn.Write(c.pending[0]); err != nil {
			c.closed = true
			c.why = "inject write: " + err.Error()
			c.pending = nil
			return
		}
		c.pending = c.pending[1:]
	}
}

// advance moves the frame tracker over bytes the Host wrote.
func (c *injectingConn) advance(p []byte) {
	for len(p) > 0 {
		if !c.headDone {
			c.tail = append(c.tail, p...)
			end := bytes.Index(c.tail, []byte("\r\n\r\n"))
			if end < 0 {
				return
			}
			rest := c.tail[end+4:]
			c.headDone, c.tail = true, nil
			p = append([]byte(nil), rest...)
			continue
		}
		if c.left > 0 {
			step := uint64(len(p))
			if step > c.left {
				step = c.left
			}
			c.left -= step
			p = p[step:]
			continue
		}
		// A frame header, possibly split across writes.
		c.tail = append(c.tail, p...)
		p = nil
		need := 2
		if len(c.tail) < need {
			return
		}
		length := uint64(c.tail[1] & 0x7F)
		switch length {
		case 126:
			need += 2
		case 127:
			need += 8
		}
		if c.tail[1]&0x80 != 0 {
			need += 4
		}
		if len(c.tail) < need {
			return
		}
		switch length {
		case 126:
			length = uint64(binary.BigEndian.Uint16(c.tail[2:]))
		case 127:
			length = binary.BigEndian.Uint64(c.tail[2:])
		}
		p = append([]byte(nil), c.tail[need:]...)
		c.tail, c.left = nil, length
	}
}
