//go:build integration

package orchestrationtest

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// HostLinkTap is a PASSIVE observer in front of a real Host's Handler.
//
// It exists because every property this lane has to prove about a Factory ↔
// Host connection lives on the wire and nowhere else a caller outside Factory
// can reach. Factory's HostLink client is internal/, its retained capability set
// is an unexported field, and Factory deliberately discards a bind or delivery
// failure (routing.Demand.bindLocked, httpapi.deliverAdmitted). So the tap is
// the only witness to what Factory actually sent and what Host actually said.
//
// PASSIVE IS THE WHOLE CONTRACT. It reads the upgrade request's headers, lets
// the real Handler answer, and copies the bytes of the hijacked connection in
// both directions. It rewrites nothing: the B8 reviewers' harness had to add
// ?format=json to reach Host at all, which is exactly the shim this lane's test
// must not have. A frame this tap cannot parse fails the case rather than being
// skipped.
type HostLinkTap struct {
	mu    sync.Mutex
	conns []*TappedConn
	next  int
}

// NewHostLinkTap returns an empty tap.
func NewHostLinkTap() *HostLinkTap { return &HostLinkTap{} }

// TappedConn is one HTTP request that reached the Host through the tap.
type TappedConn struct {
	ID           int
	Path         string
	Subprotocols []string // the request's Sec-WebSocket-Protocol values

	mu sync.Mutex
	// requestHead is set on a connection tapped at the LISTENER (see
	// WrapListener): its client bytes begin with the HTTP request head, which
	// must be read before any frame. plain marks a connection whose answer was
	// not 101: it carries HTTP, not WebSocket frames, and is not parsed further.
	requestHead bool
	plain       bool
	status      int         // the status line Host WROTE, read off the connection
	headers     http.Header // the response headers Host wrote
	toHost      frameReader
	fromHost    frameReader
	messages    []TappedMessage
	faults      []string
}

// TappedMessage is one WebSocket data message, or a close, in one direction.
type TappedMessage struct {
	FromHost  bool
	Close     bool
	CloseCode int
	Reason    string
	Payload   []byte
}

// Wrap puts the tap in front of next.
func (t *HostLinkTap) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.mu.Lock()
		t.next++
		conn := &TappedConn{ID: t.next, Path: r.URL.Path, Subprotocols: subprotocols(r.Header)}
		t.conns = append(t.conns, conn)
		t.mu.Unlock()
		next.ServeHTTP(&tapWriter{ResponseWriter: w, conn: conn}, r)
	})
}

// Conns reports every request the tap saw, in arrival order.
func (t *HostLinkTap) Conns() []*TappedConn {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*TappedConn(nil), t.conns...)
}

func subprotocols(header http.Header) []string {
	var out []string
	for _, value := range header.Values("Sec-WebSocket-Protocol") {
		for _, part := range strings.Split(value, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

// Status is the HTTP status Host answered this request with. For an upgrade it
// is read from the bytes Host wrote to the hijacked connection, not inferred
// from the fact of a hijack: a hijack is the MEANS of answering 101, not proof
// of having done so.
func (c *TappedConn) Status() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}

// ResponseHeader is the header Host wrote with its status.
func (c *TappedConn) ResponseHeader() http.Header {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.headers.Clone()
}

// Messages reports every parsed message on this connection, in wire order per
// direction.
func (c *TappedConn) Messages() []TappedMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]TappedMessage(nil), c.messages...)
}

// Faults reports anything the tap could not parse. A non-empty answer means the
// observation is incomplete and a case must not draw conclusions from it.
func (c *TappedConn) Faults() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.faults...)
}

// tapWriter records a non-upgrade status and hands out a tapped connection on
// Hijack.
type tapWriter struct {
	http.ResponseWriter
	conn *TappedConn
}

func (w *tapWriter) WriteHeader(code int) {
	w.conn.mu.Lock()
	if w.conn.status == 0 {
		w.conn.status = code
		w.conn.headers = w.ResponseWriter.Header().Clone()
	}
	w.conn.mu.Unlock()
	w.ResponseWriter.WriteHeader(code)
}

func (w *tapWriter) Write(p []byte) (int, error) {
	w.conn.mu.Lock()
	if w.conn.status == 0 {
		w.conn.status = http.StatusOK
		w.conn.headers = w.ResponseWriter.Header().Clone()
	}
	w.conn.mu.Unlock()
	return w.ResponseWriter.Write(p)
}

func (w *tapWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	raw, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return raw, rw, err
	}
	tapped := &tappedNetConn{Conn: raw, conn: w.conn}
	// THE READWRITER IS REPLACED, NOT PASSED THROUGH, and that is load-bearing.
	// The server's bufio.Reader reads the RAW connection, and centrifuge's
	// upgrader reuses it as the connection's read buffer whenever it is large
	// enough (centrifuge@v0.38.0/internal/websocket/server.go:262-264). Handing
	// it back would route every client frame around this tap and leave the
	// Factory-to-Host direction silently unobserved. Bytes the server had
	// already buffered are the client's too: they are observed here and
	// replayed ahead of the tapped connection.
	var reader io.Reader = tapped
	if rw != nil && rw.Reader.Buffered() > 0 {
		pending, _ := rw.Reader.Peek(rw.Reader.Buffered())
		pending = append([]byte(nil), pending...)
		w.conn.observe(false, pending)
		reader = io.MultiReader(bytes.NewReader(pending), tapped)
	}
	return tapped, bufio.NewReadWriter(bufio.NewReaderSize(reader, 4096), bufio.NewWriterSize(tapped, 4096)), nil
}

// tappedNetConn copies every byte in both directions into the frame readers.
type tappedNetConn struct {
	net.Conn
	conn *TappedConn
}

func (c *tappedNetConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.conn.observe(false, p[:n])
	}
	return n, err
}

func (c *tappedNetConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.conn.observe(true, p[:n])
	}
	return n, err
}

// observe feeds one direction's bytes to its parser.
func (c *TappedConn) observe(fromHost bool, p []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.plain {
		return
	}
	if fromHost {
		c.fromHost.buf = append(c.fromHost.buf, p...)
		if c.status == 0 {
			if !c.parseResponseHead() {
				return
			}
			if c.requestHead && c.status != http.StatusSwitchingProtocols {
				c.plain = true
				c.fromHost.buf, c.toHost.buf = nil, nil
				return
			}
		}
		c.drain(&c.fromHost, true)
		return
	}
	c.toHost.buf = append(c.toHost.buf, p...)
	if c.requestHead {
		end := bytes.Index(c.toHost.buf, []byte("\r\n\r\n"))
		if end < 0 {
			return
		}
		head := string(c.toHost.buf[:end])
		c.toHost.buf = c.toHost.buf[end+4:]
		c.requestHead = false
		lines := strings.Split(head, "\r\n")
		if fields := strings.Fields(lines[0]); len(fields) >= 2 {
			c.Path = fields[1]
		}
		header := http.Header{}
		for _, line := range lines[1:] {
			if name, value, found := strings.Cut(line, ":"); found {
				header.Add(strings.TrimSpace(name), strings.TrimSpace(value))
			}
		}
		c.Subprotocols = subprotocols(header)
	}
	c.drain(&c.toHost, false)
}

// WrapListener taps every connection ln accepts, below HTTP.
//
// It exists for a server whose handler cannot be wrapped -- factory.Server
// serves its own router on the listener it is handed -- and it is as PASSIVE
// as Wrap: every byte is copied, none is changed. "FromHost" then means "from
// the SERVER", whichever service that is. A connection answered with anything
// but 101 is plain HTTP and is recorded with its status only.
func (t *HostLinkTap) WrapListener(ln net.Listener) net.Listener {
	return &tapListener{Listener: ln, tap: t}
}

type tapListener struct {
	net.Listener
	tap *HostLinkTap
}

func (l *tapListener) Accept() (net.Conn, error) {
	raw, err := l.Listener.Accept()
	if err != nil {
		return raw, err
	}
	l.tap.mu.Lock()
	l.tap.next++
	conn := &TappedConn{ID: l.tap.next, requestHead: true}
	l.tap.conns = append(l.tap.conns, conn)
	l.tap.mu.Unlock()
	return &tappedNetConn{Conn: raw, conn: conn}, nil
}

// Upgraded reports whether this connection was answered 101: only an upgraded
// connection carries frames.
func (c *TappedConn) Upgraded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status == http.StatusSwitchingProtocols
}

// RequestPath is the request path, read off the wire for a listener-tapped
// connection.
func (c *TappedConn) RequestPath() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Path
}

// parseResponseHead reads the HTTP status line and headers Host wrote to the
// hijacked connection, reporting whether they are complete.
func (c *TappedConn) parseResponseHead() bool {
	end := bytes.Index(c.fromHost.buf, []byte("\r\n\r\n"))
	if end < 0 {
		return false
	}
	head := string(c.fromHost.buf[:end])
	c.fromHost.buf = c.fromHost.buf[end+4:]
	lines := strings.Split(head, "\r\n")
	fields := strings.SplitN(lines[0], " ", 3)
	if len(fields) < 2 {
		c.faults = append(c.faults, "unparseable status line "+strconv.Quote(lines[0]))
		c.status = -1
		return true
	}
	code, err := strconv.Atoi(fields[1])
	if err != nil {
		c.faults = append(c.faults, "unparseable status code "+strconv.Quote(lines[0]))
		c.status = -1
		return true
	}
	c.status = code
	c.headers = http.Header{}
	for _, line := range lines[1:] {
		name, value, found := strings.Cut(line, ":")
		if found {
			c.headers.Add(strings.TrimSpace(name), strings.TrimSpace(value))
		}
	}
	return true
}

// frameReader is one direction's RFC 6455 frame parser state.
type frameReader struct {
	buf      []byte
	partial  []byte
	fragment bool
}

// drain parses every complete frame in r.buf.
func (c *TappedConn) drain(r *frameReader, fromHost bool) {
	for {
		payload, opcode, fin, rsv, consumed, err := nextFrame(r.buf)
		if err != nil {
			c.faults = append(c.faults, err.Error())
			r.buf = nil
			return
		}
		if consumed == 0 {
			return
		}
		r.buf = r.buf[consumed:]
		if rsv != 0 {
			// permessage-deflate (RSV1) or an extension this tap cannot read.
			c.faults = append(c.faults, fmt.Sprintf("frame with RSV bits %#x: compressed or extended, unreadable", rsv))
			continue
		}
		switch opcode {
		case 0x0: // continuation
			if !r.fragment {
				c.faults = append(c.faults, "continuation frame with no message in progress")
				continue
			}
			r.partial = append(r.partial, payload...)
			if fin {
				c.messages = append(c.messages, TappedMessage{FromHost: fromHost, Payload: r.partial})
				r.partial, r.fragment = nil, false
			}
		case 0x1, 0x2: // text, binary
			if fin {
				c.messages = append(c.messages, TappedMessage{FromHost: fromHost, Payload: append([]byte(nil), payload...)})
				continue
			}
			r.partial, r.fragment = append([]byte(nil), payload...), true
		case 0x8: // close
			message := TappedMessage{FromHost: fromHost, Close: true}
			if len(payload) >= 2 {
				message.CloseCode = int(binary.BigEndian.Uint16(payload[:2]))
				message.Reason = string(payload[2:])
			}
			c.messages = append(c.messages, message)
		case 0x9, 0xA: // ping, pong: transport liveness, not HostLink
		default:
			c.faults = append(c.faults, fmt.Sprintf("unknown opcode %#x", opcode))
		}
	}
}

// nextFrame parses one frame from buf, reporting consumed == 0 when buf holds
// less than a whole frame.
func nextFrame(buf []byte) (payload []byte, opcode byte, fin bool, rsv byte, consumed int, err error) {
	if len(buf) < 2 {
		return nil, 0, false, 0, 0, nil
	}
	fin = buf[0]&0x80 != 0
	rsv = buf[0] & 0x70
	opcode = buf[0] & 0x0F
	masked := buf[1]&0x80 != 0
	length := uint64(buf[1] & 0x7F)
	offset := 2
	switch length {
	case 126:
		if len(buf) < offset+2 {
			return nil, 0, false, 0, 0, nil
		}
		length = uint64(binary.BigEndian.Uint16(buf[offset:]))
		offset += 2
	case 127:
		if len(buf) < offset+8 {
			return nil, 0, false, 0, 0, nil
		}
		length = binary.BigEndian.Uint64(buf[offset:])
		offset += 8
	}
	if length > 16<<20 {
		return nil, 0, false, 0, 0, fmt.Errorf("frame of %d bytes exceeds the tap's bound", length)
	}
	var key [4]byte
	if masked {
		if len(buf) < offset+4 {
			return nil, 0, false, 0, 0, nil
		}
		copy(key[:], buf[offset:offset+4])
		offset += 4
	}
	end := offset + int(length)
	if len(buf) < end {
		return nil, 0, false, 0, 0, nil
	}
	payload = append([]byte(nil), buf[offset:end]...)
	if masked {
		for i := range payload {
			payload[i] ^= key[i%4]
		}
	}
	return payload, opcode, fin, rsv, end, nil
}

// ---------------------------------------------------------------------------
// Centrifuge's JSON client protocol, as it appears in a tapped message.
// ---------------------------------------------------------------------------

// WireCommand is one Centrifuge JSON command a client sent.
type WireCommand struct {
	ID      uint32 `json:"id"`
	Connect *struct {
		Token string          `json:"token"`
		Data  json.RawMessage `json:"data"`
	} `json:"connect,omitempty"`
	RPC *struct {
		Method string          `json:"method"`
		Data   json.RawMessage `json:"data"`
	} `json:"rpc,omitempty"`
	Subscribe *struct {
		Channel string `json:"channel"`
	} `json:"subscribe,omitempty"`
}

// WireReply is one Centrifuge JSON reply a server sent.
type WireReply struct {
	ID    uint32 `json:"id"`
	Error *struct {
		Code    uint32 `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
	Connect *struct {
		Data json.RawMessage `json:"data"`
	} `json:"connect,omitempty"`
	RPC *struct {
		Data json.RawMessage `json:"data"`
	} `json:"rpc,omitempty"`
}

// ErrTapIncomplete reports a tapped connection whose observation cannot be
// trusted: a frame was unreadable.
var ErrTapIncomplete = errors.New("orchestrationtest: the tap could not read every frame")

// Commands decodes every command the client sent on this connection.
func (c *TappedConn) Commands() ([]WireCommand, error) {
	return decodeLines[WireCommand](c, false)
}

// Replies decodes every reply the Host sent on this connection.
func (c *TappedConn) Replies() ([]WireReply, error) {
	return decodeLines[WireReply](c, true)
}

// decodeLines splits each message into Centrifuge's newline-separated JSON
// objects. An empty object is a transport ping and is dropped.
func decodeLines[T any](c *TappedConn, fromHost bool) ([]T, error) {
	if faults := c.Faults(); len(faults) != 0 {
		return nil, fmt.Errorf("%w: %v", ErrTapIncomplete, faults)
	}
	var out []T
	for _, message := range c.Messages() {
		if message.FromHost != fromHost || message.Close {
			continue
		}
		for _, line := range bytes.Split(message.Payload, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if len(line) == 0 || bytes.Equal(line, []byte("{}")) {
				continue
			}
			var value T
			if err := json.Unmarshal(line, &value); err != nil {
				return nil, fmt.Errorf("orchestrationtest: an unparseable centrifuge line %q: %w", line, err)
			}
			out = append(out, value)
		}
	}
	return out, nil
}

// ReplyTo returns the Host's reply to command id, if it has been seen.
func ReplyTo(replies []WireReply, id uint32) (WireReply, bool) {
	for _, reply := range replies {
		if reply.ID == id {
			return reply, true
		}
	}
	return WireReply{}, false
}

// TapOf views one tapped connection as a tap of its own, so a case can read a
// single connection with the same helpers it reads a whole tap with.
func TapOf(conn *TappedConn) *HostLinkTap {
	return &HostLinkTap{conns: []*TappedConn{conn}, next: conn.ID}
}

// TapOfConns views several tapped connections as a tap of their own.
func TapOfConns(conns []*TappedConn) *HostLinkTap {
	tap := &HostLinkTap{conns: append([]*TappedConn(nil), conns...)}
	if len(conns) > 0 {
		tap.next = conns[len(conns)-1].ID
	}
	return tap
}
