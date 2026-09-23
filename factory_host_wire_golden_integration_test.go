//go:build integration

// This file is the golden machinery behind factory_host_wire_integration_test.go:
// how a frame read off a real wire becomes a frozen fixture, and how a frozen
// fixture becomes a frame again.
//
// # The fixtures are OUTPUT, never input
//
// Every file under testdata/sessionwire is written by this module's own test
// run, from bytes a real Factory, a real Host or the real controller drain
// client put on a real WebSocket. Nothing in the directory is hand-written. To
// regenerate after an INTENDED wire change:
//
//	make sessionwire-goldens
//
// which runs the producing cases with LOOPRIG_UPDATE_SESSIONWIRE=1 and then
// fails if git sees a diff -- so a regeneration is always a reviewed change,
// and an unintended one cannot slip into a commit by being regenerated. An
// ordinary run compares instead and fails on any drift, so drift fails
// `make test` and `make release-check`. This repository has no CI workflow of
// its own: the gate is whoever runs those targets.
//
// # What normalization removes, and why only that
//
// A frame carries values that differ run to run without the wire changing:
// the loopback port, wall-clock times, centrifuge's client id, derived
// idempotency keys, and counters whose value depends on scheduling (a lease
// epoch, a journal sequence, a centrifuge command id). Those are replaced by
// placeholders -- counters by 0 so the JSON TYPE stays frozen. Everything else
// is literal: every member name, every method name, every enumeration value,
// every refusal code, and every identifier the case chose. A new member, a
// renamed one, a changed code or a changed spelling on either side therefore
// changes a fixture.

package tests

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/tests/internal/orchestrationtest"
)

const (
	wireGoldenDir = "testdata/sessionwire"
	// wireUpdateEnv, set to 1, makes the producing cases WRITE their fixtures
	// instead of comparing. Only `make sessionwire-goldens` should set it.
	wireUpdateEnv = "LOOPRIG_UPDATE_SESSIONWIRE"
)

// wireGoldenManifest is every fixture the producing cases write. The manifest
// case holds the directory to exactly this set, so a fixture no case produces
// any more cannot linger as a false promise.
var wireGoldenManifest = []string{
	// Factory <-> Host, captured live.
	"hostlink_connect",
	"hostlink_attach",
	"hostlink_bind",
	"hostlink_subscribe",
	"hostlink_command_delivery",
	"hostlink_publication",
	"hostlink_unbind",
	// Factory <-> browser, captured live.
	"clientlink_connect",
	"clientlink_subscribe",
	"clientlink_journal_tip",
	"clientlink_session_reset",
	"clientlink_enduring_publication",
	"clientlink_connect_unauthenticated",
	// Factory's refusals of an authenticated principal.
	"clientlink_connect_unsupported_protocol",
	"clientlink_connect_malformed_protocol",
	"clientlink_subscribe_not_authorized",
	"clientlink_subscribe_cross_tenant",
	"http_session_read_not_authorized",
	// controller drain client <-> Host, captured live.
	"hostlink_drain",
	"hostlink_drain_status",
	// Host's answers to requests a Factory would not send.
	"hostlink_refusal_epoch_mismatch",
	"hostlink_refusal_runtime_mismatch",
	"hostlink_refusal_runtime_unavailable",
	"hostlink_refusal_no_capacity",
	"hostlink_refusal_not_admitting",
	"hostlink_malformed_request",
	"hostlink_connect_negotiation",
}

func wireUpdating() bool { return os.Getenv(wireUpdateEnv) == "1" }

// ---- normalization ------------------------------------------------------------

// wireNormalizer turns a live frame into its frozen form.
type wireNormalizer struct {
	mu   sync.Mutex
	subs [][2]string // live -> placeholder, longest live value first
}

// substitute registers one live value that must appear in a fixture as its
// placeholder. Longer values are replaced first, so a channel is replaced
// whole before any identifier inside it could be.
func (n *wireNormalizer) substitute(live, placeholder string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if live == "" {
		return
	}
	n.subs = append(n.subs, [2]string{live, placeholder})
	sort.SliceStable(n.subs, func(i, j int) bool { return len(n.subs[i][0]) > len(n.subs[j][0]) })
}

// wireCounterKeys hold numbers whose VALUE is scheduling, not wire: they are
// frozen as 0 so their presence and type still are.
var wireCounterKeys = map[string]bool{
	"id":                  true,
	"lease_epoch":         true,
	"current_lease_epoch": true,
	"journal_seq":         true,
	"covered_through":     true,
	"journal_tip":         true,
	"last_contiguous":     true,
	"seq":                 true,
}

// wireOpaqueKeys hold strings minted per run.
var wireOpaqueKeys = map[string]string{
	"idempotency_key": "${idempotency_key}",
	"client":          "${client_id}",
	"event_id":        "${event_id}",
	"expires_at":      "${time}",
	"observed_at":     "${time}",
}

// frame decodes and normalizes one JSON value.
func (n *wireNormalizer) frame(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return n.value("", value), nil
}

func (n *wireNormalizer) value(key string, value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, member := range v {
			out[k] = n.value(k, member)
		}
		if _, ok := v["body"]; ok && v["type"] == string(sessionwire.SessionRecordTypeEnduringPublication) {
			out["body"] = wireFrozenBody()
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, member := range v {
			out[i] = n.value(key, member)
		}
		return out
	case json.Number:
		if wireCounterKeys[key] {
			return json.Number("0")
		}
		return v
	case string:
		if placeholder, ok := wireOpaqueKeys[key]; ok {
			return placeholder
		}
		n.mu.Lock()
		for _, sub := range n.subs {
			v = strings.ReplaceAll(v, sub[0], sub[1])
		}
		n.mu.Unlock()
		if id, err := uuid.Parse(v); err == nil && !id.IsZero() {
			return "${uuid}"
		}
		if _, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return "${time}"
		}
		return v
	default:
		return v
	}
}

// wireFrozenBody is what an enduring publication's BODY is frozen as.
//
// The body is the RUNTIME's content -- Core carries it as an opaque public JSON
// object -- and since tests v0.12.0 the kit's stream is a real harness
// runtime's (its committed public events, relayed as host's reference adapter
// relays them), so a live body is a harness event with per-run ids and times.
// What these fixtures freeze is the SESSIONWIRE envelope around it; freezing a
// harness event's shape here would make every harness release a wire change.
// A replay sends this constant, which is a valid opaque body.
func wireFrozenBody() map[string]any {
	return map[string]any{"orchestrationtest_runtime_body": "opaque"}
}

// ---- denormalization ------------------------------------------------------------

// wireFill turns a frozen value back into a sendable one: placeholders become
// the replay's live values and a frozen counter takes the value the replay
// names for its key. A placeholder with no live value is a case error, not a
// silent pass-through.
type wireFill struct {
	strings  map[string]string // placeholder -> live
	counters map[string]uint64 // key -> live value for a frozen 0
	times    map[string]time.Time
}

func (f wireFill) apply(t *testing.T, key string, value any) any {
	t.Helper()
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, member := range v {
			out[k] = f.apply(t, k, member)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, member := range v {
			out[i] = f.apply(t, key, member)
		}
		return out
	case json.Number:
		if live, ok := f.counters[key]; ok && v == "0" {
			return json.Number(fmt.Sprint(live))
		}
		return v
	case string:
		if v == "${time}" {
			at, ok := f.times[key]
			if !ok {
				t.Fatalf("the replay names no time for %q", key)
			}
			return at.UTC().Format(time.RFC3339Nano)
		}
		for placeholder, live := range f.strings {
			v = strings.ReplaceAll(v, placeholder, live)
		}
		if strings.Contains(v, "${") {
			t.Fatalf("the replay has no live value for %q (member %q)", v, key)
		}
		return v
	default:
		return v
	}
}

// bytes is apply, encoded.
func (f wireFill) bytes(t *testing.T, value any) []byte {
	t.Helper()
	out, err := json.Marshal(f.apply(t, "", value))
	if err != nil {
		t.Fatalf("encoding a replayed frame: %v", err)
	}
	return out
}

// ---- fixture I/O ------------------------------------------------------------------

// wireGolden is one fixture: an exchange (request and reply) or a push, each a
// normalized frame. Method names what the exchange is, for a reader.
type wireGolden struct {
	Source  string `json:"source"`
	Method  string `json:"method,omitempty"`
	Request any    `json:"request,omitempty"`
	Reply   any    `json:"reply,omitempty"`
	Push    any    `json:"push,omitempty"`
}

func wireGoldenPath(name string) string { return filepath.Join(wireGoldenDir, name+".json") }

func encodeWireGolden(golden wireGolden) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(golden); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// checkWireGolden writes the fixture under update, and otherwise fails on any
// difference between what the wire carried and what is frozen.
func checkWireGolden(t *testing.T, name string, golden wireGolden) {
	t.Helper()
	if !wireManifestHas(name) {
		t.Fatalf("fixture %q is not in wireGoldenManifest", name)
	}
	got, err := encodeWireGolden(golden)
	if err != nil {
		t.Fatalf("encoding fixture %q: %v", name, err)
	}
	path := wireGoldenPath(name)
	if wireUpdating() {
		if err := os.MkdirAll(wireGoldenDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o600); err != nil {
			t.Fatalf("writing fixture %q: %v", name, err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("fixture %q is missing (%v); regenerate with `make sessionwire-goldens`", name, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("THE WIRE MOVED: %s no longer matches %s.\n--- frozen\n%s\n--- live\n%s\n"+
			"If the change is intended, regenerate with `make sessionwire-goldens` and review the diff.",
			name, path, want, got)
	}
}

// loadWireGolden reads one frozen fixture for a replay.
func loadWireGolden(t *testing.T, name string) wireGolden {
	t.Helper()
	raw, err := os.ReadFile(wireGoldenPath(name))
	if err != nil {
		t.Fatalf("reading fixture %q: %v", name, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var golden wireGolden
	if err := decoder.Decode(&golden); err != nil {
		t.Fatalf("decoding fixture %q: %v", name, err)
	}
	return golden
}

func wireManifestHas(name string) bool {
	for _, known := range wireGoldenManifest {
		if known == name {
			return true
		}
	}
	return false
}

// member walks a normalized frame by member names.
func member(value any, path ...string) any {
	for _, name := range path {
		object, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = object[name]
	}
	return value
}

// ---- reading a tap as normalized frames ------------------------------------------------

// wireLine is one centrifuge JSON line off a tapped connection, raw and
// normalized.
type wireLine struct {
	Conn       int
	FromServer bool
	Raw        []byte
	Object     map[string]any // decoded with UseNumber, NOT normalized
}

var errWireTapIncomplete = errors.New("the tap could not read every frame")

// wireLines reads every line on every upgraded connection of a tap, in wire
// order per connection.
func wireLines(t *testing.T, tap *orchestrationtest.HostLinkTap) []wireLine {
	t.Helper()
	var out []wireLine
	for _, conn := range tap.Conns() {
		if conn.Status() != 101 {
			continue
		}
		if faults := conn.Faults(); len(faults) != 0 {
			t.Fatalf("%v on conn %d: %v", errWireTapIncomplete, conn.ID, faults)
		}
		for _, message := range conn.Messages() {
			if message.Close {
				continue
			}
			for _, line := range bytes.Split(message.Payload, []byte("\n")) {
				line = bytes.TrimSpace(line)
				if len(line) == 0 || bytes.Equal(line, []byte("{}")) {
					continue
				}
				decoder := json.NewDecoder(bytes.NewReader(line))
				decoder.UseNumber()
				var object map[string]any
				if err := decoder.Decode(&object); err != nil {
					t.Fatalf("an unparseable centrifuge line on conn %d: %q: %v", conn.ID, line, err)
				}
				out = append(out, wireLine{Conn: conn.ID, FromServer: message.FromHost, Raw: line, Object: object})
			}
		}
	}
	return out
}

// wireExchange is one client command and the server's reply to it.
type wireExchange struct {
	Kind    string // connect, subscribe, or the RPC method
	Command wireLine
	Reply   *wireLine
}

// wireExchanges pairs every client command with its reply by centrifuge id on
// the same connection. A command with no reply keeps a nil Reply.
func wireExchanges(lines []wireLine) []wireExchange {
	var out []wireExchange
	for i, line := range lines {
		if line.FromServer {
			continue
		}
		exchange := wireExchange{Command: line}
		switch {
		case line.Object["connect"] != nil:
			exchange.Kind = "connect"
		case line.Object["subscribe"] != nil:
			exchange.Kind = "subscribe"
		case line.Object["unsubscribe"] != nil:
			exchange.Kind = "unsubscribe"
		case line.Object["rpc"] != nil:
			method, _ := member(line.Object, "rpc", "method").(string)
			exchange.Kind = method
		default:
			exchange.Kind = "other"
		}
		id := fmt.Sprint(line.Object["id"])
		for j := i + 1; j < len(lines); j++ {
			reply := lines[j]
			if reply.Conn == line.Conn && reply.FromServer && reply.Object["push"] == nil && fmt.Sprint(reply.Object["id"]) == id {
				exchange.Reply = &lines[j]
				break
			}
		}
		out = append(out, exchange)
	}
	return out
}

// wirePushes reports every server push carrying a publication.
func wirePushes(lines []wireLine) []wireLine {
	var out []wireLine
	for _, line := range lines {
		if line.FromServer && member(line.Object, "push", "pub") != nil {
			out = append(out, line)
		}
	}
	return out
}

// normalizeLine is the frozen form of one line.
func normalizeLine(t *testing.T, n *wireNormalizer, line wireLine) any {
	t.Helper()
	value, err := n.frame(line.Raw)
	if err != nil {
		t.Fatalf("normalizing %q: %v", line.Raw, err)
	}
	return value
}

// TestSessionwireGoldenManifest holds testdata/sessionwire to exactly the set
// the producing cases write.
func TestSessionwireGoldenManifest(t *testing.T) {
	entries, err := os.ReadDir(wireGoldenDir)
	if err != nil {
		t.Fatalf("reading %s: %v", wireGoldenDir, err)
	}
	onDisk := map[string]bool{}
	for _, entry := range entries {
		onDisk[strings.TrimSuffix(entry.Name(), ".json")] = true
		if !strings.HasSuffix(entry.Name(), ".json") || !wireManifestHas(strings.TrimSuffix(entry.Name(), ".json")) {
			t.Errorf("%s/%s is not a fixture any case produces; delete it or add its producer", wireGoldenDir, entry.Name())
		}
	}
	for _, name := range wireGoldenManifest {
		if !onDisk[name] {
			t.Errorf("fixture %s is missing; regenerate with `make sessionwire-goldens`", name)
		}
	}
}
