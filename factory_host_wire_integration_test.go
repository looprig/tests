//go:build integration

// Runbook 07, task I0.2: freeze cross-service wire compatibility.
//
// Factory and Host are released independently and meet only on a WebSocket, so
// a wire change on either side is invisible to both modules' own suites: each
// tests against its own idea of the other. This file makes that meeting point a
// frozen, reviewed artefact.
//
// # The four legs
//
//  1. CAPTURE. A real released Factory places two sessions of one tenant on a
//     real released Host; browser-shaped viewers watch; creates, an agent's
//     gate and its answer, inputs and an interrupt cross; a viewer leaves.
//     A passive tap on each wire records every frame, and the first frame of
//     every kind is frozen under testdata/sessionwire. The controller's drain
//     client and a plain Core-framed probe add what a Factory never sends: the
//     drain RPCs and every reachable refusal code.
//  2. HOST REPLAY. The frozen Factory requests are sent, as they are, to a
//     fresh real Host, and its replies must equal the frozen replies.
//  3. FACTORY REPLAY. A stand-in Host answers a real Factory with nothing but
//     the frozen replies -- plus an unknown capability token and an unknown
//     negotiation member, which a tolerant decoder must keep accepting -- and
//     the Factory must walk the whole attach/bind/subscribe/deliver path,
//     sending exactly the frozen requests, and relay the frozen publication to
//     a viewer. A frame a strict decoder must refuse is refused.
//  4. CORE. Every frozen payload decodes through Core's own strict decoder for
//     its record.
//
// Legs 2 and 3 are what make a failure SAY WHICH SIDE MOVED: leg 1 fails for a
// change on either side, leg 2 only for Host, leg 3 only for Factory.
//
// # Beside the fixtures
//
// The capture also asserts what a fixture cannot: RPC correlation, one
// physical link carrying both sessions concurrently with no cross-talk, a
// reconnect that re-presents the frozen connect, and that no public
// (ClientLink) frame carries a transport type name, a backend key, a
// credential, the raw gate answer, a runtime identity or a harness event.
//
// # Finding F1, closed by factory v0.8.1
//
// factory v0.7.1 relayed a Host publication onto the session channel it
// arrived on without checking that the record names that channel's tenant and
// session. factory v0.8.1 refuses such a record and repairs the tail (a
// session.reset to viewers, then a re-bind); leg 3 pushes both kinds of
// foreign record and asserts neither reaches the browser and each is answered
// by a reset.
//
// # Host answers every refused connect shape with one code
//
// Only-unsupported versions, an unknown member, the B8 wrapped shape, a base64
// list and an empty list are five different mistakes, and Host answers all of
// them with the same close, 4501 "unsupported wire version". That is frozen
// honestly, but a peer cannot tell "wrong version" from "malformed request"
// on the wire. The diagnostic is owed to Host; changing it will move
// hostlink_connect_negotiation.json, which is the point.
//
// # Refusal codes, and the one not driven
//
// Core names six HostLink refusal codes. Five are driven against a real Host
// and frozen. `releasing` is not: Host answers it only inside the window
// between a residency's MarkReleasing and its route's removal, and an idle
// session's release closes that window before any request can land in it --
// measured, a delivery sent immediately after a drain begins is answered
// runtime_unavailable (the route is already gone). It is frozen at the Core
// level only, by leg 4's round trip, and a deterministic driver for it is owed
// to Host (a hook that holds a residency in releasing).

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

const (
	wireTenant   = orchestrationtest.PooledTenantA
	wireSessionA = sessionwire.SessionID("session-wire-a")
	wireSessionB = sessionwire.SessionID("session-wire-b")
	wireHostID   = sessionwire.HostID("orchestrationtest-wire-host")
	wireHostGen  = uint64(7)

	// wireGateAnswer is the user's raw answer. It must reach the agent and
	// must never appear on either wire: a gate answer is private payload that
	// stays in the durable inbox.
	wireGateAnswer = "RAWGATEANSWER-VERMILION"
	wireQuestion   = "orchestrationtest: which pigment?"
)

var wireCommands = map[string]sessionwire.CommandID{
	"create_a":    "command-wire-create-a",
	"gate_answer": "command-wire-gate-answer-a",
	"create_b":    "command-wire-create-b",
	"input_a":     "command-wire-input-a",
	"input_b":     "command-wire-input-b",
	"interrupt_b": "command-wire-interrupt-b",
}

func wireContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// ---- leg 1: the live capture -------------------------------------------------------

// wireCapture is everything one live scenario put on the two wires.
type wireCapture struct {
	world   *orchestrationtest.PooledWorld
	host    *orchestrationtest.PooledHost
	factory *orchestrationtest.PooledFactory
	hostTap *orchestrationtest.HostLinkTap
	viewTap *orchestrationtest.HostLinkTap
	viewers map[sessionwire.SessionID]*orchestrationtest.PooledViewer
	// hostLinesBeforeSever is the HostLink record up to the reconnect case.
	hostLinesBeforeSever []wireLine
}

func runWireScenario(t *testing.T, ctx context.Context) *wireCapture {
	t.Helper()
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:     []sessionwire.TenantID{wireTenant},
		WithAskTool: true,
	})
	world.AskTool.Question = wireQuestion
	// Session A's create drives turn one (the tool call that raises the gate)
	// and its answer drives turn two. Everything after is plain text.
	world.LLM.Script(
		orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledAskToolName, ToolInput: `{}`},
		orchestrationtest.PooledTurn{Text: "thank you"},
	)
	capture := &wireCapture{
		world:   world,
		hostTap: orchestrationtest.NewHostLinkTap(),
		viewTap: orchestrationtest.NewHostLinkTap(),
		viewers: map[sessionwire.SessionID]*orchestrationtest.PooledViewer{},
	}
	capture.host = orchestrationtest.StartTappedPooledHost(t, ctx, world, wireHostID, wireHostGen, capture.hostTap)
	orchestrationtest.AwaitAdvertised(t, world, wireHostID)
	capture.factory = orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica:  "orchestrationtest-wire-replica",
		Listener: capture.viewTap.WrapListener,
	})
	// Viewers subscribe BEFORE the sessions exist: their demand is what makes
	// Factory bind, and a live tail is what the publication fixtures freeze.
	for _, s := range []sessionwire.SessionID{wireSessionA, wireSessionB} {
		viewer := orchestrationtest.ConnectPooledViewer(t, ctx, capture.factory, wireTenant)
		if err := viewer.Watch(t, ctx, wireTenant, s); err != nil {
			t.Fatalf("the viewer's subscribe to %s was refused: %v", s, err)
		}
		capture.viewers[s] = viewer
	}

	post := func(path string, body any) {
		t.Helper()
		status, text := capture.factory.Post(t, ctx, wireTenant, path, body)
		if status < 200 || status > 299 {
			t.Fatalf("POST %s answered %d: %s", path, status, text)
		}
	}
	settled := func(s sessionwire.SessionID, key string) {
		t.Helper()
		orchestrationtest.PooledWait(t, key+" settled", 90*time.Second, func() bool {
			state := world.CommandState(ctx, wireTenant, s, wireCommands[key])
			return state == sessionstore.InboxStateApplied || state == sessionstore.InboxStateRejected
		})
	}

	post("/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(wireCommands["create_a"])),
		SessionID:       wireSessionA,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(`[{"type":"text","text":"ask me something"}]`),
	})
	settled(wireSessionA, "create_a")

	var projected sessionwire.GateProjection
	orchestrationtest.PooledWait(t, "the gate reached Factory", 90*time.Second, func() bool {
		page := capture.factory.OpenGates(t, ctx, wireTenant, wireSessionA)
		if len(page.Gates) == 0 {
			return false
		}
		projected = page.Gates[0]
		return true
	})
	post("/v1/sessions/"+string(wireSessionA)+"/gates/"+string(projected.GateID), sessionwire.GateResponseRequest{
		CommandEnvelope:        orchestrationtest.PooledEnvelope(string(wireCommands["gate_answer"])),
		SessionID:              wireSessionA,
		GateID:                 projected.GateID,
		Action:                 "answer",
		Values:                 map[string]json.RawMessage{"answer": json.RawMessage(`"` + wireGateAnswer + `"`)},
		ExpectedOpenJournalSeq: projected.OpenedJournalSeq,
	})
	settled(wireSessionA, "gate_answer")

	post("/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(wireCommands["create_b"])),
		SessionID:       wireSessionB,
		AgentID:         orchestrationtest.PooledAgent,
	})
	settled(wireSessionB, "create_b")

	for _, key := range []string{"input_a", "input_b"} {
		s := wireSessionA
		if key == "input_b" {
			s = wireSessionB
		}
		post("/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(string(wireCommands[key])),
			SessionID:       s,
			Blocks:          json.RawMessage(`[{"type":"text","text":"hello again"}]`),
		})
	}
	settled(wireSessionA, "input_a")
	settled(wireSessionB, "input_b")
	post("/v1/sessions/"+string(wireSessionB)+"/interrupt", sessionwire.InterruptRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(wireCommands["interrupt_b"])),
		SessionID:       wireSessionB,
	})
	settled(wireSessionB, "interrupt_b")

	// Both sessions' output must cross the one shared link. Session B's tail
	// can be between an unbind and a rebind when its first commands apply,
	// so further inputs are admitted until one of its publications is seen
	// on the HostLink (bounded; the link case fails loudly if none is).
	for extra := 1; extra <= 3 && !hostPublished(t, capture.hostTap, wireSessionB); extra++ {
		command := sessionwire.CommandID(fmt.Sprintf("command-wire-extra-%d-b", extra))
		post("/v1/sessions/"+string(wireSessionB)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)),
			SessionID:       wireSessionB,
			Blocks:          json.RawMessage(`[{"type":"text","text":"once more"}]`),
		})
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) && !hostPublished(t, capture.hostTap, wireSessionB) {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// The answer reached the agent: non-vacuity for "the answer never crossed
	// a wire" below.
	orchestrationtest.PooledWait(t, "the gate answer reached the agent's tool", 60*time.Second, func() bool {
		answers := world.AskTool.Answers()
		return len(answers) == 1 && answers[0] == wireGateAnswer
	})

	// Viewer B leaves; its session's demand ends and Factory unbinds it.
	unbindsBefore := countWireKind(wireExchanges(wireLines(t, capture.hostTap)), sessionwire.HostLinkMethodUnbind, wireSessionB)
	capture.viewers[wireSessionB].Close()
	orchestrationtest.PooledWait(t, "Factory unbound the unwatched session", 30*time.Second, func() bool {
		return countWireKind(wireExchanges(wireLines(t, capture.hostTap)), sessionwire.HostLinkMethodUnbind, wireSessionB) > unbindsBefore
	})
	orchestrationtest.PooledWait(t, "every HostLink command was answered", 30*time.Second, func() bool {
		for _, exchange := range wireExchanges(wireLines(t, capture.hostTap)) {
			if exchange.Reply == nil {
				return false
			}
		}
		return true
	})
	capture.hostLinesBeforeSever = wireLines(t, capture.hostTap)
	return capture
}

// hostPublished reports whether a publication for s has crossed the HostLink.
func hostPublished(t *testing.T, tap *orchestrationtest.HostLinkTap, s sessionwire.SessionID) bool {
	t.Helper()
	for _, push := range wirePushes(wireLines(t, tap)) {
		if member(push.Object, "push", "channel") == sessionwire.HostLinkChannel(wireTenant, s) {
			return true
		}
	}
	return false
}

// countWireKind counts exchanges of one kind whose request names session s.
func countWireKind(exchanges []wireExchange, kind string, s sessionwire.SessionID) int {
	n := 0
	for _, exchange := range exchanges {
		if exchange.Kind == kind && member(exchange.Command.Object, "rpc", "data", "session_id") == string(s) {
			n++
		}
	}
	return n
}

// liveNormalizer is the substitution set for a capture: the run's own dynamic
// values, and the case's session and command identifiers, so a fixture is the
// same whichever session's frame happened to come first.
func liveNormalizer(base sessionwire.InternalEndpoint, sessions []sessionwire.SessionID, commands []sessionwire.CommandID) *wireNormalizer {
	n := &wireNormalizer{}
	n.substitute(string(base), "${host_base}")
	for _, s := range sessions {
		n.substitute(sessionwire.HostLinkChannel(wireTenant, s), "${hostlink_channel}")
		n.substitute(string(s), "${session}")
	}
	for _, command := range commands {
		n.substitute(string(command), "${command_id}")
	}
	n.substitute(orchestrationtest.PooledServiceToken, "${hostlink_token}")
	for _, bearer := range orchestrationtest.PooledBearers {
		n.substitute(bearer, "${bearer}")
	}
	return n
}

func wireCommandList() []sessionwire.CommandID {
	out := make([]sessionwire.CommandID, 0, len(wireCommands))
	for _, command := range wireCommands {
		out = append(out, command)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// firstExchange is the first exchange of kind with a reply.
func firstExchange(t *testing.T, exchanges []wireExchange, kind string) wireExchange {
	t.Helper()
	for _, exchange := range exchanges {
		if exchange.Kind == kind && exchange.Reply != nil {
			return exchange
		}
	}
	t.Fatalf("no answered %q exchange crossed the wire", kind)
	return wireExchange{}
}

func goldenExchange(t *testing.T, n *wireNormalizer, source string, exchange wireExchange) wireGolden {
	t.Helper()
	return wireGolden{
		Source:  source,
		Method:  exchange.Kind,
		Request: normalizeLine(t, n, exchange.Command),
		Reply:   normalizeLine(t, n, *exchange.Reply),
	}
}

// TestFactoryHostWireGoldens is leg 1: the live capture, frozen, plus every
// property of the wire a fixture cannot hold.
func TestFactoryHostWireGoldens(t *testing.T) {
	ctx := wireContext(t)
	capture := runWireScenario(t, ctx)
	sessions := []sessionwire.SessionID{wireSessionA, wireSessionB}
	n := liveNormalizer(capture.host.Base, sessions, wireCommandList())
	hostLines := capture.hostLinesBeforeSever
	hostExchanges := wireExchanges(hostLines)
	const factorySource = "factory -> host (released versions pinned in go.mod), captured live"

	t.Run("the HostLink exchanges are the frozen ones", func(t *testing.T) {
		for name, kind := range map[string]string{
			"hostlink_connect":   "connect",
			"hostlink_attach":    sessionwire.HostLinkMethodAttach,
			"hostlink_bind":      sessionwire.HostLinkMethodBind,
			"hostlink_unbind":    sessionwire.HostLinkMethodUnbind,
			"hostlink_subscribe": "subscribe",
		} {
			checkWireGolden(t, name, goldenExchange(t, n, factorySource, firstExchange(t, hostExchanges, kind)))
		}
		var delivery *wireExchange
		for i, exchange := range hostExchanges {
			if strings.HasPrefix(exchange.Kind, sessionwire.HostLinkChannelPrefix) && exchange.Reply != nil {
				delivery = &hostExchanges[i]
				break
			}
		}
		if delivery == nil {
			t.Fatalf("no command delivery crossed the HostLink")
		}
		golden := goldenExchange(t, n, factorySource, *delivery)
		golden.Method = "${hostlink_channel}"
		checkWireGolden(t, "hostlink_command_delivery", golden)
		pushes := wirePushes(hostLines)
		if len(pushes) == 0 {
			t.Fatalf("the Host published nothing on the HostLink")
		}
		checkWireGolden(t, "hostlink_publication", wireGolden{Source: factorySource, Push: normalizeLine(t, n, pushes[0])})
	})

	viewLines := wireLines(t, capture.viewTap)
	t.Run("the ClientLink frames are the frozen ones", func(t *testing.T) {
		exchanges := wireExchanges(viewLines)
		const source = "factory ClientLink (pinned in go.mod) -> centrifuge-go viewer, captured live"
		checkWireGolden(t, "clientlink_connect", goldenExchange(t, n, source, firstExchange(t, exchanges, "connect")))
		checkWireGolden(t, "clientlink_subscribe", goldenExchange(t, n, source, firstExchange(t, exchanges, "subscribe")))
		byType := map[string]wireLine{}
		for _, push := range wirePushes(viewLines) {
			recordType, _ := member(push.Object, "push", "pub", "data", "type").(string)
			if _, seen := byType[recordType]; !seen {
				byType[recordType] = push
			}
		}
		frozen := map[string]string{
			"clientlink_journal_tip":          string(sessionwire.SessionRecordTypeJournalTip),
			"clientlink_session_reset":        string(sessionwire.SessionRecordTypeSessionReset),
			"clientlink_enduring_publication": string(sessionwire.SessionRecordTypeEnduringPublication),
		}
		for name, recordType := range frozen {
			push, ok := byType[recordType]
			if !ok {
				t.Fatalf("no %s record reached a viewer; saw %v", recordType, keysOf(byType))
			}
			checkWireGolden(t, name, wireGolden{Source: source, Push: normalizeLine(t, n, push)})
		}
		for recordType := range byType {
			known := false
			for _, frozenType := range frozen {
				known = known || recordType == frozenType
			}
			if !known {
				t.Errorf("a viewer received a record of unfrozen type %q", recordType)
			}
		}
	})

	t.Run("an unauthenticated viewer is refused, and the refusal is frozen", func(t *testing.T) {
		before := len(capture.viewTap.Conns())
		result := rawClientLinkConnect(t, capture.factory, "orchestrationtest-not-a-bearer", `{"protocol_version":"1"}`)
		if result.Connected {
			t.Fatalf("Factory accepted a ClientLink connect with an unknown bearer")
		}
		refusal := clientLinkRefusal(t, capture.viewTap, before, n)
		checkWireGolden(t, "clientlink_connect_unauthenticated", wireGolden{
			Source: "factory ClientLink (pinned in go.mod), a connect presenting an unknown bearer",
			Reply:  refusal,
		})
	})

	t.Run("every command is answered exactly once, correlated by id", func(t *testing.T) {
		assertWireCorrelation(t, hostLines)
		assertWireCorrelation(t, viewLines)
		// And a delivery names only a command of the session its channel names.
		sessionOf := map[string]sessionwire.SessionID{}
		for _, s := range sessions {
			sessionOf[sessionwire.HostLinkChannel(wireTenant, s)] = s
		}
		deliveries := 0
		for _, exchange := range hostExchanges {
			if !strings.HasPrefix(exchange.Kind, sessionwire.HostLinkChannelPrefix) {
				continue
			}
			deliveries++
			s, known := sessionOf[exchange.Kind]
			if !known {
				t.Errorf("a delivery on an unknown channel %q", exchange.Kind)
				continue
			}
			command, _ := member(exchange.Command.Object, "rpc", "data", "command_id").(string)
			suffix := "-" + strings.TrimPrefix(string(s), "session-wire-")
			if !strings.HasSuffix(command, suffix) {
				t.Errorf("CROSS-TALK: a delivery on %s's channel names command %q", s, command)
			}
		}
		if deliveries == 0 {
			t.Fatalf("no delivery crossed the link, so the correlation claim is vacuous")
		}
	})

	t.Run("one physical link carries both sessions concurrently, without cross-talk", func(t *testing.T) {
		connected := map[int]bool{}
		for _, exchange := range hostExchanges {
			if exchange.Kind == "connect" && exchange.Reply != nil && exchange.Reply.Object["connect"] != nil {
				connected[exchange.Command.Conn] = true
			}
		}
		if len(connected) != 1 {
			t.Fatalf("the tenant's HostLink was %d connections, want exactly one", len(connected))
		}
		bound := map[sessionwire.SessionID]bool{}
		peak := 0
		delivered := map[sessionwire.SessionID]bool{}
		for _, exchange := range hostExchanges {
			if !connected[exchange.Command.Conn] {
				t.Fatalf("a command crossed a second connection %d", exchange.Command.Conn)
			}
			s := sessionwire.SessionID(fmt.Sprint(member(exchange.Command.Object, "rpc", "data", "session_id")))
			switch {
			case exchange.Kind == sessionwire.HostLinkMethodBind && exchange.Reply != nil && member(exchange.Reply.Object, "rpc", "data") == nil:
				bound[s] = true
			case exchange.Kind == sessionwire.HostLinkMethodUnbind:
				delete(bound, s)
			case strings.HasPrefix(exchange.Kind, sessionwire.HostLinkChannelPrefix):
				for _, candidate := range sessions {
					if exchange.Kind == sessionwire.HostLinkChannel(wireTenant, candidate) {
						delivered[candidate] = true
					}
				}
			}
			peak = max(peak, len(bound))
		}
		if peak < 2 {
			t.Fatalf("the link never held both sessions bound at once (peak %d)", peak)
		}
		for _, s := range sessions {
			if !delivered[s] {
				t.Errorf("no command delivery for %s crossed the shared link", s)
			}
		}
		// Every publication on the shared link names the session its channel
		// names, and each session's publications arrived on it.
		published := map[sessionwire.SessionID]int{}
		for _, push := range wirePushes(hostLines) {
			channel, _ := member(push.Object, "push", "channel").(string)
			named, _ := member(push.Object, "push", "pub", "data", "session_id").(string)
			if channel != sessionwire.HostLinkChannel(wireTenant, sessionwire.SessionID(named)) {
				t.Errorf("CROSS-TALK: a publication for %q arrived on channel %q", named, channel)
			}
			published[sessionwire.SessionID(named)]++
		}
		for _, s := range sessions {
			if published[s] == 0 {
				t.Errorf("no publication for %s crossed the shared link", s)
			}
		}
		for s, viewer := range capture.viewers {
			if strays := viewer.Strays(); len(strays) != 0 {
				t.Errorf("CROSS-TALK: %s's viewer received other sessions' records: %v", s, strays)
			}
			if len(viewer.Records()) == 0 {
				t.Errorf("%s's viewer received nothing, so its no-cross-talk claim is vacuous", s)
			}
		}
	})

	t.Run("no public frame leaks a private value", func(t *testing.T) {
		forbidden := wireForbidden(t, ctx, capture)
		scanned := 0
		for _, line := range viewLines {
			if !line.FromServer {
				continue
			}
			scanned++
			lowered := strings.ToLower(string(line.Raw))
			for name, needle := range forbidden {
				if strings.Contains(lowered, strings.ToLower(needle)) {
					t.Errorf("LEAK: a ClientLink frame carries %s (%q): %s", name, needle, line.Raw)
				}
			}
		}
		if scanned == 0 {
			t.Fatalf("no ClientLink frame was scanned")
		}
		// The HostLink is internal, but its contract is that private payload
		// stays in the durable inbox: a delivery carries a public CommandID and
		// nothing else. (The credential legitimately crosses in the connect.)
		for _, line := range hostLines {
			for name, needle := range forbidden {
				private := strings.HasPrefix(name, "the raw gate answer") ||
					strings.HasPrefix(name, "a runtime session identity") ||
					strings.HasPrefix(name, "a runtime command identity")
				if private && strings.Contains(string(line.Raw), needle) {
					t.Errorf("LEAK: a HostLink frame carries %s (%q): %s", name, needle, line.Raw)
				}
			}
		}
	})

	t.Run("a reconnect re-presents the frozen connect and rebinds before subscribing", func(t *testing.T) {
		before := len(capture.hostTap.Conns())
		if severed := capture.host.Sever(); severed == 0 {
			t.Fatalf("there was no HostLink to sever")
		}
		orchestrationtest.PooledWait(t, "Factory reconnected and re-subscribed", 60*time.Second, func() bool {
			for _, conn := range capture.hostTap.Conns()[before:] {
				for _, exchange := range wireExchanges(wireLines(t, orchestrationtest.TapOf(conn))) {
					if exchange.Kind == "subscribe" && exchange.Reply != nil {
						return true
					}
				}
			}
			return false
		})
		golden := loadWireGolden(t, "hostlink_connect")
		for _, conn := range capture.hostTap.Conns()[before:] {
			exchanges := wireExchanges(wireLines(t, orchestrationtest.TapOf(conn)))
			if len(exchanges) == 0 {
				continue
			}
			if got := normalizeLine(t, n, exchanges[0].Command); !reflect.DeepEqual(got, golden.Request) {
				t.Errorf("the reconnect's connect is %s, want the frozen %s", mustJSON(t, got), mustJSON(t, golden.Request))
			}
			bound := map[string]bool{}
			for _, exchange := range exchanges[1:] {
				switch exchange.Kind {
				case sessionwire.HostLinkMethodBind:
					s := sessionwire.SessionID(fmt.Sprint(member(exchange.Command.Object, "rpc", "data", "session_id")))
					bound[sessionwire.HostLinkChannel(wireTenant, s)] = true
				case "subscribe":
					channel, _ := member(exchange.Command.Object, "subscribe", "channel").(string)
					if !bound[channel] {
						t.Errorf("the reconnect subscribed to %s before binding it", channel)
					}
				}
			}
		}
	})
}

// assertWireCorrelation holds every client command on every connection to
// exactly one reply with its id, and every reply to a command that was sent.
func assertWireCorrelation(t *testing.T, lines []wireLine) {
	t.Helper()
	type key struct {
		conn int
		id   string
	}
	commands := map[key]int{}
	replies := map[key]int{}
	for _, line := range lines {
		if line.Object["push"] != nil {
			continue
		}
		k := key{line.Conn, fmt.Sprint(line.Object["id"])}
		if line.FromServer {
			replies[k]++
		} else {
			commands[k]++
		}
	}
	if len(commands) == 0 {
		t.Fatalf("no command to correlate")
	}
	for k, count := range commands {
		if count != 1 {
			t.Errorf("conn %d reused command id %s %d times", k.conn, k.id, count)
		}
		if replies[k] != 1 {
			t.Errorf("conn %d's command %s was answered %d times, want exactly once", k.conn, k.id, replies[k])
		}
	}
	for k := range replies {
		if commands[k] == 0 {
			t.Errorf("conn %d carried a reply %s to no command", k.conn, k.id)
		}
	}
}

// wireForbidden names every value no public frame may carry.
func wireForbidden(t *testing.T, ctx context.Context, capture *wireCapture) map[string]string {
	t.Helper()
	forbidden := map[string]string{
		// centrifuge-go's and centrifuge's Go types, error strings and package
		// paths all carry the name; the protocol's own JSON members do not.
		"a centrifuge transport type name": "centrifug",
		"the HostLink service credential":  orchestrationtest.PooledServiceToken,
		"the raw gate answer":              wireGateAnswer,
		"the storage binding":              orchestrationtest.PooledBinding,
	}
	for tenant, bearer := range orchestrationtest.PooledBearers {
		forbidden["the actor credential of "+string(tenant)] = bearer
	}
	for _, s := range []sessionwire.SessionID{wireSessionA, wireSessionB} {
		forbidden["a runtime session identity ("+string(s)+")"] = capture.world.RuntimeSessionID(t, ctx, wireTenant, s).String()
	}
	for name, command := range wireCommands {
		s := wireSessionA
		if strings.HasSuffix(name, "_b") {
			s = wireSessionB
		}
		entry, err := capture.world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: wireTenant, SessionID: s, CommandID: command,
		})
		if err != nil {
			t.Fatalf("reading %s: %v", command, err)
		}
		forbidden["a runtime command identity ("+name+")"] = string(entry.Record.Descriptor.RuntimeCommandID)
	}
	for _, private := range []any{event.GateOpened{}, event.GateResolved{}, event.GatePrepared{}, event.TurnDone{},
		event.TurnStarted{}, event.SessionStarted{}, event.StepDone{}, event.InputQueued{}, event.TurnInterrupted{}} {
		name := reflect.TypeOf(private).Name()
		forbidden["the harness event "+name] = name
	}
	keys, err := capture.world.Backend.KV.Keys(ctx, "")
	if err != nil {
		t.Fatalf("listing the backend's keys: %v", err)
	}
	blobs, err := capture.world.Backend.Blobs.List(ctx, "")
	if err != nil {
		t.Fatalf("listing the backend's blobs: %v", err)
	}
	named := 0
	for _, key := range append(keys, blobs...) {
		// A key short enough to be an ordinary word would match prose; every
		// SessionStore key is a multi-segment path far longer than this.
		if len(key) >= 16 {
			forbidden["the backend key "+key] = key
			named++
		}
	}
	if named == 0 {
		t.Fatalf("the backend holds no keys, so the backend-key scan is vacuous")
	}
	return forbidden
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// clientLinkRefusal is what Factory answered on the connections the tap saw
// from index before on: an HTTP status for a refused upgrade, or the server's
// frames and its close for an upgraded one.
func clientLinkRefusal(t *testing.T, tap *orchestrationtest.HostLinkTap, before int, n *wireNormalizer) any {
	t.Helper()
	var refusal map[string]any
	orchestrationtest.PooledWait(t, "the tap recorded Factory's refusal", 10*time.Second, func() bool {
		refusal = nil
		for _, conn := range tap.Conns()[before:] {
			switch status := conn.Status(); {
			case status == http.StatusSwitchingProtocols:
				answer := map[string]any{}
				var frames []any
				for _, line := range wireLines(t, orchestrationtest.TapOf(conn)) {
					if line.FromServer {
						frames = append(frames, normalizeLine(t, n, line))
					}
				}
				if frames != nil {
					answer["frames"] = frames
				}
				for _, message := range conn.Messages() {
					if message.Close && message.FromHost {
						answer["close"] = map[string]any{"code": message.CloseCode, "reason": message.Reason}
					}
				}
				if answer["close"] != nil {
					refusal = answer
				}
			case status != 0:
				refusal = map[string]any{"http_status": status}
			}
		}
		return refusal != nil
	})
	return refusal
}

// rawClientLinkConnect opens a ClientLink with a caller-chosen bearer and
// connect data, and reports whether Factory accepted it.
func rawClientLinkConnect(t *testing.T, f *orchestrationtest.PooledFactory, bearer, data string) orchestrationtest.RawConnectResult {
	t.Helper()
	endpoint := "ws" + strings.TrimPrefix(f.BaseURL, "http") + "/v1/realtime"
	client := centrifugego.NewJsonClient(endpoint, centrifugego.Config{
		Token:            bearer,
		Data:             []byte(data),
		Header:           http.Header{"Authorization": {"Bearer " + bearer}, "Origin": {f.BaseURL}},
		Name:             "orchestrationtest-wire-intruder",
		HandshakeTimeout: 10 * time.Second,
		LogLevel:         centrifugego.LogLevelNone,
	})
	defer client.Close()
	done := make(chan orchestrationtest.RawConnectResult, 1)
	report := func(result orchestrationtest.RawConnectResult) {
		select {
		case done <- result:
		default:
		}
	}
	client.OnConnected(func(e centrifugego.ConnectedEvent) {
		report(orchestrationtest.RawConnectResult{Connected: true, ReplyData: e.Data})
	})
	client.OnDisconnected(func(e centrifugego.DisconnectedEvent) {
		report(orchestrationtest.RawConnectResult{Disconnected: true, Code: e.Code, Reason: e.Reason})
	})
	client.OnError(func(e centrifugego.ErrorEvent) { report(orchestrationtest.RawConnectResult{Err: e.Error}) })
	if err := client.Connect(); err != nil {
		return orchestrationtest.RawConnectResult{Err: err}
	}
	select {
	case result := <-done:
		return result
	case <-time.After(15 * time.Second):
		t.Fatalf("the ClientLink connect was neither accepted nor refused")
		return orchestrationtest.RawConnectResult{}
	}
}

// ---- the Host's answers to requests a Factory never sends ------------------------------------

// hostProbe is a plain centrifuge-go JSON client on one tenant's HostLink,
// speaking Core's connect framing: how a case sends a request a Factory never
// would (a refusal's cause) and reads the Host's answer off the tap.
type hostProbe struct {
	client *centrifugego.Client
}

func dialHostProbe(t *testing.T, base sessionwire.InternalEndpoint, connect []byte) *hostProbe {
	t.Helper()
	endpoint, err := sessionwire.HostLinkEndpoint(base, wireTenant)
	if err != nil {
		t.Fatalf("deriving the HostLink address: %v", err)
	}
	client := centrifugego.NewJsonClient(string(endpoint), centrifugego.Config{
		Token:            orchestrationtest.PooledServiceToken,
		Data:             connect,
		Name:             "orchestrationtest-wire-probe",
		Header:           http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json"}},
		HandshakeTimeout: 10 * time.Second,
		LogLevel:         centrifugego.LogLevelNone,
	})
	connected := make(chan struct{}, 1)
	client.OnConnected(func(centrifugego.ConnectedEvent) {
		select {
		case connected <- struct{}{}:
		default:
		}
	})
	if err := client.Connect(); err != nil {
		t.Fatalf("the probe could not connect: %v", err)
	}
	t.Cleanup(client.Close)
	select {
	case <-connected:
	case <-time.After(15 * time.Second):
		t.Fatalf("the probe did not connect to %s", endpoint)
	}
	return &hostProbe{client: client}
}

func coreConnect(t *testing.T) []byte {
	t.Helper()
	data, err := sessionwire.EncodeHostLinkConnectRequest(sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// call sends one RPC and returns the reply data or the transport error.
func (p *hostProbe) call(t *testing.T, ctx context.Context, method string, data []byte) ([]byte, error) {
	t.Helper()
	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	result, err := p.client.RPC(callCtx, method, data)
	return result.Data, err
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encoding %T: %v", value, err)
	}
	return data
}

// lastExchange is the last answered exchange of kind on the tap.
func lastExchange(t *testing.T, tap *orchestrationtest.HostLinkTap, kind string) wireExchange {
	t.Helper()
	var found wireExchange
	orchestrationtest.PooledWait(t, "the tap recorded the "+kind+" reply", 10*time.Second, func() bool {
		exchanges := wireExchanges(wireLines(t, tap))
		for i := len(exchanges) - 1; i >= 0; i-- {
			if exchanges[i].Kind == kind {
				if exchanges[i].Reply == nil {
					return false
				}
				found = exchanges[i]
				return true
			}
		}
		return false
	})
	return found
}

// wireAdmit admits a bare create through a real Factory that places nothing,
// so a session exists in the catalog for a probe to attach by hand.
func wireAdmit(t *testing.T, ctx context.Context, f *orchestrationtest.PooledFactory, s sessionwire.SessionID, command sessionwire.CommandID) {
	t.Helper()
	status, body := f.Post(t, ctx, wireTenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)),
		SessionID:       s,
		AgentID:         orchestrationtest.PooledAgent,
	})
	if status != http.StatusCreated {
		t.Fatalf("admitting %s answered %d: %s", s, status, body)
	}
}

func wireAttachRequest(s sessionwire.SessionID, host sessionwire.HostID, generation uint64, agent sessionwire.AgentID) sessionwire.HostLinkAttachRequest {
	return sessionwire.HostLinkAttachRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: wireTenant, SessionID: s, AgentID: agent,
		HostID: host, HostGeneration: generation, Mode: sessionwire.HostLinkAttachModeCreate,
		RuntimeCompatibilityID: string(orchestrationtest.PooledCompatibility),
		ActorID:                "orchestrationtest-wire-probe", IdempotencyKey: "probe-attach-" + string(s),
	}
}

func wireBindRequest(s sessionwire.SessionID, host sessionwire.HostID, generation, epoch uint64, compat string) sessionwire.HostLinkBindRequest {
	return sessionwire.HostLinkBindRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: wireTenant, SessionID: s,
		HostID: host, HostGeneration: generation, LeaseEpoch: epoch,
		RuntimeCompatibilityID: compat, IdempotencyKey: "probe-bind-" + string(s),
	}
}

// TestHostLinkHostAnswersGoldens freezes what a real Host answers to the
// requests a Factory does not send: the controller's drain RPCs, one request
// per reachable refusal code, a malformed record, and the connect negotiation's
// accepted and refused shapes.
func TestHostLinkHostAnswersGoldens(t *testing.T) {
	ctx := wireContext(t)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{wireTenant},
	})
	admitter := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica: "orchestrationtest-wire-admitter", WithoutPendingCommands: true,
	})
	tap := orchestrationtest.NewHostLinkTap()
	pooled := orchestrationtest.StartTappedPooledHost(t, ctx, world, wireHostID, wireHostGen, tap)
	var sessions []sessionwire.SessionID
	for i := range 9 {
		s := sessionwire.SessionID(fmt.Sprintf("session-wire-probe-%d", i))
		sessions = append(sessions, s)
		wireAdmit(t, ctx, admitter, s, sessionwire.CommandID(fmt.Sprintf("command-wire-probe-create-%d", i)))
	}
	n := liveNormalizer(pooled.Base, sessions, nil)
	probe := dialHostProbe(t, pooled.Base, coreConnect(t))
	const probeSource = "a Core-framed probe -> host (pinned in go.mod); the reply is Host's"
	refusal := func(name, method string, data []byte, wantCode sessionwire.HostLinkErrorCode) {
		t.Helper()
		reply, err := probe.call(t, ctx, method, data)
		if err != nil {
			t.Fatalf("%s: the probe's %s failed at the transport: %v", name, method, err)
		}
		var refused sessionwire.HostLinkError
		if err := refused.UnmarshalJSON(reply); err != nil || refused.Code != wantCode {
			t.Fatalf("%s: the Host answered %s (%v), want a Core HostLinkError %q", name, reply, err, wantCode)
		}
		checkWireGolden(t, name, goldenExchange(t, n, probeSource, lastExchange(t, tap, method)))
	}

	refusal("hostlink_refusal_runtime_unavailable", sessionwire.HostLinkMethodAttach,
		mustJSON(t, wireAttachRequest(sessions[0], wireHostID, wireHostGen, "orchestrationtest-unregistered-agent")),
		sessionwire.HostLinkErrorRuntimeUnavailable)
	// The pooled Host's capacity is eight; the ninth attach finds none.
	for _, s := range sessions[:8] {
		reply, err := probe.call(t, ctx, sessionwire.HostLinkMethodAttach, mustJSON(t, wireAttachRequest(s, wireHostID, wireHostGen, orchestrationtest.PooledAgent)))
		var observed sessionwire.HostLinkRegistryObservation
		if err != nil || observed.UnmarshalJSON(reply) != nil {
			t.Fatalf("attaching %s: %s %v", s, reply, err)
		}
	}
	refusal("hostlink_refusal_no_capacity", sessionwire.HostLinkMethodAttach,
		mustJSON(t, wireAttachRequest(sessions[8], wireHostID, wireHostGen, orchestrationtest.PooledAgent)),
		sessionwire.HostLinkErrorNoCapacity)
	refusal("hostlink_refusal_epoch_mismatch", sessionwire.HostLinkMethodBind,
		mustJSON(t, wireBindRequest(sessions[0], wireHostID, wireHostGen, 99, string(orchestrationtest.PooledCompatibility))),
		sessionwire.HostLinkErrorEpochMismatch)
	refusal("hostlink_refusal_runtime_mismatch", sessionwire.HostLinkMethodBind,
		mustJSON(t, wireBindRequest(sessions[0], wireHostID, wireHostGen, 1, "orchestrationtest-other-build")),
		sessionwire.HostLinkErrorRuntimeMismatch)

	t.Run("a malformed request is refused at the transport, not answered as a record", func(t *testing.T) {
		_, err := probe.call(t, ctx, sessionwire.HostLinkMethodBind, []byte(`{"version":1,"orchestrationtest_unknown_member":true}`))
		if err == nil {
			t.Fatalf("the Host answered a bind carrying an unknown member as if it were valid")
		}
		checkWireGolden(t, "hostlink_malformed_request", goldenExchange(t, n, probeSource, lastExchange(t, tap, sessionwire.HostLinkMethodBind)))
	})

	t.Run("the controller's drain and drain_status, and not_admitting after it", func(t *testing.T) {
		const fixed = sessionwire.SessionID("session-wire-dedicated")
		const dedicatedID = sessionwire.HostID("orchestrationtest-wire-dedicated")
		wireAdmit(t, ctx, admitter, fixed, "command-wire-probe-create-dedicated")
		dedicatedTap := orchestrationtest.NewHostLinkTap()
		dedicated := orchestrationtest.StartDedicatedHost(t, ctx, world, dedicatedID, 3, fixed, dedicatedTap)
		dn := liveNormalizer(dedicated.Base, []sessionwire.SessionID{fixed}, nil)
		dedicated.Attach(t, ctx, wireTenant, fixed)

		drainer, err := hostLinkDrainClient()
		if err != nil {
			t.Fatal(err)
		}
		request := sessionwire.HostLinkDrainRequest{
			Version: sessionwire.CurrentWireVersion, HostID: dedicatedID, HostGeneration: 3,
			IdempotencyKey: "orchestrationtest-wire-drain", TenantID: wireTenant, SessionID: fixed,
		}
		if _, err := drainer.StartDrain(ctx, dedicated.Base, request); err != nil {
			t.Fatalf("the controller's drain: %v", err)
		}
		const drainSource = "controller drain client -> host (both pinned in go.mod), captured live"
		checkWireGolden(t, "hostlink_drain", goldenExchange(t, dn, drainSource, lastExchange(t, dedicatedTap, sessionwire.HostLinkMethodDrain)))

		// not_admitting: a bind to a Host that has begun draining.
		dprobe := dialHostProbe(t, dedicated.Base, coreConnect(t))
		reply, err := dprobe.call(t, ctx, sessionwire.HostLinkMethodBind,
			mustJSON(t, wireBindRequest(fixed, dedicatedID, 3, 1, string(orchestrationtest.PooledCompatibility))))
		var refused sessionwire.HostLinkError
		if err != nil || refused.UnmarshalJSON(reply) != nil || refused.Code != sessionwire.HostLinkErrorNotAdmitting {
			t.Fatalf("a bind to a draining Host answered %s (%v), want not_admitting", reply, err)
		}
		checkWireGolden(t, "hostlink_refusal_not_admitting", goldenExchange(t, dn, probeSource, lastExchange(t, dedicatedTap, sessionwire.HostLinkMethodBind)))

		// drain_status once the drain has finished, so the frozen state is the
		// terminal one rather than whichever a race produced.
		orchestrationtest.PooledWait(t, "the dedicated Host drained", 60*time.Second, func() bool {
			status, err := drainer.DrainStatus(ctx, dedicated.Base, request)
			return err == nil && status.State == sessionwire.HostLinkDrainStateDrained
		})
		checkWireGolden(t, "hostlink_drain_status", goldenExchange(t, dn, drainSource, lastExchange(t, dedicatedTap, sessionwire.HostLinkMethodDrainStatus)))
	})

	t.Run("version negotiation: exact accepted and refused shapes", func(t *testing.T) {
		endpoint, err := sessionwire.HostLinkEndpoint(pooled.Base, wireTenant)
		if err != nil {
			t.Fatal(err)
		}
		type attempt struct {
			Label      string `json:"label"`
			Credential string `json:"credential"`
			Data       string `json:"data"`
		}
		attempts := []attempt{
			{"the current version alone", "${hostlink_token}", `{"supported_versions":[1]}`},
			{"the current version among unsupported ones", "${hostlink_token}", `{"supported_versions":[1,2,9]}`},
			{"only unsupported versions", "${hostlink_token}", `{"supported_versions":[2]}`},
			{"an unknown member (the request decoder is strict)", "${hostlink_token}", `{"supported_versions":[1],"orchestrationtest_unknown":true}`},
			{"the B8 wrapped shape", "${hostlink_token}", `{"version_negotiation":{"supported_versions":[1]}}`},
			{"a base64 version list", "${hostlink_token}", `{"supported_versions":"AQ=="}`},
			{"no versions", "${hostlink_token}", `{"supported_versions":[]}`},
			{"an unknown service credential", "orchestrationtest-not-a-token", `{"supported_versions":[1]}`},
		}
		outcomes := make([]any, 0, len(attempts))
		requests := make([]any, 0, len(attempts))
		for _, a := range attempts {
			requests = append(requests, a)
			credential := strings.ReplaceAll(a.Credential, "${hostlink_token}", orchestrationtest.PooledServiceToken)
			result := orchestrationtest.RawHostLinkConnect(t, endpoint, credential, "centrifuge-json", []byte(a.Data), 10*time.Second)
			outcome := map[string]any{"label": a.Label}
			switch {
			case result.Connected:
				reply, err := n.frame(result.ReplyData)
				if err != nil {
					t.Fatalf("%s: the connect reply is not JSON: %v", a.Label, err)
				}
				outcome["connected"] = reply
				if _, err := sessionwire.DecodeHostLinkConnectReply(result.ReplyData); err != nil {
					t.Errorf("%s: Core refused the Host's connect reply: %v", a.Label, err)
				}
			case result.Disconnected:
				outcome["disconnected"] = map[string]any{"code": result.Code, "reason": result.Reason}
			default:
				t.Fatalf("%s: neither connected nor disconnected: %+v", a.Label, result)
			}
			outcomes = append(outcomes, outcome)
		}
		checkWireGolden(t, "hostlink_connect_negotiation", wireGolden{
			Source:  "a centrifuge-go client -> host (pinned in go.mod); the outcome is Host's",
			Request: requests,
			Reply:   outcomes,
		})
	})
}

// ---- leg 2: the frozen Factory requests, replayed to a real Host ---------------------------------

// TestHostAnswersFactoryGoldensUnchanged sends the frozen requests Factory made
// to a FRESH real Host, and holds its replies to the frozen replies. A failure
// here is a Host-side wire change: the requests are the ones frozen from the
// released Factory, byte for byte apart from this run's identifiers.
func TestHostAnswersFactoryGoldensUnchanged(t *testing.T) {
	ctx := wireContext(t)
	const replaySession = sessionwire.SessionID("session-wire-replay")
	const replayCreate = sessionwire.CommandID("command-wire-replay-create")
	const replayInput = sessionwire.CommandID("command-wire-replay-input")
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{wireTenant},
	})
	admitter := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica: "orchestrationtest-wire-admitter", WithoutPendingCommands: true,
	})
	tap := orchestrationtest.NewHostLinkTap()
	pooled := orchestrationtest.StartTappedPooledHost(t, ctx, world, wireHostID, wireHostGen, tap)
	wireAdmit(t, ctx, admitter, replaySession, replayCreate)
	n := liveNormalizer(pooled.Base, []sessionwire.SessionID{replaySession}, []sessionwire.CommandID{replayCreate, replayInput})
	now := time.Now()
	fill := wireFill{
		strings: map[string]string{
			"${session}":          string(replaySession),
			"${hostlink_channel}": sessionwire.HostLinkChannel(wireTenant, replaySession),
			"${command_id}":       string(replayCreate),
			"${idempotency_key}":  "orchestrationtest-wire-replay",
			"${hostlink_token}":   orchestrationtest.PooledServiceToken,
			"${host_base}":        string(pooled.Base),
		},
		counters: map[string]uint64{"lease_epoch": 1},
		times:    map[string]time.Time{"observed_at": now, "expires_at": now.Add(time.Minute)},
	}
	compare := func(name string, exchange wireExchange) {
		t.Helper()
		golden := loadWireGolden(t, name)
		if got := normalizeLine(t, n, *exchange.Reply); !reflect.DeepEqual(got, golden.Reply) {
			t.Errorf("HOST MOVED: its reply to the frozen %s is\n%s\nwant the frozen\n%s", name, mustJSON(t, got), mustJSON(t, golden.Reply))
		}
	}

	connect := loadWireGolden(t, "hostlink_connect")
	probe := dialHostProbe(t, pooled.Base, fill.bytes(t, member(connect.Request, "connect", "data")))
	compare("hostlink_connect", lastExchange(t, tap, "connect"))

	for _, name := range []string{"hostlink_attach", "hostlink_bind"} {
		golden := loadWireGolden(t, name)
		method, _ := member(golden.Request, "rpc", "method").(string)
		if _, err := probe.call(t, ctx, method, fill.bytes(t, member(golden.Request, "rpc", "data"))); err != nil {
			t.Fatalf("replaying %s: %v", name, err)
		}
		compare(name, lastExchange(t, tap, method))
	}

	subscribe := loadWireGolden(t, "hostlink_subscribe")
	channel, _ := fill.apply(t, "channel", member(subscribe.Request, "subscribe", "channel")).(string)
	sub, err := probe.client.NewSubscription(channel)
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Subscribe(); err != nil {
		t.Fatal(err)
	}
	compare("hostlink_subscribe", lastExchange(t, tap, "subscribe"))

	// The create was applied on attach, before anything subscribed; the
	// delivery replayed is for an input admitted now, so its publications
	// have a subscriber to reach.
	status, body := admitter.Post(t, ctx, wireTenant, "/v1/sessions/"+string(replaySession)+"/input", sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(replayInput)),
		SessionID:       replaySession,
		Blocks:          json.RawMessage(`[{"type":"text","text":"replayed"}]`),
	})
	if status < 200 || status > 299 {
		t.Fatalf("admitting the replay input answered %d: %s", status, body)
	}
	fill.strings["${command_id}"] = string(replayInput)
	delivery := loadWireGolden(t, "hostlink_command_delivery")
	method, _ := fill.apply(t, "method", delivery.Method).(string)
	if _, err := probe.call(t, ctx, method, fill.bytes(t, member(delivery.Request, "rpc", "data"))); err != nil {
		t.Fatalf("replaying the delivery: %v", err)
	}
	compare("hostlink_command_delivery", lastExchange(t, tap, method))

	orchestrationtest.PooledWait(t, "the Host published on the replayed binding", 60*time.Second, func() bool {
		return len(wirePushes(wireLines(t, tap))) > 0
	})
	publication := loadWireGolden(t, "hostlink_publication")
	if got := normalizeLine(t, n, wirePushes(wireLines(t, tap))[0]); !reflect.DeepEqual(got, publication.Push) {
		t.Errorf("HOST MOVED: its publication is\n%s\nwant the frozen\n%s", mustJSON(t, got), mustJSON(t, publication.Push))
	}

	unbind := loadWireGolden(t, "hostlink_unbind")
	if _, err := probe.call(t, ctx, sessionwire.HostLinkMethodUnbind, fill.bytes(t, member(unbind.Request, "rpc", "data"))); err != nil {
		t.Fatalf("replaying the unbind: %v", err)
	}
	compare("hostlink_unbind", lastExchange(t, tap, sessionwire.HostLinkMethodUnbind))
}

// ---- leg 3: the frozen Host replies, replayed to a real Factory ------------------------------------

// standinRun is one real Factory driving a stand-in Host that answers only
// with frozen replies.
type standinRun struct {
	world   *orchestrationtest.PooledWorld
	standin *orchestrationtest.ScriptedHost
	viewer  *orchestrationtest.PooledViewer
	tap     *orchestrationtest.HostLinkTap
	viewTap *orchestrationtest.HostLinkTap
	n       *wireNormalizer
}

// standinOptions choose what the stand-in does beyond the frozen replies.
type standinOptions struct {
	// register makes the stand-in record itself as the session's owner on
	// attach, as a real Host does. Without it, a bind can only come from
	// Factory having ACCEPTED the attach reply, which is what the strictness
	// arm needs to observe.
	register bool
	// mutateAttachReply edits the frozen attach reply before it is sent.
	mutateAttachReply func(map[string]any)
	// connectVersion, when nonzero, replaces the frozen connect reply's
	// selected wire version.
	connectVersion int
}

const (
	standinSession = sessionwire.SessionID("session-wire-standin")
	standinCreate  = sessionwire.CommandID("command-wire-standin-create")
)

// startStandinRun composes the stand-in, a real Factory and a viewer, and
// admits one create. mutateAttachReply, when set, edits the frozen attach reply
// before it is sent.
func startStandinRun(t *testing.T, ctx context.Context, options standinOptions) standinRun {
	t.Helper()
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{wireTenant},
	})
	tap := orchestrationtest.NewHostLinkTap()
	var mu sync.Mutex
	var base sessionwire.InternalEndpoint
	fillFor := func() wireFill {
		mu.Lock()
		defer mu.Unlock()
		now := time.Now()
		return wireFill{
			strings: map[string]string{
				"${session}":          string(standinSession),
				"${hostlink_channel}": sessionwire.HostLinkChannel(wireTenant, standinSession),
				"${command_id}":       string(standinCreate),
				"${idempotency_key}":  "orchestrationtest-wire-standin",
				"${host_base}":        string(base),
			},
			counters: map[string]uint64{"lease_epoch": 1},
			times:    map[string]time.Time{"observed_at": now, "expires_at": now.Add(time.Minute)},
		}
	}
	replyData := func(name string) []byte {
		data := member(loadWireGolden(t, name).Reply, "rpc", "data")
		if data == nil {
			return nil
		}
		return fillFor().bytes(t, data)
	}
	connectGolden := loadWireGolden(t, "hostlink_connect")
	var standin *orchestrationtest.ScriptedHost
	standin = orchestrationtest.StartScriptedHost(t, orchestrationtest.ScriptedHostConfig{
		ID: wireHostID, Generation: wireHostGen, Wrap: tap.Wrap,
		Connect: func([]byte) ([]byte, error) {
			// The frozen reply, plus what a NEWER Host may add: a capability
			// token this Core has no constant for, and a negotiation member it
			// has never seen. Core's reply decoder tolerates both by contract,
			// and Factory must keep connecting through them.
			reply, _ := fillFor().apply(t, "", member(connectGolden.Reply, "connect", "data")).(map[string]any)
			methods, _ := reply["hostlink_methods"].([]any)
			reply["hostlink_methods"] = append(append([]any{}, methods...), "hostlink.command.orchestrationtest_future_kind")
			reply["orchestrationtest_future_member"] = map[string]any{"any": "shape"}
			if options.connectVersion != 0 {
				reply["version"] = options.connectVersion
			}
			return json.Marshal(reply)
		},
		RPC: func(method string, data []byte) ([]byte, error) {
			switch {
			case method == sessionwire.HostLinkMethodAttach:
				var request sessionwire.HostLinkAttachRequest
				if err := request.UnmarshalJSON(data); err != nil {
					return nil, err
				}
				// A real Host registers the residency it just established;
				// Factory's directory reads that registration to find the owner.
				if options.register {
					orchestrationtest.RegisterScriptedHost(t, ctx, world, wireTenant, request.SessionID, standin)
				}
				reply := replyData("hostlink_attach")
				if options.mutateAttachReply != nil {
					var object map[string]any
					if err := json.Unmarshal(reply, &object); err != nil {
						return nil, err
					}
					options.mutateAttachReply(object)
					return json.Marshal(object)
				}
				return reply, nil
			case method == sessionwire.HostLinkMethodBind:
				return replyData("hostlink_bind"), nil
			case method == sessionwire.HostLinkMethodUnbind:
				return replyData("hostlink_unbind"), nil
			case strings.HasPrefix(method, sessionwire.HostLinkChannelPrefix):
				return replyData("hostlink_command_delivery"), nil
			}
			return nil, fmt.Errorf("the stand-in has no frozen reply for %q", method)
		},
	})
	mu.Lock()
	base = standin.Base
	mu.Unlock()
	orchestrationtest.PublishScriptedHostTarget(t, ctx, world, standin)
	viewTap := orchestrationtest.NewHostLinkTap()
	served := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica:  "orchestrationtest-wire-standin-replica",
		Listener: viewTap.WrapListener,
	})
	viewer := orchestrationtest.ConnectPooledViewer(t, ctx, served, wireTenant)
	if err := viewer.Watch(t, ctx, wireTenant, standinSession); err != nil {
		t.Fatalf("the viewer's subscribe was refused: %v", err)
	}
	wireAdmit(t, ctx, served, standinSession, standinCreate)
	return standinRun{
		world:   world,
		standin: standin,
		viewer:  viewer,
		tap:     tap,
		viewTap: viewTap,
		n:       liveNormalizer(standin.Base, []sessionwire.SessionID{standinSession}, []sessionwire.CommandID{standinCreate}),
	}
}

func (r standinRun) sent(method string) bool {
	for _, rpc := range r.standin.RPCs() {
		if rpc == method || (method == sessionwire.HostLinkChannelPrefix && strings.HasPrefix(rpc, method)) {
			return true
		}
	}
	return false
}

// standinPublication is the frozen HostLink publication, filled for session s
// at sequence seq.
func standinPublication(t *testing.T, s sessionwire.SessionID, seq uint64) map[string]any {
	t.Helper()
	data := member(loadWireGolden(t, "hostlink_publication").Push, "push", "pub", "data")
	fill := wireFill{
		strings:  map[string]string{"${session}": string(s), "${event_id}": fmt.Sprintf("event-standin-%d", seq)},
		counters: map[string]uint64{"journal_seq": seq, "covered_through": seq, "seq": seq},
	}
	record, _ := fill.apply(t, "", data).(map[string]any)
	return record
}

// publishUntilSeen republishes record until the viewer holds want. The
// stand-in's subscribe callback can return before the hub routes the channel
// to the client, and a push in that window reaches nobody; a repeat of one
// sequence is a duplicate the relay may drop.
func publishUntilSeen(t *testing.T, run standinRun, channel string, record map[string]any, want string) {
	t.Helper()
	orchestrationtest.PooledWait(t, want+" reached the viewer", 30*time.Second, func() bool {
		if run.viewer.Has(want) {
			return true
		}
		run.standin.Publish(t, channel, mustJSON(t, record))
		time.Sleep(100 * time.Millisecond)
		return run.viewer.Has(want)
	})
}

// TestFactoryAcceptsHostGoldensUnchanged drives a REAL Factory against a Host
// that answers only with the frozen replies. A failure here is a Factory-side
// wire change: the Host half of every exchange is the frozen one.
func TestFactoryAcceptsHostGoldensUnchanged(t *testing.T) {
	ctx := wireContext(t)

	t.Run("Factory walks attach, bind, subscribe and deliver on frozen replies, sending frozen requests", func(t *testing.T) {
		run := startStandinRun(t, ctx, standinOptions{register: true})
		channel := sessionwire.HostLinkChannel(wireTenant, standinSession)
		orchestrationtest.PooledWait(t, "Factory subscribed to the stand-in's session channel", 60*time.Second, func() bool {
			return run.standin.Subscribed(channel)
		})
		orchestrationtest.PooledWait(t, "Factory delivered the create", 60*time.Second, func() bool {
			return run.sent(sessionwire.HostLinkChannelPrefix)
		})
		// The stand-in records a subscribe or RPC before its reply is on the
		// wire; the comparison waits for the tap to hold both answers.
		answered := func(kind string) bool {
			for _, exchange := range wireExchanges(wireLines(t, run.tap)) {
				if exchange.Kind == kind && exchange.Reply != nil {
					return true
				}
			}
			return false
		}
		orchestrationtest.PooledWait(t, "the tap recorded the answered subscribe and delivery", 10*time.Second, func() bool {
			return answered("subscribe") && answered(channel)
		})
		exchanges := wireExchanges(wireLines(t, run.tap))
		for name, kind := range map[string]string{
			"hostlink_connect":          "connect",
			"hostlink_attach":           sessionwire.HostLinkMethodAttach,
			"hostlink_bind":             sessionwire.HostLinkMethodBind,
			"hostlink_subscribe":        "subscribe",
			"hostlink_command_delivery": channel,
		} {
			golden := loadWireGolden(t, name)
			if got := normalizeLine(t, run.n, firstExchange(t, exchanges, kind).Command); !reflect.DeepEqual(got, golden.Request) {
				t.Errorf("FACTORY MOVED: its %s request is\n%s\nwant the frozen\n%s", name, mustJSON(t, got), mustJSON(t, golden.Request))
			}
		}

		// The frozen publication reaches the viewer, and so does what a NEWER
		// Host may add to one: Core defines EnduringPublication's unknown
		// members as forward-compatible PUBLIC members, retained on decode,
		// so Factory must relay them rather than refuse the record. (Which is
		// why keeping private data out of a publication is the runtime's
		// obligation and not Factory's: Factory cannot tell a private member
		// from a future public one. The live leg holds the real Host to it.)
		//
		// After it, on the same channel, the stand-in pushes a frame that is
		// not a valid Core record, which must not reach the browser in any
		// form. (Foreign-scope records are finding F1's own run, below.)
		publication := func(s sessionwire.SessionID, seq uint64) map[string]any { return standinPublication(t, s, seq) }
		extended := publication(standinSession, 1)
		extended["orchestrationtest_future_member"] = "retained"
		// Republished until it lands: the stand-in's subscribe callback can
		// return before the hub routes the channel to the client, and a push
		// in that window reaches nobody. A repeat of one sequence is a
		// duplicate the relay may drop; the first to land is the one asserted.
		publishUntilSeen(t, run, channel, extended, "E1")
		publishUntilSeen(t, run, channel, publication(standinSession, 2), "E2")
		// Only then a record that is valid JSON but not a valid Core record
		// (no journal_seq). Factory fails the tail CLOSED on it -- measured: it
		// unsubscribes from the session channel and restarts the tail -- so
		// anything pushed behind it would be lost, which is why it goes last.
		// The unsubscribe proves Factory read and refused it rather than never
		// receiving it. (A frame that is not JSON at all is refused by the
		// centrifuge client itself, which closes the whole link with 3506 --
		// that exercises the transport, not Factory, so it is not pushed.)
		unsubscribes := func() int {
			n := 0
			for _, exchange := range wireExchanges(wireLines(t, run.tap)) {
				if exchange.Kind == "unsubscribe" {
					n++
				}
			}
			return n
		}
		before := unsubscribes()
		run.standin.Publish(t, channel, []byte(`{"type":"enduring_publication","tenant_id":"orchestrationtest-tenant-a","session_id":"session-wire-standin","event_id":"orchestrationtest-not-a-record","covered_through":3,"body":{}}`))
		orchestrationtest.PooledWait(t, "Factory failed the tail closed on the invalid record", 30*time.Second, func() bool {
			return unsubscribes() > before
		})
		t.Logf("the viewer received %v", run.viewer.Records())
		// Read off the ClientLink wire itself, not the viewer's summary: a
		// summary decodes through Core and would hide a frame Core refuses.
		retained := false
		for _, line := range wireLines(t, run.viewTap) {
			if !line.FromServer {
				continue
			}
			if strings.Contains(string(line.Raw), "orchestrationtest-not-a-record") {
				t.Errorf("LEAK: a malformed Host frame reached the browser: %s", line.Raw)
			}
			retained = retained || strings.Contains(string(line.Raw), "orchestrationtest_future_member")
		}
		if !retained {
			t.Errorf("Factory dropped a publication member Core retains as forward-compatible; a newer Host's public field would never reach a browser")
		}
	})

	// FINDING F1, CLOSED BY factory v0.8.1. Before it, Factory relayed a Host
	// publication onto the session channel it arrived on WITHOUT checking that
	// the record names that channel's tenant and session, so a record naming
	// another session of the tenant, and one naming ANOTHER TENANT, both
	// reached this session's browser (this row was a trip-wire holding that).
	// v0.8.1 refuses such a record: it never reaches a viewer, and the
	// existing refused-record repair runs -- viewers get a session.reset and
	// the tail is re-bound. Only a faulty or compromised Host can send one.
	//
	// It has its OWN stand-in run and judges the foreign records ONLY on the
	// ClientLink wire. Each foreign record is pushed on its own tail: a refusal
	// fails the tail closed, so a record pushed behind the first would be lost
	// to the teardown, not refused, and would prove nothing.
	t.Run("F1 closed: a Host record naming another session or tenant is refused and the viewer is reset", func(t *testing.T) {
		run := startStandinRun(t, ctx, standinOptions{register: true})
		channel := sessionwire.HostLinkChannel(wireTenant, standinSession)
		orchestrationtest.PooledWait(t, "Factory subscribed to the stand-in's session channel", 60*time.Second, func() bool {
			return run.standin.Subscribed(channel)
		})
		// A valid record first: proves the relay is live, so an absence
		// below is Factory's decision, not a tail that was never up.
		publishUntilSeen(t, run, channel, standinPublication(t, standinSession, 1), "E1")

		count := func(kind string) int {
			n := 0
			for _, exchange := range wireExchanges(wireLines(t, run.tap)) {
				if exchange.Kind == kind && exchange.Reply != nil {
					n++
				}
			}
			return n
		}
		resets := func() int {
			n := 0
			for _, record := range run.viewer.Records() {
				if strings.HasPrefix(record, "R") {
					n++
				}
			}
			return n
		}
		foreignTenant := standinPublication(t, standinSession, 3)
		foreignTenant["tenant_id"] = string(orchestrationtest.PooledTenantB)
		for _, push := range []struct {
			name   string
			record map[string]any
		}{
			{"another session of the tenant", standinPublication(t, "session-wire-foreign", 2)},
			{"another tenant", foreignTenant},
		} {
			// The repair's reset names the session's DURABLE tip, which must
			// be at or above what the viewer already holds, or Factory closes
			// the viewer's link instead (an incoherent reset). The stand-in
			// keeps no journal, so the kit's committed tip stands in for the
			// one a real Host's runtime would have written first.
			run.world.Tails.Hint(wireTenant, standinSession)
			run.world.Tails.Hint(wireTenant, standinSession)
			subscribes, resetsBefore := count("subscribe"), resets()
			run.standin.Publish(t, channel, mustJSON(t, push.record))
			orchestrationtest.PooledWait(t, "the viewer was reset after the record naming "+push.name, 30*time.Second, func() bool {
				return resets() > resetsBefore
			})
			orchestrationtest.PooledWait(t, "Factory re-bound the tail after refusing the record naming "+push.name, 30*time.Second, func() bool {
				return count("subscribe") > subscribes
			})
		}

		// Bounded window after both refusals: neither foreign record may
		// appear on the ClientLink wire in any form.
		needles := []string{"session-wire-foreign", string(orchestrationtest.PooledTenantB)}
		deadline := time.Now().Add(2 * time.Second)
		for {
			for _, line := range wireLines(t, run.viewTap) {
				for _, needle := range needles {
					if line.FromServer && strings.Contains(string(line.Raw), needle) {
						t.Fatalf("F1 REGRESSED: a Host record naming %q reached the browser: %s", needle, line.Raw)
					}
				}
			}
			if !time.Now().Before(deadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if strays := run.viewer.Strays(); len(strays) > 0 {
			t.Fatalf("F1 REGRESSED: the viewer received foreign records: %v", strays)
		}
		t.Logf("F1 closed: the viewer received %v (foreign records refused, each answered by a reset)", run.viewer.Records())
	})

	t.Run("control: with no registration, the frozen attach reply alone is what makes Factory bind", func(t *testing.T) {
		run := startStandinRun(t, ctx, standinOptions{})
		orchestrationtest.PooledWait(t, "Factory bound on the frozen attach reply", 60*time.Second, func() bool {
			return run.sent(sessionwire.HostLinkMethodBind)
		})
	})

	t.Run("a Host that selects a wire version Factory did not offer is never attached to", func(t *testing.T) {
		run := startStandinRun(t, ctx, standinOptions{register: true, connectVersion: 2})
		orchestrationtest.PooledWait(t, "Factory dialled the stand-in", 60*time.Second, func() bool {
			for _, exchange := range wireExchanges(wireLines(t, run.tap)) {
				if exchange.Kind == "connect" && exchange.Reply != nil {
					return true
				}
			}
			return false
		})
		// Three guards stand behind this row: Core's reply decoder, Core's
		// Validate, and Factory's own version check. Only with all three
		// mutated away does Factory attach (measured), so the row pins the
		// outcome rather than any one guard.
		//
		// Several placement passes: the control arm above attaches within one.
		time.Sleep(10 * orchestrationtest.ReconcileSweepInterval)
		for _, rpc := range run.standin.RPCs() {
			if rpc == sessionwire.HostLinkMethodAttach || rpc == sessionwire.HostLinkMethodBind {
				t.Fatalf("Factory sent %s to a Host whose connect reply selected wire version 2: %v", rpc, run.standin.RPCs())
			}
		}
	})

	t.Run("an attach reply with an unknown member is refused by Factory's strict decoder", func(t *testing.T) {
		run := startStandinRun(t, ctx, standinOptions{mutateAttachReply: func(reply map[string]any) {
			reply["orchestrationtest_unknown_member"] = true
		}})
		orchestrationtest.PooledWait(t, "Factory attached", 60*time.Second, func() bool {
			return run.sent(sessionwire.HostLinkMethodAttach)
		})
		// Several placement passes: a Factory that accepted the reply binds
		// within one.
		time.Sleep(10 * orchestrationtest.ReconcileSweepInterval)
		if run.sent(sessionwire.HostLinkMethodBind) {
			t.Fatalf("Factory BOUND on an attach reply carrying an unknown member; its decoder is no longer strict: %v", run.standin.RPCs())
		}
	})
}

// ---- leg 4: Core owns every frozen payload --------------------------------------------------------

// TestSessionwireGoldensDecodeWithCore decodes every frozen payload through
// Core's own decoder for its record, and re-encodes it: a frozen shape Core
// would refuse or rewrite is a fixture of something Core does not own.
func TestSessionwireGoldensDecodeWithCore(t *testing.T) {
	fill := wireFill{
		strings: map[string]string{
			"${session}":          "session-core",
			"${hostlink_channel}": sessionwire.HostLinkChannel(wireTenant, "session-core"),
			"${command_id}":       "command-core",
			"${idempotency_key}":  "orchestrationtest-core",
			"${host_base}":        "ws://127.0.0.1:1",
			"${event_id}":         "event-core",
		},
		counters: map[string]uint64{"lease_epoch": 1, "current_lease_epoch": 1, "journal_seq": 1, "covered_through": 1,
			"journal_tip": 1, "last_contiguous": 1, "seq": 1},
		times: map[string]time.Time{"observed_at": time.Unix(1, 0), "expires_at": time.Unix(2, 0)},
	}
	type decodable interface{ UnmarshalJSON([]byte) error }
	cases := []struct {
		golden string
		path   []string
		fresh  func() decodable
	}{
		{"hostlink_attach", []string{"request", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkAttachRequest{} }},
		{"hostlink_attach", []string{"reply", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkRegistryObservation{} }},
		{"hostlink_bind", []string{"request", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkBindRequest{} }},
		{"hostlink_unbind", []string{"request", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkUnbindRequest{} }},
		{"hostlink_command_delivery", []string{"request", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkCommandDelivery{} }},
		{"hostlink_publication", []string{"push", "push", "pub", "data"}, func() decodable { return &sessionwire.EnduringPublication{} }},
		{"hostlink_drain", []string{"request", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkDrainRequest{} }},
		{"hostlink_drain", []string{"reply", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkDrainObservation{} }},
		{"hostlink_drain_status", []string{"reply", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkDrainObservation{} }},
		{"hostlink_refusal_epoch_mismatch", []string{"reply", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkError{} }},
		{"hostlink_refusal_runtime_mismatch", []string{"reply", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkError{} }},
		{"hostlink_refusal_runtime_unavailable", []string{"reply", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkError{} }},
		{"hostlink_refusal_no_capacity", []string{"reply", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkError{} }},
		{"hostlink_refusal_not_admitting", []string{"reply", "rpc", "data"}, func() decodable { return &sessionwire.HostLinkError{} }},
		{"clientlink_journal_tip", []string{"push", "push", "pub", "data"}, func() decodable { return &sessionwire.JournalTip{} }},
		{"clientlink_session_reset", []string{"push", "push", "pub", "data"}, func() decodable { return &sessionwire.SessionReset{} }},
		{"clientlink_enduring_publication", []string{"push", "push", "pub", "data"}, func() decodable { return &sessionwire.EnduringPublication{} }},
	}
	for _, c := range cases {
		t.Run(c.golden+"/"+strings.Join(c.path, "."), func(t *testing.T) {
			golden := loadWireGolden(t, c.golden)
			root := map[string]any{"request": golden.Request, "reply": golden.Reply, "push": golden.Push}
			payload := member(root, c.path...)
			if payload == nil {
				t.Fatalf("the fixture has nothing at %v", c.path)
			}
			data := fill.bytes(t, payload)
			record := c.fresh()
			if err := record.UnmarshalJSON(data); err != nil {
				t.Fatalf("Core's decoder refuses the frozen %s: %v\n%s", c.golden, err, data)
			}
			again, err := json.Marshal(record)
			if err != nil {
				t.Fatalf("Core cannot re-encode the frozen %s: %v", c.golden, err)
			}
			var before, after map[string]any
			if err := json.Unmarshal(data, &before); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(again, &after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(keysOf(before), keysOf(after)) {
				t.Errorf("Core re-encodes the frozen %s with members %v; the wire carried %v", c.golden, keysOf(after), keysOf(before))
			}
		})
	}

	t.Run("the connect framing: Core's bare request, Core's bare and tolerant reply", func(t *testing.T) {
		golden := loadWireGolden(t, "hostlink_connect")
		request := fill.bytes(t, member(golden.Request, "connect", "data"))
		if _, err := sessionwire.DecodeHostLinkConnectRequest(request); err != nil {
			t.Fatalf("Core refuses the frozen connect request %s: %v", request, err)
		}
		reply := fill.bytes(t, member(golden.Reply, "connect", "data"))
		response, err := sessionwire.DecodeHostLinkConnectReply(reply)
		if err != nil {
			t.Fatalf("Core refuses the frozen connect reply %s: %v", reply, err)
		}
		for _, method := range []string{sessionwire.HostLinkMethodAttach, sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind,
			sessionwire.HostLinkMethodDrain, sessionwire.HostLinkMethodDrainStatus, sessionwire.HostLinkCapabilityGateResponse} {
			if !response.Supports(method) {
				t.Errorf("the frozen connect reply does not advertise %q", method)
			}
		}
		// Tolerant: a token and a member this Core has never seen are kept,
		// not refused -- the property that lets a newer Host connect to this
		// Factory at all.
		var object map[string]any
		if err := json.Unmarshal(reply, &object); err != nil {
			t.Fatal(err)
		}
		methods, _ := object["hostlink_methods"].([]any)
		object["hostlink_methods"] = append(methods, "hostlink.command.orchestrationtest_future_kind")
		object["orchestrationtest_future_member"] = true
		widened, err := sessionwire.DecodeHostLinkConnectReply(mustJSON(t, object))
		if err != nil {
			t.Fatalf("Core refuses a connect reply carrying an unknown token and member: %v", err)
		}
		if !widened.Supports("hostlink.command.orchestrationtest_future_kind") {
			t.Errorf("Core dropped an unknown capability token instead of retaining it")
		}
		// Strict: the same unknown member on the REQUEST is refused.
		var requestObject map[string]any
		if err := json.Unmarshal(request, &requestObject); err != nil {
			t.Fatal(err)
		}
		requestObject["orchestrationtest_future_member"] = true
		if _, err := sessionwire.DecodeHostLinkConnectRequest(mustJSON(t, requestObject)); err == nil {
			t.Errorf("Core accepted a connect REQUEST carrying an unknown member; the request decoder must be strict")
		}
	})

	t.Run("releasing, the one refusal no request reaches, still round-trips through Core", func(t *testing.T) {
		data, err := json.Marshal(sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorReleasing})
		if err != nil {
			t.Fatalf("Core cannot encode releasing: %v", err)
		}
		var decoded sessionwire.HostLinkError
		if err := decoded.UnmarshalJSON(data); err != nil || decoded.Code != sessionwire.HostLinkErrorReleasing {
			t.Fatalf("Core does not round-trip releasing: %s %v", data, err)
		}
	})
}

// ---- Factory's public refusals ---------------------------------------------------------------

// TestClientLinkRefusalGoldens freezes what Factory's public face answers an
// AUTHENTICATED browser it refuses: a ClientLink protocol it does not speak or
// cannot read, a session the principal is not authorized for (the
// identity.ErrUnauthorized contract: 403 over HTTP, permission denied 103 over
// ClientLink), and a session channel of another tenant. The unauthenticated
// refusal is frozen by the capture above.
func TestClientLinkRefusalGoldens(t *testing.T) {
	ctx := wireContext(t)
	const forbidden = sessionwire.SessionID("session-wire-forbidden")
	const permitted = sessionwire.SessionID("session-wire-permitted")
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{})
	tap := orchestrationtest.NewHostLinkTap()
	served := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica:                "orchestrationtest-wire-refuser",
		WithoutPendingCommands: true,
		Authorizer:             orchestrationtest.DenySessionAuthorizer{Session: forbidden},
		Listener:               tap.WrapListener,
	})
	wireAdmit(t, ctx, served, forbidden, "command-wire-forbidden-create")
	wireAdmit(t, ctx, served, permitted, "command-wire-permitted-create")
	n := liveNormalizer("ws://unused.invalid", []sessionwire.SessionID{forbidden, permitted}, nil)
	const source = "factory ClientLink/HTTP (version pinned in go.mod), an authenticated principal refused"

	for _, row := range []struct {
		name, data, label string
	}{
		{"clientlink_connect_unsupported_protocol", `{"protocol_version":"2"}`, "a protocol_version this build does not speak"},
		{"clientlink_connect_malformed_protocol", `{"protocol_version":1}`, "a protocol_version that is not a string"},
	} {
		t.Run(row.label, func(t *testing.T) {
			before := len(tap.Conns())
			result := rawClientLinkConnect(t, served, orchestrationtest.PooledBearers[wireTenant], row.data)
			if result.Connected {
				t.Fatalf("Factory accepted a ClientLink connect with %s", row.label)
			}
			checkWireGolden(t, row.name, wireGolden{
				Source:  source,
				Request: map[string]any{"connect_data": row.data},
				Reply:   clientLinkRefusal(t, tap, before, n),
			})
		})
	}

	subscribeRefusal := func(t *testing.T, name string, viewerTenant, sessionTenant sessionwire.TenantID, s sessionwire.SessionID) {
		t.Helper()
		before := len(tap.Conns())
		viewer := orchestrationtest.ConnectPooledViewer(t, ctx, served, viewerTenant)
		defer viewer.Close()
		if err := viewer.Watch(t, ctx, sessionTenant, s); err == nil {
			t.Fatalf("%s was allowed to subscribe to %s/%s", viewerTenant, sessionTenant, s)
		}
		exchange := lastExchange(t, tapFrom(tap, before), "subscribe")
		checkWireGolden(t, name, goldenExchange(t, n, source, exchange))
	}

	t.Run("a session the principal is not authorized for: permission denied (103)", func(t *testing.T) {
		subscribeRefusal(t, "clientlink_subscribe_not_authorized", wireTenant, wireTenant, forbidden)
		golden := loadWireGolden(t, "clientlink_subscribe_not_authorized")
		if code := member(golden.Reply, "error", "code"); fmt.Sprint(code) != "103" {
			t.Errorf("an unauthorized subscribe answered code %v, want Factory's contract 103 (permission denied)", code)
		}
		// Control: the same principal subscribes to a session it may see.
		viewer := orchestrationtest.ConnectPooledViewer(t, ctx, served, wireTenant)
		defer viewer.Close()
		if err := viewer.Watch(t, ctx, wireTenant, permitted); err != nil {
			t.Fatalf("the permitted subscribe was refused too, so the 103 is not the authorizer's: %v", err)
		}
	})

	// A subscribe to another tenant's channel. The authorizer here would have
	// permitted it; the refusal is Factory's own. Through factory v0.8.0 it
	// was code 100 "internal server error", temporary -- a browser was told to
	// retry a request that can never succeed. factory v0.8.1 answers it
	// permission denied (103), identical whether or not the session exists.
	t.Run("another tenant's session channel: permission denied (103)", func(t *testing.T) {
		subscribeRefusal(t, "clientlink_subscribe_cross_tenant", wireTenant, orchestrationtest.PooledTenantB, permitted)
		golden := loadWireGolden(t, "clientlink_subscribe_cross_tenant")
		if code := member(golden.Reply, "error", "code"); fmt.Sprint(code) != "103" {
			t.Errorf("a cross-tenant subscribe answered code %v, want factory v0.8.1's 103 (permission denied)", code)
		}
		if temporary := member(golden.Reply, "error", "temporary"); temporary != nil {
			t.Errorf("a cross-tenant subscribe is marked temporary (%v): a browser would retry a request that can never succeed", temporary)
		}
	})

	t.Run("the HTTP equivalent: 403 not_authorized", func(t *testing.T) {
		status, body := served.Get(t, ctx, wireTenant, "/v1/sessions/"+string(forbidden)+"/status")
		if status != http.StatusForbidden {
			t.Fatalf("an unauthorized session read answered %d: %s", status, body)
		}
		frozen, err := n.frame(body)
		if err != nil {
			t.Fatalf("the refusal body is not JSON: %s", body)
		}
		checkWireGolden(t, "http_session_read_not_authorized", wireGolden{
			Source: source,
			Method: "GET /v1/sessions/{sid}/status",
			Reply:  map[string]any{"status": status, "body": frozen},
		})
	})
}

// tapFrom views the connections a tap saw from index before on.
func tapFrom(tap *orchestrationtest.HostLinkTap, before int) *orchestrationtest.HostLinkTap {
	return orchestrationtest.TapOfConns(tap.Conns()[before:])
}
