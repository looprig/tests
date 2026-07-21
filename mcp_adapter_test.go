//go:build integration

// End-to-end tests for the adapter: real fixture MCP servers over a real stdio
// transport, bound into a real in-process Harness rig running real turns.
//
// The unit tests in this package drive scripted transports and fake loop
// controllers, which prove the adapter's own logic against this module's idea of
// Harness and of MCP. These prove the idea: a real Session's idle really does
// carry a Loop to a new catalog generation, a real permission gate really does
// see the qualified MCP identity, and a real model really is handed
// "mcp__<binding>__<tool>" and really gets an "error: ...ToolUnavailable" string
// back when the server has dropped the tool underneath it.
//
// # The one seam that is NOT driven through a real Session
//
// Deps.Gates (GateOpener) is answered by a test host, not by a Harness gate
// directory, and that is the design rather than a shortcut: there is no public
// route in Harness to OPEN a gate.KindForm/gate.KindOpenURL gate at all
// (sessionruntime's PrepareGateOpen/ActivateGate are internal, and RespondGate
// translates only the loop-actor gate kinds), and MCP's initialize-time
// elicitation has no Loop to hang a gate on in the first place. Design
// §Elicitation puts the renderer — TUI, HTTP client, headless policy —
// downstream of the adapter's GateOpener, so a host implementing GateOpener IS
// the contract. The elicitation tests below therefore assert that the envelope
// handed to that host is a real, valid Harness gate (gate.ValidateFormSchema,
// gate.MarshalPayload round-trip, gate.ParseFormAnswers) and that the human's
// answer travels all the way back to the MCP server that asked. They do not
// claim to have opened a gate in a Session's gate directory.
//
// Every other gate here — the permission gates in TestPermissionGate* — IS a
// real Harness gate, opened by the real loop runner and answered through the
// real session.SessionController.RespondGate.

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/inference"
	"github.com/looprig/inference/stream"
	"github.com/looprig/mcp/pkg/client"
	mcpharness "github.com/looprig/mcp/pkg/harness"
	"github.com/looprig/mcp/pkg/transport/stdio"
	"github.com/looprig/storage/memstore"
)

// --- the fixture server -----------------------------------------------------
//
// # Why this module builds the fixture binary itself
//
// The fixture MCP server and its build helper (mcptest.BuildFixture) live under
// github.com/looprig/mcp/internal/mcptest, and `internal` means exactly what it
// says: this module cannot import them. That is not an accident to route around.
// mcptest.NewServer returns *mcp.Server — a go-sdk type — and mcp's CLAUDE.md
// holds SDK types behind pkg/client/internal/protocol, with internal/protocol's
// leak guard explicitly allowlisting internal/mcptest. Promoting that package to
// keep this file's import working would widen mcp's public API to export the very
// SDK surface the module is built to contain.
//
// So this module builds the same binary from the same source the same way, by
// exec'ing `go build` inside the mcp module directory — where the internal path is
// a perfectly ordinary package. Nothing is stubbed and no assertion is softened:
// the tests below drive the real fixture subprocess over a real stdio transport,
// byte for byte what mcp's own suite drove.
//
// The one cost is honest and worth naming: the tool-name constants below are
// duplicated from internal/mcptest rather than imported, so a rename there surfaces
// here as a test failure rather than a compile error. The constants are the
// fixture's wire contract and change roughly never; paying for compile-time
// coupling with a widened public API on the module under test is the worse trade.

// The fixture's tool names, mirrored from github.com/looprig/mcp/internal/mcptest.
// Keep in sync with that package — it is the source of truth.
const (
	fixtureToolEcho    = "echo"
	fixtureToolMutate  = "mutate"
	fixtureToolMutated = "echo2"
	fixtureToolCrash   = "crash"
	fixtureToolElicit  = "elicit"

	fixtureExtraToolPrefix = "extra_"
	// Note the trailing space — the fixture builds its result as
	// ElicitAnswerPrefix + action.
	fixtureElicitAnswerPrefix = "elicited: "
)

// mcpModuleDir is the mcp module root, relative to this module's directory. It is
// the same directory tests' go.mod replaces github.com/looprig/mcp with.
const mcpModuleDir = "../mcp"

// fixturePkg is the command built below — an import path that is internal to the
// mcp module and therefore only ever resolved from inside it (see cmd.Dir).
const fixturePkg = "github.com/looprig/mcp/internal/mcptest/cmd/fixture"

// mcpFixtureBuildTimeout bounds the build. A cold build of mcp's vendored SDK is a
// few seconds; anything near this is a hang, and a test that hangs reports nothing.
const mcpFixtureBuildTimeout = 3 * time.Minute

// buildMCPFixture builds the fixture MCP server and returns the path to the binary.
// It is the cross-module stand-in for mcptest.BuildFixture and matches its contract:
// the binary goes in t.TempDir() (removed with the test, no two tests race over it),
// each call is a fresh `go build` that Go's build cache makes cheap after the first,
// and a build failure fails the test with the compiler's own output.
func buildMCPFixture(t *testing.T) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), mcpFixtureBuildTimeout)
	defer cancel()

	out := filepath.Join(t.TempDir(), "fixture")
	if runtime.GOOS == "windows" {
		out += ".exe"
	}
	root, err := filepath.Abs(mcpModuleDir)
	if err != nil {
		t.Fatalf("locating the mcp module: %v", err)
	}

	// Explicit argv, never a shell string. cmd.Dir is the mcp module root, so the
	// build resolves fixturePkg in the module that owns it (and against that
	// module's vendor dir) rather than through this module's dependency graph —
	// which is what makes building an internal package legitimate rather than a
	// dodge. -trimpath per mcp's build rules; CGO off for a static fixture that
	// cannot depend on the host toolchain's C environment. GOWORK=off matches the
	// Makefile so a stray workspace file cannot repoint the build.
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", out, fixturePkg) // #nosec G204 -- fixed argv; out is under the test's own TempDir
	cmd.Dir = root
	cmd.Env = append(cmd.Environ(), "CGO_ENABLED=0", "GOWORK=off")

	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building %s in %s: %v\n%s", fixturePkg, root, err, combined)
	}
	return out
}

// permissionIdentity is the stable capability identity of one MCP tool, mirrored
// from mcp's pkg/harness (where it is unexported). Spelling the format out here is
// deliberate: asserting against the adapter's own helper would be tautological —
// it would agree with any format the adapter happened to produce. This pins the
// literal an approval is persisted against, so a change to it fails loudly.
func permissionIdentity(binding, rawTool string) string {
	return "mcp:" + binding + ":" + rawTool
}

// recordingReporter is the host's Reporter sink, mirrored from mcp's pkg/harness
// unit tests (a test-only double, so it does not cross the module boundary).
type recordingReporter struct {
	mu      sync.Mutex
	notices []mcpharness.Notice
}

func (r *recordingReporter) Report(n mcpharness.Notice) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notices = append(r.notices, n)
}

func (r *recordingReporter) snapshot() []mcpharness.Notice {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]mcpharness.Notice(nil), r.notices...)
}

// itTimeout bounds each test's own work. Generous: a slow box must not turn a
// passing assertion into a flake, and every assertion below fails on its own
// terms long before this.
const itTimeout = 90 * time.Second

// settle is how long a wait-for helper will poll before giving up. It is a
// failure bound, not a latency budget.
const settle = 30 * time.Second

func itCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	t.Cleanup(cancel)
	return ctx
}

// --- the scripted model -----------------------------------------------------

// scriptLLM is an inference.Client that answers each request from a script and
// records what it was asked.
//
// It is the test's model, and it is also the test's principal OBSERVABLE: the
// tools a Loop is holding are exactly req.Tools, and a tool's result is exactly
// the ToolResultMessage in the next request's Messages. Both are facts produced
// by the real loop runner, not by anything this file arranges.
type scriptLLM struct {
	mu      sync.Mutex
	replies [][]content.Chunk
	n       int
	reqs    []inference.Request
}

func newScriptLLM(replies ...[]content.Chunk) *scriptLLM {
	return &scriptLLM{replies: replies}
}

func (s *scriptLLM) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, fmt.Errorf("scriptLLM: Invoke is not used; the loop streams")
}

func (s *scriptLLM) Stream(_ context.Context, req inference.Request) (*stream.StreamReader[content.Chunk], error) {
	s.mu.Lock()
	s.reqs = append(s.reqs, req)
	var chunks []content.Chunk
	if s.n < len(s.replies) {
		chunks = s.replies[s.n]
	} else {
		// Past the script: say something and stop. A turn that never ends would
		// hang the Session rather than fail an assertion.
		chunks = []content.Chunk{&content.TextChunk{Text: "done"}}
	}
	s.n++
	s.mu.Unlock()

	i := 0
	next := func() (content.Chunk, error) {
		if i < len(chunks) {
			c := chunks[i]
			i++
			return c, nil
		}
		return nil, io.EOF
	}
	return stream.NewStreamReader(next, nil), nil
}

// requests returns every request the loop has made so far.
func (s *scriptLLM) requests() []inference.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]inference.Request(nil), s.reqs...)
}

// waitRequests blocks until the model has been asked n times.
func (s *scriptLLM) waitRequests(t *testing.T, n int) []inference.Request {
	t.Helper()
	deadline := time.Now().Add(settle)
	for {
		reqs := s.requests()
		if len(reqs) >= n {
			return reqs
		}
		if time.Now().After(deadline) {
			t.Fatalf("the model was asked %d times, want at least %d", len(reqs), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// toolNames returns the model-facing tool names offered on request i.
func toolNames(req inference.Request) []string {
	out := make([]string, 0, len(req.Tools))
	for _, t := range req.Tools {
		out = append(out, t.Name)
	}
	return out
}

func hasName(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// toolResults flattens every ToolResultMessage's text out of a request's
// conversation. This is what the model was actually told a tool returned.
func toolResults(req inference.Request) []string {
	var out []string
	for _, msg := range req.Messages {
		res, ok := msg.(*content.ToolResultMessage)
		if !ok {
			continue
		}
		var b strings.Builder
		for _, block := range res.Blocks {
			if text, ok := block.(*content.TextBlock); ok {
				b.WriteString(text.Text)
			}
		}
		out = append(out, b.String())
	}
	return out
}

// toolUse is one streamed tool call.
func toolUse(id, name, argsJSON string) content.Chunk {
	return &content.ToolUseChunk{Index: 0, ID: id, Name: name, InputJSON: argsJSON}
}

func say(text string) []content.Chunk { return []content.Chunk{&content.TextChunk{Text: text}} }

func call(id, name, argsJSON string) []content.Chunk {
	return []content.Chunk{toolUse(id, name, argsJSON)}
}

// --- the access gate --------------------------------------------------------
//
// The loop's permission model is now a loop.AccessGate (satisfied by
// *gate.Evaluator) installed with loop.WithAccessGate. A prepared tool call is
// evaluated as a typed tool.Request whose requirements each name a capability
// kind and scope; the gate resolves each kind Deny/Gated/Allow through its
// bound gate.AccessSource. These fixtures reproduce the old auto-approve and
// ask-for-MCP gates in that model.

// allowAllAccessSource reports Allow for every capability kind and scope. A
// headless evaluator built over it never gates, so nothing prompts and every
// prepared call is authorized — the new-model equivalent of the old
// auto-approve permission gate. Empty prepared requests (e.g. delegation)
// approve trivially, with or without a binding.
type allowAllAccessSource struct{}

func (allowAllAccessSource) AccessVersion() uint16                   { return gate.CurrentAccessVersion }
func (allowAllAccessSource) AccessFor(string, string) (uint8, error) { return gate.AccessAllow, nil }

// gatedAccessSource reports Gated for every kind it is bound to. Wired to
// tool.invoke it makes each MCP tool call an unmet gated requirement, which an
// interactive evaluator resolves by opening one real Harness permission gate.
type gatedAccessSource struct{}

func (gatedAccessSource) AccessVersion() uint16                   { return gate.CurrentAccessVersion }
func (gatedAccessSource) AccessFor(string, string) (uint8, error) { return gate.AccessGated, nil }

// noRules matches nothing (no stored deny or allow) and persists nothing.
// Interactive evaluator construction requires a rule writer even though MCP
// tool.invoke requirements carry no reusable candidates, so WriteRules is never
// actually reached; a nil matcher would also leave gated requirements unmet, but
// spelling it out keeps the fixture's intent explicit.
type noRules struct{}

func (noRules) MatchesDeny(context.Context, tool.Requirement) (bool, error)  { return false, nil }
func (noRules) MatchesAllow(context.Context, tool.Requirement) (bool, error) { return false, nil }
func (noRules) WriteRules(context.Context, []tool.RuleCandidate) error       { return nil }

// accessKinds is every capability kind these fixtures can route. tool.invoke is
// the only kind MCP tools raise; the sandbox and context kinds are bound too so
// an added tool can never fall through to a fail-closed missing-source denial.
var accessKinds = []string{
	mcpharness.CapabilityToolInvoke,
	tool.CapabilityCommandExecute,
	"filesystem.read",
	"filesystem.write",
	"network",
	"context.load",
}

// allowAllBindings routes every kind to the allow-all source.
func allowAllBindings() []gate.AccessBinding {
	bindings := make([]gate.AccessBinding, 0, len(accessKinds))
	for _, kind := range accessKinds {
		bindings = append(bindings, gate.AccessBinding{Kind: kind, Source: allowAllAccessSource{}})
	}
	return bindings
}

// approveAll is the access gate every test that is not ABOUT permissions uses: a
// headless allow-all evaluator. Without an access gate wired, the loop runner
// denies every call fail-secure and nothing downstream would ever run.
func approveAll(t *testing.T) loop.AccessGate {
	t.Helper()
	evaluator, err := gate.NewHeadlessEvaluator(allowAllBindings(), noRules{}, nil)
	if err != nil {
		t.Fatalf("gate.NewHeadlessEvaluator: %v", err)
	}
	return evaluator
}

// --- the host doubles -------------------------------------------------------

// recordingPublisher is the host's event sink. The adapter's Deps.Events is an
// EventPublisher seam (see deps.go); a real *hub.Hub satisfies it structurally,
// but a Session does not hand its Hub out, so a host wires one. Recording it is
// what makes an IntegrationStatus assertable.
type recordingPublisher struct {
	mu     sync.Mutex
	events []event.IntegrationStatus
}

func (p *recordingPublisher) PublishEvent(_ context.Context, ev event.Event) error {
	status, ok := ev.(event.IntegrationStatus)
	if !ok {
		return fmt.Errorf("recordingPublisher: unexpected event %T", ev)
	}
	p.mu.Lock()
	p.events = append(p.events, status)
	p.mu.Unlock()
	return nil
}

func (p *recordingPublisher) statuses(binding string) []event.IntegrationStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]event.IntegrationStatus, 0, len(p.events))
	for _, e := range p.events {
		if e.Name == binding {
			out = append(out, e)
		}
	}
	return out
}

// waitStatus blocks until binding has published a status in state.
func (p *recordingPublisher) waitStatus(t *testing.T, binding string, state event.IntegrationState) {
	t.Helper()
	deadline := time.Now().Add(settle)
	for {
		for _, e := range p.statuses(binding) {
			if e.State == state {
				return
			}
		}
		if time.Now().After(deadline) {
			var got []event.IntegrationState
			for _, e := range p.statuses(binding) {
				got = append(got, e.State)
			}
			t.Fatalf("binding %q never reached state %v; saw %v", binding, state, got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitStatusAfterLoss blocks until the binding has published a ready status
// LATER than its first degraded/failed one — i.e. until it genuinely came back.
func (p *recordingPublisher) waitStatusAfterLoss(t *testing.T, binding string) {
	t.Helper()
	deadline := time.Now().Add(settle)
	for {
		sts := p.statuses(binding)
		lost := -1
		for i, st := range sts {
			if st.State == event.IntegrationDegraded || st.State == event.IntegrationFailed {
				lost = i
				break
			}
		}
		if lost >= 0 {
			for _, st := range sts[lost+1:] {
				if st.State == event.IntegrationReady {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			var got []event.IntegrationState
			for _, st := range sts {
				got = append(got, st.State)
			}
			t.Fatalf("binding %q never became ready again after its connection was lost; saw %v", binding, got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitNotice blocks until the reporter (tools_test.go's recordingReporter) has
// seen a notice of this kind.
func waitNotice(t *testing.T, r *recordingReporter, kind mcpharness.NoticeKind) mcpharness.Notice {
	t.Helper()
	deadline := time.Now().Add(settle)
	for {
		for _, n := range r.snapshot() {
			if n.Kind == kind {
				return n
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %v notice was ever reported; saw %+v", kind, r.snapshot())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// --- the observer -----------------------------------------------------------

// observer drains a real Session's enduring event stream. Every wait below is
// on a fact the Session published, never on a sleep.
type observer struct {
	mu     sync.Mutex
	events []event.Event
	sub    event.Subscription
}

func observe(t *testing.T, src mcpharness.EventSource) *observer {
	t.Helper()
	sub, err := src.SubscribeEvents(event.EventFilter{Enduring: event.LoopScope{All: true}})
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}
	o := &observer{sub: sub}
	go func() {
		for d := range sub.Events() {
			o.mu.Lock()
			o.events = append(o.events, d.Event)
			o.mu.Unlock()
		}
	}()
	t.Cleanup(func() { _ = sub.Close() })
	return o
}

func (o *observer) all() []event.Event {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]event.Event(nil), o.events...)
}

// find returns the first event matching pred.
func (o *observer) find(pred func(event.Event) bool) (event.Event, bool) {
	for _, ev := range o.all() {
		if pred(ev) {
			return ev, true
		}
	}
	return nil, false
}

// count returns how many events match pred.
func (o *observer) count(pred func(event.Event) bool) int {
	n := 0
	for _, ev := range o.all() {
		if pred(ev) {
			n++
		}
	}
	return n
}

// wait blocks until an event matching pred has been drained.
func (o *observer) wait(t *testing.T, what string, pred func(event.Event) bool) event.Event {
	t.Helper()
	deadline := time.Now().Add(settle)
	for {
		if ev, ok := o.find(pred); ok {
			return ev
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s was ever published", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitCount blocks until at least n events match pred.
func (o *observer) waitCount(t *testing.T, what string, n int, pred func(event.Event) bool) {
	t.Helper()
	deadline := time.Now().Add(settle)
	for {
		if got := o.count(pred); got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("saw %d %s, want at least %d", o.count(pred), what, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// loopIdle matches a Loop parking.
func loopIdle(loopID uuid.UUID) func(event.Event) bool {
	return func(ev event.Event) bool {
		idle, ok := ev.(event.LoopIdle)
		return ok && idle.EventHeader().LoopID == loopID
	}
}

// waitLoopID resolves an agent name to the Loop ID the Session minted for it,
// by watching the loop-scoped events that carry both.
func (o *observer) waitLoopID(t *testing.T, name string) uuid.UUID {
	t.Helper()
	deadline := time.Now().Add(settle)
	for {
		for _, ev := range o.all() {
			h := ev.EventHeader()
			if string(h.AgentName) == name && !h.LoopID.IsZero() {
				return h.LoopID
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no loop named %q was ever seen on the event stream", name)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// --- the fixture bindings ---------------------------------------------------

// fixtureBinding builds a Binding whose server is a real fixture subprocess.
func fixtureBinding(t *testing.T, name string, scope mcpharness.Scope, args []string, shape func(*mcpharness.Binding)) mcpharness.Binding {
	t.Helper()
	tr, err := stdio.New(stdio.Config{Command: buildMCPFixture(t), Args: args})
	if err != nil {
		t.Fatalf("stdio.New: %v", err)
	}
	// Required by default: Start only waits for required bindings, so an
	// optional one may still be dialing when a test's first Install runs, and
	// the test would then be asserting on a race rather than on the adapter. The
	// tests that are ABOUT optional startup say so explicitly.
	b := mcpharness.Binding{
		Name:     name,
		Server:   client.Definition{Name: client.Name(name), Transport: tr},
		Scope:    scope,
		Required: true,
	}
	if scope == mcpharness.ScopeSession {
		b.Visibility = mcpharness.AllLoops()
	}
	if shape != nil {
		shape(&b)
	}
	return b
}

// mcpName is the model-facing name the catalog assigns one binding's tool.
func mcpName(binding, raw string) string { return "mcp__" + binding + "__" + raw }

// --- the rig ----------------------------------------------------------------

// rigFixture is a real Session with a real Manager attached.
type rigFixture struct {
	sess    session.SessionController
	mgr     *mcpharness.Manager
	adopter *mcpharness.Adopter
	obs     *observer
	events  *recordingPublisher
	notices *recordingReporter
}

// newLoop defines one real Loop with a scripted model and an access gate.
// It is the MCP suite's counterpart to fixtures_test.go's `definition`, which
// takes neither an access gate nor delegates — both of which the tests below
// are largely about — and so cannot serve here. The model descriptor is the
// shared testModel: secret-free, never dialed (the client is always scripted).
func newLoop(t *testing.T, name string, llm inference.Client, access loop.AccessGate, delegates ...identity.AgentName) loop.Definition {
	t.Helper()
	d, err := loop.Define(
		loop.WithName(identity.AgentName(name)),
		loop.WithInference(llm, testModel(name)),
		loop.WithAccessGate(access),
		loop.WithPolicyRevision("test-v1"),
		loop.WithDelegates(delegates...),
		loop.WithDrainTimeout(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("loop.Define(%q): %v", name, err)
	}
	return d
}

// newSession stands up a real rig Session over the given loops, with the first
// as the active primer.
func newSession(t *testing.T, store *sessionstore.Store, primer string, loops ...loop.Definition) session.SessionController {
	t.Helper()
	names := make([]string, 0, len(loops))
	for _, l := range loops {
		names = append(names, string(l.Name()))
	}
	r, err := rig.Define(
		rig.WithLoops(loops...),
		rig.WithPrimers(names...),
		rig.WithActivePrimer(primer),
		rig.WithSessionStore(store),
	)
	if err != nil {
		t.Fatalf("rig.Define: %v", err)
	}
	sess, err := r.NewSession(itCtx(t))
	if err != nil {
		t.Fatalf("rig.NewSession: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
		defer cancel()
		_ = sess.Shutdown(ctx)
	})
	return sess
}

func newStore(t *testing.T) *sessionstore.Store {
	t.Helper()
	store, err := sessionstore.Open(memstore.New())
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}
	return store
}

// attach builds a Manager over a live Session, starts it, and wires adoption.
func attach(t *testing.T, sess session.SessionController, gates mcpharness.GateOpener, bindings ...mcpharness.Binding) *rigFixture {
	t.Helper()
	f := &rigFixture{
		sess:    sess,
		events:  &recordingPublisher{},
		notices: &recordingReporter{},
	}
	f.obs = observe(t, sess)
	if gates == nil {
		gates = refusingGates{}
	}
	mgr, err := mcpharness.NewManager(bindings, mcpharness.Deps{
		SessionID: sess.SessionID(),
		Gates:     gates,
		Events:    f.events,
		Reporter:  f.notices,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	f.mgr = mgr
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
		defer cancel()
		_ = mgr.Close(ctx)
	})
	return f
}

// start starts the Manager and begins servicing idle boundaries.
func (f *rigFixture) start(t *testing.T) error {
	t.Helper()
	if err := f.mgr.Start(itCtx(t)); err != nil {
		return err
	}
	a, err := f.mgr.StartAdoption(f.sess, f.sess)
	if err != nil {
		t.Fatalf("StartAdoption: %v", err)
	}
	f.adopter = a
	t.Cleanup(func() { _ = a.Close() })
	return nil
}

// refusingGates is the GateOpener for a test that must never elicit. A test that
// does elicit installs its own.
type refusingGates struct{}

func (refusingGates) OpenGate(context.Context, mcpharness.GateRequest) (mcpharness.GateResponse, error) {
	return mcpharness.GateResponse{}, fmt.Errorf("refusingGates: this test expects no elicitation")
}

// hostGates is a GateOpener standing in for the host's renderer. It records the
// envelope it was handed and answers from a script.
type hostGates struct {
	answer func(mcpharness.GateRequest) (mcpharness.GateResponse, error)

	mu   sync.Mutex
	seen []mcpharness.GateRequest
}

func (h *hostGates) OpenGate(_ context.Context, req mcpharness.GateRequest) (mcpharness.GateResponse, error) {
	h.mu.Lock()
	h.seen = append(h.seen, req)
	h.mu.Unlock()
	return h.answer(req)
}

func (h *hostGates) requests() []mcpharness.GateRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]mcpharness.GateRequest(nil), h.seen...)
}

// --- diagnostics ------------------------------------------------------------

// TestRealSessionServesMCPTools is the spine every other test here stands on: a
// real rig, a real Loop, a real fixture server over a real subprocess, and a
// real turn in which the model is handed the server's tool and its call reaches
// the server.
//
// It fails if the adapter installs nothing, if the name mapping is wrong, or if
// the call does not reach the fixture: the asserted result text is one the
// fixture's echo tool alone can produce.
func TestRealSessionServesMCPTools(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)

	llm := newScriptLLM(call("c1", mcpName("alpha", fixtureToolEcho), `{"text":"through-real-mcp"}`), say("ok"))
	planner := newLoop(t, "planner", llm, approveAll(t))
	sess := newSession(t, newStore(t), "planner", planner)

	f := attach(t, sess, nil, fixtureBinding(t, "alpha", mcpharness.ScopeSession, nil, nil))
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	loopID := sess.ActiveLoop().ID()
	if err := f.adopter.Install(ctx, loopID, "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}

	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	reqs := llm.waitRequests(t, 2)

	if names := toolNames(reqs[0]); !hasName(names, mcpName("alpha", fixtureToolEcho)) {
		t.Fatalf("the model was offered %v, want %q", names, mcpName("alpha", fixtureToolEcho))
	}
	results := toolResults(reqs[1])
	if len(results) != 1 || results[0] != "through-real-mcp" {
		t.Fatalf("tool results = %q, want the fixture's echo of the argument", results)
	}
}

// --- 1. multiple MCPs connected to one Session ------------------------------

// TestMultipleBindingsOnOneSession binds two independent fixture servers to one
// Session and proves the model sees both namespaces and that a call reaches the
// server the NAME says it does — not merely "a server".
//
// The routing claim is the point. Both fixtures serve "echo", so an echo result
// alone would prove nothing; alpha is given a filler tool beta does not have, so
// mcp__alpha__extra_0 exists and mcp__beta__extra_0 must not.
func TestMultipleBindingsOnOneSession(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)

	llm := newScriptLLM(
		call("c1", mcpName("alpha", fixtureExtraToolPrefix+"0"), `{"text":"from-alpha"}`),
		call("c2", mcpName("beta", fixtureToolEcho), `{"text":"from-beta"}`),
		say("ok"),
	)
	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", llm, approveAll(t)))

	f := attach(t, sess, nil,
		fixtureBinding(t, "alpha", mcpharness.ScopeSession, []string{"-extra-tools", "1"}, nil),
		fixtureBinding(t, "beta", mcpharness.ScopeSession, nil, nil),
	)
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, sess.ActiveLoop().ID(), "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	reqs := llm.waitRequests(t, 3)

	names := toolNames(reqs[0])
	for _, want := range []string{
		mcpName("alpha", fixtureToolEcho),
		mcpName("beta", fixtureToolEcho),
		mcpName("alpha", fixtureExtraToolPrefix+"0"),
	} {
		if !hasName(names, want) {
			t.Errorf("the model was not offered %q; got %v", want, names)
		}
	}
	// beta has no filler tools: a union that mixed the two catalogs up would put
	// this name in the namespace.
	if hasName(names, mcpName("beta", fixtureExtraToolPrefix+"0")) {
		t.Errorf("the model was offered %q, which only alpha's server serves", mcpName("beta", fixtureExtraToolPrefix+"0"))
	}

	if got := toolResults(reqs[1]); len(got) != 1 || got[0] != "from-alpha" {
		t.Errorf("alpha's tool result = %q, want [from-alpha]", got)
	}
	if got := toolResults(reqs[2]); len(got) != 2 || got[1] != "from-beta" {
		t.Errorf("results after beta's call = %q, want the second to be from-beta", got)
	}

	// Two bindings, two connections, both up: the Session owns both.
	status := f.mgr.Status()
	if len(status) != 2 {
		t.Fatalf("Status has %d bindings, want 2", len(status))
	}
	for _, st := range status {
		if st.Client.State != client.StateReady {
			t.Errorf("binding %q is %v, want ready", st.Name, st.Client.State)
		}
		if st.Scope != mcpharness.ScopeSession {
			t.Errorf("binding %q scope = %v, want session", st.Name, st.Scope)
		}
	}
}

// --- 2. mixed Session- and Loop-scoped bindings -----------------------------

// TestMixedSessionAndLoopScope gives one Loop both a shared Session-scoped
// server and a private Loop-scoped one, and proves the Loop's single namespace is
// the union of the two while the Manager still reports two different owners.
func TestMixedSessionAndLoopScope(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)

	llm := newScriptLLM(
		call("c1", mcpName("private", fixtureToolEcho), `{"text":"private-server"}`),
		say("ok"),
	)
	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", llm, approveAll(t)))
	loopID := sess.ActiveLoop().ID()

	f := attach(t, sess, nil,
		fixtureBinding(t, "shared", mcpharness.ScopeSession, nil, nil),
		fixtureBinding(t, "private", mcpharness.ScopeLoop, nil, func(b *mcpharness.Binding) { b.Loop = loopID }),
	)
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, loopID, "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	reqs := llm.waitRequests(t, 2)

	names := toolNames(reqs[0])
	if !hasName(names, mcpName("shared", fixtureToolEcho)) || !hasName(names, mcpName("private", fixtureToolEcho)) {
		t.Fatalf("the model was offered %v, want both the shared and the private binding's echo", names)
	}
	if got := toolResults(reqs[1]); len(got) != 1 || got[0] != "private-server" {
		t.Errorf("the loop-scoped call's result = %q, want [private-server]", got)
	}

	// Ownership is what Status reports, and the two bindings do not share it.
	byName := map[string]mcpharness.BindingStatus{}
	for _, st := range f.mgr.Status() {
		byName[st.Name] = st
	}
	if st := byName["shared"]; st.Scope != mcpharness.ScopeSession || !st.Loop.IsZero() {
		t.Errorf("shared = %v/%v, want session scope naming no Loop", st.Scope, st.Loop)
	}
	if st := byName["private"]; st.Scope != mcpharness.ScopeLoop || st.Loop != loopID {
		t.Errorf("private = %v/%v, want loop scope owned by %v", st.Scope, st.Loop, loopID)
	}
}

// --- 3 & 4. selectors, and a delegate that inherits nothing private ---------

// TestDelegateSeesSharedButNotPrivateBindings is the delegation rule end to end:
// a REAL delegate Loop, spawned by a real Subagent call from a real parent turn,
// against real servers.
//
// The parent has a private Loop-scoped binding and a Session-scoped binding
// whose selector permits the parent's ID alone; the Session also has a binding
// visible to every Loop. The delegate must see the last of the three and nothing
// else — not because anything filtered its parent's bindings out, but because it
// never owned them and no selector names it.
func TestDelegateSeesSharedButNotPrivateBindings(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)

	parentLLM := newScriptLLM(
		call("c1", "Subagent", `{"action":"start","agent":"builder","message":"go","wait":true}`),
		say("parent done"),
	)
	sess := newSession(t, newStore(t), "planner",
		newLoop(t, "planner", parentLLM, approveAll(t), "builder"),
		newLoop(t, "builder", newScriptLLM(say("child done")), approveAll(t)),
	)
	parentID := sess.ActiveLoop().ID()

	f := attach(t, sess, nil,
		fixtureBinding(t, "everyone", mcpharness.ScopeSession, nil, nil),
		fixtureBinding(t, "planner-only", mcpharness.ScopeSession, nil, func(b *mcpharness.Binding) {
			b.Visibility = mcpharness.Loops(parentID)
		}),
		fixtureBinding(t, "parent-private", mcpharness.ScopeLoop, nil, func(b *mcpharness.Binding) { b.Loop = parentID }),
	)
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, parentID, "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// The parent's own turn proves the positive half: it really is holding all
	// three bindings' tools.
	reqs := parentLLM.waitRequests(t, 1)
	names := toolNames(reqs[0])
	for _, want := range []string{"everyone", "planner-only", "parent-private"} {
		if !hasName(names, mcpName(want, fixtureToolEcho)) {
			t.Errorf("the parent was not offered %q; got %v", mcpName(want, fixtureToolEcho), names)
		}
	}

	// The delegate is a real Loop the Session minted; LoopStarted is where its
	// name and id are both on the record.
	started := f.obs.wait(t, "LoopStarted for the delegate", func(ev event.Event) bool {
		s, ok := ev.(event.LoopStarted)
		return ok && string(s.EventHeader().AgentName) == "builder"
	})
	childID := started.EventHeader().LoopID
	if childID == parentID || childID.IsZero() {
		t.Fatalf("delegate loop id = %v, want a fresh id distinct from the parent's %v", childID, parentID)
	}

	// Scope: the delegate owns no binding, so it inherits none of the parent's.
	if defs := f.mgr.LoopTools(childID); len(defs) != 0 {
		t.Errorf("the delegate owns %d loop-scoped tool bundles, want 0 (it never owned its parent's)", len(defs))
	}
	if defs := f.mgr.LoopTools(parentID); len(defs) != 1 {
		t.Errorf("the parent owns %d loop-scoped tool bundles, want 1", len(defs))
	}

	// Visibility: the delegate may use what the Session shares with everyone, and
	// nothing whose selector names only its parent.
	childShared := bundleNames(f.mgr.SessionTools(childID, "builder"))
	if !hasName(childShared, "mcp:everyone@1") {
		t.Errorf("the delegate's session tools = %v, want the everyone binding", childShared)
	}
	if hasName(childShared, "mcp:planner-only@1") {
		t.Errorf("the delegate's session tools = %v, want NOT the planner-only binding", childShared)
	}
	parentShared := bundleNames(f.mgr.SessionTools(parentID, "planner"))
	if !hasName(parentShared, "mcp:planner-only@1") {
		t.Errorf("the parent's session tools = %v, want the planner-only binding it is selected for", parentShared)
	}
}

// bundleNames names the tool bundles a Manager assembled. Each is
// "mcp:<binding>@<generation>" (see bundleName), so it identifies both the
// binding that contributed and the catalog it was cut from.
func bundleNames(defs []tool.Definition) []string {
	out := make([]string, 0, len(defs))
	for _, d := range defs {
		out = append(out, d.Name())
	}
	return out
}

// --- 5. required and optional startup ---------------------------------------

// deadBinding is a binding whose server exits immediately: a real transport, a
// real subprocess, and a real failure to speak MCP.
func deadBinding(t *testing.T, name string, required bool) mcpharness.Binding {
	t.Helper()
	tr, err := stdio.New(stdio.Config{Command: "/bin/sh", Args: []string{"-c", "exit 3"}})
	if err != nil {
		t.Fatalf("stdio.New: %v", err)
	}
	return mcpharness.Binding{
		Name: name,
		Server: client.Definition{
			Name:      client.Name(name),
			Transport: tr,
			// Bound the handshake: the default is 30s, and a test that waits it
			// out is a test that reports a timeout instead of a failure.
			Timeouts: client.Timeouts{Startup: 10 * time.Second},
		},
		Scope:      mcpharness.ScopeSession,
		Visibility: mcpharness.AllLoops(),
		Required:   required,
	}
}

// TestRequiredBindingFailureFailsStart proves a required binding is required: a
// server that will not come up stops the owner coming up, and the error names it.
func TestRequiredBindingFailureFailsStart(t *testing.T) {
	t.Parallel()

	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", newScriptLLM(say("x")), approveAll(t)))
	f := attach(t, sess, nil,
		fixtureBinding(t, "good", mcpharness.ScopeSession, nil, nil),
		deadBinding(t, "must-work", true),
	)

	err := f.mgr.Start(itCtx(t))
	var startup *mcpharness.StartupError
	if !errors.As(err, &startup) {
		t.Fatalf("Start() error = %T %v, want a *mcpharness.StartupError", err, err)
	}
	if len(startup.Failures) != 1 || startup.Failures[0].Binding != "must-work" {
		t.Fatalf("Start() failures = %+v, want exactly the must-work binding", startup.Failures)
	}
	if startup.Failures[0].Class == 0 {
		t.Error("the failure carries no class; an operator has nothing to act on")
	}
}

// TestOptionalBindingFailureDegradesOnlyItself proves the other half: the same
// dead server, marked optional, leaves the Session usable and the healthy
// binding's tools serving. Only the optional binding is marked failed.
func TestOptionalBindingFailureDegradesOnlyItself(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)

	llm := newScriptLLM(call("c1", mcpName("good", fixtureToolEcho), `{"text":"still-working"}`), say("ok"))
	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", llm, approveAll(t)))
	f := attach(t, sess, nil,
		fixtureBinding(t, "good", mcpharness.ScopeSession, nil, nil),
		deadBinding(t, "nice-to-have", false),
	)
	if err := f.start(t); err != nil {
		t.Fatalf("Start() error = %v, want nil: an optional binding's failure must not fail Start", err)
	}

	// The optional binding settles in the background; wait for its failure rather
	// than for a clock.
	f.events.waitStatus(t, "nice-to-have", event.IntegrationFailed)

	if err := f.adopter.Install(ctx, sess.ActiveLoop().ID(), "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	reqs := llm.waitRequests(t, 2)

	names := toolNames(reqs[0])
	if !hasName(names, mcpName("good", fixtureToolEcho)) {
		t.Errorf("the healthy binding's tools are missing: %v", names)
	}
	if hasName(names, mcpName("nice-to-have", fixtureToolEcho)) {
		t.Errorf("a failed binding contributed tools: %v", names)
	}
	if got := toolResults(reqs[1]); len(got) != 1 || got[0] != "still-working" {
		t.Errorf("the healthy binding's call returned %q, want [still-working]", got)
	}

	byName := map[string]mcpharness.BindingStatus{}
	for _, st := range f.mgr.Status() {
		byName[st.Name] = st
	}
	if got := byName["nice-to-have"].Client.State; got != client.StateFailed {
		t.Errorf("the optional binding is %v, want failed", got)
	}
	if got := byName["good"].Client.State; got != client.StateReady {
		t.Errorf("the healthy binding is %v, want ready", got)
	}
}

// --- 8, 9, 10. snapshots, adoption at idle, and structured unavailability ----

// waitCandidate blocks until the named binding's client holds a validated
// catalog generation past `after`.
//
// A list-changed notification and the refresh it triggers are asynchronous by
// design, so a test that went straight on to the next boundary would be racing
// the refresh rather than testing adoption.
// It reads the binding's generations through the public Manager.Status() rather
// than the manager's internal bindingState (which is unexported and out of reach
// from this module). client.Status reports CandidateGeneration and
// CatalogGeneration directly, and both are 0 until there is something to report —
// so "candidate past `after`" and "adopted catalog past `after`" are the same two
// conditions the internal Candidate()/Catalog() accessors answer.
func (f *rigFixture) waitCandidate(t *testing.T, binding string, after uint64) {
	t.Helper()
	deadline := time.Now().Add(settle)
	for {
		found := false
		for _, st := range f.mgr.Status() {
			if st.Name != binding {
				continue
			}
			found = true
			if st.Client.CandidateGeneration > after || st.Client.CatalogGeneration > after {
				return
			}
		}
		if !found {
			t.Fatalf("no binding named %q", binding)
		}
		if time.Now().After(deadline) {
			t.Fatalf("binding %q never produced a catalog generation past %d", binding, after)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// catalogGeneration is the ordinal of the binding's currently ADOPTED catalog (0
// before one is adopted), read through the public Manager.Status().
func (f *rigFixture) catalogGeneration(t *testing.T, binding string) uint64 {
	t.Helper()
	for _, st := range f.mgr.Status() {
		if st.Name == binding {
			return st.Client.CatalogGeneration
		}
	}
	t.Fatalf("no binding named %q", binding)
	return 0
}

// adoptions counts the toolsets the adapter has installed so far.
func (f *rigFixture) adoptions() int {
	n := 0
	for _, notice := range f.notices.snapshot() {
		if notice.Kind == mcpharness.NoticeAdopted {
			n++
		}
	}
	return n
}

// waitAdoptions blocks until n toolsets have been installed. The first is always
// the composition root's own Install, so a test waiting for an idle boundary's
// adoption waits for the second.
func (f *rigFixture) waitAdoptions(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(settle)
	for {
		if f.adoptions() >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d toolsets were installed, want %d; notices: %+v", f.adoptions(), n, f.notices.snapshot())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// carryToNextGeneration drives one Loop from its current toolset to the server's
// refreshed catalog: it waits for the refresh to land, then submits a turn whose
// idle is the safe boundary the adapter adopts at.
//
// The nudge turn is not ceremony. Adoption happens at a Loop's idle, and the idle
// that ended the mutating turn may well have arrived BEFORE the server's
// list-changed notification was even read — in which case there was nothing to
// adopt, and the next boundary is the one that carries it. That is the design
// (design §Catalog model), so a test drives another boundary rather than
// pretending the first one could have known.
func (f *rigFixture) carryToNextGeneration(t *testing.T, binding string, loopID uuid.UUID) {
	t.Helper()
	f.waitCandidate(t, binding, 1)
	before := f.adoptions()
	if _, err := f.sess.Submit(itCtx(t), []content.Block{&content.TextBlock{Text: "nudge"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	f.obs.wait(t, "an idle boundary for the nudge turn", loopIdle(loopID))
	f.waitAdoptions(t, before+1)
}

// TestActiveTurnKeepsItsToolSnapshotAndAdoptsAtIdle is the catalog model end to
// end, and the two halves are one test because they are one mechanism observed
// at two moments.
//
// The fixture's mutate tool really does add a tool and really does make a real
// SDK server emit notifications/tools/list_changed. The model is then asked again
// WITHIN THE SAME TURN: it must be offered exactly the tools it started with,
// because a turn runs under the snapshot it began with. Only at a later idle does
// the adapter carry the Loop to the new generation — so a later turn's model IS
// offered the tool that appeared, and the mutating turn's never was.
func TestActiveTurnKeepsItsToolSnapshotAndAdoptsAtIdle(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)

	llm := newScriptLLM(
		call("c1", mcpName("srv", fixtureToolMutate), `{"add":true}`),
		// Same turn, after the server has changed its list.
		call("c2", mcpName("srv", fixtureToolEcho), `{"text":"same-turn"}`),
		say("turn one done"),
	)
	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", llm, approveAll(t)))
	loopID := sess.ActiveLoop().ID()

	f := attach(t, sess, nil, fixtureBinding(t, "srv", mcpharness.ScopeSession, []string{"-mutate"}, nil))
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, loopID, "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	reqs := llm.waitRequests(t, 3)

	// 8. The snapshot held: the mutation landed between request 1 and request 3,
	// and the tool list the model saw never moved.
	added := mcpName("srv", fixtureToolMutated)
	for i, req := range reqs[:3] {
		if hasName(toolNames(req), added) {
			t.Fatalf("request %d of the mutating turn was offered %q; a turn must keep the snapshot it started with", i, added)
		}
	}
	if got := toolResults(reqs[2]); len(got) != 2 || got[1] != "same-turn" {
		t.Errorf("the same-turn echo after the mutation returned %q, want it still working", got)
	}

	// 9. The Loop parks, the refreshed catalog is adopted, and the next turn is
	// built from it.
	f.carryToNextGeneration(t, "srv", loopID)
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "again"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	all := llm.waitRequests(t, 5)
	last := all[len(all)-1]
	if !hasName(toolNames(last), added) {
		t.Fatalf("after the idle boundary the model was offered %v, want the adopted %q", toolNames(last), added)
	}
}

// TestRemovedToolReturnsStructuredResult proves design §Calling a tool step 4: a
// tool the model still holds from its snapshot, which the server has since
// removed, fails as ITSELF — a structured, attributable result the model can read
// — and never ends the turn or gets quietly re-pointed at something else.
func TestRemovedToolReturnsStructuredResult(t *testing.T) {
	t.Parallel()
	assertToolDrift(t, `{"add":false}`, "ToolUnavailable")
}

// TestSchemaChangedToolReturnsStructuredResult is the other half of step 4: the
// name survived and the contract did not. The fixture re-registers echo2 under a
// different input schema, so the arguments Harness validated describe a tool that
// no longer exists under that name — and the call must say so rather than post
// them to a server that would interpret them differently.
func TestSchemaChangedToolReturnsStructuredResult(t *testing.T) {
	t.Parallel()
	assertToolDrift(t, `{"add":true,"schema":true}`, "ToolSchemaChanged")
}

// assertToolDrift drives the shared shape of the two drift scenarios.
//
// The Loop is carried to a generation carrying echo2 and the Adopter is then shut
// down, so the Loop goes on holding that generation's tools while the binding's
// own client keeps refreshing. The server then drifts echo2 underneath it and the
// model calls it from the stale snapshot — which is precisely the state design
// §Calling a tool step 4 is about: the tool a model is holding was cut from a
// generation that is no longer the current view of the server.
//
// Shutting the Adopter down is what makes that state REACHABLE deterministically.
// The alternative — calling the tool in the same turn as the mutation — races the
// server's list-changed notification against the call, and a test that wins the
// race by luck proves nothing on the run where it loses.
func assertToolDrift(t *testing.T, driftArgs, wantMarker string) {
	t.Helper()
	ctx := itCtx(t)

	llm := newScriptLLM(
		// Turn 1: make echo2 exist, so a later generation can change it.
		call("c1", mcpName("srv", fixtureToolMutate), `{"add":true}`),
		say("added"),
		// Turn 2 is the nudge carryToNextGeneration submits; its idle adopts.
		say("nudge done"),
		// Turn 3: drift the server underneath the toolset the Loop is holding.
		call("c2", mcpName("srv", fixtureToolMutate), driftArgs),
		say("drifted"),
		// Turn 4: call the tool the snapshot still has and the server no longer
		// honors.
		call("c3", mcpName("srv", fixtureToolMutated), `{"text":"stale-args"}`),
		say("turn four done"),
	)
	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", llm, approveAll(t)))
	loopID := sess.ActiveLoop().ID()

	f := attach(t, sess, nil, fixtureBinding(t, "srv", mcpharness.ScopeSession, []string{"-mutate"}, nil))
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, loopID, "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "one"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	llm.waitRequests(t, 2)
	f.carryToNextGeneration(t, "srv", loopID)

	// From here the Loop's toolset is frozen at the generation carrying echo2.
	if err := f.adopter.Close(); err != nil {
		t.Fatalf("Adopter.Close: %v", err)
	}

	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "drift"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	llm.waitRequests(t, 5)
	// The server has announced its change and the client has fetched it: the
	// evidence that the tool drifted is in hand BEFORE the call is made.
	f.waitCandidate(t, "srv", 2)

	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "call it"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	reqs := llm.waitRequests(t, 7)

	// The premise: the calling turn really was holding the tool. Without this the
	// assertion below would pass against a Loop that never had echo2 at all.
	if !hasName(toolNames(reqs[5]), mcpName("srv", fixtureToolMutated)) {
		t.Fatalf("the calling turn was offered %v, want the retained %q", toolNames(reqs[5]), mcpName("srv", fixtureToolMutated))
	}
	// The claim: the call fails as a structured result, not as a Go error and not
	// as a dead turn — request 7 exists at all only because the turn survived.
	results := toolResults(reqs[6])
	if len(results) == 0 {
		t.Fatal("the drifted tool produced no result at all")
	}
	got := results[len(results)-1]
	if !strings.HasPrefix(got, "error: ") || !strings.Contains(got, wantMarker) {
		t.Fatalf("the drifted tool returned %q, want an \"error: ...%s...\" result", got, wantMarker)
	}
	if !strings.Contains(got, fixtureToolMutated) || !strings.Contains(got, "srv") {
		t.Errorf("the result %q names neither the tool nor the binding; it is unattributable", got)
	}
}

// --- 6. permission gates for MCP tools --------------------------------------

// askForMCP is an interactive access gate that auto-approves the Loop's own
// capabilities and GATES every MCP tool.invoke, which is the posture the
// design's §Permissions describes for a third-party server. Because tool.invoke
// resolves Gated with no saved rule (noRules matches nothing), the interactive
// evaluator opens one real combined permission gate through the loop's approval
// capability (loop.GateApprover) — which the real Session publishes and
// RespondGate answers. It is the ask-for-MCP counterpart to approveAll.
func askForMCP(t *testing.T) loop.AccessGate {
	t.Helper()
	bindings := make([]gate.AccessBinding, 0, len(accessKinds))
	for _, kind := range accessKinds {
		source := gate.AccessSource(allowAllAccessSource{})
		if kind == mcpharness.CapabilityToolInvoke {
			source = gatedAccessSource{}
		}
		bindings = append(bindings, gate.AccessBinding{Kind: kind, Source: source})
	}
	// tool.invoke carries no grant class, so no grant is minted and the issuer is
	// never consulted (nil). Interactive construction requires an approver and a
	// rule writer; noRules is a writer that persists nothing.
	evaluator, err := gate.NewInteractiveEvaluator(bindings, noRules{}, loop.GateApprover(), noRules{}, nil)
	if err != nil {
		t.Fatalf("gate.NewInteractiveEvaluator: %v", err)
	}
	return evaluator
}

// waitPermissionGate blocks until the loop runner has opened a real permission
// gate and returns both halves of it: the durable envelope (whose ID a response
// is routed by) and the typed prepared request the adapter built.
func waitPermissionGate(t *testing.T, obs *observer) (gate.Gate, tool.Request) {
	t.Helper()
	opened := obs.wait(t, "a permission GateOpened", func(ev event.Event) bool {
		g, ok := ev.(event.GateOpened)
		return ok && g.Gate.Kind == gate.KindPermission
	}).(event.GateOpened)
	requested := obs.wait(t, "a PermissionRequested", func(ev event.Event) bool {
		_, ok := ev.(event.PermissionRequested)
		return ok
	}).(event.PermissionRequested)
	return opened.Gate, requested.Request
}

// TestPermissionGateApprovesAnMCPCall drives a REAL Harness permission gate for
// an MCP tool: the real loop runner opens it, the real Session publishes it, and
// the real SessionController.RespondGate answers it.
//
// The load-bearing assertion is the identity. The gated tool.invoke requirement
// the gate carries is keyed on its Scope, and design §Permissions requires that
// to be the binding-qualified "mcp:<binding>:<raw-tool>" — an approval for the
// srv binding's echo must never satisfy some other server's tool of the same
// name.
func TestPermissionGateApprovesAnMCPCall(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)

	llm := newScriptLLM(call("c1", mcpName("srv", fixtureToolEcho), `{"text":"approved"}`), say("ok"))
	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", llm, askForMCP(t)))

	f := attach(t, sess, nil, fixtureBinding(t, "srv", mcpharness.ScopeSession, nil, nil))
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, sess.ActiveLoop().ID(), "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	g, req := waitPermissionGate(t, f.obs)
	// The gate carries exactly the one unmet gated requirement — the MCP call's
	// tool.invoke — and its Scope is the identity an approval is keyed on.
	if len(req.Requirements) != 1 {
		t.Fatalf("the gate carries %d requirements, want exactly the one gated tool.invoke", len(req.Requirements))
	}
	gated := req.Requirements[0]
	if got := gated.Scope; got != permissionIdentity("srv", fixtureToolEcho) {
		t.Errorf("the gate names the capability %q, want %q: an approval must be qualified by its binding",
			got, permissionIdentity("srv", fixtureToolEcho))
	}
	// The requirement description is what a human reads; it must name the server
	// and the tool, and must not be the raw arguments (design §Permissions).
	if !strings.Contains(gated.Description, fixtureToolEcho) || !strings.Contains(gated.Description, "srv") {
		t.Errorf("the prompt %q names neither the tool nor the server", gated.Description)
	}
	if strings.Contains(gated.Description, "approved") {
		t.Errorf("the prompt %q leaked the call's arguments", gated.Description)
	}
	if g.Blocks != gate.BlocksToolCall {
		t.Errorf("the gate blocks %q, want a tool call", g.Blocks)
	}

	if err := sess.RespondGate(ctx, gate.GateResponse{
		GateID: g.ID,
		Action: string(gate.ApprovalApprove),
		Source: gate.ResponseSource{Kind: gate.ResponseFromUser},
	}); err != nil {
		t.Fatalf("RespondGate: %v", err)
	}

	reqs := llm.waitRequests(t, 2)
	if got := toolResults(reqs[1]); len(got) != 1 || got[0] != "approved" {
		t.Fatalf("after approval the call returned %q, want the fixture's echo", got)
	}
	// NOTE: the old fixture also asserted a session-scoped approval PERSISTED a
	// grant through the gate. That assertion tested a removed concept — the old
	// tool.ApprovalScope (once/session/workspace) and the gate's Grant hook are
	// gone. The new model persists only on "Approve always for this workspace"
	// via a RuleWriter, and an MCP tool.invoke requirement deliberately carries no
	// reusable rule candidate to persist, so a plain Approve persists nothing by
	// design. The persistence sub-assertion is therefore dropped; the identity
	// and the approve-then-call-succeeds assertions above carry this test's intent.
}

// TestPermissionGateDeniesAnMCPCall proves the gate is a gate: a denial stops the
// call reaching the server at all, and the model is told so without the turn
// dying.
func TestPermissionGateDeniesAnMCPCall(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)

	llm := newScriptLLM(call("c1", mcpName("srv", fixtureToolEcho), `{"text":"never-sent"}`), say("ok"))
	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", llm, askForMCP(t)))

	f := attach(t, sess, nil, fixtureBinding(t, "srv", mcpharness.ScopeSession, nil, nil))
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, sess.ActiveLoop().ID(), "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	g, _ := waitPermissionGate(t, f.obs)
	if err := sess.RespondGate(ctx, gate.GateResponse{
		GateID: g.ID,
		Action: string(gate.ApprovalDeny),
		Source: gate.ResponseSource{Kind: gate.ResponseFromUser},
	}); err != nil {
		t.Fatalf("RespondGate: %v", err)
	}

	reqs := llm.waitRequests(t, 2)
	got := toolResults(reqs[1])
	if len(got) != 1 || !strings.Contains(got[0], "permission denied") {
		t.Fatalf("after a denial the model was told %q, want a permission-denied result", got)
	}
	// The echo never ran: the fixture returns its argument verbatim, so the
	// argument appearing in a result would mean the call reached the server.
	if strings.Contains(got[0], "never-sent") {
		t.Fatalf("a denied call reached the server: %q", got[0])
	}
}

// --- 7. form and URL elicitation through the generic gates -------------------

// TestFormElicitationReachesTheHostAndAnswersTheServer drives a real server's
// mid-call request for human input all the way through: the fixture's elicit tool
// asks, the adapter translates it into a Harness gate envelope, the HOST answers
// it, and the answer reaches the server, whose reply lands in the model's turn.
//
// The host is a test double for the RENDERER only, and that is the design: see
// this file's header for why no public Harness route can open a form gate. What
// is asserted here is that what the host receives is a real, valid Harness gate —
// not an MCP shape wearing a Harness name.
func TestFormElicitationReachesTheHostAndAnswersTheServer(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)

	gates := &hostGates{answer: func(req mcpharness.GateRequest) (mcpharness.GateResponse, error) {
		return mcpharness.GateResponse{Action: gate.FormActionAccept, Values: map[string]string{"name": "ada"}}, nil
	}}
	llm := newScriptLLM(
		call("c1", mcpName("srv", fixtureToolElicit), `{"schema":true}`),
		say("ok"),
	)
	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", llm, approveAll(t)))

	f := attach(t, sess, gates, fixtureBinding(t, "srv", mcpharness.ScopeSession, []string{"-elicit"}, func(b *mcpharness.Binding) {
		b.Server.Capabilities = client.ClientCapabilities{Elicitation: true}
	}))
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, sess.ActiveLoop().ID(), "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	reqs := llm.waitRequests(t, 2)

	// The answer travelled: the server was told "accept" and said so in its
	// result, which the real turn carried back to the model.
	if got := toolResults(reqs[1]); len(got) != 1 || got[0] != fixtureElicitAnswerPrefix+"accept" {
		t.Fatalf("the server reported %q, want %q: the human's answer did not reach it",
			got, fixtureElicitAnswerPrefix+"accept")
	}

	asked := gates.requests()
	if len(asked) != 1 {
		t.Fatalf("the host was asked %d times, want 1", len(asked))
	}
	assertRealHarnessGate(t, asked[0], gate.KindForm)
	if asked[0].Binding != "srv" {
		t.Errorf("the gate is attributed to %q, want srv", asked[0].Binding)
	}
	// The schema is the server's, carried through to a form a human could fill
	// in: ValidateFormSchema is the contract Harness's own renderers rely on.
	if err := gate.ValidateFormSchema(asked[0].Prompt.Schema); err != nil {
		t.Fatalf("the form schema handed to the host is not a valid Harness form: %v", err)
	}
	if len(asked[0].Prompt.Schema.Fields) == 0 {
		t.Error("the form has no fields; the server's requested schema was dropped")
	}
	// And the answer the host gave really does parse back through the schema it
	// was given — the round trip a policy renderer depends on.
	values := map[string]json.RawMessage{"name": json.RawMessage(`"ada"`)}
	if _, err := gate.ParseFormAnswers(asked[0].Prompt.Schema, values); err != nil {
		t.Errorf("an answer to this form does not parse against its own schema: %v", err)
	}
}

// --- url elicitation --------------------------------------------------------
//
// This path WAS uncovered here, and the note that stood in its place said it was
// uncoverable: the module advertised elicitation as a bare
// &mcp.ElicitationCapabilities{}, the SDK infers Form for that but never URL, so
// a real server asking for mode "url" was answered "client does not support
// \"url\" elicitation" and translateURL was unreachable over a real connection.
//
// That is no longer true. internal/protocol/session.go now names both modes
// explicitly, so a real fixture CAN send a url-mode request and the two tests
// below drive one — through a real subprocess server, a real stdio transport, and
// a real turn.

// urlElicit* are the canaries. The action URL is a stand-in for the real thing an
// OAuth server sends: an origin a human can reason about, plus query parameters
// that are CREDENTIALS — `state` is a CSRF token and `code_challenge` is the PKCE
// challenge. Neither may ever reach a durable record, so they are spelled as
// values no other part of the system could produce by accident, and swept for.
const (
	urlElicitOrigin    = "https://auth.example.com"
	urlElicitState     = "state-canary-must-not-be-journaled"
	urlElicitChallenge = "pkce-canary-must-not-be-journaled"
	urlElicitPath      = "/authorize"
)

// urlElicitAction is the full action URL the fixture server sends.
func urlElicitAction() string {
	return urlElicitOrigin + urlElicitPath + "?state=" + urlElicitState + "&code_challenge=" + urlElicitChallenge
}

// driveURLElicitation runs one real url-mode elicitation end to end and returns
// the envelope the host was handed.
func driveURLElicitation(t *testing.T) mcpharness.GateRequest {
	t.Helper()
	ctx := itCtx(t)

	gates := &hostGates{answer: func(mcpharness.GateRequest) (mcpharness.GateResponse, error) {
		// A url gate's accept carries no values: the human went and did the thing.
		return mcpharness.GateResponse{Action: gate.FormActionAccept}, nil
	}}
	args, err := json.Marshal(map[string]string{"mode": "url", "url": urlElicitAction()})
	if err != nil {
		t.Fatalf("marshalling the elicit call's arguments: %v", err)
	}
	llm := newScriptLLM(call("c1", mcpName("srv", fixtureToolElicit), string(args)), say("ok"))
	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", llm, approveAll(t)))

	f := attach(t, sess, gates, fixtureBinding(t, "srv", mcpharness.ScopeSession, []string{"-elicit"}, func(b *mcpharness.Binding) {
		b.Server.Capabilities = client.ClientCapabilities{Elicitation: true}
	}))
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, sess.ActiveLoop().ID(), "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	reqs := llm.waitRequests(t, 2)

	// The answer travelled back to the server that asked. If the host had never
	// been reached — the old capability bug — the fixture would report the SDK's
	// "elicit failed: ... does not support" instead.
	if got := toolResults(reqs[1]); len(got) != 1 || got[0] != fixtureElicitAnswerPrefix+"accept" {
		t.Fatalf("the server reported %q, want %q: a real url-mode elicitation did not reach the host and come back",
			got, fixtureElicitAnswerPrefix+"accept")
	}

	asked := gates.requests()
	if len(asked) != 1 {
		t.Fatalf("the host was asked %d times, want 1", len(asked))
	}
	return asked[0]
}

// TestURLElicitationBuildsAGateRealHarnessAccepts is the contract half: what the
// adapter hands a host for a url-mode request must be an envelope real Harness
// would open.
//
// This is the assertion whose absence let a live defect ship. gate.ValidateGate
// REQUIRES a bare Prompt.Origin on a KindOpenURL gate — it is the only kind with
// a real rule — and the adapter built gates without one, putting the origin in
// Body prose instead. Nothing caught it: the module's own unit test validated a
// Gate with the Prompt dropped, and this suite's assertRealHarnessGate had only
// ever been pointed at KindForm, which ValidateGate no-ops on.
func TestURLElicitationBuildsAGateRealHarnessAccepts(t *testing.T) {
	t.Parallel()

	req := driveURLElicitation(t)

	// The envelope, Prompt included, against Harness's own validator.
	assertRealHarnessGate(t, req, gate.KindOpenURL)

	if req.Binding != "srv" {
		t.Errorf("the gate is attributed to %q, want srv", req.Binding)
	}
	// The origin arrives as a validated FIELD, not as prose a renderer would have
	// to parse back out of Body. This is the value a human's trust decision is
	// made on, and the field is the contract that carries it.
	if req.Prompt.Origin != urlElicitOrigin {
		t.Errorf("the prompt's origin is %q, want %q: a renderer has nothing trustworthy to label",
			req.Prompt.Origin, urlElicitOrigin)
	}
	// Never restorable: the action target is not journaled, so a restored open-url
	// gate could only ever present an origin with no URL behind it. ValidateGate
	// refuses one, and this pins the adapter's side of that.
	if req.Restorable {
		t.Error("the open-url gate is marked restorable; its action target is not journaled, so a restored one is a broken prompt")
	}
	// The payload really does still carry the full URL — the host has to have
	// something to open. This is the control for the sweep below: it proves the
	// URL was present to leak, so the absences asserted there are real.
	payload, ok := req.Payload.(gate.OpenURLPayload)
	if !ok {
		t.Fatalf("the payload is %T, want gate.OpenURLPayload", req.Payload)
	}
	if payload.URL != urlElicitAction() {
		t.Errorf("the private payload's URL is %q, want the server's action URL: the host has nothing to open", payload.URL)
	}
}

// TestURLElicitationKeepsTheURLOutOfDurableRecords is the security half: the
// action URL is a credential — it carries the `state` token and the PKCE
// challenge — and it must reach NO durable record, while the origin must reach
// every one of them.
//
// The sweep is over MARSHALLED bytes, not field by field, and that is the point:
// a field added to Prompt or to the durable payload tomorrow is covered by
// construction rather than by someone remembering to extend this test.
//
// # What is swept, and why these are the durable records
//
// There is no public Harness route to open a KindOpenURL gate (see this file's
// header), so this cannot read the URL's absence out of a live gate directory.
// It does the next thing, which is stronger than it sounds: it builds the durable
// records Harness ITSELF would write for this gate, from this gate, using
// Harness's own codecs — and those codecs are the boundary the property lives at.
//
//   - the private payload, through gate.MarshalPayload — the GatePreparedRecord.
//   - the public envelope, through event.MarshalEvent of a real event.GateOpened —
//     what fans out to SSE, history, and the journal.
//   - the resolution, through event.MarshalEvent of a real event.GateResolved.
func TestURLElicitationKeepsTheURLOutOfDurableRecords(t *testing.T) {
	t.Parallel()

	req := driveURLElicitation(t)
	g := assertRealHarnessGate(t, req, gate.KindOpenURL)

	// The durable payload record.
	payloadBytes, err := gate.MarshalPayload(req.Payload)
	if err != nil {
		t.Fatalf("gate.MarshalPayload: %v", err)
	}
	// The public envelope record: a real GateOpened carrying the real gate.
	openedBytes, err := event.MarshalEvent(event.GateOpened{
		Header: gateEventHeader(t),
		Gate:   g,
	})
	if err != nil {
		t.Fatalf("event.MarshalEvent(GateOpened): %v", err)
	}
	// The resolution record, as the host answered it.
	resolvedBytes, err := event.MarshalEvent(event.GateResolved{
		Header:   gateEventHeader(t),
		GateID:   g.ID,
		Resolver: gate.ResolverSession,
		Action:   gate.FormActionAccept,
		Source:   gate.ResponseSource{Kind: gate.ResponseFromUser},
	})
	if err != nil {
		t.Fatalf("event.MarshalEvent(GateResolved): %v", err)
	}

	records := map[string][]byte{
		"the private payload (GatePreparedRecord)": payloadBytes,
		"the public envelope (GateOpened)":         openedBytes,
		"the resolution (GateResolved)":            resolvedBytes,
	}
	// Every secret the action URL carries, plus the URL itself and the path that
	// only the action URL has.
	secrets := map[string]string{
		"the full action URL": urlElicitAction(),
		"the state token":     urlElicitState,
		"the PKCE challenge":  urlElicitChallenge,
		"the URL's path":      urlElicitPath,
	}
	for name, record := range records {
		for what, secret := range secrets {
			if strings.Contains(string(record), secret) {
				t.Errorf("%s leaks %s (%q) into a durable record:\n%s", name, what, secret, record)
			}
		}
	}
	// The other half of the property, and the reason the sweep is not satisfiable
	// by dropping everything: the ORIGIN must survive into the records a human's
	// prompt is rebuilt from. A gate that journaled nothing would pass the sweep
	// above and be useless.
	for _, name := range []string{"the private payload (GatePreparedRecord)", "the public envelope (GateOpened)"} {
		if !strings.Contains(string(records[name]), urlElicitOrigin) {
			t.Errorf("%s does not carry the origin %q; the human's trust decision is not durably recorded:\n%s",
				name, urlElicitOrigin, records[name])
		}
	}
}

// gateEventHeader is a well-formed identity for a gate event built by hand. The
// identifiers are real and fresh, so MarshalEvent is exercised on the path a hub
// would take; none of them is under test here — the gate body is.
func gateEventHeader(t *testing.T) event.Header {
	t.Helper()
	return event.Header{
		Coordinates: identity.Coordinates{SessionID: newUUID(t), LoopID: newUUID(t)},
		EventID:     newUUID(t),
		CreatedAt:   time.Now().UTC(),
	}
}

func newUUID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	return id
}

// assertRealHarnessGate checks that what the host was handed is a gate Harness
// itself would accept: the right kind, a payload the gate codec round-trips, and
// an envelope ValidateGate passes.
//
// It returns the envelope it validated. That is what lets a caller sweep the
// DURABLE projection of the very gate Harness accepted, rather than of a
// look-alike built beside it — see TestURLElicitationKeepsTheURLOutOfDurableRecords.
//
// The Prompt is carried onto the envelope, not dropped. That matters: ValidateGate's
// only real rule today (Prompt.Origin on KindOpenURL) lives in the Prompt, so a
// helper that validated a stripped copy would pass on a gate Harness refuses.
func assertRealHarnessGate(t *testing.T, req mcpharness.GateRequest, want gate.Kind) gate.Gate {
	t.Helper()
	if req.Kind != want {
		t.Fatalf("gate kind = %q, want %q", req.Kind, want)
	}
	if req.Payload == nil {
		t.Fatal("the gate carries no payload")
	}
	if _, err := gate.MarshalPayload(req.Payload); err != nil {
		t.Errorf("the payload is not one the gate codec can carry: %v", err)
	}
	// The envelope the adapter would hand a gate directory, assembled from the
	// same parts and checked by Harness's own validator.
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	g := gate.Gate{
		ID:          id,
		Kind:        req.Kind,
		Resolver:    gate.ResolverSession,
		Blocks:      gate.BlocksSession,
		Effect:      gate.EffectResume,
		Criticality: gate.GateNonCritical,
		Prompt:      req.Prompt,
		Restorable:  req.Restorable,
	}
	if err := gate.ValidateGate(g); err != nil {
		t.Errorf("the envelope built from this request is not a valid Harness gate: %v", err)
	}
	return g
}

// --- 11. a failed refresh preserves the last valid generation ----------------

// TestRejectedRefreshPreservesTheAdoptedGeneration proves the design's rule that
// a refresh this binding will not accept costs it its view of the CHANGE, never
// its working catalog.
//
// The rejection is real and is provoked by a real server: the binding's
// MaxCatalogItems is set to exactly the fixture's tool count, so the initial
// catalog is accepted and the tool the mutate call adds pushes the refreshed one
// one item over. Nothing simulates the failure.
func TestRejectedRefreshPreservesTheAdoptedGeneration(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)

	llm := newScriptLLM(
		call("c1", mcpName("srv", fixtureToolMutate), `{"add":true}`),
		say("mutated"),
		// After the rejected refresh: the binding's existing tools must still work.
		call("c2", mcpName("srv", fixtureToolEcho), `{"text":"still-serving"}`),
		say("ok"),
	)
	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", llm, approveAll(t)))
	loopID := sess.ActiveLoop().ID()

	// The fixture with -mutate serves exactly these seven: echo, slow, fail, big,
	// progress, log, mutate. Adding echo2 makes eight.
	const fixtureToolCount = 7
	f := attach(t, sess, nil, fixtureBinding(t, "srv", mcpharness.ScopeSession, []string{"-mutate"}, func(b *mcpharness.Binding) {
		b.Server.Limits = client.Limits{MaxCatalogItems: fixtureToolCount}
	}))
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, loopID, "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	llm.waitRequests(t, 2)

	// The rejection is observable to an operator, and it is DEGRADED, not failed:
	// the binding still works, its changes do not.
	f.events.waitStatus(t, "srv", event.IntegrationDegraded)
	var degraded event.IntegrationStatus
	for _, st := range f.events.statuses("srv") {
		if st.State == event.IntegrationDegraded {
			degraded = st
		}
	}
	if !strings.Contains(degraded.Detail, "refresh was rejected") {
		t.Errorf("the degraded status says %q, want it to name the rejected refresh", degraded.Detail)
	}
	if !strings.Contains(degraded.Detail, "generation 1 is still in force") {
		t.Errorf("the degraded status says %q, want it to name the generation still in force", degraded.Detail)
	}

	// The adopted generation did not move, so the tools built from it still work.
	if got := f.catalogGeneration(t, "srv"); got != 1 {
		t.Errorf("the adopted catalog is generation %d, want 1: a rejected refresh must not replace it", got)
	}

	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "again"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	reqs := llm.waitRequests(t, 4)
	last := reqs[len(reqs)-1]
	if got := toolResults(last); len(got) == 0 || got[len(got)-1] != "still-serving" {
		t.Fatalf("after the rejected refresh the binding returned %q, want it still serving", got)
	}
	// And the tool the rejected generation would have brought never appeared.
	if hasName(toolNames(reqs[2]), mcpName("srv", fixtureToolMutated)) {
		t.Error("a rejected refresh's tool reached the model")
	}
}

// --- 13. independent failure and reconnect of one binding -------------------

// TestOneBindingFailsAndReconnectsIndependently kills ONE server for real — the
// fixture's crash tool exits the process with a reply outstanding, which is what
// a real server crash looks like from this end — and proves two things the design
// requires: the binding comes back on its own, and the Session's OTHER binding is
// untouched throughout.
func TestOneBindingFailsAndReconnectsIndependently(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)

	llm := newScriptLLM(
		call("c1", mcpName("fragile", fixtureToolCrash), `{}`),
		// The healthy binding is called immediately after the crash: its
		// connection is its own.
		call("c2", mcpName("steady", fixtureToolEcho), `{"text":"unaffected"}`),
		say("ok"),
	)
	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", llm, approveAll(t)))

	f := attach(t, sess, nil,
		fixtureBinding(t, "fragile", mcpharness.ScopeSession, []string{"-crash"}, nil),
		fixtureBinding(t, "steady", mcpharness.ScopeSession, nil, nil),
	)
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, sess.ActiveLoop().ID(), "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	reqs := llm.waitRequests(t, 3)

	// The crash did not end the turn: the model was told about it and carried on.
	crashed := toolResults(reqs[1])
	if len(crashed) != 1 || !strings.HasPrefix(crashed[0], "error: ") {
		t.Fatalf("the crashed call returned %q, want a bounded error result", crashed)
	}
	// The other binding kept working across its neighbour's death.
	if got := toolResults(reqs[2]); len(got) != 2 || got[1] != "unaffected" {
		t.Fatalf("the healthy binding returned %q while its neighbour crashed, want [.. unaffected]", got)
	}

	// The dead binding is announced, and then it comes back: a new subprocess, a
	// new handshake, a re-established connection.
	f.events.waitStatus(t, "fragile", event.IntegrationDegraded)
	// A Ready AFTER the loss, not the one from the original handshake: matching
	// the first Ready would pass against a binding that never came back.
	f.events.waitStatusAfterLoss(t, "fragile")

	// The steady binding never had anything to say beyond coming up: an
	// independent connection is one a neighbour's crash never reaches.
	for _, st := range f.events.statuses("steady") {
		if st.State == event.IntegrationDegraded || st.State == event.IntegrationFailed {
			t.Errorf("the steady binding was reported %v (%q) because its neighbour crashed", st.State, st.Detail)
		}
	}
}

// --- 14. Session and Loop shutdown cleanup ----------------------------------

// TestLoopAndSessionShutdownClosesOwnedBindings proves that ownership is what
// shutdown acts on. Closing one Loop closes the connections that Loop owns and
// nothing else; closing the Manager closes them all.
func TestLoopAndSessionShutdownClosesOwnedBindings(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)

	llm := newScriptLLM(call("c1", mcpName("private", fixtureToolEcho), `{"text":"alive"}`), say("ok"))
	sess := newSession(t, newStore(t), "planner", newLoop(t, "planner", llm, approveAll(t)))
	loopID := sess.ActiveLoop().ID()

	f := attach(t, sess, nil,
		fixtureBinding(t, "shared", mcpharness.ScopeSession, nil, nil),
		fixtureBinding(t, "private", mcpharness.ScopeLoop, nil, func(b *mcpharness.Binding) { b.Loop = loopID }),
	)
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, loopID, "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sess.Submit(ctx, []content.Block{&content.TextBlock{Text: "go"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	// The premise: the private binding really was serving before it was closed.
	if got := toolResults(llm.waitRequests(t, 2)[1]); len(got) != 1 || got[0] != "alive" {
		t.Fatalf("the private binding returned %q before shutdown, want [alive]", got)
	}

	// The Loop goes away. Its own connection goes with it; the Session's does not.
	if err := f.mgr.CloseLoop(ctx, loopID); err != nil {
		t.Fatalf("CloseLoop: %v", err)
	}
	after := f.mgr.Status()
	if len(after) != 1 || after[0].Name != "shared" {
		t.Fatalf("after CloseLoop the mcpharness.Manager holds %+v, want only the session-scoped binding", bindingNames(after))
	}
	if after[0].Client.State != client.StateReady {
		t.Errorf("the session-scoped binding is %v after a Loop closed, want ready: the Session owns it", after[0].Client.State)
	}
	// The Loop's tools go with its connection.
	if defs := f.mgr.LoopTools(loopID); len(defs) != 0 {
		t.Errorf("a closed Loop still has %d owned tool bundles", len(defs))
	}
	f.events.waitStatus(t, "private", event.IntegrationClosed)

	// The Manager goes away, and takes the rest with it.
	if err := f.mgr.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	f.events.waitStatus(t, "shared", event.IntegrationClosed)
	for _, st := range f.mgr.Status() {
		if st.Client.State != client.StateClosed {
			t.Errorf("binding %q is %v after Close, want closed", st.Name, st.Client.State)
		}
	}
	// Idempotent: a second Close is a no-op, not a panic or a double-close.
	if err := f.mgr.Close(ctx); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func bindingNames(sts []mcpharness.BindingStatus) []string {
	out := make([]string, 0, len(sts))
	for _, st := range sts {
		out = append(out, st.Name)
	}
	return out
}

// --- 12. Session restore with catalog drift ---------------------------------
//
// NOT COVERED, and the reason is a BUG in harness rather than a limit of this
// module: no external toolset can be installed on a restored Session's root loop
// at all, so there is nothing to drift.
//
// internal/sessionruntime/restore_constructor.go:765 builds the restored root
// loop's loopHandle without its `bindings` field:
//
//	s.loops[rootLoopID] = &loopHandle{id: rootLoopID, owner: s, bound: cfg, backend: l, ...}
//
// while the sibling path for every other restored loop (attachRestoredLoop, line
// 80) and the fresh-session path (session.go:1332) both pass the tool.Bindings
// that planLoops built. loopHandle.buildExternalTools calls def.Build(ctx,
// h.bindings), so on a restored root that is the zero Bindings and every
// definition is refused by tool.validateBindings:
//
//	loop: change refused (external_build_failed): tool=mcp:srv@1:
//	tool: invalid bindings: session_id
//
// A test written against this today would be a test of the bug. The scenario is
// otherwise ready: rig.RestoreSession returns the Session, its conversation comes
// back, and the Manager re-discovers the drifted catalog — only the install is
// refused. See the report accompanying this stage; the fix belongs in harness.
