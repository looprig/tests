//go:build integration

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/host"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

const (
	// liveStepToolName is the Auditable tool the lane's model calls.
	liveStepToolName = "orchestrationtest_lookup"
	// liveStepMissingTool is a name no rig registers: harness fails the call
	// before it runs, which is the case elapsed_ms is omitted for.
	liveStepMissingTool = "orchestrationtest_missing"
	// liveStepSecret rides in the RAW tool arguments. The committed StepDone
	// carries the model's tool_use input verbatim (journal parity), but no
	// live frame may: the live summary is the tool's own AuditSummary.
	liveStepSecret = "sk-LIVE-TOOL-STEP-SECRET-7f3a9c"
	// liveStepAfterText is the model's reply once the tools have run.
	liveStepAfterText = "after tools"
)

// liveStepArgs are the tool's arguments. Token is the secret; the audit
// summary names only the target.
type liveStepArgs struct {
	Target string `json:"target"`
	Token  string `json:"token,omitempty"`
	Fail   bool   `json:"fail,omitempty"`
	Hold   bool   `json:"hold,omitempty"`
	// Result, when set, is what a succeeding call returns.
	Result string `json:"result,omitempty"`
}

func liveStepInput(t *testing.T, args liveStepArgs) string {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// liveStepTool is a scripted tool.Auditable. A call with Hold parks until the
// case releases it, so the case can watch its Started frame arrive while the
// call is still running.
type liveStepTool struct {
	release chan struct{}

	mu    sync.Mutex
	calls int
}

func newLiveStepTool() *liveStepTool { return &liveStepTool{release: make(chan struct{})} }

func (l *liveStepTool) definition() tool.Definition {
	return tool.NewDefinition(liveStepToolName, 0, func(context.Context, tool.Bindings) ([]tool.InvokableTool, error) {
		return []tool.InvokableTool{l}, nil
	})
}

func (l *liveStepTool) Calls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func (*liveStepTool) Info(context.Context) (*tool.ToolInfo, error) {
	return &tool.ToolInfo{
		Name: liveStepToolName,
		Desc: "Looks up one target.",
		Schema: []byte(`{"type":"object","properties":{"target":{"type":"string"},"token":{"type":"string"},` +
			`"fail":{"type":"boolean"},"hold":{"type":"boolean"},"result":{"type":"string"}},"required":["target"],"additionalProperties":false}`),
	}, nil
}

// PrepareCall satisfies tool.CallPreparer: the pooled rig runs every tool
// under an access gate, which refuses a tool that cannot prepare a call.
func (*liveStepTool) PrepareCall(_ context.Context, executionID uuid.UUID, _ string) (tool.Request, tool.PreparedArtifact, error) {
	return tool.Request{
		ToolName:           liveStepToolName,
		Summary:            "look up one target",
		ExecutionID:        executionID.String(),
		ExpiresAtUnixMilli: time.Now().Add(time.Hour).UnixMilli(),
	}, nil, nil
}

// AuditSummary is the ONLY text of the arguments a live frame may carry. It
// names the target and never the token, and tolerates invalid JSON.
func (*liveStepTool) AuditSummary(argsJSON string) string {
	var args liveStepArgs
	if json.Unmarshal([]byte(argsJSON), &args) != nil || args.Target == "" {
		return liveStepToolName
	}
	return "lookup " + args.Target
}

func (l *liveStepTool) InvokableRun(ctx context.Context, input string) (*tool.ToolResult, error) {
	l.mu.Lock()
	l.calls++
	l.mu.Unlock()
	var args liveStepArgs
	if err := json.Unmarshal([]byte(input), &args); err != nil {
		return nil, err
	}
	if args.Hold {
		select {
		case <-l.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	// Every call that RUNS takes measurable wall time, so elapsed_ms is
	// present for it; a call that fails before running has none.
	time.Sleep(15 * time.Millisecond)
	if args.Fail {
		return nil, errors.New("lookup failed for " + args.Target)
	}
	if args.Result != "" {
		return tool.TextResult(args.Result), nil
	}
	return tool.TextResult("found " + args.Target), nil
}

// liveToolFrame is one tool-step ephemeral as a viewer received it.
type liveToolFrame struct {
	Index int
	Raw   []byte
	Body  json.RawMessage
	Step  liveToolBody
}

// liveToolBody is harness v0.42.0's public projection of ToolCallStarted and
// ToolCallCompleted (design §2.2). ElapsedMillis is a pointer so absence is
// distinguishable from zero.
type liveToolBody struct {
	V               int     `json:"v"`
	Type            string  `json:"type"`
	SessionID       string  `json:"session_id"`
	LoopID          string  `json:"loop_id"`
	TurnID          string  `json:"turn_id"`
	StepID          string  `json:"step_id"`
	ToolExecutionID string  `json:"tool_execution_id"`
	ToolUseID       string  `json:"tool_use_id"`
	ToolName        string  `json:"tool_name"`
	Summary         string  `json:"summary"`
	IsError         bool    `json:"is_error"`
	ElapsedMillis   *uint64 `json:"elapsed_ms"`
	ResultPreview   string  `json:"result_preview"`
}

// liveToolFrames returns every tool-step ephemeral in frames, in order.
func liveToolFrames(t *testing.T, frames [][]byte) []liveToolFrame {
	t.Helper()
	var out []liveToolFrame
	for i, frame := range frames {
		kind, err := sessionwire.SessionRecordTypeOf(frame)
		if err != nil || kind != sessionwire.SessionRecordTypeEphemeralPublication {
			continue
		}
		var p sessionwire.EphemeralPublication
		if err := p.UnmarshalJSON(frame); err != nil {
			t.Fatalf("frame %d: %v: %s", i, err, frame)
		}
		var body liveToolBody
		if err := json.Unmarshal(p.Body, &body); err != nil {
			t.Fatalf("frame %d body: %v: %s", i, err, p.Body)
		}
		if body.Type != "ToolCallStarted" && body.Type != "ToolCallCompleted" {
			continue
		}
		out = append(out, liveToolFrame{Index: i, Raw: frame, Body: p.Body, Step: body})
	}
	return out
}

func liveTextFrames(t *testing.T, frames [][]byte) (count int, text string) {
	t.Helper()
	var b strings.Builder
	for _, frame := range frames {
		kind, _ := sessionwire.SessionRecordTypeOf(frame)
		if kind != sessionwire.SessionRecordTypeEphemeralPublication {
			continue
		}
		var p sessionwire.EphemeralPublication
		if err := p.UnmarshalJSON(frame); err != nil {
			t.Fatal(err)
		}
		if delta, rejected, ok := oldDecodeFactoryLiveDelta(p.Body, string(p.SessionID)); ok && !rejected && delta.kind == "text" {
			count++
			b.WriteString(delta.text)
		}
	}
	return count, b.String()
}

// oldFactoryLiveDelta and oldDecodeFactoryLiveDelta are a Go port of
// decodeFactoryLiveDelta from @looprig/client 0.2.0 and 0.3.0
// (packages/client/src/factory-live-text.ts, byte-identical in both): the
// decoder every released browser runs over each ephemeral body. It returns
// ok=false for "unrelated publication" (the caller skips it) and
// rejected=true for "a correlated delta of bad shape" (the caller STOPS that
// key's preview). A tool frame must be the former, never the latter.
type oldFactoryLiveDelta struct {
	kind, loopID, turnID, text string
}

var oldClientUUID = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func oldDecodeFactoryLiveDelta(body json.RawMessage, publicSessionID string) (delta oldFactoryLiveDelta, rejected, ok bool) {
	const maxChunk = 16_384
	const maxBody = 6*maxChunk + 4_096
	var value map[string]any
	if json.Unmarshal(body, &value) != nil || value == nil {
		return delta, false, false
	}
	v, _ := value["v"].(float64)
	typ, _ := value["type"].(string)
	sid, sidOK := value["session_id"].(string)
	loop, loopOK := value["loop_id"].(string)
	turn, turnOK := value["turn_id"].(string)
	if v != 1 || typ != "TokenDelta" || !sidOK || sid != publicSessionID ||
		!loopOK || !oldClientUUID.MatchString(loop) || !turnOK || !oldClientUUID.MatchString(turn) {
		return delta, false, false
	}
	chunk, chunkOK := value["chunk"].(map[string]any)
	if !chunkOK {
		return oldFactoryLiveDelta{kind: "text", loopID: loop, turnID: turn}, true, true
	}
	var kind, field string
	switch chunk["chunk_type"] {
	case "text":
		kind, field = "text", "text"
	case "thinking":
		kind, field = "reasoning", "thinking"
	default:
		return delta, false, false
	}
	delta = oldFactoryLiveDelta{kind: kind, loopID: loop, turnID: turn}
	if len(body) > maxBody {
		return delta, true, true
	}
	text, textOK := chunk[field].(string)
	if !textOK || text == "" || len(text) > maxChunk {
		return delta, true, true
	}
	delta.text = text
	return delta, false, true
}

// committedToolUseIDs returns the tool_use block ids of every StepDone the
// viewer received, keyed by the StepDone's frame index. It reads the PUBLIC
// body, which is what a client folds its committed tool row from.
func committedToolUseIDs(t *testing.T, frames [][]byte) (steps []int, ids map[int][]string) {
	t.Helper()
	ids = map[int][]string{}
	for i, frame := range frames {
		var p sessionwire.EnduringPublication
		if p.UnmarshalJSON(frame) != nil {
			continue
		}
		var step struct {
			Type     string `json:"type"`
			Messages []struct {
				Role   string `json:"role"`
				Blocks []struct {
					Type string `json:"type"`
					ID   string `json:"ID"`
				} `json:"blocks"`
			} `json:"messages"`
		}
		if json.Unmarshal(p.Body, &step) != nil || step.Type != "StepDone" {
			continue
		}
		steps = append(steps, i)
		for _, message := range step.Messages {
			if message.Role != "assistant" {
				continue
			}
			for _, block := range message.Blocks {
				if block.Type == "tool_use" {
					ids[i] = append(ids[i], block.ID)
				}
			}
		}
	}
	return steps, ids
}

// liveStepReply is the model's text reply once the tools have run. Its end
// is held until the case has seen the text frame: Host flushes pending text
// on a timer or before the next enduring frame, and a StepDone that overtakes
// the text's projection gaps it, so an unheld reply races its own StepDone.
func liveStepReply(t *testing.T) (orchestrationtest.PooledTurn, chan struct{}) {
	t.Helper()
	finish := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-finish:
		default:
			close(finish)
		}
	})
	return orchestrationtest.PooledTurn{
		Chunks:       []content.Chunk{&content.TextChunk{Text: liveStepAfterText}},
		BeforeFinish: finish,
	}, finish
}

// awaitLiveReply waits for the reply's text frame, then lets the reply end.
func awaitLiveReply(t *testing.T, v *orchestrationtest.PooledViewer, finish chan struct{}) {
	t.Helper()
	orchestrationtest.PooledWait(t, "the reply's live text at the ClientLink", 60*time.Second, func() bool {
		count, _ := liveTextFrames(t, v.Frames())
		return count >= 1
	})
	close(finish)
}

// liveStepJournalDelay slows every harness journal append.
//
// A tool frame is best-effort by design: Host projects each ephemeral off its
// relay loop, and an enduring frame that arrives while one is still being
// projected supersedes it -- the frame is dropped and the committed StepDone
// is the truth. A call that fails before running emits its Started and
// Completed microseconds before harness commits the StepDone, so on a busy
// machine that drop is a coin toss. Delaying the commit (never the
// ephemeral) gives every tool frame a clear lead over the enduring frame
// that follows it, which is what lets this lane assert delivery exactly.
const liveStepJournalDelay = 25 * time.Millisecond

type slowAppendLedger struct {
	storage.Ledger
	delay time.Duration
}

func (l slowAppendLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	select {
	case <-time.After(l.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return l.Ledger.Append(ctx, name, expected, payload)
}

func slowJournalBackend() *storage.Composite {
	backend := memstore.New()
	backend.Ledger = slowAppendLedger{Ledger: backend.Ledger, delay: liveStepJournalDelay}
	return backend
}

type liveStepWorld struct {
	world   *orchestrationtest.PooledWorld
	tool    *liveStepTool
	viewer  *orchestrationtest.PooledViewer
	hostTap *orchestrationtest.HostLinkTap
	session sessionwire.SessionID
}

// startLiveStepWorld composes one Host with options over the released
// Factory, a watching ClientLink viewer, and creates the session; the first
// model turn is held until the Factory has bound the HostLink tail, so no
// live frame precedes the viewer's subscription.
func startLiveStepWorld(t *testing.T, ctx context.Context, name string, options *host.LiveTextOptions, turns ...orchestrationtest.PooledTurn) (*liveStepWorld, chan struct{}) {
	t.Helper()
	stepTool := newLiveStepTool()
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{orchestrationtest.PooledTenantA}, LiveText: options,
		Tools:           []tool.Definition{stepTool.definition()},
		JournalBackends: map[sessionwire.TenantID]*storage.Composite{orchestrationtest.PooledTenantA: slowJournalBackend()},
	})
	hold := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-hold:
		default:
			close(hold)
		}
	})
	turns[0].Hold = hold
	world.LLM.Script(turns...)
	session := sessionwire.SessionID("live-tool-steps-" + name)
	hostTap := orchestrationtest.NewHostLinkTap()
	h := orchestrationtest.StartTappedPooledHost(t, ctx, world, sessionwire.HostID("live-tool-steps-host-"+name), 4, hostTap)
	orchestrationtest.AwaitAdvertised(t, world, h.ID)
	factory := orchestrationtest.StartPooledFactory(t, ctx, world, "live-tool-steps-factory-"+name, nil)
	viewer := orchestrationtest.ConnectPooledViewer(t, ctx, factory, orchestrationtest.PooledTenantA)
	if err := viewer.Watch(t, ctx, orchestrationtest.PooledTenantA, session); err != nil {
		t.Fatal(err)
	}
	status, body := factory.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(session) + "-create"), SessionID: session,
		AgentID: orchestrationtest.PooledAgent, Blocks: json.RawMessage(`[{"type":"text","text":"look it up"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "Factory bound the HostLink before model output", 30*time.Second, func() bool {
		for _, link := range orchestrationtest.TappedLinks(t, hostTap) {
			if len(link.Subscribes) != 0 {
				return true
			}
		}
		return false
	})
	return &liveStepWorld{world: world, tool: stepTool, viewer: viewer, hostTap: hostTap, session: session}, hold
}

// assertNoSecretOnLiveFrames fails if any ephemeral frame, at the viewer or on
// the HostLink, carries the raw-argument secret.
func (w *liveStepWorld) assertNoSecretOnLiveFrames(t *testing.T) {
	t.Helper()
	for where, frames := range map[string][][]byte{
		"ClientLink": w.viewer.Frames(),
		"HostLink":   hostLinkSessionFrames(t, w.hostTap),
	} {
		for i, frame := range frames {
			kind, _ := sessionwire.SessionRecordTypeOf(frame)
			if kind == sessionwire.SessionRecordTypeEphemeralPublication && bytes.Contains(frame, []byte(liveStepSecret)) {
				t.Fatalf("%s ephemeral %d carries the raw tool argument secret: %s", where, i, frame)
			}
		}
	}
}

// TestFactoryHostLiveToolSteps follows a tool call's live Started and
// Completed frames from a real harness rig through host v0.15.0's opt-in
// IncludeToolSteps relay and the released Factory to a real ClientLink viewer
// (design DESIGN_LIVE_TOOL_STEPS §6.5).
func TestFactoryHostLiveToolSteps(t *testing.T) {
	t.Run("on", func(t *testing.T) {
		ctx := placementContext(t)
		first := liveStepArgs{Target: "alpha", Token: liveStepSecret, Hold: true}
		failing := liveStepArgs{Target: "beta", Token: liveStepSecret, Fail: true}
		reply, finish := liveStepReply(t)
		w, hold := startLiveStepWorld(t, ctx, "on", &host.LiveTextOptions{IncludeToolSteps: true},
			orchestrationtest.PooledTurn{ToolName: liveStepToolName, ToolInput: liveStepInput(t, first)},
			orchestrationtest.PooledTurn{ToolName: liveStepToolName, ToolInput: liveStepInput(t, failing)},
			orchestrationtest.PooledTurn{ToolName: liveStepMissingTool, ToolInput: `{"token":"` + liveStepSecret + `"}`},
			reply,
		)
		close(hold)

		// (1) Started reaches the viewer while the call is still running:
		// before its Completed and before any StepDone.
		orchestrationtest.PooledWait(t, "live ToolCallStarted at the ClientLink", 60*time.Second, func() bool {
			return len(liveToolFrames(t, w.viewer.Frames())) >= 1
		})
		frames := w.viewer.Frames()
		early := liveToolFrames(t, frames)
		if len(early) != 1 || early[0].Step.Type != "ToolCallStarted" {
			for _, f := range early {
				t.Logf("tool frame %d: %s", f.Index, f.Body)
			}
			t.Fatalf("while the tool is held the viewer has %d tool frames, want exactly one ToolCallStarted", len(early))
		}
		if steps, _ := committedToolUseIDs(t, frames); len(steps) != 0 {
			t.Fatalf("a StepDone reached the viewer before the held tool finished")
		}
		started := early[0].Step
		if started.ToolName != liveStepToolName || started.Summary != "lookup alpha" || started.ToolUseID == "" {
			t.Fatalf("Started = %+v, want tool_name %q, summary %q and a tool_use_id", started, liveStepToolName, "lookup alpha")
		}
		if w.tool.Calls() != 1 {
			t.Fatalf("tool ran %d times while held, want 1", w.tool.Calls())
		}
		close(w.tool.release)
		awaitLiveReply(t, w.viewer, finish)

		orchestrationtest.PooledWait(t, "the tool turn's terminal at the ClientLink", 60*time.Second, func() bool {
			return liveTerminalCount(w.viewer.Frames()) >= 1
		})
		frames = w.viewer.Frames()
		tools := liveToolFrames(t, frames)
		steps, stepIDs := committedToolUseIDs(t, frames)
		if len(steps) != 4 {
			t.Fatalf("viewer has %d StepDone frames, want 4 (three tool steps and the reply)", len(steps))
		}
		if len(tools) != 6 {
			for _, f := range tools {
				t.Logf("tool frame %d: %s", f.Index, f.Body)
			}
			t.Fatalf("viewer has %d tool frames, want 6 (Started and Completed for three calls)", len(tools))
		}

		// Each call: Started then Completed, one execution id, before the
		// StepDone whose committed tool_use block id equals the live one.
		type want struct {
			name, summary string
			isError, ran  bool
		}
		wants := []want{
			{name: liveStepToolName, summary: "lookup alpha", ran: true},
			{name: liveStepToolName, summary: "lookup beta", isError: true, ran: true},
			{name: liveStepMissingTool, summary: liveStepMissingTool, isError: true},
		}
		for call, expect := range wants {
			s, c := tools[2*call], tools[2*call+1]
			if s.Step.Type != "ToolCallStarted" || c.Step.Type != "ToolCallCompleted" {
				t.Fatalf("call %d frames are %s then %s, want Started then Completed", call, s.Step.Type, c.Step.Type)
			}
			for _, f := range []liveToolFrame{s, c} {
				b := f.Step
				if b.V != 1 || b.SessionID != string(w.session) || !oldClientUUID.MatchString(b.LoopID) ||
					!oldClientUUID.MatchString(b.TurnID) || !oldClientUUID.MatchString(b.ToolExecutionID) || b.StepID == "" {
					t.Fatalf("call %d %s has a bad public shape: %s", call, b.Type, f.Body)
				}
				for _, field := range []string{"journal_seq", "covered_through", "journal_tip"} {
					if bytes.Contains(f.Raw, []byte(`"`+field+`"`)) {
						t.Fatalf("call %d %s carries journal coordinate %s: %s", call, b.Type, field, f.Raw)
					}
				}
				if b.ToolName != expect.name {
					t.Fatalf("call %d %s tool_name = %q, want %q", call, b.Type, b.ToolName, expect.name)
				}
			}
			if s.Step.ToolExecutionID != c.Step.ToolExecutionID || s.Step.ToolUseID != c.Step.ToolUseID || s.Step.ToolUseID == "" {
				t.Fatalf("call %d Started/Completed disagree: %s vs %s", call, s.Body, c.Body)
			}
			if s.Step.Summary != expect.summary {
				t.Fatalf("call %d Started summary = %q, want %q", call, s.Step.Summary, expect.summary)
			}
			if c.Step.IsError != expect.isError {
				t.Fatalf("call %d Completed is_error = %v, want %v: %s", call, c.Step.IsError, expect.isError, c.Body)
			}
			if expect.ran && (c.Step.ElapsedMillis == nil || *c.Step.ElapsedMillis == 0) {
				t.Fatalf("call %d ran but Completed has no elapsed_ms: %s", call, c.Body)
			}
			if !expect.ran && c.Step.ElapsedMillis != nil {
				t.Fatalf("call %d failed before running but Completed has elapsed_ms: %s", call, c.Body)
			}
			if c.Step.ResultPreview == "" {
				t.Fatalf("call %d Completed has no result_preview: %s", call, c.Body)
			}
			// (2) The join key: the committed StepDone that follows carries
			// the tool_use block whose id is the live tool_use_id.
			step := steps[call]
			if !(s.Index < c.Index && c.Index < step) {
				t.Fatalf("call %d: Started %d, Completed %d, StepDone %d are out of order", call, s.Index, c.Index, step)
			}
			if got := stepIDs[step]; len(got) != 1 || got[0] != s.Step.ToolUseID {
				t.Fatalf("call %d: StepDone tool_use ids %v, live tool_use_id %q", call, got, s.Step.ToolUseID)
			}
		}
		if got := tools[1].Step.ResultPreview; !strings.Contains(got, "found alpha") {
			t.Fatalf("succeeding call's result_preview = %q", got)
		}

		// The same join, checked against the typed journal the StepDone was
		// committed to rather than the public body alone.
		runtimeID := w.world.RuntimeSessionID(t, ctx, orchestrationtest.PooledTenantA, w.session)
		journal := orchestrationtest.JournalEvents[event.StepDone](t, w.world, orchestrationtest.PooledTenantA, runtimeID)
		var journalIDs []string
		for _, done := range journal {
			for _, message := range done.Messages {
				if ai, ok := message.(*content.AIMessage); ok {
					for _, block := range ai.Blocks {
						if use, ok := block.(*content.ToolUseBlock); ok {
							journalIDs = append(journalIDs, use.ID)
						}
					}
				}
			}
		}
		if len(journalIDs) != 3 || journalIDs[0] != tools[0].Step.ToolUseID || journalIDs[1] != tools[2].Step.ToolUseID || journalIDs[2] != tools[4].Step.ToolUseID {
			t.Fatalf("journal tool_use ids %v do not match the live ones", journalIDs)
		}

		// Text still streams beside tool steps.
		if count, text := liveTextFrames(t, frames); count == 0 || text != liveStepAfterText {
			t.Fatalf("live text = %d frames %q, want %q", count, text, liveStepAfterText)
		}

		// (6) The raw arguments' secret never rides a live frame, though the
		// committed StepDone (journal parity) does carry the model's input.
		w.assertNoSecretOnLiveFrames(t)
		if steps, _ := committedToolUseIDs(t, frames); !bytes.Contains(frames[steps[0]], []byte(liveStepSecret)) {
			t.Fatal("the committed StepDone does not carry the tool input; the secret probe would be vacuous")
		}

		// (7) An older decoder ignores the frame: @looprig/client 0.2.0/0.3.0's
		// decodeFactoryLiveDelta treats it as unrelated (skip), never as a
		// rejected text delta (which would stop that turn's text preview).
		for _, f := range tools {
			if _, rejected, ok := oldDecodeFactoryLiveDelta(f.Body, string(w.session)); ok || rejected {
				t.Fatalf("the 0.2.0/0.3.0 decoder did not ignore a tool frame (ok=%v rejected=%v): %s", ok, rejected, f.Body)
			}
		}

		// The HostLink carried exactly the frames the viewer got.
		if link := liveToolFrames(t, hostLinkSessionFrames(t, w.hostTap)); len(link) != len(tools) {
			t.Fatalf("HostLink carried %d tool frames, viewer got %d", len(link), len(tools))
		}
	})

	t.Run("off", func(t *testing.T) {
		// (4) LiveText on, IncludeToolSteps off: text streams, the runtime
		// emits tool events, and no tool frame leaves the Host.
		ctx := placementContext(t)
		reply, finish := liveStepReply(t)
		w, hold := startLiveStepWorld(t, ctx, "off", &host.LiveTextOptions{},
			orchestrationtest.PooledTurn{ToolName: liveStepToolName, ToolInput: liveStepInput(t, liveStepArgs{Target: "alpha", Token: liveStepSecret})},
			reply,
		)
		close(hold)
		awaitLiveReply(t, w.viewer, finish)
		orchestrationtest.PooledWait(t, "the tool turn's terminal at the ClientLink", 60*time.Second, func() bool {
			return liveTerminalCount(w.viewer.Frames()) >= 1
		})
		if w.tool.Calls() != 1 {
			t.Fatalf("tool ran %d times, want 1", w.tool.Calls())
		}
		frames := w.viewer.Frames()
		if tools := liveToolFrames(t, frames); len(tools) != 0 {
			t.Fatalf("opt-out delivered %d tool frames: %s", len(tools), tools[0].Body)
		}
		if tools := liveToolFrames(t, hostLinkSessionFrames(t, w.hostTap)); len(tools) != 0 {
			t.Fatalf("opt-out put %d tool frames on the HostLink", len(tools))
		}
		if count, text := liveTextFrames(t, frames); count == 0 || text != liveStepAfterText {
			t.Fatalf("live text = %d frames %q, want %q", count, text, liveStepAfterText)
		}
		w.assertNoSecretOnLiveFrames(t)
	})

	t.Run("rate-limited", func(t *testing.T) {
		// (5) One byte per second over the minimum 4 KiB burst. The Started
		// frame spends part of the burst. The Completed frame's preview is
		// 2 KiB of backslashes, which JSON-escape to twice that, so Host fits
		// it back to just under the 4 KiB frame cap -- and that frame, with
		// its channel framing, is more than the budget holds: the transport
		// refuses it and Host DROPS it, with no retry. StepDone still
		// arrives, and the reply's text, published afterwards from the same
		// per-client budget, still streams: the drop neither wedged the relay
		// nor gapped the turn's text.
		ctx := placementContext(t)
		big := strings.Repeat(`\`, 3000)
		reply, finish := liveStepReply(t)
		w, hold := startLiveStepWorld(t, ctx, "rate", &host.LiveTextOptions{IncludeToolSteps: true, RateBytesPerSecond: 1, BurstBytes: 4096},
			orchestrationtest.PooledTurn{ToolName: liveStepToolName, ToolInput: liveStepInput(t, liveStepArgs{Target: "alpha", Result: big})},
			reply,
		)
		close(hold)
		awaitLiveReply(t, w.viewer, finish)
		orchestrationtest.PooledWait(t, "the tool turn's terminal at the ClientLink", 60*time.Second, func() bool {
			return liveTerminalCount(w.viewer.Frames()) >= 1
		})
		frames := w.viewer.Frames()
		steps, _ := committedToolUseIDs(t, frames)
		if len(steps) != 2 {
			t.Fatalf("viewer has %d StepDone frames, want 2", len(steps))
		}
		tools := liveToolFrames(t, frames)
		for _, f := range tools {
			t.Logf("delivered tool frame (%d bytes): %s", len(f.Raw), f.Step.Type)
			if len(f.Raw) > 4096 {
				t.Fatalf("tool frame is %d bytes, over the 4 KiB frame cap", len(f.Raw))
			}
		}
		if len(tools) != 1 || tools[0].Step.Type != "ToolCallStarted" {
			t.Fatalf("rate-limited viewer got %d tool frames, want only the Started (the Completed dropped)", len(tools))
		}
		// Dropped at the Host's transport budget, not lost downstream.
		if link := liveToolFrames(t, hostLinkSessionFrames(t, w.hostTap)); len(link) != 1 {
			t.Fatalf("rate-limited HostLink carried %d tool frames, want only the Started", len(link))
		}
		if count, text := liveTextFrames(t, frames); count == 0 || text != liveStepAfterText {
			t.Fatalf("after a dropped tool frame live text = %d frames %q, want %q", count, text, liveStepAfterText)
		}
	})
}
