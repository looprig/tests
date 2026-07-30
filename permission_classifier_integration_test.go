//go:build integration && (darwin || (linux && !android))

// permission_classifier_integration_test.go is Task 25's cross-module proof
// that the permission auto-review classifier feature (Harness pkg/gate +
// pkg/hustle, rig.WithPermissionReviewPolicy/Evidence/SecurityCeiling, and the
// classifiers module's real command-safety classifier) works end to end when
// composed the way a real consumer composes it: a real rig.Session, a real
// gated tool call, and a real *commandsafety.Classifier — assembled directly
// against Harness's public pkg/rig/pkg/gate/pkg/loop/pkg/tool surface, using
// the same composition seams CodeRig's internal/app/permission_review.go
// uses (classifiers/pkg/commandsafety + harness/pkg/gate + harness/pkg/rig),
// but never importing CodeRig itself (CodeRig's RuntimeAgent/sessionadapter
// wrapper is a private, product-specific convenience, not a required seam).
//
// No production policy lives here: every fixture below is either a scripted
// wire-faithful test double (the classifier and operator inference clients)
// or a minimal test tool, never a shortcut into the reviewed feature's own
// decision logic.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/classifiers/pkg/commandsafety"
	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/inference"
	"github.com/looprig/inference/model"
	"github.com/looprig/inference/stream"
)

// ---- fixtures: the gated "run_command" tool --------------------------------

// runCommandTool is a minimal, real tool.InvokableTool + tool.CallPreparer: it
// prepares a genuine tool.CapabilityCommandExecute requirement (the same
// capability kind commandsafety.Classifier.Applies matches on) for whatever
// command the model requests, and records how many times it actually ran so
// scenarios can prove exactly-once execution. It never touches a real shell —
// sandbox execution correctness is already covered by
// sandbox_integration_test.go elsewhere in this module; this fixture exists
// only to prove the permission-review gate/classifier mechanics.
type runCommandTool struct {
	mu    sync.Mutex
	calls int
	last  string
}

func (rt *runCommandTool) Info(context.Context) (*tool.ToolInfo, error) {
	return &tool.ToolInfo{
		Name:   "run_command",
		Desc:   "Runs a shell command.",
		Schema: []byte(`{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`),
	}, nil
}

type runCommandArgs struct {
	Command string `json:"command"`
}

func (rt *runCommandTool) PrepareCall(_ context.Context, executionID uuid.UUID, argsJSON string) (tool.Request, tool.PreparedArtifact, error) {
	var args runCommandArgs
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return tool.Request{}, nil, err
	}
	command := strings.TrimSpace(args.Command)
	return tool.Request{
		ToolName:           "run_command",
		Summary:            "run " + command,
		ExecutionID:        executionID.String(),
		Command:            command,
		WorkingDirectory:   "/workspace",
		ExpiresAtUnixMilli: time.Now().Add(time.Hour).UnixMilli(),
		Requirements: []tool.Requirement{{
			Kind:        tool.CapabilityCommandExecute,
			Match:       command,
			Description: "run " + command,
			GrantClass:  tool.GrantClassCommandStart,
			GrantTarget: command,
		}},
	}, tool.TokenArtifact{Token: command}, nil
}

func (rt *runCommandTool) InvokableRun(_ context.Context, argsJSON string) (*tool.ToolResult, error) {
	var args runCommandArgs
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return tool.TextResult("error: " + err.Error()), nil
	}
	rt.mu.Lock()
	rt.calls++
	rt.last = args.Command
	rt.mu.Unlock()
	return tool.TextResult("ran: " + args.Command), nil
}

func (rt *runCommandTool) callCount() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.calls
}

var (
	_ tool.InvokableTool = (*runCommandTool)(nil)
	_ tool.CallPreparer  = (*runCommandTool)(nil)
)

func runCommandDefinition(shared *runCommandTool) tool.Definition {
	return tool.NewDefinition("run_command", 0, func(context.Context, tool.Bindings) ([]tool.InvokableTool, error) {
		return []tool.InvokableTool{shared}, nil
	})
}

func commandArgsJSON(t *testing.T, command string) string {
	t.Helper()
	body, err := json.Marshal(runCommandArgs{Command: command})
	if err != nil {
		t.Fatalf("marshal run_command args: %v", err)
	}
	return string(body)
}

// gatedCommandRunAccessGate builds an interactive access gate that gates only
// tool.CapabilityCommandExecute (run_command's own requirement kind) and
// auto-allows every other capability kind, mirroring mcp_adapter_test.go's
// askForMCP for the command-execute capability instead of MCP's tool.invoke.
// fakeGrantIssuer stands in for a real sandbox executor (sandbox grant
// issuance is proven elsewhere in this module) so an approved command.execute
// requirement can still mint the grant token gate.Evaluator requires.
func gatedCommandRunAccessGate(t *testing.T) loop.AccessGate {
	t.Helper()
	bindings := make([]gate.AccessBinding, 0, len(accessKinds))
	for _, kind := range accessKinds {
		source := gate.AccessSource(allowAllAccessSource{})
		if kind == tool.CapabilityCommandExecute {
			source = gatedAccessSource{}
		}
		bindings = append(bindings, gate.AccessBinding{Kind: kind, Source: source})
	}
	evaluator, err := gate.NewInteractiveEvaluator(bindings, noRules{}, loop.GateApprover(), noRules{}, fakeGrantIssuer{})
	if err != nil {
		t.Fatalf("gate.NewInteractiveEvaluator: %v", err)
	}
	return evaluator
}

// fakeGrantIssuer stands in for a real enforcing executor (e.g.
// *sandbox.Executor): it mints a structurally valid but inert token, proving
// the gate/classifier mechanics without pulling in real OS enforcement, which
// this module already covers separately.
type fakeGrantIssuer struct{}

func (fakeGrantIssuer) GrantVersion() uint16 { return gate.CurrentGrantVersion }

func (fakeGrantIssuer) IssueGrant(_ context.Context, executionID, _, _, _, _, _, _ string, _ int64) (string, error) {
	return "fake-grant:" + executionID, nil
}

// ---- fixtures: a real command-safety classifier over a scripted model -----

// permissionClassifierTestModel is a minimal model.Model satisfying
// commandsafety.New's capability requirements (Tools, StructuredOutput, and
// StructuredOutputWithTools) — mirrors CodeRig's own
// permissionReviewTestModel (internal/app/permission_review_test.go), since
// this module's own testModel() deliberately omits these for ordinary loops.
func permissionClassifierTestModel() model.Model {
	return model.CustomModel(
		model.ProviderName("test"), model.APIFormatOpenAI,
		"http://127.0.0.1/v1", "test-classifier-model",
		model.WithTools(), model.WithStructuredOutput(), model.WithStructuredOutputWithTools(),
	)
}

// scriptedReviewClient is a fake inference.Client shaped exactly to the real
// command-safety classifier's wire contract (classifiers/internal/wire): a
// real Hustle, driven by real hustleruntime, calls Invoke (never Stream) with
// the classifier's marshaled input as the sole text block of the first user
// message; respond receives that raw input JSON and returns the model's
// structured-output JSON text verbatim. It never shortcuts
// gate.EvaluatePermissionAssessment or hand-builds an assessment — every
// verdict below travels through the real classifier's ValidateResult/policy
// reconciliation.
type scriptedReviewClient struct {
	respond func(inputJSON string) (string, error)
}

func (c *scriptedReviewClient) Invoke(_ context.Context, req inference.Request) (*inference.Response, error) {
	text, err := scriptedReviewInputText(req)
	if err != nil {
		return nil, err
	}
	out, err := c.respond(text)
	if err != nil {
		return nil, err
	}
	return &inference.Response{
		Message:      &content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{&content.TextBlock{Text: out}}}},
		Usage:        &content.Usage{},
		FinishReason: stream.FinishReasonStop,
	}, nil
}

func (c *scriptedReviewClient) Stream(context.Context, inference.Request) (*stream.StreamReader[content.Chunk], error) {
	return nil, errors.New("scriptedReviewClient.Stream not used (hustleruntime calls Invoke only)")
}

// scriptedReviewInputText extracts the sole text block of the first user
// message a real command-safety Hustle sends — the classifier's marshaled
// input JSON.
func scriptedReviewInputText(req inference.Request) (string, error) {
	if len(req.Messages) == 0 {
		return "", errors.New("scriptedReviewClient: no input message")
	}
	user, ok := req.Messages[0].(*content.UserMessage)
	if !ok || len(user.Blocks) == 0 {
		return "", errors.New("scriptedReviewClient: malformed input message")
	}
	text, ok := user.Blocks[0].(*content.TextBlock)
	if !ok {
		return "", errors.New("scriptedReviewClient: input is not text")
	}
	return text.Text, nil
}

// reviewInputEnvelope mirrors classifiers/internal/wire's private input wire
// shape just enough to read the one nested object a scripted verdict must
// echo back verbatim (DecodeOutput rejects any basis that does not match
// subject.Basis exactly).
type reviewInputEnvelope struct {
	Basis json.RawMessage `json:"basis"`
}

// reviewAllowLow builds a scriptedReviewClient.respond implementing a
// low-risk allow verdict — the shape a genuinely safe command's
// classification produces — with rationale embedded so callers can plant a
// unique marker for the no-leaked-content scenario.
func reviewAllowLow(rationale string) func(string) (string, error) {
	return func(inputJSON string) (string, error) {
		var envelope reviewInputEnvelope
		if err := json.Unmarshal([]byte(inputJSON), &envelope); err != nil {
			return "", fmt.Errorf("scriptedReviewClient: decode input: %w", err)
		}
		return fmt.Sprintf(
			`{"version":"command_safety_output.v1","basis":%s,"risk":"low","authorization":"unknown","categories":[],"recommendation":"allow","rationale":%q}`,
			string(envelope.Basis), rationale,
		), nil
	}
}

// reviewNeedsHuman builds a scriptedReviewClient.respond implementing a
// needs-human verdict: EvaluatePermissionAssessment rejects any
// Recommendation != ReviewAllow outright (pkg/gate/review_policy.go), so this
// is never eligible for auto-approval regardless of the registered policy's
// risk ceiling.
func reviewNeedsHuman(inputJSON string) (string, error) {
	var envelope reviewInputEnvelope
	if err := json.Unmarshal([]byte(inputJSON), &envelope); err != nil {
		return "", fmt.Errorf("scriptedReviewClient: decode input: %w", err)
	}
	return fmt.Sprintf(
		`{"version":"command_safety_output.v1","basis":%s,"risk":"high","authorization":"unknown","categories":["destructive_local"],"recommendation":"needs_human","rationale":"ambiguous destructive command needs human review"}`,
		string(envelope.Basis),
	), nil
}

// stubEvidenceAccess/stubEvidenceContainment are the two structurally
// required, always-succeed collaborators rig.WithPermissionReviewEvidence
// needs. None of this file's scenarios drive the classifier's evidence-tool
// loop (every scripted verdict above answers on the classifier's first
// inference call), so these never need to enforce anything real — that real
// enforcement is CodeRig's own permission_review_evidence.go, already proven
// there.
type stubEvidenceAccess struct{}

func (stubEvidenceAccess) AccessFor(tool.Requirement) (uint8, error) { return gate.AccessAllow, nil }

type stubEvidenceContainment struct{}

func (stubEvidenceContainment) VerifyEvidenceContainment(context.Context, gate.EvidenceContainmentPolicy, tool.Request) error {
	return nil
}

var (
	_ gate.EvidenceAccessEvaluator     = stubEvidenceAccess{}
	_ gate.EvidenceContainmentVerifier = stubEvidenceContainment{}
)

// newPermissionClassifierStores opens a fresh, isolated fsStores under its
// own temp root, and returns a fresh workspace materialization base
// alongside it — the one-shot setup every scenario except the restore one
// needs.
func newPermissionClassifierStores(t *testing.T) (fsStores, string) {
	t.Helper()
	return openFSStores(t, t.TempDir()), filepath.Join(t.TempDir(), "ws")
}

func permissionClassifierHustleLimits() rig.HustleLimits {
	return rig.HustleLimits{
		BlockingConcurrent: 2, BlockingQueued: 4,
		BackgroundConcurrent: 1, BackgroundQueued: 1,
		AuditTimeout: 5 * time.Second, FinalizationTimeout: 5 * time.Second, WorkerDrainTimeout: 5 * time.Second,
	}
}

// definePermissionClassifierRig assembles a real rig with one operator loop
// bound to the gated run_command tool, optionally registering a real
// command-safety classifier (enabled) built exactly the way CodeRig's own
// newPermissionReviewRegistration builds one (commandsafety.New +
// gate.NewPermissionClassifierSet + rig.WithPermissionClassifiers/
// WithPermissionReviewPolicy/WithPermissionReviewEvidence/
// WithPermissionReviewSecurityCeiling), but assembled directly against
// Harness's public pkg/rig surface rather than through CodeRig's private
// internal/app composition. The loop/tool/access-gate topology is identical
// whether enabled or not, so a disabled/enabled restore comparison isolates
// the permission-review registration as the only configuration delta.
func definePermissionClassifierRig(
	t *testing.T,
	stores fsStores,
	base string,
	operatorLLM inference.Client,
	classifierLLM inference.Client,
	shared *runCommandTool,
	enabled bool,
	allowMismatch bool,
) *rig.Rig {
	t.Helper()
	operator, err := loop.Define(
		loop.WithName(identity.AgentName("operator")),
		loop.WithInference(operatorLLM, testModel("operator")),
		loop.WithAccessGate(gatedCommandRunAccessGate(t)),
		loop.WithPolicyRevision("permission-classifier-test-v1"),
		loop.WithTools(runCommandDefinition(shared)),
	)
	if err != nil {
		t.Fatalf("loop.Define: %v", err)
	}

	opts := []rig.Option{
		rig.WithLoops(operator),
		rig.WithPrimers("operator"),
		rig.WithActivePrimer("operator"),
		rig.WithSessionStore(stores.sessions),
		// A registered classifier's real evidence tools need Harness's own
		// auto-derived workspace root (hustleruntime's runtime.evidence
		// collaborator) even though none of these scripted scenarios actually
		// drive the classifier to call one.
		rig.WithSessionWorkspaces(stores.workspace, base),
		rig.WithSnapshots(rig.SnapshotPolicy{Trigger: rig.SnapshotManual}),
	}
	if enabled {
		classifier, err := commandsafety.New(commandsafety.Options{
			Inference: classifierLLM,
			Model:     permissionClassifierTestModel(),
			Policy:    commandsafety.DefaultPolicy(),
			Evidence:  commandsafety.StandardEvidence(commandsafety.ReadEvidencePolicy{}),
		})
		if err != nil {
			t.Fatalf("commandsafety.New: %v", err)
		}
		classifiers, err := gate.NewPermissionClassifierSet(classifier)
		if err != nil {
			t.Fatalf("gate.NewPermissionClassifierSet: %v", err)
		}
		policy, err := gate.DefaultPermissionReviewPolicy("permission-classifier-review-v1")
		if err != nil {
			t.Fatalf("gate.DefaultPermissionReviewPolicy: %v", err)
		}
		opts = append(opts,
			rig.WithPermissionClassifiers(classifiers),
			rig.WithPermissionReviewPolicy(policy),
			rig.WithPermissionReviewEvidence(stubEvidenceAccess{}, stubEvidenceContainment{}, commandsafety.RequiredEvidenceKinds()),
			rig.WithPermissionReviewSecurityCeiling("permission-classifier-test-ceiling/v1"),
			rig.WithHustleLimits(permissionClassifierHustleLimits()),
		)
	}
	if allowMismatch {
		opts = append(opts, rig.WithAllowConfigMismatch())
	}
	r, err := rig.Define(opts...)
	if err != nil {
		t.Fatalf("rig.Define: %v", err)
	}
	return r
}

// ---- fixtures: event-stream gate helpers -----------------------------------

// internalEventsFor replays the FULL privileged event stream for id,
// including event.Internal-visibility audit events (PermissionReviewStarted/
// Completed) the ordinary public eventsFor helper (fixtures_test.go) cannot
// see (sessionstore.Store.OpenEventReplayer filters non-public visibility;
// only OpenInternalEventReplayer surfaces everything).
func internalEventsFor(t *testing.T, ctx context.Context, store *sessionstore.Store, id uuid.UUID) []event.Event {
	t.Helper()
	replayer, err := store.OpenInternalEventReplayer(id, sessionstore.ReplayRequest{FromSeq: 0})
	if err != nil {
		t.Fatalf("OpenInternalEventReplayer: %v", err)
	}
	cursor, err := replayer.Open(ctx, journal.ReplayRequest{From: journal.Beginning()})
	if err != nil {
		t.Fatalf("replayer.Open: %v", err)
	}
	defer cursor.Close()
	var events []event.Event
	for {
		ev, _, err := cursor.Next(ctx)
		if errors.Is(err, io.EOF) {
			return events
		}
		if err != nil {
			t.Fatalf("cursor.Next: %v", err)
		}
		events = append(events, ev)
	}
}

func waitPermissionGateOpened(t *testing.T, ctx context.Context, sub event.Subscription, timeout time.Duration) event.GateOpened {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case delivery, ok := <-sub.Events():
			if !ok {
				t.Fatalf("subscription closed waiting for a permission GateOpened: %v", sub.Err())
			}
			if opened, ok := delivery.Event.(event.GateOpened); ok && opened.Gate.Kind == gate.KindPermission {
				return opened
			}
		case <-deadline:
			t.Fatal("permission gate did not open within deadline")
		case <-ctx.Done():
			t.Fatalf("context done waiting for permission gate: %v", ctx.Err())
		}
	}
}

func waitGateResolved(t *testing.T, ctx context.Context, sub event.Subscription, gateID gate.ID, timeout time.Duration) event.GateResolved {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case delivery, ok := <-sub.Events():
			if !ok {
				t.Fatalf("subscription closed waiting for GateResolved: %v", sub.Err())
			}
			if resolved, ok := delivery.Event.(event.GateResolved); ok && resolved.GateID == gateID {
				return resolved
			}
		case <-deadline:
			t.Fatalf("gate %v did not resolve within %v", gateID, timeout)
		case <-ctx.Done():
			t.Fatalf("context done waiting for gate resolution: %v", ctx.Err())
		}
	}
}

func assertNoGateResolvedWithin(t *testing.T, sub event.Subscription, gateID gate.ID, window time.Duration) {
	t.Helper()
	deadline := time.After(window)
	for {
		select {
		case delivery, ok := <-sub.Events():
			if !ok {
				return
			}
			if resolved, ok := delivery.Event.(event.GateResolved); ok && resolved.GateID == gateID {
				t.Fatalf("gate %v resolved unexpectedly (source=%+v) before any human ever responded", gateID, resolved.Source)
			}
		case <-deadline:
			return
		}
	}
}

func waitTurnDoneEvent(t *testing.T, ctx context.Context, sub event.Subscription, timeout time.Duration) event.TurnDone {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case delivery, ok := <-sub.Events():
			if !ok {
				t.Fatalf("subscription closed waiting for TurnDone: %v", sub.Err())
			}
			switch ev := delivery.Event.(type) {
			case event.TurnDone:
				return ev
			case event.TurnFailed:
				t.Fatalf("turn failed: %+v", ev)
			}
		case <-deadline:
			t.Fatal("turn did not complete within deadline")
		case <-ctx.Done():
			t.Fatalf("context done waiting for turn done: %v", ctx.Err())
		}
	}
}

// ================================================================
// Scenario 1: safe allow.
// ================================================================

// TestPermissionClassifierSafeCommandAutoApproved proves a genuinely safe
// command, reviewed by a real command-safety classifier registered on a real
// rig.Session, is auto-approved end to end: the permission gate resolves
// with gate.ResponseFromClassifier provenance, the test never calls
// RespondGate at all, and the underlying tool executes exactly once.
func TestPermissionClassifierSafeCommandAutoApproved(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	stores, base := newPermissionClassifierStores(t)
	shared := &runCommandTool{}
	classifierLLM := &scriptedReviewClient{respond: reviewAllowLow("routine, read-only diagnostic command")}
	operatorLLM := newScriptLLM(call("run-1", "run_command", commandArgsJSON(t, "echo safe-marker-8a41c9")), say("done"))

	r := definePermissionClassifierRig(t, stores, base, operatorLLM, classifierLLM, shared, true, false)
	sess, err := r.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { shutdown(t, sess) })

	sub, err := sess.SubscribeEvents(event.EventFilter{Enduring: event.LoopScope{All: true}})
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if _, err := sess.Submit(ctx, textBlock("run the safe command")); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	opened := waitPermissionGateOpened(t, ctx, sub, 10*time.Second)
	resolved := waitGateResolved(t, ctx, sub, opened.Gate.ID, 10*time.Second)
	if resolved.Source.Kind != gate.ResponseFromClassifier {
		t.Fatalf("gate resolved with source %+v, want Kind=%q (no human RespondGate was ever called)", resolved.Source, gate.ResponseFromClassifier)
	}
	waitTurnDoneEvent(t, ctx, sub, 10*time.Second)

	if got := shared.callCount(); got != 1 {
		t.Fatalf("run_command executed %d times, want exactly 1", got)
	}
	reqs := operatorLLM.waitRequests(t, 2)
	results := toolResults(reqs[1])
	if len(results) != 1 || !strings.Contains(results[0], "safe-marker-8a41c9") {
		t.Fatalf("operator observed tool results %v, want the real run_command result containing the marker", results)
	}
}

// ================================================================
// Scenario 2: human fallback.
// ================================================================

// TestPermissionClassifierAmbiguousCommandFallsBackToHuman proves a command
// the classifier does NOT confidently approve (a needs_human verdict) never
// silently auto-approves: the gate stays open (no GateResolved) until a real
// human RespondGate call resolves it, and only then does the tool execute.
func TestPermissionClassifierAmbiguousCommandFallsBackToHuman(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	stores, base := newPermissionClassifierStores(t)
	shared := &runCommandTool{}
	classifierLLM := &scriptedReviewClient{respond: reviewNeedsHuman}
	operatorLLM := newScriptLLM(call("run-1", "run_command", commandArgsJSON(t, "rm -rf /ambiguous-target")), say("done"))

	r := definePermissionClassifierRig(t, stores, base, operatorLLM, classifierLLM, shared, true, false)
	sess, err := r.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { shutdown(t, sess) })

	sub, err := sess.SubscribeEvents(event.EventFilter{Enduring: event.LoopScope{All: true}})
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if _, err := sess.Submit(ctx, textBlock("run the ambiguous command")); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	opened := waitPermissionGateOpened(t, ctx, sub, 10*time.Second)
	assertNoGateResolvedWithin(t, sub, opened.Gate.ID, 300*time.Millisecond)
	if got := shared.callCount(); got != 0 {
		t.Fatalf("run_command executed %d times before any approval, want 0", got)
	}

	if err := sess.RespondGate(ctx, gate.GateResponse{
		GateID: opened.Gate.ID,
		Action: string(gate.ApprovalApprove),
		Source: gate.ResponseSource{Kind: gate.ResponseFromUser},
	}); err != nil {
		t.Fatalf("RespondGate: %v", err)
	}

	resolved := waitGateResolved(t, ctx, sub, opened.Gate.ID, 10*time.Second)
	if resolved.Source.Kind != gate.ResponseFromUser {
		t.Fatalf("gate resolved with source %+v, want Kind=%q (the classifier must never silently approve a needs_human verdict)", resolved.Source, gate.ResponseFromUser)
	}
	waitTurnDoneEvent(t, ctx, sub, 10*time.Second)
	if got := shared.callCount(); got != 1 {
		t.Fatalf("run_command executed %d times after human approval, want exactly 1", got)
	}
}

// ================================================================
// Scenario 3: race.
// ================================================================

// TestPermissionClassifierGateRaceHumanApprovalDoesNotDoubleResolve races a
// real human RespondGate call against the same gate a real auto-approving
// classifier is concurrently resolving. Run under -race, this proves the
// underlying gate-resolution state has no data race; the event-count
// assertions below independently prove the gate itself never double-resolves
// and the tool never double-executes regardless of which side wins.
func TestPermissionClassifierGateRaceHumanApprovalDoesNotDoubleResolve(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	stores, base := newPermissionClassifierStores(t)
	shared := &runCommandTool{}
	classifierLLM := &scriptedReviewClient{respond: reviewAllowLow("routine command, races a human approval")}
	operatorLLM := newScriptLLM(call("run-1", "run_command", commandArgsJSON(t, "echo race-marker-2f6e0d")), say("done"))

	r := definePermissionClassifierRig(t, stores, base, operatorLLM, classifierLLM, shared, true, false)
	sess, err := r.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sessionID := sess.SessionID()
	t.Cleanup(func() { shutdown(t, sess) })

	sub, err := sess.SubscribeEvents(event.EventFilter{Enduring: event.LoopScope{All: true}})
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if _, err := sess.Submit(ctx, textBlock("run the racing command")); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	opened := waitPermissionGateOpened(t, ctx, sub, 10*time.Second)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Fired immediately, with no synchronization against the classifier's
		// own in-flight resolution: this IS the race. Either outcome (this
		// wins, or the classifier already won) is an acceptable, correctly
		// handled result — a hang or a panic is not.
		_ = sess.RespondGate(ctx, gate.GateResponse{
			GateID: opened.Gate.ID,
			Action: string(gate.ApprovalApprove),
			Source: gate.ResponseSource{Kind: gate.ResponseFromUser},
		})
	}()

	waitGateResolved(t, ctx, sub, opened.Gate.ID, 10*time.Second)
	waitTurnDoneEvent(t, ctx, sub, 10*time.Second)
	wg.Wait()

	events := eventsFor(t, ctx, stores.sessions, sessionID)
	resolutions := 0
	for _, ev := range events {
		if resolved, ok := ev.(event.GateResolved); ok && resolved.GateID == opened.Gate.ID {
			resolutions++
		}
	}
	if resolutions != 1 {
		t.Fatalf("gate %v has %d GateResolved records, want exactly 1 (no double resolution)", opened.Gate.ID, resolutions)
	}
	if got := shared.callCount(); got != 1 {
		t.Fatalf("run_command executed %d times, want exactly 1 regardless of which side resolved the race", got)
	}
}

// ================================================================
// Scenario 4: restore.
// ================================================================

// TestPermissionClassifierRestoreRejectsDisabledToEnabledConfigChange proves,
// at the cross-module integration level, Phase 6's fixed restore behavior: a
// session created with permission review DISABLED cannot silently restore
// into a rig with it ENABLED (a real *session.RestoreRejectedError, mirroring
// harness's own TestRestoreSessionFingerprintFieldsMismatchPolicy for the
// AgentKind field) unless the consumer explicitly opts in via
// rig.WithAllowConfigMismatch(); once accepted, the resulting (now enabled)
// session state restores again cleanly under matching config with no
// override needed.
func TestPermissionClassifierRestoreRejectsDisabledToEnabledConfigChange(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	persistence := filepath.Join(t.TempDir(), "persistence")
	// One shared workspace base across every leg: the restore comparison below
	// must isolate the permission-review registration as the ONLY configuration
	// delta, so the workspace placement itself must never differ between legs
	// (a changed base would independently trip the SAME restore-rejection path
	// this test is not about).
	wsBase := filepath.Join(t.TempDir(), "ws")
	shared := &runCommandTool{}

	var id uuid.UUID
	func() {
		stores := openFSStores(t, persistence)
		defer func() { _ = stores.fs.Close() }()
		disabled := definePermissionClassifierRig(t, stores, wsBase, newScriptLLM(say("idle")), nil, shared, false, false)
		sess, err := disabled.NewSession(ctx)
		if err != nil {
			t.Fatalf("NewSession (disabled): %v", err)
		}
		id = sess.SessionID()
		shutdown(t, sess)
	}()

	func() {
		stores := openFSStores(t, persistence)
		defer func() { _ = stores.fs.Close() }()
		enabledNoAllow := definePermissionClassifierRig(t, stores, wsBase, newScriptLLM(say("idle")), &scriptedReviewClient{respond: reviewAllowLow("n/a")}, shared, true, false)
		restored, err := enabledNoAllow.RestoreSession(ctx, id)
		if err == nil {
			shutdown(t, restored)
			t.Fatal("restore from disabled to enabled permission review succeeded without WithAllowConfigMismatch, want rejection")
		}
		var rejected *session.RestoreRejectedError
		var mismatched *session.ConfigMismatchError
		if !errors.As(err, &rejected) && !errors.As(err, &mismatched) {
			t.Fatalf("restore error = %T %v, want *session.RestoreRejectedError or *session.ConfigMismatchError", err, err)
		}
	}()

	func() {
		stores := openFSStores(t, persistence)
		defer func() { _ = stores.fs.Close() }()
		enabledAllow := definePermissionClassifierRig(t, stores, wsBase, newScriptLLM(say("idle")), &scriptedReviewClient{respond: reviewAllowLow("n/a")}, shared, true, true)
		restoredEnabled, err := enabledAllow.RestoreSession(ctx, id)
		if err != nil {
			t.Fatalf("restore with WithAllowConfigMismatch: %v", err)
		}
		shutdown(t, restoredEnabled)
	}()

	// The persisted session now carries the ENABLED config; restoring again
	// under the SAME (enabled) config must succeed without any override.
	func() {
		stores := openFSStores(t, persistence)
		defer func() { _ = stores.fs.Close() }()
		sameConfigAgain := definePermissionClassifierRig(t, stores, wsBase, newScriptLLM(say("idle")), &scriptedReviewClient{respond: reviewAllowLow("n/a")}, shared, true, false)
		restoredAgain, err := sameConfigAgain.RestoreSession(ctx, id)
		if err != nil {
			t.Fatalf("same-config restore (no override) failed: %v", err)
		}
		shutdown(t, restoredAgain)
	}()
}

// ================================================================
// Scenario 5: no leaked review content.
// ================================================================

// TestPermissionClassifierAuditTrailNeverLeaksReviewRationale plants a unique
// secret marker ONLY in the classifier's own model-produced rationale text —
// never in the command, the tool's requirement description, or anything else
// that legitimately flows through the durable journal — and proves that
// marker never appears, unredacted, in ANY durable session event. Harness's
// event.PermissionReviewStarted/Completed structurally exclude rationale,
// evidence, and model output (pkg/event/permission_review.go); this test is
// the end-to-end proof that a real review's own content never leaks around
// that structural boundary either.
func TestPermissionClassifierAuditTrailNeverLeaksReviewRationale(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	stores, base := newPermissionClassifierStores(t)
	shared := &runCommandTool{}
	const secretRationale = "internal-classifier-secret-detail-9f3a7c1e-do-not-surface"
	classifierLLM := &scriptedReviewClient{respond: reviewAllowLow(secretRationale)}
	operatorLLM := newScriptLLM(call("run-1", "run_command", commandArgsJSON(t, "echo audit-marker-5c11")), say("done"))

	r := definePermissionClassifierRig(t, stores, base, operatorLLM, classifierLLM, shared, true, false)
	sess, err := r.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sessionID := sess.SessionID()
	t.Cleanup(func() { shutdown(t, sess) })

	sub, err := sess.SubscribeEvents(event.EventFilter{Enduring: event.LoopScope{All: true}})
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if _, err := sess.Submit(ctx, textBlock("run the safe command")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	opened := waitPermissionGateOpened(t, ctx, sub, 10*time.Second)
	waitGateResolved(t, ctx, sub, opened.Gate.ID, 10*time.Second)
	waitTurnDoneEvent(t, ctx, sub, 10*time.Second)

	// PermissionReviewStarted/Completed are event.Internal visibility
	// (pkg/sessionstore/replay.go: "Product-facing readers must use
	// OpenEventReplayer, which filters non-public event visibility") — the
	// ordinary eventsFor helper (public replay) structurally cannot see them,
	// so this scenario needs the privileged internal replayer instead. Their
	// durable publish also is not ordered against GateResolved/TurnDone (it
	// runs on review's own fire-and-forget audit goroutine), so poll until
	// the record lands rather than assuming it is already there.
	var events []event.Event
	deadline := time.Now().Add(10 * time.Second)
	for {
		events = internalEventsFor(t, ctx, stores.sessions, sessionID)
		if firstEventOfType[event.PermissionReviewCompleted](events) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("journal has no PermissionReviewCompleted within 10s; the absence of the secret marker proves nothing without a real review actually having run")
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, ev := range events {
		marshaled, err := event.MarshalEvent(ev)
		if err != nil {
			t.Fatalf("MarshalEvent(%T): %v", ev, err)
		}
		if bytes.Contains(marshaled, []byte(secretRationale)) {
			t.Fatalf("event %T carries the classifier's raw rationale unredacted: %s", ev, marshaled)
		}
	}
}

// firstEventOfType reports whether events contains at least one event of type T.
func firstEventOfType[T event.Event](events []event.Event) bool {
	for _, ev := range events {
		if _, ok := ev.(T); ok {
			return true
		}
	}
	return false
}
