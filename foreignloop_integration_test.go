//go:build integration && (darwin || (linux && !android))

package tests

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/inference"
)

func TestForeignloopPrimary(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	process := newForeignloopProcess(t, foreignloopClaude, "result text", "")
	primary := foreignloopDefinition(t, "primary", loop.EngineForeignClaude, deterministicLLM{})
	sess, store := newForeignloopSession(t, ctx, process, "primary", primary)
	sub := subscribeForeignloopEvents(t, sess)

	submitID, err := sess.Submit(ctx, textBlock("go"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitForeignloopTurnDone(t, ctx, sub, sess.ActiveLoop().ID())

	events := eventsFor(t, ctx, store, sess.SessionID())
	started := rootForeignloopStarted(t, events)
	if started.ForeignSID == "" {
		t.Fatal("primary LoopStarted.ForeignSID is empty, want prebound Claude sid")
	}
	turnEvents := foreignloopTurnEvents(events, started.LoopID)
	wantKinds := []string{"TurnStarted", "StepDone", "TurnDone"}
	if got := foreignloopEventKinds(turnEvents); !reflect.DeepEqual(got, wantKinds) {
		t.Fatalf("primary enduring sequence = %v, want %v", got, wantKinds)
	}
	if got := turnEvents[0].(event.TurnStarted).Cause.CommandID; got != submitID {
		t.Errorf("TurnStarted.Cause.CommandID = %v, want submit id %v", got, submitID)
	}
	if got := foreignloopAIText(turnEvents[2].(event.TurnDone).Message); got != "result text" {
		t.Errorf("TurnDone.Message text = %q, want %q", got, "result text")
	}
	process.assertStartNew(t, started.ForeignSID)
}

func TestForeignloopCodexPrimaryLateBound(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const boundSID = "0199a213-81c0-7800-8aa1-bbab2a035a53"
	process := newForeignloopProcess(t, foreignloopCodex, "codex result", boundSID)
	primary := foreignloopDefinition(t, "primary", loop.EngineForeignCodex, deterministicLLM{})
	sess, store := newForeignloopSession(t, ctx, process, "primary", primary)
	sub := subscribeForeignloopEvents(t, sess)

	submitID, err := sess.Submit(ctx, textBlock("go"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitForeignloopTurnDone(t, ctx, sub, sess.ActiveLoop().ID())

	events := eventsFor(t, ctx, store, sess.SessionID())
	started := rootForeignloopStarted(t, events)
	if started.ForeignSID != "" {
		t.Fatalf("primary LoopStarted.ForeignSID = %q, want empty for late-bound Codex", started.ForeignSID)
	}
	turnEvents := foreignloopTurnEvents(events, started.LoopID)
	wantKinds := []string{"TurnStarted", "ForeignSessionBound", "StepDone", "TurnDone"}
	if got := foreignloopEventKinds(turnEvents); !reflect.DeepEqual(got, wantKinds) {
		t.Fatalf("primary enduring sequence = %v, want %v", got, wantKinds)
	}
	if got := turnEvents[0].(event.TurnStarted).Cause.CommandID; got != submitID {
		t.Errorf("TurnStarted.Cause.CommandID = %v, want submit id %v", got, submitID)
	}
	bound := turnEvents[1].(event.ForeignSessionBound)
	if got := bound.ForeignSID; got != boundSID {
		t.Errorf("ForeignSessionBound.ForeignSID = %q, want %q", got, boundSID)
	}
	if bound.LoopID != started.LoopID || bound.SessionID != sess.SessionID() {
		t.Errorf("ForeignSessionBound header = session %v loop %v, want session %v loop %v", bound.SessionID, bound.LoopID, sess.SessionID(), started.LoopID)
	}
	if got := foreignloopAIText(turnEvents[3].(event.TurnDone).Message); got != "codex result" {
		t.Errorf("TurnDone.Message text = %q, want %q", got, "codex result")
	}
	process.assertStartNew(t, "")
}

func TestForeignloopSubagent(t *testing.T) {
	t.Parallel()
	testForeignloopSubagent(t, foreignloopClaude, loop.EngineForeignClaude, "subagent says hi", "", "tool-use-1")
}

func TestForeignloopCodexSubagentLateBound(t *testing.T) {
	t.Parallel()
	testForeignloopSubagent(t, foreignloopCodex, loop.EngineForeignCodex, "codex subagent final", "0199a213-81c0-7800-8aa1-bbab2a035a54", "tool-use-codex")
}

func testForeignloopSubagent(t *testing.T, provider foreignloopProvider, engine loop.Engine, final, boundSID, toolUseID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	process := newForeignloopProcess(t, provider, final, boundSID)
	parentLLM := newForeignloopScriptLLM(
		[]content.Chunk{foreignloopToolUse(toolUseID, "StartAgent", `{"agent_type":"builder","instructions":"hi","wait_for_response":true}`)},
		[]content.Chunk{&content.TextChunk{Text: "parent done"}},
	)
	parent := foreignloopDefinition(t, "planner", loop.EngineNative, parentLLM, "builder")
	child := foreignloopDefinition(t, "builder", engine, deterministicLLM{})
	sess, store := newForeignloopSession(t, ctx, process, "planner", parent, child)
	sub := subscribeForeignloopEvents(t, sess)
	primaryID := sess.ActiveLoop().ID()

	if _, err := sess.Submit(ctx, textBlock("go")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitForeignloopTurnDone(t, ctx, sub, primaryID)

	if got := parentLLM.toolResults(); !reflect.DeepEqual(got, []string{final}) {
		t.Fatalf("parent model tool results = %q, want exact subagent final text %q", got, final)
	}
	events := eventsFor(t, ctx, store, sess.SessionID())
	started := childForeignloopStarted(t, events, primaryID)
	if started.Cause.LoopID != primaryID {
		t.Errorf("sub-loop LoopStarted.Cause.LoopID = %v, want parent %v", started.Cause.LoopID, primaryID)
	}
	if started.ParentToolUseID != toolUseID {
		t.Errorf("sub-loop LoopStarted.ParentToolUseID = %q, want %q", started.ParentToolUseID, toolUseID)
	}
	turnEvents := foreignloopTurnEvents(events, started.LoopID)
	wantKinds := []string{"TurnStarted", "StepDone", "TurnDone"}
	if provider == foreignloopCodex {
		wantKinds = []string{"TurnStarted", "ForeignSessionBound", "StepDone", "TurnDone"}
	}
	if got := foreignloopEventKinds(turnEvents); !reflect.DeepEqual(got, wantKinds) {
		t.Fatalf("subagent enduring sequence = %v, want %v", got, wantKinds)
	}
	if provider == foreignloopCodex {
		if started.ForeignSID != "" {
			t.Errorf("Codex sub-loop LoopStarted.ForeignSID = %q, want empty", started.ForeignSID)
		}
		bound := turnEvents[1].(event.ForeignSessionBound)
		if got := bound.ForeignSID; got != boundSID {
			t.Errorf("ForeignSessionBound.ForeignSID = %q, want %q", got, boundSID)
		}
		if bound.LoopID != started.LoopID || bound.SessionID != sess.SessionID() {
			t.Errorf("ForeignSessionBound header = session %v loop %v, want session %v loop %v", bound.SessionID, bound.LoopID, sess.SessionID(), started.LoopID)
		}
	} else if started.ForeignSID == "" {
		t.Error("Claude sub-loop LoopStarted.ForeignSID is empty, want prebound sid")
	}
	done := turnEvents[len(turnEvents)-1].(event.TurnDone)
	if got := foreignloopAIText(done.Message); got != final {
		t.Errorf("subagent TurnDone.Message text = %q, want %q", got, final)
	}
	process.assertStartNew(t, started.ForeignSID)
}

func TestForeignloopQueuedDelegateInterrupt(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	process := newControlledForeignloopProcess(t, foreignloopClaude, "unused", "", foreignloopProcessBlock)
	var active foreignloopAgentToolResult
	var sess session.SessionController
	var handBacksMu sync.Mutex
	var handBacks []string
	parentLLM := newForeignloopScenarioLLM(
		func(context.Context, inference.Request) ([]content.Chunk, error) {
			return foreignloopToolCall("interrupt-start", `{"agent_type":"child","instructions":"A","wait_for_response":false}`), nil
		},
		func(stepCtx context.Context, request inference.Request) ([]content.Chunk, error) {
			var err error
			active, err = foreignloopLastAgentToolResult(request)
			if err != nil {
				return nil, err
			}
			if active.State != "working" {
				return nil, fmt.Errorf("start admission = %+v, want working state", active)
			}
			if err := process.waitStarted(stepCtx); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"agent_id":%q,"message":"B","wait_for_response":false}`, active.AgentID)
			return foreignloopToolCall("interrupt-send", input), nil
		},
		func(stepCtx context.Context, request inference.Request) ([]content.Chunk, error) {
			queued, err := foreignloopLastAgentToolResult(request)
			if err != nil {
				return nil, err
			}
			if queued.AgentID != active.AgentID || queued.State != "working" {
				return nil, fmt.Errorf("queued message = %+v, want working state for %s", queued, active.AgentID)
			}
			// StopAgent in the tagged Harness waits for LoopIdle, which the tagged
			// Foreignloops backend does not emit. The public controller exposes the
			// supported cancellation primitive and still exercises queued-child
			// interruption against the published foreign backend.
			if err := interruptForeignloopChild(stepCtx, sess, active.AgentID); err != nil {
				return nil, err
			}
			return foreignloopFinal("parent done"), nil
		},
		// Both backgrounded requests — the wait_for_response:false start and the
		// wait_for_response:false send — are handed back to the parent as their own
		// machine-originated turn once the child reaches its terminal state. The
		// scenario scripts one step per hand-back and records which request each one
		// closes, so the case asserts the delegation contract (one hand-back per
		// backgrounded request, correlated and terminal) rather than a step count
		// that happens to be observable at one instant.
		foreignloopRecordHandBack(&handBacks, &handBacksMu, &active),
		foreignloopRecordHandBack(&handBacks, &handBacksMu, &active),
	)
	parent := foreignloopManagedDefinition(t, "planner", loop.EngineNative, parentLLM, "child")
	child := foreignloopDefinition(t, "child", loop.EngineForeignClaude, deterministicLLM{})
	var store *sessionstore.Store
	sess, store = newForeignloopSession(t, ctx, process, "planner", parent, child)
	sub := subscribeForeignloopEvents(t, sess)
	parentID := sess.ActiveLoop().ID()
	if _, err := sess.Submit(ctx, textBlock("go")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitForeignloopTurnDone(t, ctx, sub, parentID)

	events := eventsFor(t, ctx, store, sess.SessionID())
	childStarted := childForeignloopStarted(t, events, parentID)
	events = waitForeignloopTurnTerminal(t, ctx, store, sess.SessionID(), childStarted.LoopID)
	assertForeignloopTurnKinds(t, events, childStarted.LoopID, []string{"TurnStarted", "TurnInterrupted"})
	assertForeignloopInputCancelled(t, events, childStarted.LoopID, event.CancelTurnInterrupted)
	process.assertCallCount(t, 1)
	// The child's terminal event does not order the parent's consumption of the
	// hand-backs it releases, so wait for the model calls that carry them.
	if got := parentLLM.waitCalls(t, ctx, 5); got != 5 {
		t.Fatalf("parent model calls = %d, want exactly 5: three scripted steps and one hand-back per backgrounded request", got)
	}
	assertForeignloopHandBacks(t, &handBacks, &handBacksMu, active.AgentID, 2)
	// One committed parent turn per model call, and no live turn left for the
	// cleanup shutdown to cancel.
	waitForeignloopTurnDoneCount(t, ctx, store, sess.SessionID(), parentID, 3)
}

func TestForeignloopQueuedDelegateTimeout(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	process := newControlledForeignloopProcess(t, foreignloopClaude, "unused", "", foreignloopProcessBlock)
	var active foreignloopAgentToolResult
	var sess session.SessionController
	var store *sessionstore.Store
	var handBacksMu sync.Mutex
	var handBacks []string
	parentLLM := newForeignloopScenarioLLM(
		func(context.Context, inference.Request) ([]content.Chunk, error) {
			return foreignloopToolCall("timeout-start", `{"agent_type":"child","instructions":"A","wait_for_response":false}`), nil
		},
		func(stepCtx context.Context, request inference.Request) ([]content.Chunk, error) {
			var err error
			active, err = foreignloopLastAgentToolResult(request)
			if err != nil {
				return nil, err
			}
			if active.State != "working" {
				return nil, fmt.Errorf("start admission = %+v, want working state", active)
			}
			if err := process.waitStarted(stepCtx); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"agent_id":%q,"message":"B","wait_for_response":true,"timeout_seconds":0}`, active.AgentID)
			return foreignloopToolCall("timeout-send", input), nil
		},
		func(stepCtx context.Context, request inference.Request) ([]content.Chunk, error) {
			// Harness v0.27.1+ (5fbc312b) returns a failed wait as a structured error
			// result named for the tool that issued it, not the older "error: agent …"
			// text; the raw published string is still asserted exactly.
			if err := foreignloopExpectRawToolResult(request, "MessageAgent failed: agent timed out"); err != nil {
				return nil, err
			}
			childID, err := uuid.Parse(active.AgentID)
			if err != nil {
				return nil, fmt.Errorf("parse foreignloop agent id %q: %w", active.AgentID, err)
			}
			// The timed-out foreground request retracts its own queued input, but
			// harness dispatches that retraction on a detached goroutine (v0.26.0
			// internal/sessionruntime/drain.go, `go interrupt()` in
			// drainCorrelatedWithState's ctx.Done branch) and returns the timeout to
			// the model without waiting for it. Interrupting the child before that
			// retraction has landed would resolve the same queued input under
			// CancelTurnInterrupted instead, so wait for the retraction this case is
			// about rather than racing it.
			if err := waitForeignloopInputCancelledReason(stepCtx, store, sess.SessionID(), childID, event.CancelClientRetracted); err != nil {
				return nil, err
			}
			// StopAgent has the same tagged foreign LoopIdle incompatibility as the
			// interrupt case; use the supported public controller after observing
			// the timeout while retaining the raw published timeout error assertion.
			if err := interruptForeignloopChild(stepCtx, sess, active.AgentID); err != nil {
				return nil, err
			}
			return foreignloopFinal("parent done"), nil
		},
		// The wait_for_response:false start is handed back to the parent as its own
		// machine-originated turn once the child terminates.
		foreignloopRecordHandBack(&handBacks, &handBacksMu, &active),
	)
	parent := foreignloopManagedDefinition(t, "planner", loop.EngineNative, parentLLM, "child")
	child := foreignloopDefinition(t, "child", loop.EngineForeignClaude, deterministicLLM{})
	sess, store = newForeignloopSession(t, ctx, process, "planner", parent, child)
	sub := subscribeForeignloopEvents(t, sess)
	parentID := sess.ActiveLoop().ID()
	if _, err := sess.Submit(ctx, textBlock("go")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitForeignloopTurnDone(t, ctx, sub, parentID)

	events := eventsFor(t, ctx, store, sess.SessionID())
	childStarted := childForeignloopStarted(t, events, parentID)
	events = waitForeignloopTurnTerminal(t, ctx, store, sess.SessionID(), childStarted.LoopID)
	assertForeignloopTurnKinds(t, events, childStarted.LoopID, []string{"TurnStarted", "TurnInterrupted"})
	// The timed-out foreground request retracts itself; the case waited for that
	// retraction above before interrupting the still-running active turn.
	assertForeignloopInputCancelled(t, events, childStarted.LoopID, event.CancelClientRetracted)
	process.assertCallCount(t, 1)
	if got := parentLLM.waitCalls(t, ctx, 4); got != 4 {
		t.Fatalf("parent model calls = %d, want exactly 4: three scripted steps and one hand-back for the backgrounded start", got)
	}
	assertForeignloopHandBacks(t, &handBacks, &handBacksMu, active.AgentID, 1)
	waitForeignloopTurnDoneCount(t, ctx, store, sess.SessionID(), parentID, 2)
}

func TestForeignloopProviderFailureWithQueuedDelegates(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	process := newControlledForeignloopProcess(t, foreignloopClaude, "unused", "", foreignloopProcessFailAfterRelease)
	var active foreignloopAgentToolResult
	var handBacksMu sync.Mutex
	var handBacks []string
	parentLLM := newForeignloopScenarioLLM(
		func(context.Context, inference.Request) ([]content.Chunk, error) {
			return foreignloopToolCall("failure-start", `{"agent_type":"child","instructions":"A","wait_for_response":false}`), nil
		},
		func(stepCtx context.Context, request inference.Request) ([]content.Chunk, error) {
			var err error
			active, err = foreignloopLastAgentToolResult(request)
			if err != nil {
				return nil, err
			}
			if active.State != "working" {
				return nil, fmt.Errorf("start admission = %+v, want working state", active)
			}
			if err := process.waitStarted(stepCtx); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"agent_id":%q,"message":"B","wait_for_response":false}`, active.AgentID)
			return foreignloopToolCall("failure-send-b", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			queuedB, err := foreignloopLastAgentToolResult(request)
			if err != nil {
				return nil, err
			}
			if queuedB.AgentID != active.AgentID || queuedB.State != "working" {
				return nil, fmt.Errorf("B admission = %+v, want working state for %s", queuedB, active.AgentID)
			}
			input := fmt.Sprintf(`{"agent_id":%q,"message":"C","wait_for_response":false}`, active.AgentID)
			return foreignloopToolCall("failure-send-c", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			queuedC, err := foreignloopLastAgentToolResult(request)
			if err != nil {
				return nil, err
			}
			if queuedC.AgentID != active.AgentID || queuedC.State != "working" {
				return nil, fmt.Errorf("C admission = %+v, want working state for %s", queuedC, active.AgentID)
			}
			if err := process.release(); err != nil {
				return nil, err
			}
			return foreignloopFinal("parent done"), nil
		},
		// One hand-back per backgrounded request: the start and both queued sends.
		foreignloopRecordHandBack(&handBacks, &handBacksMu, &active),
		foreignloopRecordHandBack(&handBacks, &handBacksMu, &active),
		foreignloopRecordHandBack(&handBacks, &handBacksMu, &active),
	)
	parent := foreignloopManagedDefinition(t, "planner", loop.EngineNative, parentLLM, "child")
	child := foreignloopDefinition(t, "child", loop.EngineForeignClaude, deterministicLLM{})
	sess, store := newForeignloopSession(t, ctx, process, "planner", parent, child)
	sub := subscribeForeignloopEvents(t, sess)
	parentID := sess.ActiveLoop().ID()
	if _, err := sess.Submit(ctx, textBlock("go")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitForeignloopTurnDone(t, ctx, sub, parentID)

	events := eventsFor(t, ctx, store, sess.SessionID())
	childStarted := childForeignloopStarted(t, events, parentID)
	waitForeignloopTurnTerminal(t, ctx, store, sess.SessionID(), childStarted.LoopID)
	events = waitForeignloopInputCancelled(t, ctx, store, sess.SessionID(), childStarted.LoopID, event.CancelTurnFailed, 2)
	assertForeignloopTurnKinds(t, events, childStarted.LoopID, []string{"TurnStarted", "TurnFailed"})
	queuedIDs := foreignloopCancelledRequestIDs(t, events, childStarted.LoopID, event.CancelTurnFailed, "B", "C")
	assertForeignloopAcceptedOrder(t, events, childStarted.LoopID, queuedIDs...)
	assertForeignloopInputCancelledCount(t, events, childStarted.LoopID, event.CancelTurnFailed, 2)
	process.assertCallCount(t, 1)
	// The child's terminal event does not order the parent's consumption of the
	// three hand-backs the provider failure releases.
	if got := parentLLM.waitCalls(t, ctx, 7); got != 7 {
		t.Fatalf("parent model calls = %d, want exactly 7: four scripted steps and one hand-back per backgrounded request", got)
	}
	assertForeignloopHandBacks(t, &handBacks, &handBacksMu, active.AgentID, 3)
	waitForeignloopTurnDoneCount(t, ctx, store, sess.SessionID(), parentID, 4)
}

func TestForeignloopSubagentQuota(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	process := newForeignloopProcess(t, foreignloopClaude, "ok", "")
	parentLLM := newForeignloopScenarioLLM(
		func(context.Context, inference.Request) ([]content.Chunk, error) {
			return foreignloopToolCall("quota-first", `{"agent_type":"child","instructions":"first","wait_for_response":true}`), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			if err := foreignloopExpectLastToolResult(request, "ok"); err != nil {
				return nil, err
			}
			return foreignloopToolCall("quota-second", `{"agent_type":"child","instructions":"second","wait_for_response":true}`), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			// Quota rejection publishes no LoopStarted (or quota-specific durable
			// event), so the model-facing tool result is the only place this case can
			// see it. Since harness v0.27.1 (5fbc312b) StartAgent no longer collapses
			// the refusal to a generic "error: agent failed": it returns a structured
			// error result carrying the typed SessionLoopQuotaExceeded text, so the
			// exact string below pins that it is the QUOTA that refused the second
			// start rather than any other failure. Harness's own session-runtime tests
			// assert the typed error before this integration boundary.
			const want = "StartAgent failed: session: loop spawn quota exceeded"
			if err := foreignloopExpectLastToolResult(request, want); err != nil {
				return nil, err
			}
			return foreignloopFinal("parent done"), nil
		},
	)
	parent := foreignloopDefinition(t, "planner", loop.EngineNative, parentLLM, "child")
	child := foreignloopDefinition(t, "child", loop.EngineForeignClaude, deterministicLLM{})
	sess, store := newForeignloopSessionWithOptions(
		t, ctx, process, "planner", []loop.Definition{parent, child},
		[]rig.Option{rig.WithDelegationLimits(rig.DelegationLimits{Quota: 1})},
	)
	sub := subscribeForeignloopEvents(t, sess)
	parentID := sess.ActiveLoop().ID()
	if _, err := sess.Submit(ctx, textBlock("go")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitForeignloopTurnDone(t, ctx, sub, parentID)

	events := eventsFor(t, ctx, store, sess.SessionID())
	children := foreignloopChildStarts(events, parentID)
	if len(children) != 1 {
		t.Fatalf("foreign child LoopStarted count = %d, want 1", len(children))
	}
	assertForeignloopTurnKinds(t, events, children[0].LoopID, []string{"TurnStarted", "StepDone", "TurnDone"})
	process.assertCallCount(t, 1)
}

func assertForeignloopTurnKinds(t *testing.T, events []event.Event, loopID uuid.UUID, want []string) {
	t.Helper()
	if got := foreignloopEventKinds(foreignloopTurnEvents(events, loopID)); !reflect.DeepEqual(got, want) {
		t.Fatalf("foreign child enduring sequence = %v, want %v", got, want)
	}
}

func assertForeignloopAcceptedOrder(t *testing.T, events []event.Event, loopID uuid.UUID, requestIDs ...string) {
	t.Helper()
	if err := foreignloopAcceptedOrderError(events, loopID, requestIDs...); err != nil {
		t.Fatal(err)
	}
}

func foreignloopAcceptedOrderError(events []event.Event, loopID uuid.UUID, requestIDs ...string) error {
	var got []string
	for _, value := range events {
		accepted, ok := value.(event.DelegateRequestAccepted)
		if ok && accepted.LoopID == loopID {
			if accepted.Cause.CommandID.IsZero() {
				return fmt.Errorf("DelegateRequestAccepted[%d] has a zero command id", len(got))
			}
			got = append(got, accepted.Cause.CommandID.String())
		}
	}
	if !reflect.DeepEqual(got, requestIDs) {
		return fmt.Errorf("DelegateRequestAccepted order = %v, want exact queued request IDs in FIFO order %v", got, requestIDs)
	}
	return nil
}

func foreignloopCancelledRequestIDs(t *testing.T, events []event.Event, loopID uuid.UUID, reason event.CancelReason, messages ...string) []string {
	t.Helper()
	var cancelled []event.InputCancelled
	for _, value := range events {
		input, ok := value.(event.InputCancelled)
		if ok && input.LoopID == loopID && input.Reason == reason {
			cancelled = append(cancelled, input)
		}
	}
	if len(cancelled) != len(messages) {
		t.Fatalf("InputCancelled for loop %s with reason %v = %d, want %d messages in FIFO order", loopID, reason, len(cancelled), len(messages))
	}
	ids := make([]string, len(cancelled))
	for index, input := range cancelled {
		gotMessage := foreignloopUserMessageText(input.Message)
		if gotMessage != messages[index] {
			t.Fatalf("InputCancelled[%d] message = %q, want queued message %q", index, gotMessage, messages[index])
		}
		if input.Cause.CommandID.IsZero() {
			t.Fatalf("InputCancelled[%d] has a zero command id", index)
		}
		ids[index] = input.Cause.CommandID.String()
	}
	return ids
}

func waitForeignloopInputCancelled(t *testing.T, ctx context.Context, store *sessionstore.Store, sessionID, loopID uuid.UUID, reason event.CancelReason, want int) []event.Event {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		events := eventsFor(t, ctx, store, sessionID)
		count := 0
		for _, value := range events {
			input, ok := value.(event.InputCancelled)
			if ok && input.LoopID == loopID && input.Reason == reason {
				count++
			}
		}
		if count >= want {
			return events
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for %d InputCancelled events for loop %s: %v", want, loopID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func foreignloopUserMessageText(message *content.UserMessage) string {
	if message == nil {
		return ""
	}
	var result strings.Builder
	for _, block := range message.Blocks {
		if text, ok := block.(*content.TextBlock); ok {
			result.WriteString(text.Text)
		}
	}
	return result.String()
}

func assertForeignloopInputCancelled(t *testing.T, events []event.Event, loopID uuid.UUID, want event.CancelReason) {
	t.Helper()
	var cancelled []event.InputCancelled
	for _, value := range events {
		input, ok := value.(event.InputCancelled)
		if ok && input.LoopID == loopID {
			cancelled = append(cancelled, input)
		}
	}
	if len(cancelled) != 1 || cancelled[0].Reason != want {
		t.Fatalf("InputCancelled for loop %s = %+v, want one cancellation with reason %v", loopID, cancelled, want)
	}
}

func assertForeignloopInputCancelledCount(t *testing.T, events []event.Event, loopID uuid.UUID, wantReason event.CancelReason, want int) {
	t.Helper()
	var cancelled []event.InputCancelled
	for _, value := range events {
		input, ok := value.(event.InputCancelled)
		if ok && input.LoopID == loopID && input.Reason == wantReason {
			cancelled = append(cancelled, input)
		}
	}
	if len(cancelled) != want {
		t.Fatalf("InputCancelled for loop %s with reason %v = %d, want %d", loopID, wantReason, len(cancelled), want)
	}
}

func foreignloopChildStarts(events []event.Event, parentID uuid.UUID) []event.LoopStarted {
	var result []event.LoopStarted
	for _, value := range events {
		started, ok := value.(event.LoopStarted)
		if ok && started.Cause.LoopID == parentID {
			result = append(result, started)
		}
	}
	return result
}
