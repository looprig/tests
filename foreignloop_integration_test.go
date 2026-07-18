//go:build integration && (darwin || (linux && !android))

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
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
		[]content.Chunk{foreignloopToolUse(toolUseID, "Subagent", `{"action":"start","agent":"builder","message":"hi","wait":true}`)},
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
	var active, queued foreignloopQueuedResult
	parentLLM := newForeignloopScenarioLLM(
		func(context.Context, inference.Request) ([]content.Chunk, error) {
			return foreignloopToolCall("interrupt-start", `{"action":"start","agent":"child","message":"A","wait":false}`), nil
		},
		func(stepCtx context.Context, request inference.Request) ([]content.Chunk, error) {
			var err error
			active, err = foreignloopLastQueuedResult(request)
			if err != nil {
				return nil, err
			}
			if err := process.waitStarted(stepCtx); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"send","delegate_id":%q,"message":"B","wait":false}`, active.DelegateID)
			return foreignloopToolCall("interrupt-send", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			var err error
			queued, err = foreignloopLastQueuedResult(request)
			if err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"interrupt","delegate_id":%q}`, active.DelegateID)
			return foreignloopToolCall("interrupt-child", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			want := fmt.Sprintf(`{"delegate_id":%q,"status":"interrupted"}`, active.DelegateID)
			if err := foreignloopExpectLastToolResult(request, want); err != nil {
				return nil, err
			}
			if err := process.release(); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"wait","delegate_id":%q,"request_id":%q}`, active.DelegateID, queued.RequestID)
			return foreignloopToolCall("interrupt-wait", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			if err := foreignloopExpectLastToolResult(request, "error: delegate interrupted"); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"wait","delegate_id":%q,"request_id":%q}`, active.DelegateID, active.RequestID)
			return foreignloopToolCall("interrupt-wait-active", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			if err := foreignloopExpectLastToolResult(request, "error: delegate interrupted"); err != nil {
				return nil, err
			}
			return foreignloopFinal("parent done"), nil
		},
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
	assertForeignloopTurnKinds(t, events, childStarted.LoopID, []string{"TurnStarted", "TurnInterrupted"})
	process.assertCallCount(t, 1)
	if parentLLM.callCount() != 6 {
		t.Fatalf("parent model calls = %d, want 6", parentLLM.callCount())
	}
}

func TestForeignloopQueuedDelegateTimeout(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	process := newControlledForeignloopProcess(t, foreignloopClaude, "unused", "", foreignloopProcessBlock)
	var active, queued foreignloopQueuedResult
	parentLLM := newForeignloopScenarioLLM(
		func(context.Context, inference.Request) ([]content.Chunk, error) {
			return foreignloopToolCall("timeout-start", `{"action":"start","agent":"child","message":"A","wait":false}`), nil
		},
		func(stepCtx context.Context, request inference.Request) ([]content.Chunk, error) {
			var err error
			active, err = foreignloopLastQueuedResult(request)
			if err != nil {
				return nil, err
			}
			if err := process.waitStarted(stepCtx); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"send","delegate_id":%q,"message":"B","wait":false}`, active.DelegateID)
			return foreignloopToolCall("timeout-send", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			var err error
			queued, err = foreignloopLastQueuedResult(request)
			if err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"wait","delegate_id":%q,"request_id":%q,"timeout_seconds":0}`, active.DelegateID, queued.RequestID)
			return foreignloopToolCall("timeout-target", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			if err := foreignloopExpectLastToolResult(request, "error: delegate timed out"); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"wait","delegate_id":%q,"request_id":%q}`, active.DelegateID, queued.RequestID)
			return foreignloopToolCall("timeout-confirm-target", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			if err := foreignloopExpectLastToolResult(request, "error: delegate interrupted"); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"status","delegate_id":%q}`, active.DelegateID)
			return foreignloopToolCall("timeout-status", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			text, err := foreignloopLastToolResult(request)
			if err != nil {
				return nil, err
			}
			var status struct {
				DelegateID string `json:"delegate_id"`
				Status     string `json:"status"`
			}
			if err := json.Unmarshal([]byte(text), &status); err != nil {
				return nil, fmt.Errorf("decode status result %q: %w", text, err)
			}
			if status.DelegateID != active.DelegateID || status.Status != "running" {
				return nil, fmt.Errorf("status after targeted timeout = %+v, want active delegate running", status)
			}
			input := fmt.Sprintf(`{"action":"interrupt","delegate_id":%q}`, active.DelegateID)
			return foreignloopToolCall("timeout-cleanup", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			want := fmt.Sprintf(`{"delegate_id":%q,"status":"interrupted"}`, active.DelegateID)
			if err := foreignloopExpectLastToolResult(request, want); err != nil {
				return nil, err
			}
			if err := process.release(); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"wait","delegate_id":%q,"request_id":%q}`, active.DelegateID, active.RequestID)
			return foreignloopToolCall("timeout-wait-active", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			if err := foreignloopExpectLastToolResult(request, "error: delegate interrupted"); err != nil {
				return nil, err
			}
			return foreignloopFinal("parent done"), nil
		},
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
	assertForeignloopTurnKinds(t, events, childStarted.LoopID, []string{"TurnStarted", "TurnInterrupted"})
	process.assertCallCount(t, 1)
}

func TestForeignloopProviderFailureWithQueuedDelegates(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	process := newControlledForeignloopProcess(t, foreignloopClaude, "unused", "", foreignloopProcessFailAfterRelease)
	var active, queuedB, queuedC foreignloopQueuedResult
	parentLLM := newForeignloopScenarioLLM(
		func(context.Context, inference.Request) ([]content.Chunk, error) {
			return foreignloopToolCall("failure-start", `{"action":"start","agent":"child","message":"A","wait":false}`), nil
		},
		func(stepCtx context.Context, request inference.Request) ([]content.Chunk, error) {
			var err error
			active, err = foreignloopLastQueuedResult(request)
			if err != nil {
				return nil, err
			}
			if err := process.waitStarted(stepCtx); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"send","delegate_id":%q,"message":"B","wait":false}`, active.DelegateID)
			return foreignloopToolCall("failure-send-b", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			var err error
			queuedB, err = foreignloopLastQueuedResult(request)
			if err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"send","delegate_id":%q,"message":"C","wait":false}`, active.DelegateID)
			return foreignloopToolCall("failure-send-c", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			var err error
			queuedC, err = foreignloopLastQueuedResult(request)
			if err != nil {
				return nil, err
			}
			if err := process.release(); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"wait","delegate_id":%q,"request_id":%q}`, active.DelegateID, queuedB.RequestID)
			return foreignloopToolCall("failure-wait-b", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			if err := foreignloopExpectLastToolResult(request, "error: delegate failed"); err != nil {
				return nil, err
			}
			input := fmt.Sprintf(`{"action":"wait","delegate_id":%q,"request_id":%q}`, active.DelegateID, queuedC.RequestID)
			return foreignloopToolCall("failure-wait-c", input), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			if err := foreignloopExpectLastToolResult(request, "error: delegate failed"); err != nil {
				return nil, err
			}
			return foreignloopFinal("parent done"), nil
		},
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
	assertForeignloopTurnKinds(t, events, childStarted.LoopID, []string{"TurnStarted", "TurnFailed"})
	assertForeignloopAcceptedOrder(t, events, childStarted.LoopID, queuedB.RequestID, queuedC.RequestID)
	process.assertCallCount(t, 1)
}

func TestForeignloopSubagentQuota(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	process := newForeignloopProcess(t, foreignloopClaude, "ok", "")
	parentLLM := newForeignloopScenarioLLM(
		func(context.Context, inference.Request) ([]content.Chunk, error) {
			return foreignloopToolCall("quota-first", `{"action":"start","agent":"child","message":"first","wait":true}`), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			if err := foreignloopExpectLastToolResult(request, "ok"); err != nil {
				return nil, err
			}
			return foreignloopToolCall("quota-second", `{"action":"start","agent":"child","message":"second","wait":true}`), nil
		},
		func(_ context.Context, request inference.Request) ([]content.Chunk, error) {
			const want = "error: subagent failed: session: loop spawn quota exceeded"
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
	var got []string
	for _, value := range events {
		accepted, ok := value.(event.DelegateRequestAccepted)
		if ok && accepted.LoopID == loopID {
			got = append(got, accepted.Cause.CommandID.String())
		}
	}
	positions := make([]int, len(requestIDs))
	for index := range positions {
		positions[index] = -1
	}
	for index, id := range got {
		for requestIndex, requestID := range requestIDs {
			if id == requestID {
				positions[requestIndex] = index
			}
		}
	}
	for index, position := range positions {
		if position < 0 {
			t.Fatalf("DelegateRequestAccepted order = %v, missing queued request %q", got, requestIDs[index])
		}
		if index > 0 && positions[index-1] >= position {
			t.Fatalf("DelegateRequestAccepted order = %v, want FIFO requests %v", got, requestIDs)
		}
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
