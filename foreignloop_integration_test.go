//go:build integration && (darwin || (linux && !android))

package tests

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/loop"
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
