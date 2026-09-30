//go:build integration

// This file is host v0.17.0's STOP-WITH-A-GATE-OPEN lane (harness v0.45.0).
//
// harness refuses a graceful release of a session waiting at a gate. Up to
// host v0.16.0 a Stop then left that runtime PARKED -- running, holding its
// journal lease -- until the process exited, so an embedder that stops a Host
// and restarts one in the same process could never restore the session.
// host v0.17.0 abandons the refused runtime crash-equivalently instead:
//
//	abandoned  Stop reports the session in DrainReport.Abandoned, the journal
//	           is untouched (the gate is preserved), the journal lease is given
//	           back, and a successor Host IN THE SAME PROCESS -- no process
//	           death anywhere -- restores the session with the gate
//	           answerable, and the answer reaches the waiting tool;
//	parked     when the abandon cannot give the journal lease back (harness
//	           v0.45.0 reports *session.LeaseReleaseError; Host maps it to
//	           residency-still-held), the session is Parked instead and the
//	           drain withholds `drained`: the report says draining.

package tests

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/host"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// gatedStopWorld opens a world whose agent raises a replay-safe ask_user gate
// on its second turn, creates a session on a first Host, and waits for the
// gate to reach Factory.
func gatedStopWorld(t *testing.T, s sessionwire.SessionID, first orchestrationtest.PooledHostConfig) (*orchestrationtest.PooledWorld, *orchestrationtest.PooledHost, *orchestrationtest.PooledFactory) {
	t.Helper()
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:        []sessionwire.TenantID{tenant},
		WithAskTool:    true,
		HarnessRuntime: true,
	})
	world.AskTool.Question = gateQuestion
	world.AskTool.ReplaySafe = true
	world.LLM.Script(
		orchestrationtest.PooledTurn{Text: "ready"},
		orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledAskToolName, ToolInput: `{}`},
		orchestrationtest.PooledTurn{Text: "thank you"},
	)
	first.Drain = &gatedDrainOptions
	owner := orchestrationtest.StartLifecycleHost(t, ctx, world, sessionwire.HostID(string(s)+"-first"), 1, first)
	orchestrationtest.AwaitAdvertised(t, world, owner.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, string(s)+"-replica", nil)

	createPooled(t, ctx, served, tenant, s, "stop-create", "hello")
	orchestrationtest.PooledWait(t, "the create applied", 90*time.Second, func() bool {
		return world.CommandState(ctx, tenant, s, "stop-create") == sessionstore.InboxStateApplied
	})
	runtimeID := world.RuntimeSessionID(t, ctx, tenant, s)
	orchestrationtest.PooledWait(t, "the create's turn finished", 60*time.Second, func() bool {
		return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID) >= 1
	})
	inputPooled(t, ctx, served, tenant, s, "stop-input", "please ask me")
	awaitResidentGate(t, served, tenant, s, "")
	return world, owner, served
}

// TestAStoppedHostAbandonsItsGatedSessionAndASameProcessSuccessorRestoresIt is
// the abandoned arm.
func TestAStoppedHostAbandonsItsGatedSessionAndASameProcessSuccessorRestoresIt(t *testing.T) {
	const s = sessionwire.SessionID("session-stop-abandon")
	tenant := orchestrationtest.PooledTenantA
	world, first, served := gatedStopWorld(t, s, orchestrationtest.PooledHostConfig{})
	ctx := t.Context()
	runtimeID := world.RuntimeSessionID(t, ctx, tenant, s)
	atTheGate := orchestrationtest.JournalTrace(t, world, tenant, runtimeID)

	t.Run("Stop reports the gated session Abandoned and gives its journal lease back", func(t *testing.T) {
		report, took := first.StopReport(t, gatedDrainOptions.Grace+20*time.Second)
		t.Logf("the stop took %v: %+v", took.Round(time.Millisecond), report)
		if report.State != sessionwire.HostLinkDrainStateDrained {
			t.Fatalf("the stop finished %q, want drained", report.State)
		}
		if len(report.Abandoned) != 1 || report.Abandoned[0] != (host.DrainSession{TenantID: tenant, SessionID: s}) {
			t.Fatalf("the stop reported abandoned %+v, want exactly the gated session", report.Abandoned)
		}
		if len(report.Parked) != 0 {
			t.Fatalf("the stop reported parked %+v; the abandon succeeded, so nothing may be parked", report.Parked)
		}
		if held, holder := world.JournalLeaseHeld(t, ctx, tenant, runtimeID); held {
			t.Fatalf("after the stop the abandoned runtime's journal lease is still held at epoch %d; a same-process successor could not restore it", holder)
		}
		if world.ResidencyHeld(t, ctx, tenant, s) {
			t.Fatal("after the stop the residency lease is still held")
		}
		// CRASH-EQUIVALENT: nothing was journaled, so the gate is preserved.
		if after := orchestrationtest.JournalTrace(t, world, tenant, runtimeID); strings.Join(after, " ") != strings.Join(atTheGate, " ") {
			t.Fatalf("the abandon wrote to the journal:\nbefore %v\nafter  %v", atTheGate, after)
		}
		if answers := world.AskTool.Answers(); len(answers) != 0 {
			t.Fatalf("the tool returned %v before any answer", answers)
		}
	})

	t.Run("a successor in the same process restores it with the gate answerable, and the answer reaches the tool", func(t *testing.T) {
		// NO PROCESS DIES in this case: the first Host was only stopped, its
		// goroutines are in this test binary, and nothing was Killed.
		successor := orchestrationtest.StartPooledHost(t, ctx, world, "session-stop-abandon-successor", 2)
		orchestrationtest.AwaitAdvertised(t, world, successor.ID)
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/restore", sessionwire.RestoreRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("stop-restore"), SessionID: s,
		})
		if status != http.StatusOK && status != http.StatusAccepted {
			t.Fatalf("the restore answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "the restore applied on the successor", 90*time.Second, func() bool {
			reg, found := world.Registration(t, ctx, tenant, s)
			return world.CommandState(ctx, tenant, s, "stop-restore") == sessionstore.InboxStateApplied &&
				found && reg.HostID == successor.ID && reg.Residency == sessionwire.SessionResidencyResident
		})
		if restores := successor.Rig.Restores(); len(restores) != 1 || restores[0].ID != runtimeID {
			t.Fatalf("the successor launched restores %+v, want one restore of %s", restores, runtimeID)
		}
		projected := awaitResidentGate(t, served, tenant, s, successor.ID)
		status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/gates/"+string(projected.GateID),
			sessionwire.GateResponseRequest{
				CommandEnvelope:        orchestrationtest.PooledEnvelope("stop-answer"),
				SessionID:              s,
				GateID:                 projected.GateID,
				Action:                 "answer",
				Values:                 map[string]json.RawMessage{"answer": json.RawMessage(`"` + gateAnswer + `"`)},
				ExpectedOpenJournalSeq: projected.OpenedJournalSeq,
			})
		if status != http.StatusAccepted {
			t.Fatalf("Factory answered the gate response %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "the answer settled applied", 60*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "stop-answer") == sessionstore.InboxStateApplied
		})
		orchestrationtest.PooledWait(t, "the waiting tool returned the user's answer", 60*time.Second, func() bool {
			answers := world.AskTool.Answers()
			return len(answers) == 1 && answers[0] == gateAnswer
		})
		orchestrationtest.PooledWait(t, "the resumed turn finished", 60*time.Second, func() bool {
			return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID) >= 2
		})
		for _, resolved := range orchestrationtest.JournalEvents[event.GateResolved](t, world, tenant, runtimeID) {
			if resolved.Reason == gate.CloseRestoreUnavailable {
				t.Fatalf("the restore closed the gate restore_unavailable: %+v", resolved)
			}
		}
		if got := turnsCarrying(t, world, tenant, runtimeID, "please ask me"); got != 1 {
			t.Fatalf("the gated input began %d turns, want exactly 1 (resumed, not re-run)", got)
		}
	})
}

// TestAStoppedHostWhoseLeaseReleaseFailsReportsParkedAndDraining is the
// parked arm: the abandon runs, but the journal lease cannot be given back.
func TestAStoppedHostWhoseLeaseReleaseFailsReportsParkedAndDraining(t *testing.T) {
	const s = sessionwire.SessionID("session-stop-parked")
	tenant := orchestrationtest.PooledTenantA
	world, first, _ := gatedStopWorld(t, s, orchestrationtest.PooledHostConfig{Mortal: true})
	ctx := t.Context()
	runtimeID := world.RuntimeSessionID(t, ctx, tenant, s)

	// The provider refuses the runtime's journal-lease hand-back: the release
	// never reaches it, so the grant stays held exactly as an outage leaves it.
	fault := first.Process().Fail("journal lease release", func(c orchestrationtest.ProcessCall) bool {
		return c.Plane == orchestrationtest.PlaneJournal && c.Op == "lease.release"
	})
	report, took := first.StopReport(t, gatedDrainOptions.Grace+30*time.Second)
	t.Logf("the stop took %v: %+v", took.Round(time.Millisecond), report)
	select {
	case <-fault.Fired():
	default:
		t.Fatal("the abandon never tried to release the journal lease; the fault could not be injected")
	}
	if len(report.Parked) != 1 || report.Parked[0] != (host.DrainSession{TenantID: tenant, SessionID: s}) {
		t.Fatalf("the stop reported parked %+v, want exactly the gated session", report.Parked)
	}
	if len(report.Abandoned) != 0 {
		t.Fatalf("the stop reported abandoned %+v although the lease was never given back", report.Abandoned)
	}
	if report.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("the stop finished %q, want draining: a residency still held must withhold drained", report.State)
	}
	if held, _ := world.JournalLeaseHeld(t, ctx, tenant, runtimeID); !held {
		t.Fatal("the parked runtime's journal lease is free on the store; the fault did not keep it held")
	}
}
