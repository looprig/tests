//go:build integration

package tests

// harness v0.39.0 + host v0.9.0: an OPEN ask_user GATE SURVIVES FAILOVER when
// the tool asking declares tool.UserInputReplaySafe. Before, a restore closed
// every ask_user gate restore_unavailable and the answer settled no_op and was
// dropped -- which the I2.3 drain case still asserts, deliberately, for the
// kit's default (NOT replay-safe) tool, since harness keeps the old closure
// for such a tool.
//
// This case drives the replay-safe half across TWO failovers:
//
//  1. the owner CRASHES with the gate open; a successor restores the session
//     and the gate is still open and answerable there;
//  2. that successor is DRAINED with the restored gate open. A restored gated
//     runtime is not idle, so the drain is crash-equivalent (host v0.9.0) and
//     the Host must exit after it;
//  3. a third Host restores again, and the user's answer -- admitted only now
//     -- settles applied and reaches the waiting tool, and the turn continues.

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

func TestAReplaySafeAskUserGateSurvivesACrashAndADrainAndTheAnswerReachesTheTool(t *testing.T) {
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	const s = sessionwire.SessionID("session-gate-failover")
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:     []sessionwire.TenantID{tenant},
		WithAskTool: true,
	})
	world.AskTool.Question = gateQuestion
	world.AskTool.ReplaySafe = true
	world.LLM.Script(
		orchestrationtest.PooledTurn{Text: "ready"},
		orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledAskToolName, ToolInput: `{}`},
		orchestrationtest.PooledTurn{Text: "thank you"},
	)
	mortal := func(id sessionwire.HostID, generation uint64) *orchestrationtest.PooledHost {
		h := orchestrationtest.StartLifecycleHost(t, ctx, world, id, generation, orchestrationtest.PooledHostConfig{
			Drain: &gatedDrainOptions, Mortal: true,
		})
		orchestrationtest.AwaitAdvertised(t, world, h.ID)
		return h
	}
	first := mortal("gate-failover-first", 1)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "gate-failover-replica", nil)

	createPooled(t, ctx, served, tenant, s, "failover-create", "hello")
	orchestrationtest.PooledWait(t, "the create applied", 90*time.Second, func() bool {
		return world.CommandState(ctx, tenant, s, "failover-create") == sessionstore.InboxStateApplied
	})
	runtimeID := world.RuntimeSessionID(t, ctx, tenant, s)
	orchestrationtest.PooledWait(t, "the create's turn finished", 60*time.Second, func() bool {
		return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID) >= 1
	})
	inputPooled(t, ctx, served, tenant, s, "failover-input", "please ask me")
	opened := awaitResidentGate(t, served, tenant, s, "")

	// restoreOn re-places the session with an explicit restore and waits until
	// the gates read offers the gate as answerable on the new owner.
	restoreOn := func(owner *orchestrationtest.PooledHost, command sessionwire.CommandID) sessionwire.GateProjection {
		t.Helper()
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/restore", sessionwire.RestoreRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)), SessionID: s,
		})
		if status != http.StatusOK && status != http.StatusAccepted {
			t.Fatalf("the restore answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, string(command)+" applied on "+string(owner.ID), 90*time.Second, func() bool {
			reg, found := world.Registration(t, ctx, tenant, s)
			return world.CommandState(ctx, tenant, s, command) == sessionstore.InboxStateApplied &&
				found && reg.HostID == owner.ID && reg.Residency == sessionwire.SessionResidencyResident
		})
		if restores := owner.Rig.Restores(); len(restores) != 1 || restores[0].ID != runtimeID {
			t.Fatalf("%s launched restores %+v, want one restore of %s", owner.ID, restores, runtimeID)
		}
		return awaitResidentGate(t, served, tenant, s, owner.ID)
	}
	noRestoreClosure := func(when string) {
		t.Helper()
		for _, resolved := range orchestrationtest.JournalEvents[event.GateResolved](t, world, tenant, runtimeID) {
			if resolved.Reason == gate.CloseRestoreUnavailable {
				t.Fatalf("%s the journal closed the gate restore_unavailable: %+v; a replay-safe ask_user gate must survive", when, resolved)
			}
		}
		if got := orchestrationtest.CountJournalEvents[event.TurnInterrupted](t, world, tenant, runtimeID); got != 0 {
			t.Fatalf("%s the journal holds %d TurnInterrupted; the parked turn must resume", when, got)
		}
	}

	var second *orchestrationtest.PooledHost
	t.Run("the owner crashes with the gate open and a successor restores it open", func(t *testing.T) {
		first.Kill(t)
		second = mortal("gate-failover-second", 1)
		reopened := restoreOn(second, "failover-restore-1")
		if reopened.GateID != opened.GateID {
			t.Logf("the restored gate is %s (was %s)", reopened.GateID, opened.GateID)
		}
		noRestoreClosure("after the first restore")
		if answers := world.AskTool.Answers(); len(answers) != 0 {
			t.Fatalf("the tool already returned %v before any answer", answers)
		}
	})

	t.Run("draining the Host that restored the gated session is crash-equivalent", func(t *testing.T) {
		report, took := second.StopReport(t, gatedDrainOptions.Grace+20*time.Second)
		t.Logf("the drain took %v: %+v", took.Round(time.Millisecond), report)
		steps := map[string]bool{}
		for _, failure := range report.Failures {
			steps[failure.Step] = true
		}
		// The restored runtime is parked at the resumed gate, so it never
		// goes idle and refuses a nonterminal release: the same forced path
		// as a Host that raised the gate itself.
		if !steps["wait_idle"] || !steps["release_residency"] {
			t.Fatalf("the drain recorded %+v, want the forced wait_idle and release_residency of a gated session", report.Failures)
		}
		if world.ResidencyHeld(t, ctx, tenant, s) {
			t.Fatal("after the drain the residency lease is still held")
		}
		if held, _ := world.JournalLeaseHeld(t, ctx, tenant, runtimeID); !held {
			t.Fatal("after the crash-equivalent drain the parked runtime's journal lease is free; it is held until the process exits")
		}
		// host's obligation: a Host that drained with a gate open MUST exit.
		second.Kill(t)
		noRestoreClosure("after the drain")
	})

	t.Run("a third Host restores it, and the answer settles applied and reaches the tool", func(t *testing.T) {
		third := mortal("gate-failover-third", 1)
		projected := restoreOn(third, "failover-restore-2")
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/gates/"+string(projected.GateID),
			sessionwire.GateResponseRequest{
				CommandEnvelope:        orchestrationtest.PooledEnvelope("failover-answer"),
				SessionID:              s,
				GateID:                 projected.GateID,
				Action:                 "answer",
				Values:                 map[string]json.RawMessage{"answer": json.RawMessage(`"` + gateAnswer + `"`)},
				ExpectedOpenJournalSeq: projected.OpenedJournalSeq,
			})
		if status != http.StatusAccepted {
			t.Fatalf("Factory answered the gate response %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "the answer settled", 60*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "failover-answer") == sessionstore.InboxStateApplied
		})
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: s, CommandID: "failover-answer"})
		if err != nil {
			t.Fatalf("reading the settled answer: %v", err)
		}
		if entry.Record.Outcome == nil || entry.Record.Outcome.Kind != sessionstore.DispositionApplied {
			t.Fatalf("the answer settled with outcome %+v, want applied (a dropped answer settles no_op)", entry.Record.Outcome)
		}
		orchestrationtest.PooledWait(t, "the waiting tool returned the user's answer", 60*time.Second, func() bool {
			answers := world.AskTool.Answers()
			return len(answers) == 1 && answers[0] == gateAnswer
		})
		orchestrationtest.PooledWait(t, "the resumed turn finished with the answer", 60*time.Second, func() bool {
			return world.LLM.SawInRequest(0, gateAnswer) &&
				orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID) >= 2
		})
		noRestoreClosure("after the answer")
		if got := turnsCarrying(t, world, tenant, runtimeID, "please ask me"); got != 1 {
			t.Fatalf("the gated input began %d turns across two failovers, want exactly 1 (resumed, not re-run)", got)
		}
	})
}

// awaitResidentGate waits for Factory's gates read to offer exactly one gate as
// resident-answerable, on owner when one is named, and returns it.
func awaitResidentGate(t *testing.T, served *orchestrationtest.PooledFactory, tenant sessionwire.TenantID, s sessionwire.SessionID, owner sessionwire.HostID) sessionwire.GateProjection {
	t.Helper()
	var projected sessionwire.GateProjection
	what := "the gate is resident-answerable"
	if owner != "" {
		what += " on " + string(owner)
	}
	orchestrationtest.PooledWait(t, what, 60*time.Second, func() bool {
		page := served.OpenGates(t, t.Context(), tenant, s)
		if len(page.Gates) != 1 || page.Gates[0].Answerability != sessionwire.GateAnswerabilityResident {
			return false
		}
		projected = page.Gates[0]
		return true
	})
	return projected
}
