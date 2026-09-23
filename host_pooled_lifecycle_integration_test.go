//go:build integration

// This file is runbook 07 I2.1's POOLED LIFECYCLE lane against released
// modules: factory v0.7.1 placing onto composed host v0.7.0 processes whose
// runtimes are real harness v0.36.0 rigs over real harness journals, with
// sessionstore v0.13.0 as the durable plane.
//
// host v0.7.0 is the first Host whose composition warm-releases an idle pooled
// session, and it TAKES BACK a release a busy runtime refuses. Its own review
// could prove both only against Factory's observables, not against a Factory:
// host's import boundary forbids importing one. These two cases are that owed
// cross-module proof.

package tests

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/host"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// lifecycleWarmTTL is short enough that a case sees a release in seconds and
// long enough that a create's own turn is never mistaken for idleness.
const (
	lifecycleWarmTTL  = time.Second
	lifecycleWorkPoll = 50 * time.Millisecond
)

// TestAnIdlePooledSessionIsWarmReleasedAndReplacedOnItsNextCommand is I2.1
// case 3 through a real Factory: a session reaches durable idleness, waits out
// its warm TTL, and the Host releases its runtime and its residency lease
// WITHOUT stopping the session or deleting its history. The next command finds
// no live owner, Factory's PendingSweeper places it, and the Host RESTORES the
// same conversation rather than starting a new one.
//
// host v0.6.0 never released: its composition wired no work-state source, and
// a session stayed resident until the Host stopped. This is the case that goes
// red on that behaviour.
func TestAnIdlePooledSessionIsWarmReleasedAndReplacedOnItsNextCommand(t *testing.T) {
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	const s = sessionwire.SessionID("session-warm-release")
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{tenant},
	})
	pooled := orchestrationtest.StartLifecycleHost(t, ctx, world, "i21-warm-host", 4, orchestrationtest.PooledHostConfig{
		WarmTTL:  lifecycleWarmTTL,
		WorkPoll: lifecycleWorkPoll,
	})
	orchestrationtest.AwaitAdvertised(t, world, pooled.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "i21-warm-replica", nil)

	status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("warm-create"),
		SessionID:       s,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(`[{"type":"text","text":"the remembered word is PERSIMMON"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "the create applied", 90*time.Second, func() bool {
		return world.CommandState(ctx, tenant, s, "warm-create") == sessionstore.InboxStateApplied
	})
	runtimeID := world.RuntimeSessionID(t, ctx, tenant, s)
	// Settled is not finished: harness writes the disposition frame BEFORE the
	// effect, so the create's turn may still be running here.
	orchestrationtest.PooledWait(t, "the create's turn finished", 60*time.Second, func() bool {
		return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID) >= 1
	})
	attached, found := world.Registration(t, ctx, tenant, s)
	if !found || attached.HostID != pooled.ID || attached.Residency != sessionwire.SessionResidencyResident {
		t.Fatalf("after the create the durable owner is %+v (found=%v), want resident on %s", attached, found, pooled.ID)
	}
	turnsBefore := orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID)

	t.Run("the idle session is released after its warm TTL, and nothing is stopped or deleted", func(t *testing.T) {
		took := orchestrationtest.PooledWait(t, "the Host released the idle session", 30*time.Second, func() bool {
			return pooled.SessionsIn(t, "resident") == 0 && pooled.SessionsIn(t, "releasing") == 0
		})
		t.Logf("released %v after the create's turn finished (warm TTL %v)", took.Round(time.Millisecond), lifecycleWarmTTL)

		// THE LEASE: another party can take it, which is the moment a
		// successor's attach can succeed.
		if world.ResidencyHeld(t, ctx, tenant, s) {
			t.Fatal("the Host reports no resident session but the residency lease is still held")
		}
		// THE RUNTIME: the harness session the Host launched has ended.
		if ended, found := pooled.Rig.RuntimeEnded(tenant, s); !found || !ended {
			t.Fatalf("the released session's runtime is still running (found=%v ended=%v)", found, ended)
		}
		// THE ROUTE: Factory's owner check must no longer see a live owner, or
		// the next command would be woken at a runtime that is gone.
		if owner, found := world.Registration(t, ctx, tenant, s); found &&
			owner.Residency == sessionwire.SessionResidencyResident && owner.Accepting {
			t.Fatalf("after the release the durable registration is still a live owner: %+v", owner)
		}
		// NONTERMINAL: no SessionStopped, and the history is all still there.
		if got := orchestrationtest.CountJournalEvents[event.SessionStopped](t, world, tenant, runtimeID); got != 0 {
			t.Fatalf("the warm release appended %d SessionStopped; a warm release is nonterminal", got)
		}
		if got := orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID); got != turnsBefore {
			t.Fatalf("the journal holds %d TurnDone after the release, %d before: history changed", got, turnsBefore)
		}
		if _, err := world.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: s}); err != nil {
			t.Fatalf("the session's catalog entry is unreadable after the release: %v", err)
		}
		if state := world.CommandState(ctx, tenant, s, "warm-create"); state != sessionstore.InboxStateApplied {
			t.Fatalf("the create's durable command state is %q after the release, want applied", state)
		}
	})

	t.Run("the next command is placed by Factory's sweeper and the session is restored", func(t *testing.T) {
		requestsBefore := len(world.LLM.Requests())
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("warm-input"),
			SessionID:       s,
			Blocks:          json.RawMessage(`[{"type":"text","text":"what was the remembered word?"}]`),
		})
		if status != http.StatusOK {
			t.Fatalf("the input after the release answered %d: %s", status, body)
		}
		took := orchestrationtest.PooledWait(t, "the input after the release applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "warm-input") == sessionstore.InboxStateApplied
		})
		creates, restores := pooled.Rig.Creates(), pooled.Rig.Restores()
		t.Logf("applied after %v; creates=%+v restores=%+v", took.Round(time.Millisecond), creates, restores)
		if len(creates) != 1 {
			t.Fatalf("the Host created %d runtimes, want only the first: a re-placed session must be restored", len(creates))
		}
		if len(restores) != 1 || restores[0].ID != runtimeID {
			t.Fatalf("the Host restored %+v, want exactly one restore of %s", restores, runtimeID)
		}
		// The route turns resident a beat after the attach; the input can
		// settle inside that beat, so it is waited for.
		orchestrationtest.PooledWait(t, "the re-placed route turned resident", 30*time.Second, func() bool {
			owner, found := world.Registration(t, ctx, tenant, s)
			return found && owner.Residency == sessionwire.SessionResidencyResident
		})
		owner, found := world.Registration(t, ctx, tenant, s)
		if !found || owner.Residency != sessionwire.SessionResidencyResident || owner.LeaseEpoch <= attached.LeaseEpoch {
			t.Fatalf("after re-placement the durable owner is %+v, want resident at an epoch above %d", owner, attached.LeaseEpoch)
		}
		orchestrationtest.PooledWait(t, "the restored turn finished", 60*time.Second, func() bool {
			return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID) > turnsBefore
		})
		if got := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world, tenant, runtimeID); got != 1 {
			t.Fatalf("the journal holds %d SessionStarted, want 1: the session was restarted", got)
		}
		if got := orchestrationtest.CountJournalEvents[event.RestoreDone](t, world, tenant, runtimeID); got < 1 {
			t.Fatal("the journal holds no RestoreDone")
		}
		if !world.LLM.SawInRequest(requestsBefore, "PERSIMMON") {
			t.Fatal("no model request after the re-placement carried PERSIMMON; the conversation did not survive the release")
		}
	})
}

// TestARefusedWarmReleaseIsTakenBackAndAFactoryAdmittedGateAnswerIsApplied is
// the cross-module proof host v0.7.0's review owed to this repository.
//
// Inside the release window -- after the Host has halted its consumer and
// stopped admitting, while it checkpoints -- the product starts a turn on its
// own and the agent asks the user a question. The runtime is now parked at a
// gate and refuses to release within the drain grace, so the Host TAKES THE
// RELEASE BACK: the durable registration returns to resident and accepting
// under the same generation and epoch. Then a REAL Factory:
//
//   - reads the gate through its own routed read;
//   - admits the answer, which it does only when its owner check sees a
//     resident, accepting owner AND the owner's connect reply advertises
//     Core's gate_response token -- so a 202 is those two checks passing;
//   - wakes the Host over HostLink. The Host's own reconcile is a minute, so
//     the answer can be applied inside this case only because the wake reached
//     a consumer that is running again.
//
// And nothing churns: a second capable Host is up the whole time and is never
// handed the session, and the owner is never re-attached.
func TestARefusedWarmReleaseIsTakenBackAndAFactoryAdmittedGateAnswerIsApplied(t *testing.T) {
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	const s = sessionwire.SessionID("session-take-back")
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:     []sessionwire.TenantID{tenant},
		WithAskTool: true,
	})
	world.AskTool.Question = gateQuestion
	world.LLM.Script(
		orchestrationtest.PooledTurn{Text: "ready"},
		orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledAskToolName, ToolInput: `{}`},
		orchestrationtest.PooledTurn{Text: "thank you"},
	)

	pausing := orchestrationtest.NewPausingCheckpointer()
	owner := orchestrationtest.StartLifecycleHost(t, ctx, world, "i21-takeback-host", 4, orchestrationtest.PooledHostConfig{
		WarmTTL:           lifecycleWarmTTL,
		WorkPoll:          lifecycleWorkPoll,
		ReconcileInterval: time.Minute,
		Drain:             &host.DrainOptions{Grace: time.Second, IdleBoundary: 500 * time.Millisecond, PublishBound: 500 * time.Millisecond},
		WrapCheckpointer:  pausing.Wrap,
	})
	t.Cleanup(pausing.Proceed)
	orchestrationtest.AwaitAdvertised(t, world, owner.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "i21-takeback-replica", nil)

	status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("takeback-create"),
		SessionID:       s,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(`[{"type":"text","text":"hello"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "the create applied", 90*time.Second, func() bool {
		return world.CommandState(ctx, tenant, s, "takeback-create") == sessionstore.InboxStateApplied
	})
	attached, found := world.Registration(t, ctx, tenant, s)
	if !found || attached.Residency != sessionwire.SessionResidencyResident {
		t.Fatalf("after the create the durable owner is %+v (found=%v), want resident", attached, found)
	}
	if attached.HostID != owner.ID {
		t.Fatalf("the session is owned by %s, want the only Host %s", attached.HostID, owner.ID)
	}
	// The bystander comes up only now, so the create could not have gone to it,
	// and from here on a second capable Host is there for Factory to churn to.
	bystander := orchestrationtest.StartPooledHost(t, ctx, world, "i21-takeback-bystander", 9)
	orchestrationtest.AwaitAdvertised(t, world, bystander.ID)
	runtimeID := world.RuntimeSessionID(t, ctx, tenant, s)

	var projected sessionwire.GateProjection
	t.Run("inside the release window the agent parks at a gate", func(t *testing.T) {
		pausing.AwaitEntered(t, 30*time.Second)
		mid, _ := world.Registration(t, ctx, tenant, s)
		if mid.Residency != sessionwire.SessionResidencyReleasing || mid.Accepting {
			t.Fatalf("mid-release the durable owner is %s/accepting=%v, want releasing and not accepting", mid.Residency, mid.Accepting)
		}
		owner.Rig.Submit(t, ctx, tenant, s, "please ask me")
		orchestrationtest.PooledWait(t, "the gate reached Factory's gates read", 60*time.Second, func() bool {
			page := served.OpenGates(t, ctx, tenant, s)
			if len(page.Gates) == 0 {
				return false
			}
			projected = page.Gates[0]
			return true
		})
		pausing.Proceed()
	})

	t.Run("the refused release is taken back to the same owner", func(t *testing.T) {
		orchestrationtest.PooledWait(t, "the release was taken back", 30*time.Second, func() bool {
			back, found := world.Registration(t, ctx, tenant, s)
			return found && owner.ReleaseFailures(t) == 1 &&
				back.Residency == sessionwire.SessionResidencyResident && back.Accepting
		})
		back, _ := world.Registration(t, ctx, tenant, s)
		if back.HostID != owner.ID || back.HostGeneration != attached.HostGeneration || back.LeaseEpoch != attached.LeaseEpoch {
			t.Fatalf("the taken-back owner is %+v, want %s generation %d epoch %d", back, owner.ID, attached.HostGeneration, attached.LeaseEpoch)
		}
		if got := owner.SessionsIn(t, "releasing"); got != 0 {
			t.Fatalf("host_sessions{state=\"releasing\"} = %d after the take-back", got)
		}
		if !world.ResidencyHeld(t, ctx, tenant, s) {
			t.Fatal("the residency lease was handed back while the runtime was parked at a gate")
		}
		if got := orchestrationtest.CountJournalEvents[event.SessionStopped](t, world, tenant, runtimeID); got != 0 {
			t.Fatalf("%d SessionStopped; a taken-back release terminates nothing", got)
		}
	})

	t.Run("Factory admits the answer and the Host applies it", func(t *testing.T) {
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/gates/"+string(projected.GateID),
			sessionwire.GateResponseRequest{
				CommandEnvelope:        orchestrationtest.PooledEnvelope("takeback-answer"),
				SessionID:              s,
				GateID:                 projected.GateID,
				Action:                 "answer",
				Values:                 map[string]json.RawMessage{"answer": json.RawMessage(`"` + gateAnswer + `"`)},
				ExpectedOpenJournalSeq: projected.OpenedJournalSeq,
			})
		if status != http.StatusAccepted {
			t.Fatalf("Factory answered the gate response %d: %s; its owner check or capability gate refused the taken-back owner", status, body)
		}
		orchestrationtest.PooledWait(t, "the answer settled", 30*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "takeback-answer") == sessionstore.InboxStateApplied
		})
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: s, CommandID: "takeback-answer"})
		if err != nil {
			t.Fatalf("reading the settled answer: %v", err)
		}
		if entry.Record.Outcome == nil || entry.Record.Outcome.Kind != sessionstore.DispositionApplied {
			t.Fatalf("the answer settled with outcome %+v, want applied", entry.Record.Outcome)
		}
		if entry.Record.Attempt == nil || uint64(entry.Record.Attempt.ResidencyEpoch) != attached.LeaseEpoch {
			t.Fatalf("the answer's attempt is %+v, want one authorized under the original residency %d", entry.Record.Attempt, attached.LeaseEpoch)
		}
		orchestrationtest.PooledWait(t, "the tool returned the user's answer", 30*time.Second, func() bool {
			answers := world.AskTool.Answers()
			return len(answers) == 1 && answers[0] == gateAnswer
		})
		orchestrationtest.PooledWait(t, "the agent's next turn carried the answer", 30*time.Second, func() bool {
			return world.LLM.SawInRequest(0, gateAnswer)
		})
	})

	t.Run("no placement churn", func(t *testing.T) {
		if creates, restores := bystander.Rig.Creates(), bystander.Rig.Restores(); len(creates)+len(restores) != 0 {
			t.Fatalf("the bystander Host launched creates=%+v restores=%+v; the session churned", creates, restores)
		}
		if creates, restores := owner.Rig.Creates(), owner.Rig.Restores(); len(creates) != 1 || len(restores) != 0 {
			t.Fatalf("the owner launched creates=%+v restores=%+v, want the one create and no re-attach", creates, restores)
		}
		// And the watch went on: idle again, the session is released for real.
		orchestrationtest.PooledWait(t, "the answered session was released once idle", 30*time.Second, func() bool {
			return owner.SessionsIn(t, "resident") == 0 && owner.SessionsIn(t, "releasing") == 0
		})
		if creates, restores := bystander.Rig.Creates(), bystander.Rig.Restores(); len(creates)+len(restores) != 0 {
			t.Fatalf("after the final release the bystander launched creates=%+v restores=%+v", creates, restores)
		}
	})
}
