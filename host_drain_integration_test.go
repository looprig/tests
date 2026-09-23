//go:build integration

// This file is runbook 07 I2.3, DRAIN POOLED HOSTS SAFELY, against released
// modules: factory v0.8.0 placing onto composed host v0.7.1 processes whose
// runtimes are real harness v0.37.1 rigs over real harness journals, with
// sessionstore v0.13.0 as the durable plane.
//
// # The drain caller
//
// A pooled Host's only drain caller is its own process lifecycle (host.Run's
// SIGTERM path, which is Service.Stop under the platform grace); see the kit's
// drain.go for why neither Factory, the controller nor HostLink can be one.
//
// # What vouches for what
//
// Nothing here is asserted from the kit's own bookkeeping. Every claim is read
// off something a released module wrote or served: the SessionStore target
// directory and registrations (what Factory's placement and owner checks read),
// the residency lease (probed by taking it as another party would), the harness
// journal (TurnStarted/TurnDone/SessionStarted/SessionStopped/RestoreDone), the
// model's own requests (what the agent actually saw), the Host's DrainReport and
// its /metrics. The kit's HoldingCheckpointer is a PAUSE, not a witness: it holds
// the drain at a step so the case can read the durable plane at that instant.

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/host"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// drainOptions is the drain policy the ordering case runs under: long enough
// that a held model turn finishes inside the idle boundary once released.
var drainOptions = host.DrainOptions{Grace: 20 * time.Second, IdleBoundary: 15 * time.Second, PublishBound: 2 * time.Second}

// turnsCarrying counts the harness TurnStarted events whose user message
// carries needle: the durable record of how many times the runtime began a
// turn on one input. One is "not lost and not duplicated".
func turnsCarrying(t *testing.T, world *orchestrationtest.PooledWorld, tenant sessionwire.TenantID, id uuid.UUID, needle string) int {
	t.Helper()
	count := 0
	for _, started := range orchestrationtest.JournalEvents[event.TurnStarted](t, world, tenant, id) {
		if started.Message == nil {
			continue
		}
		encoded, err := json.Marshal(started.Message.Blocks)
		if err != nil {
			t.Fatalf("encoding a TurnStarted message: %v", err)
		}
		if strings.Contains(string(encoded), needle) {
			count++
		}
	}
	return count
}

func createPooled(t *testing.T, ctx context.Context, served *orchestrationtest.PooledFactory, tenant sessionwire.TenantID, s sessionwire.SessionID, command, text string) {
	t.Helper()
	status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(command),
		SessionID:       s,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(`[{"type":"text","text":"` + text + `"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("the create of %s answered %d: %s", s, status, body)
	}
}

func inputPooled(t *testing.T, ctx context.Context, served *orchestrationtest.PooledFactory, tenant sessionwire.TenantID, s sessionwire.SessionID, command, text string) {
	t.Helper()
	status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(command),
		SessionID:       s,
		Blocks:          json.RawMessage(`[{"type":"text","text":"` + text + `"}]`),
	})
	if status != http.StatusOK {
		t.Fatalf("the input %s to %s answered %d: %s", command, s, status, body)
	}
}

// TestDrainingAPooledHostStopsAdmissionFirstAndHandsItsSessionsToASuccessor is
// I2.3 cases 1 and 2 on a pooled Host holding an IDLE session and a MID-TURN
// session, with a second capable Host up.
func TestDrainingAPooledHostStopsAdmissionFirstAndHandsItsSessionsToASuccessor(t *testing.T) {
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	const (
		idle = sessionwire.SessionID("session-drain-idle")
		busy = sessionwire.SessionID("session-drain-busy")
		late = sessionwire.SessionID("session-drain-late")
	)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{tenant},
	})
	holding := orchestrationtest.NewHoldingCheckpointer()
	drainee := orchestrationtest.StartLifecycleHost(t, ctx, world, "i23-drainee", 4, orchestrationtest.PooledHostConfig{
		Drain:            &drainOptions,
		WrapCheckpointer: holding.Wrap,
		// Mortal only for its provider view: HeldLeases reads which grants
		// the Host process itself still holds. It never dies here.
		Mortal: true,
	})
	// Registered AFTER the Host, so it runs BEFORE the Host's own Stop cleanup:
	// a failed case must not leave its drain parked at a held checkpoint.
	t.Cleanup(holding.Proceed)
	orchestrationtest.AwaitAdvertised(t, world, drainee.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "i23-drain-replica", nil)

	createPooled(t, ctx, served, tenant, idle, "idle-create", "the remembered word is PERSIMMON")
	createPooled(t, ctx, served, tenant, busy, "busy-create", "hello")
	for _, c := range []struct {
		s  sessionwire.SessionID
		id sessionwire.CommandID
	}{{idle, "idle-create"}, {busy, "busy-create"}} {
		orchestrationtest.PooledWait(t, string(c.id)+" applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, c.s, c.id) == sessionstore.InboxStateApplied
		})
	}
	idleRuntime := world.RuntimeSessionID(t, ctx, tenant, idle)
	busyRuntime := world.RuntimeSessionID(t, ctx, tenant, busy)
	for _, id := range []uuid.UUID{idleRuntime, busyRuntime} {
		orchestrationtest.PooledWait(t, "the create's turn finished", 60*time.Second, func() bool {
			return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, id) >= 1
		})
	}

	// The busy session's next turn is HELD at the model: mid-turn for as long
	// as the case wants.
	hold := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-hold:
		default:
			close(hold)
		}
	})
	world.LLM.ScriptNext(orchestrationtest.PooledTurn{Text: "the long turn is done", Hold: hold})
	requestsBefore := len(world.LLM.Requests())
	inputPooled(t, ctx, served, tenant, busy, "busy-input", "the busy word is QUINCE")
	orchestrationtest.PooledWait(t, "the busy turn reached the model", 60*time.Second, func() bool {
		return len(world.LLM.Requests()) > requestsBefore
	})

	firstOwner := map[sessionwire.SessionID]sessionwire.HostLinkRegistryObservation{}
	for _, s := range []sessionwire.SessionID{idle, busy} {
		owner, found := world.Registration(t, ctx, tenant, s)
		if !found || owner.HostID != drainee.ID || owner.Residency != sessionwire.SessionResidencyResident || !owner.Accepting {
			t.Fatalf("before the drain %s's owner is %+v (found=%v), want resident+accepting on %s", s, owner, found, drainee.ID)
		}
		firstOwner[s] = owner
	}
	busyTurnsBefore := orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, busyRuntime)

	// The successor comes up only now, so neither session could have gone to it.
	successor := orchestrationtest.StartPooledHost(t, ctx, world, "i23-successor", 5)
	orchestrationtest.AwaitAdvertised(t, world, successor.ID)

	// THE ORDERING WITNESS. At the instant the first session's checkpoint is
	// entered -- after the drain was acknowledged and the session marked
	// releasing, before anything was released -- the pooled target is read the
	// way Factory's placement reads it. A Host that stopped admitting in
	// process but published nonaccepting only on a later heartbeat would still
	// be listed here, even though it leaves the target eventually.
	var (
		orderMu              sync.Mutex
		listedAtFirstRelease *bool
	)
	holding.OnEnter = func(sessionwire.SessionID) {
		_, listed := orchestrationtest.PooledCandidates(t, ctx, world)[drainee.ID]
		orderMu.Lock()
		defer orderMu.Unlock()
		if listedAtFirstRelease == nil {
			listedAtFirstRelease = &listed
		}
	}
	stopped := drainee.BeginStop()
	drainBegan := time.Now()

	t.Run("case 1: capacity is unranked and admission stopped before anything is released", func(t *testing.T) {
		orchestrationtest.PooledWait(t, "the drainee left the pooled target", 10*time.Second, func() bool {
			_, listed := orchestrationtest.PooledCandidates(t, ctx, world)[drainee.ID]
			return !listed
		})
		if _, listed := orchestrationtest.PooledCandidates(t, ctx, world)[successor.ID]; !listed {
			t.Fatal("draining one Host withdrew its peer from the pooled target as well")
		}
		// THE IDLE SESSION IS PARKED AT ITS CHECKPOINT, which is after
		// BeginRelease and the idle wait and before the runtime release. The
		// busy one is still in its idle wait.
		orchestrationtest.PooledWait(t, "the idle session reached its drain checkpoint", 10*time.Second, func() bool {
			return len(holding.Entered()) >= 1
		})
		orderMu.Lock()
		listed := listedAtFirstRelease
		orderMu.Unlock()
		if listed == nil || *listed {
			t.Fatal("when the drain reached its first session's checkpoint the drainee was still (or was never read as) a placement candidate: capacity was not unranked before release began")
		}
		if entered := holding.Entered(); len(entered) != 1 || entered[0] != idle {
			t.Fatalf("checkpoints entered %v, want only the idle session: the mid-turn one must still be waiting for idle", entered)
		}
		for _, s := range []sessionwire.SessionID{idle, busy} {
			owner, found := world.Registration(t, ctx, tenant, s)
			if !found || owner.HostID != drainee.ID || owner.Residency != sessionwire.SessionResidencyReleasing || owner.Accepting {
				t.Fatalf("mid-drain %s's owner is %+v (found=%v), want releasing and not accepting on %s", s, owner, found, drainee.ID)
			}
			if owner.LeaseEpoch != firstOwner[s].LeaseEpoch {
				t.Fatalf("mid-drain %s's epoch moved %d -> %d", s, firstOwner[s].LeaseEpoch, owner.LeaseEpoch)
			}
			if !world.ResidencyHeld(t, ctx, tenant, s) {
				t.Fatalf("mid-drain the residency lease of %s is already free: released before its checkpoint", s)
			}
		}
		for _, id := range []uuid.UUID{idleRuntime, busyRuntime} {
			if got := orchestrationtest.CountJournalEvents[event.SessionStopped](t, world, tenant, id); got != 0 {
				t.Fatalf("mid-drain %d SessionStopped: a drain releases, it never stops", got)
			}
		}
		// The Host's own admission refuses a new residency outright.
		_, err := drainee.Service.Attach(ctx, host.AttachRequest{
			TenantID: tenant, SessionID: "session-drain-direct", AgentID: orchestrationtest.PooledAgent,
			Mode: sessionwire.HostLinkAttachModeCreate, RuntimeCompatibilityID: string(orchestrationtest.PooledCompatibility),
			ActorID: "orchestrationtest-attacher",
		})
		var refused *host.AttachError
		if !errors.As(err, &refused) || refused.Code != sessionwire.HostLinkErrorNotAdmitting {
			t.Fatalf("a direct attach on the draining Host answered %v, want a typed not_admitting refusal", err)
		}
	})

	t.Run("case 1: a session created during the drain is placed on the successor", func(t *testing.T) {
		createPooled(t, ctx, served, tenant, late, "late-create", "a session born during the drain")
		orchestrationtest.PooledWait(t, "the late create applied", 60*time.Second, func() bool {
			return world.CommandState(ctx, tenant, late, "late-create") == sessionstore.InboxStateApplied
		})
		owner, found := world.Registration(t, ctx, tenant, late)
		if !found || owner.HostID != successor.ID {
			t.Fatalf("a session created during the drain is owned by %+v (found=%v), want the successor %s", owner, found, successor.ID)
		}
		if creates := drainee.Rig.Creates(); len(creates) != 2 {
			t.Fatalf("the draining Host launched creates %+v, want only the two before the drain", creates)
		}
	})

	// FINDING (host v0.7.1), pinned as a trip-wire: the drain does NOT halt a
	// session's command consumer until ReleaseResidency, which runs AFTER the
	// checkpoint -- whereas the warm release halts it as its step 0
	// (internal/residency/warm.go, "STEP 0. Halt this Host's consumption"). So
	// an input admitted while the drain is parked at the session's checkpoint
	// is CLAIMED AND APPLIED BY THE DRAINING HOST, under its own residency,
	// while its registration says `releasing` and not accepting, and the turn
	// it starts runs across the checkpoint the release is supposed to end on.
	// Nothing is lost -- ReleaseResidency waits for idle -- but a long turn
	// here would outrun the grace and turn a graceful release into the
	// crash-equivalent one. The day Host halts consumption at BeginRelease this
	// row fails, and the input must instead be applied by the successor.
	t.Run("FINDING: a releasing session's consumer still applies input during the drain", func(t *testing.T) {
		inputPooled(t, ctx, served, tenant, idle, "idle-drain-input", "during the drain: what was the remembered word?")
		orchestrationtest.PooledWait(t, "the input admitted during the drain applied", 20*time.Second, func() bool {
			return world.CommandState(ctx, tenant, idle, "idle-drain-input") == sessionstore.InboxStateApplied
		})
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: idle, CommandID: "idle-drain-input"})
		if err != nil || entry.Record.Attempt == nil {
			t.Fatalf("reading the input admitted during the drain: %+v, %v", entry.Record.Attempt, err)
		}
		owner, found := world.Registration(t, ctx, tenant, idle)
		if !found || owner.HostID != drainee.ID || owner.Residency != sessionwire.SessionResidencyReleasing {
			t.Fatalf("while the drain is parked the idle session's owner is %+v (found=%v), want the drainee releasing", owner, found)
		}
		if got := uint64(entry.Record.Attempt.ResidencyEpoch); got != firstOwner[idle].LeaseEpoch {
			t.Fatalf("the input admitted during the drain was applied under residency %d, not the drainee's %d: "+
				"Host now halts consumption before the drain's checkpoint -- the finding is fixed; update this row", got, firstOwner[idle].LeaseEpoch)
		}
		orchestrationtest.PooledWait(t, "the draining Host began the input's turn", 20*time.Second, func() bool {
			return turnsCarrying(t, world, tenant, idleRuntime, "during the drain") == 1
		})
	})

	var report host.DrainReport
	t.Run("case 2: the mid-turn session finishes its turn, and both are released within the grace", func(t *testing.T) {
		close(hold)
		orchestrationtest.PooledWait(t, "the busy turn finished", 30*time.Second, func() bool {
			return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, busyRuntime) > busyTurnsBefore
		})
		orchestrationtest.PooledWait(t, "the busy session reached its drain checkpoint", 20*time.Second, func() bool {
			return len(holding.Entered()) == 2
		})
		holding.Proceed()
		select {
		case report = <-stopped:
		case <-time.After(drainOptions.Grace + 10*time.Second):
			t.Fatal("the drain did not finish within its grace")
		}
		elapsed := time.Since(drainBegan)
		t.Logf("drain report after %v: %+v", elapsed.Round(time.Millisecond), report)
		if report.State != sessionwire.HostLinkDrainStateDrained {
			t.Fatalf("the drain finished %q, want drained", report.State)
		}
		if len(report.Failures) != 0 {
			t.Fatalf("the drain of an idle and a finishing session recorded failures: %+v", report.Failures)
		}
		if report.Generation != 4 {
			t.Fatalf("the drain reports generation %d, want the Host's incarnation 4", report.Generation)
		}
		// Every grant the Host process took -- residency and harness journal
		// -- it released itself: this is the graceful path.
		for _, plane := range []string{orchestrationtest.PlaneStore, orchestrationtest.PlaneJournal} {
			if held := drainee.Process().HeldLeases(plane); len(held) != 0 {
				t.Fatalf("after a graceful drain the Host process still holds %s leases %v", plane, held)
			}
		}
		for _, s := range []sessionwire.SessionID{idle, busy} {
			if world.ResidencyHeld(t, ctx, tenant, s) {
				t.Fatalf("after the drain %s's residency lease is still held", s)
			}
			// THE REGISTRY ENTRY IS GONE: the epoch-fenced tombstone removed
			// the visible registration, so Factory's owner check finds no owner.
			if owner, found := world.Registration(t, ctx, tenant, s); found {
				t.Fatalf("after the drain %s still has a visible registration: %+v", s, owner)
			}
		}
		for _, id := range []uuid.UUID{idleRuntime, busyRuntime} {
			if got := orchestrationtest.CountJournalEvents[event.SessionStopped](t, world, tenant, id); got != 0 {
				t.Fatalf("the drain appended %d SessionStopped; a drain releases, it never stops", got)
			}
			// THE RELEASE IS RECORDED, with the epoch it gave up: harness
			// journals the nonterminal release and the single-writer lease
			// epoch the releasing process held.
			released := orchestrationtest.JournalEvents[event.SessionResidencyReleased](t, world, tenant, id)
			if len(released) != 1 || released[0].LeaseEpoch == 0 {
				t.Fatalf("%s's journal records releases %+v, want exactly one carrying the released lease epoch", id, released)
			}
		}
		// The mid-turn session was NOT interrupted: its held turn finished,
		// once, before the release.
		if got := turnsCarrying(t, world, tenant, busyRuntime, "QUINCE"); got != 1 {
			t.Fatalf("the busy input began %d turns, want exactly 1", got)
		}
		if got := orchestrationtest.CountJournalEvents[event.TurnInterrupted](t, world, tenant, busyRuntime) +
			orchestrationtest.CountJournalEvents[event.InputCancelled](t, world, tenant, busyRuntime); got != 0 {
			t.Fatalf("the drain interrupted or cancelled %d turns/inputs of the mid-turn session; it must wait for idle", got)
		}
		if got := drainee.SessionsIn(t, "resident") + drainee.SessionsIn(t, "releasing"); got != 0 {
			t.Fatalf("the drained Host still reports %d sessions resident or releasing", got)
		}
	})

	t.Run("case 2: the successor restores both sessions and nothing is lost or repeated", func(t *testing.T) {
		requestsAfterDrain := len(world.LLM.Requests())
		inputPooled(t, ctx, served, tenant, idle, "idle-after", "after the drain: recall the remembered word")
		inputPooled(t, ctx, served, tenant, busy, "busy-after", "after the drain: recall the busy word")
		for _, c := range []struct {
			s       sessionwire.SessionID
			id      uuid.UUID
			command sessionwire.CommandID
		}{{idle, idleRuntime, "idle-after"}, {busy, busyRuntime, "busy-after"}} {
			orchestrationtest.PooledWait(t, string(c.command)+" applied on the successor", 90*time.Second, func() bool {
				return world.CommandState(ctx, tenant, c.s, c.command) == sessionstore.InboxStateApplied
			})
			orchestrationtest.PooledWait(t, "the successor's route for "+string(c.s)+" turned resident", 30*time.Second, func() bool {
				owner, found := world.Registration(t, ctx, tenant, c.s)
				return found && owner.Residency == sessionwire.SessionResidencyResident
			})
			owner, _ := world.Registration(t, ctx, tenant, c.s)
			if owner.HostID != successor.ID || owner.LeaseEpoch <= firstOwner[c.s].LeaseEpoch {
				t.Fatalf("%s is owned by %+v, want the successor above epoch %d", c.s, owner, firstOwner[c.s].LeaseEpoch)
			}
			entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: c.s, CommandID: c.command})
			if err != nil {
				t.Fatalf("reading %s: %v", c.command, err)
			}
			if entry.Record.Attempt == nil || uint64(entry.Record.Attempt.ResidencyEpoch) != owner.LeaseEpoch {
				t.Fatalf("%s's attempt is %+v, want one under the successor's residency %d", c.command, entry.Record.Attempt, owner.LeaseEpoch)
			}
			orchestrationtest.PooledWait(t, "the restored turn on "+string(c.s)+" finished", 60*time.Second, func() bool {
				return turnsCarrying(t, world, tenant, c.id, "after the drain") == 1 &&
					len(orchestrationtest.JournalEvents[event.TurnDone](t, world, tenant, c.id)) >= 3
			})
			if got := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world, tenant, c.id); got != 1 {
				t.Fatalf("%s's journal holds %d SessionStarted, want 1: it was restarted, not restored", c.s, got)
			}
			if got := orchestrationtest.CountJournalEvents[event.RestoreDone](t, world, tenant, c.id); got < 1 {
				t.Fatalf("%s's journal holds no RestoreDone", c.s)
			}
		}
		restores := successor.Rig.Restores()
		if len(restores) != 2 {
			t.Fatalf("the successor restored %+v, want both drained sessions", restores)
		}
		if creates := successor.Rig.Creates(); len(creates) != 1 {
			t.Fatalf("the successor created %+v, want only the session born during the drain", creates)
		}
		if creates, restores := drainee.Rig.Creates(), drainee.Rig.Restores(); len(creates) != 2 || len(restores) != 0 {
			t.Fatalf("the drained Host launched creates=%+v restores=%+v after its drain", creates, restores)
		}
		// NO INPUT LOST, NONE REPEATED, across the drain: every input began
		// exactly one turn in the one journal both Hosts wrote.
		for _, c := range []struct {
			id     uuid.UUID
			needle string
		}{
			{idleRuntime, "remembered word is PERSIMMON"},
			{idleRuntime, "during the drain"},
			{idleRuntime, "after the drain"},
			{busyRuntime, "hello"},
			{busyRuntime, "QUINCE"},
			{busyRuntime, "after the drain"},
		} {
			if got := turnsCarrying(t, world, tenant, c.id, c.needle); got != 1 {
				t.Fatalf("%q began %d turns across the drain, want exactly 1", c.needle, got)
			}
		}
		// THE AGENT REMEMBERS: the successor's model requests carry what was
		// said to the drained Host.
		if !world.LLM.SawInRequest(requestsAfterDrain, "PERSIMMON") || !world.LLM.SawInRequest(requestsAfterDrain, "QUINCE") {
			t.Fatal("the successor's model requests do not carry the drained conversations")
		}
	})
}

// gatedDrainOptions is the drain policy the gated case runs under: short, so
// the bound it proves is measurable in a test.
var gatedDrainOptions = host.DrainOptions{Grace: 4 * time.Second, IdleBoundary: time.Second, PublishBound: time.Second}

// Private markers: neither may appear in anything the drain records.
const (
	drainPrivateInput    = "PRIVATE-INPUT-5b2d9e"
	drainPrivateQuestion = "PRIVATE-QUESTION-7c1e40"
)

// TestDrainingAPooledHostParkedAtAGateIsCrashEquivalentAndBounded is I2.3
// cases 3 and 4.
func TestDrainingAPooledHostParkedAtAGateIsCrashEquivalentAndBounded(t *testing.T) {
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	const s = sessionwire.SessionID("session-drain-gated")
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:     []sessionwire.TenantID{tenant},
		WithAskTool: true,
	})
	world.AskTool.Question = drainPrivateQuestion
	world.LLM.Script(
		orchestrationtest.PooledTurn{Text: "ready"},
		orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledAskToolName, ToolInput: `{}`},
	)
	drainee := orchestrationtest.StartLifecycleHost(t, ctx, world, "i23-gated-drainee", 4, orchestrationtest.PooledHostConfig{
		Drain:  &gatedDrainOptions,
		Mortal: true,
	})
	orchestrationtest.AwaitAdvertised(t, world, drainee.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "i23-gated-replica", nil)

	createPooled(t, ctx, served, tenant, s, "gated-create", "hello")
	orchestrationtest.PooledWait(t, "the create applied", 90*time.Second, func() bool {
		return world.CommandState(ctx, tenant, s, "gated-create") == sessionstore.InboxStateApplied
	})
	runtimeID := world.RuntimeSessionID(t, ctx, tenant, s)
	orchestrationtest.PooledWait(t, "the create's turn finished", 60*time.Second, func() bool {
		return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID) >= 1
	})
	inputPooled(t, ctx, served, tenant, s, "gated-input", "please ask me "+drainPrivateInput)
	var projected sessionwire.GateProjection
	orchestrationtest.PooledWait(t, "the gate reached Factory's gates read", 60*time.Second, func() bool {
		page := served.OpenGates(t, ctx, tenant, s)
		if len(page.Gates) == 0 {
			return false
		}
		projected = page.Gates[0]
		return true
	})
	attached, found := world.Registration(t, ctx, tenant, s)
	if !found || attached.HostID != drainee.ID || attached.Residency != sessionwire.SessionResidencyResident {
		t.Fatalf("before the drain the owner is %+v (found=%v), want resident on %s", attached, found, drainee.ID)
	}
	successor := orchestrationtest.StartPooledHost(t, ctx, world, "i23-gated-successor", 5)
	orchestrationtest.AwaitAdvertised(t, world, successor.ID)
	parkedJournal := orchestrationtest.JournalTrace(t, world, tenant, runtimeID)

	t.Run("case 3: the resident gate wait is accounted for before the drain", func(t *testing.T) {
		// Where a Factory reads it: the durable gate projection.
		page := served.OpenGates(t, ctx, tenant, s)
		if page.OpenGateCount != 1 || len(page.Gates) != 1 || page.Gates[0].Answerability != sessionwire.GateAnswerabilityResident {
			t.Fatalf("before the drain the gates read is %+v, want one resident-answerable gate", page)
		}
		if got := orchestrationtest.CountJournalEvents[event.GateOpened](t, world, tenant, runtimeID); got != 1 {
			t.Fatalf("the journal holds %d GateOpened, want the one the agent raised", got)
		}
		// FINDING (host v0.7.1), pinned as a trip-wire: the Host's own
		// gauge does not see it. Compose wires no GateWaits source into its
		// metrics, although the same composition's derived work-state source
		// folds this gate (it is what keeps the session from being
		// warm-released), so an operator reads zero sessions at a gate.
		if got := drainee.Metric(t, "host_sessions_gate_waiting"); got != 0 {
			t.Fatalf("host_sessions_gate_waiting = %d: Host now composes a gate-wait source -- the finding is fixed; assert 1 here", got)
		}
	})

	var report host.DrainReport
	t.Run("case 3: the drain is bounded by its policy and records the forced release", func(t *testing.T) {
		var took time.Duration
		report, took = drainee.StopReport(t, gatedDrainOptions.Grace+20*time.Second)
		t.Logf("the drain took %v: %+v", took.Round(time.Millisecond), report)
		// BOUNDED: the parked runtime never becomes idle, so the drain ends
		// on its own policy -- no sooner than the idle boundary, no later than
		// the platform grace (plus scheduling slack).
		if took < gatedDrainOptions.IdleBoundary || took > gatedDrainOptions.Grace+2*time.Second {
			t.Fatalf("the drain took %v, want between the idle boundary %v and the grace %v", took, gatedDrainOptions.IdleBoundary, gatedDrainOptions.Grace)
		}
		// FinishRelease succeeded, so Core's two-valued state says drained;
		// what was forced is in the process-local report Core cannot carry.
		if report.State != sessionwire.HostLinkDrainStateDrained || report.Generation != 4 {
			t.Fatalf("the drain finished %q at generation %d, want drained at the Host's incarnation 4", report.State, report.Generation)
		}
		steps := map[string]bool{}
		for _, failure := range report.Failures {
			if failure.TenantID != tenant || failure.SessionID != s {
				t.Fatalf("the drain recorded a failure against %s/%s, want only the gated session: %v", failure.TenantID, failure.SessionID, failure)
			}
			steps[failure.Step] = true
		}
		// THE STEP NAMES ARE HOST'S lifecycle vocabulary (internal, so
		// spelled here): the idle wait hit its boundary and the runtime
		// refused a nonterminal release of a session parked at a gate.
		if len(report.Failures) != 2 || !steps["wait_idle"] || !steps["release_residency"] {
			t.Fatalf("the drain recorded %+v, want exactly a wait_idle and a release_residency failure", report.Failures)
		}
		// NO PRIVATE PAYLOAD: neither what the user said nor what the agent
		// asked appears in anything the drain recorded.
		recorded := fmt.Sprintf("%+v %v", report, report.Failures)
		for _, failure := range report.Failures {
			recorded += " " + failure.Error()
		}
		for _, private := range []string{drainPrivateInput, drainPrivateQuestion} {
			if strings.Contains(recorded, private) {
				t.Fatalf("the drain report leaks %q: %s", private, recorded)
			}
		}
	})

	t.Run("case 4: the release is crash-equivalent -- residency freed, runtime parked on its journal lease", func(t *testing.T) {
		if world.ResidencyHeld(t, ctx, tenant, s) {
			t.Fatal("after the drain the residency lease is still held; FinishRelease must release it even when the runtime refused")
		}
		if owner, found := world.Registration(t, ctx, tenant, s); found {
			t.Fatalf("after the drain the session still has a visible registration: %+v", owner)
		}
		// The Host released its residency grant itself...
		if held := drainee.Process().HeldLeases(orchestrationtest.PlaneStore); len(held) != 0 {
			t.Fatalf("after the drain the Host process still holds residency leases %v", held)
		}
		// ...but the runtime keeps its harness journal lease until the
		// process exits: read off the Host process's own provider view.
		held := drainee.Process().HeldLeases(orchestrationtest.PlaneJournal)
		if len(held) != 1 || !strings.Contains(held[0], runtimeID.String()) {
			t.Fatalf("after the drain the Host process holds journal leases %v, want the parked runtime's %s", held, runtimeID)
		}
		// PARKED, NOT CANCELLED: the journal is exactly what it was at the
		// gate -- no GateResolved{abandoned}, no TurnInterrupted, no
		// SessionStopped and no SessionResidencyReleased -- and the tool is
		// still waiting inside RequestUserInput.
		if after := orchestrationtest.JournalTrace(t, world, tenant, runtimeID); strings.Join(after, " ") != strings.Join(parkedJournal, " ") {
			t.Fatalf("the drain wrote to the parked runtime's journal:\nbefore %v\nafter  %v", parkedJournal, after)
		}
		if answers, err := world.AskTool.Answers(), world.AskTool.LastErr(); len(answers) != 0 || err != nil {
			t.Fatalf("the parked tool returned (answers %v, err %v); a crash-equivalent drain leaves it waiting", answers, err)
		}
	})

	t.Run("case 3: no test answers a cold gate -- Factory refuses one whose owner is gone", func(t *testing.T) {
		page := served.OpenGates(t, ctx, tenant, s)
		// FINDING (sessionstore/host), pinned as a trip-wire: the gate
		// projection outlives its owner. After a crash-equivalent drain the
		// gates read still offers the gate as RESIDENT-answerable, though no
		// Host holds the session; only Factory's write path knows better.
		if len(page.Gates) != 1 || page.Gates[0].Answerability != sessionwire.GateAnswerabilityResident {
			t.Fatalf("after the drain the gates read is %+v: the projection no longer outlives its owner -- the finding is fixed; update this row", page)
		}
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/gates/"+string(projected.GateID),
			sessionwire.GateResponseRequest{
				CommandEnvelope:        orchestrationtest.PooledEnvelope("gated-cold-answer"),
				SessionID:              s,
				GateID:                 projected.GateID,
				Action:                 "answer",
				Values:                 map[string]json.RawMessage{"answer": json.RawMessage(`"` + gateAnswer + `"`)},
				ExpectedOpenJournalSeq: projected.OpenedJournalSeq,
			})
		if status != http.StatusConflict || !strings.Contains(body, "gate_not_resumable") {
			t.Fatalf("a cold gate answer answered %d %s, want 409 gate_not_resumable", status, body)
		}
		if state := world.CommandState(ctx, tenant, s, "gated-cold-answer"); state != "" {
			t.Fatalf("the refused cold answer left a durable command in state %q", state)
		}
	})

	t.Run("the Host exits and the successor restores: the gate is closed, and the resent answer reaches the agent", func(t *testing.T) {
		// host v0.4.0's obligation: a Host that drained with a gate open MUST
		// exit. Its exit is what frees the parked runtime's journal lease.
		drainee.Kill(t)
		if held := drainee.Process().HeldLeases(orchestrationtest.PlaneJournal); len(held) != 0 {
			t.Fatalf("after the exit the Host process still holds journal leases %v", held)
		}
		requestsBefore := len(world.LLM.Requests())
		inputPooled(t, ctx, served, tenant, s, "gated-resend", "my answer is "+gateAnswer)
		orchestrationtest.PooledWait(t, "the resent answer applied on the successor", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "gated-resend") == sessionstore.InboxStateApplied
		})
		orchestrationtest.PooledWait(t, "the successor's route turned resident", 30*time.Second, func() bool {
			owner, found := world.Registration(t, ctx, tenant, s)
			return found && owner.Residency == sessionwire.SessionResidencyResident
		})
		owner, _ := world.Registration(t, ctx, tenant, s)
		if owner.HostID != successor.ID || owner.LeaseEpoch <= attached.LeaseEpoch {
			t.Fatalf("after the exit the owner is %+v, want the successor above epoch %d", owner, attached.LeaseEpoch)
		}
		if creates, restores := successor.Rig.Creates(), successor.Rig.Restores(); len(creates) != 0 || len(restores) != 1 || restores[0].ID != runtimeID {
			t.Fatalf("the successor launched creates=%+v restores=%+v, want one restore of %s", creates, restores, runtimeID)
		}
		orchestrationtest.PooledWait(t, "the resent answer's turn finished", 60*time.Second, func() bool {
			return turnsCarrying(t, world, tenant, runtimeID, "my answer is") == 1 &&
				orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID) >= 2
		})
		// THE DOCUMENTED BEHAVIOUR (harness ask_user after failover is still
		// booked): restore closes the open ask_user gate as unavailable and
		// interrupts its turn, so the tool never receives an answer.
		resolved := orchestrationtest.JournalEvents[event.GateResolved](t, world, tenant, runtimeID)
		if len(resolved) != 1 || resolved[0].Reason != gate.CloseRestoreUnavailable {
			t.Fatalf("the journal resolved gates %+v, want the one ask_user gate closed restore_unavailable", resolved)
		}
		if got := orchestrationtest.CountJournalEvents[event.TurnInterrupted](t, world, tenant, runtimeID); got != 1 {
			t.Fatalf("the journal holds %d TurnInterrupted, want the gated turn's one", got)
		}
		if answers := world.AskTool.Answers(); len(answers) != 0 {
			t.Fatalf("the ask_user tool received %v; no continuation of a cold gate is claimed", answers)
		}
		if page := served.OpenGates(t, ctx, tenant, s); page.OpenGateCount != 0 || len(page.Gates) != 0 {
			t.Fatalf("after the restore the gates read still offers %+v", page)
		}
		if got := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world, tenant, runtimeID); got != 1 {
			t.Fatalf("the journal holds %d SessionStarted, want 1: restarted, not restored", got)
		}
		if got := orchestrationtest.CountJournalEvents[event.SessionStopped](t, world, tenant, runtimeID); got != 0 {
			t.Fatalf("the journal holds %d SessionStopped", got)
		}
		// NOTHING LOST OR REPEATED, and the answer reaches the agent as the
		// resent input, with the drained conversation behind it.
		if got := turnsCarrying(t, world, tenant, runtimeID, drainPrivateInput); got != 1 {
			t.Fatalf("the gated input began %d turns across the drain, want exactly 1", got)
		}
		if !world.LLM.SawInRequest(requestsBefore, gateAnswer) || !world.LLM.SawInRequest(requestsBefore, drainPrivateInput) {
			t.Fatal("the successor's model request does not carry the resent answer and the drained conversation")
		}
	})
}
