//go:build integration

// This file is runbook 07 I2.1's LEASE TAKEOVER lane: a pooled Host PROCESS
// dies holding a session, and a successor Host -- reached through a real
// factory v0.7.1's placement sweep -- takes the lapsed lease over at a higher
// epoch and RESTORES the session: same harness journal, same conversation, same
// workspace bytes, at the same model-visible path.
//
// "Dies" is PooledHost.Kill, not Stop. Stop drains -- it checkpoints, releases
// and tombstones -- and a takeover after a drain proves nothing about a crash.
// Kill cuts the dead process's durable plane and lapses its leases the way a
// provider TTL does; the corpse's goroutines keep running and can write nothing.

package tests

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/host"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// takeoverBytes is what generation one writes into its workspace. The case
// reads it back through a real tool on the successor.
const takeoverBytes = "takeover-bytes: 7f3a9c written by the first generation"

// TestAHostThatDiesIsTakenOverAndItsSessionRestored is I2.1's takeover claim.
func TestAHostThatDiesIsTakenOverAndItsSessionRestored(t *testing.T) {
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	const s = sessionwire.SessionID("session-takeover")
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:       []sessionwire.TenantID{tenant},
		WithWorkspace: true,
	})
	world.LLM.Script(
		orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledWriteToolName,
			ToolInput: `{"path":"notes.txt","content":"` + takeoverBytes + `"}`},
		orchestrationtest.PooledTurn{Text: "written"},
	)
	doomed := orchestrationtest.StartLifecycleHost(t, ctx, world, "i21-doomed-host", 4, orchestrationtest.PooledHostConfig{Mortal: true})
	orchestrationtest.AwaitAdvertised(t, world, doomed.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "i21-takeover-replica", nil)

	status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("takeover-create"),
		SessionID:       s,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(`[{"type":"text","text":"the remembered word is PERSIMMON; write the notes file"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "the create applied", 90*time.Second, func() bool {
		return world.CommandState(ctx, tenant, s, "takeover-create") == sessionstore.InboxStateApplied
	})
	runtimeID := world.RuntimeSessionID(t, ctx, tenant, s)
	orchestrationtest.PooledWait(t, "the first generation's turn finished and was checkpointed", 60*time.Second, func() bool {
		return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID) >= 1 &&
			len(orchestrationtest.JournalEvents[event.WorkspaceCheckpointed](t, world, tenant, runtimeID)) >= 1
	})
	writes := world.WorkspaceTools.Writes()
	if len(writes) != 1 || writes[0].Content != takeoverBytes {
		t.Fatalf("the first generation's writes are %+v, want the one notes file", writes)
	}
	checkpoints := orchestrationtest.JournalEvents[event.WorkspaceCheckpointed](t, world, tenant, runtimeID)
	lastCheckpoint := checkpoints[len(checkpoints)-1].Ref
	first, found := world.Registration(t, ctx, tenant, s)
	if !found || first.HostID != doomed.ID || first.Residency != sessionwire.SessionResidencyResident {
		t.Fatalf("the first owner is %+v (found=%v), want %s resident", first, found, doomed.ID)
	}
	turnsBefore := orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID)

	// THE PROCESS DIES. Nothing drains, releases, checkpoints or tombstones.
	lapsed := doomed.Kill(t)
	t.Logf("host %s died holding %d leases; last checkpoint %s", doomed.ID, lapsed, lastCheckpoint)
	// And its disk goes with it: the successor starts on an empty one, so the
	// bytes it reads can only come from the durable snapshot plane.
	if wiped := world.WipeWorkspaceDisk(t); wiped == 0 {
		t.Fatal("the dead Host left no materialized workspace to wipe; the case would prove nothing about the snapshot plane")
	}

	successor := orchestrationtest.StartPooledHost(t, ctx, world, "i21-successor-host", 5)
	orchestrationtest.AwaitAdvertised(t, world, successor.ID)

	world.LLM.Script(
		orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledReadToolName, ToolInput: `{"path":"notes.txt"}`},
		orchestrationtest.PooledTurn{Text: "read"},
	)
	requestsBefore := len(world.LLM.Requests())
	status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("takeover-input"),
		SessionID:       s,
		Blocks:          json.RawMessage(`[{"type":"text","text":"read the notes file; what was the remembered word?"}]`),
	})
	if status != http.StatusOK {
		t.Fatalf("the input after the death answered %d: %s", status, body)
	}
	took := orchestrationtest.PooledWait(t, "the successor applied the input", 120*time.Second, func() bool {
		return world.CommandState(ctx, tenant, s, "takeover-input") == sessionstore.InboxStateApplied
	})
	t.Logf("the successor applied the input %v after it was admitted", took.Round(time.Millisecond))

	t.Run("the successor took the lease over at a higher epoch", func(t *testing.T) {
		owner, found := world.Registration(t, ctx, tenant, s)
		if !found || owner.HostID != successor.ID || owner.Residency != sessionwire.SessionResidencyResident {
			t.Fatalf("the owner after the death is %+v (found=%v), want %s resident", owner, found, successor.ID)
		}
		if owner.LeaseEpoch <= first.LeaseEpoch {
			t.Fatalf("the successor holds epoch %d, not above the dead Host's %d", owner.LeaseEpoch, first.LeaseEpoch)
		}
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: s, CommandID: "takeover-input"})
		if err != nil {
			t.Fatalf("reading the settled input: %v", err)
		}
		if entry.Record.Attempt == nil || uint64(entry.Record.Attempt.ResidencyEpoch) != owner.LeaseEpoch {
			t.Fatalf("the input's attempt is %+v, want one authorized under the successor's residency %d", entry.Record.Attempt, owner.LeaseEpoch)
		}
	})

	t.Run("the successor restored the session rather than restarting it", func(t *testing.T) {
		creates, restores := successor.Rig.Creates(), successor.Rig.Restores()
		if len(creates) != 0 {
			t.Fatalf("the successor CREATED %+v; a taken-over session must be restored", creates)
		}
		if len(restores) != 1 || restores[0].ID != runtimeID {
			t.Fatalf("the successor restored %+v, want exactly one restore of %s", restores, runtimeID)
		}
		orchestrationtest.PooledWait(t, "the restored turn finished", 60*time.Second, func() bool {
			return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID) > turnsBefore
		})
		if got := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world, tenant, runtimeID); got != 1 {
			t.Fatalf("the journal holds %d SessionStarted, want 1: the session was restarted", got)
		}
		if got := orchestrationtest.CountJournalEvents[event.SessionStopped](t, world, tenant, runtimeID); got != 0 {
			t.Fatalf("the journal holds %d SessionStopped; nobody stopped this session", got)
		}
		if !world.LLM.SawInRequest(requestsBefore, "PERSIMMON") {
			t.Fatal("no model request on the successor carried PERSIMMON; the conversation did not survive")
		}
	})

	t.Run("the workspace bytes survived, at the same model-visible path", func(t *testing.T) {
		assertWorkspaceSurvived(t, world, writes[0], takeoverBytes)
		// harness journals NO WorkspaceRestored when a restore materializes the
		// last checkpoint (measured: zero), so which checkpoint came back is
		// proven by the bytes, not by a ref.
		t.Logf("the last committed checkpoint before the death was %s", lastCheckpoint)
	})
}

// assertWorkspaceSurvived reads the successor's tool RESULT: the bytes a real
// tool read back off the successor's materialized workspace.
func assertWorkspaceSurvived(t *testing.T, world *orchestrationtest.PooledWorld, written orchestrationtest.PooledWorkspaceRead, want string) {
	t.Helper()
	var read orchestrationtest.PooledWorkspaceRead
	orchestrationtest.PooledWait(t, "the successor's agent read the workspace", 60*time.Second, func() bool {
		for _, candidate := range world.WorkspaceTools.Reads() {
			if candidate.Path == written.Path {
				read = candidate
				return true
			}
		}
		return false
	})
	if !read.Present || read.Content != want {
		t.Fatalf("the successor's read tool returned present=%v %q, want %q", read.Present, read.Content, want)
	}
	orchestrationtest.AssertModelVisiblePathStable(t, written, read)
}

// TestTwoHostsRacingOneColdSessionInstallOneRuntime is I2.1 case 1: two Hosts
// race one cold session's attach. Exactly one wins the residency lease and
// installs a runtime; the other is refused and launches nothing.
func TestTwoHostsRacingOneColdSessionInstallOneRuntime(t *testing.T) {
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	const s = sessionwire.SessionID("session-cold-race")
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{tenant},
	})
	// A Factory that admits but never places, so the session is COLD and the
	// race below is the only attach it sees.
	served := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica: "i21-race-replica", WithoutPendingCommands: true,
	})
	status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("race-create"),
		SessionID:       s,
		AgentID:         orchestrationtest.PooledAgent,
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}
	hosts := []*orchestrationtest.PooledHost{
		orchestrationtest.StartPooledHost(t, ctx, world, "i21-race-host-a", 4),
		orchestrationtest.StartPooledHost(t, ctx, world, "i21-race-host-b", 4),
	}
	results := make([]error, len(hosts))
	residencies := make([]host.Residency, len(hosts))
	var start, done sync.WaitGroup
	start.Add(1)
	for i, h := range hosts {
		done.Add(1)
		go func(i int, h *orchestrationtest.PooledHost) {
			defer done.Done()
			start.Wait()
			residencies[i], results[i] = h.Service.Attach(ctx, host.AttachRequest{
				TenantID:               tenant,
				SessionID:              s,
				AgentID:                orchestrationtest.PooledAgent,
				Mode:                   sessionwire.HostLinkAttachModeCreate,
				RuntimeCompatibilityID: string(orchestrationtest.PooledCompatibility),
				ActorID:                "i21-racer",
			})
		}(i, h)
	}
	start.Done()
	done.Wait()

	winners, launched := []int{}, 0
	for i, h := range hosts {
		t.Logf("%s: attach err=%v residency=%+v creates=%d restores=%d", h.ID, results[i], residencies[i], len(h.Rig.Creates()), len(h.Rig.Restores()))
		if results[i] == nil {
			winners = append(winners, i)
		}
		launched += len(h.Rig.Creates()) + len(h.Rig.Restores())
	}
	if len(winners) != 1 {
		t.Fatalf("%d Hosts won the race for one cold session, want exactly 1", len(winners))
	}
	if launched != 1 {
		t.Fatalf("%d runtimes were launched across both Hosts, want exactly 1", launched)
	}
	winner, loser := hosts[winners[0]], hosts[1-winners[0]]
	if got := len(loser.Rig.Creates()) + len(loser.Rig.Restores()); got != 0 {
		t.Fatalf("the losing Host %s launched %d runtimes", loser.ID, got)
	}
	owner, found := world.Registration(t, ctx, tenant, s)
	if !found || owner.HostID != winner.ID || owner.Residency != sessionwire.SessionResidencyResident {
		t.Fatalf("the durable owner is %+v (found=%v), want the winner %s resident", owner, found, winner.ID)
	}
	if got := loser.SessionsIn(t, "resident"); got != 0 {
		t.Fatalf("the losing Host reports %d resident sessions", got)
	}
}
