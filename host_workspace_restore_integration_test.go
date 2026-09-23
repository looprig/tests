//go:build integration

// This file is runbook 07 I2.1's STABLE WORKSPACE lane (cases 4 and 5): a
// session's workspace bytes survive a GRACEFUL change of generation -- warm
// release on one Host, re-placement by a real Factory onto a different Host
// with a different disk -- at the same model-visible path, and two tenants that
// chose the same SessionID never see each other's registry entry, runtime,
// journal or workspace.
//
// The assertion is on the successor's TOOL RESULT -- the bytes a real tool
// read back off the successor's materialized harness workspace -- never on
// transcript text.

package tests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// TestAWorkspaceSurvivesAWarmReleaseOntoAnotherHostPerTenant is I2.1 cases 4
// and 5.
func TestAWorkspaceSurvivesAWarmReleaseOntoAnotherHostPerTenant(t *testing.T) {
	ctx := placementContext(t)
	// ONE SessionID, TWO tenants: case 5's collision is the point.
	const s = sessionwire.SessionID("session-shared-name")
	tenants := []sessionwire.TenantID{orchestrationtest.PooledTenantA, orchestrationtest.PooledTenantB}
	content := map[sessionwire.TenantID]string{
		orchestrationtest.PooledTenantA: "tenant-a workspace bytes 1c9e",
		orchestrationtest.PooledTenantB: "tenant-b workspace bytes 84d2",
	}
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{WithWorkspace: true})

	first := orchestrationtest.StartLifecycleHost(t, ctx, world, "i21-ws-first", 4, orchestrationtest.PooledHostConfig{
		WarmTTL: lifecycleWarmTTL, WorkPoll: lifecycleWorkPoll,
	})
	orchestrationtest.AwaitAdvertised(t, world, first.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "i21-ws-replica", nil)

	runtimeIDs := map[sessionwire.TenantID]uuid.UUID{}
	// Sequential per tenant so the scripted model's turns pair with the right
	// tenant's session.
	for _, tenant := range tenants {
		world.LLM.Script(
			orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledWriteToolName,
				ToolInput: fmt.Sprintf(`{"path":"notes.txt","content":%q}`, content[tenant])},
			orchestrationtest.PooledTurn{Text: "written"},
		)
		command := sessionwire.CommandID("ws-create-" + string(tenant))
		status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)),
			SessionID:       s,
			AgentID:         orchestrationtest.PooledAgent,
			Blocks:          json.RawMessage(`[{"type":"text","text":"write the notes file"}]`),
		})
		if status != http.StatusCreated {
			t.Fatalf("the create for %s answered %d: %s", tenant, status, body)
		}
		orchestrationtest.PooledWait(t, "the create for "+string(tenant)+" applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, command) == sessionstore.InboxStateApplied
		})
		runtimeIDs[tenant] = world.RuntimeSessionID(t, ctx, tenant, s)
		// Read NOW: with a one-second warm TTL the entry is tombstoned soon.
		owner, found := world.Registration(t, ctx, tenant, s)
		if !found || owner.TenantID != tenant || owner.SessionID != s || owner.HostID != first.ID {
			t.Fatalf("%s's registry entry is %+v (found=%v)", tenant, owner, found)
		}
		orchestrationtest.PooledWait(t, string(tenant)+"'s write turn finished", 60*time.Second, func() bool {
			return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeIDs[tenant]) >= 1
		})
	}
	writes := world.WorkspaceTools.Writes()
	if len(writes) != 2 {
		t.Fatalf("the first generation made %d writes, want one per tenant: %+v", len(writes), writes)
	}
	writeOf := map[sessionwire.TenantID]orchestrationtest.PooledWorkspaceRead{}
	for i, tenant := range tenants {
		writeOf[tenant] = writes[i]
		if writes[i].Content != content[tenant] {
			t.Fatalf("write %d holds %q, want %s's bytes", i, writes[i].Content, tenant)
		}
	}

	t.Run("case 5: one SessionID, two tenants, nothing shared", func(t *testing.T) {
		if runtimeIDs[tenants[0]] == runtimeIDs[tenants[1]] {
			t.Fatalf("both tenants' sessions run under runtime id %s", runtimeIDs[tenants[0]])
		}
		if writeOf[tenants[0]].LogicalRoot == writeOf[tenants[1]].LogicalRoot ||
			writeOf[tenants[0]].PhysicalRoot == writeOf[tenants[1]].PhysicalRoot {
			t.Fatalf("the two tenants share a workspace: %+v / %+v", writeOf[tenants[0]], writeOf[tenants[1]])
		}
		for _, tenant := range tenants {
			// Each tenant's journal holds exactly its own session.
			if got := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world, tenant, runtimeIDs[tenant]); got != 1 {
				t.Fatalf("%s's journal holds %d SessionStarted for its runtime, want 1", tenant, got)
			}
		}
	})

	// Both sessions go idle and are released. The last checkpoint each held
	// before the release is what a successor must restore.
	orchestrationtest.PooledWait(t, "the first Host released both idle sessions", 30*time.Second, func() bool {
		return first.SessionsIn(t, "resident") == 0 && first.SessionsIn(t, "releasing") == 0
	})
	lastCheckpoint := map[sessionwire.TenantID]uint64{}
	for _, tenant := range tenants {
		seq, ref, found := orchestrationtest.LastCheckpointSeq(t, world, tenant, runtimeIDs[tenant])
		if !found {
			t.Fatalf("%s's session was released with no committed workspace checkpoint", tenant)
		}
		lastCheckpoint[tenant] = seq
		t.Logf("%s: last checkpoint before the release at journal seq %d (%s)", tenant, seq, ref)
	}
	// The first Host goes away, gracefully: the next generation MUST run on a
	// different Host, on an empty disk.
	first.Stop()
	// A different Host is a fresh disk: nothing materialized survives it.
	if wiped := world.WipeWorkspaceDisk(t); wiped == 0 {
		t.Fatal("the first Host left no materialized workspace to wipe; the case would prove nothing about the snapshot plane")
	}
	second := orchestrationtest.StartPooledHost(t, ctx, world, "i21-ws-second", 5)
	orchestrationtest.AwaitAdvertised(t, world, second.ID)

	for _, tenant := range tenants {
		world.LLM.Script(
			orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledReadToolName, ToolInput: `{"path":"notes.txt"}`},
			orchestrationtest.PooledTurn{Text: "read"},
		)
		readsBefore := len(world.WorkspaceTools.Reads())
		command := sessionwire.CommandID("ws-input-" + string(tenant))
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)),
			SessionID:       s,
			Blocks:          json.RawMessage(`[{"type":"text","text":"read the notes file"}]`),
		})
		if status != http.StatusOK {
			t.Fatalf("the input for %s answered %d: %s", tenant, status, body)
		}
		orchestrationtest.PooledWait(t, "the second generation applied "+string(tenant)+"'s input", 120*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, command) == sessionstore.InboxStateApplied
		})
		var read orchestrationtest.PooledWorkspaceRead
		orchestrationtest.PooledWait(t, string(tenant)+"'s agent read its workspace", 60*time.Second, func() bool {
			reads := world.WorkspaceTools.Reads()
			if len(reads) <= readsBefore {
				return false
			}
			read = reads[readsBefore]
			return true
		})
		t.Run("case 4: "+string(tenant)+"'s bytes survived at the same model-visible path", func(t *testing.T) {
			written := writeOf[tenant]
			if !read.Present || read.Content != content[tenant] {
				t.Fatalf("the second generation read present=%v %q, want %s's own bytes %q", read.Present, read.Content, tenant, content[tenant])
			}
			orchestrationtest.AssertModelVisiblePathStable(t, written, read)
			// WHICH checkpoint came back is harness's own report, read off the
			// restored runtime: the journal sequence of the transition the live
			// tree was materialized from, and whether work followed it.
			status := orchestrationtest.AwaitWorkspaceStatus(t, second, tenant, s)
			if !status.HasCheckpoint || status.CheckpointSeq != lastCheckpoint[tenant] {
				t.Fatalf("%s came up on checkpoint seq %d (has=%v), want the last committed one, %d", tenant, status.CheckpointSeq, status.HasCheckpoint, lastCheckpoint[tenant])
			}
			if status.LogicalRoot != written.LogicalRoot {
				t.Fatalf("%s's model-visible workspace path is %q after the restore, want %q", tenant, status.LogicalRoot, written.LogicalRoot)
			}
			if status.PostCheckpointLoss() {
				t.Fatalf("%s reports post-checkpoint loss (%d events) after a graceful release that checkpointed last", tenant, status.PostCheckpointEvents)
			}
			if got := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world, tenant, runtimeIDs[tenant]); got != 1 {
				t.Fatalf("%s's journal holds %d SessionStarted: the session was restarted, not restored", tenant, got)
			}
		})
	}
	if creates, restores := second.Rig.Creates(), second.Rig.Restores(); len(creates) != 0 || len(restores) != 2 {
		t.Fatalf("the second Host launched creates=%+v restores=%+v, want two restores and no create", creates, restores)
	}
}
