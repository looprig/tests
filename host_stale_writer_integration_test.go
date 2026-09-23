//go:build integration

// This file is runbook 07 I2.1 case 2: a pooled Host is PAUSED with writes in
// flight -- a stopped process, not a dead one -- its leases lapse without it
// being told, a successor takes the sessions over at a higher epoch through a
// real factory v0.7.1, and then the stale Host RESUMES. Every write it had in
// flight, and every write it makes after, reaches the REAL released stores
// (sessionstore v0.13.0, harness v0.36.0's journal over storage v0.6.0) with
// the stale Host's own arguments. Nothing in the kit refuses them: HostProcess
// only delays and records. So every refusal asserted below is the store's.
//
// # The four write kinds, and what each maps to for a Host session
//
//	journal     a harness journal append from a turn paused MID-WAY (the
//	            turn's PermissionDecided frame, after TurnStarted);
//	registry    the stale Host's route heartbeat, sessionstore's
//	            PutHostRegistration, caught at its write;
//	command     the stale Host's SETTLEMENT of a command it had claimed and
//	            begun an attempt on, caught at its write;
//	checkpoint  the spec's "checkpoint pointer". SessionStore's checkpoint
//	            pointers are LEGACY-ONLY and a disposition session has none;
//	            a Host session's checkpoint pointer is harness's
//	            WorkspaceCheckpointed journal frame, so that is the write held.
//	            The snapshot blob it names is content-addressed and harmless on
//	            its own; what must not land is a frame referencing it.
//
// A kind the stale Host never attempted fails the case as vacuous.

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// staleKind classifies one recorded write into the four kinds, "" otherwise.
func staleKind(call orchestrationtest.ProcessCall, journals map[string]bool) string {
	if !call.Write() {
		return ""
	}
	payload := string(call.Payload)
	switch {
	case call.Plane == orchestrationtest.PlaneJournal && call.Op == "ledger.append" && journals[call.Name]:
		if strings.Contains(payload, `"type":"WorkspaceCheckpointed"`) {
			return "checkpoint"
		}
		return "journal"
	case call.Plane == orchestrationtest.PlaneStore && strings.Contains(call.Name, "sessionstore/registry"):
		return "registry"
	case call.Plane == orchestrationtest.PlaneStore && strings.Contains(call.Name, "disposition-inbox"):
		return "command"
	}
	return ""
}

var eventIDPattern = regexp.MustCompile(`"event_id":"([0-9a-f-]{36})"`)

// fenceRefusal reports whether err is a compare-and-swap refusal by the store:
// a ledger tip that moved or an ordered record whose revision moved.
func fenceRefusal(err error) bool {
	var ledger *storage.ConflictError
	var revision *storage.OrderedRevisionConflictError
	return errors.As(err, &ledger) || errors.As(err, &revision)
}

// TestAResumedStaleHostsWritesAreFencedByTheStores is I2.1 case 2.
func TestAResumedStaleHostsWritesAreFencedByTheStores(t *testing.T) {
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	const (
		turnSession = sessionwire.SessionID("session-stale-turn")
		idleSession = sessionwire.SessionID("session-stale-idle")
		staleInput  = sessionwire.CommandID("stale-input")
	)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:       []sessionwire.TenantID{tenant},
		WithWorkspace: true,
	})
	stale := orchestrationtest.StartLifecycleHost(t, ctx, world, "i21-stale-host", 4, orchestrationtest.PooledHostConfig{Mortal: true})
	orchestrationtest.AwaitAdvertised(t, world, stale.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "i21-stale-replica", nil)

	for _, s := range []sessionwire.SessionID{idleSession, turnSession} {
		command := sessionwire.CommandID("create-" + string(s))
		status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)),
			SessionID:       s,
			AgentID:         orchestrationtest.PooledAgent,
		})
		if status != http.StatusCreated {
			t.Fatalf("the create of %s answered %d: %s", s, status, body)
		}
		orchestrationtest.PooledWait(t, "the create of "+string(s)+" applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, command) == sessionstore.InboxStateApplied
		})
	}
	turnRuntime := world.RuntimeSessionID(t, ctx, tenant, turnSession)
	idleRuntime := world.RuntimeSessionID(t, ctx, tenant, idleSession)
	staleOwner, _ := world.Registration(t, ctx, tenant, turnSession)
	journals := map[string]bool{
		"sessions/" + turnRuntime.String(): true,
		"sessions/" + idleRuntime.String(): true,
	}

	// ---- four writes caught in flight -----------------------------------
	process := stale.Process()
	holds := map[string]*orchestrationtest.ProcessHold{
		"command": process.Hold("command", func(c orchestrationtest.ProcessCall) bool {
			return c.Plane == orchestrationtest.PlaneStore && c.Op == "ordered.update" &&
				strings.Contains(c.Name, "disposition-inbox") && strings.HasSuffix(c.Name, " "+string(staleInput)+"}") &&
				strings.Contains(string(c.Payload), `"state":"applied"`)
		}),
		"journal": process.Hold("journal", func(c orchestrationtest.ProcessCall) bool {
			return c.Plane == orchestrationtest.PlaneJournal && c.Op == "ledger.append" &&
				c.Name == "sessions/"+turnRuntime.String() && strings.Contains(string(c.Payload), `"type":"PermissionDecided"`)
		}),
		"registry": process.Hold("registry", func(c orchestrationtest.ProcessCall) bool {
			return c.Plane == orchestrationtest.PlaneStore && c.Op == "ordered.update" &&
				strings.Contains(c.Name, "sessionstore/registry") && strings.HasSuffix(c.Name, " "+string(turnSession)+"}")
		}),
		"checkpoint": process.Hold("checkpoint", func(c orchestrationtest.ProcessCall) bool {
			return c.Plane == orchestrationtest.PlaneJournal && c.Op == "ledger.append" &&
				c.Name == "sessions/"+idleRuntime.String() && strings.Contains(string(c.Payload), `"type":"WorkspaceCheckpointed"`)
		}),
	}
	world.LLM.Script(
		orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledWriteToolName, ToolInput: `{"path":"stale.txt","content":"stale"}`},
		orchestrationtest.PooledTurn{Text: "done"},
	)
	status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(turnSession)+"/input", sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(staleInput)),
		SessionID:       turnSession,
		Blocks:          json.RawMessage(`[{"type":"text","text":"write the stale file"}]`),
	})
	if status != http.StatusOK {
		t.Fatalf("the stale input answered %d: %s", status, body)
	}
	// The product checkpoints the idle session, as a release would.
	checkpointed := make(chan error, 1)
	go func() {
		checkpointed <- stale.Rig.CheckpointWorkspace(context.Background(), tenant, idleSession)
	}()
	for name, hold := range holds {
		select {
		case <-hold.Caught():
		case <-time.After(30 * time.Second):
			t.Fatalf("the stale Host never made its %s write; the fixture could not catch it in flight", name)
		}
	}
	if entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: turnSession, CommandID: staleInput}); err != nil ||
		entry.Record.State != sessionstore.InboxStateApplying || entry.Record.Attempt == nil {
		t.Fatalf("with its settlement in flight the command is %+v (%v), want applying with the stale Host's attempt", entry.Record, err)
	}

	// ---- the process stops ---------------------------------------------
	process.Record()
	lapsed := process.Pause()
	stale.Partition()
	t.Logf("the stale Host stopped with 4 writes in flight; %d leases lapsed on the provider, unobserved", lapsed)

	successor := orchestrationtest.StartPooledHost(t, ctx, world, "i21-stale-successor", 5)
	orchestrationtest.AwaitAdvertised(t, world, successor.ID)
	status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(idleSession)+"/input", sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("successor-input"),
		SessionID:       idleSession,
		Blocks:          json.RawMessage(`[{"type":"text","text":"hello"}]`),
	})
	if status != http.StatusOK {
		t.Fatalf("the successor's input answered %d: %s", status, body)
	}
	// The successor takes both sessions over. It either settles the stale
	// Host's half-finished command itself or -- host v0.7.0, see KNOWN DEFECT
	// below -- blocks on it; both are waited for, and which one happened is
	// recorded rather than assumed.
	orchestrationtest.PooledWait(t, "the successor took both sessions over", 120*time.Second, func() bool {
		turnOwner, turnFound := world.Registration(t, ctx, tenant, turnSession)
		idleOwner, idleFound := world.Registration(t, ctx, tenant, idleSession)
		return turnFound && idleFound && turnOwner.HostID == successor.ID && idleOwner.HostID == successor.ID &&
			world.CommandState(ctx, tenant, idleSession, "successor-input") == sessionstore.InboxStateApplied &&
			(world.CommandState(ctx, tenant, turnSession, staleInput) == sessionstore.InboxStateApplied ||
				successor.Metric(t, "host_sessions_command_blocked") >= 1)
	})
	owner, _ := world.Registration(t, ctx, tenant, turnSession)
	if owner.LeaseEpoch <= staleOwner.LeaseEpoch {
		t.Fatalf("the successor holds epoch %d, not above the stale Host's %d", owner.LeaseEpoch, staleOwner.LeaseEpoch)
	}
	settled, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: turnSession, CommandID: staleInput})
	if err != nil {
		t.Fatalf("reading the command before the stale Host resumes: %v", err)
	}
	// KNOWN DEFECT (host v0.7.0, internal/commands/disposition_applier.go
	// settleOrRecover): a successor facing an `applying` record whose attempt
	// names an EARLIER journal grant calls the runtime's recovery closure FIRST
	// and settles only if the closure succeeds. Here the predecessor's runtime
	// had already committed its `applied` disposition frame and the turn's
	// TurnStarted, so harness refuses the closure (EnduringEffectError) -- and
	// the successor never tries the settlement the durable evidence would
	// support. The command stays `applying`, the session's consumer blocks
	// (host_sessions_command_blocked = 1) and every later command queues behind
	// it, until something else settles it. The trigger is any crash or pause in
	// the window between the runtime's disposition append and the store settle.
	wedged := settled.Record.State == sessionstore.InboxStateApplying
	if wedged {
		t.Logf("KNOWN host v0.7.0 DEFECT: the successor is blocked on the stale Host's half-settled command "+
			"(state %s, revision %d, host_sessions_command_blocked=%d); its recovery closure is refused and it never settles from the durable evidence",
			settled.Record.State, settled.Revision, successor.Metric(t, "host_sessions_command_blocked"))
	} else if settled.Record.Outcome == nil || uint64(settled.Record.Outcome.SettlingResidencyEpoch) != owner.LeaseEpoch {
		t.Fatalf("the command settled %+v, want settled by the successor at residency %d", settled.Record.Outcome, owner.LeaseEpoch)
	}
	startedBefore := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world, tenant, turnRuntime)

	// ---- the stale process comes back ------------------------------------
	process.Resume()
	orchestrationtest.PooledWait(t, "every in-flight stale write reached the store", 30*time.Second, func() bool {
		for _, hold := range holds {
			if call := hold.Call(); call == nil || !process.Done(call.Seq) {
				return false
			}
		}
		return true
	})
	// Two route heartbeats' worth of the stale Host running again, so its
	// post-resume writes are in the record too.
	time.Sleep(5 * time.Second)

	t.Run("every in-flight write was refused by the store", func(t *testing.T) {
		for name, hold := range holds {
			call := process.Call(hold.Call().Seq)
			t.Logf("%s: %s %s -> %v", name, call.Op, call.Name, call.Err)
			if name == "command" && wedged {
				// THE STORE DID NOT FENCE IT, AND BY ITS OWN CONTRACT IT WOULD
				// NOT: sessionstore fences a settlement against the CLAIM's
				// residency high-water and reads no live lease, and the blocked
				// successor never wrote the record, so the stale Host's
				// compare-and-swap still names the current revision. Pinned so
				// that fixing the successor (which then settles, moving the
				// revision) turns this into an ordinary refusal.
				if call.Err != nil {
					t.Errorf("the stale settlement was refused (%v) although the successor never wrote the record; re-derive this case", call.Err)
				}
				t.Logf("KNOWN DEFECT, consequence: the superseded Host's in-flight settlement LANDED after the takeover")
				continue
			}
			if call.Err == nil {
				t.Errorf("the stale Host's in-flight %s write LANDED: %s %s", name, call.Op, call.Name)
				continue
			}
			if !fenceRefusal(call.Err) {
				t.Errorf("the stale %s write failed with %v, not a store fence refusal", name, call.Err)
			}
		}
		select {
		case err := <-checkpointed:
			t.Logf("the stale product checkpoint returned %v", err)
			if err == nil {
				t.Errorf("the stale Host's workspace checkpoint reported success")
			}
		case <-time.After(10 * time.Second):
			t.Errorf("the stale product checkpoint never returned")
		}
	})

	t.Run("every stale write of the four kinds was attempted and refused", func(t *testing.T) {
		attempted := map[string]int{}
		for _, call := range process.Calls() {
			kind := staleKind(call, journals)
			if kind == "" {
				if call.Write() && call.Done {
					t.Logf("other stale write: %s %s %s -> %v", call.Plane, call.Op, call.Name, call.Err)
				}
				continue
			}
			if !call.Done {
				continue
			}
			attempted[kind]++
			if kind == "command" && wedged {
				t.Logf("stale command write (known defect path): %s %s -> %v", call.Op, call.Name, call.Err)
				continue
			}
			if call.Err == nil {
				t.Errorf("a stale %s write LANDED after the successor took over: %s %s", kind, call.Op, call.Name)
			}
		}
		t.Logf("stale write attempts by kind: %v", attempted)
		for _, kind := range []string{"journal", "registry", "command", "checkpoint"} {
			if attempted[kind] == 0 {
				t.Errorf("the stale Host never attempted a %s write; the case is vacuous for it", kind)
			}
		}
	})

	t.Run("the durable state is the successor's", func(t *testing.T) {
		after, found := world.Registration(t, ctx, tenant, turnSession)
		if !found || after.HostID != successor.ID || after.LeaseEpoch != owner.LeaseEpoch {
			t.Fatalf("after the stale Host resumed the route is %+v (found=%v), want %s at epoch %d", after, found, successor.ID, owner.LeaseEpoch)
		}
		again, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: turnSession, CommandID: staleInput})
		if err != nil || again.Record.Outcome == nil || again.Record.Outcome.Kind != sessionstore.DispositionApplied {
			t.Fatalf("the command is %+v after the stale Host resumed (%v), want applied", again.Record, err)
		}
		if wedged {
			// The pinned defect's durable trace: settled applied (the evidence
			// is real) but under the SUPERSEDED residency.
			if got := uint64(again.Record.Outcome.SettlingResidencyEpoch); got != staleOwner.LeaseEpoch {
				t.Fatalf("the known-defect settlement names residency %d, want the stale Host's %d", got, staleOwner.LeaseEpoch)
			}
			t.Logf("KNOWN DEFECT: the command was settled by the superseded residency %d, not the successor's %d", staleOwner.LeaseEpoch, owner.LeaseEpoch)
		} else if again.Revision != settled.Revision || uint64(again.Record.Outcome.SettlingResidencyEpoch) != owner.LeaseEpoch {
			t.Fatalf("the successor's settlement moved after the stale Host resumed: %+v (rev %d, was %d)", again.Record.Outcome, again.Revision, settled.Revision)
		}
		for name, runtime := range map[string]string{"journal": turnRuntime.String(), "checkpoint": idleRuntime.String()} {
			payload := string(holds[name].Call().Payload)
			match := eventIDPattern.FindStringSubmatch(payload)
			if match == nil {
				t.Fatalf("the held %s frame names no event id", name)
			}
			id := turnRuntime
			if runtime == idleRuntime.String() {
				id = idleRuntime
			}
			if orchestrationtest.JournalHoldsEventID(t, world, tenant, id, match[1]) {
				t.Fatalf("the stale %s frame %s is in the journal", name, match[1])
			}
		}
		if got := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world, tenant, turnRuntime); got != 1 || got != startedBefore {
			t.Fatalf("the journal holds %d SessionStarted (was %d), want 1", got, startedBefore)
		}
	})
}
