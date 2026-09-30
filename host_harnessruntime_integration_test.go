//go:build integration

// This file is the harnessruntime lane host v0.16.0's design ruling owes
// (Gap A): host/harnessruntime.Target -- the harness adapter Host always used,
// made public -- composed into REAL Hosts behind a REAL Factory, over real
// harness rigs and journals. It proves end to end what Target declares by
// construction:
//
//	create     a create's first message reaches the model and settles applied;
//	input      an input reaches the model and settles applied;
//	failover   a Host that dies with an attempt IN FLIGHT (its journal write
//	           caught before it lands) is taken over, and the successor CLOSES
//	           the attempt not_applied under a strictly later journal grant --
//	           department.AttemptCloser wired through the public adapter;
//	fault      a journal write that fails latches the runtime's persistence
//	           fault, Host gives the session up (department.PersistenceFaults),
//	           and a restored runtime takes the next input.
//
// It also runs host v0.16.0's exported departmenttest.RunRuntimeConformance
// against both runtimes this kit registers: harnessruntime.Target and the kit's
// own pooledSession adapter.

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/host/department/departmenttest"
	"github.com/looprig/inference"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// laneLog collects Host diagnostics from many goroutines.
type laneLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *laneLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *laneLog) contains(needle string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Contains(l.buf.String(), needle)
}

// TestKitRuntimesPassRuntimeConformance runs host v0.16.0's exported runtime
// conformance against every runtime a kit Host registers. A capability that
// compiles but is not wired -- a recovery method over a session that cannot
// honour it, a principal dropped in translation -- fails here rather than in a
// failover.
//
// The ComposedHost's FakeRuntime is deliberately absent: it implements neither
// recovery capability, and its Host opts out with RuntimeProfileBestEffort.
func TestKitRuntimesPassRuntimeConformance(t *testing.T) {
	for _, arm := range []struct {
		name    string
		harness bool
	}{
		{name: "kit pooledSession adapter", harness: false},
		{name: "harnessruntime.Target", harness: true},
	} {
		t.Run(arm.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			t.Cleanup(cancel)
			world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
				Tenants:        []sessionwire.TenantID{orchestrationtest.PooledTenantA},
				HarnessRuntime: arm.harness,
			})
			world.LLM.Respond(func(inference.Request) orchestrationtest.PooledTurn {
				return orchestrationtest.PooledTurn{Text: "conformance ok"}
			})
			target, launch := world.ConformanceTarget(t)
			departmenttest.RunRuntimeConformance(t, target, launch)
		})
	}
}

// TestHarnessRuntimeTargetCreatesAppliesFailsOverAndSupervisesFaults is the
// harnessruntime cross-module lane.
func TestHarnessRuntimeTargetCreatesAppliesFailsOverAndSupervisesFaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	tenant := orchestrationtest.PooledTenantA
	logs := &laneLog{}
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:        []sessionwire.TenantID{tenant},
		HarnessRuntime: true,
		HostLogs:       logs,
	})
	world.LLM.Respond(func(inference.Request) orchestrationtest.PooledTurn {
		return orchestrationtest.PooledTurn{Text: "noted"}
	})
	first := orchestrationtest.StartLifecycleHost(t, ctx, world, "hr-first-host", 3, orchestrationtest.PooledHostConfig{Mortal: true})
	orchestrationtest.AwaitAdvertised(t, world, first.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "hr-replica", nil)

	const (
		s             = sessionwire.SessionID("session-harnessruntime")
		createWord    = "QUINCE"
		inputWord     = "MEDLAR"
		strandedWord  = "LOQUAT"
		afterWord     = "SLOE"
		faultedWord   = "ROWAN"
		recoveredWord = "BULLACE"
	)
	input := func(command sessionwire.CommandID, word string) {
		t.Helper()
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)),
			SessionID:       s,
			Blocks:          json.RawMessage(`[{"type":"text","text":"` + word + `"}]`),
		})
		if status != http.StatusOK {
			t.Fatalf("the input %s answered %d: %s", command, status, body)
		}
	}
	record := func(command sessionwire.CommandID) sessionstore.DispositionInboxRecord {
		t.Helper()
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: tenant, SessionID: s, CommandID: command,
		})
		if err != nil {
			t.Fatalf("reading %s: %v", command, err)
		}
		return entry.Record
	}

	t.Run("a create's first message reaches the model and settles applied", func(t *testing.T) {
		status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("hr-create"),
			SessionID:       s,
			AgentID:         orchestrationtest.PooledAgent,
			Blocks:          json.RawMessage(`[{"type":"text","text":"` + createWord + `"}]`),
		})
		if status != http.StatusCreated {
			t.Fatalf("the create answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "the create applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "hr-create") == sessionstore.InboxStateApplied
		})
		if !world.LLM.SawInRequest(0, createWord) {
			t.Fatalf("the create's first message never reached the model")
		}
		if creates := first.Rig.Creates(); len(creates) != 1 {
			t.Fatalf("the first Host launched %+v, want one create through harnessruntime", creates)
		}
	})

	t.Run("an input reaches the model and settles applied", func(t *testing.T) {
		input("hr-input", inputWord)
		orchestrationtest.PooledWait(t, "the input applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "hr-input") == sessionstore.InboxStateApplied
		})
		if !world.LLM.SawInRequest(0, inputWord) {
			t.Fatalf("the input never reached the model")
		}
	})

	outer := t
	runtimeID := world.RuntimeSessionID(t, ctx, tenant, s)
	journalName := "sessions/" + runtimeID.String()
	var successor *orchestrationtest.PooledHost

	t.Run("a Host dying with an attempt in flight is closed not_applied by its successor", func(t *testing.T) {
		// THE ATTEMPT IS DURABLE, ITS JOURNAL WRITE IS NOT. Host begins the
		// attempt on the store plane before it hands the command to the
		// runtime; the runtime's first journal append naming the command is
		// held in flight, so when the process dies nothing the command caused
		// is in the journal. Only a successor can close that attempt, and only
		// through the runtime's AttemptCloser.
		process := first.Process()
		hold := process.Hold("stranded input", func(c orchestrationtest.ProcessCall) bool {
			return c.Plane == orchestrationtest.PlaneJournal && c.Op == "ledger.append" &&
				c.Name == journalName && strings.Contains(string(c.Payload), "hr-stranded")
		})
		requestsBefore := len(world.LLM.Requests())
		input("hr-stranded", strandedWord)
		select {
		case <-hold.Caught():
		case <-time.After(60 * time.Second):
			t.Fatalf("the first Host never began journalling the stranded input; the attempt could not be caught in flight")
		}
		stranded := record("hr-stranded")
		if stranded.State != sessionstore.InboxStateApplying || stranded.Attempt == nil || stranded.Outcome != nil {
			t.Fatalf("with its journal write in flight the input is %+v, want applying with the first Host's attempt and no outcome", stranded)
		}
		attempt := stranded.Attempt.AttemptID

		lapsed := first.Kill(t)
		t.Logf("host %s died with attempt %s in flight; %d leases lapsed", first.ID, attempt, lapsed)

		// Started on the TEST's t, not this subtest's: a Host's cleanup stops
		// it, and the fault arm below needs it running.
		successor = orchestrationtest.StartLifecycleHost(outer, ctx, world, "hr-successor-host", 4, orchestrationtest.PooledHostConfig{Mortal: true})
		orchestrationtest.AwaitAdvertised(t, world, successor.ID)
		input("hr-after", afterWord)

		orchestrationtest.PooledWait(t, "the successor closed the stranded attempt", 120*time.Second, func() bool {
			state := world.CommandState(ctx, tenant, s, "hr-stranded")
			return state == sessionstore.InboxStateRejected || state == sessionstore.InboxStateApplied
		})
		closed := record("hr-stranded")
		t.Logf("the stranded input closed: state=%q outcome=%+v", closed.State, closed.Outcome)
		if closed.Outcome == nil || closed.Outcome.Kind != sessionstore.DispositionNotApplied {
			t.Fatalf("the stranded input closed %q with outcome %+v, want not_applied: its journal write never landed", closed.State, closed.Outcome)
		}
		if closed.Outcome.AttemptID != attempt {
			t.Fatalf("the closure names attempt %s, want the dead Host's %s", closed.Outcome.AttemptID, attempt)
		}
		if closed.Outcome.AuthorJournalEpoch <= closed.Outcome.AttemptJournalEpoch {
			t.Fatalf("the closure was authored at journal epoch %d against an attempt at %d, want strictly later",
				closed.Outcome.AuthorJournalEpoch, closed.Outcome.AttemptJournalEpoch)
		}

		orchestrationtest.PooledWait(t, "the input behind the closed attempt applied", 120*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "hr-after") == sessionstore.InboxStateApplied
		})
		if !world.LLM.SawInRequest(requestsBefore, afterWord) {
			t.Fatalf("the input behind the closed attempt never reached the model")
		}
		if world.LLM.SawInRequest(requestsBefore, strandedWord) {
			t.Fatalf("the stranded input's words reached the model; a closed attempt must not be replayed")
		}
		if creates, restores := successor.Rig.Creates(), successor.Rig.Restores(); len(creates) != 0 || len(restores) != 1 || restores[0].ID != runtimeID {
			t.Fatalf("the successor launched creates %+v and restores %+v, want exactly one restore of %s", creates, restores, runtimeID)
		}
		if got := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world, tenant, runtimeID); got != 1 {
			t.Fatalf("the journal holds %d SessionStarted, want 1: the session was restarted, not restored", got)
		}
	})
	if successor == nil {
		t.Fatal("the failover arm did not start a successor; the fault arm needs one")
	}

	t.Run("a latched persistence fault is given up and a restored runtime takes the next input", func(t *testing.T) {
		// ONE JOURNAL WRITE FAILS in an otherwise healthy process: the
		// runtime's first public record of the faulted input's turn. harness
		// latches a persistence fault on it, the runtime refuses everything
		// after, and host v0.16.0 requires the runtime to REPORT it --
		// department.PersistenceFaults, which harnessruntime.Target declares --
		// so Host gives the session up instead of keeping a dead runtime
		// resident.
		restoresBefore := len(successor.Rig.Restores())
		fault := successor.Process().Fail("turn journal write", func(c orchestrationtest.ProcessCall) bool {
			return c.Plane == orchestrationtest.PlaneJournal && c.Op == "ledger.append" &&
				c.Name == journalName && strings.Contains(string(c.Payload), `"type":"TurnStarted"`)
		})
		input("hr-faulted", faultedWord)
		select {
		case <-fault.Fired():
		case <-time.After(60 * time.Second):
			t.Fatalf("the faulted input's turn never reached the journal; the fault could not be injected")
		}
		orchestrationtest.PooledWait(t, "Host gave the faulted session up", 60*time.Second, func() bool {
			return logs.contains("persistence_fault")
		})

		requestsBefore := len(world.LLM.Requests())
		input("hr-recovered", recoveredWord)
		orchestrationtest.PooledWait(t, "a restored runtime applied the next input", 120*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "hr-recovered") == sessionstore.InboxStateApplied
		})
		if !world.LLM.SawInRequest(requestsBefore, recoveredWord) {
			t.Fatalf("the input after the fault never reached the model")
		}
		if restores := successor.Rig.Restores(); len(restores) <= restoresBefore {
			t.Fatalf("no runtime was restored after the fault (restores %+v); the faulted runtime stayed resident", restores)
		}
		faulted := record("hr-faulted")
		t.Logf("the faulted input settled: state=%q outcome=%+v", faulted.State, faulted.Outcome)
		if faulted.State != sessionstore.InboxStateApplied && faulted.State != sessionstore.InboxStateRejected {
			t.Fatalf("the faulted input is %q; the command stream must not be left blocked behind it", faulted.State)
		}
	})
}
