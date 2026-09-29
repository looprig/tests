//go:build integration

package tests

import (
	"net/http"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/host"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// TestAnAttachInFlightWhenAHostStopsIsReleasedByTheStop is the deterministic
// reduction of the C1 wedge TestFactoryHostRaceStress hits under load.
//
// An attach passes Host admission and launches the session's runtime -- which
// takes the harness journal lease -- and THEN the Host begins to stop. The
// drain releases the sessions that were resident when it began
// (lifecycle.Drainer.StartDrain reads ResidentSessions once), and the attach
// in flight is not one of them. Here the drain is parked on ANOTHER session's
// checkpoint while the attach finishes, which is the interleaving a busy Host
// produces on its own.
//
// The claim is what a successor needs: once Stop has returned, nothing on the
// stopped Host holds the session's runtime journal lease, so the next Host can
// restore the session. When it is not so, every later attach of the session is
// refused at hydrate with journal.LeaseHeldError for as long as this process
// lives, and its commands never settle -- the stress's C1 failure, with its
// interrupt left applying and every input behind it pending.
//
// It failed on host v0.13.0 through v0.15.0; host v0.15.1 fixes it.
func TestAnAttachInFlightWhenAHostStopsIsReleasedByTheStop(t *testing.T) {
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	const (
		resident = sessionwire.SessionID("session-attach-drain-resident")
		late     = sessionwire.SessionID("session-attach-drain-late")
	)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{tenant},
	})
	// A Factory that admits but never places, so every attach below is the
	// case's own.
	served := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica: "attach-drain-replica", WithoutPendingCommands: true,
	})
	for _, s := range []sessionwire.SessionID{resident, late} {
		status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(string(s) + "-create"),
			SessionID:       s,
			AgentID:         orchestrationtest.PooledAgent,
		})
		if status != http.StatusCreated {
			t.Fatalf("the create of %s answered %d: %s", s, status, body)
		}
	}
	holding := orchestrationtest.NewHoldingCheckpointer()
	h := orchestrationtest.StartLifecycleHost(t, ctx, world, "attach-drain-host", 1, orchestrationtest.PooledHostConfig{
		Drain:            &drainOptions,
		WrapCheckpointer: holding.Wrap,
	})
	// Registered AFTER the Host, so it runs BEFORE the Host's own Stop cleanup.
	t.Cleanup(holding.Proceed)
	h.Attach(t, ctx, tenant, resident)

	// The late attach launches its runtime and parks there.
	launched, release := h.Rig.HoldLaunches()
	t.Cleanup(release)
	attached := make(chan error, 1)
	go func() {
		_, err := h.Service.Attach(ctx, host.AttachRequest{
			TenantID:               tenant,
			SessionID:              late,
			AgentID:                orchestrationtest.PooledAgent,
			Mode:                   sessionwire.HostLinkAttachModeCreate,
			RuntimeCompatibilityID: string(orchestrationtest.PooledCompatibility),
			ActorID:                "attach-drain-attacher",
		})
		attached <- err
	}()
	select {
	case <-launched:
	case <-time.After(30 * time.Second):
		t.Fatal("the late attach never launched its runtime")
	}
	lateRuntime := world.RuntimeSessionID(t, ctx, tenant, late)
	if held, _ := world.JournalLeaseHeld(t, ctx, tenant, lateRuntime); !held {
		t.Fatal("the parked launch does not hold the journal lease; the hold is in the wrong place")
	}

	// The Host begins to stop; its drain parks on the resident session's
	// checkpoint, AFTER it has read which sessions to release.
	stopped := h.BeginStop()
	orchestrationtest.PooledWait(t, "the drain reached the resident session's checkpoint", 30*time.Second, func() bool {
		return len(holding.Entered()) >= 1
	})

	// The late attach finishes while the drain is still running.
	release()
	var attachErr error
	select {
	case attachErr = <-attached:
	case <-time.After(30 * time.Second):
		t.Fatal("the late attach never returned after its launch was released")
	}
	holding.Proceed()
	var report host.DrainReport
	select {
	case report = <-stopped:
	case <-time.After(60 * time.Second):
		t.Fatal("Stop did not return")
	}
	t.Logf("late attach returned %v; checkpoints entered %v; Stop report %+v", attachErr, holding.Entered(), report)

	// Whatever the attach answered, the stopped Host may not keep the late
	// session's journal lease: a successor could never restore it.
	deadline := time.Now().Add(10 * time.Second)
	for {
		held, epoch := world.JournalLeaseHeld(t, ctx, tenant, lateRuntime)
		if !held {
			return
		}
		if time.Now().After(deadline) {
			// The consequence, as the stress sees it: a successor is refused.
			successor := orchestrationtest.StartPooledHost(t, ctx, world, "attach-drain-successor", 4)
			_, successorErr := successor.Service.Attach(ctx, host.AttachRequest{
				TenantID:               tenant,
				SessionID:              late,
				AgentID:                orchestrationtest.PooledAgent,
				Mode:                   sessionwire.HostLinkAttachModeCreate,
				RuntimeCompatibilityID: string(orchestrationtest.PooledCompatibility),
				ActorID:                "attach-drain-successor",
			})
			t.Logf("a successor's attach of %s answered: %v", late, successorErr)
			t.Fatalf("Stop returned (report %+v, no failure for %s) but the stopped Host still holds %s's runtime journal lease at epoch %d; the attach that installed it returned %v",
				report, late, late, epoch, attachErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
