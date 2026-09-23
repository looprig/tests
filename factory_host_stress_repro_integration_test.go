//go:build integration

// This file holds the MINIMAL REPRODUCTIONS of the defects the I3.1 race stress
// (factory_host_stress_integration_test.go) found in the released modules.
// Each is deterministic, runs in seconds to a minute, and asserts the CORRECT
// behaviour -- so each is red on the pins it was found on and turns green on
// the release that fixes it. They are kept beside the stress because the
// stress finds them only statistically; these name them.

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// TestAViewerOnAnotherReplicaIsToldOfEveryRecordAcrossAWarmRelease is the
// stress's V2 finding, reduced.
//
// A browser watches a session through replica W while every command goes
// through replica C. The session goes quiet and the Host WARM-RELEASES it,
// which commits SessionResidencyReleased (E7 here) after the Host has stopped
// relaying the tail -- so E7 never travels live. The next input, through C,
// re-places the session and its new tail starts at E8.
//
// Measured on factory v0.8.0 / host v0.7.1 / harness v0.37.1, every run: the
// browser receives [E8 E9 E10 E11 E12 E13] with no reset or tip hint in front
// of them, and W's next pass (~5s later) sends R13/13 -- a reset whose
// LastContiguous VOUCHES FOR THE HOLE. E7 is never surfaced to this client.
//
// Two things combine. The re-placed session's NEW tail reaches W before W's
// own pass has noticed the release, and W relays it as though it continued the
// old tail -- no tail-start reset goes in front of it; and
// factory's internal/routing deliveryBinding.advance starts a binding's
// contiguous run at the first record it forwards ("the first published
// enduring record starts the run wherever it is"), so a binding that joined a
// quiet session and first forwards E8 records lastContiguous=13. A replica
// whose own pass notices the release first (or that placed the session
// itself) sends T7 and an honest R0/13 instead -- which is why this needs W
// NOT to place and to run at the production reconcile cadence.
func TestAViewerOnAnotherReplicaIsToldOfEveryRecordAcrossAWarmRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	tenant := orchestrationtest.PooledTenantA
	// WithAskTool bridges the harness session's committed events into the
	// product's tail, which is what makes the release event a publication.
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{tenant}, WithAskTool: true,
	})
	world.LLM.Respond(stressRespond)
	pooled := orchestrationtest.StartSizedPooledHost(t, ctx, world, orchestrationtest.PooledHostConfig{
		ID: "repro-warm-host", Generation: 1, Capacity: 8, WarmTTL: 2 * time.Second,
	})
	orchestrationtest.AwaitAdvertised(t, world, pooled.ID)
	// W places nothing, so every re-placement is C's: with W placing, its own
	// bind puts a correct reset in front of the new tail. And W runs at
	// Factory's DEFAULT reconcile cadence (5s) rather than the kit's 200ms: the
	// defect needs the re-placement to land before W's next pass notices the
	// release, and at the kit's cadence that is a race it usually loses -- at
	// the production default it is the common case.
	defaults := factory.DefaultReconcileLimits()
	watched := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica: "repro-watched", WithoutPendingCommands: true,
		Interval: defaults.Interval, ClaimTTL: defaults.ClaimTTL,
	})
	commands := orchestrationtest.StartPooledFactory(t, ctx, world, "repro-commands", nil)

	const session = sessionwire.SessionID("repro-warm-gap")
	status, body := commands.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("create"), SessionID: session, AgentID: orchestrationtest.PooledAgent,
		Blocks: json.RawMessage(`[{"type":"text","text":"hello"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "the create applied", 60*time.Second, func() bool {
		return world.CommandState(ctx, tenant, session, "create") == sessionstore.InboxStateApplied
	})

	// A browser joining a QUIET session -- the create's turn has finished and
	// nothing is being committed -- so the replica has forwarded nothing to it
	// when the release lands. (A browser that joins mid-turn receives live
	// records first, which anchors the replica's run and hides the defect.)
	quietSince, quietTip := time.Now(), world.Tails.Tip(tenant, session)
	orchestrationtest.PooledWait(t, "the session went quiet", 30*time.Second, func() bool {
		if tip := world.Tails.Tip(tenant, session); tip != quietTip {
			quietSince, quietTip = time.Now(), tip
		}
		return quietTip > 0 && time.Since(quietSince) > 500*time.Millisecond
	})
	// Subscribe, then read the position.
	viewer, err := orchestrationtest.DialPooledViewer(ctx, watched, tenant)
	if err != nil {
		t.Fatal(err)
	}
	defer viewer.Close()
	if err := viewer.Subscribe(ctx, tenant, session); err != nil {
		t.Fatal(err)
	}
	start := reproCapturedTip(t, ctx, watched, session)

	for cycle := 1; cycle <= 4; cycle++ {
		orchestrationtest.PooledWait(t, "the Host warm-released the session", 30*time.Second, func() bool {
			samples, err := pooled.ScrapeMetrics()
			return err == nil && orchestrationtest.SumMetric(samples, "host_sessions") == 0
		})
		command := sessionwire.CommandID(fmt.Sprintf("input-%d", cycle))
		status, body := commands.Post(t, ctx, tenant, "/v1/sessions/"+string(session)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)), SessionID: session,
			Blocks: json.RawMessage(`[{"type":"text","text":"more"}]`),
		})
		if status != http.StatusOK {
			t.Fatalf("the input answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, string(command)+" applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, session, command) == sessionstore.InboxStateApplied
		})
		time.Sleep(500 * time.Millisecond)
		if _, err := world.CoveredThrough(t, ctx, tenant, session, viewer.Records(), start); err != nil {
			t.Fatalf("cycle %d: the browser that joined at %d saw %v\n  browser: %v\n  tail: %v",
				cycle, start, err, viewer.Arrivals(), world.Tails.Timeline(tenant, session))
		}
	}
	t.Logf("the browser joined at %d and received %v", start, viewer.Records())
}

// TestASessionIsRePlacedPromptlyOnAHostRestartedUnderItsHostID is the
// stress's V3 finding, reduced.
//
// A pooled Host is restarted the way a pod is: the old process drains, and a
// new one starts under the SAME HostID at a HIGHER generation on a NEW address.
// The next command must be placed on it as promptly as on any other Host.
//
// Measured on factory v0.8.0 / host v0.7.1: 61s, every run, against 3.2s for
// the control (the successor under a NEW HostID). Factory's HostLink pool keys
// a link by (HostID, tenant) alone -- internal/realtime/hostlink
// (*Pool).acquireLocked returns the cached link without comparing the target's
// endpoint or generation -- so the replica keeps redialling the dead address,
// answers ErrLinkReconnecting, skips the Host for placement, and recovers only
// when the idle reaper (Limits.IdleTimeout, 60s) collects the link. A viewer's
// route pins the link against that reaper, so a watched session's live tail on
// that replica can stay dark for longer.
func TestASessionIsRePlacedPromptlyOnAHostRestartedUnderItsHostID(t *testing.T) {
	for _, arm := range []struct {
		name      string
		successor sessionwire.HostID
		gen       uint64
	}{
		{"control: the successor is a new HostID", "repro-restart-host-b", 1},
		{"the successor keeps its HostID at a higher generation", "repro-restart-host", 2},
	} {
		t.Run(arm.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			tenant := orchestrationtest.PooledTenantA
			world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{Tenants: []sessionwire.TenantID{tenant}})
			cfg := orchestrationtest.PooledHostConfig{ID: "repro-restart-host", Generation: 1, Capacity: 8, WarmTTL: time.Hour}
			first := orchestrationtest.StartSizedPooledHost(t, ctx, world, cfg)
			orchestrationtest.AwaitAdvertised(t, world, first.ID)
			served := orchestrationtest.StartPooledFactory(t, ctx, world, "repro-restart-replica", nil)

			const session = sessionwire.SessionID("repro-restart")
			status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope("create"), SessionID: session, AgentID: orchestrationtest.PooledAgent,
			})
			if status != http.StatusCreated {
				t.Fatalf("the create answered %d: %s", status, body)
			}
			orchestrationtest.PooledWait(t, "the create applied", 60*time.Second, func() bool {
				return world.CommandState(ctx, tenant, session, "create") == sessionstore.InboxStateApplied
			})

			first.Stop()
			cfg.ID, cfg.Generation = arm.successor, arm.gen
			successor := orchestrationtest.StartSizedPooledHost(t, ctx, world, cfg)
			orchestrationtest.AwaitAdvertised(t, world, successor.ID)

			status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(session)+"/input", sessionwire.InputRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope("input"), SessionID: session,
				Blocks: json.RawMessage(`[{"type":"text","text":"again"}]`),
			})
			if status != http.StatusOK {
				t.Fatalf("the input answered %d: %s", status, body)
			}
			took := orchestrationtest.PooledWait(t, "the input applied on the successor", 150*time.Second, func() bool {
				return world.CommandState(ctx, tenant, session, "input") == sessionstore.InboxStateApplied
			})
			t.Logf("the input applied %v after admission; the successor restored %d sessions", took.Round(time.Millisecond), len(successor.Rig.Restores()))
			// Placement sweeps every 200ms and a claim lives 2s in this kit;
			// the control lands in ~3s. Twenty seconds is slack, not a target.
			if took > 20*time.Second {
				t.Errorf("re-placement onto the successor took %v; the control takes seconds", took)
			}
		})
	}
}

// reproCapturedTip reads one session's journal page through a replica and
// returns the captured tip it reports: where a joining browser positions itself.
func reproCapturedTip(t *testing.T, ctx context.Context, f *orchestrationtest.PooledFactory, session sessionwire.SessionID) uint64 {
	t.Helper()
	status, body := f.Get(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions/"+string(session)+"/journal")
	if status != http.StatusOK {
		t.Fatalf("the journal read answered %d: %s", status, body)
		return 0
	}
	var page sessionwire.JournalPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatalf("the journal page is not a Core JournalPage (%s): %v", body, err)
		return 0
	}
	return page.CapturedTip
}
