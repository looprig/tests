//go:build integration

// This file records, in executable form, the integration-lane cases that still
// cannot be driven against the composed services, and it is much smaller than it
// was.
//
// # What A9.1 stage 2 removed from it
//
// Four premises this file used to hold have LIFTED, and each one's trip-wire
// fired on the day it did -- which is what they were for:
//
//   - `/v1/realtime` left the 501 table (`AssertFactoryComposesNoLinkPlane`).
//   - the control routes left 503 (`AssertFactoryComposesNoAdmissionPlane`).
//   - `*factory.Server` grew `Start` (`AssertFactoryExposesNoReconciler`).
//   - the command sweep reached the durable command plane, announced by the
//     recording panic seam from `Serve`'s own sweep goroutine.
//
// Those four assertions are DELETED rather than inverted. A trip-wire's whole
// job is to stop being true; keeping one after its blocker lifts turns it into a
// claim about the past that a later reader will mistake for a claim about now.
// The work they were holding is in factory_reconciliation_integration_test.go,
// factory_object_reads_integration_test.go and
// factory_reconnect_integration_test.go.
//
// # Two trip-wires did NOT fire, and one of those is a finding
//
// The Host trip-wires did not fire, and that is CORRECT: `host v0.1.0` is still
// `New` plus accessors, so I2.3's ordering content is still blocked. They stay.
//
// But `AssertFactoryComposesNoObjectPlane` and
// `AssertFactoryAdvertisesNoLaunchTargets` also did not fire, and they should
// have: `WithObjectPolicy` and `WithDepartment` both arrived. They went BLIND
// rather than staying true, because each probed a composition the KIT builds
// rather than a capability Factory has -- and the kit's composition still
// omitted the new options, so the answer never changed. That is the failure mode
// of a trip-wire written against one's own fixture, and it is why both are
// deleted here and replaced by cases that drive the real thing.

package tests

import (
	"context"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/host/department"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

const (
	blockedAgent         = sessionwire.AgentID("orchestrationtest-blocked-agent")
	blockedCompatibility = department.CompatibilityID("orchestrationtest-blocked-compat-1")
)

// TestIntegrationLaneBlockers holds the premises the still-blocked cases rest
// on, and drives the reachable half of I2.3 case 1.
func TestIntegrationLaneBlockers(t *testing.T) {
	ctx := coldReadContext(t)
	baseline := orchestrationtest.CaptureGoroutines()
	clock := orchestrationtest.NewClock(time.Unix(coldReadEpoch, 0))
	store := orchestrationtest.NewStoreFixture(t, ctx, clock)
	hostFixture := orchestrationtest.NewHostFixture(t, store, "orchestrationtest-blocked-host",
		"wss://blocked.internal.test/hostlink", blockedAgent, blockedCompatibility)
	served := orchestrationtest.NewFactoryFixture(t, store, clock)
	session := store.SeedSession(ctx, blockedAgent, string(blockedCompatibility))

	t.Run("I1.4 waits on a Host, not on Factory", func(t *testing.T) {
		// I1.4's four cases name a DeliveryBinding, a HostBinding, the selected
		// Centrifuge slow-consumer threshold, and the enduring/ephemeral drop
		// policy. Factory now composes all of that machinery -- the ClientLink
		// node, the HostLink pool and the routing table.
		//
		// What it cannot do is FILL a DeliveryBinding. A session channel's
		// records come from the Host live tail (routing.Tail, "the Host live
		// tail's control surface for one session"), which arrives over HostLink
		// from a running Host. There is no running Host, so there is no stream
		// to overflow, and I1.4 case 3's own instruction -- "record whether it
		// closes a subscription or physical link ... do not encode an assumed
		// library behavior" -- forbids the only alternative, which is a fake of
		// both ends.
		//
		// So the blocker MOVED rather than lifted: it was Factory's composition
		// and it is now Host's missing runtime surface, which the rows below
		// hold. The limits are still validated and still reachable, which is
		// what this row proves is not the obstacle.
		clientLimits := served.Server.ClientLinkLimits()
		hostLimits := served.Server.HostLinkLimits()
		if clientLimits.MaxConnections <= 0 || clientLimits.PerConnectionQueueBytes <= 0 {
			t.Fatalf("the composed client link limits are unset: %+v", clientLimits)
		}
		if hostLimits == (factory.HostLinkLimits{}) {
			t.Fatalf("the composed host link limits are zero: %+v", hostLimits)
		}
	})

	t.Run("I2.3 needs a runnable Host with a drain surface", func(t *testing.T) {
		// Unchanged by A9.1 stage 2, and deliberately re-asserted rather than
		// assumed: the Host lane is complete and host v0.1.0 is released, and
		// neither fact grew an exported composition or drain surface.
		orchestrationtest.AssertHostExposesNoRuntimeCapability(t)
		orchestrationtest.AssertHostExposesNoDrainCapability(t)
		if hostFixture.Host.Placement() != sessionwire.HostPlacementPooled {
			t.Fatalf("the drain premise was recorded against a non-pooled Host")
		}
	})

	t.Run("I2.3 case 1's durable observable: drain un-ranks target capacity", func(t *testing.T) {
		// The one leg of I2.3 that does not need a running Host. "Drain
		// un-ranks target capacity" is a durable fact about the target
		// directory, and the directory is the SAME seam Factory reads through.
		//
		// What it does NOT prove is the ORDERING in I2.3 case 1 -- that the
		// un-rank happens BEFORE the Host stops admitting. Admission is the
		// Host's own state and no composed surface reports it.
		key := sessionstore.HostTargetKey{
			AgentID:                blockedAgent,
			RuntimeCompatibilityID: string(blockedCompatibility),
			Placement:              sessionwire.HostPlacementPooled,
		}
		const draining = sessionwire.HostID("orchestrationtest-draining-host")
		const staying = sessionwire.HostID("orchestrationtest-staying-host")
		store.PublishTarget(ctx, key, draining, "wss://draining.internal.test/hostlink", 4)
		store.PublishTarget(ctx, key, staying, "wss://staying.internal.test/hostlink", 2)

		before := candidateHosts(t, ctx, served.Directory, key)
		if !before[draining] || !before[staying] {
			t.Fatalf("both hosts should advertise before the drain, got %v", before)
		}

		store.DrainTarget(ctx, key, draining)

		after := candidateHosts(t, ctx, served.Directory, key)
		if after[draining] {
			t.Fatalf("a drained host is still a placement candidate: %v", after)
		}
		// The peer is the control. Without it, a drain that withdrew EVERY row
		// would pass -- which is the blast radius the case exists to bound.
		if !after[staying] {
			t.Fatalf("draining one host withdrew its peer as well: %v", after)
		}
		// Un-ranking capacity is not releasing a session. A drain that also
		// cleared the registration would be a different, and wrong, operation.
		store.RegisterHost(ctx, session, draining, "wss://draining.internal.test/hostlink",
			blockedAgent, string(blockedCompatibility), 3)
		store.DrainTarget(ctx, key, draining)
		if _, found := store.HostRouteFor(ctx, served.Directory, session); !found {
			t.Fatalf("draining target capacity cleared a session's route; drain must not release residents")
		}
		store.ReleaseHost(ctx, session, 3)
	})

	orchestrationtest.AssertNoLeaks(t, ctx, orchestrationtest.LeakSources{
		Store:              store,
		Factories:          []*orchestrationtest.FactoryFixture{served},
		Host:               hostFixture,
		LinksUnobservable:  true,
		BaselineGoroutines: baseline,
	})
}

// candidateHosts pages the whole target directory and reports which Hosts are
// currently offering capacity.
//
// It pages to exhaustion rather than reading one page, because a short page is
// not the end of a target: HostTargetPage may return fewer rows than its limit
// while still issuing a continuation, and a helper that stopped at the first
// page would make "the drained host is absent" true for the wrong reason.
func candidateHosts(t *testing.T, ctx context.Context, directory *orchestrationtest.StoreDirectory, key sessionstore.HostTargetKey) map[sessionwire.HostID]bool {
	t.Helper()
	found := make(map[sessionwire.HostID]bool)
	var cursor sessionwire.Cursor
	for pages := 0; ; pages++ {
		if pages > 16 {
			t.Fatalf("the target directory did not terminate after %d pages", pages)
			return found
		}
		page, err := directory.Candidates(ctx, sessionstore.ListCompatibleHostsRequest{Key: key, Cursor: cursor})
		if err != nil {
			t.Fatalf("listing candidates: %v", err)
			return found
		}
		for _, report := range page.Hosts {
			found[report.HostID] = true
		}
		if page.NextCursor == "" {
			return found
		}
		cursor = page.NextCursor
	}
}
