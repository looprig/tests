//go:build integration && orchestration

// This file records, in executable form, the integration-lane cases that cannot
// be driven against the composed services released today, and drives the parts
// of them that CAN be.
//
// It is deliberately one file rather than four stubs. A task deferred in prose
// disappears -- I1.1-hostgone has been deferred twice already and survived only
// because someone re-homed it by name each time -- and a stub named after a task
// it does not perform is worse, because a skipped row and a passing row are the
// same colour on every dashboard. Nothing here skips. Every case asserts a
// PREMISE, and fails on the day that premise stops holding, which is the day the
// task it names becomes writable.
//
// What is recorded here, and what blocks it:
//
//   - I1.1 cases 3 and 4 (browser disconnect/reconnect with a cursor, Factory
//     killed mid-buffer) -- factory.New composes no ClientLink handler.
//   - I1.3 cases 2, 3 and 4 (fixed shard sweep, claim expiry and duplicate
//     placement, gate deadline intents) -- factory.New constructs neither
//     internal/admission.Reconciler nor internal/reconcile's gate sweep, and
//     both are internal to Factory.
//   - I1.4 (link backpressure blast radius) -- there is no DeliveryBinding and
//     no HostBinding in a composed Factory, so there is nothing to overflow.
//   - I2.3 (pooled drain ordering) -- *host.Host exposes nothing runnable and no
//     drain surface; the Drainer and its RPC are host-internal. Case 1's durable
//     observable IS reachable and is driven below.

package tests

import (
	"context"
	"net/http"
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

// TestIntegrationLaneBlockers holds the premises the blocked integration cases
// rest on, and drives the reachable half of I2.3 case 1.
func TestIntegrationLaneBlockers(t *testing.T) {
	ctx := coldReadContext(t)
	baseline := orchestrationtest.CaptureGoroutines()
	clock := orchestrationtest.NewClock(time.Unix(coldReadEpoch, 0))
	store := orchestrationtest.NewStoreFixture(t, ctx, clock)
	hostFixture := orchestrationtest.NewHostFixture(t, store, "orchestrationtest-blocked-host",
		"wss://blocked.internal.test/hostlink", blockedAgent, blockedCompatibility)
	served := orchestrationtest.NewFactoryFixture(t, store, clock)
	session := store.SeedSession(ctx, blockedAgent, string(blockedCompatibility))

	t.Run("I1.1 cases 3 and 4 need a ClientLink nothing composes", func(t *testing.T) {
		// A browser disconnect, three enduring events, a reconnect to the other
		// replica with the old cursor, and a Factory killed after buffering are
		// all assertions about ONE subscription's delivery. There is no
		// subscription: /v1/realtime is a pending route.
		orchestrationtest.AssertFactoryComposesNoLinkPlane(t, ctx, served)
	})

	t.Run("I1.4 has nothing to overflow", func(t *testing.T) {
		// I1.4's four cases name a DeliveryBinding, a HostBinding, the selected
		// Centrifuge slow-consumer threshold, and the enduring/ephemeral drop
		// policy. All four live behind the same absent composition as above, and
		// case 3 in particular says "do not encode an assumed library behavior"
		// -- which is precisely what a fake of both ends would do.
		//
		// factory.New ACCEPTS WithClientLinkLimits and WithHostLinkLimits and
		// validates them, so a composition looks configured for a plane that is
		// never built. That gap is the thing worth recording: the accessors
		// answer, and no request can reach anything they bound.
		clientLimits := served.Server.ClientLinkLimits()
		hostLimits := served.Server.HostLinkLimits()
		if clientLimits.MaxConnections <= 0 || clientLimits.PerConnectionQueueBytes <= 0 {
			t.Fatalf("the composed client link limits are unset: %+v", clientLimits)
		}
		if hostLimits == (factory.HostLinkLimits{}) {
			t.Fatalf("the composed host link limits are zero: %+v", hostLimits)
		}
		orchestrationtest.AssertFactoryComposesNoLinkPlane(t, ctx, served)
	})

	t.Run("I1.2, I1.3 case 1 and I2.3's admission half need an admission service", func(t *testing.T) {
		orchestrationtest.AssertFactoryComposesNoAdmissionPlane(t, ctx, served, session)
	})

	t.Run("I1.3 cases 2-4 need a sweep no composition runs", func(t *testing.T) {
		orchestrationtest.AssertFactoryExposesNoReconciler(t)
	})

	t.Run("no composed route reaches the durable command plane", func(t *testing.T) {
		// This is the half reflection cannot see. A sweep armed by a timer
		// inside Serve would export no method at all, and a control route wired
		// to a real admission service would reach Commands without exporting
		// one either. So the command plane is composed as a seam that PANICS,
		// and every route this build serves is driven through it.
		//
		// A panic is the SUBJECT here, not the verdict: the case asserts that a
		// full traversal completes without one. It is not an assertion kill and
		// must not be scored as one.
		commands := &orchestrationtest.PanicCommands{}
		placement := &orchestrationtest.PanicPlacement{}
		probe := orchestrationtest.NewFactoryFixtureWithSeams(t, store, clock, orchestrationtest.FactorySeams{
			Commands:  commands,
			Placement: placement,
		})
		for _, path := range []string{
			"/v1/bootstrap", "/v1/agents", "/v1/capabilities", "/v1/sessions", "/v1/csrf-token",
			orchestrationtest.SessionPath(session, "/status"),
			orchestrationtest.SessionPath(session, "/journal"),
			orchestrationtest.SessionPath(session, "/gates"),
			orchestrationtest.SessionPath(session, "/objects/obj-1"),
			"/v1/realtime",
		} {
			if status, body := probe.Get(t, ctx, path); status >= 500 && status != http.StatusServiceUnavailable &&
				status != http.StatusNotImplemented {
				t.Fatalf("GET %s answered %d: %s", path, status, body)
			}
		}
		for _, route := range orchestrationtest.NotComposedControlRoutes {
			probe.Post(t, ctx, orchestrationtest.SessionPath(session, route.Suffix), []byte(route.Body))
		}
		// Reaching here without a panic is the finding: WithCommands and
		// WithPlacementController are accepted, validated, stored and never
		// read. The day either is wired, this case dies with a panic naming the
		// method -- loudly, and at the composition change rather than at a
		// mutation of this test.
		//
		// Sanity: the probe really did serve. Without this the traversal could
		// have been empty and the absence of a panic would mean nothing.
		if status, _ := probe.Get(t, ctx, "/v1/sessions"); status != http.StatusOK {
			t.Fatalf("the panic-seam probe served %d for the tenant list; the traversal proves nothing", status)
		}
		// The verdict is this assertion rather than the absence of a crash.
		// net/http RECOVERS a panic raised inside a handler, so a route that
		// reached either seam would have closed its connection and left the
		// process alive; the seams record before they panic so that case is
		// attributable rather than merely noisy.
		if driven := commands.Driven(); len(driven) != 0 {
			t.Fatalf("a composed route reached the durable command plane: %v. Factory now wires "+
				"WithCommands, so runbook 07 I1.2 and I1.3 are no longer blocked on composition", driven)
		}
		if driven := placement.Driven(); len(driven) != 0 {
			t.Fatalf("a composed route reached the placement controller: %v. Factory now wires "+
				"WithPlacementController", driven)
		}
	})

	t.Run("I2.3 needs a runnable Host with a drain surface", func(t *testing.T) {
		orchestrationtest.AssertHostExposesNoRuntimeSurface(t)
		orchestrationtest.AssertHostExposesNoDrainSurface(t)
		if hostFixture.Host.Placement() != sessionwire.HostPlacementPooled {
			t.Fatalf("the drain premise was recorded against a non-pooled Host")
		}
	})

	t.Run("I2.3 case 1's durable observable: drain un-ranks target capacity", func(t *testing.T) {
		// This is the one leg of I2.3 that does not need a running Host. "Drain
		// un-ranks target capacity" is a durable fact about the target
		// directory, and the directory is the SAME seam Factory reads through:
		// the assertion below goes through the kit's real Directory adapter,
		// which is the object factory.New was composed with.
		//
		// What it does NOT prove is the ORDERING in I2.3 case 1 -- that the
		// un-rank happens BEFORE the Host stops admitting. Admission is the
		// Host's own state and no composed surface reports it. Do not read this
		// row as case 1 discharged; read it as case 1's observable half.
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
