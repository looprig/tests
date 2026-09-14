//go:build integration && orchestration

// This file is runbook 07 task I1.1: cold reads with every Host stopped, and the
// carried I1.1-hostgone criterion.
//
// # Why the extra build tag
//
// It composes the black-box kit in internal/orchestrationtest, which composes
// real `factory` and `host` objects. Neither module has a tag, so neither can
// appear in this module's go.mod, and the kit therefore only builds inside the
// workspace go.work. See the kit's doc.go: the `orchestration` constraint is
// what keeps this module's own standalone gate green today, and it is deleted in
// I0.1 half (b) once the two tags exist.
//
// # What this file does NOT do, and where to find out why
//
// I1.1's five cases do not all reach a composed Factory. Cases 3 and 4 --
// disconnect a browser, commit three enduring events, reconnect to the other
// Factory with the old cursor; kill Factory A after it has buffered -- are
// assertions about a ClientLink subscription. factory.New composes no ClientLink
// handler, /v1/realtime answers 501, and the kit's
// AssertFactoryComposesNoLinkPlane holds that premise. There is no cursor to
// carry across a reconnect because there is no connection. Those two cases are
// recorded as owed in factory_blocked_lanes_integration_test.go rather than
// approximated with a fake of both ends of a link that does not exist.

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/host/department"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

const (
	coldReadAgent         = sessionwire.AgentID("orchestrationtest-cold-agent")
	coldReadCompatibility = department.CompatibilityID("orchestrationtest-cold-compat-1")
	coldReadEpoch         = 1_700_000_000

	// residentLeaseEpoch is the epoch the stopped Host held. It is not 1, so a
	// case asserting the fence survived a release is asserting a value the
	// store could not have produced by accident.
	residentLeaseEpoch = 7
)

func coldReadContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestFactoryColdReadsWithEveryHostStopped is runbook 07 I1.1 cases 1, 2 and 5,
// and the carried I1.1-hostgone criterion's readable legs.
func TestFactoryColdReadsWithEveryHostStopped(t *testing.T) {
	ctx := coldReadContext(t)
	baseline := orchestrationtest.CaptureGoroutines()
	clock := orchestrationtest.NewClock(time.Unix(coldReadEpoch, 0))
	store := orchestrationtest.NewStoreFixture(t, ctx, clock)

	// A real Host is composed but never started -- there is nothing to start.
	// It is here as the OBSERVER of case 2: its Rig counts runtime launches and
	// its workspace provider counts materialized workspaces, so "no restore, no
	// runtime" is read off objects that would have recorded one.
	hostFixture := orchestrationtest.NewHostFixture(t, store, "orchestrationtest-cold-host",
		"wss://cold.internal.test/hostlink", coldReadAgent, coldReadCompatibility)

	// Two sessions with DIFFERENT durable histories, because "the Host is gone"
	// and "no Host was ever here" are different states and a case that only had
	// one of them could not tell a cold read from an unreachable one.
	resident := store.SeedSession(ctx, coldReadAgent, string(coldReadCompatibility))
	virgin := store.SeedSession(ctx, coldReadAgent, string(coldReadCompatibility))
	if resident == virgin {
		t.Fatalf("the kit minted the same session id twice (%q); the two-history case is degenerate", resident)
	}
	store.AppendPublicEvent(ctx, resident, "event-resident-1", []byte(`{"n":1}`))
	store.AppendPublicEvent(ctx, virgin, "event-virgin-1", []byte(`{"n":1}`))

	// The resident session really was resident: a live route is published and
	// then released, which is what a graceful Host shutdown leaves behind.
	store.RegisterHost(ctx, resident, "orchestrationtest-cold-host",
		"wss://cold.internal.test/hostlink", coldReadAgent, string(coldReadCompatibility), residentLeaseEpoch)

	observerA := orchestrationtest.NewObservedReader(store.Store)
	observerB := orchestrationtest.NewObservedReader(store.Store)
	factoryA := orchestrationtest.NewFactoryFixtureWithSeams(t, store, clock,
		orchestrationtest.FactorySeams{Reader: observerA})
	factoryB := orchestrationtest.NewFactoryFixtureWithSeams(t, store, clock,
		orchestrationtest.FactorySeams{Reader: observerB})

	t.Run("the premise is real: a route exists before every host stops", func(t *testing.T) {
		// Without this row the whole file is vacuous. "Reads work with the Host
		// gone" proves nothing if no Host was ever there: every assertion below
		// would pass against a store that has never held a registration, which
		// is the state `virgin` is in and which is deliberately kept alongside
		// as the control.
		if _, found := store.HostRouteFor(ctx, factoryA.Directory, resident); !found {
			t.Fatalf("session %q has no route before the host stops; the cold-read premise is unreachable", resident)
		}
		if _, found := store.HostRouteFor(ctx, factoryA.Directory, virgin); found {
			t.Fatalf("session %q was never registered yet reports a route", virgin)
		}
	})

	t.Run("stopping every host leaves a fenced tombstone, not an absence", func(t *testing.T) {
		store.ReleaseHost(ctx, resident, residentLeaseEpoch)

		if _, found := store.HostRouteFor(ctx, factoryA.Directory, resident); found {
			t.Fatalf("session %q still reports a routable owner after release", resident)
		}
		// A released registration is NOT handed back as a record: the store
		// refuses to vouch for a route it will not serve, and reports the
		// retained fence through the typed error instead. That is the only way
		// this package offers to observe a tombstone's epoch, so reading it any
		// other way would be reading something else.
		code, epoch := registrationFence(t, ctx, store, resident)
		if code != sessionstore.RegistryErrorReleased {
			t.Fatalf("the stopped host left registry code %q, want released", code)
		}
		// The fence is the half that does NOT expire. A reaper that deleted the
		// row would drop it, and the next write from a lost lease would be
		// admitted; asserting the epoch survives is asserting that.
		if epoch != residentLeaseEpoch {
			t.Fatalf("the tombstone carries lease epoch %d, want %d", epoch, residentLeaseEpoch)
		}
		// The control: a session no Host ever registered reports absence, and
		// absence carries no fence. Without this row "released" would be
		// indistinguishable from "there was never anything here".
		virginCode, virginEpoch := registrationFence(t, ctx, store, virgin)
		if virginCode != sessionstore.RegistryErrorNotFound {
			t.Fatalf("a never-registered session reports %q, want not_found", virginCode)
		}
		if virginEpoch != 0 {
			t.Fatalf("a never-registered session carries fence epoch %d, want 0", virginEpoch)
		}
	})

	t.Run("every served read answers through either factory", func(t *testing.T) {
		// I1.1 case 1. Each leg is asserted on BOTH replicas and the two answers
		// are compared byte for byte: one replica answering is a fact about one
		// process, and "either Factory" is the requirement.
		for _, session := range []sessionwire.SessionID{resident, virgin} {
			for _, suffix := range []string{"/status", "/journal", "/gates"} {
				path := orchestrationtest.SessionPath(session, suffix)
				statusA, bodyA := factoryA.Get(t, ctx, path)
				statusB, bodyB := factoryB.Get(t, ctx, path)
				if statusA != http.StatusOK || statusB != http.StatusOK {
					t.Fatalf("GET %s answered A=%d B=%d with every host stopped", path, statusA, statusB)
				}
				if !bytes.Equal(bodyA, bodyB) {
					t.Fatalf("GET %s differs between replicas:\nA %s\nB %s", path, bodyA, bodyB)
				}
				orchestrationtest.AssertPublicFrameIsClean(t, path, bodyA)
			}
		}

		statusA, listA := factoryA.Get(t, ctx, "/v1/sessions")
		statusB, listB := factoryB.Get(t, ctx, "/v1/sessions")
		if statusA != http.StatusOK || statusB != http.StatusOK {
			t.Fatalf("GET /v1/sessions answered A=%d B=%d", statusA, statusB)
		}
		if !bytes.Equal(listA, listB) {
			t.Fatalf("the tenant list differs between replicas:\nA %s\nB %s", listA, listB)
		}
		var page sessionwire.SessionPage
		if err := json.Unmarshal(listA, &page); err != nil {
			t.Fatalf("the served session page is not a Core SessionPage: %v", err)
		}
		seen := make(map[sessionwire.SessionID]bool, len(page.Sessions))
		for _, summary := range page.Sessions {
			seen[summary.SessionID] = true
		}
		for _, want := range []sessionwire.SessionID{resident, virgin} {
			if !seen[want] {
				t.Fatalf("session %q is absent from a cold tenant list: %s", want, listA)
			}
		}
	})

	t.Run("the agents leg is reachable but cannot discriminate", func(t *testing.T) {
		orchestrationtest.AssertFactoryAdvertisesNoLaunchTargets(t, ctx, factoryA)
	})

	t.Run("the objects leg of case 1 and of I1.1-hostgone is blocked", func(t *testing.T) {
		orchestrationtest.AssertFactoryComposesNoObjectPlane(t, ctx, factoryA, resident)
	})

	t.Run("opening a detail view causes no lease, placement, restore or runtime", func(t *testing.T) {
		// I1.1 case 2, and the readable half of I1.1-hostgone.
		//
		// The placement controller here is the kit's RECORDER rather than the
		// panic seam used in factory_blocked_lanes_integration_test.go, and the
		// split is deliberate. This case's claim is a behavioural one -- a
		// detail view places nothing -- and a recorder makes its failure an
		// ASSERTION naming what was placed. The panic seam is the day-of
		// trip-wire for the composition change and lives with the other
		// blocked-lane premises.
		probe := orchestrationtest.NewObservedReader(store.Store)
		probed := orchestrationtest.NewFactoryFixtureWithSeams(t, store, clock,
			orchestrationtest.FactorySeams{Reader: probe})

		beforeCode, beforeEpoch := registrationFence(t, ctx, store, resident)

		for _, suffix := range []string{"/status", "/journal", "/gates"} {
			path := orchestrationtest.SessionPath(resident, suffix)
			if status, body := probed.Get(t, ctx, path); status != http.StatusOK {
				t.Fatalf("GET %s answered %d: %s", path, status, body)
			}
		}

		afterCode, afterEpoch := registrationFence(t, ctx, store, resident)
		// No lease: neither the epoch high-water nor the tombstone moved. A
		// restore, a takeover or a placement would have had to move one of them.
		if afterEpoch != beforeEpoch {
			t.Fatalf("a detail view moved the lease epoch from %d to %d", beforeEpoch, afterEpoch)
		}
		if afterCode != beforeCode {
			t.Fatalf("a detail view changed the registry state from %q to %q", beforeCode, afterCode)
		}
		if afterCode != sessionstore.RegistryErrorReleased {
			t.Fatalf("the registration is %q after a detail view, want it still released", afterCode)
		}
		// No runtime and no workspace. These counters are read off the real
		// Host fixture's own seams, which would have recorded one.
		if launches := hostFixture.Rig.Launches(); launches != 0 {
			t.Fatalf("a detail view launched %d runtimes", launches)
		}
		if ensured := hostFixture.Workspaces.Ensured(); ensured != 0 {
			t.Fatalf("a detail view materialized %d workspaces", ensured)
		}
		// No placement. The recorder would have named the desired workload.
		if ensuredPlacements := probed.Placement.Ensured(); len(ensuredPlacements) != 0 {
			t.Fatalf("a detail view asked for %d placements: %+v", len(ensuredPlacements), ensuredPlacements)
		}
		if released := probed.Placement.Released(); released != 0 {
			t.Fatalf("a detail view released %d placements", released)
		}
		// And the read plane was the ONLY plane touched. Without this the three
		// counters above could all be zero because the request never reached
		// anything at all.
		if probe.Total() == 0 {
			t.Fatalf("the detail view made no durable read; the case is no longer exercising the read plane")
		}
		if got := probe.Count("ReadPublicJournal"); got != 1 {
			t.Fatalf("the detail view made %d journal reads, want exactly 1", got)
		}
		if got := probe.Count("ReadGates"); got != 1 {
			t.Fatalf("the detail view made %d gate reads, want exactly 1", got)
		}
	})

	t.Run("the initial journal view is one bounded tail at the captured tip", func(t *testing.T) {
		// I1.1 case 5. The session needs MORE history than one page, and the
		// page bound is a property of the REQUEST rather than of the answer: a
		// route that asked for everything and truncated its reply would produce
		// the same-sized page while doing unbounded work in the store. So the
		// recorded ReadPublicJournalRequest is what is asserted.
		const events = 150
		for i := 2; i <= events; i++ {
			store.AppendPublicEvent(ctx, resident,
				sessionwire.EventID(fmt.Sprintf("event-resident-%d", i)),
				[]byte(fmt.Sprintf(`{"n":%d}`, i)))
		}

		observerA.Reset()
		status, body := factoryA.Get(t, ctx, orchestrationtest.SessionPath(resident, "/journal"))
		if status != http.StatusOK {
			t.Fatalf("GET journal = %d: %s", status, body)
		}
		requests := observerA.JournalRequests()
		if len(requests) != 1 {
			t.Fatalf("the initial join made %d journal reads, want exactly 1 (a tip probe would race appends)", len(requests))
		}
		initial := requests[0]
		if !initial.Tail {
			t.Fatalf("the initial join did not ask for a tail: %+v", initial)
		}
		if initial.Cursor != "" || initial.FromSeq != 0 {
			t.Fatalf("the initial join named a position as well as a tail: %+v", initial)
		}
		if initial.Limit <= 0 || initial.Limit > 100 {
			t.Fatalf("the initial join asked for %d events, want a bounded page of at most 100", initial.Limit)
		}
		if initial.ScanLimit <= 0 {
			t.Fatalf("the initial join set no scan budget: %+v", initial)
		}

		var first sessionwire.JournalPage
		if err := json.Unmarshal(body, &first); err != nil {
			t.Fatalf("the served journal page is not a Core JournalPage: %v", err)
		}
		if err := first.Validate(); err != nil {
			t.Fatalf("the served journal page fails Core validation: %v", err)
		}
		if len(first.Events) == 0 {
			t.Fatalf("the tail page is empty with %d events appended: %s", events, body)
		}
		if len(first.Events) > initial.Limit {
			t.Fatalf("the tail returned %d events for a limit of %d", len(first.Events), initial.Limit)
		}
		if first.CapturedTip < events {
			t.Fatalf("the captured tip is %d with %d events appended", first.CapturedTip, events)
		}
		// The bound is on SEQUENCE POSITIONS, not on events, and the difference
		// is measurable here rather than theoretical: this journal's tip is
		// higher than its event count because a record can occupy a position
		// without yielding a public event, so this page carries fewer events
		// than its limit. A case that asserted len(Events) == Limit would be
		// asserting a journal with no withheld records -- and would go red the
		// first time a real one appeared, for no defect.
		span := first.Events[len(first.Events)-1].JournalSeq - first.Events[0].JournalSeq + 1
		if span > uint64(initial.Limit) {
			t.Fatalf("the tail spans %d sequence positions for a limit of %d", span, initial.Limit)
		}
		if len(first.Events) >= int(first.CapturedTip) {
			t.Fatalf("the tail returned %d events against a tip of %d; it is not a bounded window",
				len(first.Events), first.CapturedTip)
		}
		// One read, and the walk is COMPLETE at the tip it captured. An empty
		// cursor is the only thing that says so; a page that had been shortened
		// further would have to issue one, pinned to this same tip.
		if first.CoveredThrough != first.CapturedTip {
			t.Fatalf("the tail covered through %d of a captured tip of %d", first.CoveredThrough, first.CapturedTip)
		}
		if first.NextCursor != "" {
			t.Fatalf("a complete tail issued a continuation cursor %q", first.NextCursor)
		}
		// The tail is the END of the journal. This is the assertion that
		// distinguishes it from a page that started at sequence one, which is
		// the answer the route must NOT give.
		last := first.Events[len(first.Events)-1]
		if last.EventID != sessionwire.EventID(fmt.Sprintf("event-resident-%d", events)) {
			t.Fatalf("the tail's last event is %q, want the newest", last.EventID)
		}
		for _, event := range first.Events {
			if event.EventID == "event-resident-1" {
				t.Fatalf("the initial join returned the OLDEST event; older pages must be explicit")
			}
		}
		for i := 1; i < len(first.Events); i++ {
			if first.Events[i].JournalSeq <= first.Events[i-1].JournalSeq {
				t.Fatalf("the tail is not in sequence order at index %d", i)
			}
		}

		// Older history is reachable only by naming a position. That is the
		// "older pages are explicit" half, and it is rowed rather than assumed
		// because a route that silently walked backwards to the start would be
		// the unbounded read this bound exists to prevent.
		observerA.Reset()
		status, body = factoryA.Get(t, ctx, orchestrationtest.SessionPath(resident, "/journal?from_seq=1&limit=10"))
		if status != http.StatusOK {
			t.Fatalf("GET journal?from_seq=1 = %d: %s", status, body)
		}
		olderRequests := observerA.JournalRequests()
		if len(olderRequests) != 1 {
			t.Fatalf("an explicit older page made %d journal reads, want 1", len(olderRequests))
		}
		if olderRequests[0].Tail {
			t.Fatalf("an explicitly positioned page still asked for a tail: %+v", olderRequests[0])
		}
		if olderRequests[0].Limit != 10 {
			t.Fatalf("an explicit limit of 10 reached the store as %d", olderRequests[0].Limit)
		}
		var older sessionwire.JournalPage
		if err := json.Unmarshal(body, &older); err != nil {
			t.Fatalf("the older journal page is not a Core JournalPage: %v", err)
		}
		if len(older.Events) == 0 || older.Events[0].EventID != "event-resident-1" {
			t.Fatalf("an explicit from_seq=1 page does not start at the oldest event: %s", body)
		}

		// A caller's limit is CLAMPED, not honoured. The provider query work a
		// caller can demand is the amplification lever this bound exists for.
		observerA.Reset()
		status, _ = factoryA.Get(t, ctx, orchestrationtest.SessionPath(resident, "/journal?limit=1000"))
		if status == http.StatusOK {
			clamped := observerA.JournalRequests()
			if len(clamped) != 1 {
				t.Fatalf("an oversized limit made %d journal reads, want 1", len(clamped))
			}
			if clamped[0].Limit > 100 {
				t.Fatalf("a caller's limit of 1000 reached the store as %d", clamped[0].Limit)
			}
		} else if status != http.StatusBadRequest {
			t.Fatalf("an oversized limit answered %d, want 200 with a clamped page or 400", status)
		}
	})

	orchestrationtest.AssertNoLeaks(t, ctx, orchestrationtest.LeakSources{
		Store:              store,
		Factories:          []*orchestrationtest.FactoryFixture{factoryA, factoryB},
		Host:               hostFixture,
		LinksUnobservable:  true,
		BaselineGoroutines: baseline,
	})
}

// registrationFence reports the registry state of one session and the fence
// epoch its retained record carries.
//
// It exists because a session with no current route is reported as a typed
// ERROR rather than as a record: GetHostRegistration "returns one session's
// route, and returns it only while it is a route". The retained epoch travels on
// RegistryError, and the package offers no other way to observe it, so a helper
// that unwrapped the error into a boolean would discard the very value the
// tombstone assertions turn on.
//
// A live route returns the code "" and the registration's own epoch, so one
// helper covers both states and a case can compare them directly.
func registrationFence(t *testing.T, ctx context.Context, store *orchestrationtest.StoreFixture, session sessionwire.SessionID) (sessionstore.RegistryErrorCode, uint64) {
	t.Helper()
	entry, err := store.Store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{
		TenantID: store.Tenant, SessionID: session,
	})
	if err == nil {
		return "", entry.Registration.LeaseEpoch
	}
	var registryErr *sessionstore.RegistryError
	if !errors.As(err, &registryErr) {
		t.Fatalf("reading the registration of %q returned an untyped error: %v", session, err)
		return "", 0
	}
	return registryErr.Code, registryErr.Epoch
}
