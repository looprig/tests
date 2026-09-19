//go:build integration

// This file closes the OBJECT half of the carried `I1.1-hostgone` criterion, and
// runbook 07 I1.1 case 1's objects leg with it.
//
// # Why this could not be written before, and what changed
//
// The criterion is U3.2 step 3's seventh item: a retained tool capture whose Host
// and workspace are already gone must still be readable, and the read must issue
// no Host request. U3.2 deferred it because its layer had no reader. U5.2
// inherited it and could not close it either. The first pass of this work
// recorded it as still owed for a sharper reason: `factory.New` composed no
// ObjectPolicy, the router checks the nil policy BEFORE the catalog summary and
// before any reader, and so the object route answered 503 for every session in
// every state. A row reading 503 and calling it "the Host is gone" would have
// been asserting a constant -- which is the exact defect ("a type-level
// tautology, not a probe") U3.2 named when it first deferred the item.
//
// A9.1 stage 2 added WithObjectPolicy and WithObjectStoreResolver. The route can
// now discriminate, so the criterion can be closed by DRIVING it rather than by
// recording its blocker.

package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

const objectLeaseEpoch = 11

// TestFactoryServesRetainedObjectsWithEveryHostStopped is the object half of
// `I1.1-hostgone`.
func TestFactoryServesRetainedObjectsWithEveryHostStopped(t *testing.T) {
	ctx := coldReadContext(t)
	baseline := orchestrationtest.CaptureGoroutines()
	clock := orchestrationtest.NewClock(time.Unix(coldReadEpoch, 0))
	store := orchestrationtest.NewStoreFixture(t, ctx, clock)
	hostFixture := orchestrationtest.NewHostFixture(t, store, "orchestrationtest-object-host",
		"wss://object.internal.test", coldReadAgent, coldReadCompatibility)

	// TWO sessions, because the object route BRANCHES on the session's binding:
	// a zero (legacy) binding reads through the composed SessionReader and never
	// consults the resolver, a non-zero one goes through the resolver and fails
	// closed without it. That is a structural input, invisible to any sweep of
	// object ids or byte counts, and the degenerate choice here would have been
	// one session.
	legacy := store.SeedSession(ctx, coldReadAgent, string(coldReadCompatibility))
	bound := store.SeedBoundSession(ctx, coldReadAgent, string(coldReadCompatibility), orchestrationtest.KitBinding())
	if legacy == bound {
		t.Fatalf("the kit minted the same session id twice (%q)", legacy)
	}

	capture := []byte("orchestrationtest captured tool output, retained beyond its workspace")
	legacyMeta := store.PutObject(ctx, legacy, sessionstore.ObjectKindToolResult, "text/plain; charset=utf-8", capture)
	boundMeta := store.PutObject(ctx, bound, sessionstore.ObjectKindToolResult, "text/plain; charset=utf-8", capture)

	// The premise: both sessions really were resident, and then every Host
	// stopped. Without this the file asserts that objects are readable for
	// sessions that never ran, which is not the criterion.
	store.RegisterHost(ctx, legacy, "orchestrationtest-object-host",
		"wss://object.internal.test", coldReadAgent, string(coldReadCompatibility), objectLeaseEpoch)
	store.RegisterHost(ctx, bound, "orchestrationtest-object-host",
		"wss://object.internal.test", coldReadAgent, string(coldReadCompatibility), objectLeaseEpoch)

	policy := orchestrationtest.NewAllowListObjectPolicy()
	reader := &orchestrationtest.StoreObjectReader{Store: store.Store}
	resolved := &orchestrationtest.RecordingObjectStoreResolver{Reader: reader}
	observer := orchestrationtest.NewObservedReader(store.Store)

	served := orchestrationtest.NewFactoryFixtureWithSeams(t, store, clock, orchestrationtest.FactorySeams{
		Reader:       observer,
		ObjectPolicy: policy,
		// The resolver records the binding and returns a reader DISTINCT from
		// the composed SessionReader. That is the composition-site substitution
		// the A9.1 gate had to fall back on: a panic at an HTTP seam is
		// recovered by the router and becomes a 500 indistinguishable from any
		// other internal failure, so it cannot carry identity. Two readers can.
		ObjectStore: resolved.Resolve,
	})

	t.Run("the premise: the workspace is gone and no host owns either session", func(t *testing.T) {
		store.ReleaseHost(ctx, legacy, objectLeaseEpoch)
		store.ReleaseHost(ctx, bound, objectLeaseEpoch)
		for _, session := range []sessionwire.SessionID{legacy, bound} {
			if _, found := store.HostRouteFor(ctx, served.Directory, session); found {
				t.Fatalf("session %q still reports a routable owner", session)
			}
		}
		// The workspace provider never materialized anything and is not asked
		// to: an object read must not reach a filesystem that no longer exists.
		if ensured := hostFixture.Workspaces.Ensured(); ensured != 0 {
			t.Fatalf("%d workspaces exist before the first object read", ensured)
		}
	})

	t.Run("an unpermitted reference is refused, not served", func(t *testing.T) {
		// The negative comes FIRST, deliberately. Run after the allow-list is
		// populated it would still pass; run here it also proves the allow-list
		// is what decides, rather than the route serving anything it is asked
		// for. Reachability is not discrimination.
		path := orchestrationtest.SessionPath(legacy, "/objects/"+legacyMeta.Reference.ObjectID)
		status, body := served.Get(t, ctx, path)
		// The status is asserted EXACTLY, and 500 is what it measures -- which
		// is a finding rather than a preference.
		//
		// A denial by an external ObjectPolicy cannot render as 403. Factory's
		// public identity package exports no authorization sentinel (the one a
		// denial must wrap, internal/identity.ErrUnauthorized, is unreachable
		// from another module), so authorizationFailure classifies any other
		// error as an internal failure. A deployment's own policy therefore
		// cannot tell a caller "you may not read this"; it can only produce
		// "the request could not be completed". That is the A9.1 authorization
		// sentinel gap, observed live on the object route.
		//
		// Asserting merely "not 200" would NOT discriminate: measured by
		// mutation, disabling the policy-error check entirely still refuses,
		// because an empty ObjectKind fails downstream with 400 invalid_request.
		// The row would have passed with authorization switched off.
		if status != http.StatusInternalServerError {
			t.Fatalf("an unpermitted object reference answered %d (%s), want the measured 500. "+
				"If it is now 403 the authorization sentinel has been exported and this row should "+
				"assert the denial properly", status, body)
		}
		asked := policy.Asked()
		if len(asked) != 1 || asked[0].ObjectID != legacyMeta.Reference.ObjectID {
			t.Fatalf("the policy was asked %v, want exactly the requested reference", asked)
		}
		if reads := reader.Reads() + observer.Count("GetObject"); reads != 0 {
			t.Fatalf("a refused read still fetched %d object bodies", reads)
		}
	})

	t.Run("a retained capture is served after its host is gone", func(t *testing.T) {
		policy.Permit(legacyMeta.Reference, sessionstore.ObjectKindToolResult)
		path := orchestrationtest.SessionPath(legacy, "/objects/"+legacyMeta.Reference.ObjectID)
		status, body := served.Get(t, ctx, path)
		if status != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", path, status, body)
		}
		if !bytes.Equal(body, capture) {
			t.Fatalf("the served bytes differ from the retained capture:\ngot  %q\nwant %q", body, capture)
		}
		orchestrationtest.AssertPublicFrameIsClean(t, "object body", body)
	})

	t.Run("its metadata is served too, and names the same reference", func(t *testing.T) {
		path := orchestrationtest.SessionPath(legacy, "/objects/"+legacyMeta.Reference.ObjectID+"/metadata")
		status, body := served.Get(t, ctx, path)
		if status != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", path, status, body)
		}
		var metadata sessionwire.ObjectMetadata
		if err := json.Unmarshal(body, &metadata); err != nil {
			t.Fatalf("the served metadata is not Core ObjectMetadata: %v", err)
		}
		if metadata.Reference != legacyMeta.Reference {
			t.Fatalf("the served metadata names %v, want %v", metadata.Reference, legacyMeta.Reference)
		}
		if metadata.SizeBytes != uint64(len(capture)) {
			t.Fatalf("the served metadata reports %d bytes, want %d", metadata.SizeBytes, len(capture))
		}
	})

	t.Run("the read issues no host request and leaves no residency behind", func(t *testing.T) {
		// This is the criterion's load-bearing half, and it is a PROBE rather
		// than a type-level tautology: every object served above went through a
		// composition that also holds a real placement recorder, a real Host
		// fixture and a real registry, each of which would have recorded an
		// interaction.
		code, epoch := registrationFence(t, ctx, store, legacy)
		if code != sessionstore.RegistryErrorReleased || epoch != objectLeaseEpoch {
			t.Fatalf("the object reads moved the registry to %q at epoch %d", code, epoch)
		}
		if launches := hostFixture.Rig.Launches(); launches != 0 {
			t.Fatalf("an object read launched %d runtimes", launches)
		}
		if ensured := hostFixture.Workspaces.Ensured(); ensured != 0 {
			t.Fatalf("an object read materialized %d workspaces", ensured)
		}
		if touched := served.Placement.Touched(); touched != 0 {
			t.Fatalf("an object read made %d workload-controller calls: %+v", touched, served.Placement.Ensured())
		}
		// And the read really happened, so the four zeros above are not the
		// zeros of a request that never arrived.
		if reads := observer.Count("GetObject"); reads == 0 {
			t.Fatalf("no object body was fetched; the case is no longer exercising the object plane")
		}
	})

	t.Run("a BOUND session resolves its store instead of reading through the reader", func(t *testing.T) {
		// The other arm of the branch, and the reason both arms are here: the
		// router branches on `entry.Record.Binding != zero`, so a file that
		// seeded only legacy sessions would sweep one arm and report it as the
		// route.
		policy.Permit(boundMeta.Reference, sessionstore.ObjectKindToolResult)
		legacyBefore := observer.Count("GetObject")
		path := orchestrationtest.SessionPath(bound, "/objects/"+boundMeta.Reference.ObjectID)
		status, body := served.Get(t, ctx, path)
		if status != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", path, status, body)
		}
		if !bytes.Equal(body, capture) {
			t.Fatalf("the resolved store served different bytes: %q", body)
		}
		called := resolved.Called()
		if len(called) != 1 {
			t.Fatalf("the resolver was called %d times for a bound session, want 1", len(called))
		}
		if called[0] != orchestrationtest.KitBinding() {
			t.Fatalf("the resolver was handed %+v, want the session's own binding", called[0])
		}
		// Identity, not status: the RESOLVED reader served it and the legacy
		// one did not. Without this pair the row would pass against a router
		// that consulted the resolver and then read through the fallback anyway.
		if reader.Reads() != 1 {
			t.Fatalf("the resolved reader served %d bodies, want 1", reader.Reads())
		}
		if after := observer.Count("GetObject"); after != legacyBefore {
			t.Fatalf("a bound session's read also went through the legacy reader (%d -> %d)", legacyBefore, after)
		}
	})

	t.Run("a bound session whose store cannot be resolved fails closed", func(t *testing.T) {
		// The negative of the arm above, on its own composition because the
		// resolver is fixed at composition time. A refusing resolver must not
		// fall back to the legacy reader: that fallback is exactly the hazard
		// the A9.1 stage-1 review flagged when it warned that an ObjectPolicy
		// supplied without a resolver makes the legacy path live.
		refusing := &orchestrationtest.RecordingObjectStoreResolver{}
		blind := orchestrationtest.NewObservedReader(store.Store)
		closed := orchestrationtest.NewFactoryFixtureWithSeams(t, store, clock, orchestrationtest.FactorySeams{
			Reader:       blind,
			ObjectPolicy: policy,
			ObjectStore:  refusing.Resolve,
		})
		path := orchestrationtest.SessionPath(bound, "/objects/"+boundMeta.Reference.ObjectID)
		if status, body := closed.Get(t, ctx, path); status == http.StatusOK {
			t.Fatalf("a refusing resolver still served an object body: %s", body)
		}
		if len(refusing.Called()) != 1 {
			t.Fatalf("the refusing resolver was called %d times, want 1", len(refusing.Called()))
		}
		if fell := blind.Count("GetObject"); fell != 0 {
			t.Fatalf("a refused resolution fell back to the legacy reader %d times", fell)
		}
		closed.Stop(t)
	})

	orchestrationtest.AssertNoLeaks(t, ctx, orchestrationtest.LeakSources{
		Store:              store,
		Factories:          []*orchestrationtest.FactoryFixture{served},
		Host:               hostFixture,
		LinksUnobservable:  true,
		BaselineGoroutines: baseline,
	})
}
