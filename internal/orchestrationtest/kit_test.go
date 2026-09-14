//go:build integration && orchestration

package orchestrationtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/sessionstore"
)

const (
	kitAgent         = sessionwire.AgentID("orchestrationtest-agent")
	kitCompatibility = department.CompatibilityID("orchestrationtest-compat-1")
	kitEpoch         = 1_700_000_000
)

// recordingTB is a TB that RECORDS failures instead of ending the test, so the
// kit's own assertions can be proven capable of failing. testing.T cannot be
// used for that: T.Fatalf runs runtime.Goexit and T.Errorf fails the very test
// doing the proving.
//
// Fatalf deliberately does NOT stop the caller. Every kit assertion is written
// to return immediately after calling Fatalf, and that is a property this type
// exists to keep honest: if one ever forgets, a positive control panics here
// rather than passing quietly.
type recordingTB struct {
	mu       sync.Mutex
	failures []string
	cleanups []func()
}

func (r *recordingTB) Helper()             {}
func (r *recordingTB) Name() string        { return "recordingTB" }
func (r *recordingTB) Logf(string, ...any) {}
func (r *recordingTB) Cleanup(fn func()) {
	r.mu.Lock()
	r.cleanups = append(r.cleanups, fn)
	r.mu.Unlock()
}
func (r *recordingTB) Errorf(format string, args ...any) { r.record(format, args...) }
func (r *recordingTB) Fatalf(format string, args ...any) { r.record(format, args...) }

func (r *recordingTB) record(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func (r *recordingTB) Failures() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.failures...)
}

func (r *recordingTB) runCleanups() {
	r.mu.Lock()
	pending := r.cleanups
	r.cleanups = nil
	r.mu.Unlock()
	for i := len(pending) - 1; i >= 0; i-- {
		pending[i]()
	}
}

// mustFail is the positive-control harness: it runs body against a recording TB
// and fails the real test unless body reported a failure mentioning want.
func mustFail(t *testing.T, want string, body func(tb TB)) {
	t.Helper()
	rec := &recordingTB{}
	defer rec.runCleanups()
	body(rec)
	failures := rec.Failures()
	if len(failures) == 0 {
		t.Fatalf("positive control: the assertion reported no failure; it cannot fail and is therefore not evidence")
	}
	for _, failure := range failures {
		if strings.Contains(failure, want) {
			return
		}
	}
	t.Fatalf("positive control: failures %q, none mentioning %q", failures, want)
}

// fillHostSeams supplies the collaborator seams a validation case does not care
// about, so the case's own refusal is the only thing under test.
func fillHostSeams(options *host.Options, store *StoreFixture, from *HostFixture) {
	options.HostID = "orchestrationtest-host-seam"
	options.InternalEndpoint = "wss://seam.internal.test/hostlink"
	options.Department = from.Department
	options.SessionStore = &CatalogSessionStore{Store: store.Store}
	options.Workspaces = from.Workspaces
	options.Clock = store.Clock
	options.Auth = from.Auth
}

func kitContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestOrchestrationTestKit is runbook 07 I0.1's acceptance test for half (a).
func TestOrchestrationTestKit(t *testing.T) {
	ctx := kitContext(t)
	baseline := CaptureGoroutines()
	clock := NewClock(time.Unix(kitEpoch, 0))
	store := NewStoreFixture(t, ctx, clock)

	t.Run("the durable plane is real and shared", func(t *testing.T) {
		session := store.SeedSession(ctx, kitAgent, string(kitCompatibility))
		seq := store.AppendPublicEvent(ctx, session, "event-1", []byte(`{"type":"turn.completed"}`))
		if seq == 0 {
			t.Fatalf("append returned sequence 0")
		}
		// THREE events, not one. One record per journal is the degenerate
		// structural input for this axis: with a single event, order,
		// sequence monotonicity and coverage are all trivially satisfied by
		// any implementation, correct or not.
		second := store.AppendPublicEvent(ctx, session, "event-2", []byte(`{"type":"turn.started"}`))
		third := store.AppendPublicEvent(ctx, session, "event-3", []byte(`{"type":"turn.completed"}`))
		if !(seq < second && second < third) {
			t.Fatalf("sequences %d,%d,%d are not strictly increasing", seq, second, third)
		}
		page, err := store.Store.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{
			TenantID: store.Tenant, SessionID: session,
		})
		if err != nil {
			t.Fatalf("reading the public journal: %v", err)
		}
		want := []sessionwire.EventID{"event-1", "event-2", "event-3"}
		if len(page.Events) != len(want) {
			t.Fatalf("journal page holds %d events, want %d", len(page.Events), len(want))
		}
		for i, expected := range want {
			if page.Events[i].EventID != expected {
				t.Fatalf("event %d = %q, want %q", i, page.Events[i].EventID, expected)
			}
		}
	})

	t.Run("the tenant is random and prefixed", func(t *testing.T) {
		if !strings.HasPrefix(string(store.Tenant), TenantPrefix) {
			t.Fatalf("tenant %q lacks the kit prefix", store.Tenant)
		}
		other := NewTenantID(t)
		if other == store.Tenant {
			t.Fatalf("two tenant ids collided: %q", other)
		}
	})

	t.Run("restart reopens the same durable bytes", func(t *testing.T) {
		session := store.SeedSession(ctx, kitAgent, string(kitCompatibility))
		store.AppendPublicEvent(ctx, session, "event-before-restart", []byte(`{"type":"turn.started"}`))
		store.Reopen(ctx)
		page, err := store.Store.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{
			TenantID: store.Tenant, SessionID: session,
		})
		if err != nil {
			t.Fatalf("reading after restart: %v", err)
		}
		if len(page.Events) == 0 {
			t.Fatalf("the journal is empty after a restart against the same Composite")
		}
	})

	hostFixture := NewHostFixture(t, store, "orchestrationtest-host-1",
		"wss://orchestrationtest-host-1.internal.test:8443/hostlink", kitAgent, kitCompatibility)

	t.Run("the department is real and its capability discovery discovers", func(t *testing.T) {
		if hostFixture.Department.Len() != 1 {
			t.Fatalf("department holds %d agents, want 1", hostFixture.Department.Len())
		}
		target, err := hostFixture.Department.Target(kitAgent)
		if err != nil {
			t.Fatalf("resolving the kit agent: %v", err)
		}
		runtime, err := target.Create(ctx, department.CreateRequest{
			TenantID: store.Tenant, SessionID: "session-create-1", AgentID: kitAgent,
			Placement: sessionwire.HostPlacementPooled,
		})
		if err != nil {
			t.Fatalf("creating a runtime: %v", err)
		}
		if runtime.SessionID() != "session-create-1" {
			t.Fatalf("runtime session id = %q", runtime.SessionID())
		}
		if epoch, held := runtime.LeaseEpoch(); !held || epoch == 0 {
			t.Fatalf("lease epoch = (%d,%t), want a held nonzero epoch", epoch, held)
		}
		if err := runtime.ReleaseResidency(ctx); err != nil {
			t.Fatalf("releasing residency: %v", err)
		}
	})

	t.Run("an incapable rig session is refused by the real adapter", func(t *testing.T) {
		bare := &FakeRig{Session: NewBareRuntime(mustKitUUID())}
		target, err := department.NewRigTarget(bare, kitCompatibility, KitCapabilities())
		if err != nil {
			t.Fatalf("building a bare rig target: %v", err)
		}
		if _, err := target.Create(ctx, department.CreateRequest{
			TenantID: store.Tenant, SessionID: "session-bare", AgentID: kitAgent,
			Placement: sessionwire.HostPlacementPooled,
		}); err == nil {
			t.Fatalf("an identity-only session was accepted; the kit's fake is looser than the adapter")
		}
	})

	t.Run("a dedicated host is a different structural branch", func(t *testing.T) {
		pinned := store.SeedSession(ctx, kitAgent, string(kitCompatibility))
		dedicated := NewDedicatedHostFixture(t, store, "orchestrationtest-host-2",
			"wss://orchestrationtest-host-2.internal.test:8443/hostlink", kitAgent, kitCompatibility, pinned)
		if dedicated.Host.Placement() != sessionwire.HostPlacementDedicated {
			t.Fatalf("placement = %q", dedicated.Host.Placement())
		}
		if dedicated.Host.FixedSessionID() != pinned {
			t.Fatalf("fixed session = %q, want %q", dedicated.Host.FixedSessionID(), pinned)
		}
		// Row the negative of BOTH placement branches: dedicated refuses a
		// capacity above one, and pooled refuses a fixed session id at all.
		tooBig := DedicatedHostOptions(pinned)
		tooBig.Capacity = 2
		fillHostSeams(&tooBig, store, dedicated)
		if _, err := host.New(tooBig); err == nil {
			t.Fatalf("a dedicated host with capacity 2 was accepted")
		}
		pooledWithFixed := PooledHostOptions()
		pooledWithFixed.FixedSessionID = pinned
		fillHostSeams(&pooledWithFixed, store, dedicated)
		if _, err := host.New(pooledWithFixed); err == nil {
			t.Fatalf("a pooled host carrying a fixed session id was accepted")
		}
	})

	t.Run("host is composed for real and exposes nothing runnable", func(t *testing.T) {
		if hostFixture.Host.Capacity() != PooledHostOptions().Capacity {
			t.Fatalf("host capacity = %d", hostFixture.Host.Capacity())
		}
		if hostFixture.Host.Placement() != sessionwire.HostPlacementPooled {
			t.Fatalf("host placement = %q", hostFixture.Host.Placement())
		}
		AssertHostExposesNoRuntimeSurface(t)
	})

	t.Run("host refuses an invalid composition", func(t *testing.T) {
		options := PooledHostOptions()
		options.HostID = "orchestrationtest-host-bad"
		options.InternalEndpoint = "wss://x.test/hostlink"
		options.Department = hostFixture.Department
		options.SessionStore = &CatalogSessionStore{Store: store.Store}
		options.Workspaces = hostFixture.Workspaces
		options.Clock = clock
		options.Auth = hostFixture.Auth
		options.RegistryHeartbeat = options.RegistryExpiry // violates the 3x margin
		if _, err := host.New(options); err == nil {
			t.Fatalf("host accepted a heartbeat equal to its expiry")
		}
	})

	t.Run("host auth refuses the right credential for the wrong tenant", func(t *testing.T) {
		if err := hostFixture.Auth.VerifyTenant(ctx, store.Tenant, KitHostCredential); err != nil {
			t.Fatalf("the correct credential was refused: %v", err)
		}
		if err := hostFixture.Auth.VerifyTenant(ctx, "some-other-tenant", KitHostCredential); err == nil {
			t.Fatalf("a foreign tenant was accepted with the kit credential")
		}
	})

	factoryFixture := NewFactoryFixture(t, store, clock)
	session := store.SeedSession(ctx, kitAgent, string(kitCompatibility))
	store.AppendPublicEvent(ctx, session, "event-public-1", []byte(`{"type":"turn.completed","html":"\u003ctag\u003e\u0026"}`))

	t.Run("factory serves an authenticated read from the real store", func(t *testing.T) {
		status, body := factoryFixture.Get(t, ctx, "/v1/sessions")
		if status != http.StatusOK {
			t.Fatalf("GET /v1/sessions = %d, body %s", status, truncate(body))
		}
		if !strings.Contains(string(body), string(session)) {
			t.Fatalf("the seeded session is absent from the list: %s", truncate(body))
		}
		// One session in the list is the degenerate structural input: a
		// listing that returned only its first row would satisfy it.
		other := store.SeedSession(ctx, kitAgent, string(kitCompatibility))
		// Distinctness is asserted, not assumed. A substring check over a list
		// whose two names happen to be the SAME string passes without the list
		// ever having held two rows -- this row survived exactly that mutant
		// until the check below was added.
		if other == session {
			t.Fatalf("the kit minted the same session id twice (%q); the two-session case is degenerate", other)
		}
		status, body = factoryFixture.Get(t, ctx, "/v1/sessions")
		if status != http.StatusOK {
			t.Fatalf("GET /v1/sessions = %d", status)
		}
		var listed sessionwire.SessionPage
		if err := json.Unmarshal(body, &listed); err != nil {
			t.Fatalf("the served session page is not a Core SessionPage: %v", err)
		}
		seen := make(map[sessionwire.SessionID]bool, len(listed.Sessions))
		for _, summary := range listed.Sessions {
			seen[summary.SessionID] = true
		}
		for _, want := range []sessionwire.SessionID{session, other} {
			if !seen[want] {
				t.Fatalf("session %q is absent from a two-session page: %s", want, truncate(body))
			}
		}
		calls := factoryFixture.Authorize.Calls()
		if len(calls) == 0 || calls[len(calls)-1] != "AuthorizeSessionList" {
			t.Fatalf("authorizer calls = %v, want AuthorizeSessionList last", calls)
		}
	})

	t.Run("factory refuses an unauthenticated read", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, factoryFixture.BaseURL+"/v1/sessions", nil)
		if err != nil {
			t.Fatalf("building the request: %v", err)
		}
		req.Host = KitHost
		resp, err := factoryFixture.Client.Do(req)
		if err != nil {
			t.Fatalf("requesting: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("an unauthenticated read succeeded")
		}
	})

	t.Run("factory refuses an untrusted host authority", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, factoryFixture.BaseURL+"/v1/sessions", nil)
		if err != nil {
			t.Fatalf("building the request: %v", err)
		}
		req.Host = "evil.example"
		req.Header.Set("Authorization", "Bearer "+KitActorCredential)
		resp, err := factoryFixture.Client.Do(req)
		if err != nil {
			t.Fatalf("requesting: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("an untrusted authority got %d, want 403", resp.StatusCode)
		}
	})

	t.Run("a served public frame carries nothing private", func(t *testing.T) {
		status, body := factoryFixture.Get(t, ctx, SessionPath(session, "/journal"))
		if status != http.StatusOK {
			t.Fatalf("GET journal = %d, body %s", status, truncate(body))
		}
		AssertPublicFrameIsClean(t, "journal page", body)
		var page sessionwire.JournalPage
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatalf("the served journal page is not a Core JournalPage: %v", err)
		}
		if err := page.Validate(); err != nil {
			t.Fatalf("the served journal page fails Core validation: %v", err)
		}
	})

	var faultyFactory, secondFactory *FactoryFixture

	t.Run("a store fault surfaces through the real router", func(t *testing.T) {
		faulty := NewFaultyReader(store.Store)
		faulty.Arm("ListSessions")
		faultyFactory = NewFactoryFixtureWithReader(t, store, clock, faulty)
		status, _ := faultyFactory.Get(t, ctx, "/v1/sessions")
		if status == http.StatusOK {
			t.Fatalf("an armed store fault produced a 200")
		}
		faulty.AssertFired(t, "ListSessions")
	})

	t.Run("every composed seam is driven at least once", func(t *testing.T) {
		// These five seams are COMPOSED into Host and Factory and no service
		// code path in this build reaches any of them: Host exposes nothing
		// runnable, and Factory's control and object routes answer 503 on a
		// nil admission service, so nothing ever asks the Directory to resolve
		// an owner or asks the placement controller for anything.
		//
		// "Composed" is not "driven" and the difference is not cosmetic. With
		// this row absent, a hashForPath that returns a CONSTANT -- giving
		// every tenant and every session one shared workspace directory --
		// survives mutation, because the only caller is never called. The kit
		// therefore drives them directly here and says plainly that a kit
		// self-test, not a service, is what drives them.
		seamSession := store.SeedSession(ctx, kitAgent, string(kitCompatibility))
		otherSession := store.SeedSession(ctx, kitAgent, string(kitCompatibility))
		if seamSession == otherSession {
			t.Fatalf("the two seam sessions collided (%q)", seamSession)
		}

		raw, err := (&CatalogSessionStore{Store: store.Store}).LoadSession(ctx, store.Tenant, seamSession)
		if err != nil {
			t.Fatalf("CatalogSessionStore.LoadSession: %v", err)
		}
		if !bytes.Contains(raw, []byte(seamSession)) {
			t.Fatalf("LoadSession returned bytes naming no session: %s", truncate(raw))
		}
		if _, err := (&CatalogSessionStore{Store: store.Store}).LoadSession(ctx, store.Tenant, "session-absent"); err == nil {
			t.Fatalf("LoadSession answered for a session that does not exist")
		}

		first, err := hostFixture.Workspaces.EnsureWorkspace(ctx, store.Tenant, seamSession)
		if err != nil {
			t.Fatalf("EnsureWorkspace: %v", err)
		}
		second, err := hostFixture.Workspaces.EnsureWorkspace(ctx, store.Tenant, otherSession)
		if err != nil {
			t.Fatalf("EnsureWorkspace: %v", err)
		}
		if first == second {
			t.Fatalf("two sessions were handed the same workspace directory %q", first)
		}
		again, err := hostFixture.Workspaces.EnsureWorkspace(ctx, store.Tenant, seamSession)
		if err != nil {
			t.Fatalf("EnsureWorkspace (repeat): %v", err)
		}
		if again != first {
			t.Fatalf("the same session was handed two workspaces, %q then %q", first, again)
		}
		if hostFixture.Workspaces.Ensured() != 2 {
			t.Fatalf("Ensured() = %d, want 2", hostFixture.Workspaces.Ensured())
		}

		observation, owned, err := factoryFixture.Directory.Owner(ctx, store.Tenant, seamSession)
		if err != nil {
			t.Fatalf("StoreDirectory.Owner: %v", err)
		}
		if owned {
			t.Fatalf("an unplaced session reported an owner: %+v", observation)
		}
		page, err := factoryFixture.Directory.Candidates(ctx, sessionstore.ListCompatibleHostsRequest{
			Key: sessionstore.HostTargetKey{
				AgentID:                kitAgent,
				RuntimeCompatibilityID: string(kitCompatibility),
				Placement:              sessionwire.HostPlacementPooled,
			},
		})
		if err != nil {
			t.Fatalf("StoreDirectory.Candidates: %v", err)
		}
		if len(page.Hosts) != 0 {
			t.Fatalf("no host advertised capacity, yet %d candidates came back", len(page.Hosts))
		}

		if err := factoryFixture.Placement.EnsurePlacement(ctx, sessionstore.DesiredWorkload{
			PayloadVersion: "orchestrationtest/v1",
			Payload:        []byte(`{"replicas":1}`),
		}); err != nil {
			t.Fatalf("RecordingPlacement.EnsurePlacement: %v", err)
		}
		ensured := factoryFixture.Placement.Ensured()
		if len(ensured) != 1 || ensured[0].PayloadVersion != "orchestrationtest/v1" {
			t.Fatalf("Ensured() = %+v", ensured)
		}
		if err := factoryFixture.Placement.ReleasePlacement(ctx, store.Tenant, seamSession); err != nil {
			t.Fatalf("RecordingPlacement.ReleasePlacement: %v", err)
		}
		if factoryFixture.Placement.Released() != 1 {
			t.Fatalf("Released() = %d, want 1", factoryFixture.Placement.Released())
		}
	})

	t.Run("two factories share one durable plane", func(t *testing.T) {
		secondFactory = NewFactoryFixture(t, store, clock)
		status, body := secondFactory.Get(t, ctx, "/v1/sessions")
		if status != http.StatusOK {
			t.Fatalf("the second factory answered %d", status)
		}
		if !strings.Contains(string(body), string(session)) {
			t.Fatalf("the second factory cannot see the shared session")
		}
	})

	AssertNoLeaks(t, ctx, LeakSources{
		Store:              store,
		Factories:          []*FactoryFixture{factoryFixture, faultyFactory, secondFactory},
		Host:               hostFixture,
		LinksUnobservable:  true,
		BaselineGoroutines: baseline,
	})
}

// TestOrchestrationTestKitAssertionsCanFail is the kit's own correctness gate.
//
// Every assertion the kit exports, AND every assertion a kit fixture makes
// internally, gets a row here proving it reports a failure when its premise is
// false. An assertion with no row is an assertion nobody has seen fail, and a
// kit made of those is worse than no kit: it converts "untested" into "green".
//
// This sentence was once false about the one assertion the kit singles out as
// protecting another assertion's meaning. FactoryFixture.Stop checks that Serve
// returned http.ErrServerClosed -- the check that stops AssertNoLeaks's listener
// row passing because nothing was ever listening -- and it had no row, so
// replacing its comparison with `if false` survived mutation. It is rowed now,
// in both branches. The lesson is the scope of the word "exports": Stop is
// exported, but it is an assertion made BY a fixture rather than one a case
// calls, and that is the class this claim quietly excluded.
func TestOrchestrationTestKitAssertionsCanFail(t *testing.T) {
	ctx := kitContext(t)

	t.Run("AdvanceFiring", func(t *testing.T) {
		mustFail(t, "want exactly", func(tb TB) {
			NewClock(time.Unix(kitEpoch, 0)).AdvanceFiring(tb, time.Second, 1)
		})
	})

	t.Run("Signal.Await", func(t *testing.T) {
		mustFail(t, "waiting for signal", func(tb TB) {
			expired, cancel := context.WithCancel(context.Background())
			cancel()
			NewSignal().Await(tb, expired)
		})
	})

	t.Run("FaultyReader.AssertFired", func(t *testing.T) {
		mustFail(t, "armed and never reached", func(tb TB) {
			faulty := &FaultyReader{armed: map[string]bool{}, fired: map[string]int{}}
			faulty.Arm("ListSessions")
			faulty.AssertFired(tb, "ListSessions")
		})
	})

	t.Run("AssertPublicFrameIsClean", func(t *testing.T) {
		mustFail(t, "runtime_command_id", func(tb TB) {
			AssertPublicFrameIsClean(tb, "synthetic", []byte(`{"runtime_command_id":"x"}`))
		})
		mustFail(t, "centrifuge", func(tb TB) {
			AssertPublicFrameIsClean(tb, "synthetic", []byte(`{"transport":"centrifuge"}`))
		})
		mustFail(t, KitActorCredential, func(tb TB) {
			AssertPublicFrameIsClean(tb, "synthetic", []byte(`{"token":"`+KitActorCredential+`"}`))
		})
	})

	t.Run("AssertNoLeaks refuses an unstated link claim", func(t *testing.T) {
		mustFail(t, "LinksUnobservable", func(tb TB) {
			AssertNoLeaks(tb, ctx, LeakSources{BaselineGoroutines: 1})
		})
	})

	t.Run("AssertNoLeaks refuses a missing goroutine baseline", func(t *testing.T) {
		mustFail(t, "BaselineGoroutines is zero", func(tb TB) {
			AssertNoLeaks(tb, ctx, LeakSources{LinksUnobservable: true})
		})
	})

	t.Run("AssertNoLeaks reports a live listener", func(t *testing.T) {
		clock := NewClock(time.Unix(kitEpoch, 0))
		store := NewStoreFixture(t, ctx, clock)
		mustFail(t, "still accepts connections", func(tb TB) {
			fixture := NewFactoryFixture(tb, store, clock)
			// Defeat the Stop that AssertNoLeaks would perform, so the
			// listener is genuinely still live when the assertion runs.
			fixture.stopped.Do(func() {})
			AssertNoLeaks(tb, ctx, LeakSources{
				Factories: []*FactoryFixture{fixture}, LinksUnobservable: true, BaselineGoroutines: CaptureGoroutines(),
			})
			fixture.stopped = sync.Once{}
			fixture.Stop(tb)
		})
	})

	t.Run("FactoryFixture.Stop rejects a Serve that returned the wrong error", func(t *testing.T) {
		clock := NewClock(time.Unix(kitEpoch, 0))
		store := NewStoreFixture(t, ctx, clock)
		fixture := NewFactoryFixture(t, store, clock)
		// Hijack the channel Stop reads. The real Serve goroutine still sends
		// its own result to the original BUFFERED channel, so nothing blocks
		// and no goroutine leaks; the server really is stopped by this call.
		fixture.served = make(chan error, 1)
		fixture.served <- errors.New("orchestrationtest: a Serve failure that is not ErrServerClosed")
		mustFail(t, "want http.ErrServerClosed", func(tb TB) { fixture.Stop(tb) })
	})

	t.Run("FactoryFixture.Stop reports a Serve that never returns", func(t *testing.T) {
		clock := NewClock(time.Unix(kitEpoch, 0))
		store := NewStoreFixture(t, ctx, clock)
		fixture := NewFactoryFixture(t, store, clock)
		fixture.served = make(chan error) // nothing will ever arrive
		fixture.serveWait = 50 * time.Millisecond
		mustFail(t, "did not return after Stop", func(tb TB) { fixture.Stop(tb) })
	})

	t.Run("FakeRuntime refuses an unframed command", func(t *testing.T) {
		runtime := NewFakeRuntime(mustKitUUID())
		if err := runtime.ApplyCommand(ctx, department.RuntimeCommand{CommandID: "cmd-1"}); err == nil {
			t.Fatalf("a command with a zero runtime uuid was accepted")
		}
		if len(runtime.Applied()) != 0 {
			t.Fatalf("a refused command was recorded as applied")
		}
	})

	t.Run("AssertHostExposesNoDrainSurface names what would unblock I2.3", func(t *testing.T) {
		mustFail(t, "no longer blocked", func(tb TB) {
			saved := hostDrainSurfaceNames
			// *host.Host does export Capacity; standing it in for "StartDrain"
			// proves the assertion reads the real method set rather than a list
			// that happens to match nothing.
			hostDrainSurfaceNames = []string{"Capacity"}
			defer func() { hostDrainSurfaceNames = saved }()
			AssertHostExposesNoDrainSurface(tb)
		})
	})

	t.Run("FactoryFixture.Post reports an unreachable server", func(t *testing.T) {
		clock := NewClock(time.Unix(kitEpoch, 0))
		store := NewStoreFixture(t, ctx, clock)
		fixture := NewFactoryFixture(t, store, clock)
		// Port 1 on loopback refuses immediately. Stopping the fixture's own
		// server would work too, but it would also race Serve's return and make
		// this row report the WRONG failure -- which is the shape the kit's Stop
		// assertion exists to catch.
		fixture.BaseURL = "http://127.0.0.1:1"
		mustFail(t, "posting", func(tb TB) {
			fixture.Post(tb, ctx, "/v1/sessions/session-1/input", []byte(`{}`))
		})
	})

	t.Run("ObservedReader records the request, not merely the call", func(t *testing.T) {
		// The counter is not the evidence: I1.1 case 5 turns on WHAT was asked
		// for. A wrapper that counted calls and dropped the request would make
		// every bound assertion above unwritable, and nothing else would notice.
		clock := NewClock(time.Unix(kitEpoch, 0))
		store := NewStoreFixture(t, ctx, clock)
		session := store.SeedSession(ctx, kitAgent, string(kitCompatibility))
		store.AppendPublicEvent(ctx, session, "event-1", []byte(`{"n":1}`))
		observer := NewObservedReader(store.Store)
		fixture := NewFactoryFixtureWithSeams(t, store, clock, FactorySeams{Reader: observer})
		if status, _ := fixture.Get(t, ctx, SessionPath(session, "/journal")); status != http.StatusOK {
			t.Fatalf("the journal read answered %d", status)
		}
		requests := observer.JournalRequests()
		if len(requests) != 1 || observer.Count("ReadPublicJournal") != 1 {
			t.Fatalf("observed %d requests and %d calls, want 1 of each", len(requests), observer.Count("ReadPublicJournal"))
		}
		if !requests[0].Tail || requests[0].Limit <= 0 {
			t.Fatalf("the recorded request is empty of the values a bound assertion reads: %+v", requests[0])
		}
		observer.Reset()
		if len(observer.JournalRequests()) != 0 || observer.Total() != 0 {
			t.Fatalf("Reset left recorded state behind")
		}
	})

	t.Run("AssertHostExposesNoRuntimeSurface names what would unblock I0.2", func(t *testing.T) {
		mustFail(t, "no longer blocked", func(tb TB) {
			saved := hostRuntimeSurfaceNames
			// *host.Host does export Capacity; standing it in for "Serve"
			// proves the assertion reads the real method set.
			hostRuntimeSurfaceNames = []string{"Capacity"}
			defer func() { hostRuntimeSurfaceNames = saved }()
			AssertHostExposesNoRuntimeSurface(tb)
		})
	})
}
