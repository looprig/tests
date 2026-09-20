//go:build integration

// This file carries controller review CODEX_REVIEW_CONTROLLER_81A1F1E's finding
// F3 into the lane: the DRIVER-LEVEL real-Host drain case, which existed only
// in that gate's private harness because the harness needed controller-internal
// Kubernetes fixtures.
//
// # What it is about
//
// The controller ends a dedicated workload drain-before-delete, and every step
// of that sequence talks to a Host at an address it DERIVES:
// sessionwire.HostLinkEndpoint(base, tenant). The adapter renders a BARE base
// into the Pod and never reads one back from the registry; the driver hands
// that base to the drain client untouched; the client derives. Three separate
// places could get it wrong, and only a case that runs the real driver against
// a real Host can see all three at once:
//
//	HM4  `view` hands the driver a per-tenant endpoint instead of a base --
//	     invisible to a client-level test, because the client would derive from
//	     an already-derived address and the Host would answer 404;
//	HM5  the address is built by string concatenation instead of Core, which
//	     only a tenant NEEDING PATH ESCAPING can tell apart;
//	HM1  the base is dialled verbatim.
//
// So the case asserts the exact endpoint the drainer was handed, and the exact
// upgrade paths the Host saw, for a plain tenant AND for one that needs
// escaping.
//
// # What is real and what is not
//
// Real: controller/driver's own Driver, controller/hostlink's own drain client,
// a real composed host v0.4.0 serving its real Routes() on TCP and advertising
// a bare base, and a real SessionStore shared between them.
//
// Fake: the PLATFORM. controller/kubernetes and its fake API server are the
// controller's own internal fixtures and this module has no business importing
// a Kubernetes client to test an address derivation. The Workloads adapter here
// is an in-memory platform that reports one workload holding the Host's bare
// base -- which is precisely the value HM4 attacks.
//
// # The limitation, stated
//
// The session is not resident on the Host. The drain a controller sends is
// whole-Host or names the dedicated Host's one fixed session, and what this
// case measures is the ADDRESS and the ORDER, neither of which depends on the
// Host holding the session. A dedicated Host actually holding its session is
// D3.1's disposable-namespace lane and is still owed.

package tests

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/controller/driver"
	controllerhostlink "github.com/looprig/controller/hostlink"
	"github.com/looprig/controller/workload"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// controllerEscapedTenant needs path escaping in every one of its five bytes'
// worth of trouble: a space, a percent, a question mark and a hash. A
// concatenating derivation produces a different path for it and the Host routes
// it to nothing.
const controllerEscapedTenant = sessionwire.TenantID("a b%c?d#e")

// platform is the in-memory platform the driver acts through.
//
// It models the three things the driver's teardown state machine reads back:
// the persisted drain record, the persisted decision, and release. Every method
// records its call so the ORDER can be asserted, because the order is the
// contract: drain, then drained, then fence, then ONE delete, then record.
type platform struct {
	mu       sync.Mutex
	current  workload.Workload
	released bool
	calls    []string
}

func (p *platform) record(call string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, call)
}

func (p *platform) Calls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

func (p *platform) EnsureWorkload(context.Context, sessionstore.PlacementIntent) error {
	p.record("ensure")
	return nil
}

func (p *platform) ListWorkloads(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]workload.Workload, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released {
		return nil, nil
	}
	return []workload.Workload{p.current}, nil
}

func (p *platform) MarkDrain(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, w workload.Workload, d workload.Drain) (workload.Workload, error) {
	p.record("mark-drain")
	p.mu.Lock()
	defer p.mu.Unlock()
	drain := d
	p.current = w
	p.current.Drain = &drain
	return p.current, nil
}

func (p *platform) MarkDecision(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, w workload.Workload, d workload.Decision) (workload.Workload, error) {
	p.record("mark-decision:" + string(d.Kind))
	p.mu.Lock()
	defer p.mu.Unlock()
	decision := d
	p.current = w
	p.current.Decision = &decision
	return p.current, nil
}

func (p *platform) ClearMarks(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, w workload.Workload) (workload.Workload, error) {
	p.record("clear-marks")
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = w
	p.current.Drain, p.current.Decision = nil, nil
	return p.current, nil
}

func (p *platform) Terminate(_ context.Context, _ workload.Workload) error {
	p.record("terminate")
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current.Terminating = true
	p.current.Terminal = true
	return nil
}

func (p *platform) Release(_ context.Context, _ workload.Workload) error {
	p.record("release")
	p.mu.Lock()
	defer p.mu.Unlock()
	p.released = true
	return nil
}

// recordingDrainer wraps the controller's REAL drain client and records the
// endpoint each call was handed.
//
// The recording is the HM4 reader: a `view` that derived the address itself
// would hand the driver something other than the bare base, and the client
// would then derive from an already-derived address. Only the value at this
// seam can tell.
type recordingDrainer struct {
	inner driver.Drainer

	mu        sync.Mutex
	endpoints []sessionwire.InternalEndpoint
}

func (d *recordingDrainer) Endpoints() []sessionwire.InternalEndpoint {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]sessionwire.InternalEndpoint(nil), d.endpoints...)
}

func (d *recordingDrainer) StartDrain(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
	d.mu.Lock()
	d.endpoints = append(d.endpoints, endpoint)
	d.mu.Unlock()
	return d.inner.StartDrain(ctx, endpoint, req)
}

func (d *recordingDrainer) DrainStatus(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
	d.mu.Lock()
	d.endpoints = append(d.endpoints, endpoint)
	d.mu.Unlock()
	return d.inner.DrainStatus(ctx, endpoint, req)
}

type controllerToken struct{}

func (controllerToken) ServiceToken(context.Context) (string, error) {
	return orchestrationtest.PooledServiceToken, nil
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// TestTheControllerDriverDrainsARealHostThroughTheDerivedAddress is F3.
func TestTheControllerDriverDrainsARealHostThroughTheDerivedAddress(t *testing.T) {
	for name, tenant := range map[string]sessionwire.TenantID{
		"a plain tenant":            orchestrationtest.PooledTenantA,
		"a tenant needing escaping": controllerEscapedTenant,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := placementContext(t)
			world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
				Tenants: []sessionwire.TenantID{tenant},
			})
			const hostID = sessionwire.HostID("orchestrationtest-dedicated-host")
			// The generation the workload was created for, which is both the
			// Host's own HOST_GENERATION and the catalog's DesiredGeneration at
			// creation. The DELETION DESIRE below advances the catalog to 2, so
			// this workload is one "the desire no longer names" -- which is the
			// only state the teardown machine acts on. A workload at or above
			// the desired generation reads as OWNED and the driver stands back.
			const generation = uint64(1)
			session := sessionwire.SessionID("session-controller-drain")
			pooled := orchestrationtest.StartDedicatedHost(t, ctx, world, hostID, generation, session)

			now := time.Now().UTC()
			// A session the Host can actually HOLD: bound, disposition-mode,
			// with a UUID runtime session id, and DEDICATED at generation 1
			// with a workload the desire names.
			//
			// Residency is not decoration here. Host resolves a fixed-session
			// drain scope only for a tenant that CURRENTLY HOLDS the session --
			// rung 8 of its scope resolver -- and refuses anything else
			// `runtime_unavailable`, measured. A drain case over a session no
			// Host holds would be asserting about a refusal.
			if _, _, err := world.Store.CreateCatalogEntry(ctx, sessionstore.CreateCatalogEntryRequest{
				Binding:                orchestrationtest.PooledDispositionBinding(t),
				TenantID:               tenant,
				SessionID:              session,
				AgentID:                orchestrationtest.PooledAgent,
				RuntimeCompatibilityID: string(orchestrationtest.PooledCompatibility),
				CreatedAt:              now,
				LastActiveAt:           now,
				State:                  sessionwire.SessionStateIdle,
				Residency:              sessionwire.SessionResidencyCold,
				DesiredPlacement:       sessionwire.HostPlacementDedicated,
				DesiredWorkload: sessionstore.DesiredWorkload{
					PayloadVersion: "orchestrationtest/v1",
					Payload:        []byte(`{"replicas":1}`),
				},
			}); err != nil {
				t.Fatalf("seeding the dedicated session: %v", err)
			}

			residency := pooled.Attach(t, ctx, tenant, session)
			if residency.LeaseEpoch == 0 || !residency.Attached {
				t.Fatalf("the Host did not take the session resident: %+v", residency)
			}
			// The Host publishes its own registration; the driver's fence
			// clears THAT one, at THAT epoch. Reading it back is what makes the
			// termination's lease epoch an observation rather than a guess.
			var observed uint64
			var advertised sessionwire.InternalEndpoint
			orchestrationtest.PooledWait(t, "the Host published its route", 30*time.Second, func() bool {
				entry, err := world.Store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{
					TenantID: tenant, SessionID: session,
				})
				if err != nil || entry.Registration.Route == nil {
					return false
				}
				observed, advertised = entry.Registration.LeaseEpoch, entry.Registration.Route.InternalEndpoint
				return true
			})
			if advertised != pooled.Base {
				t.Fatalf("the Host advertised %q, want its bare base %q", advertised, pooled.Base)
			}

			// DELETION DESIRE, written the way Factory writes desired state: a
			// dedicated placement naming a ZERO workload. It needs no API of
			// its own, and it advances DesiredGeneration to 2 -- which is what
			// makes the generation-1 workload one "the desire no longer names"
			// and the only state the teardown machine acts on.
			entry, err := world.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: session})
			if err != nil {
				t.Fatalf("reading the seeded record: %v", err)
			}
			if _, err := world.Store.UpdateCatalogDesiredState(ctx, sessionstore.UpdateCatalogDesiredStateRequest{
				TenantID:               tenant,
				SessionID:              session,
				ExpectedRevision:       entry.Revision,
				IdempotencyKey:         "orchestrationtest-delete-desire",
				DesiredPlacement:       sessionwire.HostPlacementDedicated,
				RuntimeCompatibilityID: string(orchestrationtest.PooledCompatibility),
			}); err != nil {
				t.Fatalf("writing the deletion desire: %v", err)
			}

			client, err := hostLinkDrainClient()
			if err != nil {
				t.Fatalf("composing the controller's drain client: %v", err)
			}
			drainer := &recordingDrainer{inner: client}
			stage := &platform{current: workload.Workload{
				Name:       string(hostID),
				UID:        "uid-1",
				Revision:   "1",
				Generation: generation,
				// THE VALUE HM4 ATTACKS. The adapter renders a bare base and
				// never reads one back from the registry.
				Endpoint: pooled.Base,
				Held:     true,
			}}
			source, err := driver.NewFixedSource([]driver.Key{{TenantID: tenant, SessionID: session}}, driver.MaxKeysPerPassCeiling)
			if err != nil {
				t.Fatalf("driver.NewFixedSource: %v", err)
			}
			runner, err := driver.New(driver.Config{
				Source:         source,
				Catalog:        world.Store,
				Registry:       world.Store,
				Claims:         world.Store,
				Workloads:      stage,
				Terminations:   world.Store,
				Drainer:        drainer,
				Clock:          systemClock{},
				HolderID:       "orchestrationtest-controller",
				ClaimTTL:       30 * time.Second,
				ItemTimeout:    25 * time.Second,
				MaxKeysPerPass: 8,
				Interval:       200 * time.Millisecond,
				DrainTimeout:   30 * time.Second,
			})
			if err != nil {
				t.Fatalf("driver.New: %v", err)
			}

			// The teardown is a state machine and one pass advances it by one
			// step, so the driver is passed until the workload is gone. The
			// bound is an assertion, not a hope.
			var report driver.PassReport
			orchestrationtest.PooledWait(t, "the driver finished the teardown", 60*time.Second, func() bool {
				report, err = runner.Pass(ctx)
				if err != nil {
					t.Fatalf("driver.Pass: %v", err)
				}
				return len(report.Items) == 1 && report.Items[0].Outcome == driver.OutcomeDeleted
			})
			t.Logf("driver calls: %v", stage.Calls())

			t.Run("the drainer was handed the BARE base, never a derived address", func(t *testing.T) {
				endpoints := drainer.Endpoints()
				if len(endpoints) == 0 {
					t.Fatalf("the driver never asked the Host to drain; calls were %v", stage.Calls())
				}
				for i, endpoint := range endpoints {
					if endpoint != pooled.Base {
						t.Fatalf("drain call %d was handed %q, want the workload's bare base %q", i, endpoint, pooled.Base)
					}
				}
			})

			t.Run("every upgrade reached the Host at Core's derived tenant path", func(t *testing.T) {
				derived, err := sessionwire.HostLinkEndpoint(pooled.Base, tenant)
				if err != nil {
					t.Fatalf("Core refused to derive %q's address: %v", tenant, err)
				}
				want := strings.TrimPrefix(string(derived), string(pooled.Base))
				paths := pooled.UpgradePaths()
				if len(paths) == 0 {
					t.Fatalf("the Host saw no HostLink upgrade at all, so the derived path proves nothing")
				}
				for i, path := range paths {
					if path != want {
						t.Fatalf("upgrade %d arrived at %q, want Core's derived path %q (the whole set was %v)", i, path, want, paths)
					}
				}
				t.Logf("the Host saw %d upgrades, every one at %q", len(paths), want)
			})

			t.Run("the sequence is drain, fence, one delete, then record", func(t *testing.T) {
				calls := stage.Calls()
				if position(calls, "mark-drain") < 0 {
					t.Fatalf("the driver never persisted a drain record: %v", calls)
				}
				if got := count(calls, "terminate"); got != 1 {
					t.Fatalf("the driver deleted the workload %d times, want exactly 1: %v", got, calls)
				}
				if position(calls, "mark-drain") > position(calls, "terminate") {
					t.Fatalf("the driver deleted before it drained: %v", calls)
				}
				if position(calls, "mark-decision:graceful") > position(calls, "terminate") {
					t.Fatalf("the driver deleted before it decided how the workload ended: %v", calls)
				}
				if position(calls, "release") < position(calls, "terminate") {
					t.Fatalf("the driver released the workload before deleting it: %v", calls)
				}

				// THE FENCE. The controller's one registry write clears the
				// registration at the epoch it observed, so no later reader
				// routes to a Host that is going away.
				// A released registration is a TOMBSTONE and the read refuses
				// it typed, rather than answering a record with a nil route:
				// "a caller branching on 'there is no live route' should not
				// have to match the type that reports 'there is no such
				// session'". So the assertion is on the code.
				_, err := world.Store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{
					TenantID: tenant, SessionID: session,
				})
				var registryErr *sessionstore.RegistryError
				if !errors.As(err, &registryErr) || registryErr.Code != sessionstore.RegistryErrorReleased {
					t.Fatalf("reading the registration after the fence = %v, want a typed released tombstone", err)
				}

				// THE RECORD. GRACEFUL, at the generation whose workload ended
				// -- never the generation of the deletion desire.
				termination, err := world.Store.GetPlacementTermination(ctx, sessionstore.GetPlacementTerminationRequest{
					TenantID: tenant, SessionID: session, Generation: generation,
				})
				if err != nil {
					t.Fatalf("reading the placement termination: %v", err)
				}
				if termination.Termination.Kind != sessionstore.PlacementTerminationGraceful {
					t.Fatalf("the workload's end was recorded %q (%q), want graceful: the Host really drained",
						termination.Termination.Kind, termination.Termination.ForcedReason)
				}
				if termination.Termination.LeaseEpoch != observed {
					t.Fatalf("the termination names lease epoch %d, want the epoch the Host actually published, %d",
						termination.Termination.LeaseEpoch, observed)
				}
			})
		})
	}
}

func position(calls []string, want string) int {
	for i, call := range calls {
		if call == want {
			return i
		}
	}
	return -1
}

func count(calls []string, want string) int {
	n := 0
	for _, call := range calls {
		if call == want {
			n++
		}
	}
	return n
}

// hostLinkDrainClient composes the controller's OWN drain client.
//
// It presents the controller's own service token, which is the point of the
// package's Identity note: nothing distinguishes a controller from any other
// bearer of a token the product's verifier accepts, so the controller carries
// one a deployment can scope and revoke separately. This kit's Host verifier
// accepts one token, so the two are the same string here and the separation is
// a deployment property rather than a testable one.
func hostLinkDrainClient() (*controllerhostlink.Client, error) {
	return controllerhostlink.New(controllerhostlink.Config{
		Token:       controllerToken{},
		Version:     "orchestrationtest",
		DialTimeout: 15 * time.Second,
		RPCTimeout:  15 * time.Second,
	})
}
