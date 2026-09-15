//go:build integration

package orchestrationtest

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/sessionstore"
)

// ErrWrongCredential is what the kit's Host authenticator returns.
var ErrWrongCredential = errors.New("orchestrationtest: credential rejected")

// HostFixture is a REAL *host.Host composed from the kit's seams.
//
// Read what this is NOT. A *host.Host is a validated, immutable configuration
// value and nothing else: its exported surface is New plus nineteen pure
// accessors, with no Serve, Start, Run, Attach, Handler, Stop or Close, and
// package host imports no net/http. Everything that RUNS a Host --
// internal/compose.Service, internal/realtime/hostlink -- is under internal/
// and is therefore unreachable from this module by the Go compiler, not merely
// by policy. Host's own cmd/host/main.go says so in its package doc: "A
// PRODUCT CANNOT YET SHIP ITS OWN MAIN AGAINST THIS ... So today a product
// vendors this file, or Host grows an exported composition surface."
//
// The kit therefore composes Host's configuration and its Department for real,
// and refuses to fake a running Host. See AssertHostExposesNoRuntimeSurface.
type HostFixture struct {
	Host       *host.Host
	Department *department.Department
	Rig        *FakeRig
	Workspaces *TempWorkspaces
	Auth       *FixedCredentialAuth
}

// PooledHostOptions is a timing-valid pooled Host configuration.
//
// The timing values are not decorative. Host refuses RegistryExpiry/3 <
// RegistryHeartbeat (heartbeat_margin) and ApplyDeadline/2 < ClaimTTL
// (claim_margin), and it refuses them by DIVISION so the check cannot be
// defeated by overflow. Changing one of these constants without the other is
// how a composition starts failing for a reason that reads like a typo.
func PooledHostOptions() host.Options {
	return host.Options{
		IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
		Placement:         sessionwire.HostPlacementPooled,
		Capacity:          8,
		WarmTTL:           97 * time.Second,
		RegistryHeartbeat: 5 * time.Second,
		RegistryExpiry:    31 * time.Second,
		ClaimTTL:          11 * time.Second,
		ApplyDeadline:     47 * time.Second,
		CommandQueueSize:  257,
		ReconcileInterval: 23 * time.Second,
		ReconcileBatch:    129,
	}
}

// DedicatedHostOptions is a timing-valid DEDICATED Host configuration.
//
// It exists so the kit is not degenerate in its placement axis. Placement is a
// STRUCTURAL input, not a value one: dedicated and pooled travel different
// validation branches (dedicated requires a fixed session id and capacity
// exactly one; pooled REFUSES a fixed session id), so a kit that only ever
// composes one of them has swept no part of the other branch, however many
// timing values it varies.
func DedicatedHostOptions(session sessionwire.SessionID) host.Options {
	options := PooledHostOptions()
	options.Placement = sessionwire.HostPlacementDedicated
	options.Capacity = 1
	options.FixedSessionID = session
	return options
}

// NewHostFixture composes a real pooled Host over the shared durable plane.
func NewHostFixture(tb TB, store *StoreFixture, id sessionwire.HostID, endpoint sessionwire.InternalEndpoint, agent sessionwire.AgentID, compatibility department.CompatibilityID) *HostFixture {
	tb.Helper()
	return newHostFixture(tb, store, id, endpoint, agent, compatibility, PooledHostOptions())
}

// NewDedicatedHostFixture composes a real dedicated Host pinned to one session.
func NewDedicatedHostFixture(tb TB, store *StoreFixture, id sessionwire.HostID, endpoint sessionwire.InternalEndpoint, agent sessionwire.AgentID, compatibility department.CompatibilityID, session sessionwire.SessionID) *HostFixture {
	tb.Helper()
	return newHostFixture(tb, store, id, endpoint, agent, compatibility, DedicatedHostOptions(session))
}

func newHostFixture(tb TB, store *StoreFixture, id sessionwire.HostID, endpoint sessionwire.InternalEndpoint, agent sessionwire.AgentID, compatibility department.CompatibilityID, options host.Options) *HostFixture {
	tb.Helper()
	rig := &FakeRig{Session: NewFakeRuntime(kitRuntimeUUID)}
	dept := NewDepartment(tb, agent, compatibility, rig)
	workspaces := NewTempWorkspaces(tb)
	auth := &FixedCredentialAuth{Tenant: store.Tenant, Credential: KitHostCredential}

	options.HostID = id
	options.InternalEndpoint = endpoint
	options.Department = dept
	options.SessionStore = &CatalogSessionStore{Store: store.Store}
	options.Workspaces = workspaces
	options.Clock = store.Clock
	options.Auth = auth

	composed, err := host.New(options)
	if err != nil {
		tb.Fatalf("orchestrationtest: composing a host: %v", err)
		return nil
	}
	return &HostFixture{Host: composed, Department: dept, Rig: rig, Workspaces: workspaces, Auth: auth}
}

// KitHostCredential is the one credential the kit's Host authenticator accepts.
const KitHostCredential = "orchestrationtest-host-credential"

var kitRuntimeUUID = mustKitUUID()

// CatalogSessionStore adapts the REAL *sessionstore.Store to host.SessionStore.
//
// host.SessionStore is one method returning opaque bytes, so this adapter
// resolves the durable catalog entry and encodes it. It is an adapter, not a
// fake: every byte it returns came out of the real store.
type CatalogSessionStore struct{ Store *sessionstore.Store }

// LoadSession satisfies host.SessionStore.
func (s *CatalogSessionStore) LoadSession(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) ([]byte, error) {
	entry, err := s.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: session})
	if err != nil {
		return nil, err
	}
	return json.Marshal(entry)
}

// TempWorkspaces satisfies host.WorkspaceProvider over a per-fixture temporary
// root. Runbook 07 I0.1 step 4 forbids the user's real home or session root;
// this root is created fresh and removed on cleanup.
type TempWorkspaces struct {
	Root string

	mu      sync.Mutex
	ensured map[string]string
}

// NewTempWorkspaces creates a fresh workspace root and registers its removal.
func NewTempWorkspaces(tb TB) *TempWorkspaces {
	tb.Helper()
	root, err := os.MkdirTemp("", "orchestrationtest-workspaces-")
	if err != nil {
		tb.Fatalf("orchestrationtest: creating a workspace root: %v", err)
		return nil
	}
	tb.Cleanup(func() {
		if removeErr := os.RemoveAll(root); removeErr != nil {
			tb.Errorf("orchestrationtest: removing workspace root %q: %v", root, removeErr)
		}
	})
	return &TempWorkspaces{Root: root, ensured: make(map[string]string)}
}

// EnsureWorkspace satisfies host.WorkspaceProvider.
func (w *TempWorkspaces) EnsureWorkspace(_ context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := string(tenant) + "\x00" + string(session)
	if existing, ok := w.ensured[key]; ok {
		return existing, nil
	}
	dir := filepath.Join(w.Root, hashForPath(key))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	w.ensured[key] = dir
	return dir, nil
}

// Ensured reports how many distinct workspaces were handed out. It is a leak
// input: a workspace per attach that is never reused is a residency leak.
func (w *TempWorkspaces) Ensured() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.ensured)
}

// FixedCredentialAuth satisfies host.AuthVerifier for exactly one tenant and
// one credential. It refuses everything else, including the right credential
// for the wrong tenant, because a Host authenticator that ignores the tenant
// is precisely the fake that is looser than the dependency.
type FixedCredentialAuth struct {
	Tenant     sessionwire.TenantID
	Credential string

	mu    sync.Mutex
	calls int
}

// VerifyTenant satisfies host.AuthVerifier.
func (a *FixedCredentialAuth) VerifyTenant(_ context.Context, tenant sessionwire.TenantID, credential string) error {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	if tenant != a.Tenant || credential != a.Credential {
		return ErrWrongCredential
	}
	return nil
}

// Calls reports how many verifications were attempted.
func (a *FixedCredentialAuth) Calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

// The narrow runtime trip-wire lived here and is REPLACED by
// AssertHostExposesNoRuntimeCapability in hostsurface.go.
//
// It reflected over *host.Host's method set. The subject was right -- the
// dependency rather than this kit's fixture -- but it read ONE syntactic form of
// it, and a review built the likely alternative: a separate exported Runtime
// type carrying Serve/Start, plus a package-level func host.Serve. The wire
// stayed green. host.Host is documented as an immutable configuration value, so
// that is the shape a runtime surface will probably arrive in.
