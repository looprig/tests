//go:build integration && orchestration

package orchestrationtest

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

// KitOrigin is the one trusted origin the kit's CSRF configuration names, and
// KitHost is its authority. Factory's guard rejects a request whose Host is not
// a trusted origin's host BEFORE it looks at anything else, so every kit
// request is addressed to this authority regardless of the loopback port the
// listener actually got.
const (
	KitOrigin = "https://app.example.test"
	KitHost   = "app.example.test"
)

// KitActorCredential is the bearer value the kit's Verifier accepts.
const KitActorCredential = "orchestrationtest-actor-credential"

// FactoryFixture is a REAL factory.Server serving on a REAL loopback listener.
//
// Nothing about the server, its router, its authentication, its CSRF guard or
// its durable read plane is faked: SessionReader and Commands are the real
// *sessionstore.Store, which satisfies both interfaces exactly.
//
// What IS faked, and why: Authorizer, identity.Verifier and PlacementController
// are seams a DEPLOYER supplies -- Factory ships no implementation of any of
// them -- so a kit implementation is the only possible one, not a substitute
// for something drivable. Directory is a thin adapter over the real Store,
// because Factory's Directory shape (Owner) and the Store's shape
// (GetHostRegistration) differ; every byte it returns came out of the Store.
type FactoryFixture struct {
	Server    *factory.Server
	Listener  net.Listener
	BaseURL   string
	Client    *http.Client
	Authorize *RecordingAuthorizer
	Placement *RecordingPlacement
	Directory *StoreDirectory

	served  chan error
	stopped sync.Once
}

// SessionReaderSeam is factory.SessionReader, restated so a fixture can be
// composed over the real Store or over a fault-injecting wrapper of it.
type SessionReaderSeam interface {
	ListSessions(context.Context, sessionstore.ListSessionsRequest) (sessionstore.SessionPage, error)
	GetCatalogEntry(context.Context, sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error)
	ReadPublicJournal(context.Context, sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error)
	ReadGates(context.Context, sessionstore.ReadGatesRequest) (sessionwire.GatePage, error)
	GetObject(context.Context, sessionstore.GetObjectRequest) (io.ReadCloser, error)
	GetObjectMetadata(context.Context, sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error)
}

// NewFactoryFixture composes and starts a Factory over the shared durable plane.
func NewFactoryFixture(tb TB, store *StoreFixture, clock *Clock) *FactoryFixture {
	tb.Helper()
	return NewFactoryFixtureWithReader(tb, store, clock, store.Store)
}

// NewFactoryFixtureWithReader composes a Factory over a chosen read plane.
func NewFactoryFixtureWithReader(tb TB, store *StoreFixture, clock *Clock, reader SessionReaderSeam) *FactoryFixture {
	tb.Helper()
	authorizer := &RecordingAuthorizer{}
	placement := &RecordingPlacement{}
	directory := &StoreDirectory{Store: store.Store}

	server, err := factory.New(
		factory.WithCredentialVerifier(&FixedBearerVerifier{Tenant: store.Tenant, Credential: KitActorCredential, Clock: clock}),
		factory.WithAuthorizer(authorizer),
		factory.WithSessionReader(reader),
		factory.WithCommands(store.Store),
		factory.WithDirectory(directory),
		factory.WithPlacementController(placement),
		factory.WithClock(clock),
		factory.WithCSRF(identity.CSRFConfig{
			SharedKey:      make([]byte, identity.MinCSRFSharedKeyBytes),
			TokenTTL:       time.Hour,
			TrustedOrigins: []string{KitOrigin},
		}),
	)
	if err != nil {
		tb.Fatalf("orchestrationtest: composing a factory: %v", err)
		return nil
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("orchestrationtest: opening a loopback listener: %v", err)
		return nil
	}

	fixture := &FactoryFixture{
		Server:    server,
		Listener:  listener,
		BaseURL:   "http://" + listener.Addr().String(),
		Authorize: authorizer,
		Placement: placement,
		Directory: directory,
		served:    make(chan error, 1),
	}
	fixture.Client = &http.Client{Timeout: 10 * time.Second}
	go func() { fixture.served <- server.Serve(listener) }()
	tb.Cleanup(func() { fixture.Stop(tb) })
	return fixture
}

// Stop shuts the server down and asserts Serve returned http.ErrServerClosed.
//
// It asserts the RETURN, not merely that Stop did not error, because a Serve
// that returned early for some other reason leaves a listener assertion
// passing for the wrong reason: nothing is listening, so nothing leaked.
func (f *FactoryFixture) Stop(tb TB) {
	tb.Helper()
	f.stopped.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := f.Server.Stop(ctx); err != nil {
			tb.Errorf("orchestrationtest: stopping the factory: %v", err)
		}
		select {
		case err := <-f.served:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				tb.Errorf("orchestrationtest: Serve returned %v, want http.ErrServerClosed", err)
			}
		case <-time.After(10 * time.Second):
			tb.Errorf("orchestrationtest: Serve did not return after Stop")
		}
	})
}

// Get issues an authenticated GET against a served path and returns the status
// and body. It sets Host to KitHost so the trusted-origin guard passes; a
// bearer credential is CSRF-exempt by Factory's documented guard order, which
// is why no token is needed for a read.
func (f *FactoryFixture) Get(tb TB, ctx context.Context, path string) (int, []byte) {
	tb.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.BaseURL+path, nil)
	if err != nil {
		tb.Fatalf("orchestrationtest: building a request for %q: %v", path, err)
		return 0, nil
	}
	req.Host = KitHost
	req.Header.Set("Authorization", "Bearer "+KitActorCredential)
	resp, err := f.Client.Do(req)
	if err != nil {
		tb.Fatalf("orchestrationtest: requesting %q: %v", path, err)
		return 0, nil
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		tb.Fatalf("orchestrationtest: reading %q: %v", path, err)
		return 0, nil
	}
	return resp.StatusCode, body
}

// FixedBearerVerifier accepts exactly one bearer value for one tenant.
//
// ExpiresAt is computed from the KIT'S clock, not time.Now. Factory compares
// expiry against the Clock it was composed with, so a fixture pinned at an
// epoch and claims expiring an hour after wall-clock now would be either
// always-expired or never-expired depending on which clock won -- a fake that
// is looser than the dependency in the most literal sense.
type FixedBearerVerifier struct {
	Tenant     sessionwire.TenantID
	Credential string
	Clock      *Clock

	mu    sync.Mutex
	calls int
}

// VerifyCredential satisfies identity.Verifier.
func (v *FixedBearerVerifier) VerifyCredential(_ context.Context, credential identity.Credential) (identity.Claims, error) {
	v.mu.Lock()
	v.calls++
	v.mu.Unlock()
	if credential.Value() != v.Credential {
		return identity.Claims{}, identity.ErrUnauthenticated
	}
	return identity.Claims{
		Tenant:    v.Tenant,
		Subject:   "orchestrationtest-subject",
		Kind:      identity.KindActor,
		ExpiresAt: v.Clock.Now().Add(time.Hour),
	}, nil
}

// Calls reports how many credentials were presented.
func (v *FixedBearerVerifier) Calls() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.calls
}

// RecordingAuthorizer allows everything and records what it was asked.
//
// It cannot be used to assert a 403. Factory's public identity package exports
// no authorization sentinel -- the one a denial must wrap,
// internal/identity.ErrUnauthorized, is unreachable from another module, and
// Factory's own source records this as an open defect that must be closed
// before factory v0.1.0. Any other error an external Authorizer returns is
// classified as a different failure class. So this fixture asserts THAT the
// authorizer was consulted, never what a denial renders as; claiming the
// latter would be a case that passes for the wrong reason.
type RecordingAuthorizer struct {
	mu    sync.Mutex
	calls []string
}

func (a *RecordingAuthorizer) record(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, name)
}

// Calls reports the authorization questions asked, in order.
func (a *RecordingAuthorizer) Calls() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.calls...)
}

func (a *RecordingAuthorizer) AuthorizeSessionList(context.Context, identity.Principal) error {
	a.record("AuthorizeSessionList")
	return nil
}

func (a *RecordingAuthorizer) AuthorizeSessionRead(context.Context, identity.Principal, sessionwire.SessionID) error {
	a.record("AuthorizeSessionRead")
	return nil
}

func (a *RecordingAuthorizer) AuthorizeObjectRead(context.Context, identity.Principal, sessionwire.SessionID, sessionwire.ObjectReference) error {
	a.record("AuthorizeObjectRead")
	return nil
}

func (a *RecordingAuthorizer) AuthorizeControl(context.Context, identity.Principal, sessionwire.SessionID, sessionstore.CommandKind) error {
	a.record("AuthorizeControl")
	return nil
}

func (a *RecordingAuthorizer) AuthorizeSubscribe(context.Context, identity.Principal, string) error {
	a.record("AuthorizeSubscribe")
	return nil
}

func (a *RecordingAuthorizer) AuthorizeServiceSweep(context.Context, identity.Principal) error {
	a.record("AuthorizeServiceSweep")
	return nil
}

// RecordingPlacement satisfies factory.PlacementController and records intent.
type RecordingPlacement struct {
	mu       sync.Mutex
	ensured  []sessionstore.DesiredWorkload
	released int
}

// EnsurePlacement satisfies factory.PlacementController.
func (p *RecordingPlacement) EnsurePlacement(_ context.Context, desired sessionstore.DesiredWorkload) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensured = append(p.ensured, desired)
	return nil
}

// ReleasePlacement satisfies factory.PlacementController.
func (p *RecordingPlacement) ReleasePlacement(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.released++
	return nil
}

// Ensured reports every desired workload asked for.
func (p *RecordingPlacement) Ensured() []sessionstore.DesiredWorkload {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sessionstore.DesiredWorkload(nil), p.ensured...)
}

// Released reports how many placements were released.
func (p *RecordingPlacement) Released() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.released
}

// StoreDirectory adapts the real Store to factory.Directory.
type StoreDirectory struct{ Store *sessionstore.Store }

// Owner satisfies factory.Directory.
func (d *StoreDirectory) Owner(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	entry, err := d.Store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{TenantID: tenant, SessionID: session})
	if err != nil {
		var registrationErr *sessionstore.PointerError
		if errors.As(err, &registrationErr) {
			return sessionwire.HostLinkRegistryObservation{}, false, nil
		}
		return sessionwire.HostLinkRegistryObservation{}, false, err
	}
	observation, err := entry.Registration.Observation()
	if err != nil {
		return sessionwire.HostLinkRegistryObservation{}, false, err
	}
	return observation, true, nil
}

// Candidates satisfies factory.Directory.
func (d *StoreDirectory) Candidates(ctx context.Context, req sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	return d.Store.ListCompatibleHosts(ctx, req)
}
