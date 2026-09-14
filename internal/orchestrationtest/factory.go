//go:build integration && orchestration

package orchestrationtest

import (
	"bytes"
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

	// serveWait bounds how long Stop waits for Serve to return. It is a field
	// rather than a constant ONLY so the kit's own positive control can reach
	// the timeout branch without costing the suite ten seconds; production
	// fixtures never set it.
	serveWait time.Duration
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
	return NewFactoryFixtureWithSeams(tb, store, clock, FactorySeams{Reader: reader})
}

// FactorySeams names the collaborators a case wants to choose for itself.
//
// Every zero member takes the kit's default, so a case states only the seam it
// is making a claim about. Commands and Placement exist here so a case can
// substitute PanicCommands and PanicPlacement: a seam that factory.New accepts
// and never calls is an ABSENCE claim, and an absence claim with no probe
// behind it is as wide as an unverified presence claim.
type FactorySeams struct {
	// Reader is the durable read plane. Nil takes the real Store.
	Reader SessionReaderSeam
	// Commands is the durable command plane. Nil takes the real Store.
	Commands factory.Commands
	// Placement is the placement controller. Nil takes a RecordingPlacement,
	// which is also what FactoryFixture.Placement reports; a case that
	// substitutes its own gets a nil there and must not read it.
	Placement factory.PlacementController
	// Directory is the observed target directory. Nil takes a StoreDirectory
	// over the real Store.
	Directory factory.Directory
}

// NewFactoryFixtureWithSeams composes a Factory over chosen collaborators.
func NewFactoryFixtureWithSeams(tb TB, store *StoreFixture, clock *Clock, seams FactorySeams) *FactoryFixture {
	tb.Helper()
	authorizer := &RecordingAuthorizer{}

	var reader factory.SessionReader = store.Store
	if seams.Reader != nil {
		reader = seams.Reader
	}
	var commands factory.Commands = store.Store
	if seams.Commands != nil {
		commands = seams.Commands
	}
	var recording *RecordingPlacement
	var placement factory.PlacementController
	if seams.Placement != nil {
		placement = seams.Placement
	} else {
		recording = &RecordingPlacement{}
		placement = recording
	}
	var storeDirectory *StoreDirectory
	var directory factory.Directory
	if seams.Directory != nil {
		directory = seams.Directory
	} else {
		storeDirectory = &StoreDirectory{Store: store.Store}
		directory = storeDirectory
	}

	server, err := factory.New(
		factory.WithCredentialVerifier(&FixedBearerVerifier{Tenant: store.Tenant, Credential: KitActorCredential, Clock: clock}),
		factory.WithAuthorizer(authorizer),
		factory.WithSessionReader(reader),
		factory.WithCommands(commands),
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
		Placement: recording,
		Directory: storeDirectory,
		served:    make(chan error, 1),
		serveWait: 10 * time.Second,
	}
	fixture.Client = &http.Client{Timeout: 10 * time.Second}
	// Capture the channel in a LOCAL. Reading fixture.served inside the
	// goroutine would dereference the field at send time, which is both a data
	// race with any test that swaps it and the reason the kit's own "Serve
	// never returns" control first failed: the goroutine delivered into the
	// replacement channel instead of the original.
	results := fixture.served
	go func() { results <- server.Serve(listener) }()
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
		case <-time.After(f.serveWait):
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

// Post issues an authenticated POST with a JSON body and returns the status and
// body.
//
// It exists because the kit's GET helper cannot probe a control route: the
// control routes are POST-only, so a GET reads 405 and a row built on one would
// pass for the wrong reason -- which is exactly why the kit's earlier
// NotComposedRoutes map excluded them and booked them as owed instead.
//
// A bearer credential is CSRF-exempt by Factory's documented guard order, which
// is why no token is fetched here. A cookie-authenticated control POST would
// need one, and this helper must not be extended to cookies without it.
func (f *FactoryFixture) Post(tb TB, ctx context.Context, path string, body []byte) (int, []byte) {
	tb.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		tb.Fatalf("orchestrationtest: building a POST for %q: %v", path, err)
		return 0, nil
	}
	req.Host = KitHost
	req.Header.Set("Authorization", "Bearer "+KitActorCredential)
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.Client.Do(req)
	if err != nil {
		tb.Fatalf("orchestrationtest: posting %q: %v", path, err)
		return 0, nil
	}
	defer func() { _ = resp.Body.Close() }()
	answer, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		tb.Fatalf("orchestrationtest: reading the answer to %q: %v", path, err)
		return 0, nil
	}
	return resp.StatusCode, answer
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
		// "No owner" is *RegistryError with one of three codes, NOT a
		// *PointerError -- which is what this adapter originally matched, and
		// it was wrong. Nothing caught it until a case actually CALLED this
		// seam: a composed-but-undriven adapter is an untested adapter, and
		// this is what that costs.
		//
		// Expired and released are absences too, not failures: a registration
		// whose lease has lapsed or been given up names no current owner, and
		// reporting them as errors would make Factory treat an ordinary
		// handover as a durable-plane fault.
		var registryErr *sessionstore.RegistryError
		if errors.As(err, &registryErr) {
			switch registryErr.Code {
			case sessionstore.RegistryErrorNotFound,
				sessionstore.RegistryErrorExpired,
				sessionstore.RegistryErrorReleased:
				return sessionwire.HostLinkRegistryObservation{}, false, nil
			}
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
