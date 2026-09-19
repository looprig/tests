//go:build integration

package orchestrationtest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/sessionstore"
)

// ComposedHost is a RUNNING Host: host.Compose over the kit's shared durable
// Composite, started, and served on a real loopback listener.
//
// It is what host v0.2.1's exported composition surface made possible. Until
// then HostFixture could build only a *host.Host configuration value; everything
// that ran a Host was under internal/. Nothing below is a stand-in for Host:
// the Service, its HostLink transport, its residency manager, its durable
// command consumer and the sessionstore.Store it opens are all the released
// module's own. The fakes are the seams a DEPLOYER supplies and Host ships no
// implementation of -- the Rig (a product's agent runtime), the settlement
// evidence reader (the runtime's journal), the checkpointer, the workspaces,
// and the tenant verifier -- and each one is named where it is built.
//
// The server in front of the Handler is a PASSIVE tap (see HostLinkTap). It
// records the upgrade and the frames in both directions and modifies nothing;
// the B8 reviewers' harness needed a shim that rewrote the request to get past
// Host's JSON-protocol gate, and this one must not.
type ComposedHost struct {
	Service  *host.Service
	Rig      *FakeRig
	Runtime  *FakeRuntime
	Evidence *GatedEvidence
	Tap      *HostLinkTap
	Auth     *FixedCredentialAuth

	// Base is this Host's advertised HostLink BASE, exactly as it appears in
	// the registration and the capacity report: ws://<loopback>, with no path.
	//
	// host v0.3.0 refuses anything else at composition, and factory v0.5.0
	// derives each tenant's address from it. The kit advertised
	// ws://<loopback>/hostlink/<tenant> up to host v0.2.1; that spelling is
	// now refused with base_names_tenant, which is why this field exists
	// beside Endpoint rather than replacing its meaning.
	Base sessionwire.InternalEndpoint

	// Endpoint is the tenant's DERIVED HostLink address --
	// sessionwire.HostLinkEndpoint(Base, tenant) -- which is what a dial must
	// use. Host advertises Base, not this.
	Endpoint   sessionwire.InternalEndpoint
	ID         sessionwire.HostID
	Generation uint64
	Tenant     sessionwire.TenantID
	Agent      sessionwire.AgentID
	Compat     department.CompatibilityID
	BindingID  string

	server  *httptest.Server
	stopped sync.Once
}

// ComposedHostConfig names what a case chooses about a composed Host.
type ComposedHostConfig struct {
	ID               sessionwire.HostID
	Generation       uint64
	Agent            sessionwire.AgentID
	Compatibility    department.CompatibilityID
	StorageBindingID string
}

// ComposedHostReconcileInterval is the consumer's timer, and it is set far
// beyond any case's lifetime ON PURPOSE.
//
// Host's durable consumer runs one pass on attach and then waits for a HINT or
// this timer (host/internal/commands/consumer.go Run). A case that wants to say
// "Host received a command delivery" must be able to exclude the timer as the
// cause of a pass, and an hour does that for a case bounded in seconds. Do not
// shorten it to make a case faster: a pass the timer drove would pass the
// delivery assertion without any delivery having happened.
const ComposedHostReconcileInterval = time.Hour

// NewComposedHost composes, starts and serves a pooled Host over store's
// Backend. Its cleanup drains the Host and closes the listener, bounded.
func NewComposedHost(tb TB, ctx context.Context, store *StoreFixture, cfg ComposedHostConfig) *ComposedHost {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("orchestrationtest: opening the host listener: %v", err)
		return nil
	}
	base := sessionwire.InternalEndpoint("ws://" + listener.Addr().String())
	endpoint, err := sessionwire.HostLinkEndpoint(base, store.Tenant)
	if err != nil {
		_ = listener.Close()
		tb.Fatalf("orchestrationtest: deriving the tenant HostLink endpoint from %q: %v", base, err)
		return nil
	}

	runtime := NewFakeRuntime(kitRuntimeUUID)
	rig := &FakeRig{Session: runtime}
	evidence := &GatedEvidence{Store: store.Journal, Runtime: runtime}
	// The trivial tenant verifier the owner direction (2026-09-18) asks for:
	// auth is the application's job, so the fixture accepts exactly one
	// credential for exactly one tenant and adds nothing else.
	auth := &FixedCredentialAuth{Tenant: store.Tenant, Credential: KitServiceToken}
	workspaces := NewTempWorkspaces(tb)

	blueprint := host.Composition{
		Options: host.Options{
			HostID:            cfg.ID,
			InternalEndpoint:  base,
			IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
			Placement:         sessionwire.HostPlacementPooled,
			Capacity:          4,
			WarmTTL:           10 * time.Minute,
			RegistryHeartbeat: 10 * time.Second,
			RegistryExpiry:    60 * time.Second,
			ClaimTTL:          5 * time.Second,
			ApplyDeadline:     30 * time.Second,
			CommandQueueSize:  16,
			ReconcileInterval: ComposedHostReconcileInterval,
			ReconcileBatch:    32,
		},
		Generation:           cfg.Generation,
		Link:                 host.LinkOptions{MaxBindingsPerLink: 4, MaxBindings: 8, MaxTenantLinks: 3},
		Drain:                host.DrainOptions{Grace: 10 * time.Second, IdleBoundary: 2 * time.Second, PublishBound: 2 * time.Second},
		CompatibilityTimeout: 5 * time.Second,
		WorkPoll:             time.Second,
		Collaborators: host.Collaborators{
			Backend: store.Backend,
			// The SAME shard layout the kit's own Store was opened with. The
			// layout is recorded in the backend at first open, and a second
			// opener naming another shard count is refused with
			// keyspace layout_mismatch -- two processes over one durable plane
			// must agree on it, and this is where the Host side says so.
			StoreOptions: []sessionstore.Option{sessionstore.WithControlShards(KitControlShards)},
			JournalStores: map[host.EvidenceKey]sessionstore.DispositionEvidenceReader{
				{TenantID: store.Tenant, StorageBindingID: cfg.StorageBindingID}: evidence,
			},
			Registrar: host.RegistrarFunc(func(context.Context) ([]department.Registration, error) {
				target, err := department.NewRigTarget(rig, cfg.Compatibility, KitCapabilities())
				if err != nil {
					return nil, err
				}
				return []department.Registration{{AgentID: cfg.Agent, Target: target}}, nil
			}),
			Checkpointer: inertCheckpointer{},
			Auth:         auth,
			Workspaces:   workspaces,
			NamespaceLayout: func(tenant sessionwire.TenantID, session sessionwire.SessionID) string {
				return string(tenant) + "/" + string(session)
			},
			// RigSessionIDs is deliberately NOT supplied. host v0.3.0
			// deprecated it and consults it only for an UNBOUND (legacy)
			// record, and a create over a legacy record is now refused
			// outright -- so a kit that still supplied it would be describing
			// a path no shipped Factory produces. The runtime identity comes
			// from the binding's RuntimeSessionID instead; see KitBinding.
		},
	}
	service, err := host.Compose(ctx, blueprint)
	if err != nil {
		_ = listener.Close()
		tb.Fatalf("orchestrationtest: host.Compose: %v", err)
		return nil
	}
	if err := service.Start(ctx); err != nil {
		_ = listener.Close()
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = service.Stop(stopCtx)
		tb.Fatalf("orchestrationtest: starting the composed host: %v", err)
		return nil
	}

	tap := NewHostLinkTap()
	server := &httptest.Server{Listener: listener, Config: &http.Server{
		Handler:           tap.Wrap(service.Handler()),
		ReadHeaderTimeout: 10 * time.Second,
	}}
	server.Start()

	composed := &ComposedHost{
		Service:    service,
		Rig:        rig,
		Runtime:    runtime,
		Evidence:   evidence,
		Tap:        tap,
		Auth:       auth,
		Base:       base,
		Endpoint:   endpoint,
		ID:         cfg.ID,
		Generation: cfg.Generation,
		Tenant:     store.Tenant,
		Agent:      cfg.Agent,
		Compat:     cfg.Compatibility,
		BindingID:  cfg.StorageBindingID,
		server:     server,
	}
	tb.Cleanup(func() { composed.Stop(tb) })
	return composed
}

// URL is the HTTP form of the tenant's HostLink endpoint, for raw probes.
func (h *ComposedHost) URL() string {
	return "http" + string(h.Endpoint)[len("ws"):]
}

// Stop drains the Host and closes its listener, bounded. It is idempotent.
func (h *ComposedHost) Stop(tb TB) {
	tb.Helper()
	h.stopped.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if _, err := h.Service.Stop(ctx); err != nil {
			tb.Errorf("orchestrationtest: stopping the composed host: %v", err)
		}
		h.server.CloseClientConnections()
		h.server.Close()
	})
}

// Attach makes a session resident through Host's exported, in-process attach --
// the same entry point the hostlink.attach RPC reaches.
func (h *ComposedHost) Attach(tb TB, ctx context.Context, session sessionwire.SessionID, mode sessionwire.HostLinkAttachMode) host.Residency {
	tb.Helper()
	residency, err := h.Service.Attach(ctx, host.AttachRequest{
		TenantID:               h.Tenant,
		SessionID:              session,
		AgentID:                h.Agent,
		Mode:                   mode,
		RuntimeCompatibilityID: string(h.Compat),
		ActorID:                "orchestrationtest-attacher",
	})
	if err != nil {
		tb.Fatalf("orchestrationtest: attaching %s on the composed host: %v", session, err)
		return host.Residency{}
	}
	if residency.LeaseEpoch == 0 || residency.SessionID != session || !residency.Attached {
		tb.Fatalf("orchestrationtest: attach returned %+v, want a fresh residency with an epoch", residency)
	}
	return residency
}

// inertCheckpointer satisfies host.Checkpointer. The kit's runtime holds no
// conversation state and its workspaces are temporary, so there is nothing a
// release could lose.
type inertCheckpointer struct{}

func (inertCheckpointer) Checkpoint(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}

// ReleaseWorkspace satisfies host.Workspaces. The temporary root is removed at
// cleanup, and a release never deletes durable state.
func (w *TempWorkspaces) ReleaseWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}

// ErrEvidenceNotYetReadable is what GatedEvidence answers while its gate is shut.
var ErrEvidenceNotYetReadable = errors.New("orchestrationtest: the runtime's disposition is not yet readable")

// GatedEvidence is the kit's settlement evidence reader.
//
// It EMBEDS a real *harness sessionstore.Store, and that embedding is load
// bearing rather than decorative. As of host v0.3.0 this seam is two things at
// once: the settlement evidence reader (which this type overrides) AND the
// runtime journal an attach reads to decide whether a conversation already
// exists (which the embedded store answers). host.Compose REFUSES a reader that
// is not a harness store, so the kit's previous stand-alone fake made every
// create fail at composition. Do not "simplify" the embedding away.
//
// It answers from what the kit's runtime ACTUALLY RECORDED. An applied
// disposition is reported only for an attempt FakeRuntime was handed, so a
// settlement can never be vouched for a dispatch that did not happen; anything
// else is refused, which the store treats as "not yet" and leaves the command
// applying -- the released contract for absence.
//
// The GATE is what lets a case separate two consumer passes. While it is shut
// every read is refused, so the attach pass dispatches and then blocks; once
// Open is called the NEXT pass can settle. Which event drives that next pass is
// what the case measures.
type GatedEvidence struct {
	*harnessstore.Store
	Runtime *FakeRuntime

	mu    sync.Mutex
	open  bool
	reads []sessionstore.DispositionEvidenceRequest
	read  chan struct{}
}

// Open lets subsequent reads report what the runtime recorded.
func (e *GatedEvidence) Open() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.open = true
}

// Reads reports every evidence request the store made, in order.
func (e *GatedEvidence) Reads() []sessionstore.DispositionEvidenceRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sessionstore.DispositionEvidenceRequest(nil), e.reads...)
}

// AwaitReads blocks until at least n reads happened, failing the case at ctx's
// deadline rather than hanging.
func (e *GatedEvidence) AwaitReads(tb TB, ctx context.Context, n int) []sessionstore.DispositionEvidenceRequest {
	tb.Helper()
	for {
		e.mu.Lock()
		if len(e.reads) >= n {
			reads := append([]sessionstore.DispositionEvidenceRequest(nil), e.reads...)
			e.mu.Unlock()
			return reads
		}
		if e.read == nil {
			e.read = make(chan struct{})
		}
		wake := e.read
		e.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			tb.Fatalf("orchestrationtest: waited for %d settlement evidence reads, saw %d: %v", n, len(e.Reads()), ctx.Err())
			return nil
		}
	}
}

// ReadDispositionEvidence satisfies sessionstore.DispositionEvidenceReader.
func (e *GatedEvidence) ReadDispositionEvidence(_ context.Context, req sessionstore.DispositionEvidenceRequest) (sessionstore.DispositionEvidence, error) {
	e.mu.Lock()
	e.reads = append(e.reads, req)
	open := e.open
	if e.read != nil {
		close(e.read)
		e.read = nil
	}
	e.mu.Unlock()
	if !open {
		return sessionstore.DispositionEvidence{}, ErrEvidenceNotYetReadable
	}
	for _, applied := range e.Runtime.Applied() {
		if applied.CommandID == req.CommandID && applied.AttemptID == string(req.Attempt.AttemptID) {
			return sessionstore.DispositionEvidence{
				AttemptID:           req.Attempt.AttemptID,
				Kind:                sessionstore.DispositionApplied,
				AttemptJournalEpoch: req.Attempt.JournalEpoch,
				AuthorJournalEpoch:  req.Attempt.JournalEpoch,
				DispositionSeq:      1,
			}, nil
		}
	}
	return sessionstore.DispositionEvidence{}, fmt.Errorf("%w: the runtime recorded no dispatch of %s under attempt %s",
		ErrEvidenceNotYetReadable, req.CommandID, req.Attempt.AttemptID)
}
