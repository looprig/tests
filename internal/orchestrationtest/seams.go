//go:build integration && orchestration

package orchestrationtest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

// This file holds the seams factory.New REQUIRED as of A9.1 stage 2.
//
// Before stage 2, factory.New accepted seven options and composed a read plane.
// It now REFUSES a composition missing any of WithCatalog, WithGates,
// WithHostLinkCredential, WithHostTargets, WithReplicaID or WithServiceIdentity,
// with a typed *MissingSeamsError naming every one of them at once. That refusal
// is the reason this file exists, and it is worth recording as the loudest
// signal in this whole re-fit: the old kit composition did not silently lose
// capability, it stopped composing at all.
//
// Every adapter below is an ADAPTER over the real *sessionstore.Store wherever
// the Store can answer, and a FAKE only where Factory ships no implementation
// and the Store has no method. Each one says which it is.

// KitControlShards is the fixed control-shard count the kit's command and gate
// seams report.
//
// It is 4 rather than 1, and that is a structural choice rather than a value
// one. A shard count of 1 makes "the sweep visits each shard round-robin"
// indistinguishable from "the sweep reads the only shard there is" -- the
// degenerate constant for a rotation is exactly 1, and a case built on it would
// pass against a sweeper that had no rotation at all.
const KitControlShards = 4

// StoreCommands adapts the real *sessionstore.Store to factory.Commands.
//
// Five of the six methods are the Store's own. ControlShards is NOT: the Store
// exposes no such method, and the count is a property of the DEPLOYMENT's fixed
// shard layout rather than of the store, so a composition root supplies it. It
// is an adapter with one configured value, not a fake.
type StoreCommands struct {
	Store  *sessionstore.Store
	Shards int

	mu       sync.Mutex
	due      []sessionstore.ListDueCommandsRequest
	claims   []sessionstore.AcquireReconciliationClaimRequest
	admitted int
}

// NewStoreCommands adapts store with the kit's shard count.
func NewStoreCommands(store *sessionstore.Store) *StoreCommands {
	return &StoreCommands{Store: store, Shards: KitControlShards}
}

// ControlShards satisfies factory.Commands.
func (c *StoreCommands) ControlShards() int { return c.Shards }

// DueRequests reports every due-page query a sweep made, in order.
func (c *StoreCommands) DueRequests() []sessionstore.ListDueCommandsRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sessionstore.ListDueCommandsRequest(nil), c.due...)
}

// Admitted reports how many commands were admitted through this seam.
func (c *StoreCommands) Admitted() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.admitted
}

// AdmitCommand satisfies factory.Commands.
func (c *StoreCommands) AdmitCommand(ctx context.Context, req sessionstore.AdmitCommandRequest) (sessionstore.InboxEntry, bool, error) {
	c.mu.Lock()
	c.admitted++
	c.mu.Unlock()
	return c.Store.AdmitCommand(ctx, req)
}

// GetCommand satisfies factory.Commands.
func (c *StoreCommands) GetCommand(ctx context.Context, req sessionstore.GetCommandRequest) (sessionstore.InboxEntry, error) {
	return c.Store.GetCommand(ctx, req)
}

// RejectCommand satisfies factory.Commands.
func (c *StoreCommands) RejectCommand(ctx context.Context, req sessionstore.RejectCommandRequest) (sessionstore.InboxEntry, error) {
	return c.Store.RejectCommand(ctx, req)
}

// ListDueCommands satisfies factory.Commands and records the query.
func (c *StoreCommands) ListDueCommands(ctx context.Context, req sessionstore.ListDueCommandsRequest) (sessionstore.DueCommandPage, error) {
	c.mu.Lock()
	c.due = append(c.due, req)
	c.mu.Unlock()
	return c.Store.ListDueCommands(ctx, req)
}

// ClaimRequests reports every reconciliation claim this replica's sweeper
// attempted, in order.
//
// It exists because the holder id is the ONLY thing that suppresses duplicate
// work between two replicas, and nothing else in this module can see which
// string Factory actually files. A row that called the store directly would be
// exercising SessionStore's compare-and-swap against two strings the TEST
// supplies -- which is what this case used to do, and a Factory filing every
// claim under one constant holder id survived it.
func (c *StoreCommands) ClaimRequests() []sessionstore.AcquireReconciliationClaimRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sessionstore.AcquireReconciliationClaimRequest(nil), c.claims...)
}

// ClaimHolders reports the distinct holder ids this replica's sweeper filed
// under.
func (c *StoreCommands) ClaimHolders() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[string]bool{}
	holders := []string{}
	for _, req := range c.claims {
		if !seen[req.HolderID] {
			seen[req.HolderID] = true
			holders = append(holders, req.HolderID)
		}
	}
	return holders
}

// AcquireReconciliationClaim satisfies factory.Commands and records the claim.
func (c *StoreCommands) AcquireReconciliationClaim(ctx context.Context, req sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	c.mu.Lock()
	c.claims = append(c.claims, req)
	c.mu.Unlock()
	return c.Store.AcquireReconciliationClaim(ctx, req)
}

// ReleaseReconciliationClaim satisfies factory.Commands.
func (c *StoreCommands) ReleaseReconciliationClaim(ctx context.Context, req sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	return c.Store.ReleaseReconciliationClaim(ctx, req)
}

// StoreGates adapts the real *sessionstore.Store to factory.Gates. See
// StoreCommands for why ControlShards is configured rather than delegated.
type StoreGates struct {
	Store  *sessionstore.Store
	Shards int

	mu      sync.Mutex
	due     []sessionstore.ListDueGatesRequest
	retired []sessionstore.RetireGateDeadlineIntentRequest
}

// NewStoreGates adapts store with the kit's shard count.
func NewStoreGates(store *sessionstore.Store) *StoreGates {
	return &StoreGates{Store: store, Shards: KitControlShards}
}

// ControlShards satisfies factory.Gates.
func (g *StoreGates) ControlShards() int { return g.Shards }

// DueRequests reports every due-gate query a sweep made, in order.
func (g *StoreGates) DueRequests() []sessionstore.ListDueGatesRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]sessionstore.ListDueGatesRequest(nil), g.due...)
}

// Retired reports every gate deadline intent a sweep retired, in order.
func (g *StoreGates) Retired() []sessionstore.RetireGateDeadlineIntentRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]sessionstore.RetireGateDeadlineIntentRequest(nil), g.retired...)
}

// ListDueGates satisfies factory.Gates and records the query.
func (g *StoreGates) ListDueGates(ctx context.Context, req sessionstore.ListDueGatesRequest) (sessionstore.DueGatePage, error) {
	g.mu.Lock()
	g.due = append(g.due, req)
	g.mu.Unlock()
	return g.Store.ListDueGates(ctx, req)
}

// RetireGateDeadlineIntent satisfies factory.Gates and records the write.
func (g *StoreGates) RetireGateDeadlineIntent(ctx context.Context, req sessionstore.RetireGateDeadlineIntentRequest) error {
	g.mu.Lock()
	g.retired = append(g.retired, req)
	g.mu.Unlock()
	return g.Store.RetireGateDeadlineIntent(ctx, req)
}

// StoreHostTargets adapts the real Store to factory.HostTargets. Every byte is
// the Store's own; the recorder exists so a case can prove the record sweep ran
// rather than infer it from elapsed time.
type StoreHostTargets struct {
	Store *sessionstore.Store

	mu    sync.Mutex
	calls int
}

// Calls reports how many target reconciliations the record sweep performed.
func (h *StoreHostTargets) Calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// ReconcileHostTargets satisfies factory.HostTargets.
func (h *StoreHostTargets) ReconcileHostTargets(ctx context.Context, req sessionstore.ReconcileHostTargetsRequest) (sessionstore.HostTargetReconcileResult, error) {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	return h.Store.ReconcileHostTargets(ctx, req)
}

// FixedServiceToken satisfies factory.HostLinkCredential.
//
// This one IS a fake, and it has to be: a HostLink service token is minted by a
// deployment's own credential source and Factory ships no implementation.
type FixedServiceToken struct {
	Token string

	mu    sync.Mutex
	calls int
}

// Calls reports how many tokens were requested.
func (t *FixedServiceToken) Calls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

// ServiceToken satisfies factory.HostLinkCredential.
func (t *FixedServiceToken) ServiceToken(context.Context) (string, error) {
	t.mu.Lock()
	t.calls++
	t.mu.Unlock()
	return t.Token, nil
}

// KitServiceToken is the one HostLink service token the kit mints.
const KitServiceToken = "orchestrationtest-service-token"

// ErrObjectNotPermitted is what the kit's object policy returns for a reference
// it will not authorize.
var ErrObjectNotPermitted = errors.New("orchestrationtest: object reference not permitted")

// AllowListObjectPolicy satisfies factory.ObjectPolicy over an explicit set of
// permitted references.
//
// It is a FAKE because Factory ships no ObjectPolicy -- the interface's own
// documentation says "A9 must supply the production policy; this interface does
// not implement one". It is an ALLOW-LIST rather than an allow-all, deliberately:
// the seam's contract is that authorization comes from committed session
// evidence and never from a caller's reference syntax, and an allow-all fake
// would be looser than the dependency in exactly the way this program has been
// burned by before. A reference nobody permitted is refused.
type AllowListObjectPolicy struct {
	mu        sync.Mutex
	permitted map[string]sessionstore.ObjectKind
	asked     []sessionwire.ObjectReference
}

// NewAllowListObjectPolicy returns a policy that permits nothing yet.
func NewAllowListObjectPolicy() *AllowListObjectPolicy {
	return &AllowListObjectPolicy{permitted: make(map[string]sessionstore.ObjectKind)}
}

// Permit authorizes one object reference as one kind.
func (p *AllowListObjectPolicy) Permit(ref sessionwire.ObjectReference, kind sessionstore.ObjectKind) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.permitted[ref.ObjectID] = kind
}

// Asked reports every reference the router put to this policy, in order. It is
// the evidence that an object read reached authorization at all.
func (p *AllowListObjectPolicy) Asked() []sessionwire.ObjectReference {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sessionwire.ObjectReference(nil), p.asked...)
}

// AuthorizeReference satisfies factory.ObjectPolicy.
func (p *AllowListObjectPolicy) AuthorizeReference(_ context.Context, _ identity.Principal, _ sessionstore.CatalogEntry, ref sessionwire.ObjectReference) (sessionstore.ObjectKind, error) {
	p.mu.Lock()
	p.asked = append(p.asked, ref)
	kind, ok := p.permitted[ref.ObjectID]
	p.mu.Unlock()
	if !ok {
		return "", ErrObjectNotPermitted
	}
	return kind, nil
}

// StoreObjectReader adapts the real Store to factory.ObjectReader.
type StoreObjectReader struct {
	Store *sessionstore.Store

	mu    sync.Mutex
	reads int
}

// Reads reports how many object bodies were fetched through this reader.
func (r *StoreObjectReader) Reads() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads
}

// GetObjectMetadata satisfies factory.ObjectReader.
func (r *StoreObjectReader) GetObjectMetadata(ctx context.Context, req sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error) {
	return r.Store.GetObjectMetadata(ctx, req)
}

// GetObject satisfies factory.ObjectReader.
func (r *StoreObjectReader) GetObject(ctx context.Context, req sessionstore.GetObjectRequest) (io.ReadCloser, error) {
	r.mu.Lock()
	r.reads++
	r.mu.Unlock()
	return r.Store.GetObject(ctx, req)
}

// RecordingWorkloads satisfies factory.WorkloadController and records intent.
type RecordingWorkloads struct {
	mu      sync.Mutex
	ensured []sessionstore.PlacementIntent
}

// Ensured reports every workload intent, in order.
func (w *RecordingWorkloads) Ensured() []sessionstore.PlacementIntent {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]sessionstore.PlacementIntent(nil), w.ensured...)
}

// EnsureWorkload satisfies factory.WorkloadController.
func (w *RecordingWorkloads) EnsureWorkload(_ context.Context, intent sessionstore.PlacementIntent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ensured = append(w.ensured, intent)
	return nil
}

// KitLaunchTemplate is the one pooled launch target the kit's deployments
// advertise.
//
// It exists because /v1/agents aggregates the deployment's CONFIGURED templates
// against the observed directory: with no template the route answers an empty
// list whatever the directory holds, which is the "reachable but not
// discriminating" state the first pass of this work had to record rather than
// assert. WithDepartment now exists, so the route can discriminate and the old
// trip-wire is retired.
func KitLaunchTemplate(agent sessionwire.AgentID, compatibility string) factory.LaunchTemplate {
	return factory.LaunchTemplate{
		Key: sessionstore.HostTargetKey{
			AgentID:                agent,
			RuntimeCompatibilityID: compatibility,
			Placement:              sessionwire.HostPlacementPooled,
		},
		Capabilities: []string{"orchestrationtest-capability"},
	}
}

// SeedBoundSession creates a catalog entry carrying a NON-ZERO SessionBinding.
//
// It exists because the binding is a STRUCTURAL input to the object route, not
// a value one. internal/httpapi/objects.go branches on
// `entry.Record.Binding != unbound`: a zero binding reads through the composed
// SessionReader and NEVER consults the resolver, while a non-zero one goes
// through ObjectStoreResolver and fails closed if it is absent. A case that only
// seeded one of the two would sweep one arm of that branch and report it as
// coverage of the route.
func (f *StoreFixture) SeedBoundSession(ctx context.Context, agent sessionwire.AgentID, compatibility string, binding sessionstore.SessionBinding) sessionwire.SessionID {
	f.tb.Helper()
	session := sessionwire.SessionID("session-" + randomSuffix(f.tb))
	now := f.Clock.Now()
	if _, _, err := f.Store.CreateCatalogEntry(ctx, sessionstore.CreateCatalogEntryRequest{
		Binding:                binding,
		TenantID:               f.Tenant,
		SessionID:              session,
		AgentID:                agent,
		RuntimeCompatibilityID: compatibility,
		CreatedAt:              now,
		LastActiveAt:           now,
		State:                  sessionwire.SessionStateIdle,
		Residency:              sessionwire.SessionResidencyCold,
		DesiredPlacement:       sessionwire.HostPlacementPooled,
	}); err != nil {
		f.tb.Fatalf("orchestrationtest: seeding bound session %q: %v", session, err)
		return ""
	}
	return session
}

// KitBinding is a valid non-zero SessionBinding for the kit's bound sessions.
//
// The mode is LEGACY and that is forced rather than chosen. sessionstore's
// public Store.PutObject pins ProtocolModeLegacy (objects.go:117-119), so a
// session bound to ProtocolModeDisposition refuses an object write with
// `catalog conflict (binding.protocol_mode)` -- measured, not read. A Host
// acquires residency in DISPOSITION mode, so the object half of a real
// disposition session is not writable through the public object API at all.
// What this binding still buys is the router's structural branch, which is on
// `Binding != zero` and not on the mode; the mode gap is booked as owed.
func KitBinding() sessionstore.SessionBinding {
	return sessionstore.SessionBinding{
		StorageBindingID: "orchestrationtest-binding",
		BindingVersion:   "1",
		RuntimeSessionID: "orchestrationtest-runtime-session",
		ProtocolMode:     sessionstore.ProtocolModeLegacy,
	}
}

// PutObject stores one immutable object body and returns its metadata.
func (f *StoreFixture) PutObject(ctx context.Context, session sessionwire.SessionID, kind sessionstore.ObjectKind, mediaType string, body []byte) sessionwire.ObjectMetadata {
	f.tb.Helper()
	sum := sha256.Sum256(body)
	metadata, err := f.Store.PutObject(ctx, sessionstore.PutObjectRequest{
		TenantID:  f.Tenant,
		SessionID: session,
		Kind:      kind,
		SizeBytes: uint64(len(body)),
		SHA256:    sum,
		MediaType: mediaType,
		Body:      bytes.NewReader(body),
	})
	if err != nil {
		f.tb.Fatalf("orchestrationtest: putting an object for %q: %v", session, err)
		return sessionwire.ObjectMetadata{}
	}
	return metadata
}

// RecordingObjectStoreResolver is the composition-site probe for the object
// resolver.
//
// It cannot be a PANIC probe. Factory's router recovers a handler panic
// (recoverPanic, internal/httpapi/routes.go) and converts it to a 500
// indistinguishable from any other internal failure, so a panic at an HTTP seam
// carries no identity. This records the binding it was handed and returns a
// reader the case can tell apart from the legacy one, so WHICH arm of the
// router's binding branch ran is proved by identity rather than by a status
// code both arms can produce.
//
// Reader nil makes it REFUSE, which is the negative arm.
type RecordingObjectStoreResolver struct {
	Reader factory.ObjectReader

	mu     sync.Mutex
	called []sessionstore.SessionBinding
}

// Called reports every binding the router asked this resolver to resolve.
func (r *RecordingObjectStoreResolver) Called() []sessionstore.SessionBinding {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sessionstore.SessionBinding(nil), r.called...)
}

// Resolve satisfies factory.ObjectStoreResolver.
func (r *RecordingObjectStoreResolver) Resolve(_ context.Context, binding sessionstore.SessionBinding) (factory.ObjectReader, error) {
	r.mu.Lock()
	r.called = append(r.called, binding)
	reader := r.Reader
	r.mu.Unlock()
	if reader == nil {
		return nil, ErrObjectNotPermitted
	}
	return reader, nil
}

// AdvanceCatalogJournal moves a session's catalog record up to a journal
// sequence, which is the precondition for opening a gate against it.
//
// OpenGate refuses a gate naming a sequence above the catalog's LastJournalSeq
// -- "the gate must name an event the journal has durably committed" -- and
// appending to the journal does NOT move the catalog: the two are separate
// durable writes and a Host performs both. A fixture that appended and then
// opened would be asserting against a state no Host ever produces.
func (f *StoreFixture) AdvanceCatalogJournal(ctx context.Context, session sessionwire.SessionID, epoch uint64, seq uint64, event sessionwire.EventID, gates []sessionwire.GateProjection) {
	f.tb.Helper()
	if _, err := f.Store.UpdateCatalogHostState(ctx, sessionstore.UpdateCatalogHostStateRequest{
		TenantID:       f.Tenant,
		SessionID:      session,
		LeaseEpoch:     epoch,
		State:          sessionwire.SessionStateIdle,
		Residency:      sessionwire.SessionResidencyResident,
		LastActiveAt:   f.Clock.Now(),
		LastJournalSeq: seq,
		LastEventID:    event,
		OpenGates:      gates,
	}); err != nil {
		f.tb.Fatalf("orchestrationtest: advancing the catalog journal for %q: %v", session, err)
	}
}
