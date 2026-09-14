//go:build integration && orchestration

package orchestrationtest

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// ObservedReader wraps the REAL read plane and records the query work Factory
// asked the durable provider to do.
//
// Runbook 07 I1.1 case 5 says "record provider query-work counters", and a
// counter alone would not discharge it. What makes a page BOUNDED is not how
// many times the store was called but what each call ASKED FOR, so this records
// the ReadPublicJournalRequest values themselves -- Tail, Limit, ScanLimit,
// FromSeq and whether a cursor was presented. A case can then assert the shape
// of the request rather than a count that a wrong request would also produce.
//
// It wraps rather than replaces, for FaultyReader's reason: every answer here is
// the store's own, byte for byte, so a case reading through this observer is
// reading the real durable plane.
type ObservedReader struct {
	Inner SessionReaderSeam

	mu       sync.Mutex
	counts   map[string]int
	journals []sessionstore.ReadPublicJournalRequest
	lists    []sessionstore.ListSessionsRequest
	gates    []sessionstore.ReadGatesRequest
}

// NewObservedReader wraps inner.
func NewObservedReader(inner SessionReaderSeam) *ObservedReader {
	return &ObservedReader{Inner: inner, counts: make(map[string]int)}
}

func (o *ObservedReader) count(operation string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.counts[operation]++
}

// Count reports how many times the named read-plane operation was invoked.
func (o *ObservedReader) Count(operation string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.counts[operation]
}

// Total reports every read-plane call this observer has seen.
func (o *ObservedReader) Total() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	total := 0
	for _, n := range o.counts {
		total += n
	}
	return total
}

// JournalRequests reports every journal read, in order.
func (o *ObservedReader) JournalRequests() []sessionstore.ReadPublicJournalRequest {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]sessionstore.ReadPublicJournalRequest(nil), o.journals...)
}

// ListRequests reports every tenant-list read, in order.
func (o *ObservedReader) ListRequests() []sessionstore.ListSessionsRequest {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]sessionstore.ListSessionsRequest(nil), o.lists...)
}

// GateRequests reports every gate read, in order.
func (o *ObservedReader) GateRequests() []sessionstore.ReadGatesRequest {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]sessionstore.ReadGatesRequest(nil), o.gates...)
}

// Reset drops every recorded call, so one case can measure two phases apart.
func (o *ObservedReader) Reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.counts = make(map[string]int)
	o.journals = nil
	o.lists = nil
	o.gates = nil
}

// ListSessions satisfies factory.SessionReader.
func (o *ObservedReader) ListSessions(ctx context.Context, req sessionstore.ListSessionsRequest) (sessionstore.SessionPage, error) {
	o.count("ListSessions")
	o.mu.Lock()
	o.lists = append(o.lists, req)
	o.mu.Unlock()
	return o.Inner.ListSessions(ctx, req)
}

// GetCatalogEntry satisfies factory.SessionReader.
func (o *ObservedReader) GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	o.count("GetCatalogEntry")
	return o.Inner.GetCatalogEntry(ctx, req)
}

// ReadPublicJournal satisfies factory.SessionReader.
func (o *ObservedReader) ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	o.count("ReadPublicJournal")
	o.mu.Lock()
	o.journals = append(o.journals, req)
	o.mu.Unlock()
	return o.Inner.ReadPublicJournal(ctx, req)
}

// ReadGates satisfies factory.SessionReader.
func (o *ObservedReader) ReadGates(ctx context.Context, req sessionstore.ReadGatesRequest) (sessionwire.GatePage, error) {
	o.count("ReadGates")
	o.mu.Lock()
	o.gates = append(o.gates, req)
	o.mu.Unlock()
	return o.Inner.ReadGates(ctx, req)
}

// GetObject satisfies factory.SessionReader.
func (o *ObservedReader) GetObject(ctx context.Context, req sessionstore.GetObjectRequest) (io.ReadCloser, error) {
	o.count("GetObject")
	return o.Inner.GetObject(ctx, req)
}

// GetObjectMetadata satisfies factory.SessionReader.
func (o *ObservedReader) GetObjectMetadata(ctx context.Context, req sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error) {
	o.count("GetObjectMetadata")
	return o.Inner.GetObjectMetadata(ctx, req)
}

// ---------------------------------------------------------------------------
// Panic seams: the probe that tells "composed" from "driven".
// ---------------------------------------------------------------------------

// ErrSeamDriven is never returned. It exists so the panic seams below have one
// greppable identity in a panic message.
var ErrSeamDriven = fmt.Errorf("orchestrationtest: a seam this build believes is never driven was driven")

// PanicCommands satisfies factory.Commands and panics on every method.
//
// It is the falsifiable half of an ABSENCE claim. factory.New ACCEPTS
// WithCommands, validates it as non-nil, stores it -- and composeRouter never
// passes it to the router, so no request in this build can reach the durable
// command plane. That claim is worth exactly as much as the probe behind it: a
// recording fake would record nothing and a reader could not tell "never
// called" from "the case forgot to look". A panic cannot be missed, and it
// fires on the DAY the composition changes rather than on a mutation of the
// test.
//
// A panic is not an assertion kill and must never be counted as one in a
// mutation table. Here the panic is the SUBJECT of the case, not its verdict:
// the case asserts that a full traversal of every route completes WITHOUT one.
// It RECORDS before it panics, and both halves matter. A panic raised inside an
// http.Handler is recovered by net/http, so a handler that reached this seam
// would close the connection rather than fail the process -- loud, but not
// attributable. The record survives that, so the case's verdict is an ASSERTION
// on Driven() rather than the absence of a crash. A panic raised anywhere else
// -- a background sweep in Serve, say -- still takes the process down, which is
// the right answer for a plane nobody expected to be running.
type PanicCommands struct {
	mu     sync.Mutex
	driven []string
}

// Driven reports every method that was reached, in order.
func (p *PanicCommands) Driven() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.driven...)
}

func (p *PanicCommands) panicked(method string) {
	p.mu.Lock()
	p.driven = append(p.driven, method)
	p.mu.Unlock()
	panic(fmt.Sprintf("%v: factory.Commands.%s", ErrSeamDriven, method))
}

// AdmitCommand satisfies factory.Commands.
func (p *PanicCommands) AdmitCommand(context.Context, sessionstore.AdmitCommandRequest) (sessionstore.InboxEntry, bool, error) {
	p.panicked("AdmitCommand")
	return sessionstore.InboxEntry{}, false, nil
}

// GetCommand satisfies factory.Commands.
func (p *PanicCommands) GetCommand(context.Context, sessionstore.GetCommandRequest) (sessionstore.InboxEntry, error) {
	p.panicked("GetCommand")
	return sessionstore.InboxEntry{}, nil
}

// RejectCommand satisfies factory.Commands.
func (p *PanicCommands) RejectCommand(context.Context, sessionstore.RejectCommandRequest) (sessionstore.InboxEntry, error) {
	p.panicked("RejectCommand")
	return sessionstore.InboxEntry{}, nil
}

// ListDueCommands satisfies factory.Commands.
func (p *PanicCommands) ListDueCommands(context.Context, sessionstore.ListDueCommandsRequest) (sessionstore.DueCommandPage, error) {
	p.panicked("ListDueCommands")
	return sessionstore.DueCommandPage{}, nil
}

// AcquireReconciliationClaim satisfies factory.Commands.
func (p *PanicCommands) AcquireReconciliationClaim(context.Context, sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	p.panicked("AcquireReconciliationClaim")
	return sessionstore.ReconciliationClaimEntry{}, nil
}

// ReleaseReconciliationClaim satisfies factory.Commands.
func (p *PanicCommands) ReleaseReconciliationClaim(context.Context, sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	p.panicked("ReleaseReconciliationClaim")
	return sessionstore.ReconciliationClaimEntry{}, nil
}

// PanicPlacement satisfies factory.PlacementController and panics on every
// method. See PanicCommands for why a panic rather than a recorder.
type PanicPlacement struct {
	mu     sync.Mutex
	driven []string
}

// Driven reports every method that was reached, in order.
func (p *PanicPlacement) Driven() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.driven...)
}

func (p *PanicPlacement) panicked(method string) {
	p.mu.Lock()
	p.driven = append(p.driven, method)
	p.mu.Unlock()
	panic(fmt.Sprintf("%v: factory.PlacementController.%s", ErrSeamDriven, method))
}

// EnsurePlacement satisfies factory.PlacementController.
func (p *PanicPlacement) EnsurePlacement(context.Context, sessionstore.DesiredWorkload) error {
	p.panicked("EnsurePlacement")
	return nil
}

// ReleasePlacement satisfies factory.PlacementController.
func (p *PanicPlacement) ReleasePlacement(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	p.panicked("ReleasePlacement")
	return nil
}

// ---------------------------------------------------------------------------
// Durable residency helpers: what "every Host is stopped" means on the plane.
// ---------------------------------------------------------------------------

// HostRegistrationTTL is the heartbeat promise the kit's registrations make.
const HostRegistrationTTL = 30 * time.Second

// RegisterHost publishes a live route for one session at one lease epoch, which
// is what a resident Host's heartbeat writes.
func (f *StoreFixture) RegisterHost(ctx context.Context, session sessionwire.SessionID, hostID sessionwire.HostID, endpoint sessionwire.InternalEndpoint, agent sessionwire.AgentID, compatibility string, epoch uint64) {
	f.tb.Helper()
	now := f.Clock.Now()
	if _, err := f.Store.PutHostRegistration(ctx, sessionstore.PutHostRegistrationRequest{
		TenantID:   f.Tenant,
		SessionID:  session,
		LeaseEpoch: epoch,
		ObservedAt: now,
		ExpiresAt:  now.Add(HostRegistrationTTL),
		Route: sessionstore.HostRoute{
			HostID:                 hostID,
			HostGeneration:         1,
			AgentID:                agent,
			RuntimeCompatibilityID: compatibility,
			Placement:              sessionwire.HostPlacementPooled,
			InternalEndpoint:       endpoint,
			Residency:              sessionwire.SessionResidencyResident,
			Accepting:              true,
		},
	}); err != nil {
		f.tb.Fatalf("orchestrationtest: registering host %q for %q: %v", hostID, session, err)
	}
}

// ReleaseHost writes the released tombstone a graceful Host shutdown leaves.
//
// It is the durable meaning of "stop every Host" in runbook 07 I1.1 case 1, and
// it is deliberately NOT a delete: the epoch fence survives, which is what makes
// a later cold read distinguishable from a session that never ran.
func (f *StoreFixture) ReleaseHost(ctx context.Context, session sessionwire.SessionID, epoch uint64) {
	f.tb.Helper()
	if _, err := f.Store.ClearHostRegistration(ctx, sessionstore.ClearHostRegistrationRequest{
		TenantID:   f.Tenant,
		SessionID:  session,
		LeaseEpoch: epoch,
	}); err != nil {
		f.tb.Fatalf("orchestrationtest: releasing the host registration for %q: %v", session, err)
	}
}

// HostRouteFor reports the currently routable Host for one session, and whether
// there is one. It reads the REAL registry through the kit's real Directory
// adapter, so a case asserting "no Host owns this session" is asserting what
// Factory's own Directory seam would answer.
func (f *StoreFixture) HostRouteFor(ctx context.Context, directory *StoreDirectory, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool) {
	f.tb.Helper()
	observation, found, err := directory.Owner(ctx, f.Tenant, session)
	if err != nil {
		f.tb.Fatalf("orchestrationtest: resolving the owner of %q: %v", session, err)
		return sessionwire.HostLinkRegistryObservation{}, false
	}
	return observation, found
}

// TargetTTL is the heartbeat promise the kit's advertisements make.
const TargetTTL = 30 * time.Second

// PublishTarget advertises one Host's capacity for one launch target.
func (f *StoreFixture) PublishTarget(ctx context.Context, key sessionstore.HostTargetKey, hostID sessionwire.HostID, endpoint sessionwire.InternalEndpoint, capacity uint64) {
	f.tb.Helper()
	now := f.Clock.Now()
	if _, err := f.Store.PublishHostTarget(ctx, sessionstore.PublishHostTargetRequest{
		Key:            key,
		HostID:         hostID,
		HostGeneration: 1,
		ObservedAt:     now,
		Advertisement: sessionstore.HostAdvertisement{
			InternalEndpoint:  endpoint,
			IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
			Accepting:         true,
			AvailableCapacity: capacity,
			ExpiresAt:         now.Add(TargetTTL),
		},
	}); err != nil {
		f.tb.Fatalf("orchestrationtest: publishing target capacity for %q: %v", hostID, err)
	}
}

// DrainTarget withdraws one Host's advertisement for one launch target.
func (f *StoreFixture) DrainTarget(ctx context.Context, key sessionstore.HostTargetKey, hostID sessionwire.HostID) {
	f.tb.Helper()
	if _, err := f.Store.DrainHostTarget(ctx, sessionstore.DrainHostTargetRequest{
		Key:            key,
		HostID:         hostID,
		HostGeneration: 1,
	}); err != nil {
		f.tb.Fatalf("orchestrationtest: draining target capacity for %q: %v", hostID, err)
	}
}
