//go:build integration && orchestration

package orchestrationtest

import (
	"context"
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

// The PANIC SEAMS lived here and are DELETED. Two things killed them, and both
// are worth carrying forward.
//
// First, they worked. Composed as factory.Commands against a Serve-ing Factory,
// the seam fired from Server.Start's own sweep goroutine --
// admission.Reconciler.Sweep -> ListDueCommands -- which is exactly the event it
// existed to announce and is how this lane learned the command sweep was live.
//
// Second, they cannot be used again at this seam, for two independent reasons.
// The durable command plane is now DRIVEN, so a panicking Commands turns every
// composition that serves into a crash rather than a probe. And Factory's router
// RECOVERS a handler panic (recoverPanic, internal/httpapi/routes.go) and
// converts it to a 500 indistinguishable from any other internal failure, so a
// panic at an HTTP seam carries no identity even where one would not crash.
//
// What replaces them is composition-site value substitution plus
// identity-carriage assertions: two readers that can be told apart
// (RecordingObjectStoreResolver), and recorders on the composed seams that say
// what was ASKED FOR (StoreCommands.DueRequests, StoreGates.DueRequests).

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
