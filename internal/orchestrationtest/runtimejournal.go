//go:build integration


package orchestrationtest

import (
	"context"
	"fmt"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// OpenRuntimeJournal opens the READ of one tenant's harness runtime journal:
// a plain SessionStore over exactly the backend the Host's harness writes
// that tenant's journal on, in harness's legacy single-tenant layout. It is
// what factory v0.9.0's consumer obligation names for a harness runtime --
// sessionstore.Open(ctx, runtimeBackend(tenant),
// sessionstore.WithLegacySingleTenant(tenant)) -- and it imports no harness
// package: harness writes its frames in SessionStore's own envelope format.
//
// One tenant per backend, which is how every world in this kit is built.
func OpenRuntimeJournal(tb TB, ctx context.Context, backend *storage.Composite, tenant sessionwire.TenantID) *sessionstore.Store {
	tb.Helper()
	store, err := sessionstore.Open(ctx, backend, sessionstore.WithLegacySingleTenant(tenant))
	if err != nil {
		tb.Fatalf("orchestrationtest: opening %q's runtime journal read: %v", tenant, err)
		return nil
	}
	tb.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.Close(closeCtx); err != nil {
			tb.Errorf("orchestrationtest: closing %q's runtime journal read: %v", tenant, err)
		}
	})
	return store
}

// ErrUnknownJournalBinding is a journal resolver's refusal of a binding this
// deployment did not compose. factory v0.9.0 requires the refusal: a resolver
// that answered a default would serve one deployment's journal under
// another's configuration.
var ErrUnknownJournalBinding = fmt.Errorf("orchestrationtest: the journal resolver does not know this binding")

// journalResolver answers journals[tenant] for exactly the one storage
// binding and version this kit's Factories write, and refuses anything else.
func journalResolver(bindingID, bindingVersion string, journals func(sessionwire.TenantID) factory.JournalReader) factory.JournalResolver {
	return func(_ context.Context, tenant sessionwire.TenantID, binding sessionstore.SessionBinding) (factory.JournalReader, error) {
		if binding.StorageBindingID != bindingID || binding.BindingVersion != bindingVersion {
			return nil, fmt.Errorf("%w: %q/%q", ErrUnknownJournalBinding, binding.StorageBindingID, binding.BindingVersion)
		}
		reader := journals(tenant)
		if reader == nil {
			return nil, fmt.Errorf("%w: no runtime journal for tenant %q", ErrUnknownJournalBinding, tenant)
		}
		return reader, nil
	}
}

// CommitRuntimeEvents appends n public events to a session's RUNTIME journal --
// the tenant's harness journal backend, under the catalog binding's
// RuntimeSessionID -- and returns the sequence of the last.
//
// It is for a case whose Host is a STAND-IN with no runtime: a real Host's
// runtime journals every record before it publishes it, and factory v0.9.0
// resets a viewer to that journal's tip, so a stand-in's case must put in the
// journal what a runtime would have written first. It writes through a real
// SessionStore JournalWriter in the layout harness writes, and it refuses a
// session that is not disposition-bound.
func (world *PooledWorld) CommitRuntimeEvents(tb TB, ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, n int) uint64 {
	tb.Helper()
	entry, err := world.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: session})
	if err != nil {
		tb.Fatalf("orchestrationtest: reading %s/%s's binding: %v", tenant, session, err)
		return 0
	}
	binding := entry.Record.Binding
	if binding.ProtocolMode != sessionstore.ProtocolModeDisposition || binding.RuntimeSessionID == "" {
		tb.Fatalf("orchestrationtest: %s/%s is not a Host-owned session: %+v", tenant, session, binding)
		return 0
	}
	journal := world.RuntimeJournals[tenant]
	if journal == nil {
		tb.Fatalf("orchestrationtest: no runtime journal for tenant %q", tenant)
		return 0
	}
	writer, err := journal.OpenJournal(ctx, sessionstore.OpenJournalRequest{TenantID: tenant, SessionID: sessionwire.SessionID(binding.RuntimeSessionID)})
	if err != nil {
		tb.Fatalf("orchestrationtest: opening %s/%s's runtime journal: %v", tenant, session, err)
		return 0
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := writer.Close(closeCtx); err != nil {
			tb.Errorf("orchestrationtest: closing %s/%s's runtime journal: %v", tenant, session, err)
		}
	}()
	var last uint64
	for i := range n {
		eventID := sessionwire.EventID(fmt.Sprintf("event-runtime-%s-%d-%d", session, time.Now().UnixNano(), i))
		seq, err := writer.Append(ctx, sessionstore.Envelope{
			Kind:    sessionstore.EnvelopeKindPublicEvent,
			EventID: eventID,
			Public:  sessionstore.BodySlot{Inline: []byte(fmt.Sprintf(`{"runtime_event":%d}`, i))},
		})
		if err != nil {
			tb.Fatalf("orchestrationtest: appending to %s/%s's runtime journal: %v", tenant, session, err)
			return 0
		}
		last = seq
	}
	return last
}

// PublicJournalSeqs reads a Host-owned session's RUNTIME journal through the
// same read Factory's resolver answers -- the tenant's runtime journal (the
// product journal in a DurableTail world), under
// the catalog binding's RuntimeSessionID -- and returns every public event's
// sequence, in order. It is the ground truth a viewer's coverage is held to.
func (world *PooledWorld) PublicJournalSeqs(tb TB, ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) []uint64 {
	tb.Helper()
	entry, err := world.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: session})
	if err != nil {
		tb.Fatalf("orchestrationtest: reading %s/%s's binding: %v", tenant, session, err)
		return nil
	}
	journal := world.RuntimeJournals[tenant]
	if world.ProductJournal != nil {
		journal = world.ProductJournal
	}
	if journal == nil {
		tb.Fatalf("orchestrationtest: no runtime journal for tenant %q", tenant)
		return nil
	}
	runtime := sessionwire.SessionID(entry.Record.Binding.RuntimeSessionID)
	var seqs []uint64
	var cursor sessionwire.Cursor
	for {
		req := sessionstore.ReadPublicJournalRequest{TenantID: tenant, SessionID: runtime, Cursor: cursor}
		page, err := journal.ReadPublicJournal(ctx, req)
		if err != nil {
			tb.Fatalf("orchestrationtest: reading %s/%s's runtime journal: %v", tenant, session, err)
			return nil
		}
		for _, e := range page.Events {
			seqs = append(seqs, e.JournalSeq)
		}
		if page.NextCursor == "" {
			return seqs
		}
		cursor = page.NextCursor
	}
}
