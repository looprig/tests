//go:build integration

package orchestrationtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// StoreFixture is the shared durable plane every service in one case runs
// against: ONE storage.Composite, and a real *sessionstore.Store opened over
// it. It is not a fake. Runbook 07 I0.1 step 3 asks for "a shared selected
// Composite" and step 3 also asks for "restart with the same durable store",
// which is why the Composite and the Store have separate lifetimes here:
// Reopen closes the Store and opens a new one over the SAME Composite, which
// is what a service restart looks like from the durable plane's point of view.
//
// The backend is memstore rather than fsstore, and that is a mechanism rather
// than a convenience. sessionstore.Open REFUSES a Blobs provider that does not
// implement storage.BlobReaderLifecycle, with a typed
// InvalidBackendError{Component: "BlobReaderLifecycle"}, before any layout or
// provider I/O -- and fsstore deliberately does not implement it. memstore
// does. Do not "fix" a rejected backend here by weakening the assertion; the
// rejection is the contract.
type StoreFixture struct {
	Backend *storage.Composite
	Store   *sessionstore.Store
	Tenant  sessionwire.TenantID
	Clock   *Clock

	tb TB
}

// TenantPrefix is the fixed, greppable prefix every kit tenant carries, so a
// leaked record is attributable to the kit rather than to a real deployment.
const TenantPrefix = "orchestrationtest-"

// NewTenantID mints a random tenant id. Runbook 07 I0.1 step 4 requires a
// random tenant prefix per test: a fixed tenant makes two cases in the same
// process share a keyspace, and the resulting cross-talk reads as flakiness.
func NewTenantID(tb TB) sessionwire.TenantID {
	tb.Helper()
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		tb.Fatalf("orchestrationtest: minting a tenant id: %v", err)
		return ""
	}
	return sessionwire.TenantID(TenantPrefix + hex.EncodeToString(raw))
}

// NewStoreFixture opens a real Store over a fresh memstore Composite and
// registers its cleanup.
func NewStoreFixture(tb TB, ctx context.Context, clock *Clock) *StoreFixture {
	tb.Helper()
	fixture := &StoreFixture{
		Backend: memstore.New(),
		Tenant:  NewTenantID(tb),
		Clock:   clock,
		tb:      tb,
	}
	fixture.open(ctx)
	tb.Cleanup(func() {
		if fixture.Store == nil {
			return
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := fixture.Store.Close(closeCtx); err != nil {
			tb.Errorf("orchestrationtest: closing the store: %v", err)
		}
	})
	return fixture
}

func (f *StoreFixture) open(ctx context.Context) {
	f.tb.Helper()
	// The shard count is stated HERE as well as on the kit's Commands and Gates
	// seams, and the two MUST agree. They are two statements of one number with
	// no composition-time check between them: factory.Commands.ControlShards()
	// is what the sweeper rotates over, sessionstore's WithControlShards is
	// where due work is filed, and a deployment whose Factory reports 4 against
	// a store configured for the default 16 sweeps shards 0-3 and NEVER REACHES
	// the other twelve. Work filed there is never reconciled and nothing
	// anywhere reports it. This kit found that by making the fake faithful; see
	// KitControlShards.
	store, err := sessionstore.Open(ctx, f.Backend,
		sessionstore.WithClock(f.Clock),
		sessionstore.WithControlShards(KitControlShards))
	if err != nil {
		f.tb.Fatalf("orchestrationtest: opening sessionstore over memstore: %v", err)
		return
	}
	f.Store = store
}

// Reopen simulates a service restart against the same durable bytes.
func (f *StoreFixture) Reopen(ctx context.Context) {
	f.tb.Helper()
	if f.Store != nil {
		if err := f.Store.Close(ctx); err != nil {
			f.tb.Fatalf("orchestrationtest: closing the store before reopen: %v", err)
			return
		}
		f.Store = nil
	}
	f.open(ctx)
}

// randomSuffix mints a short random token for a per-fixture identity.
func randomSuffix(tb TB) string {
	tb.Helper()
	raw := make([]byte, 6)
	if _, err := rand.Read(raw); err != nil {
		tb.Fatalf("orchestrationtest: minting a random suffix: %v", err)
		return ""
	}
	return hex.EncodeToString(raw)
}

// SeedSession creates one catalog entry and returns its session id.
func (f *StoreFixture) SeedSession(ctx context.Context, agent sessionwire.AgentID, compatibility string) sessionwire.SessionID {
	f.tb.Helper()
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		f.tb.Fatalf("orchestrationtest: minting a session id: %v", err)
		return ""
	}
	session := sessionwire.SessionID("session-" + hex.EncodeToString(raw))
	now := f.Clock.Now()
	if _, _, err := f.Store.CreateCatalogEntry(ctx, sessionstore.CreateCatalogEntryRequest{
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
		f.tb.Fatalf("orchestrationtest: seeding session %q: %v", session, err)
		return ""
	}
	return session
}

// AppendPublicEvent appends one public journal record and returns its sequence.
//
// body must already be transport-canonical: Core requires a public body to be
// a fixed point of json.Marshal, so "<", ">", "&", U+2028 and U+2029 must
// arrive pre-escaped. The kit does NOT canonicalize it for the caller, because
// silently repairing a body would hide exactly the defect a wire-freeze case
// is looking for.
func (f *StoreFixture) AppendPublicEvent(ctx context.Context, session sessionwire.SessionID, eventID sessionwire.EventID, body []byte) uint64 {
	f.tb.Helper()
	writer, err := f.Store.OpenJournal(ctx, sessionstore.OpenJournalRequest{TenantID: f.Tenant, SessionID: session})
	if err != nil {
		f.tb.Fatalf("orchestrationtest: opening journal for %q: %v", session, err)
		return 0
	}
	defer func() {
		if closeErr := writer.Close(ctx); closeErr != nil {
			f.tb.Errorf("orchestrationtest: closing journal writer: %v", closeErr)
		}
	}()
	seq, err := writer.Append(ctx, sessionstore.Envelope{
		Kind:    sessionstore.EnvelopeKindPublicEvent,
		EventID: eventID,
		Public:  sessionstore.BodySlot{Inline: body},
	})
	if err != nil {
		f.tb.Fatalf("orchestrationtest: appending public event %q: %v", eventID, err)
		return 0
	}
	return seq
}

// Describe is a human-readable identity for failure messages.
func (f *StoreFixture) Describe() string {
	return fmt.Sprintf("tenant=%s", f.Tenant)
}
