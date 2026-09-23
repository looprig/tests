//go:build integration

// This file is the cross-module integration lane for
// github.com/looprig/sessionstore over CONCRETE storage providers. SessionStore
// itself may depend on nothing but Core and Storage — a test in that module
// parses every production import and fails on anything else — so the module can
// never name memstore, natsstore or fsstore. Choosing a provider is a
// composition root's job, and this repository is the composition root that does
// it for verification purposes.
//
// Everything asserted here is EXTERNALLY OBSERVABLE: the exported SessionStore
// API, the typed errors it documents, and the provider calls a wrapped
// storage.Composite records. Nothing here imports another module's internal
// test kit, and nothing reaches into sessionstore's unexported state; a case
// that cannot be expressed against the published surface does not belong in
// this repository.
//
// Every neutral case runs against BOTH Storage's memstore and the released
// natsstore, because a provider swap is exactly the change these contracts are
// meant to survive.

package tests

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/fsstore"
	"github.com/looprig/natsstore"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// sessionStoreCaseTimeout bounds every context this file creates. No case waits
// on wall-clock progress: expiry is driven by writing instants relative to a
// fixed base, never by sleeping.
const sessionStoreCaseTimeout = 60 * time.Second

// sessionStoreProvider is one concrete Storage backend under test, together with
// the cleanup that releases whatever it owns.
type sessionStoreProvider struct {
	name string
	// open returns a fresh, EMPTY backend. A layout case depends on the backend
	// being unmarked, so this may never hand back a reused one.
	open func(t *testing.T, ctx context.Context) *storage.Composite
}

// sessionStoreRequiredProviders names the backends the matrix must contain.
// Eleven of the thirteen cases in this file are provider cases, so an empty or
// shrunken matrix would not fail them — it would make them PASS with zero
// subtests. The matrix is the single place that failure mode can be refused,
// and refusing it is the same non-vacuity floor sessionstore's own
// production-import test applies when it parses zero files. That precedent
// floors the requirement as well as the input, so emptying this list is itself
// a failure: see the zero floor in assertSessionStoreProviderMatrix.
var sessionStoreRequiredProviders = []string{"memstore", "natsstore"}

// sessionStoreProviders is the provider matrix every neutral case runs over.
//
// natsstore is opened through its public embedded mode on a temp dir this test
// owns: an in-process JetStream engine with no TCP listener, no home or XDG
// lookup, and no external server. memstore is Storage's in-process oracle.
//
// It takes the case's *testing.T so the floor below is enforced at every call
// site: there is no separate guard test that could be -run-filtered away or
// deleted while eleven vacuous passes survived. The floor is not proof against
// someone who sets out to remove it — it is a normal function call in a test
// file — but it is returned through rather than called for effect, so dropping
// the line does not compile, and any edit that disables it disables it for all
// eleven cases at once and visibly in review.
func sessionStoreProviders(t *testing.T) []sessionStoreProvider {
	t.Helper()
	providers := []sessionStoreProvider{
		{
			name: "memstore",
			open: func(t *testing.T, _ context.Context) *storage.Composite {
				t.Helper()
				return memstore.New()
			},
		},
		{
			name: "natsstore",
			open: func(t *testing.T, ctx context.Context) *storage.Composite {
				t.Helper()
				store, err := natsstore.Open(ctx, natsstore.Options{EmbeddedDir: t.TempDir()})
				if err != nil {
					t.Fatalf("natsstore.Open: %v", err)
				}
				t.Cleanup(func() {
					closeCtx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
					defer cancel()
					if err := store.Close(closeCtx); err != nil {
						t.Errorf("natsstore Close: %v", err)
					}
				})
				return store.Backend()
			},
		},
	}
	// The cloud lane (runbook 07 P3.1) adds the released pgstore+s3store
	// composite when it is compiled in and enabled; see
	// cloud_sessionstore_integration_test.go.
	providers = append(providers, cloudSessionStoreProviders(t)...)
	return assertSessionStoreProviderMatrix(t, providers)
}

// assertSessionStoreProviderMatrix refuses a matrix that has lost a backend and
// returns the matrix it accepted, so a caller cannot drop the check and still
// compile. The floor is by NAME and not by length: a length of two would accept
// memstore listed twice, it would not say which backend went missing, and it
// would have to be edited — for no reason — the day a third released backend is
// added. Every required name must appear exactly once; additional backends are
// fine.
func assertSessionStoreProviderMatrix(t *testing.T, providers []sessionStoreProvider) []sessionStoreProvider {
	t.Helper()
	if len(sessionStoreRequiredProviders) == 0 {
		t.Fatal(
			"sessionStoreRequiredProviders is empty, so this floor requires nothing and would accept any matrix, including an empty one. Emptying it makes every provider case in this file pass with zero subtests; name each backend the matrix must contain.",
		)
	}
	counts := make(map[string]int, len(providers))
	for _, provider := range providers {
		counts[provider.name]++
	}
	for _, name := range append(append([]string(nil), sessionStoreRequiredProviders...), cloudRequiredSessionStoreProviders()...) {
		if counts[name] == 1 {
			continue
		}
		// The tail must stay true in all three shapes this catches: a missing
		// backend, an empty matrix, and a duplicate — where subtests DO run,
		// just not the ones the case claims.
		defect := fmt.Sprintf("no subtest would run against %q", name)
		if counts[name] > 1 {
			defect = fmt.Sprintf("the extra %q subtests displace a required backend and cover nothing the first one does not", name)
		}
		t.Fatalf(
			"provider matrix contains %d %q backends, want exactly one; matrix is %v. Every provider case in this file runs over this matrix, so %s, and those cases would pass without the provider coverage they claim.",
			counts[name], name, sessionStoreProviderNames(providers), defect,
		)
	}
	return providers
}

// sessionStoreProviderNames renders a matrix for a failure message.
func sessionStoreProviderNames(providers []sessionStoreProvider) []string {
	names := make([]string, 0, len(providers))
	for _, provider := range providers {
		names = append(names, provider.name)
	}
	return names
}

// randomSessionStoreTenant returns a fresh tenant namespace. Every case gets its
// own so that two cases sharing one provider can never observe each other's
// records, and so a repeated run never reuses a namespace.
func randomSessionStoreTenant(t *testing.T) sessionwire.TenantID {
	t.Helper()
	return sessionwire.TenantID("tenant-" + randomSessionStoreToken(t))
}

func randomSessionStoreToken(t *testing.T) string {
	t.Helper()
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("read random token: %v", err)
	}
	return hex.EncodeToString(raw[:])
}

// openSessionStore opens a Store over backend and closes it when the test ends.
func openSessionStore(t *testing.T, ctx context.Context, backend *storage.Composite, opts ...sessionstore.Option) *sessionstore.Store {
	t.Helper()
	store, err := sessionstore.Open(ctx, backend, opts...)
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
		defer cancel()
		if err := store.Close(closeCtx); err != nil {
			t.Errorf("sessionstore Close: %v", err)
		}
	})
	return store
}

// createSessionStoreSession creates one catalog entry and returns nothing but
// the fact that it exists: a case that needs the record reads it back through
// the API it is testing.
func createSessionStoreSession(t *testing.T, ctx context.Context, store *sessionstore.Store, tenant sessionwire.TenantID, session sessionwire.SessionID, at time.Time) sessionstore.CatalogEntry {
	t.Helper()
	entry, created, err := store.CreateCatalogEntry(ctx, sessionstore.CreateCatalogEntryRequest{
		TenantID:               tenant,
		SessionID:              session,
		AgentID:                "agent-a",
		RuntimeCompatibilityID: "runtime-v1",
		CreatedAt:              at,
		LastActiveAt:           at,
		State:                  sessionwire.SessionStateIdle,
		Residency:              sessionwire.SessionResidencyCold,
		DesiredPlacement:       sessionwire.HostPlacementPooled,
		IdempotencyKey:         "create-" + string(session),
	})
	if err != nil {
		t.Fatalf("CreateCatalogEntry(%s): %v", session, err)
	}
	if !created {
		t.Fatalf("CreateCatalogEntry(%s) created = false, want a fresh session", session)
	}
	return entry
}

// TestSessionStoreOpeningFenceIsProviderNeutral holds the journal ownership
// contract: OpenJournal commits an opening fence stamped with the grant's epoch
// at the tip it read, a second grant is refused while the first is live, and a
// later grant fences strictly above its predecessor. The public projection
// withholds both fences while still covering them.
func TestSessionStoreOpeningFenceIsProviderNeutral(t *testing.T) {
	t.Parallel()
	for _, provider := range sessionStoreProviders(t) {
		t.Run(provider.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
			defer cancel()

			store := openSessionStore(t, ctx, provider.open(t, ctx))
			tenant := randomSessionStoreTenant(t)
			session := sessionwire.SessionID("session-" + randomSessionStoreToken(t))
			base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
			createSessionStoreSession(t, ctx, store, tenant, session, base)

			first, err := store.OpenJournal(ctx, sessionstore.OpenJournalRequest{TenantID: tenant, SessionID: session})
			if err != nil {
				t.Fatalf("OpenJournal: %v", err)
			}
			firstFenceSeq := first.Sequence()
			if firstFenceSeq == 0 {
				t.Fatal("first grant Sequence = 0, want the sequence of its own opening fence")
			}

			// A second grant may not exist while the first is live, and the
			// refusal is typed: a caller distinguishes "someone else owns this
			// journal" from every other failure.
			_, err = store.OpenJournal(ctx, sessionstore.OpenJournalRequest{TenantID: tenant, SessionID: session})
			var held *sessionstore.JournalError
			if !errors.As(err, &held) || held.Code != sessionstore.JournalErrorLeaseHeld {
				t.Fatalf("second OpenJournal error = %v, want *JournalError code %q", err, sessionstore.JournalErrorLeaseHeld)
			}

			publicSeq, err := first.Append(ctx, sessionstore.Envelope{
				Kind:    sessionstore.EnvelopeKindPublicEvent,
				EventID: "event-1",
				Public:  sessionstore.BodySlot{Inline: []byte(`{"step":1}`)},
			})
			if err != nil {
				t.Fatalf("Append public event: %v", err)
			}
			if publicSeq <= firstFenceSeq {
				t.Fatalf("public append seq = %d, want above the opening fence at %d", publicSeq, firstFenceSeq)
			}
			if err := first.Close(ctx); err != nil {
				t.Fatalf("close first grant: %v", err)
			}

			second, err := store.OpenJournal(ctx, sessionstore.OpenJournalRequest{TenantID: tenant, SessionID: session})
			if err != nil {
				t.Fatalf("reopen journal: %v", err)
			}
			defer func() {
				if err := second.Close(ctx); err != nil {
					t.Errorf("close second grant: %v", err)
				}
			}()
			if second.Epoch() <= first.Epoch() {
				t.Fatalf("second grant epoch = %d, want strictly above the first grant's %d", second.Epoch(), first.Epoch())
			}
			if second.Sequence() <= publicSeq {
				t.Fatalf("second grant fence seq = %d, want above the last committed record %d", second.Sequence(), publicSeq)
			}

			// The fences are real records in the raw stream, stamped with the
			// epoch of the grant that wrote them.
			runtime, err := store.ReadRuntimeJournal(ctx, sessionstore.ReadRuntimeJournalRequest{TenantID: tenant, SessionID: session, Limit: 10})
			if err != nil {
				t.Fatalf("ReadRuntimeJournal: %v", err)
			}
			fences := map[uint64]uint64{}
			for _, record := range runtime.Records {
				if record.Envelope.Kind == sessionstore.EnvelopeKindOpeningFence {
					fences[record.Seq] = record.Envelope.LeaseEpoch
				}
			}
			if got, want := fences[firstFenceSeq], first.Epoch(); len(fences) != 2 || got != want {
				t.Fatalf("opening fences = %v, want two fences with the first at seq %d epoch %d", fences, firstFenceSeq, first.Epoch())
			}
			if got, want := fences[second.Sequence()], second.Epoch(); got != want {
				t.Fatalf("second fence epoch at seq %d = %d, want %d", second.Sequence(), got, want)
			}

			// The public projection publishes the event and withholds both
			// fences, while covering every sequence they occupy.
			page, err := store.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{TenantID: tenant, SessionID: session, Limit: 10})
			if err != nil {
				t.Fatalf("ReadPublicJournal: %v", err)
			}
			if len(page.Events) != 1 || page.Events[0].EventID != "event-1" {
				t.Fatalf("public events = %+v, want only the one public event", page.Events)
			}
			if page.CoveredThrough < second.Sequence() {
				t.Fatalf("CoveredThrough = %d, want coverage through the latest fence %d", page.CoveredThrough, second.Sequence())
			}
		})
	}
}

// appendSessionStoreRecord appends one record and fails the test on error.
func appendSessionStoreRecord(t *testing.T, ctx context.Context, writer *sessionstore.JournalWriter, env sessionstore.Envelope) uint64 {
	t.Helper()
	seq, err := writer.Append(ctx, env)
	if err != nil {
		t.Fatalf("Append(%v): %v", env.Kind, err)
	}
	return seq
}

// TestSessionStoreMixedJournalPagingIsProviderNeutral walks a journal whose
// records alternate between published events and withheld runtime control
// records. The page contract under test is the one a client depends on: only
// public bodies are published, a walk covers exactly the tip the first page
// captured, coverage advances over withheld records, and a record budget bounds
// the work one page does without losing a record.
func TestSessionStoreMixedJournalPagingIsProviderNeutral(t *testing.T) {
	t.Parallel()
	for _, provider := range sessionStoreProviders(t) {
		t.Run(provider.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
			defer cancel()

			store := openSessionStore(t, ctx, provider.open(t, ctx))
			tenant := randomSessionStoreTenant(t)
			session := sessionwire.SessionID("session-" + randomSessionStoreToken(t))
			base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
			createSessionStoreSession(t, ctx, store, tenant, session, base)

			writer, err := store.OpenJournal(ctx, sessionstore.OpenJournalRequest{TenantID: tenant, SessionID: session})
			if err != nil {
				t.Fatalf("OpenJournal: %v", err)
			}
			defer func() {
				if err := writer.Close(ctx); err != nil {
					t.Errorf("close journal: %v", err)
				}
			}()

			const publicEvents = 6
			wantEvents := make([]sessionwire.EventID, 0, publicEvents)
			var tip uint64
			for i := range publicEvents {
				eventID := sessionwire.EventID("event-" + randomSessionStoreToken(t))
				wantEvents = append(wantEvents, eventID)
				appendSessionStoreRecord(t, ctx, writer, sessionstore.Envelope{
					Kind:    sessionstore.EnvelopeKindPublicEvent,
					EventID: eventID,
					Public:  sessionstore.BodySlot{Inline: []byte(`{"step":` + strconv.Itoa(i) + `}`)},
					Runtime: sessionstore.BodySlot{Inline: []byte("private-" + strconv.Itoa(i))},
				})
				// One withheld runtime control record between every pair of
				// public events, so a public page can never be the whole
				// stream and coverage has to advance over records it does not
				// publish.
				tip = appendSessionStoreRecord(t, ctx, writer, sessionstore.Envelope{
					Kind:     sessionstore.EnvelopeKindRuntimeControl,
					RecordID: "control-" + strconv.Itoa(i),
					Runtime:  sessionstore.BodySlot{Inline: []byte("control-" + strconv.Itoa(i))},
				})
			}

			// A bounded walk publishes every public event exactly once, in
			// sequence order, and nothing else.
			var (
				got         []sessionwire.EventID
				cursor      sessionwire.Cursor
				capturedTip uint64
				covered     uint64
				pages       int
			)
			for {
				pages++
				if pages > publicEvents*4 {
					t.Fatalf("public walk did not terminate after %d pages", pages)
				}
				page, err := store.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{
					TenantID:  tenant,
					SessionID: session,
					Cursor:    cursor,
					Limit:     2,
				})
				if err != nil {
					t.Fatalf("ReadPublicJournal page %d: %v", pages, err)
				}
				if capturedTip == 0 {
					capturedTip = page.CapturedTip
				}
				if page.CapturedTip != capturedTip {
					t.Fatalf("page %d CapturedTip = %d, want the tip the first page pinned (%d)", pages, page.CapturedTip, capturedTip)
				}
				if page.CoveredThrough < covered {
					t.Fatalf("page %d CoveredThrough = %d, want no less than the previous %d", pages, page.CoveredThrough, covered)
				}
				covered = page.CoveredThrough
				if len(page.Events) > 2 {
					t.Fatalf("page %d returned %d events, want no more than the limit of 2", pages, len(page.Events))
				}
				for _, event := range page.Events {
					got = append(got, event.EventID)
					if string(event.Body) == "" {
						t.Fatalf("event %s has an empty public body", event.EventID)
					}
					if strings.Contains(string(event.Body), "private-") {
						t.Fatalf("event %s published its private runtime body: %s", event.EventID, event.Body)
					}
				}
				cursor = page.NextCursor
				if cursor == "" {
					break
				}
			}
			if capturedTip != tip {
				t.Fatalf("CapturedTip = %d, want the committed tip %d", capturedTip, tip)
			}
			if covered != tip {
				t.Fatalf("CoveredThrough = %d, want coverage through the captured tip %d", covered, tip)
			}
			if len(got) != len(wantEvents) {
				t.Fatalf("published %d events, want %d: %v", len(got), len(wantEvents), got)
			}
			for i, want := range wantEvents {
				if got[i] != want {
					t.Fatalf("event %d = %s, want %s (full walk %v)", i, got[i], want, got)
				}
			}
			if pages < 2 {
				t.Fatalf("walk finished in %d page(s), want a paged walk under a limit of 2", pages)
			}

			// A record budget bounds the records a page examines, INCLUDING the
			// withheld ones. With a budget of one, a page that lands on a
			// control record publishes nothing and still advances coverage.
			scan, err := store.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{
				TenantID:  tenant,
				SessionID: session,
				ScanLimit: 1,
				Limit:     10,
			})
			if err != nil {
				t.Fatalf("ReadPublicJournal with ScanLimit: %v", err)
			}
			if len(scan.Events) > 1 {
				t.Fatalf("ScanLimit=1 published %d events, want at most one examined record's worth", len(scan.Events))
			}
			if scan.CoveredThrough == 0 || scan.CoveredThrough >= tip {
				t.Fatalf("ScanLimit=1 CoveredThrough = %d, want a bounded advance below the tip %d", scan.CoveredThrough, tip)
			}
			if scan.NextCursor == "" {
				t.Fatal("ScanLimit=1 issued no continuation cursor, want the walk to be resumable")
			}

			// A tail selects the last window of the same captured tip.
			tail, err := store.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{
				TenantID:  tenant,
				SessionID: session,
				Tail:      true,
				Limit:     2,
			})
			if err != nil {
				t.Fatalf("ReadPublicJournal tail: %v", err)
			}
			if tail.CapturedTip != tip {
				t.Fatalf("tail CapturedTip = %d, want the committed tip %d", tail.CapturedTip, tip)
			}
			if len(tail.Events) == 0 {
				t.Fatal("tail published no events, want the final window of the journal")
			}
			last := tail.Events[len(tail.Events)-1].EventID
			if last != wantEvents[len(wantEvents)-1] {
				t.Fatalf("tail last event = %s, want the newest event %s", last, wantEvents[len(wantEvents)-1])
			}

			// A runtime page is the privileged counterpart: it returns the
			// withheld records the public projection refused to publish.
			runtime, err := store.ReadRuntimeJournal(ctx, sessionstore.ReadRuntimeJournalRequest{TenantID: tenant, SessionID: session, Limit: 100})
			if err != nil {
				t.Fatalf("ReadRuntimeJournal: %v", err)
			}
			var controls int
			for _, record := range runtime.Records {
				if record.Envelope.Kind == sessionstore.EnvelopeKindRuntimeControl {
					controls++
				}
			}
			if controls != publicEvents {
				t.Fatalf("runtime page carried %d control records, want %d", controls, publicEvents)
			}
		})
	}
}

// TestSessionStoreCatalogRankMoveIsProviderNeutral drives the documented
// difference between a strong record read and a weak ranked page: a session
// whose recency moves across a frozen cursor position between two pages leaves
// the walk, while a read by name still returns it at its new recency. A caller
// that must see every session once therefore reconciles by identity.
func TestSessionStoreCatalogRankMoveIsProviderNeutral(t *testing.T) {
	t.Parallel()
	for _, provider := range sessionStoreProviders(t) {
		t.Run(provider.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
			defer cancel()

			store := openSessionStore(t, ctx, provider.open(t, ctx))
			tenant := randomSessionStoreTenant(t)
			base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

			// Three sessions, ranked most-recent-first: newest, middle, oldest.
			newest := sessionwire.SessionID("session-newest-" + randomSessionStoreToken(t))
			middle := sessionwire.SessionID("session-middle-" + randomSessionStoreToken(t))
			oldest := sessionwire.SessionID("session-oldest-" + randomSessionStoreToken(t))
			createSessionStoreSession(t, ctx, store, tenant, oldest, base.Add(1*time.Minute))
			createSessionStoreSession(t, ctx, store, tenant, middle, base.Add(2*time.Minute))
			createSessionStoreSession(t, ctx, store, tenant, newest, base.Add(3*time.Minute))

			first, err := store.ListSessions(ctx, sessionstore.ListSessionsRequest{TenantID: tenant, Limit: 1})
			if err != nil {
				t.Fatalf("ListSessions page 1: %v", err)
			}
			if len(first.Sessions) != 1 || first.Sessions[0].SessionID != newest {
				t.Fatalf("page 1 = %+v, want only the most recent session %s", first.Sessions, newest)
			}
			if first.UnreadableSkipped != 0 {
				t.Fatalf("page 1 UnreadableSkipped = %d, want 0", first.UnreadableSkipped)
			}
			if first.NextCursor == "" {
				t.Fatal("page 1 issued no cursor, want a continuation over the remaining sessions")
			}

			// Move the middle session's recency ABOVE the position page 1
			// froze. Keyset pagination resumes from that frozen tuple, so the
			// row has moved to an already-passed position.
			moved := readSessionStoreCatalogEntry(t, ctx, store, tenant, middle)
			if _, err := store.UpdateCatalogHostState(ctx, sessionstore.UpdateCatalogHostStateRequest{
				TenantID:     tenant,
				SessionID:    middle,
				LeaseEpoch:   1,
				State:        sessionwire.SessionStateIdle,
				Residency:    sessionwire.SessionResidencyCold,
				LastActiveAt: base.Add(10 * time.Minute),
			}); err != nil {
				t.Fatalf("UpdateCatalogHostState(%s): %v", middle, err)
			}
			if moved.Record.LastActiveAt.Equal(base.Add(10 * time.Minute)) {
				t.Fatal("fixture error: the moved session already had its new recency before the update")
			}

			second, err := store.ListSessions(ctx, sessionstore.ListSessionsRequest{TenantID: tenant, Cursor: first.NextCursor, Limit: 10})
			if err != nil {
				t.Fatalf("ListSessions page 2: %v", err)
			}
			walked := []sessionwire.SessionID{first.Sessions[0].SessionID}
			for _, summary := range second.Sessions {
				walked = append(walked, summary.SessionID)
			}
			if len(walked) != 2 || walked[1] != oldest {
				t.Fatalf("walk = %v, want the moved session %s to have left the walk, leaving %v", walked, middle, []sessionwire.SessionID{newest, oldest})
			}

			// The record itself is strong: a read by name reports the move the
			// page lost.
			entry, err := store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: middle})
			if err != nil {
				t.Fatalf("GetCatalogEntry(%s): %v", middle, err)
			}
			if !entry.Record.LastActiveAt.Equal(base.Add(10 * time.Minute)) {
				t.Fatalf("moved session LastActiveAt = %s, want %s", entry.Record.LastActiveAt, base.Add(10*time.Minute))
			}

			// A restarted walk sees all three, now in the new order.
			restart, err := store.ListSessions(ctx, sessionstore.ListSessionsRequest{TenantID: tenant, Limit: 10})
			if err != nil {
				t.Fatalf("ListSessions restart: %v", err)
			}
			got := make([]sessionwire.SessionID, 0, len(restart.Sessions))
			for _, summary := range restart.Sessions {
				got = append(got, summary.SessionID)
			}
			want := []sessionwire.SessionID{middle, newest, oldest}
			if len(got) != len(want) {
				t.Fatalf("restarted walk = %v, want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("restarted walk = %v, want %v", got, want)
				}
			}
		})
	}
}

// readSessionStoreCatalogEntry reads one catalog record by name.
func readSessionStoreCatalogEntry(t *testing.T, ctx context.Context, store *sessionstore.Store, tenant sessionwire.TenantID, session sessionwire.SessionID) sessionstore.CatalogEntry {
	t.Helper()
	entry, err := store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: session})
	if err != nil {
		t.Fatalf("GetCatalogEntry(%s): %v", session, err)
	}
	return entry
}

// manualClock is the store clock a case advances by hand. Expiry contracts are
// driven by moving this clock rather than by sleeping, so no case waits on wall
// time and none is timing-flaky.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock(at time.Time) *manualClock { return &manualClock{now: at} }

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// TestSessionStoreInboxOrderAndDueTransitionsAreProviderNeutral holds command
// admission and settlement: acceptance order is an immutable opaque comparison
// key a retry receives unchanged, a mismatched retry fails closed, the due view
// carries exactly the outstanding commands, and a terminal command leaves it.
func TestSessionStoreInboxOrderAndDueTransitionsAreProviderNeutral(t *testing.T) {
	t.Parallel()
	for _, provider := range sessionStoreProviders(t) {
		t.Run(provider.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
			defer cancel()

			base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
			clock := newManualClock(base)
			// One control shard keeps the whole due sweep in a single bounded
			// page: the shard count is part of the immutable layout marker, so
			// it is chosen here rather than discovered.
			store := openSessionStore(t, ctx, provider.open(t, ctx),
				sessionstore.WithClock(clock),
				sessionstore.WithControlShards(1),
			)
			if got := store.ControlShards(); got != 1 {
				t.Fatalf("ControlShards = %d, want the configured 1", got)
			}
			tenant := randomSessionStoreTenant(t)
			session := sessionwire.SessionID("session-" + randomSessionStoreToken(t))
			createSessionStoreSession(t, ctx, store, tenant, session, base)

			admit := func(command sessionwire.CommandID, runtimeID sessionstore.RuntimeCommandID, kind sessionstore.CommandKind, deadline time.Time) (sessionstore.InboxEntry, bool) {
				t.Helper()
				entry, created, err := store.AdmitCommand(ctx, sessionstore.AdmitCommandRequest{
					TenantID:                 tenant,
					SessionID:                session,
					CommandID:                command,
					ProposedRuntimeCommandID: runtimeID,
					Kind:                     kind,
					Payload:                  []byte("payload-" + string(command)),
					AcceptedAt:               clock.Now(),
					ApplyDeadline:            deadline,
				})
				if err != nil {
					t.Fatalf("AdmitCommand(%s): %v", command, err)
				}
				return entry, created
			}

			firstCmd := sessionwire.CommandID("command-first-" + randomSessionStoreToken(t))
			secondCmd := sessionwire.CommandID("command-second-" + randomSessionStoreToken(t))
			first, created := admit(firstCmd, "runtime-first", "prompt", base.Add(10*time.Minute))
			if !created {
				t.Fatalf("AdmitCommand(%s) created = false, want this call to be the acceptance", firstCmd)
			}
			second, created := admit(secondCmd, "runtime-second", "prompt", base.Add(30*time.Minute))
			if !created {
				t.Fatalf("AdmitCommand(%s) created = false, want this call to be the acceptance", secondCmd)
			}
			if second.AcceptedOrder <= first.AcceptedOrder {
				t.Fatalf("acceptance order = %d then %d, want strictly increasing within the session", first.AcceptedOrder, second.AcceptedOrder)
			}
			if first.Record.State != sessionstore.InboxStatePending {
				t.Fatalf("admitted state = %q, want %q", first.Record.State, sessionstore.InboxStatePending)
			}

			// A retry is the same acceptance: it receives the winner's runtime
			// mapping and the same immutable order, however it proposed.
			retry, created := admit(firstCmd, "runtime-loser", "prompt", base.Add(9*time.Minute))
			if created {
				t.Fatalf("retry of %s reported created = true, want the original acceptance", firstCmd)
			}
			if retry.AcceptedOrder != first.AcceptedOrder {
				t.Fatalf("retry acceptance order = %d, want the original %d unchanged", retry.AcceptedOrder, first.AcceptedOrder)
			}
			if retry.Record.RuntimeCommandID != first.Record.RuntimeCommandID {
				t.Fatalf("retry runtime mapping = %q, want the winner's %q", retry.Record.RuntimeCommandID, first.Record.RuntimeCommandID)
			}

			// A retry whose CONTENT differs is not a retry, and it fails closed
			// rather than reporting someone else's acceptance as its own.
			_, _, err := store.AdmitCommand(ctx, sessionstore.AdmitCommandRequest{
				TenantID:                 tenant,
				SessionID:                session,
				CommandID:                firstCmd,
				ProposedRuntimeCommandID: "runtime-first",
				Kind:                     "interrupt",
				Payload:                  []byte("payload-" + string(firstCmd)),
				AcceptedAt:               clock.Now(),
				ApplyDeadline:            base.Add(10 * time.Minute),
			})
			var mismatch *sessionstore.InboxError
			if !errors.As(err, &mismatch) || mismatch.Code != sessionstore.InboxErrorCommandMismatch {
				t.Fatalf("mismatched retry error = %v, want *InboxError code %q", err, sessionstore.InboxErrorCommandMismatch)
			}

			// The due view holds exactly the commands whose deadline is at or
			// before the horizon the sweep names. That bound is the caller's,
			// not the store clock's, so the sweep is deterministic.
			due := listDueSessionStoreCommands(t, ctx, store, base.Add(11*time.Minute))
			if len(due) != 1 || due[0] != firstCmd {
				t.Fatalf("due commands = %v, want only %s", due, firstCmd)
			}

			// A claim is fenced by the session lease epoch: a lower epoch may
			// not take a claim from a higher one, and the refusal carries the
			// high-water mark.
			claimed, err := store.ClaimCommand(ctx, sessionstore.ClaimCommandRequest{
				TenantID:         tenant,
				SessionID:        session,
				CommandID:        firstCmd,
				ExpectedRevision: first.Revision,
				LeaseEpoch:       7,
				ClaimExpiresAt:   clock.Now().Add(5 * time.Minute),
			})
			if err != nil {
				t.Fatalf("ClaimCommand: %v", err)
			}
			if claimed.Record.State != sessionstore.InboxStateClaimed {
				t.Fatalf("claimed state = %q, want %q", claimed.Record.State, sessionstore.InboxStateClaimed)
			}
			_, err = store.ClaimCommand(ctx, sessionstore.ClaimCommandRequest{
				TenantID:         tenant,
				SessionID:        session,
				CommandID:        firstCmd,
				ExpectedRevision: claimed.Revision,
				LeaseEpoch:       5,
				ClaimExpiresAt:   clock.Now().Add(5 * time.Minute),
			})
			var fenced *sessionstore.InboxError
			if !errors.As(err, &fenced) || fenced.Code != sessionstore.InboxErrorEpoch {
				t.Fatalf("stale-epoch claim error = %v, want *InboxError code %q", err, sessionstore.InboxErrorEpoch)
			}
			if fenced.Epoch != 7 {
				t.Fatalf("epoch refusal carried high-water mark %d, want 7", fenced.Epoch)
			}

			applying, err := store.BeginApplyingCommand(ctx, sessionstore.BeginApplyingCommandRequest{
				TenantID:         tenant,
				SessionID:        session,
				CommandID:        firstCmd,
				ExpectedRevision: claimed.Revision,
				LeaseEpoch:       7,
				ClaimExpiresAt:   clock.Now().Add(5 * time.Minute),
			})
			if err != nil {
				t.Fatalf("BeginApplyingCommand: %v", err)
			}
			if applying.Record.State != sessionstore.InboxStateApplying {
				t.Fatalf("state = %q, want %q", applying.Record.State, sessionstore.InboxStateApplying)
			}
			applied, err := store.CompleteCommand(ctx, sessionstore.CompleteCommandRequest{
				TenantID:         tenant,
				SessionID:        session,
				CommandID:        firstCmd,
				ExpectedRevision: applying.Revision,
				LeaseEpoch:       7,
				Result: sessionstore.CommandResult{
					CompletedAt: clock.Now(),
					EventID:     "event-applied",
					JournalSeq:  2,
				},
			})
			if err != nil {
				t.Fatalf("CompleteCommand: %v", err)
			}
			if applied.Record.State != sessionstore.InboxStateApplied {
				t.Fatalf("state = %q, want %q", applied.Record.State, sessionstore.InboxStateApplied)
			}
			if applied.AcceptedOrder != first.AcceptedOrder {
				t.Fatalf("settled acceptance order = %d, want the immutable %d", applied.AcceptedOrder, first.AcceptedOrder)
			}

			// A terminal command is filed NOT DUE, so it leaves the sweep even
			// at a horizon past every deadline in the session.
			due = listDueSessionStoreCommands(t, ctx, store, base.Add(2*time.Hour))
			if len(due) != 1 || due[0] != secondCmd {
				t.Fatalf("due commands after settlement = %v, want only the outstanding %s", due, secondCmd)
			}
		})
	}
}

// listDueSessionStoreCommands sweeps the single control shard these cases
// configure and returns the command identities the due view carries. It walks
// the continuation to exhaustion under a small page limit, so the sweep is
// bounded and complete rather than one page's worth.
func listDueSessionStoreCommands(t *testing.T, ctx context.Context, store *sessionstore.Store, horizon time.Time) []sessionwire.CommandID {
	t.Helper()
	var (
		commands []sessionwire.CommandID
		cursor   sessionwire.Cursor
	)
	for pages := 0; ; pages++ {
		if pages > 16 {
			t.Fatalf("due-command sweep did not terminate after %d pages", pages)
		}
		page, err := store.ListDueCommands(ctx, sessionstore.ListDueCommandsRequest{
			Shard:         0,
			DueAtOrBefore: horizon,
			Limit:         2,
			Cursor:        cursor,
		})
		if err != nil {
			t.Fatalf("ListDueCommands: %v", err)
		}
		if page.Limit != 2 {
			t.Fatalf("due page reported effective limit %d, want the requested 2", page.Limit)
		}
		if page.Examined > page.Limit {
			t.Fatalf("due page examined %d rows, want no more than the limit %d", page.Examined, page.Limit)
		}
		if page.Unreadable != 0 {
			t.Fatalf("due page reported %d unreadable rows, want 0", page.Unreadable)
		}
		for _, command := range page.Commands {
			commands = append(commands, command.Entry.Record.CommandID)
		}
		cursor = page.NextCursor
		if cursor == "" {
			return commands
		}
	}
}

// --- provider instrumentation ---------------------------------------------
//
// Everything below wraps a storage.Composite without changing its behaviour,
// recording the provider operation and the NAME each call carries. It is how
// this file proves claims about work that is not visible in a return value:
// that a page costs bounded provider work, that a refused Open touched nothing
// but the layout marker, and that a tenant identity never reaches a provider
// name.

// providerCall is one recorded provider operation.
type providerCall struct {
	Primitive      string
	Op             string
	Name           string
	RequestedLimit int
	ReturnedRows   int
}

// providerRecorder collects provider calls. It is safe for concurrent use
// because a Store may issue provider I/O from more than one goroutine.
type providerRecorder struct {
	mu    sync.Mutex
	calls []providerCall
}

func (r *providerRecorder) record(primitive, op, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, providerCall{Primitive: primitive, Op: op, Name: name})
}

func (r *providerRecorder) recordQuery(primitive, op, name string, requestedLimit, returnedRows int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, providerCall{
		Primitive:      primitive,
		Op:             op,
		Name:           name,
		RequestedLimit: requestedLimit,
		ReturnedRows:   returnedRows,
	})
}

func (r *providerRecorder) snapshot() []providerCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]providerCall, len(r.calls))
	copy(out, r.calls)
	return out
}

func (r *providerRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

// count reports how many recorded calls match primitive and op. An empty op
// matches every operation on that primitive.
func (r *providerRecorder) count(primitive, op string) int {
	var n int
	for _, call := range r.snapshot() {
		if call.Primitive != primitive {
			continue
		}
		if op != "" && call.Op != op {
			continue
		}
		n++
	}
	return n
}

type recordingLedger struct {
	inner storage.Ledger
	rec   *providerRecorder
}

func (l recordingLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	l.rec.record("Ledger", "Append", name)
	return l.inner.Append(ctx, name, expected, payload)
}

func (l recordingLedger) Read(ctx context.Context, name string, from uint64) (storage.Cursor, error) {
	l.rec.record("Ledger", "Read", name)
	cursor, err := l.inner.Read(ctx, name, from)
	if err != nil || cursor == nil {
		return cursor, err
	}
	return recordingCursor{inner: cursor, rec: l.rec, name: name}, nil
}

// recordingCursor makes provider record work visible without changing cursor
// semantics. In particular, Next is recorded before delegation so an attempted
// advance that returns EOF or another error still counts as provider work.
type recordingCursor struct {
	inner storage.Cursor
	rec   *providerRecorder
	name  string
}

func (c recordingCursor) Next(ctx context.Context) (storage.Record, error) {
	c.rec.record("LedgerCursor", "Next", c.name)
	return c.inner.Next(ctx)
}

func (c recordingCursor) Close() error {
	c.rec.record("LedgerCursor", "Close", c.name)
	return c.inner.Close()
}

func (l recordingLedger) Tip(ctx context.Context, name string) (uint64, error) {
	l.rec.record("Ledger", "Tip", name)
	return l.inner.Tip(ctx, name)
}

func (l recordingLedger) Delete(ctx context.Context, name string) error {
	l.rec.record("Ledger", "Delete", name)
	return l.inner.Delete(ctx, name)
}

type recordingLeaser struct {
	inner storage.Leaser
	rec   *providerRecorder
}

func (l recordingLeaser) Acquire(ctx context.Context, name string) (storage.Lease, error) {
	l.rec.record("Leaser", "Acquire", name)
	return l.inner.Acquire(ctx, name)
}

type recordingKV struct {
	inner storage.KV
	rec   *providerRecorder
}

func (k recordingKV) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	k.rec.record("KV", "Get", key)
	return k.inner.Get(ctx, key)
}

func (k recordingKV) Put(ctx context.Context, key string, expectedRev uint64, val []byte) (uint64, error) {
	k.rec.record("KV", "Put", key)
	return k.inner.Put(ctx, key, expectedRev, val)
}

func (k recordingKV) Keys(ctx context.Context, prefix string) ([]string, error) {
	k.rec.record("KV", "Keys", prefix)
	return k.inner.Keys(ctx, prefix)
}

func (k recordingKV) Delete(ctx context.Context, key string) error {
	k.rec.record("KV", "Delete", key)
	return k.inner.Delete(ctx, key)
}

type recordingBlobs struct {
	inner storage.Blobs
	rec   *providerRecorder
}

func (b recordingBlobs) Put(ctx context.Context, key string, r io.Reader) error {
	b.rec.record("Blobs", "Put", key)
	return b.inner.Put(ctx, key, r)
}

func (b recordingBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	b.rec.record("Blobs", "Get", key)
	return b.inner.Get(ctx, key)
}

func (b recordingBlobs) Delete(ctx context.Context, key string) error {
	b.rec.record("Blobs", "Delete", key)
	return b.inner.Delete(ctx, key)
}

func (b recordingBlobs) List(ctx context.Context, prefix string) ([]string, error) {
	b.rec.record("Blobs", "List", prefix)
	return b.inner.List(ctx, prefix)
}

// recordingLifecycleBlobs is recordingBlobs for a provider that DOES implement
// Storage's optional bounded reader lifecycle. The capability is carried
// through deliberately: a wrapper that dropped it would turn every instrumented
// case into the rejection case.
type recordingLifecycleBlobs struct {
	recordingBlobs
	bound time.Duration
}

func (b recordingLifecycleBlobs) BlobReaderCloseBound() time.Duration { return b.bound }

type recordingOrderedIndex struct {
	inner storage.OrderedIndex
	rec   *providerRecorder
}

// orderedName renders an ordered identity as one recorded name. Every component
// is included because each is a place a tenant identity could leak.
func orderedName(id storage.OrderedID) string {
	return id.Namespace + "|" + id.OrderingScope + "|" + string(id.StableKey)
}

func (o recordingOrderedIndex) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	o.rec.record("OrderedIndex", "Get", orderedName(id))
	return o.inner.Get(ctx, id)
}

func (o recordingOrderedIndex) Create(ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	o.rec.record("OrderedIndex", "Create", orderedName(id)+"|"+rankingScope)
	return o.inner.Create(ctx, id, rankingScope, value, rank, due)
}

func (o recordingOrderedIndex) Update(ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	o.rec.record("OrderedIndex", "Update", orderedName(id))
	return o.inner.Update(ctx, id, expectedRevision, value, rank, due)
}

func (o recordingOrderedIndex) Delete(ctx context.Context, id storage.OrderedID, expectedRevision uint64) (storage.OrderedRecord, error) {
	o.rec.record("OrderedIndex", "Delete", orderedName(id))
	return o.inner.Delete(ctx, id, expectedRevision)
}

// ListOrdered is instrumented for symmetry, but NO published sessionstore
// v0.3.0 code path reaches it: the only production ordered queries are the two
// ListRanked call sites (catalog page, host placement page) and the three
// ListDue ones (due gates, due commands, host-target sweep). There is therefore
// no public API this repository can call to assert a ListOrdered limit/row
// pair, and this file does not claim one. What is asserted instead is stronger
// in the direction that matters: assertOneQueryPerPage requires the page to be
// EXACTLY one recorded query of the named kind, so if a future release started
// enumerating acceptance order behind a page, the extra ListOrdered call would
// be recorded here and fail that assertion.
func (o recordingOrderedIndex) ListOrdered(ctx context.Context, namespace string, orderingScope string, afterOrder uint64, limit int) (storage.OrderedPage, error) {
	page, err := o.inner.ListOrdered(ctx, namespace, orderingScope, afterOrder, limit)
	o.rec.recordQuery("OrderedIndex", "ListOrdered", namespace+"|"+orderingScope, limit, len(page.Records))
	return page, err
}

func (o recordingOrderedIndex) ListRanked(ctx context.Context, namespace string, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	page, err := o.inner.ListRanked(ctx, namespace, rankingScope, after, limit)
	o.rec.recordQuery("OrderedIndex", "ListRanked", namespace+"|"+rankingScope, limit, len(page.Records))
	return page, err
}

func (o recordingOrderedIndex) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	page, err := o.inner.ListDue(ctx, namespace, dueAtOrBefore, after, limit)
	o.rec.recordQuery("OrderedIndex", "ListDue", namespace, limit, len(page.Records))
	return page, err
}

// instrumentComposite wraps every primitive of backend in a recorder. The
// wrapped composite behaves exactly like the original, including whether its
// Blobs primitive advertises the bounded reader lifecycle.
func instrumentComposite(t *testing.T, backend *storage.Composite) (*storage.Composite, *providerRecorder) {
	t.Helper()
	rec := &providerRecorder{}
	blobs := storage.Blobs(recordingBlobs{inner: backend.Blobs, rec: rec})
	if lifecycle, ok := backend.Blobs.(storage.BlobReaderLifecycle); ok {
		blobs = recordingLifecycleBlobs{
			recordingBlobs: recordingBlobs{inner: backend.Blobs, rec: rec},
			bound:          lifecycle.BlobReaderCloseBound(),
		}
	}
	wrapped, err := storage.NewCompositeWithOrderedIndex(
		recordingLedger{inner: backend.Ledger, rec: rec},
		recordingLeaser{inner: backend.Leaser, rec: rec},
		recordingKV{inner: backend.KV, rec: rec},
		blobs,
		recordingOrderedIndex{inner: backend.OrderedIndex, rec: rec},
	)
	if err != nil {
		t.Fatalf("wrap composite: %v", err)
	}
	return wrapped, rec
}

// TestSessionStoreHostTargetExpiryIsProviderNeutral holds the three — and only
// three — ways a Host target row leaves the placement page: a graceful drain, a
// due-reconciler sweep, and nothing else. In particular a lapsed row is
// declined by a listing and COUNTED, but stays ranked until a sweep withdraws
// it, so a deployment that never sweeps accumulates capacity that is gone.
func TestSessionStoreHostTargetExpiryIsProviderNeutral(t *testing.T) {
	t.Parallel()
	for _, provider := range sessionStoreProviders(t) {
		t.Run(provider.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
			defer cancel()

			base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
			clock := newManualClock(base)
			store := openSessionStore(t, ctx, provider.open(t, ctx), sessionstore.WithClock(clock))

			// The target is namespaced by a random agent so two cases sharing a
			// provider cannot see each other's capacity.
			key := sessionstore.HostTargetKey{
				AgentID:                sessionwire.AgentID("agent-" + randomSessionStoreToken(t)),
				RuntimeCompatibilityID: "runtime-v1",
				Placement:              sessionwire.HostPlacementPooled,
			}
			lapsing := sessionwire.HostID("host-lapsing-" + randomSessionStoreToken(t))
			draining := sessionwire.HostID("host-draining-" + randomSessionStoreToken(t))
			publish := func(host sessionwire.HostID, generation uint64, capacity uint64, expires time.Time) sessionstore.HostTargetEntry {
				t.Helper()
				entry, err := store.PublishHostTarget(ctx, sessionstore.PublishHostTargetRequest{
					Key:            key,
					HostID:         host,
					HostGeneration: generation,
					ObservedAt:     clock.Now(),
					Advertisement: sessionstore.HostAdvertisement{
						InternalEndpoint:  sessionwire.InternalEndpoint("wss://" + string(host) + ".internal:443"),
						IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
						Accepting:         true,
						AvailableCapacity: capacity,
						ExpiresAt:         expires,
					},
				})
				if err != nil {
					t.Fatalf("PublishHostTarget(%s): %v", host, err)
				}
				return entry
			}
			publish(lapsing, 1, 10, base.Add(5*time.Minute))
			publish(draining, 1, 4, base.Add(5*time.Minute))

			hosts, page := listSessionStoreHosts(t, ctx, store, key)
			if len(hosts) != 2 {
				t.Fatalf("placement page = %v, want both advertised hosts", hosts)
			}
			if hosts[0] != lapsing {
				t.Fatalf("placement page = %v, want the higher free capacity (%s) first", hosts, lapsing)
			}
			if page.LapsedSkipped != 0 || page.UnreadableSkipped != 0 {
				t.Fatalf("placement page skipped lapsed=%d unreadable=%d, want none", page.LapsedSkipped, page.UnreadableSkipped)
			}

			// A graceful drain leaves both views at the instant the Host
			// decides, rather than when its promise runs out.
			if _, err := store.DrainHostTarget(ctx, sessionstore.DrainHostTargetRequest{
				Key:            key,
				HostID:         draining,
				HostGeneration: 2,
			}); err != nil {
				t.Fatalf("DrainHostTarget(%s): %v", draining, err)
			}
			hosts, page = listSessionStoreHosts(t, ctx, store, key)
			if len(hosts) != 1 || hosts[0] != lapsing {
				t.Fatalf("placement page after drain = %v, want only %s", hosts, lapsing)
			}
			if page.LapsedSkipped != 0 {
				t.Fatalf("drained row was counted as lapsed (%d), want it gone from the view entirely", page.LapsedSkipped)
			}

			// The remaining Host's promise runs out. The listing declines to
			// publish an endpoint it will not vouch for and reports the row as
			// lapsed — and the row is still there, which is what the sweep is
			// for.
			clock.advance(10 * time.Minute)
			hosts, page = listSessionStoreHosts(t, ctx, store, key)
			if len(hosts) != 0 {
				t.Fatalf("placement page after expiry = %v, want no routable capacity", hosts)
			}
			if page.LapsedSkipped != 1 {
				t.Fatalf("LapsedSkipped = %d, want the one lapsed row counted", page.LapsedSkipped)
			}

			result, err := store.ReconcileHostTargets(ctx, sessionstore.ReconcileHostTargetsRequest{Limit: 2, MaxPages: 4})
			if err != nil {
				t.Fatalf("ReconcileHostTargets: %v", err)
			}
			if result.Withdrawn != 1 {
				t.Fatalf("sweep withdrew %d rows, want the single lapsed row (result %+v)", result.Withdrawn, result)
			}
			if result.Unreadable != 0 || result.Contended != 0 {
				t.Fatalf("sweep reported unreadable=%d contended=%d, want none (result %+v)", result.Unreadable, result.Contended, result)
			}
			if !result.Exhausted {
				t.Fatalf("sweep reported Exhausted = false with cursor %q, want a completed pass", result.NextCursor)
			}

			hosts, page = listSessionStoreHosts(t, ctx, store, key)
			if len(hosts) != 0 {
				t.Fatalf("placement page after sweep = %v, want no capacity", hosts)
			}
			if page.LapsedSkipped != 0 {
				t.Fatalf("LapsedSkipped after sweep = %d, want the withdrawn row to have left the ranked view", page.LapsedSkipped)
			}

			// The identity survives withdrawal and is REUSED: a Host that
			// drains at shutdown and advertises again at startup is the
			// ordinary case, so nothing here may retire the row.
			publish(lapsing, 3, 7, clock.Now().Add(5*time.Minute))
			hosts, _ = listSessionStoreHosts(t, ctx, store, key)
			if len(hosts) != 1 || hosts[0] != lapsing {
				t.Fatalf("placement page after re-advertisement = %v, want %s back", hosts, lapsing)
			}

			// A superseded incarnation is refused by the row's generation
			// high-water mark, and the refusal names it.
			_, err = store.PublishHostTarget(ctx, sessionstore.PublishHostTargetRequest{
				Key:            key,
				HostID:         lapsing,
				HostGeneration: 2,
				ObservedAt:     clock.Now(),
				Advertisement: sessionstore.HostAdvertisement{
					InternalEndpoint:  sessionwire.InternalEndpoint("wss://" + string(lapsing) + ".internal:443"),
					IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
					Accepting:         true,
					AvailableCapacity: 1,
					ExpiresAt:         clock.Now().Add(5 * time.Minute),
				},
			})
			var stale *sessionstore.HostTargetError
			if !errors.As(err, &stale) || stale.Code != sessionstore.HostTargetErrorGeneration {
				t.Fatalf("superseded publish error = %v, want *HostTargetError code %q", err, sessionstore.HostTargetErrorGeneration)
			}
			if stale.Generation != 3 {
				t.Fatalf("generation refusal carried high-water mark %d, want 3", stale.Generation)
			}
		})
	}
}

// listSessionStoreHosts walks one target's placement pages to exhaustion under a
// small limit and returns the advertised hosts in page order, together with the
// last page for its counters.
func listSessionStoreHosts(t *testing.T, ctx context.Context, store *sessionstore.Store, key sessionstore.HostTargetKey) ([]sessionwire.HostID, sessionstore.HostTargetPage) {
	t.Helper()
	var (
		hosts  []sessionwire.HostID
		cursor sessionwire.Cursor
		last   sessionstore.HostTargetPage
	)
	for pages := 0; ; pages++ {
		if pages > 16 {
			t.Fatalf("placement walk did not terminate after %d pages", pages)
		}
		page, err := store.ListCompatibleHosts(ctx, sessionstore.ListCompatibleHostsRequest{Key: key, Cursor: cursor, Limit: 2})
		if err != nil {
			t.Fatalf("ListCompatibleHosts: %v", err)
		}
		last = page
		for _, report := range page.Hosts {
			hosts = append(hosts, report.HostID)
		}
		cursor = page.NextCursor
		if cursor == "" {
			return hosts, last
		}
	}
}

// TestSessionStoreRegistryTombstoneIsProviderNeutral holds the registry's two
// halves: the ROUTE expires and is withheld from a reader, while the LEASE
// EPOCH is permanent, which is why a release writes a tombstone instead of
// deleting the row.
func TestSessionStoreRegistryTombstoneIsProviderNeutral(t *testing.T) {
	t.Parallel()
	for _, provider := range sessionStoreProviders(t) {
		t.Run(provider.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
			defer cancel()

			base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
			clock := newManualClock(base)
			store := openSessionStore(t, ctx, provider.open(t, ctx), sessionstore.WithClock(clock))
			tenant := randomSessionStoreTenant(t)
			session := sessionwire.SessionID("session-" + randomSessionStoreToken(t))
			createSessionStoreSession(t, ctx, store, tenant, session, base)

			// A session that has never registered has no record and therefore
			// no fence, so the refusal carries no epoch.
			_, err := store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{TenantID: tenant, SessionID: session})
			var absent *sessionstore.RegistryError
			if !errors.As(err, &absent) || absent.Code != sessionstore.RegistryErrorNotFound {
				t.Fatalf("unregistered read error = %v, want *RegistryError code %q", err, sessionstore.RegistryErrorNotFound)
			}
			if absent.Epoch != 0 {
				t.Fatalf("not_found carried epoch %d, want zero: there is no record and so no fence", absent.Epoch)
			}

			route := sessionstore.HostRoute{
				HostID:                 "host-a",
				HostGeneration:         1,
				AgentID:                "agent-a",
				RuntimeCompatibilityID: "runtime-v1",
				Placement:              sessionwire.HostPlacementDedicated,
				InternalEndpoint:       "wss://host-a.internal:443",
				Residency:              sessionwire.SessionResidencyResident,
				Accepting:              true,
			}
			if _, err := store.PutHostRegistration(ctx, sessionstore.PutHostRegistrationRequest{
				TenantID:   tenant,
				SessionID:  session,
				LeaseEpoch: 4,
				ObservedAt: clock.Now(),
				ExpiresAt:  clock.Now().Add(5 * time.Minute),
				Route:      route,
			}); err != nil {
				t.Fatalf("PutHostRegistration: %v", err)
			}

			live, err := store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{TenantID: tenant, SessionID: session})
			if err != nil {
				t.Fatalf("GetHostRegistration: %v", err)
			}
			if live.Registration.Route == nil || live.Registration.Route.HostID != "host-a" {
				t.Fatalf("live registration route = %+v, want the published Host route", live.Registration.Route)
			}
			if live.Registration.LeaseEpoch != 4 {
				t.Fatalf("live registration epoch = %d, want 4", live.Registration.LeaseEpoch)
			}

			// Past the observation's expiry the route is withheld — the Host
			// may have died at any instant since — while the retained fence
			// travels on the refusal, because that error is the whole public
			// account of a session with no route.
			clock.advance(10 * time.Minute)
			_, err = store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{TenantID: tenant, SessionID: session})
			var expired *sessionstore.RegistryError
			if !errors.As(err, &expired) || expired.Code != sessionstore.RegistryErrorExpired {
				t.Fatalf("expired read error = %v, want *RegistryError code %q", err, sessionstore.RegistryErrorExpired)
			}
			if expired.Epoch != 4 {
				t.Fatalf("expired refusal carried epoch %d, want the retained fence 4", expired.Epoch)
			}

			// A release is a tombstone under the same fence: one nil route
			// rather than an enumeration of cleared members.
			tombstone, err := store.ClearHostRegistration(ctx, sessionstore.ClearHostRegistrationRequest{TenantID: tenant, SessionID: session, LeaseEpoch: 4})
			if err != nil {
				t.Fatalf("ClearHostRegistration: %v", err)
			}
			if tombstone.Registration.Route != nil {
				t.Fatalf("tombstone kept a route %+v, want none", tombstone.Registration.Route)
			}
			if tombstone.Registration.LeaseEpoch != 4 {
				t.Fatalf("tombstone epoch = %d, want the fence 4 retained", tombstone.Registration.LeaseEpoch)
			}

			// Cleanup is idempotent under ONE grant: a repeat returns the
			// stored tombstone without writing, so the revision does not move.
			repeat, err := store.ClearHostRegistration(ctx, sessionstore.ClearHostRegistrationRequest{TenantID: tenant, SessionID: session, LeaseEpoch: 4})
			if err != nil {
				t.Fatalf("repeat ClearHostRegistration: %v", err)
			}
			if repeat.Revision != tombstone.Revision {
				t.Fatalf("repeat release advanced revision %d -> %d, want no write", tombstone.Revision, repeat.Revision)
			}

			// A LATER grant releasing the same session is not a repeat: leaving
			// the fence at the older epoch would let every lease granted in
			// between still write.
			later, err := store.ClearHostRegistration(ctx, sessionstore.ClearHostRegistrationRequest{TenantID: tenant, SessionID: session, LeaseEpoch: 9})
			if err != nil {
				t.Fatalf("later-grant ClearHostRegistration: %v", err)
			}
			if later.Revision <= tombstone.Revision {
				t.Fatalf("later grant revision = %d, want a rewrite above %d", later.Revision, tombstone.Revision)
			}
			if later.Registration.LeaseEpoch != 9 {
				t.Fatalf("later tombstone epoch = %d, want the advanced fence 9", later.Registration.LeaseEpoch)
			}

			// A read of a released session is refused with the retained fence,
			// never with a route.
			_, err = store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{TenantID: tenant, SessionID: session})
			var released *sessionstore.RegistryError
			if !errors.As(err, &released) || released.Code != sessionstore.RegistryErrorReleased {
				t.Fatalf("released read error = %v, want *RegistryError code %q", err, sessionstore.RegistryErrorReleased)
			}
			if released.Epoch != 9 {
				t.Fatalf("released refusal carried epoch %d, want 9", released.Epoch)
			}

			// A superseded Host cannot reset the fence by writing over the
			// tombstone.
			_, err = store.PutHostRegistration(ctx, sessionstore.PutHostRegistrationRequest{
				TenantID:   tenant,
				SessionID:  session,
				LeaseEpoch: 4,
				ObservedAt: clock.Now(),
				ExpiresAt:  clock.Now().Add(5 * time.Minute),
				Route:      route,
			})
			var fenced *sessionstore.RegistryError
			if !errors.As(err, &fenced) || fenced.Code != sessionstore.RegistryErrorEpoch {
				t.Fatalf("superseded registration error = %v, want *RegistryError code %q", err, sessionstore.RegistryErrorEpoch)
			}
			if fenced.Epoch != 9 {
				t.Fatalf("epoch refusal carried high-water mark %d, want 9", fenced.Epoch)
			}
		})
	}
}

// TestSessionStoreObjectFirstReferenceIsProviderNeutral holds the crash
// polarity every large-body path in this package is built on: the object is
// persisted and VERIFIED before anything that names it is written, never the
// other way round. The instrumented provider makes that ordering observable
// rather than inferred — the blob write precedes the ledger append that carries
// its reference — and the reference itself is a logical Core identity, not a
// provider key.
func TestSessionStoreObjectFirstReferenceIsProviderNeutral(t *testing.T) {
	t.Parallel()
	for _, provider := range sessionStoreProviders(t) {
		t.Run(provider.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
			defer cancel()

			backend, rec := instrumentComposite(t, provider.open(t, ctx))
			store := openSessionStore(t, ctx, backend)
			tenant := randomSessionStoreTenant(t)
			session := sessionwire.SessionID("session-" + randomSessionStoreToken(t))
			base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
			createSessionStoreSession(t, ctx, store, tenant, session, base)

			// A directly stored object round-trips through its logical identity.
			payload := []byte("checkpoint-" + strings.Repeat("x", 4096))
			digest := sha256.Sum256(payload)
			metadata, err := store.PutObject(ctx, sessionstore.PutObjectRequest{
				TenantID:  tenant,
				SessionID: session,
				Kind:      sessionstore.ObjectKindWorkspaceCheckpoint,
				SizeBytes: uint64(len(payload)),
				SHA256:    digest,
				MediaType: "application/octet-stream",
				Body:      bytes.NewReader(payload),
			})
			if err != nil {
				t.Fatalf("PutObject: %v", err)
			}
			if metadata.SizeBytes != uint64(len(payload)) {
				t.Fatalf("object size = %d, want %d", metadata.SizeBytes, len(payload))
			}
			if metadata.Digest != "sha256:"+hex.EncodeToString(digest[:]) {
				t.Fatalf("object digest = %q, want the canonical sha256 of the body", metadata.Digest)
			}
			if got := readSessionStoreObject(t, ctx, store, tenant, session, sessionstore.ObjectKindWorkspaceCheckpoint, metadata); !bytes.Equal(got, payload) {
				t.Fatalf("object round-trip returned %d bytes, want the %d written", len(got), len(payload))
			}

			// An object is bound to the kind it was stored as: a reference
			// cannot be redeemed as a different kind of object.
			_, err = store.GetObject(ctx, sessionstore.GetObjectRequest{
				TenantID:     tenant,
				SessionID:    session,
				ExpectedKind: sessionstore.ObjectKindArtifact,
				Metadata:     metadata,
			})
			var wrongKind *sessionstore.ObjectError
			if !errors.As(err, &wrongKind) {
				t.Fatalf("cross-kind object read error = %v, want an *ObjectError", err)
			}

			// An over-threshold public journal body takes the same route: the
			// object is written and verified first, and the record that names
			// it is appended second.
			writer, err := store.OpenJournal(ctx, sessionstore.OpenJournalRequest{TenantID: tenant, SessionID: session})
			if err != nil {
				t.Fatalf("OpenJournal: %v", err)
			}
			defer func() {
				if err := writer.Close(ctx); err != nil {
					t.Errorf("close journal: %v", err)
				}
			}()
			body := []byte(`{"blob":"` + strings.Repeat("b", sessionstore.DefaultJournalOverflowThresholdBytes) + `"}`)
			rec.reset()
			seq, err := writer.Append(ctx, sessionstore.Envelope{
				Kind:    sessionstore.EnvelopeKindPublicEvent,
				EventID: "event-overflow",
				Public:  sessionstore.BodySlot{Inline: body},
			})
			if err != nil {
				t.Fatalf("Append oversized public body: %v", err)
			}
			blobPut, ledgerAppend := -1, -1
			for i, call := range rec.snapshot() {
				if blobPut < 0 && call.Primitive == "Blobs" && call.Op == "Put" {
					blobPut = i
				}
				if ledgerAppend < 0 && call.Primitive == "Ledger" && call.Op == "Append" {
					ledgerAppend = i
				}
			}
			if blobPut < 0 || ledgerAppend < 0 {
				t.Fatalf("oversized append recorded blobPut=%d ledgerAppend=%d, want both", blobPut, ledgerAppend)
			}
			if blobPut > ledgerAppend {
				t.Fatalf("blob write recorded at %d, after the ledger append at %d: the reference must never precede its object", blobPut, ledgerAppend)
			}

			// The stored record carries a REFERENCE rather than the bytes, and
			// the reference is a Core object identity.
			runtime, err := store.ReadRuntimeJournal(ctx, sessionstore.ReadRuntimeJournalRequest{TenantID: tenant, SessionID: session, Limit: 10})
			if err != nil {
				t.Fatalf("ReadRuntimeJournal: %v", err)
			}
			var stored *sessionstore.BodyReference
			for _, record := range runtime.Records {
				if record.Seq != seq {
					continue
				}
				if record.Envelope.Public.Inline != nil {
					t.Fatalf("record %d kept a %d-byte inline body, want it offloaded to an object", seq, len(record.Envelope.Public.Inline))
				}
				stored = record.Envelope.Public.Reference
			}
			if stored == nil {
				t.Fatalf("record %d carries no public object reference", seq)
			}
			if stored.SizeBytes != uint64(len(body)) {
				t.Fatalf("reference size = %d, want the body's %d", stored.SizeBytes, len(body))
			}
			if stored.SHA256 != sha256.Sum256(body) {
				t.Fatal("reference digest does not match the appended body")
			}

			// The public read resolves that reference through the ordinary
			// verified object path, so a client sees bytes and never a key.
			page, err := store.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{TenantID: tenant, SessionID: session, Limit: 10})
			if err != nil {
				t.Fatalf("ReadPublicJournal: %v", err)
			}
			var resolved []byte
			for _, event := range page.Events {
				if event.EventID == "event-overflow" {
					resolved = event.Body
				}
			}
			if !bytes.Equal(resolved, body) {
				t.Fatalf("resolved public body is %d bytes, want the %d appended", len(resolved), len(body))
			}

			// The same reference is redeemable as an object in its own right,
			// which is what "objects first, references second" buys a reader.
			referenced, err := stored.ObjectMetadata()
			if err != nil {
				t.Fatalf("BodyReference.ObjectMetadata: %v", err)
			}
			if got := readSessionStoreObject(t, ctx, store, tenant, session, sessionstore.ObjectKindJournalPublic, referenced); !bytes.Equal(got, body) {
				t.Fatalf("journal object read returned %d bytes, want %d", len(got), len(body))
			}
		})
	}
}

// readSessionStoreObject reads one object to completion and closes the stream.
func readSessionStoreObject(t *testing.T, ctx context.Context, store *sessionstore.Store, tenant sessionwire.TenantID, session sessionwire.SessionID, kind sessionstore.ObjectKind, metadata sessionwire.ObjectMetadata) []byte {
	t.Helper()
	reader, err := store.GetObject(ctx, sessionstore.GetObjectRequest{
		TenantID:     tenant,
		SessionID:    session,
		ExpectedKind: kind,
		Metadata:     metadata,
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close object stream: %v", err)
		}
	}()
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read object stream: %v", err)
	}
	return body
}

// TestSessionStoreOpaqueKeysAreProviderNeutral holds the two opacity claims a
// consumer can actually check from outside: under the tenant-scoped layout a
// TENANT identity never reaches a provider name, and a page cursor is an opaque
// token bound to the tenant, session and projection that issued it.
//
// The first claim is deliberately paired with its limit. A session identity IS
// legible at the provider — it is the record's key inside a scope the
// derivation has already established — and this case asserts that too, so the
// tenant assertion cannot pass vacuously against names that were never
// recorded.
func TestSessionStoreOpaqueKeysAreProviderNeutral(t *testing.T) {
	t.Parallel()
	for _, provider := range sessionStoreProviders(t) {
		t.Run(provider.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
			defer cancel()

			backend, rec := instrumentComposite(t, provider.open(t, ctx))
			base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
			store := openSessionStore(t, ctx, backend, sessionstore.WithClock(newManualClock(base)))
			tenant := randomSessionStoreTenant(t)
			first := sessionwire.SessionID("session-" + randomSessionStoreToken(t))
			second := sessionwire.SessionID("session-" + randomSessionStoreToken(t))
			createSessionStoreSession(t, ctx, store, tenant, first, base.Add(time.Minute))
			createSessionStoreSession(t, ctx, store, tenant, second, base.Add(2*time.Minute))

			// Exercise every record family that names a session, so the tenant
			// has as many chances to leak as the API offers.
			writer, err := store.OpenJournal(ctx, sessionstore.OpenJournalRequest{TenantID: tenant, SessionID: first})
			if err != nil {
				t.Fatalf("OpenJournal: %v", err)
			}
			appendSessionStoreRecord(t, ctx, writer, sessionstore.Envelope{
				Kind:    sessionstore.EnvelopeKindPublicEvent,
				EventID: "event-1",
				Public:  sessionstore.BodySlot{Inline: []byte(`{"step":1}`)},
			})
			if err := writer.Close(ctx); err != nil {
				t.Fatalf("close journal: %v", err)
			}
			if _, _, err := store.AdmitCommand(ctx, sessionstore.AdmitCommandRequest{
				TenantID:                 tenant,
				SessionID:                first,
				CommandID:                "command-1",
				ProposedRuntimeCommandID: "runtime-1",
				Kind:                     "prompt",
				Payload:                  []byte("payload"),
				AcceptedAt:               base,
				ApplyDeadline:            base.Add(10 * time.Minute),
			}); err != nil {
				t.Fatalf("AdmitCommand: %v", err)
			}
			body := []byte("object-body")
			digest := sha256.Sum256(body)
			if _, err := store.PutObject(ctx, sessionstore.PutObjectRequest{
				TenantID:  tenant,
				SessionID: first,
				Kind:      sessionstore.ObjectKindArtifact,
				SizeBytes: uint64(len(body)),
				SHA256:    digest,
				Body:      bytes.NewReader(body),
			}); err != nil {
				t.Fatalf("PutObject: %v", err)
			}
			if _, err := store.PutHostRegistration(ctx, sessionstore.PutHostRegistrationRequest{
				TenantID:   tenant,
				SessionID:  first,
				LeaseEpoch: 1,
				ObservedAt: base,
				ExpiresAt:  base.Add(5 * time.Minute),
				Route: sessionstore.HostRoute{
					HostID:                 "host-a",
					HostGeneration:         1,
					AgentID:                "agent-a",
					RuntimeCompatibilityID: "runtime-v1",
					Placement:              sessionwire.HostPlacementDedicated,
					InternalEndpoint:       "wss://host-a.internal:443",
					Residency:              sessionwire.SessionResidencyResident,
					Accepting:              true,
				},
			}); err != nil {
				t.Fatalf("PutHostRegistration: %v", err)
			}

			calls := rec.snapshot()
			if len(calls) == 0 {
				t.Fatal("no provider calls recorded, so this case would assert nothing")
			}
			var sessionLegible bool
			for _, call := range calls {
				if strings.Contains(call.Name, string(tenant)) {
					t.Fatalf("provider %s.%s named %q, which carries the tenant identity %q", call.Primitive, call.Op, call.Name, tenant)
				}
				if strings.Contains(call.Name, string(first)) {
					sessionLegible = true
				}
			}
			if !sessionLegible {
				t.Fatalf("no recorded provider name carried the session identity %q; the tenant check above would be vacuous", first)
			}

			// A catalog cursor is bound to the tenant it was issued for.
			//
			// The error CODE alone does not prove that binding: a catalog
			// cursor carries the provider's own ranked cursor as its payload,
			// and a provider handed a token from another ranking scope rejects
			// it too, which this Store reports as the same CatalogErrorCursor.
			// So the refusal is pinned to where it must happen instead — in the
			// Store, BEFORE the provider is asked anything. That is the claim
			// worth holding: a foreign cursor is never forwarded to a backend,
			// so tenant isolation does not depend on a provider noticing.
			page, err := store.ListSessions(ctx, sessionstore.ListSessionsRequest{TenantID: tenant, Limit: 1})
			if err != nil {
				t.Fatalf("ListSessions: %v", err)
			}
			if page.NextCursor == "" {
				t.Fatal("ListSessions issued no cursor, want a continuation over the second session")
			}
			rec.reset()
			_, err = store.ListSessions(ctx, sessionstore.ListSessionsRequest{
				TenantID: randomSessionStoreTenant(t),
				Cursor:   page.NextCursor,
				Limit:    1,
			})
			var foreignCursor *sessionstore.CatalogError
			if !errors.As(err, &foreignCursor) || foreignCursor.Code != sessionstore.CatalogErrorCursor {
				t.Fatalf("cross-tenant cursor error = %v, want *CatalogError code %q", err, sessionstore.CatalogErrorCursor)
			}
			if forwarded := rec.snapshot(); len(forwarded) != 0 {
				t.Fatalf("rejected cross-tenant cursor reached the provider as %+v, want no provider I/O: the Store's own tenant binding must refuse the token", forwarded)
			}

			// A journal cursor is bound to its projection: a public token
			// cannot be replayed into the privileged read.
			public, err := store.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{TenantID: tenant, SessionID: first, Limit: 1, ScanLimit: 1})
			if err != nil {
				t.Fatalf("ReadPublicJournal: %v", err)
			}
			if public.NextCursor == "" {
				t.Fatal("bounded public read issued no cursor, want a continuation")
			}
			_, err = store.ReadRuntimeJournal(ctx, sessionstore.ReadRuntimeJournalRequest{
				TenantID:  tenant,
				SessionID: first,
				Cursor:    public.NextCursor,
				Limit:     10,
			})
			var wrongProjection *sessionstore.JournalError
			if !errors.As(err, &wrongProjection) || wrongProjection.Code != sessionstore.JournalErrorCursor {
				t.Fatalf("public cursor in a runtime read error = %v, want *JournalError code %q", err, sessionstore.JournalErrorCursor)
			}

			// And to the session: a cursor cannot be moved between sessions.
			//
			// The target session needs a journal at least as long as the
			// snapshot the cursor names, or the reader's unrelated "captured
			// tip wider than the live stream" guard rejects the token first and
			// this assertion passes without the session binding ever being
			// consulted.
			secondWriter, err := store.OpenJournal(ctx, sessionstore.OpenJournalRequest{TenantID: tenant, SessionID: second})
			if err != nil {
				t.Fatalf("OpenJournal on the second session: %v", err)
			}
			for i := range 3 {
				appendSessionStoreRecord(t, ctx, secondWriter, sessionstore.Envelope{
					Kind:    sessionstore.EnvelopeKindPublicEvent,
					EventID: sessionwire.EventID("event-second-" + strconv.Itoa(i)),
					Public:  sessionstore.BodySlot{Inline: []byte(`{"step":` + strconv.Itoa(i) + `}`)},
				})
			}
			if err := secondWriter.Close(ctx); err != nil {
				t.Fatalf("close second journal: %v", err)
			}
			secondLive, err := store.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{TenantID: tenant, SessionID: second, Limit: 10})
			if err != nil {
				t.Fatalf("ReadPublicJournal on the second session: %v", err)
			}
			if len(secondLive.Events) < 1 {
				t.Fatalf("second session journal returned %d events, want a stream at least as long as the replayed cursor's snapshot", len(secondLive.Events))
			}
			_, err = store.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{
				TenantID:  tenant,
				SessionID: second,
				Cursor:    public.NextCursor,
				Limit:     10,
			})
			var wrongSession *sessionstore.JournalError
			if !errors.As(err, &wrongSession) || wrongSession.Code != sessionstore.JournalErrorCursor {
				t.Fatalf("cross-session cursor error = %v, want *JournalError code %q", err, sessionstore.JournalErrorCursor)
			}
		})
	}
}

// TestSessionStoreLayoutAdoptionIsProviderNeutral holds the layout contract a
// deployment lives with: a fresh unmarked backend is atomically adopted as the
// tenant-scoped layout, the historical single-tenant layout exists only when it
// is asked for by name and binds the exact tenant it was configured with, and
// every mismatch is refused at Open — before session data and before any
// provider I/O other than the marker itself.
//
// The marker key is never spelled here. It is LEARNED from the one key a fresh
// default Open writes, so this case asserts sessionstore's externally visible
// behaviour rather than a copy of its internals.
func TestSessionStoreLayoutAdoptionIsProviderNeutral(t *testing.T) {
	t.Parallel()
	for _, provider := range sessionStoreProviders(t) {
		t.Run(provider.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
			defer cancel()

			backend, rec := instrumentComposite(t, provider.open(t, ctx))

			// A fresh unmarked backend is adopted, and adoption costs exactly
			// one marker read and one marker write. Anything else here would be
			// a probe for data in another layout, which this package promises
			// never to perform.
			store := openSessionStore(t, ctx, backend)
			adoption := rec.snapshot()
			if len(adoption) != 2 {
				t.Fatalf("fresh Open recorded %+v, want exactly a marker read and a marker write", adoption)
			}
			if adoption[0].Primitive != "KV" || adoption[0].Op != "Get" {
				t.Fatalf("fresh Open first call = %+v, want a KV Get of the layout marker", adoption[0])
			}
			if adoption[1].Primitive != "KV" || adoption[1].Op != "Put" || adoption[1].Name != adoption[0].Name {
				t.Fatalf("fresh Open second call = %+v, want a KV Put of the same marker key %q", adoption[1], adoption[0].Name)
			}
			markerKey := adoption[0].Name

			// The adopted layout is tenant-scoped: an ordinary opaque tenant
			// and session work, and neither has to be a legacy UUID.
			tenant := randomSessionStoreTenant(t)
			session := sessionwire.SessionID("session-" + randomSessionStoreToken(t))
			base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
			createSessionStoreSession(t, ctx, store, tenant, session, base)
			if err := store.Close(ctx); err != nil {
				t.Fatalf("close adopting store: %v", err)
			}

			// Reopening the same backend under the same layout rebinds it.
			rec.reset()
			reopened := openSessionStore(t, ctx, backend)
			if _, err := reopened.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: session}); err != nil {
				t.Fatalf("GetCatalogEntry after reopen: %v", err)
			}
			for _, call := range rec.snapshot() {
				if call.Primitive == "KV" && call.Op == "Put" && call.Name == markerKey {
					t.Fatal("reopen rewrote the layout marker, want an immutable marker")
				}
			}

			// Asking for the legacy layout over a tenant-v1 backend is a
			// migration, not a flag flip, and it is refused before anything but
			// the marker read.
			rec.reset()
			_, err := sessionstore.Open(ctx, backend, sessionstore.WithLegacySingleTenant(tenant))
			var mismatch *sessionstore.KeyspaceError
			if !errors.As(err, &mismatch) || mismatch.Code != sessionstore.KeyspaceLayoutMismatch {
				t.Fatalf("legacy Open over a tenant-v1 backend = %v, want *KeyspaceError code %q", err, sessionstore.KeyspaceLayoutMismatch)
			}
			assertOnlyMarkerRead(t, rec.snapshot(), markerKey)

			// A different control shard count is likewise part of the layout,
			// because outstanding records would otherwise be filed in shards no
			// sweep of the old count ever visits.
			rec.reset()
			_, err = sessionstore.Open(ctx, backend, sessionstore.WithControlShards(8))
			if !errors.As(err, &mismatch) || mismatch.Code != sessionstore.KeyspaceLayoutMismatch {
				t.Fatalf("Open with a different shard count = %v, want *KeyspaceError code %q", err, sessionstore.KeyspaceLayoutMismatch)
			}
			assertOnlyMarkerRead(t, rec.snapshot(), markerKey)
		})
	}
}

// TestSessionStoreLegacyLayoutIsTenantBoundAndProviderNeutral holds the other
// half of the layout contract: the historical single-tenant layout persists the
// exact tenant it was configured with, refuses every other tenant and the
// tenant-scoped layout on reopen, and derives nothing — its session identities
// must be canonical.
func TestSessionStoreLegacyLayoutIsTenantBoundAndProviderNeutral(t *testing.T) {
	t.Parallel()
	for _, provider := range sessionStoreProviders(t) {
		t.Run(provider.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
			defer cancel()

			backend, rec := instrumentComposite(t, provider.open(t, ctx))
			tenant := randomSessionStoreTenant(t)
			base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

			store := openSessionStore(t, ctx, backend, sessionstore.WithLegacySingleTenant(tenant))
			legacyOpen := rec.snapshot()
			if len(legacyOpen) != 2 || legacyOpen[0].Op != "Get" || legacyOpen[1].Op != "Put" {
				t.Fatalf("legacy Open recorded %+v, want exactly a marker read and a marker write", legacyOpen)
			}
			markerKey := legacyOpen[0].Name

			// The legacy layout derives no names, so its session identity must
			// be the canonical one its physical name is spelled with.
			raw, err := uuid.New()
			if err != nil {
				t.Fatalf("mint session uuid: %v", err)
			}
			session := sessionwire.SessionID(raw.String())
			createSessionStoreSession(t, ctx, store, tenant, session, base)

			// A non-canonical session identity is refused, and the refusal is
			// the keyspace's rather than a record's.
			_, _, err = store.CreateCatalogEntry(ctx, sessionstore.CreateCatalogEntryRequest{
				TenantID:               tenant,
				SessionID:              "not-a-uuid",
				AgentID:                "agent-a",
				RuntimeCompatibilityID: "runtime-v1",
				CreatedAt:              base,
				LastActiveAt:           base,
				State:                  sessionwire.SessionStateIdle,
				Residency:              sessionwire.SessionResidencyCold,
				DesiredPlacement:       sessionwire.HostPlacementPooled,
				IdempotencyKey:         "create-noncanonical",
			})
			var legacySession *sessionstore.KeyspaceError
			if !errors.As(err, &legacySession) || legacySession.Code != sessionstore.KeyspaceLegacySession {
				t.Fatalf("non-canonical legacy session error = %v, want *KeyspaceError code %q", err, sessionstore.KeyspaceLegacySession)
			}

			// A foreign tenant is refused by the marker's own tenant binding,
			// before the session's data is touched.
			_, err = store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{
				TenantID:  randomSessionStoreTenant(t),
				SessionID: session,
			})
			var foreignTenant *sessionstore.KeyspaceError
			if !errors.As(err, &foreignTenant) || foreignTenant.Code != sessionstore.KeyspaceLegacyTenant {
				t.Fatalf("foreign legacy tenant error = %v, want *KeyspaceError code %q", err, sessionstore.KeyspaceLegacyTenant)
			}
			if err := store.Close(ctx); err != nil {
				t.Fatalf("close legacy store: %v", err)
			}

			// The marker is tenant-bound: the same option with the same tenant
			// rebinds, any other tenant is refused, and so is the tenant-scoped
			// layout. Each refusal costs one marker read and nothing else.
			rec.reset()
			rebound := openSessionStore(t, ctx, backend, sessionstore.WithLegacySingleTenant(tenant))
			if _, err := rebound.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: session}); err != nil {
				t.Fatalf("GetCatalogEntry after legacy reopen: %v", err)
			}
			for _, call := range rec.snapshot() {
				if call.Primitive == "KV" && call.Op == "Put" && call.Name == markerKey {
					t.Fatal("legacy reopen rewrote the layout marker, want an immutable marker")
				}
			}

			rec.reset()
			_, err = sessionstore.Open(ctx, backend, sessionstore.WithLegacySingleTenant(randomSessionStoreTenant(t)))
			var mismatch *sessionstore.KeyspaceError
			if !errors.As(err, &mismatch) || mismatch.Code != sessionstore.KeyspaceLayoutMismatch {
				t.Fatalf("legacy Open with another tenant = %v, want *KeyspaceError code %q", err, sessionstore.KeyspaceLayoutMismatch)
			}
			assertOnlyMarkerRead(t, rec.snapshot(), markerKey)

			rec.reset()
			_, err = sessionstore.Open(ctx, backend)
			if !errors.As(err, &mismatch) || mismatch.Code != sessionstore.KeyspaceLayoutMismatch {
				t.Fatalf("default Open over a legacy backend = %v, want *KeyspaceError code %q", err, sessionstore.KeyspaceLayoutMismatch)
			}
			assertOnlyMarkerRead(t, rec.snapshot(), markerKey)
		})
	}
}

// assertOnlyMarkerRead fails unless calls are exactly one read of the layout
// marker: no marker write, no session data, and no provider I/O of any other
// kind. It is what "the refusal happens before anything else" means in terms a
// consumer can observe.
func assertOnlyMarkerRead(t *testing.T, calls []providerCall, markerKey string) {
	t.Helper()
	if len(calls) != 1 {
		t.Fatalf("refused Open recorded %+v, want exactly one layout-marker read", calls)
	}
	want := providerCall{Primitive: "KV", Op: "Get", Name: markerKey}
	if calls[0] != want {
		t.Fatalf("refused Open recorded %+v, want %+v", calls[0], want)
	}
}

// openFsstore opens a filesystem store on a temp dir this test owns.
func openFsstore(t *testing.T) *fsstore.Store {
	t.Helper()
	store, err := fsstore.Open(fsstore.Options{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("fsstore.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("fsstore Close: %v", err)
		}
	})
	return store
}

// TestSessionStoreRejectsRawFsstoreBeforeAnyProviderIO holds the provider
// compatibility rule at its sharpest edge. fsstore deliberately does not claim
// Storage's bounded blob-reader lifecycle — portable regular-file I/O generally
// has no deadline support, so Close may wait on an active Read — and
// SessionStore refuses it with a typed error that names the missing capability,
// before it reads or writes the layout marker and before any session data.
//
// SessionStore cannot state this itself: its production imports are limited to
// Core and Storage, so it can never name fsstore. Naming it is this
// repository's job.
func TestSessionStoreRejectsRawFsstoreBeforeAnyProviderIO(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
	defer cancel()

	raw := openFsstore(t).Backend()
	if _, ok := raw.Blobs.(storage.BlobReaderLifecycle); ok {
		t.Fatal("fsstore Blobs now advertises BlobReaderLifecycle; this case and the rejection it guards need revisiting")
	}
	backend, rec := instrumentComposite(t, raw)

	store, err := sessionstore.Open(ctx, backend)
	if store != nil {
		t.Fatal("Open returned a Store over a provider it rejected")
	}
	var invalid *sessionstore.InvalidBackendError
	if !errors.As(err, &invalid) {
		t.Fatalf("Open over raw fsstore error = %v, want *InvalidBackendError", err)
	}
	if invalid.Component != "BlobReaderLifecycle" {
		t.Fatalf("rejection named component %q, want %q", invalid.Component, "BlobReaderLifecycle")
	}
	if calls := rec.snapshot(); len(calls) != 0 {
		t.Fatalf("rejected Open performed provider I/O %+v, want none: the capability check precedes the layout marker", calls)
	}
}

// TestSessionStoreComposesFsstorePrimitivesWithLifecycleBlobs is the other side
// of that rule: the refusal is about ONE primitive, not about the filesystem.
// fsstore's ledger, lease, KV and ordered-index primitives compose into a
// working backend as soon as the Blobs primitive comes from a provider that
// does implement bounded reader shutdown, and the resulting Store runs the
// ordinary session lifecycle including an object-backed body.
func TestSessionStoreComposesFsstorePrimitivesWithLifecycleBlobs(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
	defer cancel()

	structural := openFsstore(t).Backend()
	lifecycleBlobs := memstore.New().Blobs
	if _, ok := lifecycleBlobs.(storage.BlobReaderLifecycle); !ok {
		t.Fatal("memstore Blobs does not advertise BlobReaderLifecycle; this composition proves nothing")
	}
	composed, err := storage.NewCompositeWithOrderedIndex(
		structural.Ledger,
		structural.Leaser,
		structural.KV,
		lifecycleBlobs,
		structural.OrderedIndex,
	)
	if err != nil {
		t.Fatalf("compose fsstore primitives with lifecycle Blobs: %v", err)
	}

	instrumented, rec := instrumentComposite(t, composed)
	store := openSessionStore(t, ctx, instrumented)
	tenant := randomSessionStoreTenant(t)
	session := sessionwire.SessionID("session-" + randomSessionStoreToken(t))
	base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	createSessionStoreSession(t, ctx, store, tenant, session, base)

	writer, err := store.OpenJournal(ctx, sessionstore.OpenJournalRequest{TenantID: tenant, SessionID: session})
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	defer func() {
		if err := writer.Close(ctx); err != nil {
			t.Errorf("close journal: %v", err)
		}
	}()
	// An over-threshold body exercises the composed Blobs primitive rather than
	// only the filesystem ones — and that is ASSERTED here rather than assumed,
	// because a round-trip alone cannot tell the two apart. The body sits above
	// the overflow threshold and below MaxInlineBodyBytes, which is the only
	// band in which an offloaded reference is also resolvable, so an offload
	// that silently stopped happening would leave the body inline and every
	// value below would still match. What distinguishes them is WHERE the bytes
	// went: a Blobs.Put on this composite, and a reference in place of the
	// inline slot.
	body := []byte(`{"blob":"` + strings.Repeat("c", sessionstore.DefaultJournalOverflowThresholdBytes) + `"}`)
	rec.reset()
	appendSessionStoreRecord(t, ctx, writer, sessionstore.Envelope{
		Kind:    sessionstore.EnvelopeKindPublicEvent,
		EventID: "event-composed",
		Public:  sessionstore.BodySlot{Inline: body},
	})
	var composedBlobPut bool
	for _, call := range rec.snapshot() {
		if call.Primitive == "Blobs" && call.Op == "Put" {
			composedBlobPut = true
		}
	}
	if !composedBlobPut {
		t.Fatalf("over-threshold append recorded %+v, want a Blobs.Put: the body never reached the composed lifecycle-capable Blobs primitive", rec.snapshot())
	}

	// And the record that names it carries the reference, not the bytes, so the
	// Put above is the body's storage rather than an incidental write.
	runtime, err := store.ReadRuntimeJournal(ctx, sessionstore.ReadRuntimeJournalRequest{TenantID: tenant, SessionID: session, Limit: 10})
	if err != nil {
		t.Fatalf("ReadRuntimeJournal: %v", err)
	}
	var offloaded *sessionstore.BodyReference
	for _, record := range runtime.Records {
		if record.Envelope.EventID != "event-composed" {
			continue
		}
		if record.Envelope.Public.Inline != nil {
			t.Fatalf("record kept a %d-byte inline body over the composed backend, want it offloaded to an object", len(record.Envelope.Public.Inline))
		}
		offloaded = record.Envelope.Public.Reference
	}
	if offloaded == nil {
		t.Fatal("no runtime record for the over-threshold append carries a public object reference")
	}
	if offloaded.SizeBytes != uint64(len(body)) {
		t.Fatalf("reference size = %d, want the body's %d", offloaded.SizeBytes, len(body))
	}

	page, err := store.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{TenantID: tenant, SessionID: session, Limit: 10})
	if err != nil {
		t.Fatalf("ReadPublicJournal: %v", err)
	}
	if len(page.Events) != 1 || page.Events[0].EventID != "event-composed" {
		t.Fatalf("public events = %+v, want the one appended event", page.Events)
	}
	if !bytes.Equal(page.Events[0].Body, body) {
		t.Fatalf("resolved body is %d bytes, want the %d appended", len(page.Events[0].Body), len(body))
	}

	entry, err := store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: session})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	if entry.Record.SessionID != session {
		t.Fatalf("catalog record session = %q, want %q", entry.Record.SessionID, session)
	}
}

// TestSessionStorePagesAreBoundedByQueryWork instruments the provider to prove
// what a page COSTS, which is the claim a picker and a sweep both depend on and
// the one a return value cannot show. A recent-first tenant page is one ranked
// provider query whose cost does not grow with the tenant's history: nothing
// enumerates a key prefix, reads a row by name, or narrows a wider page
// afterwards. A due sweep is one due query per page. A journal page examines no
// more records than the budget it was given.
func TestSessionStorePagesAreBoundedByQueryWork(t *testing.T) {
	t.Parallel()
	for _, provider := range sessionStoreProviders(t) {
		t.Run(provider.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), sessionStoreCaseTimeout)
			defer cancel()

			base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
			clock := newManualClock(base)
			backend, rec := instrumentComposite(t, provider.open(t, ctx))
			store := openSessionStore(t, ctx, backend,
				sessionstore.WithClock(clock),
				sessionstore.WithControlShards(1),
			)
			tenant := randomSessionStoreTenant(t)
			neighbour := randomSessionStoreTenant(t)

			// A small history, plus another tenant's sessions the page must not
			// pay for.
			for i := range 4 {
				createSessionStoreSession(t, ctx, store, tenant, sessionwire.SessionID("session-"+randomSessionStoreToken(t)), base.Add(time.Duration(i)*time.Minute))
				createSessionStoreSession(t, ctx, store, neighbour, sessionwire.SessionID("session-"+randomSessionStoreToken(t)), base.Add(time.Duration(i)*time.Minute))
			}

			rec.reset()
			page, err := store.ListSessions(ctx, sessionstore.ListSessionsRequest{TenantID: tenant, Limit: 3})
			if err != nil {
				t.Fatalf("ListSessions: %v", err)
			}
			if len(page.Sessions) != 3 {
				t.Fatalf("page returned %d sessions, want the requested limit of 3", len(page.Sessions))
			}
			assertOneQueryPerPage(t, rec, "ListRanked", 3, 3)

			// The same page over a much longer history costs the same query.
			for i := range 20 {
				createSessionStoreSession(t, ctx, store, tenant, sessionwire.SessionID("session-"+randomSessionStoreToken(t)), base.Add(time.Duration(100+i)*time.Minute))
			}
			rec.reset()
			deeper, err := store.ListSessions(ctx, sessionstore.ListSessionsRequest{TenantID: tenant, Limit: 3})
			if err != nil {
				t.Fatalf("ListSessions over a longer history: %v", err)
			}
			if len(deeper.Sessions) != 3 {
				t.Fatalf("page returned %d sessions, want the requested limit of 3", len(deeper.Sessions))
			}
			assertOneQueryPerPage(t, rec, "ListRanked", 3, 3)

			// A placement page is one ranked query too.
			key := sessionstore.HostTargetKey{
				AgentID:                sessionwire.AgentID("agent-" + randomSessionStoreToken(t)),
				RuntimeCompatibilityID: "runtime-v1",
				Placement:              sessionwire.HostPlacementPooled,
			}
			for i := range 3 {
				if _, err := store.PublishHostTarget(ctx, sessionstore.PublishHostTargetRequest{
					Key:            key,
					HostID:         sessionwire.HostID("host-" + randomSessionStoreToken(t)),
					HostGeneration: 1,
					ObservedAt:     clock.Now(),
					Advertisement: sessionstore.HostAdvertisement{
						InternalEndpoint:  sessionwire.InternalEndpoint("wss://host-" + strconv.Itoa(i) + ".internal:443"),
						IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
						Accepting:         true,
						AvailableCapacity: uint64(i + 1),
						ExpiresAt:         clock.Now().Add(5 * time.Minute),
					},
				}); err != nil {
					t.Fatalf("PublishHostTarget: %v", err)
				}
			}
			rec.reset()
			hosts, err := store.ListCompatibleHosts(ctx, sessionstore.ListCompatibleHostsRequest{Key: key, Limit: 2})
			if err != nil {
				t.Fatalf("ListCompatibleHosts: %v", err)
			}
			if len(hosts.Hosts) != 2 {
				t.Fatalf("placement page returned %d hosts, want the requested limit of 2", len(hosts.Hosts))
			}
			assertOneQueryPerPage(t, rec, "ListRanked", 2, 2)

			// A due sweep is one due query per page, and it reads no catalog
			// record and enumerates no session's inbox.
			session := sessionwire.SessionID("session-" + randomSessionStoreToken(t))
			createSessionStoreSession(t, ctx, store, tenant, session, base)
			for i := range 3 {
				if _, _, err := store.AdmitCommand(ctx, sessionstore.AdmitCommandRequest{
					TenantID:                 tenant,
					SessionID:                session,
					CommandID:                sessionwire.CommandID("command-" + strconv.Itoa(i)),
					ProposedRuntimeCommandID: sessionstore.RuntimeCommandID("runtime-" + strconv.Itoa(i)),
					Kind:                     "prompt",
					Payload:                  []byte("payload"),
					AcceptedAt:               clock.Now(),
					ApplyDeadline:            base.Add(time.Duration(i+1) * time.Minute),
				}); err != nil {
					t.Fatalf("AdmitCommand: %v", err)
				}
			}
			rec.reset()
			due, err := store.ListDueCommands(ctx, sessionstore.ListDueCommandsRequest{
				Shard:         0,
				DueAtOrBefore: base.Add(time.Hour),
				Limit:         2,
			})
			if err != nil {
				t.Fatalf("ListDueCommands: %v", err)
			}
			if len(due.Commands) != 2 || due.Examined != 2 {
				t.Fatalf("due page returned %d commands after examining %d rows, want 2 and 2", len(due.Commands), due.Examined)
			}
			assertOneQueryPerPage(t, rec, "ListDue", 2, 2)

			// A journal page examines no more RECORDS than its budget, which is
			// what the coverage watermark reports. Private records spend the
			// budget without publishing anything, so the bound is on work and
			// not on events.
			writer, err := store.OpenJournal(ctx, sessionstore.OpenJournalRequest{TenantID: tenant, SessionID: session})
			if err != nil {
				t.Fatalf("OpenJournal: %v", err)
			}
			defer func() {
				if err := writer.Close(ctx); err != nil {
					t.Errorf("close journal: %v", err)
				}
			}()
			for i := range 12 {
				if i%2 == 0 {
					appendSessionStoreRecord(t, ctx, writer, sessionstore.Envelope{
						Kind:    sessionstore.EnvelopeKindPublicEvent,
						EventID: sessionwire.EventID("event-" + strconv.Itoa(i)),
						Public:  sessionstore.BodySlot{Inline: []byte(`{"step":` + strconv.Itoa(i) + `}`)},
					})
					continue
				}
				appendSessionStoreRecord(t, ctx, writer, sessionstore.Envelope{
					Kind:     sessionstore.EnvelopeKindRuntimeControl,
					RecordID: "runtime-" + strconv.Itoa(i),
					Runtime:  sessionstore.BodySlot{Inline: []byte("private")},
				})
			}
			const scanBudget = 3
			rec.reset()
			journal, err := store.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{
				TenantID:  tenant,
				SessionID: session,
				ScanLimit: scanBudget,
				Limit:     100,
			})
			if err != nil {
				t.Fatalf("ReadPublicJournal: %v", err)
			}
			if journal.CoveredThrough > scanBudget {
				t.Fatalf("page covered through %d, want no more than the %d-record budget", journal.CoveredThrough, scanBudget)
			}
			if len(journal.Events) > scanBudget {
				t.Fatalf("page published %d events, want no more than the %d records it was allowed to examine", len(journal.Events), scanBudget)
			}
			if journal.CapturedTip <= scanBudget {
				t.Fatalf("captured tip = %d, want a journal longer than the budget so the bound means something", journal.CapturedTip)
			}
			if got := rec.count("Ledger", "Read"); got != 1 {
				t.Fatalf("bounded journal page issued %d ledger reads, want exactly one", got)
			}
			if got := rec.count("LedgerCursor", "Next"); got != scanBudget {
				t.Fatalf("bounded journal page attempted %d cursor advances, want exactly the %d-record scan budget", got, scanBudget)
			}
			if got := rec.count("LedgerCursor", "Close"); got != 1 {
				t.Fatalf("bounded journal page closed %d ledger cursors, want exactly one", got)
			}
			if got := rec.count("Blobs", ""); got != 0 {
				t.Fatalf("bounded journal page issued %d blob calls, want none for inline bodies", got)
			}
		})
	}
}

// assertOneQueryPerPage fails unless the recorded calls are exactly one ordered
// query of the named kind: no per-row record read, no acceptance-order
// enumeration, and no key-prefix scan behind it.
func assertOneQueryPerPage(t *testing.T, rec *providerRecorder, query string, requestedLimit, returnedRows int) {
	t.Helper()
	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("page recorded %+v, want exactly one %s query", calls, query)
	}
	if calls[0].Primitive != "OrderedIndex" || calls[0].Op != query {
		t.Fatalf("page recorded %+v, want a single OrderedIndex %s", calls[0], query)
	}
	if calls[0].RequestedLimit != requestedLimit {
		t.Fatalf("page requested provider limit %d, want exactly %d", calls[0].RequestedLimit, requestedLimit)
	}
	if calls[0].ReturnedRows != returnedRows {
		t.Fatalf("provider returned %d rows, want exactly %d", calls[0].ReturnedRows, returnedRows)
	}
}
