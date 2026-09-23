//go:build integration

// This file is runbook 07 I1.1's cases 3 and 4, WHICH ARE NO LONGER BLOCKED.
//
// # How the blocker moved, and where it ended
//
//  1. Before A9.1 stage 2, `/v1/realtime` was a `pending` route answering 501.
//  2. At factory v0.1.0 the route was served but this kit's composition
//     answered the upgrade HTTP 500. That was pinned so it would fail when it
//     changed, and it CHANGED on the factory v0.2.0 pin: the upgrade answers
//     101, and the row asserts that instead.
//  3. The other half was a HOST PUBLISHER, and host v0.2.1 closed it (B1): a
//     composed Host relays a resident runtime's committed tail to a HostLink
//     subscriber of the session channel.
//  4. The last blocker was FACTORY RELAYING THAT TAIL. factory v0.2.0 never
//     sent a subscribe on HostLink and constructed no routing.Relay, so nothing
//     it received could be fanned out. A trip-wire read that off the wire and
//     FIRED on the factory v0.5.0 pin; it is deleted, and the cases below drive
//     the behaviour for real.
//
// # What "exactly once and in order" means here, and why it is not a count
//
// A ClientLink stream is not a replay: a client is covered through a sequence
// EITHER by receiving each enduring publication OR by a session.reset naming
// that tip, which tells it to read the journal through it. A reset is how a
// client is TOLD about a gap, so it is not one. What must never happen is a
// SILENT gap -- an enduring record that is neither the next sequence nor one
// the client already holds. PooledCoveredThrough is that rule, and it is the
// assertion these cases make.

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// upgradeProbe issues a syntactically complete WebSocket upgrade and reports the
// status the route answered.
//
// It is a raw probe rather than a client library call because a library reports
// "bad handshake" for every non-101 answer, which loses the one piece of
// information worth recording.
func upgradeProbe(t *testing.T, f *orchestrationtest.FactoryFixture, origin string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, f.BaseURL+"/v1/realtime", nil)
	if err != nil {
		t.Fatalf("building the upgrade probe: %v", err)
		return 0, ""
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Origin", origin)
	req.Header.Set("Authorization", "Bearer "+orchestrationtest.KitActorCredential)
	resp, err := f.Client.Do(req)
	if err != nil {
		t.Fatalf("issuing the upgrade probe: %v", err)
		return 0, ""
	}
	defer func() { _ = resp.Body.Close() }()
	// A 101's body IS the upgraded connection: reading it waits on the server
	// until it gives up on the handshake, which cost this row twenty seconds
	// the day it first answered 101. It is closed unread.
	if resp.StatusCode == http.StatusSwitchingProtocols {
		return resp.StatusCode, ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		t.Fatalf("reading the upgrade answer: %v", err)
		return 0, ""
	}
	return resp.StatusCode, string(body)
}

// TestFactoryClientLinkIsComposedButNotYetDrivable is I1.1 cases 3 and 4.
func TestFactoryClientLinkIsComposedButNotYetDrivable(t *testing.T) {
	ctx := coldReadContext(t)
	baseline := orchestrationtest.CaptureGoroutines()
	clock := orchestrationtest.NewClock(time.Unix(coldReadEpoch, 0))
	store := orchestrationtest.NewStoreFixture(t, ctx, clock)
	served := orchestrationtest.NewFactoryFixture(t, store, clock)
	session := store.SeedSession(ctx, coldReadAgent, string(coldReadCompatibility))

	t.Run("the realtime route has left the not-implemented table", func(t *testing.T) {
		// The positive, and the retirement of the trip-wire that held I0.2 and
		// I1.1 cases 3-4. A plain GET is no longer 501: the route is SERVED and
		// a non-upgrade request is simply the wrong request, which is a
		// different statement from "this deployment composes nothing".
		status, body := served.Get(t, ctx, "/v1/realtime")
		if status == http.StatusNotImplemented {
			t.Fatalf("/v1/realtime is still not implemented: %s", body)
		}
	})

	t.Run("the ClientLink upgrade completes", func(t *testing.T) {
		// This row pinned the 500 factory v0.1.0 answered, with the instruction
		// "if it is 101 the link is drivable". It is 101 on factory v0.2.0, and
		// the Factory ↔ Host lane test drives the link for real.
		status, body := upgradeProbe(t, served, "http://"+served.Listener.Addr().String())
		if status != http.StatusSwitchingProtocols {
			t.Fatalf("the ClientLink upgrade answered %d (%s), want 101", status, body)
		}
	})

	t.Run("the durable half of cases 3 and 4: three enduring events, in order", func(t *testing.T) {
		// The journal holds them in order, which is the half this module can
		// prove; what nothing can do yet is observe them arriving at a
		// subscriber, for the reason the next row pins.
		for i := 1; i <= 3; i++ {
			store.AppendPublicEvent(ctx, session,
				sessionwire.EventID(fmt.Sprintf("event-reconnect-%d", i)),
				[]byte(fmt.Sprintf(`{"n":%d}`, i)))
		}
		status, body := served.Get(t, ctx, orchestrationtest.SessionPath(session, "/journal"))
		if status != http.StatusOK {
			t.Fatalf("GET journal = %d: %s", status, body)
		}
		var page sessionwire.JournalPage
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatalf("the journal page is not a Core JournalPage: %v", err)
		}
		if len(page.Events) != 3 {
			t.Fatalf("the journal holds %d events, want the 3 that were committed", len(page.Events))
		}
		// Exactly once and in order -- on the durable side. This is the
		// assertion cases 3 and 4 will make about the DELIVERED stream once
		// there is one, and holding it here keeps the fixture that produces the
		// three events honest in the meantime.
		for i, event := range page.Events {
			want := sessionwire.EventID(fmt.Sprintf("event-reconnect-%d", i+1))
			if event.EventID != want {
				t.Fatalf("journal event %d is %q, want %q", i, event.EventID, want)
			}
			if i > 0 && event.JournalSeq <= page.Events[i-1].JournalSeq {
				t.Fatalf("journal event %d is out of sequence order", i)
			}
		}
	})

	// The trip-wire that used to stand here -- "cases 3 and 4 wait on Factory
	// relaying the Host tail" -- FIRED on the factory v0.5.0 pin and is
	// deleted. Cases 3 and 4 are now driven end to end against a real Host in
	// TestFactoryRepairsAReconnectedBrowserAcrossReplicas below.

	orchestrationtest.AssertNoLeaks(t, ctx, orchestrationtest.LeakSources{
		Store:              store,
		Factories:          []*orchestrationtest.FactoryFixture{served},
		LinksUnobservable:  true,
		BaselineGoroutines: baseline,
	})
}

// reconnectWorld stands up one durable plane, one pooled Host and TWO Factory
// replicas, and returns them with a session id that does not exist yet.
//
// TWO REPLICAS OVER ONE DURABLE PLANE is the whole point of cases 3 and 4: a
// browser that comes back must be repaired by WHICHEVER replica it reaches,
// from durable state alone, because nothing is shared between them but the
// store. A single-replica case would prove only that one process remembers.
func reconnectWorld(t *testing.T) (context.Context, *orchestrationtest.PooledWorld, *orchestrationtest.PooledFactory, *orchestrationtest.PooledFactory, sessionwire.SessionID) {
	t.Helper()
	ctx := placementContext(t)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{orchestrationtest.PooledTenantA},
	})
	pooled := orchestrationtest.StartPooledHost(t, ctx, world, "orchestrationtest-reconnect-host", 4)
	orchestrationtest.AwaitAdvertised(t, world, pooled.ID)

	replicaA := orchestrationtest.StartPooledFactory(t, ctx, world, "orchestrationtest-reconnect-a", nil)
	replicaB := orchestrationtest.StartPooledFactory(t, ctx, world, "orchestrationtest-reconnect-b", nil)
	return ctx, world, replicaA, replicaB, sessionwire.SessionID("session-reconnect")
}

// reconnectCreate posts the create and waits for it to apply.
func reconnectCreate(t *testing.T, ctx context.Context, world *orchestrationtest.PooledWorld, f *orchestrationtest.PooledFactory, session sessionwire.SessionID) {
	t.Helper()
	status, body := f.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("command-reconnect-create"),
		SessionID:       session,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(`[{"type":"text","text":"hello"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "the create applied", 90*time.Second, func() bool {
		return world.CommandState(ctx, orchestrationtest.PooledTenantA, session, "command-reconnect-create") == sessionstore.InboxStateApplied
	})
}

// reconnectInput posts one input through a replica and waits for it to apply.
func reconnectInput(t *testing.T, ctx context.Context, world *orchestrationtest.PooledWorld, f *orchestrationtest.PooledFactory, session sessionwire.SessionID, command sessionwire.CommandID) {
	t.Helper()
	status, body := f.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions/"+string(session)+"/input", sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)),
		SessionID:       session,
		Blocks:          json.RawMessage(`[{"type":"text","text":"again"}]`),
	})
	if status != http.StatusOK {
		t.Fatalf("the input %q answered %d: %s", command, status, body)
	}
	orchestrationtest.PooledWait(t, "the input "+string(command)+" applied", 60*time.Second, func() bool {
		return world.CommandState(ctx, orchestrationtest.PooledTenantA, session, command) == sessionstore.InboxStateApplied
	})
}

// reconnectCapturedTip reads one session's journal page through a replica and
// returns the captured tip it reports.
//
// THIS IS HALF OF WHAT A RECONNECTING BROWSER DOES, and the half that carries
// the events it missed. Measured here: a ClientLink subscribe to a QUIET
// session delivers NOTHING -- not a publication, not even a journal-tip hint --
// because the link is a live tail and nothing is being published. A client
// learns where it is by reading the journal, and the live tail continues from
// there. A case that waited on the socket alone would wait forever, which is
// what the first draft of this file did.
//
// # THE LIMITATION THIS PUTS ON I1.1 CASE 3, STATED
//
// The acceptance row asks the reconnected browser to "observe ALL THREE
// exactly once and in order". THIS FIXTURE CANNOT SHOW IT THE THREE. It can
// only show it a NUMBER, and the reason is structural rather than lazy:
// PooledTails is the PRODUCT's committed stream and SessionStore's journal
// holds none of it, so orchestrationtest.pooledTipReader reports the product's
// tip and CLEARS page.Events whenever that tip is above the store's. A
// reconnecting browser here therefore learns `CapturedTip` and nothing else.
//
// That limitation now binds only the cases still run in an ordinary world
// (case 4 and I1.4 case 1). Case 3 runs in a DurableTail world, where the
// product's events ARE in SessionStore's own journal, and proves the three
// missed events themselves arrive -- see
// TestFactoryRepairsAReconnectedBrowserAcrossReplicas.
func reconnectCapturedTip(t *testing.T, ctx context.Context, f *orchestrationtest.PooledFactory, session sessionwire.SessionID) uint64 {
	t.Helper()
	status, body := f.Get(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions/"+string(session)+"/journal")
	if status != http.StatusOK {
		t.Fatalf("the journal read answered %d: %s", status, body)
		return 0
	}
	var page sessionwire.JournalPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatalf("the journal page is not a Core JournalPage (%s): %v", body, err)
		return 0
	}
	return page.CapturedTip
}

// assertLiveTailIsExactlyOnceInOrder holds a viewer's stream to the rule: no
// silent gap, no stray, and no enduring sequence delivered twice.
func assertLiveTailIsExactlyOnceInOrder(t *testing.T, viewer *orchestrationtest.PooledViewer, from, through uint64) {
	t.Helper()
	covered, err := orchestrationtest.PooledCoveredThroughFrom(viewer.Records(), from)
	if err != nil || covered < through {
		t.Fatalf("the stream %v covers through %d (%v) from %d, want %d with no silent gap", viewer.Records(), covered, err, from, through)
	}
	if strays := viewer.Strays(); len(strays) != 0 {
		t.Fatalf("the viewer received records naming another session: %v", strays)
	}
	// PooledCoveredThrough tolerates a duplicate the client already holds,
	// because a repair may legitimately re-send one; what it cannot tolerate is
	// the same NEW sequence twice, which is checked here.
	seen := map[string]int{}
	for _, record := range viewer.Records() {
		if strings.HasPrefix(record, "E") {
			seen[record]++
		}
	}
	for record, count := range seen {
		if count > 1 {
			t.Fatalf("the viewer received %s %d times: %v", record, count, viewer.Records())
		}
	}
}

// TestFactoryRepairsAReconnectedBrowserAcrossReplicas is I1.1 CASE 3, in full.
//
// A browser holding a cursor disconnects, EXACTLY THREE enduring events are
// committed while it is away, and it reconnects TO THE OTHER REPLICA with its
// OLD CURSOR. It must then hold every one of the three -- by EventID and
// journal sequence, exactly once and in order -- followed by the live tail,
// with nothing shared between the replicas but the durable plane.
//
// # Why this is no longer the weaker case it was
//
// It runs in a DurableTail world: the product's committed stream IS
// a real SessionStore public journal (world.ProductJournal; each event
// appended through a real sessionstore.JournalWriter before it is published),
// and Factory's journal route reads that store, so replica B answers the
// reconnecting browser with the three events themselves, read from it. The browser is PooledBrowser, which repairs from
// that route on every (re)subscribe and fails the case on any silent gap.
// Nothing in the kit answers for a recovered event: the EXPECTATION is what
// the product committed (PooledTails.Committed), cross-checked against the
// store read directly, and the OBSERVATION is what the browser applied from
// Factory's route and ClientLink.
func TestFactoryRepairsAReconnectedBrowserAcrossReplicas(t *testing.T) {
	ctx := placementContext(t)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:     []sessionwire.TenantID{orchestrationtest.PooledTenantA},
		DurableTail: true,
	})
	pooled := orchestrationtest.StartPooledHost(t, ctx, world, "orchestrationtest-reconnect-host", 4)
	orchestrationtest.AwaitAdvertised(t, world, pooled.ID)
	replicaA := orchestrationtest.StartPooledFactory(t, ctx, world, "orchestrationtest-reconnect-a", nil)
	replicaB := orchestrationtest.StartPooledFactory(t, ctx, world, "orchestrationtest-reconnect-b", nil)
	const tenant = orchestrationtest.PooledTenantA
	session := sessionwire.SessionID("session-reconnect-durable")
	const perInput = orchestrationtest.PooledPublicationsPerInput
	committed := func() []orchestrationtest.PooledCommitted { return world.Tails.Committed(tenant, session) }
	settledThrough := func(b *orchestrationtest.PooledBrowser, want int) func() bool {
		return func() bool {
			all := committed()
			if len(all) < want {
				return false
			}
			_, position := b.Settle(t, ctx)
			return position >= all[want-1].JournalSeq
		}
	}

	// A browser on replica A, holding the session from its first event and
	// then streaming live.
	reconnectCreate(t, ctx, world, replicaA, session)
	first := orchestrationtest.OpenPooledBrowser(t, ctx, replicaA, tenant, orchestrationtest.PooledBrowserOptions{})
	if err := first.Watch(t, ctx, session, 0); err != nil {
		t.Fatalf("the first browser's subscribe was refused: %v", err)
	}
	first.Settle(t, ctx) // the join read, made at subscribe as a browser makes it
	reconnectInput(t, ctx, world, replicaA, session, "command-reconnect-before")
	orchestrationtest.PooledWait(t, "the first browser holds the create and the first input", 60*time.Second, settledThrough(first, 2*perInput))
	held, cursor := first.Settle(t, ctx)
	assertHoldsExactly(t, "the first browser", held, committed()[:2*perInput])
	if live := first.LiveEnduring(); len(live) == 0 {
		t.Fatalf("the first browser received nothing live (log %v): it never streamed, so its disconnect proves nothing", first.Log())
	}
	t.Logf("case 3: the first browser on replica A holds through %d: %v", cursor, held)

	// THE DISCONNECT, with the cursor the browser holds.
	first.Close()

	// EXACTLY THREE enduring events while it is away.
	reconnectInput(t, ctx, world, replicaA, session, "command-reconnect-while-away")
	orchestrationtest.PooledWait(t, "the three events were committed", 60*time.Second, func() bool {
		return len(committed()) == 3*perInput
	})
	missed := committed()[2*perInput : 3*perInput]
	if len(missed) != 3 || missed[0].JournalSeq <= cursor {
		t.Fatalf("the events committed while away are %v, want three above the cursor %d", missed, cursor)
	}
	// The expectation is not the kit's word alone: the three are in the REAL
	// product journal store, at those sequences, before anyone reads them back.
	assertStoreJournalHolds(t, ctx, world, tenant, session, cursor, missed)

	// THE RECONNECT, to the OTHER replica, with the OLD CURSOR.
	second := orchestrationtest.OpenPooledBrowser(t, ctx, replicaB, tenant, orchestrationtest.PooledBrowserOptions{})
	if err := second.Watch(t, ctx, session, cursor); err != nil {
		t.Fatalf("the reconnected browser's subscribe to replica B was refused: %v", err)
	}
	// The join read, made at subscribe from the old cursor -- BEFORE anything
	// more is committed, so the live tail has something left to carry.
	second.Settle(t, ctx)
	reconnectInput(t, ctx, world, replicaB, session, "command-reconnect-after-return")
	orchestrationtest.PooledWait(t, "the reconnected browser holds everything through the continued stream", 60*time.Second, settledThrough(second, 4*perInput))
	got, _ := second.Settle(t, ctx)
	t.Logf("case 3: the reconnected browser on replica B resumed at %d, repaired %+v, and holds %v (log %v)",
		cursor, second.Repairs(), got, second.Log())

	// ALL THREE MISSED, EXACTLY ONCE AND IN ORDER, then the live tail: the
	// browser holds precisely what was committed after its cursor.
	assertHoldsExactly(t, "the reconnected browser", got, committed()[2*perInput:4*perInput])

	// AND THE THREE CAME FROM THE DURABLE REPAIR, from the old cursor: the
	// first journal read started at cursor+1 -- not a replay from the head,
	// not a tail that happened to include them -- and carried all three. They
	// were committed before the browser subscribed, so no live record could
	// have supplied them.
	repairs := second.Repairs()
	if len(repairs) == 0 || repairs[0].From != cursor+1 {
		t.Fatalf("the reconnected browser's first repair is %+v, want a read from the old cursor %d", repairs, cursor+1)
	}
	var repaired []orchestrationtest.PooledCommitted
	for _, event := range repairs[0].Events {
		if event.JournalSeq > cursor {
			repaired = append(repaired, event)
		}
	}
	assertHoldsExactly(t, "the repair from the old cursor", repaired, missed)
	// AND THE TAIL CONTINUED LIVE: the three committed after the return
	// arrived as publications on replica B's ClientLink, in order.
	var live []orchestrationtest.PooledCommitted
	for _, event := range second.LiveEnduring() {
		if event.JournalSeq > missed[2].JournalSeq {
			live = append(live, event)
		}
	}
	assertHoldsExactly(t, "the reconnected browser's live tail", live, committed()[3*perInput:4*perInput])
	for _, event := range second.LiveEnduring() {
		for _, lost := range missed {
			if event.EventID == lost.EventID {
				t.Fatalf("the missed event %s arrived LIVE, so the repair is not what recovered it", lost.EventID)
			}
		}
	}
	if duplicates := second.LiveDuplicates(); len(duplicates) != 0 {
		t.Fatalf("the reconnected browser received sequences live more than once: %v", duplicates)
	}
	if failures := world.Tails.DurableFailures(); len(failures) != 0 {
		t.Fatalf("the product failed to commit durably: %v", failures)
	}
}

// assertHoldsExactly holds a browser's applied stream to an exact sequence of
// committed events: same EventIDs, same journal sequences, same order, and
// nothing else.
func assertHoldsExactly(t *testing.T, who string, got, want []orchestrationtest.PooledCommitted) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s holds %d events %v, want exactly %d %v", who, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s holds %v at position %d, want %v (got %v, want %v)", who, got[i], i, want[i], got, want)
		}
	}
}

// assertStoreJournalHolds reads the REAL store's public journal directly --
// no Factory, no kit reader -- and requires it to hold want, in order, right
// after the sequence after.
func assertStoreJournalHolds(t *testing.T, ctx context.Context, world *orchestrationtest.PooledWorld, tenant sessionwire.TenantID, session sessionwire.SessionID, after uint64, want []orchestrationtest.PooledCommitted) {
	t.Helper()
	page, err := world.ProductJournal.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{
		TenantID: tenant, SessionID: session, FromSeq: after + 1,
	})
	if err != nil {
		t.Fatalf("reading the store's journal after %d: %v", after, err)
	}
	var got []orchestrationtest.PooledCommitted
	for _, event := range page.Events {
		got = append(got, orchestrationtest.PooledCommitted{EventID: event.EventID, JournalSeq: event.JournalSeq})
	}
	if len(got) < len(want) {
		t.Fatalf("the store's journal after %d holds %v, want %v first", after, got, want)
	}
	assertHoldsExactly(t, "the store's own journal", got[:len(want)], want)
}

// TestFactoryBRepairsABrowserAfterFactoryADies is I1.1 CASE 4.
//
// Factory A is killed while a browser is attached to it and events are
// committed with nothing delivering them. The browser reconnects to Factory B,
// which must repair it from the JOURNAL SEQUENCE -- it shares no buffer, no
// cursor and no connection with A.
//
// # The approximation, stated
//
// The case as written asks for the kill to land "after it has buffered but
// before delivering". Nothing outside Factory can schedule that instant: the
// buffer is internal and the delivery is a goroutine. What is driven instead is
// the STATE that window produces and that a replica must recover from -- a
// replica gone, a browser that was attached to it, and committed events nobody
// delivered. A narrower test would be a test of this module's timing.
func TestFactoryBRepairsABrowserAfterFactoryADies(t *testing.T) {
	ctx, world, replicaA, replicaB, session := reconnectWorld(t)
	const perInput = orchestrationtest.PooledPublicationsPerInput

	attached := orchestrationtest.ConnectPooledViewer(t, ctx, replicaA, orchestrationtest.PooledTenantA)
	if err := attached.Watch(t, ctx, orchestrationtest.PooledTenantA, session); err != nil {
		t.Fatalf("the viewer's subscribe to replica A was refused: %v", err)
	}
	reconnectCreate(t, ctx, world, replicaA, session)
	orchestrationtest.PooledWait(t, "the viewer is covered through the create", 60*time.Second, func() bool {
		covered, err := orchestrationtest.PooledCoveredThrough(attached.Records())
		return err == nil && covered >= perInput
	})

	// KILL A, with the browser still attached to it.
	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := replicaA.Server.Stop(stopCtx); err != nil {
		t.Logf("case 4: replica A stopped with %v", err)
	}
	cancel()

	// Events committed with the browser's replica gone.
	reconnectInput(t, ctx, world, replicaB, session, "command-reconnect-after-death")
	orchestrationtest.PooledWait(t, "the events were committed with A gone", 60*time.Second, func() bool {
		return world.Tails.Tip(orchestrationtest.PooledTenantA, session) == 2*perInput
	})

	// THE REPAIR, on B: the journal sequence carries what A never delivered.
	repaired := orchestrationtest.ConnectPooledViewer(t, ctx, replicaB, orchestrationtest.PooledTenantA)
	if err := repaired.Watch(t, ctx, orchestrationtest.PooledTenantA, session); err != nil {
		t.Fatalf("the repaired viewer's subscribe to replica B was refused: %v", err)
	}
	resumed := reconnectCapturedTip(t, ctx, replicaB, session)
	if resumed < 2*perInput {
		t.Fatalf("replica B reported captured tip %d, want at least %d: what replica A never delivered is unreachable",
			resumed, 2*perInput)
	}
	reconnectInput(t, ctx, world, replicaB, session, "command-reconnect-recovered")
	orchestrationtest.PooledWait(t, "replica B delivered the continued stream", 60*time.Second, func() bool {
		covered, err := orchestrationtest.PooledCoveredThroughFrom(repaired.Records(), resumed)
		return err == nil && covered >= 3*perInput
	})
	t.Logf("case 4: the repaired viewer on replica B resumed at %d and received %v", resumed, repaired.Records())
	assertLiveTailIsExactlyOnceInOrder(t, repaired, resumed, 3*perInput)
}
