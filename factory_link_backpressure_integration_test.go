//go:build integration

// This file is runbook 07 task I1.4: the BLAST RADIUS of a realtime link
// failure. It was blocked behind the same trip-wire as I1.1 cases 3 and 4 --
// Factory relayed no Host tail, so there was no stream for a failure to damage
// -- and factory v0.5.0 lifted it.
//
// # What each case drives
//
// Every viewer is an orchestrationtest.PooledBrowser in a DurableTail world:
// it holds EXACT records (EventID and journal sequence), repairs from Factory's
// journal route on every (re)subscribe and reset, and fails the case on any
// silent gap. So "without loss" is asserted as the records themselves.
//
//	case 1  "Overflow one DeliveryBinding while another client shares the
//	        HostBinding; the slow client receives reset/closes and repairs; the
//	        peer continues without loss." DRIVEN AS A GENUINE OVERFLOW: a
//	        browser that stops reading its socket, under a load above the
//	        replica's ClientLink budget. It is closed (DisconnectSlow, 3008),
//	        repairs, and holds every record; two peers behind the same
//	        HostBinding are never closed, never reset, never repair past their
//	        join, and hold every record live.
//	case 2  "Force HostBinding queue failure; every DeliveryBinding for that
//	        HostBinding repairs independently while a session on another Host
//	        continues." Two Hosts, placed by capacity: tenant-a's session on
//	        Host X, tenant-b's on Host Y. THREE HostBinding failures on X, the
//	        first of them the queue failure itself:
//	        (a) A GENUINE QUEUE OVERFLOW. The composed HostBinding bound is the
//	        live-tail plane's per-session mailbox (MailboxLimit =
//	        routing.DefaultRepairLimits().HostBindingQueue = 1024 frames), and
//	        a burst of 8,192 frames injected on X's HostLink -- with the
//	        product committing enduring records through it -- overflows it:
//	        Factory drops the queued backlog, fences the tail's generation and
//	        repairs. Measured: several Factory-authored resets per browser, and
//	        some of the burst's enduring records reach browsers only through
//	        the repair. (An earlier draft of this header claimed the queue
//	        "cannot be overflowed from outside". That was wrong: a review
//	        overflowed it with this kit's own injector.)
//	        (b) a record Factory's relay REFUSES, injected on X's HostLink (a
//	        session.reset, which only Factory may author), which takes the same
//	        HostLinkClosed -> repair path without the dropped backlog; and
//	        (c) the transport under X's HostLinks, severed at TCP.
//	        After each, every browser of a is reset by Factory at a tip at or
//	        above the one before the failure (never the forged one) and holds
//	        every record exactly; the browser of b on Host Y sees nothing but
//	        its own live records.
//	case 3  the SELECTED Centrifuge slow-consumer threshold, and whether it
//	        closes a subscription or the physical link. MEASURED, in
//	        TestCentrifugeSlowConsumerThresholdAndItsBlastRadius: a real
//	        centrifuge-go client whose socket stops being read, against two
//	        replicas that differ only in PerConnectionQueueBytes. Measured
//	        outcome: the tight replica closes the slow client's PHYSICAL LINK
//	        with DisconnectSlow (3008) and the client redials; the loose one
//	        never closes it. The recovery asserted is for that measured blast
//	        radius: the slow client repairs from the durable journal with no
//	        record lost, and a fast peer of the same session and a viewer of
//	        another session on the same replica stream on untouched.
//	case 4  "enduring frames are never oldest-dropped and ephemeral coalescing
//	        remains bounded." Ephemeral publications are injected on X's
//	        HostLink (host v0.5.0 carries none). Factory's composition sets no
//	        CoalesceKey, so no delta supersedes another; what bounds ephemeral
//	        delivery is at-most-once-in-order and the transport budget. MEASURED:
//	        interleaved with enduring records, every browser received every
//	        ephemeral once, in order; a 1,024-record, ~4 MiB ephemeral flood at
//	        a stalled browser closed it at the budget (3008) after a few hundred,
//	        never redelivered one, and cost the fast peers nothing. Across the
//	        whole run every browser holds every enduring record exactly once and
//	        in order -- an oldest-drop is a silent gap, which the browser
//	        refuses.

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// TestRealtimeFailureBlastRadiusIsBoundedByTheSession is I1.4 cases 1, 2 and 4.
// Case 3 is TestCentrifugeSlowConsumerThresholdAndItsBlastRadius.
//
// # The world
//
// DurableTail, so every viewer is a PooledBrowser holding EXACT records and
// repairing from Factory's journal route. TWO HOSTS: Host X (capacity one)
// holds tenant-a's session and Host Y holds tenant-b's -- placed by capacity
// alone, no placement seam: X is full when b is created, and placement skips a
// Host with no available capacity. One Factory replica, with the tight
// ClientLink budget case 3 measured, serves every browser. Host X's HostLinks
// run through a HostLinkInjector, which is how cases 2 and 4 put a record on
// the wire that a released Host never sends.
func TestRealtimeFailureBlastRadiusIsBoundedByTheSession(t *testing.T) {
	ctx := placementContext(t)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		DurableTail:  true,
		TailPadBytes: slowConsumerPadBytes,
	})
	injector := orchestrationtest.NewHostLinkInjector()
	hostX := orchestrationtest.StartPooledHostSized(t, ctx, world, "orchestrationtest-blast-host-x", 1, 1, injector.Wrap)
	orchestrationtest.AwaitAdvertised(t, world, hostX.ID)
	served := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica:                 "orchestrationtest-blast-replica",
		PerConnectionQueueBytes: slowConsumerTightBudget,
		PingInterval:            2 * time.Minute,
		PongTimeout:             time.Minute,
		WriteTimeout:            time.Minute,
	})

	const tenantA, tenantB = orchestrationtest.PooledTenantA, orchestrationtest.PooledTenantB
	const sessionA, sessionB = sessionwire.SessionID("session-blast-a"), sessionwire.SessionID("session-blast-b")
	create := func(tn sessionwire.TenantID, s sessionwire.SessionID) {
		command := "command-blast-create-" + string(s)
		status, body := served.Post(t, ctx, tn, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(command),
			SessionID:       s,
			AgentID:         orchestrationtest.PooledAgent,
			Blocks:          json.RawMessage(`[{"type":"text","text":"hello"}]`),
		})
		if status != http.StatusCreated {
			t.Fatalf("the create of %s answered %d: %s", s, status, body)
		}
		orchestrationtest.PooledWait(t, "the create of "+string(s)+" applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tn, s, sessionwire.CommandID(command)) == sessionstore.InboxStateApplied
		})
	}

	// a on X, then b on Y because X is full.
	create(tenantA, sessionA)
	hostY := orchestrationtest.StartPooledHostSized(t, ctx, world, "orchestrationtest-blast-host-y", 1, 8, nil)
	orchestrationtest.AwaitAdvertised(t, world, hostY.ID)
	orchestrationtest.PooledWait(t, "host X reports itself full", 30*time.Second, func() bool {
		page, err := world.Store.ListCompatibleHosts(ctx, sessionstore.ListCompatibleHostsRequest{
			Key: sessionstore.HostTargetKey{
				AgentID: orchestrationtest.PooledAgent, RuntimeCompatibilityID: string(orchestrationtest.PooledCompatibility),
				Placement: sessionwire.HostPlacementPooled,
			},
			Limit: 8,
		})
		if err != nil {
			return false
		}
		for _, candidate := range page.Hosts {
			if candidate.HostID == hostX.ID {
				return candidate.AvailableCapacity == 0
			}
		}
		return true // absent from the candidate page is full too
	})
	create(tenantB, sessionB)
	if x, y := hostX.Rig.Creates(), hostY.Rig.Creates(); len(x) != 1 || x[0].Tenant != tenantA || len(y) != 1 || y[0].Tenant != tenantB {
		t.Fatalf("placement put X=%v Y=%v, want tenant-a's session alone on X and tenant-b's alone on Y", x, y)
	}

	watch := func(tn sessionwire.TenantID, s sessionwire.SessionID, options orchestrationtest.PooledBrowserOptions) *orchestrationtest.PooledBrowser {
		b := orchestrationtest.OpenPooledBrowser(t, ctx, served, tn, options)
		if err := b.Watch(t, ctx, s, 0); err != nil {
			t.Fatalf("a browser of %s was refused: %v", s, err)
		}
		b.Settle(t, ctx)
		return b
	}
	// THREE DeliveryBindings behind a's one HostBinding, and one on b.
	slow := watch(tenantA, sessionA, orchestrationtest.PooledBrowserOptions{Stallable: true, ReadBufferBytes: slowConsumerReadBuffer})
	peer := watch(tenantA, sessionA, orchestrationtest.PooledBrowserOptions{})
	peer2 := watch(tenantA, sessionA, orchestrationtest.PooledBrowserOptions{})
	onY := watch(tenantB, sessionB, orchestrationtest.PooledBrowserOptions{})
	onA := []*orchestrationtest.PooledBrowser{slow, peer, peer2}

	holdsThrough := func(tb testing.TB, b *orchestrationtest.PooledBrowser, tn sessionwire.TenantID, s sessionwire.SessionID) func() bool {
		return func() bool {
			all := world.Tails.Committed(tn, s)
			_, position := b.Settle(tb, ctx)
			return len(all) > 0 && position >= all[len(all)-1].JournalSeq
		}
	}
	allHold := func(tb testing.TB, what string) {
		for _, b := range onA {
			orchestrationtest.PooledWait(tb, what+" (tenant-a)", 90*time.Second, holdsThrough(tb, b, tenantA, sessionA))
		}
		orchestrationtest.PooledWait(tb, what+" (tenant-b, Host Y)", 90*time.Second, holdsThrough(tb, onY, tenantB, sessionB))
	}
	allHold(t, "every browser holds its create")

	// awaitLive commits enduring records to session a until one reaches b
	// LIVE, which is how a case knows a repair has finished and the live path
	// is back before it measures the next thing.
	awaitLive := func(tb *testing.T, b *orchestrationtest.PooledBrowser) {
		tb.Helper()
		world.Tails.Commit(tenantA, sessionA)
		probe := world.Tails.Committed(tenantA, sessionA)
		probeSeq := probe[len(probe)-1].JournalSeq
		orchestrationtest.PooledWait(tb, "a record arrives live again", 60*time.Second, func() bool {
			for _, event := range b.LiveEnduring() {
				if event.JournalSeq == probeSeq {
					return true
				}
			}
			world.Tails.Commit(tenantA, sessionA)
			probe = world.Tails.Committed(tenantA, sessionA)
			probeSeq = probe[len(probe)-1].JournalSeq
			return false
		})
		allHold(tb, "every browser holds through the probe")
	}

	// onYUntouched is the other-Host half of every case: the browser of the
	// session on Host Y never closes, is never reset, never repairs past its
	// join, and holds every record of its session.
	onYUntouched := func(tb *testing.T) {
		tb.Helper()
		if got := blastCloses(onY); len(got) != 0 {
			tb.Fatalf("the session on Host Y saw %v", got)
		}
		assertOnlyJoinRepair(tb, "the session on Host Y", onY)
		assertHoldsAll(tb, "the session on Host Y", onY, world.Tails.Committed(tenantB, sessionB))
	}

	t.Run("case 1: one overflowed DeliveryBinding repairs; its HostBinding peers lose nothing", func(t *testing.T) {
		// A GENUINE overflow, not the dead-client stand-in this row used to
		// be driven with: the slow browser stops reading its socket and the
		// load exceeds the replica's queue budget (case 3 measures that bound).
		slow.Stall()
		for sent := 0; sent < slowConsumerLoad; sent += slowConsumerBatch {
			for range slowConsumerBatch {
				world.Tails.Commit(tenantA, sessionA)
			}
			world.Tails.Commit(tenantB, sessionB)
			orchestrationtest.PooledWait(t, "the fast peer kept up", 60*time.Second, holdsThrough(t, peer, tenantA, sessionA))
		}
		slow.Release()
		allHold(t, "every browser holds the load")

		if !blastSawSlowClose(slow) {
			t.Fatalf("the overflowed browser saw %v, want a DisconnectSlow (3008) close", blastCloses(slow))
		}
		assertHoldsAll(t, "the overflowed browser, repaired", slow, world.Tails.Committed(tenantA, sessionA))
		if len(slow.Repairs()) < 2 {
			t.Fatalf("the overflowed browser made %d journal reads, want its repair after the close", len(slow.Repairs()))
		}
		for i, b := range []*orchestrationtest.PooledBrowser{peer, peer2} {
			if got := blastCloses(b); len(got) != 0 {
				t.Fatalf("peer %d behind the same HostBinding saw %v", i, got)
			}
			assertOnlyJoinRepair(t, "a peer behind the same HostBinding", b)
			assertHoldsAll(t, "a peer behind the same HostBinding", b, world.Tails.Committed(tenantA, sessionA))
		}
		onYUntouched(t)
	})

	t.Run("case 2: a HostBinding failure repairs every DeliveryBinding; the session on another Host continues", func(t *testing.T) {
		// FAILURE ONE, A GENUINE HOSTBINDING OVERFLOW. In composition the
		// HostBinding's inbound bound is the live-tail plane's per-session
		// MAILBOX, MailboxLimit = routing.DefaultRepairLimits().HostBindingQueue
		// = 1024 frames (factory compose.go). A burst the session's single
		// drainer cannot keep up with fills it; the plane then DROPS the
		// queued backlog, fences the tail's generation so later frames are
		// dropped too, and queues the repair (evLost -> Relay.HostLinkClosed).
		// The burst is ephemeral records -- the only records a Host can send
		// faster than the product commits -- with the product committing
		// enduring records THROUGH it, so the dropped backlog can hold
		// enduring frames that only the repair can give back.
		const burst = slowConsumerMailboxBurst
		committedBurst := world.Tails.Committed(tenantA, sessionA)
		tipBeforeBurst := committedBurst[len(committedBurst)-1].JournalSeq
		resetsBeforeBurst := map[*orchestrationtest.PooledBrowser]int{}
		ephemeralBefore := map[*orchestrationtest.PooledBrowser]int{}
		liveBefore := map[*orchestrationtest.PooledBrowser]int{}
		for _, b := range onA {
			resetsBeforeBurst[b] = len(b.Resets())
			ephemeralBefore[b] = len(b.Ephemeral())
			liveBefore[b] = len(b.LiveEnduring())
		}
		yBefore := len(onY.Log())
		for n := 1; n <= burst; n++ {
			record, err := orchestrationtest.EphemeralRecord(tenantA, sessionA, []byte(fmt.Sprintf(`{"n":%d}`, slowConsumerBurstBase+n)))
			if err != nil {
				t.Fatalf("encoding burst record %d: %v", n, err)
			}
			if _, err := injector.Push(tenantA, sessionA, record); err != nil {
				t.Fatalf("injecting burst record %d: %v", n, err)
			}
			if n%(burst/8) == 0 {
				world.Tails.Commit(tenantA, sessionA)
			}
		}
		world.Tails.Commit(tenantA, sessionA)
		world.Tails.Commit(tenantB, sessionB)
		allHold(t, "every browser holds through the mailbox overflow")
		committedAfter := world.Tails.Committed(tenantA, sessionA)
		for i, b := range onA {
			resets := b.Resets()[resetsBeforeBurst[b]:]
			if len(resets) == 0 {
				t.Fatalf("browser %d of the overflowed HostBinding received no reset: the %d-frame burst did not repair it (log tail %v)",
					i, burst, b.Log()[max(0, len(b.Log())-12):])
			}
			for _, r := range resets {
				if r.Tip < tipBeforeBurst {
					t.Fatalf("browser %d was reset to %v, below the tip %d before the burst", i, r, tipBeforeBurst)
				}
			}
			assertHoldsAll(t, fmt.Sprintf("browser %d after the mailbox overflow", i), b, committedAfter)
			got := b.Ephemeral()[ephemeralBefore[b]:]
			assertBoundedEphemeral(t, fmt.Sprintf("browser %d's burst", i), got, slowConsumerBurstBase+1, slowConsumerBurstBase+burst)
			// What the overflow cost this browser, measured: burst frames it
			// never saw, and enduring records it got from the repair rather
			// than live.
			liveNew := 0
			for _, event := range b.LiveEnduring()[liveBefore[b]:] {
				if event.JournalSeq > tipBeforeBurst {
					liveNew++
				}
			}
			t.Logf("case 2 overflow: browser %d got %d resets (%v), %d of %d burst frames, %d of %d new enduring records live",
				i, len(resets), resets, len(got), burst, liveNew, len(committedAfter)-len(committedBurst))
		}
		for _, entry := range onY.Log()[yBefore:] {
			if entry.Kind != "E" {
				t.Fatalf("the session on Host Y saw %v during X's mailbox overflow", entry)
			}
		}
		onYUntouched(t)

		// The overflow's repair is complete before the next arm is measured:
		// a record must arrive LIVE again, so no reset still in flight from
		// the burst can be credited to the refused record.
		awaitLive(t, peer)
		committedA := world.Tails.Committed(tenantA, sessionA)
		tipBefore := committedA[len(committedA)-1].JournalSeq
		resetsBefore := map[*orchestrationtest.PooledBrowser]int{}
		for _, b := range onA {
			resetsBefore[b] = len(b.Resets())
		}

		// FAILURE TWO, the relay's own refusal: Host X's HostLink
		// carries a record Factory's relay must refuse -- a session.reset,
		// which only Factory may author -- and the live-tail plane repairs
		// the session's HostBinding exactly as for a lost link.
		reset, err := sessionwire.SessionReset{TenantID: tenantA, SessionID: sessionA, LastContiguous: 1, JournalTip: 1}.MarshalJSON()
		if err != nil {
			t.Fatalf("encoding the refused record: %v", err)
		}
		if _, err := injector.Push(tenantA, sessionA, reset); err != nil {
			t.Fatalf("injecting the refused record: %v", err)
		}
		for range 3 {
			world.Tails.Commit(tenantA, sessionA)
			world.Tails.Commit(tenantB, sessionB)
		}
		allHold(t, "every browser holds through the refused-record repair")
		for i, b := range onA {
			resets := b.Resets()[resetsBefore[b]:]
			if len(resets) == 0 {
				t.Fatalf("browser %d of the failed HostBinding received no reset (log tail %v)", i, b.Log()[len(b.Log())-8:])
			}
			for _, r := range resets {
				// Factory's reset names the tip IT read, never the forged
				// record's (1/1): a forged reset relayed as-is would tell a
				// browser to rewind.
				if r.Tip < tipBefore {
					t.Fatalf("browser %d was reset to %v, below the tip %d: the Host's forged record reached it", i, r, tipBefore)
				}
			}
		}
		t.Logf("case 2: a refused Host record reset every browser of a: %v / %v / %v",
			slow.Resets()[resetsBefore[slow]:], peer.Resets()[resetsBefore[peer]:], peer2.Resets()[resetsBefore[peer2]:])

		// FAILURE THREE, the transport under X's HostBindings, severed at TCP.
		t.Logf("case 2: severed %d TCP connections to Host X", hostX.Sever())
		for range 3 {
			world.Tails.Commit(tenantA, sessionA)
			world.Tails.Commit(tenantB, sessionB)
		}
		allHold(t, "every browser holds through the sever")

		for _, b := range onA {
			assertHoldsAll(t, "a browser of the failed HostBinding", b, world.Tails.Committed(tenantA, sessionA))
		}
		// AND ANOTHER HOST'S SESSION CONTINUES, untouched by both failures.
		onYUntouched(t)
	})

	t.Run("case 4: enduring frames are never oldest-dropped and ephemeral delivery stays bounded", func(t *testing.T) {
		// Ephemeral records reach Factory only through the injector: host
		// v0.5.0 carries enduring publications alone. Factory's composition
		// sets no CoalesceKey (internal/realtime/livetail builds every
		// routing.Frame without one), so no delta supersedes another and the
		// relay's queues never fill behind a channel-wide publisher; what
		// BOUNDS ephemeral delivery is therefore "at most once, in order,
		// never amplified" and, for a consumer that cannot keep up, the same
		// transport byte budget that bounds enduring data.
		ephemeral := func(n int) []byte {
			record, err := orchestrationtest.EphemeralRecord(tenantA, sessionA,
				[]byte(fmt.Sprintf(`{"n":%d,"pad":%q}`, n, strings.Repeat("y", slowConsumerPadBytes))))
			if err != nil {
				t.Fatalf("encoding ephemeral %d: %v", n, err)
			}
			return record
		}
		inject := func(from, to int) {
			for n := from; n <= to; n++ {
				if _, err := injector.Push(tenantA, sessionA, ephemeral(n)); err != nil {
					t.Fatalf("injecting ephemeral %d: %v", n, err)
				}
			}
			// An enduring record behind the burst, on the same link: once a
			// browser holds it, every ephemeral before it has been delivered
			// or dropped.
			world.Tails.Commit(tenantA, sessionA)
		}

		// The live path must be back after case 2's sever before anything is
		// injected: a HostLink open again, and an enduring record arriving on
		// it LIVE rather than by repair.
		orchestrationtest.PooledWait(t, "Factory re-opened a HostLink to Host X", 60*time.Second, func() bool { return injector.Live() > 0 })
		awaitLive(t, peer)

		// Case 4 counts only its own ephemeral records: case 2's burst came
		// before it on the same browsers.
		epStart := map[*orchestrationtest.PooledBrowser]int{}
		for _, b := range onA {
			epStart[b] = len(b.Ephemeral())
		}
		ephemeralOf := func(b *orchestrationtest.PooledBrowser) []uint64 { return b.Ephemeral()[epStart[b]:] }

		// A: interleaved with enduring records, at a pace every browser keeps.
		const interleaved = 64
		for n := 1; n <= interleaved; n += 8 {
			inject(n, n+7)
			for _, b := range onA {
				orchestrationtest.PooledWait(t, "a browser kept up with the interleave", 60*time.Second, holdsThrough(t, b, tenantA, sessionA))
			}
		}
		for i, b := range onA {
			got := ephemeralOf(b)
			assertBoundedEphemeral(t, fmt.Sprintf("browser %d", i), got, 1, interleaved)
			t.Logf("case 4: browser %d received %d of %d interleaved ephemeral records", i, len(got), interleaved)
		}

		// B: a flood at a consumer that has stopped reading.
		closesBefore := map[*orchestrationtest.PooledBrowser]int{}
		for _, b := range onA {
			closesBefore[b] = len(blastCloses(b))
		}
		slow.Stall()
		const flood = slowConsumerLoad
		for n := interleaved + 1; n <= interleaved+flood; n += slowConsumerBatch {
			inject(n, n+slowConsumerBatch-1)
			orchestrationtest.PooledWait(t, "the fast peer kept up with the flood", 60*time.Second, holdsThrough(t, peer, tenantA, sessionA))
		}
		slow.Release()
		allHold(t, "every browser holds through the flood")
		for i, b := range onA {
			assertBoundedEphemeral(t, fmt.Sprintf("browser %d after the flood", i), ephemeralOf(b), 1, interleaved+flood)
		}
		t.Logf("case 4: under the flood the stalled browser received %d ephemeral records and saw %v; the fast peers received %d and %d",
			len(ephemeralOf(slow)), blastCloses(slow)[closesBefore[slow]:], len(ephemeralOf(peer)), len(ephemeralOf(peer2)))
		if !blastSawSlowCloseAfter(slow, closesBefore[slow]) {
			t.Fatalf("the stalled browser saw %v under the flood, want the budget's DisconnectSlow (3008) close", blastCloses(slow)[closesBefore[slow]:])
		}
		for i, b := range []*orchestrationtest.PooledBrowser{peer, peer2} {
			if got := blastCloses(b)[closesBefore[b]:]; len(got) != 0 {
				t.Fatalf("fast peer %d was closed by the flood: %v", i, got)
			}
			if got := len(ephemeralOf(b)); got != interleaved+flood {
				t.Fatalf("fast peer %d received %d of %d ephemeral records", i, got, interleaved+flood)
			}
		}
		// The flood is bounded by the budget: the stalled browser was closed
		// (again) rather than queued without limit, and did not receive the
		// whole flood.
		if got := len(ephemeralOf(slow)); got >= interleaved+flood {
			t.Fatalf("the stalled browser received all %d ephemeral records: nothing bounded its queue", got)
		}

		// THE ENDURING HALF, over the whole run: every browser holds every
		// enduring record exactly once and in order (Settle refuses any silent
		// gap -- an oldest-drop is one), and the fast peers never needed a
		// repair to get there.
		for _, b := range onA {
			assertHoldsAll(t, "a browser of a, over the whole run", b, world.Tails.Committed(tenantA, sessionA))
		}
		if strays := onY.Ephemeral(); len(strays) != 0 {
			t.Fatalf("the session on Host Y received a's ephemeral records: %v", strays)
		}
		onYUntouched(t)
	})

	if failures := world.Tails.DurableFailures(); len(failures) != 0 {
		t.Fatalf("the product failed to commit durably: %v", failures)
	}
}

// assertBoundedEphemeral requires an ephemeral stream to be a strictly
// increasing subset of [from, to]: never duplicated, never reordered, never
// anything that was not sent.
func assertBoundedEphemeral(t *testing.T, who string, got []uint64, from, to uint64) {
	t.Helper()
	var last uint64
	for _, n := range got {
		if n < from || n > to {
			t.Fatalf("%s received ephemeral %d, which was never sent (sent %d..%d)", who, n, from, to)
		}
		if n <= last {
			t.Fatalf("%s received ephemeral %d after %d: duplicated or reordered (%v)", who, n, last, got)
		}
		last = n
	}
}

// blastCloses reports every close, re-subscribe and reset a browser saw after
// its own first connect and subscribe (which carry code 0).
func blastCloses(b *orchestrationtest.PooledBrowser) []orchestrationtest.PooledBrowserEntry {
	var out []orchestrationtest.PooledBrowserEntry
	for _, entry := range b.Log() {
		switch entry.Kind {
		case "connecting", "disconnected", "subscribing", "unsubscribed":
			if entry.Code != 0 {
				out = append(out, entry)
			}
		case "R":
			out = append(out, entry)
		}
	}
	return out
}

// blastSawSlowCloseAfter reports whether a DisconnectSlow close is among a
// browser's closes after the first skip of them.
func blastSawSlowCloseAfter(b *orchestrationtest.PooledBrowser, skip int) bool {
	for _, entry := range blastCloses(b)[skip:] {
		if entry.Kind == "connecting" && entry.Code == 3008 {
			return true
		}
	}
	return false
}

// blastSawSlowClose reports whether the transport closed a browser's link with
// DisconnectSlow.
func blastSawSlowClose(b *orchestrationtest.PooledBrowser) bool {
	for _, entry := range b.Log() {
		if entry.Kind == "connecting" && entry.Code == 3008 {
			return true
		}
	}
	return false
}

// slowConsumer* size I1.4 case 3's load. The tight budget is far below the
// load and the loose one far above it, so the queue budget -- and nothing
// else -- separates the two replicas' outcomes.
const (
	slowConsumerPadBytes    = 4 << 10
	slowConsumerLoad        = 1024 // publications, ~4 MiB of bodies
	slowConsumerBatch       = 16
	slowConsumerTightBudget = 512 << 10
	slowConsumerLooseBudget = 256 << 20
	slowConsumerReadBuffer  = 4 << 10

	// slowConsumerMailboxBurst is I1.4 case 2's overflow: well above the
	// live-tail mailbox Factory composes (1024 frames), small-bodied so the
	// ClientLink budget is not what it measures. Burst records are numbered
	// from slowConsumerBurstBase so they cannot be read as case 4's.
	slowConsumerMailboxBurst = 8192
	slowConsumerBurstBase    = 1_000_000
)

// TestCentrifugeSlowConsumerThresholdAndItsBlastRadius is I1.4 CASE 3,
// MEASURED rather than assumed.
//
// # What is driven
//
// A genuinely slow ClientLink consumer: a real centrifuge-go client whose TCP
// connection STOPS BEING READ (PooledBrowser's stallable socket, with a small
// kernel receive buffer), so the bytes a real Factory sends pile up in the
// kernel and then in centrifuge's own per-connection queue. It is not a client
// that is slow in a callback: centrifuge-go reads its socket on its own
// goroutine into an unbounded callback queue, so a slow callback never reaches
// the server at all.
//
// # Sizing, and a finding it came from
//
// The budget bounds a BURST, not only a stall. At a 64 KiB budget with the
// load committed 64 records (256 KiB) at a time, the FAST peer -- a prompt
// reader on loopback -- was closed with 3008 too, twice, and repaired. That is
// the threshold working, not the slow client's blast radius, so the case
// commits 16 records (64 KiB) at a time against a 512 KiB budget: above any
// burst a prompt reader queues, far below the ~4 MiB a stalled one does. A
// deployment sizing PerConnectionQueueBytes must size it above its own
// largest burst for the same reason.
//
// TWO REPLICAS, IDENTICAL BUT FOR THE BUDGET. Factory's
// ClientLinkLimits.PerConnectionQueueBytes is the only thing that feeds
// centrifuge's ClientQueueMaxSize, and it is set to 512 KiB on one replica and
// 256 MiB on the other. The same stalled consumer, under the same ~4 MiB load,
// watches the same session on each. What differs in the outcome is therefore
// the configured threshold. The liveness bounds are raised on both, so the
// stall cannot reach a ping or write deadline instead -- the queue budget is
// the only bound in play.
//
// # What is measured, and asserted only after it was measured
//
// On the tight replica the server closes the slow client's PHYSICAL LINK --
// the transport reports a disconnect carrying centrifuge's DisconnectSlow
// (3008), and the client dials a new TCP connection -- rather than
// unsubscribing it from a channel. 3008 is in centrifuge-go's reconnect band,
// so the client comes back by itself and re-subscribes; PooledBrowser then
// repairs from the durable journal from the position it held. On the loose
// replica the same client, stalled the same way, is never closed and receives
// every record live.
//
// The blast radius asserted is the measured one: the link of the slow client
// and nothing else. A fast viewer of the SAME session on the SAME replica, and
// a viewer of ANOTHER tenant's session on it, keep streaming live throughout
// with no close, no reset and no repair.
func TestCentrifugeSlowConsumerThresholdAndItsBlastRadius(t *testing.T) {
	ctx := placementContext(t)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		DurableTail:  true,
		TailPadBytes: slowConsumerPadBytes,
	})
	pooled := orchestrationtest.StartPooledHost(t, ctx, world, "orchestrationtest-slowconsumer-host", 4)
	orchestrationtest.AwaitAdvertised(t, world, pooled.ID)
	replica := func(name string, budget int) *orchestrationtest.PooledFactory {
		return orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
			Replica:                 name,
			PerConnectionQueueBytes: budget,
			PingInterval:            2 * time.Minute,
			PongTimeout:             time.Minute,
			WriteTimeout:            time.Minute,
		})
	}
	tight := replica("orchestrationtest-slowconsumer-tight", slowConsumerTightBudget)
	loose := replica("orchestrationtest-slowconsumer-loose", slowConsumerLooseBudget)

	const tenant, otherTenant = orchestrationtest.PooledTenantA, orchestrationtest.PooledTenantB
	const session, otherSession = sessionwire.SessionID("session-slow"), sessionwire.SessionID("session-slow-other")
	create := func(tn sessionwire.TenantID, s sessionwire.SessionID) {
		command := "command-slow-create-" + string(s)
		status, body := tight.Post(t, ctx, tn, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(command),
			SessionID:       s,
			AgentID:         orchestrationtest.PooledAgent,
			Blocks:          json.RawMessage(`[{"type":"text","text":"hello"}]`),
		})
		if status != http.StatusCreated {
			t.Fatalf("the create of %s answered %d: %s", s, status, body)
		}
		orchestrationtest.PooledWait(t, "the create of "+string(s)+" applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tn, s, sessionwire.CommandID(command)) == sessionstore.InboxStateApplied
		})
	}
	create(tenant, session)
	create(otherTenant, otherSession)

	watch := func(f *orchestrationtest.PooledFactory, tn sessionwire.TenantID, s sessionwire.SessionID, options orchestrationtest.PooledBrowserOptions) *orchestrationtest.PooledBrowser {
		b := orchestrationtest.OpenPooledBrowser(t, ctx, f, tn, options)
		if err := b.Watch(t, ctx, s, 0); err != nil {
			t.Fatalf("a browser of %s was refused: %v", s, err)
		}
		b.Settle(t, ctx)
		return b
	}
	stalled := orchestrationtest.PooledBrowserOptions{Stallable: true, ReadBufferBytes: slowConsumerReadBuffer}
	slowTight := watch(tight, tenant, session, stalled)
	slowLoose := watch(loose, tenant, session, stalled)
	peer := watch(tight, tenant, session, orchestrationtest.PooledBrowserOptions{})
	other := watch(tight, otherTenant, otherSession, orchestrationtest.PooledBrowserOptions{})

	holdsThrough := func(b *orchestrationtest.PooledBrowser, tn sessionwire.TenantID, s sessionwire.SessionID) func() bool {
		return func() bool {
			all := world.Tails.Committed(tn, s)
			_, position := b.Settle(t, ctx)
			return len(all) > 0 && position >= all[len(all)-1].JournalSeq
		}
	}
	// Every browser holds the create before the stall, so what follows is
	// measured from a quiet, fully delivered start.
	for _, b := range []*orchestrationtest.PooledBrowser{slowTight, slowLoose, peer} {
		orchestrationtest.PooledWait(t, "a browser holds the create", 60*time.Second, holdsThrough(b, tenant, session))
	}
	orchestrationtest.PooledWait(t, "the other session's browser holds its create", 60*time.Second, holdsThrough(other, otherTenant, otherSession))
	before := len(world.Tails.Committed(tenant, session))

	// THE STALL, and the load. It is paced by the FAST peer, so the product's
	// own subscriber channel never overflows and every record reaches Factory:
	// whatever the slow client loses, it loses at Factory's edge.
	slowTight.Stall()
	slowLoose.Stall()
	for sent := 0; sent < slowConsumerLoad; sent += slowConsumerBatch {
		for range slowConsumerBatch {
			world.Tails.Commit(tenant, session)
		}
		// The other tenant's session keeps committing too, a little.
		world.Tails.Commit(otherTenant, otherSession)
		orchestrationtest.PooledWait(t, "the fast peer kept up with the load", 60*time.Second, holdsThrough(peer, tenant, session))
	}
	orchestrationtest.PooledWait(t, "the other session's browser kept up", 60*time.Second, holdsThrough(other, otherTenant, otherSession))
	loaded := world.Tails.Committed(tenant, session)
	if got := len(loaded) - before; got != slowConsumerLoad {
		t.Fatalf("the product committed %d of the %d-record load (failures %v)", got, slowConsumerLoad, world.Tails.DurableFailures())
	}

	// RELEASE BEFORE THE VERDICT. A server close issued from the publish path
	// cannot unwind while its write is blocked on a socket nobody drains, so
	// the outcome is read only once both slow clients read again.
	slowTight.Release()
	slowLoose.Release()
	orchestrationtest.PooledWait(t, "the slow client on the TIGHT replica repaired through the load", 90*time.Second, holdsThrough(slowTight, tenant, session))
	orchestrationtest.PooledWait(t, "the slow client on the LOOSE replica received the load", 90*time.Second, holdsThrough(slowLoose, tenant, session))

	closes := func(b *orchestrationtest.PooledBrowser) []orchestrationtest.PooledBrowserEntry {
		var out []orchestrationtest.PooledBrowserEntry
		for _, entry := range b.Log() {
			switch entry.Kind {
			case "connecting", "disconnected", "subscribing", "unsubscribed", "R":
				// The first connect and subscribe carry code 0 ("called"), and
				// are the client's own doing.
				if entry.Code != 0 || entry.Kind == "R" {
					out = append(out, entry)
				}
			}
		}
		return out
	}
	tightCloses := closes(slowTight)
	t.Logf("case 3 MEASURED: tight replica (%d B budget): dials=%d closes=%v repairs=%d",
		slowConsumerTightBudget, slowTight.Dials(), tightCloses, len(slowTight.Repairs()))
	t.Logf("case 3 MEASURED: loose replica (%d B budget): dials=%d closes=%v repairs=%d",
		slowConsumerLooseBudget, slowLoose.Dials(), closes(slowLoose), len(slowLoose.Repairs()))

	t.Run("the configured threshold closes the slow client's LINK with DisconnectSlow", func(t *testing.T) {
		slow := false
		for _, entry := range tightCloses {
			if entry.Kind == "connecting" && entry.Code == 3008 {
				slow = true
			}
		}
		if !slow {
			t.Fatalf("the slow client on the tight replica saw %v, want a transport close carrying DisconnectSlow (3008)", tightCloses)
		}
		// THE PHYSICAL LINK, not a subscription: the client had to dial again.
		if slowTight.Dials() < 2 {
			t.Fatalf("the slow client dialed %d time(s): the close did not take its link", slowTight.Dials())
		}
	})

	t.Run("below the threshold the same stall is never closed", func(t *testing.T) {
		if got := closes(slowLoose); len(got) != 0 || slowLoose.Dials() != 1 {
			t.Fatalf("the slow client on the loose replica saw %v over %d dial(s), want no close at all", got, slowLoose.Dials())
		}
		// Everything live: the only repair is the join read.
		assertOnlyJoinRepair(t, "the loose replica's slow client", slowLoose)
		assertHoldsAll(t, "the loose replica's slow client", slowLoose, world.Tails.Committed(tenant, session))
	})

	t.Run("the slow client repairs on reconnect with no record lost", func(t *testing.T) {
		assertHoldsAll(t, "the tight replica's slow client", slowTight, world.Tails.Committed(tenant, session))
		// The repair is what recovered the tail of the load: at least one
		// journal read after the join, and it carried records.
		repairs := slowTight.Repairs()
		recovered := 0
		for _, repair := range repairs[1:] {
			recovered += len(repair.Events)
		}
		if len(repairs) < 2 || recovered == 0 {
			t.Fatalf("the slow client's repairs are %d reads recovering %d records, want a durable repair after the reconnect", len(repairs), recovered)
		}
		t.Logf("case 3: the slow client recovered %d records from the journal over %d repair(s)", recovered, len(repairs)-1)
	})

	t.Run("the peer on the same session and replica, and another session, are untouched", func(t *testing.T) {
		for _, c := range []struct {
			who     string
			b       *orchestrationtest.PooledBrowser
			tn      sessionwire.TenantID
			session sessionwire.SessionID
		}{
			{"the fast peer of the same session", peer, tenant, session},
			{"the viewer of another tenant's session", other, otherTenant, otherSession},
		} {
			if got := closes(c.b); len(got) != 0 {
				t.Fatalf("%s saw %v: the slow client's close reached it", c.who, got)
			}
			assertOnlyJoinRepair(t, c.who, c.b)
			assertHoldsAll(t, c.who, c.b, world.Tails.Committed(c.tn, c.session))
		}
	})

	if failures := world.Tails.DurableFailures(); len(failures) != 0 {
		t.Fatalf("the product failed to commit durably: %v", failures)
	}
}

// assertHoldsAll requires a browser to hold exactly every committed event, in
// order, once each, and to have received no sequence twice live.
func assertHoldsAll(t *testing.T, who string, b *orchestrationtest.PooledBrowser, want []orchestrationtest.PooledCommitted) {
	t.Helper()
	got, _ := b.Settle(t, context.Background())
	if len(got) != len(want) {
		t.Fatalf("%s holds %d events, want exactly the %d committed", who, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s holds %v at position %d, want %v", who, got[i], i, want[i])
		}
	}
	if duplicates := b.LiveDuplicates(); len(duplicates) != 0 {
		t.Fatalf("%s received sequences live more than once: %v", who, duplicates)
	}
}

// assertOnlyJoinRepair requires a browser to have read the journal once, at
// its join, and to have received everything after that live.
func assertOnlyJoinRepair(t *testing.T, who string, b *orchestrationtest.PooledBrowser) {
	t.Helper()
	if repairs := b.Repairs(); len(repairs) != 1 {
		t.Fatalf("%s made %d journal reads, want only its join: %+v", who, len(repairs), repairs)
	}
}
