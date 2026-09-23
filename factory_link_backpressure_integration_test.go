//go:build integration

// This file is runbook 07 task I1.4: the BLAST RADIUS of a realtime link
// failure. It was blocked behind the same trip-wire as I1.1 cases 3 and 4 --
// Factory relayed no Host tail, so there was no stream for a failure to damage
// -- and factory v0.5.0 lifted it.
//
// # What it can and cannot measure, said plainly
//
// The runbook asks for four things. Two are drivable from outside Factory and
// are driven here; two are not, and are recorded rather than faked.
//
//	case 1  "the slow client receives reset/closes and REPAIRS; the peer
//	        continues without loss." BOTH HALVES DRIVEN, with the failure
//	        APPROXIMATED as a client that dies abruptly rather than one that
//	        overflows: a DeliveryBinding's egress buffer is internal to Factory
//	        and nothing outside it can fill that buffer on purpose without also
//	        being a test of this module's timing. The repair half reconnects the
//	        dead client and holds it to the same rule every reconnect in this
//	        lane is held to. A GENUINE overflow -- a consumer that stops
//	        reading and is closed by the queue budget -- is now driven by
//	        case 3 below, with the same two halves asserted.
//	case 2  "a HostBinding failure repairs every local DeliveryBinding
//	        independently WHILE A SESSION ON ANOTHER HOST CONTINUES."
//	        FIRST HALF DRIVEN by severing the HostLink at TCP -- the one
//	        HostBinding failure a peer CAN inflict, and a TRANSPORT failure
//	        rather than a queue failure, which is worth saying -- with two
//	        tenants and three live viewers, so "independently" has more than one
//	        subject.
//	        SECOND HALF OWED, AND REACHABLE: it is not blocked on anything.
//	        Pooled placement keys candidates on (AgentID, RuntimeCompatibilityID,
//	        Placement), so two Hosts registering DISJOINT compatibilities under
//	        two agents put two sessions on two Hosts with no Factory seam and no
//	        placement override. What it costs is a per-Host agent and
//	        compatibility in the pooled kit, which StartPooledHost does not carry
//	        yet. Not done here; do not read its absence as impossible.
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
//	case 4  "enduring frames are never oldest-dropped AND ephemeral coalescing
//	        remains bounded."
//	        FIRST CLAUSE DRIVEN, and it is the rule every case in this lane
//	        rests on: a dropped enduring frame is a SILENT GAP, and
//	        PooledCoveredThrough refuses one. A session.reset is not a drop --
//	        it is how a client is TOLD to re-read -- so the rule counts
//	        coverage, not records.
//	        SECOND CLAUSE OWED, and not measured anywhere in this module. The
//	        kit's product runtime commits only ENDURING publications: it has no
//	        ephemeral stream at all, so there is nothing here for coalescing to
//	        bound. Measuring it needs an ephemeral producer in the pooled
//	        runtime (token deltas and tool lifecycle are harness's ephemeral
//	        class) and an assertion on what survives the coalescer. Owed.

package tests

import (
	"context"
	"encoding/json"
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
func TestRealtimeFailureBlastRadiusIsBoundedByTheSession(t *testing.T) {
	ctx := placementContext(t)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{})
	pooled := orchestrationtest.StartPooledHost(t, ctx, world, "orchestrationtest-backpressure-host", 4)
	orchestrationtest.AwaitAdvertised(t, world, pooled.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "orchestrationtest-backpressure-replica", nil)

	tenants := []sessionwire.TenantID{orchestrationtest.PooledTenantA, orchestrationtest.PooledTenantB}
	sessions := map[sessionwire.TenantID]sessionwire.SessionID{
		orchestrationtest.PooledTenantA: "session-backpressure-a",
		orchestrationtest.PooledTenantB: "session-backpressure-b",
	}
	const perInput = orchestrationtest.PooledPublicationsPerInput

	// TWO VIEWERS PER SESSION. One DeliveryBinding each, both behind the one
	// HostBinding their session has: that is the sharing case 1 and case 2 are
	// about, and one viewer per session could not express either.
	primary := map[sessionwire.TenantID]*orchestrationtest.PooledViewer{}
	peer := map[sessionwire.TenantID]*orchestrationtest.PooledViewer{}
	for _, tenant := range tenants {
		for _, into := range []map[sessionwire.TenantID]*orchestrationtest.PooledViewer{primary, peer} {
			viewer := orchestrationtest.ConnectPooledViewer(t, ctx, served, tenant)
			if err := viewer.Watch(t, ctx, tenant, sessions[tenant]); err != nil {
				t.Fatalf("%s's viewer was refused its own session: %v", tenant, err)
			}
			into[tenant] = viewer
		}
	}

	for _, tenant := range tenants {
		status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("command-backpressure-create-" + string(tenant)),
			SessionID:       sessions[tenant],
			AgentID:         orchestrationtest.PooledAgent,
			Blocks:          json.RawMessage(`[{"type":"text","text":"hello"}]`),
		})
		if status != http.StatusCreated {
			t.Fatalf("the create for %s answered %d: %s", tenant, status, body)
		}
	}
	for _, tenant := range tenants {
		orchestrationtest.PooledWait(t, "the create of "+string(tenant)+" applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, sessions[tenant],
				sessionwire.CommandID("command-backpressure-create-"+string(tenant))) == sessionstore.InboxStateApplied
		})
		for _, viewer := range []*orchestrationtest.PooledViewer{primary[tenant], peer[tenant]} {
			orchestrationtest.PooledWait(t, string(tenant)+"'s viewers are covered through the create", 60*time.Second, func() bool {
				covered, err := orchestrationtest.PooledCoveredThrough(viewer.Records())
				return err == nil && covered >= perInput
			})
		}
	}

	input := func(t *testing.T, tenant sessionwire.TenantID, command string) {
		t.Helper()
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(sessions[tenant])+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(command),
			SessionID:       sessions[tenant],
			Blocks:          json.RawMessage(`[{"type":"text","text":"more"}]`),
		})
		if status != http.StatusOK {
			t.Fatalf("the input %q for %s answered %d: %s", command, tenant, status, body)
		}
		orchestrationtest.PooledWait(t, "the input "+command+" applied", 60*time.Second, func() bool {
			return world.CommandState(ctx, tenant, sessions[tenant], sessionwire.CommandID(command)) == sessionstore.InboxStateApplied
		})
	}

	t.Run("case 1: a client that dies takes none of its peer's stream with it, and repairs", func(t *testing.T) {
		// THE APPROXIMATION, again stated where it is made: a DeliveryBinding
		// that overflows and one whose socket dies are two different failures,
		// and only the second can be caused from outside. What both share, and
		// what the case is actually about, is that the binding's failure must
		// not reach the peer behind the same HostBinding.
		dying := primary[orchestrationtest.PooledTenantA]
		survivor := peer[orchestrationtest.PooledTenantA]
		before := len(survivor.Records())
		dying.Close()

		input(t, orchestrationtest.PooledTenantA, "command-backpressure-peer")
		orchestrationtest.PooledWait(t, "the surviving peer received the continued stream", 60*time.Second, func() bool {
			covered, err := orchestrationtest.PooledCoveredThrough(survivor.Records())
			return err == nil && covered >= 2*perInput
		})
		after := survivor.Records()
		t.Logf("case 1: the surviving peer: %v", after)

		// WITHOUT LOSS, which here means without a silent gap AND without a
		// reset: the peer was never disturbed, so it should not have been told
		// to re-read anything.
		covered, err := orchestrationtest.PooledCoveredThrough(after)
		if err != nil || covered < 2*perInput {
			t.Fatalf("the peer's stream %v covers through %d (%v), want %d with no silent gap", after, covered, err, 2*perInput)
		}
		for _, record := range after[before:] {
			if strings.HasPrefix(record, "R") {
				t.Fatalf("the peer was reset by its neighbour's failure: %v", after[before:])
			}
		}
		if strays := survivor.Strays(); len(strays) != 0 {
			t.Fatalf("the peer received records naming another session: %v", strays)
		}

		// THE OTHER HALF OF THE ROW: "the slow client ... repairs". The client
		// that died comes back, learns where it is from the journal -- a
		// subscribe to a quiet session delivers nothing, see
		// reconnectCapturedTip -- and its live tail continues from there, held
		// to the same rule.
		reborn := orchestrationtest.ConnectPooledViewer(t, ctx, served, orchestrationtest.PooledTenantA)
		if err := reborn.Watch(t, ctx, orchestrationtest.PooledTenantA, sessions[orchestrationtest.PooledTenantA]); err != nil {
			t.Fatalf("the repaired client's subscribe was refused: %v", err)
		}
		resumed := reconnectCapturedTip(t, ctx, served, sessions[orchestrationtest.PooledTenantA])
		if resumed < 2*perInput {
			t.Fatalf("the repaired client resumed at tip %d, want at least %d", resumed, 2*perInput)
		}
		input(t, orchestrationtest.PooledTenantA, "command-backpressure-repaired")
		orchestrationtest.PooledWait(t, "the repaired client received the continued stream", 60*time.Second, func() bool {
			covered, err := orchestrationtest.PooledCoveredThroughFrom(reborn.Records(), resumed)
			return err == nil && covered >= 3*perInput
		})
		t.Logf("case 1: the repaired client resumed at %d and received %v", resumed, reborn.Records())
		covered, err = orchestrationtest.PooledCoveredThroughFrom(reborn.Records(), resumed)
		if err != nil || covered < 3*perInput {
			t.Fatalf("the repaired client's stream %v covers through %d (%v) from %d, want %d with no silent gap",
				reborn.Records(), covered, err, resumed, 3*perInput)
		}
		if strays := reborn.Strays(); len(strays) != 0 {
			t.Fatalf("the repaired client received records naming another session: %v", strays)
		}
		// THE REPAIRED CLIENT IS NOT CARRIED INTO CASE 2, and the reason is the
		// fixture rather than the product: ConnectPooledViewer registers its
		// close on the `t` it was given, so this connection is closed when this
		// subtest returns. Carrying it further would assert about a socket the
		// harness had already shut -- measured, as a viewer that received
		// [E7 E8 E9] and then nothing across the sever while its peer on the
		// same session repaired normally.
	})

	t.Run("case 2: a HostBinding failure repairs every binding independently", func(t *testing.T) {
		// The one HostBinding failure a peer can inflict: the transport under
		// it, severed at TCP. httptest's CloseClientConnections cannot do this
		// -- it forgets a hijacked connection, and every WebSocket is hijacked.
		live := map[sessionwire.TenantID][]*orchestrationtest.PooledViewer{
			orchestrationtest.PooledTenantA: {peer[orchestrationtest.PooledTenantA]},
			orchestrationtest.PooledTenantB: {primary[orchestrationtest.PooledTenantB], peer[orchestrationtest.PooledTenantB]},
		}
		t.Logf("case 2: severed %d TCP connections to the Host", pooled.Sever())

		// tenant-a has had three inputs by now (the create, case 1's peer input
		// and case 1's repair input); tenant-b has had one.
		want := map[sessionwire.TenantID]uint64{
			orchestrationtest.PooledTenantA: 4 * perInput,
			orchestrationtest.PooledTenantB: 2 * perInput,
		}
		for _, tenant := range tenants {
			input(t, tenant, "command-backpressure-after-sever-"+string(tenant))
		}
		for _, tenant := range tenants {
			for i, viewer := range live[tenant] {
				orchestrationtest.PooledWait(t, "viewer "+string(rune('0'+i))+" of "+string(tenant)+" repaired", 90*time.Second, func() bool {
					covered, err := orchestrationtest.PooledCoveredThrough(viewer.Records())
					return err == nil && covered >= want[tenant]
				})
				t.Logf("case 2: %s viewer %d after the sever: %v", tenant, i, viewer.Records())
				covered, err := orchestrationtest.PooledCoveredThrough(viewer.Records())
				if err != nil || covered < want[tenant] {
					t.Fatalf("%s's viewer %d covers through %d (%v), want %d with no silent gap: %v",
						tenant, i, covered, err, want[tenant], viewer.Records())
				}
				// INDEPENDENTLY, and the isolation half of that word: a repair
				// must not deliver another session's records into this one.
				if strays := viewer.Strays(); len(strays) != 0 {
					t.Fatalf("%s's viewer %d received another session's records after the sever: %v", tenant, i, strays)
				}
			}
		}
	})

	t.Run("case 4: no enduring frame was ever oldest-dropped", func(t *testing.T) {
		// The whole run, re-read. An oldest-drop is a sequence that never
		// arrives and is never covered by a reset, which is exactly what
		// PooledCoveredThrough calls a silent gap; and a frame delivered twice
		// would be the other half of "never dropped" going wrong.
		for _, tenant := range tenants {
			for i, viewer := range []*orchestrationtest.PooledViewer{primary[tenant], peer[tenant]} {
				records := viewer.Records()
				if len(records) == 0 {
					continue
				}
				if _, err := orchestrationtest.PooledCoveredThrough(records); err != nil {
					t.Fatalf("%s's viewer %d has a silent gap: %v (%v)", tenant, i, err, records)
				}
				seen := map[string]int{}
				for _, record := range records {
					if strings.HasPrefix(record, "E") {
						seen[record]++
					}
				}
				for record, count := range seen {
					if count > 1 {
						t.Fatalf("%s's viewer %d received %s %d times: %v", tenant, i, record, count, records)
					}
				}
			}
		}
	})
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
			world.Tails.Hint(tenant, session)
		}
		// The other tenant's session keeps committing too, a little.
		world.Tails.Hint(otherTenant, otherSession)
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
