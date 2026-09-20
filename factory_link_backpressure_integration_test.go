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
//	        lane is held to.
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
//	        closes a subscription or the physical link. NOT MEASURED. It needs
//	        the buffer to actually overflow, which is case 1's unreachable
//	        window, and the runbook is explicit that an assumed library
//	        behaviour must not be encoded. Owed, and deliberately not guessed.
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
