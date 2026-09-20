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
//	case 1  a client sharing a HostBinding with a failed one continues without
//	        loss. DRIVEN, with the failure APPROXIMATED as a client that dies
//	        abruptly: a DeliveryBinding's egress overflow is internal to
//	        Factory and nothing outside it can fill that buffer on purpose
//	        without also being a test of this module's timing.
//	case 2  a HostBinding failure repairs every local DeliveryBinding
//	        independently. DRIVEN by severing the HostLink at TCP -- the one
//	        HostBinding failure a peer CAN inflict -- with two tenants and two
//	        viewers each, so "independently" has more than one subject.
//	        The second half of case 2, "while a session on another Host
//	        continues", is NOT MEASURED: pooled placement chooses the Host, and
//	        this module cannot pin one session to host-a and another to host-b
//	        without reaching inside Factory's placement. Owed.
//	case 3  the SELECTED Centrifuge slow-consumer threshold, and whether it
//	        closes a subscription or the physical link. NOT MEASURED. It needs
//	        the buffer to actually overflow, which is case 1's unreachable
//	        window, and the runbook is explicit that an assumed library
//	        behaviour must not be encoded. Owed, and deliberately not guessed.
//	case 4  enduring frames are never oldest-dropped. DRIVEN, and it is the
//	        same rule every case in this lane rests on: a dropped enduring
//	        frame is a SILENT GAP, and PooledCoveredThrough refuses one. A
//	        session.reset is not a drop -- it is how a client is TOLD to
//	        re-read -- so the rule counts coverage, not records.

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

	t.Run("case 1: a client that dies takes none of its peer's stream with it", func(t *testing.T) {
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

		want := map[sessionwire.TenantID]uint64{
			orchestrationtest.PooledTenantA: 3 * perInput,
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
