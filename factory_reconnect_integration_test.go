//go:build integration

// This file is runbook 07 I1.1's cases 3 and 4, and it reports them as STILL
// BLOCKED with a different blocker than before and with the evidence measured
// rather than inferred.
//
// # What changed
//
// Before A9.1 stage 2, `/v1/realtime` was a `pending` route answering 501 and
// `factory.New` composed no ClientLink handler at all. That is over: the route
// has left the not-implemented table, and the first row below is the positive
// that proves it.
//
// # What cases 3 and 4 now wait on -- TWO things, and only one was expected
//
// 1. A HOST PUBLISHER. Both cases are about DELIVERY: commit three enduring
//    events and observe all three exactly once and in order after a reconnect
//    with the old cursor; kill Factory A after it buffered and repair from
//    journal sequence on B. A session channel's records reach a DeliveryBinding
//    from the Host live tail -- `routing.Tail` is documented as "the Host live
//    tail's control surface for one session" -- which arrives over HostLink from
//    a running Host. `host v0.1.0` exposes `New` plus accessors and nothing
//    runnable, so no Host can bind and no record is ever published. This is the
//    blocker the first pass predicted would remain.
//
// 2. A WORKING UPGRADE IN THIS COMPOSITION, WHICH I DID NOT GET. A real
//    `centrifuge-go` client against a real loopback listener fails with
//    `websocket: bad handshake`, and a raw upgrade probe -- correct
//    `Connection`, `Upgrade`, `Sec-WebSocket-Version`, `Sec-WebSocket-Key`,
//    `Origin` and bearer credential -- is answered **HTTP 500 "Internal Server
//    Error"**, not 101 and not the 503 Factory's own `Start` documentation says
//    the route answers until the node boots.
//
//    I am deliberately NOT calling that a Factory defect. It is equally
//    consistent with a seam this kit composes wrongly, and the router recovers a
//    handler panic into an indistinguishable 500 (`recoverPanic`), so the status
//    alone cannot tell the two apart from outside. What I can say is measured:
//    in the composition this kit builds, the upgrade does not complete, so no
//    ClientLink can be driven from here yet. Resolving it needs Factory-side
//    visibility this module does not have.
//
// The row below pins the measured answer so that the day it changes -- to 101,
// or to the documented 503 -- this file fails and names what to do next. That is
// the only honest shape available: an unverified absence claim is as wide as an
// unverified presence claim, and "cases 3 and 4 are blocked" is an absence
// claim.

package tests

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
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

	t.Run("the upgrade does not complete in this composition", func(t *testing.T) {
		// MEASURED, and pinned so it fails when it changes.
		//
		// 101 means the link is drivable and cases 3 and 4 move on to their
		// real blocker. 503 means the node has not booted, which is what
		// Factory's Start documentation describes and would point at a
		// composition or ordering problem with a named cause. 500 is what this
		// build actually answers and is the least informative of the three,
		// because the router recovers a handler panic into exactly that.
		status, body := upgradeProbe(t, served, "http://"+served.Listener.Addr().String())
		if status != http.StatusInternalServerError {
			t.Fatalf("the ClientLink upgrade answered %d (%s), not the 500 this composition measured. "+
				"If it is 101 the link is drivable: write I1.1 cases 3 and 4 against it and delete this row. "+
				"If it is 503 the node did not boot and Start's own documentation names the cause",
				status, body)
		}
	})

	t.Run("cases 3 and 4 also wait on a Host publisher", func(t *testing.T) {
		// The second blocker, stated independently of the first so that fixing
		// the upgrade does not silently look like unblocking the cases.
		//
		// The three enduring events are committed for real. The journal holds
		// them in order, which is the half this module can prove; what nothing
		// can do today is observe them arriving at a subscriber, because the
		// only publisher into a session channel is a Host live tail and no Host
		// can run.
		orchestrationtest.AssertHostExposesNoRuntimeCapability(t)

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

	orchestrationtest.AssertNoLeaks(t, ctx, orchestrationtest.LeakSources{
		Store:              store,
		Factories:          []*orchestrationtest.FactoryFixture{served},
		LinksUnobservable:  true,
		BaselineGoroutines: baseline,
	})
}
