//go:build integration

// This file is runbook 07 I1.1's cases 3 and 4, and it reports them as STILL
// BLOCKED -- on a third blocker, measured on the wire rather than inferred.
//
// # How the blocker has moved
//
//  1. Before A9.1 stage 2, `/v1/realtime` was a `pending` route answering 501.
//  2. At factory v0.1.0 the route was served but, in this kit's composition, the
//     upgrade answered HTTP 500 -- pinned here so it would fail when it changed.
//     It CHANGED on the factory v0.2.0 pin: the upgrade answers 101, and the
//     row now asserts that instead.
//  3. The other half was a HOST PUBLISHER, blocked because host v0.1.0 exposed
//     nothing runnable. host v0.2.1 closed that (B1): a composed Host relays a
//     resident runtime's committed tail to a HostLink subscriber of the session
//     channel, which host's own composed round-trip test drives.
//
// # What cases 3 and 4 wait on now: FACTORY RELAYING THE TAIL
//
// A publication reaches a browser only if Factory subscribes to the session
// channel on HostLink and fans what it receives out to its ClientLink viewers.
// factory v0.2.0 does neither: it never sends a subscribe on HostLink and
// constructs no routing.Relay (the type exists; nothing calls NewRelay). The
// last row drives a real Factory against a real, bound Host and pins that on the
// wire, so it fails the day Factory starts relaying.

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
