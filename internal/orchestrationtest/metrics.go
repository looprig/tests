//go:build integration && orchestration

package orchestrationtest

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// LeakSources names every resource runbook 07 I0.1 step 5 requires a leak
// assertion over. Every field is REQUIRED, including the two that acknowledge
// something is not observable, and AssertNoLeaks refuses a zero value for them.
//
// That is the design point. "Assert leaks for goroutines, open links, claims,
// leases, runtime instances, and temporary listeners" is six obligations, and a
// helper that quietly checks the four it was handed reports a green that means
// "four of six" while reading as "no leaks". Forcing the caller to say
// LinksUnobservable: true makes the missing check a written claim that someone
// has to defend, and makes it greppable the day it becomes false.
type LeakSources struct {
	Store *StoreFixture
	Host  *HostFixture

	// Factories is every Factory the case started. It is a SLICE rather than
	// one server because the goroutine check below is only meaningful once
	// every server has stopped: a still-serving Factory is not a leak, so
	// counting it as one makes the assertion fire for the wrong reason, and
	// exempting it by raising the slack makes it stop firing for the right
	// one.
	Factories []*FactoryFixture

	// LinksUnobservable must be true today. Factory composes NO link plane:
	// factory.New never constructs a clientlink handler or a hostlink pool, so
	// there is no link count to read and no link that could leak. Set it false
	// and supply a real count the moment Factory composes one.
	LinksUnobservable bool

	// BaselineGoroutines is the count captured before the case ran.
	BaselineGoroutines int
}

// CaptureGoroutines is the baseline half of the goroutine leak assertion.
func CaptureGoroutines() int { return runtime.NumGoroutine() }

// AssertNoLeaks checks every resource class the runbook names.
func AssertNoLeaks(tb TB, ctx context.Context, sources LeakSources) {
	tb.Helper()
	if !sources.LinksUnobservable {
		tb.Fatalf("orchestrationtest: LeakSources.LinksUnobservable is false but no link count was supplied; " +
			"either Factory now composes a link plane (assert it) or this is an unchecked leak class")
		return
	}
	if sources.BaselineGoroutines == 0 {
		tb.Fatalf("orchestrationtest: LeakSources.BaselineGoroutines is zero; capture it with CaptureGoroutines before the case")
		return
	}

	// Stop every server FIRST, then let goroutines settle. The order is the
	// assertion: goroutines counted while a server is still accepting are a
	// measurement of the server, not of a leak.
	for _, served := range sources.Factories {
		served.Stop(tb)
		assertListenerClosed(tb, served.Listener.Addr().String())
	}

	// Goroutines settle asynchronously; poll rather than sleep a fixed amount.
	deadline := time.Now().Add(10 * time.Second)
	for {
		settled := runtime.NumGoroutine()
		if settled <= sources.BaselineGoroutines+goroutineSlack {
			break
		}
		if time.Now().After(deadline) {
			tb.Errorf("orchestrationtest: goroutines did not settle: %d now, %d at baseline", settled, sources.BaselineGoroutines)
			break
		}
		runtime.Gosched()
	}

	if sources.Store != nil {
		assertNoResidualClaimsOrLeases(tb, ctx, sources.Store)
	}
	if sources.Host != nil {
		launches := sources.Host.Rig.Launches()
		if runtime, ok := sources.Host.Rig.Session.(*FakeRuntime); ok && launches > 0 && runtime.Released() < launches {
			tb.Errorf("orchestrationtest: %d runtime launches and only %d residency releases", launches, runtime.Released())
		}
	}
}

// goroutineSlack tolerates the runtime's own background goroutines and the
// http transport's idle-connection reaper, which is not a leak and is not
// synchronously stoppable.
const goroutineSlack = 4

func assertListenerClosed(tb TB, addr string) {
	tb.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err == nil {
		_ = conn.Close()
		tb.Errorf("orchestrationtest: %s still accepts connections after Stop", addr)
	}
}

func assertNoResidualClaimsOrLeases(tb TB, ctx context.Context, store *StoreFixture) {
	tb.Helper()
	page, err := store.Store.ListSessions(ctx, sessionstore.ListSessionsRequest{TenantID: store.Tenant})
	if err != nil {
		tb.Errorf("orchestrationtest: listing sessions for leak check: %v", err)
		return
	}
	for _, summary := range page.Sessions {
		entry, err := store.Store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{
			TenantID:  store.Tenant,
			SessionID: summary.SessionID,
		})
		if err != nil {
			continue // no registration is the expected shape
		}
		if entry.Registration != (sessionstore.HostRegistration{}) {
			tb.Errorf("orchestrationtest: session %q still holds a host registration after the case", summary.SessionID)
		}
	}
}

// forbiddenInPublicFrames is runbook 07 I0.2 step 3's list, made concrete.
var forbiddenInPublicFrames = []string{
	"centrifuge",          // a Centrifuge-Go type name
	"Centrifuge",          //
	"runtime_command_id",  // a Harness private correlation id
	"raw_runtime_payload", // a Harness private event body
	"gate_answer",         // a raw gate answer
	"signed_url",          // a backend object key in URL form
	"secret",              //
	KitActorCredential,    // a credential
	KitHostCredential,     //
}

// AssertPublicFrameIsClean fails if a frame served to a browser carries any of
// the things a public frame may never carry.
//
// The match is case-sensitive and substring-based on purpose. A structural
// check (decode, then inspect known fields) can only find leaks in fields it
// already knows about, and the leak worth catching is the one in a field nobody
// thought of -- which is exactly the field a structural check skips.
func AssertPublicFrameIsClean(tb TB, what string, frame []byte) {
	tb.Helper()
	for _, forbidden := range forbiddenInPublicFrames {
		if bytes.Contains(frame, []byte(forbidden)) {
			tb.Errorf("orchestrationtest: public frame %s carries %q", what, forbidden)
		}
	}
}

// NotComposedRoutes are the Factory routes that answer "not composed" in this
// build, with the status each answers.
//
// This map holds exactly ONE route, /v1/realtime, and the comment says so
// because an earlier version of it described the control routes too and the map
// never contained them.
//
// /v1/realtime is the ClientLink WebSocket endpoint and it answers 501:
// factory.New composes no clientlink handler. Be precise about the import
// claim: no non-test file in factory imports internal/realtime/clientlink or
// internal/realtime/hostlink -- but factory/internal/routing/repair.go:11 DOES
// import internal/realtime/delivery, so "nothing imports realtime" is false and
// must not be written down again.
//
// The control routes (/v1/sessions/{id}/input, /interrupt, /restore, and the
// gate response) separately answer 503 on factory.New's nil admission service,
// and the object routes answer 503 on its absent object store. They are NOT in
// this map: they are POST-only, so the kit's GET helper would read 405 from
// them and the row would pass for the wrong reason. They are booked as owed
// instead.
//
// So the kit's "bounded ClientLink client" (runbook 07 I0.1 step 2) and the
// whole of task I0.2 are blocked on a Factory composition change, not on test
// work. AssertFactoryComposesNoLinkPlane fails the day this route starts
// answering something else -- which is the day that blocker lifts.
var NotComposedRoutes = map[string]int{
	"/v1/realtime": http.StatusNotImplemented,
}

// AssertFactoryComposesNoLinkPlane records the blocker in executable form.
func AssertFactoryComposesNoLinkPlane(tb TB, ctx context.Context, f *FactoryFixture) {
	tb.Helper()
	for path, want := range NotComposedRoutes {
		status, body := f.Get(tb, ctx, path)
		if status != want {
			tb.Fatalf("orchestrationtest: %s answered %d, want %d. Factory has composed its link plane, "+
				"so runbook 07 I0.2 is no longer blocked: drive ClientLink for real and delete this trip-wire (body %s)",
				path, status, want, truncate(body))
			return
		}
	}
}

func truncate(body []byte) string {
	const limit = 200
	text := strings.TrimSpace(string(body))
	if len(text) > limit {
		return text[:limit] + "..."
	}
	return text
}

// SessionPath builds a served read path for one session.
func SessionPath(session sessionwire.SessionID, suffix string) string {
	return "/v1/sessions/" + string(session) + suffix
}
