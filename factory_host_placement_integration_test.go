//go:build integration

// This file is the cross-module placement lane: a REAL released Factory
// (v0.5.0) placing sessions onto REAL released, composed Hosts (v0.4.0) whose
// runtimes are REAL harness rigs (v0.35.0) over REAL harness journals, with
// real browser-shaped ClientLink viewers watching the output. Every module here
// is the published one; nothing between them is a shim.
//
// # Why it lives in this module and nowhere else
//
// Each claim below spans at least three modules, and each was proven only once
// before, in a PRIVATE harness that was never committed: factory v0.5.0's proof
// (CODEX_RESULT_FACTORY_V050_LOGS/proof/zz_proof_test.go) and host v0.4.0's
// regate probes. A proof that exists only in a result document is a proof that
// stops running the next day. These are the same claims, carried here as
// permanent cases.
//
// # The one thing to know before editing
//
// Nothing here sleeps to make something happen; every wait polls a DURABLE or
// RECORDED condition through PooledWait, which fails by assertion naming what
// did not happen. Two sleeps remain and both are settling windows AFTER a
// condition was already waited for, each commented where it is.

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// placementContext bounds a whole cross-module case. These cases run real
// WebSocket transports, real placement sweeps and real agent turns, so the
// bound is minutes rather than seconds; every wait inside is bounded far
// tighter and fails first.
func placementContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// TestFactoryPlacesTwoTenantsOnOnePooledHost is the cross-module claim, whole:
//
//	P0  a Host advertises a BARE base, and nothing derived;
//	P1  two tenants each get a session PLACED on the SAME Host at the same
//	    time, over two per-tenant HostLinks Factory derived itself;
//	P2  each session's input is applied and its live output reaches only its
//	    own tenant's viewer;
//	P3  a viewer of one tenant is REFUSED the other tenant's session channel;
//	P4  a HostLink sever recovers BOTH tenants;
//	P5  a re-placed session is RESTORED, not restarted, on a second Host.
//
// The subtests are sequential and share one world on purpose: P4 is a claim
// about recovering a stream P2 established, and P5 is a claim about a
// conversation P1 started. Splitting them would replace those claims with five
// weaker ones.
func TestFactoryPlacesTwoTenantsOnOnePooledHost(t *testing.T) {
	ctx := placementContext(t)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{})

	hostA := orchestrationtest.StartPooledHost(t, ctx, world, "orchestrationtest-host-a", 4)
	advertised := orchestrationtest.AwaitAdvertised(t, world, "orchestrationtest-host-a")

	t.Run("P0: the Host advertises a bare base", func(t *testing.T) {
		if advertised.InternalEndpoint != hostA.Base {
			t.Fatalf("host-a advertised %q, want its own base %q", advertised.InternalEndpoint, hostA.Base)
		}
		// Not merely "equal to what we configured": it must be a value Core
		// will derive from. A base carrying a path is refused, and a Factory
		// would then skip this Host for every tenant.
		for _, tenant := range []sessionwire.TenantID{orchestrationtest.PooledTenantA, orchestrationtest.PooledTenantB} {
			derived, err := sessionwire.HostLinkEndpoint(advertised.InternalEndpoint, tenant)
			if err != nil {
				t.Fatalf("Core refused to derive %q's address from the advertised base %q: %v", tenant, advertised.InternalEndpoint, err)
			}
			if want := string(hostA.Base) + orchestrationtest.TenantPath(tenant); string(derived) != want {
				t.Fatalf("the derived address for %q is %q, want %q", tenant, derived, want)
			}
		}
	})

	served := orchestrationtest.StartPooledFactory(t, ctx, world, "orchestrationtest-replica-1", nil)

	sessions := map[sessionwire.TenantID]sessionwire.SessionID{
		orchestrationtest.PooledTenantA: "session-tenant-a",
		orchestrationtest.PooledTenantB: "session-tenant-b",
	}
	// One remembered word per tenant. They are what P5 reads out of a model
	// request: a restored session must still carry its OWN word and must not
	// carry the other tenant's.
	words := map[sessionwire.TenantID]string{
		orchestrationtest.PooledTenantA: "PERSIMMON",
		orchestrationtest.PooledTenantB: "QUINCE",
	}
	tenants := []sessionwire.TenantID{orchestrationtest.PooledTenantA, orchestrationtest.PooledTenantB}

	// The viewers subscribe BEFORE the sessions exist anywhere. A viewer that
	// subscribed after the first publication would be asserting about a replay
	// instead of a live tail.
	viewers := map[sessionwire.TenantID]*orchestrationtest.PooledViewer{}
	for _, tenant := range tenants {
		viewer := orchestrationtest.ConnectPooledViewer(t, ctx, served, tenant)
		if err := viewer.Watch(t, ctx, tenant, sessions[tenant]); err != nil {
			t.Fatalf("%s's own subscribe to its own session was refused: %v", tenant, err)
		}
		viewers[tenant] = viewer
	}

	var intruder *orchestrationtest.PooledViewer
	t.Run("P3: a cross-tenant viewer is refused", func(t *testing.T) {
		// The authorizer this harness composes PERMITS every subscribe, so a
		// refusal here can only be Factory's own channel routing. If the seam
		// refused, this case would be asserting that the kit refuses.
		intruder = orchestrationtest.ConnectPooledViewer(t, ctx, served, orchestrationtest.PooledTenantB)
		err := intruder.Watch(t, ctx, orchestrationtest.PooledTenantA, sessions[orchestrationtest.PooledTenantA])
		if err == nil {
			t.Fatalf("a %s viewer was allowed to subscribe to %s's session channel",
				orchestrationtest.PooledTenantB, orchestrationtest.PooledTenantA)
		}
		// RECORDED, not asserted: the refusal arrives as Factory's fault code
		// 100 ("internal server error"), not as a denial. That is factory
		// v0.5.0's documented ErrUnroutableChannel behaviour (its release
		// residue names it), and a client cannot tell it from a transient
		// server fault, so it will retry. The case asserts the SUBSCRIBE WAS
		// REFUSED and that nothing was delivered; pinning the code here would
		// pin a classification Factory itself calls residue.
		t.Logf("P3 the cross-tenant subscribe was refused: %v", err)
	})

	t.Run("P1: both tenants are placed on the same Host, over their own links", func(t *testing.T) {
		var wg sync.WaitGroup
		for _, tenant := range tenants {
			wg.Add(1)
			go func(tenant sessionwire.TenantID) {
				defer wg.Done()
				status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
					CommandEnvelope: orchestrationtest.PooledEnvelope("create-" + string(tenant)),
					SessionID:       sessions[tenant],
					AgentID:         orchestrationtest.PooledAgent,
					Blocks:          json.RawMessage(fmt.Sprintf(`[{"type":"text","text":"the remembered word is %s"}]`, words[tenant])),
				})
				if status != http.StatusCreated {
					t.Errorf("the create for %s answered %d: %s", tenant, status, body)
				}
			}(tenant)
		}
		wg.Wait()

		for _, tenant := range tenants {
			took := orchestrationtest.PooledWait(t, "the create of "+string(tenant)+" applied", 90*time.Second, func() bool {
				return world.CommandState(ctx, tenant, sessions[tenant], sessionwire.CommandID("create-"+string(tenant))) == sessionstore.InboxStateApplied
			})
			owner, found, err := served.Directory.Owner(ctx, tenant, sessions[tenant])
			if err != nil || !found {
				t.Fatalf("%s's session has no owner after its create applied: found=%v err=%v", tenant, found, err)
			}
			if owner.HostID != hostA.ID {
				t.Fatalf("%s's session is owned by %q, want the one pooled Host %q", tenant, owner.HostID, hostA.ID)
			}
			t.Logf("P1 %s/%s applied after %v, owned by %s generation %d",
				tenant, sessions[tenant], took.Round(time.Millisecond), owner.HostID, owner.HostGeneration)
		}

		// ONE Host, TWO tenants, at the same time. This is the claim Gap 1 was
		// about, and it is read off the Host's own rig rather than inferred
		// from the two owners above.
		creates := hostA.Rig.Creates()
		if len(creates) != 2 {
			t.Fatalf("host-a created %d runtime sessions, want one per tenant: %+v", len(creates), creates)
		}
		seen := map[sessionwire.TenantID]bool{}
		for _, launch := range creates {
			seen[launch.Tenant] = true
		}
		for _, tenant := range tenants {
			if !seen[tenant] {
				t.Fatalf("host-a launched nothing for %s: %+v", tenant, creates)
			}
		}

		// And every dial went to the DERIVED per-tenant path. A verbatim dial
		// of the base is the pre-v0.5.0 behaviour and answers 404, so it would
		// show as a missing path rather than as an error anywhere.
		for _, tenant := range tenants {
			if hostA.CountPath(orchestrationtest.TenantPath(tenant)) == 0 {
				t.Fatalf("Factory never dialled host-a's %s link at %s; it dialled %v",
					tenant, orchestrationtest.TenantPath(tenant), hostA.UpgradePaths())
			}
		}
		if verbatim := hostA.CountPath("/"); verbatim != 0 {
			t.Fatalf("Factory dialled host-a's base verbatim %d times; every dial must be derived", verbatim)
		}
	})

	// reached is the harness journal position each tenant's runtime last
	// published live, the target every coverage wait below is held to.
	reached := map[sessionwire.TenantID]uint64{}
	t.Run("P2: each tenant's live output reaches only its own viewer", func(t *testing.T) {
		for _, tenant := range tenants {
			// The create's turn has finished before the input is sent, so the
			// input's records are strictly above this.
			reached[tenant] = world.AwaitQuietTip(t, tenant, sessions[tenant], 0)
			status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(sessions[tenant])+"/input", sessionwire.InputRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope("input-1-" + string(tenant)),
				SessionID:       sessions[tenant],
				Blocks:          json.RawMessage(`[{"type":"text","text":"hello"}]`),
			})
			if status != http.StatusOK {
				t.Fatalf("the input for %s answered %d: %s", tenant, status, body)
			}
		}
		for _, tenant := range tenants {
			orchestrationtest.PooledWait(t, "the input of "+string(tenant)+" applied", 60*time.Second, func() bool {
				return world.CommandState(ctx, tenant, sessions[tenant], sessionwire.CommandID("input-1-"+string(tenant))) == sessionstore.InboxStateApplied
			})
			// Through the input's turn: the position the runtime reached in
			// its own journal, as the Host relayed it live.
			throughInput := world.AwaitQuietTip(t, tenant, sessions[tenant], reached[tenant])
			reached[tenant] = throughInput
			viewer := viewers[tenant]
			// COVERAGE, not a particular record. A client is covered through a
			// sequence either by receiving each publication or by a
			// session.reset naming that tip -- a reset is how it is TOLD to
			// read the journal, so both are correct and which one a repair
			// chooses is a timing detail. Waiting for a literal E6 would make
			// this case fail on a legitimate reset, which was measured: one
			// tenant's viewer was covered by [T0 ... R0/6] and the other's by
			// [T0 T0 T0 R0/3 E4 E5 E6] in the same run.
			orchestrationtest.PooledWait(t, string(tenant)+"'s viewer covered through "+fmt.Sprint(throughInput), 60*time.Second, func() bool {
				through, err := world.CoveredThrough(t, ctx, tenant, sessions[tenant], viewer.Records(), 0)
				return err == nil && through >= throughInput
			})
			t.Logf("P2 %s viewer: %v", tenant, viewer.Records())

			// And the absence of a SILENT gap, re-read once the wait is over.
			// See PooledCoveredThrough.
			through, err := world.CoveredThrough(t, ctx, tenant, sessions[tenant], viewer.Records(), 0)
			if err != nil || through < throughInput {
				t.Fatalf("%s's viewer stream %v covers through %d (%v), want %d with no silent gap",
					tenant, viewer.Records(), through, err, throughInput)
			}
			if strays := viewer.Strays(); len(strays) != 0 {
				t.Fatalf("%s's viewer received records naming another tenant or session: %v", tenant, strays)
			}
		}
		// The refused viewer received nothing at all. Without this, P3 would
		// prove only that the subscribe was answered with an error, not that
		// the channel stayed closed.
		if got := intruder.Records(); len(got) != 0 {
			t.Fatalf("the refused cross-tenant viewer still received %v", got)
		}
	})

	t.Run("P4: a HostLink sever recovers both tenants", func(t *testing.T) {
		before := map[sessionwire.TenantID]int{}
		dials := map[sessionwire.TenantID]int{}
		for _, tenant := range tenants {
			before[tenant] = len(viewers[tenant].Records())
			dials[tenant] = hostA.CountPath(orchestrationtest.TenantPath(tenant))
		}

		// At TCP, through the tracking listener. httptest's
		// CloseClientConnections forgets a hijacked connection, and every
		// WebSocket is hijacked, so it cannot sever a HostLink.
		t.Logf("P4 severed %d TCP connections to host-a", hostA.Sever())

		for _, tenant := range tenants {
			path := orchestrationtest.TenantPath(tenant)
			orchestrationtest.PooledWait(t, string(tenant)+"'s HostLink was re-dialled", 60*time.Second, func() bool {
				return hostA.CountPath(path) > dials[tenant]
			})
		}
		// A SETTLING WINDOW, not a wait for an event: the re-dial is observed
		// above, and this lets each repaired link re-bind and re-subscribe
		// before more output is produced. Publishing into a link that has
		// re-dialled but not yet re-subscribed would test the repair's timing
		// rather than its correctness.
		time.Sleep(2 * time.Second)

		for _, tenant := range tenants {
			status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(sessions[tenant])+"/input", sessionwire.InputRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope("input-2-" + string(tenant)),
				SessionID:       sessions[tenant],
				Blocks:          json.RawMessage(`[{"type":"text","text":"again"}]`),
			})
			if status != http.StatusOK {
				t.Fatalf("the post-sever input for %s answered %d: %s", tenant, status, body)
			}
		}

		for _, tenant := range tenants {
			orchestrationtest.PooledWait(t, "the post-sever input of "+string(tenant)+" applied", 90*time.Second, func() bool {
				return world.CommandState(ctx, tenant, sessions[tenant], sessionwire.CommandID("input-2-"+string(tenant))) == sessionstore.InboxStateApplied
			})
			throughSecondInput := world.AwaitQuietTip(t, tenant, sessions[tenant], reached[tenant])
			viewer := viewers[tenant]
			orchestrationtest.PooledWait(t, string(tenant)+"'s viewer covered through "+fmt.Sprint(throughSecondInput)+" after the sever", 90*time.Second, func() bool {
				through, err := world.CoveredThrough(t, ctx, tenant, sessions[tenant], viewer.Records(), 0)
				return err == nil && through >= throughSecondInput
			})
			after := viewer.Records()[before[tenant]:]
			t.Logf("P4 %s viewer after the sever: %v", tenant, after)

			// A session.reset is how a client is TOLD about the gap the sever
			// left. Its absence would mean the client silently resumed at the
			// wrong position.
			sawReset := false
			for _, record := range after {
				if strings.HasPrefix(record, "R") {
					sawReset = true
				}
			}
			if !sawReset {
				t.Fatalf("%s's viewer got %v after the sever, want a session.reset before the continued stream", tenant, after)
			}
			through, err := world.CoveredThrough(t, ctx, tenant, sessions[tenant], viewer.Records(), 0)
			if err != nil || through < throughSecondInput {
				t.Fatalf("%s's viewer stream %v covers through %d (%v), want %d with no silent gap",
					tenant, viewer.Records(), through, err, throughSecondInput)
			}
			if strays := viewer.Strays(); len(strays) != 0 {
				t.Fatalf("%s's viewer received another tenant's records after the sever: %v", tenant, strays)
			}
		}
		paths := hostA.UpgradePaths()
		sort.Strings(paths)
		t.Logf("P4 host-a upgrade paths over its life: %v", paths)
	})

	t.Run("P5: a re-placed session is restored, not restarted", func(t *testing.T) {
		tenant := orchestrationtest.PooledTenantA
		s := sessions[tenant]
		runtimeID := world.RuntimeSessionID(t, ctx, tenant, s)

		hostA.Stop()
		orchestrationtest.PooledWait(t, "the registry stops reporting a live owner", 60*time.Second, func() bool {
			owner, found, _ := served.Directory.Owner(ctx, tenant, s)
			return !found ||
				owner.Residency != sessionwire.SessionResidencyResident ||
				!owner.Accepting ||
				!owner.ExpiresAt.After(time.Now())
		})

		hostB := orchestrationtest.StartPooledHost(t, ctx, world, "orchestrationtest-host-b", 7)
		orchestrationtest.AwaitAdvertised(t, world, "orchestrationtest-host-b")

		requestsBefore := len(world.LLM.Requests())
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("input-3"),
			SessionID:       s,
			Blocks:          json.RawMessage(`[{"type":"text","text":"what was the remembered word?"}]`),
		})
		if status != http.StatusOK {
			t.Fatalf("the re-placement input answered %d: %s", status, body)
		}
		took := orchestrationtest.PooledWait(t, "host-b applied the re-placed input", 120*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "input-3") == sessionstore.InboxStateApplied
		})

		creates, restores := hostB.Rig.Creates(), hostB.Rig.Restores()
		t.Logf("P5 input-3 applied after %v; host-b creates=%+v restores=%+v",
			took.Round(time.Millisecond), creates, restores)
		// THE CLAIM. Factory sends every attach as CREATE; Host decides from
		// the journal it finds under the binding's runtime session id. A Host
		// that got this wrong would silently re-open the stream and start the
		// conversation over, which is what host v0.3.0 fixed.
		if len(creates) != 0 {
			t.Fatalf("host-b CREATED %+v; a re-placed session must be restored", creates)
		}
		if len(restores) != 1 || restores[0].ID != runtimeID || restores[0].Tenant != tenant {
			t.Fatalf("host-b restores=%+v, want exactly one restore of %s for %s", restores, runtimeID, tenant)
		}

		// The journal is the authority on "restarted or not": a restart writes
		// a SECOND SessionStarted.
		orchestrationtest.PooledWait(t, "the restored turn finished", 60*time.Second, func() bool {
			return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID) >= 4
		})
		started := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world, tenant, runtimeID)
		restored := orchestrationtest.CountJournalEvents[event.RestoreDone](t, world, tenant, runtimeID)
		t.Logf("P5 journal %s: SessionStarted=%d RestoreDone=%d TurnDone=%d",
			runtimeID, started, restored, orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtimeID))
		if started != 1 {
			t.Fatalf("the journal holds %d SessionStarted events for %s, want exactly 1: the session was restarted", started, runtimeID)
		}
		if restored < 1 {
			t.Fatalf("the journal holds no RestoreDone for %s", runtimeID)
		}

		// And the conversation really is the old one: the restored turn's model
		// request carries THIS tenant's remembered word and NOT the other's.
		// Without the negative half, a Host that handed every session one
		// shared conversation would pass.
		if !world.LLM.SawInRequest(requestsBefore, words[tenant]) {
			t.Fatalf("no model request after the re-placement carried %q; the conversation did not survive", words[tenant])
		}
		if world.LLM.SawInRequest(requestsBefore, words[orchestrationtest.PooledTenantB]) {
			t.Fatalf("a model request after the re-placement carried %q, the OTHER tenant's word",
				words[orchestrationtest.PooledTenantB])
		}
	})
}

// TestAVerbatimDialOfAHostBaseIs404 measures the compatibility window host
// v0.3.0 opened and factory v0.5.0 closed.
//
// It is here rather than in a result document because it is the thing an
// operator most needs to be able to reproduce: a Factory that does not derive
// dials the advertised base, gets 404, places NOTHING and LOGS NOTHING. The
// symptom is silence, so the measurement has to be a test.
func TestAVerbatimDialOfAHostBaseIs404(t *testing.T) {
	ctx := placementContext(t)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{})
	pooled := orchestrationtest.StartPooledHost(t, ctx, world, "orchestrationtest-host-window", 4)

	probe := func(path string) int {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet,
			"http"+strings.TrimPrefix(string(pooled.Base), "ws")+path, nil)
		if err != nil {
			t.Fatalf("building the upgrade probe for %q: %v", path, err)
		}
		request.Header.Set("Connection", "Upgrade")
		request.Header.Set("Upgrade", "websocket")
		request.Header.Set("Sec-WebSocket-Version", "13")
		request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
		request.Header.Set("Sec-WebSocket-Protocol", "centrifuge-json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("the upgrade probe for %q: %v", path, err)
		}
		_ = response.Body.Close()
		return response.StatusCode
	}

	if status := probe(""); status != http.StatusNotFound {
		t.Fatalf("a verbatim dial of the advertised base answered %d, want 404", status)
	}
	for _, tenant := range []sessionwire.TenantID{orchestrationtest.PooledTenantA, orchestrationtest.PooledTenantB} {
		if status := probe(orchestrationtest.TenantPath(tenant)); status != http.StatusSwitchingProtocols {
			t.Fatalf("a dial of %s answered %d, want 101", orchestrationtest.TenantPath(tenant), status)
		}
	}
}
