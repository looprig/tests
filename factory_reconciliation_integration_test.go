//go:build integration

// This file is runbook 07 task I1.3 cases 2, 3 and 4.
//
// # What changed, and why this file could not exist before
//
// I1.3 cases 2-4 are assertions about a SWEEP. Before A9.1 stage 2, Factory had
// both sweepers -- internal/admission.Reconciler and internal/reconcile's gate
// sweep -- and factory.New constructed NEITHER, so nothing in a composed
// deployment ever called one, and both packages were internal to Factory. This
// module could not construct one either, and reimplementing the rotation here
// would have asserted a fact about the test.
//
// Stage 2 composes all three sweeps and Server.Start runs them, each on its own
// goroutine and its own timer, with the FIRST pass running immediately. That is
// what makes this file possible and it is also what makes it bounded: nothing
// here sleeps, every wait polls a durable or recorded condition to a deadline.
//
// # The one thing to know before editing it
//
// The sweep loop uses `time.NewTimer(s.cfg.reconcile.Interval)` -- REAL wall
// time -- not the composed Clock, which the kit drives virtually. So the cadence
// is real and the DUE TIMES are virtual, and the two must not be confused: an
// assertion about when a pass happens is about wall time, and an assertion about
// what a pass finds is about the kit clock.

package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// sweepInterval is the cadence the reconciliation cases run at.
//
// It is short because the loop's timer is real, and it is not shorter because a
// pass that has not finished by the next tick loses the cadence. Nothing asserts
// a duration; it only bounds how long the polls below take.
const sweepInterval = 25 * time.Millisecond

func reconcileLimits() factory.ReconcileLimits {
	limits := factory.DefaultReconcileLimits()
	limits.Interval = sweepInterval
	return limits
}

// awaitDue polls until the command sweep has recorded at least want due-page
// queries, and FAILS naming the call that did not happen rather than hanging.
//
// Every wait in this file is bounded and named. A fixture that cannot fail hangs
// instead, and a hang is not an assertion failure: it is a test that reports
// nothing at all after burning its whole timeout.
func awaitDue(t *testing.T, commands *orchestrationtest.StoreCommands, want int) []sessionstore.ListDueCommandsRequest {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := commands.DueRequests()
		if len(got) >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("the command sweep made %d due-page queries in 30s, want at least %d; "+
				"Server.Start did not run the commands sweep, or it is not reaching the durable command plane",
				len(got), want)
			return nil
		}
		time.Sleep(time.Millisecond)
	}
}

// awaitClaims polls until one replica's sweeper has filed at least one
// reconciliation claim, and FAILS naming the replica rather than hanging.
// seedOverdueCommand admits one command whose apply deadline has already passed
// at the kit clock, which is what makes it settleable due work.
func seedOverdueCommand(t *testing.T, ctx context.Context, store *orchestrationtest.StoreFixture, clock *orchestrationtest.Clock, tag string) sessionwire.SessionID {
	t.Helper()
	session := store.SeedSession(ctx, blockedAgent, string(blockedCompatibility))
	now := clock.Now()
	if _, _, err := store.Store.AdmitCommand(ctx, sessionstore.AdmitCommandRequest{
		TenantID:                 store.Tenant,
		SessionID:                session,
		CommandID:                sessionwire.CommandID("orchestrationtest-claim-" + tag + "-" + string(session)),
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(kitRuntimeCommandUUID()),
		Kind:                     "input",
		Payload:                  []byte(`{"text":"claim me"}`),
		AcceptedAt:               now.Add(-time.Minute),
		ApplyDeadline:            now.Add(-time.Second),
	}); err != nil {
		t.Fatalf("admitting overdue work for %s: %v", tag, err)
	}
	return session
}

func awaitClaims(t *testing.T, commands *orchestrationtest.StoreCommands, who string) []string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		holders := commands.ClaimHolders()
		if len(holders) > 0 {
			return holders
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s's sweeper filed no reconciliation claim in 30s; either it is not sweeping or "+
				"the seeded work is not due", who)
			return nil
		}
		time.Sleep(time.Millisecond)
	}
}

func awaitDueGates(t *testing.T, gates *orchestrationtest.StoreGates, want int) []sessionstore.ListDueGatesRequest {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := gates.DueRequests()
		if len(got) >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("the gate sweep made %d due-page queries in 30s, want at least %d; "+
				"Server.Start did not run the gates sweep", len(got), want)
			return nil
		}
		time.Sleep(time.Millisecond)
	}
}

// TestFactoryReconciliationSweeps is I1.3 cases 2, 3 and 4.
func TestFactoryReconciliationSweeps(t *testing.T) {
	ctx := coldReadContext(t)
	baseline := orchestrationtest.CaptureGoroutines()
	clock := orchestrationtest.NewClock(time.Unix(coldReadEpoch, 0))
	store := orchestrationtest.NewStoreFixture(t, ctx, clock)

	commands := orchestrationtest.NewStoreCommands(store.Store)
	replicaA := orchestrationtest.NewFactoryFixtureWithSeams(t, store, clock, orchestrationtest.FactorySeams{
		Commands:  commands,
		ReplicaID: "orchestrationtest-replica-a",
		Reconcile: reconcileLimits(),
	})
	gates := replicaA.Gates

	t.Run("case 2: the sweep visits every shard, round robin, in bounded pages", func(t *testing.T) {
		// The shard count is 4, not 1. A rotation over one shard is
		// indistinguishable from no rotation at all -- the degenerate constant
		// for this structural input is exactly 1 -- so the kit's seams report
		// four and this row asserts all four are reached.
		shards := orchestrationtest.KitControlShards
		requests := awaitDue(t, commands, shards*2)

		seen := make(map[int]int, shards)
		for i, req := range requests {
			if req.Shard < 0 || req.Shard >= shards {
				t.Fatalf("query %d named shard %d, outside [0,%d)", i, req.Shard, shards)
			}
			seen[req.Shard]++
			// Bounded PAGES, not a bounded total. A sweep that asked for
			// everything and truncated its own reply would do unbounded work in
			// the store, which is the cost this bound exists to prevent.
			if req.Limit <= 0 {
				t.Fatalf("query %d asked for an unbounded due page: %+v", i, req)
			}
			if req.Limit > storeOrderedPageCeiling {
				t.Fatalf("query %d asked for %d records, above the store's page ceiling", i, req.Limit)
			}
		}
		if len(seen) != shards {
			t.Fatalf("after %d queries the sweep reached shards %v, want all %d", len(requests), seen, shards)
		}
		// Round-robin, not random and not always-shard-zero: consecutive
		// queries must advance. Asserting only "all shards eventually" would
		// pass for a sweeper that picked uniformly at random.
		for i := 1; i < len(requests) && i < shards; i++ {
			if requests[i].Shard == requests[i-1].Shard && requests[i].Cursor == "" {
				t.Fatalf("queries %d and %d both opened shard %d; the rotation does not advance",
					i-1, i, requests[i].Shard)
			}
		}
	})

	t.Run("case 2: the page shape does not grow with tenant count", func(t *testing.T) {
		// "Independent of tenant count" is the case's own words and it is a
		// STRUCTURAL claim: adding tenants must not change what one pass asks
		// the store for. The row measures the request shape before and after
		// twenty more tenants exist.
		before := shapeOf(commands.DueRequests())
		for i := range 20 {
			tenant := sessionwire.TenantID(fmt.Sprintf("%sbulk-%02d", orchestrationtest.TenantPrefix, i))
			seedForeignSession(t, ctx, store, tenant, i)
		}
		mark := len(commands.DueRequests())
		after := shapeOf(awaitDue(t, commands, mark+orchestrationtest.KitControlShards*2)[mark:])
		if before.maxLimit != after.maxLimit {
			t.Fatalf("the due-page limit moved from %d to %d when twenty tenants were added",
				before.maxLimit, after.maxLimit)
		}
		if after.maxLimit <= 0 {
			t.Fatalf("the post-seed sweep asked for an unbounded page")
		}
	})

	t.Run("case 3: a terminal command never consumes a due page", func(t *testing.T) {
		// A SEPARATE durable plane, with no Factory attached to it.
		//
		// That is not isolation for tidiness: the claim is that a TERMINAL
		// command is absent from the due view, and replicaA's command sweep is
		// concurrently settling due work on the shared store. A row that waited
		// for a command to appear there would be racing the sweeper's own
		// removal of it, and would report "terminal commands are excluded" for
		// whichever of the two reasons won. The due view is SessionStore's
		// contract and is read here directly.
		store := orchestrationtest.NewStoreFixture(t, ctx, clock)
		session := store.SeedSession(ctx, blockedAgent, string(blockedCompatibility))
		commandID := sessionwire.CommandID("orchestrationtest-cmd-" + string(session))
		now := clock.Now()
		entry, _, err := store.Store.AdmitCommand(ctx, sessionstore.AdmitCommandRequest{
			TenantID:                 store.Tenant,
			SessionID:                session,
			CommandID:                commandID,
			ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(kitRuntimeCommandUUID()),
			Kind:                     "input",
			Payload:                  []byte(`{"text":"hello"}`),
			AcceptedAt:               now,
			ApplyDeadline:            now.Add(time.Hour),
		})
		if err != nil {
			t.Fatalf("admitting a command: %v", err)
		}
		if entry.Record.CommandID != commandID {
			t.Fatalf("the admitted record names %q, want %q", entry.Record.CommandID, commandID)
		}

		// The positive first: the command IS due work, so it appears. Without
		// this the disappearance below would be indistinguishable from a sweep
		// that never looked at this session at all.
		if !appearsInDuePages(t, ctx, store, commandID, true) {
			t.Fatalf("an accepted command with a future deadline is not due work; the case cannot discriminate")
		}

		if _, err := store.Store.RejectCommand(ctx, sessionstore.RejectCommandRequest{
			TenantID:         store.Tenant,
			SessionID:        session,
			CommandID:        commandID,
			ExpectedRevision: entry.Revision,
			Rejection: sessionwire.ErrorDetail{
				Code:    sessionwire.ErrorCodeInvalidRequest,
				Message: "orchestrationtest: terminal for the due-page case",
			},
		}); err != nil {
			t.Fatalf("rejecting the command: %v", err)
		}
		if appearsInDuePages(t, ctx, store, commandID, false) {
			t.Fatalf("a terminal (rejected) command is still consuming a due page")
		}
	})

	t.Run("case 3: a claim refuses a peer and recovers after expiry", func(t *testing.T) {
		// The durable half, and labelled as what it is: this exercises
		// SessionStore's claim compare-and-swap directly, with the two holder
		// ids supplied by the test. It is NOT evidence about Factory -- the row
		// above is -- and the previous version of this file said otherwise.
		//
		// It is kept because the mechanism it covers is the one the row above
		// depends on: distinct holder ids are only worth anything if the store
		// refuses a second holder and releases on expiry.
		isolated := orchestrationtest.NewStoreFixture(t, ctx, clock)
		session := isolated.SeedSession(ctx, blockedAgent, string(blockedCompatibility))
		held, err := isolated.Store.AcquireReconciliationClaim(ctx, sessionstore.AcquireReconciliationClaimRequest{
			TenantID:  isolated.Tenant,
			SessionID: session,
			HolderID:  "orchestrationtest-holder-one",
			ExpiresAt: clock.Now().Add(time.Minute),
		})
		if err != nil {
			t.Fatalf("the first holder could not claim: %v", err)
		}
		if held.Claim.HolderID != "orchestrationtest-holder-one" {
			t.Fatalf("the claim names holder %q", held.Claim.HolderID)
		}
		if _, err := isolated.Store.AcquireReconciliationClaim(ctx, sessionstore.AcquireReconciliationClaimRequest{
			TenantID:  isolated.Tenant,
			SessionID: session,
			HolderID:  "orchestrationtest-holder-two",
			ExpiresAt: clock.Now().Add(time.Minute),
		}); err == nil {
			t.Fatalf("a second holder took a live claim")
		}
		// And it RECOVERS: a holder that never returns must not own the session
		// forever, which is the "claims expire/recover after reconciler crash"
		// half. The clock moves past the expiry rather than the test sleeping.
		clock.Advance(2 * time.Minute)
		recovered, err := isolated.Store.AcquireReconciliationClaim(ctx, sessionstore.AcquireReconciliationClaimRequest{
			TenantID:  isolated.Tenant,
			SessionID: session,
			HolderID:  "orchestrationtest-holder-two",
			ExpiresAt: clock.Now().Add(time.Minute),
		})
		if err != nil {
			t.Fatalf("the second holder could not recover an expired claim: %v", err)
		}
		if recovered.Claim.HolderID != "orchestrationtest-holder-two" {
			t.Fatalf("the recovered claim names %q", recovered.Claim.HolderID)
		}
	})

	t.Run("case 4: the gate sweep runs and Factory does not resolve an open gate", func(t *testing.T) {
		// An OPEN gate past its deadline is due work nobody has dealt with, and
		// the case's rule is that the sweep must NOT answer it: answering a cold
		// gate is a decision only a participant can make. So the assertion is a
		// negative on durable state, held across several sweep passes.
		session := store.SeedSession(ctx, blockedAgent, string(blockedCompatibility))
		gateID := sessionwire.GateID("orchestrationtest-gate-open")
		openGate(t, ctx, store, session, gateID, clock.Now().Add(-time.Minute))

		mark := len(gates.DueRequests())
		requests := awaitDueGates(t, gates, mark+orchestrationtest.KitControlShards*2)
		shards := map[int]bool{}
		for _, req := range requests {
			shards[req.Shard] = true
		}
		if len(shards) != orchestrationtest.KitControlShards {
			t.Fatalf("the gate sweep reached shards %v, want all %d", shards, orchestrationtest.KitControlShards)
		}

		page, err := store.Store.ReadGates(ctx, sessionstore.ReadGatesRequest{
			TenantID: store.Tenant, SessionID: session,
		})
		if err != nil {
			t.Fatalf("reading gates: %v", err)
		}
		found := false
		for _, gate := range page.Gates {
			if gate.GateID == gateID {
				found = true
			}
		}
		if !found {
			t.Fatalf("the sweep resolved or removed a still-open gate; Factory must not answer a cold gate")
		}
		if page.OpenGateCount == 0 {
			t.Fatalf("the open gate count fell to zero while the gate is still open")
		}
	})

	t.Run("case 3: each replica's own sweeper files claims under its own holder id", func(t *testing.T) {
		// THIS is the row that makes case 3's replica half evidence about
		// Factory, and it replaces one that was not.
		//
		// The previous version composed two Factory servers, asserted their
		// ReplicaID fields differed, and then called
		// store.Store.AcquireReconciliationClaim three times itself. Factory
		// was not in the path: it demonstrated SessionStore's claim CAS against
		// two strings the TEST supplied. A Factory that filed EVERY claim under
		// one constant holder id -- precisely the deployment failure the row's
		// own comment named -- survived that as a mutation, exit 0.
		//
		// The holder id is the only thing that suppresses duplicate work
		// between replicas, so what has to be observed is which string each
		// replica's OWN SWEEPER files. The composed command seam records it.
		// The two replicas are observed on SEPARATE work rather than racing for
		// one row, and that is a deliberate choice with a measured reason.
		//
		// Factory's own predicate refuses a row whose claim is live BEFORE it
		// attempts a claim of its own (settleable -> DispositionClaimLive), and
		// the winner then SETTLES the row, so it stops being due. Two replicas
		// contending for one command therefore produce exactly one claim, and
		// the loser files nothing -- which is the suppression working, and is
		// also indistinguishable from a replica that is not sweeping at all.
		// Measured: waiting for the peer's claim on a contended row times out.
		//
		// So each replica is given its own due row, and the assertion is the
		// one that actually discriminates: the string each sweeper files.
		// factory v0.5.0 files under a holder id NAMESPACED BY SWEEP --
		// "<replica>/commands" -- rather than under the bare replica id. That
		// is a widening, not a drift: the commands sweep and the gates sweep
		// are two rotations over one shard space, and one holder string for
		// both would let either suppress the other's work. What the row must
		// keep discriminating is the property the id exists for, so it asserts
		// the replica's OWN identity is what distinguishes the string, not the
		// literal spelling of the suffix.
		seedOverdueCommand(t, ctx, store, clock, "a")
		holdersA := awaitClaims(t, commands, "replica A")
		if len(holdersA) != 1 || !strings.HasPrefix(holdersA[0], replicaA.ReplicaID) {
			t.Fatalf("replica A's sweeper filed claims under %v, want exactly one carrying its own replica id %q", holdersA, replicaA.ReplicaID)
		}

		// Replica A is stopped and replica B composed only now, so the second
		// row is unambiguously B's. Stop shuts the sweeps down and WAITS for
		// them, so this is a fence rather than a hope -- which is why this
		// subtest runs last.
		replicaA.Stop(t)
		second := orchestrationtest.NewStoreCommands(store.Store)
		replicaB := orchestrationtest.NewFactoryFixtureWithSeams(t, store, clock, orchestrationtest.FactorySeams{
			Commands:  second,
			ReplicaID: "orchestrationtest-replica-b",
			Reconcile: reconcileLimits(),
		})
		t.Cleanup(func() { replicaB.Stop(t) })
		if replicaA.ReplicaID == replicaB.ReplicaID {
			t.Fatalf("both replicas composed the holder id %q", replicaA.ReplicaID)
		}
		seedOverdueCommand(t, ctx, store, clock, "b")
		holdersB := awaitClaims(t, second, "replica B")
		if len(holdersB) != 1 || !strings.HasPrefix(holdersB[0], replicaB.ReplicaID) {
			t.Fatalf("replica B's sweeper filed claims under %v, want exactly one carrying its own replica id %q", holdersB, replicaB.ReplicaID)
		}

		if holdersA[0] == holdersB[0] {
			t.Fatalf("both replicas' sweepers filed under one holder id %q; a claim cannot suppress "+
				"duplicate work between replicas that share an identity", holdersA[0])
		}
		// Neither replica ever filed under the other's name. Without this the
		// pair above would pass for a build that used whichever id it saw last.
		for _, holder := range second.ClaimHolders() {
			if strings.HasPrefix(holder, replicaA.ReplicaID) {
				t.Fatalf("replica B's sweeper filed a claim under replica A's holder id %q", holder)
			}
		}
	})

	orchestrationtest.AssertNoLeaks(t, ctx, orchestrationtest.LeakSources{
		Store:              store,
		Factories:          []*orchestrationtest.FactoryFixture{replicaA},
		LinksUnobservable:  true,
		BaselineGoroutines: baseline,
	})
}

// storeOrderedPageCeiling is Storage's page ceiling, restated so a bound
// assertion has something to compare against without importing storage here.
const storeOrderedPageCeiling = 1000

type duePageShape struct{ maxLimit int }

func shapeOf(requests []sessionstore.ListDueCommandsRequest) duePageShape {
	shape := duePageShape{}
	for _, req := range requests {
		if req.Limit > shape.maxLimit {
			shape.maxLimit = req.Limit
		}
	}
	return shape
}

// seedForeignSession creates one session under a tenant that is not the
// fixture's, so the tenant-count axis is real rather than a second session.
func seedForeignSession(t *testing.T, ctx context.Context, store *orchestrationtest.StoreFixture, tenant sessionwire.TenantID, n int) {
	t.Helper()
	now := store.Clock.Now()
	if _, _, err := store.Store.CreateCatalogEntry(ctx, sessionstore.CreateCatalogEntryRequest{
		TenantID:               tenant,
		SessionID:              sessionwire.SessionID(fmt.Sprintf("session-bulk-%02d", n)),
		AgentID:                blockedAgent,
		RuntimeCompatibilityID: string(blockedCompatibility),
		CreatedAt:              now,
		LastActiveAt:           now,
		State:                  sessionwire.SessionStateIdle,
		Residency:              sessionwire.SessionResidencyCold,
		DesiredPlacement:       sessionwire.HostPlacementPooled,
	}); err != nil {
		t.Fatalf("seeding a foreign-tenant session: %v", err)
	}
}

// appearsInDuePages reports whether one command is reachable in any shard's due
// page at the fixture clock's now.
//
// It reads the store directly rather than watching the sweep, because the sweep
// SETTLES what it finds: a row that waited for the sweeper to report the command
// would be racing the sweeper's own removal of it.
func appearsInDuePages(t *testing.T, ctx context.Context, store *orchestrationtest.StoreFixture, command sessionwire.CommandID, want bool) bool {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		for shard := range orchestrationtest.KitControlShards {
			page, err := store.Store.ListDueCommands(ctx, sessionstore.ListDueCommandsRequest{
				Shard:         shard,
				DueAtOrBefore: store.Clock.Now().Add(2 * time.Hour),
				Limit:         100,
			})
			if err != nil {
				t.Fatalf("listing due commands in shard %d: %v", shard, err)
				return false
			}
			for _, due := range page.Commands {
				if due.Entry.Record.CommandID == command {
					if want {
						return true
					}
					break
				}
			}
		}
		present := false
		for shard := range orchestrationtest.KitControlShards {
			page, _ := store.Store.ListDueCommands(ctx, sessionstore.ListDueCommandsRequest{
				Shard:         shard,
				DueAtOrBefore: store.Clock.Now().Add(2 * time.Hour),
				Limit:         100,
			})
			for _, due := range page.Commands {
				if due.Entry.Record.CommandID == command {
					present = true
				}
			}
		}
		if present == want {
			return present
		}
		if time.Now().After(deadline) {
			return present
		}
		time.Sleep(time.Millisecond)
	}
}

func openGate(t *testing.T, ctx context.Context, store *orchestrationtest.StoreFixture, session sessionwire.SessionID, gate sessionwire.GateID, deadline time.Time) {
	t.Helper()
	eventID := sessionwire.EventID("event-gate-" + string(gate))
	seq := store.AppendPublicEvent(ctx, session, eventID, []byte(`{"gate":"opened"}`))
	store.AdvanceCatalogJournal(ctx, session, 1, seq, eventID, nil)
	if _, err := store.Store.OpenGate(ctx, sessionstore.OpenGateRequest{
		TenantID:   store.Tenant,
		SessionID:  session,
		LeaseEpoch: 1,
		Gate: sessionwire.GateProjection{
			GateID:           gate,
			Kind:             "permission",
			OpenedEventID:    sessionwire.EventID("event-gate-" + string(gate)),
			OpenedJournalSeq: seq,
			Deadline:         deadline,
			Answerability:    sessionwire.GateAnswerabilityResident,
			Prompt:           sessionwire.GatePrompt{Title: "orchestrationtest gate"},
		},
	}); err != nil {
		t.Fatalf("opening gate %q: %v", gate, err)
	}
}

func kitRuntimeCommandUUID() string { return "3f2a1b0c-4d5e-4a6b-8c7d-9e0f1a2b3c4d" }
