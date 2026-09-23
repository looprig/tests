//go:build integration

// This file is runbook 07 task I1.3 cases 2, 3 and 4 over the DURABLE plane.
// Case 1 and case 3's placement half are driven across real replicas and a
// real pooled Host in factory_reconciliation_pooled_integration_test.go; case 5
// is factory_reconciliation_brokerless_integration_test.go.
//
// # Which sweep each test drives, and why that matters
//
// Factory runs FOUR sweeps. TestFactoryReconciliationSweeps drives the LEGACY
// command deadline sweep, which Factory's own composition calls unreachable in
// production: legacy rows live only on legacy sessions and nothing admits into
// them. Its rows are kept, and seed legacy rows directly, because the sweep
// still runs and a store may still hold such rows. The sweep production
// actually runs is the DISPOSITION deadline sweep, and
// TestFactoryDispositionSweepVisitsEveryShardInBoundedPages holds it to the
// same contract with every command admitted through Factory.
// TestFactoryGateSweepRetiresStaleIntents is the gate sweep (case 4).
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
	"net/http"
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

// TestFactoryReconciliationSweeps is I1.3 cases 2-4 over the LEGACY command
// sweep and the store's claim and gate contracts. See the file header for why
// the disposition sweep has its own test.
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

	t.Run("case 2 (legacy sweep): the sweep visits every shard, round robin, in bounded pages", func(t *testing.T) {
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
		// queries must advance. This inspects only the first `shards` queries,
		// which makes it a STRUCTURAL check of the rotation's first cycle; the
		// disposition test holds EVERY consecutive pair to it. Asserting only
		// "all shards eventually" would pass for a sweeper that picked
		// uniformly at random.
		for i := 1; i < len(requests) && i < shards; i++ {
			if requests[i].Shard == requests[i-1].Shard && requests[i].Cursor == "" {
				t.Fatalf("queries %d and %d both opened shard %d; the rotation does not advance",
					i-1, i, requests[i].Shard)
			}
		}
	})

	t.Run("case 3 (legacy store view): a terminal command leaves the due view", func(t *testing.T) {
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

	t.Run("case 3 (store contract): a claim refuses a peer and recovers after expiry", func(t *testing.T) {
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

	t.Run("case 4 (shard reach): the gate sweep runs and Factory does not resolve an open gate", func(t *testing.T) {
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

	t.Run("case 3 (legacy sweep): each replica's own sweeper files claims under its own holder id", func(t *testing.T) {
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
		// both would let either suppress the other's work.
		//
		// THE COMPARISON IS EQUALITY, not a prefix. A prefix test stops
		// discriminating exactly where the row matters: "replica-1" is a prefix
		// of "replica-10", so two replicas whose ids differ only by a trailing
		// digit would read as each other's and the duplicate-suppression claim
		// would pass with no suppression happening. The suffix is Factory's and
		// is named here so that a change to it fails loudly rather than
		// silently loosening this row.
		seedOverdueCommand(t, ctx, store, clock, "a")
		holdersA := awaitClaims(t, commands, "replica A")
		if len(holdersA) != 1 || holdersA[0] != replicaA.ReplicaID+commandSweepHolderSuffix {
			t.Fatalf("replica A's sweeper filed claims under %v, want exactly [%q]", holdersA, replicaA.ReplicaID+commandSweepHolderSuffix)
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
		if len(holdersB) != 1 || holdersB[0] != replicaB.ReplicaID+commandSweepHolderSuffix {
			t.Fatalf("replica B's sweeper filed claims under %v, want exactly [%q]", holdersB, replicaB.ReplicaID+commandSweepHolderSuffix)
		}

		if holdersA[0] == holdersB[0] {
			t.Fatalf("both replicas' sweepers filed under one holder id %q; a claim cannot suppress "+
				"duplicate work between replicas that share an identity", holdersA[0])
		}
		// Neither replica ever filed under the other's name. Without this the
		// pair above would pass for a build that used whichever id it saw last.
		for _, holder := range second.ClaimHolders() {
			if holder == replicaA.ReplicaID+commandSweepHolderSuffix {
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

// commandSweepHolderSuffix is what factory v0.5.0 appends to a replica id when
// the COMMAND sweep files a reconciliation claim.
//
// It is restated here because Factory exports no constant for it, and it is
// restated rather than approximated with a prefix test so that a drift fails
// loudly. See the holder-id row for why a prefix would stop discriminating.
const commandSweepHolderSuffix = "/commands"

// storeOrderedPageCeiling is Storage's page ceiling, restated so a bound
// assertion has something to compare against without importing storage here.
const storeOrderedPageCeiling = 1000

// appearsInDuePages reports whether one command is reachable in any shard's due
// view at the fixture clock's now plus two hours, walking every page of every
// shard. It waits up to ten seconds for the answer to become want.
//
// It reads the store directly rather than watching the sweep, because the sweep
// SETTLES what it finds: a row that waited for the sweeper to report the command
// would be racing the sweeper's own removal of it. Every page is followed to
// its end and every error fails the case -- a scan that stopped at the first
// page, or swallowed a failed read, would report "absent" for a row it never
// looked at.
func appearsInDuePages(t *testing.T, ctx context.Context, store *orchestrationtest.StoreFixture, command sessionwire.CommandID, want bool) bool {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		present := false
		for shard := range orchestrationtest.KitControlShards {
			req := sessionstore.ListDueCommandsRequest{
				Shard:         shard,
				DueAtOrBefore: store.Clock.Now().Add(2 * time.Hour),
				Limit:         100,
			}
			for {
				page, err := store.Store.ListDueCommands(ctx, req)
				if err != nil {
					t.Fatalf("listing due commands in shard %d: %v", shard, err)
					return false
				}
				for _, due := range page.Commands {
					if due.Entry.Record.CommandID == command {
						present = true
					}
				}
				if page.NextCursor == "" {
					break
				}
				req = sessionstore.ListDueCommandsRequest{Shard: shard, Limit: 100, Cursor: page.NextCursor}
			}
		}
		if present == want || time.Now().After(deadline) {
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

// ---- the DISPOSITION deadline sweep: the one production runs ----------------

// dispositionPageLimit and dispositionMaxPages are the bounds the disposition
// and gate rows compose. They are deliberately tiny: a bound of 256 over a
// fixture of twenty rows is never reached, so a row built on the defaults
// could not tell a bounded sweep from an unbounded one. With MaxPages 1 every
// due-page query IS one pass, which is what lets the rows below read "queries
// per pass" off the recorded request stream.
const (
	dispositionPageLimit = 2
	dispositionMaxPages  = 1
	dispositionHolderSfx = "/dispositions"
)

func boundedReconcileLimits() factory.ReconcileLimits {
	limits := reconcileLimits()
	limits.MaxDuePerSweep = dispositionPageLimit
	limits.MaxConcurrent = dispositionMaxPages
	return limits
}

// dispositionWorld is one real Factory with the create plane composed, over a
// real Store, admitting in many tenants, with its command seam recorded.
type dispositionWorld struct {
	store    *orchestrationtest.StoreFixture
	clock    *orchestrationtest.Clock
	commands *orchestrationtest.StoreCommands
	replica  *orchestrationtest.FactoryFixture
	bearers  []string
	tenants  map[string]sessionwire.TenantID
}

func newDispositionWorld(t *testing.T, ctx context.Context, tenants int) *dispositionWorld {
	t.Helper()
	clock := orchestrationtest.NewClock(time.Unix(coldReadEpoch, 0))
	store := orchestrationtest.NewStoreFixture(t, ctx, clock)
	world := &dispositionWorld{
		store:    store,
		clock:    clock,
		commands: orchestrationtest.NewStoreCommands(store.Store),
		tenants:  map[string]sessionwire.TenantID{orchestrationtest.KitActorCredential: store.Tenant},
		bearers:  []string{orchestrationtest.KitActorCredential},
	}
	for i := 1; i < tenants; i++ {
		bearer := fmt.Sprintf("orchestrationtest-bearer-%02d", i)
		world.tenants[bearer] = sessionwire.TenantID(fmt.Sprintf("%s%s-t%02d", orchestrationtest.TenantPrefix, store.Tenant[len(orchestrationtest.TenantPrefix):], i))
		world.bearers = append(world.bearers, bearer)
	}
	world.replica = orchestrationtest.NewFactoryFixtureWithSeams(t, store, clock, orchestrationtest.FactorySeams{
		Commands:              world.commands,
		ReplicaID:             "orchestrationtest-disposition-replica",
		Reconcile:             boundedReconcileLimits(),
		Templates:             []factory.LaunchTemplate{orchestrationtest.KitLaunchTemplate(orchestrationtest.LaneAgent, string(orchestrationtest.LaneCompatibility))},
		SessionBindingID:      orchestrationtest.LaneStorageBinding,
		SessionBindingVersion: orchestrationtest.LaneBindingVersion,
		Verifier:              &orchestrationtest.MultiTenantVerifier{Bearers: world.tenants, Clock: clock},
	})
	return world
}

// dueCommand names one admitted command.
type dueCommand struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
	command sessionwire.CommandID
}

// create admits one session's create THROUGH FACTORY, as bearer's tenant. The
// create is a disposition command whose apply deadline Factory stamps from its
// own composed clock and ReconcileLimits.ApplyDeadline.
func (w *dispositionWorld) create(t *testing.T, ctx context.Context, bearer, tag string) dueCommand {
	t.Helper()
	session := sessionwire.SessionID("session-" + tag)
	command := sessionwire.CommandID("command-" + tag)
	status, body := w.replica.PostAs(t, ctx, bearer, "/v1/sessions", orchestrationtest.CreateBody(session, command))
	if status != http.StatusCreated {
		t.Fatalf("Factory answered the create %s with %d: %s", tag, status, body)
	}
	return dueCommand{tenant: w.tenants[bearer], session: session, command: command}
}

func (w *dispositionWorld) state(t *testing.T, ctx context.Context, c dueCommand) sessionstore.InboxState {
	t.Helper()
	entry, err := w.store.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID: c.tenant, SessionID: c.session, CommandID: c.command,
	})
	if err != nil {
		t.Fatalf("reading %s/%s: %v", c.session, c.command, err)
	}
	return entry.Record.State
}

// shardsOf reads, straight from the store, which shard's due view holds each
// named command within bound. It walks every page of every shard.
func (w *dispositionWorld) shardsOf(t *testing.T, ctx context.Context, bound time.Time, commands []dueCommand) map[sessionwire.CommandID]int {
	t.Helper()
	want := map[sessionwire.CommandID]bool{}
	for _, c := range commands {
		want[c.command] = true
	}
	found := map[sessionwire.CommandID]int{}
	for shard := range orchestrationtest.KitControlShards {
		req := sessionstore.ListDueDispositionCommandsRequest{Shard: shard, DueAtOrBefore: bound, Limit: 100}
		for {
			page, err := w.store.Store.ListDueDispositionCommands(ctx, req)
			if err != nil {
				t.Fatalf("listing shard %d's due disposition commands: %v", shard, err)
			}
			for _, entry := range page.Commands {
				if id := entry.Record.Descriptor.CommandID; want[id] {
					found[id] = shard
				}
			}
			if page.NextCursor == "" {
				break
			}
			req = sessionstore.ListDueDispositionCommandsRequest{Shard: shard, Limit: 100, Cursor: page.NextCursor}
		}
	}
	return found
}

// awaitRejected waits until the sweep has rejected every named command, and
// fails naming the ones it never reached and the shards they live in.
func (w *dispositionWorld) awaitRejected(t *testing.T, ctx context.Context, commands []dueCommand, shards map[sessionwire.CommandID]int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var pending []string
		for _, c := range commands {
			if w.state(t, ctx, c) != sessionstore.InboxStateRejected {
				pending = append(pending, fmt.Sprintf("%s(shard %d)", c.command, shards[c.command]))
			}
		}
		if len(pending) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after 30s the disposition sweep had not rejected %d expired commands: %v", len(pending), pending)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertBoundedRotation holds one window of the disposition sweep's recorded
// queries to the fixed-shard contract.
//
// Every query is ONE PASS, because MaxPages is 1, and consecutive passes visit
// consecutive shards: a pass is not allowed to linger on a shard, to skip one,
// or to answer for all of them from shard zero. Every query asks for exactly
// the composed page limit. A resumed pass carries a cursor and no bound of its
// own; a fresh pass carries a bound and no cursor.
func assertBoundedRotation(t *testing.T, phase string, requests []sessionstore.ListDueDispositionCommandsRequest) (resumed int) {
	t.Helper()
	shards := orchestrationtest.KitControlShards
	if len(requests) < shards {
		t.Fatalf("%s: only %d disposition queries were recorded; a rotation needs at least %d", phase, len(requests), shards)
	}
	seen := map[int]bool{}
	for i, req := range requests {
		if req.Shard < 0 || req.Shard >= shards {
			t.Fatalf("%s: query %d named shard %d, outside [0,%d)", phase, i, req.Shard, shards)
		}
		seen[req.Shard] = true
		if req.Limit != dispositionPageLimit {
			t.Fatalf("%s: query %d asked for %d rows; the composed page limit is %d", phase, i, req.Limit, dispositionPageLimit)
		}
		if (req.Cursor == "") == req.DueAtOrBefore.IsZero() {
			t.Fatalf("%s: query %d carries cursor %q and bound %v; a pass is either fresh or resumed", phase, i, req.Cursor, req.DueAtOrBefore)
		}
		if req.Cursor != "" {
			resumed++
		}
		if i > 0 && req.Shard != (requests[i-1].Shard+1)%shards {
			t.Fatalf("%s: query %d visited shard %d right after shard %d; the fixed-shard rotation does not advance one shard per pass",
				phase, i, req.Shard, requests[i-1].Shard)
		}
	}
	if len(seen) != shards {
		t.Fatalf("%s: the sweep reached shards %v, want all %d", phase, seen, shards)
	}
	return resumed
}

// TestFactoryDispositionSweepVisitsEveryShardInBoundedPages is I1.3 cases 2
// and 3 (terminal rows, holder identity) over the DISPOSITION deadline sweep --
// the sweep a deployment actually runs, since every command Factory admits is
// a disposition command and the legacy sweep's rows can only exist on a store
// that already held them.
//
// Every command is admitted THROUGH FACTORY, in every tenant, and is settled
// by Factory's own sweep. The evidence is durable: each expired command ends
// REJECTED in the store, and the recorder only reads what the sweep asked.
func TestFactoryDispositionSweepVisitsEveryShardInBoundedPages(t *testing.T) {
	ctx := coldReadContext(t)
	const manyTenants = 20
	w := newDispositionWorld(t, ctx, manyTenants+1)
	applyDeadline := factory.DefaultReconcileLimits().ApplyDeadline

	// PHASE ONE: one tenant, enough sessions that EVERY shard holds more than
	// one page of expired work. A shard with a page or less is one no pass
	// ever has to resume in, and "bounded pages" would be vacuous there.
	var first []dueCommand
	var shards map[sessionwire.CommandID]int
	perShard := func() map[int]int {
		counts := map[int]int{}
		for _, shard := range shards {
			counts[shard]++
		}
		return counts
	}
	for i := 0; ; i++ {
		first = append(first, w.create(t, ctx, orchestrationtest.KitActorCredential, fmt.Sprintf("disp-one-%03d", i)))
		if len(first) < 4*orchestrationtest.KitControlShards {
			continue
		}
		shards = w.shardsOf(t, ctx, w.clock.Now().Add(2*applyDeadline), first)
		if len(shards) != len(first) {
			t.Fatalf("%d of %d admitted creates are not in any shard's due view", len(first)-len(shards), len(first))
		}
		enough := len(perShard()) == orchestrationtest.KitControlShards
		for _, n := range perShard() {
			enough = enough && n > dispositionPageLimit
		}
		if enough {
			break
		}
		if i > 200 {
			t.Fatalf("200 sessions did not fill every shard past one page: %v", perShard())
		}
	}
	t.Logf("phase one: %d commands, per shard %v", len(first), perShard())

	// Nothing is due until the kit clock passes the deadline Factory stamped.
	// The sweep is already running; it must not have settled anything.
	for _, c := range first {
		if state := w.state(t, ctx, c); state != sessionstore.InboxStatePending {
			t.Fatalf("%s is %q before its apply deadline", c.command, state)
		}
	}
	mark := len(w.commands.DispositionDueRequests())
	w.clock.Advance(applyDeadline + time.Second)
	w.awaitRejected(t, ctx, first, shards)
	phaseOne := w.commands.DispositionDueRequests()[mark:]
	resumedOne := assertBoundedRotation(t, "phase one", phaseOne)
	if resumedOne == 0 {
		t.Fatalf("phase one: every shard held more than %d expired rows and no pass resumed from a cursor; "+
			"the sweep is not paging", dispositionPageLimit)
	}

	t.Run("case 3: the disposition sweep claims under its own holder, never placement's", func(t *testing.T) {
		// The disposition sweep claims each session before it rejects, and it
		// must do so under a holder of its OWN. The store treats an acquire by
		// the claim's current holder as an EXTENSION, so under the bare replica
		// id -- which is what placement claims under -- this sweep would
		// "extend" a claim placement holds mid-attach, reject, and RELEASE it,
		// letting a second replica attach the same session (Factory's B5 N1).
		//
		// THIS ROW ONLY DRIVES THE STRING, NOT THE HAZARD: it asserts holder
		// identity by string equality on Factory's unexported "/dispositions"
		// suffix. It does not construct the contended scenario above -- the
		// disposition sweep actually racing placement's live attach claim on
		// one session -- so the extend-and-release hazard itself is never
		// exercised here. A holder-string regression is caught; the
		// behavioural hazard it exists to prevent is not.
		want := w.replica.ReplicaID + dispositionHolderSfx
		requests := w.commands.ClaimRequests()
		if len(requests) < len(first) {
			t.Fatalf("the sweep filed %d claims for %d rejections", len(requests), len(first))
		}
		for _, req := range requests {
			if req.HolderID != want {
				t.Fatalf("the disposition sweep claimed %s under %q, want exactly %q", req.SessionID, req.HolderID, want)
			}
			if req.HolderID == w.replica.ReplicaID {
				t.Fatalf("the disposition sweep claimed under the bare replica id, which is placement's holder")
			}
		}
		if got := len(w.commands.RejectedDispositions()); got < len(first) {
			t.Fatalf("the sweep recorded %d rejections, want at least %d", got, len(first))
		}
	})

	t.Run("case 3: terminal rows never consume a due page", func(t *testing.T) {
		// Every shard now holds more than one PAGE of terminal (rejected) rows
		// with EARLIER deadlines than anything admitted next. If terminal rows
		// stayed in the due view they would fill a Limit-sized first page ahead
		// of the live row. The live row must be on the first page, and the
		// page must have examined nothing else.
		probe := w.create(t, ctx, orchestrationtest.KitActorCredential, "disp-probe")
		shard, ok := w.shardsOf(t, ctx, w.clock.Now().Add(2*applyDeadline), []dueCommand{probe})[probe.command]
		if !ok {
			t.Fatalf("the probe is not due work; the row cannot discriminate")
		}
		if perShard()[shard] <= dispositionPageLimit {
			t.Fatalf("shard %d holds only %d terminal rows; the row needs more than a page", shard, perShard()[shard])
		}
		page, err := w.store.Store.ListDueDispositionCommands(ctx, sessionstore.ListDueDispositionCommandsRequest{
			Shard: shard, DueAtOrBefore: w.clock.Now().Add(2 * applyDeadline), Limit: dispositionPageLimit,
		})
		if err != nil {
			t.Fatalf("reading shard %d's first due page: %v", shard, err)
		}
		if len(page.Commands) != 1 || page.Commands[0].Record.Descriptor.CommandID != probe.command {
			t.Fatalf("shard %d's first page is %d rows, want exactly the live probe: terminal rows are consuming it", shard, len(page.Commands))
		}
		if page.Examined != 1 {
			t.Fatalf("shard %d's first page examined %d rows for one live command; %d terminal rows are still in the due view",
				shard, page.Examined, page.Examined-1)
		}
		first = append(first, probe)
		shards[probe.command] = shard
	})

	// PHASE TWO: twenty MORE tenants, each admitting through the same Factory.
	// The page shape and the rotation must be exactly what they were with one.
	var second []dueCommand
	for i, bearer := range w.bearers[1:] {
		second = append(second, w.create(t, ctx, bearer, fmt.Sprintf("disp-many-%02d", i)))
	}
	for i := range 2 * orchestrationtest.KitControlShards {
		second = append(second, w.create(t, ctx, orchestrationtest.KitActorCredential, fmt.Sprintf("disp-many-own-%02d", i)))
	}
	shardsTwo := w.shardsOf(t, ctx, w.clock.Now().Add(2*applyDeadline), second)
	for k, v := range shardsTwo {
		shards[k] = v
	}
	tenantsSeen := map[sessionwire.TenantID]bool{}
	for _, c := range second {
		tenantsSeen[c.tenant] = true
	}
	if len(tenantsSeen) != manyTenants+1 {
		t.Fatalf("phase two admitted work in %d tenants, want %d", len(tenantsSeen), manyTenants+1)
	}
	mark = len(w.commands.DispositionDueRequests())
	w.clock.Advance(applyDeadline + time.Second)
	w.awaitRejected(t, ctx, append(second, first[len(first)-1]), shards)
	phaseTwo := w.commands.DispositionDueRequests()[mark:]

	t.Run("case 2: the sweep visits every shard, one bounded page per pass, whatever the tenant count", func(t *testing.T) {
		resumedTwo := assertBoundedRotation(t, "phase two", phaseTwo)
		// The per-pass shape is compared, not merely bounded: one query of
		// exactly Limit rows per pass with one tenant and with twenty-one.
		//
		// Two different things make that true, and only one is DRIVEN here.
		// The driven half: rotation and page shape are measured identically
		// across 1 and 21 tenants (above), which is real evidence Factory's
		// sweep does not change behaviour with tenant count. The structural
		// half: the request itself NAMES no tenant at all --
		// ListDueDispositionCommandsRequest has no tenant member -- so the
		// tenant count cannot enter the query by construction; what it could
		// change is how many rows a shard holds, which is what the resumed
		// passes absorb. That structural fact is not something this row
		// exercises; it is read from the request's own shape.
		t.Logf("phase one: %d passes (%d resumed) for %d commands in 1 tenant; phase two: %d passes (%d resumed) for %d commands in %d tenants",
			len(phaseOne), resumedOne, len(first), len(phaseTwo), resumedTwo, len(second), len(tenantsSeen))
		byShard := map[int]int{}
		for _, c := range second {
			byShard[shards[c.command]]++
		}
		if len(byShard) != orchestrationtest.KitControlShards {
			t.Fatalf("phase two's work landed in shards %v; the row needs due work in every shard", byShard)
		}
	})
}

// ---- case 4: gate deadline intents ------------------------------------------

// seedGate opens one legacy gate on session through store, at seq. It is the
// multi-gate sibling of openGate: openGate re-projects the catalog with no
// gates, which on a session that already has one DROPS it -- turning an open
// gate into a remnant the case did not mean to make.
func seedGate(t *testing.T, ctx context.Context, store *sessionstore.Store, fixture *orchestrationtest.StoreFixture, session sessionwire.SessionID, gate sessionwire.GateID, seq uint64, deadline time.Time) error {
	t.Helper()
	_, err := store.OpenGate(ctx, sessionstore.OpenGateRequest{
		TenantID:   fixture.Tenant,
		SessionID:  session,
		LeaseEpoch: 1,
		Gate: sessionwire.GateProjection{
			GateID:           gate,
			Kind:             "permission",
			OpenedEventID:    sessionwire.EventID(fmt.Sprintf("event-gate-%d", seq)),
			OpenedJournalSeq: seq,
			Deadline:         deadline,
			Answerability:    sessionwire.GateAnswerabilityResident,
			Prompt:           sessionwire.GatePrompt{Title: "orchestrationtest gate"},
		},
	})
	return err
}

// historyGates is how many gates case 4 opens and resolves cleanly after the
// sweep has retired the stale intents.
const historyGates = 10

// dueGateScan is one full walk of every shard's due-gate view.
type dueGateScan struct {
	examined int
	open     map[sessionwire.GateID]bool
	remnants map[sessionwire.GateID]bool
}

func scanDueGates(t *testing.T, ctx context.Context, store *orchestrationtest.StoreFixture, bound time.Time) dueGateScan {
	t.Helper()
	scan := dueGateScan{open: map[sessionwire.GateID]bool{}, remnants: map[sessionwire.GateID]bool{}}
	for shard := range orchestrationtest.KitControlShards {
		req := sessionstore.ListDueGatesRequest{Shard: shard, DueAtOrBefore: bound, Limit: 100}
		for {
			page, err := store.Store.ListDueGates(ctx, req)
			if err != nil {
				t.Fatalf("listing shard %d's due gates: %v", shard, err)
			}
			scan.examined += page.Examined
			for _, gate := range page.Gates {
				scan.open[gate.Gate.GateID] = true
			}
			for _, remnant := range page.Remnants {
				scan.remnants[remnant.GateID] = true
			}
			if page.NextCursor == "" {
				break
			}
			req = sessionstore.ListDueGatesRequest{Shard: shard, Limit: 100, Cursor: page.NextCursor}
		}
	}
	return scan
}

// TestFactoryGateSweepRetiresStaleIntents is I1.3 case 4.
//
// Three kinds of gate deadline intent are seeded on ONE session, so they share
// a shard and are ordered by deadline in one due view:
//
//   - three STILL-OPEN gates, past their deadlines, at the head of the view;
//   - a CRASH-BEFORE-OPEN intent: OpenGate made the intent durable and the
//     process died before the projection write (driven by failing that write
//     in a second real Store over the same bytes, not by writing the intent by
//     hand);
//   - a DURABLY-RESOLVED intent: ResolveGate cleared the projection and died
//     before tombstoning the intent.
//
// The two stale intents sit BEHIND more than one page of open gates, and open
// gates never leave the view -- Factory must not answer them -- so a sweep can
// reach the stale ones only by resuming from its cursor.
func TestFactoryGateSweepRetiresStaleIntents(t *testing.T) {
	ctx := coldReadContext(t)
	clock := orchestrationtest.NewClock(time.Unix(coldReadEpoch, 0))
	store := orchestrationtest.NewStoreFixture(t, ctx, clock)
	crashing, faults := store.OpenCrashingStore(ctx)

	session := store.SeedSession(ctx, blockedAgent, string(blockedCompatibility))
	var seq uint64
	// Every event any gate below names is committed up front and the catalog
	// advanced ONCE. Advancing it again later would re-project the session's
	// open gates wholesale and drop them, manufacturing remnants.
	const events = 5 + historyGates
	for i := 1; i <= events; i++ {
		seq = store.AppendPublicEvent(ctx, session, sessionwire.EventID(fmt.Sprintf("event-gate-%d", i)), []byte(fmt.Sprintf(`{"n":%d}`, i)))
	}
	store.AdvanceCatalogJournal(ctx, session, 1, seq, sessionwire.EventID(fmt.Sprintf("event-gate-%d", events)), nil)

	now := clock.Now()
	open := []sessionwire.GateID{"gate-open-1", "gate-open-2", "gate-open-3"}
	for i, gate := range open {
		if err := seedGate(t, ctx, store.Store, store, session, gate, uint64(i+1), now.Add(time.Duration(i-60)*time.Minute)); err != nil {
			t.Fatalf("opening %s: %v", gate, err)
		}
	}
	const crashed, resolved = sessionwire.GateID("gate-crash-before-open"), sessionwire.GateID("gate-durably-resolved")

	faults.FailUpdates(true)
	if err := seedGate(t, ctx, crashing, store, session, crashed, 4, now.Add(-30*time.Minute)); err == nil {
		t.Fatalf("the open survived its projection write failing; the crash point was not reached")
	}
	faults.FailUpdates(false)

	if err := seedGate(t, ctx, store.Store, store, session, resolved, 5, now.Add(-20*time.Minute)); err != nil {
		t.Fatalf("opening %s: %v", resolved, err)
	}
	faults.FailDeletes(true)
	if _, err := crashing.ResolveGate(ctx, sessionstore.ResolveGateRequest{
		TenantID: store.Tenant, SessionID: session, LeaseEpoch: 1, GateID: resolved,
	}); err == nil {
		t.Fatalf("the resolve survived its intent tombstone failing; the crash point was not reached")
	}
	faults.FailDeletes(false)
	if faults.Fired() != 2 {
		t.Fatalf("%d crash points fired, want 2", faults.Fired())
	}

	// The durable state the crashes left, read before any Factory exists.
	before := scanDueGates(t, ctx, store, clock.Now())
	for _, gate := range open {
		if !before.open[gate] {
			t.Fatalf("%s is not an open due gate; the fixture is wrong", gate)
		}
	}
	if !before.remnants[crashed] || !before.remnants[resolved] {
		t.Fatalf("the crashes left remnants %v, want %s and %s", before.remnants, crashed, resolved)
	}
	if len(open) <= dispositionPageLimit {
		t.Fatalf("%d open gates do not fill a %d-row page; the cursor half cannot discriminate", len(open), dispositionPageLimit)
	}

	replica := orchestrationtest.NewFactoryFixtureWithSeams(t, store, clock, orchestrationtest.FactorySeams{
		ReplicaID: "orchestrationtest-gate-replica",
		Reconcile: boundedReconcileLimits(),
	})
	gates := replica.Gates

	// A remnant younger than MinGateIntentRemnantAge is indistinguishable from
	// an open in flight, and the store refuses to retire it. The sweep runs
	// over it several times first; nothing may be retired yet.
	awaitDueGates(t, gates, 4*orchestrationtest.KitControlShards)
	if got := scanDueGates(t, ctx, store, clock.Now()); !got.remnants[crashed] || !got.remnants[resolved] {
		t.Fatalf("a remnant was retired inside its in-flight window: %v", got.remnants)
	}

	clock.Advance(sessionstore.MinGateIntentRemnantAge + time.Minute)
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := scanDueGates(t, ctx, store, clock.Now())
		if len(got.remnants) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after 30s the gate sweep had not retired remnants %v; retired requests %v", got.remnants, gates.Retired())
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Run("case 4: stale intents retire, through Factory's own sweep", func(t *testing.T) {
		retired := map[sessionwire.GateID]bool{}
		for _, req := range gates.Retired() {
			retired[req.GateID] = true
		}
		if !retired[crashed] || !retired[resolved] {
			t.Fatalf("Factory's sweep retired %v; want both %s and %s", retired, crashed, resolved)
		}
		for _, gate := range open {
			if retired[gate] {
				t.Fatalf("Factory's sweep asked to retire the still-open gate %s", gate)
			}
		}
	})

	t.Run("case 4: open gates are not resolved by Factory", func(t *testing.T) {
		page, err := store.Store.ReadGates(ctx, sessionstore.ReadGatesRequest{TenantID: store.Tenant, SessionID: session})
		if err != nil {
			t.Fatalf("reading gates: %v", err)
		}
		projected := map[sessionwire.GateID]bool{}
		for _, gate := range page.Gates {
			projected[gate.GateID] = true
		}
		for _, gate := range open {
			if !projected[gate] {
				t.Fatalf("the still-open gate %s left the projection", gate)
			}
		}
		if page.OpenGateCount != uint64(len(open)) {
			t.Fatalf("OpenGateCount is %d, want %d", page.OpenGateCount, len(open))
		}
	})

	t.Run("case 4: cursors reach work behind a full page of open gates", func(t *testing.T) {
		// Open gates never leave the view and sort first, so every page read
		// from the head of this shard is open gates and nothing else. The
		// remnants were retired, so the sweep reached them -- and it can only
		// have done that by resuming. This is the continuation being USED.
		resumed := 0
		for _, req := range gates.DueRequests() {
			if req.Limit != dispositionPageLimit {
				t.Fatalf("a gate query asked for %d rows; the composed page limit is %d", req.Limit, dispositionPageLimit)
			}
			if req.Cursor != "" {
				resumed++
			}
		}
		if resumed == 0 {
			t.Fatalf("the gate sweep never resumed from a cursor, yet retired rows behind a full page")
		}
	})

	t.Run("case 4: retired gates do not accumulate in due pages", func(t *testing.T) {
		// STORE PROPERTY, not a Factory behaviour: the ten history gates are
		// tombstoned by the store's own ResolveGate, not retired by Factory's
		// sweep (that is the row above, over RetireGateDeadlineIntent
		// remnants). This row is evidence that sessionstore's own due view
		// does not re-surface a resolved gate's tombstone as a row a page
		// pays for -- Factory never touches these ten.
		//
		// History is made on purpose: historyGates more gates opened and resolved
		// cleanly, each leaving a tombstone. The due view must still examine
		// exactly the three open gates -- tombstones are not rows a page pays
		// for, or the view would slow down forever as a session ages.
		for i := range historyGates {
			gate := sessionwire.GateID(fmt.Sprintf("gate-history-%02d", i))
			if err := seedGate(t, ctx, store.Store, store, session, gate, uint64(6+i), clock.Now().Add(-time.Minute)); err != nil {
				t.Fatalf("opening %s: %v", gate, err)
			}
			if _, err := store.Store.ResolveGate(ctx, sessionstore.ResolveGateRequest{
				TenantID: store.Tenant, SessionID: session, LeaseEpoch: 1, GateID: gate,
			}); err != nil {
				t.Fatalf("resolving %s: %v", gate, err)
			}
		}
		after := scanDueGates(t, ctx, store, clock.Now().Add(time.Hour))
		if after.examined != len(open) || len(after.open) != len(open) || len(after.remnants) != 0 {
			t.Fatalf("the due view examined %d rows (%d open, %d remnants) after retirement and ten resolved gates, want exactly the %d open ones",
				after.examined, len(after.open), len(after.remnants), len(open))
		}
	})
}

// TestFactoryGateSweepRetiresADispositionRemnant is I1.3 case 4 on the
// sessions production actually gates on.
//
// Since host v0.4.0 every gate a Host publishes is on a DISPOSITION session,
// written under a store-issued residency grant. A crash between OpenGate's
// intent write and its projection write leaves a remnant there exactly as it
// does on a legacy session.
//
// THIS ROW WAS A TRIP-WIRE until the sessionstore v0.13.0 pin. v0.12.0's
// RetireGateDeadlineIntent reserved the LEGACY protocol mode before deleting,
// so on a disposition-bound session it refused `catalog conflict
// (binding.protocol_mode)` forever, Factory's sweep asked again every pass, and
// disposition remnants accumulated in the due view for the life of the session.
//
// v0.13.0 PARKS a disposition remnant instead of tombstoning it: the unchanged
// intent bytes are re-filed NOT-DUE (so a later OpenGate can revive them). So
// this row asserts what retirement means on a disposition session -- the
// remnant LEFT THE DUE VIEW and the sweep STOPPED asking about it -- and
// deliberately does NOT assert a tombstone: the parked row still exists, and a
// retire of it again is an idempotent success that writes nothing.
func TestFactoryGateSweepRetiresADispositionRemnant(t *testing.T) {
	ctx := coldReadContext(t)
	w := newDispositionWorld(t, ctx, 1)
	created := w.create(t, ctx, orchestrationtest.KitActorCredential, "gate-disposition")
	crashing, faults := w.store.OpenCrashingStore(ctx)
	grant, err := crashing.AcquireResidency(ctx, sessionstore.AcquireResidencyRequest{TenantID: created.tenant, SessionID: created.session})
	if err != nil {
		t.Fatalf("acquiring residency on the disposition session: %v", err)
	}
	const gate = sessionwire.GateID("gate-disposition-crash-before-open")
	faults.FailUpdates(true)
	if _, err := crashing.OpenGate(ctx, sessionstore.OpenGateRequest{
		TenantID: created.tenant, SessionID: created.session, Residency: grant,
		Gate: sessionwire.GateProjection{
			GateID: gate, Kind: "permission", OpenedEventID: "event-disposition-gate", OpenedJournalSeq: 1,
			Deadline: w.clock.Now().Add(-time.Minute), Answerability: sessionwire.GateAnswerabilityResident,
			Prompt: sessionwire.GatePrompt{Title: "orchestrationtest gate"},
		},
	}); err == nil {
		t.Fatalf("the open survived its projection write failing; the crash point was not reached")
	}
	faults.FailUpdates(false)
	var captured *sessionstore.RemnantGateIntent
	for _, remnant := range scanDueGatesRemnants(t, ctx, w.store, w.clock.Now()) {
		if remnant.GateID == gate {
			captured = &remnant
		}
	}
	if captured == nil {
		t.Fatalf("the crash left no remnant on the disposition session; the fixture is wrong")
	}

	w.clock.Advance(sessionstore.MinGateIntentRemnantAge + time.Minute)
	orchestrationtest.PooledWait(t, "the disposition remnant left the due view", 30*time.Second, func() bool {
		return !scanDueGates(t, ctx, w.store, w.clock.Now().Add(time.Hour)).remnants[gate]
	})
	asked := func() int {
		n := 0
		for _, req := range w.replica.Gates.Retired() {
			if req.GateID == gate {
				n++
			}
		}
		return n
	}
	if asked() == 0 {
		t.Fatalf("the remnant left the due view but Factory's sweep never asked to retire it; something else retired it")
	}
	// Several more full passes: a retired remnant is no longer listed, so the
	// sweep must stop asking. Under v0.12.0 this count grew every pass.
	settled := asked()
	awaitDueGates(t, w.replica.Gates, len(w.replica.Gates.DueRequests())+2*orchestrationtest.KitControlShards)
	if again := asked(); again != settled {
		t.Fatalf("the sweep asked to retire the remnant %d more time(s) after it left the due view", again-settled)
	}
	if scanDueGates(t, ctx, w.store, w.clock.Now().Add(time.Hour)).remnants[gate] {
		t.Fatalf("the disposition remnant returned to the due view")
	}
	// Parked, not tombstoned: retiring it again is the store's idempotent
	// repeat case.
	if err := w.store.Store.RetireGateDeadlineIntent(ctx, sessionstore.RetireGateDeadlineIntentRequest(*captured)); err != nil {
		t.Fatalf("re-retiring the parked disposition remnant answered %v, want the idempotent success", err)
	}
}

// scanDueGatesRemnants returns every remnant in every shard's due view.
func scanDueGatesRemnants(t *testing.T, ctx context.Context, store *orchestrationtest.StoreFixture, bound time.Time) []sessionstore.RemnantGateIntent {
	t.Helper()
	var remnants []sessionstore.RemnantGateIntent
	for shard := range orchestrationtest.KitControlShards {
		req := sessionstore.ListDueGatesRequest{Shard: shard, DueAtOrBefore: bound, Limit: 100}
		for {
			page, err := store.Store.ListDueGates(ctx, req)
			if err != nil {
				t.Fatalf("listing shard %d's due gates: %v", shard, err)
			}
			remnants = append(remnants, page.Remnants...)
			if page.NextCursor == "" {
				break
			}
			req = sessionstore.ListDueGatesRequest{Shard: shard, Limit: 100, Cursor: page.NextCursor}
		}
	}
	return remnants
}
