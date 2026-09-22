//go:build integration

// This file is runbook 07 task I1.3 case 1 and the PLACEMENT half of case 3,
// driven across real Factory replicas, a real pooled Host and the real
// SessionStore. Nothing here stands in for either service: the HostLink tap is
// a passive observer of the wire, ReplicaCommands records what a replica's
// sweepers asked the real Store, and SlowDirectory adds latency to the real
// directory read placement makes between claiming a session and attaching it.

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
	"github.com/looprig/factory"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// reconcileCreate posts one create with a first message through replica.
func reconcileCreate(t *testing.T, replica *orchestrationtest.ReconcileReplica, session sessionwire.SessionID, command sessionwire.CommandID) {
	t.Helper()
	status, body := replica.Post(t, t.Context(), orchestrationtest.PooledTenantA, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)),
		SessionID:       session,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(`[{"type":"text","text":"hello"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("replica %s answered the create of %s with %d: %s", replica.ID, session, status, body)
	}
}

// highestTipHint returns the largest journal_tip hint a viewer received, and
// whether it received any.
func highestTipHint(records []string) (uint64, bool) {
	var best uint64
	found := false
	for _, record := range records {
		var tip uint64
		if _, err := fmt.Sscanf(record, "T%d", &tip); err == nil && strings.HasPrefix(record, "T") {
			found = true
			best = max(best, tip)
		}
	}
	return best, found
}

// TestFactorySubscriberDemandFindsATipAppliedElsewhere is I1.3 CASE 1.
//
// A browser watches a session through Factory A. The work is accepted by
// Factory B and applied by the Host B placed it on. Factory A has NO HostLink:
// it presents a service credential the Host refuses, so every attempt its
// demand plane makes to bind the session fails at the connect -- while its
// directory, its store and its ClientLink are real. Nothing crosses from B to
// A. The only way A's viewer can learn how far the session has got is A's own
// subscriber-demand poll reading the durable tip and publishing the
// journal_tip hint, and it must do so within the bound Factory composed:
// one ownership-poll gap plus one poll.
func TestFactorySubscriberDemandFindsATipAppliedElsewhere(t *testing.T) {
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{tenant},
	})
	tap := orchestrationtest.NewHostLinkTap()
	pooled := orchestrationtest.StartTappedPooledHost(t, ctx, world, "orchestrationtest-demand-host", 4, tap)
	orchestrationtest.AwaitAdvertised(t, world, pooled.ID)

	const refusedToken = "orchestrationtest-replica-a-has-no-hostlink"
	watcher := orchestrationtest.StartReconcileReplica(t, ctx, world, orchestrationtest.ReconcileReplicaOptions{
		Replica:                "orchestrationtest-demand-a",
		ServiceToken:           refusedToken,
		WithoutPendingCommands: true,
	})
	applier := orchestrationtest.StartReconcileReplica(t, ctx, world, orchestrationtest.ReconcileReplicaOptions{
		Replica: "orchestrationtest-demand-b",
	})

	session := sessionwire.SessionID("session-demand")
	viewer := orchestrationtest.ConnectPooledViewer(t, ctx, watcher.PooledFactory, tenant)
	if err := viewer.Watch(t, ctx, tenant, session); err != nil {
		t.Fatalf("the viewer's subscribe to replica A was refused: %v", err)
	}

	reconcileCreate(t, applier, session, "command-demand-create")
	orchestrationtest.PooledWait(t, "replica B's create applied on the Host", 90*time.Second, func() bool {
		return world.CommandState(ctx, tenant, session, "command-demand-create") == sessionstore.InboxStateApplied
	})
	orchestrationtest.PooledWait(t, "the Host committed the create's publications", 30*time.Second, func() bool {
		return world.Tails.Tip(tenant, session) == orchestrationtest.PooledPublicationsPerInput
	})
	tip := world.Tails.Tip(tenant, session)

	// THE BOUND IS FACTORY'S OWN COMPOSITION, restated from its inputs: the
	// next poll is armed OwnershipPollInterval after the last one FINISHED, and
	// one poll -- the failing bind, the tip read and the publish -- is bounded
	// by DemandTimeout. A tip that moved just after a hint was read is therefore
	// in the next hint at most one gap plus one poll later. The 250ms is
	// scheduling slack for this process, not a Factory figure.
	bound := watcher.OwnershipPollInterval + watcher.DemandTimeout + 250*time.Millisecond
	elapsed := orchestrationtest.PooledWait(t, fmt.Sprintf("replica A's viewer learned tip %d", tip), bound, func() bool {
		got, ok := highestTipHint(viewer.Records())
		return ok && got >= tip
	})
	t.Logf("case 1: replica A's viewer learned tip %d %s after it was committed (bound %s); records %v",
		tip, elapsed, bound, viewer.Records())

	t.Run("A delivered nothing but hints: the tip did not arrive over a HostLink", func(t *testing.T) {
		for _, record := range viewer.Records() {
			if !strings.HasPrefix(record, "T") {
				t.Fatalf("replica A's viewer received %s; A has no HostLink, so anything but a journal_tip hint came from somewhere this case says cannot exist", record)
			}
		}
		if strays := viewer.Strays(); len(strays) != 0 {
			t.Fatalf("the viewer received records naming another session: %v", strays)
		}
	})

	t.Run("A tried to reach the Host and never opened a link", func(t *testing.T) {
		// "No HostLink on A" is engineered at the LINK, and this is what makes
		// it a precondition rather than an assumption. A replica that simply
		// never looked for the owner would also have no link -- and would also
		// never hint. So A's attempts must EXIST, and every one must have been
		// refused at the connect with nothing sent on it.
		var attempts []orchestrationtest.TappedLink
		orchestrationtest.PooledWait(t, "replica A attempted a HostLink", 10*time.Second, func() bool {
			attempts = attempts[:0]
			for _, link := range orchestrationtest.TappedLinks(t, tap) {
				if link.Token == refusedToken {
					attempts = append(attempts, link)
				}
			}
			return len(attempts) > 0
		})
		for _, link := range attempts {
			if link.Connected || len(link.Methods) != 0 || len(link.Subscribes) != 0 {
				t.Fatalf("replica A's connection %d was accepted (%t) or carried %v / %v", link.Conn, link.Connected, link.Methods, link.Subscribes)
			}
		}
		// And B, by contrast, did link, attach and bind: the Host was
		// reachable, so A's failure is A's credential and nothing else.
		boundByB := false
		for _, link := range orchestrationtest.TappedLinks(t, tap) {
			if link.Token == orchestrationtest.PooledServiceToken && link.Connected {
				for _, method := range link.Methods {
					boundByB = boundByB || method == sessionwire.HostLinkMethodBind
				}
			}
		}
		if !boundByB {
			t.Fatalf("replica B never bound the session over a HostLink; the case's control is missing")
		}
	})
}

// attachesPerSession tallies the attaches sent, and those accepted, per session.
func attachesPerSession(t *testing.T, tap *orchestrationtest.HostLinkTap) (map[sessionwire.SessionID]int, map[sessionwire.SessionID]int) {
	t.Helper()
	sent, accepted := map[sessionwire.SessionID]int{}, map[sessionwire.SessionID]int{}
	for _, attach := range orchestrationtest.TappedAttaches(t, tap) {
		sent[attach.Request.SessionID]++
		if attach.Accepted {
			accepted[attach.Request.SessionID]++
		}
	}
	return sent, accepted
}

// TestFactoryReplicasDoNotDuplicatePlacement is I1.3 CASE 3, the placement
// half: two Factory replicas running AT THE SAME TIME over one durable plane
// and one pooled Host, both sweeping the same pending work.
//
// # How the contention is made certain rather than likely
//
// Factory bounds every sweep pass by the sweep interval, so a replica can hold
// a claim open for at most one interval, and two independently-phased sweeps
// may simply never overlap inside that window. A row that relied on them
// overlapping would pass, some of the time, for a Factory with no suppression.
//
// So the overlap is arranged. Replica A sweeps slowly (a 2s interval) and its
// directory read -- the one placement makes AFTER taking the claim and BEFORE
// attaching -- is a RENDEZVOUS: it waits, inside A's claim, until replica B's
// sweeper has been observed attempting a claim and being REFUSED, bounded at
// 1.5s. Replica B is started only once A is inside its claim, and sweeps fast.
// B's first pass therefore meets A's live claim on the session A holds. With
// distinct holders the store refuses B, the rendezvous releases A, and A
// attaches alone. With a shared holder the store treats B's acquire as an
// EXTENSION: B attaches, the rendezvous times out, and A attaches the same
// session a second time -- the duplicate this row exists to catch.
func TestFactoryReplicasDoNotDuplicatePlacement(t *testing.T) {
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{tenant},
	})
	tap := orchestrationtest.NewHostLinkTap()
	pooled := orchestrationtest.StartTappedPooledHost(t, ctx, world, "orchestrationtest-placement-host", 8, tap)
	orchestrationtest.AwaitAdvertised(t, world, pooled.ID)

	commandsA := orchestrationtest.NewReplicaCommands(world.Store)
	commandsB := orchestrationtest.NewReplicaCommands(world.Store)
	var rendezvous *orchestrationtest.SlowDirectory
	replicaA := orchestrationtest.StartReconcileReplica(t, ctx, world, orchestrationtest.ReconcileReplicaOptions{
		Replica:  "orchestrationtest-place-a",
		Commands: commandsA,
		Interval: 2 * time.Second,
		ClaimTTL: 4 * time.Second,
		Directory: func(inner factory.Directory) factory.Directory {
			rendezvous = &orchestrationtest.SlowDirectory{Inner: inner, Hold: func(ctx context.Context) {
				mark := len(commandsB.Claims())
				deadline := time.Now().Add(1500 * time.Millisecond)
				for time.Now().Before(deadline) && ctx.Err() == nil {
					for _, claim := range commandsB.Claims()[mark:] {
						if !claim.Held {
							return
						}
					}
					time.Sleep(5 * time.Millisecond)
				}
			}}
			return rendezvous
		},
	})

	const sessions = 3
	var ids []sessionwire.SessionID
	for i := range sessions {
		session := sessionwire.SessionID(fmt.Sprintf("session-place-%d", i))
		reconcileCreate(t, replicaA, session, sessionwire.CommandID(fmt.Sprintf("command-place-%d", i)))
		ids = append(ids, session)
	}
	var contested sessionwire.SessionID
	orchestrationtest.PooledWait(t, "replica A holds a claim and is inside it", 30*time.Second, func() bool {
		for _, claim := range commandsA.Claims() {
			if claim.Held && rendezvous.Entered() > 0 {
				contested = claim.Request.SessionID
				return true
			}
		}
		return false
	})
	replicaB := orchestrationtest.StartReconcileReplica(t, ctx, world, orchestrationtest.ReconcileReplicaOptions{
		Replica: "orchestrationtest-place-b", Commands: commandsB,
	})

	for i, session := range ids {
		command := sessionwire.CommandID(fmt.Sprintf("command-place-%d", i))
		orchestrationtest.PooledWait(t, "the create of "+string(session)+" applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, session, command) == sessionstore.InboxStateApplied
		})
	}

	t.Run("each replica claims under its own identity", func(t *testing.T) {
		// The claim is the only thing that suppresses duplicate placement, and
		// it suppresses nothing between two replicas filing under one string:
		// the store treats a second acquire by the same holder as an extension.
		for _, replica := range []struct {
			id     string
			claims []orchestrationtest.RecordedClaim
		}{{replicaA.ID, commandsA.Claims()}, {replicaB.ID, commandsB.Claims()}} {
			if len(replica.claims) == 0 {
				t.Fatalf("replica %s attempted no placement claim; both replicas must be sweeping for the row to discriminate", replica.id)
			}
			for _, claim := range replica.claims {
				if claim.Request.HolderID != replica.id {
					t.Fatalf("replica %s's placement claimed %s under %q, want its own replica id",
						replica.id, claim.Request.SessionID, claim.Request.HolderID)
				}
			}
		}
	})

	t.Run("B met A's live claim and was refused", func(t *testing.T) {
		// Without this refusal the two sweeps never contended, and "no
		// duplicate" below would be true for a Factory with no suppression.
		refused := 0
		for _, claim := range commandsB.Claims() {
			if claim.Request.SessionID == contested && !claim.Held {
				refused++
			}
		}
		if refused == 0 {
			t.Fatalf("replica B was never refused %s while A held it; the replicas did not contend", contested)
		}
		t.Logf("case 3: B was refused %d times on %s, which A held", refused, contested)
	})

	t.Run("exactly one attach and one launch per session", func(t *testing.T) {
		// Duplicate placement is judged by attach REQUESTS sent and Host
		// launches, not by accepted replies: a placement pass is bounded
		// (200ms as of factory v0.6.0), so under load an attach's reply can
		// legitimately arrive after the tap is read even though exactly one
		// attach was sent and residency took effect once. The accepted-reply
		// count stays informational.
		sent, accepted := attachesPerSession(t, tap)
		attaches := orchestrationtest.TappedAttaches(t, tap)
		defer func() {
			if t.Failed() {
				for _, attach := range attaches {
					t.Logf("recorded attach: %v", attach)
				}
			}
		}()
		for _, session := range ids {
			if sent[session] != 1 {
				t.Fatalf("session %s: %d attach requests were sent, want exactly 1 (two replicas placed it)", session, sent[session])
			}
			if accepted[session] != 1 {
				t.Logf("session %s: %d attach(es) sent, accepted-reply observed for %d (informational only; a reply can be missed under load)", session, sent[session], accepted[session])
			}
		}
		launches := map[string]int{}
		for _, launch := range pooled.Rig.Creates() {
			launches[launch.ID.String()]++
		}
		if len(pooled.Rig.Creates()) != sessions {
			t.Fatalf("the Host launched %d runtimes for %d sessions", len(pooled.Rig.Creates()), sessions)
		}
		for _, session := range ids {
			if id := world.RuntimeSessionID(t, ctx, tenant, session).String(); launches[id] != 1 {
				t.Fatalf("session %s's runtime %s was launched %d times", session, id, launches[id])
			}
		}
	})
}

// TestFactoryReplicaCrashMidClaimIsRecoveredAfterClaimTTL is I1.3 CASE 3, the
// crash/recover half, driven through Factory rather than through the store's
// compare-and-swap.
//
// Replica A takes a session's placement claim and hangs inside the claim --
// its directory read never returns -- and then CRASHES: its durable plane stops
// dead, so it releases nothing. Replica B starts. It must meet A's claim as
// live and defer, and it must place the session once the claim lapses at its
// TTL, not before and not never.
func TestFactoryReplicaCrashMidClaimIsRecoveredAfterClaimTTL(t *testing.T) {
	ctx := placementContext(t)
	tenant := orchestrationtest.PooledTenantA
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{tenant},
	})
	tap := orchestrationtest.NewHostLinkTap()
	pooled := orchestrationtest.StartTappedPooledHost(t, ctx, world, "orchestrationtest-crash-host", 4, tap)
	orchestrationtest.AwaitAdvertised(t, world, pooled.ID)

	crashed := orchestrationtest.NewReplicaCommands(world.Store)
	var hung *orchestrationtest.SlowDirectory
	replicaA := orchestrationtest.StartReconcileReplica(t, ctx, world, orchestrationtest.ReconcileReplicaOptions{
		Replica:  "orchestrationtest-crash-a",
		Commands: crashed,
		// Widened from the default 2s ReconcileClaimTTL: B's first claim
		// attempt lands roughly 1.4-1.6s after A's claim (a fixed
		// shutdown/startup cost, not CPU), which left only ~0.4-0.55s of
		// margin against a 2s TTL before "B was never refused" -- meaning
		// A's claim had already lapsed by the time B's first attempt landed,
		// so recovery-after-expiry was never actually exercised. 5s keeps
		// comfortable margin while total runtime stays reasonable (B still
		// only waits for A's claim to lapse, not the full TTL from test
		// start).
		ClaimTTL: 5 * time.Second,
		Directory: func(inner factory.Directory) factory.Directory {
			hung = &orchestrationtest.SlowDirectory{Inner: inner}
			hung.Block()
			return hung
		},
	})
	const session, command = sessionwire.SessionID("session-crash"), sessionwire.CommandID("command-crash-create")
	reconcileCreate(t, replicaA, session, command)

	var held orchestrationtest.RecordedClaim
	orchestrationtest.PooledWait(t, "replica A holds the claim and is inside it", 30*time.Second, func() bool {
		for _, claim := range crashed.Claims() {
			if claim.Held && claim.Request.SessionID == session {
				held = claim
				return hung.Entered() > 0
			}
		}
		return false
	})
	crashed.Crash()
	replicaA.Stop()
	if claim, err := world.Store.GetReconciliationClaim(ctx, sessionstore.GetReconciliationClaimRequest{TenantID: tenant, SessionID: session}); err != nil ||
		claim.Claim.HolderID != replicaA.ID {
		t.Fatalf("after A's crash the durable claim is %+v (%v); want A's, left to lapse", claim.Claim, err)
	}

	survivor := orchestrationtest.NewReplicaCommands(world.Store)
	replicaB := orchestrationtest.StartReconcileReplica(t, ctx, world, orchestrationtest.ReconcileReplicaOptions{
		Replica: "orchestrationtest-crash-b", Commands: survivor,
	})
	orchestrationtest.PooledWait(t, "replica B placed and applied the session", 60*time.Second, func() bool {
		return world.CommandState(ctx, tenant, session, command) == sessionstore.InboxStateApplied
	})

	t.Run("B deferred to A's live claim and took it only after it lapsed", func(t *testing.T) {
		refusedBefore, won := 0, false
		for _, claim := range survivor.Claims() {
			if claim.Request.SessionID != session {
				continue
			}
			if claim.Request.HolderID != replicaB.ID {
				t.Fatalf("replica B claimed under %q", claim.Request.HolderID)
			}
			if !claim.Held {
				if won {
					continue
				}
				refusedBefore++
				continue
			}
			if !won {
				won = true
				if claim.Claim.ClaimedAt.Before(held.Claim.ExpiresAt) {
					t.Fatalf("replica B took the claim at %v, before A's lapsed at %v", claim.Claim.ClaimedAt, held.Claim.ExpiresAt)
				}
			}
		}
		if !won {
			t.Fatalf("replica B never held the claim, yet the session was placed")
		}
		if refusedBefore == 0 {
			t.Fatalf("replica B was never refused: A's claim was not live when B arrived, so recovery-after-expiry was not exercised")
		}
		t.Logf("case 3: B was refused %d times, then claimed after A's claim lapsed at %v", refusedBefore, held.Claim.ExpiresAt)
	})

	t.Run("the session was placed exactly once, by B", func(t *testing.T) {
		// As in the duplicate-placement row: judge this by attach REQUESTS
		// sent and Host launches, not by an accepted reply. A placement pass
		// is bounded, so under load the reply to a lone attach can arrive
		// after the tap is read even though the session was placed exactly
		// once; treat Accepted as informational.
		attaches := orchestrationtest.TappedAttaches(t, tap)
		defer func() {
			if t.Failed() {
				for _, attach := range attaches {
					t.Logf("recorded attach: %v", attach)
				}
			}
		}()
		sent := 0
		accepted := 0
		for _, attach := range attaches {
			if attach.Request.SessionID != session {
				continue
			}
			sent++
			if attach.Accepted {
				accepted++
			}
		}
		if sent != 1 {
			t.Fatalf("the Host saw %d attach requests for %s, want exactly 1 (two replicas placed it)", sent, session)
		}
		if accepted != 1 {
			t.Logf("session %s: 1 attach sent, accepted-reply observed for %d (informational only; a reply can be missed under load)", session, accepted)
		}
		if len(pooled.Rig.Creates()) != 1 {
			t.Fatalf("the Host launched %d runtimes, want 1", len(pooled.Rig.Creates()))
		}
	})
}
