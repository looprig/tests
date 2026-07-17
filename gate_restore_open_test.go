//go:build integration

// Gates that were still OPEN at shutdown and restore: a Session whose process was
// killed mid-elicitation (a host form/open-url gate raised but never answered) or
// mid-permission (a loop gate parked but never decided) must come back cleanly.
//
// On restore, foldRestoredGates finds these dangling-open gates and
// appendRestoreUnavailableGates synthesizes a GateResolved{Reason:
// CloseRestoreUnavailable} for each, durably appended through MarshalEvent. That
// append validates identity against the resolver-selected profile (the #2 fix):
//
//   - A host-owned gate (gate.ResolverSession, form/open-url) carries no
//     TurnID/StepID, so it validates ONLY under hostGateProfile — which requires
//     the synthesized GateResolved to carry Resolver: ResolverSession. Omitting the
//     resolver defaults it to the strict stepProfile, whose required TurnID/StepID
//     the host gate lacks, so the append fails and restore fails. That is the
//     regression this file pins.
//   - A loop-owned gate (gate.ResolverLoop, permission) keeps the full step
//     profile: it carries a real turn+step, so its unavailable-resolution validates
//     either way. This file confirms the fix did NOT weaken that path.
//
// The proof restores TWICE. The first restore appends the synthesized GateResolved;
// the second restore must read that appended record back through the identity-
// validating decode path, so a record that marshalled clean but cannot survive its
// own re-decode is caught here and not merely at write time.
//
// These are fsstore-backed on purpose: only a real journal + RestoreSession reach
// the marshal/decode boundary the value-level pkg/event tests cannot.

package tests

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/harness/pkg/tool"
)

// gatePreparer is the loop-owned gate seam the concrete controller exposes: it lets
// a test park a permission gate (PrepareGateOpen + ActivateGate) without driving a
// full tool-permission turn, mirroring pkg/rig/lifecycle_test.go's "open gate cap".
type gatePreparer interface {
	PrepareGateOpen(context.Context, uuid.UUID, gate.Gate, gate.Payload) (gate.ID, error)
	ActivateGate(context.Context, gate.ID, gate.Route) error
}

// TestRestoreSurvivesDanglingHostGates leaves three host-owned gates OPEN at
// shutdown — a form gate with turn+step, an open-url gate with a turn but no step,
// and a startup form gate with no loop at all — then restores twice. Before the fix
// the FIRST restore fails at the synthesized-GateResolved append with
// "invalid GateResolved: TurnID must be set" (the host gate has no turn/step and the
// omitted resolver holds it to stepProfile). After the fix both restores are clean.
func TestRestoreSurvivesDanglingHostGates(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	persistence := filepath.Join(t.TempDir(), "persistence")

	var id uuid.UUID
	func() {
		stores := openFSStores(t, persistence)
		defer func() { _ = stores.fs.Close() }()
		r := defineSessionRig(t, stores, filepath.Join(t.TempDir(), "ws-one"), false, rig.SnapshotPolicy{Trigger: rig.SnapshotManual})
		sess, err := r.NewSession(ctx)
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		shutdownSess := registerSessionCleanup(t, sess)
		id = sess.SessionID()
		loopID := sess.ActiveLoop().ID()

		host, ok := sess.(session.GateHost)
		if !ok {
			t.Fatal("session controller does not implement session.GateHost")
		}

		turnID, err := uuid.New()
		if err != nil {
			t.Fatal(err)
		}
		stepID, err := uuid.New()
		if err != nil {
			t.Fatal(err)
		}
		formPayload := gate.FormPayload{Title: "Sign in", Schema: gateHostRestoreSchema()}

		// (1) Form gate, turn+step, left OPEN (never answered).
		formEnvelope := gate.Gate{
			Kind:     gate.KindForm,
			Resolver: gate.ResolverSession,
			Blocks:   gate.BlocksToolCall,
			Effect:   gate.EffectResume,
			Subject:  gate.Subject{TurnID: turnID, StepID: stepID},
			Prompt: gate.Prompt{
				Title:    "Sign in",
				Schema:   gateHostRestoreSchema(),
				Controls: gateHostFormControls(),
			},
		}
		if _, err := host.OpenHostGate(ctx, loopID, formEnvelope, formPayload); err != nil {
			t.Fatalf("OpenHostGate(form): %v", err)
		}

		// (2) Open-url gate, turn but NO step, left OPEN.
		openURLEnvelope := gate.Gate{
			Kind:     gate.KindOpenURL,
			Resolver: gate.ResolverSession,
			Blocks:   gate.BlocksToolCall,
			Effect:   gate.EffectResume,
			Subject:  gate.Subject{TurnID: turnID},
			Prompt: gate.Prompt{
				Title:    "Authorize access",
				Controls: gateHostFormControls(),
			},
		}
		openURLPayload := gate.OpenURLPayload{
			DisplayOrigin:      "https://github.com",
			URL:                "https://github.com/login/oauth/authorize?state=SECRET",
			RequiresCompletion: true,
		}
		if _, err := host.OpenHostGate(ctx, loopID, openURLEnvelope, openURLPayload); err != nil {
			t.Fatalf("OpenHostGate(open-url): %v", err)
		}

		// (3) Startup form gate, zero LoopID, left OPEN.
		startupEnvelope := gate.Gate{
			Kind:     gate.KindForm,
			Resolver: gate.ResolverSession,
			Blocks:   gate.BlocksSession,
			Effect:   gate.EffectResume,
			Prompt: gate.Prompt{
				Title:    "Connect account",
				Schema:   gateHostRestoreSchema(),
				Controls: gateHostFormControls(),
			},
		}
		if _, err := host.OpenHostGate(ctx, uuid.UUID{}, startupEnvelope, formPayload); err != nil {
			t.Fatalf("OpenHostGate(startup, zero loop): %v", err)
		}

		// The gates really are dangling-open: three GateOpened, zero GateResolved.
		before := eventsFor(t, ctx, stores.sessions, id)
		if got := countEvents[event.GateOpened](before); got != 3 {
			t.Fatalf("journal has %d GateOpened, want 3 open host gates", got)
		}
		if got := countEvents[event.GateResolved](before); got != 0 {
			t.Fatalf("journal has %d GateResolved, want 0 (gates left open)", got)
		}

		if err := shutdownSess(ctx); err != nil {
			t.Fatalf("shutdown leg one: %v", err)
		}
	}()

	// First restore: this is the append that fails pre-fix. It synthesizes three
	// GateResolved{CloseRestoreUnavailable}, one per dangling host gate.
	restoreDanglingLeg(t, ctx, persistence, "ws-two", id, 3)

	// Second restore: the appended GateResolved records are now in the journal and
	// must survive their own identity-validating re-decode on replay. A record with
	// an omitted resolver would fail here even if it had somehow marshalled.
	restoreDanglingLeg(t, ctx, persistence, "ws-three", id, 3)
}

// TestRestoreSurvivesDanglingPermissionGate leaves a loop-owned PERMISSION gate
// (gate.ResolverLoop, with a real turn+step) OPEN at shutdown, then restores twice.
// Its synthesized unavailable-resolution validates under stepProfile because the
// gate carries a turn and step — the fix must not weaken this loop-owned path. This
// passes both before and after the fix (its resolver maps to the SAME stepProfile
// the omitted default used), which is exactly the invariant being confirmed.
func TestRestoreSurvivesDanglingPermissionGate(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	persistence := filepath.Join(t.TempDir(), "persistence")

	var id uuid.UUID
	func() {
		stores := openFSStores(t, persistence)
		defer func() { _ = stores.fs.Close() }()
		r := defineSessionRig(t, stores, filepath.Join(t.TempDir(), "ws-one"), false, rig.SnapshotPolicy{Trigger: rig.SnapshotManual})
		sess, err := r.NewSession(ctx)
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		shutdownSess := registerSessionCleanup(t, sess)
		id = sess.SessionID()
		loopID := sess.ActiveLoop().ID()

		preparer, ok := sess.(gatePreparer)
		if !ok {
			t.Fatal("session controller does not expose PrepareGateOpen/ActivateGate")
		}

		turnID, err := uuid.New()
		if err != nil {
			t.Fatal(err)
		}
		stepID, err := uuid.New()
		if err != nil {
			t.Fatal(err)
		}
		envelope := gate.Gate{
			Kind:     gate.KindPermission,
			Resolver: gate.ResolverLoop,
			Blocks:   gate.BlocksToolCall,
			Effect:   gate.EffectResume,
			Subject:  gate.Subject{TurnID: turnID, StepID: stepID},
		}
		payload := gate.PermissionPayload{Request: tool.BashRequest{Command: "echo ok"}}
		gateID, err := preparer.PrepareGateOpen(ctx, loopID, envelope, payload)
		if err != nil {
			t.Fatalf("PrepareGateOpen(permission): %v", err)
		}
		if err := preparer.ActivateGate(ctx, gateID, gate.Route{GateID: gateID, LoopID: gate.ID(loopID)}); err != nil {
			t.Fatalf("ActivateGate(permission): %v", err)
		}

		before := eventsFor(t, ctx, stores.sessions, id)
		if got := countEvents[event.GateOpened](before); got != 1 {
			t.Fatalf("journal has %d GateOpened, want 1 open permission gate", got)
		}
		if got := countEvents[event.GateResolved](before); got != 0 {
			t.Fatalf("journal has %d GateResolved, want 0 (gate left open)", got)
		}

		if err := shutdownSess(ctx); err != nil {
			t.Fatalf("shutdown leg one: %v", err)
		}
	}()

	restoreDanglingLeg(t, ctx, persistence, "ws-two", id, 1)
	restoreDanglingLeg(t, ctx, persistence, "ws-three", id, 1)
}

// restoreDanglingLeg reopens persistence as a fresh cold rig, restores id, asserts
// the restore did not error (zero RestoreErrored, at least one RestoreDone), confirms
// every dangling gate is now durably resolved (exactly wantResolved GateResolved
// records — the synthesized resolutions the first restore appended, unchanged on
// every later leg because the gates are no longer open), and shuts the restored
// session down so the next cold leg can reopen the store without lease contention.
//
// It deliberately does NOT use assertCleanRestore: that asserts the WHOLE lifecycle
// list, but a journal restored more than once accumulates one RestoreStarted▸
// RestoreDone pair per leg. The invariant that matters across legs is that no leg
// ever errored — a re-decode failure of the synthesized GateResolved would surface
// as a RestoreErrored (or a hard RestoreSession error above), and this catches both.
func restoreDanglingLeg(t *testing.T, ctx context.Context, persistence, wsName string, id uuid.UUID, wantResolved int) {
	t.Helper()
	stores := openFSStores(t, persistence)
	defer func() { _ = stores.fs.Close() }()
	r := defineSessionRig(t, stores, filepath.Join(t.TempDir(), wsName), true, rig.SnapshotPolicy{Trigger: rig.SnapshotManual})
	restored, err := r.RestoreSession(ctx, id)
	if err != nil {
		t.Fatalf("RestoreSession (%s): %v\n"+
			"this is the dangling-open-gate regression: appendRestoreUnavailableGates synthesizes a GateResolved for each gate still open at shutdown, and a host gate's synthesized resolution must carry Resolver: ResolverSession or the strict stepProfile rejects its identity at the durable append",
			wsName, err)
	}
	shutdownRestored := registerSessionCleanup(t, restored)

	events := eventsFor(t, ctx, stores.sessions, id)
	if got := countEvents[event.RestoreErrored](events); got != 0 {
		t.Fatalf("restore (%s) recorded %d RestoreErrored, want 0", wsName, got)
	}
	if got := countEvents[event.RestoreDone](events); got < 1 {
		t.Fatalf("restore (%s) recorded %d RestoreDone, want >= 1", wsName, got)
	}
	if got := countEvents[event.GateResolved](events); got != wantResolved {
		t.Fatalf("after restore (%s): %d GateResolved, want %d", wsName, got, wantResolved)
	}

	sc, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()
	if err := shutdownRestored(sc); err != nil {
		t.Fatalf("shutdown restored (%s): %v", wsName, err)
	}
}
