//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/serve"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/inference"
)

func TestRigLifecycleAcrossFreshFSStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	persistence := filepath.Join(t.TempDir(), "persistence")
	stores := openFSStores(t, persistence)
	baseOne := filepath.Join(t.TempDir(), "workspaces-one")
	r := defineSessionRig(t, stores, baseOne, false, rig.SnapshotPolicy{Trigger: rig.SnapshotOnIdle, Priority: rig.SnapshotRequired})
	sess, err := r.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := sess.SessionID()
	sub, err := sess.SubscribeEvents(event.EventFilter{Enduring: event.LoopScope{All: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	ids := primerIDs(t, stores.sessions, id)
	if len(ids) != 2 {
		t.Fatalf("primer ids = %v", ids)
	}
	root := filepath.Join(canonical(t, baseOne), id.String())
	if err := os.WriteFile(filepath.Join(root, "work.txt"), []byte("first generation\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.SubmitToLoop(ctx, ids["planner"], textBlock("plan")); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.SubmitToLoop(ctx, ids["builder"], textBlock("build")); err != nil {
		t.Fatal(err)
	}
	checkpoint := waitEvent[event.WorkspaceCheckpointed](t, ctx, sub)
	if err := waitIdle(ctx, sess); err != nil {
		t.Fatalf("WaitIdle: %v", err)
	}
	builder, ok := sess.LoopController(ids["builder"])
	if !ok {
		t.Fatal("builder controller missing")
	}
	if err := builder.Change(ctx, loop.ChangeModel(model("builder-routed")), loop.ChangeEffort(inference.EffortHigh)); err != nil {
		t.Fatal(err)
	}
	if err := sess.SetActiveLoop(ctx, ids["builder"]); err != nil {
		t.Fatal(err)
	}
	turnsBefore := countEvents[event.TurnDone](eventsFor(t, stores.sessions, id))
	if err := sess.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := stores.fs.Close(); err != nil {
		t.Fatal(err)
	}

	freshStores := openFSStores(t, persistence)
	defer freshStores.fs.Close()
	if freshStores.fs == stores.fs || freshStores.sessions == stores.sessions {
		t.Fatal("fresh leg reused process-local store state")
	}
	baseTwo := filepath.Join(t.TempDir(), "workspaces-two")
	freshRig := defineSessionRig(t, freshStores, baseTwo, true, rig.SnapshotPolicy{Trigger: rig.SnapshotOnIdle, Priority: rig.SnapshotRequired})
	restored, err := freshRig.RestoreSession(ctx, id)
	if err != nil {
		t.Fatalf("RestoreSession: %v", err)
	}
	defer restored.Shutdown(context.Background())
	restoredSub, err := restored.SubscribeEvents(event.EventFilter{Enduring: event.LoopScope{All: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer restoredSub.Close()
	if got, err := os.ReadFile(filepath.Join(canonical(t, baseTwo), id.String(), "work.txt")); err != nil || string(got) != "first generation\n" {
		t.Fatalf("restored work.txt = %q err=%v", got, err)
	}
	if restored.ActiveLoop().ID() != ids["builder"] {
		t.Fatalf("active loop = %v, want %v", restored.ActiveLoop().ID(), ids["builder"])
	}
	handle, ok := restored.Loop(ids["builder"])
	if !ok || handle.Model().Name != "builder-routed" || handle.Model().Sampling.Effort != inference.EffortHigh {
		t.Fatalf("restored builder = %+v", handle)
	}
	if latestCheckpoint(t, eventsFor(t, freshStores.sessions, id)).Ref != checkpoint.Ref {
		t.Fatal("restore changed journal-authoritative checkpoint")
	}
	if _, err := restored.Submit(ctx, textBlock("continue")); err != nil {
		t.Fatal(err)
	}
	waitEvent[event.WorkspaceCheckpointed](t, ctx, restoredSub)
	if err := waitIdle(ctx, restored); err != nil {
		t.Fatal(err)
	}
	if got := countEvents[event.TurnDone](eventsFor(t, freshStores.sessions, id)); got != turnsBefore+1 {
		t.Fatalf("TurnDone count = %d, want %d", got, turnsBefore+1)
	}
}

func TestRigSeededSessionsDivergeAndRestoreJournalAuthoritativeTrees(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	persistence := filepath.Join(t.TempDir(), "persistence")
	stores := openFSStores(t, persistence)
	seedDir := t.TempDir()
	buildTree(t, seedDir, "seed")
	seedRef, err := stores.workspace.Snapshot(ctx, seedDir)
	if err != nil {
		t.Fatal(err)
	}
	baseOne := filepath.Join(t.TempDir(), "first-base")
	r := defineSessionRig(t, stores, baseOne, false, rig.SnapshotPolicy{Trigger: rig.SnapshotManual, Priority: rig.SnapshotRequired})
	one, err := r.NewSession(ctx, rig.WithSeedSnapshot(seedRef))
	if err != nil {
		t.Fatal(err)
	}
	two, err := r.NewSession(ctx, rig.WithSeedSnapshot(seedRef))
	if err != nil {
		_ = one.Shutdown(ctx)
		t.Fatal(err)
	}
	rootOne := filepath.Join(canonical(t, baseOne), one.SessionID().String())
	rootTwo := filepath.Join(canonical(t, baseOne), two.SessionID().String())
	if err := os.WriteFile(filepath.Join(rootOne, "branch.txt"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootTwo, "branch.txt"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	oneRef, err := one.CheckpointWorkspace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	twoRef, err := two.CheckpointWorkspace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if oneRef == twoRef || oneRef == seedRef || twoRef == seedRef {
		t.Fatalf("refs seed=%s one=%s two=%s", seedRef, oneRef, twoRef)
	}
	oneID, twoID := one.SessionID(), two.SessionID()
	if err := one.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := two.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := stores.fs.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(canonical(t, baseOne)); err != nil {
		t.Fatal(err)
	}

	freshStores := openFSStores(t, persistence)
	defer freshStores.fs.Close()
	baseTwo := filepath.Join(t.TempDir(), "fresh-base")
	freshRig := defineSessionRig(t, freshStores, baseTwo, true, rig.SnapshotPolicy{Trigger: rig.SnapshotManual, Priority: rig.SnapshotRequired})
	restoredOne, err := freshRig.RestoreSession(ctx, oneID)
	if err != nil {
		t.Fatal(err)
	}
	defer restoredOne.Shutdown(context.Background())
	restoredTwo, err := freshRig.RestoreSession(ctx, twoID)
	if err != nil {
		t.Fatal(err)
	}
	defer restoredTwo.Shutdown(context.Background())
	freshOne := filepath.Join(canonical(t, baseTwo), oneID.String())
	freshTwo := filepath.Join(canonical(t, baseTwo), twoID.String())
	if body, err := os.ReadFile(filepath.Join(freshOne, "branch.txt")); err != nil || string(body) != "one\n" {
		t.Fatalf("one branch = %q err=%v", body, err)
	}
	if body, err := os.ReadFile(filepath.Join(freshTwo, "branch.txt")); err != nil || string(body) != "two\n" {
		t.Fatalf("two branch = %q err=%v", body, err)
	}
	// Retain the prior cross-process suspend/resume proof for nested directories,
	// executable mode bits, and relative symlink targets.
	seedWithOneBranch := t.TempDir()
	if err := freshStores.workspace.Materialize(ctx, oneRef, seedWithOneBranch); err != nil {
		t.Fatal(err)
	}
	assertTreesEqual(t, seedWithOneBranch, freshOne)
	if latestCheckpoint(t, eventsFor(t, freshStores.sessions, oneID)).Ref != string(oneRef) {
		t.Fatal("session one restored the wrong durable ref")
	}
	if latestCheckpoint(t, eventsFor(t, freshStores.sessions, twoID)).Ref != string(twoRef) {
		t.Fatal("session two restored the wrong durable ref")
	}
	assertCleanRestore(t, eventsFor(t, freshStores.sessions, oneID))
	assertCleanRestore(t, eventsFor(t, freshStores.sessions, twoID))
}

func TestRigExclusiveRootContentionHandoffAndLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stores := openFSStores(t, filepath.Join(t.TempDir(), "persistence"))
	defer stores.fs.Close()
	root := filepath.Join(t.TempDir(), "exclusive")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	leaser := &trackingLeaser{Leaser: stores.fs.Leaser}
	blocker := &blockingLLM{started: make(chan struct{})}
	define := func() *rig.Rig {
		r, err := rig.Define(
			rig.WithLoops(definition(t, "planner", blocker)),
			rig.WithPrimers("planner"),
			rig.WithSessionStore(stores.sessions),
			rig.WithExclusiveWorkspace(stores.workspace, root, leaser),
			rig.WithSnapshots(rig.SnapshotPolicy{Trigger: rig.SnapshotManual}),
		)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	r1, r2 := define(), define()
	first, err := r1.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := r2.NewSession(ctx)
	if err == nil || second != nil {
		t.Fatalf("contending NewSession = (%v, %v)", second, err)
	}
	var busy *rig.WorkspaceRootBusyError
	if !errors.As(err, &busy) || busy.Root != canonical(t, root) || busy.HolderEpoch == 0 {
		t.Fatalf("busy error = %T %v", err, err)
	}
	if err := first.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	handoff, err := r2.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handoff.Submit(ctx, textBlock("running")); err != nil {
		t.Fatal(err)
	}
	<-blocker.started
	if err := leaser.loseWorkspace(ctx); err != nil {
		t.Fatal(err)
	}
	if err := waitIdle(ctx, handoff); err == nil {
		t.Fatal("WaitIdle succeeded after root lease loss")
	} else {
		var lost *rig.WorkspaceRootLeaseLostError
		if !errors.As(err, &lost) {
			t.Fatalf("WaitIdle error = %T %v", err, err)
		}
	}
	if _, err := handoff.Submit(ctx, textBlock("fenced")); err == nil {
		t.Fatal("Submit succeeded after root lease loss")
	} else {
		var lost *rig.WorkspaceRootLeaseLostError
		if !errors.As(err, &lost) {
			t.Fatalf("Submit error = %T %v", err, err)
		}
	}
	if err := handoff.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRigPersistenceOutsideWorkspaceArchiveAndOverlapRejected(t *testing.T) {
	ctx := context.Background()
	top := t.TempDir()
	persistence := filepath.Join(top, "persistence")
	stores := openFSStores(t, persistence)
	defer stores.fs.Close()
	base := filepath.Join(top, "workspaces")
	r := defineSessionRig(t, stores, base, false, rig.SnapshotPolicy{Trigger: rig.SnapshotManual, Priority: rig.SnapshotRequired})
	sess, err := r.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(canonical(t, base), sess.SessionID().String())
	if err := os.WriteFile(filepath.Join(root, "only-workspace.txt"), []byte("workspace\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ref, err := sess.CheckpointWorkspace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(filepath.Join(persistence, "streams", "sessions")); err != nil || len(entries) == 0 {
		t.Fatalf("journal layout missing: entries=%v err=%v", entries, err)
	}
	digest := strings.TrimPrefix(string(ref), "v1:sha256:")
	if _, err := os.Stat(filepath.Join(persistence, "blobs", "workspaces", digest)); err != nil {
		t.Fatalf("workspace blob missing outside archive root: %v", err)
	}
	materialized := filepath.Join(t.TempDir(), "materialized")
	if err := stores.workspace.Materialize(ctx, ref, materialized); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(materialized)
	if err != nil || len(entries) != 1 || entries[0].Name() != "only-workspace.txt" {
		t.Fatalf("archive entries = %v err=%v", entries, err)
	}
	_, err = rig.Define(
		rig.WithLoops(definition(t, "planner", deterministicLLM{})),
		rig.WithPrimers("planner"),
		rig.WithSessionStore(stores.sessions),
		rig.WithSessionWorkspaces(stores.workspace, top),
		rig.WithSnapshots(rig.SnapshotPolicy{Trigger: rig.SnapshotManual}),
	)
	var overlap *rig.PersistenceOverlapError
	if !errors.As(err, &overlap) || overlap.PersistencePath != canonical(t, persistence) {
		t.Fatalf("overlap error = %T %v", err, err)
	}
}

func TestServeLifecycleWireOverConcreteRigAndFSStore(t *testing.T) {
	stores := openFSStores(t, filepath.Join(t.TempDir(), "persistence"))
	defer stores.fs.Close()
	concrete, err := rig.Define(
		rig.WithLoops(definition(t, "agent", deterministicLLM{})),
		rig.WithPrimers("agent"),
		rig.WithSessionStore(stores.sessions),
	)
	if err != nil {
		t.Fatal(err)
	}
	captured := &captureRig{inner: concrete}
	handler := serve.Handler[session.SessionController, rig.SessionOption](captured, nil)
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, httptest.NewRequest(http.MethodPost, "/v1/sessions", http.NoBody))
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var createResponse struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &createResponse); err != nil {
		t.Fatal(err)
	}
	createdSession := captured.captured()
	if createdSession == nil || createdSession.SessionID().String() != createResponse.SessionID {
		t.Fatalf("wire id=%s concrete=%v", createResponse.SessionID, createdSession)
	}
	if err := createdSession.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	restored := httptest.NewRecorder()
	handler.ServeHTTP(restored, httptest.NewRequest(http.MethodPost, "/v1/sessions/"+createResponse.SessionID+"/restore", http.NoBody))
	if restored.Code != http.StatusOK {
		t.Fatalf("restore status=%d body=%s", restored.Code, restored.Body.String())
	}
	var restoreResponse struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(restored.Body.Bytes(), &restoreResponse); err != nil {
		t.Fatal(err)
	}
	if restoreResponse.SessionID != createResponse.SessionID || captured.captured().SessionID().String() != createResponse.SessionID {
		t.Fatalf("restore wire=%s concrete=%v", restoreResponse.SessionID, captured.captured())
	}
	if err := captured.captured().Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
