//go:build integration && cloud

// This file pins the defects the P3.1 cloud lane found in RELEASED modules,
// against the BARE modules (no lane shim). Each case PASSES while its defect
// reproduces and FAILS once it stops reproducing -- at which point the
// matching shim in cloud_backend_test.go (cloudShims) must be deleted, and the
// composition cases re-run with LOOPRIG_CLOUD_NO_SHIMS=1 to prove it.
//
// They are witnesses rather than red tests on purpose: the lane is meant to
// be run, and a lane that is always red for a known upstream reason stops
// being read. CLAUDE_RESULT_P3.1.md carries the full account of each.
package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/pgstore"
	"github.com/looprig/s3store"
	"github.com/looprig/sessionstore"
)

// TestCloudKnownDefectD1S3storeLongLogicalKeyOnMinIO pins D1: s3store v0.1.1
// maps a logical key to <prefix>/blobs/v1/<sha256hex>/<base64url(key)>, so the
// WHOLE logical key is one object-key path segment. MinIO refuses a segment
// over 255 bytes, which base64url reaches at a 192-byte key. Storage names may
// be 512 bytes, and every SessionStore object key is ~235 bytes, so no
// SessionStore object can be stored through s3store on MinIO.
func TestCloudKnownDefectD1S3storeLongLogicalKeyOnMinIO(t *testing.T) {
	cfg := requireCloud(t)
	ctx := cloudContext(t, 2*time.Minute)
	backend := openCloudBackendWith(t, ctx, cfg, newCloudDeployment(t), cloudShims{defaultDeadline: 30 * time.Second})

	keyOf := func(n int) string { return "k/" + strings.Repeat("a", n-2) }
	if err := backend.Blobs.Put(ctx, keyOf(s3storeMinIOMaxLogicalKey), strings.NewReader("fits")); err != nil {
		t.Fatalf("a %d-byte logical key was refused (%s); the measured boundary moved", s3storeMinIOMaxLogicalKey, s3store.RedactedErrorText(err))
	}
	err := backend.Blobs.Put(ctx, keyOf(s3storeMinIOMaxLogicalKey+1), strings.NewReader("too long a segment"))
	var backendErr *s3store.BackendError
	if err == nil {
		t.Fatalf("D1 NO LONGER REPRODUCES: a %d-byte logical key stored on MinIO. Delete cloudShims.hashLongKeys and re-run the lane with LOOPRIG_CLOUD_NO_SHIMS=1", s3storeMinIOMaxLogicalKey+1)
	}
	if !errors.As(err, &backendErr) {
		t.Fatalf("a %d-byte key failed with %T (%s), want the *s3store.BackendError D1 produces", s3storeMinIOMaxLogicalKey+1, err, s3store.RedactedErrorText(err))
	}
	t.Logf("D1 reproduces: a %d-byte key stores, a %d-byte key fails: %s", s3storeMinIOMaxLogicalKey, s3storeMinIOMaxLogicalKey+1, s3store.RedactedErrorText(err))

	// And the consequence at SessionStore's surface: its first object fails.
	store, err := sessionstore.Open(ctx, backend.Composite)
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = store.Close(closeCtx)
	})
	tenant := sessionwire.TenantID("tenant-" + cloudToken(t))
	session := sessionwire.SessionID("session-" + cloudToken(t))
	createSessionStoreSession(t, ctx, store, tenant, session, time.Now().UTC())
	body := []byte("a tool capture")
	_, err = store.PutObject(ctx, sessionstore.PutObjectRequest{
		TenantID: tenant, SessionID: session, Kind: sessionstore.ObjectKindToolResult,
		SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body), MediaType: "text/plain", Body: bytes.NewReader(body),
	})
	if err == nil {
		t.Fatalf("D1 NO LONGER REPRODUCES at SessionStore: an object stored through bare s3store on MinIO")
	}
	t.Logf("D1 at SessionStore: PutObject = %v", err)
}

// TestCloudKnownDefectD2DeadlineLessStoreOpen pins D2: pgstore (and s3store)
// refuse every call whose context has no deadline, a precondition the Storage
// contract does not state, while host v0.5.0's Compose opens SessionStore on
// context.WithoutCancel(ctx) (compose.go, "THE STORE OUTLIVES THE COMPOSING
// CALL"). That is exactly the Open below, so no Host composes over pgstore:
// Compose fails "Collaborators.Backend: the session store could not be
// opened over it: sessionstore: keyspace backend".
func TestCloudKnownDefectD2DeadlineLessStoreOpen(t *testing.T) {
	cfg := requireCloud(t)
	ctx := cloudContext(t, 2*time.Minute)
	backend := openCloudBackendWith(t, ctx, cfg, newCloudDeployment(t), cloudShims{})

	_, err := sessionstore.Open(context.WithoutCancel(ctx), backend.Composite)
	var deadline *pgstore.DeadlineRequiredError
	if err == nil {
		t.Fatalf("D2 NO LONGER REPRODUCES: SessionStore opened over bare pgstore on a deadline-less context. Delete cloudShims.defaultDeadline and re-run the lane with LOOPRIG_CLOUD_NO_SHIMS=1")
	}
	if !errors.As(err, &deadline) {
		t.Fatalf("the deadline-less Open failed with %v, want a wrapped *pgstore.DeadlineRequiredError", err)
	}
	t.Logf("D2 reproduces: %v (cause: %v)", err, deadline)

	store, err := sessionstore.Open(ctx, backend.Composite)
	if err != nil {
		t.Fatalf("the SAME Open with a deadline failed too (%v); the defect is not the deadline", err)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = store.Close(closeCtx)
}
