//go:build integration && cloud

// This file holds the P3.1 lane's regressions for the two provider defects its
// first round found in released modules, run against the BARE providers with
// nothing between SessionStore and them but the lane's pass-through recorder.
//
//   - D1 (fixed in s3store v0.2.0): s3store v0.1.x put the whole logical key
//     into ONE object-key path segment, and MinIO refuses a segment over 255
//     bytes, so any logical key over 191 bytes -- every SessionStore object
//     key -- failed on MinIO.
//   - D2 (fixed in pgstore v0.2.0 and s3store v0.2.0): both refused every call
//     whose context had no deadline, so host's Compose, which opens SessionStore
//     on context.WithoutCancel, could not compose over pgstore at all.
//
// The third defect, D3 (a runtime left resident after a storage outage), is
// held by the outage cases in cloud_failure_integration_test.go.
package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/s3store"
	"github.com/looprig/sessionstore"
)

// TestCloudS3storeStoresEveryStorageNameLengthOnMinIO is D1's regression: keys
// at, just past, and far past the old 191-byte boundary, up to Storage's
// 512-byte name limit, round-trip on MinIO -- and so does a SessionStore
// object, whose key is ~235 bytes.
func TestCloudS3storeStoresEveryStorageNameLengthOnMinIO(t *testing.T) {
	cfg := requireCloud(t)
	ctx := cloudContext(t, 2*time.Minute)
	backend := openCloudBackend(t, ctx, cfg, newCloudDeployment(t))

	for _, n := range []int{191, 192, 256, 400, 512} {
		key := "k/" + strings.Repeat("a", n-2)
		body := "length " + strings.Repeat("x", n)
		if err := backend.Blobs.Put(ctx, key, strings.NewReader(body)); err != nil {
			t.Fatalf("a %d-byte logical key was refused on MinIO: %s", n, s3store.RedactedErrorText(err))
		}
		reader, err := backend.Blobs.Get(ctx, key)
		if err != nil {
			t.Fatalf("a %d-byte logical key stored but does not read back: %s", n, s3store.RedactedErrorText(err))
		}
		got, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil || string(got) != body {
			t.Fatalf("a %d-byte logical key read back %d bytes (%v), want the %d written", n, len(got), err, len(body))
		}
		listed, err := backend.Blobs.List(ctx, "k/")
		if err != nil {
			t.Fatalf("List after a %d-byte key: %s", n, s3store.RedactedErrorText(err))
		}
		found := false
		for _, name := range listed {
			found = found || name == key
		}
		if !found {
			t.Fatalf("List omits the %d-byte logical key", n)
		}
	}

	store := openLaneStore(t, ctx, backend)
	tenant := sessionwire.TenantID("tenant-" + cloudToken(t))
	session := sessionwire.SessionID("session-" + cloudToken(t))
	createSessionStoreSession(t, ctx, store, tenant, session, time.Now().UTC())
	body := []byte("a tool capture")
	meta, err := store.PutObject(ctx, sessionstore.PutObjectRequest{
		TenantID: tenant, SessionID: session, Kind: sessionstore.ObjectKindToolResult,
		SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body), MediaType: "text/plain", Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("a SessionStore object did not store through s3store on MinIO: %v", err)
	}
	if got := readSessionStoreObject(t, ctx, store, tenant, session, sessionstore.ObjectKindToolResult, meta); !bytes.Equal(got, body) {
		t.Fatalf("the SessionStore object read back different bytes")
	}
}

// TestCloudDeadlineFreeStoreOpenIsBounded is D2's regression: SessionStore
// opens over pgstore+s3store on a context with NO deadline, exactly as
// host.Compose does (context.WithoutCancel), and serves a write and a read on
// one. The providers' default bound, not a caller deadline, is what makes it
// safe.
func TestCloudDeadlineFreeStoreOpenIsBounded(t *testing.T) {
	cfg := requireCloud(t)
	ctx := cloudContext(t, 2*time.Minute)
	backend := openCloudBackend(t, ctx, cfg, newCloudDeployment(t))
	undated := context.WithoutCancel(ctx)
	if _, ok := undated.Deadline(); ok {
		t.Fatalf("the probe context carries a deadline; the case would prove nothing")
	}

	store, err := sessionstore.Open(undated, backend.Composite)
	if err != nil {
		t.Fatalf("SessionStore did not open on a deadline-free context: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = store.Close(closeCtx)
	})
	tenant := sessionwire.TenantID("tenant-" + cloudToken(t))
	session := sessionwire.SessionID("session-" + cloudToken(t))
	createSessionStoreSession(t, undated, store, tenant, session, time.Now().UTC())
	if listed := listLaneSessions(t, undated, store, tenant); len(listed) != 1 || listed[0] != session {
		t.Fatalf("a deadline-free list answered %v", listed)
	}
	if undatedCalls := backend.Metrics.Undated(); len(undatedCalls) == 0 {
		t.Fatalf("the recorder saw no deadline-free provider call; the case did not exercise the providers' default bound")
	} else {
		t.Logf("deadline-free provider calls served: %v", undatedCalls)
	}
}
