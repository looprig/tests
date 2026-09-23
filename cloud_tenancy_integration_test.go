//go:build integration && cloud

// This file is runbook 07 task P3.1's prefix and tenant isolation over the
// cloud composite. Two axes, and they are different mechanisms:
//
//   - DEPLOYMENT isolation: two SessionStores whose pgstore schemas and
//     s3store deployment prefixes differ, in ONE PostgreSQL database and ONE
//     bucket. The isolation is the providers' own (schema-qualified tables;
//     namespaceRoot's prefix), so the case holds identical tenant, session and
//     object bytes in both and requires that neither can see the other.
//   - TENANT isolation: two tenants in ONE deployment with the SAME session id.
//     Here the isolation is SessionStore's keyspace derivation, and the case
//     requires it to survive the real providers.
//
// Two tenants sharing one pooled Host over this composite are covered end to
// end by TestCloudFactoryAndPooledHostsOverPgstoreAndS3store.
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
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// openLaneStore opens a SessionStore over one cloud backend.
func openLaneStore(t *testing.T, ctx context.Context, backend *cloudBackend) *sessionstore.Store {
	t.Helper()
	return openSessionStore(t, ctx, backend.Composite)
}

// putLaneObject writes one tool capture and returns its metadata.
func putLaneObject(t *testing.T, ctx context.Context, store *sessionstore.Store, tenant sessionwire.TenantID, session sessionwire.SessionID, body []byte) sessionwire.ObjectMetadata {
	t.Helper()
	meta, err := store.PutObject(ctx, sessionstore.PutObjectRequest{
		TenantID: tenant, SessionID: session, Kind: sessionstore.ObjectKindToolResult,
		SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body), MediaType: "text/plain", Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	return meta
}

// requireBlobAbsent requires a refused object read to be ABSENCE at the
// provider (SessionStore preserves storage.BlobNotFoundError as the cause), not
// a transient failure that would pass a "was refused" check by accident.
func requireBlobAbsent(t *testing.T, err error) {
	t.Helper()
	var absent *storage.BlobNotFoundError
	if !errors.As(err, &absent) {
		t.Fatalf("the refusal is %v, want a provider BlobNotFoundError: a refusal for any other reason proves nothing about isolation", err)
	}
}

// listLaneSessions returns every session id a tenant's catalog lists.
func listLaneSessions(t *testing.T, ctx context.Context, store *sessionstore.Store, tenant sessionwire.TenantID) []sessionwire.SessionID {
	t.Helper()
	page, err := store.ListSessions(ctx, sessionstore.ListSessionsRequest{TenantID: tenant, Limit: 100})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if page.UnreadableSkipped != 0 {
		t.Fatalf("ListSessions skipped %d unreadable rows", page.UnreadableSkipped)
	}
	out := make([]sessionwire.SessionID, 0, len(page.Sessions))
	for _, summary := range page.Sessions {
		out = append(out, summary.SessionID)
	}
	return out
}

// TestCloudDeploymentsShareOneDatabaseAndBucketWithoutCrossing is deployment
// isolation: same tenant, same session, same object bytes, two deployments.
func TestCloudDeploymentsShareOneDatabaseAndBucketWithoutCrossing(t *testing.T) {
	cfg := requireCloud(t)
	ctx := cloudContext(t, 3*time.Minute)
	depA, depB := newCloudDeployment(t), newCloudDeployment(t)
	backendA := openCloudBackend(t, ctx, cfg, depA)
	backendB := openCloudBackend(t, ctx, cfg, depB)
	storeA, storeB := openLaneStore(t, ctx, backendA), openLaneStore(t, ctx, backendB)

	tenant := sessionwire.TenantID("tenant-" + cloudToken(t))
	shared := sessionwire.SessionID("session-" + cloudToken(t))
	onlyA := sessionwire.SessionID("session-" + cloudToken(t))
	now := time.Now().UTC()
	createSessionStoreSession(t, ctx, storeA, tenant, shared, now)
	createSessionStoreSession(t, ctx, storeB, tenant, shared, now)
	createSessionStoreSession(t, ctx, storeA, tenant, onlyA, now)

	body := []byte(strings.Repeat("identical capture in two deployments; ", 32))
	metaA := putLaneObject(t, ctx, storeA, tenant, shared, body)
	metaB := putLaneObject(t, ctx, storeB, tenant, shared, body)

	t.Run("each catalog lists only its own deployment's sessions", func(t *testing.T) {
		listedA, listedB := listLaneSessions(t, ctx, storeA, tenant), listLaneSessions(t, ctx, storeB, tenant)
		if len(listedA) != 2 {
			t.Fatalf("deployment A lists %v, want its two sessions", listedA)
		}
		if len(listedB) != 1 || listedB[0] != shared {
			t.Fatalf("deployment B lists %v, want only its one session; A's session crossed", listedB)
		}
	})

	t.Run("an object reference from one deployment does not resolve in the other", func(t *testing.T) {
		if got := readSessionStoreObject(t, ctx, storeA, tenant, shared, sessionstore.ObjectKindToolResult, metaA); !bytes.Equal(got, body) {
			t.Fatalf("A read back different bytes")
		}
		if got := readSessionStoreObject(t, ctx, storeB, tenant, shared, sessionstore.ObjectKindToolResult, metaB); !bytes.Equal(got, body) {
			t.Fatalf("B read back different bytes")
		}
		t.Logf("identical bytes, references equal across deployments: %v", metaA.Reference == metaB.Reference)
		// A's reference, presented to B for the very same tenant and session.
		if reader, err := storeB.GetObject(ctx, sessionstore.GetObjectRequest{
			TenantID: tenant, SessionID: shared, ExpectedKind: sessionstore.ObjectKindToolResult, Metadata: metaA,
		}); err == nil {
			_ = reader.Close()
			t.Fatalf("deployment B served deployment A's object")
		} else {
			requireBlobAbsent(t, err)
			t.Logf("B refused A's reference as absent: %v", err)
		}
	})

	t.Run("every S3 object sits under its own deployment prefix, SSE-KMS", func(t *testing.T) {
		client := rawS3(t, ctx, cfg)
		objectsA := listRaw(t, ctx, client, cfg, depA.Prefix+"/")
		objectsB := listRaw(t, ctx, client, cfg, depB.Prefix+"/")
		if len(objectsA) == 0 || len(objectsB) == 0 {
			t.Fatalf("deployment objects A=%d B=%d, want both non-empty", len(objectsA), len(objectsB))
		}
		for _, group := range [][]rawObject{objectsA, objectsB} {
			for _, object := range group {
				if object.Encryption != "aws:kms" {
					t.Fatalf("an object reports encryption %q, want aws:kms", object.Encryption)
				}
			}
		}
		// The prefixes are disjoint by construction; what the listing proves
		// is that neither store wrote OUTSIDE its own.
		t.Logf("S3 objects: A=%d B=%d", len(objectsA), len(objectsB))
	})

	t.Run("each deployment's structured state lives in its own schema", func(t *testing.T) {
		if tablesA, tablesB := pgTables(t, ctx, cfg, depA.Schema), pgTables(t, ctx, cfg, depB.Schema); len(tablesA) == 0 || len(tablesA) != len(tablesB) {
			t.Fatalf("schema census A=%v B=%v", tablesA, tablesB)
		}
	})
}

// TestCloudTenantsWithTheSameSessionIDStayApart is tenant isolation inside one
// deployment.
func TestCloudTenantsWithTheSameSessionIDStayApart(t *testing.T) {
	cfg := requireCloud(t)
	ctx := cloudContext(t, 3*time.Minute)
	store := openLaneStore(t, ctx, openCloudBackend(t, ctx, cfg, newCloudDeployment(t)))

	tenantA := sessionwire.TenantID("tenant-" + cloudToken(t))
	tenantB := sessionwire.TenantID("tenant-" + cloudToken(t))
	session := sessionwire.SessionID("session-" + cloudToken(t))
	now := time.Now().UTC()
	createSessionStoreSession(t, ctx, store, tenantA, session, now)
	createSessionStoreSession(t, ctx, store, tenantB, session, now)

	bodyA := []byte("tenant A's capture")
	bodyB := []byte("tenant B's capture")
	metaA := putLaneObject(t, ctx, store, tenantA, session, bodyA)
	metaB := putLaneObject(t, ctx, store, tenantB, session, bodyB)

	for _, tenant := range []sessionwire.TenantID{tenantA, tenantB} {
		if listed := listLaneSessions(t, ctx, store, tenant); len(listed) != 1 || listed[0] != session {
			t.Fatalf("a tenant lists %v, want exactly its own session", listed)
		}
	}
	if got := readSessionStoreObject(t, ctx, store, tenantA, session, sessionstore.ObjectKindToolResult, metaA); !bytes.Equal(got, bodyA) {
		t.Fatalf("tenant A read %q", got)
	}
	if got := readSessionStoreObject(t, ctx, store, tenantB, session, sessionstore.ObjectKindToolResult, metaB); !bytes.Equal(got, bodyB) {
		t.Fatalf("tenant B read %q", got)
	}
	// Tenant B presenting tenant A's reference for the same session id.
	if reader, err := store.GetObject(ctx, sessionstore.GetObjectRequest{
		TenantID: tenantB, SessionID: session, ExpectedKind: sessionstore.ObjectKindToolResult, Metadata: metaA,
	}); err == nil {
		_ = reader.Close()
		t.Fatalf("tenant B was served tenant A's object")
	} else {
		requireBlobAbsent(t, err)
		t.Logf("tenant B refused tenant A's reference as absent: %v", err)
	}
}
