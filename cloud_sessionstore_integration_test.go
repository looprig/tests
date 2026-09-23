//go:build integration && cloud

// This file is runbook 07 task P3.1 steps 1, 2 and 4 over the RELEASED
// pgstore and s3store: one storage.Composite of pgstore Ledger/Leaser/KV/
// OrderedIndex and s3store Blobs, handed to SessionStore.
//
//   - Step 2 (session list, journal fence, inbox ordering/due, Host target
//     directory, registry tombstone, object-first capture, opaque IDs) is NOT
//     re-implemented here. The provider-neutral cases in
//     sessionstore_provider_integration_test.go already state those contracts
//     over memstore and natsstore; this file adds "pgstore+s3store" to their
//     matrix, so every one of them runs over the cloud composite unchanged.
//   - The orchestration cases below run a REAL released Factory and REAL
//     pooled Hosts over that composite -- and put every harness journal on its
//     own pgstore+s3store composite too, with a low offload threshold, so the
//     runtime's own records are object-first captures in S3 and a restore on a
//     second Host must read them back.
//   - Step 4's measurements are logged by every case: per-primitive calls,
//     errors and latency, blob throughput, and PostgreSQL's own index-scan,
//     statement-latency and connection counts, with no identifier or secret.
package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// cloudProviderName is the matrix name of the cloud composite.
const cloudProviderName = "pgstore+s3store"

// cloudJournalOffloadThreshold is low enough that an ordinary turn's records
// exceed it, so harness offloads them to Blobs (S3) before appending a pointer
// to the Ledger (PostgreSQL). At the 512 KiB default nothing this lane does
// would ever reach S3 from a journal.
const cloudJournalOffloadThreshold = 256

// cloudSessionStoreProviders adds the cloud composite to the provider-neutral
// matrix when the lane is enabled.
func cloudSessionStoreProviders(t *testing.T) []sessionStoreProvider {
	t.Helper()
	if !cloudEnabled() {
		return nil
	}
	cfg := requireCloud(t)
	return []sessionStoreProvider{{
		name: cloudProviderName,
		open: func(t *testing.T, ctx context.Context) *storage.Composite {
			t.Helper()
			backend := openCloudBackend(t, ctx, cfg, newCloudDeployment(t))
			t.Cleanup(func() { t.Log(backend.Metrics.Report(cloudProviderName)) })
			return backend.Composite
		},
	}}
}

// cloudRequiredSessionStoreProviders makes the matrix floor REQUIRE the cloud
// composite once the lane is enabled, so a matrix that silently lost it fails
// rather than passing over memstore and natsstore alone.
func cloudRequiredSessionStoreProviders() []string {
	if !cloudEnabled() {
		return nil
	}
	return []string{cloudProviderName}
}

// TestCloudCompositeIsExactlyPgstorePlusS3store is step 1: the composite
// SessionStore receives is the two released providers and nothing else, and
// SessionStore accepts it -- which it would not if the Blobs provider lacked
// the bounded reader lifecycle (fsstore is refused exactly there).
func TestCloudCompositeIsExactlyPgstorePlusS3store(t *testing.T) {
	cfg := requireCloud(t)
	ctx := cloudContext(t, 2*time.Minute)
	dep := newCloudDeployment(t)
	backend := openCloudBackend(t, ctx, cfg, dep)
	_, via := cfg.structuredDSN()
	t.Logf("structured primitives via %s", via)

	if _, ok := backend.Composite.Blobs.(storage.BlobReaderLifecycle); !ok {
		t.Fatalf("the composite's Blobs lost the bounded reader lifecycle")
	}
	if blobs, ok := backend.Composite.Blobs.(timedBlobs); !ok || blobs.inner != backend.Blobs {
		t.Fatalf("the composite's Blobs is %T, want the released s3store (behind the timing recorder only)", backend.Composite.Blobs)
	}
	if bound := backend.Blobs.BlobReaderCloseBound(); bound <= 0 {
		t.Fatalf("s3store declares close bound %v", bound)
	}
	store, err := sessionstore.Open(ctx, backend.Composite)
	if err != nil {
		t.Fatalf("sessionstore.Open over pgstore+s3store: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = store.Close(closeCtx)
	})

	// No PostgreSQL blob fallback: pgstore's schema holds no table that could
	// store blob bytes, so an object cannot land in PostgreSQL by accident.
	tables := pgTables(t, ctx, cfg, dep.Schema)
	t.Logf("pgstore tables: %v", tables)
	for _, table := range tables {
		if strings.Contains(table, "blob") || strings.Contains(table, "object") {
			t.Fatalf("pgstore created %q: a PostgreSQL blob fallback exists", table)
		}
	}
	if len(tables) == 0 {
		t.Fatalf("pgstore created no tables in the deployment schema; the census is vacuous")
	}

	// And an object written through SessionStore lands in S3, encrypted,
	// under this deployment's prefix -- read back through SessionStore.
	tenant := sessionwire.TenantID("tenant-" + cloudToken(t))
	session := sessionwire.SessionID("session-" + cloudToken(t))
	createSessionStoreSession(t, ctx, store, tenant, session, time.Now().UTC())
	body := []byte(strings.Repeat("object-first capture over s3store; ", 64))
	meta, err := store.PutObject(ctx, sessionstore.PutObjectRequest{
		TenantID: tenant, SessionID: session, Kind: sessionstore.ObjectKindToolResult,
		SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body),
		MediaType: "text/plain", Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	got := readSessionStoreObject(t, ctx, store, tenant, session, sessionstore.ObjectKindToolResult, meta)
	if string(got) != string(body) {
		t.Fatalf("the object read back differs from the bytes written")
	}
	objects := listRaw(t, ctx, rawS3(t, ctx, cfg), cfg, dep.Prefix+"/")
	if len(objects) == 0 {
		t.Fatalf("no object reached S3 under the deployment prefix")
	}
	for _, object := range objects {
		if object.Encryption != "aws:kms" {
			t.Fatalf("an object under the deployment prefix reports server-side encryption %q, want aws:kms", object.Encryption)
		}
	}
	t.Logf("S3 objects under the deployment prefix: %d, all aws:kms", len(objects))
	t.Log(backend.Metrics.Report("step 1"))
	t.Log(pgReport(t, ctx, cfg, dep.Schema))
}

// cloudWorld is a pooled world whose SessionStore AND harness journals are all
// pgstore+s3store composites, each in its own deployment.
type cloudWorld struct {
	*orchestrationtest.PooledWorld
	cfg      cloudConfig
	state    *cloudBackend
	journals map[sessionwire.TenantID]*cloudBackend
}

func newCloudWorld(t *testing.T, ctx context.Context, cfg cloudConfig, options orchestrationtest.PooledWorldOptions) *cloudWorld {
	t.Helper()
	world := &cloudWorld{
		cfg:      cfg,
		state:    openCloudBackend(t, ctx, cfg, newCloudDeployment(t)),
		journals: map[sessionwire.TenantID]*cloudBackend{},
	}
	options.Backend = world.state.Composite
	options.JournalBackend = func(_ orchestrationtest.TB, tenant sessionwire.TenantID) *storage.Composite {
		backend := openCloudBackend(t, ctx, cfg, newCloudDeployment(t))
		world.journals[tenant] = backend
		return backend.Composite
	}
	options.JournalOptions = append(options.JournalOptions, harnessstore.WithOffloadThreshold(cloudJournalOffloadThreshold))
	world.PooledWorld = orchestrationtest.NewPooledWorld(t, ctx, options)
	t.Cleanup(func() { world.report(t) })
	return world
}

// report logs step 4's measurements for every deployment in the world.
func (w *cloudWorld) report(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, via := w.cfg.structuredDSN()
	t.Logf("structured primitives via %s", via)
	t.Log(w.state.Metrics.Report("session state"))
	t.Log(pgReport(t, ctx, w.cfg, w.state.Deployment.Schema))
	for tenant, journal := range w.journals {
		label := "journal " + tenantLabel(tenant)
		t.Log(journal.Metrics.Report(label))
		t.Log(pgReport(t, ctx, w.cfg, journal.Deployment.Schema))
	}
}

// tenantLabel names a kit tenant without its prefix.
func tenantLabel(tenant sessionwire.TenantID) string {
	return strings.TrimPrefix(string(tenant), orchestrationtest.TenantPrefix)
}

// assertEncryptedJournalObjects asserts the tenant's journal really did
// capture objects in S3 and that every one is SSE-KMS encrypted.
func (w *cloudWorld) assertEncryptedJournalObjects(t *testing.T, ctx context.Context, tenant sessionwire.TenantID) int {
	t.Helper()
	journal := w.journals[tenant]
	if puts := journal.Metrics.Calls("blobs.put"); puts == 0 {
		t.Fatalf("%s's journal offloaded nothing to S3; the object-first leg is vacuous", tenantLabel(tenant))
	}
	objects := listRaw(t, ctx, rawS3(t, ctx, w.cfg), w.cfg, journal.Deployment.Prefix+"/")
	if len(objects) == 0 {
		t.Fatalf("%s's journal reports blob puts but S3 holds nothing under its prefix", tenantLabel(tenant))
	}
	for _, object := range objects {
		if object.Encryption != "aws:kms" {
			t.Fatalf("a journal object reports server-side encryption %q, want aws:kms", object.Encryption)
		}
	}
	return len(objects)
}

// cloudPost posts a command and requires the status.
func cloudPost(t *testing.T, ctx context.Context, f *orchestrationtest.PooledFactory, tenant sessionwire.TenantID, path string, body any, want int) {
	t.Helper()
	status, answer := f.Post(t, ctx, tenant, path, body)
	if status != want {
		t.Fatalf("POST %s as %s answered %d, want %d: %s", path, tenantLabel(tenant), status, want, answer)
	}
}

func cloudInput(id string, s sessionwire.SessionID, text string) sessionwire.InputRequest {
	return sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(id),
		SessionID:       s,
		Blocks:          json.RawMessage(fmt.Sprintf(`[{"type":"text","text":%q}]`, text)),
	}
}

func cloudCreate(id string, s sessionwire.SessionID, text string) sessionwire.CreateRequest {
	return sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(id),
		SessionID:       s,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(fmt.Sprintf(`[{"type":"text","text":%q}]`, text)),
	}
}

// awaitApplied waits for one command to settle applied.
func awaitApplied(t *testing.T, ctx context.Context, w *orchestrationtest.PooledWorld, tenant sessionwire.TenantID, s sessionwire.SessionID, command string, within time.Duration) time.Duration {
	t.Helper()
	return orchestrationtest.PooledWait(t, command+" applied", within, func() bool {
		return w.CommandState(ctx, tenant, s, sessionwire.CommandID(command)) == sessionstore.InboxStateApplied
	})
}

// cloudPlacementContext bounds a whole cloud orchestration case.
func cloudPlacementContext(t *testing.T) context.Context {
	return cloudContext(t, 8*time.Minute)
}

// TestCloudFactoryAndPooledHostsOverPgstoreAndS3store is the composition,
// whole: a real Factory and real pooled Hosts whose every durable byte is in
// PostgreSQL or S3.
//
//	C1  create with a first message, for two tenants, on one pooled Host;
//	C2  a command (input) is applied exactly once;
//	C3  the runtime's journal records reached S3, every object SSE-KMS;
//	C4  a re-placed session is RESTORED on a second Host from those objects;
//	C5  cold reads with every Host stopped, from a Factory replica that never
//	    served the session.
func TestCloudFactoryAndPooledHostsOverPgstoreAndS3store(t *testing.T) {
	cfg := requireCloud(t)
	ctx := cloudPlacementContext(t)
	world := newCloudWorld(t, ctx, cfg, orchestrationtest.PooledWorldOptions{})
	tenants := []sessionwire.TenantID{orchestrationtest.PooledTenantA, orchestrationtest.PooledTenantB}
	sessions := map[sessionwire.TenantID]sessionwire.SessionID{
		orchestrationtest.PooledTenantA: "session-cloud-a",
		orchestrationtest.PooledTenantB: "session-cloud-b",
	}
	words := map[sessionwire.TenantID]string{
		orchestrationtest.PooledTenantA: "TAMARIND",
		orchestrationtest.PooledTenantB: "MEDLAR",
	}

	hostA := orchestrationtest.StartPooledHost(t, ctx, world.PooledWorld, "orchestrationtest-cloud-host-a", 4)
	orchestrationtest.AwaitAdvertised(t, world.PooledWorld, hostA.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world.PooledWorld, "orchestrationtest-cloud-replica-1", nil)

	t.Run("C1: create with a first message is placed and applied", func(t *testing.T) {
		for _, tenant := range tenants {
			cloudPost(t, ctx, served, tenant, "/v1/sessions",
				cloudCreate("create-"+string(tenant), sessions[tenant], "the remembered word is "+words[tenant]), http.StatusCreated)
		}
		for _, tenant := range tenants {
			took := awaitApplied(t, ctx, world.PooledWorld, tenant, sessions[tenant], "create-"+string(tenant), 120*time.Second)
			owner, found, err := served.Directory.Owner(ctx, tenant, sessions[tenant])
			if err != nil || !found || owner.HostID != hostA.ID {
				t.Fatalf("%s's session owner = %+v found=%v err=%v, want %s", tenantLabel(tenant), owner, found, err, hostA.ID)
			}
			t.Logf("C1 %s create applied after %v on %s", tenantLabel(tenant), took.Round(time.Millisecond), owner.HostID)
		}
		if creates := hostA.Rig.Creates(); len(creates) != 2 {
			t.Fatalf("host-a created %d runtimes, want one per tenant: %+v", len(creates), creates)
		}
	})

	t.Run("C2: an input is applied exactly once", func(t *testing.T) {
		for _, tenant := range tenants {
			cloudPost(t, ctx, served, tenant, "/v1/sessions/"+string(sessions[tenant])+"/input",
				cloudInput("input-1-"+string(tenant), sessions[tenant], "hello"), http.StatusOK)
		}
		for _, tenant := range tenants {
			awaitApplied(t, ctx, world.PooledWorld, tenant, sessions[tenant], "input-1-"+string(tenant), 90*time.Second)
			runtimeID := world.RuntimeSessionID(t, ctx, tenant, sessions[tenant])
			evidence := orchestrationtest.ReadCommandEvidence(t, world.PooledWorld, tenant, runtimeID)
			if apps := evidence.ApplicationsOf(sessionwire.CommandID("input-1-" + string(tenant))); len(apps) != 1 {
				t.Fatalf("%s's journal holds %d applications of its input, want exactly 1", tenantLabel(tenant), len(apps))
			}
		}
	})

	t.Run("C3: the runtime's records are object-first captures in S3, all SSE-KMS", func(t *testing.T) {
		for _, tenant := range tenants {
			n := world.assertEncryptedJournalObjects(t, ctx, tenant)
			t.Logf("C3 %s journal: %d S3 objects, all aws:kms", tenantLabel(tenant), n)
		}
		// SessionStore's own composite shares the bucket, and no journal
		// object may have landed under ITS prefix: each deployment's objects
		// stay under that deployment's prefix.
		for _, object := range listRaw(t, ctx, rawS3(t, ctx, cfg), cfg, world.state.Deployment.Prefix+"/") {
			if object.Encryption != "aws:kms" {
				t.Fatalf("a session-state object reports encryption %q", object.Encryption)
			}
		}
	})

	var hostB *orchestrationtest.PooledHost
	t.Run("C4: a re-placed session is restored on a second Host from S3-backed records", func(t *testing.T) {
		runtimeIDs := map[sessionwire.TenantID]string{}
		for _, tenant := range tenants {
			runtimeIDs[tenant] = world.RuntimeSessionID(t, ctx, tenant, sessions[tenant]).String()
		}
		getsBefore := world.journals[orchestrationtest.PooledTenantA].Metrics.Calls("blobs.get")
		hostA.Stop()
		for _, tenant := range tenants {
			orchestrationtest.PooledWait(t, "the registry stops reporting a live owner for "+tenantLabel(tenant), 90*time.Second, func() bool {
				owner, found, _ := served.Directory.Owner(ctx, tenant, sessions[tenant])
				return !found || owner.Residency != sessionwire.SessionResidencyResident || !owner.Accepting || !owner.ExpiresAt.After(time.Now())
			})
		}
		hostB = orchestrationtest.StartPooledHost(t, ctx, world.PooledWorld, "orchestrationtest-cloud-host-b", 7)
		orchestrationtest.AwaitAdvertised(t, world.PooledWorld, hostB.ID)

		requestsBefore := len(world.LLM.Requests())
		for _, tenant := range tenants {
			cloudPost(t, ctx, served, tenant, "/v1/sessions/"+string(sessions[tenant])+"/input",
				cloudInput("input-2-"+string(tenant), sessions[tenant], "what was the remembered word?"), http.StatusOK)
		}
		for _, tenant := range tenants {
			took := awaitApplied(t, ctx, world.PooledWorld, tenant, sessions[tenant], "input-2-"+string(tenant), 150*time.Second)
			t.Logf("C4 %s re-placed input applied after %v", tenantLabel(tenant), took.Round(time.Millisecond))
		}
		if creates := hostB.Rig.Creates(); len(creates) != 0 {
			t.Fatalf("host-b CREATED %+v; a re-placed session must be restored", creates)
		}
		if restores := hostB.Rig.Restores(); len(restores) != 2 {
			t.Fatalf("host-b restored %+v, want both tenants' sessions", restores)
		}
		for _, tenant := range tenants {
			runtimeID := world.RuntimeSessionID(t, ctx, tenant, sessions[tenant])
			if runtimeID.String() != runtimeIDs[tenant] {
				t.Fatalf("%s's runtime id moved across the re-placement", tenantLabel(tenant))
			}
			orchestrationtest.PooledWait(t, tenantLabel(tenant)+"'s restored turn finished", 90*time.Second, func() bool {
				return orchestrationtest.CountJournalEvents[event.TurnDone](t, world.PooledWorld, tenant, runtimeID) >= 3
			})
			if started := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world.PooledWorld, tenant, runtimeID); started != 1 {
				t.Fatalf("%s's journal holds %d SessionStarted, want 1: the session was restarted", tenantLabel(tenant), started)
			}
			if restored := orchestrationtest.CountJournalEvents[event.RestoreDone](t, world.PooledWorld, tenant, runtimeID); restored < 1 {
				t.Fatalf("%s's journal holds no RestoreDone", tenantLabel(tenant))
			}
		}
		// The restored conversation carried each tenant's own word, which
		// was only ever stored in S3-offloaded journal records.
		for _, tenant := range tenants {
			if !world.LLM.SawInRequest(requestsBefore, words[tenant]) {
				t.Fatalf("no model request after the re-placement carried %s's word; the conversation did not survive", tenantLabel(tenant))
			}
		}
		if gets := world.journals[orchestrationtest.PooledTenantA].Metrics.Calls("blobs.get"); gets <= getsBefore {
			t.Fatalf("the restore read no journal object from S3 (%d gets before, %d after); the S3 leg of the restore is vacuous", getsBefore, gets)
		}
	})

	t.Run("C5: cold reads with every Host stopped, from a Factory that never served the session", func(t *testing.T) {
		if hostB == nil {
			t.Skip("C4 did not start host-b")
		}
		hostB.Stop()
		served.Stop()
		launchesBefore := len(hostB.Rig.Creates()) + len(hostB.Rig.Restores())
		cold := orchestrationtest.StartPooledFactory(t, ctx, world.PooledWorld, "orchestrationtest-cloud-replica-2", nil)
		for _, tenant := range tenants {
			status, body := cold.Get(t, ctx, tenant, "/v1/sessions")
			if status != http.StatusOK {
				t.Fatalf("the cold session list for %s answered %d: %s", tenantLabel(tenant), status, body)
			}
			var page sessionwire.SessionPage
			if err := json.Unmarshal(body, &page); err != nil {
				t.Fatalf("the session list is not a Core SessionPage: %v", err)
			}
			if len(page.Sessions) != 1 || page.Sessions[0].SessionID != sessions[tenant] {
				t.Fatalf("%s's cold list is %+v, want exactly its own session", tenantLabel(tenant), page.Sessions)
			}
			for _, suffix := range []string{"/journal", "/gates"} {
				status, body := cold.Get(t, ctx, tenant, orchestrationtest.SessionPath(sessions[tenant], suffix))
				if status != http.StatusOK {
					t.Fatalf("cold GET %s for %s answered %d: %s", suffix, tenantLabel(tenant), status, body)
				}
			}
		}
		if launches := len(hostB.Rig.Creates()) + len(hostB.Rig.Restores()); launches != launchesBefore {
			t.Fatalf("a cold read launched a runtime")
		}
		// And the journal itself -- every S3-offloaded record -- reads back
		// whole with no Host running.
		for _, tenant := range tenants {
			runtimeID := world.RuntimeSessionID(t, ctx, tenant, sessions[tenant])
			if done := orchestrationtest.CountJournalEvents[event.TurnDone](t, world.PooledWorld, tenant, runtimeID); done < 3 {
				t.Fatalf("%s's cold journal holds %d TurnDone, want at least 3", tenantLabel(tenant), done)
			}
		}
	})
}

// TestCloudGateAnsweredOverPgstoreAndS3store is the gate chain over the cloud
// composite: an agent's ask_user gate is projected into PostgreSQL, Factory
// serves it, the answer is admitted, applied and settles applied, and the
// agent's tool receives it.
func TestCloudGateAnsweredOverPgstoreAndS3store(t *testing.T) {
	cfg := requireCloud(t)
	ctx := cloudPlacementContext(t)
	tenant := orchestrationtest.PooledTenantA
	world := newCloudWorld(t, ctx, cfg, orchestrationtest.PooledWorldOptions{
		Tenants:     []sessionwire.TenantID{tenant},
		WithAskTool: true,
	})
	world.AskTool.Question = "cloud: which colour?"
	world.LLM.Script(
		orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledAskToolName, ToolInput: `{}`},
		orchestrationtest.PooledTurn{Text: "thank you"},
	)
	const session = sessionwire.SessionID("session-cloud-gate")
	const answer = "VERMILION"

	hostA := orchestrationtest.StartPooledHost(t, ctx, world.PooledWorld, "orchestrationtest-cloud-gate-host", 4)
	orchestrationtest.AwaitAdvertised(t, world.PooledWorld, hostA.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world.PooledWorld, "orchestrationtest-cloud-gate-replica", nil)

	cloudPost(t, ctx, served, tenant, "/v1/sessions", cloudCreate("gate-create", session, "ask me something"), http.StatusCreated)
	awaitApplied(t, ctx, world.PooledWorld, tenant, session, "gate-create", 120*time.Second)

	var projected sessionwire.GateProjection
	orchestrationtest.PooledWait(t, "the gate reached Factory's gates read", 90*time.Second, func() bool {
		page := served.OpenGates(t, ctx, tenant, session)
		if len(page.Gates) == 0 {
			return false
		}
		projected = page.Gates[0]
		return true
	})
	if projected.Answerability != sessionwire.GateAnswerabilityResident || projected.OpenedJournalSeq == 0 {
		t.Fatalf("the projected gate is %+v, want a resident gate with an opening sequence", projected)
	}
	status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(session)+"/gates/"+string(projected.GateID),
		sessionwire.GateResponseRequest{
			CommandEnvelope:        orchestrationtest.PooledEnvelope("gate-answer"),
			SessionID:              session,
			GateID:                 projected.GateID,
			Action:                 "answer",
			Values:                 map[string]json.RawMessage{"answer": json.RawMessage(`"` + answer + `"`)},
			ExpectedOpenJournalSeq: projected.OpenedJournalSeq,
		})
	if status != http.StatusAccepted {
		t.Fatalf("Factory answered the gate response %d: %s", status, body)
	}
	awaitApplied(t, ctx, world.PooledWorld, tenant, session, "gate-answer", 90*time.Second)
	entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID: tenant, SessionID: session, CommandID: "gate-answer",
	})
	if err != nil || entry.Record.Outcome == nil || entry.Record.Outcome.Kind != sessionstore.DispositionApplied {
		t.Fatalf("the answer settled %+v (%v), want applied", entry.Record.Outcome, err)
	}
	orchestrationtest.PooledWait(t, "the tool returned the user's answer", 60*time.Second, func() bool {
		answers := world.AskTool.Answers()
		return len(answers) == 1 && answers[0] == answer
	})
	orchestrationtest.PooledWait(t, "the gate projection cleared", 60*time.Second, func() bool {
		return len(served.OpenGates(t, ctx, tenant, session).Gates) == 0
	})
	runtimeID := world.RuntimeSessionID(t, ctx, tenant, session)
	if resolved := orchestrationtest.JournalEvents[event.GateResolved](t, world.PooledWorld, tenant, runtimeID); len(resolved) != 1 {
		t.Fatalf("the journal holds %d GateResolved, want exactly 1", len(resolved))
	}
	world.assertEncryptedJournalObjects(t, ctx, tenant)
}
