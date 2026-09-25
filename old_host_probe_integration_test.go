//go:build integration

// A released Host v0.10.3 runs in a separate process and module graph. Its
// v0.13.1 SessionStore reader is deliberately beside v3 rows only in this
// compatibility probe, never as a supported production rollout pattern.
package tests

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/natsstore"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

func startOldHostJetStream(t *testing.T) string {
	t.Helper()
	engine, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoSigs: true, NoLog: true,
	})
	if err != nil {
		t.Fatalf("start JetStream: %v", err)
	}
	go engine.Start()
	if !engine.ReadyForConnections(10 * time.Second) {
		engine.Shutdown()
		t.Fatal("JetStream did not become ready")
	}
	t.Cleanup(func() { engine.Shutdown(); engine.WaitForShutdown() })
	return engine.ClientURL()
}

type oldHostProcess struct {
	ID   sessionwire.HostID
	Base sessionwire.InternalEndpoint
}

func startOldHostProcess(t *testing.T, ctx context.Context, natsURL string, id sessionwire.HostID) oldHostProcess {
	t.Helper()
	module := filepath.Join("oldhostlane")
	// The build-only module must remain on released old code. A future
	// transitive bump that lifts Host or harness would void this probe.
	versions := exec.CommandContext(ctx, "go", "list", "-m", "github.com/looprig/host", "github.com/looprig/harness")
	versions.Dir = module
	versions.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=go1.26.8")
	listed, err := versions.CombinedOutput()
	if err != nil || !bytes.Contains(listed, []byte("github.com/looprig/host v0.10.3")) ||
		!bytes.Contains(listed, []byte("github.com/looprig/harness v0.40.2")) {
		t.Fatalf("oldhostlane has wrong published pins: %s (%v)", listed, err)
	}
	binary := filepath.Join(t.TempDir(), "oldhost")
	build := exec.CommandContext(ctx, "go", "build", "-tags", "integration kind", "-o", binary, "./cmd/oldhost")
	build.Dir = module
	build.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=go1.26.8")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building released old Host: %v\n%s", err, output)
	}
	command := exec.CommandContext(ctx, binary)
	command.Env = append(os.Environ(), "OLDHOST_STORE_URL="+natsURL, "OLDHOST_ID="+string(id))
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := os.CreateTemp(t.TempDir(), "oldhost-stderr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stderr.Close() })
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		t.Fatalf("starting released old Host: %v", err)
	}
	finished := make(chan error, 1)
	go func() { finished <- command.Wait() }()
	t.Cleanup(func() {
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-finished:
			if err != nil {
				data, _ := os.ReadFile(stderr.Name())
				t.Logf("old Host exit: %v; stderr: %s", err, data)
			}
		case <-time.After(15 * time.Second):
			_ = command.Process.Kill()
			<-finished
			t.Error("old Host did not drain within 15 seconds")
		}
	})
	type announcement struct {
		ID   string `json:"id"`
		Base string `json:"base"`
	}
	ready := make(chan struct {
		line []byte
		err  error
	}, 1)
	go func() {
		reader := bufio.NewReader(stdout)
		line, err := reader.ReadBytes('\n')
		ready <- struct {
			line []byte
			err  error
		}{line, err}
		_, _ = io.Copy(io.Discard, reader)
	}()
	select {
	case result := <-ready:
		if result.err != nil {
			data, _ := os.ReadFile(stderr.Name())
			t.Fatalf("old Host did not announce: %v; stderr: %s", result.err, data)
		}
		var announced announcement
		if err := json.Unmarshal(result.line, &announced); err != nil || announced.ID != string(id) || announced.Base == "" {
			t.Fatalf("old Host announcement %q: %+v (%v)", result.line, announced, err)
		}
		return oldHostProcess{ID: id, Base: sessionwire.InternalEndpoint(announced.Base)}
	case <-time.After(30 * time.Second):
		data, _ := os.ReadFile(stderr.Name())
		t.Fatalf("old Host startup timed out; stderr: %s", data)
		return oldHostProcess{}
	}
}

func assertOldHostNoAttempt(t *testing.T, ctx context.Context, world *orchestrationtest.PooledWorld, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) {
	t.Helper()
	entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID: tenant, SessionID: session, CommandID: command,
	})
	if err != nil {
		t.Fatalf("reading pending command %s: %v", command, err)
	}
	if entry.Record.State != sessionstore.InboxStatePending || entry.Record.Attempt != nil {
		t.Fatalf("old Host could reach an attempt: state=%s attempt=%+v", entry.Record.State, entry.Record.Attempt)
	}
}

func TestOldHostNeverReceivesAStampedCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	natsURL := startOldHostJetStream(t)
	store, err := natsstore.Open(ctx, natsstore.Options{URL: natsURL})
	if err != nil {
		t.Fatalf("natsstore.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	tenant := orchestrationtest.PooledTenantA
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{tenant}, Backend: store.Composite,
	})
	old := startOldHostProcess(t, ctx, natsURL, "i-oldhost-v0103")
	orchestrationtest.AwaitAdvertised(t, world, old.ID)
	if orchestrationtest.HostAdvertises(t, old.Base, tenant, sessionwire.HostLinkCapabilityAttributionPrincipal) {
		t.Fatal("released host v0.10.3 advertised the attribution token")
	}

	plain := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{Replica: "old-plain"})
	const resident = sessionwire.SessionID("old-resident")
	status, body := plain.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("old-create-1"), SessionID: resident,
		AgentID: orchestrationtest.PooledAgent,
	})
	if status != http.StatusCreated {
		t.Fatalf("plain create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "bare create applied on old Host", 120*time.Second, func() bool {
		return world.CommandState(ctx, tenant, resident, "old-create-1") == sessionstore.InboxStateApplied
	})
	if owner, ok := world.Registration(t, ctx, tenant, resident); !ok || owner.HostID != old.ID {
		t.Fatalf("resident owner = %+v, found=%v; want %s", owner, ok, old.ID)
	}
	plain.Stop()

	stamping := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica: "old-stamping", PrincipalStamping: true,
	})
	status, body = stamping.Post(t, ctx, tenant, "/v1/sessions/"+string(resident)+"/input", sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("old-input-1"), SessionID: resident,
		Blocks: principalLaneBlocks(t, "hello"),
	})
	if status != http.StatusUnprocessableEntity || !strings.Contains(body, "runtime_unavailable") {
		t.Fatalf("input to incapable owner answered %d: %s", status, body)
	}
	if state := world.CommandState(ctx, tenant, resident, "old-input-1"); state != "" {
		t.Fatalf("refused input left durable state %s", state)
	}

	const waiting = sessionwire.SessionID("old-waiting")
	status, body = stamping.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("old-create-2"), SessionID: waiting,
		AgentID: orchestrationtest.PooledAgent, Blocks: principalLaneBlocks(t, "wait for a capable host"),
	})
	if status != http.StatusCreated {
		t.Fatalf("stamped create answered %d: %s", status, body)
	}
	deadline := time.Now().Add(5 * orchestrationtest.ReconcileSweepInterval)
	for time.Now().Before(deadline) {
		if state := world.CommandState(ctx, tenant, waiting, "old-create-2"); state != sessionstore.InboxStatePending {
			t.Fatalf("stamped create left pending (%s) with only an incapable Host", state)
		}
		time.Sleep(orchestrationtest.ReconcileSweepInterval / 2)
	}
	assertOldHostNoAttempt(t, ctx, world, tenant, waiting, "old-create-2")
	capable := orchestrationtest.StartPooledHost(t, ctx, world, "i-newhost-v0110", 1)
	orchestrationtest.PooledWait(t, "stamped create applied on capable Host", 120*time.Second, func() bool {
		return world.CommandState(ctx, tenant, waiting, "old-create-2") == sessionstore.InboxStateApplied
	})
	if owner, ok := world.Registration(t, ctx, tenant, waiting); !ok || owner.HostID != capable.ID {
		t.Fatalf("stamped create owner = %+v, found=%v; want %s", owner, ok, capable.ID)
	}
	// The old Host cannot have begun either attempt: the input has no row;
	// the create had no Attempt before the capable Host arrived, and its
	// terminal registration names the capable Host. Released v0.10.3 offers
	// no attempt-begun operator log, so durable state is the evidence.
	if state := world.CommandState(ctx, tenant, resident, "old-input-1"); state != "" {
		t.Fatalf("old Host input acquired state %s after capable placement", state)
	}
	t.Logf("released old Host %s remained incapable; stamped create placed on %s", old.ID, capable.ID)
}
