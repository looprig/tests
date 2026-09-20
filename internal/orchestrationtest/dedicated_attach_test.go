//go:build integration

package orchestrationtest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

type readyDedicatedWorkload struct {
	RecordingWorkloads
	host *PooledHost
}

func (w *readyDedicatedWorkload) WorkloadEndpoint(_ context.Context, _ sessionstore.PlacementIntent) (sessionwire.HostID, uint64, sessionwire.InternalEndpoint, bool, error) {
	return w.host.ID, 1, w.host.Base, true, nil
}

// TestFactoryDedicatedAttachToReleasedHost proves Factory's first attach and
// bind against a released dedicated Host running a real harness rig and journal.
func TestFactoryDedicatedAttachToReleasedHost(t *testing.T) {
	ctx := laneContext(t)
	const (
		tenant  = PooledTenantA
		sid     = sessionwire.SessionID("session-dedicated-first-attach")
		command = sessionwire.CommandID("command-dedicated-first-attach")
	)
	world := NewPooledWorld(t, ctx, PooledWorldOptions{Tenants: []sessionwire.TenantID{tenant}})
	tap := NewHostLinkTap()
	h := StartDedicatedHost(t, ctx, world, "host-dedicated-first-attach", 1, sid, tap)
	if _, err := world.Store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{TenantID: tenant, SessionID: sid}); err == nil {
		t.Fatal("Host started with a resident registration")
	} else {
		var absence *sessionstore.KeyspaceError
		if !errors.As(err, &absence) || absence.Code != sessionstore.KeyspaceBindingNotFound {
			t.Fatalf("pre-create registration lookup: %v, want binding_not_found", err)
		}
	}
	if len(h.Rig.Creates()) != 0 || len(world.LLM.Requests()) != 0 {
		t.Fatal("Host runtime started before Factory attach")
	}
	workload := &readyDedicatedWorkload{host: h}
	f := StartDedicatedFactory(t, ctx, world, "dedicated-factory", io.Discard, workload)
	request := sessionwire.CreateRequest{CommandEnvelope: PooledEnvelope(string(command)), SessionID: sid, AgentID: PooledAgent,
		Blocks: json.RawMessage(`[{"type":"text","text":"first dedicated input"}]`)}
	status, body := f.Post(t, ctx, tenant, "/v1/sessions", request)
	if status != http.StatusCreated {
		t.Fatalf("Factory create answered %d: %s", status, body)
	}
	PooledWait(t, "dedicated command applied by harness", 15*time.Second, func() bool {
		return world.CommandState(ctx, tenant, sid, command) == sessionstore.InboxStateApplied && world.LLM.SawInRequest(0, "first dedicated input")
	})
	if len(workload.Ensured()) == 0 || len(h.Rig.Creates()) != 1 {
		t.Fatalf("workload ensures=%d, real harness creates=%d", len(workload.Ensured()), len(h.Rig.Creates()))
	}
	entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: sid, CommandID: command})
	if err != nil || entry.Record.Attempt == nil {
		t.Fatalf("settled command lacks an attempt: %+v %v", entry.Record, err)
	}
	evidence, err := world.Journals[tenant].ReadDispositionEvidence(ctx, sessionstore.DispositionEvidenceRequest{
		TenantID: tenant, SessionID: sid, CommandID: command, Kind: "create",
		RuntimeCommandID: entry.Record.Descriptor.RuntimeCommandID, Binding: entry.Record.Descriptor.Binding, Attempt: *entry.Record.Attempt,
	})
	if err != nil || evidence.Kind != sessionstore.DispositionApplied {
		t.Fatalf("real harness journal disposition=%+v err=%v", evidence, err)
	}
	owner, found, err := (&StoreDirectory{Store: world.Store}).Owner(ctx, tenant, sid)
	if err != nil || !found || owner.LeaseEpoch == 0 || owner.HostID != h.ID {
		t.Fatalf("Host registration=%+v found=%t err=%v", owner, found, err)
	}
	var attachEpoch, bindEpoch uint64
	for _, conn := range tap.Conns() {
		if conn.Path != "/hostlink/"+string(tenant) {
			continue
		}
		for _, rpc := range rpcsFor(t, conn, sessionwire.HostLinkMethodAttach) {
			requireAccepted(t, rpc, "dedicated attach")
			var observed sessionwire.HostLinkRegistryObservation
			if err := json.Unmarshal(rpc.reply.RPC.Data, &observed); err != nil {
				t.Fatalf("attach reply: %v", err)
			}
			attachEpoch = observed.LeaseEpoch
		}
		for _, rpc := range rpcsFor(t, conn, sessionwire.HostLinkMethodBind) {
			requireAccepted(t, rpc, "dedicated bind")
			var bind sessionwire.HostLinkBindRequest
			if err := json.Unmarshal(rpc.data, &bind); err != nil {
				t.Fatalf("bind request: %v", err)
			}
			bindEpoch = bind.LeaseEpoch
		}
	}
	if attachEpoch == 0 || bindEpoch != attachEpoch || owner.LeaseEpoch != attachEpoch {
		t.Fatalf("attach epoch=%d bind epoch=%d registry epoch=%d", attachEpoch, bindEpoch, owner.LeaseEpoch)
	}
}
