//go:build integration

package orchestrationtest

import (
	"context"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/host/department"
	"github.com/looprig/host/department/departmenttest"
)

// ConformanceTarget is the launch target a Host of this world registers --
// the kit's pooledSession adapter, or harnessruntime.Target in a
// HarnessRuntime world -- built over this world's REAL per-tenant harness
// rigs, with the launch function host v0.16.0's
// departmenttest.RunRuntimeConformance needs.
//
// The launch moves the probe onto the world's first tenant (the rigs are per
// tenant, and the conformance probe names a tenant no rig knows), which the
// Launch contract permits, and leaves RigSessionID alone. Its Recorder reads
// the session's own harness journal: harness stamps the admitted
// RuntimeCommandID as the cause of the turn an input starts, and the turn's
// MessageInput carries what the runtime received -- so a principal or metadata
// dropped anywhere between Host's seam and harness is seen, not assumed.
//
// The world's model must answer: the conformance applies a real input.
func (w *PooledWorld) ConformanceTarget(tb TB) (department.LaunchTarget, departmenttest.Launch) {
	tb.Helper()
	tenant := w.tenants[0]
	rigs := map[sessionwire.TenantID]*rig.Rig{}
	for _, each := range w.tenants {
		rigs[each] = w.defineRig(tb, each, w.Journals[each], w.workspaces, w.workspaceBase)
	}
	product := &PooledRig{rigs: rigs, recorder: &pooledRecorder{}, tails: w.Tails, bridge: w.ProductJournal == nil, live: map[pooledTailKey]*pooledSession{}}
	target, err := w.pooledTarget(product)
	if err != nil {
		tb.Fatalf("orchestrationtest: building the conformance target: %v", err)
		return nil, nil
	}
	launch := func(ctx context.Context, target department.LaunchTarget, request department.CreateRequest) (department.Runtime, departmenttest.Recorder, error) {
		request.TenantID = tenant
		runtime, err := target.Create(ctx, request)
		if err != nil {
			return nil, nil, err
		}
		return runtime, conformanceRecorder{tb: tb, world: w, tenant: tenant, runtime: runtime}, nil
	}
	return target, launch
}

// conformanceRecorder is a departmenttest.Recorder over a session's harness
// journal.
type conformanceRecorder struct {
	tb      TB
	world   *PooledWorld
	tenant  sessionwire.TenantID
	runtime department.Runtime
}

func (r conformanceRecorder) Applied(command department.RuntimeCommand) (departmenttest.Applied, bool) {
	id, ok := department.RigSessionID(r.runtime)
	if !ok {
		return departmenttest.Applied{}, false
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var found *departmenttest.Applied
		walkJournal(r.tb, r.world, r.tenant, id, func(next event.Event, _ uint64) {
			var header event.Header
			var input *event.MessageInput
			switch value := next.(type) {
			case event.TurnStarted:
				header, input = value.Header, value.Input
			case event.TurnFoldedInto:
				header, input = value.Header, value.Input
			default:
				return
			}
			if found == nil && input != nil && header.Cause.CommandID == command.RuntimeCommandID {
				found = &departmenttest.Applied{Principal: input.Principal, Metadata: input.Metadata}
			}
		})
		if found != nil {
			return *found, true
		}
		if time.Now().After(deadline) {
			return departmenttest.Applied{}, false
		}
		time.Sleep(50 * time.Millisecond)
	}
}
