//go:build integration

// This file is the durable-profile lane host v0.16.0's design ruling owes
// (Gap A, decision A2): Host REFUSES a composition whose runtime cannot
// recover, at composition time, and holds a declaration true at launch.
//
//	refused      under the default RuntimeProfileDurable, host.Compose refuses
//	             a registration whose target declares no
//	             department.Recovery, naming the agent and what it lacks;
//	opted out    RuntimeProfileBestEffort composes the same registration and
//	             logs one WARN on Start;
//	accepted     both targets this kit really runs -- the pooledSession
//	             adapter and harnessruntime.Target -- compose under Durable;
//	held true    a target that DECLARES recovery over a runtime that has
//	             neither capability composes (a declaration is trusted at
//	             composition) and is refused at its first launch, with the
//	             session released.
//
// Every value is the released module's own: host.Compose, department,
// harnessruntime. The FakeRig/FakeRuntime pair is the kit's product-seam fake,
// which is exactly what an undeclared product runtime looks like.

package tests

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// TestADurableHostRefusesARuntimeThatCannotRecover is the lane.
func TestADurableHostRefusesARuntimeThatCannotRecover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{orchestrationtest.PooledTenantA},
	})
	const base = sessionwire.InternalEndpoint("ws://127.0.0.1:9")

	fakeTarget := func(t *testing.T, capabilities department.Capabilities) (department.LaunchTarget, *orchestrationtest.FakeRuntime) {
		t.Helper()
		id, err := uuid.New()
		if err != nil {
			t.Fatal(err)
		}
		runtime := orchestrationtest.NewFakeRuntime(id)
		target, err := department.NewRigTarget(&orchestrationtest.FakeRig{Session: runtime}, orchestrationtest.PooledCompatibility, capabilities)
		if err != nil {
			t.Fatalf("building the fake target: %v", err)
		}
		return target, runtime
	}
	registering := func(target department.LaunchTarget) host.Registrar {
		return host.RegistrarFunc(func(context.Context) ([]department.Registration, error) {
			return []department.Registration{{AgentID: orchestrationtest.PooledAgent, Target: target}}, nil
		})
	}
	closeUnstarted := func(t *testing.T, service *host.Service) {
		t.Helper()
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := service.CloseUnstarted(closeCtx); err != nil {
			t.Errorf("closing the composed, unstarted host: %v", err)
		}
	}
	stop := func(t *testing.T, service *host.Service) {
		t.Helper()
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := service.Stop(stopCtx); err != nil {
			t.Logf("stopping the composed host: %v", err)
		}
	}

	t.Run("the default profile refuses a target that declares no recovery, at Compose", func(t *testing.T) {
		blueprint, _ := orchestrationtest.PooledHostComposition(t, world, "rp-refused", 1, base)
		if blueprint.RuntimeProfile != host.RuntimeProfileDurable {
			t.Fatalf("the kit's composition runs under %s, want the durable default", blueprint.RuntimeProfile)
		}
		target, _ := fakeTarget(t, orchestrationtest.KitCapabilities())
		blueprint.Collaborators.Registrar = registering(target)
		service, err := host.Compose(ctx, blueprint)
		if err == nil {
			closeUnstarted(t, service)
			t.Fatal("host.Compose accepted a target that declares no recovery under RuntimeProfileDurable")
		}
		var invalid *host.InvalidCompositionError
		if !errors.As(err, &invalid) || invalid.Field != "Collaborators.Registrar" {
			t.Fatalf("host.Compose refused with %v, want *host.InvalidCompositionError on Collaborators.Registrar", err)
		}
		for _, named := range []string{string(orchestrationtest.PooledAgent), "AttemptCloser", "PersistenceFaults"} {
			if !strings.Contains(invalid.Reason, named) {
				t.Errorf("the refusal does not name %q: %s", named, invalid.Reason)
			}
		}
	})

	t.Run("RuntimeProfileBestEffort composes the same target and warns once on Start", func(t *testing.T) {
		blueprint, _ := orchestrationtest.PooledHostComposition(t, world, "rp-best-effort", 1, base)
		target, _ := fakeTarget(t, orchestrationtest.KitCapabilities())
		logs := &laneLog{}
		blueprint.Collaborators.Registrar = registering(target)
		blueprint.Collaborators.Logger = slog.New(slog.NewJSONHandler(logs, nil))
		blueprint.RuntimeProfile = host.RuntimeProfileBestEffort
		service, err := host.Compose(ctx, blueprint)
		if err != nil {
			t.Fatalf("host.Compose refused a best-effort composition: %v", err)
		}
		if err := service.Start(ctx); err != nil {
			stop(t, service)
			t.Fatalf("starting the best-effort host: %v", err)
		}
		stop(t, service)
		logs.mu.Lock()
		text := logs.buf.String()
		logs.mu.Unlock()
		if got := strings.Count(text, "RuntimeProfileBestEffort admits runtimes without declared recovery"); got != 1 {
			t.Fatalf("the best-effort host logged its warning %d times, want once:\n%s", got, text)
		}
	})

	t.Run("both runtimes this kit runs compose under the durable default", func(t *testing.T) {
		for _, arm := range []struct {
			name    string
			harness bool
		}{{"kit pooledSession adapter", false}, {"harnessruntime.Target", true}} {
			armWorld := world
			if arm.harness {
				armWorld = orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
					Tenants:        []sessionwire.TenantID{orchestrationtest.PooledTenantA},
					HarnessRuntime: true,
				})
			}
			blueprint, _ := orchestrationtest.PooledHostComposition(t, armWorld, sessionwire.HostID("rp-durable-"+map[bool]string{false: "kit", true: "harness"}[arm.harness]), 1, base)
			service, err := host.Compose(ctx, blueprint)
			if err != nil {
				t.Fatalf("%s: host.Compose refused the kit's own durable composition: %v", arm.name, err)
			}
			closeUnstarted(t, service)
		}
	})

	t.Run("a declaration the runtime cannot honour is refused at its first launch", func(t *testing.T) {
		target, runtime := fakeTarget(t, orchestrationtest.PooledCapabilities())
		blueprint, _ := orchestrationtest.PooledHostComposition(t, world, "rp-false-declaration", 1, base)
		blueprint.Collaborators.Registrar = registering(target)
		service, err := host.Compose(ctx, blueprint)
		if err != nil {
			t.Fatalf("host.Compose refused a target that DECLARES recovery (the declaration is trusted at composition): %v", err)
		}
		closeUnstarted(t, service)

		_, err = target.Create(ctx, department.CreateRequest{
			TenantID:  orchestrationtest.PooledTenantA,
			SessionID: "session-false-declaration",
			AgentID:   orchestrationtest.PooledAgent,
			Placement: sessionwire.HostPlacementPooled,
		})
		var incapable *department.IncapableRuntimeError
		if !errors.As(err, &incapable) {
			t.Fatalf("launching over a runtime without recovery answered %v, want *department.IncapableRuntimeError", err)
		}
		missing := strings.Join(incapable.Missing, "; ")
		for _, want := range []string{"declared AttemptCloser absent", "declared PersistenceFaults absent"} {
			if !strings.Contains(missing, want) {
				t.Errorf("the refusal's Missing is %q, want it to name %q", missing, want)
			}
		}
		if released := runtime.Released(); released != 1 {
			t.Fatalf("the refused runtime was released %d times, want once: a refused launch must give its session back", released)
		}
	})
}
