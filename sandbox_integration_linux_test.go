//go:build integration && linux

package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/looprig/sandbox"
)

func TestSandboxLinuxSelectedRungReportsOnlyEnforcedGuarantees(t *testing.T) {
	workspace := t.TempDir()
	profile := newSandboxIntegrationProfile(t, workspace, sandbox.Gated, sandbox.Deny)
	set, err := sandbox.NewExecutorSet(profile, sandbox.WithScratchRoot(t.TempDir()), sandbox.WithMaxExecutors(1))
	if err != nil {
		t.Fatalf("NewExecutorSet: %v", err)
	}
	t.Cleanup(func() { _ = set.Close() })
	executor := sandboxIntegrationExecutor(t, set, "linux-rung")
	guarantees := executor.Guarantees()
	if !guarantees.WriteBoundary || !guarantees.NetworkBoundary || !guarantees.EnvScrub {
		t.Fatalf("selected Linux rung guarantees = %+v, want write, network, and environment boundaries", guarantees)
	}
	if guarantees.TargetNetwork {
		t.Fatalf("selected Linux rung claimed target-network enforcement without the parent-proxy boundary: %+v", guarantees)
	}
	switch executor.Level() {
	case sandbox.LevelFull:
		if !guarantees.ProcessBoundary || !guarantees.AddressNetwork {
			t.Fatalf("rung 1 guarantees = %+v, want process and address boundaries", guarantees)
		}
	case sandbox.LevelDegraded:
		if guarantees.ProcessBoundary || guarantees.AddressNetwork {
			t.Fatalf("rung 2 guarantees = %+v, must not claim process or address boundaries", guarantees)
		}
	default:
		t.Fatalf("selected Linux rung level = %d, want LevelFull or LevelDegraded", executor.Level())
	}
}

func TestSandboxLinuxTargetGrantFailsClosedWithoutTargetGuarantee(t *testing.T) {
	workspace := t.TempDir()
	profile := newSandboxIntegrationProfile(t, workspace, sandbox.Allow, sandbox.Gated)
	route, err := sandbox.NewUpstreamEgressRoute("http://127.0.0.1:1", true)
	if err != nil {
		t.Fatalf("NewUpstreamEgressRoute: %v", err)
	}
	set, err := sandbox.NewExecutorSet(profile, sandbox.WithScratchRoot(t.TempDir()), sandbox.WithMaxExecutors(1), sandbox.WithEgressRoute(route))
	if err != nil {
		t.Fatalf("NewExecutorSet: %v", err)
	}
	t.Cleanup(func() { _ = set.Close() })
	executor := sandboxIntegrationExecutor(t, set, "linux-target")
	if executor.Guarantees().TargetNetwork {
		t.Fatalf("Linux executor claimed TargetNetwork without child-to-proxy enforcement")
	}
	_, err = executor.IssueGrant(context.Background(), "linux-target", "true", workspace,
		"network", "", "network.proxy-target.v1", "tcp:example.com:443", time.Now().Add(time.Minute).UnixMilli())
	if !errors.Is(err, sandbox.ErrGrantGuaranteeMismatch) {
		t.Fatalf("Linux target grant error = %v, want ErrGrantGuaranteeMismatch", err)
	}
}
