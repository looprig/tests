//go:build integration && !darwin && !linux

package tests

import (
	"errors"
	"testing"

	"github.com/looprig/sandbox"
)

func TestSandboxUnsupportedPlatformFailsClosed(t *testing.T) {
	workspace := t.TempDir()
	profile, err := sandbox.NewProfile(sandbox.ProfileConfig{
		WorkspaceRoot:  workspace,
		WorkspaceRead:  sandbox.Allow,
		WorkspaceWrite: sandbox.Deny,
		HostRead:       sandbox.Deny,
		HostWrite:      sandbox.Deny,
		Network:        sandbox.Deny,
		Command:        sandbox.Allow,
		Home:           sandbox.IsolatedHome,
		Isolation:      sandbox.Sandboxed,
	})
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	set, err := sandbox.NewExecutorSet(profile, sandbox.WithScratchRoot(t.TempDir()), sandbox.WithMaxExecutors(1))
	if err != nil {
		t.Fatalf("NewExecutorSet: %v", err)
	}
	t.Cleanup(func() { _ = set.Close() })
	if executor, err := set.For("unsupported"); !errors.Is(err, sandbox.ErrSandboxUnavailable) || executor != nil {
		t.Fatalf("unsupported platform ExecutorSet.For = (%v, %v), want (nil, ErrSandboxUnavailable)", executor, err)
	}
}
