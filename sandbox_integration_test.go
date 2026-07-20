//go:build integration && (darwin || linux)

package tests

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/looprig/sandbox"
)

func TestSandboxExecutorSetOwnsHomeAndTempLifecycle(t *testing.T) {
	workspace := t.TempDir()
	scratch := t.TempDir()
	sentinel := filepath.Join(scratch, "caller-owned")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	profile := newSandboxIntegrationProfile(t, workspace, sandbox.Allow, sandbox.Deny)
	set, err := sandbox.NewExecutorSet(profile, sandbox.WithScratchRoot(scratch), sandbox.WithMaxExecutors(2))
	if err != nil {
		t.Fatalf("NewExecutorSet: %v", err)
	}
	t.Cleanup(func() { _ = set.Close() })

	alpha := sandboxIntegrationExecutor(t, set, "alpha")
	beta := sandboxIntegrationExecutor(t, set, "beta")
	alphaHome, alphaTemp := sandboxChildHomeAndTemp(t, alpha, workspace)
	betaHome, betaTemp := sandboxChildHomeAndTemp(t, beta, workspace)
	if alphaHome == betaHome || alphaTemp == betaTemp {
		t.Fatalf("distinct executors shared HOME or TMPDIR: alpha=(%q,%q) beta=(%q,%q)", alphaHome, alphaTemp, betaHome, betaTemp)
	}
	ownedRoot := filepath.Dir(alphaHome)
	canonicalScratch, err := filepath.EvalSymlinks(scratch)
	if err != nil {
		t.Fatalf("resolve caller scratch: %v", err)
	}
	if filepath.Dir(ownedRoot) != canonicalScratch {
		t.Fatalf("set-owned root %q is not an immediate child of canonical caller scratch %q", ownedRoot, canonicalScratch)
	}
	for _, path := range []string{ownedRoot, alphaHome, alphaTemp, betaHome, betaTemp} {
		assertOwnerOnlyDirectory(t, path)
		if path != ownedRoot && filepath.Dir(path) != ownedRoot {
			t.Fatalf("executor path %q is not an immediate child of owned root %q", path, ownedRoot)
		}
	}
	if filepath.Dir(betaHome) != ownedRoot || filepath.Dir(betaTemp) != ownedRoot {
		t.Fatalf("executor paths do not share the set-owned root %q", ownedRoot)
	}

	if err := set.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(ownedRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("set-owned root remains after Close: %v", err)
	}
	if got, err := os.ReadFile(sentinel); err != nil || string(got) != "keep" {
		t.Fatalf("caller scratch content after Close = %q, %v", got, err)
	}
	if _, err := set.For("closed"); !errors.Is(err, sandbox.ErrExecutorSetClosed) {
		t.Fatalf("For after Close error = %v, want ErrExecutorSetClosed", err)
	}
}

func TestSandboxFilesystemPathAndTreeGrantsAreDistinct(t *testing.T) {
	workspace := t.TempDir()
	profile := newSandboxIntegrationProfile(t, workspace, sandbox.Gated, sandbox.Deny)
	set, err := sandbox.NewExecutorSet(profile, sandbox.WithScratchRoot(t.TempDir()), sandbox.WithMaxExecutors(1))
	if err != nil {
		t.Fatalf("NewExecutorSet: %v", err)
	}
	t.Cleanup(func() { _ = set.Close() })
	executor := sandboxIntegrationExecutor(t, set, "grants")

	exact := filepath.Join(workspace, "exact.txt")
	if err := os.WriteFile(exact, []byte("original"), 0o600); err != nil {
		t.Fatalf("pre-create exact Path target: %v", err)
	}
	exactCommand := "printf path > " + shellQuote(exact)
	exactGrant := issueSandboxGrant(t, executor, "path-create", exactCommand, workspace,
		"filesystem.write", exact, "filesystem.path.write.v1", exact)
	if out, code, err := executor.RunCommandWithGrants(context.Background(), "path-create", workspace, exactCommand, []string{exactGrant}); err != nil || code != 0 {
		t.Fatalf("exact Path grant run = code %d err %v out %q", code, err, out)
	}
	if got, err := os.ReadFile(exact); err != nil || string(got) != "path" {
		t.Fatalf("exact Path target after grant = %q, %v", got, err)
	}

	sibling := filepath.Join(workspace, "sibling.txt")
	siblingCommand := "printf denied > " + shellQuote(sibling)
	exactOnly := issueSandboxGrant(t, executor, "path-not-tree", siblingCommand, workspace,
		"filesystem.write", exact, "filesystem.path.write.v1", exact)
	if out, code, err := executor.RunCommandWithGrants(context.Background(), "path-not-tree", workspace, siblingCommand, []string{exactOnly}); err != nil {
		t.Fatalf("exact Path denial returned a spawn error: %v (out=%q)", err, out)
	} else if code == 0 {
		t.Fatalf("exact Path grant widened to sibling %q (out=%q)", sibling, out)
	}
	if _, err := os.Stat(sibling); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sibling exists after exact Path grant: %v", err)
	}

	nested := filepath.Join(workspace, "nested")
	child := filepath.Join(nested, "child.txt")
	treeCommand := "mkdir -p " + shellQuote(nested) + " && printf tree > " + shellQuote(child)
	treeGrant := issueSandboxGrant(t, executor, "tree-create", treeCommand, workspace,
		"filesystem.write", "tree:"+workspace, "filesystem.tree.write.v1", workspace)
	if out, code, err := executor.RunCommandWithGrants(context.Background(), "tree-create", workspace, treeCommand, []string{treeGrant}); err != nil || code != 0 {
		t.Fatalf("recursive Tree grant run = code %d err %v out %q", code, err, out)
	}
	if got, err := os.ReadFile(child); err != nil || string(got) != "tree" {
		t.Fatalf("tree-created child = %q, %v", got, err)
	}
}

func TestSandboxGrantRejectsPostApprovalSymlinkSwap(t *testing.T) {
	workspace := t.TempDir()
	profile := newSandboxIntegrationProfile(t, workspace, sandbox.Gated, sandbox.Deny)
	set, err := sandbox.NewExecutorSet(profile, sandbox.WithScratchRoot(t.TempDir()), sandbox.WithMaxExecutors(1))
	if err != nil {
		t.Fatalf("NewExecutorSet: %v", err)
	}
	t.Cleanup(func() { _ = set.Close() })
	executor := sandboxIntegrationExecutor(t, set, "swap")

	target := filepath.Join(workspace, "approved.txt")
	outside := filepath.Join(t.TempDir(), "outside.txt")
	for _, path := range []string{target, outside} {
		if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	const command = "true"
	grant := issueSandboxGrant(t, executor, "symlink-swap", command, workspace,
		"filesystem.write", target, "filesystem.path.write.v1", target)
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, target); err != nil {
		t.Fatal(err)
	}
	if _, _, err := executor.RunCommandWithGrants(context.Background(), "symlink-swap", workspace, command, []string{grant}); !errors.Is(err, sandbox.ErrGrantTargetChanged) {
		t.Fatalf("post-approval symlink swap error = %v, want ErrGrantTargetChanged", err)
	}
}

func TestSandboxBroadNetworkGrantCarriesDNS(t *testing.T) {
	curl := requireExternalHTTPS(t)
	workspace := t.TempDir()
	profile := newSandboxIntegrationProfile(t, workspace, sandbox.Allow, sandbox.Gated)
	set, err := sandbox.NewExecutorSet(profile, sandbox.WithScratchRoot(t.TempDir()), sandbox.WithMaxExecutors(1))
	if err != nil {
		t.Fatalf("NewExecutorSet: %v", err)
	}
	t.Cleanup(func() { _ = set.Close() })
	executor := sandboxIntegrationExecutor(t, set, "broad-dns")
	if !executor.Guarantees().NetworkBoundary {
		t.Fatalf("NetworkBoundary guarantee is false for gated network")
	}

	command := shellQuote(curl) + " --noproxy '*' --fail --silent --show-error --connect-timeout 8 --max-time 15 https://example.com/"
	grant := issueSandboxGrant(t, executor, "broad-dns", command, workspace,
		"network", "", "network.broad.v1", "tcp:*:443")
	out, code, err := executor.RunCommandWithGrants(context.Background(), "broad-dns", workspace, command, []string{grant})
	if err != nil || code != 0 || len(out) == 0 {
		t.Fatalf("broad network + DNS run = code %d err %v out %q", code, err, out)
	}
}

func newSandboxIntegrationProfile(t *testing.T, workspace string, workspaceWrite, network sandbox.Access) *sandbox.Profile {
	t.Helper()
	profile, err := sandbox.NewProfile(sandbox.ProfileConfig{
		WorkspaceRoot:  workspace,
		WorkspaceRead:  sandbox.Allow,
		WorkspaceWrite: workspaceWrite,
		HostRead:       sandbox.Allow,
		HostWrite:      sandbox.Deny,
		Network:        network,
		Command:        sandbox.Allow,
		Home:           sandbox.IsolatedHome,
		Isolation:      sandbox.Sandboxed,
	})
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	return profile
}

func sandboxIntegrationExecutor(t *testing.T, set *sandbox.ExecutorSet, key string) *sandbox.Executor {
	t.Helper()
	executor, err := set.For(key)
	if errors.Is(err, sandbox.ErrSandboxUnavailable) && runtime.GOOS == "linux" {
		t.Skip("Linux sandbox prerequisites unavailable: requires either rung 1 user/mount/network namespaces or rung 2 Landlock v4 plus seccomp")
	}
	if err != nil {
		t.Fatalf("ExecutorSet.For(%q): %v", key, err)
	}
	return executor
}

func sandboxChildHomeAndTemp(t *testing.T, executor *sandbox.Executor, workspace string) (string, string) {
	t.Helper()
	out, code, err := executor.RunCommand(context.Background(), workspace, `printf '%s\n%s\n' "$HOME" "$TMPDIR"`)
	if err != nil || code != 0 {
		t.Fatalf("read child HOME/TMPDIR = code %d err %v out %q", code, err, out)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 2 || lines[0] == "" || lines[1] == "" {
		t.Fatalf("child HOME/TMPDIR output = %q", out)
	}
	return lines[0], lines[1]
}

func assertOwnerOnlyDirectory(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("%q mode = %v, want owner-only directory", path, info.Mode())
	}
}

func issueSandboxGrant(t *testing.T, executor *sandbox.Executor, executionID, command, cwd, kind, scope, class, target string) string {
	t.Helper()
	token, err := executor.IssueGrant(context.Background(), executionID, command, cwd, kind, scope, class, target, time.Now().Add(time.Minute).UnixMilli())
	if err != nil {
		t.Fatalf("IssueGrant(%s): %v", class, err)
	}
	return token
}

func requireExternalHTTPS(t *testing.T) string {
	t.Helper()
	curl, err := exec.LookPath("curl")
	if err != nil {
		t.Skip("broad DNS integration requires curl")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, curl, "--noproxy", "*", "--fail", "--silent", "--show-error", "--connect-timeout", "8", "--max-time", "15", "https://example.com/")
	if output, err := command.CombinedOutput(); err != nil {
		t.Skipf("live external DNS/HTTPS prerequisite unavailable before sandboxing: %v (%s)", err, output)
	}
	return curl
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
