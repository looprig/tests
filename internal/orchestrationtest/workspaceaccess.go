//go:build integration

package orchestrationtest

import (
	"os"
	"path/filepath"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/harness/pkg/workspacestore"
	"github.com/looprig/storage/memstore"
	"github.com/looprig/tools"
)

// ---- harness v0.44.0's loop.WithWorkspaceAccess, as a product composes it ----
//
// A PooledWorldOptions.WorkspaceAccess world gives the agent the STANDARD
// WriteFile tool (github.com/looprig/tools) over ONE shared directory, under
// loop.WithWorkspaceAccess(loop.WorkspaceAccess{Roots: {that directory}}) with
// no Approver and no Rules -- the defaults, which are the claim under test:
//
//   - a write inside the root opens the loop's DURABLE permission gate, which
//     Host projects and Factory answers with a gate_response, so the same
//     approver serves a browser as serves the TUI;
//   - "Approve always for this workspace" is remembered in an in-memory rule
//     store per session loop, so it skips the next prompt IN THAT SESSION ONLY.
//
// The directory is a rig.WithSharedWorkspace root rather than a per-session
// workspace ON PURPOSE: every session's WriteFile then resolves one relative
// path to the SAME absolute file, so a second session being prompted for a
// file the first session was told "always" for is a statement about the rule
// store's scope, not about two different paths.

// WorkspaceAccessRoot is the shared directory a WorkspaceAccess world's agent
// may write, with approval; empty in any other world. It is the canonical
// (symlink-resolved) path, which is what the gate and the tool both judge.
func (w *PooledWorld) WorkspaceAccessRoot() string { return w.accessRoot }

// openWorkspaceAccessRoot makes the shared root.
func openWorkspaceAccessRoot(tb TB) string {
	tb.Helper()
	root := filepath.Join(NewTempWorkspaces(tb).Root, "shared-workspace")
	if err := os.MkdirAll(root, 0o700); err != nil {
		tb.Fatalf("orchestrationtest: creating the shared workspace root: %v", err)
		return ""
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		tb.Fatalf("orchestrationtest: resolving the shared workspace root: %v", err)
		return ""
	}
	return canonical
}

// workspaceAccessLoopOptions are the loop options a WorkspaceAccess world's
// agent is defined with: its tools plus the workspace gate, which takes the
// access-gate slot and derives the policy revision itself.
func (w *PooledWorld) workspaceAccessLoopOptions(extra []tool.Definition) []loop.Option {
	return []loop.Option{
		loop.WithTools(append(extra, tools.WriteFileDefinition())...),
		loop.WithWorkspaceAccess(loop.WorkspaceAccess{Roots: []string{w.accessRoot}}),
	}
}

// workspaceAccessRigOptions place every session of tenant's rig on the shared
// root. A placement requires a snapshot store and policy; the policy is
// manual and best-effort (a shared root cannot require one), and nothing in the
// lane asks for a snapshot.
func (w *PooledWorld) workspaceAccessRigOptions(tb TB, tenant sessionwire.TenantID) []rig.Option {
	tb.Helper()
	spool := filepath.Join(filepath.Dir(w.accessRoot), "spool-access-"+hashForPath(string(tenant)))
	if err := os.MkdirAll(spool, 0o700); err != nil {
		tb.Fatalf("orchestrationtest: creating the shared workspace spool: %v", err)
		return nil
	}
	store, err := workspacestore.Open(memstore.New().Blobs, workspacestore.WithSpoolDir(spool))
	if err != nil {
		tb.Fatalf("orchestrationtest: opening the shared workspace store: %v", err)
		return nil
	}
	return []rig.Option{
		rig.WithSharedWorkspace(store, w.accessRoot),
		rig.WithSnapshots(rig.SnapshotPolicy{Trigger: rig.SnapshotManual}),
	}
}
