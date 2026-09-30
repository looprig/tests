//go:build integration

// This file is the workspace-gate lane host v0.16.0 / harness v0.44.0's design
// ruling owes (Gap C): a BROWSER session -- every command sent as a ClientLink
// RPC, the way a browser sends one -- whose loop is defined with
// loop.WithWorkspaceAccess and nothing else: no Approver, no Rules. The claim
// is harness's "same approver everywhere":
//
//	a write inside the root opens the loop's DURABLE permission gate; Host
//	projects it; the browser answers it through Factory's gate.respond; Host
//	applies the answer as a gate_response; and the write lands;
//
//	"Approve always for this workspace" skips the next prompt for that file IN
//	THAT SESSION ONLY -- another session writing the very same file is asked.
//
// Everything is real: factory.New and host.Compose over TCP, the harness rig
// (through harnessruntime.Target) and its journal, the standard WriteFile tool
// from github.com/looprig/tools, and the file on disk.

package tests

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// TestABrowserAnswersAWorkspaceWriteGateAndAlwaysIsPerSession is the lane.
func TestABrowserAnswersAWorkspaceWriteGateAndAlwaysIsPerSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	tenant := orchestrationtest.PooledTenantA
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:         []sessionwire.TenantID{tenant},
		HarnessRuntime:  true,
		WorkspaceAccess: true,
	})
	root := world.WorkspaceAccessRoot()
	orchestrationtest.StartPooledHost(t, ctx, world, "wa-host", 1)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "wa-replica", nil)
	browser := orchestrationtest.OpenPooledBrowser(t, ctx, served, tenant, orchestrationtest.PooledBrowserOptions{})

	const (
		first  = sessionwire.SessionID("session-workspace-first")
		second = sessionwire.SessionID("session-workspace-second")
	)
	rpc := func(method string, request any) {
		t.Helper()
		if _, err := browser.RPC(t, ctx, method, request); err != nil {
			t.Fatalf("the browser's %s was refused: %v", method, err)
		}
	}
	create := func(s sessionwire.SessionID) {
		t.Helper()
		command := sessionwire.CommandID("create-" + string(s))
		rpc("session.create", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)),
			SessionID:       s,
			AgentID:         orchestrationtest.PooledAgent,
		})
		orchestrationtest.PooledWait(t, "the create of "+string(s)+" applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, command) == sessionstore.InboxStateApplied
		})
	}
	// write asks the agent, in session s, to WriteFile path with content: the
	// scripted model calls the standard tool, then answers once it sees the
	// tool's result.
	write := func(s sessionwire.SessionID, command sessionwire.CommandID, path, content string) {
		t.Helper()
		arguments, err := json.Marshal(map[string]string{"path": path, "content": content})
		if err != nil {
			t.Fatal(err)
		}
		world.LLM.Script(
			orchestrationtest.PooledTurn{ToolName: "WriteFile", ToolInput: string(arguments)},
			orchestrationtest.PooledTurn{Text: "done with " + path},
		)
		blocks, err := json.Marshal([]map[string]string{{"type": "text", "text": "write " + path}})
		if err != nil {
			t.Fatal(err)
		}
		rpc("session.input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)),
			SessionID:       s,
			Blocks:          blocks,
		})
	}
	// awaitGate waits for the one open, answerable gate of session s.
	awaitGate := func(s sessionwire.SessionID) sessionwire.GateProjection {
		t.Helper()
		var projected sessionwire.GateProjection
		orchestrationtest.PooledWait(t, "a resident gate on "+string(s), 90*time.Second, func() bool {
			page := served.OpenGates(t, ctx, tenant, s)
			if len(page.Gates) != 1 || page.Gates[0].Answerability != sessionwire.GateAnswerabilityResident {
				return false
			}
			projected = page.Gates[0]
			return true
		})
		return projected
	}
	// answer sends the browser's answer through ClientLink's gate.respond and
	// waits for Host to settle it -- a gate_response applied through the
	// disposition path.
	answer := func(s sessionwire.SessionID, projected sessionwire.GateProjection, command sessionwire.CommandID, action gate.ApprovalAction) {
		t.Helper()
		rpc("gate.respond", sessionwire.GateResponseRequest{
			CommandEnvelope:        orchestrationtest.PooledEnvelope(string(command)),
			SessionID:              s,
			GateID:                 projected.GateID,
			Action:                 string(action),
			Values:                 map[string]json.RawMessage{},
			ExpectedOpenJournalSeq: projected.OpenedJournalSeq,
		})
		orchestrationtest.PooledWait(t, "the gate response "+string(command)+" applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, command) == sessionstore.InboxStateApplied
		})
	}
	settled := func(s sessionwire.SessionID, command sessionwire.CommandID) {
		t.Helper()
		orchestrationtest.PooledWait(t, "the input "+string(command)+" applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, command) == sessionstore.InboxStateApplied
		})
	}
	// turnDone waits for session s's next turn to finish in its journal (one
	// input is one turn), so the tool's effect or refusal is final before it
	// is read.
	turns := map[sessionwire.SessionID]int{}
	turnDone := func(s sessionwire.SessionID) uuid.UUID {
		t.Helper()
		turns[s]++
		runtime := world.RuntimeSessionID(t, ctx, tenant, s)
		orchestrationtest.PooledWait(t, "the turn finished on "+string(s), 60*time.Second, func() bool {
			return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtime) >= turns[s]
		})
		return runtime
	}
	fileHolds := func(name string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, name)) // #nosec G304 -- the lane's own temp root
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		return string(data)
	}
	gatesOpened := func(runtime uuid.UUID) int {
		t.Helper()
		return len(orchestrationtest.JournalEvents[event.GateOpened](t, world, tenant, runtime))
	}

	create(first)

	t.Run("a gated write reaches the browser, is approved through gate.respond, and lands", func(t *testing.T) {
		write(first, "wa-once", "once.txt", "approved once")
		projected := awaitGate(first)
		t.Logf("the write gate reached Factory: kind=%q title=%q", projected.Kind, projected.Prompt.Title)
		if _, err := os.Stat(filepath.Join(root, "once.txt")); !os.IsNotExist(err) {
			t.Fatalf("once.txt exists before its gate was answered (%v); the write was not gated", err)
		}
		answer(first, projected, "wa-once-approve", gate.ApprovalApprove)
		settled(first, "wa-once")
		turnDone(first)
		if got := fileHolds("once.txt"); got != "approved once" {
			t.Fatalf("once.txt holds %q after the approval", got)
		}
	})

	t.Run("approve always skips the next prompt for that file in the same session", func(t *testing.T) {
		write(first, "wa-always", "always.txt", "first version")
		projected := awaitGate(first)
		answer(first, projected, "wa-always-approve", gate.ApprovalApproveAlwaysWorkspace)
		settled(first, "wa-always")
		runtime := turnDone(first)
		if got := fileHolds("always.txt"); got != "first version" {
			t.Fatalf("always.txt holds %q after the approve-always", got)
		}
		opened := gatesOpened(runtime)

		write(first, "wa-again", "always.txt", "second version")
		settled(first, "wa-again")
		turnDone(first)
		if got := fileHolds("always.txt"); got != "second version" {
			t.Fatalf("always.txt holds %q; the remembered approval did not let the second write land", got)
		}
		if again := gatesOpened(runtime); again != opened {
			t.Fatalf("the session opened %d gates after the approve-always, want %d: the remembered rule did not skip the prompt", again, opened)
		}
		if page := served.OpenGates(t, ctx, tenant, first); len(page.Gates) != 0 {
			t.Fatalf("the session still shows open gates %+v", page.Gates)
		}
	})

	t.Run("another session writing the same file is asked again", func(t *testing.T) {
		create(second)
		write(second, "wa-other", "always.txt", "other session")
		projected := awaitGate(second)
		if !strings.Contains(projected.Prompt.Body+projected.Prompt.Title, "always.txt") {
			t.Logf("the gate's prompt does not name the file (title %q, body %q)", projected.Prompt.Title, projected.Prompt.Body)
		}
		answer(second, projected, "wa-other-deny", gate.ApprovalDeny)
		settled(second, "wa-other")
		turnDone(second)
		if got := fileHolds("always.txt"); got != "second version" {
			t.Fatalf("always.txt holds %q; a denied write in another session landed", got)
		}
	})
}
