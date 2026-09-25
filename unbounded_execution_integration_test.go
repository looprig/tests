//go:build integration

package tests

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/hustle"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

func TestUnboundedOpaqueToolInputSurvivesRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	tenant := orchestrationtest.PooledTenantA
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{tenant}, WithWorkspace: true,
	})
	const duplicateInput = `{"path":"a.txt","path":"b.txt","content":"x"}`
	world.LLM.Script(
		orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledWriteToolName, ToolInput: duplicateInput},
		orchestrationtest.PooledTurn{Text: "wrote"},
	)
	first := orchestrationtest.StartPooledHost(t, ctx, world, "i-opaque-first", 1)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "i-opaque-factory", nil)
	const s = sessionwire.SessionID("i-opaque-tool-input")
	status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("opaque-create"), SessionID: s,
		AgentID: orchestrationtest.PooledAgent, Blocks: principalLaneBlocks(t, "write duplicate arguments"),
	})
	if status != http.StatusCreated {
		t.Fatalf("create answered %d: %s", status, body)
	}
	runtime := world.RuntimeSessionID(t, ctx, tenant, s)
	orchestrationtest.PooledWait(t, "duplicate-key tool call finished", 90*time.Second, func() bool {
		return len(world.WorkspaceTools.Writes()) == 1 &&
			orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtime) >= 1
	})
	if write := world.WorkspaceTools.Writes()[0]; write.Path != "b.txt" || write.Content != "x" {
		t.Fatalf("tool parsed duplicate keys as %+v, want last path b.txt", write)
	}
	assertInput := func(when string) {
		t.Helper()
		var found int
		for _, step := range orchestrationtest.JournalEvents[event.StepDone](t, world, tenant, runtime) {
			for _, message := range step.Messages {
				assistant, ok := message.(*content.AIMessage)
				if !ok {
					continue
				}
				for _, block := range assistant.Blocks {
					use, ok := block.(*content.ToolUseBlock)
					if !ok || use.Name != orchestrationtest.PooledWriteToolName {
						continue
					}
					found++
					if !bytes.Equal(use.Input, []byte(duplicateInput)) {
						t.Fatalf("%s tool_use.input = %s, want %s", when, use.Input, duplicateInput)
					}
				}
			}
		}
		if found != 1 {
			t.Fatalf("%s found %d original tool_use blocks, want 1", when, found)
		}
	}
	assertInput("before restore")
	first.Stop()
	orchestrationtest.StartPooledHost(t, ctx, world, "i-opaque-second", 1)
	status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("opaque-next"), SessionID: s,
		Blocks: principalLaneBlocks(t, "continue after restore"),
	})
	if status != http.StatusOK {
		t.Fatalf("input after opaque-tool restore answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "successor replayed duplicate-key journal", 90*time.Second, func() bool {
		return world.CommandState(ctx, tenant, s, "opaque-next") == sessionstore.InboxStateApplied &&
			orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtime) >= 2
	})
	assertInput("after restore")
	// harness v0.40.2 rejected this StepDone on restore. The v0.41.0
	// duplicate-key check treats only tool_use.input as opaque; surrounding
	// event structure is still strictly checked by harness's own unit tests.
}

func TestUnboundedToolLoopPassesTheFormerIterationCap(t *testing.T) {
	for _, row := range []struct {
		name   string
		id     string
		limits loop.ToolLimits
		want   int
	}{
		{"unlimited", "unlimited", loop.ToolLimits{Iterations: loop.Unlimited, Calls: loop.Unlimited}, 30},
		{"default control", "default", loop.ToolLimits{}, 25},
	} {
		t.Run(row.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			t.Cleanup(cancel)
			tenant := orchestrationtest.PooledTenantA
			world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
				Tenants: []sessionwire.TenantID{tenant}, WithWorkspace: true, ToolLimits: row.limits,
			})
			turns := make([]orchestrationtest.PooledTurn, 0, 31)
			for range 30 {
				turns = append(turns, orchestrationtest.PooledTurn{
					ToolName: orchestrationtest.PooledReadToolName, ToolInput: `{"path":"a.txt"}`,
				})
			}
			turns = append(turns, orchestrationtest.PooledTurn{Text: "finished"})
			world.LLM.Script(turns...)
			orchestrationtest.StartPooledHost(t, ctx, world, sessionwire.HostID("i-unbounded-"+row.id), 1)
			served := orchestrationtest.StartPooledFactory(t, ctx, world, "i-unbounded-"+row.id, nil)
			s := sessionwire.SessionID("i-unbounded-" + row.id)
			status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope("unbounded-create"), SessionID: s,
				AgentID: orchestrationtest.PooledAgent, Blocks: principalLaneBlocks(t, "read a lot"),
			})
			if status != http.StatusCreated {
				t.Fatalf("create answered %d: %s", status, body)
			}
			runtime := world.RuntimeSessionID(t, ctx, tenant, s)
			orchestrationtest.PooledWait(t, "tool turn terminal", 90*time.Second, func() bool {
				return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtime)+
					orchestrationtest.CountJournalEvents[event.TurnFailed](t, world, tenant, runtime) >= 1
			})
			reads := world.WorkspaceTools.Reads()
			if len(reads) != row.want {
				t.Fatalf("tool ran %d reads, want %d", len(reads), row.want)
			}
			failed := orchestrationtest.JournalEvents[event.TurnFailed](t, world, tenant, runtime)
			if row.name == "unlimited" {
				if len(failed) != 0 || orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtime) != 1 {
					t.Fatalf("unlimited turn failed %+v or did not finish", failed)
				}
			} else if len(failed) != 1 || event.ErrKind(failed[0].Err) != event.KindToolLimit {
				t.Fatalf("default control failed %+v, want tool_limit", failed)
			}
		})
	}
}

func TestUnboundedZeroTimeoutHustleRestores(t *testing.T) {
	definition, err := hustle.Define(
		hustle.WithName("lane.idle"),
		hustle.WithParticipation(hustle.ParticipationBackground),
		hustle.WithCurrentLoopModel(),
		hustle.WithTimeout(0),
		hustle.WithLimits(hustle.Limits{InputBytes: 1024, OutputBytes: 512}),
		hustle.WithSystemPrompt("x", "lane-v1"),
		hustle.WithPolicyRevision("lane-v1"),
	)
	if err != nil {
		t.Fatalf("zero-timeout hustle.Define: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	tenant := orchestrationtest.PooledTenantA
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{tenant}, WithWorkspace: true, Hustles: []hustle.Definition{definition},
	})
	first := orchestrationtest.StartPooledHost(t, ctx, world, "i-zero-hustle-first", 1)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "i-zero-hustle-factory", nil)
	const s = sessionwire.SessionID("i-zero-hustle")
	status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("zero-create"), SessionID: s,
		AgentID: orchestrationtest.PooledAgent,
	})
	if status != http.StatusCreated {
		t.Fatalf("bare create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "zero-timeout session created", 90*time.Second, func() bool {
		return world.CommandState(ctx, tenant, s, "zero-create") == sessionstore.InboxStateApplied
	})
	first.Stop()
	orchestrationtest.StartPooledHost(t, ctx, world, "i-zero-hustle-second", 1)
	status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("zero-input"), SessionID: s,
		Blocks: principalLaneBlocks(t, "after restore"),
	})
	if status != http.StatusOK {
		t.Fatalf("input after zero-timeout restore answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "zero-timeout hustle descriptor replayed and input applied", 90*time.Second, func() bool {
		return world.CommandState(ctx, tenant, s, "zero-input") == sessionstore.InboxStateApplied &&
			orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, world.RuntimeSessionID(t, ctx, tenant, s)) >= 1
	})
	// Execution without a deadline is covered by harness's own unit tests;
	// this released-module lane proves durable descriptor replay accepts zero.
}
