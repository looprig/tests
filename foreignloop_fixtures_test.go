//go:build integration && (darwin || (linux && !android))

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloop/backend"
	"github.com/looprig/foreignloop/driver"
	"github.com/looprig/foreignloop/driver/claude"
	"github.com/looprig/foreignloop/driver/codex"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/inference"
	"github.com/looprig/inference/stream"
	"github.com/looprig/storage/memstore"
)

type foreignloopProvider uint8

const (
	foreignloopClaude foreignloopProvider = iota
	foreignloopCodex
)

type foreignloopProcess struct {
	provider     foreignloopProvider
	agent        driver.Agent
	argsPath     string
	cwd          string
	lateBoundSID string
}

func newForeignloopProcess(t *testing.T, provider foreignloopProvider, text, boundSID string) foreignloopProcess {
	t.Helper()
	root := t.TempDir()
	argsPath := filepath.Join(root, "argv")
	execPath := filepath.Join(root, "fake-provider")
	if err := os.WriteFile(execPath, []byte(foreignloopExecutable(provider, text, boundSID)), 0o700); err != nil {
		t.Fatalf("write fake provider: %v", err)
	}
	parentEnv := []string{"LOOPRIG_FAKE_ARGS=" + argsPath}
	var (
		agent driver.Agent
		err   error
	)
	switch provider {
	case foreignloopClaude:
		agent, err = claude.NewAgent(parentEnv, claude.Config{
			ExecPath: execPath,
			Home:     filepath.Join(root, "home"),
			Model:    "fixture-model",
			EnvAllow: []string{"LOOPRIG_FAKE_ARGS"},
		})
	case foreignloopCodex:
		agent, err = codex.NewAgent(parentEnv, codex.Config{
			ExecPath:         execPath,
			Model:            "fixture-model",
			Sandbox:          codex.SandboxReadOnly,
			Approval:         codex.ApprovalNever,
			EnvAllow:         []string{"LOOPRIG_FAKE_ARGS"},
			IgnoreUserConfig: true,
			IgnoreRules:      true,
			SkipGitRepoCheck: true,
		})
	default:
		t.Fatalf("unknown foreignloop provider %d", provider)
	}
	if err != nil {
		t.Fatalf("construct fake provider agent: %v", err)
	}
	return foreignloopProcess{provider: provider, agent: agent, argsPath: argsPath, cwd: t.TempDir(), lateBoundSID: boundSID}
}

func foreignloopExecutable(provider foreignloopProvider, text, boundSID string) string {
	lines := []string{"#!/bin/sh", "set -eu", `: > "$LOOPRIG_FAKE_ARGS"`, `for arg in "$@"; do printf '%s\n' "$arg" >> "$LOOPRIG_FAKE_ARGS"; done`}
	var output []any
	switch provider {
	case foreignloopClaude:
		output = []any{
			map[string]any{"type": "system", "subtype": "init"},
			map[string]any{"type": "stream_event", "event": map[string]any{"type": "content_block_delta", "delta": map[string]any{"type": "text_delta", "text": text}}},
			map[string]any{"type": "assistant", "message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": text}}}},
			map[string]any{"type": "result", "subtype": "success"},
		}
	case foreignloopCodex:
		output = []any{
			map[string]any{"type": "thread.started", "thread_id": boundSID},
			map[string]any{"type": "turn.started"},
			map[string]any{"type": "item.completed", "item": map[string]any{"type": "agent_message", "text": text}},
			map[string]any{"type": "turn.completed"},
		}
	}
	for _, value := range output {
		encoded, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}
		lines = append(lines, "printf '%s\\n' "+shellSingleQuote(string(encoded)))
	}
	return strings.Join(lines, "\n") + "\n"
}

func shellSingleQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func (p foreignloopProcess) config() backend.Config {
	mode := backend.SIDPrebound
	if p.provider == foreignloopCodex {
		mode = backend.SIDLateBound
	}
	return backend.Config{
		Agent:   p.agent,
		Cwd:     p.cwd,
		Posture: driver.PostureDefault,
		SIDMode: mode,
	}
}

func (p foreignloopProcess) assertStartNew(t *testing.T, foreignSID string) {
	t.Helper()
	data, err := os.ReadFile(p.argsPath)
	if err != nil {
		t.Fatalf("read fake provider argv: %v", err)
	}
	args := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	switch p.provider {
	case foreignloopClaude:
		index := indexForeignloopArg(args, "--session-id")
		if index < 0 || index+1 == len(args) {
			t.Fatalf("Claude argv = %q, want --session-id <prebound sid>", args)
		}
		if args[index+1] != foreignSID {
			t.Fatalf("Claude --session-id = %q, want LoopStarted sid %q", args[index+1], foreignSID)
		}
		if indexForeignloopArg(args, "--resume") >= 0 {
			t.Fatalf("Claude first-turn argv unexpectedly resumes: %q", args)
		}
	case foreignloopCodex:
		if foreignSID != "" {
			t.Fatalf("Codex first turn received prebound sid %q, want empty before thread.started", foreignSID)
		}
		if len(args) < 2 || args[0] != "exec" || args[1] != "--json" {
			t.Fatalf("Codex first-turn argv = %q, want start form beginning exec --json", args)
		}
		if indexForeignloopArg(args, "resume") >= 0 {
			t.Fatalf("Codex first-turn argv unexpectedly resumes: %q", args)
		}
		if p.lateBoundSID != "" && indexForeignloopArg(args, p.lateBoundSID) >= 0 {
			t.Fatalf("Codex start argv contains late-bound sid %q: %q", p.lateBoundSID, args)
		}
	}
}

func indexForeignloopArg(args []string, want string) int {
	for index, arg := range args {
		if arg == want {
			return index
		}
	}
	return -1
}

func foreignloopDefinition(t *testing.T, name string, engine loop.Engine, llm inference.Client, delegates ...string) loop.Definition {
	t.Helper()
	opts := []loop.Option{
		loop.WithName(identity.AgentName(name)),
		loop.WithInference(llm, testModel(name)),
		loop.WithSystem("foreignloop integration system"),
		loop.WithEngine(engine),
		loop.WithDrainTimeout(time.Second),
	}
	if len(delegates) > 0 {
		names := make([]identity.AgentName, len(delegates))
		for index, delegate := range delegates {
			names[index] = identity.AgentName(delegate)
		}
		opts = append(opts,
			loop.WithDelegates(names...),
			loop.WithPermissionFactory(func(context.Context, tool.Bindings) (loop.PermissionGate, error) {
				return foreignloopAllowAll{}, nil
			}),
			loop.WithPolicyRevision("foreignloop-integration-v1"),
		)
	}
	definition, err := loop.Define(opts...)
	if err != nil {
		t.Fatalf("loop.Define(%q): %v", name, err)
	}
	return definition
}

type foreignloopAllowAll struct{}

func (foreignloopAllowAll) Check(context.Context, tool.InvokableTool, string, string) loop.Effect {
	return loop.EffectAutoApprove
}

func (foreignloopAllowAll) Grant(context.Context, string, string, tool.ApprovalScope) error {
	return nil
}

func newForeignloopSession(t *testing.T, ctx context.Context, process foreignloopProcess, primary string, definitions ...loop.Definition) (session.SessionController, *sessionstore.Store) {
	t.Helper()
	store, err := sessionstore.Open(memstore.New())
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}
	cfg := process.config()
	r, err := rig.Define(
		rig.WithLoops(definitions...),
		rig.WithPrimers(primary),
		rig.WithActivePrimer(primary),
		rig.WithSessionStore(store),
		rig.WithForeignBuilders(backend.BuildWith(cfg), backend.BuildRestoredWith(cfg)),
	)
	if err != nil {
		t.Fatalf("rig.Define: %v", err)
	}
	sess, err := r.NewSession(ctx)
	if err != nil {
		t.Fatalf("rig.NewSession: %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := sess.Shutdown(shutdownCtx); err != nil {
			t.Errorf("session shutdown: %v", err)
		}
	})
	return sess, store
}

func subscribeForeignloopEvents(t *testing.T, sess session.SessionController) event.Subscription {
	t.Helper()
	sub, err := sess.SubscribeEvents(event.EventFilter{Enduring: event.LoopScope{All: true}})
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

func waitForeignloopTurnDone(t *testing.T, ctx context.Context, sub event.Subscription, loopID uuid.UUID) event.TurnDone {
	t.Helper()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("wait for loop %s TurnDone: %v", loopID, ctx.Err())
		case delivery, ok := <-sub.Events():
			if !ok {
				t.Fatalf("subscription closed waiting for loop %s TurnDone: %v", loopID, sub.Err())
			}
			switch terminal := delivery.Event.(type) {
			case event.TurnDone:
				if terminal.LoopID == loopID {
					return terminal
				}
			case event.TurnFailed:
				if terminal.LoopID == loopID {
					t.Fatalf("loop %s failed: %v", loopID, terminal.Err)
				}
			case event.TurnInterrupted:
				if terminal.LoopID == loopID {
					t.Fatalf("loop %s was interrupted", loopID)
				}
			}
		}
	}
}

func rootForeignloopStarted(t *testing.T, events []event.Event) event.LoopStarted {
	t.Helper()
	for _, value := range events {
		if started, ok := value.(event.LoopStarted); ok && started.Cause.LoopID.IsZero() {
			return started
		}
	}
	t.Fatal("no root LoopStarted event")
	return event.LoopStarted{}
}

func childForeignloopStarted(t *testing.T, events []event.Event, parent uuid.UUID) event.LoopStarted {
	t.Helper()
	for _, value := range events {
		if started, ok := value.(event.LoopStarted); ok && started.Cause.LoopID == parent {
			return started
		}
	}
	t.Fatalf("no child LoopStarted event caused by %s", parent)
	return event.LoopStarted{}
}

func foreignloopTurnEvents(events []event.Event, loopID uuid.UUID) []event.Event {
	var result []event.Event
	for _, value := range events {
		if value.EventHeader().LoopID != loopID {
			continue
		}
		switch value.(type) {
		case event.TurnStarted, event.ForeignSessionBound, event.StepDone, event.TurnDone, event.TurnFailed, event.TurnInterrupted:
			result = append(result, value)
		}
	}
	return result
}

func foreignloopEventKinds(events []event.Event) []string {
	result := make([]string, len(events))
	for index, value := range events {
		switch value.(type) {
		case event.TurnStarted:
			result[index] = "TurnStarted"
		case event.ForeignSessionBound:
			result[index] = "ForeignSessionBound"
		case event.StepDone:
			result[index] = "StepDone"
		case event.TurnDone:
			result[index] = "TurnDone"
		case event.TurnFailed:
			result[index] = "TurnFailed"
		case event.TurnInterrupted:
			result[index] = "TurnInterrupted"
		default:
			result[index] = fmt.Sprintf("%T", value)
		}
	}
	return result
}

func foreignloopAIText(message *content.AIMessage) string {
	if message == nil {
		return ""
	}
	var result strings.Builder
	for _, block := range message.Blocks {
		if text, ok := block.(*content.TextBlock); ok {
			result.WriteString(text.Text)
		}
	}
	return result.String()
}

type foreignloopScriptLLM struct {
	mu       sync.Mutex
	replies  [][]content.Chunk
	next     int
	requests []inference.Request
}

func newForeignloopScriptLLM(replies ...[]content.Chunk) *foreignloopScriptLLM {
	return &foreignloopScriptLLM{replies: replies}
}

func (*foreignloopScriptLLM) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("foreignloopScriptLLM: Invoke is not used")
}

func (s *foreignloopScriptLLM) Stream(_ context.Context, request inference.Request) (*stream.StreamReader[content.Chunk], error) {
	s.mu.Lock()
	s.requests = append(s.requests, request)
	var chunks []content.Chunk
	if s.next < len(s.replies) {
		chunks = s.replies[s.next]
	} else {
		chunks = []content.Chunk{&content.TextChunk{Text: "done"}}
	}
	s.next++
	s.mu.Unlock()
	index := 0
	return stream.NewStreamReader(func() (content.Chunk, error) {
		if index == len(chunks) {
			return nil, io.EOF
		}
		chunk := chunks[index]
		index++
		return chunk, nil
	}, nil), nil
}

func (s *foreignloopScriptLLM) toolResults() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []string
	for _, request := range s.requests {
		for _, message := range request.Messages {
			toolResult, ok := message.(*content.ToolResultMessage)
			if !ok {
				continue
			}
			var text strings.Builder
			for _, block := range toolResult.Blocks {
				if typed, ok := block.(*content.TextBlock); ok {
					text.WriteString(typed.Text)
				}
			}
			result = append(result, text.String())
		}
	}
	return result
}

func foreignloopToolUse(id, name, input string) content.Chunk {
	return &content.ToolUseChunk{Index: 0, ID: id, Name: name, InputJSON: input}
}

var _ inference.Client = (*foreignloopScriptLLM)(nil)
var _ loop.PermissionGate = foreignloopAllowAll{}
