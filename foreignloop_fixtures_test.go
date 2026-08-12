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
	"github.com/looprig/foreignloops/backend"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/foreignloops/driver/claude"
	"github.com/looprig/foreignloops/driver/codex"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/harness/pkg/sessionstore"
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
	callsPath    string
	startedPath  string
	releasePath  string
	cwd          string
	lateBoundSID string
}

type foreignloopProcessControl uint8

const (
	foreignloopProcessImmediate foreignloopProcessControl = iota
	foreignloopProcessBlock
	foreignloopProcessFailAfterRelease
)

func newForeignloopProcess(t *testing.T, provider foreignloopProvider, text, boundSID string) foreignloopProcess {
	t.Helper()
	return newControlledForeignloopProcess(t, provider, text, boundSID, foreignloopProcessImmediate)
}

func newControlledForeignloopProcess(t *testing.T, provider foreignloopProvider, text, boundSID string, control foreignloopProcessControl) foreignloopProcess {
	t.Helper()
	root := t.TempDir()
	argsPath := filepath.Join(root, "argv")
	callsPath := filepath.Join(root, "calls")
	startedPath := filepath.Join(root, "started")
	releasePath := filepath.Join(root, "release")
	execPath := filepath.Join(root, "fake-provider")
	if err := os.WriteFile(execPath, []byte(foreignloopExecutable(provider, text, boundSID, control)), 0o700); err != nil {
		t.Fatalf("write fake provider: %v", err)
	}
	parentEnv := []string{
		"LOOPRIG_FAKE_ARGS=" + argsPath,
		"LOOPRIG_FAKE_CALLS=" + callsPath,
		"LOOPRIG_FAKE_STARTED=" + startedPath,
		"LOOPRIG_FAKE_RELEASE=" + releasePath,
	}
	envAllow := []string{"LOOPRIG_FAKE_ARGS", "LOOPRIG_FAKE_CALLS", "LOOPRIG_FAKE_STARTED", "LOOPRIG_FAKE_RELEASE"}
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
			EnvAllow: envAllow,
		})
	case foreignloopCodex:
		agent, err = codex.NewAgent(parentEnv, codex.Config{
			ExecPath:         execPath,
			Model:            "fixture-model",
			Sandbox:          codex.SandboxReadOnly,
			Approval:         codex.ApprovalNever,
			EnvAllow:         envAllow,
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
	return foreignloopProcess{
		provider: provider, agent: agent, argsPath: argsPath, callsPath: callsPath,
		startedPath: startedPath, releasePath: releasePath, cwd: t.TempDir(), lateBoundSID: boundSID,
	}
}

func foreignloopExecutable(provider foreignloopProvider, text, boundSID string, control foreignloopProcessControl) string {
	lines := []string{
		"#!/bin/sh", "set -eu", `: > "$LOOPRIG_FAKE_ARGS"`,
		`for arg in "$@"; do printf '%s\n' "$arg" >> "$LOOPRIG_FAKE_ARGS"; done`,
		`printf '1\n' >> "$LOOPRIG_FAKE_CALLS"`,
	}
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
	outputLimit := len(output)
	if control != foreignloopProcessImmediate {
		outputLimit = 1
	}
	for _, value := range output[:outputLimit] {
		encoded, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}
		lines = append(lines, "printf '%s\\n' "+shellSingleQuote(string(encoded)))
	}
	if control != foreignloopProcessImmediate {
		// The published driver interrupts the whole provider process group. Keep
		// this fixture cooperative at that boundary so cancellation tests exercise
		// the driver's process-group contract rather than a shell that ignores
		// SIGINT while waiting for its polling child.
		lines = append(lines,
			`trap 'exit 130' INT TERM`,
			`: > "$LOOPRIG_FAKE_STARTED"`,
			`while [ ! -e "$LOOPRIG_FAKE_RELEASE" ]; do sleep 0.01; done`,
		)
		if control == foreignloopProcessFailAfterRelease {
			lines = append(lines, "exit 7")
		} else {
			for _, value := range output[outputLimit:] {
				encoded, err := json.Marshal(value)
				if err != nil {
					panic(err)
				}
				lines = append(lines, "printf '%s\\n' "+shellSingleQuote(string(encoded)))
			}
		}
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

func (p foreignloopProcess) waitStarted(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(p.startedPath); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat fake provider start marker: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for fake provider start: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (p foreignloopProcess) release() error {
	if err := os.WriteFile(p.releasePath, []byte("release\n"), 0o600); err != nil {
		return fmt.Errorf("release fake provider: %w", err)
	}
	return nil
}

func (p foreignloopProcess) callCount() (int, error) {
	data, err := os.ReadFile(p.callsPath)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read fake provider calls: %w", err)
	}
	return len(strings.Fields(string(data))), nil
}

func (p foreignloopProcess) assertCallCount(t *testing.T, want int) {
	t.Helper()
	got, err := p.callCount()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("fake provider calls = %d, want %d", got, want)
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
	return foreignloopDefinitionWithStyle(t, name, engine, llm, loop.DelegationSyncOnly, delegates...)
}

func foreignloopManagedDefinition(t *testing.T, name string, engine loop.Engine, llm inference.Client, delegates ...string) loop.Definition {
	t.Helper()
	return foreignloopDefinitionWithStyle(t, name, engine, llm, loop.DelegationManaged, delegates...)
}

func foreignloopDefinitionWithStyle(t *testing.T, name string, engine loop.Engine, llm inference.Client, style loop.DelegationStyle, delegates ...string) loop.Definition {
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
			// A permissive headless allow-all access gate, the new-model equivalent
			// of the old auto-approve permission gate (shared with the MCP suite via
			// approveAll). Delegating loops need it wired so subagent tool calls are
			// authorized rather than fail-secure denied.
			loop.WithAccessGate(approveAll(t)),
			loop.WithPolicyRevision("foreignloop-integration-v1"),
			loop.WithDelegation(loop.Delegation{Style: style}),
		)
	}
	definition, err := loop.Define(opts...)
	if err != nil {
		t.Fatalf("loop.Define(%q): %v", name, err)
	}
	return definition
}

func newForeignloopSession(t *testing.T, ctx context.Context, process foreignloopProcess, primary string, definitions ...loop.Definition) (session.SessionController, *sessionstore.Store) {
	t.Helper()
	return newForeignloopSessionWithOptions(t, ctx, process, primary, definitions, nil)
}

func newForeignloopSessionWithOptions(t *testing.T, ctx context.Context, process foreignloopProcess, primary string, definitions []loop.Definition, extra []rig.Option) (session.SessionController, *sessionstore.Store) {
	t.Helper()
	store, err := sessionstore.Open(memstore.New())
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}
	cfg := process.config()
	options := []rig.Option{
		rig.WithLoops(definitions...),
		rig.WithPrimers(primary),
		rig.WithActivePrimer(primary),
		rig.WithSessionStore(store),
		rig.WithForeignBuilders(backend.BuildWith(cfg), backend.BuildRestoredWith(cfg)),
	}
	options = append(options, extra...)
	r, err := rig.Define(options...)
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

// waitForeignloopTurnTerminal replays the durable journal until a child turn's
// terminal event is committed. Parent turns may finish before an asynchronously
// supervised foreign child reports provider failure, so a single immediate
// replay would race the journal rather than test the failure contract.
func waitForeignloopTurnTerminal(t *testing.T, ctx context.Context, store *sessionstore.Store, sessionID, loopID uuid.UUID) []event.Event {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("wait for loop %s terminal event: %v", loopID, ctx.Err())
		default:
		}
		events := eventsFor(t, ctx, store, sessionID)
		turnEvents := foreignloopTurnEvents(events, loopID)
		if len(turnEvents) > 0 {
			switch turnEvents[len(turnEvents)-1].(type) {
			case event.TurnDone, event.TurnFailed, event.TurnInterrupted:
				return events
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for loop %s terminal event: %v", loopID, ctx.Err())
		case <-ticker.C:
		}
	}
}

// interruptForeignloopChild uses the public loop controller because the tagged
// StopAgent tool waits for LoopIdle, while foreignloops v0.2.4 publishes only
// the TurnInterrupted terminal for an interrupted turn.
func interruptForeignloopChild(ctx context.Context, sess session.SessionController, agentID string) error {
	childID, err := uuid.Parse(agentID)
	if err != nil {
		return fmt.Errorf("parse foreignloop agent id %q: %w", agentID, err)
	}
	controller, ok := sess.LoopController(childID)
	if !ok {
		return fmt.Errorf("foreignloop child %s not registered", agentID)
	}
	return controller.Interrupt(ctx)
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

type foreignloopScenarioStep func(context.Context, inference.Request) ([]content.Chunk, error)

type foreignloopScenarioLLM struct {
	mu    sync.Mutex
	steps []foreignloopScenarioStep
	next  int
}

func newForeignloopScenarioLLM(steps ...foreignloopScenarioStep) *foreignloopScenarioLLM {
	return &foreignloopScenarioLLM{steps: steps}
}

func (*foreignloopScenarioLLM) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("foreignloopScenarioLLM: Invoke is not used")
}

func (s *foreignloopScenarioLLM) Stream(ctx context.Context, request inference.Request) (*stream.StreamReader[content.Chunk], error) {
	s.mu.Lock()
	index := s.next
	s.next++
	s.mu.Unlock()
	if index >= len(s.steps) {
		return nil, fmt.Errorf("foreignloop scenario requested unexpected model step %d", index)
	}
	chunks, err := s.steps[index](ctx, request)
	if err != nil {
		return nil, fmt.Errorf("foreignloop scenario step %d: %w", index, err)
	}
	chunkIndex := 0
	return stream.NewStreamReader(func() (content.Chunk, error) {
		if chunkIndex == len(chunks) {
			return nil, io.EOF
		}
		chunk := chunks[chunkIndex]
		chunkIndex++
		return chunk, nil
	}, nil), nil
}

func (s *foreignloopScenarioLLM) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next
}

func foreignloopToolCall(id, input string) []content.Chunk {
	name, wire := foreignloopAgentToolCall(input)
	return []content.Chunk{foreignloopToolUse(id, name, wire)}
}

func foreignloopAgentToolCall(input string) (string, string) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(input), &raw); err != nil {
		return "StartAgent", input
	}
	action, _ := raw["action"].(string)
	if action == "" {
		switch {
		case raw["agent_id"] != nil && raw["message"] != nil:
			return "MessageAgent", input
		case raw["agent_id"] != nil:
			return "StopAgent", input
		default:
			return "StartAgent", input
		}
	}
	switch action {
	case "start":
		wire := map[string]any{
			"agent_type":        raw["agent"],
			"instructions":      raw["message"],
			"wait_for_response": raw["wait"],
		}
		return "StartAgent", marshalForeignloopToolInput(wire)
	case "send":
		wire := map[string]any{
			"agent_id":          raw["delegate_id"],
			"message":           raw["message"],
			"wait_for_response": raw["wait"],
		}
		return "MessageAgent", marshalForeignloopToolInput(wire)
	case "interrupt":
		return "StopAgent", marshalForeignloopToolInput(map[string]any{"agent_id": raw["delegate_id"]})
	case "status":
		return "ListAgents", marshalForeignloopToolInput(map[string]any{"agent_id": raw["delegate_id"]})
	default:
		return "StartAgent", input
	}
}

func marshalForeignloopToolInput(input map[string]any) string {
	encoded, err := json.Marshal(input)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func foreignloopFinal(text string) []content.Chunk {
	return []content.Chunk{&content.TextChunk{Text: text}}
}

func foreignloopLastToolResult(request inference.Request) (string, error) {
	for index := len(request.Messages) - 1; index >= 0; index-- {
		message, ok := request.Messages[index].(*content.ToolResultMessage)
		if !ok {
			continue
		}
		var result strings.Builder
		for _, block := range message.Blocks {
			text, ok := block.(*content.TextBlock)
			if ok {
				result.WriteString(text.Text)
			}
		}
		return result.String(), nil
	}
	return "", errors.New("model request has no tool result")
}

type foreignloopAgentToolResult struct {
	AgentID        string `json:"agent_id"`
	Name           string `json:"name"`
	State          string `json:"state"`
	PreviousState  string `json:"previous_state"`
	DeliveryStatus string `json:"delivery_status"`
	ResponseStatus string `json:"response_status"`
	Response       string `json:"response"`
}

func foreignloopLastAgentToolResult(request inference.Request) (foreignloopAgentToolResult, error) {
	text, err := foreignloopLastToolResult(request)
	if err != nil {
		return foreignloopAgentToolResult{}, err
	}
	var result foreignloopAgentToolResult
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		return foreignloopAgentToolResult{}, fmt.Errorf("decode agent tool result %q: %w", text, err)
	}
	if result.AgentID == "" {
		return foreignloopAgentToolResult{}, fmt.Errorf("agent tool result = %+v, want agent id", result)
	}
	return result, nil
}

func foreignloopExpectLastToolResult(request inference.Request, want string) error {
	got, err := foreignloopLastToolResult(request)
	if err != nil {
		return err
	}
	if got != want && foreignloopForegroundResponse(got) != want {
		return fmt.Errorf("tool result = %q, want %q", got, want)
	}
	return nil
}

func foreignloopExpectRawToolResult(request inference.Request, want string) error {
	got, err := foreignloopLastToolResult(request)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("raw tool result = %q, want %q", got, want)
	}
	return nil
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
					text.WriteString(foreignloopForegroundResponse(typed.Text))
				}
			}
			result = append(result, text.String())
		}
	}
	return result
}

func foreignloopForegroundResponse(text string) string {
	var result struct {
		Response *string `json:"response"`
	}
	if err := json.Unmarshal([]byte(text), &result); err == nil && result.Response != nil {
		return *result.Response
	}
	return text
}

func foreignloopToolUse(id, name, input string) content.Chunk {
	return &content.ToolUseChunk{Index: 0, ID: id, Name: name, InputJSON: input}
}

var _ inference.Client = (*foreignloopScriptLLM)(nil)
var _ inference.Client = (*foreignloopScenarioLLM)(nil)
