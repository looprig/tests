//go:build integration

package tests

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/fsstore"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/harness/pkg/workspacestore"
	"github.com/looprig/inference"
	"github.com/looprig/storage"
)

const integrationCleanupTimeout = 5 * time.Second

type fsStores struct {
	fs        *fsstore.Store
	sessions  *sessionstore.Store
	workspace *workspacestore.Store
}

func openFSStores(t *testing.T, root string) fsStores {
	t.Helper()
	fs, err := fsstore.Open(fsstore.Options{Root: root})
	if err != nil {
		t.Fatalf("fsstore.Open: %v", err)
	}
	sessions, err := sessionstore.Open(fs.Backend())
	if err != nil {
		_ = fs.Close()
		t.Fatalf("sessionstore.Open: %v", err)
	}
	spool := filepath.Join(root, "spool")
	if err := os.MkdirAll(spool, 0o700); err != nil {
		_ = fs.Close()
		t.Fatalf("create workspace spool: %v", err)
	}
	workspace, err := workspacestore.Open(fs.Blobs, workspacestore.WithSpoolDir(spool))
	if err != nil {
		_ = fs.Close()
		t.Fatalf("workspacestore.Open: %v", err)
	}
	// Registered before any session cleanup, so testing's LIFO cleanup order always
	// shuts live sessions down before closing their backing store.
	t.Cleanup(func() { _ = fs.Close() })
	return fsStores{fs: fs, sessions: sessions, workspace: workspace}
}

// registerSessionCleanup immediately protects an acquired controller with bounded,
// exactly-once shutdown. The returned function is used by tests that intentionally hand off
// or reopen a store; the later t.Cleanup safely observes that explicit shutdown.
func registerSessionCleanup(t *testing.T, sess session.SessionController) func(context.Context) error {
	t.Helper()
	if sess == nil {
		return func(context.Context) error { return nil }
	}
	var once sync.Once
	var shutdownErr error
	shutdown := func(ctx context.Context) error {
		once.Do(func() { shutdownErr = sess.Shutdown(ctx) })
		return shutdownErr
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), integrationCleanupTimeout)
		defer cancel()
		if err := shutdown(ctx); err != nil {
			t.Errorf("session cleanup Shutdown: %v", err)
		}
	})
	return shutdown
}

func model(name string) inference.Model {
	return inference.Model{
		Provider:  inference.ProviderName("integration"),
		APIFormat: inference.APIFormatOpenAI,
		BaseURL:   "http://127.0.0.1",
		Name:      name,
	}
}

type deterministicLLM struct{}

func (deterministicLLM) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("Invoke not used")
}

func (deterministicLLM) Stream(context.Context, inference.Request) (*inference.StreamReader[content.Chunk], error) {
	return doneStream(), nil
}

func doneStream() *inference.StreamReader[content.Chunk] {
	chunks := []content.Chunk{&content.TextChunk{Text: "done"}}
	index := 0
	return inference.NewStreamReader(func() (content.Chunk, error) {
		if index == len(chunks) {
			return nil, io.EOF
		}
		chunk := chunks[index]
		index++
		return chunk, nil
	}, nil)
}

type recordingLLM struct {
	mu       sync.Mutex
	requests map[string][][]string
}

func (*recordingLLM) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("Invoke not used")
}

func (l *recordingLLM) Stream(_ context.Context, request inference.Request) (*inference.StreamReader[content.Chunk], error) {
	history := make([]string, 0, len(request.Messages))
	for _, message := range request.Messages {
		history = append(history, conversationText(message))
	}
	l.mu.Lock()
	if l.requests == nil {
		l.requests = make(map[string][][]string)
	}
	l.requests[request.Model.Name] = append(l.requests[request.Model.Name], history)
	l.mu.Unlock()
	return doneStream(), nil
}

func conversationText(message content.Conversation) string {
	var blocks []content.Block
	var role content.Role
	switch typed := message.(type) {
	case *content.UserMessage:
		blocks = typed.Blocks
		role = typed.Role
	case *content.AIMessage:
		blocks = typed.Blocks
		role = typed.Role
	case *content.SystemMessage:
		blocks = typed.Blocks
		role = typed.Role
	case *content.ToolResultMessage:
		blocks = typed.Blocks
		role = typed.Role
	}
	var result string
	for _, block := range blocks {
		if text, ok := block.(*content.TextBlock); ok {
			result += text.Text
		}
	}
	return string(role) + ":" + result
}

type blockingLLM struct {
	started chan struct{}
	once    sync.Once
}

func (*blockingLLM) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("Invoke not used")
}

func (l *blockingLLM) Stream(ctx context.Context, _ inference.Request) (*inference.StreamReader[content.Chunk], error) {
	l.once.Do(func() { close(l.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func definition(t *testing.T, name string, client inference.Client) loop.Definition {
	t.Helper()
	d, err := loop.Define(
		loop.WithName(identity.AgentName(name)),
		loop.WithInference(client, model(name)),
	)
	if err != nil {
		t.Fatalf("loop.Define(%q): %v", name, err)
	}
	return d
}

func defineSessionRig(t *testing.T, stores fsStores, base string, allowMismatch bool, policy rig.SnapshotPolicy) *rig.Rig {
	return defineSessionRigWithClient(t, stores, base, allowMismatch, policy, deterministicLLM{})
}

func defineSessionRigWithClient(t *testing.T, stores fsStores, base string, allowMismatch bool, policy rig.SnapshotPolicy, client inference.Client) *rig.Rig {
	t.Helper()
	opts := []rig.Option{
		rig.WithLoops(definition(t, "planner", client), definition(t, "builder", client)),
		rig.WithPrimers("planner", "builder"),
		rig.WithActivePrimer("planner"),
		rig.WithSessionStore(stores.sessions),
		rig.WithSessionWorkspaces(stores.workspace, base),
		rig.WithSnapshots(policy),
	}
	if allowMismatch {
		opts = append(opts, rig.WithAllowConfigMismatch())
	}
	r, err := rig.Define(opts...)
	if err != nil {
		t.Fatalf("rig.Define: %v", err)
	}
	return r
}

func textBlock(text string) []content.Block {
	return []content.Block{&content.TextBlock{Text: text}}
}

func canonical(t *testing.T, path string) string {
	t.Helper()
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return resolved
	}
	return filepath.Clean(abs)
}

func eventsFor(t *testing.T, ctx context.Context, store *sessionstore.Store, id uuid.UUID) []event.Event {
	t.Helper()
	replayer, err := store.OpenEventReplayer(id, sessionstore.ReplayRequest{FromSeq: 0})
	if err != nil {
		t.Fatalf("OpenEventReplayer: %v", err)
	}
	cursor, err := replayer.Open(ctx, journal.ReplayRequest{From: journal.Beginning()})
	if err != nil {
		t.Fatalf("replayer.Open: %v", err)
	}
	defer cursor.Close()
	var events []event.Event
	for {
		ev, _, err := cursor.Next(ctx)
		if errors.Is(err, io.EOF) {
			return events
		}
		if err != nil {
			t.Fatalf("cursor.Next: %v", err)
		}
		events = append(events, ev)
	}
}

func primerIDs(t *testing.T, ctx context.Context, store *sessionstore.Store, id uuid.UUID) map[string]uuid.UUID {
	t.Helper()
	result := make(map[string]uuid.UUID)
	for _, ev := range eventsFor(t, ctx, store, id) {
		if started, ok := ev.(event.LoopStarted); ok && started.Cause.Coordinates.LoopID.IsZero() {
			result[string(started.AgentName)] = started.LoopID
		}
	}
	return result
}

func countEvents[T event.Event](events []event.Event) int {
	count := 0
	for _, ev := range events {
		if _, ok := ev.(T); ok {
			count++
		}
	}
	return count
}

func assertPrimerTurnDone(t *testing.T, events []event.Event, loopIDs ...uuid.UUID) {
	t.Helper()
	counts := make(map[uuid.UUID]int)
	for _, ev := range events {
		if done, ok := ev.(event.TurnDone); ok {
			counts[done.LoopID]++
		}
	}
	for _, loopID := range loopIDs {
		if counts[loopID] != 1 {
			t.Fatalf("loop %s durable TurnDone count = %d, want exactly 1", loopID, counts[loopID])
		}
	}
}

func assertCapturedHistory(t *testing.T, recorder *recordingLLM, model string, want ...string) {
	t.Helper()
	recorder.mu.Lock()
	requests := append([][]string(nil), recorder.requests[model]...)
	recorder.mu.Unlock()
	if len(requests) != 1 {
		t.Fatalf("model %q request count = %d, want 1; requests=%v", model, len(requests), requests)
	}
	if !reflect.DeepEqual(requests[0], want) {
		t.Fatalf("model %q restored history = %v, want %v", model, requests[0], want)
	}
}

type checkpointWant struct {
	Ref     workspacestore.Ref
	Trigger event.SnapshotTriggerKind
}

func assertCheckpointSequence(t *testing.T, events []event.Event, want ...checkpointWant) {
	t.Helper()
	var got []checkpointWant
	for _, ev := range events {
		if checkpoint, ok := ev.(event.WorkspaceCheckpointed); ok {
			got = append(got, checkpointWant{Ref: workspacestore.Ref(checkpoint.Ref), Trigger: checkpoint.Trigger})
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("checkpoint sequence = %+v, want %+v", got, want)
	}
}

func latestCheckpoint(t *testing.T, events []event.Event) event.WorkspaceCheckpointed {
	t.Helper()
	for i := len(events) - 1; i >= 0; i-- {
		if checkpoint, ok := events[i].(event.WorkspaceCheckpointed); ok {
			return checkpoint
		}
	}
	t.Fatal("journal has no WorkspaceCheckpointed")
	return event.WorkspaceCheckpointed{}
}

func assertCleanRestore(t *testing.T, events []event.Event) {
	t.Helper()
	var lifecycle []string
	for _, ev := range events {
		switch ev.(type) {
		case event.RestoreStarted:
			lifecycle = append(lifecycle, "RestoreStarted")
		case event.RestoreDone:
			lifecycle = append(lifecycle, "RestoreDone")
		case event.RestoreErrored:
			lifecycle = append(lifecycle, "RestoreErrored")
		}
	}
	want := []string{"RestoreStarted", "RestoreDone"}
	if !reflect.DeepEqual(lifecycle, want) {
		t.Fatalf("restore lifecycle tail = %v, want exact %v", lifecycle, want)
	}
}

func waitEvent[T event.Event](t *testing.T, ctx context.Context, sub event.Subscription) T {
	t.Helper()
	var zero T
	var seen []string
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("wait for %T: %v (seen %v)", zero, ctx.Err(), seen)
		case delivery, ok := <-sub.Events():
			if !ok {
				t.Fatalf("subscription closed waiting for %T: %v", zero, sub.Err())
			}
			if matched, ok := delivery.Event.(T); ok {
				return matched
			}
			seen = append(seen, fmt.Sprintf("%T", delivery.Event))
		}
	}
}

func waitIdle(ctx context.Context, sess any) error {
	waiter, ok := sess.(interface{ WaitIdle(context.Context) error })
	if !ok {
		return errors.New("concrete session lacks deterministic idle acknowledgement")
	}
	return waiter.WaitIdle(ctx)
}

type trackingLeaser struct {
	storage.Leaser
	mu     sync.Mutex
	leases map[string]storage.Lease
}

func (l *trackingLeaser) Acquire(ctx context.Context, name string) (storage.Lease, error) {
	lease, err := l.Leaser.Acquire(ctx, name)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	if l.leases == nil {
		l.leases = make(map[string]storage.Lease)
	}
	l.leases[name] = lease
	l.mu.Unlock()
	return lease, nil
}

func (l *trackingLeaser) loseWorkspace(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for name, lease := range l.leases {
		if len(name) >= len("workspace-roots/") && name[:len("workspace-roots/")] == "workspace-roots/" {
			return lease.Release(ctx)
		}
	}
	return errors.New("workspace lease not acquired")
}

type captureRig struct {
	inner *rig.Rig
	mu    sync.Mutex
	last  session.SessionController
}

func (r *captureRig) NewSession(ctx context.Context, opts ...rig.SessionOption) (session.SessionController, error) {
	sess, err := r.inner.NewSession(ctx, opts...)
	if err == nil {
		r.mu.Lock()
		r.last = sess
		r.mu.Unlock()
	}
	return sess, err
}

func (r *captureRig) RestoreSession(ctx context.Context, id uuid.UUID) (session.SessionController, error) {
	sess, err := r.inner.RestoreSession(ctx, id)
	if err == nil {
		r.mu.Lock()
		r.last = sess
		r.mu.Unlock()
	}
	return sess, err
}

func (r *captureRig) captured() session.SessionController {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

type treeNode struct {
	isDir   bool
	perm    os.FileMode
	content string
	target  string
}

func buildTree(t *testing.T, root, marker string) {
	t.Helper()
	write := func(rel, body string, mode os.FileMode) {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
	}
	write("readme.txt", "hello "+marker+"\n", 0o644)
	write("bin/run.sh", "#!/bin/sh\necho "+marker+"\n", 0o755)
	if err := os.Symlink("readme.txt", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
}

func snapshotTree(t *testing.T, root string) map[string]treeNode {
	t.Helper()
	result := make(map[string]treeNode)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || path == root {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			result[key] = treeNode{target: target}
		case info.IsDir():
			result[key] = treeNode{isDir: true, perm: info.Mode().Perm()}
		default:
			body, err := os.ReadFile(path) // #nosec G304 -- test-owned TempDir tree
			if err != nil {
				return err
			}
			result[key] = treeNode{perm: info.Mode().Perm(), content: string(body)}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return result
}

func assertTreesEqual(t *testing.T, want, got string) {
	t.Helper()
	if before, after := snapshotTree(t, want), snapshotTree(t, got); !reflect.DeepEqual(before, after) {
		t.Fatalf("tree mismatch:\nwant=%v\ngot=%v", before, after)
	}
}
