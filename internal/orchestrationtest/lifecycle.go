//go:build integration

package orchestrationtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/harness/pkg/workspacestore"
	"github.com/looprig/host"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// This file is the kit's POOLED LIFECYCLE surface (runbook 07 I2.1): a Host
// whose knobs a case may turn (warm TTL, drain grace, reconcile interval, a
// checkpoint seam it can hold), a Host whose PROCESS can die, a real harness
// workspace per session, and the Host's own metrics read the way an operator
// reads them.
//
// Nothing here vouches for anything. Every claim a lifecycle case makes is read
// off a released module: the residency lease another party can or cannot take,
// the durable registration Factory's owner check reads, the harness journal,
// the Host's own metrics endpoint, and the bytes a real tool read back from a
// real harness workspace.

// PooledHostConfig is the lifecycle knobs of one pooled Host. The zero value is
// exactly the Host every earlier lane composes.
type PooledHostConfig struct {
	// ID and Generation name the Host for StartSizedPooledHost. The other
	// constructors take them as arguments and ignore these.
	ID         sessionwire.HostID
	Generation uint64

	// Capacity, when positive, is a POOLED Host's advertised capacity. Zero
	// keeps the kit's 8. Placement skips a full Host, so capacity one is how
	// a case puts its next session on another Host (see StartPooledHostSized).
	Capacity uint64
	// WarmTTL is how long a session must stay idle before a pooled Host
	// releases it. Zero keeps the kit's 90s, which no earlier case reaches.
	WarmTTL time.Duration
	// WorkPoll is how often resident sessions are asked what they are doing.
	WorkPoll time.Duration
	// ReconcileInterval is the Host's own inbox reconcile. A case proving that
	// a HostLink WAKE was accepted sets it long, so a reconcile pass cannot
	// apply the command in the wake's place.
	ReconcileInterval time.Duration
	// Drain, when set, replaces the drain options. Its Grace bounds a warm
	// release's runtime release too, which is what makes a refused release
	// observable in seconds.
	Drain *host.DrainOptions
	// WrapCheckpointer, when set, wraps the product's checkpointer.
	WrapCheckpointer func(host.Checkpointer) host.Checkpointer
	// WorkspaceBase, when set, is this Host's OWN physical workspace base
	// path. Empty takes the world's one shared base. See
	// PooledWorld.WipeWorkspaceDisk.
	WorkspaceBase string
	// Mortal gives this Host a durable-plane view (HostProcess) that its
	// death can cut (PooledHost.Kill) or that can be PAUSED and resumed
	// (HostProcess.Pause/Resume), as a stopped process would be.
	Mortal bool
	// MaxBindingsPerLink and MaxBindings bound the Host's HostLink routing
	// table; zero keeps 16 and 32. They must be sized with Capacity or a
	// Factory's bind is refused.
	MaxBindingsPerLink int
	MaxBindings        int
	// CommandQueueSize is the Host's local command buffer; zero keeps 16.
	CommandQueueSize int
}

// StartLifecycleHost is StartPooledHost with lifecycle knobs.
func StartLifecycleHost(tb TB, ctx context.Context, world *PooledWorld, id sessionwire.HostID, generation uint64, cfg PooledHostConfig) *PooledHost {
	tb.Helper()
	return startHostConfigured(tb, ctx, world, id, generation, "", nil, cfg)
}

// ---- the Host's own metrics --------------------------------------------------

// Metric reads one unlabelled or fully-labelled sample off the Host's own
// metrics endpoint, the way an operator or an autoscaler reads it. A sample
// that is absent is a failure: a renamed metric would otherwise read as zero.
func (h *PooledHost) Metric(tb TB, sample string) int {
	tb.Helper()
	recorder := httptest.NewRecorder()
	h.Service.MetricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		if value, found := strings.CutPrefix(line, sample+" "); found {
			count, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			if err != nil {
				tb.Fatalf("orchestrationtest: %s = %q: %v", sample, value, err)
			}
			return int(count)
		}
	}
	tb.Fatalf("orchestrationtest: host %s's metrics carry no %s:\n%s", h.ID, sample, recorder.Body.String())
	return 0
}

// SessionsIn reads host_sessions for one local residency state.
func (h *PooledHost) SessionsIn(tb TB, state string) int {
	tb.Helper()
	return h.Metric(tb, `host_sessions{state="`+state+`"}`)
}

// ReleaseFailures reads host_release_failures_total: a release that was taken
// back or held counts here.
func (h *PooledHost) ReleaseFailures(tb TB) int {
	tb.Helper()
	return h.Metric(tb, "host_release_failures_total")
}

// ---- durable observations ----------------------------------------------------

// ResidencyHeld reports whether some Host holds the session's residency lease,
// by trying to take it as another party would. A successful take is released
// at once.
//
// It is a PROBE THAT BRIEFLY HOLDS THE LEASE. A Host attaching in that instant
// is refused and retried by Factory, so a case calls this only once it has
// already observed the release some other way.
func (w *PooledWorld) ResidencyHeld(tb TB, ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID) bool {
	tb.Helper()
	grant, err := w.Store.AcquireResidency(ctx, sessionstore.AcquireResidencyRequest{TenantID: tenant, SessionID: s})
	if err != nil {
		return true
	}
	if err := grant.Release(ctx); err != nil {
		tb.Fatalf("orchestrationtest: releasing the probe residency: %v", err)
	}
	return false
}

// Registration reads a session's durable Host registration exactly as
// Factory's owner check does. found is false when there is none.
func (w *PooledWorld) Registration(tb TB, ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool) {
	tb.Helper()
	entry, err := w.Store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{TenantID: tenant, SessionID: s})
	if err != nil {
		return sessionwire.HostLinkRegistryObservation{}, false
	}
	observation, err := entry.Registration.Observation()
	if err != nil {
		return sessionwire.HostLinkRegistryObservation{}, false
	}
	return observation, true
}

// ---- the product's checkpoint seam -------------------------------------------

// pooledCheckpointer is the PRODUCT's host.Checkpointer: on a release it
// snapshots the live session's harness workspace, which is the only state a
// release could lose that the harness journal does not already hold. In a
// world with no workspace there is nothing to checkpoint.
type pooledCheckpointer struct {
	rig        *PooledRig
	workspaces bool
}

func (c pooledCheckpointer) Checkpoint(ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID) error {
	if !c.workspaces {
		return nil
	}
	live := c.rig.liveSession(tenant, s)
	if live == nil {
		return fmt.Errorf("orchestrationtest: no live runtime for %s/%s to checkpoint", tenant, s)
	}
	_, err := live.controller.CheckpointWorkspace(ctx)
	return err
}

// PausingCheckpointer holds the FIRST checkpoint it is asked for until the case
// proceeds -- a seam that opens the release window at the one step (after the
// Host stopped admitting, before the runtime release) a case needs to act in.
type PausingCheckpointer struct {
	Inner host.Checkpointer

	entered chan struct{}
	proceed chan struct{}
	first   sync.Once
	opened  sync.Once
}

// NewPausingCheckpointer returns a checkpointer whose first call waits.
func NewPausingCheckpointer() *PausingCheckpointer {
	return &PausingCheckpointer{entered: make(chan struct{}), proceed: make(chan struct{})}
}

// Wrap satisfies PooledHostConfig.WrapCheckpointer.
func (c *PausingCheckpointer) Wrap(inner host.Checkpointer) host.Checkpointer {
	c.Inner = inner
	return c
}

// Checkpoint satisfies host.Checkpointer.
func (c *PausingCheckpointer) Checkpoint(ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID) error {
	first := false
	c.first.Do(func() { first = true })
	if first {
		close(c.entered)
		select {
		case <-c.proceed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.Inner.Checkpoint(ctx, tenant, s)
}

// AwaitEntered waits until a release has reached its checkpoint.
func (c *PausingCheckpointer) AwaitEntered(tb TB, within time.Duration) {
	tb.Helper()
	select {
	case <-c.entered:
	case <-time.After(within):
		tb.Fatalf("orchestrationtest: no release reached its checkpoint within %s", within)
	}
}

// Proceed lets the held checkpoint continue. It is idempotent.
func (c *PausingCheckpointer) Proceed() { c.opened.Do(func() { close(c.proceed) }) }

// ---- the live runtime ----------------------------------------------------------

func (p *PooledRig) liveSession(tenant sessionwire.TenantID, s sessionwire.SessionID) *pooledSession {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.live[pooledTailKey{tenant, s}]
}

// Submit hands the live runtime of one session a turn DIRECTLY, not through
// Factory or Host's command path.
//
// It is the product doing work on its own -- a scheduled job, a timer, a
// background event -- and it exists because a warm release HALTS Host's
// command consumer before it re-confirms idle: no command Factory admits can
// make a runtime busy inside the release window, so a case proving what
// happens when one does has to start the work the way a product would.
func (p *PooledRig) Submit(tb TB, ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID, text string) {
	tb.Helper()
	live := p.liveSession(tenant, s)
	if live == nil {
		tb.Fatalf("orchestrationtest: no live runtime for %s/%s", tenant, s)
		return
	}
	if _, err := live.controller.Submit(ctx, []content.Block{&content.TextBlock{Text: text}}); err != nil {
		tb.Fatalf("orchestrationtest: submitting to %s/%s: %v", tenant, s, err)
	}
}

// WorkspaceRoots reports the WorkspaceRoot Host handed each launch, in order.
func (p *PooledRig) WorkspaceRoots() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.workspaceRoots...)
}

// ---- a real harness workspace, and the agent's file tools --------------------

// Tool names the workspace world's agent calls.
const (
	PooledWriteToolName = "orchestrationtest_write_file"
	PooledReadToolName  = "orchestrationtest_read_file"
)

// PooledWorkspaceRead is one read the agent's read tool made: what it returned,
// and where the agent saw its workspace.
type PooledWorkspaceRead struct {
	// LogicalRoot is the MODEL-VISIBLE workspace path, which harness derives
	// from the session's identity alone.
	LogicalRoot string
	// PhysicalRoot is this process's location of the same tree.
	PhysicalRoot string
	Path         string
	Present      bool
	Content      string
}

// PooledWorkspaceTools is the agent's two file tools over harness's REAL
// per-session workspace binding. They record what they did so a case can
// assert on the tool RESULT -- the bytes a real tool read back off a real
// materialized workspace -- rather than on transcript text.
type PooledWorkspaceTools struct {
	mu     sync.Mutex
	writes []PooledWorkspaceRead
	reads  []PooledWorkspaceRead
}

// Reads reports every read, in order.
func (t *PooledWorkspaceTools) Reads() []PooledWorkspaceRead {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]PooledWorkspaceRead(nil), t.reads...)
}

// Writes reports every write, in order.
func (t *PooledWorkspaceTools) Writes() []PooledWorkspaceRead {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]PooledWorkspaceRead(nil), t.writes...)
}

func (t *PooledWorkspaceTools) definitions() []tool.Definition {
	build := func(name string) tool.Definition {
		return tool.NewDefinition(name, tool.RequiresWorkspace, func(_ context.Context, bindings tool.Bindings) ([]tool.InvokableTool, error) {
			if bindings.Workspace == nil {
				return nil, fmt.Errorf("orchestrationtest: %s was built with no workspace binding", name)
			}
			return []tool.InvokableTool{&pooledFileTool{name: name, binding: *bindings.Workspace, record: t}}, nil
		})
	}
	return []tool.Definition{build(PooledWriteToolName), build(PooledReadToolName)}
}

type pooledFileTool struct {
	name    string
	binding tool.WorkspaceBinding
	record  *PooledWorkspaceTools
}

type pooledFileInput struct {
	Path    string `json:"path"`
	Content string `json:"content,omitempty"`
}

func (f *pooledFileTool) Info(context.Context) (*tool.ToolInfo, error) {
	return &tool.ToolInfo{
		Name:   f.name,
		Desc:   "Reads or writes one file in the session workspace.",
		Schema: []byte(`{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path"],"additionalProperties":false}`),
	}, nil
}

// PrepareCall satisfies tool.CallPreparer; see PooledAskTool.PrepareCall for
// why a tool under an AccessGate must have one.
func (f *pooledFileTool) PrepareCall(_ context.Context, executionID uuid.UUID, _ string) (tool.Request, tool.PreparedArtifact, error) {
	return tool.Request{
		ToolName:           f.name,
		Summary:            "touch one workspace file",
		ExecutionID:        executionID.String(),
		ExpiresAtUnixMilli: time.Now().Add(time.Hour).UnixMilli(),
	}, nil, nil
}

func (f *pooledFileTool) InvokableRun(ctx context.Context, input string) (*tool.ToolResult, error) {
	var args pooledFileInput
	if err := json.Unmarshal([]byte(input), &args); err != nil {
		return nil, err
	}
	if args.Path == "" || strings.Contains(args.Path, "..") || filepath.IsAbs(args.Path) {
		return nil, fmt.Errorf("orchestrationtest: %q is not a workspace-relative path", args.Path)
	}
	target := filepath.Join(f.binding.Root, args.Path)
	observed := PooledWorkspaceRead{LogicalRoot: f.binding.LogicalRoot, PhysicalRoot: f.binding.Root, Path: args.Path}
	if f.name == PooledWriteToolName {
		if f.binding.Coordinator != nil {
			permit, err := f.binding.Coordinator.Acquire(ctx, tool.WorkspaceOperationPathMutation, target)
			if err != nil {
				return nil, err
			}
			defer permit.Release()
		}
		if err := os.WriteFile(target, []byte(args.Content), 0o600); err != nil {
			return nil, err
		}
		observed.Present, observed.Content = true, args.Content
		f.record.mu.Lock()
		f.record.writes = append(f.record.writes, observed)
		f.record.mu.Unlock()
		return tool.TextResult(fmt.Sprintf("wrote %s under %s", args.Path, f.binding.LogicalRoot)), nil
	}
	data, err := os.ReadFile(target) // #nosec G304 -- the path is confined to the session workspace above
	switch {
	case err == nil:
		observed.Present, observed.Content = true, string(data)
	case errors.Is(err, os.ErrNotExist):
	default:
		return nil, err
	}
	f.record.mu.Lock()
	f.record.reads = append(f.record.reads, observed)
	f.record.mu.Unlock()
	if !observed.Present {
		return tool.TextResult(fmt.Sprintf("%s is absent under %s", args.Path, f.binding.LogicalRoot)), nil
	}
	return tool.TextResult(fmt.Sprintf("%s under %s holds: %s", args.Path, f.binding.LogicalRoot, observed.Content)), nil
}

var (
	_ tool.InvokableTool = (*pooledFileTool)(nil)
	_ tool.CallPreparer  = (*pooledFileTool)(nil)
)

// WipeWorkspaceDisk removes every materialized workspace, as a fresh Pod's
// empty disk would have none. It reports how many session roots it removed.
//
// # Why Hosts share ONE physical base path by default
//
// harness v0.36.0 folds a workspace placement's canonical PHYSICAL base into the
// session's config fingerprint (rig.placementFingerprint), and its default
// restore decider REJECTS a restore whose fingerprint moved ("restore rejected
// by policy: 1 warn category (workspace)") -- measured here, with each Host on
// its own directory. So, before v0.37.1, a pooled fleet had to mount the base at the
// same path on every Host, which is what a real deployment of identical Pods
// does. harness v0.37.1 compares two per-session placements by mode alone, so
// a relocated base now restores; a case proving that gives each Host its own
// PooledHostConfig.WorkspaceBase. In one test binary the same path is the same
// disk, so a case using the shared base wipes it
// between generations: the bytes a successor then reads can only have come
// from the durable snapshot plane.
func (w *PooledWorld) WipeWorkspaceDisk(tb TB) int {
	tb.Helper()
	entries, err := os.ReadDir(w.workspaceBase)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		tb.Fatalf("orchestrationtest: reading the workspace base: %v", err)
		return 0
	}
	if err := os.RemoveAll(w.workspaceBase); err != nil {
		tb.Fatalf("orchestrationtest: wiping the workspace base: %v", err)
		return 0
	}
	if err := os.MkdirAll(w.workspaceBase, 0o700); err != nil {
		tb.Fatalf("orchestrationtest: recreating the workspace base: %v", err)
		return 0
	}
	return len(entries)
}

// pooledWorkspaceOptions places each session's workspace at base/<runtime id>, snapshots it into the world's durable plane at every
// turn's end, and lets a restore materialize it from there.
func pooledWorkspaceOptions(tb TB, durable *storage.Composite, base string, tenant sessionwire.TenantID) []rig.Option {
	tb.Helper()
	spool := filepath.Join(filepath.Dir(base), "spool-"+hashForPath(string(tenant)))
	if err := os.MkdirAll(spool, 0o700); err != nil {
		tb.Fatalf("orchestrationtest: creating the snapshot spool: %v", err)
		return nil
	}
	if err := os.MkdirAll(base, 0o700); err != nil {
		tb.Fatalf("orchestrationtest: creating the workspace base: %v", err)
		return nil
	}
	store, err := workspacestore.Open(durable.Blobs, workspacestore.WithSpoolDir(spool))
	if err != nil {
		tb.Fatalf("orchestrationtest: opening the workspace store: %v", err)
		return nil
	}
	return []rig.Option{
		rig.WithSessionWorkspaces(store, base),
		rig.WithSnapshots(rig.SnapshotPolicy{Trigger: rig.SnapshotOnTurnDone}),
	}
}

// RuntimeEnded reports whether the latest runtime this Host launched for a
// session has ended (its Done channel is closed). found is false when this
// Host never launched one.
func (p *PooledRig) RuntimeEnded(tenant sessionwire.TenantID, s sessionwire.SessionID) (ended, found bool) {
	live := p.liveSession(tenant, s)
	if live == nil {
		return false, false
	}
	select {
	case <-live.Done():
		return true, true
	default:
		return false, true
	}
}

// AssertModelVisiblePathStable checks that a restored generation's tool saw its
// workspace at the same model-visible path the first generation did.
//
// harness v0.36.0 bound every loop planned at restore with an EMPTY
// LogicalRoot (restoreTopologySession handed planLoops a probe session whose id
// was zero); harness v0.37.1 fixed it, and this is now a hard assertion.
func AssertModelVisiblePathStable(tb TB, written, restored PooledWorkspaceRead) {
	tb.Helper()
	if written.LogicalRoot == "" {
		tb.Fatalf("orchestrationtest: the first generation's tool saw no model-visible workspace path")
		return
	}
	if restored.LogicalRoot != written.LogicalRoot {
		tb.Fatalf("orchestrationtest: the model-visible workspace path moved from %q to %q", written.LogicalRoot, restored.LogicalRoot)
	}
}

// WorkspaceStatus reads harness's own workspace report off the latest runtime
// this Host launched for a session: where the workspace is, which checkpoint
// the live tree came up on, and whether journalled work followed it. found is
// false when this Host launched none, or its runtime reports no workspace.
func (p *PooledRig) WorkspaceStatus(tenant sessionwire.TenantID, s sessionwire.SessionID) (status session.WorkspaceStatus, found bool) {
	live := p.liveSession(tenant, s)
	if live == nil {
		return session.WorkspaceStatus{}, false
	}
	reporter, ok := live.controller.(session.WorkspaceReporter)
	if !ok {
		return session.WorkspaceStatus{}, false
	}
	return reporter.WorkspaceStatus(), true
}

// CheckpointWorkspace asks the latest runtime this Host launched for a session
// to checkpoint its workspace, as the product's release checkpointer does.
func (p *PooledRig) CheckpointWorkspace(ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID) error {
	live := p.liveSession(tenant, s)
	if live == nil {
		return fmt.Errorf("orchestrationtest: no live runtime for %s/%s", tenant, s)
	}
	_, err := live.controller.CheckpointWorkspace(ctx)
	return err
}

// JournalSeqs returns every event of one type in a tenant's journal with the
// JOURNAL SEQUENCE it was committed at.
func JournalSeqs[E event.Event](tb TB, world *PooledWorld, tenant sessionwire.TenantID, id uuid.UUID) ([]E, []uint64) {
	tb.Helper()
	var found []E
	var seqs []uint64
	walkJournal(tb, world, tenant, id, func(next event.Event, seq uint64) {
		if typed, ok := next.(E); ok {
			found = append(found, typed)
			seqs = append(seqs, seq)
		}
	})
	return found, seqs
}

// JournalHoldsEventID reports whether any event in a tenant's journal carries
// the event id.
func JournalHoldsEventID(tb TB, world *PooledWorld, tenant sessionwire.TenantID, id uuid.UUID, eventID string) bool {
	tb.Helper()
	held := false
	walkJournal(tb, world, tenant, id, func(next event.Event, _ uint64) {
		if next.EventHeader().EventID.String() == eventID {
			held = true
		}
	})
	return held
}

func walkJournal(tb TB, world *PooledWorld, tenant sessionwire.TenantID, id uuid.UUID, visit func(event.Event, uint64)) {
	tb.Helper()
	replayer, err := world.Journals[tenant].OpenInternalEventReplayer(id, harnessstore.ReplayRequest{FromSeq: 0})
	if err != nil {
		tb.Fatalf("orchestrationtest: opening a replayer for %s: %v", id, err)
		return
	}
	cursor, err := replayer.Open(context.Background(), journal.ReplayRequest{From: journal.Beginning()})
	if err != nil {
		tb.Fatalf("orchestrationtest: opening a replay cursor for %s: %v", id, err)
		return
	}
	defer cursor.Close()
	for {
		next, seq, err := cursor.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			tb.Fatalf("orchestrationtest: replaying %s: %v", id, err)
			return
		}
		visit(next, seq)
	}
}

// LastCheckpointSeq is the journal sequence and ref of the last
// WorkspaceCheckpointed in a session's journal. found is false when there is
// none.
func LastCheckpointSeq(tb TB, world *PooledWorld, tenant sessionwire.TenantID, id uuid.UUID) (seq uint64, ref string, found bool) {
	tb.Helper()
	events, seqs := JournalSeqs[event.WorkspaceCheckpointed](tb, world, tenant, id)
	if len(events) == 0 {
		return 0, "", false
	}
	return seqs[len(seqs)-1], events[len(events)-1].Ref, true
}

// AwaitWorkspaceStatus waits for the Host's latest runtime for a session to
// report a workspace, and returns what it reports.
func AwaitWorkspaceStatus(tb TB, host *PooledHost, tenant sessionwire.TenantID, s sessionwire.SessionID) session.WorkspaceStatus {
	tb.Helper()
	var status session.WorkspaceStatus
	PooledWait(tb, "host "+string(host.ID)+" reported "+string(s)+"'s workspace", 30*time.Second, func() bool {
		reported, found := host.Rig.WorkspaceStatus(tenant, s)
		status = reported
		return found && reported.Root != ""
	})
	return status
}

// AwaitJournalQuiet waits until a session's journal tip has not moved for
// quiet, and returns that tip. A turn's trailing loop records land
// asynchronously after TurnDone, so "the turn finished" is not "the journal
// stopped".
func AwaitJournalQuiet(tb TB, world *PooledWorld, tenant sessionwire.TenantID, id uuid.UUID, quiet time.Duration) uint64 {
	tb.Helper()
	tip := func() uint64 {
		var last uint64
		walkJournal(tb, world, tenant, id, func(_ event.Event, seq uint64) { last = seq })
		return last
	}
	current, since := tip(), time.Now()
	PooledWait(tb, "the journal went quiet", 30*time.Second, func() bool {
		if next := tip(); next != current {
			current, since = next, time.Now()
			return false
		}
		return time.Since(since) >= quiet
	})
	return current
}
