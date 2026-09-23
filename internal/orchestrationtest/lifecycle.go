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
	"github.com/looprig/harness/pkg/rig"
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
	// Mortal gives this Host a durable-plane view its death can cut. See
	// PooledHost.Kill.
	Mortal bool
}

// StartLifecycleHost is StartPooledHost with lifecycle knobs.
func StartLifecycleHost(tb TB, ctx context.Context, world *PooledWorld, id sessionwire.HostID, generation uint64, cfg PooledHostConfig) *PooledHost {
	tb.Helper()
	return startHostConfigured(tb, ctx, world, id, generation, "", nil, cfg)
}

// ---- a Host process that can die --------------------------------------------

// ErrHostProcessDead is what every durable-plane call from a dead Host answers.
var ErrHostProcessDead = errors.New("orchestrationtest: this Host process is dead")

// HostProcess is ONE Host process's view of the shared durable plane.
//
// # Why a view, and not Stop
//
// Stop DRAINS: it releases every lease and tombstones every registration, which
// is the graceful path and proves nothing about takeover. A process that dies
// does none of that. Its leases lapse when their provider's TTL runs out, its
// registrations expire when it stops heartbeating, and it writes nothing ever
// again. memstore never lapses a lease, so a dead in-process Host would hold
// its session forever; this view is what makes death look like death:
//
//   - every call through the view fails ErrHostProcessDead, so the corpse's
//     goroutines -- which are still running in this test binary -- can write no
//     journal frame, no registration, no command state;
//   - every lease it acquired is RELEASED ON THE UNDERLYING PROVIDER and its
//     Lost channel closes, which is exactly a TTL lapse: the next Acquire, by
//     anyone, is granted at a strictly greater epoch.
//
// The lapse is immediate rather than after a TTL. That shortens the case; it
// does not change what a successor sees.
type HostProcess struct {
	mu     sync.Mutex
	dead   bool
	leases []*processLease
}

// View wraps inner so this process's death can cut it.
func (p *HostProcess) View(tb TB, inner *storage.Composite) *storage.Composite {
	tb.Helper()
	blobs := storage.Blobs(processBlobs{inner: inner.Blobs, process: p})
	if lifecycle, ok := inner.Blobs.(storage.BlobReaderLifecycle); ok {
		// PRESERVED, not invented: sessionstore refuses a Blobs provider that
		// does not implement the bounded reader lifecycle, and this view must
		// not claim a lifecycle the provider underneath does not have.
		blobs = processLifecycleBlobs{processBlobs: processBlobs{inner: inner.Blobs, process: p}, bound: lifecycle.BlobReaderCloseBound()}
	}
	view, err := storage.NewCompositeWithOrderedIndex(
		processLedger{inner: inner.Ledger, process: p},
		processLeaser{inner: inner.Leaser, process: p},
		processKV{inner: inner.KV, process: p},
		blobs,
		processOrdered{OrderedIndex: inner.OrderedIndex, process: p},
	)
	if err != nil {
		tb.Fatalf("orchestrationtest: composing a Host process view: %v", err)
		return nil
	}
	return view
}

func (p *HostProcess) check() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dead {
		return ErrHostProcessDead
	}
	return nil
}

// kill marks the process dead and lapses every lease it holds.
func (p *HostProcess) kill() int {
	p.mu.Lock()
	p.dead = true
	leases := p.leases
	p.leases = nil
	p.mu.Unlock()
	for _, lease := range leases {
		lease.lapse()
	}
	return len(leases)
}

type processLedger struct {
	inner   storage.Ledger
	process *HostProcess
}

func (l processLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	if err := l.process.check(); err != nil {
		return err
	}
	return l.inner.Append(ctx, name, expected, payload)
}

func (l processLedger) Read(ctx context.Context, name string, from uint64) (storage.Cursor, error) {
	if err := l.process.check(); err != nil {
		return nil, err
	}
	return l.inner.Read(ctx, name, from)
}

func (l processLedger) Tip(ctx context.Context, name string) (uint64, error) {
	if err := l.process.check(); err != nil {
		return 0, err
	}
	return l.inner.Tip(ctx, name)
}

func (l processLedger) Delete(ctx context.Context, name string) error {
	if err := l.process.check(); err != nil {
		return err
	}
	return l.inner.Delete(ctx, name)
}

type processLeaser struct {
	inner   storage.Leaser
	process *HostProcess
}

func (l processLeaser) Acquire(ctx context.Context, name string) (storage.Lease, error) {
	if err := l.process.check(); err != nil {
		return nil, err
	}
	inner, err := l.inner.Acquire(ctx, name)
	if err != nil {
		return nil, err
	}
	lease := &processLease{inner: inner, lost: make(chan struct{})}
	go func() {
		select {
		case <-inner.Lost():
			lease.closeLost()
		case <-lease.lost:
		}
	}()
	l.process.mu.Lock()
	dead := l.process.dead
	if !dead {
		l.process.leases = append(l.process.leases, lease)
	}
	l.process.mu.Unlock()
	if dead {
		lease.lapse()
		return nil, ErrHostProcessDead
	}
	return lease, nil
}

type processLease struct {
	inner storage.Lease
	lost  chan struct{}
	once  sync.Once
}

func (l *processLease) Epoch() uint64         { return l.inner.Epoch() }
func (l *processLease) Lost() <-chan struct{} { return l.lost }
func (l *processLease) closeLost()            { l.once.Do(func() { close(l.lost) }) }
func (l *processLease) Release(ctx context.Context) error {
	return l.inner.Release(ctx)
}

// lapse is what a provider's TTL does to a dead holder's grant.
func (l *processLease) lapse() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = l.inner.Release(ctx)
	l.closeLost()
}

type processKV struct {
	inner   storage.KV
	process *HostProcess
}

func (k processKV) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	if err := k.process.check(); err != nil {
		return nil, 0, err
	}
	return k.inner.Get(ctx, key)
}

func (k processKV) Put(ctx context.Context, key string, expectedRev uint64, val []byte) (uint64, error) {
	if err := k.process.check(); err != nil {
		return 0, err
	}
	return k.inner.Put(ctx, key, expectedRev, val)
}

func (k processKV) Keys(ctx context.Context, prefix string) ([]string, error) {
	if err := k.process.check(); err != nil {
		return nil, err
	}
	return k.inner.Keys(ctx, prefix)
}

func (k processKV) Delete(ctx context.Context, key string) error {
	if err := k.process.check(); err != nil {
		return err
	}
	return k.inner.Delete(ctx, key)
}

type processBlobs struct {
	inner   storage.Blobs
	process *HostProcess
}

func (b processBlobs) Put(ctx context.Context, key string, r io.Reader) error {
	if err := b.process.check(); err != nil {
		return err
	}
	return b.inner.Put(ctx, key, r)
}

func (b processBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := b.process.check(); err != nil {
		return nil, err
	}
	return b.inner.Get(ctx, key)
}

func (b processBlobs) Delete(ctx context.Context, key string) error {
	if err := b.process.check(); err != nil {
		return err
	}
	return b.inner.Delete(ctx, key)
}

func (b processBlobs) List(ctx context.Context, prefix string) ([]string, error) {
	if err := b.process.check(); err != nil {
		return nil, err
	}
	return b.inner.List(ctx, prefix)
}

type processLifecycleBlobs struct {
	processBlobs
	bound time.Duration
}

func (b processLifecycleBlobs) BlobReaderCloseBound() time.Duration { return b.bound }

type processOrdered struct {
	storage.OrderedIndex
	process *HostProcess
}

func (o processOrdered) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	if err := o.process.check(); err != nil {
		return storage.OrderedRecord{}, err
	}
	return o.OrderedIndex.Get(ctx, id)
}

func (o processOrdered) Create(ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	if err := o.process.check(); err != nil {
		return storage.OrderedRecord{}, false, err
	}
	return o.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due)
}

func (o processOrdered) Update(ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	if err := o.process.check(); err != nil {
		return storage.OrderedRecord{}, err
	}
	return o.OrderedIndex.Update(ctx, id, expectedRevision, value, rank, due)
}

func (o processOrdered) Delete(ctx context.Context, id storage.OrderedID, expectedRevision uint64) (storage.OrderedRecord, error) {
	if err := o.process.check(); err != nil {
		return storage.OrderedRecord{}, err
	}
	return o.OrderedIndex.Delete(ctx, id, expectedRevision)
}

func (o processOrdered) ListOrdered(ctx context.Context, namespace string, orderingScope string, afterOrder uint64, limit int) (storage.OrderedPage, error) {
	if err := o.process.check(); err != nil {
		return storage.OrderedPage{}, err
	}
	return o.OrderedIndex.ListOrdered(ctx, namespace, orderingScope, afterOrder, limit)
}

func (o processOrdered) ListRanked(ctx context.Context, namespace string, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	if err := o.process.check(); err != nil {
		return storage.RankedPage{}, err
	}
	return o.OrderedIndex.ListRanked(ctx, namespace, rankingScope, after, limit)
}

func (o processOrdered) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	if err := o.process.check(); err != nil {
		return storage.DuePage{}, err
	}
	return o.OrderedIndex.ListDue(ctx, namespace, dueAtOrBefore, after, limit)
}

// Kill is this Host's PROCESS DYING: nothing is drained, released, checkpointed
// or tombstoned. Its durable plane is cut and its leases lapse (see
// HostProcess), and its listener and every accepted connection close, so no
// Factory can reach it. The in-memory corpse is left running, as a paused or
// partitioned process would be; it can no longer write anything durable.
//
// Only a Host started with PooledHostConfig.Mortal can die. It reports how many
// leases lapsed.
func (h *PooledHost) Kill(tb TB) int {
	tb.Helper()
	if h.process == nil {
		tb.Fatalf("orchestrationtest: host %s is not mortal; start it with PooledHostConfig.Mortal", h.ID)
		return 0
	}
	lapsed := h.process.kill()
	_ = h.tracker.Close()
	h.tracker.Sever()
	return lapsed
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
// # Why every Host shares ONE physical base path
//
// harness v0.36.0 folds a workspace placement's canonical PHYSICAL base into the
// session's config fingerprint (rig.placementFingerprint), and its default
// restore decider REJECTS a restore whose fingerprint moved ("restore rejected
// by policy: 1 warn category (workspace)") -- measured here, with each Host on
// its own directory. So a pooled fleet must mount the workspace base at the
// same path on every Host, which is what a real deployment of identical Pods
// does. In one test binary the same path is the same disk, so a case wipes it
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
// # KNOWN DEFECT IN harness v0.36.0, PINNED SO ITS FIX IS NOTICED
//
// A RESTORED session binds its loops' tools with an EMPTY LogicalRoot:
// restoreTopologySession (internal/sessionruntime/restore_constructor.go:492)
// hands planLoops `probe.newWorkspaceBinding`, and `probe` is a bare
// &Session{} whose sessionID is zero, so logicalWorkspaceRoot yields "". The
// physical root and the bytes are right; the path harness promises is stable
// across Hosts ("a journalled instruction naming a file must still resolve")
// is simply absent after every restore. Still present on harness main at
// cf01e492. Until a release fixes it, an empty restored LogicalRoot is logged,
// not failed; a NON-empty one must equal the first generation's, and the
// physical root -- the same base path on every Host -- must match.
func AssertModelVisiblePathStable(tb TB, written, restored PooledWorkspaceRead) {
	tb.Helper()
	if written.LogicalRoot == "" {
		tb.Fatalf("orchestrationtest: the first generation's tool saw no model-visible workspace path")
		return
	}
	if restored.PhysicalRoot != written.PhysicalRoot {
		tb.Fatalf("orchestrationtest: the workspace moved from %q to %q", written.PhysicalRoot, restored.PhysicalRoot)
		return
	}
	if restored.LogicalRoot == "" {
		tb.Logf("KNOWN harness v0.36.0 DEFECT: the restored tool binding carries no LogicalRoot (want %q)", written.LogicalRoot)
		return
	}
	if restored.LogicalRoot != written.LogicalRoot {
		tb.Fatalf("orchestrationtest: the model-visible workspace path moved from %q to %q", written.LogicalRoot, restored.LogicalRoot)
	}
}
