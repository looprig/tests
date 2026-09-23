//go:build integration

package orchestrationtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/looprig/storage"
)

// ErrHostProcessDead is what every durable-plane call from a dead Host answers.
var ErrHostProcessDead = errors.New("orchestrationtest: this Host process is dead")

// The durable planes a Host process reaches, as HostProcess labels them.
const (
	// PlaneStore is the SessionStore backend: catalog, inbox, registry.
	PlaneStore = "store"
	// PlaneJournal is the harness runtime journal backend.
	PlaneJournal = "journal"
	// PlaneWorkspace is the durable workspace snapshot plane.
	PlaneWorkspace = "workspace"
)

// ProcessCall is one call a Host process made into its durable plane, as the
// process view saw it. Only calls made while the view was RECORDING are kept.
type ProcessCall struct {
	Seq   int
	Plane string
	// Op is the primitive: "ledger.append", "kv.put", "ordered.update", ...
	Op string
	// Name is the ledger name, KV key, blob key or ordered id.
	Name string
	// Payload is the bytes a ledger append or a KV/ordered write carried.
	Payload []byte
	// Held reports the call was one a hold caught IN FLIGHT; Paused that it
	// arrived while the process was paused. Either way it reached the real
	// store only after Resume.
	Held, Paused bool
	// Done and Err are the outcome the REAL store returned.
	Done bool
	Err  error
}

// Write reports whether the call writes.
func (c ProcessCall) Write() bool {
	switch c.Op {
	case "ledger.append", "ledger.delete", "kv.put", "kv.delete", "blobs.put", "blobs.delete",
		"ordered.create", "ordered.update", "ordered.delete":
		return true
	}
	return false
}

// HostProcess is ONE Host process's view of the shared durable plane.
//
// # Death
//
// Stop DRAINS: it releases every lease and tombstones every registration, which
// is the graceful path and proves nothing about takeover. A process that dies
// does none of that. Kill makes death look like death:
//
//   - every call through the view fails ErrHostProcessDead, so the corpse's
//     goroutines -- which are still running in this test binary -- can write no
//     journal frame, no registration, no command state;
//   - every lease it acquired is RELEASED ON THE UNDERLYING PROVIDER and its
//     Lost channel closes, which is a TTL lapse: the next Acquire, by anyone, is
//     granted at a strictly greater epoch.
//
// # A pause, and a stale process that comes back
//
// Kill is the wrong fixture for FENCING, because it refuses the corpse's writes
// at the fixture. Pause is a stopped process (a GC pause, SIGSTOP, a partition):
//
//   - every call BLOCKS instead of failing, and a Hold catches a chosen call
//     already IN FLIGHT at the pause;
//   - every lease lapses on the provider, but the process is NOT TOLD: Lost stays
//     open, because a stopped process cannot observe its own lapse;
//   - on Resume every blocked call reaches the REAL store, with the stale
//     process's own arguments (expected revisions, ledger tips), and the context
//     cancellation it may have accrued while stopped is dropped: a request
//     already on the wire is delivered.
//
// So whatever refuses a resumed stale write is the store, never this view. The
// view records every call it forwarded while RECORDING, with the store's answer.
type HostProcess struct {
	mu       sync.Mutex
	dead     bool
	paused   bool
	resumed  chan struct{}
	leases   []*processLease
	holds    []*ProcessHold
	record   bool
	calls    []*ProcessCall
	nextCall int
}

// ProcessHold catches the first call matching its predicate and keeps it in
// flight until the process resumes.
type ProcessHold struct {
	Name  string
	match func(ProcessCall) bool

	entered chan struct{}
	call    *ProcessCall
	caught  ProcessCall
}

// Call reports a copy of the caught call as it was caught, nil until one is.
// Its outcome is HostProcess.Call(seq)'s.
func (h *ProcessHold) Call() *ProcessCall {
	select {
	case <-h.entered:
		return &h.caught
	default:
		return nil
	}
}

// Caught is closed once the hold has caught its call.
func (h *ProcessHold) Caught() <-chan struct{} { return h.entered }

// Hold arms a hold. It catches the FIRST call that matches, from now on.
func (p *HostProcess) Hold(name string, match func(ProcessCall) bool) *ProcessHold {
	hold := &ProcessHold{Name: name, match: match, entered: make(chan struct{})}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.resumed == nil {
		p.resumed = make(chan struct{})
	}
	p.holds = append(p.holds, hold)
	return hold
}

// Record starts recording every call the view forwards.
func (p *HostProcess) Record() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.record = true
}

// Calls reports every recorded call, in arrival order. The values are copies.
func (p *HostProcess) Calls() []ProcessCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]ProcessCall, 0, len(p.calls))
	for _, c := range p.calls {
		out = append(out, *c)
	}
	return out
}

// Call reports one recorded call by sequence.
func (p *HostProcess) Call(seq int) ProcessCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.calls {
		if c.Seq == seq {
			return *c
		}
	}
	return ProcessCall{}
}

// Done reports whether the recorded call has returned.
func (p *HostProcess) Done(seq int) bool { return p.Call(seq).Done }

// Pause stops the process: every later call blocks until Resume, and every
// lease it holds lapses on the provider WITHOUT the process being told. It
// reports how many leases lapsed.
func (p *HostProcess) Pause() int {
	p.mu.Lock()
	p.paused = true
	if p.resumed == nil {
		p.resumed = make(chan struct{})
	}
	leases := p.leases
	p.leases = nil
	p.mu.Unlock()
	for _, lease := range leases {
		lease.lapseSilently()
	}
	return len(leases)
}

// Resume lets every blocked and held call reach the real store.
func (p *HostProcess) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	if p.resumed != nil {
		close(p.resumed)
		p.resumed = nil
	}
}

// View wraps inner, labelling every call with plane.
func (p *HostProcess) View(tb TB, plane string, inner *storage.Composite) *storage.Composite {
	tb.Helper()
	base := processBlobs{inner: inner.Blobs, process: p, plane: plane}
	blobs := storage.Blobs(base)
	if lifecycle, ok := inner.Blobs.(storage.BlobReaderLifecycle); ok {
		// PRESERVED, not invented: sessionstore refuses a Blobs provider that
		// does not implement the bounded reader lifecycle, and this view must
		// not claim a lifecycle the provider underneath does not have.
		blobs = processLifecycleBlobs{processBlobs: base, bound: lifecycle.BlobReaderCloseBound()}
	}
	view, err := storage.NewCompositeWithOrderedIndex(
		processLedger{inner: inner.Ledger, process: p, plane: plane},
		processLeaser{inner: inner.Leaser, process: p, plane: plane},
		processKV{inner: inner.KV, process: p, plane: plane},
		blobs,
		processOrdered{OrderedIndex: inner.OrderedIndex, process: p, plane: plane},
	)
	if err != nil {
		tb.Fatalf("orchestrationtest: composing a Host process view: %v", err)
		return nil
	}
	return view
}

// enter admits one call. It returns the context to forward with, the recorded
// call (nil when not recording) and an error when the process is dead.
func (p *HostProcess) enter(ctx context.Context, call ProcessCall) (context.Context, *ProcessCall, error) {
	p.mu.Lock()
	if p.dead {
		p.mu.Unlock()
		return ctx, nil, ErrHostProcessDead
	}
	var hold *ProcessHold
	for _, candidate := range p.holds {
		if candidate.call == nil && candidate.match(call) {
			hold = candidate
			break
		}
	}
	call.Held, call.Paused = hold != nil, hold == nil && p.paused
	var recorded *ProcessCall
	if p.record || hold != nil {
		p.nextCall++
		call.Seq = p.nextCall
		recorded = &call
		p.calls = append(p.calls, recorded)
	}
	var wait chan struct{}
	if hold != nil {
		hold.call = recorded
		hold.caught = *recorded
		close(hold.entered)
		wait = p.resumed
	} else if p.paused {
		wait = p.resumed
	}
	p.mu.Unlock()
	if wait == nil {
		return ctx, recorded, nil
	}
	<-wait
	// A request already on the wire is delivered: the cancellation a stopped
	// process accrued does not reach the store.
	return context.WithoutCancel(ctx), recorded, nil
}

func (p *HostProcess) leave(call *ProcessCall, err error) {
	if call == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	call.Done, call.Err = true, err
}

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
	plane   string
}

func (l processLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	ctx, call, err := l.process.enter(ctx, ProcessCall{Plane: l.plane, Op: "ledger.append", Name: name, Payload: append([]byte(nil), payload...)})
	if err != nil {
		return err
	}
	err = l.inner.Append(ctx, name, expected, payload)
	l.process.leave(call, err)
	return err
}

func (l processLedger) Read(ctx context.Context, name string, from uint64) (storage.Cursor, error) {
	ctx, call, err := l.process.enter(ctx, ProcessCall{Plane: l.plane, Op: "ledger.read", Name: name})
	if err != nil {
		return nil, err
	}
	cursor, err := l.inner.Read(ctx, name, from)
	l.process.leave(call, err)
	return cursor, err
}

func (l processLedger) Tip(ctx context.Context, name string) (uint64, error) {
	ctx, call, err := l.process.enter(ctx, ProcessCall{Plane: l.plane, Op: "ledger.tip", Name: name})
	if err != nil {
		return 0, err
	}
	tip, err := l.inner.Tip(ctx, name)
	l.process.leave(call, err)
	return tip, err
}

func (l processLedger) Delete(ctx context.Context, name string) error {
	ctx, call, err := l.process.enter(ctx, ProcessCall{Plane: l.plane, Op: "ledger.delete", Name: name})
	if err != nil {
		return err
	}
	err = l.inner.Delete(ctx, name)
	l.process.leave(call, err)
	return err
}

type processLeaser struct {
	inner   storage.Leaser
	process *HostProcess
	plane   string
}

func (l processLeaser) Acquire(ctx context.Context, name string) (storage.Lease, error) {
	ctx, call, err := l.process.enter(ctx, ProcessCall{Plane: l.plane, Op: "lease.acquire", Name: name})
	if err != nil {
		return nil, err
	}
	inner, err := l.inner.Acquire(ctx, name)
	l.process.leave(call, err)
	if err != nil {
		return nil, err
	}
	lease := &processLease{inner: inner, lost: make(chan struct{}), stop: make(chan struct{}), name: name, plane: l.plane}
	go lease.watch()
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

// processLease forwards a provider grant. Its Lost follows the provider's,
// except across a silent lapse (Pause), which a stopped process cannot see.
type processLease struct {
	inner  storage.Lease
	lost   chan struct{}
	once   sync.Once
	stop   chan struct{}
	stopMu sync.Once

	// name and plane identify the grant; released records that the PROCESS
	// released it itself. See HostProcess.HeldLeases.
	name     string
	plane    string
	released atomic.Bool
}

func (l *processLease) watch() {
	select {
	case <-l.inner.Lost():
		select {
		case <-l.stop:
			return
		default:
		}
		l.closeLost()
	case <-l.stop:
	case <-l.lost:
	}
}

func (l *processLease) Epoch() uint64         { return l.inner.Epoch() }
func (l *processLease) Lost() <-chan struct{} { return l.lost }
func (l *processLease) closeLost()            { l.once.Do(func() { close(l.lost) }) }
func (l *processLease) Release(ctx context.Context) error {
	err := l.inner.Release(ctx)
	if err == nil {
		l.released.Store(true)
	}
	return err
}

// HeldLeases reports the names of every lease this process acquired on plane
// and has NOT released itself, while it is alive. A lease a Kill or Pause
// lapsed is not counted: the process never released it, but it no longer
// holds it either. It is the provider-side evidence that a runtime still holds
// a grant after its Host "drained", which is what a crash-equivalent drain of a
// parked session leaves behind until the process exits.
func (p *HostProcess) HeldLeases(plane string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var held []string
	for _, lease := range p.leases {
		if lease.plane == plane && !lease.released.Load() {
			held = append(held, lease.name)
		}
	}
	return held
}

// lapse is what a provider's TTL does to a dead holder's grant, observed.
func (l *processLease) lapse() {
	l.release()
	l.closeLost()
}

// lapseSilently is the same lapse, unobserved by the holder.
func (l *processLease) lapseSilently() {
	l.stopMu.Do(func() { close(l.stop) })
	l.release()
}

func (l *processLease) release() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = l.inner.Release(ctx)
}

type processKV struct {
	inner   storage.KV
	process *HostProcess
	plane   string
}

func (k processKV) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	ctx, call, err := k.process.enter(ctx, ProcessCall{Plane: k.plane, Op: "kv.get", Name: key})
	if err != nil {
		return nil, 0, err
	}
	val, rev, err := k.inner.Get(ctx, key)
	k.process.leave(call, err)
	return val, rev, err
}

func (k processKV) Put(ctx context.Context, key string, expectedRev uint64, val []byte) (uint64, error) {
	ctx, call, err := k.process.enter(ctx, ProcessCall{Plane: k.plane, Op: "kv.put", Name: key, Payload: append([]byte(nil), val...)})
	if err != nil {
		return 0, err
	}
	rev, err := k.inner.Put(ctx, key, expectedRev, val)
	k.process.leave(call, err)
	return rev, err
}

func (k processKV) Keys(ctx context.Context, prefix string) ([]string, error) {
	ctx, call, err := k.process.enter(ctx, ProcessCall{Plane: k.plane, Op: "kv.keys", Name: prefix})
	if err != nil {
		return nil, err
	}
	keys, err := k.inner.Keys(ctx, prefix)
	k.process.leave(call, err)
	return keys, err
}

func (k processKV) Delete(ctx context.Context, key string) error {
	ctx, call, err := k.process.enter(ctx, ProcessCall{Plane: k.plane, Op: "kv.delete", Name: key})
	if err != nil {
		return err
	}
	err = k.inner.Delete(ctx, key)
	k.process.leave(call, err)
	return err
}

type processBlobs struct {
	inner   storage.Blobs
	process *HostProcess
	plane   string
}

func (b processBlobs) Put(ctx context.Context, key string, r io.Reader) error {
	ctx, call, err := b.process.enter(ctx, ProcessCall{Plane: b.plane, Op: "blobs.put", Name: key})
	if err != nil {
		return err
	}
	err = b.inner.Put(ctx, key, r)
	b.process.leave(call, err)
	return err
}

func (b processBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	ctx, call, err := b.process.enter(ctx, ProcessCall{Plane: b.plane, Op: "blobs.get", Name: key})
	if err != nil {
		return nil, err
	}
	body, err := b.inner.Get(ctx, key)
	b.process.leave(call, err)
	return body, err
}

func (b processBlobs) Delete(ctx context.Context, key string) error {
	ctx, call, err := b.process.enter(ctx, ProcessCall{Plane: b.plane, Op: "blobs.delete", Name: key})
	if err != nil {
		return err
	}
	err = b.inner.Delete(ctx, key)
	b.process.leave(call, err)
	return err
}

func (b processBlobs) List(ctx context.Context, prefix string) ([]string, error) {
	ctx, call, err := b.process.enter(ctx, ProcessCall{Plane: b.plane, Op: "blobs.list", Name: prefix})
	if err != nil {
		return nil, err
	}
	keys, err := b.inner.List(ctx, prefix)
	b.process.leave(call, err)
	return keys, err
}

type processLifecycleBlobs struct {
	processBlobs
	bound time.Duration
}

func (b processLifecycleBlobs) BlobReaderCloseBound() time.Duration { return b.bound }

type processOrdered struct {
	storage.OrderedIndex
	process *HostProcess
	plane   string
}

func orderedName(id storage.OrderedID) string { return fmt.Sprintf("%v", id) }

func (o processOrdered) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	ctx, call, err := o.process.enter(ctx, ProcessCall{Plane: o.plane, Op: "ordered.get", Name: orderedName(id)})
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	record, err := o.OrderedIndex.Get(ctx, id)
	o.process.leave(call, err)
	return record, err
}

func (o processOrdered) Create(ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	ctx, call, err := o.process.enter(ctx, ProcessCall{Plane: o.plane, Op: "ordered.create", Name: orderedName(id), Payload: append([]byte(nil), value...)})
	if err != nil {
		return storage.OrderedRecord{}, false, err
	}
	record, created, err := o.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due)
	o.process.leave(call, err)
	return record, created, err
}

func (o processOrdered) Update(ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	ctx, call, err := o.process.enter(ctx, ProcessCall{Plane: o.plane, Op: "ordered.update", Name: orderedName(id), Payload: append([]byte(nil), value...)})
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	record, err := o.OrderedIndex.Update(ctx, id, expectedRevision, value, rank, due)
	o.process.leave(call, err)
	return record, err
}

func (o processOrdered) Delete(ctx context.Context, id storage.OrderedID, expectedRevision uint64) (storage.OrderedRecord, error) {
	ctx, call, err := o.process.enter(ctx, ProcessCall{Plane: o.plane, Op: "ordered.delete", Name: orderedName(id)})
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	record, err := o.OrderedIndex.Delete(ctx, id, expectedRevision)
	o.process.leave(call, err)
	return record, err
}

func (o processOrdered) ListOrdered(ctx context.Context, namespace string, orderingScope string, afterOrder uint64, limit int) (storage.OrderedPage, error) {
	ctx, call, err := o.process.enter(ctx, ProcessCall{Plane: o.plane, Op: "ordered.list", Name: namespace})
	if err != nil {
		return storage.OrderedPage{}, err
	}
	page, err := o.OrderedIndex.ListOrdered(ctx, namespace, orderingScope, afterOrder, limit)
	o.process.leave(call, err)
	return page, err
}

func (o processOrdered) ListRanked(ctx context.Context, namespace string, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	ctx, call, err := o.process.enter(ctx, ProcessCall{Plane: o.plane, Op: "ordered.ranked", Name: namespace})
	if err != nil {
		return storage.RankedPage{}, err
	}
	page, err := o.OrderedIndex.ListRanked(ctx, namespace, rankingScope, after, limit)
	o.process.leave(call, err)
	return page, err
}

func (o processOrdered) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	ctx, call, err := o.process.enter(ctx, ProcessCall{Plane: o.plane, Op: "ordered.due", Name: namespace})
	if err != nil {
		return storage.DuePage{}, err
	}
	page, err := o.OrderedIndex.ListDue(ctx, namespace, dueAtOrBefore, after, limit)
	o.process.leave(call, err)
	return page, err
}

// Process reports this Host's durable-plane view, nil unless it is mortal.
func (h *PooledHost) Process() *HostProcess { return h.process }

// Kill is this Host's PROCESS DYING: nothing is drained, released, checkpointed
// or tombstoned. Its durable plane is cut and its leases lapse (see
// HostProcess), and its listener and every accepted connection close, so no
// Factory can reach it. The in-memory corpse is left running; it can no longer
// write anything durable.
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

// Partition closes this Host's listener and severs every accepted connection,
// so no Factory can reach it, and nothing else. A paused Host is partitioned as
// well: a stopped process answers no HostLink.
func (h *PooledHost) Partition() {
	_ = h.tracker.Close()
	h.tracker.Sever()
}
