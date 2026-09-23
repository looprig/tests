//go:build integration

package orchestrationtest

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// TestOrchestrationTestKitHostProcess is the HostProcess fixture's own proof:
// every lifecycle case leans on it, and a fixture that silently let a dead
// process write, or refused a paused one's writes itself, would vouch for the
// very fencing the cases claim the stores provide.
func TestOrchestrationTestKitHostProcess(t *testing.T) {
	ctx := context.Background()

	t.Run("the view preserves the provider's bounded reader lifecycle", func(t *testing.T) {
		inner := memstore.New()
		if _, ok := inner.Blobs.(storage.BlobReaderLifecycle); !ok {
			t.Fatal("memstore no longer implements BlobReaderLifecycle; this row is vacuous")
		}
		view := (&HostProcess{}).View(t, PlaneStore, inner)
		if _, ok := view.Blobs.(storage.BlobReaderLifecycle); !ok {
			t.Fatal("the process view hides the provider's BlobReaderLifecycle, so sessionstore would refuse it")
		}
	})

	t.Run("death: every call fails, leases lapse observed, a successor gets a higher epoch", func(t *testing.T) {
		inner := memstore.New()
		process := &HostProcess{}
		view := process.View(t, PlaneStore, inner)
		lease, err := view.Leaser.Acquire(ctx, "session")
		if err != nil {
			t.Fatal(err)
		}
		if err := view.Ledger.Append(ctx, "log", 0, []byte("alive")); err != nil {
			t.Fatalf("a live process's append failed: %v", err)
		}
		if lapsed := process.kill(); lapsed != 1 {
			t.Fatalf("kill lapsed %d leases, want 1", lapsed)
		}
		select {
		case <-lease.Lost():
		default:
			t.Fatal("a dead process's lease did not report Lost")
		}
		if err := view.Ledger.Append(ctx, "log", 1, []byte("corpse")); !errors.Is(err, ErrHostProcessDead) {
			t.Fatalf("a dead process's append answered %v, want ErrHostProcessDead", err)
		}
		if _, _, err := view.KV.Get(ctx, "k"); !errors.Is(err, ErrHostProcessDead) {
			t.Fatalf("a dead process's KV read answered %v", err)
		}
		if _, err := view.Leaser.Acquire(ctx, "other"); !errors.Is(err, ErrHostProcessDead) {
			t.Fatalf("a dead process acquired a lease: %v", err)
		}
		successor, err := inner.Leaser.Acquire(ctx, "session")
		if err != nil {
			t.Fatalf("a successor could not take the dead process's lease: %v", err)
		}
		if successor.Epoch() <= lease.Epoch() {
			t.Fatalf("the successor's epoch %d is not above the dead holder's %d", successor.Epoch(), lease.Epoch())
		}
		if tip, _ := inner.Ledger.Tip(ctx, "log"); tip != 1 {
			t.Fatalf("the ledger tip is %d, want only the live append", tip)
		}
	})

	t.Run("pause: calls block, leases lapse unobserved, resumed writes reach the real store", func(t *testing.T) {
		inner := memstore.New()
		process := &HostProcess{}
		view := process.View(t, PlaneJournal, inner)
		lease, err := view.Leaser.Acquire(ctx, "session")
		if err != nil {
			t.Fatal(err)
		}
		if err := view.Ledger.Append(ctx, "log", 0, []byte("one")); err != nil {
			t.Fatal(err)
		}
		// A write IN FLIGHT at the pause, caught by a hold.
		hold := process.Hold("in flight", func(c ProcessCall) bool {
			return c.Op == "ledger.append" && bytes.Equal(c.Payload, []byte("stale-in-flight"))
		})
		inFlight := make(chan error, 1)
		go func() { inFlight <- view.Ledger.Append(ctx, "log", 1, []byte("stale-in-flight")) }()
		select {
		case <-hold.Caught():
		case <-time.After(5 * time.Second):
			t.Fatal("the hold never caught the in-flight append")
		}
		process.Record()
		if lapsed := process.Pause(); lapsed != 1 {
			t.Fatalf("pause lapsed %d leases, want 1", lapsed)
		}
		select {
		case <-lease.Lost():
			t.Fatal("a PAUSED process was told its lease lapsed; a stopped process cannot observe that")
		default:
		}
		// A call made while paused blocks -- and its context expiring does not
		// stop it being delivered.
		shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		paused := make(chan error, 1)
		go func() { paused <- view.Ledger.Append(shortCtx, "log", 1, []byte("stale-paused")) }()
		select {
		case err := <-paused:
			t.Fatalf("a paused process's call returned (%v) instead of blocking", err)
		case <-time.After(200 * time.Millisecond):
		}
		// The successor takes over and writes.
		successor, err := inner.Leaser.Acquire(ctx, "session")
		if err != nil || successor.Epoch() <= lease.Epoch() {
			t.Fatalf("the successor's acquire: epoch %v err %v", successor, err)
		}
		if err := inner.Ledger.Append(ctx, "log", 1, []byte("successor")); err != nil {
			t.Fatal(err)
		}
		process.Resume()
		for name, ch := range map[string]chan error{"in-flight": inFlight, "paused": paused} {
			select {
			case err := <-ch:
				var conflict *storage.ConflictError
				if !errors.As(err, &conflict) {
					t.Fatalf("the resumed %s append answered %v, want the REAL ledger's conflict", name, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("the resumed %s append never returned", name)
			}
		}
		calls := process.Calls()
		if len(calls) != 2 {
			t.Fatalf("the view recorded %d calls, want the held and the paused append: %+v", len(calls), calls)
		}
		for _, call := range calls {
			if !call.Done || call.Err == nil || !(call.Held || call.Paused) {
				t.Fatalf("recorded call %+v, want a done, refused, held-or-paused write", call)
			}
		}
		// And the view refuses nothing itself: a resumed write the store would
		// accept is accepted.
		if err := view.Ledger.Append(ctx, "log", 2, []byte("after")); err != nil {
			t.Fatalf("a resumed process's valid append was refused: %v", err)
		}
	})
}
