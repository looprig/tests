//go:build integration

package orchestrationtest

import (
	"context"
	"fmt"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/host"
	"github.com/looprig/sessionstore"
)

// This file is the kit's POOLED DRAIN seam, runbook 07 I2.3.
//
// # Who drains a pooled Host
//
// Not Factory: factory v0.8.0's only drain path is WorkloadController's
// RequestDrain, a dedicated-placement seam. Not the controller that implements
// it: it drains only the dedicated Pods it owns. And not HostLink: host v0.7.1's drain
// resolver refuses a WHOLE-HOST scope off the wire, because every link is
// authenticated for one tenant, and a pooled Host holds no fixed session a
// tenant could name. The one drain caller a pooled Host has is its OWN PROCESS
// LIFECYCLE -- host.Run's SIGTERM path, which is Service.Stop under the
// platform grace. So that is what these cases drive, and they drive the exported
// Service.Stop rather than a signal, because Run's only addition is the signal
// and the grace deadline around the same call.

// BeginStop starts this Host's Stop -- its drain -- WITHOUT waiting for it, and
// returns a channel that yields Stop's report once it has returned. Stop is
// idempotent, so a later Stop or the case's cleanup waits for this one.
//
// It exists because Stop is synchronous and the ORDERING claims of a drain are
// about the instants inside it: admission stopped and capacity unranked while
// sessions are still being released.
func (h *PooledHost) BeginStop() <-chan host.DrainReport {
	go h.stop()
	out := make(chan host.DrainReport, 1)
	go func() {
		<-h.stopped
		h.mu.Lock()
		report := h.report
		h.mu.Unlock()
		out <- report
	}()
	return out
}

// StopReport runs Stop to completion and reports what it returned.
func (h *PooledHost) StopReport(tb TB, within time.Duration) (host.DrainReport, time.Duration) {
	tb.Helper()
	began := time.Now()
	done := h.BeginStop()
	select {
	case report := <-done:
		h.mu.Lock()
		err := h.stopErr
		h.mu.Unlock()
		if err != nil {
			tb.Fatalf("orchestrationtest: host %s Stop refused: %v", h.ID, err)
		}
		return report, time.Since(began)
	case <-time.After(within):
		tb.Fatalf("orchestrationtest: host %s Stop did not return within %s", h.ID, within)
		return host.DrainReport{}, 0
	}
}

// HoldingCheckpointer holds EVERY checkpoint it is asked for until the case
// proceeds, and records who asked, in order. Unlike PausingCheckpointer, which
// holds only the first, it keeps a drain of several sessions parked at the one
// step (after BeginRelease and the idle wait, before the runtime release) where
// a case can observe what the Host has and has not done yet.
type HoldingCheckpointer struct {
	Inner host.Checkpointer

	// OnEnter, when set, runs as each checkpoint is entered and BEFORE it is
	// held, on the drain's own goroutine: the instant after the Host has
	// acknowledged its drain and marked the session releasing, and before it
	// has released anything. A case reads the durable plane there.
	OnEnter func(sessionwire.SessionID)

	proceed chan struct{}
	opened  sync.Once

	mu      sync.Mutex
	entered []sessionwire.SessionID
}

// NewHoldingCheckpointer returns a checkpointer that holds every call.
func NewHoldingCheckpointer() *HoldingCheckpointer {
	return &HoldingCheckpointer{proceed: make(chan struct{})}
}

// Wrap satisfies PooledHostConfig.WrapCheckpointer.
func (c *HoldingCheckpointer) Wrap(inner host.Checkpointer) host.Checkpointer {
	c.Inner = inner
	return c
}

// Checkpoint satisfies host.Checkpointer.
func (c *HoldingCheckpointer) Checkpoint(ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID) error {
	if c.OnEnter != nil {
		c.OnEnter(s)
	}
	c.mu.Lock()
	c.entered = append(c.entered, s)
	c.mu.Unlock()
	select {
	case <-c.proceed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return c.Inner.Checkpoint(ctx, tenant, s)
}

// Entered reports every session a checkpoint was asked for, in order.
func (c *HoldingCheckpointer) Entered() []sessionwire.SessionID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sessionwire.SessionID(nil), c.entered...)
}

// Proceed releases every held and every later checkpoint. It is idempotent.
func (c *HoldingCheckpointer) Proceed() { c.opened.Do(func() { close(c.proceed) }) }

// PooledCandidates pages the pooled target to exhaustion and reports every
// Host it offers, with its advertisement, exactly as Factory's placement read
// sees it.
func PooledCandidates(tb TB, ctx context.Context, world *PooledWorld) map[sessionwire.HostID]sessionwire.HostLinkCapacityReport {
	tb.Helper()
	found := map[sessionwire.HostID]sessionwire.HostLinkCapacityReport{}
	var cursor sessionwire.Cursor
	for pages := 0; ; pages++ {
		if pages > 16 {
			tb.Fatalf("orchestrationtest: the pooled target did not terminate after %d pages", pages)
			return found
		}
		page, err := world.Store.ListCompatibleHosts(ctx, sessionstore.ListCompatibleHostsRequest{
			Key: sessionstore.HostTargetKey{
				AgentID:                PooledAgent,
				RuntimeCompatibilityID: string(PooledCompatibility),
				Placement:              sessionwire.HostPlacementPooled,
			},
			Cursor: cursor,
			Limit:  8,
		})
		if err != nil {
			tb.Fatalf("orchestrationtest: listing the pooled target: %v", err)
			return found
		}
		for _, candidate := range page.Hosts {
			found[candidate.HostID] = candidate
		}
		if page.NextCursor == "" {
			return found
		}
		cursor = page.NextCursor
	}
}

// JournalTrace renders one runtime's harness journal as "seq:Type" entries, in
// order. It is for a failing case's log: the durable story of what the runtime
// did, read back the way a successor would read it.
func JournalTrace(tb TB, world *PooledWorld, tenant sessionwire.TenantID, id uuid.UUID) []string {
	tb.Helper()
	var trace []string
	walkJournal(tb, world, tenant, id, func(e event.Event, seq uint64) {
		trace = append(trace, fmt.Sprintf("%d:%T", seq, e))
	})
	return trace
}
