//go:build integration

// Package orchestrationtest is the black-box service test kit for the Factory
// and Host orchestration program.
//
// It composes real `github.com/looprig/factory` and `github.com/looprig/host`
// objects at their RELEASED pins -- `host v0.1.0` and `factory v0.1.0` -- and
// is verified standalone, `GOWORK=off`, exactly as a consumer would get them.
// Every file carries the module's ordinary `integration` constraint and nothing
// more; runbook 07 I0.1 half (a)'s extra `orchestration` tag and the untagged
// doc file that shadowed it are gone. As with every other integration file,
// naming this package without `-tags integration` reports "build constraints
// exclude all Go files"; `./...` patterns and every Makefile target are unaffected.
//
// DO NOT RUN `go mod tidy` TO MOVE A PIN IN THIS MODULE.
//
// Measured during half (a): with a dependency the module could not name at a
// release, tidy EXITED 0 and silently rewrote `go.mod`/`go.sum` to
// pseudo-versions. A command that goes green and moves your pins is the
// dangerous shape. Move a pin with `go get module@version`, then confirm
// `GOWORK=off go mod tidy -diff` (read-only; what `make mod-check` runs) is
// empty. Never add a `replace`.
package orchestrationtest

import (
	"context"

	"sort"
	"sync"
	"time"
)

// Clock is the kit's deterministic time seam.
//
// It satisfies BOTH service clock interfaces, which are different shapes:
// factory.Clock is {Now, AfterFunc} and host.Clock is {Now, NewTimer}. One
// object behind both is what makes "the time Factory sees is the time Host
// sees" true by construction rather than by two fixtures being advanced in
// step by hand.
//
// NewTimer returns a REAL *time.Timer armed far in the future and Reset(0)-ed
// when the virtual deadline passes, rather than a &time.Timer{C: ch} literal.
// The literal is the usual trick and it is wrong here: Stop and Reset on a
// Timer built that way panic with "time: Stop called on uninitialized Timer",
// so a consumer that stops its timer -- which is the normal, correct thing to
// do -- would crash inside the kit. A crash is not an assertion failure, and a
// kit that converts a consumer's correct behaviour into a panic is a kit that
// cannot be trusted to report anything.
type Clock struct {
	mu     sync.Mutex
	now    time.Time
	nextID int64
	timers map[int64]*virtualTimer
}

type virtualTimer struct {
	deadline time.Time
	fire     func()
}

// NewClock returns a Clock pinned at start.
func NewClock(start time.Time) *Clock {
	return &Clock{now: start, timers: make(map[int64]*virtualTimer)}
}

// Now reports the virtual time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc registers f to run once the virtual clock reaches now+d.
//
// A non-positive d fires immediately and synchronously, which is what
// time.AfterFunc does, so a consumer cannot tell the kit apart by that case.
func (c *Clock) AfterFunc(d time.Duration, f func()) (stop func() bool) {
	c.mu.Lock()
	if d <= 0 {
		c.mu.Unlock()
		f()
		return func() bool { return false }
	}
	id := c.nextID
	c.nextID++
	c.timers[id] = &virtualTimer{deadline: c.now.Add(d), fire: f}
	c.mu.Unlock()
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		_, live := c.timers[id]
		delete(c.timers, id)
		return live
	}
}

// NewTimer satisfies host.Clock.
func (c *Clock) NewTimer(d time.Duration) *time.Timer {
	timer := time.NewTimer(oneCentury)
	c.AfterFunc(d, func() { timer.Reset(0) })
	return timer
}

const oneCentury = 100 * 365 * 24 * time.Hour

// Pending reports how many registered timers have not yet fired.
func (c *Clock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

// Advance moves the virtual clock forward and fires every timer whose deadline
// it passes, in deadline order, so a timer registered by another timer's
// callback within the same window still fires within that window.
func (c *Clock) Advance(d time.Duration) int {
	fired := 0
	target := c.Now().Add(d)
	for {
		c.mu.Lock()
		due := make([]int64, 0, len(c.timers))
		for id, t := range c.timers {
			if !t.deadline.After(target) {
				due = append(due, id)
			}
		}
		if len(due) == 0 {
			c.now = target
			c.mu.Unlock()
			return fired
		}
		sort.Slice(due, func(i, j int) bool {
			return c.timers[due[i]].deadline.Before(c.timers[due[j]].deadline)
		})
		id := due[0]
		t := c.timers[id]
		delete(c.timers, id)
		c.now = t.deadline
		c.mu.Unlock()
		t.fire()
		fired++
	}
}

// AdvanceFiring advances by d and FAILS if it did not fire exactly want timers.
//
// This is the loud-failure half of the clock. A case written as "advance past
// the deadline and observe the effect" silently stops exercising its deadline
// the moment the code under test stops registering a timer -- the advance still
// succeeds, the effect is still observed for some other reason, and the case
// has become a tautology. Naming the count turns that into a failure.
func (c *Clock) AdvanceFiring(tb TB, d time.Duration, want int) {
	tb.Helper()
	if got := c.Advance(d); got != want {
		tb.Fatalf("orchestrationtest: advancing %s fired %d timers, want exactly %d", d, got, want)
	}
}

// Signal is a waitable one-shot state transition. The kit uses these instead of
// sleeps: runbook 07 I0.1 step 2 requires every fake to expose waitable
// transitions, and a sleep is an unfalsifiable assertion about a machine's load.
type Signal struct {
	once sync.Once
	ch   chan struct{}
}

// NewSignal returns an unfired Signal.
func NewSignal() *Signal { return &Signal{ch: make(chan struct{})} }

// Fire releases every waiter. It is idempotent.
func (s *Signal) Fire() { s.once.Do(func() { close(s.ch) }) }

// Fired reports whether Fire has been called.
func (s *Signal) Fired() bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

// Await blocks until the Signal fires or ctx ends, and fails on ctx.
func (s *Signal) Await(tb TB, ctx context.Context) {
	tb.Helper()
	select {
	case <-s.ch:
	case <-ctx.Done():
		tb.Fatalf("orchestrationtest: waiting for signal: %v", ctx.Err())
	}
}

// TB is the subset of testing.TB the kit's assertions use.
//
// It is an interface rather than *testing.T so the kit's own POSITIVE CONTROLS
// can hand an assertion a recording TB and prove the assertion reports a
// failure. An assertion that has never been observed to fail is not evidence.
type TB interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Cleanup(func())
	Logf(format string, args ...any)
	Name() string
}
