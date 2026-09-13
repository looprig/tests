//go:build integration && orchestration

package orchestrationtest

import (
	"testing"
	"time"
)

func TestClockIsDeterministicAndWaitable(t *testing.T) {
	c := NewClock(time.Unix(1_700_000_000, 0))
	if got := c.Now(); !got.Equal(time.Unix(1_700_000_000, 0)) {
		t.Fatalf("Now() = %v", got)
	}
	fired := make(chan struct{}, 1)
	stop := c.AfterFunc(5*time.Second, func() { fired <- struct{}{} })
	if stop == nil {
		t.Fatal("AfterFunc returned a nil stop")
	}
	if c.Pending() != 1 {
		t.Fatalf("Pending() = %d, want 1", c.Pending())
	}
	c.Advance(4 * time.Second)
	select {
	case <-fired:
		t.Fatal("timer fired before its deadline")
	default:
	}
	c.Advance(time.Second)
	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatal("timer did not fire at its deadline")
	}
	if c.Pending() != 0 {
		t.Fatalf("Pending() = %d after firing, want 0", c.Pending())
	}
}
