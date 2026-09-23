//go:build integration && kind

// D3.1 (runbook 07) dedicated RELEASE in a disposable namespace on a real
// cluster: step 4 (release ordering before deletion; the durable session
// readable and restorable after the Pod is gone), the termination finalizer,
// the SIGTERM failure backstop inside terminationGracePeriodSeconds, and step
// 3's "restart the controller during delete". The harness is
// dedicated_kind_lane_test.go.
//
// Runbook 07 D2.2 (amended 2026-09-18/19) fixes the order this file observes:
// deletion desire -> hostlink.drain -> drained for this workload -> the Kind
// decided and persisted on the Pod -> ClearHostRegistration(epoch) -> ONE
// UID-preconditioned delete -> RecordPlacementTermination -> finalizer
// released. "Checkpoint pointer at current epoch" is unimplementable for Host
// sessions (amended) and is not asserted.

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	controllerk8s "github.com/looprig/controller/kubernetes"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/kindlane"
	"github.com/looprig/tests/internal/orchestrationtest"
)

func TestDedicatedDrainInDisposableNamespace(t *testing.T) {
	ctx := kindContext(t)
	var (
		released   = sessionwire.SessionID("d31-released")
		evicted    = sessionwire.SessionID("d31-evicted")
		restarting = sessionwire.SessionID("d31-restarting")
	)
	lane := newKindLane(t, ctx, []sessionwire.SessionID{released, evicted, restarting}, 1)

	t.Run("D1 deletion desire drains, fences, deletes once and records graceful, in that order", func(t *testing.T) {
		lane.t = t
		lane.Create(ctx, released, "d31-released-create", "a session to release")
		lane.AwaitApplied(ctx, released, "d31-released-create", 4*time.Minute)
		pod := lane.onlyPod(released)
		owner, err := lane.Owner(ctx, released)
		if err != nil || owner.Registration.Route == nil {
			t.Fatalf("no live registration before release: %+v %v", owner, err)
		}
		watch := lane.watchTeardown(ctx, released, pod)

		lane.writeDeletionDesire(ctx, released)
		desired := time.Now()
		lane.WaitFor("the pod is gone", 3*time.Minute, func() bool { return watch.seen("pod gone") })
		lane.WaitFor("the termination is recorded", time.Minute, func() bool { return watch.seen("termination recorded") })
		watch.stop()
		timeline := watch.timeline(desired)
		lane.Log("teardown timeline (from deletion desire):\n%s", timeline)

		// The registry route is RELEASED BY THE HOST when its drain completes
		// (before the controller's decision); the controller's fence then
		// meets a released record. What must hold is that no route names the
		// Pod when it is deleted.
		watch.requireOrder(t, "drain annotation", "termination decision annotation", "deletionTimestamp", "termination recorded", "pod gone")
		watch.requireOrder(t, "drain annotation", "registry released", "deletionTimestamp")
		if !strings.Contains(watch.decision, "graceful") {
			t.Fatalf("persisted decision %q, want graceful", watch.decision)
		}
		if n := watch.deletions(); n != 1 {
			t.Fatalf("observed %d deletionTimestamp transitions, want one", n)
		}
		if watch.seen("replacement pod (new uid) under the same name") {
			t.Fatal("a Pod was recreated under the released workload's name")
		}
		termination := watch.termination
		if termination.Kind != sessionstore.PlacementTerminationGraceful || termination.Generation != 1 ||
			termination.LeaseEpoch != owner.Registration.LeaseEpoch {
			t.Fatalf("termination %+v, want graceful at generation 1, epoch %d", termination, owner.Registration.LeaseEpoch)
		}
		lane.Log("termination record: kind=%s generation=%d lease_epoch=%d recorded_at=%s", termination.Kind,
			termination.Generation, termination.LeaseEpoch, termination.RecordedAt.Format(time.RFC3339Nano))
	})

	t.Run("D2 the durable session stays readable after its Pod is gone and restores onto a new Host", func(t *testing.T) {
		lane.t = t
		if pods := lane.SessionPods(released); len(pods) != 0 {
			t.Fatalf("session %s still has pods %v", released, podUIDs(pods))
		}
		for _, path := range []string{"/v1/sessions/" + string(released) + "/status", "/v1/sessions/" + string(released) + "/journal"} {
			status, body := lane.Get(ctx, path)
			if status != http.StatusOK {
				t.Fatalf("GET %s after the Pod is gone: %d %s", path, status, body)
			}
			lane.Log("GET %s -> %d %s", path, status, truncate(body, 240))
		}
		entry, err := lane.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: kindlane.Tenant, SessionID: released})
		if err != nil {
			t.Fatal(err)
		}
		runtimeID := entry.Record.Binding.RuntimeSessionID

		status, body := lane.Post(ctx, "/v1/sessions/"+string(released)+"/restore", sessionwire.RestoreRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("d31-released-restore"), SessionID: released,
		})
		if status != http.StatusOK && status != http.StatusAccepted {
			t.Fatalf("restore answered %d: %s", status, body)
		}
		// FINDING (factory v0.7.1 + controller v0.2.0): Factory's pending
		// placement hands EnsureWorkload the STORED intent, and after release
		// that intent is the deletion desire's zero workload, which the adapter
		// refuses (unsupported payload version). Nothing in Factory writes the
		// launch template's workload back, so an admitted restore of a released
		// dedicated session is never placed. Observed for a bounded window, then
		// the desire is re-expressed the way Factory's create path writes it.
		time.Sleep(30 * time.Second)
		if state := lane.CommandState(ctx, released, "d31-released-restore"); state == sessionstore.InboxStateApplied {
			lane.Log("Factory re-placed the released session on its own (the gap is closed)")
		} else {
			lane.Log("30s after admission the restore is %q and the session has %d pods: Factory did not re-place a released dedicated session", state, len(lane.SessionPods(released)))
			lane.writeTemplateDesire(ctx, released)
		}
		took := lane.AwaitApplied(ctx, released, "d31-released-restore", 5*time.Minute)
		lane.Input(ctx, released, "d31-released-after", "after the restore")
		lane.AwaitApplied(ctx, released, "d31-released-after", 3*time.Minute)
		pod := lane.onlyPod(released)
		after, err := lane.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: kindlane.Tenant, SessionID: released})
		if err != nil {
			t.Fatal(err)
		}
		if after.Record.Binding.RuntimeSessionID != runtimeID {
			t.Fatalf("runtime session id changed %s -> %s", runtimeID, after.Record.Binding.RuntimeSessionID)
		}
		lane.Log("restore applied %s after admission on pod %s (generation label %s, desired generation %d); same runtime session %s; a later input applied",
			took.Round(time.Millisecond), pod.Metadata.Name, pod.Metadata.Labels[controllerk8s.LabelGeneration], after.Record.DesiredGeneration, runtimeID)
	})

	t.Run("D3 a platform deletion is held by the finalizer, drained by SIGTERM inside the grace period, and recorded forced", func(t *testing.T) {
		lane.t = t
		lane.Create(ctx, evicted, "d31-evicted-create", "a session whose pod is deleted out from under it")
		lane.AwaitApplied(ctx, evicted, "d31-evicted-create", 4*time.Minute)
		pod := lane.onlyPod(evicted)
		watch := lane.watchTeardown(ctx, evicted, pod)

		// As the cluster ADMIN, not the controller: a platform deletion.
		lane.Kubectl("-n", lane.Namespace, "delete", "pod", pod.Metadata.Name, "--wait=false")
		deleted := time.Now()
		lane.WaitFor("the termination is recorded", 3*time.Minute, func() bool { return watch.seen("termination recorded") })
		lane.WaitFor("the original pod object is gone", 2*time.Minute, func() bool { return watch.seen("pod gone") })
		watch.stop()
		lane.Log("platform-deletion timeline (from kubectl delete):\n%s", watch.timeline(deleted))

		if !watch.seen("terminating with finalizer") {
			t.Fatal("never observed the Pod Terminating while the finalizer held it")
		}
		watch.requireOrder(t, "deletionTimestamp", "container terminated", "termination recorded", "pod gone")
		if watch.termination.Kind != sessionstore.PlacementTerminationForced || watch.termination.ForcedReason != sessionstore.PlacementForcedPlatformDeleted {
			t.Fatalf("termination %+v, want forced platform_deleted", watch.termination)
		}
		grace := time.Duration(controllerk8s.TerminationGraceSeconds(kindDrainCeiling, kindCommitMargin)) * time.Second
		if watch.exitCode != 0 {
			t.Fatalf("the Host exited %d after SIGTERM, want a clean drain", watch.exitCode)
		}
		// The Host's own SIGTERM-to-exit measurement, from its termination
		// message (the API's timestamps are whole seconds).
		var signalToExit int64
		if _, err := fmt.Sscanf(watch.exitMessage, "signal_to_exit_ms=%d", &signalToExit); err != nil {
			t.Fatalf("the Host's termination message %q carries no signal_to_exit_ms", watch.exitMessage)
		}
		exit := time.Duration(signalToExit) * time.Millisecond
		if exit >= grace {
			t.Fatalf("the Host took %s from SIGTERM to exit, not inside the %s grace period", exit, grace)
		}
		lane.Log("SIGTERM->exit %s measured by the Host (grace %s = drain ceiling %s + commit margin %s; HOST_DRAIN_GRACE %s): exit code %d, termination message %q; unused grace %s",
			exit, grace, kindDrainCeiling, kindCommitMargin, kindHostDrainGrace, watch.exitCode, watch.exitMessage, grace-exit)
		// Desire still names generation 1: record what the controller does next.
		time.Sleep(10 * time.Second)
		lane.Log("after the forced termination the session has pods %v", podUIDs(lane.SessionPods(evicted)))
	})

	t.Run("D4 a controller restarted during delete still drains, deletes once and records once", func(t *testing.T) {
		lane.t = t
		lane.Create(ctx, restarting, "d31-restarting-create", "a session released across a controller restart")
		lane.AwaitApplied(ctx, restarting, "d31-restarting-create", 4*time.Minute)
		pod := lane.onlyPod(restarting)
		watch := lane.watchTeardown(ctx, restarting, pod)
		lane.writeDeletionDesire(ctx, restarting)
		desired := time.Now()
		lane.WaitFor("the drain has begun", time.Minute, func() bool { return watch.seen("drain annotation") })
		lane.Kubectl("-n", lane.Namespace, "delete", "pod", "-l", "app.kubernetes.io/name=looprig-control", "--wait=false")
		lane.Log("deleted the control replica %s after the drain annotation", time.Since(desired).Round(time.Millisecond))
		lane.WaitFor("the pod is gone", 4*time.Minute, func() bool { return watch.seen("pod gone") })
		lane.WaitFor("the termination is recorded", time.Minute, func() bool { return watch.seen("termination recorded") })
		watch.stop()
		lane.Log("teardown across a controller restart:\n%s", watch.timeline(desired))
		watch.requireOrder(t, "drain annotation", "termination decision annotation", "deletionTimestamp", "termination recorded", "pod gone")
		watch.requireOrder(t, "drain annotation", "registry released", "deletionTimestamp")
		if n := watch.deletions(); n != 1 {
			t.Fatalf("%d deletions, want one", n)
		}
		if watch.termination.Generation != 1 {
			t.Fatalf("termination %+v, want generation 1", watch.termination)
		}
		lane.Log("termination after restart: kind=%s reason=%q generation=%d", watch.termination.Kind, watch.termination.ForcedReason, watch.termination.Generation)
	})
}

func (l *kindLane) onlyPod(s sessionwire.SessionID) kindPod {
	l.t.Helper()
	var pods []kindPod
	l.WaitFor("exactly one pod for "+string(s), time.Minute, func() bool {
		pods = l.SessionPods(s)
		return len(pods) == 1
	})
	return pods[0]
}

// writeDeletionDesire writes deletion desire the way Factory writes desired
// state: a dedicated placement naming a ZERO workload.
func (l *kindLane) writeDeletionDesire(ctx context.Context, s sessionwire.SessionID) {
	l.t.Helper()
	entry, err := l.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: kindlane.Tenant, SessionID: s})
	if err != nil {
		l.t.Fatal(err)
	}
	updated, err := l.Store.UpdateCatalogDesiredState(ctx, sessionstore.UpdateCatalogDesiredStateRequest{
		TenantID: kindlane.Tenant, SessionID: s, ExpectedRevision: entry.Revision,
		IdempotencyKey: "d31-delete-" + string(s), DesiredPlacement: sessionwire.HostPlacementDedicated,
		RuntimeCompatibilityID: string(orchestrationtest.PooledCompatibility),
	})
	if err != nil {
		l.t.Fatalf("writing deletion desire for %s: %v", s, err)
	}
	l.Log("deletion desire written for %s: desired generation %d -> %d, zero workload", s, entry.Record.DesiredGeneration, updated.Record.DesiredGeneration)
}

// writeTemplateDesire re-expresses dedicated desire with the launch template's
// workload, exactly the bytes Factory's create path stores.
func (l *kindLane) writeTemplateDesire(ctx context.Context, s sessionwire.SessionID) {
	l.t.Helper()
	entry, err := l.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: kindlane.Tenant, SessionID: s})
	if err != nil {
		l.t.Fatal(err)
	}
	payload, err := json.Marshal(l.payload())
	if err != nil {
		l.t.Fatal(err)
	}
	updated, err := l.Store.UpdateCatalogDesiredState(ctx, sessionstore.UpdateCatalogDesiredStateRequest{
		TenantID: kindlane.Tenant, SessionID: s, ExpectedRevision: entry.Revision,
		IdempotencyKey: "d31-redesire-" + string(s), DesiredPlacement: sessionwire.HostPlacementDedicated,
		RuntimeCompatibilityID: string(orchestrationtest.PooledCompatibility),
		DesiredWorkload:        sessionstore.DesiredWorkload{PayloadVersion: controllerk8s.PayloadVersionV1, Payload: payload},
	})
	if err != nil {
		l.t.Fatalf("re-expressing dedicated desire for %s: %v", s, err)
	}
	l.Log("dedicated desire re-expressed for %s: desired generation %d -> %d with the template workload", s, entry.Record.DesiredGeneration, updated.Record.DesiredGeneration)
}

// teardownWatch polls the Pod object, the Host registry and the termination
// record every 200ms and records the first time each transition is seen.
type teardownWatch struct {
	mu                sync.Mutex
	first             map[string]time.Time
	order             []string
	decision          string
	termination       sessionstore.PlacementTermination
	deletionTimestamp *time.Time
	containerFinished time.Time
	exitCode          int
	exitMessage       string
	uid               string
	deletionMarks     int
	done              chan struct{}
	wg                sync.WaitGroup
}

func (l *kindLane) watchTeardown(ctx context.Context, s sessionwire.SessionID, pod kindPod) *teardownWatch {
	name := pod.Metadata.Name
	// The termination row is keyed by the WORKLOAD's generation -- never the
	// deletion desire's (sessionstore v0.11.0).
	generation := mustUint(l.t, pod.Metadata.Labels[controllerk8s.LabelGeneration])
	w := &teardownWatch{first: map[string]time.Time{}, done: make(chan struct{})}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		var lastDeleting bool
		for {
			select {
			case <-w.done:
				return
			case <-time.After(200 * time.Millisecond):
			}
			now := time.Now()
			out, err := exec.Command("kubectl", "--context", l.kubeContext, "-n", l.Namespace, "get", "pod", name, "-o", "json").CombinedOutput()
			switch {
			case err != nil && strings.Contains(string(out), "NotFound"):
				w.mark("pod gone", now)
			case err == nil:
				var pod kindPod
				if decodeErr := jsonUnmarshal(out, &pod); decodeErr == nil {
					w.observePod(pod, now, &lastDeleting)
				}
			}
			reg, err := l.Store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{TenantID: kindlane.Tenant, SessionID: s})
			var registryErr *sessionstore.RegistryError
			if (err == nil && reg.Registration.Route == nil) || (errors.As(err, &registryErr) && registryErr.Code == sessionstore.RegistryErrorReleased) {
				w.mark("registry released", now)
			}
			if term, err := l.Store.GetPlacementTermination(ctx, sessionstore.GetPlacementTerminationRequest{TenantID: kindlane.Tenant, SessionID: s, Generation: generation}); err == nil && term.Termination.Generation > 0 {
				w.mu.Lock()
				w.termination = term.Termination
				w.mu.Unlock()
				w.mark("termination recorded", now)
			}
		}
	}()
	return w
}

func (w *teardownWatch) observePod(pod kindPod, now time.Time, lastDeleting *bool) {
	w.mu.Lock()
	if w.uid == "" {
		w.uid = pod.Metadata.UID
	}
	replaced := w.uid != pod.Metadata.UID
	w.mu.Unlock()
	if replaced {
		// The watched object is gone and the name now belongs to a NEW Pod
		// (the controller recreated the generation). Nothing about the new
		// object is attributed to the old one's teardown.
		w.mark("pod gone", now)
		w.mark("replacement pod (new uid) under the same name", now)
		return
	}
	if _, ok := pod.Metadata.Annotations[controllerk8s.AnnotationDrain]; ok {
		w.mark("drain annotation", now)
	}
	if decision, ok := pod.Metadata.Annotations[controllerk8s.AnnotationTermination]; ok {
		w.mu.Lock()
		w.decision = decision
		w.mu.Unlock()
		w.mark("termination decision annotation", now)
	}
	deleting := pod.Metadata.DeletionTimestamp != nil
	if deleting {
		w.mu.Lock()
		if !*lastDeleting {
			w.deletionMarks++
		}
		w.deletionTimestamp = pod.Metadata.DeletionTimestamp
		w.mu.Unlock()
		w.mark("deletionTimestamp", now)
		for _, f := range pod.Metadata.Finalizers {
			if f == controllerk8s.Finalizer {
				w.mark("terminating with finalizer", now)
			}
		}
	}
	*lastDeleting = deleting
	for _, status := range pod.Status.ContainerStatuses {
		if term := status.State.Terminated; term != nil {
			w.mu.Lock()
			w.containerFinished, w.exitCode, w.exitMessage = term.FinishedAt, term.ExitCode, term.Message
			w.mu.Unlock()
			w.mark("container terminated", now)
		}
	}
}

func (w *teardownWatch) mark(what string, at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.first[what]; !ok {
		w.first[what] = at
		w.order = append(w.order, what)
	}
}

func (w *teardownWatch) seen(what string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.first[what]
	return ok
}

func (w *teardownWatch) deletions() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deletionMarks
}

func (w *teardownWatch) stop() {
	close(w.done)
	w.wg.Wait()
}

func (w *teardownWatch) timeline(from time.Time) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var b strings.Builder
	for _, what := range w.order {
		b.WriteString("  +" + w.first[what].Sub(from).Round(10*time.Millisecond).String() + "  " + what + "\n")
	}
	if w.decision != "" {
		b.WriteString("  decision annotation: " + w.decision + "\n")
	}
	return b.String()
}

// requireOrder asserts each named transition was first seen no later than the
// next. Equal times are allowed: the watch polls every 200ms.
func (w *teardownWatch) requireOrder(t *testing.T, sequence ...string) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, what := range sequence {
		at, ok := w.first[what]
		if !ok {
			t.Fatalf("never observed %q (saw %v)", what, w.order)
		}
		if i > 0 {
			if prev := w.first[sequence[i-1]]; at.Before(prev) {
				t.Fatalf("%q was first seen %s BEFORE %q", what, prev.Sub(at), sequence[i-1])
			}
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
