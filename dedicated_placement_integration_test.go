//go:build integration && kind

// D3.1 (runbook 07) dedicated PLACEMENT in a disposable namespace on a real
// cluster. The harness, and what is real, is dedicated_kind_lane_test.go.
//
// Steps covered here: 1 (disposable namespace, least-privilege ServiceAccount,
// run-labelled resources, exact cleanup), 2 (a dedicated session placed by two
// racing Factory reconcilers: one deterministic workload, one lease winner,
// ready registry, command application, no HPA), 3 (Factory/controller restart
// during create; the Host killed mid-command), and 5 (the Host rejects a
// non-fixed SessionID; no cross-tenant pooled credential enters the Pod).
// Step 4 (release ordering) is dedicated_drain_integration_test.go.

package tests

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	controllerhostlink "github.com/looprig/controller/hostlink"
	controllerk8s "github.com/looprig/controller/kubernetes"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/kindlane"
	"github.com/looprig/tests/internal/orchestrationtest"
)

func TestDedicatedPlacementInDisposableNamespace(t *testing.T) {
	ctx := kindContext(t)
	var (
		placed    = sessionwire.SessionID("d31-placed")
		restarted = sessionwire.SessionID("d31-restarted")
		killed    = sessionwire.SessionID("d31-killed")
	)
	// TWO control replicas: two Factories (each with its own pending sweep and
	// placement reconciler) and two controller drivers race every session.
	lane := newKindLane(t, ctx, []sessionwire.SessionID{placed, restarted, killed}, 2)

	var hostPod kindPod
	t.Run("P1 two racing Factory reconcilers place one deterministic workload and the first message applies", func(t *testing.T) {
		lane.t = t
		lane.Create(ctx, placed, "d31-placed-create", "first dedicated message")
		took := lane.AwaitApplied(ctx, placed, "d31-placed-create", 4*time.Minute)
		lane.Log("create settled applied %s after Factory admitted it", took.Round(time.Millisecond))

		pods := lane.SessionPods(placed)
		if len(pods) != 1 {
			t.Fatalf("session %s has %d Host Pods, want exactly one", placed, len(pods))
		}
		hostPod = pods[0]
		requireDedicatedPodShape(t, lane, hostPod, placed)

		owner, err := lane.Owner(ctx, placed)
		if err != nil {
			t.Fatalf("reading the Host registration: %v", err)
		}
		base := fmt.Sprintf("ws://%s.%s.%s.svc:%d", hostPod.Metadata.Name, kindlane.HostSubdomain, lane.Namespace, kindlane.HostPort)
		route := owner.Registration.Route
		if route == nil || string(route.HostID) != hostPod.Env("HOST_ID") || string(route.InternalEndpoint) != base ||
			owner.Registration.LeaseEpoch == 0 || route.Placement != sessionwire.HostPlacementDedicated {
			t.Fatalf("registry %+v route %+v, want the Pod's HOST_ID %q at base %q with a lease", owner.Registration, route, hostPod.Env("HOST_ID"), base)
		}
		lane.Log("pod %s uid=%s ready=%t; registry host_id=%s generation=%d lease_epoch=%d endpoint=%s residency=%s",
			hostPod.Metadata.Name, hostPod.Metadata.UID, hostPod.Ready(), route.HostID,
			route.HostGeneration, owner.Registration.LeaseEpoch, route.InternalEndpoint, route.Residency)

		// Both replicas keep reconciling; the workload must not churn and no
		// second lease holder may appear.
		time.Sleep(15 * time.Second)
		after := lane.SessionPods(placed)
		if len(after) != 1 || after[0].Metadata.UID != hostPod.Metadata.UID {
			t.Fatalf("after 15s of two racing reconcilers: %d pods (uid %v), want the one uid %s", len(after), podUIDs(after), hostPod.Metadata.UID)
		}
		again, err := lane.Owner(ctx, placed)
		if err != nil || again.Registration.Route == nil || again.Registration.Route.HostID != route.HostID || again.Registration.LeaseEpoch != owner.Registration.LeaseEpoch {
			t.Fatalf("lease moved without cause: %+v -> %+v (%v)", owner.Registration, again.Registration, err)
		}
		if out := lane.Kubectl("-n", lane.Namespace, "get", "hpa", "-o", "name"); strings.TrimSpace(out) != "" {
			t.Fatalf("an HPA exists in the namespace: %s", out)
		}
		lane.Log("no HPA in the namespace; one pod, one lease holder after two replicas raced for 15s")

		// Pod DNS through the headless Service, resolved from INSIDE the
		// cluster -- the name Factory attached over.
		fqdn := fmt.Sprintf("%s.%s.%s.svc.cluster.local", hostPod.Metadata.Name, kindlane.HostSubdomain, lane.Namespace)
		out, err := lane.TryKubectl(nil, "-n", lane.Namespace, "run", "d31-dns-probe", "--rm", "-i", "--restart=Never",
			"--image=busybox:1.36", "--labels="+kindRunLabel+"="+lane.RunID, "--", "nslookup", fqdn)
		if err != nil || !strings.Contains(out, hostPod.Status.PodIP) {
			t.Fatalf("in-cluster nslookup %s (want %s): %v\n%s", fqdn, hostPod.Status.PodIP, err, out)
		}
		lane.Log("in-cluster nslookup %s -> %s", fqdn, hostPod.Status.PodIP)
	})

	t.Run("P2 a later input applies on the same Host", func(t *testing.T) {
		lane.t = t
		lane.Input(ctx, placed, "d31-placed-input-1", "second dedicated message")
		took := lane.AwaitApplied(ctx, placed, "d31-placed-input-1", 2*time.Minute)
		lane.Log("input settled applied after %s", took.Round(time.Millisecond))
	})

	t.Run("P3 the Host refuses a session that is not its fixed session", func(t *testing.T) {
		lane.t = t
		if hostPod.Metadata.Name == "" {
			t.Skip("P1 did not place a Host")
		}
		gen := hostPod.Metadata.Labels[controllerk8s.LabelGeneration]
		attach := func(session sessionwire.SessionID, key string) (string, *sessionwire.HostLinkError, error) {
			return lane.probeAttach(ctx, hostPod, sessionwire.HostLinkAttachRequest{
				Version: sessionwire.CurrentWireVersion, TenantID: kindlane.Tenant,
				SessionID: session, HostID: sessionwire.HostID(hostPod.Env("HOST_ID")),
				HostGeneration: mustUint(t, gen), AgentID: orchestrationtest.PooledAgent,
				RuntimeCompatibilityID: string(orchestrationtest.PooledCompatibility),
				Mode:                   sessionwire.HostLinkAttachModeCreate, ActorID: "d31-probe", IdempotencyKey: key,
			})
		}
		// CONTROL: the identical request for the Host's OWN fixed session is a
		// well-formed attach the Host answers with a Core body (it is already
		// resident, so the answer is that residency).
		controlReply, controlRefusal, controlErr := attach(placed, "d31-probe-control")
		if controlErr != nil {
			t.Fatalf("the control attach for the fixed session failed at the transport: %v", controlErr)
		}
		lane.Log("control attach for the fixed session %s: refusal=%v reply=%s", placed, controlRefusal, controlReply)

		const foreign = sessionwire.SessionID("d31-not-the-fixed-session")
		reply, refusal, transportErr := attach(foreign, "d31-probe-foreign")
		switch {
		case refusal != nil:
			lane.Log("attach for a non-fixed session refused with Core code %s: %s", refusal.Code, reply)
		case transportErr != nil:
			// host v0.6.0: the residency manager refuses runtime_mismatch
			// WITHOUT the runtime_compatibility_id Core's HostLinkError
			// requires for that code, so the body cannot be marshalled and the
			// refusal reaches the wire as Centrifuge 107 "bad request". The
			// control above proves the request itself is well formed.
			lane.Log("attach for a non-fixed session refused at the transport: %v (Core body unpublishable; booked against host)", transportErr)
		default:
			t.Fatalf("a dedicated Host fixed to %s ACCEPTED an attach for %s: %s", placed, foreign, reply)
		}
		if _, err := lane.Owner(ctx, foreign); err == nil {
			t.Fatalf("the foreign session %s has a Host registration after the refused attach", foreign)
		}
		if _, err := lane.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: kindlane.Tenant, SessionID: foreign}); err == nil {
			t.Fatalf("the foreign session %s has a catalog entry", foreign)
		}
		lane.Log("no registration and no catalog entry exist for %s after the refusal", foreign)
	})

	t.Run("P4 the controller ServiceAccount is refused everything outside its Role", func(t *testing.T) {
		lane.t = t
		allowed := [][]string{
			{"create", "pods"}, {"delete", "pods"}, {"get", "pods"}, {"list", "pods"}, {"update", "pods"},
		}
		refused := [][]string{
			{"patch", "pods"}, {"watch", "pods"}, {"deletecollection", "pods"},
			{"create", "pods", "--subresource=exec"}, {"get", "pods", "--subresource=log"}, {"create", "pods", "--subresource=eviction"},
			{"get", "secrets"}, {"list", "secrets"}, {"create", "services"}, {"delete", "services"}, {"create", "deployments.apps"},
			{"create", "horizontalpodautoscalers.autoscaling"}, {"update", "pods", "--subresource=status"},
			{"list", "pods", "-n", "default"}, {"create", "pods", "-n", "kube-system"},
			{"create", "namespaces"}, {"create", "clusterroles.rbac.authorization.k8s.io"}, {"escalate", "roles.rbac.authorization.k8s.io"},
		}
		for _, verb := range allowed {
			out, err := lane.AsController(append([]string{"auth", "can-i", "-n", lane.Namespace}, verb...)...)
			if err != nil || strings.TrimSpace(out) != "yes" {
				t.Fatalf("can-i %v = %q (%v), want yes", verb, strings.TrimSpace(out), err)
			}
		}
		for _, verb := range refused {
			args := append([]string{"auth", "can-i"}, verb...)
			if !slices.Contains(verb, "-n") {
				args = append(args, "-n", lane.Namespace)
			}
			out, _ := lane.AsController(args...)
			// kubectl prefixes a warning for a cluster-scoped resource.
			if lastLine(out) != "no" {
				t.Fatalf("can-i %v = %q, want no", verb, strings.TrimSpace(out))
			}
		}
		// And real requests, answered by the API server's authorizer.
		for _, attempt := range [][]string{
			{"-n", lane.Namespace, "get", "secret", "d31-hostlink-auth"},
			{"-n", lane.Namespace, "get", "secret", "pooled-cross-tenant-credential"},
			{"-n", lane.Namespace, "delete", "service", "looprig-hosts"},
			{"-n", lane.Namespace, "patch", "pod", hostPod.Metadata.Name, "--type=merge", "-p", `{"metadata":{"labels":{"x":"y"}}}`},
			{"-n", "default", "get", "pods"},
			{"-n", lane.Namespace, "exec", hostPod.Metadata.Name, "--", "true"},
		} {
			out, err := lane.AsController(attempt...)
			if err == nil || !strings.Contains(out, "Forbidden") && !strings.Contains(out, "forbidden") {
				t.Fatalf("as the controller, kubectl %v = %v, want Forbidden:\n%s", attempt, err, out)
			}
			lane.Log("as controller: kubectl %s -> %s", strings.Join(attempt, " "), firstLine(out))
		}
		lane.Log("can-i yes: %v; can-i no: %d checks", allowed, len(refused))
	})

	t.Run("P5 Factory and controller restarted during create still yield one workload and the command applies", func(t *testing.T) {
		lane.t = t
		lane.Create(ctx, restarted, "d31-restarted-create", "created across a control-plane restart")
		// Kill EVERY control replica before the Host can have been placed.
		lane.Kubectl("-n", lane.Namespace, "delete", "pod", "-l", "app.kubernetes.io/name=looprig-control", "--wait=false")
		lane.Log("deleted both control replicas immediately after admission")
		took := lane.AwaitApplied(ctx, restarted, "d31-restarted-create", 5*time.Minute)
		lane.Kubectl("-n", lane.Namespace, "rollout", "status", "deployment/looprig-control", "--timeout=180s")
		time.Sleep(5 * time.Second)
		pods := lane.SessionPods(restarted)
		if len(pods) != 1 {
			t.Fatalf("after a restart during create: %d pods, want 1", len(pods))
		}
		lane.Log("applied %s after admission across the restart; one pod %s uid=%s", took.Round(time.Millisecond), pods[0].Metadata.Name, pods[0].Metadata.UID)
	})

	t.Run("P6 a Host killed mid-command is replaced, the command settles before its deadline and the session continues", func(t *testing.T) {
		lane.t = t
		lane.Create(ctx, killed, "d31-killed-create", "before the kill")
		lane.AwaitApplied(ctx, killed, "d31-killed-create", 4*time.Minute)
		pods := lane.SessionPods(killed)
		if len(pods) != 1 {
			t.Fatalf("%d pods before the kill", len(pods))
		}
		victim := pods[0]
		before, err := lane.Owner(ctx, killed)
		if err != nil {
			t.Fatal(err)
		}

		lane.Input(ctx, killed, "d31-killed-input", "in flight at the kill")
		lane.sigkillHost(victim)
		killedAt := time.Now()
		lane.Log("SIGKILLed host container of %s (uid %s) right after admitting d31-killed-input", victim.Metadata.Name, victim.Metadata.UID)

		var settled sessionstore.InboxState
		lane.WaitFor("the in-flight input reaches a terminal state", 4*time.Minute, func() bool {
			settled = lane.CommandState(ctx, killed, "d31-killed-input")
			return settled == sessionstore.InboxStateApplied || settled == sessionstore.InboxStateRejected
		})
		lane.Log("in-flight input settled %q %s after the kill", settled, time.Since(killedAt).Round(time.Second))

		lane.Input(ctx, killed, "d31-killed-after", "after the kill")
		lane.AwaitApplied(ctx, killed, "d31-killed-after", 4*time.Minute)
		after, err := lane.Owner(ctx, killed)
		if err != nil {
			t.Fatal(err)
		}
		if after.Registration.LeaseEpoch <= before.Registration.LeaseEpoch {
			t.Fatalf("lease epoch %d after takeover, want above %d", after.Registration.LeaseEpoch, before.Registration.LeaseEpoch)
		}
		now := lane.SessionPods(killed)
		if len(now) != 1 || now[0].Metadata.UID == victim.Metadata.UID {
			t.Fatalf("after the kill: pods %v, want one replacement (not %s)", podUIDs(now), victim.Metadata.UID)
		}
		termination, err := lane.Store.GetPlacementTermination(ctx, sessionstore.GetPlacementTerminationRequest{TenantID: kindlane.Tenant, SessionID: killed, Generation: 1})
		lane.Log("replacement pod %s uid=%s; lease epoch %d -> %d; termination record %+v (err %v)",
			now[0].Metadata.Name, now[0].Metadata.UID, before.Registration.LeaseEpoch, after.Registration.LeaseEpoch, termination.Termination, err)
	})
}

// requireDedicatedPodShape checks the adapter-rendered Pod in the real API
// server: strict ownership, the finalizer, the drain-sized grace, no API token,
// the fixed session, and only allowlisted credentials.
func requireDedicatedPodShape(t *testing.T, lane *kindLane, pod kindPod, session sessionwire.SessionID) {
	t.Helper()
	if !pod.Ready() {
		t.Fatalf("pod %s is not Ready", pod.Metadata.Name)
	}
	if !slices.Contains(pod.Metadata.Finalizers, controllerk8s.Finalizer) {
		t.Fatalf("pod finalizers %v lack %s", pod.Metadata.Finalizers, controllerk8s.Finalizer)
	}
	if len(pod.Metadata.OwnerReferences) != 0 {
		t.Fatalf("pod has owner references %v", pod.Metadata.OwnerReferences)
	}
	wantGrace := controllerk8s.TerminationGraceSeconds(kindDrainCeiling, kindCommitMargin)
	if pod.Spec.TerminationGracePeriodSeconds == nil || *pod.Spec.TerminationGracePeriodSeconds != wantGrace {
		t.Fatalf("terminationGracePeriodSeconds %v, want %d", pod.Spec.TerminationGracePeriodSeconds, wantGrace)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("the Host Pod mounts a ServiceAccount token")
	}
	for name, want := range map[string]string{
		"HOST_FIXED_SESSION_ID": string(session), "HOST_PLACEMENT": string(sessionwire.HostPlacementDedicated),
		"HOST_CAPACITY": "1", "HOST_ISOLATION_CLASS": string(sessionwire.HostIsolationClassTenantExclusive),
		"HOST_DRAIN_GRACE": kindHostDrainGrace,
	} {
		if got := pod.Env(name); got != want {
			t.Fatalf("%s=%q, want %q", name, got, want)
		}
	}
	var secrets []string
	for _, v := range pod.Spec.Volumes {
		if v.Secret != nil {
			secrets = append(secrets, v.Secret.SecretName)
		}
	}
	slices.Sort(secrets)
	if !slices.Equal(secrets, []string{"d31-hostlink-auth", "d31-session-store"}) {
		t.Fatalf("pod mounts secrets %v, want exactly the two allowlisted ones", secrets)
	}
	for _, c := range pod.Spec.Containers {
		if len(c.EnvFrom) != 0 {
			t.Fatalf("container has envFrom %v", c.EnvFrom)
		}
		for _, e := range c.Env {
			if e.ValueFrom != nil {
				t.Fatalf("env %s is sourced from %v", e.Name, e.ValueFrom)
			}
			if strings.Contains(e.Value, "token") || strings.Contains(e.Value, "pooled-cross-tenant") {
				t.Fatalf("env %s carries a credential-looking value", e.Name)
			}
		}
		if c.Image != lane.HostImage {
			t.Fatalf("image %s, want %s", c.Image, lane.HostImage)
		}
	}
	lane.Log("pod %s: finalizer=%v grace=%d automountSAToken=false ownerRefs=0 secrets=%v fixed_session=%s labels=%v",
		pod.Metadata.Name, pod.Metadata.Finalizers, *pod.Spec.TerminationGracePeriodSeconds, secrets, pod.Env("HOST_FIXED_SESSION_ID"), pod.Metadata.Labels)
}

// probeAttach port-forwards to a Host Pod and sends one hostlink.attach with
// Factory's credential, naming the Pod's DNS base as the address (so the
// derived tenant path is exactly what an in-cluster Factory dials) while the
// TCP connection goes through the forward. It returns the reply bytes and,
// when the Host refused, the decoded Core HostLinkError.
func (l *kindLane) probeAttach(ctx context.Context, pod kindPod, request sessionwire.HostLinkAttachRequest) (string, *sessionwire.HostLinkError, error) {
	l.t.Helper()
	local := l.portForward(ctx, pod.Metadata.Name, kindlane.HostPort)
	address, err := sessionwire.HostLinkEndpoint(sessionwire.InternalEndpoint(pod.Env("HOST_INTERNAL_ENDPOINT")), kindlane.Tenant)
	if err != nil {
		l.t.Fatal(err)
	}
	connect, err := controllerhostlink.ConnectRequest()
	if err != nil {
		l.t.Fatal(err)
	}
	connected := make(chan sessionwire.VersionNegotiationResponse, 1)
	failed := make(chan string, 1)
	client := centrifugego.NewJsonClient(string(address), centrifugego.Config{
		GetToken: func(centrifugego.ConnectionTokenEvent) (string, error) {
			return "factory-" + l.RunID + "-token", nil
		},
		Data:   connect,
		Header: http.Header{"Sec-WebSocket-Protocol": {controllerhostlink.Subprotocol}},
		Name:   "d31-probe",
		NetDialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, local)
		},
		HandshakeTimeout: 10 * time.Second,
		ReadTimeout:      10 * time.Second,
		LogLevel:         centrifugego.LogLevelNone,
	})
	defer client.Close()
	client.OnConnected(func(e centrifugego.ConnectedEvent) {
		reply, err := sessionwire.DecodeHostLinkConnectReply(e.Data)
		if err != nil {
			failed <- err.Error()
			return
		}
		connected <- reply
	})
	client.OnDisconnected(func(e centrifugego.DisconnectedEvent) {
		select {
		case failed <- fmt.Sprintf("disconnected %d %s", e.Code, e.Reason):
		default:
		}
	})
	if err := client.Connect(); err != nil {
		l.t.Fatal(err)
	}
	select {
	case negotiated := <-connected:
		l.Log("probe connected over the forward to %s; host advertises %v", address, negotiated.HostLinkMethods())
	case reason := <-failed:
		l.t.Fatalf("probe connect: %s", reason)
	case <-time.After(15 * time.Second):
		l.t.Fatal("probe connect timed out")
	}
	body, err := json.Marshal(request)
	if err != nil {
		l.t.Fatal(err)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	result, err := client.RPC(rpcCtx, sessionwire.HostLinkMethodAttach, body)
	if err != nil {
		return "", nil, err
	}
	var refusal sessionwire.HostLinkError
	if json.Unmarshal(result.Data, &refusal) == nil && refusal.Code != "" {
		return string(result.Data), &refusal, nil
	}
	return string(result.Data), nil, nil
}

var forwarding = regexp.MustCompile(`Forwarding from 127\.0\.0\.1:(\d+)`)

func (l *kindLane) portForward(ctx context.Context, pod string, port int) string {
	l.t.Helper()
	cmd := exec.CommandContext(ctx, "kubectl", "--context", l.kubeContext, "-n", l.Namespace, "port-forward", "pod/"+pod, fmt.Sprintf(":%d", port))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		l.t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		l.t.Fatal(err)
	}
	l.t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	found := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if m := forwarding.FindStringSubmatch(scanner.Text()); m != nil {
				found <- "127.0.0.1:" + m[1]
			}
		}
	}()
	select {
	case addr := <-found:
		return addr
	case <-time.After(30 * time.Second):
		l.t.Fatal("port-forward did not start")
		return ""
	}
}

// sigkillHost SIGKILLs the Host container through the node's container
// runtime -- a crash, not a Kubernetes deletion: no SIGTERM, no drain.
func (l *kindLane) sigkillHost(pod kindPod) {
	l.t.Helper()
	nodeOut := l.Kubectl("-n", l.Namespace, "get", "pod", pod.Metadata.Name, "-o", "jsonpath={.spec.nodeName}")
	node := strings.TrimSpace(nodeOut)
	idOut, err := exec.Command("docker", "exec", node, "crictl", "ps", "-q", "--name", "host", "--label", "io.kubernetes.pod.uid="+pod.Metadata.UID).CombinedOutput()
	id := strings.TrimSpace(string(idOut))
	if err != nil || id == "" {
		l.t.Fatalf("locating the host container on %s: %v %s", node, err, idOut)
	}
	// crictl stop with a zero timeout sends SIGKILL at once.
	if out, err := exec.Command("docker", "exec", node, "crictl", "stop", "--timeout", "0", id).CombinedOutput(); err != nil {
		l.t.Fatalf("crictl stop: %v %s", err, out)
	}
}

func podUIDs(pods []kindPod) []string {
	var uids []string
	for _, p := range pods {
		uids = append(uids, p.Metadata.Name+"/"+p.Metadata.UID)
	}
	return uids
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return line
}

func mustUint(t *testing.T, s string) uint64 {
	t.Helper()
	var v uint64
	if _, err := fmt.Sscan(s, &v); err != nil {
		t.Fatalf("parsing %q: %v", s, err)
	}
	return v
}
