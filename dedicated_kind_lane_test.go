//go:build integration && kind

// This file is the D3.1 disposable-namespace lane's harness (runbook 07,
// "Task D3.1: Run in a disposable namespace"). The cases are in
// dedicated_placement_integration_test.go and dedicated_drain_integration_test.go.
//
// # Running it
//
// It needs a real cluster and is double-gated: the `kind` build tag AND
// LOOPRIG_KIND=1. scripts/kind-d31.sh creates the cluster and a local
// registry, builds the lane's two product binaries (internal/kindlane/cmd) into
// images and prints the digest-pinned references this file reads from
// KIND_HOST_IMAGE and KIND_CONTROL_IMAGE.
//
// # Isolation and cleanup
//
// Every case creates ONE namespace named and labelled with a fresh run id, and
// everything the lane creates lives in it. Cleanup deletes exactly that
// namespace, after the controller README's finalizer-removal procedure (scale
// the controller to zero, strip the termination finalizer from the Pods
// selected by THIS deployment's owner digest) -- because a Host Pod's finalizer
// otherwise holds the namespace's deletion forever once the controller is gone.
//
// # What is real
//
// Kubernetes (kind v0.33): the API server, RBAC, the kubelet's Pod lifecycle,
// finalizers, UID-preconditioned deletes, headless-Service Pod DNS,
// terminationGracePeriodSeconds and SIGTERM. The controller's released
// adapter, driver and HostLink drain client; the released Factory; the
// released Host (host.Run); SessionStore over a real NATS JetStream server.
// The harness rig and journal are real; the model is scripted (the kit's).
package tests

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"text/template"
	"time"

	controllerk8s "github.com/looprig/controller/kubernetes"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/natsstore"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/tests/internal/kindlane"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// The NodePorts are FIXED (the kind config maps them to the host), so runs are
// SERIAL: two cases cannot overlap on one cluster, and a namespace a failed
// run leaked keeps the ports until it is deleted.
const (
	kindStoreNodePort   = 30422
	kindFactoryNodePort = 30480
	kindRunLabel        = "looprig.dev/d31-run"
	kindDrainCeiling    = 15 * time.Second
	kindCommitMargin    = 5 * time.Second
	kindHostDrainGrace  = "15s"
)

// kindLane is one run: one namespace, its NATS servers, its control plane
// replicas, and the test process's own SessionStore handle on the durable
// plane (through the NodePort).
type kindLane struct {
	t            *testing.T
	RunID        string
	Namespace    string
	ControllerID string
	Sessions     []sessionwire.SessionID
	Store        *sessionstore.Store
	FactoryURL   string
	HostImage    string
	Replicas     int

	// Backend is the durable plane's storage composite (through the NodePort).
	Backend *storage.Composite

	// root is the case's own *testing.T. Subtests reassign t; cleanup, which
	// runs after they have finished, reports through root.
	root *testing.T

	kubeContext string
	http        *http.Client
}

func kindContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// newKindLane stands up one run. sessions are the session ids the controller
// driver is configured with (the released driver reads a FIXED operator list;
// it has no durable work source), so a case must name them in advance.
func newKindLane(t *testing.T, ctx context.Context, sessions []sessionwire.SessionID, replicas int) *kindLane {
	t.Helper()
	if os.Getenv("LOOPRIG_KIND") != "1" {
		t.Skip("D3.1 kind lane: set LOOPRIG_KIND=1 (and run scripts/kind-d31.sh up/images)")
	}
	hostImage, controlImage := os.Getenv("KIND_HOST_IMAGE"), os.Getenv("KIND_CONTROL_IMAGE")
	if hostImage == "" || controlImage == "" {
		t.Fatal("KIND_HOST_IMAGE and KIND_CONTROL_IMAGE must name the digest-pinned images scripts/kind-d31.sh images printed")
	}
	raw := make([]byte, 4)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	run := hex.EncodeToString(raw)
	lane := &kindLane{
		t:            t,
		root:         t,
		RunID:        run,
		Namespace:    "looprig-d31-" + run,
		ControllerID: "d31-" + run,
		Sessions:     sessions,
		FactoryURL:   fmt.Sprintf("http://127.0.0.1:%d", kindFactoryNodePort),
		HostImage:    hostImage,
		Replicas:     replicas,
		kubeContext:  envOr("KIND_CONTEXT", "kind-looprig-d31"),
		http:         &http.Client{Timeout: 20 * time.Second},
	}
	t.Logf("D3.1 run %s: namespace %s", run, lane.Namespace)

	lane.Kubectl("create", "namespace", lane.Namespace)
	t.Cleanup(lane.cleanup)
	lane.Kubectl("label", "namespace", lane.Namespace, kindRunLabel+"="+run)

	// The RELEASED controller's own deployment artifacts, from the pinned
	// module: the least-privilege ServiceAccount/Role/RoleBinding and the
	// headless Service with publishNotReadyAddresses.
	deploy := filepath.Join(moduleDir(t, "github.com/looprig/controller"), "deploy")
	for _, file := range []string{"rbac.yaml", "hosts-service.yaml"} {
		lane.Kubectl("-n", lane.Namespace, "apply", "-f", filepath.Join(deploy, file))
	}
	lane.apply(kindDataPlane, nil)
	lane.Kubectl("-n", lane.Namespace, "rollout", "status", "deployment/nats-store", "--timeout=180s")
	lane.Kubectl("-n", lane.Namespace, "rollout", "status", "deployment/nats-journal", "--timeout=180s")

	// The case's own context, not a short open bound: natsstore and
	// sessionstore keep what Open is handed for the life of the handle, and a
	// cancelled open context silently fails every later read.
	// A Ready NATS Pod is not yet a programmed NodePort: retry the connect.
	var backend *natsstore.Store
	var err error
	for deadline := time.Now().Add(90 * time.Second); ; time.Sleep(time.Second) {
		backend, err = natsstore.Open(ctx, natsstore.Options{URL: fmt.Sprintf("nats://127.0.0.1:%d", kindStoreNodePort)})
		if err == nil || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		t.Fatalf("opening the durable plane through the NodePort: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	store, err := sessionstore.Open(ctx, backend.Composite)
	if err != nil {
		t.Fatalf("opening sessionstore over the lane's NATS: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	lane.Store = store
	lane.Backend = backend.Composite

	payload, err := json.Marshal(lane.payload())
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]map[string]string, 0, len(sessions))
	for _, s := range sessions {
		keys = append(keys, map[string]string{"tenant_id": string(kindlane.Tenant), "session_id": string(s)})
	}
	sessionJSON, _ := json.Marshal(keys)
	lane.apply(kindControlPlane, map[string]any{
		"ControlImage": controlImage,
		"Replicas":     replicas,
		"Sessions":     string(sessionJSON),
		"Payload":      string(payload),
	})
	lane.Kubectl("-n", lane.Namespace, "rollout", "status", "deployment/looprig-control", "--timeout=180s")
	lane.WaitFor("Factory answers through the NodePort", 90*time.Second, func() bool {
		status, _ := lane.Get(ctx, "/v1/sessions")
		return status == http.StatusOK
	})
	return lane
}

// payload is the dedicated LaunchTemplate's PayloadV1 body: the released
// controller's deploy/dedicated-host-payload.json shape, with the lane's Host
// image and short test timings.
func (l *kindLane) payload() map[string]any {
	return map[string]any{
		"image":       l.HostImage,
		"resources":   map[string]string{"cpu": "250m", "memory": "256Mi", "ephemeral_storage": "512Mi"},
		"workspace":   map[string]string{"size_limit": "256Mi"},
		"credentials": []string{kindlane.StoreCredential, kindlane.HostLinkCredential},
		"host_settings": map[string]string{
			"HOST_WARM_TTL":              "5m",
			"HOST_REGISTRY_HEARTBEAT":    "2s",
			"HOST_REGISTRY_EXPIRY":       "10s",
			"HOST_CLAIM_TTL":             "5s",
			"HOST_APPLY_DEADLINE":        "2m",
			"HOST_RECONCILE_INTERVAL":    "1s",
			"HOST_COMMAND_QUEUE_SIZE":    "16",
			"HOST_RECONCILE_BATCH":       "8",
			"HOST_MAX_BINDINGS_PER_LINK": "1",
			"HOST_MAX_BINDINGS":          "1",
			"HOST_MAX_TENANT_LINKS":      "4",
			"HOST_DRAIN_GRACE":           kindHostDrainGrace,
			"HOST_DRAIN_IDLE_BOUNDARY":   "3s",
			"HOST_DRAIN_PUBLISH_BOUND":   "3s",
			"HOST_COMPATIBILITY_TIMEOUT": "10s",
			"HOST_WORK_POLL":             "1s",
		},
	}
}

func (l *kindLane) apply(manifest string, data map[string]any) {
	l.t.Helper()
	values := map[string]any{
		"Namespace":    l.Namespace,
		"Run":          l.RunID,
		"RunLabel":     kindRunLabel,
		"ControllerID": l.ControllerID,
		"StoreNode":    kindStoreNodePort,
		"FactoryNode":  kindFactoryNodePort,
		"Ceiling":      kindDrainCeiling.String(),
		"Margin":       kindCommitMargin.String(),
	}
	for k, v := range data {
		values[k] = v
	}
	var rendered bytes.Buffer
	if err := template.Must(template.New("m").Parse(manifest)).Execute(&rendered, values); err != nil {
		l.t.Fatal(err)
	}
	l.kubectlIn(rendered.Bytes(), "-n", l.Namespace, "apply", "-f", "-")
}

// Kubectl runs kubectl as the cluster ADMIN and fails the case on error.
func (l *kindLane) Kubectl(args ...string) string {
	l.t.Helper()
	return l.kubectlIn(nil, args...)
}

func (l *kindLane) kubectlIn(stdin []byte, args ...string) string {
	l.t.Helper()
	out, err := l.TryKubectl(stdin, args...)
	if err != nil {
		l.t.Fatalf("kubectl %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// TryKubectl runs kubectl and returns its combined output and error.
func (l *kindLane) TryKubectl(stdin []byte, args ...string) (string, error) {
	cmd := exec.Command("kubectl", append([]string{"--context", l.kubeContext}, args...)...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// AsController runs kubectl IMPERSONATING the controller's ServiceAccount.
func (l *kindLane) AsController(args ...string) (string, error) {
	return l.TryKubectl(nil, append([]string{"--as", "system:serviceaccount:" + l.Namespace + ":looprig-controller"}, args...)...)
}

// kindPod is the part of a Pod the cases read.
type kindPod struct {
	Metadata struct {
		Name              string            `json:"name"`
		UID               string            `json:"uid"`
		Labels            map[string]string `json:"labels"`
		Annotations       map[string]string `json:"annotations"`
		Finalizers        []string          `json:"finalizers"`
		DeletionTimestamp *time.Time        `json:"deletionTimestamp"`
		OwnerReferences   []any             `json:"ownerReferences"`
	} `json:"metadata"`
	Spec struct {
		Hostname                      string `json:"hostname"`
		Subdomain                     string `json:"subdomain"`
		TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds"`
		AutomountServiceAccountToken  *bool  `json:"automountServiceAccountToken"`
		ServiceAccountName            string `json:"serviceAccountName"`
		RestartPolicy                 string `json:"restartPolicy"`
		Containers                    []struct {
			Image string `json:"image"`
			Env   []struct {
				Name      string `json:"name"`
				Value     string `json:"value"`
				ValueFrom any    `json:"valueFrom"`
			} `json:"env"`
			EnvFrom []any `json:"envFrom"`
		} `json:"containers"`
		Volumes []struct {
			Name   string `json:"name"`
			Secret *struct {
				SecretName string `json:"secretName"`
			} `json:"secret"`
		} `json:"volumes"`
	} `json:"spec"`
	Status struct {
		Phase      string `json:"phase"`
		PodIP      string `json:"podIP"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
		ContainerStatuses []struct {
			RestartCount int `json:"restartCount"`
			State        struct {
				Terminated *struct {
					ExitCode   int       `json:"exitCode"`
					Reason     string    `json:"reason"`
					Message    string    `json:"message"`
					StartedAt  time.Time `json:"startedAt"`
					FinishedAt time.Time `json:"finishedAt"`
				} `json:"terminated"`
			} `json:"state"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

func (p kindPod) Env(name string) string {
	for _, c := range p.Spec.Containers {
		for _, e := range c.Env {
			if e.Name == name {
				return e.Value
			}
		}
	}
	return ""
}

func (p kindPod) Ready() bool {
	for _, c := range p.Status.Conditions {
		if c.Type == "Ready" {
			return c.Status == "True"
		}
	}
	return false
}

// HostPods lists the Pods THIS run's controller deployment manages.
func (l *kindLane) HostPods() []kindPod {
	l.t.Helper()
	out, err := l.TryKubectl(nil, "-n", l.Namespace, "get", "pods", "-l", controllerk8s.OwnerSelector(l.ControllerID), "-o", "json")
	if err != nil {
		l.t.Fatalf("listing host pods: %v\n%s", err, out)
	}
	var list struct {
		Items []kindPod `json:"items"`
	}
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		l.t.Fatalf("decoding pods: %v", err)
	}
	return list.Items
}

// SessionPods are this run's Host Pods for one session.
func (l *kindLane) SessionPods(s sessionwire.SessionID) []kindPod {
	var pods []kindPod
	for _, pod := range l.HostPods() {
		if pod.Metadata.Labels[controllerk8s.LabelSession] == controllerk8s.SessionHash(kindlane.Tenant, s) {
			pods = append(pods, pod)
		}
	}
	return pods
}

// WaitFor polls ok until it holds or the bound passes.
func (l *kindLane) WaitFor(what string, within time.Duration, ok func() bool) time.Duration {
	l.t.Helper()
	start := time.Now()
	for time.Since(start) < within {
		if ok() {
			return time.Since(start)
		}
		time.Sleep(250 * time.Millisecond)
	}
	l.dumpDiagnostics()
	l.t.Fatalf("D3.1: %s did not happen within %s", what, within)
	return 0
}

func (l *kindLane) dumpDiagnostics() {
	out, _ := l.TryKubectl(nil, "-n", l.Namespace, "get", "pods", "-o", "wide", "--show-labels")
	l.t.Logf("pods:\n%s", out)
	out, _ = l.TryKubectl(nil, "-n", l.Namespace, "logs", "-l", "app.kubernetes.io/name=looprig-control", "--tail=400", "--prefix")
	var kept []string
	for _, line := range strings.Split(out, "\n") {
		// A configured session nobody has created yet fails every pass.
		if !strings.Contains(line, "binding_not_found") && !strings.Contains(line, "unsupported workload payload version") {
			kept = append(kept, line)
		}
	}
	l.t.Logf("control logs (binding_not_found passes elided):\n%s", strings.Join(kept, "\n"))
	for _, pod := range l.HostPods() {
		out, _ = l.TryKubectl(nil, "-n", l.Namespace, "logs", pod.Metadata.Name, "--tail=40")
		l.t.Logf("host %s logs:\n%s", pod.Metadata.Name, out)
	}
}

// Post and Get call Factory's public HTTP API as the tenant's actor.
func (l *kindLane) Post(ctx context.Context, path string, body any) (int, string) {
	l.t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		l.t.Fatal(err)
	}
	return l.do(ctx, http.MethodPost, path, encoded)
}

func (l *kindLane) Get(ctx context.Context, path string) (int, string) {
	return l.do(ctx, http.MethodGet, path, nil)
}

func (l *kindLane) do(ctx context.Context, method, path string, body []byte) (int, string) {
	request, err := http.NewRequestWithContext(ctx, method, l.FactoryURL+path, bytes.NewReader(body))
	if err != nil {
		l.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+orchestrationtest.PooledBearers[kindlane.Tenant])
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := l.http.Do(request)
	if err != nil {
		return 0, err.Error()
	}
	defer response.Body.Close()
	answer, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(answer)
}

// Create creates a session through Factory with a first message.
func (l *kindLane) Create(ctx context.Context, s sessionwire.SessionID, command, text string) {
	l.t.Helper()
	status, body := l.Post(ctx, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(command),
		SessionID:       s,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(fmt.Sprintf(`[{"type":"text","text":%q}]`, text)),
	})
	if status != http.StatusCreated {
		l.t.Fatalf("Factory create of %s answered %d: %s", s, status, body)
	}
}

// Input sends one input through Factory.
func (l *kindLane) Input(ctx context.Context, s sessionwire.SessionID, command, text string) {
	l.t.Helper()
	status, body := l.Post(ctx, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(command),
		SessionID:       s,
		Blocks:          json.RawMessage(fmt.Sprintf(`[{"type":"text","text":%q}]`, text)),
	})
	if status != http.StatusOK {
		l.t.Fatalf("Factory input %s on %s answered %d: %s", command, s, status, body)
	}
}

// CommandState is one disposition command's durable state ("" if unreadable).
func (l *kindLane) CommandState(ctx context.Context, s sessionwire.SessionID, command string) sessionstore.InboxState {
	entry, err := l.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID: kindlane.Tenant, SessionID: s, CommandID: sessionwire.CommandID(command),
	})
	if err != nil {
		return ""
	}
	return entry.Record.State
}

// AwaitApplied waits for one command to settle applied, failing fast on any
// other terminal state.
func (l *kindLane) AwaitApplied(ctx context.Context, s sessionwire.SessionID, command string, within time.Duration) time.Duration {
	l.t.Helper()
	return l.WaitFor(fmt.Sprintf("command %s on %s applied", command, s), within, func() bool {
		switch state := l.CommandState(ctx, s, command); state {
		case sessionstore.InboxStateApplied:
			return true
		case sessionstore.InboxStateRejected:
			l.t.Fatalf("command %s on %s settled %s", command, s, state)
		}
		return false
	})
}

// Owner reads the session's live Host registration.
func (l *kindLane) Owner(ctx context.Context, s sessionwire.SessionID) (sessionstore.HostRegistrationEntry, error) {
	return l.Store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{TenantID: kindlane.Tenant, SessionID: s})
}

// Log appends one line of evidence to the case output.
func (l *kindLane) Log(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("EVIDENCE "+format, args...)
}

func (l *kindLane) cleanup() {
	if os.Getenv("KIND_KEEP") == "1" {
		l.root.Logf("KIND_KEEP=1: leaving namespace %s", l.Namespace)
		return
	}
	// The controller README's removal procedure, verbatim in effect: stop the
	// controller first so it cannot race the removal, then strip the
	// finalizer only from the Pods THIS deployment's owner digest selects.
	_, _ = l.TryKubectl(nil, "-n", l.Namespace, "scale", "deployment/looprig-control", "--replicas=0")
	_, _ = l.TryKubectl(nil, "-n", l.Namespace, "wait", "--for=delete", "pod", "-l", "app.kubernetes.io/name=looprig-control", "--timeout=60s")
	names, _ := l.TryKubectl(nil, "-n", l.Namespace, "get", "pods", "-l", controllerk8s.OwnerSelector(l.ControllerID), "-o", "name")
	for _, name := range strings.Fields(names) {
		_, _ = l.TryKubectl(nil, "-n", l.Namespace, "patch", name, "--type=merge", "-p", `{"metadata":{"finalizers":null}}`)
	}
	if out, err := l.TryKubectl(nil, "delete", "namespace", l.Namespace, "--wait=true", "--timeout=180s"); err != nil {
		l.root.Errorf("deleting namespace %s: %v\n%s", l.Namespace, err, out)
	}
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func moduleDir(t *testing.T, module string) string {
	t.Helper()
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", module)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("locating %s: %v", module, err)
	}
	return strings.TrimSpace(string(out))
}

// kindDataPlane is the durable plane: two NATS JetStream servers (the
// SessionStore plane, exposed to the test by NodePort, and the harness
// journal), and the lane's Secrets. The HostLink tokens are minted per run.
var kindDataPlane = `
apiVersion: v1
kind: Secret
metadata:
  name: d31-session-store
  labels: {"{{.RunLabel}}": "{{.Run}}"}
stringData:
  store_url: nats://nats-store:4222
  journal_url: nats://nats-journal:4222
---
apiVersion: v1
kind: Secret
metadata:
  name: d31-hostlink-auth
  labels: {"{{.RunLabel}}": "{{.Run}}"}
stringData:
  factory: factory-{{.Run}}-token
  controller: controller-{{.Run}}-token
---
apiVersion: v1
kind: Secret
metadata:
  name: controller-hostlink-token
  labels: {"{{.RunLabel}}": "{{.Run}}"}
stringData:
  token: controller-{{.Run}}-token
---
apiVersion: v1
kind: Secret
metadata:
  name: factory-hostlink-token
  labels: {"{{.RunLabel}}": "{{.Run}}"}
stringData:
  token: factory-{{.Run}}-token
---
# A DECOY: a pooled deployment's cross-tenant credential in the same namespace.
# Nothing may mount it into a dedicated Pod; the allowlist does not name it.
apiVersion: v1
kind: Secret
metadata:
  name: pooled-cross-tenant-credential
  labels: {"{{.RunLabel}}": "{{.Run}}"}
stringData:
  token: pooled-cross-tenant-{{.Run}}
` + natsManifest("nats-store", true) + natsManifest("nats-journal", false)

func natsManifest(name string, exposed bool) string {
	service := `
---
apiVersion: v1
kind: Service
metadata:
  name: ` + name + `
  labels: {"{{.RunLabel}}": "{{.Run}}"}
spec:
  selector: {app: ` + name + `}
  ports: [{name: client, port: 4222, targetPort: 4222}]`
	if exposed {
		service = `
---
apiVersion: v1
kind: Service
metadata:
  name: ` + name + `
  labels: {"{{.RunLabel}}": "{{.Run}}"}
spec:
  type: NodePort
  selector: {app: ` + name + `}
  ports: [{name: client, port: 4222, targetPort: 4222, nodePort: {{.StoreNode}}}]`
	}
	return `
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ` + name + `
  labels: {"{{.RunLabel}}": "{{.Run}}"}
spec:
  replicas: 1
  selector: {matchLabels: {app: ` + name + `}}
  template:
    metadata:
      labels: {app: ` + name + `, "{{.RunLabel}}": "{{.Run}}"}
    spec:
      containers:
        - name: nats
          image: nats:2.12-alpine
          imagePullPolicy: IfNotPresent
          args: ["-js", "-sd", "/data"]
          ports: [{containerPort: 4222}]
          readinessProbe: {tcpSocket: {port: 4222}, periodSeconds: 2}
          resources:
            requests: {cpu: 50m, memory: 64Mi}
            limits: {cpu: "1", memory: 512Mi}
          volumeMounts: [{name: data, mountPath: /data}]
      volumes: [{name: data, emptyDir: {}}]` + service
}

// kindControlPlane is the in-cluster control plane: kindcontrol replicas
// (Factory + controller driver over one adapter) under the controller's
// ServiceAccount, and Factory's NodePort. The CONTROLLER_* variables are the
// released cmd/controller's.
const kindControlPlane = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: looprig-control
  labels: {"{{.RunLabel}}": "{{.Run}}"}
spec:
  replicas: {{.Replicas}}
  selector: {matchLabels: {app.kubernetes.io/name: looprig-control}}
  template:
    metadata:
      labels: {app.kubernetes.io/name: looprig-control, "{{.RunLabel}}": "{{.Run}}"}
    spec:
      serviceAccountName: looprig-controller
      terminationGracePeriodSeconds: 10
      containers:
        - name: control
          image: {{.ControlImage}}
          imagePullPolicy: IfNotPresent
          ports: [{name: http, containerPort: 8080}]
          readinessProbe: {tcpSocket: {port: 8080}, periodSeconds: 2}
          resources:
            requests: {cpu: 100m, memory: 128Mi}
            limits: {cpu: "1", memory: 512Mi}
          env:
            - {name: CONTROLLER_NAMESPACE, valueFrom: {fieldRef: {fieldPath: metadata.namespace}}}
            - {name: CONTROLLER_REPLICA_ID, valueFrom: {fieldRef: {fieldPath: metadata.name}}}
            - {name: CONTROLLER_ID, value: "{{.ControllerID}}"}
            - {name: CONTROLLER_HOST_SUBDOMAIN, value: looprig-hosts}
            - {name: CONTROLLER_HOST_PORT, value: "7443"}
            - {name: CONTROLLER_CREDENTIALS, value: "session-store=d31-session-store,hostlink-auth=d31-hostlink-auth"}
            - {name: CONTROLLER_SESSIONS, value: '{{.Sessions}}'}
            - {name: CONTROLLER_INTERVAL, value: 1s}
            - {name: CONTROLLER_CLAIM_TTL, value: 10s}
            - {name: CONTROLLER_ITEM_TIMEOUT, value: 8s}
            - {name: CONTROLLER_DRAIN_CEILING, value: "{{.Ceiling}}"}
            - {name: CONTROLLER_COMMIT_MARGIN, value: "{{.Margin}}"}
            - {name: CONTROLLER_HOSTLINK_TOKEN_FILE, value: /var/run/d31/controller/token}
            - {name: KIND_STORE_URL, value: "nats://nats-store:4222"}
            - {name: KIND_FACTORY_LISTEN, value: ":8080"}
            - {name: KIND_FACTORY_ORIGIN, value: "http://127.0.0.1:{{.FactoryNode}}"}
            - {name: KIND_FACTORY_TOKEN_FILE, value: /var/run/d31/factory/token}
            - {name: KIND_PAYLOAD, value: '{{.Payload}}'}
          volumeMounts:
            - {name: controller-token, mountPath: /var/run/d31/controller, readOnly: true}
            - {name: factory-token, mountPath: /var/run/d31/factory, readOnly: true}
      volumes:
        - {name: controller-token, secret: {secretName: controller-hostlink-token, defaultMode: 288}}
        - {name: factory-token, secret: {secretName: factory-hostlink-token, defaultMode: 288}}
---
apiVersion: v1
kind: Service
metadata:
  name: looprig-factory
  labels: {"{{.RunLabel}}": "{{.Run}}"}
spec:
  type: NodePort
  selector: {app.kubernetes.io/name: looprig-control}
  ports: [{name: http, port: 8080, targetPort: 8080, nodePort: {{.FactoryNode}}}]
`

func jsonUnmarshal(raw []byte, v any) error { return json.Unmarshal(raw, v) }
