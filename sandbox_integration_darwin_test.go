//go:build integration && darwin

package tests

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/looprig/sandbox"
)

func TestSandboxDarwinTargetNetworkGrantUsesAuthenticatedRoute(t *testing.T) {
	if _, err := os.Stat("/usr/bin/sandbox-exec"); err != nil {
		t.Skipf("native Seatbelt integration requires /usr/bin/sandbox-exec: %v", err)
	}
	curl := requireCurl(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("target-ok"))
	}))
	defer upstream.Close()
	route, err := sandbox.NewUpstreamEgressRoute(upstream.URL, true)
	if err != nil {
		t.Fatalf("NewUpstreamEgressRoute: %v", err)
	}
	workspace := t.TempDir()
	profile := newSandboxIntegrationProfile(t, workspace, sandbox.Allow, sandbox.Gated)
	set, err := sandbox.NewExecutorSet(profile, sandbox.WithScratchRoot(t.TempDir()), sandbox.WithMaxExecutors(1), sandbox.WithEgressRoute(route))
	if err != nil {
		t.Fatalf("NewExecutorSet: %v", err)
	}
	t.Cleanup(func() { _ = set.Close() })
	executor := sandboxIntegrationExecutor(t, set, "target-route")
	guarantees := executor.Guarantees()
	if !guarantees.NetworkBoundary || !guarantees.TargetNetwork || !guarantees.AddressNetwork {
		t.Fatalf("route guarantees = %+v, want NetworkBoundary, TargetNetwork, and trusted AddressNetwork", guarantees)
	}

	allowedCommand := shellQuote(curl) + " --fail --silent --show-error --max-time 10 http://allowed.example/"
	allowedGrant := issueSandboxGrant(t, executor, "target-allowed", allowedCommand, workspace,
		"network", "", "network.proxy-target.v1", "tcp:allowed.example:80")
	if out, code, err := executor.RunCommandWithGrants(context.Background(), "target-allowed", workspace, allowedCommand, []string{allowedGrant}); err != nil || code != 0 || string(out) != "target-ok" {
		t.Fatalf("allowed target run = code %d err %v out %q", code, err, out)
	}

	deniedCommand := shellQuote(curl) + " --fail --silent --show-error --max-time 10 http://denied.example/"
	deniedGrant := issueSandboxGrant(t, executor, "target-denied", deniedCommand, workspace,
		"network", "", "network.proxy-target.v1", "tcp:allowed.example:80")
	if _, _, err := executor.RunCommandWithGrants(context.Background(), "target-denied", workspace, deniedCommand, []string{deniedGrant}); !errors.Is(err, sandbox.ErrNetworkTargetDenied) {
		t.Fatalf("denied target error = %v, want ErrNetworkTargetDenied", err)
	}
}

func requireCurl(t *testing.T) string {
	t.Helper()
	for _, path := range []string{"/usr/bin/curl", "/opt/homebrew/bin/curl", "/usr/local/bin/curl"} {
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path
		}
	}
	t.Skip("target-network integration requires curl")
	return ""
}
