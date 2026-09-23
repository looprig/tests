//go:build integration && kind

// Package kindlane is the D3.1 disposable-namespace lane's shared vocabulary:
// the names, tokens and credential layout the in-cluster product binaries
// (cmd/kindhost, cmd/kindcontrol) and the out-of-cluster test agree on.
//
// # What runs where
//
// IN the cluster: a dedicated Host Pod per session, rendered by the released
// controller's Kubernetes adapter and running cmd/kindhost (released host
// v0.6.0's host.Run over the orchestration kit's REAL harness rig and a
// scripted model); two replicas of cmd/kindcontrol, each a released Factory
// (factory.New) sharing one released controller adapter with a released
// controller driver; and two NATS JetStream servers -- one the SessionStore
// durable plane, one the harness journal -- because a Host Pod's own disk
// does not outlive it.
//
// OUT of the cluster: the test process. It drives Factory's public HTTP API
// through a NodePort, reads the durable plane through a second NodePort, and
// observes/perturbs the cluster with kubectl. It dials no Host: the Host's
// address is Pod DNS under the headless Service, resolvable only in-cluster,
// and every HostLink dial in this lane (Factory attach/bind, controller drain)
// is made from inside.
package kindlane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/tests/internal/orchestrationtest"
)

const (
	// Tenant is the lane's one tenant. Every run has its own namespace and
	// its own NATS servers, so a fixed tenant cannot collide across runs.
	Tenant = orchestrationtest.PooledTenantA

	// HostSubdomain is the headless Service (controller deploy/hosts-service.yaml).
	HostSubdomain = "looprig-hosts"
	// HostPort is the HostLink port that Service names.
	HostPort = 7443

	// CredentialRoot is where the controller adapter mounts each allowlisted
	// credential reference (kubernetes/spec.go credentialRoot).
	CredentialRoot = "/var/run/looprig/credentials/"
	// StoreCredential is the reference whose Secret carries the two NATS URLs.
	StoreCredential = "session-store"
	// HostLinkCredential is the reference whose Secret carries the HostLink
	// service tokens the Host accepts.
	HostLinkCredential = "hostlink-auth"

	// FactoryHostLinkToken and ControllerHostLinkToken are the two DISTINCT
	// HostLink service credentials. Their values live only in Secrets.
	FactoryHostLinkTokenKey    = "factory"
	ControllerHostLinkTokenKey = "controller"

	// StoreURLKey and JournalURLKey are the session-store Secret's keys.
	StoreURLKey   = "store_url"
	JournalURLKey = "journal_url"
)

// ReadCredential reads one key of a mounted credential reference.
func ReadCredential(ref, key string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(CredentialRoot, ref, key))
	if err != nil {
		return "", fmt.Errorf("kindlane: reading credential %s/%s: %w", ref, key, err)
	}
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return "", fmt.Errorf("kindlane: credential %s/%s is empty", ref, key)
	}
	return value, nil
}

// ProcessTB lets a product main reuse the orchestration kit's composition,
// which reports through a testing.TB-shaped interface. Fatalf exits the
// process after running registered cleanups; nothing else is test-specific.
type ProcessTB struct {
	Log  *slog.Logger
	name string

	mu       sync.Mutex
	cleanups []func()
}

// NewProcessTB returns a TB for a process named name.
func NewProcessTB(name string, log *slog.Logger) *ProcessTB {
	return &ProcessTB{Log: log, name: name}
}

var _ orchestrationtest.TB = (*ProcessTB)(nil)

func (*ProcessTB) Helper() {}

func (p *ProcessTB) Errorf(format string, args ...any) {
	p.Log.Error(fmt.Sprintf(format, args...))
}

func (p *ProcessTB) Fatalf(format string, args ...any) {
	p.Log.Error(fmt.Sprintf(format, args...))
	p.RunCleanups()
	os.Exit(1)
}

func (p *ProcessTB) Logf(format string, args ...any) {
	p.Log.Info(fmt.Sprintf(format, args...))
}

func (p *ProcessTB) Name() string { return p.name }

func (p *ProcessTB) Cleanup(f func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cleanups = append(p.cleanups, f)
}

// RunCleanups runs registered cleanups in reverse order, once.
func (p *ProcessTB) RunCleanups() {
	p.mu.Lock()
	cleanups := p.cleanups
	p.cleanups = nil
	p.mu.Unlock()
	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}

// Lookup reads a required environment variable.
func Lookup(name string) (string, error) {
	value, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("kindlane: %s is required", name)
	}
	return value, nil
}

// ErrWrongCredential is what the lane's Host verifier answers a credential it
// does not hold.
var ErrWrongCredential = errors.New("kindlane: HostLink credential refused")

// TokenVerifier accepts exactly the listed service tokens for exactly Tenant.
// A Factory and a controller present different tokens; both are accepted, and
// nothing else is -- including the right token for another tenant.
type TokenVerifier struct {
	Tokens []string
}

// VerifyTenant satisfies host.AuthVerifier.
func (v TokenVerifier) VerifyTenant(_ context.Context, tenant sessionwire.TenantID, credential string) error {
	if tenant != Tenant {
		return ErrWrongCredential
	}
	for _, token := range v.Tokens {
		if credential == token {
			return nil
		}
	}
	return ErrWrongCredential
}
