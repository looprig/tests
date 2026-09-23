//go:build integration && kind

// Command kindcontrol is the D3.1 lane's in-cluster CONTROL PLANE replica: one
// released Factory (factory.New, served over HTTP, placement sweep running)
// and one released controller driver, sharing ONE released controller
// Kubernetes adapter -- which is Factory's WorkloadController and pre-attach
// WorkloadEndpointDiscovery and the driver's Workloads at once.
//
// It runs as its own Pod because every HostLink address it dials is Pod DNS
// under the headless Service, which resolves only inside the cluster. Its
// ServiceAccount is the controller's least-privilege Role, applied from the
// released controller's deploy/rbac.yaml.
//
// Configuration is the released cmd/controller's CONTROLLER_* variables plus:
//
//	KIND_STORE_URL           the SessionStore NATS URL
//	KIND_FACTORY_LISTEN      Factory's HTTP listen address
//	KIND_FACTORY_ORIGIN      Factory's externally reached base URL (CSRF)
//	KIND_FACTORY_TOKEN_FILE  Factory's HostLink service token
//	KIND_PAYLOAD             the dedicated LaunchTemplate's PayloadV1 body
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/looprig/controller/driver"
	"github.com/looprig/controller/hostlink"
	"github.com/looprig/controller/kubernetes"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/natsstore"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/kindlane"
	"github.com/looprig/tests/internal/orchestrationtest"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("kindcontrol exiting", "error", err.Error())
		os.Exit(1)
	}
}

type fileToken struct{ path string }

func (f fileToken) ServiceToken(context.Context) (string, error) {
	raw, err := os.ReadFile(f.path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", errors.New("kindcontrol: empty token file")
	}
	return token, nil
}

func must(name string) string {
	value, err := kindlane.Lookup(name)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return value
}

func duration(name string) time.Duration {
	parsed, err := time.ParseDuration(must(name))
	if err != nil {
		fmt.Fprintf(os.Stderr, "kindcontrol: %s: %v\n", name, err)
		os.Exit(2)
	}
	return parsed
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	replica := must("CONTROLLER_REPLICA_ID")
	port, err := strconv.ParseInt(must("CONTROLLER_HOST_PORT"), 10, 32)
	if err != nil {
		return err
	}
	credentials := map[string]string{}
	for _, pair := range strings.Split(must("CONTROLLER_CREDENTIALS"), ",") {
		ref, secret, ok := strings.Cut(pair, "=")
		if !ok {
			return fmt.Errorf("kindcontrol: CONTROLLER_CREDENTIALS entry %q", pair)
		}
		credentials[ref] = secret
	}
	var sessions []struct {
		TenantID  string `json:"tenant_id"`
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal([]byte(must("CONTROLLER_SESSIONS")), &sessions); err != nil {
		return fmt.Errorf("kindcontrol: CONTROLLER_SESSIONS: %w", err)
	}
	keys := make([]driver.Key, 0, len(sessions))
	for _, s := range sessions {
		keys = append(keys, driver.Key{TenantID: sessionwire.TenantID(s.TenantID), SessionID: sessionwire.SessionID(s.SessionID)})
	}
	source, err := driver.NewFixedSource(keys, driver.MaxKeysPerPassCeiling)
	if err != nil {
		return err
	}
	drainCeiling, commitMargin := duration("CONTROLLER_DRAIN_CEILING"), duration("CONTROLLER_COMMIT_MARGIN")

	openCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	backend, err := natsstore.Open(openCtx, natsstore.Options{URL: must("KIND_STORE_URL")})
	if err != nil {
		return fmt.Errorf("open the session store backend: %w", err)
	}
	defer func() { _ = backend.Close(context.Background()) }()

	tb := kindlane.NewProcessTB("kindcontrol-"+replica, log)
	defer tb.RunCleanups()
	world := orchestrationtest.NewPooledWorld(tb, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{kindlane.Tenant},
		Backend: backend.Composite,
	})

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	client, err := k8s.NewForConfig(restConfig)
	if err != nil {
		return err
	}
	adapter, err := kubernetes.New(kubernetes.Config{
		Client:        client,
		Namespace:     must("CONTROLLER_NAMESPACE"),
		ControllerID:  must("CONTROLLER_ID"),
		HostSubdomain: must("CONTROLLER_HOST_SUBDOMAIN"),
		HostPort:      int32(port),
		Credentials:   credentials,
		Registry:      world.Store,
		Clock:         kubernetes.SystemClock{},
		DrainCeiling:  drainCeiling,
		CommitMargin:  commitMargin,
	})
	if err != nil {
		return fmt.Errorf("compose the controller adapter: %w", err)
	}

	// ---- Factory -----------------------------------------------------------
	factoryToken, err := fileToken{path: must("KIND_FACTORY_TOKEN_FILE")}.ServiceToken(ctx)
	if err != nil {
		return err
	}
	workload := sessionstore.DesiredWorkload{PayloadVersion: kubernetes.PayloadVersionV1, Payload: []byte(must("KIND_PAYLOAD"))}
	opts := orchestrationtest.DedicatedFactoryOptions(tb, world, orchestrationtest.PooledFactoryConfig{
		Replica:      replica,
		Logs:         os.Stderr,
		ServiceToken: factoryToken,
		Workload:     &workload,
	}, adapter, must("KIND_FACTORY_ORIGIN"))
	server, err := factory.New(opts...)
	if err != nil {
		return fmt.Errorf("compose factory: %w", err)
	}
	if err := server.Start(ctx); err != nil {
		return fmt.Errorf("start factory: %w", err)
	}
	listener, err := net.Listen("tcp", must("KIND_FACTORY_LISTEN"))
	if err != nil {
		return err
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()

	// ---- controller driver -------------------------------------------------
	drainer, err := hostlink.New(hostlink.Config{
		Token:       fileToken{path: must("CONTROLLER_HOSTLINK_TOKEN_FILE")},
		Version:     "kindcontrol",
		DialTimeout: 5 * time.Second,
		RPCTimeout:  10 * time.Second,
	})
	if err != nil {
		return err
	}
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return err
	}
	last := map[driver.Key]string{}
	d, err := driver.New(driver.Config{
		Source:         source,
		Clock:          kubernetes.SystemClock{},
		HolderID:       replica + "." + hex.EncodeToString(suffix[:]),
		ClaimTTL:       duration("CONTROLLER_CLAIM_TTL"),
		ItemTimeout:    duration("CONTROLLER_ITEM_TIMEOUT"),
		MaxKeysPerPass: len(keys),
		Interval:       duration("CONTROLLER_INTERVAL"),
		DrainTimeout:   drainCeiling + commitMargin,
		Logger:         log,
		Catalog:        world.Store,
		Registry:       world.Store,
		Claims:         world.Store,
		Workloads:      adapter,
		Terminations:   world.Store,
		Drainer:        drainer,
		OnPass: func(report driver.PassReport, err error) {
			// One line per CHANGE of a session's outcome: a pass runs every
			// interval and an unchanged outcome is noise.
			for _, item := range report.Items {
				line := fmt.Sprint(item.Outcome, " ", item.Err)
				if last[item.Key] == line {
					continue
				}
				last[item.Key] = line
				log.Info("controller item", "session", string(item.Key.SessionID), "outcome", fmt.Sprint(item.Outcome), "error", fmt.Sprint(item.Err))
			}
			if err != nil {
				log.Warn("controller pass", "error", err.Error())
			}
		},
	})
	if err != nil {
		return fmt.Errorf("compose the controller driver: %w", err)
	}
	log.Info("kindcontrol starting", "replica", replica, "pod_selector", kubernetes.OwnerSelector(must("CONTROLLER_ID")))
	driven := make(chan error, 1)
	go func() { driven <- d.Run(ctx) }()

	select {
	case <-ctx.Done():
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("factory serve", "error", fmt.Sprint(err))
		}
	case err := <-driven:
		log.Error("driver stopped", "error", fmt.Sprint(err))
	}
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelStop()
	_ = server.Stop(stopCtx)
	return nil
}
