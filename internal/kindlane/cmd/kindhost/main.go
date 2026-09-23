//go:build integration && kind

// Command kindhost is the D3.1 lane's PRODUCT Host: the released host v0.8.1
// run through host.Run -- compose, start, serve, drain on SIGTERM -- exactly as
// a product main would, over the orchestration kit's real harness rig.
//
// It reads ONLY what the released controller adapter renders into a dedicated
// Pod: the adapter-owned HOST_* identity (HOST_ID, HOST_GENERATION,
// HOST_INTERNAL_ENDPOINT, HOST_ISOLATION_CLASS, HOST_PLACEMENT, HOST_CAPACITY,
// HOST_FIXED_SESSION_ID, HOST_LISTEN_ADDRESS), the payload's allowlisted tuning
// variables, and the allowlisted credential references mounted read-only under
// /var/run/looprig/credentials. Nothing reaches it any other way.
//
// The root filesystem is read-only (the adapter's securityContext), so the
// process points TMPDIR and HOME at the /workspace emptyDir before composing.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/host"
	"github.com/looprig/natsstore"
	"github.com/looprig/storage"
	"github.com/looprig/tests/internal/kindlane"
	"github.com/looprig/tests/internal/orchestrationtest"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("kindhost exiting", "error", err.Error())
		os.Exit(1)
	}
	log.Info("kindhost exited cleanly")
}

func run(log *slog.Logger) error {
	for _, name := range []string{"TMPDIR", "HOME"} {
		if err := os.Setenv(name, "/workspace"); err != nil {
			return err
		}
	}
	env, err := readEnv()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	storeURL, err := kindlane.ReadCredential(kindlane.StoreCredential, kindlane.StoreURLKey)
	if err != nil {
		return err
	}
	journalURL, err := kindlane.ReadCredential(kindlane.StoreCredential, kindlane.JournalURLKey)
	if err != nil {
		return err
	}
	factoryToken, err := kindlane.ReadCredential(kindlane.HostLinkCredential, kindlane.FactoryHostLinkTokenKey)
	if err != nil {
		return err
	}
	controllerToken, err := kindlane.ReadCredential(kindlane.HostLinkCredential, kindlane.ControllerHostLinkTokenKey)
	if err != nil {
		return err
	}

	openCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	store, err := natsstore.Open(openCtx, natsstore.Options{URL: storeURL})
	if err != nil {
		return fmt.Errorf("open the session store backend: %w", err)
	}
	defer func() { _ = store.Close(context.Background()) }()
	journal, err := natsstore.Open(openCtx, natsstore.Options{URL: journalURL})
	if err != nil {
		return fmt.Errorf("open the harness journal backend: %w", err)
	}
	defer func() { _ = journal.Close(context.Background()) }()

	tb := kindlane.NewProcessTB("kindhost", log)
	defer tb.RunCleanups()
	world := orchestrationtest.NewPooledWorld(tb, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:         []sessionwire.TenantID{kindlane.Tenant},
		Backend:         store.Composite,
		JournalBackends: map[sessionwire.TenantID]*storage.Composite{kindlane.Tenant: journal.Composite},
	})

	blueprint, _ := orchestrationtest.DedicatedHostComposition(tb, world,
		sessionwire.HostID(env.str["HOST_ID"]), env.count["HOST_GENERATION"],
		sessionwire.SessionID(env.str["HOST_FIXED_SESSION_ID"]),
		sessionwire.InternalEndpoint(env.str["HOST_INTERNAL_ENDPOINT"]))
	options := &blueprint.Options
	options.IsolationClass = sessionwire.HostIsolationClass(env.str["HOST_ISOLATION_CLASS"])
	options.Placement = sessionwire.HostPlacement(env.str["HOST_PLACEMENT"])
	options.Capacity = env.count["HOST_CAPACITY"]
	options.WarmTTL = env.dur["HOST_WARM_TTL"]
	options.RegistryHeartbeat = env.dur["HOST_REGISTRY_HEARTBEAT"]
	options.RegistryExpiry = env.dur["HOST_REGISTRY_EXPIRY"]
	options.ClaimTTL = env.dur["HOST_CLAIM_TTL"]
	options.ApplyDeadline = env.dur["HOST_APPLY_DEADLINE"]
	options.CommandQueueSize = int(env.count["HOST_COMMAND_QUEUE_SIZE"])
	options.ReconcileInterval = env.dur["HOST_RECONCILE_INTERVAL"]
	options.ReconcileBatch = int(env.count["HOST_RECONCILE_BATCH"])
	blueprint.Link.MaxBindingsPerLink = int(env.count["HOST_MAX_BINDINGS_PER_LINK"])
	blueprint.Link.MaxBindings = int(env.count["HOST_MAX_BINDINGS"])
	blueprint.Link.MaxTenantLinks = int(env.count["HOST_MAX_TENANT_LINKS"])
	blueprint.Drain = host.DrainOptions{
		Grace:        env.dur["HOST_DRAIN_GRACE"],
		IdleBoundary: env.dur["HOST_DRAIN_IDLE_BOUNDARY"],
		PublishBound: env.dur["HOST_DRAIN_PUBLISH_BOUND"],
	}
	blueprint.CompatibilityTimeout = env.dur["HOST_COMPATIBILITY_TIMEOUT"]
	blueprint.WorkPoll = env.dur["HOST_WORK_POLL"]
	blueprint.Collaborators.Auth = kindlane.TokenVerifier{Tokens: []string{factoryToken, controllerToken}}
	blueprint.Collaborators.Logger = log

	log.Info("kindhost starting",
		"host_id", env.str["HOST_ID"], "generation", env.count["HOST_GENERATION"],
		"fixed_session", env.str["HOST_FIXED_SESSION_ID"], "base", env.str["HOST_INTERNAL_ENDPOINT"],
		"drain_grace", env.dur["HOST_DRAIN_GRACE"].String())
	// The SIGTERM-to-exit time is the lane's measurement of the commit
	// margin the controller's grace period must cover (runbook 07 D2.2 step 3).
	signalled := make(chan time.Time, 1)
	go func() {
		<-ctx.Done()
		signalled <- time.Now()
		log.Info("kindhost signalled; draining")
	}()
	err = host.Run(ctx, blueprint, host.ListenOptions{Address: env.str["HOST_LISTEN_ADDRESS"]})
	select {
	case at := <-signalled:
		took := time.Since(at)
		log.Info("kindhost drained after signal", "signal_to_exit_ms", took.Milliseconds(), "error", fmt.Sprint(err))
		// The kubelet mounts /dev/termination-log even on a read-only root
		// filesystem and copies it into the Pod's container status, where it
		// outlives the container's logs.
		_ = os.WriteFile("/dev/termination-log", []byte(fmt.Sprintf("signal_to_exit_ms=%d error=%v", took.Milliseconds(), err)), 0o600)
	default:
	}
	return err
}

type environment struct {
	str   map[string]string
	count map[string]uint64
	dur   map[string]time.Duration
}

var (
	stringVars   = []string{"HOST_ID", "HOST_INTERNAL_ENDPOINT", "HOST_ISOLATION_CLASS", "HOST_PLACEMENT", "HOST_FIXED_SESSION_ID", "HOST_LISTEN_ADDRESS"}
	countVars    = []string{"HOST_GENERATION", "HOST_CAPACITY", "HOST_COMMAND_QUEUE_SIZE", "HOST_RECONCILE_BATCH", "HOST_MAX_BINDINGS_PER_LINK", "HOST_MAX_BINDINGS", "HOST_MAX_TENANT_LINKS"}
	durationVars = []string{"HOST_WARM_TTL", "HOST_REGISTRY_HEARTBEAT", "HOST_REGISTRY_EXPIRY", "HOST_CLAIM_TTL", "HOST_APPLY_DEADLINE", "HOST_RECONCILE_INTERVAL", "HOST_DRAIN_GRACE", "HOST_DRAIN_IDLE_BOUNDARY", "HOST_DRAIN_PUBLISH_BOUND", "HOST_COMPATIBILITY_TIMEOUT", "HOST_WORK_POLL"}
)

func readEnv() (environment, error) {
	env := environment{str: map[string]string{}, count: map[string]uint64{}, dur: map[string]time.Duration{}}
	for _, name := range stringVars {
		value, err := kindlane.Lookup(name)
		if err != nil {
			return env, err
		}
		env.str[name] = value
	}
	for _, name := range countVars {
		value, err := kindlane.Lookup(name)
		if err != nil {
			return env, err
		}
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil || parsed == 0 {
			return env, fmt.Errorf("kindhost: %s must be a positive count", name)
		}
		env.count[name] = parsed
	}
	for _, name := range durationVars {
		value, err := kindlane.Lookup(name)
		if err != nil {
			return env, err
		}
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			return env, fmt.Errorf("kindhost: %s must be a positive duration", name)
		}
		env.dur[name] = parsed
	}
	return env, nil
}
