//go:build integration && kind

// Command oldhost serves the released tests v0.13.2 kit's Host v0.10.3 in a
// separate process for the mixed-fleet attribution probe. It shares only the
// NATS durable plane with the parent test process, never a local Go workspace.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/natsstore"
	"github.com/looprig/tests/internal/kindlane"
	"github.com/looprig/tests/internal/orchestrationtest"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	openCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	store, err := natsstore.Open(openCtx, natsstore.Options{URL: os.Getenv("OLDHOST_STORE_URL")})
	cancel()
	if err != nil {
		log.Error("open store", "error", err.Error())
		os.Exit(1)
	}
	defer func() { _ = store.Close(context.Background()) }()
	tb := kindlane.NewProcessTB("oldhost", log)
	defer tb.RunCleanups()
	world := orchestrationtest.NewPooledWorld(tb, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{orchestrationtest.PooledTenantA}, Backend: store.Composite,
	})
	h := orchestrationtest.StartPooledHost(tb, ctx, world, sessionwire.HostID(os.Getenv("OLDHOST_ID")), 1)
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"id": string(h.ID), "base": string(h.Base)}); err != nil {
		log.Error("announce host", "error", err.Error())
		os.Exit(1)
	}
	<-ctx.Done()
}
