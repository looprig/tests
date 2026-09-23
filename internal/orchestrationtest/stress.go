//go:build integration

package orchestrationtest

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// This file is the kit's surface for the I3.1 race stress: the same real
// Factory, Host and harness runtimes the cross-module lanes use, with the three
// things a many-goroutine driver needs that those lanes do not.
//
//   - A Host that can be SIZED and made to WARM-RELEASE quickly. The lanes'
//     Host holds eight sessions for ninety seconds, which is the right Host for
//     one session and the wrong one for hundreds.
//   - A viewer that REPORTS a failure instead of calling Fatalf. Fatalf from a
//     goroutine that is not the test's own stops only that goroutine, so a
//     driver built on the fatal helpers hangs on its WaitGroup instead of
//     failing.
//   - A metrics scrape, which is how a queue bound is observed without
//     reaching into a Host's internals.

// StartSizedPooledHost composes, starts and serves one pooled Host with the
// supplied identity and sizing, over the world's shared plane. The sizing
// fields of PooledHostConfig (Capacity, WarmTTL, MaxBindingsPerLink,
// MaxBindings, CommandQueueSize) are the ones a many-session driver turns.
func StartSizedPooledHost(tb TB, ctx context.Context, world *PooledWorld, cfg PooledHostConfig) *PooledHost {
	tb.Helper()
	return startHostConfigured(tb, ctx, world, cfg.ID, cfg.Generation, "", nil, cfg)
}

// ScrapeMetrics reads the Host's own Prometheus exposition and returns every
// unlabelled sample and every labelled sample keyed as name{labels}.
//
// It reads the exposition text rather than the collectors because the text is
// the Host's public observability contract; a collector is an implementation.
func (h *PooledHost) ScrapeMetrics() (map[string]float64, error) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.Service.MetricsHandler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		return nil, fmt.Errorf("orchestrationtest: host %s metrics answered %d", h.ID, recorder.Code)
	}
	samples := map[string]float64{}
	scanner := bufio.NewScanner(recorder.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cut := strings.LastIndexByte(line, ' ')
		if cut < 0 {
			continue
		}
		value, err := strconv.ParseFloat(line[cut+1:], 64)
		if err != nil {
			continue
		}
		samples[line[:cut]] = value
	}
	return samples, scanner.Err()
}

// SumMetric adds every sample of one metric family, across its labels.
func SumMetric(samples map[string]float64, name string) float64 {
	total := 0.0
	for key, value := range samples {
		if key == name || strings.HasPrefix(key, name+"{") {
			total += value
		}
	}
	return total
}

// DialPooledViewer is ConnectPooledViewer that returns its failure. It is safe
// to call from any goroutine.
func DialPooledViewer(ctx context.Context, f *PooledFactory, tenant sessionwire.TenantID) (*PooledViewer, error) {
	endpoint := "ws" + strings.TrimPrefix(f.BaseURL, "http") + "/v1/realtime"
	client := centrifugego.NewJsonClient(endpoint, centrifugego.Config{
		Token: PooledBearers[tenant],
		Data:  []byte(`{"protocol_version":"1"}`),
		Header: http.Header{
			"Authorization": {"Bearer " + PooledBearers[tenant]},
			"Origin":        {f.BaseURL},
		},
		Name:             "orchestrationtest-stress-viewer",
		HandshakeTimeout: 10 * time.Second,
		LogLevel:         centrifugego.LogLevelNone,
	})
	connected := make(chan struct{}, 1)
	client.OnConnected(func(centrifugego.ConnectedEvent) {
		select {
		case connected <- struct{}{}:
		default:
		}
	})
	if err := client.Connect(); err != nil {
		client.Close()
		return nil, fmt.Errorf("orchestrationtest: viewer for %q could not connect: %w", tenant, err)
	}
	select {
	case <-connected:
	case <-ctx.Done():
		client.Close()
		return nil, fmt.Errorf("orchestrationtest: viewer for %q did not connect: %w", tenant, ctx.Err())
	}
	return &PooledViewer{Tenant: tenant, client: client}, nil
}

// Subscribe is Watch that returns every failure, including a build or send
// failure, instead of calling Fatalf. A refusal and a transport failure are
// both errors; the caller decides which it expected.
func (v *PooledViewer) Subscribe(ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID) error {
	sub, err := v.client.NewSubscription(ClientLinkChannel(tenant, s))
	if err != nil {
		return fmt.Errorf("orchestrationtest: building a subscription: %w", err)
	}
	answered := make(chan error, 1)
	sub.OnSubscribed(func(centrifugego.SubscribedEvent) {
		select {
		case answered <- nil:
		default:
		}
	})
	sub.OnError(func(e centrifugego.SubscriptionErrorEvent) {
		select {
		case answered <- e.Error:
		default:
		}
	})
	sub.OnPublication(func(e centrifugego.PublicationEvent) {
		summary, recordTenant, recordSession := pooledSummarise(e.Data)
		v.mu.Lock()
		defer v.mu.Unlock()
		v.records = append(v.records, summary)
		v.arrived = append(v.arrived, time.Now().Format("15:04:05.000000")+" "+summary)
		if recordTenant != tenant || recordSession != s {
			v.strays = append(v.strays, fmt.Sprintf("%s(%s/%s)", summary, recordTenant, recordSession))
		}
	})
	if err := sub.Subscribe(); err != nil {
		return fmt.Errorf("orchestrationtest: subscribing: %w", err)
	}
	select {
	case err := <-answered:
		return err
	case <-ctx.Done():
		return fmt.Errorf("orchestrationtest: the subscribe was never answered: %w", ctx.Err())
	}
}

// Connected reports whether the viewer's client is connected now.
func (v *PooledViewer) Connected() bool {
	return v.client.State() == centrifugego.StateConnected
}
