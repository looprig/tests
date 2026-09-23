//go:build integration

package orchestrationtest

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// ScriptedHost is a HostLink server whose every answer comes from the case.
//
// It exists for one question the wire-freeze lane asks: does a REAL Factory
// still accept a Host's side of the wire as it was frozen? The case supplies
// the frozen replies; this supplies only the transport a Host serves them on --
// the tenant path Core derives, the JSON subprotocol gate a real Host enforces,
// and centrifuge's own framing -- so a Factory that stopped naming the
// subprotocol would be refused here exactly as a real Host refuses it.
//
// It is not a Host: it holds no residency and applies nothing. What it says is
// what the case scripted, and a case that asserts about Factory must script
// what a real Host said (the frozen fixtures), not what it wishes one said.
type ScriptedHost struct {
	Base       sessionwire.InternalEndpoint
	ID         sessionwire.HostID
	Generation uint64

	node *centrifuge.Node

	mu         sync.Mutex
	rpcs       []string
	subscribed map[string]bool
}

// ScriptedHostConfig is what a case scripts.
type ScriptedHostConfig struct {
	ID         sessionwire.HostID
	Generation uint64
	// Wrap, when set, goes in front of the handler (a HostLinkTap).
	Wrap func(http.Handler) http.Handler
	// Connect answers a connect's Data with the reply Data, or refuses it.
	Connect func(data []byte) ([]byte, error)
	// RPC answers one RPC with its reply Data (nil for none), or an error,
	// which the transport answers as a centrifuge error.
	RPC func(method string, data []byte) ([]byte, error)
}

// StartScriptedHost serves one on loopback and registers its shutdown.
func StartScriptedHost(tb TB, cfg ScriptedHostConfig) *ScriptedHost {
	tb.Helper()
	node, err := centrifuge.New(centrifuge.Config{Name: "orchestrationtest-scripted", Version: "v1"})
	if err != nil {
		tb.Fatalf("orchestrationtest: building the scripted host node: %v", err)
		return nil
	}
	scripted := &ScriptedHost{ID: cfg.ID, Generation: cfg.Generation, node: node, subscribed: map[string]bool{}}
	node.OnConnecting(func(_ context.Context, event centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		data, err := cfg.Connect(event.Data)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.Disconnect{Code: 4501, Reason: err.Error()}
		}
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: "orchestrationtest-scripted"},
			Data:        data,
		}, nil
	})
	node.OnConnect(func(client *centrifuge.Client) {
		client.OnRPC(func(event centrifuge.RPCEvent, callback centrifuge.RPCCallback) {
			scripted.mu.Lock()
			scripted.rpcs = append(scripted.rpcs, event.Method)
			scripted.mu.Unlock()
			data, err := cfg.RPC(event.Method, event.Data)
			if err != nil {
				callback(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
				return
			}
			callback(centrifuge.RPCReply{Data: data}, nil)
		})
		client.OnSubscribe(func(event centrifuge.SubscribeEvent, callback centrifuge.SubscribeCallback) {
			scripted.mu.Lock()
			scripted.subscribed[event.Channel] = true
			scripted.mu.Unlock()
			callback(centrifuge.SubscribeReply{}, nil)
		})
	})
	if err := node.Run(); err != nil {
		tb.Fatalf("orchestrationtest: running the scripted host node: %v", err)
		return nil
	}
	websockets := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		CheckOrigin: func(*http.Request) bool { return true },
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("orchestrationtest: opening the scripted host listener: %v", err)
		return nil
	}
	var handler http.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !strings.HasPrefix(request.URL.Path, sessionwire.HostLinkPathPrefix) {
			http.NotFound(writer, request)
			return
		}
		if !strings.Contains(request.Header.Get("Sec-WebSocket-Protocol"), "centrifuge-json") {
			http.Error(writer, "HostLink requires the JSON protocol", http.StatusBadRequest)
			return
		}
		websockets.ServeHTTP(writer, request)
	})
	if cfg.Wrap != nil {
		handler = cfg.Wrap(handler)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}}
	server.Start()
	scripted.Base = sessionwire.InternalEndpoint("ws://" + listener.Addr().String())
	tb.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = node.Shutdown(shutdown)
	})
	return scripted
}

// RPCs reports every RPC method a client sent, in order.
func (h *ScriptedHost) RPCs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.rpcs...)
}

// Subscribed reports whether a client subscribed to channel.
func (h *ScriptedHost) Subscribed(channel string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.subscribed[channel]
}

// Publish pushes data to every subscriber of channel.
func (h *ScriptedHost) Publish(tb TB, channel string, data []byte) {
	tb.Helper()
	if _, err := h.node.Publish(channel, data); err != nil {
		tb.Fatalf("orchestrationtest: the scripted host's publish on %s: %v", channel, err)
	}
}

// PublishScriptedHostTarget advertises the scripted Host as a pooled launch
// target for the world's agent, as a real Host's capacity report would.
func PublishScriptedHostTarget(tb TB, ctx context.Context, world *PooledWorld, h *ScriptedHost) {
	tb.Helper()
	now := time.Now().UTC()
	if _, err := world.Store.PublishHostTarget(ctx, sessionstore.PublishHostTargetRequest{
		Key: sessionstore.HostTargetKey{
			AgentID: PooledAgent, RuntimeCompatibilityID: string(PooledCompatibility), Placement: sessionwire.HostPlacementPooled,
		},
		HostID:         h.ID,
		HostGeneration: h.Generation,
		ObservedAt:     now,
		Advertisement: sessionstore.HostAdvertisement{
			InternalEndpoint:  h.Base,
			IsolationClass:    sessionwire.HostIsolationClassCrossTenantIsolated,
			Accepting:         true,
			AvailableCapacity: 8,
			ExpiresAt:         now.Add(2 * time.Minute),
		},
	}); err != nil {
		tb.Fatalf("orchestrationtest: publishing the scripted host's target: %v", err)
	}
}

// RegisterScriptedHost records the scripted Host as the session's owner at
// lease epoch 1, as a real Host's attach does. A second call for a session it
// already owns is a no-op.
func RegisterScriptedHost(tb TB, ctx context.Context, world *PooledWorld, tenant sessionwire.TenantID, s sessionwire.SessionID, h *ScriptedHost) {
	tb.Helper()
	if current, err := world.Store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{
		TenantID: tenant, SessionID: s,
	}); err == nil && current.Registration.Route.HostID == h.ID {
		return
	}
	now := time.Now().UTC()
	if _, err := world.Store.PutHostRegistration(ctx, sessionstore.PutHostRegistrationRequest{
		TenantID:   tenant,
		SessionID:  s,
		LeaseEpoch: 1,
		ObservedAt: now,
		ExpiresAt:  now.Add(2 * time.Minute),
		Route: sessionstore.HostRoute{
			HostID:                 h.ID,
			HostGeneration:         h.Generation,
			AgentID:                PooledAgent,
			RuntimeCompatibilityID: string(PooledCompatibility),
			Placement:              sessionwire.HostPlacementPooled,
			InternalEndpoint:       h.Base,
			Residency:              sessionwire.SessionResidencyResident,
			Accepting:              true,
		},
	}); err != nil {
		tb.Errorf("orchestrationtest: registering the scripted host for %s: %v", s, err)
	}
}
