//go:build integration

package orchestrationtest

import (
	"context"
	"encoding/json"
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

// IncapableHost is a HostLink server that speaks Core's connect framing and
// advertises EXACTLY the five reserved methods -- and no capability token.
//
// # Why this exists at all
//
// host.Compose ALWAYS wires both gate seams, so every composed v0.4.0 Host
// advertises Core's hostlink.command.gate_response token. The composition that
// does not is unreachable through Host's public surface, and inventing a
// crippled composition would be asserting about a Host nobody ships.
//
// What IS shipped, and what this stands for, is the MIXED FLEET: a v0.3.0 Host,
// which has no gate code at all, owning a session a browser then tries to
// answer. A v0.3.0 Host's connect reply carries exactly the five methods, which
// is byte for byte what this server sends -- through Core's own encoder, so a
// change to the reply's shape moves this stand-in with it rather than leaving
// it pinned to a spelling.
//
// # What it is NOT
//
// It is not a Host. It holds no session, applies no command and answers every
// RPC with an empty object. It exists for one question, asked before any of
// that would matter: what does Factory do with a gate response whose OWNER does
// not advertise the token?
type IncapableHost struct {
	// Base is the bare HostLink base this stand-in advertises, in the same
	// shape a real Host advertises one.
	Base sessionwire.InternalEndpoint

	// ID and Generation are the identity a registration takeover names.
	ID         sessionwire.HostID
	Generation uint64

	node   *centrifuge.Node
	server *httptest.Server

	mu       sync.Mutex
	connects int
	rpcs     []string
}

// StartIncapableHost serves one on loopback and registers its shutdown.
func StartIncapableHost(tb TB, ctx context.Context) *IncapableHost {
	tb.Helper()
	node, err := centrifuge.New(centrifuge.Config{Name: "orchestrationtest-incapable", Version: "v1"})
	if err != nil {
		tb.Fatalf("orchestrationtest: building the incapable host node: %v", err)
		return nil
	}
	standin := &IncapableHost{ID: "orchestrationtest-incapable-standin", Generation: 9, node: node}

	node.OnConnecting(func(_ context.Context, event centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		// Core's framing, both directions, exactly as a Host does it: the
		// connect Data is the BARE request and the reply is the BARE response.
		// A wrapped request is refused by Core's strict decoder, which is the
		// B8 regression this stand-in must not re-introduce.
		request, err := sessionwire.DecodeHostLinkConnectRequest(event.Data)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.Disconnect{Code: 4501, Reason: "unsupported wire version"}
		}
		response, err := sessionwire.NegotiateVersion(request)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.Disconnect{Code: 4501, Reason: "unsupported wire version"}
		}
		// THE FIVE RESERVED METHODS AND NOTHING ELSE. This is the whole of what
		// the stand-in is: a Host that can bind, unbind, attach and drain, and
		// cannot apply a gate response.
		response = response.WithHostLinkMethods(
			sessionwire.HostLinkMethodBind,
			sessionwire.HostLinkMethodUnbind,
			sessionwire.HostLinkMethodAttach,
			sessionwire.HostLinkMethodDrain,
			sessionwire.HostLinkMethodDrainStatus,
		)
		data, err := sessionwire.EncodeHostLinkConnectReply(response)
		if err != nil {
			return centrifuge.ConnectReply{}, err
		}
		standin.mu.Lock()
		standin.connects++
		standin.mu.Unlock()
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: "orchestrationtest-incapable"},
			Data:        data,
		}, nil
	})
	node.OnConnect(func(client *centrifuge.Client) {
		client.OnRPC(func(event centrifuge.RPCEvent, callback centrifuge.RPCCallback) {
			standin.mu.Lock()
			standin.rpcs = append(standin.rpcs, event.Method)
			standin.mu.Unlock()
			callback(centrifuge.RPCReply{Data: json.RawMessage(`{}`)}, nil)
		})
		client.OnSubscribe(func(_ centrifuge.SubscribeEvent, callback centrifuge.SubscribeCallback) {
			callback(centrifuge.SubscribeReply{}, nil)
		})
	})
	if err := node.Run(); err != nil {
		tb.Fatalf("orchestrationtest: running the incapable host node: %v", err)
		return nil
	}

	websockets := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		CheckOrigin: func(*http.Request) bool { return true },
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("orchestrationtest: opening the incapable host listener: %v", err)
		return nil
	}
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// The tenant path Core derives, and the JSON subprotocol gate a real
		// Host enforces. Without the gate, a Factory that stopped naming the
		// subprotocol would still connect here and the stand-in would be more
		// permissive than the thing it stands for.
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
	server := &httptest.Server{Listener: listener, Config: &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}}
	server.Start()
	standin.server = server
	standin.Base = sessionwire.InternalEndpoint("ws://" + listener.Addr().String())

	tb.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = node.Shutdown(shutdown)
	})
	return standin
}

// Connects reports how many HostLink connects this stand-in answered. It is the
// non-vacuity guard for a case asserting a capability refusal: a refusal
// reached without any connect would be Factory failing to dial, not Factory
// reading a capability set.
func (h *IncapableHost) Connects() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.connects
}

// RPCs reports every method Factory called on this stand-in, in order.
func (h *IncapableHost) RPCs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.rpcs...)
}

// TakeOverRegistration republishes one session's Host registration so that the
// stand-in is its observed owner, at a strictly higher lease epoch.
//
// A higher epoch is what a re-placement looks like in the registry, and it is
// also what the store requires: the registration's epoch only ratchets upward.
// The route is otherwise the one a real Host publishes -- a BARE base, resident
// and accepting -- so Factory derives the tenant's address from it exactly as
// it would for any Host.
func TakeOverRegistration(tb TB, ctx context.Context, world *PooledWorld, tenant sessionwire.TenantID, s sessionwire.SessionID, standin *IncapableHost) {
	tb.Helper()
	current, err := world.Store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{
		TenantID: tenant, SessionID: s,
	})
	if err != nil {
		tb.Fatalf("orchestrationtest: reading %s's registration before the takeover: %v", s, err)
		return
	}
	now := time.Now().UTC()
	if _, err := world.Store.PutHostRegistration(ctx, sessionstore.PutHostRegistrationRequest{
		TenantID:   tenant,
		SessionID:  s,
		LeaseEpoch: current.Registration.LeaseEpoch + 1,
		ObservedAt: now,
		ExpiresAt:  now.Add(2 * time.Minute),
		Route: sessionstore.HostRoute{
			HostID:                 standin.ID,
			HostGeneration:         standin.Generation,
			AgentID:                PooledAgent,
			RuntimeCompatibilityID: string(PooledCompatibility),
			Placement:              sessionwire.HostPlacementPooled,
			InternalEndpoint:       standin.Base,
			Residency:              sessionwire.SessionResidencyResident,
			Accepting:              true,
		},
	}); err != nil {
		tb.Fatalf("orchestrationtest: taking %s's registration over for the stand-in: %v", s, err)
	}
}
