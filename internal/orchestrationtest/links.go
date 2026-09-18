//go:build integration

package orchestrationtest

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// This file holds the two REAL WebSocket clients the Factory ↔ Host lane drives
// and the raw probes its B8 regression controls need. None of them is a
// stand-in for Factory: Factory's own HostLink dialer is what connects to the
// Host, and the only way to make it dial from outside the module is to give it
// demand -- a ClientLink subscriber watching a session -- which is what
// ClientLinkViewer is.

// ClientLinkChannel is Factory's ClientLink session channel,
// "session:<tenant>:<session>" (factory internal/realtime/clientlink/demand.go).
// It is restated because Factory exports no helper for it; a drift would fail
// the subscribe loudly, not silently.
func ClientLinkChannel(tenant sessionwire.TenantID, session sessionwire.SessionID) string {
	return "session:" + string(tenant) + ":" + string(session)
}

// ClientLinkViewer is one browser-shaped ClientLink connection to a Factory.
type ClientLinkViewer struct {
	client *centrifugego.Client
}

// ConnectClientLink connects a ClientLink viewer to f, bounded by ctx.
func ConnectClientLink(tb TB, ctx context.Context, f *FactoryFixture) *ClientLinkViewer {
	tb.Helper()
	endpoint := "ws" + strings.TrimPrefix(f.BaseURL, "http") + "/v1/realtime"
	client := centrifugego.NewJsonClient(endpoint, centrifugego.Config{
		Token: KitActorCredential,
		Data:  []byte(`{"protocol_version":"1"}`),
		Header: http.Header{
			"Authorization": {"Bearer " + KitActorCredential},
			"Origin":        {f.BaseURL},
		},
		Name:             "orchestrationtest-viewer",
		HandshakeTimeout: 5 * time.Second,
		LogLevel:         centrifugego.LogLevelNone,
	})
	connected := make(chan struct{}, 1)
	disconnected := make(chan centrifugego.DisconnectedEvent, 1)
	client.OnConnected(func(centrifugego.ConnectedEvent) {
		select {
		case connected <- struct{}{}:
		default:
		}
	})
	client.OnDisconnected(func(e centrifugego.DisconnectedEvent) {
		select {
		case disconnected <- e:
		default:
		}
	})
	if err := client.Connect(); err != nil {
		client.Close()
		tb.Fatalf("orchestrationtest: ClientLink connect: %v", err)
		return nil
	}
	tb.Cleanup(client.Close)
	select {
	case <-connected:
	case e := <-disconnected:
		tb.Fatalf("orchestrationtest: the ClientLink was refused: code=%d reason=%q", e.Code, e.Reason)
		return nil
	case <-ctx.Done():
		tb.Fatalf("orchestrationtest: the ClientLink did not connect: %v", ctx.Err())
		return nil
	}
	return &ClientLinkViewer{client: client}
}

// Watch subscribes to a session's channel and returns once Factory answered.
//
// FACTORY'S ANSWER IS NOT THE HOST'S. A subscribe succeeds whether or not the
// HostLink bind behind it did: routing.Demand.bindLocked discards a bind error
// by design and serves the journal-tip hint instead. So a successful Watch says
// only that demand is held; what happened on HostLink is read off the tap.
func (v *ClientLinkViewer) Watch(tb TB, ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) {
	tb.Helper()
	channel := ClientLinkChannel(tenant, session)
	sub, err := v.client.NewSubscription(channel)
	if err != nil {
		tb.Fatalf("orchestrationtest: a subscription to %s: %v", channel, err)
		return
	}
	subscribed := make(chan struct{}, 1)
	failed := make(chan error, 1)
	sub.OnSubscribed(func(centrifugego.SubscribedEvent) {
		select {
		case subscribed <- struct{}{}:
		default:
		}
	})
	sub.OnError(func(e centrifugego.SubscriptionErrorEvent) {
		select {
		case failed <- e.Error:
		default:
		}
	})
	sub.OnUnsubscribed(func(e centrifugego.UnsubscribedEvent) {
		select {
		case failed <- &unsubscribedError{code: e.Code, reason: e.Reason}:
		default:
		}
	})
	if err := sub.Subscribe(); err != nil {
		tb.Fatalf("orchestrationtest: subscribing to %s: %v", channel, err)
		return
	}
	select {
	case <-subscribed:
	case err := <-failed:
		tb.Fatalf("orchestrationtest: Factory refused the subscription to %s: %v", channel, err)
	case <-ctx.Done():
		tb.Fatalf("orchestrationtest: Factory did not answer the subscription to %s: %v", channel, ctx.Err())
	}
}

type unsubscribedError struct {
	code   uint32
	reason string
}

func (e *unsubscribedError) Error() string {
	return "unsubscribed: " + e.reason
}

// RawUpgradeStatus issues a syntactically complete WebSocket upgrade to url and
// reports the status the server answered, closing whatever it got.
//
// subprotocol "" sends NO Sec-WebSocket-Protocol header at all, which is what
// centrifuge-go's JSON client sent before factory v0.2.0 and what Host's
// JSON-protocol gate answers 400. It is a raw request rather than a client
// library call because a library reports "bad handshake" for every non-101,
// which loses the one number worth recording.
func RawUpgradeStatus(tb TB, ctx context.Context, url, credential, subprotocol string) int {
	tb.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		tb.Fatalf("orchestrationtest: building the raw upgrade: %v", err)
		return 0
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Authorization", "Bearer "+credential)
	if subprotocol != "" {
		req.Header.Set("Sec-WebSocket-Protocol", subprotocol)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		tb.Fatalf("orchestrationtest: the raw upgrade to %s: %v", url, err)
		return 0
	}
	// A 101's body is the upgraded CONNECTION: reading it would wait on the
	// peer, and client.Timeout does not bound it, so it is closed unread. A
	// refusal's body is its text, drained a little and closed.
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_, _ = io.CopyN(io.Discard, resp.Body, 512)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// RawConnectResult is what a raw HostLink connect produced.
type RawConnectResult struct {
	Connected    bool
	ReplyData    []byte
	Disconnected bool
	Code         uint32
	Reason       string
	Err          error
}

// RawHostLinkConnect drives a plain centrifuge-go JSON client at a Host with
// caller-chosen connect Data, and reports the connect reply or the disconnect,
// bounded by timeout. It exists for the B8 framing control: the connect Data is
// the one thing Factory's own dialer will not let a caller choose.
//
// subprotocol "" omits the Sec-WebSocket-Protocol header.
func RawHostLinkConnect(tb TB, endpoint sessionwire.InternalEndpoint, credential, subprotocol string, data []byte, timeout time.Duration) RawConnectResult {
	tb.Helper()
	header := http.Header{}
	if subprotocol != "" {
		header.Set("Sec-WebSocket-Protocol", subprotocol)
	}
	client := centrifugego.NewJsonClient(string(endpoint), centrifugego.Config{
		Token:            credential,
		Data:             data,
		Name:             "orchestrationtest-raw",
		Header:           header,
		HandshakeTimeout: timeout,
		LogLevel:         centrifugego.LogLevelNone,
	})
	defer client.Close()
	connected := make(chan []byte, 1)
	disconnected := make(chan centrifugego.DisconnectedEvent, 1)
	failed := make(chan error, 1)
	client.OnConnected(func(e centrifugego.ConnectedEvent) {
		select {
		case connected <- append([]byte(nil), e.Data...):
		default:
		}
	})
	client.OnDisconnected(func(e centrifugego.DisconnectedEvent) {
		select {
		case disconnected <- e:
		default:
		}
	})
	client.OnError(func(e centrifugego.ErrorEvent) {
		select {
		case failed <- e.Error:
		default:
		}
	})
	if err := client.Connect(); err != nil {
		return RawConnectResult{Err: err}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case reply := <-connected:
		return RawConnectResult{Connected: true, ReplyData: reply}
	case e := <-disconnected:
		return RawConnectResult{Disconnected: true, Code: e.Code, Reason: e.Reason}
	case err := <-failed:
		// A transport error (a refused upgrade among them) is reported as such,
		// so a caller asserting a disconnect code fails on it by ASSERTION
		// instead of waiting out the timer.
		return RawConnectResult{Err: err}
	case <-timer.C:
		tb.Fatalf("orchestrationtest: a raw HostLink connect neither connected nor was refused within %s", timeout)
		return RawConnectResult{}
	}
}
