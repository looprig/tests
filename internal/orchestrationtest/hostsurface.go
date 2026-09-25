//go:build integration

package orchestrationtest

import (
	"context"
	"slices"
	"sort"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// This file holds the Host trip-wires, and as of host v0.2.1 they read a
// CAPABILITY THE HOST DECLARES ON THE WIRE rather than any name in its Go API.
//
// # What happened to the old wires
//
// AssertHostExposesNoRuntimeCapability and AssertHostExposesNoDrainCapability
// scanned host's whole exported declaration surface for runtime and drain verbs.
// Both FIRED on the host v0.2.1 pin -- `Attach`, `Handler`, `Run`, `Start` and
// `Stop` arrived with B1's exported composition -- which is what they were for.
// They are DELETED rather than inverted, by this kit's standing rule
// (blocked.go): a trip-wire kept after its blocker lifts becomes a claim about
// the past that a later reader takes for a claim about now. The runtime they
// guarded against is now driven for real by ComposedHost.
//
// # Why the replacement is not another name scan
//
// "Fire on capability, not spelling" had a precise meaning for the old wires --
// a verb on ANY type, not just *host.Host -- and it has a sharper one now.
// Host itself publishes its capability set: every HostLink connect reply carries
// Core's optional hostlink_methods (core v0.9.0), derived from Host's own
// dispatch table, and Factory gates Bind, Unbind and Attach on it. That set is
// the contract a Factory acts on, so it is the thing to pin. A renamed Go method
// changes none of it; a Host that stops dispatching hostlink.attach, or starts
// dispatching something new, changes it whatever the Go API is called.
//
// The drain wire's lane (runbook 07 I2.3) is therefore no longer blocked on
// Host: hostlink.drain and hostlink.drain_status are in the set. What I2.3
// still lacks is a DRAIN CALLER -- Factory has none and the D2.2 ruling puts it
// in looprig/controller -- which is not a Host capability and is not pinned here.

// HostLinkSurfaceAtV011 is the exact hostlink_methods set released host v0.11.0
// advertises, spelled in Core's constants.
//
// The first six entries are the five RPC methods plus the gate-response
// capability token. host v0.4.0
// advertises Core's capability TOKEN hostlink.command.gate_response in the same
// reply member, after the five reserved methods, and only when its composition
// wires both gate seams (host.Compose always does). Nothing dispatches it: it
// is the signal factory v0.5.0's GateResponseCapable reads to decide whether to
// admit a gate response at all, and a Factory that does not see it answers
// 409 gate_not_resumable. The two kinds share one member because Core's
// negotiation reply is the only HostLink record with a TOLERANT decoder -- a
// new field on the capacity report or the registry observation would be refused
// by every existing Factory.
//
// The seventh entry, hostlink.attribution.principal, arrived in host v0.11.0.
// Factory v0.12.0 gates stamped placement and dispatch on this exact token.
// Thus both added capabilities are backed by Factory gates, not inferred from
// Host registration or silently accepted as an arbitrary change to the wire.
func HostLinkSurfaceAtV011() []string {
	return []string{
		sessionwire.HostLinkMethodBind,
		sessionwire.HostLinkMethodUnbind,
		sessionwire.HostLinkMethodAttach,
		sessionwire.HostLinkMethodDrain,
		sessionwire.HostLinkMethodDrainStatus,
		sessionwire.HostLinkCapabilityGateResponse,
		sessionwire.HostLinkCapabilityAttributionPrincipal,
	}
}

// DecodeHostCapabilities decodes a Host's connect reply Data with Core's own
// connect codec -- the one factory v0.2.0 decodes with -- and fails the case if
// Core refuses it. A reply Core refuses is a Host no Factory can connect to.
func DecodeHostCapabilities(tb TB, replyData []byte) sessionwire.VersionNegotiationResponse {
	tb.Helper()
	response, err := sessionwire.DecodeHostLinkConnectReply(replyData)
	if err != nil {
		tb.Fatalf("orchestrationtest: Core refused the Host's connect reply %s: %v", replyData, err)
		return sessionwire.VersionNegotiationResponse{}
	}
	return response
}

// ProbeHostCapabilities connects a raw Core-framed client to a running Host and
// returns the negotiation it answered. It is a CAPABILITY read: it asks the
// running process, not its source.
func ProbeHostCapabilities(tb TB, h *ComposedHost) sessionwire.VersionNegotiationResponse {
	tb.Helper()
	request, err := sessionwire.EncodeHostLinkConnectRequest(sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
	})
	if err != nil {
		tb.Fatalf("orchestrationtest: encoding Core's connect request: %v", err)
		return sessionwire.VersionNegotiationResponse{}
	}
	result := RawHostLinkConnect(tb, h.Endpoint, KitServiceToken, "centrifuge-json", request, 5*time.Second)
	if !result.Connected {
		tb.Fatalf("orchestrationtest: the capability probe did not connect: %+v", result)
		return sessionwire.VersionNegotiationResponse{}
	}
	return DecodeHostCapabilities(tb, result.ReplyData)
}

// HostAdvertises dials a tenant URL derived from a running Host's BARE base
// and reads the exact capability token from Core's connect reply. The old-Host
// probe uses this rather than inferring support from a registration record.
func HostAdvertises(tb TB, base sessionwire.InternalEndpoint, tenant sessionwire.TenantID, token string) bool {
	tb.Helper()
	endpoint, err := sessionwire.HostLinkEndpoint(base, tenant)
	if err != nil {
		tb.Fatalf("orchestrationtest: deriving tenant HostLink endpoint from %q: %v", base, err)
		return false
	}
	request, err := sessionwire.EncodeHostLinkConnectRequest(sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
	})
	if err != nil {
		tb.Fatalf("orchestrationtest: encoding capability connect request: %v", err)
		return false
	}
	result := RawHostLinkConnect(tb, endpoint, PooledServiceToken, "centrifuge-json", request, 10*time.Second)
	if !result.Connected {
		tb.Fatalf("orchestrationtest: capability probe did not connect to %q: %+v", endpoint, result)
		return false
	}
	return DecodeHostCapabilities(tb, result.ReplyData).Supports(token)
}

// AssertHostCapabilities is the Host trip-wire. It fires when the capability set
// a running Host advertises is not exactly want, in either direction, and says
// which lane each difference moves.
//
// The comparison is a SET comparison. Order is the Host's dispatch-table order
// and carries no meaning Core assigns; duplicates cannot reach here because
// Core's decoder refuses them.
func AssertHostCapabilities(tb TB, got sessionwire.VersionNegotiationResponse, want []string) {
	tb.Helper()
	if got.Version != sessionwire.CurrentWireVersion {
		tb.Fatalf("orchestrationtest: the Host negotiated wire version %d, want %d", got.Version, sessionwire.CurrentWireVersion)
		return
	}
	methods := got.HostLinkMethods()
	if len(methods) == 0 {
		tb.Fatalf("orchestrationtest: the Host advertised NO HostLink methods. A Factory treats that as a Host " +
			"predating core v0.9.0 and will not bind, unbind or attach to it at all")
		return
	}
	have := slices.Clone(methods)
	sort.Strings(have)
	expected := slices.Clone(want)
	sort.Strings(expected)
	if slices.Equal(have, expected) {
		return
	}
	var gained, lost []string
	for _, method := range have {
		if !slices.Contains(expected, method) {
			gained = append(gained, method)
		}
	}
	for _, method := range expected {
		if !slices.Contains(have, method) {
			lost = append(lost, method)
		}
	}
	tb.Fatalf("orchestrationtest: the Host's HostLink capability set moved: gained %v, lost %v (advertised %v). "+
		"A LOST method is one a Factory now refuses locally -- losing hostlink.attach takes this Host out of B5 "+
		"placement, losing hostlink.drain re-blocks runbook 07 I2.3. A GAINED method or capability "+
		"requires auditing Factory's gate and the integration lane before moving this pin", gained, lost, methods)
}

// AssertFactorySubscribesToNoHostChannel was I1.1 cases 3-4 and I1.4's
// trip-wire and is DELETED.
//
// It FIRED on the factory v0.5.0 / host v0.4.0 pin, which is what it was for:
// Factory now constructs a routing.Relay, subscribes to the session channel on
// HostLink and fans the Host's committed tail out to its ClientLink
// subscribers. The kit's standing rule is that a trip-wire kept after its
// blocker lifts becomes a claim about the past a later reader takes for a claim
// about now, so it is deleted rather than inverted. The behaviour it guarded is
// now driven for real -- see factory_reconnect_integration_test.go (I1.1 cases
// 3-4) and factory_link_backpressure_integration_test.go (I1.4).

// AssertFactorySubscribesToTheSessionChannel is the POSITIVE reader that
// replaced the deleted relay trip-wire.
//
// It asks the same tap the same question -- did Factory send a subscribe on
// this HostLink? -- with the opposite expectation, and it names the channel
// Core derives rather than any spelling of this module's own, so a Factory that
// subscribed to something else fails here rather than passing on a prefix.
//
// It POLLS. The subscribe is sent by Factory's relay after the bind is
// answered, so a single sample right after Watch returns is a race; the wait is
// bounded by ctx and fails by assertion.
func AssertFactorySubscribesToTheSessionChannel(tb TB, ctx context.Context, lane *FactoryHostLane, session sessionwire.SessionID) {
	tb.Helper()
	want := sessionwire.HostLinkChannel(lane.Store.Tenant, session)
	var seen []string
	for {
		seen = nil
		vacuous := true
		for _, conn := range lane.HostLinks() {
			commands, err := conn.Commands()
			if err != nil {
				tb.Fatalf("orchestrationtest: the tapped HostLink could not be read: %v", err)
				return
			}
			if len(commands) > 0 {
				vacuous = false
			}
			for _, command := range commands {
				if command.Subscribe == nil {
					continue
				}
				if command.Subscribe.Channel == want {
					return
				}
				seen = append(seen, command.Subscribe.Channel)
			}
		}
		select {
		case <-ctx.Done():
			if vacuous {
				tb.Fatalf("orchestrationtest: no Factory command reached the Host at all, so the absence of "+
					"a subscribe to %q proves nothing", want)
				return
			}
			tb.Fatalf("orchestrationtest: Factory never subscribed to %q on HostLink (it subscribed to %v). "+
				"Without that subscribe nothing the Host publishes can reach a ClientLink viewer", want, seen)
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
}
