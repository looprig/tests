//go:build integration

package orchestrationtest

import (
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

// HostLinkMethodsAtV021 is the exact HostLink capability set released host
// v0.2.1 advertises, spelled in Core's constants.
func HostLinkMethodsAtV021() []string {
	return []string{
		sessionwire.HostLinkMethodBind,
		sessionwire.HostLinkMethodUnbind,
		sessionwire.HostLinkMethodAttach,
		sessionwire.HostLinkMethodDrain,
		sessionwire.HostLinkMethodDrainStatus,
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
		"placement, losing hostlink.drain re-blocks runbook 07 I2.3. A GAINED method is new Host behaviour no "+
		"Factory gates yet: audit Factory's capability gate and the integration lane, then move this pin", gained, lost, methods)
}

// AssertFactorySubscribesToNoHostChannel is runbook 07 I1.1 cases 3-4 and
// I1.4's trip-wire, MOVED from Host to Factory.
//
// Those cases need a Host publication to reach a browser through Factory. The
// Host half now exists: host v0.2.1 relays a resident runtime's committed tail
// to a HostLink subscriber of the session channel (host's own
// TestAComposedHostRunsTheAttachAndLiveLinkRoundtrip). The Factory half does
// not: factory v0.2.0 constructs no routing.Relay and never subscribes to a
// session channel on HostLink, so nothing it receives can be fanned out. The
// wire is where that is decided, so the wire is what this reads: it fires the
// first time Factory SENDS a subscribe on a HostLink connection.
func AssertFactorySubscribesToNoHostChannel(tb TB, conn *TappedConn) {
	tb.Helper()
	commands, err := conn.Commands()
	if err != nil {
		tb.Fatalf("orchestrationtest: the tapped HostLink could not be read: %v", err)
		return
	}
	if len(commands) == 0 {
		tb.Fatalf("orchestrationtest: the tapped HostLink carried no Factory command at all; " +
			"an absence of subscribes read off an empty connection is vacuous")
		return
	}
	for _, command := range commands {
		if command.Subscribe != nil {
			tb.Fatalf("orchestrationtest: Factory subscribed to %q on HostLink. Factory now relays the Host live "+
				"tail, so runbook 07 I1.1 cases 3-4 and I1.4 are no longer blocked on it: drive them for real "+
				"and delete this trip-wire", command.Subscribe.Channel)
			return
		}
	}
}
