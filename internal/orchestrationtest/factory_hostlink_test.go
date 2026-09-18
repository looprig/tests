//go:build integration

// This file is the FIRST test anywhere that connects a real Factory to a real
// Host, and it exists because two Factory ↔ Host mismatches shipped for want of
// one: B6 (command routing -- Factory sent `hostlink.command`, Host routed every
// non-reserved method as a channel name) and B8 (the connect handshake: a
// wrapped request Host disconnected 4501, a missing `centrifuge-json`
// subprotocol Host answered HTTP 400, and bare refusals Factory read as
// success). factory v0.2.0 fixed all of them and two reviewers proved it in
// throwaway harnesses inside Factory's own package. This makes it permanent,
// from OUTSIDE both modules, against their released tags.
//
// # How a Factory is made to dial
//
// Factory's HostLink client is internal/ and has no exported trigger. The ONE
// production path that dials is demand: a ClientLink viewer subscribes to a
// session, routing.Demand reads the owner from the Directory, and the pool binds
// to the endpoint the owner record names. So that is what these cases do. The
// owner record is the Host's OWN registration, written by the Host's attach and
// read back through the real Store.
//
// # How the wire is observed
//
// Factory discards a bind failure and a delivery failure by design
// (routing.Demand.bindLocked, httpapi.deliverAdmitted), and its retained
// capability set is an unexported field. The wire is therefore the only place
// Factory's behaviour is visible, and ComposedHost's passive HostLinkTap reads it
// in both directions. It rewrites nothing; see hostlinktap.go.

package orchestrationtest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

func laneContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// boundedContext is a sub-deadline for ONE wait, so a wait that cannot succeed
// fails at its own line rather than exhausting the case's whole budget.
func boundedContext(t *testing.T, parent context.Context, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, d)
	t.Cleanup(cancel)
	return ctx
}

// rpcsFor returns every Factory RPC on conn carrying method, with Host's reply.
func rpcsFor(t *testing.T, conn *TappedConn, method string) []tappedRPC {
	t.Helper()
	commands, err := conn.Commands()
	if err != nil {
		t.Fatalf("reading Factory's commands off the tap: %v", err)
	}
	replies, err := conn.Replies()
	if err != nil {
		t.Fatalf("reading Host's replies off the tap: %v", err)
	}
	var out []tappedRPC
	for _, command := range commands {
		if command.RPC == nil || command.RPC.Method != method {
			continue
		}
		reply, answered := ReplyTo(replies, command.ID)
		out = append(out, tappedRPC{data: command.RPC.Data, reply: reply, answered: answered})
	}
	return out
}

type tappedRPC struct {
	data     json.RawMessage
	reply    WireReply
	answered bool
}

// requireAccepted fails unless Host's reply to an RPC is an ACCEPTANCE: a
// transport success whose body is not a Core HostLinkError. Host answers a
// refusal as a transport success carrying a bare HostLinkError body -- the shape
// v0.1.x Factories read as success -- so "no transport error" alone is not
// acceptance, and this is the check that says so.
func requireAccepted(tb TB, rpc tappedRPC, what string) {
	tb.Helper()
	switch {
	case !rpc.answered:
		tb.Fatalf("Host never answered the %s", what)
		return
	case rpc.reply.Error != nil:
		tb.Fatalf("Host answered the %s with transport error %d %q", what, rpc.reply.Error.Code, rpc.reply.Error.Message)
		return
	case rpc.reply.RPC == nil:
		tb.Fatalf("Host's answer to the %s carries no rpc result", what)
		return
	}
	if refusal, isRefusal := decodeRefusal(rpc.reply.RPC.Data); isRefusal {
		tb.Fatalf("Host refused the %s: %s", what, refusal.Code)
	}
}

// requireRefusal fails unless Host's reply is the B8 refusal shape -- a
// transport SUCCESS whose body is a bare, Core-valid HostLinkError -- carrying
// exactly want. Any other refusal code is a different Host decision and is not
// accepted in its place.
func requireRefusal(tb TB, rpc tappedRPC, want sessionwire.HostLinkErrorCode) {
	tb.Helper()
	if !rpc.answered || rpc.reply.Error != nil || rpc.reply.RPC == nil {
		tb.Fatalf("Host's answer is not a transport success carrying a body: %+v", rpc.reply)
		return
	}
	refusal, isRefusal := decodeRefusal(rpc.reply.RPC.Data)
	if !isRefusal {
		tb.Fatalf("Host's answer %s is not a Core HostLinkError", rpc.reply.RPC.Data)
		return
	}
	if refusal.Code != want {
		tb.Fatalf("Host refused with %q, want %s", refusal.Code, want)
	}
}

// decodeRefusal decodes a reply body as Core's HostLinkError and reports
// whether it IS one. Core's UnmarshalJSON is strict and calls Validate before
// it returns, so a body that decodes here is a Core-valid refusal; a separate
// Validate call was measured to be an equivalent mutant and is not repeated.
func decodeRefusal(data json.RawMessage) (sessionwire.HostLinkError, bool) {
	if len(data) == 0 || string(data) == "null" {
		return sessionwire.HostLinkError{}, false
	}
	var refusal sessionwire.HostLinkError
	if err := json.Unmarshal(data, &refusal); err != nil {
		return sessionwire.HostLinkError{}, false
	}
	return refusal, true
}

// TestFactoryHostLinkDialsAReleasedHost is the booked test.
//
// Released factory v0.2.0's real HostLink dialer connects to a released host
// v0.2.1 composed with host.Compose, over a real WebSocket, with no shim; binds
// with the lease epoch the Host's own attach granted; and delivers a command the
// Host's durable consumer acts on.
func TestFactoryHostLinkDialsAReleasedHost(t *testing.T) {
	ctx := laneContext(t)
	lane := NewFactoryHostLane(t, ctx)
	const (
		session = sessionwire.SessionID("session-lane-resident")
		create  = sessionwire.CommandID("command-lane-create")
	)

	// ---- Factory creates; Host attaches -----------------------------------
	filed := lane.Create(t, ctx, session, create)
	if filed.Record.State != sessionstore.InboxStatePending || filed.Record.Descriptor.Binding.ProtocolMode != sessionstore.ProtocolModeDisposition {
		t.Fatalf("Factory's create filed %q in mode %q, want a pending DISPOSITION command", filed.Record.State, filed.Record.Descriptor.Binding.ProtocolMode)
	}
	if len(lane.HostLinks()) != 0 {
		t.Fatalf("a create alone opened a HostLink; demand is the only trigger this case assumes")
	}

	residency := lane.Host.Attach(t, ctx, session, sessionwire.HostLinkAttachModeCreate)

	// The attach pass: Host's consumer claims the create, authorizes an attempt,
	// dispatches it to the runtime and asks for evidence, which the shut gate
	// refuses. The evidence READ is the pass's last act, so it is the signal
	// that the pass is over.
	lane.Host.Evidence.AwaitReads(t, boundedContext(t, ctx, 10*time.Second), 1)
	afterAttach := lane.Command(t, ctx, session, create)
	if afterAttach.Record.State != sessionstore.InboxStateApplying {
		t.Fatalf("after the attach pass the create is %q, want applying (dispatched, evidence refused)", afterAttach.Record.State)
	}
	if afterAttach.Record.Claim == nil || uint64(afterAttach.Record.Claim.ResidencyEpoch) != residency.LeaseEpoch {
		t.Fatalf("the create was claimed under %+v, want the attach's residency epoch %d", afterAttach.Record.Claim, residency.LeaseEpoch)
	}

	// Open the runtime's journal. From here the NEXT consumer pass settles the
	// create; what drives that pass is what the rest of the case measures.
	lane.Host.Evidence.Open()

	// ---- Factory dials, negotiates and binds ------------------------------
	viewer := ConnectClientLink(t, boundedContext(t, ctx, 10*time.Second), lane.Factory)
	viewer.Watch(t, boundedContext(t, ctx, 10*time.Second), lane.Store.Tenant, session)
	link := lane.OnlyHostLink(t)

	t.Run("the upgrade succeeds, HTTP 101 via the centrifuge-json subprotocol", func(t *testing.T) {
		if faults := link.Faults(); len(faults) != 0 {
			t.Fatalf("the tap could not read the link: %v", faults)
		}
		if !slices.Contains(link.Subprotocols, "centrifuge-json") {
			t.Fatalf("Factory's upgrade offered subprotocols %v, want centrifuge-json (B8: without it Host answers 400)", link.Subprotocols)
		}
		if status := link.Status(); status != http.StatusSwitchingProtocols {
			t.Fatalf("Host answered Factory's upgrade %d, want 101", status)
		}
		if selected := link.ResponseHeader().Get("Sec-WebSocket-Protocol"); selected != "centrifuge-json" {
			t.Fatalf("Host selected subprotocol %q, want centrifuge-json", selected)
		}
		if link.Path != "/hostlink/"+string(lane.Store.Tenant) {
			t.Fatalf("Factory dialled %q, want the tenant path Host advertised", link.Path)
		}
	})

	t.Run("negotiation completes and the capability set Factory received is exactly Host's", func(t *testing.T) {
		commands, err := link.Commands()
		if err != nil {
			t.Fatal(err)
		}
		replies, err := link.Replies()
		if err != nil {
			t.Fatal(err)
		}
		if len(commands) == 0 || commands[0].Connect == nil {
			t.Fatalf("Factory's first command on the link is not a connect: %+v", commands)
		}
		// B8 framing: the connect Data is Core's BARE request, byte for byte.
		wantRequest, err := sessionwire.EncodeHostLinkConnectRequest(sessionwire.VersionNegotiationRequest{
			SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
		})
		if err != nil {
			t.Fatal(err)
		}
		if string(commands[0].Connect.Data) != string(wantRequest) {
			t.Fatalf("Factory's connect Data = %s, want Core's bare request %s", commands[0].Connect.Data, wantRequest)
		}
		if commands[0].Connect.Token != KitServiceToken {
			t.Fatalf("Factory presented %q, want the composed HostLink service token", commands[0].Connect.Token)
		}
		reply, answered := ReplyTo(replies, commands[0].ID)
		if !answered || reply.Error != nil || reply.Connect == nil {
			t.Fatalf("Host did not accept Factory's connect: %+v", reply)
		}
		negotiated := DecodeHostCapabilities(t, reply.Connect.Data)
		// EXACTLY the set, in either direction -- the capability wire, applied
		// to the reply Factory itself received on its own connection.
		AssertHostCapabilities(t, negotiated, HostLinkMethodsAtV021())
		if !negotiated.Supports(sessionwire.HostLinkMethodAttach) {
			t.Fatalf("Supports(hostlink.attach) = false on the negotiation Factory received")
		}
	})

	t.Run("Factory binds with the lease epoch the attach granted, and Host accepts", func(t *testing.T) {
		binds := rpcsFor(t, link, sessionwire.HostLinkMethodBind)
		if len(binds) != 1 {
			t.Fatalf("Factory sent %d binds, want exactly 1", len(binds))
		}
		bind := DecodeBind(t, binds[0].data)
		if bind.TenantID != lane.Store.Tenant || bind.SessionID != session {
			t.Fatalf("Factory bound %s/%s, want %s/%s", bind.TenantID, bind.SessionID, lane.Store.Tenant, session)
		}
		if bind.HostID != LaneHostID || bind.HostGeneration != LaneHostGeneration {
			t.Fatalf("Factory bound host %q generation %d, want %q at %d", bind.HostID, bind.HostGeneration, LaneHostID, LaneHostGeneration)
		}
		if bind.LeaseEpoch != residency.LeaseEpoch {
			t.Fatalf("Factory bound with lease epoch %d, want the attach's %d", bind.LeaseEpoch, residency.LeaseEpoch)
		}
		requireAccepted(t, binds[0], "bind")
	})

	t.Run("a bind alone does not wake Host's consumer", func(t *testing.T) {
		// The control that makes the delivery assertion below mean something.
		// The gate is open, the route is live, and the consumer timer is an
		// hour away: if anything but a delivery could drive a pass, it would
		// settle the create here.
		time.Sleep(750 * time.Millisecond)
		if reads := len(lane.Host.Evidence.Reads()); reads != 1 {
			t.Fatalf("Host's consumer ran %d evidence reads before any delivery, want 1 (the attach pass)", reads)
		}
		if state := lane.Command(t, ctx, session, create).Record.State; state != sessionstore.InboxStateApplying {
			t.Fatalf("the create moved to %q before any delivery", state)
		}
	})

	// ---- Factory delivers; Host receives -----------------------------------
	//
	// The delivery is Factory's idempotent CREATE REPLAY, and that is not a
	// choice. factory v0.2.0 delivers only after its own admission, and every
	// control but create admits into the LEGACY inbox, which sessionstore
	// refuses for a disposition session -- the only kind a Host can hold. See
	// TestFactoryCannotAdmitInputToAHostResidentSession.
	status, body := lane.Factory.Post(t, ctx, "/v1/sessions", CreateBody(session, create))
	if status != http.StatusCreated {
		t.Fatalf("the create replay answered %d: %s", status, body)
	}

	t.Run("the delivery is a channel RPC named by Core, and Host accepts it", func(t *testing.T) {
		channel := sessionwire.HostLinkChannel(lane.Store.Tenant, session)
		deliveries := rpcsFor(t, link, channel)
		if len(deliveries) != 1 {
			t.Fatalf("Factory sent %d RPCs on %q, want exactly 1 (B6: the method IS the channel name)", len(deliveries), channel)
		}
		var delivery sessionwire.HostLinkCommandDelivery
		if err := json.Unmarshal(deliveries[0].data, &delivery); err != nil {
			t.Fatalf("Core refused the delivery Factory sent (%s): %v", deliveries[0].data, err)
		}
		if delivery.CommandID != create {
			t.Fatalf("Factory delivered %q, want %q", delivery.CommandID, create)
		}
		requireAccepted(t, deliveries[0], "delivery")
	})

	t.Run("Host received it: its durable consumer settled the command", func(t *testing.T) {
		// THE STRONGEST AVAILABLE PROOF, and why it is this one. Host's reply
		// says the RPC reached Multiplexer.Deliver; this says what Deliver did.
		// Deliver's only effect is Consumer.Hint, and a hint's only effect is a
		// consumer pass. Nothing else could have driven one here: the timer is
		// an hour away and the control above watched the open gate sit idle.
		// So a durable transition to `applied`, settled under THIS residency,
		// is the Host's consumer acting on Factory's delivery -- written by
		// another process into the shared store, and read back from it.
		settled := lane.AwaitCommandState(t, boundedContext(t, ctx, 10*time.Second), session, create, sessionstore.InboxStateApplied)
		outcome := settled.Record.Outcome
		if outcome == nil || outcome.Kind != sessionstore.DispositionApplied {
			t.Fatalf("the create settled with outcome %+v, want applied", outcome)
		}
		if uint64(outcome.SettlingResidencyEpoch) != residency.LeaseEpoch {
			t.Fatalf("settled by residency %d, want the attach's %d", outcome.SettlingResidencyEpoch, residency.LeaseEpoch)
		}
		if reads := len(lane.Host.Evidence.Reads()); reads != 2 {
			t.Fatalf("Host's consumer made %d evidence reads, want 2: the attach pass and the delivered one", reads)
		}
		cursor, err := lane.Store.Store.LoadDispositionCommandCursor(ctx, sessionstore.LoadDispositionCommandCursorRequest{
			TenantID: lane.Store.Tenant, SessionID: session,
		})
		if err != nil {
			t.Fatalf("reading the consumption cursor: %v", err)
		}
		if cursor.Cursor.ConsumedOrder != settled.AcceptedOrder || cursor.Cursor.LeaseEpoch != residency.LeaseEpoch {
			t.Fatalf("the consumption cursor is %+v, want consumed through %d under epoch %d",
				cursor.Cursor, settled.AcceptedOrder, residency.LeaseEpoch)
		}
		if applied := lane.Host.Runtime.Applied(); len(applied) != 1 || applied[0].CommandID != create {
			t.Fatalf("the runtime was dispatched %+v, want the create exactly once", applied)
		}
	})

	t.Run("Factory still holds a single link and relays no Host channel", func(t *testing.T) {
		lane.OnlyHostLink(t)
		AssertFactorySubscribesToNoHostChannel(t, link)
	})
}

// TestFactoryHostLinkBindToANonResidentSessionIsARefusal is the case factory
// v0.1.x got wrong: Host answers a refused bind with a bare Core HostLinkError
// body inside a transport SUCCESS, and v0.1.x decoded it as success.
func TestFactoryHostLinkBindToANonResidentSessionIsARefusal(t *testing.T) {
	ctx := laneContext(t)
	lane := NewFactoryHostLane(t, ctx)
	const (
		resident      = sessionwire.SessionID("session-lane-resident")
		residentMake  = sessionwire.CommandID("command-lane-create-resident")
		nonResident   = sessionwire.SessionID("session-lane-not-resident")
		nonResidentMk = sessionwire.CommandID("command-lane-create-not-resident")
	)
	lane.Create(t, ctx, resident, residentMake)
	lane.Create(t, ctx, nonResident, nonResidentMk)
	residency := lane.Host.Attach(t, ctx, resident, sessionwire.HostLinkAttachModeCreate)

	// The owner Factory is told for the non-resident session is the resident
	// one's REAL registration, readdressed: the right Host, generation,
	// endpoint and runtime, a live epoch -- and a session that Host does not
	// hold. It is a stale registry read, and it is the only pin in the case.
	real, found, err := lane.Directory.Inner.Owner(ctx, lane.Store.Tenant, resident)
	if err != nil || !found {
		t.Fatalf("reading the resident session's registration: found=%v err=%v", found, err)
	}
	if real.LeaseEpoch != residency.LeaseEpoch || real.InternalEndpoint != lane.Host.Endpoint {
		t.Fatalf("the registration names epoch %d at %q, want %d at %q", real.LeaseEpoch, real.InternalEndpoint, residency.LeaseEpoch, lane.Host.Endpoint)
	}
	stale := real
	stale.SessionID = nonResident
	lane.Directory.Pin(nonResident, stale)
	if _, err := lane.Store.Store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{
		TenantID: lane.Store.Tenant, SessionID: nonResident,
	}); err == nil {
		t.Fatalf("the non-resident session has a registration; the case's premise is false")
	}

	viewer := ConnectClientLink(t, boundedContext(t, ctx, 10*time.Second), lane.Factory)
	viewer.Watch(t, boundedContext(t, ctx, 10*time.Second), lane.Store.Tenant, nonResident)
	link := lane.OnlyHostLink(t)

	t.Run("Host refuses the bind with a bare runtime_unavailable", func(t *testing.T) {
		binds := rpcsFor(t, link, sessionwire.HostLinkMethodBind)
		if len(binds) != 1 {
			t.Fatalf("Factory sent %d binds, want 1", len(binds))
		}
		if bind := DecodeBind(t, binds[0].data); bind.SessionID != nonResident {
			t.Fatalf("Factory bound %q, want %q", bind.SessionID, nonResident)
		}
		// The B8 shape, exactly: a TRANSPORT SUCCESS whose body is a bare Core
		// HostLinkError, decoded with Core's strict decoder and validated.
		requireRefusal(t, binds[0], sessionwire.HostLinkErrorRuntimeUnavailable)
	})

	t.Run("Factory read it as a refusal: it holds no route and delivers nothing", func(t *testing.T) {
		// What distinguishes a Factory that read the refusal correctly from
		// v0.1.x is what it does NEXT. A Factory that took the refusal for
		// success holds a bound route, and its create replay is delivered on
		// the session channel. A Factory that read it as a refusal holds no
		// route (routing.Bindings.Deliver answers ErrNoBinding locally) and the
		// replay sends nothing.
		status, body := lane.Factory.Post(t, ctx, "/v1/sessions", CreateBody(nonResident, nonResidentMk))
		if status != http.StatusCreated {
			t.Fatalf("the create replay answered %d: %s", status, body)
		}
		if deliveries := rpcsFor(t, link, sessionwire.HostLinkChannel(lane.Store.Tenant, nonResident)); len(deliveries) != 0 {
			t.Fatalf("Factory delivered %d commands on a route Host refused; it read the refusal as success", len(deliveries))
		}
	})

	t.Run("control: the same replay IS delivered on a route Host accepted", func(t *testing.T) {
		// Without this, "no delivery" above could be a replay that never
		// delivers anything. The resident session's route is accepted, and the
		// identical replay on it produces exactly one channel RPC.
		viewer.Watch(t, boundedContext(t, ctx, 10*time.Second), lane.Store.Tenant, resident)
		status, body := lane.Factory.Post(t, ctx, "/v1/sessions", CreateBody(resident, residentMake))
		if status != http.StatusCreated {
			t.Fatalf("the create replay answered %d: %s", status, body)
		}
		deliveries := rpcsFor(t, link, sessionwire.HostLinkChannel(lane.Store.Tenant, resident))
		if len(deliveries) != 1 {
			t.Fatalf("the replay on an accepted route produced %d deliveries, want 1", len(deliveries))
		}
		requireAccepted(t, deliveries[0], "control delivery")
	})
}

// TestHostLinkB8RegressionControls drives the two B8 wire defects against the
// released Host directly. Each must fail by ASSERTION: every wait is bounded
// and a transport error is reported, not waited out.
func TestHostLinkB8RegressionControls(t *testing.T) {
	ctx := laneContext(t)
	lane := NewFactoryHostLane(t, ctx)
	host := lane.Host

	t.Run("an upgrade without the centrifuge-json subprotocol is HTTP 400", func(t *testing.T) {
		if status := RawUpgradeStatus(t, ctx, host.URL(), KitServiceToken, ""); status != http.StatusBadRequest {
			t.Fatalf("Host answered a subprotocol-less upgrade %d, want 400 from its JSON-protocol gate", status)
		}
		// The control: the identical request WITH the subprotocol is upgraded,
		// so the 400 above is the header and nothing else about the request.
		if status := RawUpgradeStatus(t, ctx, host.URL(), KitServiceToken, "centrifuge-json"); status != http.StatusSwitchingProtocols {
			t.Fatalf("Host answered the same upgrade with the subprotocol %d, want 101", status)
		}
	})

	t.Run("the old wrapped connect request is disconnected 4501", func(t *testing.T) {
		wrapped := []byte(`{"version_negotiation":{"supported_versions":[1]}}`)
		result := RawHostLinkConnect(t, host.Endpoint, KitServiceToken, "centrifuge-json", wrapped, 5*time.Second)
		if result.Err != nil {
			t.Fatalf("the wrapped connect failed at the transport, not the handshake: %v", result.Err)
		}
		if result.Connected {
			t.Fatalf("Host ACCEPTED the v0.1.x wrapped request; reply %s", result.ReplyData)
		}
		if result.Code != 4501 {
			t.Fatalf("Host disconnected the wrapped request with %d (%q), want 4501", result.Code, result.Reason)
		}
		// The control: Core's bare request, on the same path and credential,
		// connects -- and its reply is the capability set, exactly.
		bare, err := sessionwire.EncodeHostLinkConnectRequest(sessionwire.VersionNegotiationRequest{
			SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
		})
		if err != nil {
			t.Fatal(err)
		}
		control := RawHostLinkConnect(t, host.Endpoint, KitServiceToken, "centrifuge-json", bare, 5*time.Second)
		if !control.Connected {
			t.Fatalf("Host refused Core's bare request too (%+v); the 4501 above proves nothing", control)
		}
		AssertHostCapabilities(t, DecodeHostCapabilities(t, control.ReplyData), HostLinkMethodsAtV021())
	})
}

// TestFactoryCannotAdmitInputToAHostResidentSession is a GAP MARKER, found by
// this lane and not previously recorded: factory v0.2.0 cannot deliver an input
// to any session a Host holds.
//
// Every control Factory admits except create -- input, interrupt, restore, gate
// response -- goes through sessionstore.AdmitCommand, the LEGACY inbox, which
// binds the session's protocol mode legacy. A Host takes residency only through
// AcquireResidency, which requires DISPOSITION. So on every Host-resident
// session the admission is refused by the store before anything is delivered,
// and Factory answers 500. This case fails the day Factory admits controls in
// the disposition family; delete it then and deliver an input for real.
func TestFactoryCannotAdmitInputToAHostResidentSession(t *testing.T) {
	ctx := laneContext(t)
	lane := NewFactoryHostLane(t, ctx)
	const (
		session = sessionwire.SessionID("session-lane-input")
		create  = sessionwire.CommandID("command-lane-input-create")
		input   = sessionwire.CommandID("command-lane-input")
	)
	lane.Create(t, ctx, session, create)
	lane.Host.Attach(t, ctx, session, sessionwire.HostLinkAttachModeCreate)
	viewer := ConnectClientLink(t, boundedContext(t, ctx, 10*time.Second), lane.Factory)
	viewer.Watch(t, boundedContext(t, ctx, 10*time.Second), lane.Store.Tenant, session)
	link := lane.OnlyHostLink(t)
	binds := rpcsFor(t, link, sessionwire.HostLinkMethodBind)
	if len(binds) != 1 {
		t.Fatalf("the premise is a live route; Factory sent %d binds", len(binds))
	}
	requireAccepted(t, binds[0], "bind this marker's premise needs")

	body := []byte(`{"version":1,"command_id":"` + string(input) + `","session_id":"` + string(session) +
		`","blocks":[{"type":"text","text":"hello"}]}`)
	status, answer := lane.Factory.Post(t, ctx, "/v1/sessions/"+string(session)+"/input", body)
	if status != http.StatusInternalServerError {
		t.Fatalf("Factory answered an input to a Host-resident session %d (%s). If it is 200 the gap is "+
			"closed: deliver an input through the lane for real and delete this marker", status, answer)
	}
	// The cause, read from the store Factory admits through, so the 500 cannot
	// be some other fault wearing the same status.
	_, _, err := lane.Store.Store.AdmitCommand(ctx, sessionstore.AdmitCommandRequest{
		TenantID: lane.Store.Tenant, SessionID: session, CommandID: input,
		Kind: sessionstore.CommandKind("input"), AcceptedAt: lane.Clock.Now(), ApplyDeadline: lane.Clock.Now().Add(time.Minute),
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(kitRuntimeUUID.String()),
		Payload:                  body,
	})
	var catalogErr *sessionstore.CatalogError
	if !errors.As(err, &catalogErr) || catalogErr.Code != sessionstore.CatalogErrorConflict || catalogErr.Field != "binding.protocol_mode" {
		t.Fatalf("the legacy admission on a disposition session = %v, want catalog conflict (binding.protocol_mode)", err)
	}
	if deliveries := rpcsFor(t, link, sessionwire.HostLinkChannel(lane.Store.Tenant, session)); len(deliveries) != 0 {
		t.Fatalf("Factory delivered %d commands for a refused admission", len(deliveries))
	}
}

// TestFactoryHostLaneAssertionsCanFail is the positive control for every
// assertion helper the lane cases above rest on. A helper that could not fail
// would make each of those cases pass for the wrong reason, and a wait that
// could not end would make them hang instead of failing -- which is the one
// failure mode this lane has broken before.
func TestFactoryHostLaneAssertionsCanFail(t *testing.T) {
	coreRefusal := func(code sessionwire.HostLinkErrorCode, epoch uint64) json.RawMessage {
		t.Helper()
		body, err := json.Marshal(sessionwire.HostLinkError{Code: code, CurrentLeaseEpoch: epoch})
		if err != nil {
			t.Fatalf("encoding a Core refusal: %v", err)
		}
		return body
	}
	answered := func(data json.RawMessage) tappedRPC {
		reply := WireReply{ID: 2}
		reply.RPC = &struct {
			Data json.RawMessage `json:"data"`
		}{Data: data}
		return tappedRPC{reply: reply, answered: true}
	}
	transportFailure := func() tappedRPC {
		failure := tappedRPC{answered: true, reply: WireReply{ID: 2}}
		failure.reply.Error = &struct {
			Code    uint32 `json:"code"`
			Message string `json:"message"`
		}{Code: 100, Message: "internal server error"}
		return failure
	}
	unavailable := answered(coreRefusal(sessionwire.HostLinkErrorRuntimeUnavailable, 0))
	mismatch := answered(coreRefusal(sessionwire.HostLinkErrorEpochMismatch, 9))
	acceptance := answered(nil)

	t.Run("requireRefusal takes only the refusal it names", func(t *testing.T) {
		requireRefusal(t, unavailable, sessionwire.HostLinkErrorRuntimeUnavailable)
		mustFail(t, "want runtime_unavailable", func(tb TB) {
			requireRefusal(tb, mismatch, sessionwire.HostLinkErrorRuntimeUnavailable)
		})
		mustFail(t, "not a Core HostLinkError", func(tb TB) {
			requireRefusal(tb, acceptance, sessionwire.HostLinkErrorRuntimeUnavailable)
		})
		mustFail(t, "not a Core HostLinkError", func(tb TB) {
			requireRefusal(tb, answered(json.RawMessage(`{"code":"not_a_core_code"}`)), sessionwire.HostLinkErrorRuntimeUnavailable)
		})
		mustFail(t, "not a transport success", func(tb TB) {
			requireRefusal(tb, tappedRPC{}, sessionwire.HostLinkErrorRuntimeUnavailable)
		})
		// A TRANSPORT error is not the refusal either: Host's refusal is a
		// decision carried in a successful reply, and a transport failure says
		// nothing about what Host decided.
		mustFail(t, "not a transport success", func(tb TB) {
			requireRefusal(tb, transportFailure(), sessionwire.HostLinkErrorRuntimeUnavailable)
		})
	})

	t.Run("requireAccepted refuses the bare refusal v0.1.x read as success", func(t *testing.T) {
		requireAccepted(t, acceptance, "rpc")
		mustFail(t, "Host refused the rpc: runtime_unavailable", func(tb TB) { requireAccepted(tb, unavailable, "rpc") })
		mustFail(t, "never answered", func(tb TB) { requireAccepted(tb, tappedRPC{}, "rpc") })
		mustFail(t, "transport error", func(tb TB) { requireAccepted(tb, transportFailure(), "rpc") })
	})

	t.Run("the tap refuses a frame it cannot read instead of skipping it", func(t *testing.T) {
		conn := &TappedConn{}
		// A text frame with RSV1 set: permessage-deflate, which the tap cannot
		// read. It must surface as a fault, and the decoders must refuse.
		conn.observe(false, []byte{0xC1, 0x02, '{', '}'})
		if len(conn.Faults()) == 0 {
			t.Fatalf("a compressed frame was accepted silently")
		}
		if _, err := conn.Commands(); !errors.Is(err, ErrTapIncomplete) {
			t.Fatalf("Commands over an unreadable frame = %v, want ErrTapIncomplete", err)
		}
		// And a masked client frame is unmasked, not read as noise.
		masked := &TappedConn{}
		payload := []byte(`{"id":1}`)
		key := []byte{1, 2, 3, 4}
		frame := []byte{0x81, 0x80 | byte(len(payload))}
		frame = append(frame, key...)
		for i, b := range payload {
			frame = append(frame, b^key[i%4])
		}
		masked.observe(false, frame)
		commands, err := masked.Commands()
		if err != nil || len(commands) != 1 || commands[0].ID != 1 {
			t.Fatalf("a masked client frame decoded to %+v, %v", commands, err)
		}
	})

	t.Run("every lane wait is bounded and fails by assertion", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		lane := NewFactoryHostLane(t, ctx)
		const session = sessionwire.SessionID("session-lane-bounded")
		lane.Create(t, ctx, session, "command-lane-bounded")
		short, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		defer stop()
		mustFail(t, "never reached", func(tb TB) {
			lane.AwaitCommandState(tb, short, session, "command-lane-bounded", sessionstore.InboxStateApplied)
		})
		mustFail(t, "settlement evidence reads", func(tb TB) {
			lane.Host.Evidence.AwaitReads(tb, short, 1)
		})
	})
}
