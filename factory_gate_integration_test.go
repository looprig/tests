//go:build integration

// This file is the cross-module GATE lane: a real agent, running on a real
// harness rig inside a real composed host v0.4.0, raises a real ask_user gate;
// a real factory v0.5.0 reads it, admits the answer, and the agent continues.
//
// # Why every one of those has to be real
//
// A gate crosses more module boundaries than anything else this program
// builds, and each boundary has its own failure that the others cannot see:
//
//	harness  journals GateOpened and, on an answered gate, exactly one
//	         GateResolved whose cause is the admitted runtime command;
//	host     publishes the journal's open gates into SessionStore's durable
//	         projection under a store-issued residency grant, and applies the
//	         answer as a kind-5 disposition command;
//	sessionstore  holds the projection and settles the command from the
//	         runtime's own evidence -- and REFUSES these gate pages to any
//	         reader below v0.12.0;
//	factory  serves the projection to a browser, gates admission on the Host's
//	         advertised capability token, and stores the answer inline;
//	core     owns the request record, the token and the framing.
//
// Only the agent's own tool result can tell you the whole chain worked, which
// is why the last assertion is that the answer reached the agent.
//
// # The ROLLOUT RULE this case is the reader for
//
// sessionstore v0.12.0's rule is that every ReadGates caller -- every Factory
// -- must be on v0.12.0 BEFORE any Host publishes a gate, because an older
// reader refuses these pages with `catalog sequence (gates)`. The positive case
// below reads the page through Factory's own routed read rather than the store,
// so a regression in that direction fails here rather than in production.

package tests

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

const (
	gateSession  = sessionwire.SessionID("session-gate")
	gateCreate   = sessionwire.CommandID("command-gate-create")
	gateAnswerID = sessionwire.CommandID("command-gate-answer")

	// gateAnswer is the word the user answers with. It is unique enough to be
	// searched for in a model request, which is how the case proves the agent
	// CONTINUED rather than merely that the gate closed.
	gateAnswer = "ULTRAMARINE"

	// gateQuestion is what the agent asks. It travels the other way -- into the
	// gate's public prompt -- so the case can prove the projection carries the
	// AGENT's prompt rather than a placeholder Host invented.
	gateQuestion = "orchestrationtest: which colour?"
)

// TestAnAgentsGateReachesFactoryAndItsAnswerSettlesApplied is the whole gate
// chain, in one case.
func TestAnAgentsGateReachesFactoryAndItsAnswerSettlesApplied(t *testing.T) {
	ctx := placementContext(t)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:     []sessionwire.TenantID{orchestrationtest.PooledTenantA},
		WithAskTool: true,
	})
	world.AskTool.Question = gateQuestion
	// Turn one calls the tool, which raises the gate and blocks inside it.
	// Turn two is the model's reply once the tool result -- the user's answer
	// -- comes back.
	world.LLM.Script(
		orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledAskToolName, ToolInput: `{}`},
		orchestrationtest.PooledTurn{Text: "thank you"},
	)

	pooled := orchestrationtest.StartPooledHost(t, ctx, world, "orchestrationtest-gate-host", 4)
	orchestrationtest.AwaitAdvertised(t, world, pooled.ID)

	t.Run("the Host advertises Core's gate_response capability token", func(t *testing.T) {
		// factory v0.5.0 refuses a gate response 409 gate_not_resumable unless
		// the OWNER's connect reply carries this token, so a Host that stopped
		// advertising it would make every case below fail with a refusal that
		// reads like a gate problem. Reading it first says which end moved.
		derived, err := sessionwire.HostLinkEndpoint(pooled.Base, orchestrationtest.PooledTenantA)
		if err != nil {
			t.Fatalf("deriving the tenant address: %v", err)
		}
		request, err := sessionwire.EncodeHostLinkConnectRequest(sessionwire.VersionNegotiationRequest{
			SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
		})
		if err != nil {
			t.Fatal(err)
		}
		result := orchestrationtest.RawHostLinkConnect(t, derived, orchestrationtest.PooledServiceToken,
			"centrifuge-json", request, 10*time.Second)
		if !result.Connected {
			t.Fatalf("the capability probe did not connect: %+v", result)
		}
		reply := orchestrationtest.DecodeHostCapabilities(t, result.ReplyData)
		if !reply.Supports(sessionwire.HostLinkCapabilityGateResponse) {
			t.Fatalf("the Host's connect reply advertises %v, without Core's gate_response token",
				reply.HostLinkMethods())
		}
	})

	served := orchestrationtest.StartPooledFactory(t, ctx, world, "orchestrationtest-gate-replica", nil)

	status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(gateCreate)),
		SessionID:       gateSession,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(`[{"type":"text","text":"ask me something"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "the create applied", 90*time.Second, func() bool {
		return world.CommandState(ctx, orchestrationtest.PooledTenantA, gateSession, gateCreate) == sessionstore.InboxStateApplied
	})

	var projected sessionwire.GateProjection
	t.Run("the agent's gate reaches Factory through ReadGates", func(t *testing.T) {
		orchestrationtest.PooledWait(t, "the gate reached Factory's gates read", 90*time.Second, func() bool {
			page := served.OpenGates(t, ctx, orchestrationtest.PooledTenantA, gateSession)
			if len(page.Gates) == 0 {
				return false
			}
			projected = page.Gates[0]
			return true
		})
		t.Logf("the projected gate: id=%s kind=%q answerability=%q opened_seq=%d prompt=%q",
			projected.GateID, projected.Kind, projected.Answerability, projected.OpenedJournalSeq, projected.Prompt.Title)

		// The identity is harness's: Host publishes the journal's own gate id,
		// not one it minted, and the answer is correlated by it.
		if id, err := uuid.Parse(string(projected.GateID)); err != nil || id.IsZero() {
			t.Fatalf("the projected gate id %q is not a harness gate identity: %v", projected.GateID, err)
		}
		if projected.Answerability != sessionwire.GateAnswerabilityResident {
			t.Fatalf("the gate is %q, want resident: only a resident gate is answerable", projected.Answerability)
		}
		if projected.OpenedJournalSeq == 0 {
			t.Fatalf("the projected gate names no opening journal sequence: %+v", projected)
		}
		// The AGENT's question, not a placeholder. Without this the case would
		// pass for a Host that published an empty prompt for every gate.
		if !strings.Contains(projected.Prompt.Title+" "+projected.Prompt.Body, gateQuestion) {
			t.Fatalf("the projected prompt is %+v, want the agent's question %q", projected.Prompt, gateQuestion)
		}
	})

	runtimeID := world.RuntimeSessionID(t, ctx, orchestrationtest.PooledTenantA, gateSession)

	t.Run("the answer is admitted, applied and settles", func(t *testing.T) {
		status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA,
			"/v1/sessions/"+string(gateSession)+"/gates/"+string(projected.GateID),
			sessionwire.GateResponseRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope(string(gateAnswerID)),
				SessionID:       gateSession,
				GateID:          projected.GateID,
				Action:          "answer",
				Values:          map[string]json.RawMessage{"answer": json.RawMessage(`"` + gateAnswer + `"`)},
				// The version the answer was written against. Host REJECTS an
				// answer whose ExpectedOpen* is not the projected one, so this
				// is not decoration.
				ExpectedOpenJournalSeq: projected.OpenedJournalSeq,
			})
		if status != http.StatusAccepted {
			t.Fatalf("Factory answered the gate response %d: %s; 409 would mean the owner's capability token was not seen", status, body)
		}

		settled := orchestrationtest.PooledWait(t, "the gate response settled", 90*time.Second, func() bool {
			return world.CommandState(ctx, orchestrationtest.PooledTenantA, gateSession, gateAnswerID) == sessionstore.InboxStateApplied
		})
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: orchestrationtest.PooledTenantA, SessionID: gateSession, CommandID: gateAnswerID,
		})
		if err != nil {
			t.Fatalf("reading the settled gate response: %v", err)
		}
		t.Logf("the answer settled after %v: state=%q outcome=%+v", settled.Round(time.Millisecond), entry.Record.State, entry.Record.Outcome)
		if entry.Record.Outcome == nil || entry.Record.Outcome.Kind != sessionstore.DispositionApplied {
			t.Fatalf("the answer settled with outcome %+v, want applied", entry.Record.Outcome)
		}

		// ONE resolution, BY THE USER, CAUSED BY THIS COMMAND. All three
		// matter: two would mean the answer was applied twice, a policy source
		// would mean the deadline won the race, and a cause that is not the
		// admitted runtime command is how a crash between the answer and its
		// disposition would show.
		resolutions := orchestrationtest.JournalEvents[event.GateResolved](t, world, orchestrationtest.PooledTenantA, runtimeID)
		if len(resolutions) != 1 {
			t.Fatalf("the journal holds %d GateResolved events, want exactly 1: %+v", len(resolutions), resolutions)
		}
		if resolutions[0].Source.Kind != gate.ResponseFromUser {
			t.Fatalf("the gate was resolved by %q, want the user: a policy source means the deadline won", resolutions[0].Source.Kind)
		}
		admitted, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID))
		if err != nil {
			t.Fatalf("the admitted runtime command id %q is not a UUID: %v", entry.Record.Descriptor.RuntimeCommandID, err)
		}
		if resolutions[0].Cause.CommandID != admitted {
			t.Fatalf("the GateResolved's cause is %v, want the admitted runtime command %v",
				resolutions[0].Cause.CommandID, admitted)
		}

		// The projection clears: an answered gate must stop being offered, or
		// every later reader would answer it again.
		orchestrationtest.PooledWait(t, "the gate projection cleared", 60*time.Second, func() bool {
			return len(served.OpenGates(t, ctx, orchestrationtest.PooledTenantA, gateSession).Gates) == 0
		})
	})

	t.Run("the agent continues: the answer reached its tool result", func(t *testing.T) {
		// THE END OF THE CHAIN. Everything above is observable at a store or on
		// a wire, and all of it could hold while the answer never reached the
		// running agent -- which is the only outcome a user would notice.
		orchestrationtest.PooledWait(t, "the tool returned the user's answer", 60*time.Second, func() bool {
			answers := world.AskTool.Answers()
			return len(answers) == 1 && answers[0] == gateAnswer
		})
		orchestrationtest.PooledWait(t, "the agent's next turn carried the answer", 60*time.Second, func() bool {
			return world.LLM.SawInRequest(0, gateAnswer)
		})
		orchestrationtest.PooledWait(t, "the gated turn finished", 60*time.Second, func() bool {
			return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, orchestrationtest.PooledTenantA, runtimeID) >= 1
		})
	})
}

// TestAGateResponseToAHostWithoutTheCapabilityIsRefused is the negative arm:
// a Host whose connect reply does not carry Core's gate_response token gets no
// answer at all, and the refusal is 409 gate_not_resumable with nothing written.
//
// # Why the Host is a stand-in and not a composed one
//
// host.Compose ALWAYS wires both gate seams, so a composed v0.4.0 Host always
// advertises the token: the unwired composition is unreachable through Host's
// public surface. What is reachable, and what this arm is really about, is the
// MIXED FLEET -- a v0.3.0 Host, or any Host whose gate seams a deployment left
// out, owning a session a browser then tries to answer. Both advertise exactly
// the five reserved methods and no capability, which is what this stand-in
// does, byte for byte through Core's own encoder.
//
// The refusal matters more than it looks: without it the answer would be
// admitted, claimed by a Host that cannot apply it, and left `applying` until a
// capable successor settled it `not_applied` -- a user's answer silently lost
// for as long as the fleet stays mixed.
func TestAGateResponseToAHostWithoutTheCapabilityIsRefused(t *testing.T) {
	ctx := placementContext(t)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:     []sessionwire.TenantID{orchestrationtest.PooledTenantA},
		WithAskTool: true,
	})
	world.AskTool.Question = gateQuestion
	world.LLM.Script(
		orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledAskToolName, ToolInput: `{}`},
		orchestrationtest.PooledTurn{Text: "thank you"},
	)

	pooled := orchestrationtest.StartPooledHost(t, ctx, world, "orchestrationtest-incapable-host", 4)
	orchestrationtest.AwaitAdvertised(t, world, pooled.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "orchestrationtest-incapable-replica", nil)

	const incapableSession = sessionwire.SessionID("session-gate-incapable")
	status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope("command-incapable-create"),
		SessionID:       incapableSession,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(`[{"type":"text","text":"ask me something"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}
	var projected sessionwire.GateProjection
	orchestrationtest.PooledWait(t, "the gate reached Factory", 90*time.Second, func() bool {
		page := served.OpenGates(t, ctx, orchestrationtest.PooledTenantA, incapableSession)
		if len(page.Gates) == 0 {
			return false
		}
		projected = page.Gates[0]
		return true
	})

	// The stand-in takes the session's registration over at a HIGHER epoch,
	// which is what a re-placement onto an older Host looks like from Factory:
	// the owner's endpoint and generation change, and Factory re-reads the
	// capability from the new owner's link.
	standin := orchestrationtest.StartIncapableHost(t, ctx)
	orchestrationtest.TakeOverRegistration(t, ctx, world, orchestrationtest.PooledTenantA, incapableSession, standin)

	t.Run("the answer is refused 409 gate_not_resumable, with nothing written", func(t *testing.T) {
		var status int
		var body string
		orchestrationtest.PooledWait(t, "Factory saw the incapable owner", 60*time.Second, func() bool {
			status, body = served.Post(t, ctx, orchestrationtest.PooledTenantA,
				"/v1/sessions/"+string(incapableSession)+"/gates/"+string(projected.GateID),
				sessionwire.GateResponseRequest{
					CommandEnvelope:        orchestrationtest.PooledEnvelope("command-incapable-answer"),
					SessionID:              incapableSession,
					GateID:                 projected.GateID,
					Action:                 "answer",
					Values:                 map[string]json.RawMessage{"answer": json.RawMessage(`"` + gateAnswer + `"`)},
					ExpectedOpenJournalSeq: projected.OpenedJournalSeq,
				})
			// 503 is the documented TRANSIENT answer while the owner's link is
			// reconnecting or not yet dialled, so it is retried rather than
			// treated as the refusal. Anything else is the terminal answer.
			return status != http.StatusServiceUnavailable
		})
		t.Logf("the answer to an incapable owner was answered %d: %s", status, body)
		if status != http.StatusConflict {
			t.Fatalf("Factory answered %d (%s), want 409 for an owner that does not advertise the gate_response token", status, body)
		}
		if !strings.Contains(body, "gate_not_resumable") {
			t.Fatalf("the refusal body is %s, want gate_not_resumable", body)
		}
		// NOTHING WRITTEN. A refusal that still admitted the command would put
		// a user's answer into a stream no Host can apply.
		if _, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: orchestrationtest.PooledTenantA, SessionID: incapableSession, CommandID: "command-incapable-answer",
		}); err == nil {
			t.Fatalf("the refused gate response was admitted anyway")
		}
		// NON-VACUITY. A 409 reached without Factory ever connecting to the
		// stand-in would be Factory failing to dial, not Factory reading a
		// capability set, and the two are indistinguishable from the status
		// code alone.
		if connects := standin.Connects(); connects == 0 {
			t.Fatalf("Factory never connected to the stand-in Host, so the refusal says nothing about its capabilities")
		}
	})
}
