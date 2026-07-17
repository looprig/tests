//go:build integration

// Host-owned gates and restore: a Session that raised a form/open-url/startup
// elicitation through session.GateHost must come back cleanly from its journal.
//
// Host-owned gates (gate.ResolverSession, KindForm/KindOpenURL) carry no
// turn/step coordinates by design — an MCP server's elicitation during
// initialization belongs to no turn, and a startup one to no loop either. The
// write path used to journal those zero-coordinate GatePrepared/GateOpened/
// GateResolved records without complaint (it validated only the body), while the
// restore path validated full identity under a step profile — so any Session that
// ever raised a host gate became permanently unrestorable, re-breaking on every
// restore because the journal is append-only.
//
// This drives the real published surface — session.GateHost.OpenHostGate, the way
// harness/pkg/rig/gate_host_test.go does — across a genuine shutdown/restore
// boundary. It is the end-to-end proof the value-level tests in pkg/event cannot
// give: only a real journal and a real RestoreSession reach the decode path that
// was rejecting these records.

package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/storage/memstore"
)

// gateHostRestoreSchema is a one-field form schema an elicitation asks and an
// answer is validated against.
func gateHostRestoreSchema() gate.PromptSchema {
	return gate.PromptSchema{Fields: []gate.Field{
		{Name: "username", Label: "Username", Kind: gate.FieldText, Required: true},
	}}
}

func gateHostFormControls() []gate.Control {
	return []gate.Control{
		{Action: gate.FormActionAccept, Label: "Submit"},
		{Action: gate.FormActionDecline, Label: "Decline"},
	}
}

// TestRestoreSurvivesHostOwnedGates is the decisive proof. A Session raises three
// host-owned gates whose coordinates are exactly the shapes that used to poison a
// journal — a form gate with turn+step, an open-url gate with a turn but no step,
// and a startup form gate with no loop at all — resolves each one, shuts down, and
// restores. Before the fix, RestoreSession failed at replay decode with
// "invalid GatePrepared: TurnID must be set" (or LoopID for the startup case).
func TestRestoreSurvivesHostOwnedGates(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	store, err := sessionstore.Open(memstore.New())
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}

	// --- leg one: raise and resolve host-owned gates, then shut down ---
	sessOne, err := newRestorableRig(t, store, "planner", newScriptLLM(say("idle")), "").rig.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := sessOne.SessionID()
	loopID := sessOne.ActiveLoop().ID()

	host, ok := sessOne.(session.GateHost)
	if !ok {
		t.Fatal("session controller does not implement session.GateHost")
	}

	turnID, err := uuid.New()
	if err != nil {
		t.Fatal(err)
	}
	stepID, err := uuid.New()
	if err != nil {
		t.Fatal(err)
	}

	// (1) Form gate with a full turn+step subject, ANSWERED via RespondGate so the
	// answered-close path (buildGateResolved) stamps the resolver discriminator.
	formEnvelope := gate.Gate{
		Kind:     gate.KindForm,
		Resolver: gate.ResolverSession,
		Blocks:   gate.BlocksToolCall,
		Effect:   gate.EffectResume,
		Subject:  gate.Subject{TurnID: turnID, StepID: stepID},
		Prompt: gate.Prompt{
			Title:    "Sign in",
			Schema:   gateHostRestoreSchema(),
			Controls: gateHostFormControls(),
		},
	}
	formPayload := gate.FormPayload{Title: "Sign in", Schema: gateHostRestoreSchema()}
	formID, err := host.OpenHostGate(ctx, loopID, formEnvelope, formPayload)
	if err != nil {
		t.Fatalf("OpenHostGate(form): %v", err)
	}
	if err := sessOne.RespondGate(ctx, gate.GateResponse{
		GateID: formID,
		Action: gate.FormActionAccept,
		Values: map[string]json.RawMessage{"username": json.RawMessage(`"ada lovelace"`)},
		Source: gate.ResponseSource{Kind: gate.ResponseFromUser},
	}); err != nil {
		t.Fatalf("RespondGate(form): %v", err)
	}

	// (2) Open-url gate with a turn but NO step — the exact shape that decoded as
	// "StepID must be set". Resolved by owner close (buildGateClosed).
	openURLEnvelope := gate.Gate{
		Kind:     gate.KindOpenURL,
		Resolver: gate.ResolverSession,
		Blocks:   gate.BlocksToolCall,
		Effect:   gate.EffectResume,
		Subject:  gate.Subject{TurnID: turnID},
		Prompt: gate.Prompt{
			Title:    "Authorize access",
			Controls: gateHostFormControls(),
		},
	}
	openURLPayload := gate.OpenURLPayload{
		DisplayOrigin:      "https://github.com",
		URL:                "https://github.com/login/oauth/authorize?state=SECRET",
		RequiresCompletion: true,
	}
	openURLID, err := host.OpenHostGate(ctx, loopID, openURLEnvelope, openURLPayload)
	if err != nil {
		t.Fatalf("OpenHostGate(open-url): %v", err)
	}
	if err := host.CloseGate(ctx, openURLID, gate.CloseAbandoned); err != nil {
		t.Fatalf("CloseGate(open-url): %v", err)
	}

	// (3) Startup form gate with NO loop — a zero LoopID, the shape that decoded as
	// "LoopID must be set". An MCP server's elicitation during initialization.
	startupEnvelope := gate.Gate{
		Kind:     gate.KindForm,
		Resolver: gate.ResolverSession,
		Blocks:   gate.BlocksSession,
		Effect:   gate.EffectResume,
		Prompt: gate.Prompt{
			Title:    "Connect account",
			Schema:   gateHostRestoreSchema(),
			Controls: gateHostFormControls(),
		},
	}
	startupID, err := host.OpenHostGate(ctx, uuid.UUID{}, startupEnvelope, formPayload)
	if err != nil {
		t.Fatalf("OpenHostGate(startup, zero loop): %v", err)
	}
	if err := host.CloseGate(ctx, startupID, gate.CloseAbandoned); err != nil {
		t.Fatalf("CloseGate(startup): %v", err)
	}

	// The poison records really are in the journal: three public GateOpened plus
	// three GateResolved (each also has a private GatePreparedRecord, which the
	// internal replayer RestoreSession uses decodes — the exact record whose zero
	// coordinates used to fail replay). Without them the restore proves nothing.
	before := eventsFor(t, ctx, store, id)
	if got := countEvents[event.GateOpened](before); got != 3 {
		t.Fatalf("journal has %d GateOpened, want 3 host gates", got)
	}
	if got := countEvents[event.GateResolved](before); got != 3 {
		t.Fatalf("journal has %d GateResolved, want 3", got)
	}

	shutdown(t, sessOne)

	// --- leg two: restore. This is the step that used to fail at replay decode. ---
	restored, err := newRestorableRig(t, store, "planner", newScriptLLM(say("restored")), "").rig.RestoreSession(ctx, id)
	if err != nil {
		t.Fatalf("RestoreSession after host-owned gates: %v\n"+
			"this is the poisoned-journal failure the fix addresses: a zero-coordinate host gate marshalled clean but failed identity decode on replay", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), itTimeout)
		defer cancel()
		_ = restored.Shutdown(c)
	})

	// The restore is clean — RestoreStarted then RestoreDone, no RestoreErrored.
	assertCleanRestore(t, eventsFor(t, ctx, store, id))
}
