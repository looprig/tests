//go:build integration

// This file is the harness gate's finding F5, which is now this lane's.
//
// # The version boundary nobody else can stand on
//
// harness proves that the disposition frames its runtime writes are readable by
// a released sessionstore — but it proves it against the sessionstore IT PINS,
// which is v0.9.0. A deployed Host settles through sessionstore v0.12.0. Those
// are two different released stores reading one producer, and harness cannot
// test the second without taking a dependency it does not want.
//
// This lane is the only place where BOTH are the released ones: the kit opens
// sessionstore v0.12.0 for the catalog, the inbox, the claims, the attempts and
// the settlement, and the evidence reader behind it is a real harness session
// store built against v0.9.0. Every settlement in the whole suite therefore
// crosses that boundary already; what this file adds is the case that SAYS SO,
// for every one of the five kinds, so a regression names the boundary instead
// of looking like a timeout somewhere else.
//
// # And the falsifier for "nothing vouches"
//
// The other half of the v0.10.0 round is that the kit's last vouching arm is
// gone. That is an absence, and an absence is only worth something if its
// opposite is observable: so this file also drives a command the product
// runtime REFUSES and proves it never settles. If anything were still vouching,
// it would settle anyway.

package tests

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/runtimecommand"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// TestTheReleasedStoreSettlesHarnessEvidenceForEveryKind is F5.
//
// Each kind is driven through the real Factory, dispatched by the real Host,
// applied by a real harness runtime, and settled by sessionstore v0.12.0 from
// the frame that runtime wrote. The assertion in every row is the same and is
// the boundary itself: a terminal state, an outcome, and the ATTEMPT the store
// authorized — an outcome naming no attempt would mean the store settled from
// something other than an authorized dispatch.
func TestTheReleasedStoreSettlesHarnessEvidenceForEveryKind(t *testing.T) {
	ctx, world, _, served := createWorld(t, "orchestrationtest-evidence-host")
	const session = sessionwire.SessionID("session-evidence")

	settled := func(t *testing.T, command sessionwire.CommandID, want sessionstore.DispositionOutcomeKind) {
		t.Helper()
		orchestrationtest.PooledWait(t, "the "+string(command)+" settled", 90*time.Second, func() bool {
			state := world.CommandState(ctx, orchestrationtest.PooledTenantA, session, command)
			return state == sessionstore.InboxStateApplied || state == sessionstore.InboxStateRejected
		})
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: orchestrationtest.PooledTenantA, SessionID: session, CommandID: command,
		})
		if err != nil {
			t.Fatalf("reading %s: %v", command, err)
		}
		t.Logf("%s settled: state=%q outcome=%+v", command, entry.Record.State, entry.Record.Outcome)
		if entry.Record.Outcome == nil {
			t.Fatalf("%s settled with no outcome; sessionstore v0.12.0 wrote nothing from harness's frame", command)
		}
		if entry.Record.Outcome.Kind != want {
			t.Fatalf("%s settled %q, want %q", command, entry.Record.Outcome.Kind, want)
		}
		if entry.Record.Outcome.AttemptID == "" {
			t.Fatalf("%s settled naming no attempt: %+v", command, entry.Record.Outcome)
		}
	}

	t.Run("create", func(t *testing.T) {
		status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("command-evidence-create"),
			SessionID:       session,
			AgentID:         orchestrationtest.PooledAgent,
			Blocks:          json.RawMessage(`[{"type":"text","text":"FEIJOA"}]`),
		})
		if status != http.StatusCreated {
			t.Fatalf("the create answered %d: %s", status, body)
		}
		settled(t, "command-evidence-create", sessionstore.DispositionApplied)
	})

	t.Run("input", func(t *testing.T) {
		status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions/"+string(session)+"/input",
			sessionwire.InputRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope("command-evidence-input"),
				SessionID:       session,
				Blocks:          json.RawMessage(`[{"type":"text","text":"GUAVA"}]`),
			})
		if status != http.StatusOK {
			t.Fatalf("the input answered %d: %s", status, body)
		}
		settled(t, "command-evidence-input", sessionstore.DispositionApplied)
	})

	t.Run("restore", func(t *testing.T) {
		status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions/"+string(session)+"/restore",
			sessionwire.RestoreRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope("command-evidence-restore"),
				SessionID:       session,
			})
		if status != http.StatusOK && status != http.StatusAccepted {
			t.Fatalf("the restore answered %d: %s", status, body)
		}
		settled(t, "command-evidence-restore", sessionstore.DispositionApplied)
	})

	t.Run("interrupt", func(t *testing.T) {
		status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions/"+string(session)+"/interrupt",
			sessionwire.InterruptRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope("command-evidence-interrupt"),
				SessionID:       session,
			})
		if status != http.StatusOK && status != http.StatusAccepted {
			t.Fatalf("the interrupt answered %d: %s", status, body)
		}
		// An interrupt of an idle session is a legitimate NO-OP: there is no
		// turn to stop. What the boundary claim needs is that the store settled
		// it FROM THE RUNTIME'S FRAME, whichever terminal that frame named, so
		// the row accepts either and pins the attempt.
		orchestrationtest.PooledWait(t, "the interrupt settled", 90*time.Second, func() bool {
			state := world.CommandState(ctx, orchestrationtest.PooledTenantA, session, "command-evidence-interrupt")
			return state == sessionstore.InboxStateApplied || state == sessionstore.InboxStateRejected
		})
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: orchestrationtest.PooledTenantA, SessionID: session, CommandID: "command-evidence-interrupt",
		})
		if err != nil {
			t.Fatalf("reading the interrupt: %v", err)
		}
		t.Logf("the interrupt settled: state=%q outcome=%+v", entry.Record.State, entry.Record.Outcome)
		if entry.Record.Outcome == nil || entry.Record.Outcome.AttemptID == "" {
			t.Fatalf("the interrupt settled with outcome %+v, want one naming its attempt", entry.Record.Outcome)
		}
	})

	// gate_response is the fifth kind and is driven end to end, with the agent
	// continuing, in factory_gate_integration_test.go. It is named here rather
	// than repeated, so this file's claim to cover the vocabulary is honest.
}

// TestTheKitsCommandVocabularyIsTheReleasedOne guards the kit's own restatement
// of a vocabulary it does not own.
//
// It exists because the constants would otherwise be decorative, and because
// this exact kind of fixture has now been overtaken TWICE: "gate_response" was
// an unknown kind until harness v0.35.0 named it, and "restore" until v0.36.0
// did. Both times a row built on the old set went green the day the set widened,
// while whatever it was guarding was live. host v0.5.0 had to move its own
// closure-validation row off "restore" for exactly this reason.
func TestTheKitsCommandVocabularyIsTheReleasedOne(t *testing.T) {
	kinds := orchestrationtest.PooledKinds()
	if len(kinds) != 5 {
		t.Fatalf("the kit names %d command kinds, want 5: %v", len(kinds), kinds)
	}
	for _, kind := range kinds {
		if !runtimecommand.Kind(kind).Valid() {
			t.Fatalf("the kit names %q, which harness does not apply", kind)
		}
	}

	// THE UNKNOWN KIND MUST BE UNKNOWN TO BOTH VOCABULARIES, now and after the
	// next widening. harness's Closure.Validate tracks Kind.Valid deliberately,
	// so this one check covers the closure path as well as the apply path.
	if runtimecommand.Kind(orchestrationtest.PooledUnknownKind).Valid() {
		t.Fatalf("%q is a kind harness applies; the kit's unknown-kind fixture has been overtaken again",
			orchestrationtest.PooledUnknownKind)
	}
	closure := runtimecommand.Closure{
		CommandID:           "command-vocabulary",
		RuntimeCommandID:    uuid.MustParse("6f1c2d3e-4a5b-4c6d-8e7f-9a0b1c2d3e4f"),
		Kind:                runtimecommand.Kind(orchestrationtest.PooledUnknownKind),
		AttemptID:           "attempt-vocabulary",
		AttemptJournalEpoch: 1,
	}
	if err := closure.Validate(); err == nil {
		t.Fatalf("harness accepted a closure naming %q; the kit's unknown-kind fixture no longer discriminates",
			orchestrationtest.PooledUnknownKind)
	}
}

// TestACommandTheRuntimeRefusesNeverSettles is the falsifier for "nothing
// vouches any more".
//
// The v0.10.0 round's headline is an ABSENCE: the kit's last recorder-backed
// evidence arm is gone, so a command the product runtime did not really apply
// cannot settle. An absence is worth nothing unless its opposite is
// observable, so this drives one.
//
// The command is admitted into the disposition inbox directly, carrying a kind
// NEITHER vocabulary has ever held. That is deliberate: Factory admits only the
// five, so there is no route that produces this, and the point is not to model
// a Factory that misbehaves. The point is to put a command in front of the real
// Host and watch the real store decline to settle it — which is exactly what a
// reader answering from a list of dispatches this module made would have
// papered over.
//
// # WHICH refusal this measures, said exactly
//
// MEASURED: the record stays `pending`, with no claim and no outcome. So the
// refusal is HOST'S OWN KIND GATE, not the kit runtime's — host's apply.go
// deliberately does not reject an unknown kind, because "a newer Factory may
// admit a kind a newer Host applies", so it leaves the record for a Host that
// understands it. The kit's runtime never sees the command.
//
// THE KIT'S OWN `default` ARM IS THEREFORE UNREACHABLE THROUGH A REAL HOST
// TODAY, and this case does not pretend to exercise it: the kit names all five
// kinds, so there is no kind Host would dispatch and the kit would refuse. The
// arm exists for the next widening, and it is the honest shape for one — a
// refusal before any durable write, leaving the record for a Host that knows
// the kind. What this case does prove is the property the round is about: a
// command nothing applied does not settle.
func TestACommandTheRuntimeRefusesNeverSettles(t *testing.T) {
	ctx, world, _, served := createWorld(t, "orchestrationtest-refusal-host")
	const (
		session = sessionwire.SessionID("session-refusal")
		create  = sessionwire.CommandID("command-refusal-create")
		refused = sessionwire.CommandID("command-refusal-unknown")
	)
	status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(create)),
		SessionID:       session,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(`[{"type":"text","text":"HONEYBERRY"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "the create settled", 90*time.Second, func() bool {
		return world.CommandState(ctx, orchestrationtest.PooledTenantA, session, create) == sessionstore.InboxStateApplied
	})

	entry, err := world.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{
		TenantID: orchestrationtest.PooledTenantA, SessionID: session,
	})
	if err != nil {
		t.Fatalf("reading the session's binding: %v", err)
	}
	runtimeCommand, err := uuid.New()
	if err != nil {
		t.Fatalf("minting a runtime command id: %v", err)
	}
	now := time.Now().UTC()
	if _, _, err := world.Store.AdmitDispositionCommand(ctx, sessionstore.AdmitDispositionCommandRequest{
		TenantID:                 orchestrationtest.PooledTenantA,
		SessionID:                session,
		CommandID:                refused,
		Binding:                  entry.Record.Binding,
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(runtimeCommand.String()),
		Kind:                     sessionstore.CommandKind(orchestrationtest.PooledUnknownKind),
		Payload:                  []byte(`{"version":1,"command_id":"` + string(refused) + `"}`),
		AcceptedAt:               now,
		ApplyDeadline:            now.Add(10 * time.Minute),
	}); err != nil {
		t.Fatalf("admitting the refused command: %v", err)
	}

	// AN ABSENCE, so it is sampled rather than polled for. The Host's consumer
	// reaches this record on its next pass; what must not happen is a terminal
	// state, because nothing applied it.
	time.Sleep(5 * time.Second)
	got, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID: orchestrationtest.PooledTenantA, SessionID: session, CommandID: refused,
	})
	if err != nil {
		t.Fatalf("reading the refused command: %v", err)
	}
	t.Logf("the refused command is %q with outcome %+v", got.Record.State, got.Record.Outcome)
	if got.Record.State == sessionstore.InboxStateApplied {
		t.Fatalf("a command the runtime refused settled %q with outcome %+v; something is vouching again",
			got.Record.State, got.Record.Outcome)
	}
	if got.Record.Outcome != nil && got.Record.Outcome.Kind == sessionstore.DispositionApplied {
		t.Fatalf("a command the runtime refused carries an APPLIED outcome: %+v", got.Record.Outcome)
	}
}
