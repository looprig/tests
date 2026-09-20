//go:build integration

// This file is the case the whole create/restore chain exists for, and the one
// the lane could not write until today.
//
// # What was wrong, in one paragraph
//
// Factory admits a session's FIRST command as a `create` disposition record.
// Until harness v0.36.0, runtimecommand.Kind named three kinds and `create` was
// not one, so host v0.4.0's adapter refused it AFTER it had durably begun the
// dispatch attempt: no disposition frame, nothing to settle from, a deadline
// sweep that skips attempt-bearing records, and a successor that refused the
// same kind. Every session was wedged on its first command. This lane could not
// see it, because its product runtime VOUCHED for its own dispatches -- the
// finding it raised at v0.9.0 as F2.
//
// harness v0.36.0 adds KindCreate and KindRestore. host v0.5.0 decodes a
// create's Core CreateRequest and re-presents its blocks in the input-shaped
// body a BlockDecoder reads. And the bump ALONE would have been worse than the
// wedge: a create crossing with no blocks applies nothing and settles
// `applied`, so the user's first message is dropped in silence with the record
// looking perfectly settled.
//
// # So the assertion has two halves, and the second is the one that matters
//
// `applied` alone is exactly what the payload defect produces. Every case below
// that admits a create asserts BOTH that it settles and that its message
// reached the MODEL REQUEST -- and does so BLOCK BY BLOCK, because a product
// that concatenated a multi-block message into one string, or dropped a
// non-text block, would pass every substring check while losing precisely what
// a multimodal first message is for. Host's own gate recorded that gap as F3:
// every create fixture in that repository carries a single text block.
//
// Nothing here vouches for anything. The kit's evidence reader is the embedded
// real harness session store and nothing else, so a create that did not really
// apply does not settle.

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// The three texts of the multi-block first message, and the image beside them.
//
// Three distinct words rather than one repeated: a concatenation keeps all
// three substrings, so only the BLOCK COUNT and the per-block text can tell the
// faithful case from the lossy one.
const (
	createWordA = "ALPHINE"
	createWordB = "BERGAMOT"
	createWordC = "CARDAMOM"
)

// createFirstMessage is the multi-block, multimodal first message a create
// carries. It is built with Core's own encoder, so it is the shape Factory
// stores and the shape a Host's decoder must read.
func createFirstMessage(t *testing.T) json.RawMessage {
	t.Helper()
	blocks := []content.Block{
		&content.TextBlock{Text: createWordA},
		&content.TextBlock{Text: createWordB},
		// THE NON-TEXT BLOCK. A text-only decoder drops it and every substring
		// assertion still passes.
		&content.ImageBlock{
			MediaType: content.MediaTypeImagePNG,
			Source:    content.ImageSource{Data: []byte{0x89, 0x50, 0x4e, 0x47}},
		},
		&content.TextBlock{Text: createWordC},
	}
	encoded, err := content.MarshalBlocks(blocks)
	if err != nil {
		t.Fatalf("encoding the create's first message: %v", err)
	}
	return json.RawMessage(encoded)
}

// createWorld stands up one durable plane, one pooled Host and one real
// Factory, for a single tenant.
func createWorld(t *testing.T, id sessionwire.HostID) (context.Context, *orchestrationtest.PooledWorld, *orchestrationtest.PooledHost, *orchestrationtest.PooledFactory) {
	t.Helper()
	ctx := placementContext(t)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{orchestrationtest.PooledTenantA},
	})
	pooled := orchestrationtest.StartPooledHost(t, ctx, world, id, 4)
	orchestrationtest.AwaitAdvertised(t, world, id)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "orchestrationtest-create-replica", nil)
	return ctx, world, pooled, served
}

// TestACreatesMultiBlockFirstMessageReachesTheModelAndSettlesApplied is the
// headline case: a real Factory admits a create carrying a multi-block,
// multimodal first message; a real Host dispatches it under an attempt over a
// real HostLink; the store settles it `applied` from the runtime's OWN harness
// evidence; and every block of that message reaches the model request.
func TestACreatesMultiBlockFirstMessageReachesTheModelAndSettlesApplied(t *testing.T) {
	ctx, world, pooled, served := createWorld(t, "orchestrationtest-create-host")
	const (
		session = sessionwire.SessionID("session-create-first-message")
		create  = sessionwire.CommandID("command-create-first-message")
	)

	status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(create)),
		SessionID:       session,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          createFirstMessage(t),
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}

	t.Run("it settles applied, from harness evidence alone", func(t *testing.T) {
		took := orchestrationtest.PooledWait(t, "the create settled applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, orchestrationtest.PooledTenantA, session, create) == sessionstore.InboxStateApplied
		})
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: orchestrationtest.PooledTenantA, SessionID: session, CommandID: create,
		})
		if err != nil {
			t.Fatalf("reading the settled create: %v", err)
		}
		t.Logf("the create settled after %v: state=%q outcome=%+v", took.Round(time.Millisecond), entry.Record.State, entry.Record.Outcome)
		if entry.Record.Outcome == nil || entry.Record.Outcome.Kind != sessionstore.DispositionApplied {
			t.Fatalf("the create settled with outcome %+v, want applied", entry.Record.Outcome)
		}
		// UNDER AN ATTEMPT. A settlement with no attempt would mean the store
		// settled from something other than an authorized dispatch.
		if entry.Record.Outcome.AttemptID == "" {
			t.Fatalf("the create settled with no attempt: %+v", entry.Record.Outcome)
		}
		// And it crossed a REAL HostLink: the dial is the derived per-tenant
		// address, never the advertised base.
		if pooled.CountPath(orchestrationtest.TenantPath(orchestrationtest.PooledTenantA)) == 0 {
			t.Fatalf("no HostLink dial reached the tenant path; the Host saw %v", pooled.UpgradePaths())
		}
		if verbatim := pooled.CountPath("/"); verbatim != 0 {
			t.Fatalf("Factory dialled the base verbatim %d times", verbatim)
		}
	})

	t.Run("THE REGRESSION: the turn is not empty, and every block arrived", func(t *testing.T) {
		// A create that settles with an EMPTY turn is what the bump without
		// host v0.5.0's decode arm produces, and it is indistinguishable from
		// success at the store. So the model is asked.
		orchestrationtest.PooledWait(t, "the first message reached the model", 60*time.Second, func() bool {
			return len(world.LLM.UserBlocksContaining(0, createWordA)) > 0
		})
		blocks := world.LLM.UserBlocksContaining(0, createWordA)
		t.Logf("the model's first user message carried %d blocks: %s", len(blocks), mustEncodeBlocks(t, blocks))

		// FOUR BLOCKS, not one. A concatenating decoder keeps every substring
		// and fails here.
		if len(blocks) != 4 {
			t.Fatalf("the model saw %d blocks, want the 4 the create carried: %s", len(blocks), mustEncodeBlocks(t, blocks))
		}
		var texts []string
		images := 0
		for _, block := range blocks {
			switch typed := block.(type) {
			case *content.TextBlock:
				texts = append(texts, typed.Text)
			case *content.ImageBlock:
				images++
				if typed.MediaType != content.MediaTypeImagePNG {
					t.Fatalf("the image block arrived as %q, want %q", typed.MediaType, content.MediaTypeImagePNG)
				}
				if len(typed.Source.Data) == 0 {
					t.Fatalf("the image block arrived with no data: %+v", typed.Source)
				}
			default:
				t.Fatalf("an unexpected block type reached the model: %T", block)
			}
		}
		if images != 1 {
			t.Fatalf("the model saw %d image blocks, want 1: a text-only decoder drops it and every substring check still passes", images)
		}
		want := []string{createWordA, createWordB, createWordC}
		if len(texts) != len(want) {
			t.Fatalf("the model saw texts %v, want %v each in its own block", texts, want)
		}
		for i, text := range want {
			if texts[i] != text {
				t.Fatalf("block %d carried %q, want %q: the order is the message", i, texts[i], text)
			}
		}
	})

	t.Run("and the runtime really ran a turn", func(t *testing.T) {
		// The journal is the authority on "a turn happened". A create that
		// applied nothing leaves TurnDone at zero while the record says
		// `applied`, which is the whole failure this case exists for.
		runtimeID := world.RuntimeSessionID(t, ctx, orchestrationtest.PooledTenantA, session)
		orchestrationtest.PooledWait(t, "the create's turn finished", 60*time.Second, func() bool {
			return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, orchestrationtest.PooledTenantA, runtimeID) >= 1
		})
		started := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world, orchestrationtest.PooledTenantA, runtimeID)
		if started != 1 {
			t.Fatalf("the journal holds %d SessionStarted events, want exactly 1", started)
		}
	})
}

// TestABareCreateSettlesAppliedAndDrivesNoTurn is the create case's necessary
// complement.
//
// harness makes Blocks OPTIONAL for KindCreate so that a session created with
// no opening message can still settle. A product that refused a bare create --
// or that invented an empty turn for one -- would be wrong in the other
// direction, and neither is visible from the record alone.
func TestABareCreateSettlesAppliedAndDrivesNoTurn(t *testing.T) {
	ctx, world, _, served := createWorld(t, "orchestrationtest-bare-create-host")
	const (
		session = sessionwire.SessionID("session-bare-create")
		create  = sessionwire.CommandID("command-bare-create")
	)
	status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(create)),
		SessionID:       session,
		AgentID:         orchestrationtest.PooledAgent,
	})
	if status != http.StatusCreated {
		t.Fatalf("the bare create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "the bare create settled applied", 90*time.Second, func() bool {
		return world.CommandState(ctx, orchestrationtest.PooledTenantA, session, create) == sessionstore.InboxStateApplied
	})
	// ZERO model requests. The session exists and the runtime is resident; what
	// must not have happened is a turn.
	if requests := len(world.LLM.Requests()); requests != 0 {
		t.Fatalf("a bare create drove %d model requests, want 0", requests)
	}
}

// TestARestoreSettlesApplied is the fifth kind.
//
// A restore carries NOTHING -- Core's RestoreRequest has no blocks member and
// Admitted.Validate refuses a restore that carries any -- so what it proves is
// that the kind crosses, is dispatched under an attempt, and settles from the
// runtime's own evidence. Under host v0.4.0 it wedged exactly as a create did.
func TestARestoreSettlesApplied(t *testing.T) {
	ctx, world, _, served := createWorld(t, "orchestrationtest-restore-host")
	const (
		session = sessionwire.SessionID("session-restore")
		create  = sessionwire.CommandID("command-restore-create")
		restore = sessionwire.CommandID("command-restore")
	)
	status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(create)),
		SessionID:       session,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(`[{"type":"text","text":"hello"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "the create settled", 90*time.Second, func() bool {
		return world.CommandState(ctx, orchestrationtest.PooledTenantA, session, create) == sessionstore.InboxStateApplied
	})
	requestsBefore := len(world.LLM.Requests())

	status, body = served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions/"+string(session)+"/restore",
		sessionwire.RestoreRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(string(restore)),
			SessionID:       session,
		})
	if status != http.StatusOK && status != http.StatusAccepted {
		t.Fatalf("the restore answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "the restore settled applied", 90*time.Second, func() bool {
		return world.CommandState(ctx, orchestrationtest.PooledTenantA, session, restore) == sessionstore.InboxStateApplied
	})
	entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID: orchestrationtest.PooledTenantA, SessionID: session, CommandID: restore,
	})
	if err != nil {
		t.Fatalf("reading the settled restore: %v", err)
	}
	if entry.Record.Outcome == nil || entry.Record.Outcome.Kind != sessionstore.DispositionApplied {
		t.Fatalf("the restore settled with outcome %+v, want applied", entry.Record.Outcome)
	}
	// AND IT DROVE NO TURN. A restore that carried a payload, or that borrowed
	// the input arm, would show as a model request here.
	if after := len(world.LLM.Requests()); after != requestsBefore {
		t.Fatalf("the restore drove %d further model requests, want 0", after-requestsBefore)
	}
}

func mustEncodeBlocks(t *testing.T, blocks []content.Block) string {
	t.Helper()
	encoded, err := content.MarshalBlocks(blocks)
	if err != nil {
		return fmt.Sprintf("<unencodable: %v>", err)
	}
	return string(encoded)
}
