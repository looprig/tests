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
	"bytes"
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

	// createImageIndex is where the non-text block sits. It is INSIDE the
	// message rather than at either end, so a decoder that reordered blocks
	// while keeping the texts in sequence is caught.
	createImageIndex = 2
)

// createTexts is the message by POSITION, with the image's slot left empty.
// Asserting against it is what binds order rather than mere presence.
var createTexts = map[int]string{0: createWordA, 1: createWordB, 3: createWordC}

// createImageBytes is the image's payload, compared BYTE FOR BYTE at the model.
// A PNG magic number, so a substitution is obvious in a failure message.
var createImageBytes = []byte{0x89, 0x50, 0x4e, 0x47}

// createFirstMessage is the multi-block, multimodal first message a create
// carries. It is built with Core's own encoder, so it is the shape Factory
// stores and the shape a Host's decoder must read.
func createFirstMessage(t *testing.T) json.RawMessage {
	t.Helper()
	blocks := []content.Block{
		&content.TextBlock{Text: createTexts[0]},
		&content.TextBlock{Text: createTexts[1]},
		// THE NON-TEXT BLOCK. A text-only decoder drops it and every substring
		// assertion still passes.
		&content.ImageBlock{
			MediaType: content.MediaTypeImagePNG,
			Source:    content.ImageSource{Data: createImageBytes},
		},
		&content.TextBlock{Text: createTexts[3]},
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
		// EVERY BLOCK IS PINNED BY POSITION, including the non-text one.
		//
		// An earlier version of this row collected the texts in encounter order
		// and merely COUNTED the images, which left two mutants alive: moving
		// the image to index 0 with the texts untouched, and replacing its
		// bytes. Both are exactly host F3's axis -- that repository asserts a
		// single text block, and this lane exists to be the place a non-text
		// block is bound -- so both are now assertion kills.
		for i, block := range blocks {
			if i == createImageIndex {
				image, ok := block.(*content.ImageBlock)
				if !ok {
					t.Fatalf("block %d is %T, want the image block at that position: the order is the message", i, block)
				}
				if image.MediaType != content.MediaTypeImagePNG {
					t.Fatalf("the image block arrived as %q, want %q", image.MediaType, content.MediaTypeImagePNG)
				}
				// THE BYTES, not their length. A decoder that re-encoded or
				// substituted the payload would keep the length and lose the
				// image.
				if !bytes.Equal(image.Source.Data, createImageBytes) {
					t.Fatalf("the image block arrived carrying %v, want the %v the create sent", image.Source.Data, createImageBytes)
				}
				if image.Source.URL != "" {
					t.Fatalf("the image block arrived as a URL reference %q; it was sent inline", image.Source.URL)
				}
				continue
			}
			text, ok := block.(*content.TextBlock)
			if !ok {
				t.Fatalf("block %d is %T, want a text block", i, block)
			}
			if want := createTexts[i]; text.Text != want {
				t.Fatalf("block %d carried %q, want %q: the order is the message", i, text.Text, want)
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
	// THE CREATE'S SETTLEMENT IS NOT THE END OF THE CREATE'S TURN, and taking
	// the baseline here without waiting is a real flake -- it failed 1 run in 5,
	// `make check` among them.
	//
	// harness writes the disposition frame BEFORE the effect: that ordering is
	// the crash-safety property (a redelivery that finds a prefix knows the
	// command was applied), so `applied` means "durably recorded", not
	// "finished". The create's turn is still in flight, and a baseline sampled
	// at the settlement attributes that turn to the restore --
	// `the restore drove 1 further model requests, want 0`.
	//
	// So the wait is on the TURN, which is what the headline case already does.
	runtimeID := world.RuntimeSessionID(t, ctx, orchestrationtest.PooledTenantA, session)
	orchestrationtest.PooledWait(t, "the create's own turn finished", 60*time.Second, func() bool {
		return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, orchestrationtest.PooledTenantA, runtimeID) >= 1
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

// TestASuccessorClosesAStrandedCreateAndTheStreamUnblocks is the CROSS-MODULE
// migration case: what an operator's upgrade from host v0.4.0 actually looks
// like.
//
// # What a v0.4.0 Host left behind
//
// It refused a create from inside its runtime adapter, which Host reaches only
// AFTER BeginAttempt has durably authorized the dispatch. So the record sits
// `applying` with an attempt and no disposition frame: the store has nothing to
// settle from, the deadline sweep skips attempt-bearing records so it never
// expires, and every later command on that session is blocked behind it,
// because the consumer will not advance its cursor past a non-terminal record.
// The session existed, the agent was resident, and nobody could talk to it.
//
// host v0.5.0's claim is that this needs NO operator action: the next Host to
// take the session closes the attempt `not_applied` under its own strictly
// later journal grant, and the stream continues. Host proves that against its
// own applier; this drives it through a real Factory and two real Hosts, which
// is the shape an upgrade has.
//
// # The claim that is easiest to get wrong
//
// The create's first message is NOT recovered. It was never delivered, and the
// only honest outcome is that those words never reach the model -- a successor
// that "helpfully" replayed them would be inventing a turn the user did not
// see accepted. The case asserts that absence as hard as it asserts the
// recovery.
func TestASuccessorClosesAStrandedCreateAndTheStreamUnblocks(t *testing.T) {
	ctx := placementContext(t)
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{orchestrationtest.PooledTenantA},
	})
	// THE PREDECESSOR: a Host whose runtime refuses a create after the attempt
	// is durable, which is host v0.4.0's shape exactly.
	predecessor := orchestrationtest.StartPooledHost(t, ctx, world, "orchestrationtest-stranding-host", 4)
	predecessor.Rig.RefuseCreates(true)
	orchestrationtest.AwaitAdvertised(t, world, predecessor.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "orchestrationtest-migration-replica", nil)

	const (
		session      = sessionwire.SessionID("session-stranded-create")
		create       = sessionwire.CommandID("command-stranded-create")
		input        = sessionwire.CommandID("command-behind-the-strand")
		strandedWord = "DAMSON"
		behindWord   = "ELDERFLOWER"
	)

	status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(create)),
		SessionID:       session,
		AgentID:         orchestrationtest.PooledAgent,
		Blocks:          json.RawMessage(`[{"type":"text","text":"` + strandedWord + `"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}

	var attemptID sessionstore.DispositionAttemptID
	t.Run("the create strands: applying, with an attempt, bounded by nothing", func(t *testing.T) {
		orchestrationtest.PooledWait(t, "the create reached applying with an attempt", 90*time.Second, func() bool {
			entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
				TenantID: orchestrationtest.PooledTenantA, SessionID: session, CommandID: create,
			})
			return err == nil && entry.Record.State == sessionstore.InboxStateApplying && entry.Record.Attempt != nil
		})
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: orchestrationtest.PooledTenantA, SessionID: session, CommandID: create,
		})
		if err != nil {
			t.Fatalf("reading the stranded create: %v", err)
		}
		attemptID = entry.Record.Attempt.AttemptID
		t.Logf("the create is stranded: state=%q attempt=%s outcome=%+v", entry.Record.State, attemptID, entry.Record.Outcome)
		if entry.Record.Outcome != nil {
			t.Fatalf("the stranded create already has an outcome %+v; the premise is false", entry.Record.Outcome)
		}
		if attemptID == "" {
			t.Fatalf("the stranded create carries no attempt; a refusal BEFORE the attempt is a different and bounded failure")
		}
	})

	t.Run("and everything behind it is blocked", func(t *testing.T) {
		status, body := served.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions/"+string(session)+"/input",
			sessionwire.InputRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope(string(input)),
				SessionID:       session,
				Blocks:          json.RawMessage(`[{"type":"text","text":"` + behindWord + `"}]`),
			})
		if status != http.StatusOK {
			t.Fatalf("the input behind the strand answered %d: %s", status, body)
		}
		// AN ABSENCE, so it is sampled rather than polled for: the consumer
		// will not advance its cursor past a non-terminal record, so the input
		// must stay unapplied for as long as the create is stranded.
		time.Sleep(3 * time.Second)
		if state := world.CommandState(ctx, orchestrationtest.PooledTenantA, session, input); state == sessionstore.InboxStateApplied {
			t.Fatalf("the input behind a stranded create settled %q; the premise that the stream is blocked is false", state)
		}
		if world.LLM.SawInRequest(0, behindWord) {
			t.Fatalf("the input behind a stranded create reached the model; the stream is not blocked")
		}
	})

	t.Run("a successor closes the attempt not_applied and the stream continues", func(t *testing.T) {
		predecessor.Stop()
		orchestrationtest.PooledWait(t, "the registry stops reporting a live owner", 60*time.Second, func() bool {
			owner, found, _ := served.Directory.Owner(ctx, orchestrationtest.PooledTenantA, session)
			return !found || owner.Residency != sessionwire.SessionResidencyResident ||
				!owner.Accepting || !owner.ExpiresAt.After(time.Now())
		})
		successor := orchestrationtest.StartPooledHost(t, ctx, world, "orchestrationtest-successor-host", 7)
		orchestrationtest.AwaitAdvertised(t, world, successor.ID)

		// THE CREATE IS CLOSED, and the OUTCOME is asserted rather than the
		// state. `rejected` is reachable several ways -- a pre-attempt
		// rejection, a deadline sweep -- and only one of them is this claim:
		// the successor found the predecessor's attempt with no effect behind
		// it and closed it.
		orchestrationtest.PooledWait(t, "the successor closed the stranded create", 120*time.Second, func() bool {
			state := world.CommandState(ctx, orchestrationtest.PooledTenantA, session, create)
			return state == sessionstore.InboxStateRejected || state == sessionstore.InboxStateApplied
		})
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: orchestrationtest.PooledTenantA, SessionID: session, CommandID: create,
		})
		if err != nil {
			t.Fatalf("reading the closed create: %v", err)
		}
		t.Logf("the stranded create closed: state=%q outcome=%+v", entry.Record.State, entry.Record.Outcome)
		if entry.Record.Outcome == nil {
			t.Fatalf("the create closed with no outcome: %+v", entry.Record)
		}
		if entry.Record.Outcome.Kind != sessionstore.DispositionNotApplied {
			t.Fatalf("the create closed %q/%q, want not_applied: nothing ever applied it",
				entry.Record.State, entry.Record.Outcome.Kind)
		}
		if entry.Record.Outcome.AttemptID != attemptID {
			t.Fatalf("the closure names attempt %s, want the predecessor's %s",
				entry.Record.Outcome.AttemptID, attemptID)
		}
		// A STRICTLY LATER JOURNAL GRANT is what makes the closure safe: it is
		// how the successor can say the predecessor's attempt produced nothing
		// without racing a predecessor that is still running.
		if entry.Record.Outcome.AuthorJournalEpoch <= entry.Record.Outcome.AttemptJournalEpoch {
			t.Fatalf("the closure was authored at journal epoch %d against an attempt at %d, want strictly later",
				entry.Record.Outcome.AuthorJournalEpoch, entry.Record.Outcome.AttemptJournalEpoch)
		}

		// THE STREAM CONTINUES: the input behind it settles and reaches the
		// model.
		orchestrationtest.PooledWait(t, "the input behind the strand settled", 120*time.Second, func() bool {
			return world.CommandState(ctx, orchestrationtest.PooledTenantA, session, input) == sessionstore.InboxStateApplied
		})
		orchestrationtest.PooledWait(t, "the input behind the strand reached the model", 60*time.Second, func() bool {
			return world.LLM.SawInRequest(0, behindWord)
		})

		// AND THE CREATE'S OWN WORDS NEVER DO. They were never delivered; a
		// successor that replayed them would be inventing a turn the user never
		// saw accepted. This is the half an over-helpful recovery gets wrong.
		if world.LLM.SawInRequest(0, strandedWord) {
			t.Fatalf("the stranded create's first message %q reached the model; it was never delivered and must not be invented",
				strandedWord)
		}
	})
}
