//go:build integration

// Restore across a changed MCP catalog: a Session journaled under one set of MCP
// servers, brought back under a different one.
//
// This is the scenario design §Session restore is about, and its claim is a
// negative one, which is why it needs a real journal rather than a unit test:
//
//	"Changing servers, tools, schemas, auth posture, transports, or filters does
//	 not make old journal records unreadable. Historical MCP calls remain data."
//
// MCP connections are live resources and are never restored from journal bytes.
// What restores is the Session; the servers are recreated from CURRENT
// configuration and rediscovered, so the catalog a restored Session comes back
// to is whatever the servers offer today — which may be a catalog that no longer
// contains a tool the journal records the model calling.
//
// # Why these tests are worth their runtime
//
// Until recently they could not have passed, and not for a reason any unit test
// could have shown. A restored root Loop came back with zero tool bindings, so
// the adapter's install was rejected against a zero SessionID before it ever
// reached the requirements check — meaning NO external tool could install on ANY
// restored Session, and the entire catalog-drift-across-restore path was
// unreachable. The adapter's own tests passed throughout: they build Managers
// over freshly created Sessions. Only a real restore of a real journal reaches
// it, which is what these do.

package tests

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/mcp/pkg/client"
	mcpharness "github.com/looprig/mcp/pkg/harness"
	"github.com/looprig/mcp/pkg/transport/stdio"
	"github.com/looprig/storage/memstore"
)

// --- restore fixtures -------------------------------------------------------

// restorableRig is a rig whose Sessions can be shut down and brought back. It is
// separate from newSession (mcp_adapter_test.go) because that helper owns its
// rig and registers a shutdown cleanup, which is exactly what a restore test
// must control: the first leg has to end before the second can begin.
type restorableRig struct {
	rig   *rig.Rig
	store *sessionstore.Store
}

// newRestorableRig defines a rig over one scripted Loop.
//
// externalRev is the value the composition root contributes to the config
// fingerprint — mcpharness.Manager.ConfigDigest in a real application. Empty
// means "this application attached no external capability", which is both the
// default and what every journal written before the field existed carries.
func newRestorableRig(t *testing.T, store *sessionstore.Store, name string, llm *scriptLLM, externalRev string, extra ...rig.Option) *restorableRig {
	t.Helper()
	options := []rig.Option{
		rig.WithLoops(newLoop(t, name, llm, approveAll(t))),
		rig.WithPrimers(name),
		rig.WithActivePrimer(name),
		rig.WithSessionStore(store),
		rig.WithFingerprintFields(rig.ConfigFingerprintFields{ExternalCapabilityRev: externalRev}),
	}
	options = append(options, extra...)
	r, err := rig.Define(options...)
	if err != nil {
		t.Fatalf("rig.Define: %v", err)
	}
	return &restorableRig{rig: r, store: store}
}

// shutdown ends a Session leg, so the next one restores rather than shares.
func shutdown(t *testing.T, sess session.SessionController) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()
	if err := sess.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// journalMentions reports whether any journal record's rendered content contains
// want.
//
// It reads the durable event stream — the same records a transcript, an audit, or
// a support engineer reads — rather than any live adapter state, because the
// claim under test is about what SURVIVES. A live Manager could report anything;
// the journal is the thing that has to still make sense.
func journalMentions(t *testing.T, ctx context.Context, store *sessionstore.Store, id uuid.UUID, want string) bool {
	t.Helper()
	for _, ev := range eventsFor(t, ctx, store, id) {
		if strings.Contains(renderEvent(ev), want) {
			return true
		}
	}
	return false
}

// renderEvent flattens the parts of a journal record a historical MCP call would
// appear in.
//
// StepDone is the record that matters, and it is the only one that could be:
// ToolCallStarted and ToolCallCompleted are EPHEMERAL — they are for a live UI
// and never reach the journal — so the durable trace of a tool call is the
// conversation itself, which StepDone carries. That is the right place for the
// claim anyway: "historical MCP calls remain data" is a statement about the
// transcript a model and a human read back, not about a progress notification.
func renderEvent(ev event.Event) string {
	step, ok := ev.(event.StepDone)
	if !ok {
		return ""
	}
	var b strings.Builder
	for _, message := range step.Messages {
		switch typed := message.(type) {
		case *content.AIMessage:
			writeBlocks(&b, typed.Blocks)
		case *content.ToolResultMessage:
			writeBlocks(&b, typed.Blocks)
		case *content.UserMessage:
			writeBlocks(&b, typed.Blocks)
		}
	}
	return b.String()
}

// writeBlocks renders the block kinds a tool call and its result occupy: the
// model's tool_use (its name and its arguments) and the result that came back.
func writeBlocks(b *strings.Builder, blocks []content.Block) {
	for _, block := range blocks {
		switch typed := block.(type) {
		case *content.TextBlock:
			b.WriteString(typed.Text)
		case *content.ToolUseBlock:
			b.WriteString(typed.Name)
			b.Write(typed.Input)
		case *content.ToolResultBlock:
			writeBlocks(b, typed.Content)
		}
	}
}

// --- the tests --------------------------------------------------------------

// TestRestoreUnderChangedMCPCatalogServesTools is the load-bearing one: a real
// journal, written under a server whose catalog has since changed, restores and
// then SERVES MCP tools again.
//
// The catalog really does change across the boundary. The first leg drives the
// fixture's mutate tool to add echo2 at runtime and calls it, so the journal
// records a call to a tool that exists only in that process. The second leg
// restores against a FRESH fixture process, whose catalog has never heard of
// echo2 — so the Session comes back to a genuinely different offering than the
// one it was journaled under.
//
// Three claims, and the third is the one that was unreachable before the restored
// root Loop got its bindings back:
//
//  1. the restore is clean — a changed catalog is not a corrupt journal;
//  2. the historical MCP call is still readable journal data;
//  3. the restored Session can install and call MCP tools at all.
func TestRestoreUnderChangedMCPCatalogServesTools(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	store, err := sessionstore.Open(memstore.New())
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}

	// --- leg one: journal a call to a tool that will not exist next time ---
	llmOne := newScriptLLM(
		call("c1", mcpName("srv", fixtureToolMutate), `{"add":true}`),
		say("added echo2"),
		say("nudge done"),
		call("c2", mcpName("srv", fixtureToolMutated), `{"text":"historical-call"}`),
		say("called echo2"),
	)
	rigOne := newRestorableRig(t, store, "planner", llmOne, "")
	sessOne, err := rigOne.rig.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := sessOne.SessionID()
	loopOne := sessOne.ActiveLoop().ID()

	fOne := attach(t, sessOne, nil, fixtureBinding(t, "srv", mcpharness.ScopeSession, []string{"-mutate"}, nil))
	if err := fOne.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := fOne.adopter.Install(ctx, loopOne, "planner"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := sessOne.Submit(ctx, []content.Block{&content.TextBlock{Text: "make it"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	llmOne.waitRequests(t, 2)
	fOne.carryToNextGeneration(t, "srv", loopOne)

	if _, err := sessOne.Submit(ctx, []content.Block{&content.TextBlock{Text: "call it"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	reqs := llmOne.waitRequests(t, 5)
	// The premise: echo2 really was called and really did work. Without this the
	// journal claims below would be about a call that never happened.
	if !hasName(toolNames(reqs[3]), mcpName("srv", fixtureToolMutated)) {
		t.Fatalf("leg one was never offered %q; the journal will not record the call this test is about", mcpName("srv", fixtureToolMutated))
	}
	results := toolResults(reqs[4])
	if len(results) == 0 || strings.HasPrefix(results[len(results)-1], "error: ") {
		t.Fatalf("leg one's echo2 call did not succeed: %v", results)
	}
	shutdown(t, sessOne)

	// --- leg two: restore against a server that never had echo2 -------------
	llmTwo := newScriptLLM(say("restored and idle"))
	rigTwo := newRestorableRig(t, store, "planner", llmTwo, "")
	restored, err := rigTwo.rig.RestoreSession(ctx, id)
	if err != nil {
		t.Fatalf("RestoreSession under a changed MCP catalog: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), itTimeout)
		defer cancel()
		_ = restored.Shutdown(c)
	})

	// Claim 1: the restore is clean.
	assertCleanRestore(t, eventsFor(t, ctx, store, id))

	// Claim 2: the historical MCP call is still readable data. The tool it names
	// does not exist on any server running right now, and that changes nothing
	// about the record.
	if !journalMentions(t, ctx, store, id, "historical-call") {
		t.Error("the journal no longer carries the arguments of the historical MCP call")
	}
	if !journalMentions(t, ctx, store, id, mcpName("srv", fixtureToolMutated)) {
		t.Errorf("the journal no longer names %q: a call to a tool that has since vanished must remain data", mcpName("srv", fixtureToolMutated))
	}

	// Claim 3: the restored Session serves MCP tools. A fresh fixture: its
	// catalog has echo and mutate, and has never had echo2.
	loopTwo := restored.ActiveLoop().ID()
	fTwo := attach(t, restored, nil, fixtureBinding(t, "srv", mcpharness.ScopeSession, []string{"-mutate"}, nil))
	if err := fTwo.start(t); err != nil {
		t.Fatalf("Start on the restored Session: %v", err)
	}
	if err := fTwo.adopter.Install(ctx, loopTwo, "planner"); err != nil {
		t.Fatalf("Install on the restored Session: %v: this is the failure the restored-root-loop bindings fix addressed", err)
	}
	if _, err := restored.Submit(ctx, []content.Block{&content.TextBlock{Text: "anything"}}); err != nil {
		t.Fatalf("Submit on the restored Session: %v", err)
	}
	restoredReqs := llmTwo.waitRequests(t, 1)
	names := toolNames(restoredReqs[0])
	if !hasName(names, mcpName("srv", fixtureToolEcho)) {
		t.Errorf("the restored Session's model was offered %v, want the MCP toolset to include %q", names, mcpName("srv", fixtureToolEcho))
	}
	// And the catalog really is the changed one: echo2 is gone.
	if hasName(names, mcpName("srv", fixtureToolMutated)) {
		t.Errorf("the restored Session was offered %q, but a fresh server has never created it: the test is not exercising a changed catalog", mcpName("srv", fixtureToolMutated))
	}
}

// TestRemovedToolOnARestoredSessionReturnsToolUnavailable is design §Calling a
// tool step 4 on the far side of a restore: a Loop that restored, adopted a
// catalog, and is then overtaken by a server that removes the tool must get a
// structured result — not a dead turn, and not an unrestorable Session.
//
// It is the same mechanism TestRemovedToolReturnsStructuredResult proves on a
// fresh Session, and it is worth proving again here rather than assumed: the
// restored Loop is a different Loop, reconstructed from journal records, and
// "the adapter's tools work on it at all" was false until recently.
//
// Shutting the Adopter down is what makes the stale-snapshot state reachable
// deterministically. The alternative — calling the tool in the same turn as the
// removal — races the server's list-changed notification against the call, and a
// test that wins the race by luck proves nothing on the run where it loses.
func TestRemovedToolOnARestoredSessionReturnsToolUnavailable(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	store, err := sessionstore.Open(memstore.New())
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}

	// Leg one: a plain session, journaled and shut down. Its only job is to make
	// leg two a restore.
	llmOne := newScriptLLM(say("leg one done"))
	rigOne := newRestorableRig(t, store, "planner", llmOne, "")
	sessOne, err := rigOne.rig.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := sessOne.SessionID()
	if _, err := sessOne.Submit(ctx, []content.Block{&content.TextBlock{Text: "hello"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	llmOne.waitRequests(t, 1)
	shutdown(t, sessOne)

	// Leg two: restore, then drive the drift against the restored Loop.
	llmTwo := newScriptLLM(
		call("c1", mcpName("srv", fixtureToolMutate), `{"add":true}`),
		say("added"),
		say("nudge done"),
		call("c2", mcpName("srv", fixtureToolMutate), `{"add":false}`),
		say("removed"),
		call("c3", mcpName("srv", fixtureToolMutated), `{"text":"stale-args"}`),
		say("turn four done"),
	)
	rigTwo := newRestorableRig(t, store, "planner", llmTwo, "")
	restored, err := rigTwo.rig.RestoreSession(ctx, id)
	if err != nil {
		t.Fatalf("RestoreSession: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), itTimeout)
		defer cancel()
		_ = restored.Shutdown(c)
	})
	loopID := restored.ActiveLoop().ID()

	f := attach(t, restored, nil, fixtureBinding(t, "srv", mcpharness.ScopeSession, []string{"-mutate"}, nil))
	if err := f.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.adopter.Install(ctx, loopID, "planner"); err != nil {
		t.Fatalf("Install on the restored Session: %v", err)
	}

	if _, err := restored.Submit(ctx, []content.Block{&content.TextBlock{Text: "one"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	llmTwo.waitRequests(t, 2)
	f.carryToNextGeneration(t, "srv", loopID)

	// From here the restored Loop's toolset is frozen at the generation carrying
	// echo2.
	if err := f.adopter.Close(); err != nil {
		t.Fatalf("Adopter.Close: %v", err)
	}
	if _, err := restored.Submit(ctx, []content.Block{&content.TextBlock{Text: "remove it"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	llmTwo.waitRequests(t, 5)
	// The server has announced the removal and the client has fetched it: the
	// evidence is in hand BEFORE the call is made.
	f.waitCandidate(t, "srv", 2)

	if _, err := restored.Submit(ctx, []content.Block{&content.TextBlock{Text: "call it"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	reqs := llmTwo.waitRequests(t, 7)

	// The premise: the calling turn really was holding the removed tool.
	if !hasName(toolNames(reqs[5]), mcpName("srv", fixtureToolMutated)) {
		t.Fatalf("the calling turn was offered %v, want the retained %q", toolNames(reqs[5]), mcpName("srv", fixtureToolMutated))
	}
	// The claim: a structured result, not a Go error and not a dead turn —
	// request 7 exists at all only because the turn survived.
	results := toolResults(reqs[6])
	if len(results) == 0 {
		t.Fatal("the removed tool produced no result at all")
	}
	got := results[len(results)-1]
	if !strings.HasPrefix(got, "error: ") || !strings.Contains(got, "ToolUnavailable") {
		t.Fatalf("the removed tool returned %q, want an \"error: ...ToolUnavailable...\" result on a restored Session", got)
	}
	if !strings.Contains(got, fixtureToolMutated) || !strings.Contains(got, "srv") {
		t.Errorf("the result %q names neither the tool nor the binding; it is unattributable", got)
	}
}

// TestMCPConfigDriftRequiresExplicitAdoption is the fingerprint half of the
// story: when an application contributes its MCP identity to the configuration
// manifest, a changed MCP configuration is classified as warning drift. The
// fail-secure default rejects it; an application that explicitly accepts the
// warning gets a durable configuration epoch.
//
// There are no MCP servers in this test, and that is deliberate. What is under
// test is the seam, not the digest: that ExternalCapabilityRev reaches the
// journal, that restore compares it, and that a difference is durably reported
// rather than silently ignored. The digest's own properties — that it
// moves when a catalog moves, and that it never carries a secret — are
// mcp/pkg/harness's tests, where they can be asserted exhaustively instead of
// through a subprocess.
func TestMCPConfigDriftRequiresExplicitAdoption(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	store, err := sessionstore.Open(memstore.New())
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}

	const revOne = "aa08cfe9f431598f187f5bec202f211f3bc50325ec3e0415b63aabdcdbf9b5fd"
	const revTwo = "55b53f0b46cc411ac1abdc533628d8a4db57e2c202fae09b40187c72c5bc5645"

	sessOne, err := newRestorableRig(t, store, "planner", newScriptLLM(say("one")), revOne).rig.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := sessOne.SessionID()
	shutdown(t, sessOne)

	// The journal really carries the identity: without this the mismatch below
	// could be about any field at all.
	var stamped string
	for _, ev := range eventsFor(t, ctx, store, id) {
		if started, ok := ev.(event.SessionStarted); ok {
			stamped = started.Config.ExternalCapabilityRev
		}
	}
	if stamped != revOne {
		t.Fatalf("SessionStarted.Config.ExternalCapabilityRev = %q, want %q: the composition root's MCP identity never reached the journal", stamped, revOne)
	}

	// The same MCP configuration restores without creating a new epoch, so the
	// later adoption is about the change and not the field's mere presence.
	same, err := newRestorableRig(t, store, "planner", newScriptLLM(say("same")), revOne).rig.RestoreSession(ctx, id)
	if err != nil {
		t.Fatalf("restore under an UNCHANGED MCP configuration was refused: %v", err)
	}
	shutdown(t, same)

	// External catalog drift is Warn, so the fail-secure default refuses it.
	_, err = newRestorableRig(t, store, "planner", newScriptLLM(say("two-default")), revTwo).rig.RestoreSession(ctx, id)
	var rejected *session.RestoreRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("default policy error = %v, want *session.RestoreRejectedError", err)
	}

	// Explicit application policy accepts the warning and records exactly what
	// changed as configuration epoch 2.
	resumed, err := newRestorableRig(t, store, "planner", newScriptLLM(say("two")), revTwo, rig.WithRestoreDecider(session.AcceptAllDecider{})).rig.RestoreSession(ctx, id)
	if err != nil {
		t.Fatalf("explicit policy refused MCP configuration drift: %v", err)
	}
	shutdown(t, resumed)
	assertExternalCapabilityAdoption(t, ctx, store, id, revOne, revTwo)
}

// TestOldJournalWithoutExternalCapabilityRestores is the additive property where
// it actually matters: a Session journaled by a build that had no MCP identity in
// its fingerprint at all — every session that exists today — must restore under
// an application that still attaches none.
//
// pkg/event proves this at the value level. This proves it end to end, through a
// real journal and a real restore comparison, which is the layer that would
// actually break.
func TestOldJournalWithoutExternalCapabilityRestores(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	store, err := sessionstore.Open(memstore.New())
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}

	sessOne, err := newRestorableRig(t, store, "planner", newScriptLLM(say("one")), "").rig.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := sessOne.SessionID()
	shutdown(t, sessOne)

	for _, ev := range eventsFor(t, ctx, store, id) {
		if started, ok := ev.(event.SessionStarted); ok {
			if got := started.Config.ExternalCapabilityRev; got != "" {
				t.Fatalf("a rig that attached no external capability stamped %q, want empty", got)
			}
		}
	}

	restored, err := newRestorableRig(t, store, "planner", newScriptLLM(say("two")), "").rig.RestoreSession(ctx, id)
	if err != nil {
		t.Fatalf("a journal with no external capability failed to restore: %v", err)
	}
	shutdown(t, restored)
}

// --- the creation side: MCP identity stamped BEFORE the Session exists -------

// composeMCP is an application's MCP phase, in the order
// 2026-07-16-session-versioning-migration-design.md §Configuration freeze and
// adoption mandates: build the bindings, connect, discover, and report the
// identity of what was actually found — all before any Session exists.
//
// The Manager it returns has no SessionID, and that is the point. It cannot have
// one: the digest it produces is an INPUT to rig.Define, which is what
// rig.NewSession stamps onto SessionStarted, so the Session whose ID it would
// need is downstream of it. The caller binds it once the Session it serves has
// been created.
//
// The fixture binary is passed in rather than built here so both legs of a
// restore run the same command: stdio's RedactedOrigin is the command's base name
// and argument count, and it is part of a binding's identity, so a per-leg build
// path would be a configuration change this test never meant to make.
func composeMCP(t *testing.T, bin string, args []string) (*mcpharness.Manager, string) {
	t.Helper()
	tr, err := stdio.New(stdio.Config{Command: bin, Args: args})
	if err != nil {
		t.Fatalf("stdio.New: %v", err)
	}
	mgr, err := mcpharness.NewManager(
		[]mcpharness.Binding{{
			Name:   "srv",
			Server: client.Definition{Name: client.Name("srv"), Transport: tr},
			Scope:  mcpharness.ScopeSession,
			// Required: Start returns once the required bindings have settled, so
			// this is what makes the digest below a fact about a discovered server
			// rather than a race with one still connecting (see ConfigIdentity).
			Required:   true,
			Visibility: mcpharness.AllLoops(),
		}},
		mcpharness.Deps{Gates: refusingGates{}, Events: &recordingPublisher{}},
	)
	if err != nil {
		t.Fatalf("NewManager before a Session: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
		defer cancel()
		_ = mgr.Close(ctx)
	})
	if err := mgr.Start(itCtx(t)); err != nil {
		t.Fatalf("Start before a Session: %v", err)
	}
	return mgr, mgr.ConfigDigest()
}

// stampedRev returns the ExternalCapabilityRev the Session's journal recorded at
// SessionStarted.
func stampedRev(t *testing.T, ctx context.Context, store *sessionstore.Store, id uuid.UUID) string {
	t.Helper()
	for _, ev := range eventsFor(t, ctx, store, id) {
		if started, ok := ev.(event.SessionStarted); ok {
			return started.Config.ExternalCapabilityRev
		}
	}
	t.Fatalf("no SessionStarted in the journal of session %v", id)
	return ""
}

// TestMCPIdentityStampedAtCreationRestoresWithoutDrift is the creation side of
// the fingerprint story, and it is the one that was broken.
//
// TestMCPConfigDriftIsReportedThroughConfigMismatch proves the COMPARISON seam
// with literal revs. It cannot catch this, because it never builds a Manager: it
// asserts that two strings an application supplies are compared, which they are.
// The defect was that a real application could not produce the first of those
// strings at all — Deps.SessionID was required, so the Manager whose digest
// rig.Define needs could not exist before rig.NewSession had minted an ID. Every
// application therefore journaled an empty rev on its first run and computed a
// real one on its second, and reported drift, forever, on a configuration nobody
// had touched.
//
// So the claim here is the whole round trip an application actually makes:
// discover → stamp a REAL rev → restore under the SAME servers → no drift.
func TestMCPIdentityStampedAtCreationRestoresWithoutDrift(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	store, err := sessionstore.Open(memstore.New())
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}
	bin := buildMCPFixture(t)

	// --- run one: discover, then create --------------------------------------
	mgrOne, revOne := composeMCP(t, bin, []string{"-mutate"})
	if revOne == "" {
		t.Fatal("ConfigDigest before the Session is empty though a server was discovered: the application would stamp \"no external capability\" and mismatch against itself on every later run")
	}
	sessOne, err := newRestorableRig(t, store, "planner", newScriptLLM(say("one")), revOne).rig.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := sessOne.SessionID()
	if err := mgrOne.BindSession(id); err != nil {
		t.Fatalf("BindSession: %v", err)
	}

	// The journal really carries the discovered identity. Without this the
	// no-drift claim below could be two empty strings agreeing with each other,
	// which is exactly the broken state this test exists to exclude.
	if got := stampedRev(t, ctx, store, id); got != revOne {
		t.Fatalf("SessionStarted.Config.ExternalCapabilityRev = %q, want the discovered %q", got, revOne)
	}
	shutdown(t, sessOne)

	// --- run two: same servers, so no drift ---------------------------------
	mgrTwo, revTwo := composeMCP(t, bin, []string{"-mutate"})
	// The premise: rediscovering an UNCHANGED configuration yields the same
	// identity. A digest that moved between two identical runs would make the
	// restore below fail for a reason that is not the one under test.
	if revTwo != revOne {
		t.Fatalf("rediscovering the same servers gave %q, want the %q of run one: the digest is not stable across processes", revTwo, revOne)
	}
	restored, err := newRestorableRig(t, store, "planner", newScriptLLM(say("two")), revTwo).rig.RestoreSession(ctx, id)
	if err != nil {
		t.Fatalf("restore under UNCHANGED MCP configuration was refused: %v\nthis is the spurious mismatch the pre-Session digest exists to prevent: run one journaled its MCP identity, run two recomputed the same one, and they must compare equal", err)
	}
	if err := mgrTwo.BindSession(restored.SessionID()); err != nil {
		t.Fatalf("BindSession on the restored Session: %v", err)
	}
	shutdown(t, restored)
}

// TestChangedMCPServersAdoptDriftAtRestore is the other half: the digest must
// move when the servers really do, and that movement must be durably adopted as
// informational drift rather than breaking the Session.
//
// Both legs run the same command with the same number of arguments, so the
// binding's declared configuration — its transport, its origin, its filter, its
// limits — is byte-for-byte identical, and the ONLY thing that differs is the
// catalog the server turned out to serve. That is deliberate: it proves the digest
// covers what was NEGOTIATED, which is the half no application could compute for
// itself and the half that needs a live connection before the Session exists.
func TestChangedMCPServersAdoptDriftAtRestore(t *testing.T) {
	t.Parallel()
	ctx := itCtx(t)
	store, err := sessionstore.Open(memstore.New())
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}
	bin := buildMCPFixture(t)

	mgrOne, revOne := composeMCP(t, bin, []string{"-mutate"})
	sessOne, err := newRestorableRig(t, store, "planner", newScriptLLM(say("one")), revOne).rig.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := sessOne.SessionID()
	if err := mgrOne.BindSession(id); err != nil {
		t.Fatalf("BindSession: %v", err)
	}
	shutdown(t, sessOne)

	// A different server: same argv shape, a different toolset.
	_, revTwo := composeMCP(t, bin, []string{"-crash"})
	if revTwo == revOne {
		t.Fatalf("a server offering a different toolset digested to the same %q: the identity does not cover the negotiated catalog, and this test proves nothing", revOne)
	}

	_, err = newRestorableRig(t, store, "planner", newScriptLLM(say("two-default")), revTwo).rig.RestoreSession(ctx, id)
	var rejected *session.RestoreRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("default policy error = %v, want *session.RestoreRejectedError", err)
	}

	resumed, err := newRestorableRig(t, store, "planner", newScriptLLM(say("two")), revTwo, rig.WithRestoreDecider(session.AcceptAllDecider{})).rig.RestoreSession(ctx, id)
	if err != nil {
		t.Fatalf("explicit policy refused drift from changed MCP servers: %v", err)
	}
	shutdown(t, resumed)
	assertExternalCapabilityAdoption(t, ctx, store, id, revOne, revTwo)
}

func assertExternalCapabilityAdoption(t *testing.T, ctx context.Context, store *sessionstore.Store, id uuid.UUID, oldRev, newRev string) {
	t.Helper()
	var adoptions []event.ConfigurationAdopted
	for _, ev := range eventsFor(t, ctx, store, id) {
		if adopted, ok := ev.(event.ConfigurationAdopted); ok {
			adoptions = append(adoptions, adopted)
		}
	}
	if len(adoptions) != 1 {
		t.Fatalf("ConfigurationAdopted count = %d, want 1", len(adoptions))
	}
	adopted := adoptions[0]
	if adopted.Epoch != 2 || adopted.Source != event.DecisionSourcePolicy {
		t.Fatalf("ConfigurationAdopted epoch/source = %d/%q, want 2/%q", adopted.Epoch, adopted.Source, event.DecisionSourcePolicy)
	}
	if adopted.Manifest.ExternalCapabilityRev != newRev {
		t.Fatalf("adopted external capability rev = %q, want %q", adopted.Manifest.ExternalCapabilityRev, newRev)
	}
	if len(adopted.Drift) != 1 {
		t.Fatalf("ConfigurationAdopted drift = %+v, want one external change", adopted.Drift)
	}
	change := adopted.Drift[0]
	if change.Category != event.DriftExternal || change.Severity != event.DriftWarn || change.Old != oldRev || change.New != newRev {
		t.Fatalf("external drift = %+v, want category=%q severity=%q old=%q new=%q", change, event.DriftExternal, event.DriftWarn, oldRev, newRev)
	}
}
