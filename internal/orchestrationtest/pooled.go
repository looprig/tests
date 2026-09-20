//go:build integration

package orchestrationtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/factory"
	"github.com/looprig/factory/identity"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/runtimecommand"
	"github.com/looprig/harness/pkg/session"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/inference"
	"github.com/looprig/inference/model"
	"github.com/looprig/inference/stream"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// This file is the kit's CROSS-MODULE harness: a real released Factory placing
// sessions onto real released, composed Hosts whose runtimes are REAL harness
// rigs over REAL harness journals, with real browser-shaped ClientLink viewers
// watching the output.
//
// # Why it is not ComposedHost
//
// ComposedHost exists to drive Host's own surface -- attach, HostLink framing,
// the durable consumer -- and its runtime is a FakeRig that records what it was
// handed. That is the right fake for those cases and the wrong one for these:
// every claim this harness makes is about something only a real runtime
// produces. "A re-placed session is RESTORED, not restarted" is a claim about
// the harness journal; "the agent continues after its gate is answered" is a
// claim about a tool's return value reaching a model request; "live output
// reaches a viewer" is a claim about a committed publication stream. A fake
// runtime would make every one of them a statement about the fake.
//
// # What is real and what is not
//
// Real: factory.New served over TCP with its placement sweep and ClientLink
// running; host.Compose served over TCP advertising a BARE base; sessionstore
// over one shared memstore; one harness rig and one harness journal PER TENANT;
// centrifuge-go ClientLink viewers.
//
// Fake, and only because the module ships no implementation: the inference
// client (a scripted model), the credential verifier and authorizer, the
// HostLink service token, the Host's tenant verifier, its checkpointer and its
// workspaces.
//
// The backend is memstore rather than fsstore, and that is forced: harness
// v0.34.0's sessionstore REFUSES a Blobs provider without
// storage.BlobReaderLifecycle, which fsstore deliberately does not implement.
// See StoreFixture for the full note.

// PooledTenantA and PooledTenantB are the two tenants this harness places on
// ONE pooled Host at once. Two is the smallest number that can distinguish
// "isolated" from "there is only one of them".
const (
	PooledTenantA = sessionwire.TenantID("orchestrationtest-tenant-a")
	PooledTenantB = sessionwire.TenantID("orchestrationtest-tenant-b")

	// PooledServiceToken is the HostLink service credential Factory presents
	// and the Host's verifier accepts.
	PooledServiceToken = "orchestrationtest-pooled-service-token"

	// PooledBinding and PooledBindingVersion are the deployment's
	// SessionBinding. Factory stamps them into every create and Host resolves
	// its journal store by (tenant, StorageBindingID); they must agree or no
	// command could settle and no attach could read a journal.
	PooledBinding        = "orchestrationtest-pooled-binding"
	PooledBindingVersion = "v1"

	// PooledAgent and PooledCompatibility name the one launch target.
	PooledAgent         = sessionwire.AgentID("orchestrationtest-pooled-agent")
	PooledCompatibility = department.CompatibilityID("orchestrationtest-pooled-compat")

	// PooledOrigin is the browser origin the CSRF guard trusts.
	PooledOrigin = "https://app.orchestrationtest.invalid"

	// PooledPublicationsPerInput is how many EnduringPublications the product
	// runtime commits for each applied create or input. It is 3 rather than 1
	// so a case can tell "the stream continued" from "one more record arrived".
	PooledPublicationsPerInput = 3
)

// PooledBearers is the per-tenant actor credential. A viewer presenting
// tenant-b's token is tenant-b, which is what makes the cross-tenant refusal a
// refusal rather than a missing header.
var PooledBearers = map[sessionwire.TenantID]string{
	PooledTenantA: "orchestrationtest-bearer-tenant-a",
	PooledTenantB: "orchestrationtest-bearer-tenant-b",
}

// ---- the scripted model ----------------------------------------------------

// PooledTurn is one scripted model turn: either plain text, or a tool call.
type PooledTurn struct {
	Text string

	// ToolName and ToolInput, when ToolName is set, make the turn a tool call
	// instead. The turn AFTER a tool call is the model's reply to the result.
	ToolName  string
	ToolInput string
}

// PooledLLM is the harness's inference client: a queue of scripted turns, with
// every request it was handed recorded.
//
// It records requests because that is the only place a restored conversation is
// observable from outside harness: the proof that a re-placed session kept its
// history is that the NEXT turn's messages still contain the earlier one's word.
type PooledLLM struct {
	mu       sync.Mutex
	turns    []PooledTurn
	next     int
	requests []inference.Request
}

// NewPooledLLM returns a model that answers with plain text forever.
func NewPooledLLM() *PooledLLM { return &PooledLLM{} }

// Script appends turns to the queue. Once the queue is exhausted the model
// answers plain text, so a case scripts only the turns it cares about.
func (l *PooledLLM) Script(turns ...PooledTurn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.turns = append(l.turns, turns...)
}

// Invoke satisfies inference.Client. Nothing in this harness invokes.
func (*PooledLLM) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("orchestrationtest: PooledLLM.Invoke is unused")
}

// Stream satisfies inference.Client.
func (l *PooledLLM) Stream(_ context.Context, request inference.Request) (*stream.StreamReader[content.Chunk], error) {
	l.mu.Lock()
	l.requests = append(l.requests, request)
	var turn PooledTurn
	if l.next < len(l.turns) {
		turn = l.turns[l.next]
	}
	l.next++
	call := l.next
	l.mu.Unlock()

	chunks := []content.Chunk{}
	if turn.ToolName != "" {
		chunks = append(chunks, &content.ToolUseChunk{
			Index:     0,
			ID:        fmt.Sprintf("orchestrationtest-call-%d", call),
			Name:      turn.ToolName,
			InputJSON: turn.ToolInput,
		})
	} else {
		text := turn.Text
		if text == "" {
			text = "ok"
		}
		chunks = append(chunks, &content.TextChunk{Text: text})
	}
	sent := 0
	return stream.NewStreamReader(func() (content.Chunk, error) {
		if sent >= len(chunks) {
			return nil, io.EOF
		}
		chunk := chunks[sent]
		sent++
		return chunk, nil
	}, nil), nil
}

// Requests reports every model request, in order.
func (l *PooledLLM) Requests() []inference.Request {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]inference.Request(nil), l.requests...)
}

// UserBlocksContaining returns the BLOCKS of the first user message, in any
// model request at or after from, that carries needle.
//
// IT IS BLOCK-LEVEL ON PURPOSE. "The first message reached the model" is not a
// substring question: a product that concatenated three paragraphs into one
// text block, or dropped an image and kept the caption, satisfies every
// substring check while losing exactly what a multimodal first message is for.
// Returning the blocks lets a case assert their COUNT and their TYPES, which is
// the gap host's own gate recorded as F3 -- every create fixture in that repo
// carries one text block, so the "byte-identical to the input Factory would
// have admitted" claim was asserted over a single example.
func (l *PooledLLM) UserBlocksContaining(from int, needle string) []content.Block {
	for i, request := range l.Requests() {
		if i < from {
			continue
		}
		for _, message := range request.Messages {
			user, ok := message.(*content.UserMessage)
			if !ok {
				continue
			}
			encoded, err := json.Marshal(user.Blocks)
			if err != nil || !strings.Contains(string(encoded), needle) {
				continue
			}
			return append([]content.Block(nil), user.Blocks...)
		}
	}
	return nil
}

// SawInRequest reports whether any model request after index from carried
// needle anywhere in its messages.
func (l *PooledLLM) SawInRequest(from int, needle string) bool {
	for i, request := range l.Requests() {
		if i < from {
			continue
		}
		encoded, err := json.Marshal(request.Messages)
		if err != nil {
			continue
		}
		if strings.Contains(string(encoded), needle) {
			return true
		}
	}
	return false
}

// ---- the tool that raises an ask_user gate ---------------------------------

// PooledAskToolName is the tool the gate case's model calls.
const PooledAskToolName = "orchestrationtest_ask"

// PooledAskTool raises a real ask_user gate from inside a tool call and returns
// the answer as its tool result.
//
// # KNOWN HAZARD: A PARKED AGENT HAS NO BOUND OF ITS OWN HERE
//
// A tool sitting inside loop.RequestUserInput is not idle, so a Host drain's
// ReleaseResidency waits on harness's WaitIdle and blocks. Observed in a
// release gate against a host v0.3.0 control: the test binary sat at 0% CPU for
// TWENTY-THREE MINUTES, parked in
// lifecycle.(*Drainer).release -> pooledSession.ReleaseResidency ->
// Session.WaitIdle -> hub.WaitIdle, with the agent still in the tool. It does
// not reproduce at this pin -- the incapable-Host case deliberately leaves a
// gate unanswered and stops cleanly -- so it is control-specific. But THE ONLY
// BOUND THIS LANE HAS AGAINST IT IS `go test -timeout`, which reports a
// timeout rather than the wait that caused it.
//
// If a gated case ever hangs, look here first. The fix, when one is wanted, is
// a bound inside the fixture's own release path rather than a shorter global
// timeout.
//
// That return value is the whole point. An answered gate whose answer never
// reaches the agent is indistinguishable from an abandoned one at the store, so
// the case asserts the agent CONTINUED by looking for the answer text in the
// next model request -- and it can only get there through this result.
type PooledAskTool struct {
	Question string

	mu      sync.Mutex
	calls   int
	answers []string
	lastErr error
}

// Calls reports how many times the tool ran.
func (t *PooledAskTool) Calls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

// LastErr reports the last failure the gate request returned, if any. It exists
// so a case that never sees a gate can say whether the tool ran and was refused
// rather than never running at all.
func (t *PooledAskTool) LastErr() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastErr
}

// Info satisfies tool.InvokableTool.
func (t *PooledAskTool) Info(context.Context) (*tool.ToolInfo, error) {
	return &tool.ToolInfo{
		Name:   PooledAskToolName,
		Desc:   "Asks the user a question and returns their answer.",
		Schema: []byte(`{"type":"object","properties":{},"additionalProperties":false}`),
	}, nil
}

// PrepareCall satisfies tool.CallPreparer, and the tool must have one.
//
// A loop under an AccessGate refuses a tool that cannot prepare a call --
// "permission denied: tool has no call preparation", measured -- so the absence
// of this method is indistinguishable from a denial. The prepared request names
// NO requirement, which the evaluator approves trivially: this tool asks the
// user a question, it executes nothing, and inventing a capability requirement
// for it would be modelling a permission this lane is not about.
func (t *PooledAskTool) PrepareCall(_ context.Context, executionID uuid.UUID, _ string) (tool.Request, tool.PreparedArtifact, error) {
	return tool.Request{
		ToolName:           PooledAskToolName,
		Summary:            "ask the user a question",
		ExecutionID:        executionID.String(),
		ExpiresAtUnixMilli: time.Now().Add(time.Hour).UnixMilli(),
	}, nil, nil
}

// InvokableRun satisfies tool.InvokableTool.
func (t *PooledAskTool) InvokableRun(ctx context.Context, _ string) (*tool.ToolResult, error) {
	t.mu.Lock()
	t.calls++
	question := t.Question
	t.mu.Unlock()
	if question == "" {
		question = "orchestrationtest: what should I do?"
	}
	answer, err := loop.RequestUserInput(ctx, question, nil)
	if err != nil {
		t.mu.Lock()
		t.lastErr = err
		t.mu.Unlock()
		return nil, err
	}
	t.mu.Lock()
	t.answers = append(t.answers, answer)
	t.mu.Unlock()
	return tool.TextResult("the user answered: " + answer), nil
}

// Answers reports every answer the gate handed back to the agent, in order.
func (t *PooledAskTool) Answers() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.answers...)
}

var (
	_ tool.InvokableTool = (*PooledAskTool)(nil)
	_ tool.CallPreparer  = (*PooledAskTool)(nil)
)

// pooledAllowAll is the access gate the gated loop runs under.
//
// It is REQUIRED, not decorative: with no loop.AccessGate wired, the loop
// runner denies every prepared call fail-secure and the tool never runs at all
// -- measured here as an agent that made two model calls, finished its turn and
// never entered the tool, with no error anywhere. It is allow-all because this
// lane is about the GATE a tool raises, not about the permission model that
// decides whether the tool may run; the permission model has its own cases.
func pooledAllowAll(tb TB) loop.AccessGate {
	tb.Helper()
	kinds := []string{
		tool.CapabilityCommandExecute,
		"tool.invoke",
		"filesystem.read",
		"filesystem.write",
		"network",
		"context.load",
	}
	bindings := make([]gate.AccessBinding, 0, len(kinds))
	for _, kind := range kinds {
		bindings = append(bindings, gate.AccessBinding{Kind: kind, Source: pooledAllowSource{}})
	}
	evaluator, err := gate.NewHeadlessEvaluator(bindings, pooledNoRules{}, nil)
	if err != nil {
		tb.Fatalf("orchestrationtest: gate.NewHeadlessEvaluator: %v", err)
		return nil
	}
	return evaluator
}

type pooledAllowSource struct{}

func (pooledAllowSource) AccessVersion() uint16                   { return gate.CurrentAccessVersion }
func (pooledAllowSource) AccessFor(string, string) (uint8, error) { return gate.AccessAllow, nil }

type pooledNoRules struct{}

func (pooledNoRules) MatchesDeny(context.Context, tool.Requirement) (bool, error)  { return false, nil }
func (pooledNoRules) MatchesAllow(context.Context, tool.Requirement) (bool, error) { return false, nil }
func (pooledNoRules) WriteRules(context.Context, []tool.RuleCandidate) error       { return nil }

func pooledAskDefinition(shared *PooledAskTool) tool.Definition {
	return tool.NewDefinition(PooledAskToolName, 0, func(context.Context, tool.Bindings) ([]tool.InvokableTool, error) {
		return []tool.InvokableTool{shared}, nil
	})
}

// ---- the committed live tail -----------------------------------------------

type pooledTailKey struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
}

// PooledTails is the product runtime's committed publication stream, one
// sequence per session, shared across Hosts so a restored session CONTINUES its
// numbering rather than starting over.
//
// IT FANS OUT TO EVERY LIVE SUBSCRIBER, which is not an optimisation: Host
// calls SubscribeCommitted once per live link, and it re-subscribes after a
// re-bind, so a session has MORE THAN ONE subscriber over its life. A fixture
// that kept only the latest channel published into one nobody was reading, and
// the symptom was a viewer that received the session.reset and then silence --
// measured here, and the same defect host v0.4.0 fixed in its own live-link
// fixture (regate dd9b479). The released adapter fans out; so does this.
//
// A Host relays what its runtime's SubscribeCommitted yields. Harness's own
// event stream is not a sessionwire publication stream, so the product supplies
// one -- which is exactly what a real product does.
type PooledTails struct {
	mu        sync.Mutex
	committed map[pooledTailKey]uint64
	current   map[pooledTailKey][]chan sessionwire.EnduringPublication
	subs      []pooledTailKey
	dropped   int
	notes     []string
}

// bridgeNote records one bridge lifecycle observation.
func (t *PooledTails) bridgeNote(note string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.notes = append(t.notes, note)
}

// BridgeNotes reports the committed-event bridge's lifecycle, in order.
func (t *PooledTails) BridgeNotes() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.notes...)
}

// NewPooledTails returns an empty tail set.
func NewPooledTails() *PooledTails {
	return &PooledTails{
		committed: map[pooledTailKey]uint64{},
		current:   map[pooledTailKey][]chan sessionwire.EnduringPublication{},
	}
}

func (t *PooledTails) subscribe(key pooledTailKey) chan sessionwire.EnduringPublication {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch := make(chan sessionwire.EnduringPublication, 1024)
	t.current[key] = append(t.current[key], ch)
	t.subs = append(t.subs, key)
	return ch
}

// Subscribes reports every SubscribeCommitted the Hosts made, in order.
func (t *PooledTails) Subscribes() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := []string{}
	for _, k := range t.subs {
		out = append(out, string(k.tenant)+"/"+string(k.session))
	}
	return out
}

// Dropped reports publications emitted with no subscriber.
func (t *PooledTails) Dropped() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dropped
}

// Emit commits n publications for one session.
func (t *PooledTails) Emit(key pooledTailKey, n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for range n {
		t.committed[key]++
		seq := t.committed[key]
		publication := sessionwire.EnduringPublication{
			TenantID:       key.tenant,
			SessionID:      key.session,
			EventID:        sessionwire.EventID(fmt.Sprintf("event-%s-%d", key.session, seq)),
			JournalSeq:     seq,
			CoveredThrough: seq,
			Body:           json.RawMessage(fmt.Sprintf(`{"tenant":%q,"seq":%d}`, key.tenant, seq)),
		}
		subscribers := t.current[key]
		if len(subscribers) == 0 {
			t.dropped++
			continue
		}
		for _, ch := range subscribers {
			select {
			case ch <- publication:
			default:
				t.dropped++
			}
		}
	}
}

// Hint commits ONE publication for a session whose tail is driven by the
// harness session's own committed events.
//
// It exists for a property of host v0.4.0 that is not obvious and cost a day to
// find: HOST'S GATE PUBLISHER PASSES ONLY ON A HINT FROM THE RUNTIME'S
// COMMITTED PUBLICATION STREAM. internal/gates' run loop subscribes to that
// stream and folds the journal after every delivery; its only timer is a retry
// for a FAILED subscription. So a runtime that opens a gate and then commits no
// publication leaves the gate journaled, unprojected and invisible to Factory
// for as long as the session stays quiet.
//
// A real product never notices, because its committed stream IS its session's
// event stream and a gate opening is an event on it. This kit's stream is
// synthetic -- three publications per applied command -- so the gated world
// bridges the harness stream into hints explicitly. See pooledSession.
//
// In a BRIDGED world this is the ONLY thing that commits a publication:
// ApplyCommand emits nothing, so the product's stream is one publication per
// committed harness event, which is what a real product's stream is. In an
// UNBRIDGED world nothing calls this and the stream is three publications per
// applied command, which keeps a viewer's sequence numbers deterministic for
// the cases that assert on them.
func (t *PooledTails) Hint(tenant sessionwire.TenantID, s sessionwire.SessionID) {
	t.Emit(pooledTailKey{tenant, s}, 1)
}

// Tip reports the highest committed sequence for one session.
func (t *PooledTails) Tip(tenant sessionwire.TenantID, s sessionwire.SessionID) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.committed[pooledTailKey{tenant, s}]
}

// ---- the product's department.Rig over real harness rigs -------------------

// PooledLaunch records one runtime launch: the tenant and the identity it ran
// under.
type PooledLaunch struct {
	Tenant sessionwire.TenantID
	ID     uuid.UUID
}

// PooledDispatch records one command the Host's runtime accepted.
type PooledDispatch struct {
	CommandID sessionwire.CommandID
	AttemptID string
}

// pooledRecorder records what the product runtime was DISPATCHED. It is an
// observation for a case to read; it is not, and must never again become, the
// source a settlement is vouched from. See PooledEvidence.
type pooledRecorder struct {
	mu        sync.Mutex
	dispatch  []PooledDispatch
	commanded []department.RuntimeCommand
}

// Dispatched reports every command the product runtime accepted, in order.
func (r *pooledRecorder) Dispatched() []PooledDispatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]PooledDispatch(nil), r.dispatch...)
}

func (r *pooledRecorder) add(cmd department.RuntimeCommand) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dispatch = append(r.dispatch, PooledDispatch{CommandID: cmd.CommandID, AttemptID: cmd.AttemptID})
	r.commanded = append(r.commanded, cmd)
}

// PooledRig is the PRODUCT's department.Rig: one real harness *rig.Rig per
// tenant, launched under the identity the Host asks for.
//
// Honouring RigSessionID is host v0.3.0's obligation and the reason the restore
// case works at all: department refuses a launch whose ID() is not the one it
// requested, and Host decides create-against-restore from the journal it finds
// under that id.
type PooledRig struct {
	rigs     map[sessionwire.TenantID]*rig.Rig
	recorder *pooledRecorder
	tails    *PooledTails
	// bridge forwards the harness session's own committed public events into
	// the product's tail as hints. See PooledTails.Hint.
	bridge bool

	mu            sync.Mutex
	creates       []PooledLaunch
	restores      []PooledLaunch
	refuseCreates bool
}

// RefuseCreates makes this Host's runtime refuse every create AFTER Host has
// durably begun its dispatch attempt.
//
// IT REPRODUCES THE host v0.4.0 SHAPE EXACTLY, which is the only reason it
// exists. Under v0.4.0 the adapter's kind gate refused a create -- a kind
// runtimecommand.Kind did not name -- and it did so from inside ApplyCommand,
// which Host reaches only after BeginAttempt has authorized the dispatch. So
// the record is left `applying` WITH an attempt: no disposition frame, nothing
// for the store to settle from, a deadline sweep that skips attempt-bearing
// records, and every later command on that session blocked behind it.
//
// A refusal raised anywhere earlier would be a different and far less
// interesting failure, because an unattempted command is bounded by Factory's
// deadline sweep.
func (p *PooledRig) RefuseCreates(refuse bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refuseCreates = refuse
}

func (p *PooledRig) refusingCreates() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.refuseCreates
}

// NewSession satisfies department.Rig.
func (p *PooledRig) NewSession(ctx context.Context, req department.RigCreateRequest) (department.RigSession, error) {
	target := p.rigs[req.TenantID]
	if target == nil {
		return nil, fmt.Errorf("orchestrationtest: no rig for tenant %q", req.TenantID)
	}
	var options []rig.SessionOption
	if !req.RigSessionID.IsZero() {
		options = append(options, rig.WithSessionID(req.RigSessionID))
	}
	controller, err := target.NewSession(ctx, options...)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.creates = append(p.creates, PooledLaunch{req.TenantID, req.RigSessionID})
	p.mu.Unlock()
	return p.adapt(controller, pooledTailKey{req.TenantID, req.SessionID}), nil
}

// RestoreSession satisfies department.Rig.
func (p *PooledRig) RestoreSession(ctx context.Context, id uuid.UUID, req department.RigRestoreRequest) (department.RigSession, error) {
	target := p.rigs[req.TenantID]
	if target == nil {
		return nil, fmt.Errorf("orchestrationtest: no rig for tenant %q", req.TenantID)
	}
	controller, err := target.RestoreSession(ctx, id)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.restores = append(p.restores, PooledLaunch{req.TenantID, id})
	p.mu.Unlock()
	return p.adapt(controller, pooledTailKey{req.TenantID, req.SessionID}), nil
}

// adapt wraps one launched harness session as Host's department.RigSession,
// starting the committed-event bridge when this world needs one.
func (p *PooledRig) adapt(controller session.SessionController, key pooledTailKey) department.RigSession {
	adapted := &pooledSession{controller: controller, recorder: p.recorder, tails: p.tails, key: key, rig: p, bridged: p.bridge}
	if p.bridge {
		adapted.startBridge()
	}
	return adapted
}

// Creates and Restores report what this Host's rig launched, in order.
func (p *PooledRig) Creates() []PooledLaunch {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PooledLaunch(nil), p.creates...)
}

// Dispatched reports every command this Host's runtime accepted, in order. It
// is an observation, never evidence: see PooledEvidence.
func (p *PooledRig) Dispatched() []PooledDispatch { return p.recorder.Dispatched() }

// Restores reports every restore, in order.
func (p *PooledRig) Restores() []PooledLaunch {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PooledLaunch(nil), p.restores...)
}

// pooledSession adapts a harness session.SessionController to Host's
// department.RigSession, discovering each segregated capability by assertion --
// which is what department.NewRigTarget's adapter does to it in turn.
type pooledSession struct {
	controller session.SessionController
	recorder   *pooledRecorder
	tails      *PooledTails
	key        pooledTailKey
	rig        *PooledRig

	// bridged reports which of the two tail disciplines this session runs
	// under. They are exclusive, and mixing them would interleave two
	// sequences into one stream.
	bridged bool
}

// startBridge forwards every committed public event the harness session
// produces into the product's tail as a hint.
//
// It is what makes a GATE visible. Host's gate publisher folds the journal only
// when the runtime's committed stream delivers, so without this a gate opens,
// is journaled, and is never projected -- measured. A real product gets this
// for free because its committed stream is the session's own event stream; this
// kit's is synthetic, so the bridge is explicit.
//
// A session whose persistence cannot report committed bytes reports the
// capability as absent, and the bridge is simply not started: that is the
// two-result form's whole point, and guessing here would be worse than not
// bridging.
func (s *pooledSession) startBridge() {
	provider, ok := s.controller.(session.CommittedPublicEventProvider)
	if !ok {
		s.tails.bridgeNote("no CommittedPublicEventProvider")
		return
	}
	source, ok := provider.CommittedPublicEvents()
	if !ok {
		s.tails.bridgeNote("capability absent")
		return
	}
	// Enduring from EVERY loop, which is the filter host's own adapter uses.
	// The ZERO filter selects no loop at all: it delivered one event in a whole
	// gated session and the gate was never projected. A filter is DECLARED
	// INTEREST evaluated before the send, so an empty one is not "everything".
	subscription, err := source.SubscribeCommittedPublicEvents(event.EventFilter{
		Enduring: event.LoopScope{All: true},
	})
	if err != nil {
		s.tails.bridgeNote("subscribe failed: " + err.Error())
		return
	}
	s.tails.bridgeNote("subscribed")
	go func() {
		defer func() { _ = subscription.Close() }()
		for delivery := range subscription.Events() {
			s.tails.bridgeNote(fmt.Sprintf("delivery %T", delivery))
			s.tails.Hint(s.key.tenant, s.key.session)
		}
		s.tails.bridgeNote("bridge closed")
	}()
}

func (s *pooledSession) ID() uuid.UUID { return s.controller.SessionID() }

func (s *pooledSession) WaitIdle(ctx context.Context) error {
	return s.controller.(session.IdleWaiter).WaitIdle(ctx)
}

func (s *pooledSession) Done() <-chan struct{} { return s.controller.(session.Liveness).Done() }

func (s *pooledSession) ReleaseResidency(ctx context.Context) error {
	return s.controller.(session.Releaser).ReleaseResidency(ctx)
}

func (s *pooledSession) LeaseEpoch() (uint64, bool) {
	return s.controller.(session.LeaseEpochReporter).LeaseEpoch()
}

func (s *pooledSession) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	return s.tails.subscribe(s.key), nil
}

// ApplyCommand satisfies department.CommandApplier, through HARNESS'S OWN
// runtime-command seam, FOR ALL FIVE ADMITTED KINDS.
//
// # The two fakes that used to live here, and why neither may come back
//
// It first called Submit for anything carrying text and let a recorder-backed
// evidence reader vouch that the command had applied. That was LOOSER than the
// dependency and was measured doing exactly the damage that implies: a gate
// response arrived here, matched no text, was recorded as dispatched, and
// SETTLED APPLIED while the agent's gate stayed open and the user's answer
// never reached it. A green test said the whole chain worked.
//
// The vouching then survived for `create` and `restore` alone, because
// harness's runtimecommand.Kind named only three kinds and those two had no
// path to a disposition frame at all. harness v0.36.0 added KindCreate and
// KindRestore and host v0.5.0 decodes a create's body, so THE EXCEPTION IS
// GONE: every kind is applied through runtimecommand.Applier, which writes the
// durable prefix and the kind's disposition frame before the effect, and the
// embedded harness store is the ONLY evidence reader. A command harness refuses
// is a command that does not settle.
//
// # The decode is BY KIND and by Core's own records
//
// A create's stored body is a Core CreateRequest and an input's is an
// InputRequest -- Factory canonically encodes the whole request, so the two are
// different shapes carrying the same blocks. host v0.5.0's own adapter
// re-presents a create's blocks in the input-shaped body its BlockDecoder
// reads; this product reaches the same place by decoding each record with
// Core's decoder and handing the blocks to content.UnmarshalBlocks.
//
// It replaced a walk that scraped every "text" member out of any JSON. That
// walk was shape-blind in both directions: it accepted a body of the wrong kind
// without noticing, and it could carry nothing but text -- so a multi-block or
// multimodal first message arrived at the model as one concatenated string, or
// not at all. Host's own gate recorded that as F3.
//
// It REFUSES an unframed command for the reason FakeRuntime does: Host mints a
// runtime UUID for every command it applies, and a zero one means the case
// stopped exercising the correlation it claims to.
func (s *pooledSession) ApplyCommand(ctx context.Context, cmd department.RuntimeCommand) error {
	if cmd.RuntimeCommandID.IsZero() {
		return ErrUnframedCommand
	}
	applier, ok := s.controller.(runtimecommand.Applier)
	if !ok {
		return ErrRuntimeCannotApply
	}
	epoch, held := s.LeaseEpoch()
	if !held {
		return ErrRuntimeHoldsNoLease
	}
	admitted := runtimecommand.Admitted{
		CommandID:        runtimecommand.CommandID(cmd.CommandID),
		RuntimeCommandID: cmd.RuntimeCommandID,
		LeaseEpoch:       epoch,
		AttemptID:        runtimecommand.AttemptID(cmd.AttemptID),
	}
	switch cmd.Kind {
	case PooledKindCreate:
		if s.rig != nil && s.rig.refusingCreates() {
			return fmt.Errorf("%w: this runtime refuses %q, after the attempt is durable",
				ErrUnknownCommandKind, cmd.Kind)
		}
		blocks, err := PooledCreateBlocks(cmd.Payload)
		if err != nil {
			return err
		}
		// A BARE create -- one carrying no first message -- crosses with no
		// blocks and drives no turn. harness makes Blocks OPTIONAL for this
		// kind precisely so an idle create can still settle; refusing one here
		// would wedge every session created without an opening message.
		admitted.Kind, admitted.Blocks = runtimecommand.KindCreate, blocks
	case PooledKindRestore:
		// A restore carries NOTHING. Core's RestoreRequest has no blocks member
		// and Admitted.Validate refuses a restore that carries any, so a
		// product that forwarded a stray payload here would be refused after
		// the attempt was already durable.
		admitted.Kind = runtimecommand.KindRestore
	case PooledKindGateResponse:
		answer, err := pooledGateAnswer(cmd)
		if err != nil {
			return err
		}
		admitted.Kind = runtimecommand.KindGateResponse
		admitted.GateResponse = answer
	case PooledKindInterrupt:
		admitted.Kind = runtimecommand.KindInterrupt
	case PooledKindInput:
		blocks, err := PooledInputBlocks(cmd.Payload)
		if err != nil {
			return err
		}
		// Unlike a create, harness REQUIRES blocks for an input and refuses one
		// carrying none, so this arm cannot fail open the way the create arm
		// structurally can.
		admitted.Kind, admitted.Blocks = runtimecommand.KindInput, blocks
	default:
		// A kind a newer Factory admits and this product does not know. It is
		// REFUSED rather than guessed at: guessing is what silently dropped a
		// create's first message for a whole release, and a refusal before any
		// durable write leaves the record for a Host that understands it.
		return fmt.Errorf("%w: %q", ErrUnknownCommandKind, cmd.Kind)
	}
	if _, err := applier.ApplyRuntimeCommand(ctx, admitted); err != nil {
		return err
	}
	if !s.bridged && len(admitted.Blocks) > 0 {
		s.tails.Emit(s.key, PooledPublicationsPerInput)
	}
	s.recorder.add(cmd)
	return nil
}

// CloseAttempt satisfies department.AttemptCloser, which is the capability a
// SUCCESSOR needs to free a session a predecessor stranded.
//
// # Why a product runtime must offer it, and what happens if it does not
//
// A predecessor that dies (or, in host v0.4.0's case, refuses a kind it could
// not name) leaves a record `applying` with a durable attempt and no
// disposition frame. The store has nothing to settle from; the deadline sweep
// SKIPS an attempt-bearing record, so it never expires; and the consumer will
// not advance its cursor past a non-terminal record, so every later command on
// that session is blocked behind it. The only exit is a successor writing the
// recovery closure -- and Host asks the RUNTIME for it, because only the
// runtime's journal can say whether the attempt left an effect.
//
// A runtime that omits this method is refused `department.ErrNoAttemptCloser`
// and the session stays wedged FOREVER. That was measured here before this
// method existed: the successor took the session, the create stayed `applying`
// for the whole two-minute bound, and the input behind it never settled. It is
// a consumer obligation host v0.5.0's own §9 does not list.
//
// Nothing is decided here. The author grant is deliberately NOT a parameter --
// harness stamps it from the live lease the successor holds -- and harness
// refuses the closure outright if its grant is not strictly later than the
// attempt's, or if the journal holds ANY enduring event caused by that runtime
// command, because a tombstone over a committed effect is the one error this
// protocol cannot recover from.
func (s *pooledSession) CloseAttempt(
	ctx context.Context,
	command sessionwire.CommandID,
	runtimeCommand uuid.UUID,
	kind string,
	attempt string,
	attemptJournalEpoch uint64,
) error {
	closer, ok := s.controller.(runtimecommand.AttemptCloser)
	if !ok {
		return ErrRuntimeCannotClose
	}
	closure := runtimecommand.Closure{
		CommandID:           runtimecommand.CommandID(command),
		RuntimeCommandID:    runtimeCommand,
		Kind:                runtimecommand.Kind(kind),
		AttemptID:           runtimecommand.AttemptID(attempt),
		AttemptJournalEpoch: attemptJournalEpoch,
	}
	// VALIDATED ON THE RELEASED TYPE'S OWN RULE rather than a restatement of
	// it: a closure that cannot name an attempt is one that could tombstone the
	// wrong command.
	if err := closure.Validate(); err != nil {
		return err
	}
	_, err := closer.CloseAttempt(ctx, closure)
	return err
}

// The five admitted command kinds Factory files, restated because Factory
// exports none of them.
//
// THEY ARE FIVE, NOT THREE. Any fixture in this kit that enumerates the
// runtime-command vocabulary must name all five as of harness v0.36.0, and
// nothing here may use "restore" as an example of an unknown kind: harness
// accepts it now, and a row built on that would go green while saying nothing.
const (
	PooledKindCreate       = "create"
	PooledKindRestore      = "restore"
	PooledKindInput        = "input"
	PooledKindInterrupt    = "interrupt"
	PooledKindGateResponse = "gate_response"
)

// PooledKinds is the vocabulary as one value, so a case can iterate it rather
// than restate it.
func PooledKinds() []string {
	return []string{PooledKindCreate, PooledKindRestore, PooledKindInput, PooledKindInterrupt, PooledKindGateResponse}
}

// PooledUnknownKind is a kind string NEITHER vocabulary has ever held.
//
// It exists because the obvious choices keep being overtaken: "gate_response"
// was an unknown kind until harness v0.35.0 named it, and "restore" until
// v0.36.0 did. A fixture that used either went green the day the vocabulary
// widened, while whatever it was guarding was live.
const PooledUnknownKind = "orchestrationtest_no_such_kind"

// The three refusals this product runtime makes on its own behalf.
var (
	ErrRuntimeCannotApply  = errors.New("orchestrationtest: the harness session cannot apply an admitted runtime command")
	ErrRuntimeHoldsNoLease = errors.New("orchestrationtest: the harness session reports no held journal lease")
	ErrUnknownCommandKind  = errors.New("orchestrationtest: this product runtime does not apply this command kind")
	ErrRuntimeCannotClose  = errors.New("orchestrationtest: the harness session offers no recovery closure")
)

// PooledCreateBlocks reads a create's stored body -- Core's CreateRequest --
// and returns its first message as content blocks.
//
// An EMPTY payload is a bare create and yields no blocks and no error. Anything
// else Core refuses is an error, because a body this product cannot read is one
// it must not silently apply as an empty turn: that is the exact shape of the
// defect host v0.5.0 exists to close.
func PooledCreateBlocks(payload []byte) ([]content.Block, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var request sessionwire.CreateRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, fmt.Errorf("orchestrationtest: the create body is not a Core CreateRequest: %w", err)
	}
	return pooledBlocks(request.Blocks)
}

// PooledInputBlocks reads an input's stored body -- Core's InputRequest -- and
// returns its blocks.
func PooledInputBlocks(payload []byte) ([]content.Block, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("orchestrationtest: the input body is empty")
	}
	var request sessionwire.InputRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, fmt.Errorf("orchestrationtest: the input body is not a Core InputRequest: %w", err)
	}
	return pooledBlocks(request.Blocks)
}

// pooledBlocks decodes a Core request's blocks member with CORE'S OWN decoder.
//
// content.UnmarshalBlocks is what a composition's BlockDecoder is written
// around, and using it here is what makes a MULTI-BLOCK and a NON-TEXT first
// message cross faithfully. A decoder that read only text would drop an image
// and concatenate three paragraphs into one, and neither loss is visible at the
// store.
func pooledBlocks(raw json.RawMessage) ([]content.Block, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	blocks, err := content.UnmarshalBlocks(raw)
	if err != nil {
		return nil, fmt.Errorf("orchestrationtest: the body's blocks are not Core content blocks: %w", err)
	}
	return blocks, nil
}

// pooledGateAnswer reads Core's gate-response record out of an admitted
// command's body and builds harness's answer from it.
//
// The SOURCE is the user, and it is asserted here rather than taken from the
// body: Core's record carries no source, a Host sets it, and harness refuses a
// source a caller may not assert. Everything else about the answer -- whether
// the gate is open, whether the action is one of its controls, whether the
// values satisfy its schema -- is the session's to decide, and this does not
// pre-empt any of it.
func pooledGateAnswer(cmd department.RuntimeCommand) (*gate.GateResponse, error) {
	if len(cmd.Payload) == 0 {
		return nil, fmt.Errorf("orchestrationtest: the gate response %s carries no inline body", cmd.CommandID)
	}
	var request sessionwire.GateResponseRequest
	if err := json.Unmarshal(cmd.Payload, &request); err != nil {
		return nil, fmt.Errorf("orchestrationtest: the gate response %s is not a Core record: %w", cmd.CommandID, err)
	}
	gateID, err := uuid.Parse(string(request.GateID))
	if err != nil {
		return nil, fmt.Errorf("orchestrationtest: the gate response %s names %q, which is not a gate identity: %w",
			cmd.CommandID, request.GateID, err)
	}
	return &gate.GateResponse{
		GateID: gate.ID(gateID),
		Action: request.Action,
		Values: request.Values,
		Source: gate.ResponseSource{Kind: gate.ResponseFromUser},
	}, nil
}

// PooledEvidence is the settlement evidence reader AND Host's runtime-journal
// reader: one value doing both jobs, which is what host v0.3.0 made this seam.
//
// IT OVERRIDES NOTHING, FOR ANY KIND. The embedded *harness sessionstore.Store
// implements sessionstore.DispositionEvidenceReader, and its answer is the
// runtime's OWN durable disposition frame.
//
// # The standing rule, and it now has no exception
//
// DO NOT REINTRODUCE A RECORDER-BACKED EVIDENCE READER FOR ANY OF THE FIVE
// KINDS. It has hidden a broken chain twice. The first time, every kind was
// vouched for and a gate response settled `applied` while the gate stayed open
// and the user's answer never reached the agent. The second time only `create`
// and `restore` were vouched for -- they had no path to a harness disposition
// frame at all, because runtimecommand.Kind named three kinds -- and that
// exception hid the defect host v0.5.0 exists to close: a create crossing with
// no blocks, driving no turn, and settling `applied` with the user's first
// message dropped in silence.
//
// harness v0.36.0 names all five kinds and host v0.5.0 decodes a create's body,
// so there is nothing left to vouch for. A kind this product cannot apply is
// REFUSED at ApplyCommand, before any durable write, and the command does not
// settle -- which is the honest outcome and the one a reader can act on.
type PooledEvidence struct {
	*harnessstore.Store
}

// ---- the world -------------------------------------------------------------

// PooledWorld is the durable plane and the agent definition every Host and
// Factory in one case share.
type PooledWorld struct {
	Backend  *storage.Composite
	Store    *sessionstore.Store
	Journals map[sessionwire.TenantID]*harnessstore.Store
	LLM      *PooledLLM
	Tails    *PooledTails
	AskTool  *PooledAskTool

	tenants []sessionwire.TenantID
	gated   bool
}

// PooledWorldOptions chooses what a case's agent can do.
type PooledWorldOptions struct {
	// Tenants are the tenants this world serves. Empty means both.
	Tenants []sessionwire.TenantID
	// WithAskTool gives the agent the tool that raises a real ask_user gate.
	WithAskTool bool
}

// NewPooledWorld opens the shared durable plane and one harness journal per
// tenant.
//
// The harness journals sit on their OWN memstore backends, separate from the
// SessionStore composite. They are different keyspaces owned by different
// modules, and a real deployment does not co-locate them; more usefully, a case
// that reads a journal here cannot be reading a SessionStore record by accident.
func NewPooledWorld(tb TB, ctx context.Context, options PooledWorldOptions) *PooledWorld {
	tb.Helper()
	tenants := options.Tenants
	if len(tenants) == 0 {
		tenants = []sessionwire.TenantID{PooledTenantA, PooledTenantB}
	}
	world := &PooledWorld{
		Backend:  memstore.New(),
		Journals: map[sessionwire.TenantID]*harnessstore.Store{},
		LLM:      NewPooledLLM(),
		Tails:    NewPooledTails(),
		tenants:  tenants,
		gated:    options.WithAskTool,
	}
	if options.WithAskTool {
		world.AskTool = &PooledAskTool{}
	}
	for _, tenant := range tenants {
		journalStore, err := harnessstore.Open(memstore.New(), harnessstore.WithTenant(tenant))
		if err != nil {
			tb.Fatalf("orchestrationtest: opening the harness journal for %q: %v", tenant, err)
			return nil
		}
		world.Journals[tenant] = journalStore
	}
	store, err := sessionstore.Open(ctx, world.Backend)
	if err != nil {
		tb.Fatalf("orchestrationtest: opening sessionstore over the pooled backend: %v", err)
		return nil
	}
	world.Store = store
	tb.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.Close(closeCtx); err != nil {
			tb.Errorf("orchestrationtest: closing the pooled store: %v", err)
		}
	})
	return world
}

// PooledModel is the model every pooled loop runs on.
//
// It DECLARES TOOL SUPPORT, and that is load bearing rather than tidy: a model
// whose capabilities omit tools has its tool set stripped before the request,
// so a scripted tool call is never dispatched and the tool never runs. Measured
// -- the gate case's agent made two model calls, finished its turn and never
// entered the tool, with no error anywhere.
func PooledModel() model.Model {
	return model.CustomModel(
		model.ProviderName("orchestrationtest"),
		model.APIFormatOpenAI,
		"http://127.0.0.1/v1",
		"orchestrationtest-model",
		model.WithTools(),
	)
}

func (w *PooledWorld) defineRig(tb TB, tenant sessionwire.TenantID) *rig.Rig {
	tb.Helper()
	loopOptions := []loop.Option{
		loop.WithName("orchestrationtest-agent"),
		loop.WithInference(w.LLM, PooledModel()),
	}
	if w.gated {
		loopOptions = append(loopOptions,
			loop.WithTools(pooledAskDefinition(w.AskTool)),
			loop.WithAccessGate(pooledAllowAll(tb)),
			// An access gate obliges a policy revision: loop.Define refuses
			// missing_policy_revision otherwise.
			loop.WithPolicyRevision("orchestrationtest-v1"),
		)
	}
	definition, err := loop.Define(loopOptions...)
	if err != nil {
		tb.Fatalf("orchestrationtest: loop.Define: %v", err)
		return nil
	}
	defined, err := rig.Define(
		rig.WithLoops(definition),
		rig.WithPrimers("orchestrationtest-agent"),
		rig.WithSessionStore(w.Journals[tenant]),
	)
	if err != nil {
		tb.Fatalf("orchestrationtest: rig.Define for %q: %v", tenant, err)
		return nil
	}
	return defined
}

// ---- the Host --------------------------------------------------------------

// PooledHost is a running, composed Host serving its real Routes() on TCP and
// advertising a BARE base.
type PooledHost struct {
	Service *host.Service
	Rig     *PooledRig
	ID      sessionwire.HostID

	// Base is the advertised bare base. Nothing derived is ever advertised.
	Base sessionwire.InternalEndpoint

	tracker *pooledTrackingListener
	server  *httptest.Server
	stop    func()

	mu    sync.Mutex
	paths []string
}

// pooledTrackingListener records every accepted connection so a case can sever
// the HostLinks at TCP.
//
// httptest's CloseClientConnections FORGETS a connection once it is hijacked,
// and every WebSocket is, so it cannot sever a HostLink. This can.
type pooledTrackingListener struct {
	net.Listener

	mu    sync.Mutex
	conns []net.Conn
}

func (l *pooledTrackingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns = append(l.conns, conn)
		l.mu.Unlock()
	}
	return conn, err
}

// Sever closes every connection accepted so far and reports how many.
func (l *pooledTrackingListener) Sever() int {
	l.mu.Lock()
	conns := l.conns
	l.conns = nil
	l.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	return len(conns)
}

// StartPooledHost composes, starts and serves one POOLED Host over the world's
// shared backend, with one real harness rig per tenant.
func StartPooledHost(tb TB, ctx context.Context, world *PooledWorld, id sessionwire.HostID, generation uint64) *PooledHost {
	tb.Helper()
	return startHost(tb, ctx, world, id, generation, "")
}

// StartDedicatedHost composes, starts and serves one DEDICATED Host pinned to
// one session for its lifetime.
//
// The pinning is what a controller-placed Host is, and it is what the drain
// scope names: Core's drain request carries a (tenant, session) scope that
// "identifies the fixed session of a dedicated Host", and a POOLED Host that
// does not hold the session refuses it `runtime_unavailable` -- measured, and
// the reason this constructor exists rather than a placement field on the
// pooled one.
func StartDedicatedHost(tb TB, ctx context.Context, world *PooledWorld, id sessionwire.HostID, generation uint64, fixed sessionwire.SessionID) *PooledHost {
	tb.Helper()
	return startHost(tb, ctx, world, id, generation, fixed)
}

func startHost(tb TB, ctx context.Context, world *PooledWorld, id sessionwire.HostID, generation uint64, fixed sessionwire.SessionID) *PooledHost {
	tb.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("orchestrationtest: opening the pooled host listener: %v", err)
		return nil
	}
	listener := &pooledTrackingListener{Listener: raw}
	base := sessionwire.InternalEndpoint("ws://" + listener.Addr().String())

	// A dedicated Host is pinned to one session and must have capacity
	// exactly one; a pooled one REFUSES a fixed session id. Host validates
	// both, so the two arms travel different composition branches.
	placement, capacity := sessionwire.HostPlacementPooled, uint64(8)
	if fixed != "" {
		placement, capacity = sessionwire.HostPlacementDedicated, 1
	}
	recorder := &pooledRecorder{}
	rigs := map[sessionwire.TenantID]*rig.Rig{}
	journals := map[host.EvidenceKey]sessionstore.DispositionEvidenceReader{}
	for _, tenant := range world.tenants {
		rigs[tenant] = world.defineRig(tb, tenant)
		journals[host.EvidenceKey{TenantID: tenant, StorageBindingID: PooledBinding}] =
			PooledEvidence{Store: world.Journals[tenant]}
	}
	product := &PooledRig{rigs: rigs, recorder: recorder, tails: world.Tails, bridge: world.gated}

	blueprint := host.Composition{
		Options: host.Options{
			HostID:           id,
			InternalEndpoint: base,
			// CROSS-TENANT ISOLATED, not tenant-exclusive: this is the
			// advertisement that lets Factory place two tenants on one Host,
			// which is the whole of the pooled multi-tenancy case.
			IsolationClass:    sessionwire.HostIsolationClassCrossTenantIsolated,
			Placement:         placement,
			Capacity:          capacity,
			FixedSessionID:    fixed,
			WarmTTL:           90 * time.Second,
			RegistryHeartbeat: 2 * time.Second,
			RegistryExpiry:    10 * time.Second,
			ClaimTTL:          5 * time.Second,
			ApplyDeadline:     60 * time.Second,
			CommandQueueSize:  16,
			ReconcileInterval: 200 * time.Millisecond,
			ReconcileBatch:    32,
		},
		Generation:           generation,
		Link:                 host.LinkOptions{MaxBindingsPerLink: 16, MaxBindings: 32, MaxTenantLinks: 4},
		Drain:                host.DrainOptions{Grace: 10 * time.Second, IdleBoundary: 5 * time.Second, PublishBound: 2 * time.Second},
		CompatibilityTimeout: 20 * time.Second,
		WorkPoll:             time.Second,
		Collaborators: host.Collaborators{
			Backend:       world.Backend,
			JournalStores: journals,
			Registrar: host.RegistrarFunc(func(context.Context) ([]department.Registration, error) {
				target, err := department.NewRigTarget(product, PooledCompatibility, KitCapabilities())
				if err != nil {
					return nil, err
				}
				return []department.Registration{{AgentID: PooledAgent, Target: target}}, nil
			}),
			Checkpointer: inertCheckpointer{},
			Auth:         pooledAuth{},
			Workspaces:   NewTempWorkspaces(tb),
			NamespaceLayout: func(tenant sessionwire.TenantID, s sessionwire.SessionID) string {
				return string(tenant) + "/" + string(s)
			},
		},
	}
	service, err := host.Compose(ctx, blueprint)
	if err != nil {
		_ = listener.Close()
		tb.Fatalf("orchestrationtest: composing pooled host %q: %v", id, err)
		return nil
	}
	if err := service.Start(ctx); err != nil {
		_ = listener.Close()
		tb.Fatalf("orchestrationtest: starting pooled host %q: %v", id, err)
		return nil
	}

	pooled := &PooledHost{Service: service, Rig: product, ID: id, Base: base, tracker: listener}
	routes := service.Routes()
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Upgrade") != "" {
			pooled.mu.Lock()
			// THE ESCAPED PATH, which is what was on the wire. url.URL.Path is
			// the DECODED form, so a tenant needing escaping would compare
			// equal to a concatenating derivation that never escaped anything
			// -- the exact HM5 mutant this lane is supposed to kill. Host's own
			// router reads the decoded Path; what is recorded here is the
			// request, not Host's reading of it.
			pooled.paths = append(pooled.paths, request.URL.EscapedPath())
			pooled.mu.Unlock()
		}
		routes.ServeHTTP(writer, request)
	})
	server := &httptest.Server{Listener: listener, Config: &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}}
	server.Start()
	pooled.server = server

	var once sync.Once
	pooled.stop = func() {
		once.Do(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := service.Stop(stopCtx); err != nil {
				tb.Logf("orchestrationtest: pooled host %q Stop: %v", id, err)
			}
			server.CloseClientConnections()
			server.Close()
		})
	}
	tb.Cleanup(pooled.stop)
	return pooled
}

// Stop drains this Host and closes its listener. It is idempotent.
func (h *PooledHost) Stop() { h.stop() }

// Attach makes a session resident through Host's exported in-process attach --
// the same entry point the hostlink.attach RPC reaches.
func (h *PooledHost) Attach(tb TB, ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID) host.Residency {
	tb.Helper()
	residency, err := h.Service.Attach(ctx, host.AttachRequest{
		TenantID:               tenant,
		SessionID:              s,
		AgentID:                PooledAgent,
		Mode:                   sessionwire.HostLinkAttachModeCreate,
		RuntimeCompatibilityID: string(PooledCompatibility),
		ActorID:                "orchestrationtest-attacher",
	})
	if err != nil {
		tb.Fatalf("orchestrationtest: attaching %s/%s on %s: %v", tenant, s, h.ID, err)
		return host.Residency{}
	}
	return residency
}

// PooledDispositionBinding is the immutable binding a Host-held session carries.
//
// The runtime session id is a FRESH UUID per call: host v0.3.0 reads the
// runtime identity out of the binding and refuses a non-UUID or the zero UUID
// before any launch, and two sessions sharing one id would share a
// conversation. The protocol mode is DISPOSITION because that is the only
// family a Host can take residency on.
func PooledDispositionBinding(tb TB) sessionstore.SessionBinding {
	tb.Helper()
	id, err := uuid.New()
	if err != nil {
		tb.Fatalf("orchestrationtest: minting a runtime session id: %v", err)
		return sessionstore.SessionBinding{}
	}
	return sessionstore.SessionBinding{
		StorageBindingID: PooledBinding,
		BindingVersion:   PooledBindingVersion,
		RuntimeSessionID: id.String(),
		ProtocolMode:     sessionstore.ProtocolModeDisposition,
	}
}

// Sever closes every TCP connection the Host has accepted and reports how many.
func (h *PooledHost) Sever() int { return h.tracker.Sever() }

// UpgradePaths reports every WebSocket upgrade path the Host saw, in order.
func (h *PooledHost) UpgradePaths() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.paths...)
}

// CountPath reports how many upgrades arrived at exactly path.
func (h *PooledHost) CountPath(path string) int {
	count := 0
	for _, seen := range h.UpgradePaths() {
		if seen == path {
			count++
		}
	}
	return count
}

// TenantPath is the HostLink path a dial for tenant must arrive at.
func TenantPath(tenant sessionwire.TenantID) string {
	return host.HostLinkPathPrefix + string(tenant)
}

type pooledAuth struct{}

func (pooledAuth) VerifyTenant(_ context.Context, _ sessionwire.TenantID, credential string) error {
	if credential != PooledServiceToken {
		return ErrWrongCredential
	}
	return nil
}

// ---- the Factory -----------------------------------------------------------

// PooledFactory is a real factory.Server, served over TCP, with the pending
// placement sweep and the ClientLink running.
type PooledFactory struct {
	Server    *factory.Server
	BaseURL   string
	Directory factory.Directory

	client *http.Client
}

type pooledVerifier struct{}

func (pooledVerifier) VerifyCredential(_ context.Context, credential identity.Credential) (identity.Claims, error) {
	for tenant, bearer := range PooledBearers {
		if credential.Value() == bearer {
			return identity.Claims{
				Tenant:    tenant,
				Subject:   "user-" + string(tenant),
				Kind:      identity.KindActor,
				ExpiresAt: time.Now().Add(time.Hour),
			}, nil
		}
	}
	return identity.Claims{}, identity.ErrUnauthenticated
}

// pooledAuthorizer permits everything, INCLUDING a cross-tenant subscribe.
//
// AuthorizeSubscribe being permissive is deliberate and is what gives the
// cross-tenant refusal its meaning: if this seam refused, the case would be
// asserting that the KIT refuses, which says nothing about Factory.
type pooledAuthorizer struct{}

func (pooledAuthorizer) AuthorizeSessionList(context.Context, identity.Principal) error { return nil }

func (pooledAuthorizer) AuthorizeSessionRead(context.Context, identity.Principal, sessionwire.SessionID) error {
	return nil
}

func (pooledAuthorizer) AuthorizeObjectRead(context.Context, identity.Principal, sessionwire.SessionID, sessionwire.ObjectReference) error {
	return nil
}

func (pooledAuthorizer) AuthorizeControl(context.Context, identity.Principal, sessionwire.SessionID, sessionstore.CommandKind) error {
	return nil
}

func (pooledAuthorizer) AuthorizeSubscribe(context.Context, identity.Principal, string) error {
	return nil
}

func (pooledAuthorizer) AuthorizeServiceSweep(context.Context, identity.Principal) error { return nil }

type pooledCredential struct{}

func (pooledCredential) ServiceToken(context.Context) (string, error) { return PooledServiceToken, nil }

// pooledTipReader is the Store, except that a session's journal tip is the
// product runtime's committed sequence.
//
// It exists because the harness journal is not the SessionStore journal: a
// session.reset Factory builds must name a tip at or above what it already
// delivered, and SessionStore holds no record of the product's own stream. It
// is an adapter over the real Store, not a replacement for it.
type pooledTipReader struct {
	*sessionstore.Store
	tails *PooledTails
}

func (r pooledTipReader) ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	page, err := r.Store.ReadPublicJournal(ctx, req)
	if err != nil {
		return page, err
	}
	if tip := r.tails.Tip(req.TenantID, req.SessionID); tip > page.CapturedTip {
		page.CapturedTip, page.CoveredThrough, page.Events, page.NextCursor = tip, tip, nil, ""
	}
	return page, nil
}

// StartPooledFactory composes, starts and serves a real Factory over the
// world's store, with placement (WithPendingCommands) enabled.
func StartPooledFactory(tb TB, ctx context.Context, world *PooledWorld, replica string, logs io.Writer) *PooledFactory {
	tb.Helper()
	directory, err := factory.NewStoreDirectory(world.Store, factory.DefaultDirectoryLimits())
	if err != nil {
		tb.Fatalf("orchestrationtest: factory.NewStoreDirectory: %v", err)
		return nil
	}
	service, err := identity.NewPrincipal(world.tenants[0], "orchestrationtest-sweeper", identity.KindService)
	if err != nil {
		tb.Fatalf("orchestrationtest: minting the sweeper identity: %v", err)
		return nil
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("orchestrationtest: opening the pooled factory listener: %v", err)
		return nil
	}
	base := "http://" + listener.Addr().String()

	reconcile := factory.DefaultReconcileLimits()
	reconcile.Interval = 200 * time.Millisecond
	reconcile.ClaimTTL = 2 * time.Second
	clientLink := factory.DefaultClientLinkLimits()
	clientLink.DemandReleaseDebounce = 200 * time.Millisecond

	if logs == nil {
		logs = io.Discard
	}
	server, err := factory.New(
		factory.WithCredentialVerifier(pooledVerifier{}),
		factory.WithAuthorizer(pooledAuthorizer{}),
		factory.WithSessionReader(pooledTipReader{Store: world.Store, tails: world.Tails}),
		factory.WithCommands(world.Store),
		factory.WithDirectory(directory),
		factory.WithCatalog(world.Store),
		factory.WithGates(world.Store),
		factory.WithHostTargets(world.Store),
		factory.WithHostLinkCredential(pooledCredential{}),
		factory.WithServiceIdentity(service),
		factory.WithReplicaID(replica),
		factory.WithCSRF(identity.CSRFConfig{
			SharedKey:      make([]byte, identity.MinCSRFSharedKeyBytes),
			TokenTTL:       time.Hour,
			TrustedOrigins: []string{PooledOrigin, base},
		}),
		factory.WithReconcileLimits(reconcile),
		factory.WithClientLinkLimits(clientLink),
		factory.WithDepartment(factory.LaunchTemplate{Key: sessionstore.HostTargetKey{
			AgentID:                PooledAgent,
			RuntimeCompatibilityID: string(PooledCompatibility),
			Placement:              sessionwire.HostPlacementPooled,
		}}),
		factory.WithSessionBinding(PooledBinding, PooledBindingVersion),
		factory.WithPublicCreates(world.Store),
		factory.WithObjectStoreResolver(func(context.Context, sessionstore.SessionBinding) (factory.ObjectReader, error) {
			return nil, ErrObjectNotPermitted
		}),
		// WITHOUT THIS NOTHING IS EVER PLACED. WithPendingCommands is what
		// triggers pooled placement: a deployment that omits it logs a WARN at
		// Start and every session waits forever with no other symptom.
		factory.WithPendingCommands(world.Store),
		factory.WithLogger(slog.New(slog.NewJSONHandler(logs, nil))),
	)
	if err != nil {
		_ = listener.Close()
		tb.Fatalf("orchestrationtest: composing the pooled factory: %v", err)
		return nil
	}
	if err := server.Start(ctx); err != nil {
		_ = listener.Close()
		tb.Fatalf("orchestrationtest: starting the pooled factory: %v", err)
		return nil
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	tb.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = server.Stop(stopCtx)
		<-served
	})
	return &PooledFactory{
		Server:    server,
		BaseURL:   base,
		Directory: directory,
		client:    &http.Client{Timeout: 15 * time.Second},
	}
}

// Post sends one authenticated JSON request as tenant and returns the status
// and body.
func (f *PooledFactory) Post(tb TB, ctx context.Context, tenant sessionwire.TenantID, path string, body any) (int, string) {
	tb.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		tb.Fatalf("orchestrationtest: encoding a %s body: %v", path, err)
		return 0, ""
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, f.BaseURL+path, strings.NewReader(string(encoded)))
	if err != nil {
		tb.Fatalf("orchestrationtest: building POST %s: %v", path, err)
		return 0, ""
	}
	request.Header.Set("Authorization", "Bearer "+PooledBearers[tenant])
	request.Header.Set("Content-Type", "application/json")
	response, err := f.client.Do(request)
	if err != nil {
		tb.Fatalf("orchestrationtest: POST %s as %s: %v", path, tenant, err)
		return 0, ""
	}
	defer response.Body.Close()
	answer, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(answer)
}

// Get sends one authenticated read as tenant and returns the status and body.
func (f *PooledFactory) Get(tb TB, ctx context.Context, tenant sessionwire.TenantID, path string) (int, []byte) {
	tb.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.BaseURL+path, nil)
	if err != nil {
		tb.Fatalf("orchestrationtest: building GET %s: %v", path, err)
		return 0, nil
	}
	request.Header.Set("Authorization", "Bearer "+PooledBearers[tenant])
	response, err := f.client.Do(request)
	if err != nil {
		tb.Fatalf("orchestrationtest: GET %s as %s: %v", path, tenant, err)
		return 0, nil
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response.StatusCode, body
}

// OpenGates reads one session's open gate projection THROUGH FACTORY's own
// routed read, which is the path a browser takes and the one that runs
// sessionstore's ReadGates behind it.
//
// Reading the store directly would prove the Host published a gate and say
// nothing about whether Factory can serve it -- and serving it is the half that
// the sessionstore v0.12.0 ROLLOUT RULE is about: a Factory below v0.12.0
// refuses these pages outright.
func (f *PooledFactory) OpenGates(tb TB, ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID) sessionwire.GatePage {
	tb.Helper()
	status, body := f.Get(tb, ctx, tenant, "/v1/sessions/"+string(s)+"/gates")
	if status != http.StatusOK {
		tb.Fatalf("orchestrationtest: Factory answered the gates read for %s %d: %s", s, status, body)
		return sessionwire.GatePage{}
	}
	var page sessionwire.GatePage
	if err := page.UnmarshalJSON(body); err != nil {
		tb.Fatalf("orchestrationtest: the gates page is not a Core GatePage (%s): %v", body, err)
		return sessionwire.GatePage{}
	}
	return page
}

// PooledEnvelope is a wire command envelope at the current version.
func PooledEnvelope(command string) sessionwire.CommandEnvelope {
	return sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: sessionwire.CommandID(command)}
}

// CommandState reports one disposition command's durable state, empty if it
// cannot be read.
func (w *PooledWorld) CommandState(ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID, command sessionwire.CommandID) sessionstore.InboxState {
	entry, err := w.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID: tenant, SessionID: s, CommandID: command,
	})
	if err != nil {
		return ""
	}
	return entry.Record.State
}

// RuntimeSessionID reads the runtime identity out of a session's durable
// binding, which is where host v0.3.0 takes it from.
func (w *PooledWorld) RuntimeSessionID(tb TB, ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID) uuid.UUID {
	tb.Helper()
	entry, err := w.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: s})
	if err != nil {
		tb.Fatalf("orchestrationtest: reading the catalog entry for %s/%s: %v", tenant, s, err)
		return uuid.UUID{}
	}
	id, err := uuid.Parse(entry.Record.Binding.RuntimeSessionID)
	if err != nil {
		tb.Fatalf("orchestrationtest: the binding's runtime session id %q is not a UUID: %v",
			entry.Record.Binding.RuntimeSessionID, err)
		return uuid.UUID{}
	}
	return id
}

// CountJournalEvents counts events of one type in a tenant's harness journal
// for one runtime session.
func CountJournalEvents[E event.Event](tb TB, world *PooledWorld, tenant sessionwire.TenantID, id uuid.UUID) int {
	tb.Helper()
	replayer, err := world.Journals[tenant].OpenInternalEventReplayer(id, harnessstore.ReplayRequest{FromSeq: 0})
	if err != nil {
		tb.Fatalf("orchestrationtest: opening a replayer for %s: %v", id, err)
		return 0
	}
	cursor, err := replayer.Open(context.Background(), journal.ReplayRequest{From: journal.Beginning()})
	if err != nil {
		tb.Fatalf("orchestrationtest: opening a replay cursor for %s: %v", id, err)
		return 0
	}
	defer cursor.Close()
	count := 0
	for {
		next, _, err := cursor.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return count
		}
		if err != nil {
			tb.Fatalf("orchestrationtest: replaying %s: %v", id, err)
			return count
		}
		if _, ok := next.(E); ok {
			count++
		}
	}
}

// JournalEvents returns every event of one type in a tenant's journal.
func JournalEvents[E event.Event](tb TB, world *PooledWorld, tenant sessionwire.TenantID, id uuid.UUID) []E {
	tb.Helper()
	replayer, err := world.Journals[tenant].OpenInternalEventReplayer(id, harnessstore.ReplayRequest{FromSeq: 0})
	if err != nil {
		tb.Fatalf("orchestrationtest: opening a replayer for %s: %v", id, err)
		return nil
	}
	cursor, err := replayer.Open(context.Background(), journal.ReplayRequest{From: journal.Beginning()})
	if err != nil {
		tb.Fatalf("orchestrationtest: opening a replay cursor for %s: %v", id, err)
		return nil
	}
	defer cursor.Close()
	var found []E
	for {
		next, _, err := cursor.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return found
		}
		if err != nil {
			tb.Fatalf("orchestrationtest: replaying %s: %v", id, err)
			return found
		}
		if typed, ok := next.(E); ok {
			found = append(found, typed)
		}
	}
}

// ---- viewers ---------------------------------------------------------------

// PooledViewer is one browser-shaped ClientLink connection that RECORDS what it
// receives and can be refused without failing the case.
//
// It is separate from ClientLinkViewer, which fails the case on a refusal: a
// case whose whole claim is "the cross-tenant subscribe was refused" needs the
// refusal as a value.
type PooledViewer struct {
	Tenant sessionwire.TenantID

	client *centrifugego.Client

	mu      sync.Mutex
	records []string
	strays  []string
}

// ConnectPooledViewer opens a ClientLink connection as tenant.
func ConnectPooledViewer(tb TB, ctx context.Context, f *PooledFactory, tenant sessionwire.TenantID) *PooledViewer {
	tb.Helper()
	endpoint := "ws" + strings.TrimPrefix(f.BaseURL, "http") + "/v1/realtime"
	client := centrifugego.NewJsonClient(endpoint, centrifugego.Config{
		Token: PooledBearers[tenant],
		Data:  []byte(`{"protocol_version":"1"}`),
		Header: http.Header{
			"Authorization": {"Bearer " + PooledBearers[tenant]},
			"Origin":        {f.BaseURL},
		},
		Name:             "orchestrationtest-pooled-viewer",
		HandshakeTimeout: 10 * time.Second,
		LogLevel:         centrifugego.LogLevelNone,
	})
	connected := make(chan struct{}, 1)
	client.OnConnected(func(centrifugego.ConnectedEvent) {
		select {
		case connected <- struct{}{}:
		default:
		}
	})
	if err := client.Connect(); err != nil {
		client.Close()
		tb.Fatalf("orchestrationtest: the pooled viewer for %q could not connect: %v", tenant, err)
		return nil
	}
	tb.Cleanup(client.Close)
	select {
	case <-connected:
	case <-ctx.Done():
		tb.Fatalf("orchestrationtest: the pooled viewer for %q did not connect: %v", tenant, ctx.Err())
		return nil
	}
	return &PooledViewer{Tenant: tenant, client: client}
}

// Watch subscribes to one session channel and RETURNS the refusal rather than
// failing on it. A record naming any other tenant or session than the channel's
// is recorded as a stray.
func (v *PooledViewer) Watch(tb TB, ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID) error {
	tb.Helper()
	sub, err := v.client.NewSubscription(ClientLinkChannel(tenant, s))
	if err != nil {
		tb.Fatalf("orchestrationtest: building a subscription: %v", err)
		return nil
	}
	answered := make(chan error, 1)
	sub.OnSubscribed(func(centrifugego.SubscribedEvent) {
		select {
		case answered <- nil:
		default:
		}
	})
	sub.OnError(func(e centrifugego.SubscriptionErrorEvent) {
		select {
		case answered <- e.Error:
		default:
		}
	})
	sub.OnPublication(func(e centrifugego.PublicationEvent) {
		summary, recordTenant, recordSession := pooledSummarise(e.Data)
		v.mu.Lock()
		defer v.mu.Unlock()
		v.records = append(v.records, summary)
		if recordTenant != tenant || recordSession != s {
			v.strays = append(v.strays, fmt.Sprintf("%s(%s/%s)", summary, recordTenant, recordSession))
		}
	})
	if err := sub.Subscribe(); err != nil {
		tb.Fatalf("orchestrationtest: subscribing: %v", err)
		return nil
	}
	select {
	case err := <-answered:
		return err
	case <-ctx.Done():
		tb.Fatalf("orchestrationtest: the subscribe was never answered: %v", ctx.Err())
		return nil
	}
}

// Close disconnects this viewer, as a browser tab closing does. It is
// idempotent; the fixture's cleanup closes it again.
func (v *PooledViewer) Close() { v.client.Close() }

// Records reports every publication this viewer received, summarised.
func (v *PooledViewer) Records() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.records...)
}

// Strays reports every record naming another tenant or session.
func (v *PooledViewer) Strays() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.strays...)
}

// Has reports whether one summarised record arrived.
func (v *PooledViewer) Has(record string) bool {
	for _, seen := range v.Records() {
		if seen == record {
			return true
		}
	}
	return false
}

// pooledSummarise renders one ClientLink record as E<seq>, R<last>/<tip> or
// T<tip>, with the tenant and session it names.
func pooledSummarise(data []byte) (string, sessionwire.TenantID, sessionwire.SessionID) {
	recordType, err := sessionwire.SessionRecordTypeOf(data)
	if err != nil {
		return "?" + string(data), "", ""
	}
	switch recordType {
	case sessionwire.SessionRecordTypeEnduringPublication:
		var publication sessionwire.EnduringPublication
		if err := publication.UnmarshalJSON(data); err != nil {
			return "?" + string(data), "", ""
		}
		return fmt.Sprintf("E%d", publication.JournalSeq), publication.TenantID, publication.SessionID
	case sessionwire.SessionRecordTypeSessionReset:
		var reset sessionwire.SessionReset
		if err := reset.UnmarshalJSON(data); err != nil {
			return "?" + string(data), "", ""
		}
		return fmt.Sprintf("R%d/%d", reset.LastContiguous, reset.JournalTip), reset.TenantID, reset.SessionID
	case sessionwire.SessionRecordTypeJournalTip:
		var tip sessionwire.JournalTip
		if err := tip.UnmarshalJSON(data); err != nil {
			return "?" + string(data), "", ""
		}
		return fmt.Sprintf("T%d", tip.Tip), tip.TenantID, tip.SessionID
	}
	return string(recordType), "", ""
}

// PooledCoveredThrough walks a viewer's stream the way a CLIENT must and
// returns the highest sequence it is covered through.
//
// This is the assertion I1.1 case 3 actually needs: "all three exactly once and
// in order" is not a count, it is the absence of a SILENT GAP. An enduring
// record must be the next sequence or a duplicate the client already holds;
// anything else is a gap. A session.reset R<last>/<tip> tells the client to read
// the journal through tip, so it covers through tip -- a reset is how a client
// is told about a gap, and is therefore not one. A journal_tip hint changes
// nothing.
func PooledCoveredThrough(records []string) (uint64, error) {
	return PooledCoveredThroughFrom(records, 0)
}

// PooledCoveredThroughFrom is PooledCoveredThrough for a client that already
// holds everything through start.
//
// A RECONNECTING BROWSER IS EXACTLY THAT CLIENT. It learns its position from a
// journal read, not from the socket -- a subscribe to a quiet session delivers
// nothing at all -- so its live tail legitimately begins at start+1 and a rule
// anchored at zero would read the first record as a silent gap. Measured: a
// browser that came back at tip 6 received [E7 E8 E9], exactly once and in
// order, with no reset, and the zero-anchored rule called it a gap.
func PooledCoveredThroughFrom(records []string, start uint64) (uint64, error) {
	position := start
	for _, record := range records {
		var first, second uint64
		switch {
		case strings.HasPrefix(record, "E"):
			if _, err := fmt.Sscanf(record, "E%d", &first); err != nil {
				return position, err
			}
			if first <= position {
				continue
			}
			if first != position+1 {
				return position, fmt.Errorf("silent gap: %s after %d", record, position)
			}
			position = first
		case strings.HasPrefix(record, "R"):
			if _, err := fmt.Sscanf(record, "R%d/%d", &first, &second); err != nil {
				return position, err
			}
			if second > position {
				position = second
			}
		}
	}
	return position, nil
}

// ---- waits -----------------------------------------------------------------

// PooledWait polls until ok, and FAILS naming what did not happen rather than
// hanging. Every wait in the cross-module cases goes through it.
func PooledWait(tb TB, what string, within time.Duration, ok func() bool) time.Duration {
	tb.Helper()
	start := time.Now()
	for time.Since(start) < within {
		if ok() {
			return time.Since(start)
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("orchestrationtest: %s did not happen within %s", what, within)
	return 0
}

// AwaitAdvertised waits until a Host appears in the compatible-host directory,
// and returns its advertised capacity report.
func AwaitAdvertised(tb TB, world *PooledWorld, id sessionwire.HostID) sessionwire.HostLinkCapacityReport {
	tb.Helper()
	var found sessionwire.HostLinkCapacityReport
	PooledWait(tb, "host "+string(id)+" advertised capacity", 20*time.Second, func() bool {
		page, err := world.Store.ListCompatibleHosts(context.Background(), sessionstore.ListCompatibleHostsRequest{
			Key: sessionstore.HostTargetKey{
				AgentID:                PooledAgent,
				RuntimeCompatibilityID: string(PooledCompatibility),
				Placement:              sessionwire.HostPlacementPooled,
			},
			Limit: 8,
		})
		if err != nil {
			return false
		}
		for _, candidate := range page.Hosts {
			if candidate.HostID == id {
				found = candidate
				return true
			}
		}
		return false
	})
	return found
}
