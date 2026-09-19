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
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
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
// That return value is the whole point. An answered gate whose answer never
// reaches the agent is indistinguishable from an abandoned one at the store, so
// the case asserts the agent CONTINUED by looking for the answer text in the
// next model request -- and it can only get there through this result.
type PooledAskTool struct {
	Question string

	mu      sync.Mutex
	calls   int
	answers []string
}

// Info satisfies tool.InvokableTool.
func (t *PooledAskTool) Info(context.Context) (*tool.ToolInfo, error) {
	return &tool.ToolInfo{
		Name:   PooledAskToolName,
		Desc:   "Asks the user a question and returns their answer.",
		Schema: []byte(`{"type":"object","properties":{},"additionalProperties":false}`),
	}, nil
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

var _ tool.InvokableTool = (*PooledAskTool)(nil)

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

type pooledRecorder struct {
	mu        sync.Mutex
	dispatch  []PooledDispatch
	commanded []department.RuntimeCommand
}

func (r *pooledRecorder) add(cmd department.RuntimeCommand) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dispatch = append(r.dispatch, PooledDispatch{CommandID: cmd.CommandID, AttemptID: cmd.AttemptID})
	r.commanded = append(r.commanded, cmd)
}

func (r *pooledRecorder) found(command sessionwire.CommandID, attempt string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, seen := range r.dispatch {
		if seen.CommandID == command && seen.AttemptID == attempt {
			return true
		}
	}
	return false
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

	mu       sync.Mutex
	creates  []PooledLaunch
	restores []PooledLaunch
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
	return &pooledSession{
		controller: controller,
		recorder:   p.recorder,
		tails:      p.tails,
		key:        pooledTailKey{req.TenantID, req.SessionID},
	}, nil
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
	return &pooledSession{
		controller: controller,
		recorder:   p.recorder,
		tails:      p.tails,
		key:        pooledTailKey{req.TenantID, req.SessionID},
	}, nil
}

// Creates and Restores report what this Host's rig launched, in order.
func (p *PooledRig) Creates() []PooledLaunch {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PooledLaunch(nil), p.creates...)
}

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

// ApplyCommand satisfies department.CommandApplier.
//
// It REFUSES an unframed command for the reason FakeRuntime does: Host mints a
// runtime UUID for every command it applies, and a zero one means the case
// stopped exercising the correlation it claims to.
func (s *pooledSession) ApplyCommand(ctx context.Context, cmd department.RuntimeCommand) error {
	if cmd.RuntimeCommandID.IsZero() {
		return ErrUnframedCommand
	}
	if texts := pooledTexts(cmd.Payload); len(texts) > 0 {
		if _, err := s.controller.Submit(ctx, []content.Block{&content.TextBlock{Text: strings.Join(texts, "\n")}}); err != nil {
			return err
		}
		s.tails.Emit(s.key, PooledPublicationsPerInput)
	}
	s.recorder.add(cmd)
	return nil
}

// pooledTexts pulls every "text" member out of a command payload, whatever its
// nesting. The payload shape is Core's and this harness does not restate it.
func pooledTexts(payload []byte) []string {
	var decoded any
	if json.Unmarshal(payload, &decoded) != nil {
		return nil
	}
	var texts []string
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case map[string]any:
			for key, member := range typed {
				if text, ok := member.(string); ok && key == "text" {
					texts = append(texts, text)
				} else {
					walk(member)
				}
			}
		case []any:
			for _, member := range typed {
				walk(member)
			}
		}
	}
	walk(decoded)
	return texts
}

// PooledEvidence is the settlement evidence reader, EMBEDDING the real harness
// journal store: the same value is Host's runtime-journal reader, which
// host.Compose refuses unless it is a harness store.
//
// It vouches only for a dispatch the product runtime actually recorded, so a
// settlement can never be claimed for a command that never reached the runtime.
type PooledEvidence struct {
	*harnessstore.Store
	recorder *pooledRecorder
}

// ReadDispositionEvidence satisfies sessionstore.DispositionEvidenceReader.
func (e PooledEvidence) ReadDispositionEvidence(_ context.Context, req sessionstore.DispositionEvidenceRequest) (sessionstore.DispositionEvidence, error) {
	if e.recorder.found(req.CommandID, string(req.Attempt.AttemptID)) {
		return sessionstore.DispositionEvidence{
			AttemptID:           req.Attempt.AttemptID,
			Kind:                sessionstore.DispositionApplied,
			AttemptJournalEpoch: req.Attempt.JournalEpoch,
			AuthorJournalEpoch:  req.Attempt.JournalEpoch,
			DispositionSeq:      1,
		}, nil
	}
	return sessionstore.DispositionEvidence{}, fmt.Errorf(
		"%w: the runtime recorded no dispatch of %s under attempt %s",
		ErrEvidenceNotYetReadable, req.CommandID, req.Attempt.AttemptID)
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
func PooledModel() model.Model {
	return model.Model{
		Provider:  "orchestrationtest",
		APIFormat: model.APIFormatOpenAI,
		BaseURL:   "http://127.0.0.1/v1",
		Name:      "orchestrationtest-model",
	}
}

func (w *PooledWorld) defineRig(tb TB, tenant sessionwire.TenantID) *rig.Rig {
	tb.Helper()
	loopOptions := []loop.Option{
		loop.WithName("orchestrationtest-agent"),
		loop.WithInference(w.LLM, PooledModel()),
	}
	if w.gated {
		loopOptions = append(loopOptions, loop.WithTools(pooledAskDefinition(w.AskTool)))
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

// StartPooledHost composes, starts and serves one Host over the world's shared
// backend, with one real harness rig per tenant.
func StartPooledHost(tb TB, ctx context.Context, world *PooledWorld, id sessionwire.HostID, generation uint64) *PooledHost {
	tb.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("orchestrationtest: opening the pooled host listener: %v", err)
		return nil
	}
	listener := &pooledTrackingListener{Listener: raw}
	base := sessionwire.InternalEndpoint("ws://" + listener.Addr().String())

	recorder := &pooledRecorder{}
	rigs := map[sessionwire.TenantID]*rig.Rig{}
	journals := map[host.EvidenceKey]sessionstore.DispositionEvidenceReader{}
	for _, tenant := range world.tenants {
		rigs[tenant] = world.defineRig(tb, tenant)
		journals[host.EvidenceKey{TenantID: tenant, StorageBindingID: PooledBinding}] =
			PooledEvidence{Store: world.Journals[tenant], recorder: recorder}
	}
	product := &PooledRig{rigs: rigs, recorder: recorder, tails: world.Tails}

	blueprint := host.Composition{
		Options: host.Options{
			HostID:           id,
			InternalEndpoint: base,
			// CROSS-TENANT ISOLATED, not tenant-exclusive: this is the
			// advertisement that lets Factory place two tenants on one Host,
			// which is the whole of the pooled multi-tenancy case.
			IsolationClass:    sessionwire.HostIsolationClassCrossTenantIsolated,
			Placement:         sessionwire.HostPlacementPooled,
			Capacity:          8,
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
			pooled.paths = append(pooled.paths, request.URL.Path)
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
	var position uint64
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
