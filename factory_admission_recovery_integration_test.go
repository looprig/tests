//go:build integration

// Runbook 07, task I1.2: admission, crash, and two-Factory ordering.
//
// Every case here runs REAL released Factories (two replicas over one durable
// store), a REAL released Host running a REAL harness runtime, and the REAL
// SessionStore. The claims are about durability and identity, so every
// assertion is read from one of three authorities and never from a recorder:
//
//   - the SessionStore inbox record (what was admitted, in which order, how it
//     settled and when);
//   - the runtime's own harness journal, walked with harness's privileged
//     replayer -- the application prefix that correlates a public CommandID to
//     its runtime command UUID, the disposition frame, and every event CAUSED
//     by that runtime command;
//   - the model request, which is the conversation exactly as the agent saw it.
//
// A command applied twice shows up in all three. A command lost shows up in all
// three. There is nothing in between for a fake to vouch for.

package tests

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/runtimecommand"
	"github.com/looprig/natsstore"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/tests/internal/orchestrationtest"
)

const admissionTenant = orchestrationtest.PooledTenantA

// admissionWorld is one durable plane for one tenant.
func admissionWorld(t *testing.T, ctx context.Context, backend *storage.Composite) *orchestrationtest.PooledWorld {
	t.Helper()
	return orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{admissionTenant},
		Backend: backend,
	})
}

// admissionCreate creates a session with a BARE create -- no first message, so
// no turn -- and waits until it settles applied. Every later model request is
// then caused by a command the case itself admitted.
func admissionCreate(t *testing.T, ctx context.Context, world *orchestrationtest.PooledWorld, served *orchestrationtest.PooledFactory, session sessionwire.SessionID) {
	t.Helper()
	create := sessionwire.CommandID("create-" + string(session))
	status, body := served.Post(t, ctx, admissionTenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(create)),
		SessionID:       session,
		AgentID:         orchestrationtest.PooledAgent,
	})
	if status != http.StatusCreated {
		t.Fatalf("the create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "the create settled applied", 90*time.Second, func() bool {
		return world.CommandState(ctx, admissionTenant, session, create) == sessionstore.InboxStateApplied
	})
}

// admissionStopOwner stops a Host and waits until the directory stops naming a
// live owner for session, so the case's next commands have nobody to go to.
func admissionStopOwner(t *testing.T, ctx context.Context, owner *orchestrationtest.PooledHost, served *orchestrationtest.PooledFactory, session sessionwire.SessionID) {
	t.Helper()
	owner.Stop()
	orchestrationtest.PooledWait(t, "the registry stops reporting a live owner", 60*time.Second, func() bool {
		observed, found, _ := served.Directory.Owner(ctx, admissionTenant, session)
		return !found || observed.Residency != sessionwire.SessionResidencyResident ||
			!observed.Accepting || !observed.ExpiresAt.After(time.Now())
	})
}

func admissionInput(command sessionwire.CommandID, session sessionwire.SessionID, text string) sessionwire.InputRequest {
	return sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)),
		SessionID:       session,
		Blocks:          json.RawMessage(`[{"type":"text","text":"` + text + `"}]`),
	}
}

func inputPath(session sessionwire.SessionID) string {
	return "/v1/sessions/" + string(session) + "/input"
}

// decodeCommandStatus decodes a control answer with Core's own strict decoder.
func decodeCommandStatus(t *testing.T, body []byte) sessionwire.CommandStatus {
	t.Helper()
	var status sessionwire.CommandStatus
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("the answer %s is not a Core CommandStatus: %v", body, err)
	}
	return status
}

// sessionCommands reads every disposition command of one session in ascending
// acceptance order, through the consumption read a Host uses.
func sessionCommands(ctx context.Context, store *sessionstore.Store, session sessionwire.SessionID) ([]sessionstore.DispositionInboxEntry, error) {
	var all []sessionstore.DispositionInboxEntry
	var after uint64
	for {
		page, err := store.ListSessionDispositionCommands(ctx, sessionstore.ListSessionDispositionCommandsRequest{
			TenantID: admissionTenant, SessionID: session, AfterOrder: after, Limit: 64,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, page.Commands...)
		if len(page.Commands) == 0 || page.NextAfterOrder == 0 || page.NextAfterOrder == after {
			return all, nil
		}
		after = page.NextAfterOrder
	}
}

func mustCommand(t *testing.T, ctx context.Context, world *orchestrationtest.PooledWorld, session sessionwire.SessionID, command sessionwire.CommandID) sessionstore.DispositionInboxEntry {
	t.Helper()
	entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID: admissionTenant, SessionID: session, CommandID: command,
	})
	if err != nil {
		t.Fatalf("reading command %q: %v", command, err)
	}
	return entry
}

// assertAppliedExactlyOnce is the exactly-once claim, read from the runtime's
// own journal: ONE application prefix names the public CommandID, it carries
// exactly the runtime command UUID the inbox record's winning admission
// minted, ONE disposition frame says applied, and the effect -- a durable
// TurnStarted or TurnFoldedInto caused by that runtime command -- exists.
//
// harness writes the `applied` disposition, and Host settles the SessionStore
// record from it, BEFORE the loop's effect is durable:
// runtimecommand.DispositionApplied means only "durably accepted into the
// execution path", not "the effect is durable" (harness@v0.36.0
// internal/sessionruntime/runtime_command.go; see
// docs/plans/2026-08-29-factory-host-orchestration-implementation/CLAUDE_DEBUG_I12_CASE1.md).
// A single journal read taken right after the record settles therefore races
// that append -- measured to flake under -race. This polls for the effect
// (bounded, so a genuinely missing effect still fails) and re-reads the
// journal on every attempt, so the exactly-one-prefix and
// exactly-one-disposition checks below run against the FINAL read and still
// catch a late duplicate prefix.
func assertAppliedExactlyOnce(t *testing.T, ctx context.Context, world *orchestrationtest.PooledWorld, session sessionwire.SessionID, command sessionwire.CommandID) (orchestrationtest.CommandEvidence, uuid.UUID) {
	t.Helper()
	entry := mustCommand(t, ctx, world, session, command)
	if entry.Record.State != sessionstore.InboxStateApplied || entry.Record.Outcome == nil ||
		entry.Record.Outcome.Kind != sessionstore.DispositionApplied {
		t.Fatalf("command %q is %q with outcome %+v, want applied", command, entry.Record.State, entry.Record.Outcome)
	}
	runtimeCommand, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID))
	if err != nil || runtimeCommand.IsZero() {
		t.Fatalf("command %q carries runtime command id %q, want a non-zero UUID: %v",
			command, entry.Record.Descriptor.RuntimeCommandID, err)
	}
	runtimeSession := world.RuntimeSessionID(t, ctx, admissionTenant, session)

	var evidence orchestrationtest.CommandEvidence
	orchestrationtest.PooledWait(t, fmt.Sprintf(
		"%q's runtime id %s causing a durable effect (TurnStarted/TurnFoldedInto) or a terminal TurnRejected/InputCancelled",
		command, runtimeCommand,
	), 30*time.Second, func() bool {
		evidence = orchestrationtest.ReadCommandEvidence(t, world, admissionTenant, runtimeSession)
		// A rejection or cancellation is terminal for this runtime command:
		// no later read will ever produce an effect, so stop waiting and let
		// the explicit checks below fail loudly with the specific reason
		// rather than exhausting the timeout on "applied with no effect".
		if evidence.RejectedOf(runtimeCommand) || evidence.CancelledOf(runtimeCommand) {
			return true
		}
		return evidence.EffectsOf(runtimeCommand) > 0
	})

	applications := evidence.ApplicationsOf(command)
	if len(applications) != 1 {
		t.Fatalf("the runtime journal holds %d application prefixes for %q, want exactly 1: %+v", len(applications), command, applications)
	}
	app := applications[0].Application
	if string(app.CommandID) != string(command) {
		t.Fatalf("the application prefix names %q, want the public CommandID %q byte for byte", app.CommandID, command)
	}
	if app.RuntimeCommandID != runtimeCommand {
		t.Fatalf("the application prefix maps %q to runtime id %s, want the inbox record's %s",
			command, app.RuntimeCommandID, runtimeCommand)
	}
	dispositions := evidence.DispositionsOf(command)
	if len(dispositions) != 1 {
		t.Fatalf("the runtime journal holds %d disposition frames for %q, want exactly 1: %+v", len(dispositions), command, dispositions)
	}
	if d := dispositions[0].Disposition; d.RuntimeCommandID != runtimeCommand || d.Disposition != runtimecommand.DispositionApplied {
		t.Fatalf("the disposition frame for %q is %+v, want applied under runtime id %s", command, d, runtimeCommand)
	}
	if evidence.RejectedOf(runtimeCommand) {
		t.Fatalf("%q's runtime id %s caused a TurnRejected: the record settled applied but the input was refused, not applied", command, runtimeCommand)
	}
	if evidence.CancelledOf(runtimeCommand) {
		t.Fatalf("%q's runtime id %s caused an InputCancelled: the record settled applied but the input left the loop's queue without ever starting or folding into a turn", command, runtimeCommand)
	}
	if evidence.EffectsOf(runtimeCommand) == 0 {
		t.Fatalf("no durable effect (TurnStarted/TurnFoldedInto) in the runtime journal is caused by %q's runtime id %s: applied with no effect", command, runtimeCommand)
	}
	return evidence, runtimeCommand
}

// countWord reports how many user messages the model was last shown carry
// word. A command applied twice is its words twice.
func countWord(texts []string, word string) int {
	n := 0
	for _, text := range texts {
		n += strings.Count(text, word)
	}
	return n
}

// awaitModelSaw waits until the conversation the model was last shown carries
// every word, and returns it.
func awaitModelSaw(t *testing.T, world *orchestrationtest.PooledWorld, words ...string) []string {
	t.Helper()
	var texts []string
	orchestrationtest.PooledWait(t, fmt.Sprintf("the model was shown %v", words), 90*time.Second, func() bool {
		texts = world.LLM.UserTextsInLastRequest()
		for _, word := range words {
			if countWord(texts, word) == 0 {
				return false
			}
		}
		return true
	})
	return texts
}

// ---------------------------------------------------------------------------
// Case 1: the same public CommandID raced through two Factories.
// ---------------------------------------------------------------------------

// TestI12SameCommandIDRacedThroughTwoFactoriesAdmitsOnce races one public
// CommandID through two real Factory replicas over one store and asserts one
// inbox record, one runtime command UUID, one immutable acceptance order and a
// byte-equivalent answer from every call -- and then that the one record is
// applied exactly once by the runtime.
//
// The race runs with NO live owner: a record the Host could settle mid-race
// would legitimately change the answer from accepted to applied, and "the
// bytes differ" would then say nothing about admission.
func TestI12SameCommandIDRacedThroughTwoFactoriesAdmitsOnce(t *testing.T) {
	ctx := placementContext(t)
	world := admissionWorld(t, ctx, nil)
	first := orchestrationtest.StartPooledHost(t, ctx, world, "i12-same-id-host-1", 3)
	orchestrationtest.AwaitAdvertised(t, world, first.ID)
	replicaA := orchestrationtest.StartPooledFactory(t, ctx, world, "i12-same-id-replica-a", nil)
	replicaB := orchestrationtest.StartPooledFactory(t, ctx, world, "i12-same-id-replica-b", nil)

	const (
		session = sessionwire.SessionID("session-i12-same-id")
		command = sessionwire.CommandID("command-i12-raced-same")
		word    = "QUINCE"
		racers  = 16
	)
	admissionCreate(t, ctx, world, replicaA, session)
	admissionStopOwner(t, ctx, first, replicaA, session)

	type answer struct {
		replica string
		status  int
		body    []byte
		err     error
	}
	answers := make([]answer, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range racers {
		served, name := replicaA, "A"
		if i%2 == 1 {
			served, name = replicaB, "B"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			status, body, err := served.PostRaw(ctx, admissionTenant, inputPath(session), admissionInput(command, session, word))
			answers[i] = answer{replica: name, status: status, body: body, err: err}
		}()
	}
	close(start)
	wg.Wait()

	var reference []byte
	for i, got := range answers {
		if got.err != nil {
			t.Fatalf("racer %d (replica %s) failed in transport: %v", i, got.replica, got.err)
		}
		if got.status != http.StatusOK {
			t.Fatalf("racer %d (replica %s) answered %d: %s", i, got.replica, got.status, got.body)
		}
		if reference == nil {
			reference = got.body
			continue
		}
		if !bytes.Equal(got.body, reference) {
			t.Fatalf("racer %d (replica %s) answered %s, racer 0 answered %s: the answers are not byte-equivalent",
				i, got.replica, got.body, reference)
		}
	}
	answered := decodeCommandStatus(t, reference)
	t.Logf("all %d racers across two replicas answered %s", racers, reference)
	if answered.CommandID != command || answered.State != sessionwire.CommandStateAccepted || answered.AcceptedOrder == 0 {
		t.Fatalf("the shared answer is %+v, want command %q accepted with an acceptance order", answered, command)
	}

	t.Run("one inbox record, one runtime command UUID, one immutable order", func(t *testing.T) {
		commands, err := sessionCommands(ctx, world.Store, session)
		if err != nil {
			t.Fatalf("listing the session's commands: %v", err)
		}
		var matches []sessionstore.DispositionInboxEntry
		for _, entry := range commands {
			if entry.Record.Descriptor.CommandID == command {
				matches = append(matches, entry)
			}
		}
		if len(matches) != 1 {
			t.Fatalf("the inbox holds %d records for %q, want exactly 1 (all: %d)", len(matches), command, len(commands))
		}
		// The create and this input: nothing else may have been admitted.
		if len(commands) != 2 {
			t.Fatalf("the session holds %d commands, want the create and one input", len(commands))
		}
		entry := matches[0]
		if entry.AcceptedOrder != answered.AcceptedOrder {
			t.Fatalf("the record's acceptance order is %d, every racer was told %d", entry.AcceptedOrder, answered.AcceptedOrder)
		}
		runtimeCommand, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID))
		if err != nil || runtimeCommand.IsZero() {
			t.Fatalf("the record's runtime command id %q is not a non-zero UUID: %v", entry.Record.Descriptor.RuntimeCommandID, err)
		}
		// IMMUTABLE: a later named read returns the same order and runtime id.
		again := mustCommand(t, ctx, world, session, command)
		if again.AcceptedOrder != entry.AcceptedOrder || again.Record.Descriptor.RuntimeCommandID != entry.Record.Descriptor.RuntimeCommandID {
			t.Fatalf("a re-read changed the record's identity: order %d->%d runtime %q->%q",
				entry.AcceptedOrder, again.AcceptedOrder, entry.Record.Descriptor.RuntimeCommandID, again.Record.Descriptor.RuntimeCommandID)
		}
	})

	t.Run("the one record is applied exactly once", func(t *testing.T) {
		second := orchestrationtest.StartPooledHost(t, ctx, world, "i12-same-id-host-2", 5)
		orchestrationtest.AwaitAdvertised(t, world, second.ID)
		orchestrationtest.PooledWait(t, "the raced input settled applied", 120*time.Second, func() bool {
			return world.CommandState(ctx, admissionTenant, session, command) == sessionstore.InboxStateApplied
		})
		assertAppliedExactlyOnce(t, ctx, world, session, command)
		texts := awaitModelSaw(t, world, word)
		if n := countWord(texts, word); n != 1 {
			t.Fatalf("the model was shown %q %d times, want once: %q", word, n, texts)
		}
	})

	t.Run("a retry after settlement answers identically from both replicas", func(t *testing.T) {
		_, fromA, errA := replicaA.PostRaw(ctx, admissionTenant, inputPath(session), admissionInput(command, session, word))
		_, fromB, errB := replicaB.PostRaw(ctx, admissionTenant, inputPath(session), admissionInput(command, session, word))
		if errA != nil || errB != nil {
			t.Fatalf("retrying: A %v, B %v", errA, errB)
		}
		if !bytes.Equal(fromA, fromB) {
			t.Fatalf("after settlement A answers %s and B answers %s", fromA, fromB)
		}
		settled := decodeCommandStatus(t, fromA)
		if settled.State != sessionwire.CommandStateApplied || settled.AcceptedOrder != answered.AcceptedOrder {
			t.Fatalf("the settled retry answers %+v, want applied at order %d", settled, answered.AcceptedOrder)
		}
		assertAppliedExactlyOnce(t, ctx, world, session, command)
	})
}

// ---------------------------------------------------------------------------
// Case 2: distinct commands for one session raced through two Factories.
// ---------------------------------------------------------------------------

// orderWatch is a concurrent reader of one session's acceptance-ordered
// command stream. It asserts, on every read, the property a consumer relies on
// to advance a cursor: once an order is visible, no command with a LOWER order
// may appear later, and no visible command's order ever changes.
type orderWatch struct {
	mu         sync.Mutex
	known      map[sessionwire.CommandID]uint64
	maxSeen    uint64
	reads      int
	violations []string
}

func (w *orderWatch) observe(commands []sessionstore.DispositionInboxEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.reads++
	var previous uint64
	pageMax := w.maxSeen
	for _, entry := range commands {
		id, order := entry.Record.Descriptor.CommandID, entry.AcceptedOrder
		if order <= previous {
			w.violations = append(w.violations, fmt.Sprintf("read %d: order %d listed after %d", w.reads, order, previous))
		}
		previous = order
		if was, seen := w.known[id]; seen {
			if was != order {
				w.violations = append(w.violations, fmt.Sprintf("read %d: %q moved from order %d to %d", w.reads, id, was, order))
			}
			continue
		}
		if order < w.maxSeen {
			w.violations = append(w.violations, fmt.Sprintf(
				"read %d: %q appeared at order %d after order %d was already visible", w.reads, id, order, w.maxSeen))
		}
		w.known[id] = order
		if order > pageMax {
			pageMax = order
		}
	}
	w.maxSeen = pageMax
}

func (w *orderWatch) readCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.reads
}

// TestI12DistinctCommandsRacedThroughTwoFactoriesKeepOneOrder races distinct
// commands for one session through two replicas while a reader watches the
// stream, then proves the runtime applied them in exactly that order.
func TestI12DistinctCommandsRacedThroughTwoFactoriesKeepOneOrder(t *testing.T) {
	ctx := placementContext(t)
	world := admissionWorld(t, ctx, nil)
	owner := orchestrationtest.StartPooledHost(t, ctx, world, "i12-order-host", 3)
	orchestrationtest.AwaitAdvertised(t, world, owner.ID)
	replicaA := orchestrationtest.StartPooledFactory(t, ctx, world, "i12-order-replica-a", nil)
	replicaB := orchestrationtest.StartPooledFactory(t, ctx, world, "i12-order-replica-b", nil)

	const (
		session = sessionwire.SessionID("session-i12-order")
		racers  = 12
	)
	admissionCreate(t, ctx, world, replicaA, session)

	commandOf := func(i int) sessionwire.CommandID {
		return sessionwire.CommandID(fmt.Sprintf("command-i12-order-%02d", i))
	}
	wordOf := func(i int) string { return fmt.Sprintf("ORDERWORD%02dX", i) }

	watch := &orderWatch{known: map[sessionwire.CommandID]uint64{}}
	stopWatch := make(chan struct{})
	watched := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stopWatch:
				watched <- nil
				return
			default:
			}
			commands, err := sessionCommands(ctx, world.Store, session)
			if err != nil {
				watched <- err
				return
			}
			watch.observe(commands)
		}
	}()

	answers := make([]sessionwire.CommandStatus, racers)
	failures := make([]string, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range racers {
		served := replicaA
		if i%2 == 1 {
			served = replicaB
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			status, body, err := served.PostRaw(ctx, admissionTenant, inputPath(session), admissionInput(commandOf(i), session, wordOf(i)))
			if err != nil || status != http.StatusOK {
				failures[i] = fmt.Sprintf("status %d err %v body %s", status, err, body)
				return
			}
			if err := json.Unmarshal(body, &answers[i]); err != nil {
				failures[i] = fmt.Sprintf("undecodable answer %s: %v", body, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	// Keep watching until the stream has been read at least
	// watchReadsAfterAdmission more times after the last admission answered,
	// so a late lower order has somewhere to show up and the watcher's
	// concurrent-read density does not collapse to a token floor under
	// -race (measured 6-37 reads/run there, against ~600 without race). A
	// fixed sleep gave no such guarantee. Bounded by watchTimeout so a
	// stalled watcher still fails loudly instead of hanging the test.
	const (
		watchReadsAfterAdmission = 20
		watchTimeout             = 30 * time.Second
	)
	readsAtLastAdmission := watch.readCount()
	deadline := time.Now().Add(watchTimeout)
	for watch.readCount()-readsAtLastAdmission < watchReadsAfterAdmission {
		if time.Now().After(deadline) {
			t.Fatalf("the order watcher made only %d reads (want %d) in the %s after the last admission answered",
				watch.readCount()-readsAtLastAdmission, watchReadsAfterAdmission, watchTimeout)
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stopWatch)
	if err := <-watched; err != nil {
		t.Fatalf("the order watcher failed to read the stream: %v", err)
	}
	for i, failure := range failures {
		if failure != "" {
			t.Fatalf("racer %d: %s", i, failure)
		}
	}

	t.Run("acceptance order is immutable, distinct and monotonic as observed", func(t *testing.T) {
		watch.mu.Lock()
		violations, reads := append([]string(nil), watch.violations...), watch.reads
		watch.mu.Unlock()
		t.Logf("the watcher made %d concurrent reads of the stream", reads)
		if reads < watchReadsAfterAdmission {
			t.Fatalf("the watcher read the stream %d times, want at least %d; the property was not observed concurrently",
				reads, watchReadsAfterAdmission)
		}
		if len(violations) != 0 {
			t.Fatalf("the acceptance order was violated as observed:\n%s", strings.Join(violations, "\n"))
		}
		seen := map[uint64]sessionwire.CommandID{}
		for i, answer := range answers {
			if answer.CommandID != commandOf(i) || answer.AcceptedOrder == 0 {
				t.Fatalf("racer %d was answered %+v", i, answer)
			}
			if other, dup := seen[answer.AcceptedOrder]; dup {
				t.Fatalf("%q and %q were both told order %d", other, answer.CommandID, answer.AcceptedOrder)
			}
			seen[answer.AcceptedOrder] = answer.CommandID
			if stored := mustCommand(t, ctx, world, session, commandOf(i)).AcceptedOrder; stored != answer.AcceptedOrder {
				t.Fatalf("%q was answered order %d, the record holds %d", commandOf(i), answer.AcceptedOrder, stored)
			}
		}
	})

	t.Run("the runtime applied them in acceptance order, each exactly once", func(t *testing.T) {
		for i := range racers {
			orchestrationtest.PooledWait(t, fmt.Sprintf("%q settled applied", commandOf(i)), 120*time.Second, func() bool {
				return world.CommandState(ctx, admissionTenant, session, commandOf(i)) == sessionstore.InboxStateApplied
			})
		}
		byAcceptance := make([]int, racers)
		for i := range byAcceptance {
			byAcceptance[i] = i
		}
		sort.Slice(byAcceptance, func(a, b int) bool {
			return answers[byAcceptance[a]].AcceptedOrder < answers[byAcceptance[b]].AcceptedOrder
		})

		var evidence orchestrationtest.CommandEvidence
		for i := range racers {
			evidence, _ = assertAppliedExactlyOnce(t, ctx, world, session, commandOf(i))
		}
		// The journal's application prefixes, in LEDGER order, restricted to
		// the raced commands, must be the acceptance order.
		index := map[sessionwire.CommandID]int{}
		for i := range racers {
			index[commandOf(i)] = i
		}
		var applied []int
		for _, app := range evidence.Applications {
			if i, ok := index[sessionwire.CommandID(app.Application.CommandID)]; ok {
				applied = append(applied, i)
			}
		}
		if fmt.Sprint(applied) != fmt.Sprint(byAcceptance) {
			t.Fatalf("the runtime applied %v, the store accepted %v", applied, byAcceptance)
		}

		// And the model was shown every word once, in that order.
		words := make([]string, racers)
		for i := range racers {
			words[i] = wordOf(i)
		}
		texts := awaitModelSaw(t, world, words...)
		conversation := strings.Join(texts, "\n")
		last := -1
		for _, i := range byAcceptance {
			if n := strings.Count(conversation, wordOf(i)); n != 1 {
				t.Fatalf("the model was shown %q %d times, want once", wordOf(i), n)
			}
			at := strings.Index(conversation, wordOf(i))
			if at < last {
				t.Fatalf("the model was shown %q before an earlier-accepted command's words: %q", wordOf(i), texts)
			}
			last = at
		}
	})
}

// ---------------------------------------------------------------------------
// Case 3: a Factory dies after the inbox commit and before the forward.
// ---------------------------------------------------------------------------

// TestI12ACommandCommittedByADeadFactoryIsAppliedByAnother kills a replica
// between SessionInbox commit and forward -- it commits, then never answers
// and never delivers -- with no Host resident, so NOTHING in the world has been
// told about the command. A different replica is then started over the same
// store, and the command must become durably applied before the deadline the
// dead replica stamped on it.
func TestI12ACommandCommittedByADeadFactoryIsAppliedByAnother(t *testing.T) {
	ctx := placementContext(t)
	world := admissionWorld(t, ctx, nil)
	first := orchestrationtest.StartPooledHost(t, ctx, world, "i12-crash-host-1", 3)
	orchestrationtest.AwaitAdvertised(t, world, first.ID)

	// The deadline is SHORT so "before its original deadline" is a bound the
	// case can fail, not a formality five minutes away.
	const deadline = 60 * time.Second
	stall := orchestrationtest.NewStallAfterCommit(world.Store)
	doomed := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica: "i12-crash-replica-doomed", Commands: stall, ApplyDeadline: deadline,
	})

	const (
		session = sessionwire.SessionID("session-i12-crash")
		command = sessionwire.CommandID("command-i12-committed-then-crashed")
		word    = "SLOEBERRY"
	)
	admissionCreate(t, ctx, world, doomed, session)
	admissionStopOwner(t, ctx, first, doomed, session)

	stall.Arm()
	requestCtx, abandon := context.WithCancel(ctx)
	returned := make(chan error, 1)
	go func() {
		status, body, err := doomed.PostRaw(requestCtx, admissionTenant, inputPath(session), admissionInput(command, session, word))
		if err == nil && status == http.StatusOK {
			err = fmt.Errorf("the dying replica answered %d %s; it must never have answered success", status, body)
		} else {
			err = nil
		}
		returned <- err
	}()
	var committed sessionstore.DispositionInboxEntry
	select {
	case committed = <-stall.Committed():
	case <-time.After(30 * time.Second):
		t.Fatalf("the doomed replica never committed the command")
	}
	abandon()
	if err := <-returned; err != nil {
		t.Fatal(err)
	}
	doomed.Stop()

	t.Run("the dead replica left a committed record nobody was told about", func(t *testing.T) {
		entry := mustCommand(t, ctx, world, session, command)
		t.Logf("after the crash: state=%q order=%d deadline=%s claim=%v attempt=%v",
			entry.Record.State, entry.AcceptedOrder, entry.Record.ApplyDeadline.Format(time.RFC3339Nano), entry.Record.Claim, entry.Record.Attempt)
		if entry.Record.State != sessionstore.InboxStatePending || entry.Record.Claim != nil || entry.Record.Attempt != nil {
			t.Fatalf("the committed command is %q claim=%v attempt=%v; the premise that nobody took it is false",
				entry.Record.State, entry.Record.Claim, entry.Record.Attempt)
		}
		if entry.AcceptedOrder != committed.AcceptedOrder {
			t.Fatalf("the record's order %d is not the committed %d", entry.AcceptedOrder, committed.AcceptedOrder)
		}
	})

	second := orchestrationtest.StartPooledHost(t, ctx, world, "i12-crash-host-2", 5)
	orchestrationtest.AwaitAdvertised(t, world, second.ID)
	survivor := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica: "i12-crash-replica-survivor",
	})

	t.Run("another replica sees it durably applied before the original deadline", func(t *testing.T) {
		original := committed.Record.ApplyDeadline
		orchestrationtest.PooledWait(t, "the orphaned command reached a terminal state", time.Until(original), func() bool {
			state := world.CommandState(ctx, admissionTenant, session, command)
			return state == sessionstore.InboxStateApplied || state == sessionstore.InboxStateRejected
		})
		entry := mustCommand(t, ctx, world, session, command)
		if !entry.Record.ApplyDeadline.Equal(original) {
			t.Fatalf("the deadline moved from %s to %s; the survivor must honour the original", original, entry.Record.ApplyDeadline)
		}
		if entry.Record.Outcome == nil || !entry.Record.Outcome.SettledAt.Before(original) {
			t.Fatalf("the command settled %+v, want settled strictly before its original deadline %s", entry.Record.Outcome, original)
		}
		t.Logf("settled %q at %s, %s before the original deadline", entry.Record.Outcome.Kind,
			entry.Record.Outcome.SettledAt.Format(time.RFC3339Nano), original.Sub(entry.Record.Outcome.SettledAt).Round(time.Millisecond))
		assertAppliedExactlyOnce(t, ctx, world, session, command)
		texts := awaitModelSaw(t, world, word)
		if n := countWord(texts, word); n != 1 {
			t.Fatalf("the model was shown %q %d times, want once: %q", word, n, texts)
		}
	})

	t.Run("the client's retry through the survivor resolves its unknown outcome", func(t *testing.T) {
		status, body, err := survivor.PostRaw(ctx, admissionTenant, inputPath(session), admissionInput(command, session, word))
		if err != nil || status != http.StatusOK {
			t.Fatalf("the retry answered %d %s: %v", status, body, err)
		}
		answered := decodeCommandStatus(t, body)
		if answered.State != sessionwire.CommandStateApplied || answered.AcceptedOrder != committed.AcceptedOrder {
			t.Fatalf("the retry answered %+v, want applied at the committed order %d", answered, committed.AcceptedOrder)
		}
		assertAppliedExactlyOnce(t, ctx, world, session, command)
	})
}

// ---------------------------------------------------------------------------
// Case 4: HostLink replies are lost and the command is redelivered.
// ---------------------------------------------------------------------------

// TestI12LostHostLinkRepliesAndRedeliveryApplyOnce puts a fault on the wire
// between both real Factories and the real Host that deletes the Host's reply
// to EVERY command delivery. The Host acts on each delivery; no Factory ever
// learns it did. The client then redelivers the same command through both
// replicas, before and after it settles.
//
// The claim is the application prefix: whatever arrives how often, the
// runtime journal correlates the public CommandID to its runtime UUID once,
// and the effect happens once.
func TestI12LostHostLinkRepliesAndRedeliveryApplyOnce(t *testing.T) {
	ctx := placementContext(t)
	world := admissionWorld(t, ctx, nil)
	dropper := orchestrationtest.NewReplyDropper()
	owner := orchestrationtest.StartPooledHostWith(t, ctx, world, "i12-lost-replies-host", 3, dropper.Wrap)
	orchestrationtest.AwaitAdvertised(t, world, owner.ID)
	replicaA := orchestrationtest.StartPooledFactory(t, ctx, world, "i12-lost-replies-replica-a", nil)
	replicaB := orchestrationtest.StartPooledFactory(t, ctx, world, "i12-lost-replies-replica-b", nil)

	const (
		session = sessionwire.SessionID("session-i12-lost-replies")
		command = sessionwire.CommandID("command-i12-lost-replies")
		after   = sessionwire.CommandID("command-i12-after-lost-replies")
		word    = "MEDLAR"
		later   = "LOQUAT"
	)
	admissionCreate(t, ctx, world, replicaA, session)

	// A viewer on EACH replica gives each one demand for the session, so each
	// holds a route and every admission it answers is followed by a delivery
	// over its own HostLink.
	for _, served := range []*orchestrationtest.PooledFactory{replicaA, replicaB} {
		viewer := orchestrationtest.ConnectPooledViewer(t, ctx, served, admissionTenant)
		t.Cleanup(viewer.Close)
		if err := viewer.Watch(t, ctx, admissionTenant, session); err != nil {
			t.Fatalf("watching the session: %v", err)
		}
	}

	post := func(served *orchestrationtest.PooledFactory, id sessionwire.CommandID, text string) sessionwire.CommandStatus {
		t.Helper()
		began := time.Now()
		status, body, err := served.PostRaw(ctx, admissionTenant, inputPath(session), admissionInput(id, session, text))
		if err != nil || status != http.StatusOK {
			t.Fatalf("posting %q answered %d %s: %v", id, status, body, err)
		}
		// Measured, not asserted: Factory delivers on the request's own
		// context BEFORE it answers, so a lost delivery reply delays the
		// durable acknowledgement by the HostLink RPC bound.
		t.Logf("posting %q answered %s after %v", id, body, time.Since(began).Round(time.Millisecond))
		return decodeCommandStatus(t, body)
	}
	// Delivered and REdelivered CONCURRENTLY through both replicas, so the
	// redeliveries reach the Host while the first delivery is still being
	// applied rather than after it settled. (Sequential posts would not:
	// each answer waits out a lost reply's RPC bound, and the command
	// settles inside the first one.)
	concurrent := []*orchestrationtest.PooledFactory{replicaA, replicaB, replicaA, replicaB}
	early := make([]sessionwire.CommandStatus, len(concurrent))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, served := range concurrent {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			status, body, err := served.PostRaw(ctx, admissionTenant, inputPath(session), admissionInput(command, session, word))
			if err != nil || status != http.StatusOK {
				t.Errorf("concurrent post %d answered %d %s: %v", i, status, body, err)
				return
			}
			if err := json.Unmarshal(body, &early[i]); err != nil {
				t.Errorf("concurrent post %d: undecodable %s: %v", i, body, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	for i, answer := range early {
		if answer.AcceptedOrder != early[0].AcceptedOrder || answer.CommandID != command {
			t.Fatalf("concurrent post %d was answered %+v, post 0 %+v", i, answer, early[0])
		}
	}
	orchestrationtest.PooledWait(t, "the redelivered input settled applied", 120*time.Second, func() bool {
		return world.CommandState(ctx, admissionTenant, session, command) == sessionstore.InboxStateApplied
	})
	// And redelivered after it.
	post(replicaB, command, word)
	post(replicaA, command, word)

	t.Run("the replies really were lost, and the command really was redelivered", func(t *testing.T) {
		if faults := dropper.Faults(); len(faults) != 0 {
			t.Fatalf("the reply dropper could not parse the wire, so a reply may have passed: %v", faults)
		}
		var ours, dropped int
		conns := map[int]bool{}
		for _, delivery := range dropper.Deliveries() {
			if delivery.CommandID != command {
				continue
			}
			ours++
			conns[delivery.Conn] = true
			if delivery.ReplyDropped {
				dropped++
			}
		}
		settledAt := mustCommand(t, ctx, world, session, command).Record.Outcome.SettledAt
		beforeSettlement := 0
		for _, delivery := range dropper.Deliveries() {
			if delivery.CommandID == command && delivery.At.Before(settledAt) {
				beforeSettlement++
			}
		}
		t.Logf("the Host received %q %d times over %d HostLinks, %d of them before it settled; %d replies were deleted",
			command, ours, len(conns), beforeSettlement, dropped)
		if ours < 2 {
			t.Fatalf("the Host received %q %d times; the case needs a REdelivery", command, ours)
		}
		if len(conns) < 2 {
			t.Fatalf("every delivery of %q came over one HostLink; the case needs both replicas' links", command)
		}
		if dropped == 0 {
			t.Fatalf("no reply to a delivery of %q was deleted; the fault did not fire", command)
		}
		// The interesting half of this case is a redelivery racing the FIRST
		// application, not one arriving after the command already settled: the
		// two posts issued after PooledWait (line ~765-766) are guaranteed
		// redeliveries, but they prove nothing about a race.
		//
		// beforeSettlement >= 2 would prove TWO independent deliveries raced
		// the first application concurrently (the reviewer measured exactly 2
		// in 20/20 runs under -race). That floor was tried here and is NOT
		// reliable in this environment: one -race run of this case produced
		// beforeSettlement == 1 (1 of 4 deliveries before settlement), so a
		// >= 2 requirement flakes rather than pinning a real invariant. The
		// assertion is therefore beforeSettlement >= 1: at least one
		// redelivery reached the Host before the command settled, which still
		// proves a redelivery raced (or immediately preceded) the first
		// application rather than arriving only after it was already durable.
		// If this starts failing (beforeSettlement == 0), either the race
		// genuinely stopped happening, or a Factory change moved delivery off
		// the request path -- e.g. a queued, asynchronously-woken delivery
		// such as the factory v0.7.1 design -- in which case this case's
		// premise needs to be re-derived against that Factory's actual
		// delivery timing when tests bumps its factory pin.
		if beforeSettlement < 1 {
			t.Fatalf("no delivery of %q happened before settlement (0 of %d); this case needs at least one "+
				"redelivery racing the FIRST application rather than every delivery arriving only after it was "+
				"already durable -- otherwise it silently stops exercising a redelivery racing the first "+
				"application. (A stronger beforeSettlement >= 2 would additionally prove two independent "+
				"deliveries raced it concurrently, but that floor measured as low as 1 under -race in this "+
				"environment and would flake.)",
				command, ours)
		}
	})

	t.Run("the application prefix correlates public and runtime ids exactly once", func(t *testing.T) {
		assertAppliedExactlyOnce(t, ctx, world, session, command)
		texts := awaitModelSaw(t, world, word)
		if n := countWord(texts, word); n != 1 {
			t.Fatalf("the model was shown %q %d times, want once: %q", word, n, texts)
		}
	})

	t.Run("and the session still takes new commands", func(t *testing.T) {
		post(replicaB, after, later)
		orchestrationtest.PooledWait(t, "the next input settled applied", 120*time.Second, func() bool {
			return world.CommandState(ctx, admissionTenant, session, after) == sessionstore.InboxStateApplied
		})
		assertAppliedExactlyOnce(t, ctx, world, session, after)
		assertAppliedExactlyOnce(t, ctx, world, session, command)
	})
}

// ---------------------------------------------------------------------------
// Case 5: an opaque CommandID round-trips.
// ---------------------------------------------------------------------------

// opaqueCommandID is a VALID Core CommandID carrying every character class a
// careless layer mangles: ':' and '/' (path and key separators), uppercase
// (case folding), and base64url's '-' and '_' (URL and key encoders).
func opaqueCommandID(salt string) sessionwire.CommandID {
	encoded := base64.RawURLEncoding.EncodeToString([]byte{0xfb, 0xff, 0xbf, 0x3e, 0x7f, 0x01, 0xfe})
	return sessionwire.CommandID("Tenant:Cmd/" + salt + "/UPPER:lower/" + encoded)
}

// TestI12OpaqueCommandIDRoundTripsThroughBothReplicasAndTheProvider admits an
// opaque CommandID through one replica, retries it through the other, and
// follows it byte for byte into the store and into the runtime journal -- over
// each released provider the pooled world can select.
func TestI12OpaqueCommandIDRoundTripsThroughBothReplicasAndTheProvider(t *testing.T) {
	providers := []struct {
		name string
		open func(t *testing.T, ctx context.Context) *storage.Composite
	}{
		{name: "memstore", open: func(*testing.T, context.Context) *storage.Composite { return nil }},
		{name: "natsstore", open: func(t *testing.T, ctx context.Context) *storage.Composite {
			t.Helper()
			store, err := natsstore.Open(ctx, natsstore.Options{EmbeddedDir: t.TempDir()})
			if err != nil {
				t.Fatalf("natsstore.Open: %v", err)
			}
			t.Cleanup(func() {
				closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := store.Close(closeCtx); err != nil {
					t.Errorf("natsstore Close: %v", err)
				}
			})
			return store.Backend()
		}},
	}
	for _, provider := range providers {
		t.Run(provider.name, func(t *testing.T) {
			ctx := placementContext(t)
			world := admissionWorld(t, ctx, provider.open(t, ctx))
			owner := orchestrationtest.StartPooledHost(t, ctx, world, sessionwire.HostID("i12-opaque-host-"+provider.name), 3)
			orchestrationtest.AwaitAdvertised(t, world, owner.ID)
			replicaA := orchestrationtest.StartPooledFactory(t, ctx, world, "i12-opaque-replica-a", nil)
			replicaB := orchestrationtest.StartPooledFactory(t, ctx, world, "i12-opaque-replica-b", nil)

			session := sessionwire.SessionID("session-i12-opaque-" + provider.name)
			command := opaqueCommandID(provider.name)
			if err := command.Validate(); err != nil {
				t.Fatalf("the opaque id %q is not a valid Core CommandID: %v", command, err)
			}
			const word = "PERSIMMON"
			admissionCreate(t, ctx, world, replicaA, session)

			status, fromA, err := replicaA.PostRaw(ctx, admissionTenant, inputPath(session), admissionInput(command, session, word))
			if err != nil || status != http.StatusOK {
				t.Fatalf("admitting through A answered %d %s: %v", status, fromA, err)
			}
			answered := decodeCommandStatus(t, fromA)
			if answered.CommandID != command {
				t.Fatalf("A echoed command id %q, want %q byte for byte", answered.CommandID, command)
			}

			orchestrationtest.PooledWait(t, "the opaque command settled applied", 120*time.Second, func() bool {
				return world.CommandState(ctx, admissionTenant, session, command) == sessionstore.InboxStateApplied
			})

			t.Run("both replicas answer it identically", func(t *testing.T) {
				_, againA, errA := replicaA.PostRaw(ctx, admissionTenant, inputPath(session), admissionInput(command, session, word))
				_, fromB, errB := replicaB.PostRaw(ctx, admissionTenant, inputPath(session), admissionInput(command, session, word))
				if errA != nil || errB != nil {
					t.Fatalf("retrying: A %v, B %v", errA, errB)
				}
				if !bytes.Equal(againA, fromB) {
					t.Fatalf("A answers %s and B answers %s", againA, fromB)
				}
				if got := decodeCommandStatus(t, fromB); got.CommandID != command || got.AcceptedOrder != answered.AcceptedOrder {
					t.Fatalf("B answered %+v, want %q at order %d", got, command, answered.AcceptedOrder)
				}
			})

			t.Run("the provider stores it byte for byte, and only it", func(t *testing.T) {
				entry := mustCommand(t, ctx, world, session, command)
				if entry.Record.Descriptor.CommandID != command {
					t.Fatalf("the provider returned %q for %q", entry.Record.Descriptor.CommandID, command)
				}
				commands, err := sessionCommands(ctx, world.Store, session)
				if err != nil {
					t.Fatalf("listing the session's commands: %v", err)
				}
				found := 0
				for _, listed := range commands {
					if listed.Record.Descriptor.CommandID == command {
						found++
					}
				}
				if found != 1 {
					t.Fatalf("the listed stream holds %d records for %q, want 1", found, command)
				}
				// OPAQUE: an id differing only by case, or by an escaped
				// separator, is a different command that does not exist.
				for _, near := range []sessionwire.CommandID{
					sessionwire.CommandID(strings.ToLower(string(command))),
					sessionwire.CommandID(strings.ReplaceAll(string(command), "/", "%2F")),
					sessionwire.CommandID(strings.ReplaceAll(string(command), ":", "_")),
				} {
					_, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
						TenantID: admissionTenant, SessionID: session, CommandID: near,
					})
					if err == nil {
						t.Fatalf("the provider answered a record for %q, a different id than %q", near, command)
					}
					// A provider or key-encoding failure on a near-miss id (for
					// example a NATS subject problem with "%") must not be
					// mistaken for "no such record": pin the specific
					// not-found code so this stays a proof of absence rather
					// than a proof of any error at all.
					var inboxErr *sessionstore.InboxError
					if !errors.As(err, &inboxErr) || inboxErr.Code != sessionstore.InboxErrorNotFound {
						t.Fatalf("looking up near-miss %q: want *sessionstore.InboxError{Code: InboxErrorNotFound}, got %v", near, err)
					}
				}
			})

			t.Run("the runtime journal correlates it byte for byte", func(t *testing.T) {
				assertAppliedExactlyOnce(t, ctx, world, session, command)
				runtimeSession := world.RuntimeSessionID(t, ctx, admissionTenant, session)
				if orchestrationtest.CountJournalEvents[event.TurnDone](t, world, admissionTenant, runtimeSession) == 0 {
					orchestrationtest.PooledWait(t, "the opaque command's turn finished", 60*time.Second, func() bool {
						return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, admissionTenant, runtimeSession) >= 1
					})
				}
			})
		})
	}
}
