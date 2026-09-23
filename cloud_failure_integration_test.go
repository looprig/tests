//go:build integration && cloud

// This file is runbook 07 task P3.1 step 3 over the cloud composite:
// independently stop and restart PostgreSQL, S3, Factory and Host, and require
// that no ACKNOWLEDGED command and no object reference disappears, and that a
// command whose outcome was AMBIGUOUS to its caller resolves by reread --
// re-posting the same command id -- to exactly one durable command applied
// exactly once.
//
// # What "at each durable prefix" is here, and what it is not
//
// PostgreSQL and MinIO are CRASHED (docker kill, SIGKILL) and restarted while a
// real Factory and a real pooled Host are serving, so every write in flight at
// the moment of the kill is cut at whatever prefix it had reached. Factory and
// Host run in this process, so their "restart" is Stop and a fresh start
// (a new replica / a new generation of the same Host id) -- not a SIGKILL.
// Exhaustive enumeration of every durable write prefix is SessionStore's own
// crash-prefix suite over memstore; this lane does not repeat it over real
// containers.
//
// # What the storage-crash phases require (round 2)
//
// Round 1 found D3: on host v0.5.0 / harness v0.36.0 one failed journal write
// left the runtime unable to persist while Host kept it resident, so the
// session's command stream wedged. harness v0.38.0 reports the latched fault
// and host v0.8.0 releases such a runtime so a successor RESTORES it. The
// phases now require, strictly: if the crash failed any runtime journal
// write, the session is restored by a successor (never restarted); every
// acknowledged input is applied exactly once, except that the ONE input in
// flight when a runtime was lost may instead be visibly closed (rejected,
// outcome not_applied or refused, no effect, never seen by the model); every
// later input applies; and nothing is left pending, claimed or applying.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// outageLedger tracks what a caller was told about every command.
type outageLedger struct {
	acked     []string
	ambiguous []string
	// inFlight names the acknowledged command that was in flight when its
	// runtime was lost -- a Host stopped (crash-equivalent for an in-flight
	// attempt) or a runtime abandoned after a persistence fault (host
	// v0.8.0). The successor settles it from evidence or closes it
	// not_applied under a later journal grant, or the stopping runtime
	// refuses it. Either is an honest durable outcome rather than a lost
	// command, so for these ids "rejected (not_applied or refused) with no
	// application" is accepted -- and logged, because the user's words were
	// acknowledged and never applied.
	inFlight map[string]bool
}

// post sends one input and classifies the answer. A 200 is an
// acknowledgement; anything else -- a fault status or a transport error -- is
// ambiguous: the caller cannot know whether it was recorded.
func (l *outageLedger) post(t *testing.T, ctx context.Context, f *orchestrationtest.PooledFactory, tenant sessionwire.TenantID, s sessionwire.SessionID, id string, within time.Duration) int {
	t.Helper()
	postCtx, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	status, body, err := f.PostRaw(postCtx, tenant, "/v1/sessions/"+string(s)+"/input", cloudInput(id, s, "outage probe "+id))
	switch {
	case err != nil:
		t.Logf("%s: transport error (ambiguous): %v", id, err)
		l.ambiguous = append(l.ambiguous, id)
	case status == http.StatusOK:
		l.acked = append(l.acked, id)
	default:
		t.Logf("%s: answered %d (ambiguous): %s", id, status, body)
		l.ambiguous = append(l.ambiguous, id)
	}
	return status
}

// resolve re-posts an ambiguous command with the SAME id until it is
// acknowledged. Admission is create-if-absent on the id, so this is the
// reread: it can never mint a second command.
func (l *outageLedger) resolve(t *testing.T, ctx context.Context, f *orchestrationtest.PooledFactory, tenant sessionwire.TenantID, s sessionwire.SessionID, id string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		postCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		status, _, err := f.PostRaw(postCtx, tenant, "/v1/sessions/"+string(s)+"/input", cloudInput(id, s, "outage probe "+id))
		cancel()
		if err == nil && status == http.StatusOK {
			l.acked = append(l.acked, id)
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("the ambiguous command %s was never acknowledged on retry within %v", id, within)
}

// commandState reads one command's durable state, reporting a read error.
func commandState(ctx context.Context, w *orchestrationtest.PooledWorld, tenant sessionwire.TenantID, s sessionwire.SessionID, id string) (sessionstore.InboxState, error) {
	entry, err := w.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID: tenant, SessionID: s, CommandID: sessionwire.CommandID(id),
	})
	if err != nil {
		return "", err
	}
	return entry.Record.State, nil
}

// awaitAllApplied waits for every acknowledged command to settle applied, and
// on timeout reports the command's whole durable record -- state, claim,
// attempt, outcome -- because "not applied" alone does not say where it stuck.
func (l *outageLedger) awaitAllApplied(t *testing.T, ctx context.Context, w *orchestrationtest.PooledWorld, tenant sessionwire.TenantID, s sessionwire.SessionID, within time.Duration) {
	t.Helper()
	for _, id := range l.acked {
		started := time.Now()
		for {
			state, _ := commandState(ctx, w, tenant, s, id)
			if state == sessionstore.InboxStateApplied {
				t.Logf("%s applied (waited %v)", id, time.Since(started).Round(time.Millisecond))
				break
			}
			if l.inFlight[id] && state == sessionstore.InboxStateRejected {
				entry, err := w.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
					TenantID: tenant, SessionID: s, CommandID: sessionwire.CommandID(id),
				})
				if err == nil && entry.Record.Outcome != nil && visiblyClosed(entry.Record.Outcome.Kind) {
					t.Logf("FINDING: acknowledged %s was in flight when its runtime was lost and was closed %s (waited %v); its words must be resent",
						id, entry.Record.Outcome.Kind, time.Since(started).Round(time.Millisecond))
					break
				}
			}
			if time.Since(started) > within {
				entry, err := w.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
					TenantID: tenant, SessionID: s, CommandID: sessionwire.CommandID(id),
				})
				record, _ := json.Marshal(struct {
					State   sessionstore.InboxState
					Claim   any
					Attempt any
					Outcome any
				}{entry.Record.State, entry.Record.Claim, entry.Record.Attempt, entry.Record.Outcome})
				t.Fatalf("acknowledged %s (accepted order %d) was not applied within %v; durable record (err=%v): %s\n%s",
					id, entry.AcceptedOrder, within, err, record, streamDiagnosis(ctx, w, tenant, s))
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}

// streamDiagnosis renders the session's whole command stream in acceptance
// order, and the Host's durable consumption cursor, so a wedged stream says
// where it stuck. It prints command ids and states only, no payloads.
func streamDiagnosis(ctx context.Context, w *orchestrationtest.PooledWorld, tenant sessionwire.TenantID, s sessionwire.SessionID) string {
	var b strings.Builder
	page, err := w.Store.ListSessionDispositionCommands(ctx, sessionstore.ListSessionDispositionCommandsRequest{TenantID: tenant, SessionID: s, Limit: 100})
	if err != nil {
		fmt.Fprintf(&b, "command stream unreadable: %v\n", err)
	}
	for _, c := range page.Commands {
		claimed := ""
		if c.Record.Claim != nil {
			claimed = " claimed"
		}
		attempt := ""
		if c.Record.Attempt != nil {
			attempt = " attempt"
		}
		fmt.Fprintf(&b, "  order=%d id=%s state=%s%s%s\n", c.AcceptedOrder, c.Record.Descriptor.CommandID, c.Record.State, claimed, attempt)
	}
	cursor, err := w.Store.LoadDispositionCommandCursor(ctx, sessionstore.LoadDispositionCommandCursorRequest{TenantID: tenant, SessionID: s})
	fmt.Fprintf(&b, "  consumption cursor: consumed_order=%d lease_epoch=%d (err=%v)", cursor.Cursor.ConsumedOrder, cursor.Cursor.LeaseEpoch, err)
	return b.String()
}

// visiblyClosed reports the terminal outcomes an in-flight command may take
// when its runtime is lost: closed not_applied by a successor, or refused by
// the stopping runtime itself (harness refuses a command write once the
// runtime is sealed). Both are durable and visible to the caller; neither
// applied anything.
func visiblyClosed(kind sessionstore.DispositionOutcomeKind) bool {
	return kind == sessionstore.DispositionNotApplied || kind == sessionstore.DispositionRefused
}

// s3ObjectCount counts the raw objects under a deployment prefix.
func s3ObjectCount(t *testing.T, ctx context.Context, w *cloudWorld, prefix string) int {
	t.Helper()
	return len(listRaw(t, ctx, rawS3(t, ctx, w.cfg), w.cfg, prefix+"/"))
}

// outageEnv is one independent world for one fault: its own deployments, its
// own Host and Factory, one session with a baseline of applied commands.
type outageEnv struct {
	cfg        cloudConfig
	ctx        context.Context
	world      *cloudWorld
	tenant     sessionwire.TenantID
	session    sessionwire.SessionID
	host       *orchestrationtest.PooledHost
	generation uint64
	served     *orchestrationtest.PooledFactory
	ledger     *outageLedger
	objects    int
}

const outageHostID = sessionwire.HostID("orchestrationtest-cloud-outage-host")

func newOutageEnv(t *testing.T, cfg cloudConfig) *outageEnv {
	t.Helper()
	env := &outageEnv{
		cfg: cfg, ctx: cloudContext(t, 8*time.Minute),
		tenant: orchestrationtest.PooledTenantA, session: "session-cloud-outage",
		generation: 4, ledger: &outageLedger{},
	}
	hostLogs := &lockedLog{}
	env.world = newCloudWorld(t, env.ctx, cfg, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{env.tenant}, HostLogs: hostLogs,
	})
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Host WARN/ERROR diagnostics:\n%s", hostLogs.notable())
		}
	})
	env.host = orchestrationtest.StartPooledHost(t, env.ctx, env.world.PooledWorld, outageHostID, env.generation)
	orchestrationtest.AwaitAdvertised(t, env.world.PooledWorld, outageHostID)
	env.served = orchestrationtest.StartPooledFactory(t, env.ctx, env.world.PooledWorld, "orchestrationtest-cloud-outage-replica-1", nil)
	cloudPost(t, env.ctx, env.served, env.tenant, "/v1/sessions", cloudCreate("outage-create", env.session, "the remembered word is SORB"), http.StatusCreated)
	env.ledger.acked = append(env.ledger.acked, "outage-create")
	env.post(t, "outage-baseline", 30*time.Second)
	env.ledger.awaitAllApplied(t, env.ctx, env.world.PooledWorld, env.tenant, env.session, 120*time.Second)
	env.objects = s3ObjectCount(t, env.ctx, env.world, env.world.journals[env.tenant].Deployment.Prefix)
	t.Logf("baseline: %d journal objects in S3", env.objects)
	return env
}

func (e *outageEnv) post(t *testing.T, id string, within time.Duration) int {
	t.Helper()
	return e.ledger.post(t, e.ctx, e.served, e.tenant, e.session, id, within)
}

// settle is every fault's closing claim: every acknowledged command is
// applied, exactly once; every ambiguous one resolved by reread to exactly one
// command; the durable command set is exactly the acknowledged set; the
// journal's S3 object count never fell; and the whole journal -- every
// offloaded body -- replays.
func (e *outageEnv) settle(t *testing.T) {
	t.Helper()
	e.ledger.awaitAllApplied(t, e.ctx, e.world.PooledWorld, e.tenant, e.session, 150*time.Second)
	if now := s3ObjectCount(t, e.ctx, e.world, e.world.journals[e.tenant].Deployment.Prefix); now < e.objects {
		t.Fatalf("the journal's S3 object count fell from %d to %d", e.objects, now)
	}
	t.Logf("acknowledged: %v; ambiguous then resolved by reread: %v", e.ledger.acked, e.ledger.ambiguous)
	runtimeID := e.world.RuntimeSessionID(t, e.ctx, e.tenant, e.session)
	// ReadCommandEvidence fails closed on a frame it cannot read, including
	// an offloaded body whose S3 object is gone.
	evidence := orchestrationtest.ReadCommandEvidence(t, e.world.PooledWorld, e.tenant, runtimeID)
	for _, id := range uniqueSorted(append(append([]string(nil), e.ledger.acked...), e.ledger.ambiguous...)) {
		if id == "outage-create" {
			continue
		}
		apps := evidence.ApplicationsOf(sessionwire.CommandID(id))
		if e.ledger.inFlight[id] {
			state, _ := commandState(e.ctx, e.world.PooledWorld, e.tenant, e.session, id)
			switch {
			case state == sessionstore.InboxStateApplied && len(apps) == 1:
				t.Logf("in-flight %s applied exactly once", id)
			case state == sessionstore.InboxStateRejected && len(apps) <= 1:
				// A visible rejection may carry the runtime's APPLICATION
				// PREFIX (the intent record written before applying) -- the
				// S3 shape writes one and is then sealed, so the command is
				// closed refused -- but it must have had no EFFECT and the
				// model must never have seen it: an input that reached a turn
				// and was then reported refused would tell the user to resend
				// words the agent already acted on.
				if e.world.LLM.SawInRequest(0, "outage probe "+id) {
					t.Fatalf("in-flight %s was reported rejected but the model saw its words", id)
				}
				entry, err := e.world.Store.GetDispositionCommand(e.ctx, sessionstore.GetDispositionCommandRequest{
					TenantID: e.tenant, SessionID: e.session, CommandID: sessionwire.CommandID(id),
				})
				if err != nil {
					t.Fatalf("reading in-flight %s: %v", id, err)
				}
				runtimeCommand, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID))
				if err != nil {
					t.Fatalf("in-flight %s names runtime command %q: %v", id, entry.Record.Descriptor.RuntimeCommandID, err)
				}
				if effects := evidence.EffectsOf(runtimeCommand); effects != 0 {
					t.Fatalf("in-flight %s was reported %s but its input reached %d turns", id, entry.Record.Outcome.Kind, effects)
				}
				t.Logf("in-flight %s was visibly closed %s with %d application prefix(es), no effect, unseen by the model", id, entry.Record.Outcome.Kind, len(apps))
			default:
				entry, _ := e.world.Store.GetDispositionCommand(e.ctx, sessionstore.GetDispositionCommandRequest{
					TenantID: e.tenant, SessionID: e.session, CommandID: sessionwire.CommandID(id),
				})
				outcome, effects := "", -1
				if entry.Record.Outcome != nil {
					outcome = string(entry.Record.Outcome.Kind)
				}
				if runtimeCommand, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID)); err == nil {
					effects = evidence.EffectsOf(runtimeCommand)
				}
				t.Fatalf("in-flight %s is %s (outcome %q) with %d applications, %d dispositions and %d effects; the model saw its words: %v -- want applied once, or rejected with no effect",
					id, state, outcome, len(apps), len(evidence.DispositionsOf(sessionwire.CommandID(id))), effects,
					e.world.LLM.SawInRequest(0, "outage probe "+id))
			}
			continue
		}
		if len(apps) != 1 {
			t.Fatalf("%s was applied %d times, want exactly once", id, len(apps))
		}
	}
	page, err := e.world.Store.ListSessionDispositionCommands(e.ctx, sessionstore.ListSessionDispositionCommandsRequest{
		TenantID: e.tenant, SessionID: e.session, Limit: 100,
	})
	if err != nil {
		t.Fatalf("listing the session's commands: %v", err)
	}
	ids := make([]string, 0, len(page.Commands))
	for _, command := range page.Commands {
		ids = append(ids, string(command.Record.Descriptor.CommandID))
	}
	sort.Strings(ids)
	for _, command := range page.Commands {
		switch command.Record.State {
		case sessionstore.InboxStateApplied, sessionstore.InboxStateRejected:
		default:
			t.Fatalf("command %s is left %s; nothing may be stuck after recovery\n%s",
				command.Record.Descriptor.CommandID, command.Record.State, streamDiagnosis(e.ctx, e.world.PooledWorld, e.tenant, e.session))
		}
	}
	if want := uniqueSorted(e.ledger.acked); fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Fatalf("the session holds commands %v, want exactly the acknowledged set %v", ids, want)
	}
}

// journalWriteFailures counts the runtime journal's failed writes so far --
// the event that latches harness's persistence fault.
func (e *outageEnv) journalWriteFailures() int {
	metrics := e.world.journals[e.tenant].Metrics
	return metrics.Errors("ledger.append") + metrics.Errors("blobs.put")
}

// requireRestoredBySuccessor requires that, IF the crash failed a runtime
// journal write (latching the runtime's persistence fault), the faulted
// runtime was released and the session RESTORED by a successor -- the
// recovery D3 lacked -- and in every case that the conversation was not
// restarted. A crash that failed no journal write (PgBouncer in front of a
// restarting PostgreSQL can hold a query until the server is back) faults
// nothing, so no restore is owed; that is logged rather than required.
func (e *outageEnv) requireRestoredBySuccessor(t *testing.T, restoresBefore, failuresBefore int) {
	t.Helper()
	restores := len(e.host.Rig.Restores())
	failures := e.journalWriteFailures() - failuresBefore
	switch {
	case failures > 0 && restores <= restoresBefore:
		t.Fatalf("the crash failed %d runtime journal writes but no successor restored the session (restores %d -> %d); the faulted runtime was never released",
			failures, restoresBefore, restores)
	case failures > 0:
		t.Logf("the crash failed %d runtime journal writes; restored by a successor (restores %d -> %d)", failures, restoresBefore, restores)
	default:
		t.Logf("the crash failed no runtime journal write, so nothing faulted and no restore was owed (restores %d -> %d)", restoresBefore, restores)
	}
	runtimeID := e.world.RuntimeSessionID(t, e.ctx, e.tenant, e.session)
	if started := orchestrationtest.CountJournalEvents[event.SessionStarted](t, e.world.PooledWorld, e.tenant, runtimeID); started != 1 {
		t.Fatalf("the journal holds %d SessionStarted, want 1: the session was restarted, not restored", started)
	}
}

// TestCloudOutagesLoseNoAcknowledgedCommandOrObject is step 3: each fault in
// its own world, so one fault's damage cannot be mistaken for another's.
func TestCloudOutagesLoseNoAcknowledgedCommandOrObject(t *testing.T) {
	cfg := requireCloud(t)

	t.Run("PostgreSQL crash and restart", func(t *testing.T) {
		env := newOutageEnv(t, cfg)
		restoresBefore, failuresBefore := len(env.host.Rig.Restores()), env.journalWriteFailures()
		env.post(t, "pg-before", 30*time.Second)
		// host v0.8.0: the PostgreSQL shape rejects the input in flight at the
		// crash rather than redelivering it in place.
		env.ledger.inFlight = map[string]bool{"pg-before": true}
		cloudDocker(t, "kill", cfg.pgContainer)
		status := env.post(t, "pg-during", 10*time.Second)
		t.Logf("PostgreSQL killed; pg-during answered %d", status)
		cloudDocker(t, "start", cfg.pgContainer)
		t.Logf("PostgreSQL back after %v", awaitPostgres(t, env.ctx, cfg.pgDSN, 90*time.Second).Round(time.Millisecond))
		if status != http.StatusOK {
			env.ledger.resolve(t, env.ctx, env.served, env.tenant, env.session, "pg-during", 120*time.Second)
		}
		env.post(t, "pg-after", 60*time.Second)
		env.settle(t)
		env.requireRestoredBySuccessor(t, restoresBefore, failuresBefore)
	})

	t.Run("S3 crash and restart", func(t *testing.T) {
		env := newOutageEnv(t, cfg)
		restoresBefore, failuresBefore := len(env.host.Rig.Restores()), env.journalWriteFailures()
		// s3-during's turn writes to S3, so it is the input in flight when
		// the runtime faults. host v0.8.0 settles it applied in most runs;
		// in roughly a third (measured) it is instead closed refused after
		// its application prefix, which the ledger accepts only with no
		// effect and unseen by the model.
		env.ledger.inFlight = map[string]bool{"s3-during": true}
		cloudDocker(t, "kill", cfg.s3Container)
		// Admission touches only PostgreSQL, so this can be acknowledged
		// while S3 is down; its APPLICATION offloads the turn's records to
		// S3, and must still happen once S3 is back.
		status := env.post(t, "s3-during", 15*time.Second)
		t.Logf("MinIO killed; s3-during answered %d", status)
		time.Sleep(5 * time.Second) // let the Host attempt the apply against a dead S3
		cloudDocker(t, "start", cfg.s3Container)
		t.Logf("MinIO back after %v", awaitS3(t, env.ctx, cfg, 90*time.Second).Round(time.Millisecond))
		if status != http.StatusOK {
			env.ledger.resolve(t, env.ctx, env.served, env.tenant, env.session, "s3-during", 120*time.Second)
		}
		env.post(t, "s3-after", 60*time.Second)
		env.settle(t)
		env.requireRestoredBySuccessor(t, restoresBefore, failuresBefore)
	})

	t.Run("Factory stop and a fresh replica", func(t *testing.T) {
		env := newOutageEnv(t, cfg)
		env.post(t, "factory-before", 30*time.Second)
		env.served.Stop()
		env.served = orchestrationtest.StartPooledFactory(t, env.ctx, env.world.PooledWorld, "orchestrationtest-cloud-outage-replica-2", nil)
		env.post(t, "factory-after", 60*time.Second)
		env.settle(t)
	})

	t.Run("Host stop and restart at a newer generation", func(t *testing.T) {
		env := newOutageEnv(t, cfg)
		env.post(t, "host-before", 30*time.Second)
		env.ledger.inFlight = map[string]bool{"host-before": true}
		env.host.Stop()
		env.generation++
		env.host = orchestrationtest.StartPooledHost(t, env.ctx, env.world.PooledWorld, outageHostID, env.generation)
		orchestrationtest.AwaitAdvertised(t, env.world.PooledWorld, outageHostID)
		env.post(t, "host-after", 60*time.Second)
		env.settle(t)
	})
}

// lockedLog collects Host diagnostics from many goroutines.
type lockedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// notable returns the WARN and ERROR lines, which carry no payloads.
func (l *lockedLog) notable() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, line := range strings.Split(l.buf.String(), "\n") {
		if strings.Contains(line, `"level":"WARN"`) || strings.Contains(line, `"level":"ERROR"`) {
			out = append(out, line)
		}
	}
	if len(out) > 200 {
		out = append(out[:100], append([]string{"..."}, out[len(out)-100:]...)...)
	}
	return strings.Join(out, "\n")
}

func uniqueSorted(in []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, v := range in {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}
