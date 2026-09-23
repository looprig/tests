//go:build integration

// This file is runbook 07's I3.1: the LOWER-COUNT RACE STRESS.
//
// Many sessions of two tenants run interleaved over at least two REAL Factory
// replicas and two REAL pooled Hosts, under -race, while a chaos loop restarts
// Factories, restarts Hosts, severs HostLinks and probes tenant isolation, and
// the Hosts warm-release idle sessions under them. Every module is the
// published one: factory.New, host.Compose, sessionstore, and one harness rig
// and journal per tenant. The only fakes are the kit's (a responsive model, the
// credential seams, a synthetic committed-publication stream).
//
// # What it asserts, and from what
//
// Every invariant is read from REAL EVIDENCE, never from what a driver was told:
//
//	C1  every ACKNOWLEDGED command reaches a terminal durable state (applied
//	    with an outcome, or rejected) -- the SessionStore record;
//	C2  a command retried with the same CommandID through any replica keeps one
//	    record and one acceptance order -- every acknowledgement's body;
//	C3  each command is APPLIED AT MOST ONCE: at most one application prefix and
//	    one disposition frame, the frame agreeing with the settled outcome, and
//	    an applied input or first message having exactly one resolution -- the
//	    runtime's own harness journal;
//	C4  the runtime applied each session's commands in acceptance order -- the
//	    journal's ledger order against the store's accepted order;
//	C5  one conversation per session: no model request carries another
//	    session's words -- the model's own request log;
//	C6  every answered gate that settled applied was resolved ONCE, by the
//	    user, caused by that command -- the journal;
//	V1  no viewer ever receives a record naming another tenant or session, and
//	    a cross-tenant subscribe is refused;
//	V2  no viewer's stream has a SILENT gap or a repeated new sequence, over
//	    every connection it ever had;
//	V3  once repaired, every final viewer is covered through its session's tip
//	    (a trailing release event it was told of by a tip hint aside);
//	Q1  a Host's resident sessions never exceed its capacity and its command
//	    queue never exceeds its bound; no session is left command-blocked;
//	L1  once everything is stopped, goroutines return to the baseline.
//
// # What it found
//
// On factory v0.8.0 / host v0.7.1 / harness v0.37.1 it fails V2 and V3, for
// two defects reduced to deterministic cases in
// factory_host_stress_repro_integration_test.go -- read those, not a stress
// log, to see them:
//
//   - V2: a browser on a replica that did not re-place a warm-released session
//     is sent the new residency's records with no reset, and a later reset
//     vouches for the release event it never got
//     (TestAViewerOnAnotherReplicaIsToldOfEveryRecordAcrossAWarmRelease);
//   - V3: a replica that held a HostLink to a Host keeps redialling that Host's
//     OLD address after it restarts under the same HostID, so it neither
//     places on nor relays from the successor until its 60s idle reaper runs
//     (TestASessionIsRePlacedPromptlyOnAHostRestartedUnderItsHostID).
//
// L1 also accounts for one upstream leak rather than failing on it; see
// assertGoroutinesSettle.
//
// # Reproducing a run
//
// The seed is logged. Operation CHOICES are a function of it; goroutine
// scheduling is not, so a seed reproduces the workload, not the interleaving.
// Scale with the LOOPRIG_STRESS_* variables named at stressConfigFromEnv; the
// defaults are the CI count. I3.2 (the non-race 5,000 ClientLink soak) is the
// resource measurement; this file measures nothing but correctness.
//
// # What it deliberately does not do
//
// Object paging: the pooled kit's sessions are Host-owned disposition
// sessions, which have no object source at all (sessionstore's pointers and
// PutObject are legacy-only), and the kit's resolver refuses every object. The
// cold paths it CAN drive -- status, journal and gate reads of a warm-released
// session through any replica -- are driven instead.
//
// A Host restart is only taken with no gate open anywhere. That is host
// v0.4.0's documented obligation rather than timidity: a Host that drains with
// a gate open must EXIT, because the parked runtime is uncancelled and keeps
// its lease-holding timers. An in-process Host cannot exit, so asking one to
// drain over a parked gate would measure the obligation's violation.

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/inference"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// ---- configuration ---------------------------------------------------------

type stressConfig struct {
	seed              uint64
	sessions          int
	hosts             int
	replicas          int
	opsPerSession     int
	viewersPerSession int
	warmTTL           time.Duration
	chaosEvery        time.Duration
	settle            time.Duration
}

// stressConfigFromEnv reads the knobs. The defaults are the CI count: sized so
// the whole case, -race included, stays within a few minutes on a laptop.
//
//	LOOPRIG_STRESS_SEED       the workload seed (default: time-derived, logged)
//	LOOPRIG_STRESS_SESSIONS   sessions across both tenants (default 100)
//	LOOPRIG_STRESS_HOSTS      pooled Hosts (default 2, minimum 2)
//	LOOPRIG_STRESS_REPLICAS   live Factory replicas (default 2, minimum 2)
//	LOOPRIG_STRESS_OPS        operations per session after its create (default 12)
//	LOOPRIG_STRESS_LOGDIR     write each Factory replica's JSON log here
//	LOOPRIG_STRESS_VIEWERS    viewers per session (default 2)
//	LOOPRIG_STRESS_SETTLE     bound on the final settle (default 3m)
func stressConfigFromEnv(t *testing.T) stressConfig {
	t.Helper()
	cfg := stressConfig{
		seed:              uint64(time.Now().UnixNano()),
		sessions:          100,
		hosts:             2,
		replicas:          2,
		opsPerSession:     12,
		viewersPerSession: 2,
		warmTTL:           3 * time.Second,
		chaosEvery:        2500 * time.Millisecond,
		settle:            3 * time.Minute,
	}
	integer := func(name string, into *int, minimum int) {
		raw := os.Getenv(name)
		if raw == "" {
			return
		}
		value, err := strconv.Atoi(raw)
		if err != nil || value < minimum {
			t.Fatalf("%s=%q: want an integer >= %d", name, raw, minimum)
		}
		*into = value
	}
	if raw := os.Getenv("LOOPRIG_STRESS_SEED"); raw != "" {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("LOOPRIG_STRESS_SEED=%q: %v", raw, err)
		}
		cfg.seed = value
	}
	integer("LOOPRIG_STRESS_SESSIONS", &cfg.sessions, 2)
	integer("LOOPRIG_STRESS_HOSTS", &cfg.hosts, 2)
	integer("LOOPRIG_STRESS_REPLICAS", &cfg.replicas, 2)
	integer("LOOPRIG_STRESS_OPS", &cfg.opsPerSession, 1)
	integer("LOOPRIG_STRESS_VIEWERS", &cfg.viewersPerSession, 0)
	if raw := os.Getenv("LOOPRIG_STRESS_SETTLE"); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("LOOPRIG_STRESS_SETTLE=%q: %v", raw, err)
		}
		cfg.settle = value
	}
	return cfg
}

// ---- the fleet -------------------------------------------------------------

// stressReplica is one live Factory replica. dead closes when it is stopped, so
// a viewer attached to it knows to go elsewhere.
type stressReplica struct {
	name string
	f    *orchestrationtest.PooledFactory
	dead chan struct{}
}

// stressHost is one Host slot: a stable HostID whose process is replaced, at a
// higher generation, on every restart.
type stressHost struct {
	id         sessionwire.HostID
	generation uint64
	host       *orchestrationtest.PooledHost
}

type stressFleet struct {
	mu       sync.RWMutex
	replicas []*stressReplica
	retired  []*stressReplica
	hosts    []*stressHost
	started  int
	// every is every Host process ever started, so the end of the case can
	// read what each one's runtime launched.
	every []*orchestrationtest.PooledHost
}

func (fl *stressFleet) pick(rng *rand.Rand) *stressReplica {
	fl.mu.RLock()
	defer fl.mu.RUnlock()
	return fl.replicas[rng.IntN(len(fl.replicas))]
}

func (fl *stressFleet) liveHosts() []*orchestrationtest.PooledHost {
	fl.mu.RLock()
	defer fl.mu.RUnlock()
	out := make([]*orchestrationtest.PooledHost, 0, len(fl.hosts))
	for _, slot := range fl.hosts {
		out = append(out, slot.host)
	}
	return out
}

// ---- the ledger ------------------------------------------------------------

type stressAck struct {
	replica string
	status  int
	order   uint64
}

// stressCommand is one command the driver sent, with every answer it got.
// Only an ACKNOWLEDGED command (a 2xx) carries an obligation.
type stressCommand struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
	id      sessionwire.CommandID
	kind    string
	// word is the unique token an input or first message carries; answer is
	// the token a gate response carries.
	word   string
	answer string

	mu        sync.Mutex
	acks      []stressAck
	refusals  []string
	transient int
}

func (c *stressCommand) acknowledged() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.acks) > 0
}

type stressSession struct {
	index  int
	tenant sessionwire.TenantID
	id     sessionwire.SessionID

	mu       sync.Mutex
	commands []*stressCommand
	created  bool
	// final is the flush command: the live output every final viewer
	// subscribed before.
	final *stressCommand
}

func (s *stressSession) add(c *stressCommand) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, c)
}

func (s *stressSession) snapshot() []*stressCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*stressCommand(nil), s.commands...)
}

// stressEpoch is ONE viewer connection's life: what it was told its position
// was when it joined, and everything it received afterwards.
type stressEpoch struct {
	session *stressSession
	replica *stressReplica
	viewer  *orchestrationtest.PooledViewer
	start   uint64
	// subscribed and read stamp the two halves of joining, for a gap report.
	subscribed, read string
}

// stressCounters are observations, logged at the end so a run can be compared
// with another without reading the whole log.
type stressCounters struct {
	commandsSent, commandsAcked, retries, duplicateAcks, refusals atomic.Int64
	gatesSeen, gatesAnswered, gatesMissed                         atomic.Int64
	viewerEpochs, viewerDialFailures, subscribeFailures           atomic.Int64
	coldReads, crossTenantProbes                                  atomic.Int64
	hostRestarts, hostRestartsSkipped, factoryRestarts, severs    atomic.Int64
	metricSamples                                                 atomic.Int64
}

// ---- the stress ------------------------------------------------------------

type stress struct {
	// dumped names the sessions whose journal a failure already printed, and
	// dumpRuntime the runtime session and evidence each is judged against.
	dumpMu      sync.Mutex
	dumped      map[sessionwire.SessionID]bool
	dumpRuntime map[sessionwire.SessionID]func() string

	t      *testing.T
	ctx    context.Context
	cfg    stressConfig
	world  *orchestrationtest.PooledWorld
	fleet  *stressFleet
	client *http.Client

	sessions []*stressSession

	epochMu sync.Mutex
	epochs  []*stressEpoch

	counters stressCounters

	// askPaused turns every ask into a plain input while a Host restart waits
	// for the fleet to have no gate open; asksOutstanding counts asks whose
	// gate is not yet answered or abandoned.
	askPaused       atomic.Bool
	asksOutstanding atomic.Int64
	// fenceRounds names each fence pass's gate answers uniquely.
	fenceRounds atomic.Int64

	// stopOps ends the operation phase; viewersFinal tells viewers to stop
	// cycling and hold one live connection.
	stopOps      chan struct{}
	viewersFinal chan struct{}
	stopViewers  chan struct{}

	capacity   uint64
	queueBound int
}

const (
	stressCapacityQueue = 32
	stressAskToken      = "STRESSASK"
)

// stressWord matches every token the driver puts into a conversation, and names
// the session that owns it: W<session>x<op>.
var stressWord = regexp.MustCompile(`W(\d+)x(\d+)`)

func TestFactoryHostRaceStress(t *testing.T) {
	if testing.Short() {
		t.Skip("the race stress runs real Factories, Hosts and harness runtimes for minutes")
	}
	cfg := stressConfigFromEnv(t)
	t.Logf("I3.1 stress: seed=%d sessions=%d hosts=%d replicas=%d ops=%d viewers/session=%d warmTTL=%s (reproduce the workload with LOOPRIG_STRESS_SEED=%d)",
		cfg.seed, cfg.sessions, cfg.hosts, cfg.replicas, cfg.opsPerSession, cfg.viewersPerSession, cfg.warmTTL, cfg.seed)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{WithAskTool: true})
	world.AskTool.Question = "orchestrationtest stress: answer me"
	world.LLM.Respond(stressRespond)

	// THE BASELINE is taken with the durable plane open and nothing serving,
	// so what L1 compares is exactly what the Factories, Hosts, runtimes and
	// clients started.
	baseline, baselineEagles := runtime.NumGoroutine(), stressEagles()

	// Total capacity is three quarters of the sessions, so placement contends
	// and warm release has to free room for the next command.
	capacity := uint64((cfg.sessions*3/4 + cfg.hosts - 1) / cfg.hosts)
	if capacity < 4 {
		capacity = 4
	}
	s := &stress{
		t: t, ctx: ctx, cfg: cfg, world: world,
		fleet:        &stressFleet{},
		client:       &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}},
		stopOps:      make(chan struct{}),
		viewersFinal: make(chan struct{}),
		stopViewers:  make(chan struct{}),
		capacity:     capacity,
		queueBound:   stressCapacityQueue,
	}
	for i := range cfg.hosts {
		slot := &stressHost{id: sessionwire.HostID(fmt.Sprintf("stress-host-%d", i)), generation: 1}
		slot.host = s.startHost(slot)
		s.fleet.hosts = append(s.fleet.hosts, slot)
	}
	for _, slot := range s.fleet.hosts {
		orchestrationtest.AwaitAdvertised(t, world, slot.id)
	}
	for range cfg.replicas {
		s.fleet.replicas = append(s.fleet.replicas, s.startReplica())
	}

	tenants := []sessionwire.TenantID{orchestrationtest.PooledTenantA, orchestrationtest.PooledTenantB}
	for i := range cfg.sessions {
		s.sessions = append(s.sessions, &stressSession{
			index:  i,
			tenant: tenants[i%len(tenants)],
			id:     sessionwire.SessionID(fmt.Sprintf("stress-%d-%d", cfg.seed%100000, i)),
		})
	}

	// ---- the operation phase: sessions and viewers, with chaos -------------
	metricsDone := make(chan struct{})
	stopMetrics := make(chan struct{})
	go func() {
		defer close(metricsDone)
		s.sampleMetrics(stopMetrics)
	}()

	var viewers sync.WaitGroup
	for _, session := range s.sessions {
		for v := range cfg.viewersPerSession {
			viewers.Add(1)
			go func() {
				defer viewers.Done()
				s.runViewer(session, rand.New(rand.NewPCG(cfg.seed, uint64(1_000_000+session.index*16+v))))
			}()
		}
	}
	var workers sync.WaitGroup
	for _, session := range s.sessions {
		workers.Add(1)
		go func() {
			defer workers.Done()
			s.runSession(session, rand.New(rand.NewPCG(cfg.seed, uint64(session.index))))
		}()
	}
	workersDone := make(chan struct{})
	go func() {
		workers.Wait()
		close(workersDone)
	}()

	// THE CHAOS LOOP RUNS ON THE TEST GOROUTINE, because restarting a Host or
	// a Factory goes through kit constructors that call Fatalf, which is only
	// sound here.
	chaos := rand.New(rand.NewPCG(cfg.seed, 7))
	phaseStart := time.Now()
	for done := false; !done; {
		select {
		case <-workersDone:
			done = true
		case <-ctx.Done():
			t.Fatalf("the operation phase outlived the case bound")
		case <-time.After(cfg.chaosEvery/2 + time.Duration(chaos.Int64N(int64(cfg.chaosEvery)))):
			s.chaosEvent(chaos)
		}
	}
	close(s.stopOps)
	t.Logf("I3.1 the operation phase took %v", time.Since(phaseStart).Round(time.Millisecond))

	// ---- the final phase: hold viewers, flush every session, settle --------
	close(s.viewersFinal)
	s.awaitFinalViewers()
	s.drainGates("drain")
	s.flush()
	s.awaitSettled()
	s.drainGates("final")
	s.verifyCommands()
	s.verifyConversations()
	s.verifyViewers()
	close(stopMetrics)
	<-metricsDone
	s.verifyHostsUnblocked()

	// ---- shutdown and L1 ------------------------------------------------------
	close(s.stopViewers)
	viewers.Wait()
	s.shutdown()
	s.logCounters()
	s.assertGoroutinesSettle(baseline, baselineEagles)
}

// ---- construction ------------------------------------------------------------

func (s *stress) startHost(slot *stressHost) *orchestrationtest.PooledHost {
	h := s.startSizedHost(slot)
	s.fleet.mu.Lock()
	s.fleet.every = append(s.fleet.every, h)
	s.fleet.mu.Unlock()
	return h
}

func (s *stress) startSizedHost(slot *stressHost) *orchestrationtest.PooledHost {
	return orchestrationtest.StartSizedPooledHost(s.t, s.ctx, s.world, orchestrationtest.PooledHostConfig{
		ID:                 slot.id,
		Generation:         slot.generation,
		Capacity:           s.capacity,
		WarmTTL:            s.cfg.warmTTL,
		MaxBindingsPerLink: int(s.capacity) * 2,
		MaxBindings:        int(s.capacity) * 4,
		CommandQueueSize:   s.queueBound,
	})
}

func (s *stress) startReplica() *stressReplica {
	s.fleet.mu.Lock()
	s.fleet.started++
	name := fmt.Sprintf("stress-replica-%d", s.fleet.started)
	s.fleet.mu.Unlock()
	var logs io.Writer
	if dir := os.Getenv("LOOPRIG_STRESS_LOGDIR"); dir != "" {
		file, err := os.Create(dir + "/" + name + ".jsonl")
		if err != nil {
			s.t.Fatalf("opening %s's log: %v", name, err)
		}
		s.t.Cleanup(func() { _ = file.Close() })
		logs = file
	}
	f := orchestrationtest.StartPooledFactoryWith(s.t, s.ctx, s.world, orchestrationtest.PooledFactoryConfig{Replica: name, Logs: logs})
	return &stressReplica{name: name, f: f, dead: make(chan struct{})}
}

// ---- the model ---------------------------------------------------------------

// stressRespond chooses a turn from the request alone: a tool result is
// answered with text; a user message carrying the ask token is answered with
// a call to the tool that raises an ask_user gate; anything else is text.
func stressRespond(request inference.Request) orchestrationtest.PooledTurn {
	for i := len(request.Messages) - 1; i >= 0; i-- {
		switch message := request.Messages[i].(type) {
		case *content.ToolResultMessage:
			return orchestrationtest.PooledTurn{Text: "thanks"}
		case *content.UserMessage:
			for _, block := range message.Blocks {
				if text, ok := block.(*content.TextBlock); ok && strings.Contains(text.Text, stressAskToken) {
					return orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledAskToolName, ToolInput: `{}`}
				}
			}
			return orchestrationtest.PooledTurn{Text: "ok"}
		}
	}
	return orchestrationtest.PooledTurn{Text: "ok"}
}

// ---- HTTP --------------------------------------------------------------------

func (s *stress) do(ctx context.Context, method string, r *stressReplica, tenant sessionwire.TenantID, path string, body any) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = strings.NewReader(string(encoded))
	}
	request, err := http.NewRequestWithContext(ctx, method, r.f.BaseURL+path, reader)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+orchestrationtest.PooledBearers[tenant])
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := s.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(response.Body)
	return response.StatusCode, answer, err
}

// send delivers one command through randomly chosen replicas until a replica
// answers it definitively, RETRYING THE SAME CommandID on a transport failure
// or a retryable status -- which is what a browser whose Factory died does.
// Once acknowledged, it is sometimes replayed through another replica on
// purpose: C2's evidence.
func (s *stress) send(rng *rand.Rand, c *stressCommand, path string, body any) bool {
	s.counters.commandsSent.Add(1)
	deadline := time.Now().Add(90 * time.Second)
	for attempt := 0; ; attempt++ {
		if time.Now().After(deadline) || s.ctx.Err() != nil {
			c.mu.Lock()
			c.refusals = append(c.refusals, "gave up: no definitive answer within 90s")
			c.mu.Unlock()
			return false
		}
		if attempt > 0 {
			s.counters.retries.Add(1)
			time.Sleep(time.Duration(50+rng.IntN(250)) * time.Millisecond)
		}
		r := s.fleet.pick(rng)
		status, answer, err := s.do(s.ctx, http.MethodPost, r, c.tenant, path, body)
		switch {
		case err != nil || status == http.StatusServiceUnavailable || status == http.StatusTooManyRequests ||
			status == http.StatusBadGateway || status == http.StatusGatewayTimeout:
			c.mu.Lock()
			c.transient++
			c.mu.Unlock()
			continue
		case status >= 200 && status < 300:
			c.mu.Lock()
			c.acks = append(c.acks, stressAck{replica: r.name, status: status, order: acceptedOrder(answer)})
			c.mu.Unlock()
			s.counters.commandsAcked.Add(1)
			if rng.IntN(5) == 0 {
				s.replay(rng, c, path, body)
			}
			return true
		default:
			c.mu.Lock()
			c.refusals = append(c.refusals, fmt.Sprintf("%s answered %d: %s", r.name, status, truncateBody(answer)))
			c.mu.Unlock()
			s.counters.refusals.Add(1)
			return false
		}
	}
}

// replay re-sends an acknowledged command, byte for byte, through a replica.
func (s *stress) replay(rng *rand.Rand, c *stressCommand, path string, body any) {
	r := s.fleet.pick(rng)
	status, answer, err := s.do(s.ctx, http.MethodPost, r, c.tenant, path, body)
	if err != nil || status == http.StatusServiceUnavailable {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if status >= 200 && status < 300 {
		c.acks = append(c.acks, stressAck{replica: r.name, status: status, order: acceptedOrder(answer)})
		s.counters.duplicateAcks.Add(1)
		return
	}
	// A replay of an acknowledged command must never be refused: the
	// CommandID and the bytes are the ones the store already holds.
	c.refusals = append(c.refusals, fmt.Sprintf("REPLAY via %s answered %d: %s", r.name, status, truncateBody(answer)))
}

func acceptedOrder(body []byte) uint64 {
	var shape struct {
		AcceptedOrder uint64 `json:"accepted_order"`
	}
	_ = json.Unmarshal(body, &shape)
	return shape.AcceptedOrder
}

func truncateBody(body []byte) string {
	text := strings.TrimSpace(string(body))
	if len(text) > 240 {
		return text[:240] + "..."
	}
	return text
}

func (s *stress) sleep(rng *rand.Rand, low, high time.Duration) bool {
	select {
	case <-time.After(low + time.Duration(rng.Int64N(int64(high-low)+1))):
		return true
	case <-s.ctx.Done():
		return false
	}
}

// ---- the session worker --------------------------------------------------------

func (s *stress) runSession(session *stressSession, rng *rand.Rand) {
	// Stagger the creates so placement sees a stream rather than one burst.
	if !s.sleep(rng, 0, 3*time.Second) {
		return
	}
	create := &stressCommand{
		tenant: session.tenant, session: session.id, kind: orchestrationtest.PooledKindCreate,
		id: sessionwire.CommandID(fmt.Sprintf("create-%d", session.index)),
	}
	request := sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(create.id)),
		SessionID:       session.id,
		AgentID:         orchestrationtest.PooledAgent,
	}
	if rng.IntN(2) == 0 {
		create.word = fmt.Sprintf("W%dx0", session.index)
		request.Blocks = stressBlocks("the first message is " + create.word)
	}
	session.add(create)
	if !s.send(rng, create, "/v1/sessions", request) {
		s.t.Errorf("session %s: its create was never acknowledged: %v", session.id, create.refusals)
		return
	}
	session.mu.Lock()
	session.created = true
	session.mu.Unlock()

	for op := 1; op <= s.cfg.opsPerSession; op++ {
		if !s.sleep(rng, 0, 300*time.Millisecond) {
			return
		}
		if rng.IntN(3) == 0 {
			s.answerGates(rng, session, fmt.Sprintf("pre%d", op), 0)
		}
		switch roll := rng.IntN(100); {
		case roll < 45:
			s.input(rng, session, op, false)
		case roll < 60:
			s.input(rng, session, op, true)
		case roll < 72:
			s.interrupt(rng, session, op)
		case roll < 78:
			s.restore(rng, session, op)
		case roll < 90:
			s.coldView(rng, session)
		default:
			// Idle long enough for the Host to warm-release the session, so
			// the next command has to re-place and RESTORE it.
			s.sleep(rng, s.cfg.warmTTL, 2*s.cfg.warmTTL+time.Second)
		}
	}
}

func stressBlocks(text string) json.RawMessage {
	encoded, _ := json.Marshal([]map[string]string{{"type": "text", "text": text}})
	return encoded
}

func (s *stress) input(rng *rand.Rand, session *stressSession, op int, ask bool) {
	if ask && s.askPaused.Load() {
		ask = false
	}
	c := &stressCommand{
		tenant: session.tenant, session: session.id, kind: orchestrationtest.PooledKindInput,
		id:   sessionwire.CommandID(fmt.Sprintf("input-%d-%d", session.index, op)),
		word: fmt.Sprintf("W%dx%d", session.index, op),
	}
	text := "please consider " + c.word
	if ask {
		text = stressAskToken + " " + text
		s.asksOutstanding.Add(1)
		defer s.asksOutstanding.Add(-1)
	}
	session.add(c)
	if !s.send(rng, c, "/v1/sessions/"+string(session.id)+"/input", sessionwire.InputRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(c.id)),
		SessionID:       session.id,
		Blocks:          stressBlocks(text),
	}) || !ask {
		return
	}
	if s.answerGates(rng, session, fmt.Sprintf("op%d", op), 30*time.Second) == 0 {
		s.counters.gatesMissed.Add(1)
	}
}

// answerGates reads the session's open gates through a random replica --
// waiting up to wait for at least one -- and answers every resident gate it
// sees, each under its own CommandID. It returns how many it answered.
//
// A gate is not tied to the ask that raised it: an ask folded into a turn that
// is already parked raises its gate only after the first one is answered, so
// answering "the gate my ask raised" leaves the second parked forever and every
// input behind it queued. Answering whatever is open is what a user does.
//
// A gate that never appears -- the ask was folded or interrupted before the
// tool ran -- is abandoned and counted; that is not a defect. A stale
// projection answered 409 gate_resolved is likewise the correct refusal.
func (s *stress) answerGates(rng *rand.Rand, session *stressSession, tag string, wait time.Duration) int {
	deadline := time.Now().Add(wait)
	var open []sessionwire.GateProjection
	for {
		if s.ctx.Err() != nil {
			return 0
		}
		r := s.fleet.pick(rng)
		status, body, err := s.do(s.ctx, http.MethodGet, r, session.tenant, "/v1/sessions/"+string(session.id)+"/gates", nil)
		if err == nil && status == http.StatusOK {
			var page sessionwire.GatePage
			if err := page.UnmarshalJSON(body); err != nil {
				s.t.Errorf("session %s: the gates page from %s is not a Core GatePage: %v", session.id, r.name, err)
				return 0
			}
			open = open[:0]
			for _, projected := range page.Gates {
				if projected.Answerability == sessionwire.GateAnswerabilityResident {
					open = append(open, projected)
				}
			}
		} else if err == nil && status != http.StatusServiceUnavailable {
			s.t.Errorf("session %s: the gates read via %s answered %d: %s", session.id, r.name, status, truncateBody(body))
			return 0
		}
		if len(open) > 0 || !time.Now().Add(150*time.Millisecond).Before(deadline) {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	answered := 0
	for n, projected := range open {
		s.counters.gatesSeen.Add(1)
		c := &stressCommand{
			tenant: session.tenant, session: session.id, kind: orchestrationtest.PooledKindGateResponse,
			id:     sessionwire.CommandID(fmt.Sprintf("gate-%d-%s-%d", session.index, tag, n)),
			answer: fmt.Sprintf("A%dx%sx%d", session.index, tag, n),
		}
		session.add(c)
		if s.send(rng, c, "/v1/sessions/"+string(session.id)+"/gates/"+string(projected.GateID), sessionwire.GateResponseRequest{
			CommandEnvelope:        orchestrationtest.PooledEnvelope(string(c.id)),
			SessionID:              session.id,
			GateID:                 projected.GateID,
			Action:                 "answer",
			Values:                 map[string]json.RawMessage{"answer": json.RawMessage(`"` + c.answer + `"`)},
			ExpectedOpenJournalSeq: projected.OpenedJournalSeq,
		}) {
			s.counters.gatesAnswered.Add(1)
			answered++
		}
	}
	return answered
}

// drainGates answers every gate in the fleet until none has been open for
// several consecutive reads of every session. It is what makes C3's "an
// applied input resolved" a fair claim: an input queued behind a parked gate
// is durably accepted and waiting, not dropped.
func (s *stress) drainGates(round string) {
	deadline := time.Now().Add(s.cfg.settle)
	var wg sync.WaitGroup
	for _, session := range s.sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(s.cfg.seed, uint64(3_000_000+session.index)))
			quiet := 0
			for n := 0; quiet < 3 && time.Now().Before(deadline) && s.ctx.Err() == nil; n++ {
				if s.answerGates(rng, session, fmt.Sprintf("%s%d", round, n), 0) == 0 {
					quiet++
				} else {
					quiet = 0
				}
				time.Sleep(700 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	for time.Now().Before(deadline) && s.gatesWaiting() != 0 {
		time.Sleep(200 * time.Millisecond)
	}
	if waiting := s.gatesWaiting(); waiting != 0 {
		s.t.Errorf("drain %s: %.0f sessions still report a gate waiting after %s", round, waiting, s.cfg.settle)
	}
}

func (s *stress) interrupt(rng *rand.Rand, session *stressSession, op int) {
	c := &stressCommand{
		tenant: session.tenant, session: session.id, kind: orchestrationtest.PooledKindInterrupt,
		id: sessionwire.CommandID(fmt.Sprintf("interrupt-%d-%d", session.index, op)),
	}
	session.add(c)
	s.send(rng, c, "/v1/sessions/"+string(session.id)+"/interrupt", sessionwire.InterruptRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(c.id)),
		SessionID:       session.id,
	})
}

func (s *stress) restore(rng *rand.Rand, session *stressSession, op int) {
	c := &stressCommand{
		tenant: session.tenant, session: session.id, kind: orchestrationtest.PooledKindRestore,
		id: sessionwire.CommandID(fmt.Sprintf("restore-%d-%d", session.index, op)),
	}
	session.add(c)
	s.send(rng, c, "/v1/sessions/"+string(session.id)+"/restore", sessionwire.RestoreRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(c.id)),
		SessionID:       session.id,
	})
}

// coldView reads a session the way a browser opening it does, through any
// replica, whether or not any Host holds it. Every read is served from the
// durable plane, so a refusal is a defect.
func (s *stress) coldView(rng *rand.Rand, session *stressSession) {
	s.counters.coldReads.Add(1)
	r := s.fleet.pick(rng)
	for _, suffix := range []string{"/status", "/journal", "/gates"} {
		status, body, err := s.do(s.ctx, http.MethodGet, r, session.tenant, "/v1/sessions/"+string(session.id)+suffix, nil)
		if err != nil {
			// The replica may have been restarted under the read.
			select {
			case <-r.dead:
				return
			default:
			}
			s.t.Errorf("cold view %s%s via %s: %v", session.id, suffix, r.name, err)
			return
		}
		if status != http.StatusOK {
			s.t.Errorf("cold view %s%s via %s answered %d: %s", session.id, suffix, r.name, status, truncateBody(body))
		}
	}
}

// ---- viewers -----------------------------------------------------------------

// runViewer is one browser tab: it connects to a replica, subscribes, learns
// its position from a journal read, watches for a while, and leaves -- over and
// over, until the final phase, when it holds one live connection.
func (s *stress) runViewer(session *stressSession, rng *rand.Rand) {
	var current *stressEpoch
	defer func() {
		if current != nil {
			current.viewer.Close()
		}
	}()
	for {
		final := false
		select {
		case <-s.stopViewers:
			return
		case <-s.viewersFinal:
			final = true
		default:
		}
		if current != nil {
			select {
			case <-current.replica.dead:
				current.viewer.Close()
				current = nil
			default:
			}
		}
		if final && current != nil {
			select {
			case <-s.stopViewers:
				return
			case <-current.replica.dead:
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		if current != nil {
			current.viewer.Close()
			current = nil
		}
		// Do not subscribe to a session that does not exist yet: a subscribe
		// to an unknown session is a legitimate refusal and not the case.
		session.mu.Lock()
		created := session.created
		session.mu.Unlock()
		if !created {
			if !s.sleep(rng, 100*time.Millisecond, 400*time.Millisecond) {
				return
			}
			continue
		}
		current = s.openEpoch(session, rng)
		if current == nil {
			if !s.sleep(rng, 100*time.Millisecond, 500*time.Millisecond) {
				return
			}
			continue
		}
		if final {
			continue
		}
		select {
		case <-s.stopViewers:
			return
		case <-s.viewersFinal:
		case <-current.replica.dead:
		case <-time.After(time.Duration(500+rng.IntN(6000)) * time.Millisecond):
		}
	}
}

func (s *stress) openEpoch(session *stressSession, rng *rand.Rand) *stressEpoch {
	r := s.fleet.pick(rng)
	dialCtx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
	defer cancel()
	viewer, err := orchestrationtest.DialPooledViewer(dialCtx, r.f, session.tenant)
	if err != nil {
		s.counters.viewerDialFailures.Add(1)
		return nil
	}
	if err := viewer.Subscribe(dialCtx, session.tenant, session.id); err != nil {
		viewer.Close()
		s.counters.subscribeFailures.Add(1)
		return nil
	}
	subscribed := time.Now().Format("15:04:05.000000")
	// SUBSCRIBE, THEN READ THE POSITION. Reading first would leave a window in
	// which a committed event is in neither the read nor the live tail.
	status, body, err := s.do(dialCtx, http.MethodGet, r, session.tenant, "/v1/sessions/"+string(session.id)+"/journal", nil)
	if err != nil || status != http.StatusOK {
		viewer.Close()
		s.counters.subscribeFailures.Add(1)
		return nil
	}
	var page sessionwire.JournalPage
	if err := json.Unmarshal(body, &page); err != nil {
		viewer.Close()
		s.t.Errorf("session %s: the journal page from %s is not a Core JournalPage: %v", session.id, r.name, err)
		return nil
	}
	epoch := &stressEpoch{session: session, replica: r, viewer: viewer, start: page.CapturedTip,
		subscribed: subscribed, read: time.Now().Format("15:04:05.000000")}
	s.epochMu.Lock()
	s.epochs = append(s.epochs, epoch)
	s.epochMu.Unlock()
	s.counters.viewerEpochs.Add(1)
	return epoch
}

// ---- chaos -------------------------------------------------------------------

func (s *stress) chaosEvent(rng *rand.Rand) {
	switch roll := rng.IntN(100); {
	case roll < 30:
		s.fleet.mu.RLock()
		slot := s.fleet.hosts[rng.IntN(len(s.fleet.hosts))]
		s.fleet.mu.RUnlock()
		n := slot.host.Sever()
		s.counters.severs.Add(1)
		s.t.Logf("chaos: severed %d HostLink connections to %s", n, slot.id)
	case roll < 60:
		s.restartFactory(rng)
	case roll < 85:
		s.restartHost(rng)
	default:
		s.probeCrossTenant(rng)
	}
}

func (s *stress) restartFactory(rng *rand.Rand) {
	s.fleet.mu.Lock()
	i := rng.IntN(len(s.fleet.replicas))
	victim := s.fleet.replicas[i]
	s.fleet.replicas = append(s.fleet.replicas[:i:i], s.fleet.replicas[i+1:]...)
	s.fleet.retired = append(s.fleet.retired, victim)
	s.fleet.mu.Unlock()
	close(victim.dead)
	victim.f.Stop()
	replacement := s.startReplica()
	s.fleet.mu.Lock()
	s.fleet.replicas = append(s.fleet.replicas, replacement)
	s.fleet.mu.Unlock()
	s.counters.factoryRestarts.Add(1)
	s.t.Logf("chaos: restarted Factory %s as %s", victim.name, replacement.name)
}

// restartHost drains a Host and starts its successor, at a higher generation,
// under the same HostID -- but only with NO gate open anywhere. See the file
// comment for why that is the documented obligation and not a dodge.
//
// The tool is FENCED first, so no new gate can open, and every open gate is
// answered until none of the tool's invocations is still parked. A fence that
// only watched a metric would race an ask already on its way into the tool,
// and one missed parked gate turns the drain crash-equivalent: its runtime
// keeps its journal lease and the session is wedged for the rest of the case.
func (s *stress) restartHost(rng *rand.Rand) {
	s.askPaused.Store(true)
	s.world.AskTool.Fence(true)
	defer func() {
		s.world.AskTool.Fence(false)
		s.askPaused.Store(false)
	}()
	deadline := time.Now().Add(30 * time.Second)
	for s.world.AskTool.Active() != 0 || s.gatesWaiting() != 0 {
		if time.Now().After(deadline) {
			s.counters.hostRestartsSkipped.Add(1)
			s.t.Logf("chaos: skipped a Host restart; %d gates stayed parked for 30s", s.world.AskTool.Active())
			return
		}
		round := fmt.Sprintf("fence%d", s.fenceRounds.Add(1))
		var wg sync.WaitGroup
		for _, session := range s.sessions {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.answerGates(rand.New(rand.NewPCG(s.cfg.seed, uint64(4_000_000+session.index))), session, round, 0)
			}()
		}
		wg.Wait()
		time.Sleep(200 * time.Millisecond)
	}
	s.fleet.mu.RLock()
	slot := s.fleet.hosts[rng.IntN(len(s.fleet.hosts))]
	s.fleet.mu.RUnlock()
	took := time.Now()
	resident := 0.0
	if samples, err := slot.host.ScrapeMetrics(); err == nil {
		resident = orchestrationtest.SumMetric(samples, "host_sessions")
	}
	slot.host.Stop()
	s.fleet.mu.Lock()
	slot.generation++
	s.fleet.mu.Unlock()
	next := s.startHost(slot)
	s.fleet.mu.Lock()
	slot.host = next
	s.fleet.mu.Unlock()
	orchestrationtest.AwaitAdvertised(s.t, s.world, slot.id)
	s.counters.hostRestarts.Add(1)
	s.t.Logf("chaos: restarted Host %s (holding %.0f sessions) at generation %d in %v", slot.id, resident, slot.generation, time.Since(took).Round(time.Millisecond))
}

func (s *stress) gatesWaiting() float64 {
	total := 0.0
	for _, h := range s.fleet.liveHosts() {
		samples, err := h.ScrapeMetrics()
		if err != nil {
			return 1
		}
		total += orchestrationtest.SumMetric(samples, "host_sessions_gate_waiting")
	}
	return total
}

// probeCrossTenant is V1's active half: a viewer of one tenant asks for a
// session of the other and must be refused and sent nothing.
func (s *stress) probeCrossTenant(rng *rand.Rand) {
	target := s.sessions[rng.IntN(len(s.sessions))]
	other := orchestrationtest.PooledTenantA
	if target.tenant == other {
		other = orchestrationtest.PooledTenantB
	}
	r := s.fleet.pick(rng)
	ctx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
	defer cancel()
	intruder, err := orchestrationtest.DialPooledViewer(ctx, r.f, other)
	if err != nil {
		return
	}
	defer intruder.Close()
	s.counters.crossTenantProbes.Add(1)
	if err := intruder.Subscribe(ctx, target.tenant, target.id); err == nil {
		s.t.Errorf("V1: a %s viewer on %s was allowed to subscribe to %s/%s", other, r.name, target.tenant, target.id)
	}
	time.Sleep(300 * time.Millisecond)
	if got := intruder.Records(); len(got) != 0 {
		s.t.Errorf("V1: the refused cross-tenant viewer still received %v", got)
	}
}

// ---- Q1: queue bounds ----------------------------------------------------------

func (s *stress) sampleMetrics(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-time.After(200 * time.Millisecond):
		}
		for _, h := range s.fleet.liveHosts() {
			samples, err := h.ScrapeMetrics()
			if err != nil {
				continue
			}
			s.counters.metricSamples.Add(1)
			if held := orchestrationtest.SumMetric(samples, "host_sessions"); held > float64(s.capacity) {
				s.t.Errorf("Q1: host %s holds %.0f sessions, capacity %d: %v", h.ID, held, s.capacity, stressFamily(samples, "host_sessions"))
			}
			if depth := orchestrationtest.SumMetric(samples, "host_command_queue_depth"); depth > float64(s.queueBound) {
				s.t.Errorf("Q1: host %s command queue depth %.0f exceeds its bound %d", h.ID, depth, s.queueBound)
			}
		}
	}
}

func stressFamily(samples map[string]float64, name string) map[string]float64 {
	out := map[string]float64{}
	for key, value := range samples {
		if key == name || strings.HasPrefix(key, name+"{") {
			out[key] = value
		}
	}
	return out
}

func (s *stress) verifyHostsUnblocked() {
	for _, h := range s.fleet.liveHosts() {
		samples, err := h.ScrapeMetrics()
		if err != nil {
			s.t.Errorf("Q1: scraping host %s: %v", h.ID, err)
			continue
		}
		if blocked := orchestrationtest.SumMetric(samples, "host_sessions_command_blocked"); blocked != 0 {
			s.t.Errorf("Q1: host %s reports %.0f command-blocked sessions after the settle: a wedged command stream", h.ID, blocked)
		}
		s.t.Logf("Q1 host %s at the end: sessions=%v queue=%v release_failures=%v", h.ID,
			stressFamily(samples, "host_sessions"), stressFamily(samples, "host_command_queue_depth"),
			stressFamily(samples, "host_release_failures_total"))
	}
}

// ---- the final phase -----------------------------------------------------------

func (s *stress) awaitFinalViewers() {
	if s.cfg.viewersPerSession == 0 {
		return
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		live := map[sessionwire.SessionID]int{}
		for _, epoch := range s.finalEpochs() {
			live[epoch.session.id]++
		}
		missing := 0
		for _, session := range s.sessions {
			if live[session.id] < s.cfg.viewersPerSession {
				missing++
			}
		}
		if missing == 0 {
			return
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("V3: %d sessions still lack their final viewers after 60s", missing)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// finalEpochs is every epoch still attached to a live replica with an open
// connection: the LAST epoch of each viewer, on a live replica.
func (s *stress) finalEpochs() []*stressEpoch {
	s.epochMu.Lock()
	defer s.epochMu.Unlock()
	// Every epoch owns its own client and a viewer closes its client when it
	// leaves, so the open epochs on live replicas are exactly the connections
	// the viewers hold now.
	var out []*stressEpoch
	for _, epoch := range s.epochs {
		select {
		case <-epoch.replica.dead:
			continue
		default:
		}
		if epoch.viewer.Connected() {
			out = append(out, epoch)
		}
	}
	return out
}

// flush sends every session one last plain input, so every final viewer sees
// live output it subscribed before, and V3 has a tip to be covered through.
func (s *stress) flush() {
	var wg sync.WaitGroup
	for _, session := range s.sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(s.cfg.seed, uint64(2_000_000+session.index)))
			c := &stressCommand{
				tenant: session.tenant, session: session.id, kind: orchestrationtest.PooledKindInput,
				id:   sessionwire.CommandID(fmt.Sprintf("input-%d-final", session.index)),
				word: fmt.Sprintf("W%dx%d", session.index, s.cfg.opsPerSession+1),
			}
			session.add(c)
			if s.send(rng, c, "/v1/sessions/"+string(session.id)+"/input", sessionwire.InputRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope(string(c.id)),
				SessionID:       session.id,
				Blocks:          stressBlocks("finally " + c.word),
			}) {
				session.mu.Lock()
				session.final = c
				session.mu.Unlock()
			}
		}()
	}
	wg.Wait()
}

// awaitSettled is C1: every acknowledged command reaches a terminal state.
func (s *stress) awaitSettled() {
	start := time.Now()
	deadline := start.Add(s.cfg.settle)
	for {
		var open []string
		for _, session := range s.sessions {
			for _, c := range session.snapshot() {
				if !c.acknowledged() {
					continue
				}
				state := s.world.CommandState(s.ctx, c.tenant, c.session, c.id)
				if state != sessionstore.InboxStateApplied && state != sessionstore.InboxStateRejected {
					open = append(open, fmt.Sprintf("%s/%s=%q", c.session, c.id, state))
				}
			}
		}
		if len(open) == 0 {
			s.t.Logf("C1 every acknowledged command settled within %v of the operation phase ending", time.Since(start).Round(time.Millisecond))
			return
		}
		if time.Now().After(deadline) {
			sort.Strings(open)
			s.t.Errorf("C1: %d acknowledged commands never reached a terminal state within %s: %v", len(open), s.cfg.settle, open)
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// ---- verification: commands ------------------------------------------------------

func (s *stress) verifyCommands() {
	byOutcome := map[string]int{}
	// ONE deadline for every session's effects, so a run with several failing
	// sessions fails in a minute rather than a minute each.
	deadline := time.Now().Add(60 * time.Second)
	for _, session := range s.sessions {
		commands := session.snapshot()
		if len(commands) == 0 {
			continue
		}
		session.mu.Lock()
		created := session.created
		session.mu.Unlock()
		if !created {
			continue
		}
		runtimeID := s.world.RuntimeSessionID(s.t, s.ctx, session.tenant, session.id)
		// Poll the journal until every applied command's effect is durable:
		// harness writes the applied disposition BEFORE the effect, so one read
		// right after settlement races the append (see assertAppliedExactlyOnce).
		var evidence orchestrationtest.CommandEvidence
		entries := map[sessionwire.CommandID]sessionstore.DispositionInboxEntry{}
		for {
			evidence = orchestrationtest.ReadCommandEvidence(s.t, s.world, session.tenant, runtimeID)
			pending := 0
			for _, c := range commands {
				if !c.acknowledged() || c.word == "" {
					continue
				}
				entry, err := s.world.Store.GetDispositionCommand(s.ctx, sessionstore.GetDispositionCommandRequest{
					TenantID: c.tenant, SessionID: c.session, CommandID: c.id,
				})
				if err != nil || entry.Record.Outcome == nil || entry.Record.Outcome.Kind != sessionstore.DispositionApplied {
					continue
				}
				rc, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID))
				if err != nil {
					continue
				}
				if evidence.EffectsOf(rc)+stressBool(evidence.RejectedOf(rc))+stressBool(evidence.CancelledOf(rc)) == 0 {
					pending++
				}
			}
			if pending == 0 || time.Now().After(deadline) {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}

		s.dumpMu.Lock()
		if s.dumped == nil {
			s.dumped, s.dumpRuntime = map[sessionwire.SessionID]bool{}, map[sessionwire.SessionID]func() string{}
		}
		tenant, final := session.tenant, evidence
		s.dumpRuntime[session.id] = func() string { return s.stressDumpJournal(tenant, runtimeID, final) }
		s.dumpMu.Unlock()
		for _, c := range commands {
			c.mu.Lock()
			acks, refusals := append([]stressAck(nil), c.acks...), append([]string(nil), c.refusals...)
			c.mu.Unlock()
			for _, refusal := range refusals {
				if strings.HasPrefix(refusal, "REPLAY") {
					s.t.Errorf("C2: %s/%s: %s", c.session, c.id, refusal)
				}
			}
			if len(acks) == 0 {
				byOutcome["unacknowledged:"+c.kind]++
				continue
			}
			entry, err := s.world.Store.GetDispositionCommand(s.ctx, sessionstore.GetDispositionCommandRequest{
				TenantID: c.tenant, SessionID: c.session, CommandID: c.id,
			})
			if err != nil {
				s.t.Errorf("C1: acknowledged %s/%s has no durable record: %v", c.session, c.id, err)
				continue
			}
			entries[c.id] = entry
			// C2: one record, one order, whichever replica answered.
			for _, ack := range acks {
				if ack.order != 0 && ack.order != entry.AcceptedOrder {
					s.t.Errorf("C2: %s/%s was answered order %d by %s; the record holds %d (acks %+v)",
						c.session, c.id, ack.order, ack.replica, entry.AcceptedOrder, acks)
				}
			}
			outcome := string(entry.Record.State)
			if entry.Record.Outcome != nil {
				outcome += "/" + string(entry.Record.Outcome.Kind)
			}
			byOutcome[c.kind+":"+outcome]++
			s.verifyEvidence(c, entry, evidence)
		}
		s.verifyOrder(session, entries, evidence)
		s.verifyGates(session, commands, entries, runtimeID)
	}
	keys := make([]string, 0, len(byOutcome))
	for key := range byOutcome {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, byOutcome[key]))
	}
	s.t.Logf("C1 outcomes: %s", strings.Join(parts, " "))
}

func (s *stress) dumpOnce(c *stressCommand) {
	s.dumpMu.Lock()
	defer s.dumpMu.Unlock()
	if s.dumped[c.session] || s.dumpRuntime[c.session] == nil {
		return
	}
	s.dumped[c.session] = true
	s.t.Logf("C3 evidence for %s:\n%s", c.session, s.dumpRuntime[c.session]())
}

func stressBool(b bool) int {
	if b {
		return 1
	}
	return 0
}

// verifyEvidence is C3 for one command, against the runtime's own journal.
func (s *stress) verifyEvidence(c *stressCommand, entry sessionstore.DispositionInboxEntry, evidence orchestrationtest.CommandEvidence) {
	applications := evidence.ApplicationsOf(c.id)
	dispositions := evidence.DispositionsOf(c.id)
	if len(applications) > 1 {
		s.t.Errorf("C3: %s/%s has %d application prefixes in the journal, want at most 1: %+v", c.session, c.id, len(applications), applications)
	}
	if len(dispositions) > 1 {
		s.t.Errorf("C3: %s/%s has %d disposition frames in the journal, want at most 1: %+v", c.session, c.id, len(dispositions), dispositions)
	}
	if entry.Record.State == sessionstore.InboxStateRejected {
		// A rejection is pre-attempt or post-claim with no attempt: nothing
		// may have reached the runtime.
		if len(applications) != 0 {
			s.t.Errorf("C3: %s/%s settled REJECTED but the runtime journal holds its application prefix: %+v", c.session, c.id, applications)
		}
		return
	}
	outcome := entry.Record.Outcome
	if outcome == nil {
		s.t.Errorf("C1: %s/%s is %q with no outcome", c.session, c.id, entry.Record.State)
		return
	}
	// The store settles from the runtime's frame, so they must agree on
	// the kind -- except a not_applied the store wrote as a recovery closure
	// after a predecessor left no frame, which harness also records as a
	// frame (the closure). Either way there is exactly one account.
	if len(dispositions) != 1 {
		s.t.Errorf("C3: %s/%s settled %q with %d disposition frames in the journal, want exactly 1",
			c.session, c.id, outcome.Kind, len(dispositions))
		return
	}
	frame := dispositions[0].Disposition
	if string(frame.Disposition) != string(outcome.Kind) {
		s.t.Errorf("C3: %s/%s settled %q in the store, but the runtime's frame says %q", c.session, c.id, outcome.Kind, frame.Disposition)
	}
	if rc, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID)); err != nil || frame.RuntimeCommandID != rc {
		s.t.Errorf("C3: %s/%s: the frame names runtime command %s, the record %q", c.session, c.id, frame.RuntimeCommandID, entry.Record.Descriptor.RuntimeCommandID)
	}
	if outcome.Kind == sessionstore.DispositionNotApplied && len(applications) == 1 {
		// Legitimate only if nothing was caused by it; harness refuses a
		// closure over a committed effect, so an effect here is a defect.
		rc := frame.RuntimeCommandID
		if evidence.EffectsOf(rc) != 0 {
			s.t.Errorf("C3: %s/%s settled not_applied, but the journal holds %d effects it caused", c.session, c.id, evidence.EffectsOf(rc))
		}
	}
	if c.word != "" && outcome.Kind == sessionstore.DispositionApplied {
		rc := frame.RuntimeCommandID
		effects, rejected, cancelled := evidence.EffectsOf(rc), evidence.RejectedOf(rc), evidence.CancelledOf(rc)
		resolutions := effects + stressBool(rejected) + stressBool(cancelled)
		if resolutions == 0 {
			s.t.Errorf("C3: %s/%s (%s, runtime command %s) settled applied but caused no TurnStarted/TurnFoldedInto/TurnRejected/InputCancelled: its words were dropped", c.session, c.id, c.kind, rc)
			s.dumpOnce(c)
		}
		if effects > 1 {
			s.t.Errorf("C3: %s/%s (%s) caused %d turn effects, want exactly 1: applied twice", c.session, c.id, c.kind, effects)
		}
	}
}

// stressDumpJournal renders one runtime session's journal -- every record in
// ledger order, with the cause each event names -- so a C3 failure carries the
// evidence it was judged from.
func (s *stress) stressDumpJournal(tenant sessionwire.TenantID, runtimeID uuid.UUID, evidence orchestrationtest.CommandEvidence) string {
	var out strings.Builder
	for _, app := range evidence.Applications {
		fmt.Fprintf(&out, "  seq %d APPLICATION %s -> runtime %s kind=%s\n", app.Seq, app.Application.CommandID, app.Application.RuntimeCommandID, app.Application.Kind)
	}
	for _, d := range evidence.Dispositions {
		fmt.Fprintf(&out, "  seq %d DISPOSITION %s -> runtime %s %s\n", d.Seq, d.Disposition.CommandID, d.Disposition.RuntimeCommandID, d.Disposition.Disposition)
	}
	for i, ev := range orchestrationtest.JournalEvents[event.Event](s.t, s.world, tenant, runtimeID) {
		header := ev.EventHeader()
		cause := ""
		if !header.Cause.CommandID.IsZero() {
			cause = " cause=" + header.Cause.CommandID.String()
		}
		fmt.Fprintf(&out, "  event %d %T%s\n", i, ev, cause)
	}
	return out.String()
}

// verifyOrder is C4: the runtime's application prefixes, in ledger order, are
// in the store's acceptance order.
func (s *stress) verifyOrder(session *stressSession, entries map[sessionwire.CommandID]sessionstore.DispositionInboxEntry, evidence orchestrationtest.CommandEvidence) {
	var last uint64
	var lastID string
	for _, app := range evidence.Applications {
		entry, ok := entries[sessionwire.CommandID(app.Application.CommandID)]
		if !ok {
			continue
		}
		if entry.AcceptedOrder < last {
			s.t.Errorf("C4: session %s applied %q (order %d) after %q (order %d)",
				session.id, app.Application.CommandID, entry.AcceptedOrder, lastID, last)
		}
		last, lastID = entry.AcceptedOrder, string(app.Application.CommandID)
	}
}

// verifyGates is C6.
func (s *stress) verifyGates(session *stressSession, commands []*stressCommand, entries map[sessionwire.CommandID]sessionstore.DispositionInboxEntry, runtimeID uuid.UUID) {
	var answered []*stressCommand
	for _, c := range commands {
		entry, ok := entries[c.id]
		if c.kind != orchestrationtest.PooledKindGateResponse || !ok || entry.Record.Outcome == nil ||
			entry.Record.Outcome.Kind != sessionstore.DispositionApplied {
			continue
		}
		answered = append(answered, c)
	}
	if len(answered) == 0 {
		return
	}
	resolutions := orchestrationtest.JournalEvents[event.GateResolved](s.t, s.world, session.tenant, runtimeID)
	for _, c := range answered {
		rc, _ := uuid.Parse(string(entries[c.id].Record.Descriptor.RuntimeCommandID))
		count := 0
		for _, resolved := range resolutions {
			if resolved.Cause.CommandID == rc {
				count++
				if resolved.Source.Kind != gate.ResponseFromUser {
					s.t.Errorf("C6: %s/%s's gate was resolved by %q, want the user", c.session, c.id, resolved.Source.Kind)
				}
			}
		}
		if count != 1 {
			s.t.Errorf("C6: %s/%s settled applied and caused %d GateResolved events, want exactly 1", c.session, c.id, count)
		}
		found := false
		for _, answer := range s.world.AskTool.Answers() {
			if answer == c.answer {
				found = true
			}
		}
		if !found {
			s.t.Errorf("C6: %s/%s settled applied but its answer %q never reached the agent's tool", c.session, c.id, c.answer)
		}
	}
}

// verifyConversations is C5: every model request is ONE session's conversation.
func (s *stress) verifyConversations() {
	requests := s.world.LLM.Requests()
	mixed := 0
	for i, request := range requests {
		owners := map[string]bool{}
		for _, message := range request.Messages {
			user, ok := message.(*content.UserMessage)
			if !ok {
				continue
			}
			for _, block := range user.Blocks {
				text, ok := block.(*content.TextBlock)
				if !ok {
					continue
				}
				for _, match := range stressWord.FindAllStringSubmatch(text.Text, -1) {
					owners[match[1]] = true
				}
			}
		}
		if len(owners) > 1 {
			mixed++
			if mixed <= 5 {
				s.t.Errorf("C5: model request %d carries the words of %d sessions: %v", i, len(owners), owners)
			}
		}
	}
	if mixed > 5 {
		s.t.Errorf("C5: %d model requests in all mixed sessions' conversations", mixed)
	}
	s.t.Logf("C5 checked %d model requests", len(requests))
}

// ---- verification: viewers -------------------------------------------------------

func (s *stress) verifyViewers() {
	s.epochMu.Lock()
	epochs := append([]*stressEpoch(nil), s.epochs...)
	s.epochMu.Unlock()

	// The public positions of each session's runtime journal, read through
	// the resolver's own read: a skip over private-only positions is not a gap.
	public := map[sessionwire.SessionID][]uint64{}
	for _, session := range s.sessions {
		session.mu.Lock()
		created := session.created
		session.mu.Unlock()
		if created {
			public[session.id] = s.world.PublicJournalSeqs(s.t, s.ctx, session.tenant, session.id)
		}
	}

	// V1 and V2 over EVERY connection any viewer ever had.
	gaps := 0
	for _, epoch := range epochs {
		records := epoch.viewer.Records()
		if strays := epoch.viewer.Strays(); len(strays) != 0 {
			s.t.Errorf("V1: a viewer of %s/%s via %s received records naming another tenant or session: %v",
				epoch.session.tenant, epoch.session.id, epoch.replica.name, strays)
		}
		if _, err := orchestrationtest.PooledCoveredThroughPublic(records, epoch.start, public[epoch.session.id]); err != nil {
			gaps++
			if gaps <= 10 {
				s.t.Errorf("V2: a viewer of %s via %s (joined at %d) saw a silent gap: %v in %v",
					epoch.session.id, epoch.replica.name, epoch.start, err, records)
				s.t.Logf("V2 evidence for %s via %s: subscribed %s, journal read %s (tip %d)\n  viewer: %v\n  tail:\n    %s",
					epoch.session.id, epoch.replica.name, epoch.subscribed, epoch.read, epoch.start, epoch.viewer.Arrivals(),
					strings.Join(s.world.Tails.Timeline(epoch.session.tenant, epoch.session.id), "\n    "))
			}
		}
		seen := map[string]int{}
		for _, record := range records {
			if !strings.HasPrefix(record, "E") {
				continue
			}
			var seq uint64
			if _, err := fmt.Sscanf(record, "E%d", &seq); err == nil && seq > epoch.start {
				seen[record]++
			}
		}
		for record, count := range seen {
			if count > 1 {
				s.t.Errorf("V2: a viewer of %s via %s received the new sequence %s %d times: %v",
					epoch.session.id, epoch.replica.name, record, count, records)
			}
		}
	}
	if gaps > 10 {
		s.t.Errorf("V2: %d viewer connections in all saw a silent gap", gaps)
	}

	// V3: once repaired, every final viewer is covered through its session's
	// committed tip as it stood when the fleet went quiet -- except for ONE
	// record under a condition: a SessionResidencyReleased that is the
	// journal's last event, of which the viewer was told by a tip hint. See
	// observeTrailing.
	final := s.finalEpochs()
	// The target is the RUNTIME JOURNAL's last public position, read through
	// the same resolver read Factory uses -- not the highest position the
	// Hosts relayed, which a record committed after the tail stopped (the
	// trailing release) sits above. before is the public position in front of
	// the last one: where a viewer one release short stands.
	targets := map[sessionwire.SessionID]uint64{}
	before := map[sessionwire.SessionID]uint64{}
	releasedLast := map[sessionwire.SessionID]bool{}
	for _, session := range s.sessions {
		session.mu.Lock()
		created := session.created
		session.mu.Unlock()
		if !created {
			continue
		}
		seqs := s.world.PublicJournalSeqs(s.t, s.ctx, session.tenant, session.id)
		if n := len(seqs); n > 0 {
			targets[session.id] = seqs[n-1]
			if n > 1 {
				before[session.id] = seqs[n-2]
			}
		}
		releasedLast[session.id] = s.lastEventIsRelease(session)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		var behind []string
		trailing, hinted := 0, 0
		for _, epoch := range final {
			records := epoch.viewer.Records()
			covered, err := orchestrationtest.PooledCoveredThroughPublic(records, epoch.start, public[epoch.session.id])
			target := targets[epoch.session.id]
			switch {
			case err != nil || covered >= target:
			case covered >= before[epoch.session.id] && releasedLast[epoch.session.id]:
				trailing++
				if stressHintedThrough(records) >= target {
					hinted++
				}
			default:
				behind = append(behind, fmt.Sprintf("%s via %s covered %d < %d", epoch.session.id, epoch.replica.name, covered, target))
			}
		}
		if len(behind) == 0 {
			s.t.Logf("V3 %d final viewers are covered through their sessions' tips; %d viewer connections checked in all", len(final), len(epochs))
			s.observeTrailing(trailing, hinted, len(final))
			return
		}
		if time.Now().After(deadline) {
			sort.Strings(behind)
			s.t.Errorf("V3: %d final viewers are not covered through their session's tip 60s after the fleet went quiet: %v", len(behind), behind)
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// lastEventIsRelease reports whether the session's runtime journal ends in a
// SessionResidencyReleased -- a warm release or a drain was its last act.
func (s *stress) lastEventIsRelease(session *stressSession) bool {
	session.mu.Lock()
	created := session.created
	session.mu.Unlock()
	if !created {
		return false
	}
	runtimeID := s.world.RuntimeSessionID(s.t, s.ctx, session.tenant, session.id)
	events := orchestrationtest.JournalEvents[event.Event](s.t, s.world, session.tenant, runtimeID)
	if len(events) == 0 {
		return false
	}
	_, released := events[len(events)-1].(event.SessionResidencyReleased)
	return released
}

// observeTrailing holds the one shortfall V3 allows to its condition.
//
// A warm-released (or drained) session's LAST committed public event is its
// SessionResidencyReleased, committed after the Host has stopped relaying the
// session's tail, so it never arrives live. That is allowed only because the
// client is TOLD of it: a journal_tip hint naming the new tip is what prompts
// it to read the journal. A viewer one short with no such hint is a viewer
// nothing will prompt until the session is next placed, and fails V3.
func (s *stress) observeTrailing(trailing, hinted, final int) {
	s.t.Logf("V3 trailing release: %d of %d final viewers are one short of a journal whose last event is SessionResidencyReleased; %d of them were told the new tip by a journal_tip hint",
		trailing, final, hinted)
	if trailing > hinted {
		s.t.Errorf("V3: %d final viewers were never told of their session's trailing SessionResidencyReleased", trailing-hinted)
	}
}

// stressHintedThrough is the highest tip any journal_tip hint named.
func stressHintedThrough(records []string) uint64 {
	var best uint64
	for _, record := range records {
		var tip uint64
		if _, err := fmt.Sscanf(record, "T%d", &tip); err == nil && tip > best {
			best = tip
		}
	}
	return best
}

// ---- shutdown and L1 ---------------------------------------------------------------

func (s *stress) shutdown() {
	s.fleet.mu.Lock()
	replicas, hosts := s.fleet.replicas, s.fleet.hosts
	s.fleet.mu.Unlock()
	for _, r := range replicas {
		close(r.dead)
		r.f.Stop()
	}
	for _, slot := range hosts {
		slot.host.Stop()
	}
	s.client.CloseIdleConnections()
}

func (s *stress) logCounters() {
	c := &s.counters
	creates, restores := 0, 0
	s.fleet.mu.RLock()
	for _, h := range s.fleet.every {
		creates += len(h.Rig.Creates())
		restores += len(h.Rig.Restores())
	}
	s.fleet.mu.RUnlock()
	// Restores are the evidence that warm release and Host restarts happened:
	// every one is a session re-placed after it stopped being resident.
	s.t.Logf("I3.1 runtime launches: creates=%d restores=%d across %d Host processes", creates, restores, len(s.fleet.every))
	s.t.Logf("I3.1 counters: commands sent=%d acked=%d retries=%d replays=%d refusals=%d; gates seen=%d answered=%d missed=%d; "+
		"viewer connections=%d dial-failures=%d subscribe-failures=%d; cold reads=%d cross-tenant probes=%d; "+
		"host restarts=%d (skipped %d) factory restarts=%d severs=%d; metric samples=%d; model requests=%d; kit tail drops=%d",
		c.commandsSent.Load(), c.commandsAcked.Load(), c.retries.Load(), c.duplicateAcks.Load(), c.refusals.Load(),
		c.gatesSeen.Load(), c.gatesAnswered.Load(), c.gatesMissed.Load(),
		c.viewerEpochs.Load(), c.viewerDialFailures.Load(), c.subscribeFailures.Load(),
		c.coldReads.Load(), c.crossTenantProbes.Load(),
		c.hostRestarts.Load(), c.hostRestartsSkipped.Load(), c.factoryRestarts.Load(), c.severs.Load(),
		c.metricSamples.Load(), len(s.world.LLM.Requests()), s.world.Tails.Dropped())
	for _, session := range s.sessions {
		for _, cmd := range session.snapshot() {
			cmd.mu.Lock()
			if len(cmd.acks) == 0 && len(cmd.refusals) > 0 {
				s.t.Logf("unacknowledged %s/%s (%s): %v", cmd.session, cmd.id, cmd.kind, cmd.refusals)
			}
			cmd.mu.Unlock()
		}
	}
}

// assertGoroutinesSettle is L1. The slack covers the runtime's own background
// goroutines and connections a peer is still closing; a leak of one goroutine
// per session or per connection is far above it.
//
// # The one leak it accounts for rather than fails on
//
// centrifuge v0.38.0 starts an eagle metrics aggregator for EVERY Node
// (node.go initMetrics: eagle.New, whose aggregate loop exits only on
// Eagle.Close) and Node.Shutdown never closes it, so each Node leaks one
// goroutine -- eagle.(*Eagle).aggregate, a 60s ticker loop -- for the life of
// the process. Factory builds one Node per ClientLink server and Host one per
// tenant HostLink server, so a case that starts and stops them leaks exactly
// that many. They are counted separately, bounded by the Nodes this case could
// have started, and reported; anything else above the baseline fails.
func (s *stress) assertGoroutinesSettle(baseline, baselineEagles int) {
	const slack = 4
	s.fleet.mu.RLock()
	nodes := s.fleet.started + len(s.fleet.every)*2
	s.fleet.mu.RUnlock()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var dump strings.Builder
		_ = pprof.Lookup("goroutine").WriteTo(&dump, 1)
		eagles := stressGroupCount(dump.String(), stressEagleFrame) - baselineEagles
		others := runtime.NumGoroutine() - eagles - baseline
		if others <= slack && eagles <= nodes {
			s.t.Logf("L1 goroutines settled: %d above the baseline of %d, plus %d unclosed centrifuge eagle aggregators (this case started at most %d Nodes)",
				others, baseline, eagles, nodes)
			return
		}
		if time.Now().After(deadline) {
			s.t.Errorf("L1: %d goroutines above the baseline of %d remain 30s after every Factory, Host and viewer stopped, plus %d eagle aggregators for at most %d Nodes:\n%s",
				others, baseline, eagles, nodes, stressLeakSummary(dump.String()))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

const stressEagleFrame = "github.com/FZambia/eagle.(*Eagle).aggregate"

// stressEagles counts the unclosed eagle aggregators alive now -- earlier cases
// in the same test binary leave theirs too.
func stressEagles() int {
	var dump strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&dump, 1)
	return stressGroupCount(dump.String(), stressEagleFrame)
}

// stressGroupCount sums the goroutines of every profile group naming frame.
func stressGroupCount(dump, frame string) int {
	total := 0
	for _, group := range strings.Split(dump, "\n\n") {
		if !strings.Contains(group, frame) {
			continue
		}
		count := 0
		if _, err := fmt.Sscanf(strings.TrimPrefix(group, "goroutine profile: "), "%d @", &count); err != nil {
			// The first group follows the "goroutine profile: total N" line.
			if lines := strings.SplitN(group, "\n", 2); len(lines) == 2 {
				_, _ = fmt.Sscanf(lines[1], "%d @", &count)
			}
		}
		total += count
	}
	return total
}

// stressLeakSummary keeps every goroutine group but the test binary's own,
// largest first, so a leak report names its owner.
func stressLeakSummary(dump string) string {
	groups := strings.Split(dump, "\n\n")
	type group struct {
		count int
		text  string
	}
	var kept []group
	for _, g := range groups {
		if strings.Contains(g, "runtime/pprof.writeGoroutine") || strings.Contains(g, "testing.(*M).Run") {
			continue
		}
		count := 0
		if _, err := fmt.Sscanf(g, "%d @", &count); err != nil {
			if lines := strings.SplitN(g, "\n", 2); len(lines) == 2 {
				_, _ = fmt.Sscanf(lines[1], "%d @", &count)
			}
		}
		kept = append(kept, group{count, g})
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].count > kept[j].count })
	var out strings.Builder
	for i, g := range kept {
		if i >= 12 {
			break
		}
		out.WriteString(g.text)
		out.WriteString("\n\n")
	}
	return out.String()
}
