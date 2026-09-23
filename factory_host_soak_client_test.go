//go:build integration && soak

// This file is the CLIENT half of runbook 07's I3.2 soak: the fleet of
// browser-shaped ClientLink connections, run in a CHILD PROCESS of the soak so
// that the server process's CPU, RSS, goroutines and file descriptors measure
// Factories and Hosts rather than five thousand WebSocket clients.
//
// The parent (factory_host_soak_test.go) re-executes this test binary with
// soakChildEnv set and -test.run naming TestSoakClientFleetChild, hands it a
// spec and then drives it one operation at a time over two pipes (fd 3 in, fd
// 4 out), one JSON value per operation and one per reply. Nothing here reaches
// into a server: every byte a browser learns comes over its real ClientLink or
// through a real Factory's journal route.
//
// # What one browser does
//
// A soakBrowser is ONE LOGICAL browser tab that outlives its physical
// connections. It holds a position -- the highest journal sequence it is
// covered through -- and a FOLD, the (sequence, ordinal) of every enduring
// record it has applied, exactly once each. It applies an enduring record that
// is the next sequence, skips one it already holds (counting it as a wire
// duplicate or a repair overlap), and REPAIRS by reading the journal route from
// its position whenever it (re)subscribes, is reset, or is told of a tip above
// it. An enduring record beyond the next sequence that nothing announced is a
// SILENT GAP: it is counted as a violation and then repaired, so one defect
// does not end the run. Reconnecting -- a refresh, or its Factory dying --
// replaces the connection and keeps the position, as a browser with a cursor
// does.
//
// Every live enduring record's latency is its arrival time minus the commit
// time the product stamped into its body (PooledWorldOptions.TailStampBodies);
// both clocks are this machine's wall clock.

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/tests/internal/orchestrationtest"
)

const soakChildEnv = "LOOPRIG_SOAK_CHILD"

// ---- the wire between the processes -----------------------------------------

type soakReplicaSpec struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type soakSessionSpec struct {
	Tenant  sessionwire.TenantID  `json:"tenant"`
	Session sessionwire.SessionID `json:"session"`
}

type soakChildSpec struct {
	Replicas    []soakReplicaSpec `json:"replicas"`
	Sessions    []soakSessionSpec `json:"sessions"`
	Connections int               `json:"connections"`
	// Slow is how many browsers are dialled STALLABLE with a small receive
	// buffer. Browser i watches session i%len(Sessions), so the first Slow
	// browsers are on distinct sessions while Slow <= len(Sessions).
	Slow       int    `json:"slow"`
	OutDir     string `json:"out_dir"`
	SampleEach string `json:"sample_each"`
}

type soakOp struct {
	Op string `json:"op"`
	// refresh
	Fraction float64 `json:"fraction,omitempty"`
	Seed     uint64  `json:"seed,omitempty"`
	// replica_dead
	Dead        string           `json:"dead,omitempty"`
	Replacement *soakReplicaSpec `json:"replacement,omitempty"`
	// await: session index -> sequence every browser of it must hold through
	Targets map[int]uint64 `json:"targets,omitempty"`
	Timeout string         `json:"timeout,omitempty"`
	// rpc
	Count       int    `json:"count,omitempty"`
	Concurrency int    `json:"concurrency,omitempty"`
	Prefix      string `json:"prefix,omitempty"`
	// verify: session index -> every committed [sequence, ordinal]
	Histories map[int][][2]uint64 `json:"histories,omitempty"`
	// mark names a latency window: live latencies are recorded into the
	// named window until the next mark.
	Window string `json:"window,omitempty"`
}

type soakReply struct {
	OK    bool            `json:"ok"`
	Error string          `json:"error,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
}

// soakDist is a percentile summary, in milliseconds.
type soakDist struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	Max   float64 `json:"max_ms"`
	Mean  float64 `json:"mean_ms"`
}

func soakDistOf(samples []float64) soakDist {
	if len(samples) == 0 {
		return soakDist{}
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	at := func(q float64) float64 {
		i := int(q * float64(len(sorted)-1))
		return sorted[i]
	}
	sum := 0.0
	for _, v := range sorted {
		sum += v
	}
	return soakDist{Count: len(sorted), P50: at(0.5), P90: at(0.9), P99: at(0.99), P999: at(0.999), Max: sorted[len(sorted)-1], Mean: sum / float64(len(sorted))}
}

// soakHist is a fixed-size latency histogram of 100µs buckets up to 120s, so
// millions of deliveries cost constant memory.
type soakHist struct {
	buckets []atomic.Uint64
	max     atomic.Int64
	sum     atomic.Int64
	count   atomic.Int64
}

const soakHistBucket = 100 * time.Microsecond

func newSoakHist() *soakHist {
	return &soakHist{buckets: make([]atomic.Uint64, int(120*time.Second/soakHistBucket)+1)}
}

func (h *soakHist) add(d time.Duration) {
	if d < 0 {
		d = 0
	}
	i := int(d / soakHistBucket)
	if i >= len(h.buckets) {
		i = len(h.buckets) - 1
	}
	h.buckets[i].Add(1)
	h.count.Add(1)
	h.sum.Add(int64(d))
	for {
		old := h.max.Load()
		if int64(d) <= old || h.max.CompareAndSwap(old, int64(d)) {
			break
		}
	}
}

func (h *soakHist) dist() soakDist {
	n := h.count.Load()
	if n == 0 {
		return soakDist{}
	}
	ms := func(i int) float64 { return float64(time.Duration(i+1)*soakHistBucket) / float64(time.Millisecond) }
	quantile := func(q float64) float64 {
		want := uint64(q * float64(n))
		var seen uint64
		for i := range h.buckets {
			seen += h.buckets[i].Load()
			if seen > want {
				return ms(i)
			}
		}
		return ms(len(h.buckets) - 1)
	}
	return soakDist{
		Count: int(n), P50: quantile(0.5), P90: quantile(0.9), P99: quantile(0.99), P999: quantile(0.999),
		Max:  float64(h.max.Load()) / float64(time.Millisecond),
		Mean: float64(h.sum.Load()) / float64(n) / float64(time.Millisecond),
	}
}

// ---- the child's entry point --------------------------------------------------

// TestSoakClientFleetChild is not a test: it is the client process of
// TestFactoryHostClientLinkSoak5000 and skips unless that test started it.
func TestSoakClientFleetChild(t *testing.T) {
	if os.Getenv(soakChildEnv) != "1" {
		t.Skip("the soak's client process; TestFactoryHostClientLinkSoak5000 starts it")
	}
	control := os.NewFile(3, "soak-control")
	report := os.NewFile(4, "soak-report")
	if control == nil || report == nil {
		t.Fatalf("the soak child needs its control pipes on fd 3 and 4")
	}
	decoder := json.NewDecoder(control)
	encoder := json.NewEncoder(report)
	var spec soakChildSpec
	if err := decoder.Decode(&spec); err != nil {
		t.Fatalf("reading the soak spec: %v", err)
	}
	fleet := newSoakFleet(spec)
	stopSampler := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		sampleEach, _ := time.ParseDuration(spec.SampleEach)
		soakSampleSelf(spec.OutDir+"/resources_client.csv", sampleEach, stopSampler, nil)
	}()
	defer func() {
		close(stopSampler)
		<-samplerDone
	}()
	for {
		var op soakOp
		if err := decoder.Decode(&op); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			t.Fatalf("reading a soak operation: %v", err)
		}
		data, err := fleet.handle(op)
		reply := soakReply{OK: err == nil, Data: data}
		if err != nil {
			reply.Error = err.Error()
		}
		if err := encoder.Encode(reply); err != nil {
			t.Fatalf("writing the reply to %s: %v", op.Op, err)
		}
		if op.Op == "exit" {
			return
		}
	}
}

// ---- the fleet ----------------------------------------------------------------

type soakFleet struct {
	spec soakChildSpec
	http *http.Client

	mu       sync.RWMutex
	replicas []soakReplicaSpec // live replicas
	browsers []*soakBrowser

	latency      *soakHist
	slowLatency  *soakHist
	windowMu     sync.Mutex
	windows      map[string]*soakHist
	window       atomic.Pointer[soakHist]
	connectMs    []float64
	joinMs       []float64
	reconnectMu  sync.Mutex
	reconnectMs  map[string][]float64
	commandsMu   sync.Mutex
	commandsSent int
}

func newSoakFleet(spec soakChildSpec) *soakFleet {
	transport := &http.Transport{
		MaxIdleConns:        2048,
		MaxIdleConnsPerHost: 512,
		IdleConnTimeout:     90 * time.Second,
	}
	f := &soakFleet{
		spec:        spec,
		http:        &http.Client{Timeout: 30 * time.Second, Transport: transport},
		replicas:    append([]soakReplicaSpec(nil), spec.Replicas...),
		latency:     newSoakHist(),
		slowLatency: newSoakHist(),
		windows:     map[string]*soakHist{},
		reconnectMs: map[string][]float64{},
	}
	for i := range spec.Connections {
		session := spec.Sessions[i%len(spec.Sessions)]
		f.browsers = append(f.browsers, &soakBrowser{
			fleet: f, id: i, index: i % len(spec.Sessions),
			tenant: session.Tenant, session: session.Session,
			slow:  i < spec.Slow,
			codes: map[uint32]int{},
		})
	}
	return f
}

func (f *soakFleet) handle(op soakOp) (json.RawMessage, error) {
	var out any
	var err error
	switch op.Op {
	case "connect":
		out, err = f.connectAll()
	case "mark":
		f.windowMu.Lock()
		h := f.windows[op.Window]
		if h == nil {
			h = newSoakHist()
			f.windows[op.Window] = h
		}
		f.windowMu.Unlock()
		f.window.Store(h)
	case "refresh":
		out, err = f.refresh(op.Fraction, op.Seed)
	case "replica_dead":
		out, err = f.replicaDead(op.Dead, op.Replacement)
	case "stall":
		out = f.stall(true)
	case "release":
		out = f.stall(false)
	case "await":
		timeout, _ := time.ParseDuration(op.Timeout)
		out, err = f.await(op.Targets, timeout)
	case "rpc":
		out, err = f.rpc(op.Count, op.Concurrency, op.Prefix, op.Seed)
	case "stats":
		out = f.stats()
	case "verify":
		out = f.verify(op.Histories)
	case "close":
		out = f.closeAll()
	case "exit":
	default:
		err = fmt.Errorf("unknown soak op %q", op.Op)
	}
	if err != nil || out == nil {
		return nil, err
	}
	encoded, err := json.Marshal(out)
	return encoded, err
}

func (f *soakFleet) liveReplica(i int) soakReplicaSpec {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.replicas[i%len(f.replicas)]
}

func (f *soakFleet) otherReplica(not string, rng *rand.Rand) soakReplicaSpec {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var candidates []soakReplicaSpec
	for _, r := range f.replicas {
		if r.Name != not {
			candidates = append(candidates, r)
		}
	}
	if len(candidates) == 0 {
		return f.replicas[0]
	}
	return candidates[rng.IntN(len(candidates))]
}

// connectAll opens every browser, interleaving them across the replicas, with
// a bounded number of dials in flight: a listen backlog is small on this
// platform, and a burst above it measures SYN retransmission, not Factory.
func (f *soakFleet) connectAll() (any, error) {
	type result struct {
		connect, join time.Duration
		err           error
	}
	results := make([]result, len(f.browsers))
	work := make(chan int)
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				b := f.browsers[i]
				replica := f.liveReplica(i / len(f.spec.Sessions))
				var last error
				for attempt := 0; attempt < 5; attempt++ {
					connect, join, err := b.open(replica, 0)
					if err == nil {
						results[i] = result{connect: connect, join: join}
						last = nil
						break
					}
					last = err
					time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
				}
				if last != nil {
					results[i] = result{err: last}
				}
			}
		}()
	}
	start := time.Now()
	for i := range f.browsers {
		work <- i
	}
	close(work)
	wg.Wait()
	failures := []string{}
	for i, r := range results {
		if r.err != nil {
			if len(failures) < 10 {
				failures = append(failures, fmt.Sprintf("browser %d: %v", i, r.err))
			}
			continue
		}
		f.connectMs = append(f.connectMs, float64(r.connect)/float64(time.Millisecond))
		f.joinMs = append(f.joinMs, float64(r.join)/float64(time.Millisecond))
	}
	failed := len(results) - len(f.connectMs)
	return map[string]any{
		"connected": len(f.connectMs), "failed": failed, "failures": failures,
		"took_ms": time.Since(start).Milliseconds(),
		"connect": soakDistOf(f.connectMs), "join": soakDistOf(f.joinMs),
	}, nil
}

func (f *soakFleet) noteReconnect(kind string, d time.Duration) {
	f.reconnectMu.Lock()
	defer f.reconnectMu.Unlock()
	f.reconnectMs[kind] = append(f.reconnectMs[kind], float64(d)/float64(time.Millisecond))
}

// refresh is a browser refresh: a fraction of the browsers close their
// connection and come back ON ANOTHER REPLICA with their cursor, as a page
// reload behind a load balancer with no affinity does.
func (f *soakFleet) refresh(fraction float64, seed uint64) (any, error) {
	rng := rand.New(rand.NewPCG(seed, 11))
	var chosen []*soakBrowser
	for _, b := range f.browsers {
		if !b.slow && rng.Float64() < fraction {
			chosen = append(chosen, b)
		}
	}
	return f.reconnect("refresh", chosen, func(b *soakBrowser) soakReplicaSpec {
		return f.otherReplica(b.replicaName(), rand.New(rand.NewPCG(seed, uint64(b.id))))
	})
}

// replicaDead moves every browser of a stopped replica to the live ones, the
// way a load balancer sends a reconnecting browser to a surviving pod.
func (f *soakFleet) replicaDead(dead string, replacement *soakReplicaSpec) (any, error) {
	f.mu.Lock()
	kept := f.replicas[:0:0]
	for _, r := range f.replicas {
		if r.Name != dead {
			kept = append(kept, r)
		}
	}
	if replacement != nil {
		kept = append(kept, *replacement)
	}
	f.replicas = kept
	f.mu.Unlock()
	var orphans []*soakBrowser
	for _, b := range f.browsers {
		if b.replicaName() == dead {
			orphans = append(orphans, b)
		}
	}
	return f.reconnect("factory_restart", orphans, func(b *soakBrowser) soakReplicaSpec {
		return f.liveReplica(b.id)
	})
}

func (f *soakFleet) reconnect(kind string, chosen []*soakBrowser, target func(*soakBrowser) soakReplicaSpec) (any, error) {
	work := make(chan *soakBrowser)
	var wg sync.WaitGroup
	var failed atomic.Int64
	var mu sync.Mutex
	var failures []string
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range work {
				start := time.Now()
				var last error
				for attempt := 0; attempt < 5; attempt++ {
					b.closeConnection()
					if _, _, err := b.open(target(b), b.position()); err == nil {
						last = nil
						break
					} else {
						last = err
					}
					time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
				}
				if last != nil {
					failed.Add(1)
					mu.Lock()
					if len(failures) < 10 {
						failures = append(failures, fmt.Sprintf("browser %d: %v", b.id, last))
					}
					mu.Unlock()
					continue
				}
				f.noteReconnect(kind, time.Since(start))
			}
		}()
	}
	start := time.Now()
	for _, b := range chosen {
		work <- b
	}
	close(work)
	wg.Wait()
	f.reconnectMu.Lock()
	dist := soakDistOf(f.reconnectMs[kind])
	f.reconnectMu.Unlock()
	return map[string]any{"moved": len(chosen), "failed": failed.Load(), "failures": failures,
		"took_ms": time.Since(start).Milliseconds(), "reconnect_cumulative": dist}, nil
}

func (f *soakFleet) stall(on bool) any {
	n := 0
	for _, b := range f.browsers {
		if !b.slow {
			continue
		}
		n++
		if on {
			b.gate.stall()
		} else {
			b.gate.release()
		}
	}
	return map[string]any{"browsers": n}
}

// await waits until every browser of each target session holds through its
// target, and reports how long each took from the call.
func (f *soakFleet) await(targets map[int]uint64, timeout time.Duration) (any, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	pending := map[*soakBrowser]uint64{}
	for _, b := range f.browsers {
		if target, ok := targets[b.index]; ok {
			pending[b] = target
		}
	}
	var took []float64
	for len(pending) > 0 && time.Now().Before(deadline) {
		for b, target := range pending {
			if b.position() >= target {
				took = append(took, float64(time.Since(start))/float64(time.Millisecond))
				delete(pending, b)
			} else if b.hintedAbove() {
				// A browser that was told of a tip above it but has not read
				// it is prompted to, as a browser's hint handler would.
				b.kick()
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	var behind []string
	for b, target := range pending {
		if len(behind) < 20 {
			behind = append(behind, fmt.Sprintf("browser %d (%s via %s) holds %d < %d; %s", b.id, b.session, b.replicaName(), b.position(), target, b.describe()))
		}
	}
	sort.Strings(behind)
	return map[string]any{"covered": len(took), "behind": len(pending), "behind_examples": behind, "took": soakDistOf(took)}, nil
}

// rpc sends count session.input commands over the ClientLink RPC from random
// connected browsers, with concurrency in flight, and reports every command it
// was ACKNOWLEDGED for -- the only ones that carry an obligation.
func (f *soakFleet) rpc(count, concurrency int, prefix string, seed uint64) (any, error) {
	type acked struct {
		Tenant  sessionwire.TenantID  `json:"tenant"`
		Session sessionwire.SessionID `json:"session"`
		Command sessionwire.CommandID `json:"command"`
		Status  string                `json:"status"`
	}
	rng := rand.New(rand.NewPCG(seed, 13))
	jobs := make(chan int)
	var mu sync.Mutex
	var acks []acked
	var latencies []float64
	refusals := map[string]int{}
	var transportFailures int
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				mu.Lock()
				b := f.browsers[rng.IntN(len(f.browsers))]
				mu.Unlock()
				command := sessionwire.CommandID(fmt.Sprintf("%s-%d", prefix, n))
				data, _ := json.Marshal(sessionwire.InputRequest{
					CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)),
					SessionID:       b.session,
					Blocks:          json.RawMessage(`[{"type":"text","text":"soak ` + string(command) + `"}]`),
				})
				start := time.Now()
				var reply []byte
				var err error
				for attempt := 0; attempt < 4; attempt++ {
					// The SAME CommandID on every retry: an unknown outcome
					// is retried, never re-minted.
					reply, err = b.rpc("session.input", data)
					if err == nil {
						break
					}
					time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
				}
				took := time.Since(start)
				mu.Lock()
				if err != nil {
					transportFailures++
					refusals["transport: "+soakShort(err.Error())]++
					mu.Unlock()
					continue
				}
				var status sessionwire.CommandStatus
				if json.Unmarshal(reply, &status) == nil && status.CommandID == command {
					latencies = append(latencies, float64(took)/float64(time.Millisecond))
					acks = append(acks, acked{Tenant: b.tenant, Session: b.session, Command: command, Status: string(status.State)})
				} else {
					refusals[soakShort(string(reply))]++
				}
				mu.Unlock()
			}
		}()
	}
	start := time.Now()
	for n := range count {
		jobs <- n
	}
	close(jobs)
	wg.Wait()
	f.commandsMu.Lock()
	f.commandsSent += count
	f.commandsMu.Unlock()
	return map[string]any{"sent": count, "acked": acks, "refusals": refusals, "transport_failures": transportFailures,
		"latency": soakDistOf(latencies), "took_ms": time.Since(start).Milliseconds()}, nil
}

func soakShort(s string) string {
	if len(s) > 160 {
		return s[:160]
	}
	return s
}

// soakCounters are one browser's (or the fleet's) observations.
type soakCounters struct {
	LiveApplied    int            `json:"live_applied"`
	RepairApplied  int            `json:"repair_applied"`
	WireDuplicates int            `json:"wire_duplicates"`
	RepairOverlaps int            `json:"repair_overlaps"`
	SilentGaps     int            `json:"silent_gaps"`
	Strays         int            `json:"strays"`
	Resets         int            `json:"resets"`
	Hints          int            `json:"hints"`
	Repairs        int            `json:"repairs"`
	RepairFailures int            `json:"repair_failures"`
	Subscribes     int            `json:"subscribes"`
	Dials          int            `json:"dials"`
	Undecodable    int            `json:"undecodable"`
	Codes          map[string]int `json:"transport_codes"`
}

func (c *soakCounters) add(o soakCounters) {
	c.LiveApplied += o.LiveApplied
	c.RepairApplied += o.RepairApplied
	c.WireDuplicates += o.WireDuplicates
	c.RepairOverlaps += o.RepairOverlaps
	c.SilentGaps += o.SilentGaps
	c.Strays += o.Strays
	c.Resets += o.Resets
	c.Hints += o.Hints
	c.Repairs += o.Repairs
	c.RepairFailures += o.RepairFailures
	c.Subscribes += o.Subscribes
	c.Dials += o.Dials
	c.Undecodable += o.Undecodable
	if c.Codes == nil {
		c.Codes = map[string]int{}
	}
	for k, v := range o.Codes {
		c.Codes[k] += v
	}
}

func (f *soakFleet) stats() any {
	var total, slow, peers soakCounters
	connected := 0
	for _, b := range f.browsers {
		c := b.counters()
		total.add(c)
		switch {
		case b.slow:
			slow.add(c)
		case b.index < f.spec.Slow:
			// A browser of a session a stalled browser also watches: it
			// receives the same burst the stall is measured under.
			peers.add(c)
		}
		if b.connected() {
			connected++
		}
	}
	f.windowMu.Lock()
	windows := map[string]soakDist{}
	for name, h := range f.windows {
		windows[name] = h.dist()
	}
	f.windowMu.Unlock()
	f.reconnectMu.Lock()
	reconnects := map[string]soakDist{}
	for kind, samples := range f.reconnectMs {
		reconnects[kind] = soakDistOf(samples)
	}
	f.reconnectMu.Unlock()
	return map[string]any{
		"connected": connected, "browsers": len(f.browsers),
		"counters": total, "slow_counters": slow, "burst_peer_counters": peers,
		"delivery_latency": f.latency.dist(), "delivery_latency_windows": windows, "slow_delivery_latency": f.slowLatency.dist(),
		"connect": soakDistOf(f.connectMs), "join": soakDistOf(f.joinMs), "reconnect": reconnects,
	}
}

// verify holds every browser's fold against its session's committed history:
// the fold must BE the history -- every committed identity applied once, in
// sequence order, nothing else.
func (f *soakFleet) verify(histories map[int][][2]uint64) any {
	lost, extra, mismatched, duplicated, exact := 0, 0, 0, 0, 0
	var examples []string
	for _, b := range f.browsers {
		history := histories[b.index]
		fold := b.foldCopy()
		want := map[uint64]uint64{}
		for _, h := range history {
			want[h[0]] = h[1]
		}
		got := map[uint64]int{}
		bad := false
		for _, entry := range fold {
			got[entry.seq]++
			if got[entry.seq] > 1 {
				duplicated++
				bad = true
				continue
			}
			ordinal, ok := want[entry.seq]
			switch {
			case !ok:
				extra++
				bad = true
			case ordinal != entry.ordinal:
				mismatched++
				bad = true
			}
		}
		missing := 0
		for _, h := range history {
			if got[h[0]] == 0 {
				missing++
			}
		}
		lost += missing
		inOrder := sort.SliceIsSorted(fold, func(i, j int) bool { return fold[i].seq < fold[j].seq })
		if missing > 0 || !inOrder {
			bad = true
		}
		if bad {
			if len(examples) < 20 {
				examples = append(examples, fmt.Sprintf("browser %d of %s: fold %d records, history %d, missing %d, in order %v; %s",
					b.id, b.session, len(fold), len(history), missing, inOrder, b.describe()))
			}
			continue
		}
		exact++
	}
	return map[string]any{"browsers": len(f.browsers), "exact": exact, "lost": lost, "extra": extra,
		"mismatched": mismatched, "fold_duplicates": duplicated, "examples": examples}
}

func (f *soakFleet) closeAll() any {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 128)
	for _, b := range f.browsers {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			b.closeConnection()
		}()
	}
	wg.Wait()
	f.http.CloseIdleConnections()
	return map[string]any{"closed": len(f.browsers)}
}

// ---- one browser --------------------------------------------------------------

type soakFold struct {
	seq     uint64
	ordinal uint64
}

type soakBrowser struct {
	fleet   *soakFleet
	id      int
	index   int
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
	slow    bool
	gate    *stallSoakGate

	mu         sync.Mutex
	gen        int
	replica    soakReplicaSpec
	client     *centrifugego.Client
	pos        uint64
	lastLive   uint64
	hinted     uint64
	fold       []soakFold
	c          soakCounters
	codes      map[uint32]int
	lastRepair string
}

func (b *soakBrowser) replicaName() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.replica.Name
}

func (b *soakBrowser) position() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pos
}

func (b *soakBrowser) hintedAbove() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hinted > b.pos
}

func (b *soakBrowser) connected() bool {
	b.mu.Lock()
	client := b.client
	b.mu.Unlock()
	return client != nil && client.State() == centrifugego.StateConnected
}

func (b *soakBrowser) foldCopy() []soakFold {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]soakFold(nil), b.fold...)
}

func (b *soakBrowser) counters() soakCounters {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.c
	c.Codes = map[string]int{}
	for code, n := range b.codes {
		c.Codes[strconv.Itoa(int(code))] += n
	}
	return c
}

func (b *soakBrowser) describe() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := "none"
	if b.client != nil {
		state = string(b.client.State())
	}
	return fmt.Sprintf("state=%s pos=%d hinted=%d resets=%d repairs=%d gaps=%d codes=%v last repair: %s",
		state, b.pos, b.hinted, b.c.Resets, b.c.Repairs, b.c.SilentGaps, b.codes, b.lastRepair)
}

// open dials a new physical connection to replica for this browser, from
// position resume, and waits until it is subscribed and has repaired. It
// returns the connect and the join (connect + subscribe + first repair) times.
func (b *soakBrowser) open(replica soakReplicaSpec, resume uint64) (time.Duration, time.Duration, error) {
	start := time.Now()
	if b.slow && b.gate == nil {
		b.gate = newStallSoakGate()
	}
	config := centrifugego.Config{
		Token: orchestrationtest.PooledBearers[b.tenant],
		Data:  []byte(`{"protocol_version":"1"}`),
		Header: http.Header{
			"Authorization": {"Bearer " + orchestrationtest.PooledBearers[b.tenant]},
			"Origin":        {replica.URL},
		},
		Name:             "looprig-soak-browser",
		HandshakeTimeout: 20 * time.Second,
		LogLevel:         centrifugego.LogLevelNone,
	}
	gate := b.gate
	if gate != nil {
		// Only the SERVER may end a stalled browser's connection: a client
		// that gave up on the server's pings first (connecting code 2) would
		// hide whether the server bounds the queue at all.
		config.MaxServerPingDelay = 10 * time.Minute
	}
	config.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		var dialer net.Dialer
		if gate != nil {
			// The receive buffer is set BEFORE connect, so the window the
			// kernel advertises -- and auto-tunes from -- starts small. Set
			// after connect it is not honoured on this platform, and megabytes
			// of kernel buffering stand in for the server's queue.
			dialer.Control = func(_, _ string, raw syscall.RawConn) error {
				var serr error
				if err := raw.Control(func(fd uintptr) {
					serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4<<10)
				}); err != nil {
					return err
				}
				return serr
			}
		}
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		b.mu.Lock()
		b.c.Dials++
		b.mu.Unlock()
		if gate == nil {
			return conn, nil
		}
		return &stallSoakConn{Conn: conn, gate: gate}, nil
	}
	endpoint := "ws" + strings.TrimPrefix(replica.URL, "http") + "/v1/realtime"
	client := centrifugego.NewJsonClient(endpoint, config)

	b.mu.Lock()
	b.gen++
	gen := b.gen
	b.client, b.replica = client, replica
	if resume > b.pos {
		b.pos = resume
	}
	b.mu.Unlock()

	connected := make(chan struct{}, 1)
	joined := make(chan error, 1)
	client.OnConnected(func(centrifugego.ConnectedEvent) {
		select {
		case connected <- struct{}{}:
		default:
		}
	})
	client.OnConnecting(func(e centrifugego.ConnectingEvent) { b.noteCode(gen, e.Code) })
	client.OnDisconnected(func(e centrifugego.DisconnectedEvent) { b.noteCode(gen, e.Code) })
	if err := client.Connect(); err != nil {
		client.Close()
		return 0, 0, err
	}
	select {
	case <-connected:
	case <-time.After(30 * time.Second):
		client.Close()
		return 0, 0, fmt.Errorf("no connect within 30s")
	}
	connectTook := time.Since(start)
	sub, err := client.NewSubscription(orchestrationtest.ClientLinkChannel(b.tenant, b.session))
	if err != nil {
		client.Close()
		return 0, 0, err
	}
	sub.OnSubscribed(func(centrifugego.SubscribedEvent) {
		// Every (re)subscribe is a moment the browser cannot know what it
		// missed: it reads the journal from where it is.
		if !b.current(gen) {
			return
		}
		b.mu.Lock()
		b.c.Subscribes++
		b.mu.Unlock()
		err := b.repair(gen)
		select {
		case joined <- err:
		default:
		}
	})
	sub.OnUnsubscribed(func(e centrifugego.UnsubscribedEvent) {
		if !b.current(gen) {
			return
		}
		b.noteCode(gen, e.Code)
		if e.Code >= 2000 && e.Code < 2500 {
			go func() { _ = sub.Subscribe() }()
		}
	})
	sub.OnError(func(e centrifugego.SubscriptionErrorEvent) {
		select {
		case joined <- e.Error:
		default:
		}
	})
	sub.OnPublication(func(e centrifugego.PublicationEvent) { b.onRecord(gen, e.Data) })
	if err := sub.Subscribe(); err != nil {
		client.Close()
		return 0, 0, err
	}
	select {
	case err := <-joined:
		if err != nil {
			client.Close()
			return 0, 0, err
		}
	case <-time.After(60 * time.Second):
		client.Close()
		return 0, 0, fmt.Errorf("no subscribe and repair within 60s")
	}
	return connectTook, time.Since(start), nil
}

func (b *soakBrowser) current(gen int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gen == gen
}

func (b *soakBrowser) noteCode(gen int, code uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.gen == gen && code != 0 {
		b.codes[code]++
	}
}

func (b *soakBrowser) closeConnection() {
	b.mu.Lock()
	b.gen++
	client := b.client
	b.client = nil
	b.mu.Unlock()
	if b.gate != nil {
		b.gate.release()
	}
	if client != nil {
		client.Close()
	}
}

func (b *soakBrowser) rpc(method string, data []byte) ([]byte, error) {
	b.mu.Lock()
	client := b.client
	b.mu.Unlock()
	if client == nil {
		return nil, errors.New("not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	result, err := client.RPC(ctx, method, data)
	return result.Data, err
}

// kick performs a repair outside a callback, for a browser told of a tip it
// has not read. It is serialised with the callbacks by the browser's lock
// order only loosely, which is sound: a repair is a read of durable state and
// the fold only ever advances.
func (b *soakBrowser) kick() {
	b.mu.Lock()
	gen := b.gen
	b.mu.Unlock()
	go func() { _ = b.repair(gen) }()
}

// onRecord applies one session-channel record. The client library calls it
// from its one callback goroutine, so records are processed in arrival order.
func (b *soakBrowser) onRecord(gen int, data []byte) {
	if !b.current(gen) {
		return
	}
	now := time.Now()
	recordType, err := sessionwire.SessionRecordTypeOf(data)
	if err != nil {
		b.mu.Lock()
		b.c.Undecodable++
		b.mu.Unlock()
		return
	}
	switch recordType {
	case sessionwire.SessionRecordTypeEnduringPublication:
		var p sessionwire.EnduringPublication
		if err := p.UnmarshalJSON(data); err != nil {
			b.mu.Lock()
			b.c.Undecodable++
			b.mu.Unlock()
			return
		}
		var body struct {
			Ordinal uint64 `json:"ordinal"`
			At      int64  `json:"at"`
		}
		_ = json.Unmarshal(p.Body, &body)
		b.mu.Lock()
		if p.TenantID != b.tenant || p.SessionID != b.session {
			b.c.Strays++
			b.mu.Unlock()
			return
		}
		live := p.JournalSeq > b.lastLive
		if live {
			b.lastLive = p.JournalSeq
			if body.At != 0 {
				// A stalled browser's latency is the stall, by design: it is
				// kept apart so it does not stand in for everyone's.
				latency := now.Sub(time.Unix(0, body.At))
				if b.slow {
					b.fleet.slowLatency.add(latency)
				} else {
					b.fleet.latency.add(latency)
					if w := b.fleet.window.Load(); w != nil {
						w.add(latency)
					}
				}
			}
		}
		switch {
		case !live:
			b.c.WireDuplicates++
			b.mu.Unlock()
		case p.JournalSeq <= b.pos:
			b.c.RepairOverlaps++
			b.mu.Unlock()
		case p.JournalSeq == b.pos+1:
			ordinal := soakOrdinal(p.EventID, b.session)
			b.fold = append(b.fold, soakFold{seq: p.JournalSeq, ordinal: ordinal})
			b.pos = p.JournalSeq
			b.c.LiveApplied++
			b.mu.Unlock()
		default:
			// A SILENT GAP: nothing announced the positions between. Counted
			// as the violation it is, then repaired so the run goes on.
			b.c.SilentGaps++
			b.mu.Unlock()
			_ = b.repair(gen)
		}
	case sessionwire.SessionRecordTypeSessionReset:
		var r sessionwire.SessionReset
		if err := r.UnmarshalJSON(data); err != nil {
			return
		}
		b.mu.Lock()
		b.c.Resets++
		stray := r.TenantID != b.tenant || r.SessionID != b.session
		if stray {
			b.c.Strays++
		}
		b.mu.Unlock()
		if !stray {
			_ = b.repair(gen)
		}
	case sessionwire.SessionRecordTypeJournalTip:
		var tip sessionwire.JournalTip
		if err := tip.UnmarshalJSON(data); err != nil {
			return
		}
		b.mu.Lock()
		b.c.Hints++
		if tip.Tip > b.hinted {
			b.hinted = tip.Tip
		}
		above := tip.Tip > b.pos
		b.mu.Unlock()
		if above {
			_ = b.repair(gen)
		}
	}
}

// soakOrdinal reads the product's ordinal back out of its EventID
// ("event-<session>-<ordinal>"); a record of another session reads as zero,
// which no history holds.
func soakOrdinal(id sessionwire.EventID, session sessionwire.SessionID) uint64 {
	prefix := "event-" + string(session) + "-"
	if !strings.HasPrefix(string(id), prefix) {
		return 0
	}
	n, _ := strconv.ParseUint(strings.TrimPrefix(string(id), prefix), 10, 64)
	return n
}

// repair reads the journal route from the browser's position, following
// cursors, and applies what it finds. Only the connection generation that
// asked may apply it, so a replaced connection cannot write into the fold.
func (b *soakBrowser) repair(gen int) error {
	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {
		if !b.current(gen) {
			return nil
		}
		err := b.repairOnce(gen)
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(time.Duration(250*(attempt+1)) * time.Millisecond)
	}
	b.mu.Lock()
	b.c.RepairFailures++
	b.lastRepair = "FAILED: " + lastErr.Error()
	b.mu.Unlock()
	return lastErr
}

func (b *soakBrowser) repairOnce(gen int) error {
	b.mu.Lock()
	replica := b.replica
	from := b.pos + 1
	b.mu.Unlock()
	query := url.Values{"from_seq": {strconv.FormatUint(from, 10)}}
	var events []soakFold
	var covered uint64
	for pages := 0; ; pages++ {
		if pages > 10_000 {
			return errors.New("the journal read did not terminate")
		}
		request, err := http.NewRequest(http.MethodGet, replica.URL+"/v1/sessions/"+string(b.session)+"/journal?"+query.Encode(), nil)
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+orchestrationtest.PooledBearers[b.tenant])
		response, err := b.fleet.http.Do(request)
		if err != nil {
			// The replica may have been stopped under the read; a browser
			// retries through whatever the balancer offers.
			replica = b.fleet.liveReplica(b.id + pages + 1)
			return err
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("journal read from %d via %s answered %d: %s", from, replica.Name, response.StatusCode, soakShort(string(body)))
		}
		var page sessionwire.JournalPage
		if err := json.Unmarshal(body, &page); err != nil {
			return fmt.Errorf("the journal page is not a Core JournalPage: %w", err)
		}
		for _, event := range page.Events {
			events = append(events, soakFold{seq: event.JournalSeq, ordinal: soakOrdinal(event.EventID, b.session)})
		}
		if page.CoveredThrough > covered {
			covered = page.CoveredThrough
		}
		if page.NextCursor == "" {
			break
		}
		query = url.Values{"cursor": {string(page.NextCursor)}}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.gen != gen {
		return nil
	}
	applied := 0
	for _, event := range events {
		if event.seq <= b.pos {
			continue
		}
		b.fold = append(b.fold, event)
		b.pos = event.seq
		applied++
	}
	if covered > b.pos {
		b.pos = covered
	}
	b.c.Repairs++
	b.c.RepairApplied += applied
	b.lastRepair = fmt.Sprintf("from %d via %s: %d events, covered %d", from, replica.Name, applied, covered)
	return nil
}

// ---- a socket that can stop being read --------------------------------------

type stallSoakGate struct {
	mu      sync.Mutex
	stalled bool
	wake    chan struct{}
}

func newStallSoakGate() *stallSoakGate {
	g := &stallSoakGate{wake: make(chan struct{})}
	close(g.wake)
	return g
}

func (g *stallSoakGate) stall() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.stalled {
		g.stalled, g.wake = true, make(chan struct{})
	}
}

func (g *stallSoakGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stalled {
		g.stalled = false
		close(g.wake)
	}
}

func (g *stallSoakGate) wait() {
	g.mu.Lock()
	wake := g.wake
	g.mu.Unlock()
	<-wake
}

type stallSoakConn struct {
	net.Conn
	gate *stallSoakGate
}

func (c *stallSoakConn) Read(p []byte) (int, error) {
	c.gate.wait()
	return c.Conn.Read(p)
}
