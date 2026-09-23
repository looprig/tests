//go:build integration && soak

// This file is runbook 07's I3.2: the NON-RACE 5,000 ClientLink SOAK, and
// acceptance row AC17 ("5,000 ClientLinks bounded CPU/RSS/FD/queues and no
// enduring loss").
//
//	ulimit -n 65536
//	LOOPRIG_SOAK=1 GOWORK=off GOTOOLCHAIN=go1.26.8 go test -tags 'integration soak' \
//	    -run '^TestFactoryHostClientLinkSoak5000$' -count=1 -timeout=60m .
//
// It is behind the `soak` tag AND LOOPRIG_SOAK=1 because it is a resource
// measurement, not a correctness lane: it holds this many sockets and runs for
// many minutes. It must never run under -race -- the race detector multiplies
// memory and CPU and would measure itself (runbook 07 step 6).
//
// # The shape
//
//   - SERVER PROCESS (this test): the durable plane, TWO REAL pooled Hosts
//     (host.Compose, one real harness rig per tenant) and TWO REAL Factory
//     replicas (factory.New), each served over loopback TCP. Sessions of two
//     tenants are placed by Factory's own pooled placement across both Hosts.
//   - CLIENT PROCESS (factory_host_soak_client_test.go, the same test binary
//     re-executed): 5,000 REAL centrifuge-go ClientLink connections, one
//     browser-shaped client each, interleaved across the replicas and the
//     sessions. It is a separate process so the server's CPU, RSS, goroutines
//     and file descriptors measure Factories and Hosts, not the clients.
//
// The product stream is a DurableTail world's: every enduring record is
// appended to a real SessionStore journal before it is published, stamped with
// its commit time, so a browser that misses a record live can read it back
// through a real Factory's journal route, and a browser in the other process
// can measure commit-to-delivery latency from the record alone.
//
// # The phases, per round (repeated until LOOPRIG_SOAK_STEADY has elapsed)
//
//	idle         no records for a while; connections held (pings only)
//	streaming    records committed at LOOPRIG_SOAK_RATE across every session
//	refresh      a fifth of the browsers reconnect, with their cursor, to
//	             ANOTHER replica (a reload behind a balancer with no affinity)
//	command RPC  session.input commands over the ClientLink RPC
//	slow consumer  LOOPRIG_SOAK_SLOW browsers stop READING THEIR SOCKETS while
//	             their sessions are burst above the per-connection budget
//	HostLink repair  every HostLink to one Host is severed at TCP
//	Factory restart  one replica is stopped and replaced; its browsers
//	             reconnect to the live replicas with their cursors
//	checkpoint   streaming paused; every browser must hold through its
//	             session's tip; resources sampled after a GC
//
// # What it requires (and fails on)
//
//	S1  all N connections established and, at every checkpoint and at the
//	    end, every browser covered through its session's committed tip;
//	S2  NO ENDURING LOSS: every browser's fold is EXACTLY its session's
//	    committed history -- each identity once, in order, nothing else --
//	    across every reconnect, reset and repair. Wire duplicates are allowed
//	    and counted; a silent gap, a stray record or a failed repair is not;
//	S3  every command ACKNOWLEDGED over the RPC settles, and an applied one
//	    was applied once (one application prefix, one disposition frame, at
//	    most one turn effect -- the runtime's own journal);
//	S4  bounded: server goroutines and FDs do not grow across checkpoints,
//	    heap growth per committed record stays under a bound that a
//	    per-delivery leak would exceed, and each Host's resident sessions and
//	    command queue stay within their configured bounds;
//	S5  the slow-consumer blast radius: every stalled browser is closed by
//	    the server at a bound -- the per-connection queue budget
//	    (DisconnectSlow, 3008) or the ClientLink write timeout, whichever it
//	    reaches first -- and repairs exactly; no other browser is closed;
//	S6  no leak after shutdown: when the clients leave, the server returns to
//	    its pre-client goroutine and FD baseline; when the fleet stops,
//	    goroutines return to the case's baseline (the centrifuge eagle leak
//	    accounted as in I3.1); the client process exits 0.
//
// Every sample, checkpoint and percentile is written under LOOPRIG_SOAK_OUT
// (resources_server.csv, resources_client.csv, checkpoints.csv, summary.json,
// environment.json) for the release review.

package tests

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// ---- configuration -------------------------------------------------------------

type soakConfig struct {
	connections int
	sessions    int
	hosts       int
	replicas    int
	steady      time.Duration
	rate        int
	pad         int
	slow        int
	burst       int
	burstPad    int
	burstRate   int
	// control bursts WITHOUT stalling anyone: the experiment that tells a
	// peer closed by the stall from a peer closed by the burst's own volume.
	control    bool
	rpcs       int
	out        string
	sampleEach time.Duration
}

const soakRunbookConnections = 5000

func soakConfigFromEnv(t *testing.T) soakConfig {
	t.Helper()
	cfg := soakConfig{
		connections: soakRunbookConnections,
		sessions:    250,
		hosts:       2,
		replicas:    2,
		steady:      10 * time.Minute,
		rate:        250,
		pad:         256,
		slow:        10,
		burst:       2500,
		burstPad:    4096,
		burstRate:   125,
		rpcs:        500,
		sampleEach:  2 * time.Second,
	}
	integer := func(name string, into *int, minimum int) {
		if raw := os.Getenv(name); raw != "" {
			v, err := strconv.Atoi(raw)
			if err != nil || v < minimum {
				t.Fatalf("%s=%q: want an integer >= %d", name, raw, minimum)
			}
			*into = v
		}
	}
	integer("LOOPRIG_SOAK_CONNECTIONS", &cfg.connections, 1)
	integer("LOOPRIG_SOAK_SESSIONS", &cfg.sessions, 2)
	integer("LOOPRIG_SOAK_HOSTS", &cfg.hosts, 2)
	integer("LOOPRIG_SOAK_REPLICAS", &cfg.replicas, 2)
	integer("LOOPRIG_SOAK_RATE", &cfg.rate, 1)
	integer("LOOPRIG_SOAK_PAD", &cfg.pad, 0)
	integer("LOOPRIG_SOAK_SLOW", &cfg.slow, 0)
	integer("LOOPRIG_SOAK_BURST", &cfg.burst, 1)
	integer("LOOPRIG_SOAK_BURST_PAD", &cfg.burstPad, 0)
	integer("LOOPRIG_SOAK_BURST_RATE", &cfg.burstRate, 1)
	cfg.control = os.Getenv("LOOPRIG_SOAK_BURST_CONTROL") == "1"
	integer("LOOPRIG_SOAK_RPCS", &cfg.rpcs, 0)
	if raw := os.Getenv("LOOPRIG_SOAK_STEADY"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("LOOPRIG_SOAK_STEADY=%q: %v", raw, err)
		}
		cfg.steady = d
	}
	if cfg.slow > cfg.sessions || cfg.slow > cfg.connections {
		t.Fatalf("LOOPRIG_SOAK_SLOW=%d must not exceed the sessions (%d) or connections (%d)", cfg.slow, cfg.sessions, cfg.connections)
	}
	cfg.out = os.Getenv("LOOPRIG_SOAK_OUT")
	if cfg.out == "" {
		cfg.out = filepath.Join(os.TempDir(), "looprig-soak-"+time.Now().Format("20060102-150405"))
	}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		t.Fatalf("creating %s: %v", cfg.out, err)
	}
	return cfg
}

// ---- preflight (H6) ---------------------------------------------------------------

type soakEnvironment struct {
	Started          string   `json:"started"`
	GoVersion        string   `json:"go_version"`
	GOOS             string   `json:"goos"`
	GOARCH           string   `json:"goarch"`
	CPUs             int      `json:"cpus"`
	GOMAXPROCS       int      `json:"gomaxprocs"`
	MemoryBytes      uint64   `json:"memory_bytes"`
	NofileSoft       uint64   `json:"nofile_soft"`
	NofileHard       uint64   `json:"nofile_hard"`
	MaxFilesPerProc  string   `json:"kern_maxfilesperproc"`
	PortRange        [2]int   `json:"ephemeral_port_range"`
	Somaxconn        string   `json:"kern_ipc_somaxconn"`
	LoadAtStart      string   `json:"load_average_at_start"`
	Machine          string   `json:"machine"`
	Config           []string `json:"config"`
	ModulePins       []string `json:"module_pins"`
	ConnectionsBelow bool     `json:"connections_below_runbook"`
}

func soakSysctl(name string) string {
	out, err := exec.Command("sysctl", "-n", name).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// soakPreflight is runbook step 1. A machine that cannot hold the count is
// reported as the H6 GATE and the case stops: the runbook forbids lowering
// the count and calling it equivalent.
func soakPreflight(t *testing.T, cfg soakConfig) soakEnvironment {
	t.Helper()
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatalf("H6: reading RLIMIT_NOFILE: %v", err)
	}
	env := soakEnvironment{
		Started: time.Now().Format(time.RFC3339), GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		CPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		NofileSoft: limit.Cur, NofileHard: limit.Max,
		MaxFilesPerProc: soakSysctl("kern.maxfilesperproc"), Somaxconn: soakSysctl("kern.ipc.somaxconn"),
		LoadAtStart: soakSysctl("vm.loadavg"), Machine: soakSysctl("machdep.cpu.brand_string"),
		ConnectionsBelow: cfg.connections < soakRunbookConnections,
	}
	env.MemoryBytes, _ = strconv.ParseUint(soakSysctl("hw.memsize"), 10, 64)
	env.PortRange[0], _ = strconv.Atoi(soakSysctl("net.inet.ip.portrange.first"))
	env.PortRange[1], _ = strconv.Atoi(soakSysctl("net.inet.ip.portrange.last"))
	env.Config = []string{
		fmt.Sprintf("connections=%d", cfg.connections), fmt.Sprintf("sessions=%d", cfg.sessions),
		fmt.Sprintf("hosts=%d", cfg.hosts), fmt.Sprintf("replicas=%d", cfg.replicas),
		fmt.Sprintf("steady=%s", cfg.steady), fmt.Sprintf("rate=%d/s", cfg.rate), fmt.Sprintf("pad=%dB", cfg.pad),
		fmt.Sprintf("slow=%d", cfg.slow), fmt.Sprintf("burst=%dx%dB@%d/s/session", cfg.burst, cfg.burstPad, cfg.burstRate), fmt.Sprintf("rpcs/round=%d", cfg.rpcs),
	}
	if info, ok := debugModules(); ok {
		env.ModulePins = info
	}

	// Each process holds one end of every ClientLink, plus HTTP repair
	// connections, HostLinks and listeners.
	need := uint64(cfg.connections + 4096)
	if limit.Cur < need {
		t.Fatalf("H6 GATE: RLIMIT_NOFILE soft limit is %d, below the %d this %d-connection soak needs per process. Raise it (ulimit -n 65536) -- the count is not lowered",
			limit.Cur, need, cfg.connections)
	}
	if ports := env.PortRange[1] - env.PortRange[0] + 1; env.PortRange[0] > 0 && ports < 2*cfg.connections {
		t.Fatalf("H6 GATE: the ephemeral port range holds %d ports, below twice the %d connections a soak with reconnects needs", ports, cfg.connections)
	}
	if env.MemoryBytes > 0 && env.MemoryBytes < 8<<30 {
		t.Fatalf("H6 GATE: %d bytes of memory is below the 8 GiB this soak is sized for", env.MemoryBytes)
	}
	if cfg.connections != soakRunbookConnections {
		t.Logf("running at %d connections, which is NOT the runbook's %d: this is not the AC17 result", cfg.connections, soakRunbookConnections)
	}
	return env
}

// ---- resource sampling ------------------------------------------------------------

// soakSample is one reading of one process.
type soakSample struct {
	At         time.Time
	RSSKB      int64
	CPUPercent float64
	Goroutines int
	FDs        int
	HeapInuse  uint64
	HeapSys    uint64
	Extra      map[string]float64
}

// soakFDs counts this process's open descriptors. Names only: an lstat per
// entry races descriptors closing under the listing and fails the read.
func soakFDs() int {
	dir, err := os.Open("/dev/fd")
	if err != nil {
		return -1
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return -1
	}
	return len(names) - 1 // the directory's own descriptor
}

func soakRSS() int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return -1
	}
	v, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	return v
}

func soakCPUTime() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

// soakSampleSelf samples THIS process every interval into a CSV until stop.
// extra, when set, adds named columns; its key set is fixed by its first call.
func soakSampleSelf(path string, every time.Duration, stop <-chan struct{}, extra func() map[string]float64) []soakSample {
	if every <= 0 {
		every = 2 * time.Second
	}
	file, err := os.Create(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	var samples []soakSample
	var keys []string
	header := false
	lastCPU, lastAt := soakCPUTime(), time.Now()
	start := lastAt
	for {
		select {
		case <-stop:
			return samples
		case <-time.After(every):
		}
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		now, cpu := time.Now(), soakCPUTime()
		s := soakSample{
			At: now, RSSKB: soakRSS(), Goroutines: runtime.NumGoroutine(), FDs: soakFDs(),
			HeapInuse: mem.HeapInuse, HeapSys: mem.HeapSys,
			CPUPercent: 100 * float64(cpu-lastCPU) / float64(now.Sub(lastAt)),
		}
		lastCPU, lastAt = cpu, now
		if extra != nil {
			s.Extra = extra()
		}
		if !header {
			header = true
			for k := range s.Extra {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			_ = writer.Write(append([]string{"time", "elapsed_s", "rss_kb", "cpu_percent", "goroutines", "fds", "heap_inuse_bytes", "heap_sys_bytes"}, keys...))
		}
		row := []string{now.Format(time.RFC3339Nano), fmt.Sprintf("%.1f", now.Sub(start).Seconds()),
			strconv.FormatInt(s.RSSKB, 10), fmt.Sprintf("%.1f", s.CPUPercent), strconv.Itoa(s.Goroutines),
			strconv.Itoa(s.FDs), strconv.FormatUint(s.HeapInuse, 10), strconv.FormatUint(s.HeapSys, 10)}
		for _, k := range keys {
			row = append(row, strconv.FormatFloat(s.Extra[k], 'f', -1, 64))
		}
		_ = writer.Write(row)
		writer.Flush()
		samples = append(samples, s)
	}
}

// soakResourceSummary is steady and peak of one process's samples.
type soakResourceSummary struct {
	Samples           int     `json:"samples"`
	RSSMBMedian       float64 `json:"rss_mb_median"`
	RSSMBPeak         float64 `json:"rss_mb_peak"`
	CPUPercentMedian  float64 `json:"cpu_percent_median"`
	CPUPercentP90     float64 `json:"cpu_percent_p90"`
	CPUPercentPeak    float64 `json:"cpu_percent_peak"`
	GoroutinesMedian  float64 `json:"goroutines_median"`
	GoroutinesPeak    float64 `json:"goroutines_peak"`
	FDsMedian         float64 `json:"fds_median"`
	FDsPeak           float64 `json:"fds_peak"`
	HeapInuseMBMedian float64 `json:"heap_inuse_mb_median"`
	HeapInuseMBPeak   float64 `json:"heap_inuse_mb_peak"`
}

func soakSummarise(rows [][]float64) soakResourceSummary {
	// rows: rss_kb, cpu, goroutines, fds, heap_inuse
	col := func(i int) []float64 {
		out := make([]float64, 0, len(rows))
		for _, r := range rows {
			out = append(out, r[i])
		}
		sort.Float64s(out)
		return out
	}
	q := func(v []float64, p float64) float64 {
		if len(v) == 0 {
			return 0
		}
		return v[int(p*float64(len(v)-1))]
	}
	rss, cpu, gor, fds, heap := col(0), col(1), col(2), col(3), col(4)
	return soakResourceSummary{
		Samples:     len(rows),
		RSSMBMedian: q(rss, 0.5) / 1024, RSSMBPeak: q(rss, 1) / 1024,
		CPUPercentMedian: q(cpu, 0.5), CPUPercentP90: q(cpu, 0.9), CPUPercentPeak: q(cpu, 1),
		GoroutinesMedian: q(gor, 0.5), GoroutinesPeak: q(gor, 1),
		FDsMedian: q(fds, 0.5), FDsPeak: q(fds, 1),
		HeapInuseMBMedian: q(heap, 0.5) / (1 << 20), HeapInuseMBPeak: q(heap, 1) / (1 << 20),
	}
}

func soakRowsOf(samples []soakSample) [][]float64 {
	out := make([][]float64, 0, len(samples))
	for _, s := range samples {
		out = append(out, []float64{float64(s.RSSKB), s.CPUPercent, float64(s.Goroutines), float64(s.FDs), float64(s.HeapInuse)})
	}
	return out
}

// soakRowsFromCSV reads a process's resources CSV back (the client's).
func soakRowsFromCSV(path string) [][]float64 {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	records, err := csv.NewReader(file).ReadAll()
	if err != nil || len(records) < 2 {
		return nil
	}
	var out [][]float64
	for _, r := range records[1:] {
		v := func(i int) float64 { f, _ := strconv.ParseFloat(r[i], 64); return f }
		out = append(out, []float64{v(2), v(3), v(4), v(5), v(6)})
	}
	return out
}

// ---- a listener that measures a Factory's ClientLink ----------------------------

type soakCountingListener struct {
	net.Listener
	written  atomic.Int64
	active   atomic.Int64
	accepted atomic.Int64
}

func (l *soakCountingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.active.Add(1)
	l.accepted.Add(1)
	return &soakCountingConn{Conn: conn, owner: l}, nil
}

type soakCountingConn struct {
	net.Conn
	owner *soakCountingListener
	once  sync.Once
}

func (c *soakCountingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.owner.written.Add(int64(n))
	return n, err
}

func (c *soakCountingConn) Close() error {
	c.once.Do(func() { c.owner.active.Add(-1) })
	return c.Conn.Close()
}

// ---- the child process ---------------------------------------------------------------

type soakChild struct {
	cmd     *exec.Cmd
	encoder *json.Encoder
	decoder *json.Decoder
	control *os.File
	done    chan error
}

func startSoakChild(t *testing.T, spec soakChildSpec) *soakChild {
	t.Helper()
	controlR, controlW, err := os.Pipe()
	if err != nil {
		t.Fatalf("control pipe: %v", err)
	}
	reportR, reportW, err := os.Pipe()
	if err != nil {
		t.Fatalf("report pipe: %v", err)
	}
	logFile, err := os.Create(filepath.Join(spec.OutDir, "client.log"))
	if err != nil {
		t.Fatalf("client log: %v", err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestSoakClientFleetChild$", "-test.count=1", "-test.timeout=0", "-test.v")
	cmd.Env = append(os.Environ(), soakChildEnv+"=1", "GOMAXPROCS="+strconv.Itoa(runtime.NumCPU()))
	cmd.ExtraFiles = []*os.File{controlR, reportW}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the soak's client process: %v", err)
	}
	_ = controlR.Close()
	_ = reportW.Close()
	child := &soakChild{
		cmd: cmd, encoder: json.NewEncoder(controlW), decoder: json.NewDecoder(bufio.NewReaderSize(reportR, 1<<20)),
		control: controlW, done: make(chan error, 1),
	}
	go func() {
		child.done <- cmd.Wait()
		_ = logFile.Close()
	}()
	t.Cleanup(func() {
		select {
		case <-child.done:
		default:
			_ = cmd.Process.Kill()
		}
	})
	if err := child.encoder.Encode(spec); err != nil {
		t.Fatalf("sending the soak spec: %v", err)
	}
	return child
}

// call runs one operation in the client process and decodes its reply into
// into. A client process that does not answer within timeout fails the case.
func (c *soakChild) call(t *testing.T, op soakOp, timeout time.Duration, into any) {
	t.Helper()
	if err := c.encoder.Encode(op); err != nil {
		t.Fatalf("sending %s to the client process: %v", op.Op, err)
	}
	type answer struct {
		reply soakReply
		err   error
	}
	got := make(chan answer, 1)
	go func() {
		var reply soakReply
		err := c.decoder.Decode(&reply)
		got <- answer{reply, err}
	}()
	select {
	case a := <-got:
		if a.err != nil {
			t.Fatalf("the client process's reply to %s: %v", op.Op, a.err)
		}
		if !a.reply.OK {
			t.Fatalf("the client process refused %s: %s", op.Op, a.reply.Error)
		}
		if into != nil && len(a.reply.Data) > 0 {
			if err := json.Unmarshal(a.reply.Data, into); err != nil {
				t.Fatalf("decoding the reply to %s: %v", op.Op, err)
			}
		}
	case err := <-c.done:
		t.Fatalf("the client process EXITED during %s: %v (see client.log)", op.Op, err)
	case <-time.After(timeout):
		t.Fatalf("the client process did not answer %s within %s", op.Op, timeout)
	}
}

// ---- the soak ---------------------------------------------------------------------------

type soakReplica struct {
	name     string
	f        *orchestrationtest.PooledFactory
	listener *soakCountingListener
}

type soakAwait struct {
	Covered        int      `json:"covered"`
	Behind         int      `json:"behind"`
	BehindExamples []string `json:"behind_examples"`
	Took           soakDist `json:"took"`
}

type soakStats struct {
	Connected              int                 `json:"connected"`
	Browsers               int                 `json:"browsers"`
	Counters               soakCounters        `json:"counters"`
	SlowCounters           soakCounters        `json:"slow_counters"`
	BurstPeerCounters      soakCounters        `json:"burst_peer_counters"`
	DeliveryLatency        soakDist            `json:"delivery_latency"`
	DeliveryLatencyWindows map[string]soakDist `json:"delivery_latency_windows"`
	SlowDeliveryLatency    soakDist            `json:"slow_delivery_latency"`
	Connect                soakDist            `json:"connect"`
	Join                   soakDist            `json:"join"`
	Reconnect              map[string]soakDist `json:"reconnect"`
}

type soakCheckpoint struct {
	Name           string   `json:"name"`
	ElapsedS       float64  `json:"elapsed_s"`
	Committed      int64    `json:"committed"`
	BodyBytes      int64    `json:"body_bytes"`
	Goroutines     int      `json:"goroutines"`
	FDs            int      `json:"fds"`
	HeapInuseMB    float64  `json:"heap_inuse_mb"`
	RSSMB          float64  `json:"rss_mb"`
	ServerConns    int64    `json:"server_clientlink_conns"`
	Coverage       soakDist `json:"coverage_ms"`
	HostBytesOut   int64    `json:"host_bytes_out"`
	ClientLinkOut  int64    `json:"clientlink_bytes_out"`
	LiveDeliveries int      `json:"live_deliveries"`
}

type soakRPCResult struct {
	Sent  int `json:"sent"`
	Acked []struct {
		Tenant  sessionwire.TenantID  `json:"tenant"`
		Session sessionwire.SessionID `json:"session"`
		Command sessionwire.CommandID `json:"command"`
		Status  string                `json:"status"`
	} `json:"acked"`
	Refusals          map[string]int `json:"refusals"`
	TransportFailures int            `json:"transport_failures"`
	Latency           soakDist       `json:"latency"`
	TookMs            int64          `json:"took_ms"`
}

type soak struct {
	t     *testing.T
	ctx   context.Context
	cfg   soakConfig
	world *orchestrationtest.PooledWorld
	start time.Time

	mu       sync.Mutex
	replicas []*soakReplica
	retired  []*soakReplica
	started  int
	hosts    []*orchestrationtest.PooledHost
	capacity uint64
	queue    int

	sessions []soakSessionSpec
	child    *soakChild

	streaming atomic.Bool
	committed atomic.Int64
	// bodyBytes approximates the durable bytes committed: every record's
	// body, which the in-process durable plane legitimately keeps.
	bodyBytes atomic.Int64
	commitMu  sync.Mutex

	checkpoints []soakCheckpoint
	phases      []map[string]any
	rpcAcks     []soakRPCResult
	q1          atomic.Int64
	q1Examples  []string
	q1Mu        sync.Mutex
	peakQueue   atomic.Int64
	peakHeld    atomic.Int64
}

func TestFactoryHostClientLinkSoak5000(t *testing.T) {
	if os.Getenv("LOOPRIG_SOAK") != "1" {
		t.Skip("the 5,000 ClientLink soak runs for many minutes and holds ~10,000 sockets; set LOOPRIG_SOAK=1")
	}
	cfg := soakConfigFromEnv(t)
	env := soakPreflight(t, cfg)
	soakWriteJSON(t, filepath.Join(cfg.out, "environment.json"), env)
	t.Logf("I3.2 soak: %v; evidence in %s; load at start %s", env.Config, cfg.out, env.LoadAtStart)

	budget := cfg.steady + 40*time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		DurableTail: true, TailPadBytes: cfg.pad, TailStampBodies: true, TailQuiet: true,
	})
	baseline, baselineEagles := runtime.NumGoroutine(), stressEagles()

	s := &soak{t: t, ctx: ctx, cfg: cfg, world: world, start: time.Now(), queue: 64}
	// Capacity is just above an even split, so Factory's placement has to use
	// BOTH Hosts and every session stays resident (no warm release: this case
	// measures links, not lifecycle -- I3.1 owns that).
	s.capacity = uint64((cfg.sessions+cfg.hosts-1)/cfg.hosts + cfg.sessions/20 + 1)
	for i := range cfg.hosts {
		h := orchestrationtest.StartSizedPooledHost(t, ctx, world, orchestrationtest.PooledHostConfig{
			ID: sessionwire.HostID(fmt.Sprintf("soak-host-%d", i)), Generation: 1,
			Capacity: s.capacity, WarmTTL: time.Hour,
			MaxBindingsPerLink: int(s.capacity) * 2, MaxBindings: int(s.capacity) * (cfg.replicas + 2) * 2,
			CommandQueueSize: s.queue,
		})
		s.hosts = append(s.hosts, h)
	}
	for _, h := range s.hosts {
		orchestrationtest.AwaitAdvertised(t, world, h.ID)
	}
	for range cfg.replicas {
		s.replicas = append(s.replicas, s.startReplica())
	}

	s.createSessions()
	s.placementSpread()

	// ---- the pre-client baseline: the fleet serving, no ClientLink open ----
	runtime.GC()
	time.Sleep(2 * time.Second)
	preClientGoroutines, preClientFDs := runtime.NumGoroutine(), soakFDs()
	t.Logf("I3.2 pre-client baseline: %d goroutines, %d FDs", preClientGoroutines, preClientFDs)

	stopSampler := make(chan struct{})
	var serverSamples []soakSample
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		serverSamples = soakSampleSelf(filepath.Join(cfg.out, "resources_server.csv"), cfg.sampleEach, stopSampler, s.extraMetrics)
	}()
	stopStream := make(chan struct{})
	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		s.stream(stopStream)
	}()

	// ---- the client process -------------------------------------------------
	spec := soakChildSpec{Sessions: s.sessions, Connections: cfg.connections, Slow: cfg.slow, OutDir: cfg.out, SampleEach: cfg.sampleEach.String()}
	for _, r := range s.liveReplicas() {
		spec.Replicas = append(spec.Replicas, soakReplicaSpec{Name: r.name, URL: r.f.BaseURL})
	}
	s.child = startSoakChild(t, spec)
	var connected struct {
		Connected int      `json:"connected"`
		Failed    int      `json:"failed"`
		Failures  []string `json:"failures"`
		TookMs    int64    `json:"took_ms"`
		Connect   soakDist `json:"connect"`
		Join      soakDist `json:"join"`
	}
	s.child.call(t, soakOp{Op: "connect"}, 20*time.Minute, &connected)
	t.Logf("I3.2 connect: %d/%d browsers connected in %dms (failed %d %v); connect %+v; join %+v",
		connected.Connected, cfg.connections, connected.TookMs, connected.Failed, connected.Failures, connected.Connect, connected.Join)
	s.phase("connect", map[string]any{"result": connected})
	if connected.Failed != 0 {
		t.Errorf("S1: %d of %d ClientLinks could not be established: %v", connected.Failed, cfg.connections, connected.Failures)
	}
	s.checkpoint("connected")

	// ---- the rounds -----------------------------------------------------------
	for round := 1; time.Since(s.start) < cfg.steady || round == 1; round++ {
		if ctx.Err() != nil {
			t.Fatalf("the soak outlived its bound during round %d", round)
		}
		s.round(round)
	}

	// ---- the end: every browser holds exactly its session's history ------------
	s.streaming.Store(false)
	s.checkpoint("final")
	s.verify()
	s.verifyCommands()
	var final soakStats
	s.child.call(t, soakOp{Op: "stats"}, 2*time.Minute, &final)
	s.judgeCounters(final)

	close(stopStream)
	<-streamDone

	// ---- S6: the clients leave; the server returns to its pre-client baseline ---
	s.child.call(t, soakOp{Op: "close"}, 5*time.Minute, nil)
	s.awaitServerReturns(preClientGoroutines, preClientFDs)
	s.child.call(t, soakOp{Op: "exit"}, time.Minute, nil)
	select {
	case err := <-s.child.done:
		if err != nil {
			t.Errorf("S6: the client process exited %v (see client.log)", err)
		}
	case <-time.After(2 * time.Minute):
		t.Errorf("S6: the client process did not exit")
	}
	close(stopSampler)
	<-samplerDone

	s.judgeGrowth()
	summary := s.summary(env, serverSamples, final)
	soakWriteJSON(t, filepath.Join(cfg.out, "summary.json"), summary)
	s.writeCheckpoints()

	// ---- S6: the fleet stops; goroutines return to the case's baseline -------
	s.shutdown()
	soakGoroutinesSettle(t, baseline, baselineEagles, s.started+cfg.hosts*2)
	t.Logf("I3.2 evidence written to %s", cfg.out)
}

func soakWriteJSON(t *testing.T, path string, v any) {
	t.Helper()
	encoded, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Errorf("encoding %s: %v", path, err)
		return
	}
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		t.Errorf("writing %s: %v", path, err)
	}
}

func (s *soak) startReplica() *soakReplica {
	s.mu.Lock()
	s.started++
	name := fmt.Sprintf("soak-replica-%d", s.started)
	s.mu.Unlock()
	logs, err := os.Create(filepath.Join(s.cfg.out, name+".jsonl"))
	if err != nil {
		s.t.Fatalf("opening %s's log: %v", name, err)
	}
	s.t.Cleanup(func() { _ = logs.Close() })
	r := &soakReplica{name: name}
	r.f = orchestrationtest.StartPooledFactoryWith(s.t, s.ctx, s.world, orchestrationtest.PooledFactoryConfig{
		Replica: name, Logs: logs,
		Listener: func(inner net.Listener) net.Listener {
			r.listener = &soakCountingListener{Listener: inner}
			return r.listener
		},
	})
	return r
}

func (s *soak) liveReplicas() []*soakReplica {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*soakReplica(nil), s.replicas...)
}

// createSessions creates every session through the REST route of the
// replicas in turn, and waits until each create has APPLIED and its runtime
// has launched -- the product journal is keyed by the runtime's id.
func (s *soak) createSessions() {
	tenants := []sessionwire.TenantID{orchestrationtest.PooledTenantA, orchestrationtest.PooledTenantB}
	for i := range s.cfg.sessions {
		s.sessions = append(s.sessions, soakSessionSpec{Tenant: tenants[i%2], Session: sessionwire.SessionID(fmt.Sprintf("soak-%d", i))})
	}
	start := time.Now()
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	var failed atomic.Int64
	replicas := s.liveReplicas()
	for i, session := range s.sessions {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			command := "soak-create-" + string(session.Session)
			for attempt := 0; attempt < 10; attempt++ {
				status, body, err := replicas[(i+attempt)%len(replicas)].f.PostRaw(s.ctx, session.Tenant, "/v1/sessions", sessionwire.CreateRequest{
					CommandEnvelope: orchestrationtest.PooledEnvelope(command),
					SessionID:       session.Session,
					AgentID:         orchestrationtest.PooledAgent,
					Blocks:          json.RawMessage(`[{"type":"text","text":"soak"}]`),
				})
				if err == nil && status >= 200 && status < 300 {
					return
				}
				if err == nil && status != http.StatusServiceUnavailable && status != http.StatusTooManyRequests {
					s.t.Logf("create %s answered %d: %s", session.Session, status, body)
					break
				}
				time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
			}
			failed.Add(1)
		}()
	}
	wg.Wait()
	if failed.Load() != 0 {
		s.t.Fatalf("%d session creates were never acknowledged", failed.Load())
	}
	orchestrationtest.PooledWait(s.t, "every create applied and its runtime launched", 10*time.Minute, func() bool {
		for _, session := range s.sessions {
			if s.world.CommandState(s.ctx, session.Tenant, session.Session, sessionwire.CommandID("soak-create-"+string(session.Session))) != sessionstore.InboxStateApplied ||
				s.world.Tails.RuntimeSessionID(session.Tenant, session.Session) == "" {
				return false
			}
		}
		return true
	})
	// One record per session, so every history is non-empty before a browser
	// joins.
	for _, session := range s.sessions {
		s.commit(session)
	}
	s.t.Logf("I3.2 %d sessions created, applied and launched in %v", len(s.sessions), time.Since(start).Round(time.Millisecond))
}

// placementSpread requires Factory's placement to have used every Host
// substantially: the sessions are interleaved across Hosts, not piled on one.
func (s *soak) placementSpread() {
	var parts []string
	for _, h := range s.hosts {
		n := len(h.Rig.Creates())
		parts = append(parts, fmt.Sprintf("%s=%d", h.ID, n))
		if n < s.cfg.sessions/(2*s.cfg.hosts) {
			s.t.Errorf("placement put only %d of %d sessions on %s: the soak needs sessions interleaved across Hosts", n, s.cfg.sessions, h.ID)
		}
	}
	s.t.Logf("I3.2 placement: %s", strings.Join(parts, " "))
	s.phase("placement", map[string]any{"hosts": parts})
}

func (s *soak) commit(session soakSessionSpec) {
	s.world.Tails.Commit(session.Tenant, session.Session)
	s.committed.Add(1)
	s.bodyBytes.Add(int64(s.cfg.pad + soakBodyOverhead))
}

func (s *soak) commitPadded(session soakSessionSpec, pad int) {
	s.world.Tails.CommitPadded(session.Tenant, session.Session, pad)
	s.committed.Add(1)
	s.bodyBytes.Add(int64(pad + soakBodyOverhead))
}

// soakBodyOverhead is a record body's bytes beyond its pad: the tenant,
// ordinal and commit stamp.
const soakBodyOverhead = 100

// soakClientIdlePool is the client process's HTTP MaxIdleConns (see
// newSoakFleet): the most idle journal-repair connections it may keep open.
const soakClientIdlePool = 2048

// stream commits records round-robin across every session at the configured
// rate while streaming is on.
func (s *soak) stream(stop <-chan struct{}) {
	const tick = 20 * time.Millisecond
	perTick := float64(s.cfg.rate) * tick.Seconds()
	owed := 0.0
	next := 0
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		if !s.streaming.Load() {
			owed = 0
			continue
		}
		owed += perTick
		for ; owed >= 1; owed-- {
			s.commit(s.sessions[next%len(s.sessions)])
			next++
		}
	}
}

func (s *soak) extraMetrics() map[string]float64 {
	out := map[string]float64{"committed": float64(s.committed.Load())}
	s.mu.Lock()
	hosts := append([]*orchestrationtest.PooledHost(nil), s.hosts...)
	replicas := append([]*soakReplica(nil), s.replicas...)
	retired := append([]*soakReplica(nil), s.retired...)
	s.mu.Unlock()
	var conns, written float64
	for _, r := range append(replicas, retired...) {
		conns += float64(r.listener.active.Load())
		written += float64(r.listener.written.Load())
	}
	out["clientlink_server_conns"] = conns
	out["clientlink_bytes_out"] = written
	for i, h := range hosts {
		out[fmt.Sprintf("host%d_bytes_out", i)] = float64(h.BytesOut())
		samples, err := h.ScrapeMetrics()
		if err != nil {
			continue
		}
		held := orchestrationtest.SumMetric(samples, "host_sessions")
		depth := orchestrationtest.SumMetric(samples, "host_command_queue_depth")
		out[fmt.Sprintf("host%d_sessions", i)] = held
		out[fmt.Sprintf("host%d_command_queue_depth", i)] = depth
		out[fmt.Sprintf("host%d_command_lag", i)] = orchestrationtest.SumMetric(samples, "host_command_lag")
		if int64(depth) > s.peakQueue.Load() {
			s.peakQueue.Store(int64(depth))
		}
		if int64(held) > s.peakHeld.Load() {
			s.peakHeld.Store(int64(held))
		}
		if held > float64(s.capacity) || depth > float64(s.queue) {
			s.q1.Add(1)
			s.q1Mu.Lock()
			if len(s.q1Examples) < 5 {
				s.q1Examples = append(s.q1Examples, fmt.Sprintf("%s held=%.0f (cap %d) queue=%.0f (bound %d)", h.ID, held, s.capacity, depth, s.queue))
			}
			s.q1Mu.Unlock()
		}
	}
	return out
}

func (s *soak) phase(name string, data map[string]any) {
	data["phase"] = name
	data["elapsed_s"] = time.Since(s.start).Seconds()
	s.mu.Lock()
	s.phases = append(s.phases, data)
	s.mu.Unlock()
}

func (s *soak) mark(window string) {
	s.child.call(s.t, soakOp{Op: "mark", Window: window}, time.Minute, nil)
}

// round is one pass through every phase.
func (s *soak) round(n int) {
	t := s.t
	sleep := func(d time.Duration) {
		select {
		case <-time.After(d):
		case <-s.ctx.Done():
		}
	}
	t.Logf("I3.2 round %d at %v", n, time.Since(s.start).Round(time.Second))

	// idle: connections held, nothing committed.
	s.streaming.Store(false)
	s.mark(fmt.Sprintf("r%d-idle", n))
	sleep(30 * time.Second)
	s.phase("idle", map[string]any{"round": n})

	// streaming.
	s.mark(fmt.Sprintf("r%d-stream", n))
	s.streaming.Store(true)
	sleep(30 * time.Second)
	s.phase("stream", map[string]any{"round": n, "committed": s.committed.Load()})

	// refresh/reconnect under streaming.
	s.mark(fmt.Sprintf("r%d-refresh", n))
	var refreshed map[string]any
	s.child.call(t, soakOp{Op: "refresh", Fraction: 0.2, Seed: uint64(n)}, 15*time.Minute, &refreshed)
	s.phase("refresh", map[string]any{"round": n, "result": refreshed})
	t.Logf("I3.2 round %d refresh: moved=%v failed=%v took=%vms", n, refreshed["moved"], refreshed["failed"], refreshed["took_ms"])
	if f, _ := refreshed["failed"].(float64); f != 0 {
		t.Errorf("S1: round %d: %v refreshed browsers could not reconnect: %v", n, f, refreshed["failures"])
	}
	sleep(10 * time.Second)

	// command RPC under streaming.
	if s.cfg.rpcs > 0 {
		s.mark(fmt.Sprintf("r%d-rpc", n))
		var rpc soakRPCResult
		s.child.call(t, soakOp{Op: "rpc", Count: s.cfg.rpcs, Concurrency: 100, Prefix: fmt.Sprintf("soak-r%d-input", n), Seed: uint64(n)}, 15*time.Minute, &rpc)
		s.rpcAcks = append(s.rpcAcks, rpc)
		s.phase("rpc", map[string]any{"round": n, "sent": rpc.Sent, "acked": len(rpc.Acked), "refusals": rpc.Refusals,
			"transport_failures": rpc.TransportFailures, "latency": rpc.Latency, "took_ms": rpc.TookMs})
		t.Logf("I3.2 round %d command RPC: sent=%d acked=%d refusals=%v transport=%d latency %+v", n, rpc.Sent, len(rpc.Acked), rpc.Refusals, rpc.TransportFailures, rpc.Latency)
		if len(rpc.Acked) != rpc.Sent {
			t.Errorf("S3: round %d: %d of %d command RPCs were not acknowledged: %v (transport failures %d)", n, rpc.Sent-len(rpc.Acked), rpc.Sent, rpc.Refusals, rpc.TransportFailures)
		}
	}

	// slow consumer.
	if s.cfg.slow > 0 {
		s.slowConsumer(n)
	}

	// HostLink repair: sever every HostLink to one Host at TCP.
	s.mark(fmt.Sprintf("r%d-hostlink", n))
	h := s.hosts[(n-1)%len(s.hosts)]
	severed := h.Sever()
	t.Logf("I3.2 round %d: severed %d HostLink connections to %s", n, severed, h.ID)
	sleep(15 * time.Second)
	coverage := s.awaitAll(fmt.Sprintf("round %d after the HostLink sever", n), false)
	s.phase("hostlink_repair", map[string]any{"round": n, "host": h.ID, "severed": severed, "coverage": coverage})

	// Factory restart: stop one replica and replace it.
	s.mark(fmt.Sprintf("r%d-factory-restart", n))
	s.mu.Lock()
	victim := s.replicas[(n-1)%len(s.replicas)]
	s.mu.Unlock()
	took := time.Now()
	victim.f.Stop()
	replacement := s.startReplica()
	s.mu.Lock()
	for i, r := range s.replicas {
		if r == victim {
			s.replicas[i] = replacement
		}
	}
	s.retired = append(s.retired, victim)
	s.mu.Unlock()
	var moved map[string]any
	s.child.call(t, soakOp{Op: "replica_dead", Dead: victim.name, Replacement: &soakReplicaSpec{Name: replacement.name, URL: replacement.f.BaseURL}}, 15*time.Minute, &moved)
	t.Logf("I3.2 round %d: restarted Factory %s as %s in %v; moved=%v failed=%v", n, victim.name, replacement.name, time.Since(took).Round(time.Millisecond), moved["moved"], moved["failed"])
	if f, _ := moved["failed"].(float64); f != 0 {
		t.Errorf("S1: round %d: %v browsers of the restarted Factory could not reconnect: %v", n, f, moved["failures"])
	}
	s.phase("factory_restart", map[string]any{"round": n, "victim": victim.name, "replacement": replacement.name, "result": moved})
	sleep(15 * time.Second)

	s.checkpoint(fmt.Sprintf("round-%d", n))
}

// slowConsumer stalls the slow browsers' SOCKETS and bursts their sessions
// above the per-connection budget, then lets them read again.
func (s *soak) slowConsumer(n int) {
	t := s.t
	var before, after soakStats
	s.child.call(t, soakOp{Op: "stats"}, 2*time.Minute, &before)
	s.mark(fmt.Sprintf("r%d-slow", n))
	if !s.cfg.control {
		s.child.call(t, soakOp{Op: "stall"}, time.Minute, nil)
	}
	start := time.Now()
	// PACED at burstRate records a second per session: far above the
	// per-connection budget in total for a socket that is not read, and
	// within what a browser that IS reading takes in -- so a peer closed here
	// is harm from the stall, not a peer that could not keep up either.
	const tick = 40 * time.Millisecond
	perTick := max(1, int(float64(s.cfg.burstRate)*tick.Seconds()))
	for sent := 0; sent < s.cfg.burst; sent += perTick {
		began := time.Now()
		for i := range s.cfg.slow {
			for range perTick {
				s.commitPadded(s.sessions[i], s.cfg.burstPad)
			}
		}
		time.Sleep(tick - time.Since(began))
	}
	burstTook := time.Since(start)
	time.Sleep(5 * time.Second)
	s.child.call(t, soakOp{Op: "release"}, time.Minute, nil)
	coverage := s.awaitAll(fmt.Sprintf("round %d after the slow-consumer burst", n), false)
	s.child.call(t, soakOp{Op: "stats"}, 2*time.Minute, &after)
	// A stalled socket is closed by whichever server bound it reaches first:
	// the per-connection queue budget (DisconnectSlow, 3008, when the close
	// frame gets out) or the ClientLink WriteTimeout, which closes the TCP
	// connection under a write that cannot complete -- the client sees its
	// transport closed (centrifuge-go connecting code 1) with no frame. Either
	// is the queue being BOUNDED; a stalled browser the server never closed is
	// a queue that grew for as long as the stall lasted.
	closes := func(c soakCounters) int { return c.Codes["3008"] + c.Codes["1"] }
	slowCloses := closes(after.SlowCounters) - closes(before.SlowCounters)
	slowDisconnectSlow := after.SlowCounters.Codes["3008"] - before.SlowCounters.Codes["3008"]
	peerCloses := closes(after.BurstPeerCounters) - closes(before.BurstPeerCounters)
	otherCloses := (closes(after.Counters) - closes(after.SlowCounters) - closes(after.BurstPeerCounters)) -
		(closes(before.Counters) - closes(before.SlowCounters) - closes(before.BurstPeerCounters))
	t.Logf("I3.2 round %d slow consumer: %d stalled browsers, %d records of %dB each at %d/s per session in %v; stalled browsers closed by the server %d times (%d as DisconnectSlow 3008, the rest transport-closed by the write timeout); their sessions' other browsers closed %d times; every other browser %d times; coverage %+v",
		n, s.cfg.slow, s.cfg.burst, s.cfg.burstPad, s.cfg.burstRate, burstTook.Round(time.Millisecond), slowCloses, slowDisconnectSlow, peerCloses, otherCloses, coverage)
	if noPing := after.SlowCounters.Codes["2"] - before.SlowCounters.Codes["2"]; noPing != 0 && !s.cfg.control {
		t.Logf("I3.2 round %d: %d stalled browsers gave up on the server's pings themselves", n, noPing)
	}
	if s.cfg.control {
		t.Logf("I3.2 round %d CONTROL (nobody stalled): the burst alone closed %d designated-slow, %d burst-peer and %d other browsers", n, slowCloses, peerCloses, otherCloses)
	} else if slowCloses < s.cfg.slow {
		t.Errorf("S5: round %d: the server closed the %d stalled browsers only %d times: a stalled browser's queue was not bounded", n, s.cfg.slow, slowCloses)
	}
	if !s.cfg.control && (peerCloses != 0 || otherCloses != 0) {
		t.Errorf("S5: round %d: browsers that never stalled had their transport closed by the server: %d on the stalled browsers' own sessions, %d elsewhere", n, peerCloses, otherCloses)
	}
	s.phase("slow_consumer", map[string]any{"round": n, "stalled": s.cfg.slow, "burst_per_session": s.cfg.burst,
		"burst_took_ms": burstTook.Milliseconds(), "stalled_closed": slowCloses, "stalled_3008": slowDisconnectSlow, "burst_peer_closed": peerCloses, "other_closed": otherCloses, "coverage": coverage,
		"slow_counters_after": after.SlowCounters})
}

// awaitAll commits nothing (or, if probe, one record per session) and waits
// until every browser holds through its session's committed tip.
func (s *soak) awaitAll(what string, probe bool) soakDist {
	if probe {
		for _, session := range s.sessions {
			s.commit(session)
		}
	}
	targets := map[int]uint64{}
	for i, session := range s.sessions {
		committed := s.world.Tails.Committed(session.Tenant, session.Session)
		if len(committed) > 0 {
			targets[i] = committed[len(committed)-1].JournalSeq
		}
	}
	var result soakAwait
	s.child.call(s.t, soakOp{Op: "await", Targets: targets, Timeout: "180s"}, 5*time.Minute, &result)
	if result.Behind != 0 {
		s.t.Errorf("S1: %s: %d browsers are not covered through their session's tip after 180s: %v", what, result.Behind, result.BehindExamples)
	}
	return result.Took
}

// checkpoint pauses streaming, probes every session, waits for coverage and
// samples the server after a GC -- the comparable point S4 is judged at.
func (s *soak) checkpoint(name string) {
	wasStreaming := s.streaming.Swap(false)
	coverage := s.awaitAll("checkpoint "+name, true)
	var stats soakStats
	s.child.call(s.t, soakOp{Op: "stats"}, 2*time.Minute, &stats)
	runtime.GC()
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	var hostOut, linkOut, conns int64
	s.mu.Lock()
	for _, h := range s.hosts {
		hostOut += h.BytesOut()
	}
	for _, r := range append(append([]*soakReplica(nil), s.replicas...), s.retired...) {
		linkOut += r.listener.written.Load()
		conns += r.listener.active.Load()
	}
	s.mu.Unlock()
	c := soakCheckpoint{
		Name: name, ElapsedS: time.Since(s.start).Seconds(), Committed: s.committed.Load(), BodyBytes: s.bodyBytes.Load(),
		Goroutines: runtime.NumGoroutine(), FDs: soakFDs(), HeapInuseMB: float64(mem.HeapInuse) / (1 << 20),
		RSSMB: float64(soakRSS()) / 1024, ServerConns: conns, Coverage: coverage,
		HostBytesOut: hostOut, ClientLinkOut: linkOut, LiveDeliveries: stats.Counters.LiveApplied + stats.Counters.RepairOverlaps + stats.Counters.WireDuplicates,
	}
	s.checkpoints = append(s.checkpoints, c)
	s.t.Logf("I3.2 checkpoint %s: goroutines=%d fds=%d heap=%.1fMiB rss=%.1fMiB server-conns=%d committed=%d coverage %+v; client connected=%d",
		name, c.Goroutines, c.FDs, c.HeapInuseMB, c.RSSMB, c.ServerConns, c.Committed, coverage, stats.Connected)
	if stats.Connected != s.cfg.connections {
		s.t.Errorf("S1: checkpoint %s: %d of %d browsers are connected", name, stats.Connected, s.cfg.connections)
	}
	s.streaming.Store(wasStreaming)
}

// verify is S2: every browser's fold is exactly its session's history.
func (s *soak) verify() {
	histories := map[int][][2]uint64{}
	for i, session := range s.sessions {
		for _, committed := range s.world.Tails.Committed(session.Tenant, session.Session) {
			ordinal := soakOrdinal(committed.EventID, session.Session)
			histories[i] = append(histories[i], [2]uint64{committed.JournalSeq, ordinal})
		}
	}
	var result struct {
		Browsers       int      `json:"browsers"`
		Exact          int      `json:"exact"`
		Lost           int      `json:"lost"`
		Extra          int      `json:"extra"`
		Mismatched     int      `json:"mismatched"`
		FoldDuplicates int      `json:"fold_duplicates"`
		Examples       []string `json:"examples"`
	}
	s.child.call(s.t, soakOp{Op: "verify", Histories: histories}, 10*time.Minute, &result)
	s.t.Logf("I3.2 S2: %d of %d browsers hold EXACTLY their session's history; lost=%d extra=%d mismatched=%d fold-duplicates=%d",
		result.Exact, result.Browsers, result.Lost, result.Extra, result.Mismatched, result.FoldDuplicates)
	s.phase("verify", map[string]any{"result": result})
	if result.Exact != result.Browsers || result.Lost != 0 || result.Extra != 0 || result.Mismatched != 0 || result.FoldDuplicates != 0 {
		s.t.Errorf("S2: ENDURING LOSS OR DUPLICATION: %d of %d browsers are not exactly their history (lost %d, extra %d, mismatched %d, fold duplicates %d): %v",
			result.Browsers-result.Exact, result.Browsers, result.Lost, result.Extra, result.Mismatched, result.FoldDuplicates, result.Examples)
	}
	if failures := s.world.Tails.DurableFailures(); len(failures) != 0 {
		s.t.Errorf("the product journal refused %d appends (first %v): the histories are not what was published", len(failures), failures[0])
	}
}

// verifyCommands is S3 over every command RPC that was acknowledged.
func (s *soak) verifyCommands() {
	type key struct {
		tenant  sessionwire.TenantID
		session sessionwire.SessionID
	}
	bySession := map[key][]sessionwire.CommandID{}
	total := 0
	for _, round := range s.rpcAcks {
		for _, ack := range round.Acked {
			bySession[key{ack.Tenant, ack.Session}] = append(bySession[key{ack.Tenant, ack.Session}], ack.Command)
			total++
		}
	}
	start := time.Now()
	deadline := start.Add(5 * time.Minute)
	outcomes := map[string]int{}
	for k, commands := range bySession {
		for _, command := range commands {
			var state sessionstore.InboxState
			for {
				state = s.world.CommandState(s.ctx, k.tenant, k.session, command)
				if state == sessionstore.InboxStateApplied || state == sessionstore.InboxStateRejected || time.Now().After(deadline) {
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			outcomes[string(state)]++
			if state != sessionstore.InboxStateApplied && state != sessionstore.InboxStateRejected {
				s.t.Errorf("S3: acknowledged %s/%s is %q, not terminal, 5m after the soak", k.session, command, state)
			}
		}
	}
	// Applied at most once: the runtime's own journal.
	checked := 0
	for k, commands := range bySession {
		runtimeID := s.world.RuntimeSessionID(s.t, s.ctx, k.tenant, k.session)
		evidence := orchestrationtest.ReadCommandEvidence(s.t, s.world, k.tenant, runtimeID)
		for _, command := range commands {
			entry, err := s.world.Store.GetDispositionCommand(s.ctx, sessionstore.GetDispositionCommandRequest{TenantID: k.tenant, SessionID: k.session, CommandID: command})
			if err != nil {
				s.t.Errorf("S3: acknowledged %s/%s has no durable record: %v", k.session, command, err)
				continue
			}
			if n := len(evidence.ApplicationsOf(command)); n > 1 {
				s.t.Errorf("S3: %s/%s has %d application prefixes", k.session, command, n)
			}
			if entry.Record.State == sessionstore.InboxStateApplied {
				if n := len(evidence.DispositionsOf(command)); n != 1 {
					s.t.Errorf("S3: %s/%s settled applied with %d disposition frames, want 1", k.session, command, n)
				}
				if rc, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID)); err == nil && evidence.EffectsOf(rc) > 1 {
					s.t.Errorf("S3: %s/%s caused %d turn effects: applied more than once", k.session, command, evidence.EffectsOf(rc))
				}
			}
			checked++
		}
	}
	s.t.Logf("I3.2 S3: %d acknowledged command RPCs, outcomes %v, %d checked against the runtime journal, settled within %v", total, outcomes, checked, time.Since(start).Round(time.Millisecond))
	s.phase("commands", map[string]any{"acknowledged": total, "outcomes": outcomes, "checked": checked})
}

func (s *soak) judgeCounters(final soakStats) {
	c := final.Counters
	s.t.Logf("I3.2 client counters: %+v", c)
	s.t.Logf("I3.2 delivery latency (commit -> browser, live, never-stalled browsers): %+v; stalled browsers %+v", final.DeliveryLatency, final.SlowDeliveryLatency)
	for _, name := range soakSortedKeys(final.DeliveryLatencyWindows) {
		s.t.Logf("I3.2 delivery latency in %s: %+v", name, final.DeliveryLatencyWindows[name])
	}
	if c.SilentGaps != 0 {
		s.t.Errorf("S2: %d SILENT GAPS: an enduring record arrived beyond the next sequence with nothing announcing the gap", c.SilentGaps)
	}
	if c.Strays != 0 {
		s.t.Errorf("S2: %d records naming another tenant or session reached a browser", c.Strays)
	}
	if c.RepairFailures != 0 {
		s.t.Errorf("S2: %d journal repairs failed after retries", c.RepairFailures)
	}
	if c.Undecodable != 0 {
		s.t.Errorf("S2: %d records were not decodable as Core session records", c.Undecodable)
	}
	if final.Connected != s.cfg.connections {
		s.t.Errorf("S1: at the end %d of %d browsers are connected", final.Connected, s.cfg.connections)
	}
	if n := s.q1.Load(); n != 0 {
		s.t.Errorf("S4: %d samples found a Host above its capacity or command-queue bound: %v", n, s.q1Examples)
	}
}

// judgeGrowth is S4 across checkpoints: the first comparable checkpoint is
// "connected" (every client joined, nothing yet cycled).
func (s *soak) judgeGrowth() {
	if len(s.checkpoints) < 2 {
		return
	}
	first, last := s.checkpoints[0], s.checkpoints[len(s.checkpoints)-1]
	goroutineSlack := max(100, first.Goroutines/20)
	if last.Goroutines > first.Goroutines+goroutineSlack {
		s.t.Errorf("S4: server goroutines grew from %d (checkpoint %s) to %d (checkpoint %s), beyond the %d slack: unbounded growth",
			first.Goroutines, first.Name, last.Goroutines, last.Name, goroutineSlack)
	}
	// FDs are judged NET OF THE OPEN ACCEPTED CONNECTIONS: the client's HTTP
	// keep-alive pool for journal repairs legitimately holds a varying number
	// of idle connections (bounded by its MaxIdleConns), and each is a server
	// descriptor. What must not grow is every other descriptor, and the
	// connection count must stay within the ClientLinks plus that pool.
	firstOther, lastOther := int64(first.FDs)-first.ServerConns, int64(last.FDs)-last.ServerConns
	if lastOther > firstOther+16 {
		s.t.Errorf("S4: server descriptors other than accepted connections grew from %d (checkpoint %s) to %d (checkpoint %s): leaked descriptors",
			firstOther, first.Name, lastOther, last.Name)
	}
	for _, c := range s.checkpoints {
		if c.ServerConns > int64(s.cfg.connections+soakClientIdlePool) {
			s.t.Errorf("S4: checkpoint %s: %d accepted connections are open, above the %d ClientLinks plus the client's %d-connection HTTP pool", c.Name, c.ServerConns, s.cfg.connections, soakClientIdlePool)
		}
	}
	events, bytes := last.Committed-first.Committed, last.BodyBytes-first.BodyBytes
	if events > 0 {
		growth := (last.HeapInuseMB - first.HeapInuseMB) * (1 << 20)
		allowed := float64(soakHeapPerBodyByte*bytes + soakHeapPerRecord*events + soakHeapPerConnection*int64(s.cfg.connections))
		s.t.Logf("I3.2 S4: heap grew %.1f MiB over %d committed records carrying %.1f MiB of bodies: %.0f bytes per record, %.2f per body byte; allowed %.1f MiB (the durable plane is in this process)",
			growth/(1<<20), events, float64(bytes)/(1<<20), growth/float64(events), growth/float64(max(bytes, 1)), allowed/(1<<20))
		if growth > allowed {
			s.t.Errorf("S4: heap grew %.1f MiB, above the %.1f MiB the committed records account for: growth a per-delivery leak would produce", growth/(1<<20), allowed/(1<<20))
		}
	}
}

// soakHeapPerBodyByte and soakHeapPerRecord bound the heap a committed
// record may cost the server process. The durable plane is IN this process
// (the product journal and the SessionStore are memstores), so a record
// legitimately costs its stored envelope -- about its body plus fixed framing
// -- and the kit's history entry. Every record is delivered to about
// connections/sessions browsers (20 by default), so retaining even a fraction
// of each DELIVERY would exceed four times the body plus a KiB.
//
// soakHeapPerConnection is the one allowance that is not per record: a
// connection's transport buffers grow to the largest frames it has carried
// (the slow-consumer bursts) and stay grown. That is bounded by the number of
// connections, not by time, so it is allowed once per connection.
const (
	soakHeapPerBodyByte   = 4
	soakHeapPerRecord     = 1 << 10
	soakHeapPerConnection = 16 << 10
)

func (s *soak) awaitServerReturns(goroutines, fds int) {
	deadline := time.Now().Add(3 * time.Minute)
	slack := 200
	for {
		runtime.GC()
		g, f := runtime.NumGoroutine(), soakFDs()
		if g <= goroutines+slack && f <= fds+32 {
			s.t.Logf("I3.2 S6: with every client gone the server is at %d goroutines (pre-client %d) and %d FDs (pre-client %d)", g, goroutines, f, fds)
			s.phase("clients_gone", map[string]any{"goroutines": g, "fds": f, "pre_client_goroutines": goroutines, "pre_client_fds": fds})
			return
		}
		if time.Now().After(deadline) {
			var dump strings.Builder
			_ = pprof.Lookup("goroutine").WriteTo(&dump, 1)
			s.t.Errorf("S6: 3m after every client left the server still holds %d goroutines (pre-client %d) and %d FDs (pre-client %d):\n%s",
				g, goroutines, f, fds, stressLeakSummary(dump.String()))
			return
		}
		time.Sleep(time.Second)
	}
}

func (s *soak) shutdown() {
	s.mu.Lock()
	replicas, hosts := s.replicas, s.hosts
	s.mu.Unlock()
	for _, r := range replicas {
		r.f.Stop()
	}
	for _, h := range hosts {
		h.Stop()
	}
}

// soakGoroutinesSettle is I3.1's L1 with the same accounting for the
// centrifuge eagle leak (see assertGoroutinesSettle).
func soakGoroutinesSettle(t *testing.T, baseline, baselineEagles, nodes int) {
	const slack = 4
	deadline := time.Now().Add(60 * time.Second)
	for {
		var dump strings.Builder
		_ = pprof.Lookup("goroutine").WriteTo(&dump, 1)
		eagles := stressGroupCount(dump.String(), stressEagleFrame) - baselineEagles
		others := runtime.NumGoroutine() - eagles - baseline
		if others <= slack && eagles <= nodes {
			t.Logf("I3.2 S6 goroutines settled after shutdown: %d above the baseline of %d, plus %d unclosed centrifuge eagle aggregators (at most %d Nodes)", others, baseline, eagles, nodes)
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("S6: %d goroutines above the baseline of %d remain 60s after the fleet stopped, plus %d eagle aggregators for at most %d Nodes:\n%s",
				others, baseline, eagles, nodes, stressLeakSummary(dump.String()))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (s *soak) writeCheckpoints() {
	file, err := os.Create(filepath.Join(s.cfg.out, "checkpoints.csv"))
	if err != nil {
		return
	}
	defer file.Close()
	w := csv.NewWriter(file)
	defer w.Flush()
	_ = w.Write([]string{"name", "elapsed_s", "committed", "goroutines", "fds", "heap_inuse_mb", "rss_mb", "server_clientlink_conns",
		"coverage_p50_ms", "coverage_p99_ms", "coverage_max_ms", "host_bytes_out", "clientlink_bytes_out", "live_deliveries"})
	for _, c := range s.checkpoints {
		_ = w.Write([]string{c.Name, fmt.Sprintf("%.1f", c.ElapsedS), strconv.FormatInt(c.Committed, 10), strconv.Itoa(c.Goroutines),
			strconv.Itoa(c.FDs), fmt.Sprintf("%.1f", c.HeapInuseMB), fmt.Sprintf("%.1f", c.RSSMB), strconv.FormatInt(c.ServerConns, 10),
			fmt.Sprintf("%.1f", c.Coverage.P50), fmt.Sprintf("%.1f", c.Coverage.P99), fmt.Sprintf("%.1f", c.Coverage.Max),
			strconv.FormatInt(c.HostBytesOut, 10), strconv.FormatInt(c.ClientLinkOut, 10), strconv.Itoa(c.LiveDeliveries)})
	}
}

func (s *soak) summary(env soakEnvironment, serverSamples []soakSample, final soakStats) map[string]any {
	server := soakSummarise(soakRowsOf(serverSamples))
	client := soakSummarise(soakRowsFromCSV(filepath.Join(s.cfg.out, "resources_client.csv")))
	var hostOut, linkOut int64
	for _, h := range s.hosts {
		hostOut += h.BytesOut()
	}
	for _, r := range append(append([]*soakReplica(nil), s.replicas...), s.retired...) {
		linkOut += r.listener.written.Load()
	}
	committed := s.committed.Load()
	liveDeliveries := final.Counters.LiveApplied + final.Counters.RepairOverlaps + final.Counters.WireDuplicates
	amplification := map[string]any{
		"records_committed":              committed,
		"live_enduring_deliveries":       liveDeliveries,
		"deliveries_per_record":          float64(liveDeliveries) / float64(max(committed, 1)),
		"host_hostlink_bytes_out":        hostOut,
		"factory_clientlink_bytes_out":   linkOut,
		"clientlink_bytes_per_host_byte": float64(linkOut) / float64(max(hostOut, 1)),
		"per_host_bytes_out":             s.perHostBytes(),
	}
	s.t.Logf("I3.2 server process: %+v", server)
	s.t.Logf("I3.2 client process: %+v", client)
	s.t.Logf("I3.2 amplification: %v", amplification)
	s.t.Logf("I3.2 reconnect durations: %+v; connect %+v; join %+v", final.Reconnect, final.Connect, final.Join)
	var rpcLatency []soakDist
	for _, r := range s.rpcAcks {
		rpcLatency = append(rpcLatency, r.Latency)
	}
	return map[string]any{
		"environment": env, "server_process": server, "client_process": client,
		"checkpoints": s.checkpoints, "phases": s.phases, "final_client_stats": final,
		"command_rpc_latency_per_round": rpcLatency, "amplification": amplification,
		"host_peak_resident_sessions": s.peakHeld.Load(), "host_capacity": s.capacity,
		"host_peak_command_queue_depth": s.peakQueue.Load(), "host_command_queue_bound": s.queue,
		"host_bound_violations": s.q1.Load(), "product_tail_dropped": s.world.Tails.Dropped(),
		"passed": !s.t.Failed(),
	}
}

func (s *soak) perHostBytes() map[string]int64 {
	out := map[string]int64{}
	for _, h := range s.hosts {
		out[string(h.ID)] = h.BytesOut()
	}
	return out
}

func soakSortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// debugModules lists the looprig module versions this binary was built with.
func debugModules() ([]string, bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil, false
	}
	var out []string
	for _, dep := range info.Deps {
		if strings.HasPrefix(dep.Path, "github.com/looprig/") || strings.HasPrefix(dep.Path, "github.com/centrifugal/") {
			out = append(out, dep.Path+"@"+dep.Version)
		}
	}
	return out, true
}
