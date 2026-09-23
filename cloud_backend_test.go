//go:build integration && cloud

// This file is the shared plumbing of runbook 07 task P3.1, the CLOUD
// composition lane: one storage.Composite whose Ledger, Leaser, KV and
// OrderedIndex are the RELEASED pgstore and whose Blobs is the RELEASED
// s3store, handed to SessionStore (and, in the orchestration cases, to every
// harness journal as well).
//
// # What "cloud" means here
//
// Nothing in this lane contacts a cloud. scripts/cloud-up.sh starts LOCAL
// containers, each pinned by digest: PostgreSQL 17 (with pg_stat_statements),
// PgBouncer in transaction mode in front of it, and MinIO serving S3 over TLS
// with a static KMS key. The lane is opt-in twice: the `cloud` build tag
// compiles it, and LOOPRIG_CLOUD=1 (written by cloud-up.sh's env file) runs
// it. Ordinary CI builds neither.
//
// # What it deliberately does NOT compose
//
// Step 1 of the task names four things that must be absent: a NATS cache, a
// notifier, a dual write, and a PostgreSQL blob fallback. The composite is
// built field by field below from exactly two providers and nothing wraps
// Blobs except a timing recorder; TestCloudCompositeIsExactlyPgstorePlusS3store
// holds that.
package tests

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/looprig/pgstore"
	"github.com/looprig/s3store"
	"github.com/looprig/storage"
)

// cloudConfig is everything cloud-up.sh's env file names. None of it is ever
// printed: a DSN carries a password, and the S3 credentials travel only
// through the AWS SDK's standard chain.
type cloudConfig struct {
	pgDSN      string
	bouncerDSN string
	s3Endpoint string
	bucket     string
	kmsKeyID   string

	pgContainer      string
	bouncerContainer string
	s3Container      string
}

// cloudEnabled reports whether this run asked for the cloud lane.
func cloudEnabled() bool { return os.Getenv("LOOPRIG_CLOUD") == "1" }

// requireCloud skips a case unless the lane is enabled, and FAILS it when the
// lane is enabled but misconfigured: a half-written environment must not read
// as "skipped, nothing to see".
func requireCloud(t *testing.T) cloudConfig {
	t.Helper()
	if !cloudEnabled() {
		t.Skip("cloud lane not enabled; source the env file scripts/cloud-up.sh prints (it sets LOOPRIG_CLOUD=1)")
	}
	cfg := cloudConfig{
		pgDSN:            os.Getenv("LOOPRIG_CLOUD_PG_DSN"),
		bouncerDSN:       os.Getenv("LOOPRIG_CLOUD_BOUNCER_DSN"),
		s3Endpoint:       os.Getenv("LOOPRIG_CLOUD_S3_ENDPOINT"),
		bucket:           os.Getenv("LOOPRIG_CLOUD_S3_BUCKET"),
		kmsKeyID:         os.Getenv("LOOPRIG_CLOUD_S3_KMS_KEY_ID"),
		pgContainer:      os.Getenv("LOOPRIG_CLOUD_PG_CONTAINER"),
		bouncerContainer: os.Getenv("LOOPRIG_CLOUD_BOUNCER_CONTAINER"),
		s3Container:      os.Getenv("LOOPRIG_CLOUD_S3_CONTAINER"),
	}
	missing := []string{}
	for name, value := range map[string]string{
		"LOOPRIG_CLOUD_PG_DSN": cfg.pgDSN, "LOOPRIG_CLOUD_BOUNCER_DSN": cfg.bouncerDSN,
		"LOOPRIG_CLOUD_S3_ENDPOINT": cfg.s3Endpoint, "LOOPRIG_CLOUD_S3_BUCKET": cfg.bucket,
		"LOOPRIG_CLOUD_S3_KMS_KEY_ID": cfg.kmsKeyID, "LOOPRIG_CLOUD_PG_CONTAINER": cfg.pgContainer,
		"LOOPRIG_CLOUD_BOUNCER_CONTAINER": cfg.bouncerContainer, "LOOPRIG_CLOUD_S3_CONTAINER": cfg.s3Container,
		"AWS_CA_BUNDLE": os.Getenv("AWS_CA_BUNDLE"),
	} {
		if value == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("LOOPRIG_CLOUD=1 but %v are unset; source the env file scripts/cloud-up.sh prints", missing)
	}
	return cfg
}

// structuredDSN is the DSN pgstore connects through. LOOPRIG_CLOUD_PG_VIA=bouncer
// routes every pgstore pool through PgBouncer in transaction mode, which is the
// pooling topology pgstore is written for; the default is PostgreSQL direct.
func (c cloudConfig) structuredDSN() (string, string) {
	if os.Getenv("LOOPRIG_CLOUD_PG_VIA") == "bouncer" {
		return c.bouncerDSN, "pgbouncer(transaction)"
	}
	return c.pgDSN, "postgresql(direct)"
}

// cloudDeployment is one isolated keyspace inside the shared containers: its
// own PostgreSQL schema and its own S3 deployment prefix. Two deployments in
// one database and one bucket must never see each other's records; that is
// P3.1's prefix-isolation claim, and it is also how each case gets the EMPTY
// backend a layout case depends on.
type cloudDeployment struct {
	Schema string
	Prefix string
}

func newCloudDeployment(t *testing.T) cloudDeployment {
	t.Helper()
	token := cloudToken(t)
	return cloudDeployment{Schema: "p31_" + token, Prefix: "p31-" + token}
}

func cloudToken(t *testing.T) string {
	t.Helper()
	var raw [6]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("cloud: minting a token: %v", err)
	}
	return hex.EncodeToString(raw[:])
}

// cloudContext bounds a store-level cloud case.
func cloudContext(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// cloudBackend is one opened pgstore+s3store composite and what it was built
// from, so a case can reach the raw providers for the checks the Storage
// contract cannot express (raw bucket listing, PostgreSQL statistics).
type cloudBackend struct {
	Composite  *storage.Composite
	Structured *pgstore.Store
	Blobs      *s3store.Store
	Deployment cloudDeployment
	Metrics    *cloudMetrics
}

// openCloudBackend opens pgstore and s3store for one deployment and assembles
// the composite. Blobs is SSE-KMS with RequireConfirmedEncryption, so a
// posture s3store cannot put on the wire itself is refused at Open.
func openCloudBackend(t testing.TB, ctx context.Context, cfg cloudConfig, dep cloudDeployment) *cloudBackend {
	t.Helper()
	dsn, _ := cfg.structuredDSN()
	openCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	structured, err := pgstore.Open(openCtx, pgstore.Options{
		DSN:                        dsn,
		Schema:                     dep.Schema,
		MaxConns:                   8,
		Migrations:                 pgstore.MigrationApply,
		AllowInsecureLocalhostOnly: true,
	})
	if err != nil {
		t.Fatalf("pgstore.Open: %v", err)
	}
	t.Cleanup(structured.Close)
	blobs, err := s3store.Open(openCtx, s3store.Options{
		Endpoint:                   cfg.s3Endpoint,
		Region:                     "us-east-1",
		Bucket:                     cfg.bucket,
		DeploymentPrefix:           dep.Prefix,
		AddressingStyle:            s3store.AddressingPath,
		Encryption:                 s3store.EncryptionKMS,
		KMSKeyID:                   cfg.kmsKeyID,
		RequireConfirmedEncryption: true,
	})
	if err != nil {
		t.Fatalf("s3store.Open: %v", s3store.RedactedErrorText(err))
	}
	metrics := newCloudMetrics()
	composite, err := storage.NewCompositeWithOrderedIndex(
		timedLedger{structured.Ledger, metrics},
		timedLeaser{structured.Leaser, metrics},
		timedKV{structured.KV, metrics},
		timedBlobs{blobs, metrics},
		timedOrderedIndex{structured.OrderedIndex, metrics},
	)
	if err != nil {
		t.Fatalf("storage.NewCompositeWithOrderedIndex: %v", err)
	}
	return &cloudBackend{Composite: composite, Structured: structured, Blobs: blobs, Deployment: dep, Metrics: metrics}
}

// ---- metrics ----------------------------------------------------------------

// cloudMetrics records, per primitive operation, how many calls were made, how
// many failed, and their latency. It records NO key, name, tenant or payload:
// step 4 asks for measurements without identifiers or secrets, and a recorder
// that never holds one cannot leak one.
type cloudMetrics struct {
	mu  sync.Mutex
	ops map[string]*cloudOpStats
	// undated counts calls that arrived with NO context deadline, per op,
	// and remembers the first non-lane caller of each. pgstore/s3store v0.1.x
	// refused every such call (defect D2); v0.2.0 bounds each one by its
	// DefaultOperationTimeout (30s). The census stays so an operator can see
	// which callers lean on that provider default.
	undated       map[string]int
	undatedCaller map[string]string
	putBytes      int64
	getBytes      int64
	putTime       time.Duration
	getTime       time.Duration
}

type cloudOpStats struct {
	calls  int
	errors int
	total  time.Duration
	max    time.Duration
}

func newCloudMetrics() *cloudMetrics {
	return &cloudMetrics{ops: map[string]*cloudOpStats{}, undated: map[string]int{}, undatedCaller: map[string]string{}}
}

// bound records a deadline-less call. It passes the context through
// UNCHANGED, so the providers' own default bound is what is exercised. It
// returns a cancel func only so every call site keeps one shape.
func (m *cloudMetrics) bound(ctx context.Context, op string) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	caller := firstForeignCaller()
	m.mu.Lock()
	m.undated[op]++
	if _, seen := m.undatedCaller[op]; !seen {
		m.undatedCaller[op] = caller
	}
	m.mu.Unlock()
	return ctx, func() {}
}

// firstForeignCaller names the chain of looprig module frames (outside this
// lane and the two providers) that made a deadline-less storage call,
// innermost first, up to three: the immediate SessionStore/harness frame and
// the Host/Factory/harness frames that handed it the undated context.
func firstForeignCaller() string {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var chain []string
	for len(chain) < 3 {
		frame, more := frames.Next()
		fn := frame.Function
		if strings.HasPrefix(fn, "github.com/looprig/") &&
			!strings.HasPrefix(fn, "github.com/looprig/tests") &&
			!strings.HasPrefix(fn, "github.com/looprig/pgstore") &&
			!strings.HasPrefix(fn, "github.com/looprig/s3store") &&
			!strings.HasPrefix(fn, "github.com/looprig/storage") {
			short := strings.TrimPrefix(fn, "github.com/looprig/")
			if len(chain) == 0 || chain[len(chain)-1] != short {
				chain = append(chain, short)
			}
		}
		if !more {
			break
		}
	}
	if len(chain) == 0 {
		return "unknown"
	}
	return strings.Join(chain, " <- ")
}

// Undated reports the deadline-less call census.
func (m *cloudMetrics) Undated() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]string{}
	for op, n := range m.undated {
		out[op] = fmt.Sprintf("%d calls, first from %s", n, m.undatedCaller[op])
	}
	return out
}

func (m *cloudMetrics) observe(op string, started time.Time, err error) {
	elapsed := time.Since(started)
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.ops[op]
	if stats == nil {
		stats = &cloudOpStats{}
		m.ops[op] = stats
	}
	stats.calls++
	if err != nil {
		stats.errors++
	}
	stats.total += elapsed
	if elapsed > stats.max {
		stats.max = elapsed
	}
}

func (m *cloudMetrics) addPut(n int64, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putBytes += n
	m.putTime += d
}

func (m *cloudMetrics) addGet(n int64, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getBytes += n
	m.getTime += d
}

// Calls reports how many calls op received.
func (m *cloudMetrics) Calls(op string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if stats := m.ops[op]; stats != nil {
		return stats.calls
	}
	return 0
}

// Errors reports how many calls to op failed.
func (m *cloudMetrics) Errors(op string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if stats := m.ops[op]; stats != nil {
		return stats.errors
	}
	return 0
}

// Report renders the recorder as a table for a test log.
func (m *cloudMetrics) Report(label string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.ops))
	for name := range m.ops {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	fmt.Fprintf(&b, "provider operations (%s):\n", label)
	fmt.Fprintf(&b, "  %-28s %7s %6s %10s %10s\n", "op", "calls", "errors", "mean", "max")
	for _, name := range names {
		s := m.ops[name]
		mean := time.Duration(0)
		if s.calls > 0 {
			mean = s.total / time.Duration(s.calls)
		}
		fmt.Fprintf(&b, "  %-28s %7d %6d %10s %10s\n", name, s.calls, s.errors,
			mean.Round(time.Microsecond), s.max.Round(time.Microsecond))
	}
	fmt.Fprintf(&b, "  blob bytes put=%d (%s)  read=%d (%s)\n",
		m.putBytes, throughput(m.putBytes, m.putTime), m.getBytes, throughput(m.getBytes, m.getTime))
	if len(m.undated) > 0 {
		undatedOps := make([]string, 0, len(m.undated))
		for op := range m.undated {
			undatedOps = append(undatedOps, op)
		}
		sort.Strings(undatedOps)
		fmt.Fprintf(&b, "  calls with NO context deadline (bounded by the provider's 30s default):\n")
		for _, op := range undatedOps {
			fmt.Fprintf(&b, "    %-20s %5d  first: %s\n", op, m.undated[op], m.undatedCaller[op])
		}
	}
	return b.String()
}

func throughput(n int64, d time.Duration) string {
	if d <= 0 || n == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2f MiB/s", float64(n)/(1<<20)/d.Seconds())
}

type timedLedger struct {
	storage.Ledger
	m *cloudMetrics
}

func (l timedLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	ctx, cancel := l.m.bound(ctx, "ledger.append")
	defer cancel()
	started := time.Now()
	err := l.Ledger.Append(ctx, name, expected, payload)
	l.m.observe("ledger.append", started, err)
	return err
}

// Read binds the returned cursor to the recorder too: pgstore requires a
// deadline on every Next as well as on Read.
func (l timedLedger) Read(ctx context.Context, name string, from uint64) (storage.Cursor, error) {
	ctx, cancel := l.m.bound(ctx, "ledger.read")
	defer cancel()
	started := time.Now()
	cursor, err := l.Ledger.Read(ctx, name, from)
	l.m.observe("ledger.read", started, err)
	if err != nil {
		return nil, err
	}
	return timedCursor{Cursor: cursor, m: l.m}, nil
}

func (l timedLedger) Tip(ctx context.Context, name string) (uint64, error) {
	ctx, cancel := l.m.bound(ctx, "ledger.tip")
	defer cancel()
	started := time.Now()
	tip, err := l.Ledger.Tip(ctx, name)
	l.m.observe("ledger.tip", started, err)
	return tip, err
}

func (l timedLedger) Delete(ctx context.Context, name string) error {
	ctx, cancel := l.m.bound(ctx, "ledger.delete")
	defer cancel()
	started := time.Now()
	err := l.Ledger.Delete(ctx, name)
	l.m.observe("ledger.delete", started, err)
	return err
}

type timedCursor struct {
	storage.Cursor
	m *cloudMetrics
}

func (c timedCursor) Next(ctx context.Context) (storage.Record, error) {
	ctx, cancel := c.m.bound(ctx, "cursor.next")
	defer cancel()
	return c.Cursor.Next(ctx)
}

type timedLeaser struct {
	storage.Leaser
	m *cloudMetrics
}

func (l timedLeaser) Acquire(ctx context.Context, name string) (storage.Lease, error) {
	ctx, cancel := l.m.bound(ctx, "leaser.acquire")
	defer cancel()
	started := time.Now()
	lease, err := l.Leaser.Acquire(ctx, name)
	l.m.observe("leaser.acquire", started, err)
	if err != nil {
		return nil, err
	}
	return timedLease{Lease: lease, m: l.m}, nil
}

type timedLease struct {
	storage.Lease
	m *cloudMetrics
}

func (l timedLease) Release(ctx context.Context) error {
	ctx, cancel := l.m.bound(ctx, "lease.release")
	defer cancel()
	started := time.Now()
	err := l.Lease.Release(ctx)
	l.m.observe("lease.release", started, err)
	return err
}

type timedKV struct {
	storage.KV
	m *cloudMetrics
}

func (k timedKV) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	ctx, cancel := k.m.bound(ctx, "kv.get")
	defer cancel()
	started := time.Now()
	value, rev, err := k.KV.Get(ctx, key)
	k.m.observe("kv.get", started, err)
	return value, rev, err
}

func (k timedKV) Put(ctx context.Context, key string, expectedRev uint64, val []byte) (uint64, error) {
	ctx, cancel := k.m.bound(ctx, "kv.put")
	defer cancel()
	started := time.Now()
	rev, err := k.KV.Put(ctx, key, expectedRev, val)
	k.m.observe("kv.put", started, err)
	return rev, err
}

func (k timedKV) Keys(ctx context.Context, prefix string) ([]string, error) {
	ctx, cancel := k.m.bound(ctx, "kv.keys")
	defer cancel()
	started := time.Now()
	keys, err := k.KV.Keys(ctx, prefix)
	k.m.observe("kv.keys", started, err)
	return keys, err
}

func (k timedKV) Delete(ctx context.Context, key string) error {
	ctx, cancel := k.m.bound(ctx, "kv.delete")
	defer cancel()
	started := time.Now()
	err := k.KV.Delete(ctx, key)
	k.m.observe("kv.delete", started, err)
	return err
}

// timedBlobs keeps the BlobReaderLifecycle capability: sessionstore.Open
// refuses a Blobs provider without it, so a recorder that dropped it would
// turn every case into an Open failure.
type timedBlobs struct {
	inner *s3store.Store
	m     *cloudMetrics
}

var _ storage.BlobReaderLifecycle = timedBlobs{}

func (b timedBlobs) BlobReaderCloseBound() time.Duration { return b.inner.BlobReaderCloseBound() }

func (b timedBlobs) Put(ctx context.Context, key string, r io.Reader) error {
	ctx, cancel := b.m.bound(ctx, "blobs.put")
	defer cancel()
	counted := &countingReader{r: r}
	started := time.Now()
	err := b.inner.Put(ctx, key, counted)
	b.m.observe("blobs.put", started, err)
	if err == nil {
		b.m.addPut(counted.n, time.Since(started))
	}
	return err
}

// Get holds bound's cancel until the reader closes: s3store bounds the
// returned stream by the Get call's context.
func (b timedBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	ctx, cancel := b.m.bound(ctx, "blobs.get")
	started := time.Now()
	reader, err := b.inner.Get(ctx, key)
	b.m.observe("blobs.get", started, err)
	if err != nil {
		cancel()
		return nil, err
	}
	return &timedBlobReader{ReadCloser: reader, m: b.m, started: started, cancel: cancel}, nil
}

func (b timedBlobs) Delete(ctx context.Context, key string) error {
	ctx, cancel := b.m.bound(ctx, "blobs.delete")
	defer cancel()
	started := time.Now()
	err := b.inner.Delete(ctx, key)
	b.m.observe("blobs.delete", started, err)
	return err
}

func (b timedBlobs) List(ctx context.Context, prefix string) ([]string, error) {
	ctx, cancel := b.m.bound(ctx, "blobs.list")
	defer cancel()
	started := time.Now()
	keys, err := b.inner.List(ctx, prefix)
	b.m.observe("blobs.list", started, err)
	return keys, err
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// timedBlobReader forwards Read and Close unchanged -- the lifecycle contract
// (Close releases an in-flight Read, Close is idempotent, Read and Close are
// concurrency-safe) is the provider's, and this wrapper's only state is
// atomic or Once-guarded so it cannot break it.
type timedBlobReader struct {
	io.ReadCloser
	m       *cloudMetrics
	started time.Time
	cancel  context.CancelFunc
	n       atomic.Int64
	once    sync.Once
}

func (r *timedBlobReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.n.Add(int64(n))
	return n, err
}

func (r *timedBlobReader) Close() error {
	err := r.ReadCloser.Close()
	r.once.Do(func() {
		r.m.addGet(r.n.Load(), time.Since(r.started))
		r.cancel()
	})
	return err
}

type timedOrderedIndex struct {
	storage.OrderedIndex
	m *cloudMetrics
}

func (o timedOrderedIndex) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	ctx, cancel := o.m.bound(ctx, "ordered.get")
	defer cancel()
	started := time.Now()
	rec, err := o.OrderedIndex.Get(ctx, id)
	o.m.observe("ordered.get", started, err)
	return rec, err
}

func (o timedOrderedIndex) Create(ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	ctx, cancel := o.m.bound(ctx, "ordered.create")
	defer cancel()
	started := time.Now()
	rec, created, err := o.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due)
	o.m.observe("ordered.create", started, err)
	return rec, created, err
}

func (o timedOrderedIndex) Update(ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	ctx, cancel := o.m.bound(ctx, "ordered.update")
	defer cancel()
	started := time.Now()
	rec, err := o.OrderedIndex.Update(ctx, id, expectedRevision, value, rank, due)
	o.m.observe("ordered.update", started, err)
	return rec, err
}

func (o timedOrderedIndex) Delete(ctx context.Context, id storage.OrderedID, expectedRevision uint64) (storage.OrderedRecord, error) {
	ctx, cancel := o.m.bound(ctx, "ordered.delete")
	defer cancel()
	started := time.Now()
	rec, err := o.OrderedIndex.Delete(ctx, id, expectedRevision)
	o.m.observe("ordered.delete", started, err)
	return rec, err
}

func (o timedOrderedIndex) ListOrdered(ctx context.Context, namespace string, orderingScope string, afterOrder uint64, limit int) (storage.OrderedPage, error) {
	ctx, cancel := o.m.bound(ctx, "ordered.list_ordered")
	defer cancel()
	started := time.Now()
	page, err := o.OrderedIndex.ListOrdered(ctx, namespace, orderingScope, afterOrder, limit)
	o.m.observe("ordered.list_ordered", started, err)
	return page, err
}

func (o timedOrderedIndex) ListRanked(ctx context.Context, namespace string, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	ctx, cancel := o.m.bound(ctx, "ordered.list_ranked")
	defer cancel()
	started := time.Now()
	page, err := o.OrderedIndex.ListRanked(ctx, namespace, rankingScope, after, limit)
	o.m.observe("ordered.list_ranked", started, err)
	return page, err
}

func (o timedOrderedIndex) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	ctx, cancel := o.m.bound(ctx, "ordered.list_due")
	defer cancel()
	started := time.Now()
	page, err := o.OrderedIndex.ListDue(ctx, namespace, dueAtOrBefore, after, limit)
	o.m.observe("ordered.list_due", started, err)
	return page, err
}

// ---- PostgreSQL-side statistics ----------------------------------------------

// pgReport reads the server's own account of one deployment's schema: index
// and sequential scans per table, statement latency from pg_stat_statements,
// and connection use. Table names are reported by their pgstore SUFFIX and the
// schema is redacted from statement text, so the report names no deployment.
// It connects DIRECTLY to PostgreSQL even when the lane routes pgstore through
// PgBouncer, because PgBouncer does not proxy the statistics views faithfully.
func pgReport(t *testing.T, ctx context.Context, cfg cloudConfig, schema string) string {
	t.Helper()
	// Backends flush table statistics at most once a second; let the last
	// ones land before reading.
	time.Sleep(1500 * time.Millisecond)
	conn, err := pgx.Connect(ctx, cfg.pgDSN)
	if err != nil {
		return "pg statistics unavailable: connect failed"
	}
	defer conn.Close(ctx)
	var b strings.Builder
	b.WriteString("postgresql tables (by pgstore suffix):\n")
	fmt.Fprintf(&b, "  %-28s %9s %9s %9s %9s\n", "table", "idx_scan", "seq_scan", "n_live", "inserts")
	rows, err := conn.Query(ctx, `SELECT relname, COALESCE(idx_scan,0), COALESCE(seq_scan,0), n_live_tup, n_tup_ins
		FROM pg_stat_user_tables WHERE schemaname = $1 ORDER BY relname`, schema)
	if err == nil {
		for rows.Next() {
			var name string
			var idx, seq, live, ins int64
			if rows.Scan(&name, &idx, &seq, &live, &ins) == nil {
				fmt.Fprintf(&b, "  %-28s %9d %9d %9d %9d\n", strings.TrimPrefix(name, "looprig_"), idx, seq, live, ins)
			}
		}
		rows.Close()
	}
	var calls int64
	var total, maxMs float64
	if err := conn.QueryRow(ctx, `SELECT COALESCE(sum(calls),0), COALESCE(sum(total_exec_time),0), COALESCE(max(max_exec_time),0)
		FROM pg_stat_statements WHERE query LIKE '%' || $1 || '%'`, schema).Scan(&calls, &total, &maxMs); err == nil && calls > 0 {
		fmt.Fprintf(&b, "statements touching this schema: calls=%d mean=%.3fms max=%.3fms\n", calls, total/float64(calls), maxMs)
	}
	rows, err = conn.Query(ctx, `SELECT calls, mean_exec_time, max_exec_time, left(regexp_replace(query, '\s+', ' ', 'g'), 110)
		FROM pg_stat_statements WHERE query LIKE '%' || $1 || '%' ORDER BY total_exec_time DESC LIMIT 5`, schema)
	if err == nil {
		b.WriteString("  top statements by total time (schema redacted):\n")
		for rows.Next() {
			var c int64
			var mean, mx float64
			var q string
			if rows.Scan(&c, &mean, &mx, &q) == nil {
				fmt.Fprintf(&b, "    calls=%-6d mean=%.3fms max=%.3fms  %s\n", c, mean, mx, strings.ReplaceAll(q, schema, "<schema>"))
			}
		}
		rows.Close()
	}
	rows, err = conn.Query(ctx, `SELECT COALESCE(state,'?'), count(*) FROM pg_stat_activity
		WHERE datname = current_database() AND pid <> pg_backend_pid() GROUP BY 1 ORDER BY 1`)
	if err == nil {
		b.WriteString("  server connections by state:")
		for rows.Next() {
			var state string
			var n int64
			if rows.Scan(&state, &n) == nil {
				fmt.Fprintf(&b, " %s=%d", state, n)
			}
		}
		rows.Close()
		b.WriteString("\n")
	}
	return b.String()
}

// pgTables lists the tables pgstore created for one deployment.
func pgTables(t *testing.T, ctx context.Context, cfg cloudConfig, schema string) []string {
	t.Helper()
	conn, err := pgx.Connect(ctx, cfg.pgDSN)
	if err != nil {
		t.Fatalf("connecting for the table census: %v", err)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY 1`, schema)
	if err != nil {
		t.Fatalf("reading the table census: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scanning the table census: %v", err)
		}
		out = append(out, name)
	}
	return out
}

// ---- raw S3 -------------------------------------------------------------------

// rawS3 is a plain SDK client on the lane's bucket. It is how a case checks
// what s3store actually put on the service -- server-side encryption on every
// object, and which deployment prefix each object landed under -- which the
// Storage contract cannot express.
func rawS3(t *testing.T, ctx context.Context, cfg cloudConfig) *s3.Client {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"))
	if err != nil {
		t.Fatalf("loading the raw S3 client's configuration failed")
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.s3Endpoint)
		o.UsePathStyle = true
	})
}

// rawObject is one object as the service reports it.
type rawObject struct {
	Key        string
	Size       int64
	Encryption string
}

// listRaw lists every object under prefix and heads each one for its
// server-side encryption.
func listRaw(t *testing.T, ctx context.Context, client *s3.Client, cfg cloudConfig, prefix string) []rawObject {
	t.Helper()
	var out []rawObject
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.bucket), Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Fatalf("listing the bucket under a deployment prefix failed")
		}
		for _, object := range page.Contents {
			head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(cfg.bucket), Key: object.Key})
			if err != nil {
				t.Fatalf("heading a listed object failed")
			}
			out = append(out, rawObject{Key: aws.ToString(object.Key), Size: aws.ToInt64(object.Size), Encryption: string(head.ServerSideEncryption)})
		}
	}
	return out
}

// ---- container control -----------------------------------------------------

// cloudDocker runs one docker command against a lane container.
func cloudDocker(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v: %s", strings.Join(args, " "), err, out)
	}
}

// awaitPostgres waits until PostgreSQL answers a query again.
func awaitPostgres(t *testing.T, ctx context.Context, dsn string, within time.Duration) time.Duration {
	t.Helper()
	started := time.Now()
	deadline := started.Add(within)
	for time.Now().Before(deadline) {
		probe, cancel := context.WithTimeout(ctx, 2*time.Second)
		conn, err := pgx.Connect(probe, dsn)
		if err == nil {
			err = conn.Ping(probe)
			conn.Close(probe)
		}
		cancel()
		if err == nil {
			return time.Since(started)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("PostgreSQL did not answer within %v", within)
	return 0
}

// awaitS3 waits until MinIO reports itself live again.
func awaitS3(t *testing.T, ctx context.Context, cfg cloudConfig, within time.Duration) time.Duration {
	t.Helper()
	pem, err := os.ReadFile(os.Getenv("AWS_CA_BUNDLE"))
	if err != nil {
		t.Fatalf("reading the lane's CA bundle: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	started := time.Now()
	deadline := started.Add(within)
	for time.Now().Before(deadline) {
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, cfg.s3Endpoint+"/minio/health/ready", nil)
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return time.Since(started)
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("MinIO did not report ready within %v", within)
	return 0
}
