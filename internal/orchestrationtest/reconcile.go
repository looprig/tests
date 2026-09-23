//go:build integration

package orchestrationtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// This file is the kit's half of runbook 07 task I1.3: the pieces a
// reconciliation case needs that no earlier case did. It is a separate file on
// purpose, so a parallel lane editing pooled.go does not collide with it.
//
// Nothing here stands in for the thing under test. Every adapter WRAPS a real
// collaborator -- the real Store, the real directory, a real Host's Handler --
// and either observes it or injects one named fault (latency, a crash), and
// each one says which.

// ---- many tenants, admitted through Factory ---------------------------------

// MultiTenantVerifier accepts one bearer per tenant, so a single Factory can
// ADMIT work in many tenants. The tenant-count axis of a sweep case is only
// evidence about Factory when the work in every tenant went through Factory's
// own admission.
type MultiTenantVerifier struct {
	Bearers map[string]sessionwire.TenantID
	Clock   *Clock
}

// VerifyCredential satisfies identity.Verifier.
func (v *MultiTenantVerifier) VerifyCredential(_ context.Context, credential identity.Credential) (identity.Claims, error) {
	tenant, ok := v.Bearers[credential.Value()]
	if !ok {
		return identity.Claims{}, identity.ErrUnauthenticated
	}
	return identity.Claims{
		Tenant:    tenant,
		Subject:   "orchestrationtest-subject",
		Kind:      identity.KindActor,
		ExpiresAt: v.Clock.Now().Add(time.Hour),
	}, nil
}

// PostAs is Post with a chosen bearer credential.
func (f *FactoryFixture) PostAs(tb TB, ctx context.Context, bearer, path string, body []byte) (int, []byte) {
	tb.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		tb.Fatalf("orchestrationtest: building a POST for %q: %v", path, err)
		return 0, nil
	}
	req.Host = KitHost
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.Client.Do(req)
	if err != nil {
		tb.Fatalf("orchestrationtest: posting %q: %v", path, err)
		return 0, nil
	}
	defer func() { _ = resp.Body.Close() }()
	answer, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		tb.Fatalf("orchestrationtest: reading the answer to %q: %v", path, err)
		return 0, nil
	}
	return resp.StatusCode, answer
}

// ---- claims, observed and crashable ------------------------------------------

// RecordedClaim is one reconciliation claim a replica's sweeper ATTEMPTED, with
// the store's answer and the wall-clock instant it was asked.
type RecordedClaim struct {
	Request sessionstore.AcquireReconciliationClaimRequest
	Held    bool
	Claim   sessionstore.ReconciliationClaim
	At      time.Time
}

// ErrReplicaCrashed is what a crashed replica's durable seam answers.
var ErrReplicaCrashed = errors.New("orchestrationtest: this replica has crashed")

// ReplicaCommands is the real Store's command plane for ONE replica, recording
// every claim that replica's sweepers attempt and what the store answered.
//
// Crash makes it answer like a process that has died: nothing it does reaches
// the store any more -- in particular it RELEASES NOTHING, which is the one
// property of a crash a graceful Stop does not have. A claim it held is left to
// lapse, which is the state "claims expire/recover after a reconciler crash" is
// about.
type ReplicaCommands struct {
	*StoreCommands

	mu      sync.Mutex
	crashed bool
	claims  []RecordedClaim
}

// NewReplicaCommands wraps the real Store.
//
// Its shard count is the STORE'S OWN, read from the Store, not the kit's
// KitControlShards. A pooled world's Store is opened with the default layout,
// and a seam reporting 4 over a 16-shard store sweeps shards 0-3 and never
// reaches the rest -- measured here first as "nothing is ever placed", with no
// error anywhere. See StoreFixture.open.
func NewReplicaCommands(store *sessionstore.Store) *ReplicaCommands {
	commands := NewStoreCommands(store)
	commands.Shards = store.ControlShards()
	return &ReplicaCommands{StoreCommands: commands}
}

// Crash stops this replica's durable plane dead.
func (c *ReplicaCommands) Crash() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.crashed = true
}

func (c *ReplicaCommands) isCrashed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.crashed
}

// Claims reports every claim attempt, in order.
func (c *ReplicaCommands) Claims() []RecordedClaim {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]RecordedClaim(nil), c.claims...)
}

// AcquireReconciliationClaim satisfies factory.Commands.
func (c *ReplicaCommands) AcquireReconciliationClaim(ctx context.Context, req sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	if c.isCrashed() {
		return sessionstore.ReconciliationClaimEntry{}, ErrReplicaCrashed
	}
	at := time.Now()
	entry, err := c.StoreCommands.AcquireReconciliationClaim(ctx, req)
	c.mu.Lock()
	c.claims = append(c.claims, RecordedClaim{Request: req, Held: err == nil, Claim: entry.Claim, At: at})
	c.mu.Unlock()
	return entry, err
}

// ReleaseReconciliationClaim satisfies factory.Commands. A crashed replica
// releases nothing.
func (c *ReplicaCommands) ReleaseReconciliationClaim(ctx context.Context, req sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	if c.isCrashed() {
		return sessionstore.ReconciliationClaimEntry{}, ErrReplicaCrashed
	}
	return c.StoreCommands.ReleaseReconciliationClaim(ctx, req)
}

// ---- a slow registry ----------------------------------------------------------

// SlowDirectory is a real factory.Directory whose Candidates read takes Delay,
// or blocks until the caller gives up when Block is set.
//
// It is LATENCY INJECTION over the real directory, not a substitute: every
// answer is the inner directory's. It exists to WIDEN the one window
// duplicate-placement suppression is about -- the time between a replica taking
// a session's claim and its attach reaching the Host. placement.placePooled
// reads Candidates after the claim and before the attach, so a slow read there
// holds the claim open for Delay. A window narrower than the sweep interval is
// one two replicas may simply never overlap in, and a case built on it would
// pass for timing rather than for the claim.
type SlowDirectory struct {
	Inner factory.Directory
	Delay time.Duration
	// Hold, when set, runs inside every Candidates read before the inner
	// directory is asked. A case uses it as a RENDEZVOUS: hold the claim open
	// until a peer has been observed meeting it, bounded by the case.
	Hold func(ctx context.Context)

	mu      sync.Mutex
	block   bool
	entered int
}

// Block makes every later Candidates read wait for its caller's context.
func (d *SlowDirectory) Block() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.block = true
}

// Entered reports how many Candidates reads have begun.
func (d *SlowDirectory) Entered() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.entered
}

// Owner satisfies factory.Directory.
func (d *SlowDirectory) Owner(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	return d.Inner.Owner(ctx, tenant, session)
}

// Candidates satisfies factory.Directory.
func (d *SlowDirectory) Candidates(ctx context.Context, req sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	d.mu.Lock()
	d.entered++
	block := d.block
	d.mu.Unlock()
	if block {
		<-ctx.Done()
		return sessionstore.HostTargetPage{}, ctx.Err()
	}
	if d.Hold != nil {
		d.Hold(ctx)
	}
	if d.Delay > 0 {
		timer := time.NewTimer(d.Delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return sessionstore.HostTargetPage{}, ctx.Err()
		}
	}
	return d.Inner.Candidates(ctx, req)
}

// ---- a pooled replica with the reconciliation seams exposed --------------------

// ReconcileReplicaOptions chooses what differs about one pooled replica.
type ReconcileReplicaOptions struct {
	Replica string
	Logs    io.Writer
	// ServiceToken is the HostLink credential this replica presents. Empty
	// takes PooledServiceToken, which the world's Hosts accept; anything else
	// is refused by every Host, which is how a case composes a replica with NO
	// HostLink while leaving its directory and durable plane real.
	ServiceToken string
	// WithoutPendingCommands composes no placement trigger: the replica serves,
	// admits, watches and sweeps deadlines, and never places.
	WithoutPendingCommands bool
	// Commands, when set, is this replica's command plane (and therefore its
	// placement claim seam and its pending-command reader).
	Commands *ReplicaCommands
	// Directory wraps the replica's real store directory.
	Directory func(factory.Directory) factory.Directory
	// DemandTimeout bounds one subscriber-demand poll. Zero takes 2s.
	DemandTimeout time.Duration
	// Interval and ClaimTTL override the sweep cadence and the claim
	// lifetime. Zero takes ReconcileSweepInterval and ReconcileClaimTTL.
	// Factory bounds each sweep PASS by Interval, so a case that holds a
	// claim open for longer than one interval must widen it here.
	Interval time.Duration
	ClaimTTL time.Duration
}

// ReconcileReplica is a running pooled Factory composed from options, with the
// bounds its demand plane was composed with. Its Stop is PooledFactory's.
type ReconcileReplica struct {
	*PooledFactory
	ID string
	// OwnershipPollInterval is the gap between two demand polls, as Factory
	// derives it: DemandReleaseDebounce + reconcile Interval
	// (factory compose.go composeLive).
	OwnershipPollInterval time.Duration
	// DemandTimeout bounds one demand poll.
	DemandTimeout time.Duration
}

// ReconcileSweepInterval and ReconcileClaimTTL are the pooled replicas' sweep
// cadence and claim lifetime -- the numbers startPooledFactory composes by
// default.
const (
	ReconcileSweepInterval = 200 * time.Millisecond
	ReconcileClaimTTL      = 2 * time.Second
	reconcileDebounce      = 200 * time.Millisecond
)

// StartReconcileReplica composes, starts and serves a real pooled Factory over
// the world. It is StartPooledFactoryWith with the reconciliation seams chosen
// by the case -- ONE Factory builder, so a reconciliation replica and an
// admission replica cannot drift apart in composition.
func StartReconcileReplica(tb TB, ctx context.Context, world *PooledWorld, options ReconcileReplicaOptions) *ReconcileReplica {
	tb.Helper()
	cfg := PooledFactoryConfig{
		Replica:                options.Replica,
		Logs:                   options.Logs,
		WithoutPendingCommands: options.WithoutPendingCommands,
		Directory:              options.Directory,
		ServiceToken:           options.ServiceToken,
		DemandTimeout:          options.DemandTimeout,
		Interval:               options.Interval,
		ClaimTTL:               options.ClaimTTL,
	}
	if cfg.DemandTimeout == 0 {
		cfg.DemandTimeout = 2 * time.Second
	}
	if options.Commands != nil {
		// Assigned only when set: a nil *ReplicaCommands in an interface is
		// not a nil interface.
		cfg.Commands = options.Commands
		cfg.Pending = options.Commands
	}
	interval := cfg.Interval
	if interval == 0 {
		interval = ReconcileSweepInterval
	}
	pooled := StartPooledFactoryWith(tb, ctx, world, cfg)
	if pooled == nil {
		return nil
	}
	return &ReconcileReplica{
		PooledFactory:         pooled,
		ID:                    options.Replica,
		OwnershipPollInterval: reconcileDebounce + interval,
		DemandTimeout:         cfg.DemandTimeout,
	}
}

type fixedToken string

func (t fixedToken) ServiceToken(context.Context) (string, error) { return string(t), nil }

// StartTappedPooledHost is StartPooledHost with a passive HostLink tap in front
// of the Host's real Routes().
func StartTappedPooledHost(tb TB, ctx context.Context, world *PooledWorld, id sessionwire.HostID, generation uint64, tap *HostLinkTap) *PooledHost {
	tb.Helper()
	return startHost(tb, ctx, world, id, generation, "", tap)
}

// ---- reading the tap -----------------------------------------------------------

// TappedAttach is one hostlink.attach RPC a Factory sent, and whether the Host
// accepted it.
//
// Accepted is best-effort: it is only true when the tap has ALSO recorded the
// Host's reply to this exact RPC by the time a caller reads it. A placement
// pass is bounded (200ms as of factory v0.6.0), so under load the reply can
// legitimately arrive after the pass has already moved on -- residency still
// took effect, but Accepted reads false and ReplySeen reads false. Judge
// duplicate placement by counting attach REQUESTS (and Host launches), not by
// counting accepted replies; treat Accepted/ReplySeen as informational.
type TappedAttach struct {
	Conn     int
	Token    string
	Request  sessionwire.HostLinkAttachRequest
	Accepted bool
	// ReplySeen reports whether the tap recorded any reply to this attach by
	// the time it was read. RawReply is that reply's raw JSON, or "no reply
	// observed" when ReplySeen is false.
	ReplySeen bool
	RawReply  string
}

// String reports the attach and its raw reply, so a %v/%+v of a
// []TappedAttach prints exactly what a triager needs on a failed row: no
// assembly required at the call site.
func (a TappedAttach) String() string {
	reply := a.RawReply
	if !a.ReplySeen {
		reply = "no reply observed"
	}
	return fmt.Sprintf("{conn:%d token:%q session:%s accepted:%t reply:%s}",
		a.Conn, a.Token, a.Request.SessionID, a.Accepted, reply)
}

// TappedLink is one HostLink connection attempt: the credential presented,
// whether the Host accepted the connect, and every RPC method and subscribe
// channel the client sent on it.
type TappedLink struct {
	Conn       int
	Token      string
	Connected  bool
	Methods    []string
	Subscribes []string
}

// TappedLinks decodes every connection the tap saw.
func TappedLinks(tb TB, tap *HostLinkTap) []TappedLink {
	tb.Helper()
	var links []TappedLink
	for _, conn := range tap.Conns() {
		commands, err := conn.Commands()
		if err != nil {
			tb.Fatalf("orchestrationtest: reading conn %d's commands: %v", conn.ID, err)
			return nil
		}
		replies, err := conn.Replies()
		if err != nil {
			tb.Fatalf("orchestrationtest: reading conn %d's replies: %v", conn.ID, err)
			return nil
		}
		link := TappedLink{Conn: conn.ID}
		for _, command := range commands {
			switch {
			case command.Connect != nil:
				link.Token = command.Connect.Token
				if reply, ok := ReplyTo(replies, command.ID); ok && reply.Error == nil && reply.Connect != nil {
					link.Connected = true
				}
			case command.RPC != nil:
				link.Methods = append(link.Methods, command.RPC.Method)
			case command.Subscribe != nil:
				link.Subscribes = append(link.Subscribes, command.Subscribe.Channel)
			}
		}
		links = append(links, link)
	}
	return links
}

// TappedAttaches decodes every hostlink.attach RPC the tap saw, in order.
func TappedAttaches(tb TB, tap *HostLinkTap) []TappedAttach {
	tb.Helper()
	var out []TappedAttach
	for _, conn := range tap.Conns() {
		commands, err := conn.Commands()
		if err != nil {
			tb.Fatalf("orchestrationtest: reading conn %d's commands: %v", conn.ID, err)
			return nil
		}
		replies, err := conn.Replies()
		if err != nil {
			tb.Fatalf("orchestrationtest: reading conn %d's replies: %v", conn.ID, err)
			return nil
		}
		token := ""
		for _, command := range commands {
			if command.Connect != nil {
				token = command.Connect.Token
			}
			if command.RPC == nil || command.RPC.Method != sessionwire.HostLinkMethodAttach {
				continue
			}
			var request sessionwire.HostLinkAttachRequest
			if err := json.Unmarshal(command.RPC.Data, &request); err != nil {
				tb.Fatalf("orchestrationtest: an attach on conn %d is not a Core attach request: %v", conn.ID, err)
				return nil
			}
			attach := TappedAttach{Conn: conn.ID, Token: token, Request: request}
			if reply, ok := ReplyTo(replies, command.ID); ok {
				attach.ReplySeen = true
				if reply.Error == nil && reply.RPC != nil {
					var refusal sessionwire.HostLinkError
					attach.Accepted = refusal.UnmarshalJSON(reply.RPC.Data) != nil
				}
				if raw, err := json.Marshal(reply); err == nil {
					attach.RawReply = string(raw)
				}
			}
			out = append(out, attach)
		}
	}
	return out
}

// ---- crash points inside the durable plane --------------------------------------

// CrashingOrderedIndex is the real OrderedIndex with one operation that can be
// made to fail. A second Store opened over it shares every byte with the
// fixture's own Store, so an operation that fails half way through leaves
// exactly the durable prefix a process crash at that point would leave.
type CrashingOrderedIndex struct {
	storage.OrderedIndex

	mu     sync.Mutex
	update bool
	delete bool
	fired  int
}

// FailUpdates arms or disarms Update.
func (o *CrashingOrderedIndex) FailUpdates(on bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.update = on
}

// FailDeletes arms or disarms Delete.
func (o *CrashingOrderedIndex) FailDeletes(on bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.delete = on
}

// Fired reports how many operations were failed.
func (o *CrashingOrderedIndex) Fired() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.fired
}

// Update satisfies storage.OrderedIndex.
func (o *CrashingOrderedIndex) Update(ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	o.mu.Lock()
	fail := o.update
	if fail {
		o.fired++
	}
	o.mu.Unlock()
	if fail {
		return storage.OrderedRecord{}, ErrInjectedStoreFault
	}
	return o.OrderedIndex.Update(ctx, id, expectedRevision, value, rank, due)
}

// Delete satisfies storage.OrderedIndex.
func (o *CrashingOrderedIndex) Delete(ctx context.Context, id storage.OrderedID, expectedRevision uint64) (storage.OrderedRecord, error) {
	o.mu.Lock()
	fail := o.delete
	if fail {
		o.fired++
	}
	o.mu.Unlock()
	if fail {
		return storage.OrderedRecord{}, ErrInjectedStoreFault
	}
	return o.OrderedIndex.Delete(ctx, id, expectedRevision)
}

// OpenCrashingStore opens a SECOND real Store over the fixture's own backend,
// with its ordered index behind a CrashingOrderedIndex. It is closed at
// cleanup.
func (f *StoreFixture) OpenCrashingStore(ctx context.Context) (*sessionstore.Store, *CrashingOrderedIndex) {
	f.tb.Helper()
	index := &CrashingOrderedIndex{OrderedIndex: f.Backend.OrderedIndex}
	backend := &storage.Composite{
		Ledger:       f.Backend.Ledger,
		Leaser:       f.Backend.Leaser,
		KV:           f.Backend.KV,
		Blobs:        f.Backend.Blobs,
		OrderedIndex: index,
	}
	store, err := sessionstore.Open(ctx, backend,
		sessionstore.WithClock(f.Clock),
		sessionstore.WithControlShards(KitControlShards))
	if err != nil {
		f.tb.Fatalf("orchestrationtest: opening the crashing store: %v", err)
		return nil, nil
	}
	f.tb.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = store.Close(closeCtx)
	})
	return store, index
}
