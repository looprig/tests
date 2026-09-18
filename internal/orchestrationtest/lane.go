//go:build integration

package orchestrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/host/department"
	"github.com/looprig/sessionstore"
)

// FactoryHostLane is one released Factory and one released, RUNNING Host over
// one shared durable plane: the smallest composition in which Factory's own
// HostLink dialer reaches a real Host.
//
// Nothing between the two is a shim. Factory dials the endpoint Host itself
// advertised in its registry record, with Factory's real CentrifugeDialer, over
// a real loopback WebSocket; the only thing in the path is ComposedHost's
// passive tap. What makes Factory dial at all is DEMAND -- a ClientLink viewer
// watching the session -- because that is the only trigger factory v0.2.0 has:
// routing.Demand is the sole caller of the pool's Bind.
type FactoryHostLane struct {
	Clock     *Clock
	Store     *StoreFixture
	Host      *ComposedHost
	Factory   *FactoryFixture
	Directory *PinnedDirectory
}

// LaneStorageBinding and LaneBindingVersion are the deployment's SessionBinding.
// Factory stamps them into every create; Host resolves settlement evidence by
// (tenant, StorageBindingID). They must agree or no command could ever settle.
const (
	LaneStorageBinding = "orchestrationtest-binding"
	LaneBindingVersion = "v1"
	LaneHostID         = sessionwire.HostID("orchestrationtest-host-a")
	LaneHostGeneration = uint64(7)
	LaneAgent          = sessionwire.AgentID("orchestrationtest-lane-agent")
	LaneCompatibility  = department.CompatibilityID("orchestrationtest-lane-compat-1")
)

// NewFactoryHostLane composes the lane. The Host is composed FIRST so that its
// cleanup runs LAST: Factory's links close before the Host drains.
//
// The kit clock starts at wall-clock now rather than the kit's fixed epoch, and
// that is a mechanism. Factory stamps a command's AcceptedAt and ApplyDeadline
// from the clock it was composed with, and the Host -- a separate process with
// its own system clock -- consumes that command. A Factory clock years in the
// past would hand Host commands whose deadline had long expired.
func NewFactoryHostLane(tb TB, ctx context.Context) *FactoryHostLane {
	tb.Helper()
	clock := NewClock(time.Now().UTC())
	store := NewStoreFixture(tb, ctx, clock)
	composed := NewComposedHost(tb, ctx, store, ComposedHostConfig{
		ID:               LaneHostID,
		Generation:       LaneHostGeneration,
		Agent:            LaneAgent,
		Compatibility:    LaneCompatibility,
		StorageBindingID: LaneStorageBinding,
	})
	directory := &PinnedDirectory{Inner: &StoreDirectory{Store: store.Store}}
	served := NewFactoryFixtureWithSeams(tb, store, clock, FactorySeams{
		Directory: directory,
		Templates: []factory.LaunchTemplate{{Key: sessionstore.HostTargetKey{
			AgentID:                LaneAgent,
			RuntimeCompatibilityID: string(LaneCompatibility),
			Placement:              sessionwire.HostPlacementPooled,
		}}},
		SessionBindingID:      LaneStorageBinding,
		SessionBindingVersion: LaneBindingVersion,
	})
	return &FactoryHostLane{Clock: clock, Store: store, Host: composed, Factory: served, Directory: directory}
}

// CreateBody is a V1 create request, byte for byte what a browser would POST.
func CreateBody(session sessionwire.SessionID, command sessionwire.CommandID) []byte {
	return []byte(fmt.Sprintf(`{"version":1,"command_id":%q,"session_id":%q,"agent_id":%q}`, command, session, LaneAgent))
}

// Create POSTs a V1 create through Factory and returns the durable disposition
// record it filed. A retry of the SAME body is Factory's idempotent replay, and
// it is also -- as of factory v0.2.0 -- the only admission whose delivery can
// reach a Host-resident session; see the lane test for why.
func (l *FactoryHostLane) Create(tb TB, ctx context.Context, session sessionwire.SessionID, command sessionwire.CommandID) sessionstore.DispositionInboxEntry {
	tb.Helper()
	status, body := l.Factory.Post(tb, ctx, "/v1/sessions", CreateBody(session, command))
	if status != http.StatusCreated {
		tb.Fatalf("orchestrationtest: POST /v1/sessions for %s answered %d: %s", session, status, body)
		return sessionstore.DispositionInboxEntry{}
	}
	return l.Command(tb, ctx, session, command)
}

// Command reads one disposition record straight from the durable plane.
func (l *FactoryHostLane) Command(tb TB, ctx context.Context, session sessionwire.SessionID, command sessionwire.CommandID) sessionstore.DispositionInboxEntry {
	tb.Helper()
	entry, err := l.Store.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID: l.Store.Tenant, SessionID: session, CommandID: command,
	})
	if err != nil {
		tb.Fatalf("orchestrationtest: reading disposition command %s/%s: %v", session, command, err)
		return sessionstore.DispositionInboxEntry{}
	}
	return entry
}

// AwaitCommandState polls the durable record until it reaches want, failing at
// ctx's deadline. Polling a durable read is the waitable state here: the
// transition is written by another process's consumer, which signals nobody.
func (l *FactoryHostLane) AwaitCommandState(tb TB, ctx context.Context, session sessionwire.SessionID, command sessionwire.CommandID, want sessionstore.InboxState) sessionstore.DispositionInboxEntry {
	tb.Helper()
	for {
		entry := l.Command(tb, ctx, session, command)
		if entry.Record.State == want {
			return entry
		}
		select {
		case <-ctx.Done():
			tb.Fatalf("orchestrationtest: %s/%s is %q, never reached %q: %v", session, command, entry.Record.State, want, ctx.Err())
			return entry
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// HostLinks reports the tapped connections that arrived on the HostLink path.
func (l *FactoryHostLane) HostLinks() []*TappedConn {
	var out []*TappedConn
	for _, conn := range l.Host.Tap.Conns() {
		if strings.HasPrefix(conn.Path, "/hostlink/") {
			out = append(out, conn)
		}
	}
	return out
}

// OnlyHostLink returns the single HostLink Factory opened, failing otherwise.
func (l *FactoryHostLane) OnlyHostLink(tb TB) *TappedConn {
	tb.Helper()
	links := l.HostLinks()
	if len(links) != 1 {
		tb.Fatalf("orchestrationtest: the Host saw %d HostLink connections, want exactly the one Factory dialled", len(links))
		return nil
	}
	return links[0]
}

// PinnedDirectory is factory.Directory over the real Store, with one override:
// a case may PIN the owner Factory is told for a named session.
//
// A pin models a STALE registry read -- an owner record naming a Host that no
// longer holds the session, which is what a lapsed or superseded registration
// looks like to a Factory between two polls. It is how a case makes Factory
// bind to a Host for a session that Host is not holding without a second Host
// or a real expiry, and it is the ONLY thing here that is not the Store's own
// answer. A session with no pin is answered by StoreDirectory unchanged.
type PinnedDirectory struct {
	Inner *StoreDirectory

	mu   sync.Mutex
	pins map[sessionwire.SessionID]sessionwire.HostLinkRegistryObservation
}

// Pin makes Owner answer observation for session.
func (d *PinnedDirectory) Pin(session sessionwire.SessionID, observation sessionwire.HostLinkRegistryObservation) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pins == nil {
		d.pins = map[sessionwire.SessionID]sessionwire.HostLinkRegistryObservation{}
	}
	d.pins[session] = observation
}

// Owner satisfies factory.Directory.
func (d *PinnedDirectory) Owner(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	d.mu.Lock()
	pinned, ok := d.pins[session]
	d.mu.Unlock()
	if ok {
		return pinned, true, nil
	}
	return d.Inner.Owner(ctx, tenant, session)
}

// Candidates satisfies factory.Directory.
func (d *PinnedDirectory) Candidates(ctx context.Context, req sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	return d.Inner.Candidates(ctx, req)
}

// DecodeBind decodes a Factory bind with Core's strict decoder.
func DecodeBind(tb TB, data json.RawMessage) sessionwire.HostLinkBindRequest {
	tb.Helper()
	var bind sessionwire.HostLinkBindRequest
	if err := json.Unmarshal(data, &bind); err != nil {
		tb.Fatalf("orchestrationtest: Core refused the bind Factory sent (%s): %v", data, err)
	}
	return bind
}
