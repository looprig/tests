//go:build integration

package tests

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/hub"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/inference"
)

// The helpers below are faithful copies of the unexported package-session test helpers this
// e2e test depends on (from harness/pkg/session's restore_roundtrip_test.go,
// restore_workspace_test.go, and session_test.go). Only the package name (now e2e) and the
// qualification of harness symbols differ from the originals; behavior is identical. They use
// ONLY public harness APIs, so they compile from this external package.

// --- inference stub -----------------------------------------------------------------

// stubLLM is a controllable inference.Client for session tests.
type stubLLM struct {
	chunks           []content.Chunk
	blockUntilCancel bool
	ignoreCtx        bool // with blockUntilCancel: block forever (provider ignores ctx)
}

func (s *stubLLM) Invoke(ctx context.Context, req inference.Request) (*inference.Response, error) {
	return nil, errors.New("stubLLM.Invoke not used")
}

func (s *stubLLM) Stream(ctx context.Context, req inference.Request) (*inference.StreamReader[content.Chunk], error) {
	i := 0
	next := func() (content.Chunk, error) {
		if i < len(s.chunks) {
			c := s.chunks[i]
			i++
			return c, nil
		}
		if s.blockUntilCancel {
			if s.ignoreCtx {
				select {} // provider ignores cancellation; only safe under a bounded test
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return nil, io.EOF
	}
	return inference.NewStreamReader(next, nil), nil
}

// validModel returns a minimal but VALID inference.Model (passes inference.Model.Validate): a
// known provider speaking a supported dialect at a loopback endpoint. It replaces the
// retired ModelSpec in session tests that construct a loop.Config.
func validModel(name string) inference.Model {
	return inference.Model{
		Provider:  inference.ProviderName("lmstudio"),
		APIFormat: inference.APIFormatOpenAI,
		BaseURL:   "http://localhost:1234",
		Name:      name,
	}
}

// --- store / journal / lease wiring -------------------------------------------------

func mustSessionID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	return id
}

// mustAcquireLease acquires single-writer ownership of sid's stream through the store.
func mustAcquireLease(t *testing.T, store *sessionstore.Store, sid uuid.UUID) journal.Lease {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lease, err := store.AcquireLease(ctx, sid)
	if err != nil {
		t.Fatalf("AcquireLease for %v: %v", sid, err)
	}
	return lease
}

// handOver releases the original run's lease so Restore can acquire single-writer
// ownership (the handover boundary). A failed release fails the test loudly.
func handOver(t *testing.T, lease journal.Lease) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release original lease (handover): %v", err)
	}
}

// testFactory mints deterministic, monotonically increasing EventIDs and a fixed
// CreatedAt so persisted events get stable, non-zero ids/times for journal dedup.
func testFactory() *event.Factory {
	var n byte = 0x90
	ts := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	return event.NewFactory(func() (uuid.UUID, error) {
		n++
		return uuid.UUID{n}, nil
	}, func() time.Time { return ts })
}

// eventStamper mints a fresh, distinct EventID for each directly-published event so the
// journal's idempotency id (the EventID) never collides — a zero EventID on every event
// would dedup them all to one. The hub does NOT stamp a TRIGGERING event (only its
// derived session events), so a direct publisher must stamp them itself, exactly as the
// real loop's eventFactory does for the events it emits.
type eventStamper struct{ n byte }

// stamp sets a fresh EventID + CreatedAt on ev's Header and publishes it through the
// journal-backed hub, failing the test on a publish error.
func (es *eventStamper) stamp(t *testing.T, ctx context.Context, h *hub.Hub, ev event.Event) {
	t.Helper()
	es.n++
	hdr := ev.EventHeader()
	hdr.EventID = uuid.UUID{0xE0, es.n}
	hdr.CreatedAt = time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	stamped := setHeader(t, ev, hdr)
	if err := h.PublishEvent(ctx, stamped); err != nil {
		t.Fatalf("PublishEvent(%T): %v", stamped, err)
	}
}

// setHeader returns a copy of a directly-published original-run event with hdr
// substituted. The set is exactly the events the original-run builders publish.
func setHeader(t *testing.T, ev event.Event, hdr event.Header) event.Event {
	t.Helper()
	switch e := ev.(type) {
	case event.SessionStarted:
		e.Header = hdr
		return e
	case event.LoopStarted:
		e.Header = hdr
		return e
	case event.TurnStarted:
		e.Header = hdr
		return e
	case event.StepDone:
		e.Header = hdr
		return e
	case event.TurnFoldedInto:
		e.Header = hdr
		return e
	case event.TurnDone:
		e.Header = hdr
		return e
	case event.LoopIdle:
		e.Header = hdr
		return e
	case event.WorkspaceCheckpointed:
		e.Header = hdr
		return e
	default:
		t.Fatalf("setHeader: unexpected event %T", ev)
		return nil
	}
}

// --- loop.Config builders -----------------------------------------------------------

// restoreCfg is the loop.Config both the original run AND the restore use. A System
// prompt + model id make the config fingerprint non-empty, so match/mismatch is real.
func restoreCfg(client inference.Client, model, system string) loop.Config {
	return loop.Config{
		Client:       client,
		Model:        validModel(model),
		System:       system,
		DrainTimeout: 200 * time.Millisecond,
	}
}

// restoreCfgNamed is restoreCfg with an AgentName set, so a restore can validate the
// configured primary's attribution name against the persisted root loop's stamped name.
func restoreCfgNamed(client inference.Client, model, system string, agent identity.AgentName) loop.Config {
	c := restoreCfg(client, model, system)
	c.AgentName = agent
	return c
}

// --- original-run wiring ------------------------------------------------------------

// persistedStream is the durable record of an ORIGINAL session run plus the facts the
// restore assertions need: the session/loop ids, the still-held lease (released for the
// handover), and the committed state the original ended with.
type persistedStream struct {
	sessionID     uuid.UUID
	primaryLoopID uuid.UUID
	lease         journal.Lease
	committedMsgs content.AgenticMessages
	committedTurn event.TurnIndex
}

// newOriginalHub wires a journal-backed hub for an original run with an UNNAMED root
// loop (the common case). It is newOriginalHubNamed with an empty AgentName.
func newOriginalHub(t *testing.T, store *sessionstore.Store, fp event.ConfigFingerprint) (*hub.Hub, uuid.UUID, uuid.UUID, journal.Lease, *eventStamper) {
	t.Helper()
	return newOriginalHubNamed(t, store, fp, "")
}

// newOriginalHubNamed wires a journal-backed hub for an original run (the durable-tap
// wiring): a real SessionJournal over a freshly-acquired lease, a JournalEventAppender as
// the hub's required durable tap, and a deterministic Factory. It stamps the root
// LoopStarted with agentName — exactly what NewLoop does from cfg.AgentName on a fresh run
// — so a restore can validate the persisted root name. It returns the hub, the session/loop
// ids, the held lease, and the stamper used for direct publishes.
func newOriginalHubNamed(t *testing.T, store *sessionstore.Store, fp event.ConfigFingerprint, agentName identity.AgentName) (*hub.Hub, uuid.UUID, uuid.UUID, journal.Lease, *eventStamper) {
	t.Helper()
	sessionID := mustSessionID(t)
	primaryLoopID := mustSessionID(t)
	lease := mustAcquireLease(t, store, sessionID)

	openCtx, openCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer openCancel()
	j, err := store.OpenJournal(openCtx, sessionID, lease)
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	h := hub.New(sessionID, hub.WithAppender(journal.NewJournalEventAppender(j)), hub.WithFactory(testFactory()))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	es := &eventStamper{}
	// The session records its start (carrying the config fingerprint) + the root loop —
	// exactly what newSession/NewLoop publish on a fresh run.
	es.stamp(t, ctx, h, event.SessionStarted{
		Header: event.Header{Coordinates: identity.Coordinates{SessionID: sessionID}},
		Config: fp,
	})
	es.stamp(t, ctx, h, event.LoopStarted{
		Header: event.Header{
			Coordinates: identity.Coordinates{SessionID: sessionID, LoopID: primaryLoopID},
			AgentName:   agentName,
		},
	})
	return h, sessionID, primaryLoopID, lease, es
}

// --- workspace tree helpers ---------------------------------------------------------

// wsTreeNode is a location-independent snapshot of one filesystem node used to assert two
// trees are logically identical. perm is zeroed for symlinks (link perms are platform
// noise); content applies to files; target applies to symlinks.
type wsTreeNode struct {
	isDir   bool
	perm    os.FileMode
	content string
	target  string
}

// wsBuildTree writes a small representative tree under root — a regular file, an
// executable in a subdirectory, and a relative symlink — so a workspace round-trip
// exercises contents, modes, and symlinks. marker is woven into file contents so distinct
// markers produce content-distinct (and thus Ref-distinct) trees, which the last-checkpoint
// -wins case relies on.
func wsBuildTree(t *testing.T, root, marker string) {
	t.Helper()
	write := func(rel, content string, perm os.FileMode) {
		abs := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", filepath.Dir(abs), err)
		}
		if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
			t.Fatalf("WriteFile(%q): %v", abs, err)
		}
		if err := os.Chmod(abs, perm); err != nil {
			t.Fatalf("Chmod(%q): %v", abs, err)
		}
	}
	write("readme.txt", "hello "+marker+"\n", 0o644)
	write("bin/run.sh", "#!/bin/sh\necho "+marker+"\n", 0o755)
	if err := os.Symlink("readme.txt", filepath.Join(root, "link")); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
}

// wsSnapshotTree walks root and returns a slash-relative map of its nodes (the root itself
// excluded), reading file contents and symlink targets so two snapshots compare by value,
// independent of where each tree lives on disk.
func wsSnapshotTree(t *testing.T, root string) map[string]wsTreeNode {
	t.Helper()
	out := make(map[string]wsTreeNode)
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root {
			return nil
		}
		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		info, infoErr := d.Info()
		if infoErr != nil {
			return infoErr
		}
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, rlErr := os.Readlink(p)
			if rlErr != nil {
				return rlErr
			}
			out[rel] = wsTreeNode{target: target}
		case info.IsDir():
			out[rel] = wsTreeNode{isDir: true, perm: info.Mode().Perm()}
		default:
			data, rdErr := os.ReadFile(p) // #nosec G304 -- test-controlled tree under t.TempDir
			if rdErr != nil {
				return rdErr
			}
			out[rel] = wsTreeNode{perm: info.Mode().Perm(), content: string(data)}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("wsSnapshotTree(%q): %v", root, err)
	}
	return out
}

// wsAssertTreesEqual fails unless the trees rooted at want and got are logically identical
// (names, contents, permission modes, symlink targets) — the property content-addressing
// relies on. It compares by value so the two trees may live at different paths on disk.
func wsAssertTreesEqual(t *testing.T, want, got string) {
	t.Helper()
	w := wsSnapshotTree(t, want)
	g := wsSnapshotTree(t, got)
	if !reflect.DeepEqual(w, g) {
		t.Errorf("materialized tree mismatch:\n want %v\n got  %v", w, g)
	}
}

// stampCheckpoint wires an original run (SessionStarted + root LoopStarted) and stamps a
// WorkspaceCheckpointed for each ref in order through the journal-backed hub — the durable
// record the restore path reads back. Each ref must already be durable in the shared ws
// Blobs (via a prior Snapshot) for the happy/warm paths; the failure paths deliberately
// stamp a ref with no backing blob or a malformed ref. The lease is left held for the
// caller to release (handover).
func stampCheckpoint(t *testing.T, store *sessionstore.Store, fp event.ConfigFingerprint, refs ...string) persistedStream {
	t.Helper()
	h, sessionID, primaryLoopID, lease, es := newOriginalHub(t, store, fp)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, ref := range refs {
		es.stamp(t, ctx, h, event.WorkspaceCheckpointed{
			Header: event.Header{Coordinates: identity.Coordinates{SessionID: sessionID}},
			Ref:    ref,
		})
	}
	return persistedStream{sessionID: sessionID, primaryLoopID: primaryLoopID, lease: lease}
}

// --- restore-tail assertions --------------------------------------------------------

// restoreEventTail replays the stream scoped to the primary loop (session events + that
// loop's events, in stream order) and returns only the restore-lifecycle events
// (RestoreStarted/RestoreDone/RestoreErrored and any TurnInterrupted that closed an open
// turn) — the tail the assertions check. It goes through the SAME facade Restore uses:
// FromSeq 0 on the replayer, the primary LoopID narrowing carried on the Open request.
func restoreEventTail(t *testing.T, store *sessionstore.Store, sessionID, primaryLoopID uuid.UUID) []event.Event {
	t.Helper()
	r, err := store.OpenEventReplayer(sessionID, sessionstore.ReplayRequest{FromSeq: 0})
	if err != nil {
		t.Fatalf("OpenEventReplayer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cursor, err := r.Open(ctx, journal.ReplayRequest{LoopID: primaryLoopID, Follow: false})
	if err != nil {
		t.Fatalf("replay Open: %v", err)
	}
	defer func() { _ = cursor.Close() }()

	var tail []event.Event
	for {
		ev, _, err := cursor.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("replay Next: %v", err)
		}
		switch ev.(type) {
		case event.RestoreStarted, event.RestoreDone, event.RestoreErrored, event.TurnInterrupted:
			tail = append(tail, ev)
		}
	}
	return tail
}

// assertTail fails unless got's concrete types match want's, in order.
func assertTail(t *testing.T, got, want []event.Event) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("restore-event tail = %v, want %v", tailTypes(got), tailTypes(want))
	}
	for i := range want {
		if reflect.TypeOf(got[i]) != reflect.TypeOf(want[i]) {
			t.Errorf("restore-event tail[%d] = %T, want %T", i, got[i], want[i])
		}
	}
}

func tailTypes(evs []event.Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = reflect.TypeOf(e).String()
	}
	return out
}
