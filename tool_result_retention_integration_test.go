//go:build integration

// This file is runbook 07 I2.2: a tool's FULL output outlives the workspace
// and the Host that produced it, and is readable afterwards by the model that
// was shown only a preview AND by an authorized client -- never by anyone else.
//
// Everything on the path is released code composed the way a product composes
// it (orchestrationtest/toolresults.go): a real Factory placing a session on a
// real Host whose runtime is a real harness rig, running REAL Bash (tools)
// under readable retention (harness rig.WithToolResultObjects over the
// tenant's own journal store), paging with the REAL read_tool_result, and a
// Factory object route composed with the production evidence policy
// (internal/toolresultobjects, built on harness LookupToolResultCapture). The
// only scripted part is the model, and it can only act on what it is SHOWN:
// it learns a capture id from the retention marker and each next offset from a
// page footer.
//
// Case map (the task's numbering):
//
//  1. oversized result -> object; model sees preview + marker; read_tool_result
//     pages reassemble the exact bytes; every page <= ResultBytes;
//  2. a client reads the object through Factory's object route, digest
//     verified;
//  3. isolation: other session, other tenant, forged id, harness-grammar id
//     over the real digest, and an ORPHAN with a perfectly good metadata row
//     are all the same 404, with no captured byte in any answer;
//  4. after warm release, Host replacement, workspace wipe and a Factory
//     restart (cold evidence cache), the capture is still readable by a
//     restored read_tool_result and through the new Factory;
//  5. a small result creates no object;
//  6. the capture ceiling: D7 composition bound, a result exactly AT the
//     8 MiB default ceiling (retained whole), and one over it (truncated,
//     recorded as such, the marker says the tail is unavailable, the retained
//     prefix is exact).

package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/factory"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
	"github.com/looprig/tests/internal/toolresultobjects"
)

const (
	// retentionPreview is the model's preview budget (ToolLimits.ResultBytes).
	retentionPreview = 8192
	// retentionCeiling is the capture ceiling the world composes: harness's
	// default, left unset in the composition on purpose, so the lane proves
	// the production default rather than a test-sized one.
	retentionCeiling = loop.DefaultToolResultCaptureBytes
	// retentionFactoryPage is the page size a client reads objects in.
	retentionFactoryPage = 1 << 20
)

// awkLines is a Bash command printing n numbered 13-byte lines, and
// linesOutput is exactly what Bash returns (and captures) for it.
func awkLines(n int) string {
	return fmt.Sprintf(`awk 'BEGIN{for(i=0;i<%d;i++) printf "line %%07d\n", i}'`, n)
}

func linesBody(n int) []byte {
	var b bytes.Buffer
	b.Grow(13*n + 16)
	for i := range n {
		fmt.Fprintf(&b, "line %07d\n", i)
	}
	return b.Bytes()
}

const exitTrailer = "[exit code: 0]"

func sha256Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// retentionTurn submits text to a session (a create the first time) and waits
// for the turn it starts to END, returning the captures committed during it.
func retentionTurn(t *testing.T, ctx context.Context, world *orchestrationtest.PooledWorld, served *orchestrationtest.PooledFactory,
	tenant sessionwire.TenantID, s sessionwire.SessionID, command string, create bool, text string) []event.ToolResultCapture {
	t.Helper()
	var runtime uuid.UUID
	turnsBefore, stepsBefore := 0, 0
	if !create {
		runtime = world.RuntimeSessionID(t, ctx, tenant, s)
		turnsBefore = orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtime)
		stepsBefore = len(orchestrationtest.JournalEvents[event.StepDone](t, world, tenant, runtime))
	}
	blocks, err := json.Marshal([]map[string]string{{"type": "text", "text": text}})
	if err != nil {
		t.Fatalf("encoding blocks: %v", err)
	}
	if create {
		status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(command),
			SessionID:       s,
			AgentID:         orchestrationtest.PooledAgent,
			Blocks:          blocks,
		})
		if status != http.StatusCreated {
			t.Fatalf("the create of %s/%s answered %d: %s", tenant, s, status, body)
		}
	} else {
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope(command),
			SessionID:       s,
			Blocks:          blocks,
		})
		if status != http.StatusOK {
			t.Fatalf("the input to %s/%s answered %d: %s", tenant, s, status, body)
		}
	}
	orchestrationtest.PooledWait(t, "command "+command+" applied", 120*time.Second, func() bool {
		return world.CommandState(ctx, tenant, s, sessionwire.CommandID(command)) == sessionstore.InboxStateApplied
	})
	if create {
		runtime = world.RuntimeSessionID(t, ctx, tenant, s)
	}
	orchestrationtest.PooledWait(t, "the turn of "+command+" ended", 120*time.Second, func() bool {
		return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtime) > turnsBefore
	})
	var captures []event.ToolResultCapture
	for _, done := range orchestrationtest.JournalEvents[event.StepDone](t, world, tenant, runtime)[stepsBefore:] {
		captures = append(captures, done.Captures...)
	}
	return captures
}

// referenced returns the one capture in captures that carries an object
// reference, failing the case unless there is exactly one.
func referenced(t *testing.T, captures []event.ToolResultCapture) event.ToolResultCapture {
	t.Helper()
	var found []event.ToolResultCapture
	for _, capture := range captures {
		if capture.Reference != nil {
			found = append(found, capture)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one capture with an object reference, got %d in %+v", len(found), captures)
	}
	return found[0]
}

// assertAbsent checks one refused object read: the absent-object 404, byte
// for byte the same as absentBody, carrying none of the captured bytes.
func assertAbsent(t *testing.T, what string, answer orchestrationtest.ObjectResponse, absentBody []byte, secret []byte) {
	t.Helper()
	if answer.Status != http.StatusNotFound {
		t.Fatalf("%s answered %d (%s), want the absent-object 404", what, answer.Status, answer.Body)
	}
	if !bytes.Equal(answer.Body, absentBody) {
		t.Fatalf("%s answered %s, not byte-identical to an absent object's %s", what, answer.Body, absentBody)
	}
	if len(secret) >= 32 && bytes.Contains(answer.Body, secret[:32]) {
		t.Fatalf("%s leaked captured bytes", what)
	}
	if answer.Header.Get("X-Object-Digest") != "" || answer.Header.Get("X-Object-Size") != "" {
		t.Fatalf("%s leaked object headers: %v", what, answer.Header)
	}
}

func TestToolOutputOutlivesTheWorkspaceAndIsReadableOnlyByItsSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	tenantA, tenantB := orchestrationtest.PooledTenantA, orchestrationtest.PooledTenantB
	const (
		s1 = sessionwire.SessionID("i22-session-one")
		s2 = sessionwire.SessionID("i22-session-two")
	)
	retention := &orchestrationtest.ToolResultRetention{ResultBytes: retentionPreview}
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		WithWorkspace: true,
		ToolResults:   retention,
	})
	model := &orchestrationtest.ToolResultModel{}
	world.LLM.Respond(model.Respond)

	t.Run("case 6a: the composition bounds the capture ceiling by Factory's verification ceiling (D7)", func(t *testing.T) {
		limits := retention.ObjectLimits()
		if err := toolresultobjects.CheckCaptureCeiling(retention.CaptureBytes, limits); err != nil {
			t.Fatalf("the lane's own composition fails D7: %v", err)
		}
		if got := toolresultobjects.EffectiveCaptureBytes(retention.CaptureBytes); got != retentionCeiling || uint64(got) > limits.MaxVerificationBytes || limits.MaxVerificationBytes != 64<<20 {
			t.Fatalf("effective ceiling %d against verification %d, want harness's %d within Factory's 64 MiB", got, limits.MaxVerificationBytes, retentionCeiling)
		}
		for name, bad := range map[string]struct {
			capture int
			limits  factory.ObjectLimits
		}{
			"a ceiling above 64 MiB":                    {64<<20 + 1, limits},
			"the default ceiling over a lowered bound":  {0, factory.ObjectLimits{MaxPageBytes: 1 << 20, MaxVerificationBytes: 4 << 20}},
			"a verification bound Factory would refuse": {1 << 20, factory.ObjectLimits{MaxPageBytes: 1 << 20, MaxVerificationBytes: 128 << 20}},
		} {
			_, err := toolresultobjects.FactoryOptions(toolresultobjects.Config{
				Binding: toolresultobjects.Binding{StorageBindingID: orchestrationtest.PooledBinding, BindingVersion: orchestrationtest.PooledBindingVersion},
				Evidence: func(sessionwire.TenantID) (toolresultobjects.Evidence, bool) {
					return world.Journals[tenantA], true
				},
				Objects: func(sessionwire.TenantID) (factory.ObjectReader, bool) {
					return world.RuntimeJournals[tenantA], true
				},
				CaptureBytes: bad.capture, Limits: bad.limits,
			})
			if !errors.Is(err, toolresultobjects.ErrCaptureCeiling) {
				t.Fatalf("%s: composed without the D7 refusal: %v", name, err)
			}
		}
	})

	first := orchestrationtest.StartLifecycleHost(t, ctx, world, "i22-first", 4, orchestrationtest.PooledHostConfig{
		WarmTTL: lifecycleWarmTTL, WorkPoll: lifecycleWorkPoll,
	})
	orchestrationtest.AwaitAdvertised(t, world, first.ID)
	served := orchestrationtest.StartPooledFactory(t, ctx, world, "i22-replica-1", nil)

	// ---- case 5: a small result is its own complete capture ---------------
	small := retentionTurn(t, ctx, world, served, tenantA, s1, "i22-a1-create", true, orchestrationtest.ToolResultRun+"printf 'hello retention\\n'")
	runtimeA1 := world.RuntimeSessionID(t, ctx, tenantA, s1)
	t.Run("case 5: a small result creates no object", func(t *testing.T) {
		shown := model.TakeShown()
		if len(small) != 1 || small[0].Reference != nil || small[0].Truncated {
			t.Fatalf("small result captures = %+v, want one reference-free, untruncated capture", small)
		}
		if want := "hello retention\n" + exitTrailer; len(shown) != 1 || shown[0] != want {
			t.Fatalf("the model was shown %q, want exactly %q with no marker", shown, want)
		}
		if keys := world.ToolResultObjectKeys(t, ctx, tenantA, runtimeA1.String()); len(keys) != 0 {
			t.Fatalf("a small result wrote tool-result objects: %v", keys)
		}
	})

	// ---- case 1: an oversized result is retained and paged back -----------
	const mediumLines = 9000
	medium := append(linesBody(mediumLines), exitTrailer...)
	mediumCaptures := retentionTurn(t, ctx, world, served, tenantA, s1, "i22-a1-medium", false, orchestrationtest.ToolResultRunAndPage+awkLines(mediumLines))
	capture := referenced(t, mediumCaptures)
	objectID := capture.Reference.ObjectID
	captureID := capture.ToolExecutionID.String()
	t.Run("case 1: the oversized result is an object, previewed with a marker, and paged back exactly", func(t *testing.T) {
		shown := model.TakeShown()
		pages := model.TakePages()
		if capture.Truncated || capture.CapturedBytes != uint64(len(medium)) {
			t.Fatalf("capture = %+v, want an untruncated %d-byte capture", capture, len(medium))
		}
		if original, exact := capture.OriginalSize(); !exact || original != uint64(len(medium)) {
			t.Fatalf("capture original size = %d (exact=%v), want exactly %d", original, exact, len(medium))
		}
		if !strings.HasPrefix(objectID, "v1:"+string(sessionstore.ObjectKindToolResult)+":") {
			t.Fatalf("object id %q is not a store-issued tool-result reference", objectID)
		}
		if len(shown) < 2 {
			t.Fatalf("the model was shown %d tool results, want the preview then pages", len(shown))
		}
		preview := shown[0]
		wantMarker := fmt.Sprintf("\n[tool output shaped; all %d bytes retained; read the rest with read_tool_result capture_id=%q]\n", len(medium), captureID)
		if !strings.HasSuffix(preview, wantMarker) {
			t.Fatalf("the preview ends %q, want the readable marker %q", preview[max(0, len(preview)-200):], wantMarker)
		}
		if len(preview) > retentionPreview {
			t.Fatalf("the preview is %d bytes, over the %d-byte model budget", len(preview), retentionPreview)
		}
		if !strings.HasPrefix(preview, "line 0000000\nline 0000001\n") {
			t.Fatalf("the preview does not start with the output's head: %.80q", preview)
		}
		if len(pages) < 2 {
			t.Fatalf("the model read %d pages, want the capture over several", len(pages))
		}
		for i, page := range pages {
			if len(page) > retentionPreview {
				t.Fatalf("page %d is %d bytes, over the %d-byte preview budget", i, len(page), retentionPreview)
			}
		}
		reassembled, total := orchestrationtest.ReassemblePages(t, captureID, pages)
		if total != uint64(len(medium)) || !bytes.Equal(reassembled, medium) {
			t.Fatalf("the pages reassemble %d bytes (footer total %d), want the exact %d-byte output", len(reassembled), total, len(medium))
		}
		// Pages are never themselves retained: one object for this session.
		if keys := world.ToolResultObjectKeys(t, ctx, tenantA, runtimeA1.String()); len(keys) != 1 {
			t.Fatalf("the session holds %d tool-result objects, want exactly the one capture: %v", len(keys), keys)
		}
	})

	// ---- case 2: a client reads the same bytes through Factory ------------
	var absentBody []byte
	t.Run("case 2: an authorized client reads the exact bytes through Factory's object route", func(t *testing.T) {
		whole := served.GetObject(t, ctx, tenantA, s1, objectID, false, "")
		if whole.Status != http.StatusOK || !bytes.Equal(whole.Body, medium) {
			t.Fatalf("the whole-object read answered %d with %d bytes, want 200 and the exact %d bytes", whole.Status, len(whole.Body), len(medium))
		}
		if got := whole.Header.Get("X-Object-Digest"); got != sha256Digest(medium) {
			t.Fatalf("X-Object-Digest = %q, want %q", got, sha256Digest(medium))
		}
		if got := whole.Header.Get("X-Object-Size"); got != fmt.Sprint(len(medium)) {
			t.Fatalf("X-Object-Size = %q, want %d", got, len(medium))
		}
		paged, metadata := served.ReadObjectPaged(t, ctx, tenantA, s1, objectID, 10_000)
		if !bytes.Equal(paged, medium) || metadata.Digest != sha256Digest(medium) || metadata.SizeBytes != uint64(len(medium)) || metadata.Reference.ObjectID != objectID {
			t.Fatalf("ranged pages reassemble %d bytes with metadata %+v, want the exact %d bytes and digest %s", len(paged), metadata, len(medium), sha256Digest(medium))
		}
		// The runtime session id never reaches the client.
		if bytes.Contains(mustJSON(t, metadata), []byte(runtimeA1.String())) || strings.Contains(fmt.Sprint(whole.Header), runtimeA1.String()) {
			t.Fatalf("an object answer names the runtime session %s", runtimeA1)
		}
		// A reference in the store's own canonical grammar -- this capture's
		// generation, another digest -- that nothing ever issued.
		never := sha256.Sum256([]byte("never issued"))
		absent := served.GetObject(t, ctx, tenantA, s1, strings.Join(append(strings.Split(objectID, ":")[:3], hex.EncodeToString(never[:])), ":"), false, "")
		if absent.Status != http.StatusNotFound {
			t.Fatalf("a never-issued reference answered %d: %s", absent.Status, absent.Body)
		}
		absentBody = absent.Body
	})
	if absentBody == nil {
		t.Fatal("case 2 did not establish the absent-object answer")
	}

	// A second session in tenant A, and a session in tenant B under the SAME
	// public id as A's first, each with its own real capture.
	otherCaptures := retentionTurn(t, ctx, world, served, tenantA, s2, "i22-a2-create", true, orchestrationtest.ToolResultRun+awkLines(2000))
	otherObject := referenced(t, otherCaptures)
	bCaptures := retentionTurn(t, ctx, world, served, tenantB, s1, "i22-b1-create", true, orchestrationtest.ToolResultRun+awkLines(3000))
	bObject := referenced(t, bCaptures)
	model.TakeShown()

	// ---- case 3: isolation ------------------------------------------------
	t.Run("case 3: another session, another tenant, a forged id and an orphan are all absent", func(t *testing.T) {
		// Positive controls first: each session reads its OWN capture, so
		// every refusal below is a refusal of the reference, not of the
		// session or the tenant.
		if got, _ := served.ReadObjectPaged(t, ctx, tenantB, s1, bObject.Reference.ObjectID, 10_000); !bytes.Equal(got, append(linesBody(3000), exitTrailer...)) {
			t.Fatalf("tenant B read %d bytes of its own capture, want its exact output", len(got))
		}
		if got, _ := served.ReadObjectPaged(t, ctx, tenantA, s2, otherObject.Reference.ObjectID, 10_000); !bytes.Equal(got, append(linesBody(2000), exitTrailer...)) {
			t.Fatalf("tenant A's second session read %d bytes of its own capture, want its exact output", len(got))
		}
		// An ORPHAN: a real tool-result object in this very session's scope
		// with a perfectly good metadata row, which no committed StepDone
		// names (a publish whose step never committed). The metadata index is
		// not evidence, so it is absent too.
		orphan := []byte("orphaned bytes that no committed step references 71c3")
		metadata, err := world.Journals[tenantA].ToolResultObjects().PublishToolResultObject(ctx, runtimeA1, bytes.NewReader(orphan), uint64(len(orphan)), sha256.Sum256(orphan))
		if err != nil {
			t.Fatalf("publishing the orphan: %v", err)
		}
		if keys := world.ToolResultObjectKeys(t, ctx, tenantA, runtimeA1.String()); len(keys) != 2 {
			t.Fatalf("the orphan is not a real object beside the capture: %v", keys)
		}
		assertAbsent(t, "an orphan", served.GetObject(t, ctx, tenantA, s1, metadata.Reference.ObjectID, false, ""), absentBody, orphan)
		assertAbsent(t, "an orphan's metadata", served.GetObject(t, ctx, tenantA, s1, metadata.Reference.ObjectID, true, ""), absentBody, orphan)
		if answer := served.GetObject(t, ctx, tenantA, s1, metadata.Reference.ObjectID, false, ""); bytes.Contains(answer.Body, orphan) {
			t.Fatal("the orphan's bytes were served")
		}

		// A's second session's capture, asked for through A's first session.
		assertAbsent(t, "another session's capture", served.GetObject(t, ctx, tenantA, s1, otherObject.Reference.ObjectID, false, ""), absentBody, linesBody(2000))
		// And the reverse.
		assertAbsent(t, "a capture read through another session", served.GetObject(t, ctx, tenantA, s2, objectID, false, ""), absentBody, medium)
		// Tenant B's session has the SAME public id as A's: A's capture
		// through B's session, and B's capture through A's.
		assertAbsent(t, "tenant A's capture read as tenant B", served.GetObject(t, ctx, tenantB, s1, objectID, false, ""), absentBody, medium)
		assertAbsent(t, "tenant B's capture read as tenant A", served.GetObject(t, ctx, tenantA, s1, bObject.Reference.ObjectID, false, ""), absentBody, linesBody(3000))
		assertAbsent(t, "tenant A's capture metadata read as tenant B", served.GetObject(t, ctx, tenantB, s1, objectID, true, ""), absentBody, medium)
		// A tenant that holds no session of that name at all gets the
		// session's own absence, not the object's.
		if answer := served.GetObject(t, ctx, tenantB, s2, objectID, false, ""); answer.Status != http.StatusNotFound || bytes.Contains(answer.Body, medium[:32]) {
			t.Fatalf("a read under tenant B's nonexistent session answered %d: %s", answer.Status, answer.Body)
		}
		// Forged references: a well-formed store grammar with the right
		// digest but the wrong generation, and harness's retired content
		// grammar over the REAL digest. Knowing the bytes' digest buys nothing.
		digest := strings.TrimPrefix(sha256Digest(medium), "sha256:")
		parts := strings.Split(objectID, ":")
		generation := []byte(parts[2])
		if generation[0] == '0' {
			generation[0] = '1'
		} else {
			generation[0] = '0'
		}
		assertAbsent(t, "a forged generation over the real digest", served.GetObject(t, ctx, tenantA, s1, strings.Join([]string{parts[0], parts[1], string(generation), digest}, ":"), false, ""), absentBody, medium)
		assertAbsent(t, "a harness-grammar id over the real digest", served.GetObject(t, ctx, tenantA, s1, "v1:sha256:"+digest, false, ""), absentBody, medium)
		assertAbsent(t, "another kind over the real digest", served.GetObject(t, ctx, tenantA, s1, "v1:command-payload:1:"+digest, false, ""), absentBody, medium)

		// The model is scoped the same way: another session's and another
		// tenant's capture ids are unknown to this session's reader.
		for _, foreign := range []string{otherObject.ToolExecutionID.String(), bObject.ToolExecutionID.String(), "00000000-0000-4000-8000-000000000000"} {
			retentionTurn(t, ctx, world, served, tenantA, s1, "i22-a1-foreign-"+foreign[:8], false, orchestrationtest.ToolResultRead+foreign+" 0 once")
			if shown := model.TakeShown(); len(shown) != 1 || shown[0] != "error: read tool result: unknown capture_id" {
				t.Fatalf("reading foreign capture %s showed %q, want the unknown-capture refusal", foreign, shown)
			}
		}
	})

	// ---- case 6: the capture ceiling --------------------------------------
	// Exactly AT the ceiling: numbered lines, then an unterminated pad, then
	// Bash's newline and trailer, summing to the ceiling to the byte.
	const atLines = 645000
	atPad := retentionCeiling - 13*atLines - 1 - len(exitTrailer)
	atCeiling := append(linesBody(atLines), bytes.Repeat([]byte("x"), atPad)...)
	atCeiling = append(append(atCeiling, '\n'), exitTrailer...)
	const overLines = 800000
	over := append(linesBody(overLines), exitTrailer...)
	var atCapture, overCapture event.ToolResultCapture
	t.Run("case 6b: a result exactly at the ceiling is retained whole", func(t *testing.T) {
		if len(atCeiling) != retentionCeiling {
			t.Fatalf("the at-ceiling fixture is %d bytes, want %d", len(atCeiling), retentionCeiling)
		}
		atCapture = referenced(t, retentionTurn(t, ctx, world, served, tenantA, s2, "i22-a2-at-ceiling", false,
			orchestrationtest.ToolResultRun+awkLines(atLines)+fmt.Sprintf(`; head -c %d /dev/zero | tr '\0' x`, atPad)))
		shown := model.TakeShown()
		if atCapture.Truncated || atCapture.CapturedBytes != uint64(retentionCeiling) {
			t.Fatalf("at-ceiling capture = %+v, want untruncated with exactly %d bytes", atCapture, retentionCeiling)
		}
		if len(shown) != 1 || !strings.Contains(shown[0], fmt.Sprintf("; all %d bytes retained; read the rest with read_tool_result", retentionCeiling)) || len(shown[0]) > retentionPreview {
			t.Fatalf("the at-ceiling preview is %d bytes ending %q", len(shown[0]), shown[0][max(0, len(shown[0])-160):])
		}
		got, metadata := served.ReadObjectPaged(t, ctx, tenantA, s2, atCapture.Reference.ObjectID, retentionFactoryPage)
		if !bytes.Equal(got, atCeiling) || metadata.Digest != sha256Digest(atCeiling) {
			t.Fatalf("the at-ceiling object reads %d bytes (digest %s), want the exact %d bytes", len(got), metadata.Digest, len(atCeiling))
		}
		// A whole-object read of more than one page is refused, not truncated.
		if whole := served.GetObject(t, ctx, tenantA, s2, atCapture.Reference.ObjectID, false, ""); whole.Status != http.StatusRequestEntityTooLarge {
			t.Fatalf("an unpaged read of an 8 MiB object answered %d, want 413", whole.Status)
		}
	})
	t.Run("case 6c: a result over the ceiling is truncated, recorded and announced", func(t *testing.T) {
		overCapture = referenced(t, retentionTurn(t, ctx, world, served, tenantA, s2, "i22-a2-over-ceiling", false,
			orchestrationtest.ToolResultRun+awkLines(overLines)))
		shown := model.TakeShown()
		if !overCapture.Truncated || overCapture.TruncationReason != event.ToolResultTruncatedCaptureCeiling || overCapture.CapturedBytes != uint64(retentionCeiling) {
			t.Fatalf("over-ceiling capture = %+v, want truncated at the %d-byte ceiling for capture_ceiling", overCapture, retentionCeiling)
		}
		if original, exact := overCapture.OriginalSize(); !exact || original != uint64(len(over)) {
			t.Fatalf("over-ceiling original size = %d (exact=%v), want exactly %d", original, exact, len(over))
		}
		wantMarker := fmt.Sprintf("; %d of %d bytes retained; the last %d bytes exceeded the capture ceiling and are unavailable; read the retained bytes with read_tool_result capture_id=%q]",
			retentionCeiling, len(over), len(over)-retentionCeiling, overCapture.ToolExecutionID.String())
		if len(shown) != 1 || !strings.Contains(shown[0], wantMarker) || len(shown[0]) > retentionPreview {
			t.Fatalf("the over-ceiling preview does not announce the unavailable tail %q: ends %q", wantMarker, shown[0][max(0, len(shown[0])-300):])
		}
		got, metadata := served.ReadObjectPaged(t, ctx, tenantA, s2, overCapture.Reference.ObjectID, retentionFactoryPage)
		if !bytes.Equal(got, over[:retentionCeiling]) || metadata.SizeBytes != uint64(retentionCeiling) || metadata.Digest != sha256Digest(over[:retentionCeiling]) {
			t.Fatalf("the over-ceiling object reads %d bytes (metadata %+v), want exactly the first %d bytes of the output", len(got), metadata, retentionCeiling)
		}
		// The model can page the retained tail: its last page ends the capture.
		tail := uint64(retentionCeiling - 1000)
		retentionTurn(t, ctx, world, served, tenantA, s2, "i22-a2-over-tail", false,
			fmt.Sprintf("%s%s %d", orchestrationtest.ToolResultRead, overCapture.ToolExecutionID, tail))
		pages := model.TakePages()
		model.TakeShown()
		if len(pages) != 1 {
			t.Fatalf("reading the retained tail took %d pages, want 1", len(pages))
		}
		loc := orchestrationtest.ToolResultFooterPattern.FindStringSubmatchIndex(pages[0])
		if loc == nil || loc[10] != -1 || pages[0][:loc[0]] != string(over[tail:retentionCeiling]) {
			t.Fatalf("the tail page is not the last 1000 retained bytes ending the capture: %.200q", pages[0])
		}
		if !strings.Contains(pages[0], "truncated:") {
			t.Fatalf("the tail page's footer does not say bytes were not retained: %q", pages[0][loc[0]:])
		}
	})

	// ---- case 4: the capture survives the Host, the workspace and Factory --
	orchestrationtest.PooledWait(t, "the first Host released every idle session", 60*time.Second, func() bool {
		return first.SessionsIn(t, "resident") == 0 && first.SessionsIn(t, "releasing") == 0
	})
	first.Stop()
	if wiped := world.WipeWorkspaceDisk(t); wiped == 0 {
		t.Fatal("the first Host left no materialized workspace to wipe; the case would prove nothing about outliving it")
	}
	assertNoSpill(t, world)
	served.Stop()
	second := orchestrationtest.StartPooledHost(t, ctx, world, "i22-second", 5)
	orchestrationtest.AwaitAdvertised(t, world, second.ID)
	restarted := orchestrationtest.StartPooledFactory(t, ctx, world, "i22-replica-2", nil)

	t.Run("case 4a: a restarted Factory, with a cold evidence cache, serves the capture", func(t *testing.T) {
		got, metadata := restarted.ReadObjectPaged(t, ctx, tenantA, s1, objectID, 10_000)
		if !bytes.Equal(got, medium) || metadata.Digest != sha256Digest(medium) {
			t.Fatalf("after the restart the object reads %d bytes (digest %s), want the exact %d bytes", len(got), metadata.Digest, len(medium))
		}
		assertAbsent(t, "after the restart, another session's capture", restarted.GetObject(t, ctx, tenantA, s1, otherObject.Reference.ObjectID, false, ""), absentBody, linesBody(2000))
		assertAbsent(t, "after the restart, tenant A's capture as tenant B", restarted.GetObject(t, ctx, tenantB, s1, objectID, false, ""), absentBody, medium)
	})
	t.Run("case 4b: the session restored on another Host pages the same capture", func(t *testing.T) {
		retentionTurn(t, ctx, world, restarted, tenantA, s1, "i22-a1-after-failover", false, orchestrationtest.ToolResultRead+captureID+" 0")
		pages := model.TakePages()
		model.TakeShown()
		reassembled, total := orchestrationtest.ReassemblePages(t, captureID, pages)
		if total != uint64(len(medium)) || !bytes.Equal(reassembled, medium) {
			t.Fatalf("after failover the pages reassemble %d bytes (total %d), want the exact %d bytes", len(reassembled), total, len(medium))
		}
		for i, page := range pages {
			if len(page) > retentionPreview {
				t.Fatalf("restored page %d is %d bytes, over the budget", i, len(page))
			}
		}
		restores := second.Rig.Restores()
		if len(second.Rig.Creates()) != 0 || len(restores) != 1 || restores[0].ID != runtimeA1 {
			t.Fatalf("the second Host launched creates=%+v restores=%+v, want one restore of %s", second.Rig.Creates(), restores, runtimeA1)
		}
		if got := orchestrationtest.CountJournalEvents[event.SessionStarted](t, world, tenantA, runtimeA1); got != 1 {
			t.Fatalf("the journal holds %d SessionStarted: the session was restarted, not restored", got)
		}
	})
}

// assertNoSpill checks that no capture spill base still holds a session's
// spill root once the first Host has released its sessions: the spill is a
// staging area, never the retained copy.
func assertNoSpill(t *testing.T, world *orchestrationtest.PooledWorld) {
	t.Helper()
	entries := world.SpillEntries(t)
	if len(entries) == 0 {
		t.Fatal("the world composed no capture spill base")
	}
	for base, n := range entries {
		if n != 0 {
			t.Fatalf("spill base %s still holds %d session spill roots after release", base, n)
		}
	}
}
