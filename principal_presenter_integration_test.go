//go:build integration

// This lane crosses a real Factory, Host, harness rig and SessionStore. It
// checks the durable effect and public projection, not only admission status.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/inference"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

func principalLanePostBearer(t *testing.T, ctx context.Context, f *orchestrationtest.PooledFactory, bearer, path string, body any) (int, []byte) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, f.BaseURL+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, answer
}

func principalLaneBlocks(t *testing.T, texts ...string) json.RawMessage {
	t.Helper()
	var blocks []map[string]string
	for _, text := range texts {
		blocks = append(blocks, map[string]string{"type": "text", "text": text})
	}
	encoded, err := json.Marshal(blocks)
	if err != nil {
		t.Fatalf("encoding blocks: %v", err)
	}
	return encoded
}

func principalLaneText(blocks []content.Block) []string {
	var out []string
	for _, block := range blocks {
		if text, ok := block.(*content.TextBlock); ok {
			out = append(out, text.Text)
		}
	}
	return out
}

func TestPrincipalMetadataAndPresenterAcrossFactoryHostHarness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	tenant := orchestrationtest.PooledTenantA
	presenter := orchestrationtest.NewCountingPresenter()
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants: []sessionwire.TenantID{tenant}, WithAskTool: true, Presenter: presenter,
	})
	world.LLM.Respond(func(_ inference.Request) orchestrationtest.PooledTurn {
		return orchestrationtest.PooledTurn{Text: "ok"}
	})
	orchestrationtest.StartPooledHost(t, ctx, world, "i-principal-host", 1)
	auditor := &orchestrationtest.AuditingAuthorizer{}
	served := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
		Replica: "i-principal", PrincipalStamping: true, AuditAuthorizer: auditor,
	})
	want := sessionwire.Principal{Tenant: tenant, Subject: sessionwire.SubjectID("user-" + string(tenant)), Kind: sessionwire.PrincipalKindActor}
	const s = sessionwire.SessionID("i-principal-one")

	t.Run("stamped create is presented once and journaled assembled", func(t *testing.T) {
		status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("p-create"), SessionID: s,
			AgentID: orchestrationtest.PooledAgent, Blocks: principalLaneBlocks(t, "add milk"),
			Metadata: sessionwire.MessageMetadata{"space": "family", "client": "lane"},
		})
		if status != http.StatusCreated {
			t.Fatalf("create answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "p-create applied", 120*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "p-create") == sessionstore.InboxStateApplied
		})
		runtime := world.RuntimeSessionID(t, ctx, tenant, s)
		var started []event.TurnStarted
		orchestrationtest.PooledWait(t, "stamped TurnStarted", 30*time.Second, func() bool {
			started = orchestrationtest.JournalEvents[event.TurnStarted](t, world, tenant, runtime)
			return len(started) == 1
		})
		got := started[0]
		if got.Input == nil || got.Input.Principal == nil || *got.Input.Principal != want {
			t.Fatalf("TurnStarted.Input = %+v, want principal %+v", got.Input, want)
		}
		if got.Input.Metadata["space"] != "family" || got.Input.Prefix != 1 || got.Input.Suffix != 0 {
			t.Fatalf("TurnStarted.Input = %+v", got.Input)
		}
		if texts := principalLaneText(got.Message.Blocks); strings.Join(texts, "|") != "[from: "+string(want.Subject)+" · space: family]|add milk" {
			t.Fatalf("assembled message = %q", texts)
		}
		own := got.Message.Blocks[got.Input.Prefix : len(got.Message.Blocks)-got.Input.Suffix]
		if strings.Join(principalLaneText(own), "|") != "add milk" {
			t.Fatalf("user blocks = %q", principalLaneText(own))
		}
		if presenter.Count("add milk") != 1 {
			t.Fatalf("presenter ran %d times, want 1", presenter.Count("add milk"))
		}
		if !world.LLM.SawInRequest(0, "[from: "+string(want.Subject)) {
			t.Fatal("model request did not carry the presenter frame")
		}
	})

	t.Run("stamped input with metadata is presented once", func(t *testing.T) {
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("p-input"), SessionID: s,
			Blocks: principalLaneBlocks(t, "and eggs"), Metadata: sessionwire.MessageMetadata{"space": "family"},
		})
		if status != http.StatusOK {
			t.Fatalf("input answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "p-input applied", 120*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "p-input") == sessionstore.InboxStateApplied
		})
		if presenter.Count("and eggs") != 1 {
			t.Fatalf("presenter ran %d times for input", presenter.Count("and eggs"))
		}
	})

	t.Run("public journal keeps principal and frame count but strips metadata", func(t *testing.T) {
		status, page := served.Get(t, ctx, tenant, "/v1/sessions/"+string(s)+"/journal")
		if status != http.StatusOK {
			t.Fatalf("journal answered %d: %s", status, page)
		}
		if !bytes.Contains(page, []byte(`"subject":"`+string(want.Subject)+`"`)) || !bytes.Contains(page, []byte(`"prefix":1`)) {
			t.Fatalf("public journal lacks attribution/frame count: %s", page)
		}
		for _, secret := range []string{`"metadata"`, `"client":"lane"`} {
			if bytes.Contains(page, []byte(secret)) {
				t.Fatalf("public journal leaked %s: %s", secret, page)
			}
		}
	})

	t.Run("audit route discloses members only through AuditAuthorizer", func(t *testing.T) {
		status, body := served.Get(t, ctx, tenant, "/v1/sessions/"+string(s)+"/commands/p-create")
		if status != http.StatusOK {
			t.Fatalf("audit route answered %d: %s", status, body)
		}
		var audit struct {
			Principal *sessionwire.Principal      `json:"principal"`
			Metadata  sessionwire.MessageMetadata `json:"metadata"`
		}
		if err := json.Unmarshal(body, &audit); err != nil {
			t.Fatalf("decoding %s: %v", body, err)
		}
		if audit.Principal == nil || *audit.Principal != want || audit.Metadata["client"] != "lane" {
			t.Fatalf("audit = %+v", audit)
		}
		if auditor.AuditCount() == 0 {
			t.Fatal("AuthorizeAuditRead was never consulted")
		}
		plain := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
			Replica: "i-principal-plain", PrincipalStamping: true,
		})
		status, bare := plain.Get(t, ctx, tenant, "/v1/sessions/"+string(s)+"/commands/p-create")
		if status != http.StatusOK || bytes.Contains(bare, []byte(`"principal"`)) || bytes.Contains(bare, []byte(`"metadata"`)) {
			t.Fatalf("without AuditAuthorizer, route answered %d with %s", status, bare)
		}
	})

	t.Run("client-supplied principal is refused before any durable write", func(t *testing.T) {
		forged := want
		forged.Subject = "someone-else"
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("p-forged"), SessionID: s,
			Blocks: principalLaneBlocks(t, "forged"), Principal: &forged,
		})
		// Factory intentionally sanitizes the typed ErrClientPrincipal at its
		// public HTTP boundary; the stable public fact is invalid_request.
		if status != http.StatusBadRequest || !strings.Contains(body, `"code":"invalid_request"`) {
			t.Fatalf("forged principal answered %d: %s", status, body)
		}
		_, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: tenant, SessionID: s, CommandID: "p-forged",
		})
		var inboxErr *sessionstore.InboxError
		if !errors.As(err, &inboxErr) || inboxErr.Code != sessionstore.InboxErrorNotFound {
			t.Fatalf("refused command lookup = %v, want InboxErrorNotFound", err)
		}
	})

	t.Run("gate response and interrupt carry the verified principal", func(t *testing.T) {
		world.LLM.Respond(nil)
		world.AskTool.Question = "which colour?"
		world.LLM.ScriptNext(
			orchestrationtest.PooledTurn{ToolName: orchestrationtest.PooledAskToolName, ToolInput: `{}`},
			orchestrationtest.PooledTurn{Text: "answer received"},
		)
		status, body := served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("p-gate-open"), SessionID: s,
			Blocks: principalLaneBlocks(t, "please ask"), Metadata: sessionwire.MessageMetadata{"space": "family"},
		})
		if status != http.StatusOK {
			t.Fatalf("gate-opening input answered %d: %s", status, body)
		}
		var gate sessionwire.GateProjection
		orchestrationtest.PooledWait(t, "gate projected", 90*time.Second, func() bool {
			page := served.OpenGates(t, ctx, tenant, s)
			if len(page.Gates) != 1 || page.Gates[0].Answerability != sessionwire.GateAnswerabilityResident {
				return false
			}
			gate = page.Gates[0]
			return true
		})
		status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/gates/"+string(gate.GateID), sessionwire.GateResponseRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("p-gate-answer"), SessionID: s,
			GateID: gate.GateID, Action: "answer", Values: map[string]json.RawMessage{"answer": json.RawMessage(`"blue"`)},
			ExpectedOpenJournalSeq: gate.OpenedJournalSeq,
		})
		if status != http.StatusAccepted {
			t.Fatalf("gate answer answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "gate answer applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "p-gate-answer") == sessionstore.InboxStateApplied
		})
		runtime := world.RuntimeSessionID(t, ctx, tenant, s)
		var resolved []event.GateResolved
		orchestrationtest.PooledWait(t, "attributed GateResolved", 30*time.Second, func() bool {
			resolved = orchestrationtest.JournalEvents[event.GateResolved](t, world, tenant, runtime)
			return len(resolved) > 0
		})
		lastResolved := resolved[len(resolved)-1]
		if lastResolved.Principal == nil || *lastResolved.Principal != want {
			t.Fatalf("GateResolved.Principal = %+v, want %+v", lastResolved.Principal, want)
		}
		orchestrationtest.PooledWait(t, "gate turn finished", 60*time.Second, func() bool {
			return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtime) >= 3
		})

		hold := make(chan struct{})
		releaseHold := sync.OnceFunc(func() { close(hold) })
		t.Cleanup(releaseHold)
		world.LLM.ScriptNext(orchestrationtest.PooledTurn{Text: "too late", Hold: hold})
		status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("p-held"), SessionID: s,
			Blocks: principalLaneBlocks(t, "interrupt me"), Metadata: sessionwire.MessageMetadata{"space": "family"},
		})
		if status != http.StatusOK {
			t.Fatalf("held input answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "held turn reached model", 30*time.Second, func() bool {
			return world.LLM.SawInRequest(0, "interrupt me")
		})
		status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/interrupt", sessionwire.InterruptRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("p-interrupt"), SessionID: s,
		})
		if status != http.StatusOK && status != http.StatusAccepted {
			t.Fatalf("interrupt answered %d: %s", status, body)
		}
		var interrupted []event.TurnInterrupted
		orchestrationtest.PooledWait(t, "attributed TurnInterrupted", 90*time.Second, func() bool {
			interrupted = orchestrationtest.JournalEvents[event.TurnInterrupted](t, world, tenant, runtime)
			return len(interrupted) > 0
		})
		lastInterrupted := interrupted[len(interrupted)-1]
		if lastInterrupted.Principal == nil || *lastInterrupted.Principal != want {
			t.Fatalf("TurnInterrupted.Principal = %+v, want %+v", lastInterrupted.Principal, want)
		}
		if presenter.Count("") != 0 {
			t.Fatal("an interrupt or gate response was presented; neither carries a message")
		}
	})

	t.Run("same command id from a different verified subject is rejected", func(t *testing.T) {
		input := sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("p-input"), SessionID: s,
			Blocks: principalLaneBlocks(t, "and eggs"), Metadata: sessionwire.MessageMetadata{"space": "family"},
		}
		created := sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("p-create"), SessionID: s,
			AgentID: orchestrationtest.PooledAgent, Blocks: principalLaneBlocks(t, "add milk"),
			Metadata: sessionwire.MessageMetadata{"space": "family", "client": "lane"},
		}
		for _, row := range []struct {
			name, path string
			request    any
			command    sessionwire.CommandID
		}{
			{"input", "/v1/sessions/" + string(s) + "/input", input, "p-input"},
			{"create", "/v1/sessions", created, "p-create"},
		} {
			t.Run(row.name, func(t *testing.T) {
				status, body := principalLanePostBearer(t, ctx, served, orchestrationtest.PooledBearersAlt[tenant], row.path, row.request)
				if status != http.StatusConflict || !bytes.Contains(body, []byte(`"code":"command_rejected"`)) {
					t.Fatalf("different-subject retry answered %d: %s", status, body)
				}
				entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
					TenantID: tenant, SessionID: s, CommandID: row.command,
				})
				if err != nil || entry.Record.Descriptor.Principal == nil || *entry.Record.Descriptor.Principal != want {
					t.Fatalf("stored principal changed: %+v (%v)", entry.Record.Descriptor.Principal, err)
				}
			})
		}
	})
}

func principalLaneMarshalBlocks(t *testing.T, blocks []content.Block) []byte {
	t.Helper()
	encoded, err := content.MarshalBlocks(blocks)
	if err != nil {
		t.Fatalf("marshaling content blocks: %v", err)
	}
	return encoded
}

func TestPresenterRenderingSurvivesRestoreFailoverAndRedelivery(t *testing.T) {
	t.Run("restore keeps the committed rendering byte for byte", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)
		tenant := orchestrationtest.PooledTenantA
		presenter := orchestrationtest.NewCountingPresenter()
		world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
			Tenants: []sessionwire.TenantID{tenant}, Presenter: presenter,
		})
		world.LLM.Respond(func(_ inference.Request) orchestrationtest.PooledTurn {
			return orchestrationtest.PooledTurn{Text: "ok"}
		})
		first := orchestrationtest.StartPooledHost(t, ctx, world, "i-render-first", 1)
		served := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
			Replica: "i-render-factory", PrincipalStamping: true,
		})
		const s = sessionwire.SessionID("i-render-restore")
		status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("r-1"), SessionID: s,
			AgentID: orchestrationtest.PooledAgent, Blocks: principalLaneBlocks(t, "restore me"),
			Metadata: sessionwire.MessageMetadata{"space": "family"},
		})
		if status != http.StatusCreated {
			t.Fatalf("create answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "r-1 applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "r-1") == sessionstore.InboxStateApplied
		})
		runtime := world.RuntimeSessionID(t, ctx, tenant, s)
		orchestrationtest.PooledWait(t, "r-1 turn finished", 30*time.Second, func() bool {
			return orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtime) == 1
		})
		started := orchestrationtest.JournalEvents[event.TurnStarted](t, world, tenant, runtime)
		if len(started) != 1 || started[0].Input == nil || started[0].Input.Prefix != 1 {
			t.Fatalf("initial TurnStarted = %+v", started)
		}
		before := principalLaneMarshalBlocks(t, started[0].Message.Blocks)
		firstModel := world.LLM.UserBlocksContaining(0, "restore me")
		if got := principalLaneMarshalBlocks(t, firstModel); !bytes.Equal(got, before) {
			t.Fatalf("first model message = %s, journal = %s", got, before)
		}
		first.Stop()
		orchestrationtest.StartPooledHost(t, ctx, world, "i-render-second", 1)
		status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("r-2"), SessionID: s,
			Blocks: principalLaneBlocks(t, "continue"), Metadata: sessionwire.MessageMetadata{"space": "family"},
		})
		if status != http.StatusOK {
			t.Fatalf("input after restore answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "r-2 applied and model requested", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "r-2") == sessionstore.InboxStateApplied &&
				world.LLM.SawInRequest(1, "continue")
		})
		restoredModel := world.LLM.UserBlocksContaining(1, "restore me")
		if got := principalLaneMarshalBlocks(t, restoredModel); !bytes.Equal(got, before) {
			t.Fatalf("restored model message = %s, want %s", got, before)
		}
		again := orchestrationtest.JournalEvents[event.TurnStarted](t, world, tenant, runtime)
		if len(again) < 1 || !bytes.Equal(principalLaneMarshalBlocks(t, again[0].Message.Blocks), before) {
			t.Fatalf("the first committed rendering changed after restore: %+v", again)
		}
		if got := presenter.Count("restore me"); got != 1 {
			t.Fatalf("restore invoked presenter %d times for the first message, want 1", got)
		}
	})

	t.Run("a committed presentation is not repeated after a Host death", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)
		tenant := orchestrationtest.PooledTenantA
		presenter := orchestrationtest.NewCountingPresenter()
		world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
			Tenants: []sessionwire.TenantID{tenant}, Presenter: presenter,
		})
		first := orchestrationtest.StartLifecycleHost(t, ctx, world, "i-present-fail-first", 1,
			orchestrationtest.PooledHostConfig{Mortal: true})
		served := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
			Replica: "i-present-fail-factory", PrincipalStamping: true,
		})
		const s = sessionwire.SessionID("i-present-failover")
		status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("f-create"), SessionID: s,
			AgentID: orchestrationtest.PooledAgent,
		})
		if status != http.StatusCreated {
			t.Fatalf("bare create answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "bare create applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "f-create") == sessionstore.InboxStateApplied
		})
		hold := make(chan struct{})
		releaseHold := sync.OnceFunc(func() { close(hold) })
		t.Cleanup(releaseHold)
		world.LLM.ScriptNext(orchestrationtest.PooledTurn{Text: "held", Hold: hold})
		status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("f-1"), SessionID: s,
			Blocks: principalLaneBlocks(t, "f-1 text"), Metadata: sessionwire.MessageMetadata{"space": "family"},
		})
		if status != http.StatusOK {
			t.Fatalf("input answered %d: %s", status, body)
		}
		runtime := world.RuntimeSessionID(t, ctx, tenant, s)
		var started []event.TurnStarted
		orchestrationtest.PooledWait(t, "committed f-1 TurnStarted while model held", 60*time.Second, func() bool {
			started = orchestrationtest.JournalEvents[event.TurnStarted](t, world, tenant, runtime)
			return len(started) == 1 && world.LLM.SawInRequest(0, "f-1 text")
		})
		before := principalLaneMarshalBlocks(t, started[0].Message.Blocks)
		if got := presenter.Count("f-1 text"); got != 1 {
			t.Fatalf("before death, presenter ran %d times, want 1", got)
		}
		first.Kill(t)
		// A held model call belongs to the dead process. Releasing it cannot
		// commit through that process's severed durable-plane view.
		releaseHold()
		second := orchestrationtest.StartPooledHost(t, ctx, world, "i-present-fail-second", 1)
		orchestrationtest.AwaitAdvertised(t, world, second.ID)
		orchestrationtest.PooledWait(t, "dead owner no longer accepting", 30*time.Second, func() bool {
			owner, found, err := served.Directory.Owner(ctx, tenant, s)
			return err == nil && (!found || owner.Residency != sessionwire.SessionResidencyResident ||
				!owner.Accepting || !owner.ExpiresAt.After(time.Now()))
		})
		world.LLM.ScriptNext(orchestrationtest.PooledTurn{Text: "resumed"})
		// A settled input leaves no pending placement work of its own. Request
		// an explicit restore, as the product's recovery lane does, to make the
		// successor resident and settle the committed turn's terminal state.
		status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/restore", sessionwire.RestoreRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("f-restore"), SessionID: s,
		})
		if status != http.StatusOK && status != http.StatusAccepted {
			t.Fatalf("restore after failover answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "f-1 and restore applied; old turn terminated", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "f-1") == sessionstore.InboxStateApplied &&
				world.CommandState(ctx, tenant, s, "f-restore") == sessionstore.InboxStateApplied &&
				orchestrationtest.CountJournalEvents[event.TurnInterrupted](t, world, tenant, runtime) >= 1
		})
		restored, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: tenant, SessionID: s, CommandID: "f-restore",
		})
		if err != nil {
			t.Fatal(err)
		}
		wantPrincipal := sessionwire.Principal{
			Tenant: tenant, Subject: sessionwire.SubjectID("user-" + string(tenant)), Kind: sessionwire.PrincipalKindActor,
		}
		if restored.Record.Descriptor.Principal == nil || *restored.Record.Descriptor.Principal != wantPrincipal {
			t.Fatalf("restore disposition principal = %+v, want %+v", restored.Record.Descriptor.Principal, wantPrincipal)
		}
		// A model call cut by process death is terminal on restore, not replayed
		// as a new TurnDone. Prove the successor can advance with a fresh turn.
		status, body = served.Post(t, ctx, tenant, "/v1/sessions/"+string(s)+"/input", sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("f-next"), SessionID: s,
			Blocks: principalLaneBlocks(t, "after failover"), Metadata: sessionwire.MessageMetadata{"space": "family"},
		})
		if status != http.StatusOK {
			t.Fatalf("successor input answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "successor turn finished", 60*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "f-next") == sessionstore.InboxStateApplied &&
				orchestrationtest.CountJournalEvents[event.TurnDone](t, world, tenant, runtime) >= 1
		})
		if got := presenter.Count("f-1 text"); got != 1 {
			t.Fatalf("failover presented f-1 %d times, want 1", got)
		}
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: tenant, SessionID: s, CommandID: "f-1",
		})
		if err != nil {
			t.Fatal(err)
		}
		commandID, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID))
		if err != nil {
			t.Fatal(err)
		}
		found := 0
		for _, ev := range orchestrationtest.JournalEvents[event.TurnStarted](t, world, tenant, runtime) {
			if ev.Cause.CommandID != commandID {
				continue
			}
			found++
			if got := principalLaneMarshalBlocks(t, ev.Message.Blocks); !bytes.Equal(got, before) {
				t.Fatalf("f-1 rendering changed: %s, want %s", got, before)
			}
		}
		if found != 1 {
			t.Fatalf("f-1 caused %d TurnStarted records, want 1", found)
		}
	})

	t.Run("lost HostLink replies redeliver one input without re-presenting it", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)
		tenant := orchestrationtest.PooledTenantA
		presenter := orchestrationtest.NewCountingPresenter()
		world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
			Tenants: []sessionwire.TenantID{tenant}, Presenter: presenter,
		})
		dropper := orchestrationtest.NewReplyDropper()
		owner := orchestrationtest.StartPooledHostWith(t, ctx, world, "i-present-redeliver-host", 1, dropper.Wrap)
		orchestrationtest.AwaitAdvertised(t, world, owner.ID)
		first := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
			Replica: "i-present-redeliver-a", PrincipalStamping: true,
		})
		second := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{
			Replica: "i-present-redeliver-b", PrincipalStamping: true,
		})
		const s = sessionwire.SessionID("i-present-redeliver")
		status, body := first.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("d-create"), SessionID: s,
			AgentID: orchestrationtest.PooledAgent,
		})
		if status != http.StatusCreated {
			t.Fatalf("bare create answered %d: %s", status, body)
		}
		orchestrationtest.PooledWait(t, "d-create applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "d-create") == sessionstore.InboxStateApplied
		})
		for _, replica := range []*orchestrationtest.PooledFactory{first, second} {
			viewer := orchestrationtest.ConnectPooledViewer(t, ctx, replica, tenant)
			if err := viewer.Watch(t, ctx, tenant, s); err != nil {
				t.Fatalf("watching redelivery session: %v", err)
			}
		}
		request := sessionwire.InputRequest{
			CommandEnvelope: orchestrationtest.PooledEnvelope("d-1"), SessionID: s,
			Blocks: principalLaneBlocks(t, "d-1 text"), Metadata: sessionwire.MessageMetadata{"space": "family"},
		}
		results := make([]struct {
			status int
			body   []byte
			err    error
		}, 4)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range results {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				replica := first
				if i%2 == 1 {
					replica = second
				}
				results[i].status, results[i].body, results[i].err = replica.PostRaw(ctx, tenant, "/v1/sessions/"+string(s)+"/input", request)
			}(i)
		}
		close(start)
		wg.Wait()
		for i, result := range results {
			if result.err != nil || result.status != http.StatusOK {
				t.Fatalf("concurrent retry %d answered %d %s: %v", i, result.status, result.body, result.err)
			}
		}
		orchestrationtest.PooledWait(t, "d-1 applied", 90*time.Second, func() bool {
			return world.CommandState(ctx, tenant, s, "d-1") == sessionstore.InboxStateApplied
		})
		orchestrationtest.PooledWait(t, "two d-1 deliveries with deleted replies", 30*time.Second, func() bool {
			var delivered, dropped int
			for _, delivery := range dropper.Deliveries() {
				if delivery.CommandID == "d-1" {
					delivered++
					if delivery.ReplyDropped {
						dropped++
					}
				}
			}
			return delivered >= 2 && dropped >= 2
		})
		if faults := dropper.Faults(); len(faults) != 0 {
			t.Fatalf("reply dropper parse faults: %v", faults)
		}
		entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
			TenantID: tenant, SessionID: s, CommandID: "d-1",
		})
		if err != nil {
			t.Fatal(err)
		}
		commandID, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID))
		if err != nil {
			t.Fatal(err)
		}
		runtime := world.RuntimeSessionID(t, ctx, tenant, s)
		var starts int
		orchestrationtest.PooledWait(t, "d-1 TurnStarted", 30*time.Second, func() bool {
			starts = 0
			for _, ev := range orchestrationtest.JournalEvents[event.TurnStarted](t, world, tenant, runtime) {
				if ev.Cause.CommandID == commandID {
					starts++
				}
			}
			return starts >= 1
		})
		if starts != 1 || presenter.Count("d-1 text") != 1 {
			t.Fatalf("redelivery caused %d turns and %d presentations, want one each", starts, presenter.Count("d-1 text"))
		}
	})
}
