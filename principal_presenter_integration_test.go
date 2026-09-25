//go:build integration

// This lane crosses a real Factory, Host, harness rig and SessionStore. It
// checks the durable effect and public projection, not only admission status.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
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
		if len(auditor.Audit) == 0 {
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
		if world.CommandState(ctx, tenant, s, "p-forged") != "" {
			t.Fatal("refused command left a durable record")
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
		t.Cleanup(func() { close(hold) })
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
