//go:build integration

package tests

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/host"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

// TestFactoryHostReasoningOnClientLink proves that visible reasoning crosses
// Harness, Host and Factory in model order, while private continuation state
// and runtime identities never reach the public subscriber.
func TestFactoryHostReasoningOnClientLink(t *testing.T) {
	for _, include := range []bool{true, false} {
		name := "off"
		if include {
			name = "on"
		}
		t.Run(name, func(t *testing.T) {
			ctx := placementContext(t)
			const tenant = orchestrationtest.PooledTenantA
			session := sessionwire.SessionID("reasoning-wire-" + name)
			command := sessionwire.CommandID("reasoning-create-" + name)
			world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
				HarnessRuntime: true,
				Tenants:        []sessionwire.TenantID{tenant}, LiveText: &host.LiveTextOptions{IncludeReasoning: include},
			})
			hold, textStart, textSecond, finish := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			world.LLM.Script(orchestrationtest.PooledTurn{Hold: hold, Chunks: []content.Chunk{
				&content.ThinkingChunk{Thinking: "visible thought one "},
				&content.ThinkingChunk{Thinking: "visible thought two"},
				&content.ThinkingChunk{Signature: "PRIVATE_SIGNATURE_SENTINEL", SignatureFormat: "anthropic"},
				&content.TextChunk{Text: "answer "}, &content.TextChunk{Text: "ready"},
			}, BeforeChunk: []<-chan struct{}{nil, nil, nil, textStart, textSecond}, BeforeFinish: finish})
			tap := orchestrationtest.NewHostLinkTap()
			h := orchestrationtest.StartTappedPooledHost(t, ctx, world, sessionwire.HostID("reasoning-host-"+name), 1, tap)
			orchestrationtest.AwaitAdvertised(t, world, h.ID)
			served := orchestrationtest.StartPooledFactory(t, ctx, world, "reasoning-factory-"+name, nil)
			viewer := orchestrationtest.ConnectPooledViewer(t, ctx, served, tenant)
			if err := viewer.Watch(t, ctx, tenant, session); err != nil {
				t.Fatal(err)
			}
			status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)), SessionID: session,
				AgentID: orchestrationtest.PooledAgent, Blocks: json.RawMessage(`[{"type":"text","text":"hello"}]`),
			})
			if status != http.StatusCreated {
				t.Fatalf("create answered %d: %s", status, body)
			}
			orchestrationtest.PooledWait(t, "HostLink live tail bound", 20*time.Second, func() bool {
				for _, link := range orchestrationtest.TappedLinks(t, tap) {
					if len(link.Subscribes) > 0 {
						return true
					}
				}
				return false
			})
			close(hold)
			close(textStart)
			if include {
				orchestrationtest.PooledWait(t, "reasoning preview before StepDone", 20*time.Second, func() bool {
					return len(round2PreviewTypes(viewer.Frames(), "thinking")) > 0
				})
			}
			close(textSecond)
			orchestrationtest.PooledWait(t, "final text preview before StepDone", 20*time.Second, func() bool {
				return bytes.Contains(bytes.Join(round2PreviewTypes(viewer.Frames(), "text"), nil), []byte("ready"))
			})
			close(finish)
			orchestrationtest.PooledWait(t, "ClientLink StepDone", 60*time.Second, func() bool { return liveStepIndex(viewer.Frames()) >= 0 })
			orchestrationtest.PooledWait(t, "create applied", 60*time.Second, func() bool {
				return world.CommandState(ctx, tenant, session, command) == sessionstore.InboxStateApplied
			})
			frames := viewer.Frames()
			runtimeID := world.RuntimeSessionID(t, ctx, tenant, session)
			entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: session, CommandID: command})
			if err != nil {
				t.Fatal(err)
			}
			runtimeCommandID := string(entry.Record.Descriptor.RuntimeCommandID)
			if runtimeCommandID == "" {
				t.Fatal("missing runtime command ID")
			}
			signatureOnlyEmitted := false
			for _, chunk := range world.LLM.EmittedChunks() {
				if reasoning, ok := chunk.(*content.ThinkingChunk); ok && reasoning.Thinking == "" && reasoning.Signature == "PRIVATE_SIGNATURE_SENTINEL" {
					signatureOnlyEmitted = true
				}
			}
			if !signatureOnlyEmitted {
				t.Fatal("model did not emit the signature-only reasoning chunk")
			}
			var kinds []string
			var thinking, answer strings.Builder
			for i, frame := range frames {
				if bytes.Contains(frame, []byte(runtimeID.String())) || bytes.Contains(frame, []byte(runtimeCommandID)) {
					t.Fatalf("frame %d leaks private identity: %s", i, frame)
				}
				kind, err := sessionwire.SessionRecordTypeOf(frame)
				if err != nil {
					t.Fatalf("frame %d: %v", i, err)
				}
				if kind != sessionwire.SessionRecordTypeEphemeralPublication {
					continue
				}
				if bytes.Contains(frame, []byte("PRIVATE_SIGNATURE_SENTINEL")) {
					t.Fatalf("preview %d leaks signature-only reasoning: %s", i, frame)
				}
				if i >= liveStepIndex(frames) {
					t.Fatalf("preview %d followed StepDone", i)
				}
				var p sessionwire.EphemeralPublication
				if err := p.UnmarshalJSON(frame); err != nil {
					t.Fatal(err)
				}
				if p.TenantID != tenant || p.SessionID != session {
					t.Fatalf("wrong public scope: %+v", p)
				}
				var delta struct {
					V         int    `json:"v"`
					Type      string `json:"type"`
					SessionID string `json:"session_id"`
					LoopID    string `json:"loop_id"`
					TurnID    string `json:"turn_id"`
					Chunk     struct {
						Type     string `json:"chunk_type"`
						Thinking string `json:"thinking"`
						Text     string `json:"text"`
					} `json:"chunk"`
				}
				if err := json.Unmarshal(p.Body, &delta); err != nil {
					t.Fatal(err)
				}
				if delta.V != 1 || delta.Type != "TokenDelta" || delta.SessionID != string(session) || delta.LoopID == "" || delta.TurnID == "" {
					t.Fatalf("bad public delta: %s", p.Body)
				}
				kinds = append(kinds, delta.Chunk.Type)
				switch delta.Chunk.Type {
				case "thinking":
					if delta.Chunk.Thinking == "" || delta.Chunk.Text != "" {
						t.Fatalf("bad thinking: %s", p.Body)
					}
					thinking.WriteString(delta.Chunk.Thinking)
				case "text":
					if delta.Chunk.Text == "" || delta.Chunk.Thinking != "" {
						t.Fatalf("bad text: %s", p.Body)
					}
					answer.WriteString(delta.Chunk.Text)
				default:
					t.Fatalf("unexpected preview: %s", p.Body)
				}
			}
			if include {
				if thinking.String() != "visible thought one visible thought two" || answer.String() != "answer ready" {
					t.Fatalf("previews thinking=%q text=%q kinds=%v", thinking.String(), answer.String(), kinds)
				}
				seenText := false
				for _, kind := range kinds {
					if kind == "text" {
						seenText = true
					}
					if seenText && kind == "thinking" {
						t.Fatalf("thinking followed text: %v", kinds)
					}
				}
			} else if thinking.Len() != 0 || answer.String() != "answer ready" {
				t.Fatalf("opt-out previews thinking=%q text=%q", thinking.String(), answer.String())
			}
		})
	}
}

func round2PreviewTypes(frames [][]byte, want string) [][]byte {
	var out [][]byte
	for _, frame := range frames {
		kind, _ := sessionwire.SessionRecordTypeOf(frame)
		if kind != sessionwire.SessionRecordTypeEphemeralPublication {
			continue
		}
		var p sessionwire.EphemeralPublication
		if p.UnmarshalJSON(frame) != nil {
			continue
		}
		var delta struct {
			Chunk struct {
				Type string `json:"chunk_type"`
			} `json:"chunk"`
		}
		if json.Unmarshal(p.Body, &delta) == nil && delta.Chunk.Type == want {
			out = append(out, frame)
		}
	}
	return out
}

// The ID hashes to control shard 15 under SessionStore's published 16-shard
// layout. A periodic rotor starting at shard zero cannot reach it in 3 seconds
// with Factory's default 5-second interval.
func TestFactoryPlacesColdAdmissionBeforeSweepRotation(t *testing.T) {
	ctx := placementContext(t)
	const tenant = orchestrationtest.PooledTenantA
	const session = sessionwire.SessionID("fast-placement-11")
	const command = sessionwire.CommandID("fast-placement-create")
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{HarnessRuntime: true, Tenants: []sessionwire.TenantID{tenant}})
	if got := world.Store.ControlShards(); got != sessionstore.DefaultControlShards {
		t.Fatalf("control shards=%d, want %d", got, sessionstore.DefaultControlShards)
	}
	// Pin the test input to the last shard, so the timing assertion cannot
	// silently become a first-shard sweep test if this ID is edited.
	if got := round2ControlShard(tenant, session, world.Store.ControlShards()); got != 15 {
		t.Fatalf("test session maps to shard %d, want 15", got)
	}
	world.LLM.Script(orchestrationtest.PooledTurn{Text: "placed"})
	h := orchestrationtest.StartPooledHost(t, ctx, world, "fast-placement-host", 1)
	orchestrationtest.AwaitAdvertised(t, world, h.ID)
	defaults := factory.DefaultReconcileLimits()
	served := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{Replica: "fast-placement-factory", Interval: defaults.Interval, ClaimTTL: defaults.ClaimTTL})
	started := time.Now()
	status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)), SessionID: session, AgentID: orchestrationtest.PooledAgent, Blocks: json.RawMessage(`[{"type":"text","text":"hello"}]`)})
	if status != http.StatusCreated {
		t.Fatalf("create answered %d: %s", status, body)
	}
	deadline := time.After(3 * time.Second)
	for world.CommandState(ctx, tenant, session, command) != sessionstore.InboxStateApplied {
		select {
		case <-deadline:
			t.Fatalf("first command not applied within 3s; elapsed %s", time.Since(started))
		case <-time.After(20 * time.Millisecond):
		}
	}
	if elapsed := time.Since(started); elapsed >= 3*time.Second {
		t.Fatalf("first command applied after %s", elapsed)
	}
}

// A watch made before the session exists must bind to the freshly placed
// Host's live tail without waiting for DemandReleaseDebounce (default 30s).
func TestFactoryEarlyViewerGetsFirstTurnLiveText(t *testing.T) {
	ctx := placementContext(t)
	if got := factory.DefaultClientLinkLimits().DemandReleaseDebounce; got != 30*time.Second {
		t.Fatalf("Factory default demand release debounce = %s, want 30s", got)
	}
	const tenant = orchestrationtest.PooledTenantA
	const session = sessionwire.SessionID("early-viewer-first-turn")
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{HarnessRuntime: true, Tenants: []sessionwire.TenantID{tenant}, LiveText: &host.LiveTextOptions{}})
	hold, second, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	world.LLM.Script(orchestrationtest.PooledTurn{Hold: hold, Chunks: []content.Chunk{&content.TextChunk{Text: "early "}, &content.TextChunk{Text: "preview"}}, BeforeChunk: []<-chan struct{}{nil, second}, BeforeFinish: finish})
	h := orchestrationtest.StartPooledHost(t, ctx, world, "early-viewer-host", 1)
	orchestrationtest.AwaitAdvertised(t, world, h.ID)
	served := orchestrationtest.StartPooledFactoryWith(t, ctx, world, orchestrationtest.PooledFactoryConfig{Replica: "early-viewer-factory", UseDefaultDemandReleaseDebounce: true})
	viewer := orchestrationtest.ConnectPooledViewer(t, ctx, served, tenant)
	if err := viewer.Watch(t, ctx, tenant, session); err != nil {
		t.Fatal(err)
	}
	status, body := served.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{CommandEnvelope: orchestrationtest.PooledEnvelope("early-viewer-create"), SessionID: session, AgentID: orchestrationtest.PooledAgent, Blocks: json.RawMessage(`[{"type":"text","text":"hello"}]`)})
	if status != http.StatusCreated {
		t.Fatalf("create answered %d: %s", status, body)
	}
	close(hold)
	close(second)
	orchestrationtest.PooledWait(t, "model emitted first-turn text", 20*time.Second, func() bool { return len(world.LLM.EmittedChunks()) >= 2 })
	// The model cannot end its turn yet. A missing preview therefore cannot be
	// mistaken for a durable StepDone that was replayed after a late tail bind.
	deadline := time.After(8 * time.Second)
	for len(round2PreviewTypes(viewer.Frames(), "text")) == 0 {
		select {
		case <-deadline:
			t.Fatal("early viewer received no first-turn live text before the default debounce")
		case <-time.After(20 * time.Millisecond):
		}
	}
	if liveStepIndex(viewer.Frames()) >= 0 {
		t.Fatal("StepDone arrived before the held turn finished")
	}
	close(finish)
	orchestrationtest.PooledWait(t, "early viewer StepDone", 60*time.Second, func() bool { return liveStepIndex(viewer.Frames()) >= 0 })
}

// SessionStore's v1 shard frame is length-prefixed and SHA-256 hashed. This
// local calculation guards the chosen fixture ID; production assigns the shard.
func round2ControlShard(tenant sessionwire.TenantID, session sessionwire.SessionID, count int) int {
	var frame []byte
	for _, part := range []string{"looprig/sessionstore/shard/v1", string(tenant), string(session)} {
		frame = binary.BigEndian.AppendUint32(frame, uint32(len(part)))
		frame = append(frame, part...)
	}
	sum := sha256.Sum256(frame)
	return int(binary.BigEndian.Uint64(sum[:8]) % uint64(count))
}
