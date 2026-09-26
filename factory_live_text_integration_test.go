//go:build integration

package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/host"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/orchestrationtest"
)

const liveText = "amber birch cedar"

// TestFactoryHostLiveText follows one model stream from the real harness rig,
// through Host and Factory, to a real centrifuge-go ClientLink subscriber.
func TestFactoryHostLiveText(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		name := "off"
		if enabled {
			name = "on"
		}
		t.Run(name, func(t *testing.T) {
			ctx := placementContext(t)
			var options *host.LiveTextOptions
			if enabled {
				options = &host.LiveTextOptions{}
			}
			world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
				Tenants: []sessionwire.TenantID{orchestrationtest.PooledTenantA}, LiveText: options,
			})
			hold, second, third, finish := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			const thinking = "PRIVATE_THINKING_SENTINEL"
			world.LLM.Script(orchestrationtest.PooledTurn{
				Hold: hold,
				Chunks: []content.Chunk{
					&content.ThinkingChunk{Thinking: thinking},
					&content.TextChunk{Text: "amber "},
					&content.TextChunk{Text: "birch "},
					&content.TextChunk{Text: "cedar"},
				},
				BeforeChunk: []<-chan struct{}{nil, nil, second, third}, BeforeFinish: finish,
			})
			const session = sessionwire.SessionID("live-text-public-session")
			const command = sessionwire.CommandID("live-text-create")
			hostTap := orchestrationtest.NewHostLinkTap()
			h := orchestrationtest.StartTappedPooledHost(t, ctx, world, "live-text-host-"+sessionwire.HostID(name), 4, hostTap)
			orchestrationtest.AwaitAdvertised(t, world, h.ID)
			factory := orchestrationtest.StartPooledFactory(t, ctx, world, "live-text-factory-"+name, nil)
			viewer := orchestrationtest.ConnectPooledViewer(t, ctx, factory, orchestrationtest.PooledTenantA)
			if err := viewer.Watch(t, ctx, orchestrationtest.PooledTenantA, session); err != nil {
				t.Fatal(err)
			}
			status, body := factory.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions", sessionwire.CreateRequest{
				CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)), SessionID: session,
				AgentID: orchestrationtest.PooledAgent, Blocks: json.RawMessage(`[{"type":"text","text":"hello"}]`),
			})
			if status != http.StatusCreated {
				t.Fatalf("create answered %d: %s", status, body)
			}
			orchestrationtest.PooledWait(t, "Factory bound the HostLink before model output", 30*time.Second, func() bool {
				for _, link := range orchestrationtest.TappedLinks(t, hostTap) {
					if len(link.Subscribes) != 0 {
						return true
					}
				}
				return false
			})
			close(hold)
			if enabled {
				waitLiveText(t, viewer, 1)
				close(second)
				waitLiveText(t, viewer, 2)
				close(third)
				waitLiveText(t, viewer, 3)
			} else {
				close(second)
				close(third)
			}
			close(finish)
			orchestrationtest.PooledWait(t, "durable StepDone at the ClientLink", 60*time.Second, func() bool {
				return liveStepIndex(viewer.Frames()) >= 0
			})
			orchestrationtest.PooledWait(t, "durable turn terminal at the ClientLink", 60*time.Second, func() bool {
				return liveTerminalCount(viewer.Frames()) >= 1
			})
			orchestrationtest.PooledWait(t, "create settled applied", 60*time.Second, func() bool {
				return world.CommandState(ctx, orchestrationtest.PooledTenantA, session, command) == sessionstore.InboxStateApplied
			})
			runtimeID := world.RuntimeSessionID(t, ctx, orchestrationtest.PooledTenantA, session)
			orchestrationtest.PooledWait(t, "journal StepDone", 60*time.Second, func() bool {
				return len(orchestrationtest.JournalEvents[event.StepDone](t, world, orchestrationtest.PooledTenantA, runtimeID)) > 0
			})
			steps := orchestrationtest.JournalEvents[event.StepDone](t, world, orchestrationtest.PooledTenantA, runtimeID)
			if len(steps) != 1 {
				t.Fatalf("journal has %d StepDone events, want one", len(steps))
			}
			if got := stepText(steps[0]); got != liveText {
				t.Fatalf("durable StepDone text = %q, want %q", got, liveText)
			}
			frames := viewer.Frames()
			var publicStep sessionwire.EnduringPublication
			if err := publicStep.UnmarshalJSON(frames[liveStepIndex(frames)]); err != nil {
				t.Fatal(err)
			}
			if got := publicStepText(t, publicStep.Body); got != liveText {
				t.Fatalf("ClientLink StepDone text = %q, want %q", got, liveText)
			}
			var got strings.Builder
			ephemerals := 0
			for i, frame := range frames {
				if bytes.Contains(frame, []byte(runtimeID.String())) {
					t.Fatalf("frame %d leaks runtime session ID: %s", i, frame)
				}
				kind, err := sessionwire.SessionRecordTypeOf(frame)
				if err != nil {
					t.Fatalf("frame %d: %v: %s", i, err, frame)
				}
				if kind != sessionwire.SessionRecordTypeEphemeralPublication {
					continue
				}
				if bytes.Contains(frame, []byte(thinking)) {
					t.Fatalf("ephemeral %d leaks thinking: %s", i, frame)
				}
				ephemerals++
				if i >= liveStepIndex(frames) {
					t.Fatalf("ephemeral %d arrived after StepDone", i)
				}
				var p sessionwire.EphemeralPublication
				if err := p.UnmarshalJSON(frame); err != nil {
					t.Fatal(err)
				}
				if p.SessionID != session || p.TenantID != orchestrationtest.PooledTenantA {
					t.Fatalf("wrong public identity: %+v", p)
				}
				var delta struct {
					V         int    `json:"v"`
					Type      string `json:"type"`
					SessionID string `json:"session_id"`
					LoopID    string `json:"loop_id"`
					TurnID    string `json:"turn_id"`
					Chunk     struct {
						Type string `json:"chunk_type"`
						Text string `json:"text"`
					} `json:"chunk"`
				}
				if err := json.Unmarshal(p.Body, &delta); err != nil {
					t.Fatal(err)
				}
				if delta.V != 1 || delta.Type != "TokenDelta" || delta.SessionID != string(session) || delta.LoopID == "" || delta.TurnID == "" || delta.Chunk.Type != "text" || delta.Chunk.Text == "" {
					t.Fatalf("bad public delta: %s", p.Body)
				}
				for _, field := range []string{"journal_seq", "covered_through", "event_id", "journal_tip"} {
					if bytes.Contains(frame, []byte(`"`+field+`"`)) {
						t.Fatalf("ephemeral has journal coordinate %s: %s", field, frame)
					}
				}
				got.WriteString(delta.Chunk.Text)
			}
			if enabled && (ephemerals < 3 || got.String() != liveText) {
				t.Fatalf("%d ephemerals joined to %q, want 3+ and %q", ephemerals, got.String(), liveText)
			}
			if !enabled && ephemerals != 0 {
				t.Fatalf("opt-out delivered %d ephemerals", ephemerals)
			}
			entry, err := world.Store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: orchestrationtest.PooledTenantA, SessionID: session, CommandID: command})
			if err != nil {
				t.Fatal(err)
			}
			runtimeCommandID := string(entry.Record.Descriptor.RuntimeCommandID)
			if runtimeCommandID == "" {
				t.Fatal("create has no runtime command identity to scan")
			}
			for i, frame := range frames {
				if bytes.Contains(frame, []byte(runtimeCommandID)) {
					t.Fatalf("frame %d leaks runtime command ID: %s", i, frame)
				}
			}
			if enabled {
				linkFrames := hostLinkSessionFrames(t, hostTap)
				var linkText strings.Builder
				linkEphemerals := 0
				for i, frame := range linkFrames {
					if bytes.Contains(frame, []byte(runtimeID.String())) || bytes.Contains(frame, []byte(runtimeCommandID)) {
						t.Fatalf("HostLink publication %d leaks a runtime identity: %s", i, frame)
					}
					kind, _ := sessionwire.SessionRecordTypeOf(frame)
					if kind != sessionwire.SessionRecordTypeEphemeralPublication {
						continue
					}
					if i >= liveStepIndex(linkFrames) {
						t.Fatalf("HostLink ephemeral %d followed StepDone", i)
					}
					var p sessionwire.EphemeralPublication
					if err := p.UnmarshalJSON(frame); err != nil {
						t.Fatal(err)
					}
					var delta struct {
						Chunk struct {
							Text string `json:"text"`
						} `json:"chunk"`
					}
					if err := json.Unmarshal(p.Body, &delta); err != nil {
						t.Fatal(err)
					}
					linkText.WriteString(delta.Chunk.Text)
					linkEphemerals++
				}
				if linkEphemerals < 3 || linkText.String() != liveText {
					t.Fatalf("HostLink saw %d previews with text %q", linkEphemerals, linkText.String())
				}
			}
			if enabled {
				// A later model response carries a tool call. Its name and
				// arguments may enter the durable result, never a text delta.
				const toolName = "PRIVATE_TOOL_CALL_SENTINEL"
				continuation := make(chan struct{})
				t.Cleanup(func() {
					select {
					case <-continuation:
					default:
						close(continuation)
					}
				})
				world.LLM.ScriptNext(
					orchestrationtest.PooledTurn{ToolName: toolName, ToolInput: `{"secret":"PRIVATE_ARGUMENT_SENTINEL"}`},
					orchestrationtest.PooledTurn{Hold: continuation, Text: "after tool"},
				)
				const input = sessionwire.CommandID("live-text-tool-input")
				status, body := factory.Post(t, ctx, orchestrationtest.PooledTenantA, "/v1/sessions/"+string(session)+"/input", sessionwire.InputRequest{
					CommandEnvelope: orchestrationtest.PooledEnvelope(string(input)), SessionID: session,
					Blocks: json.RawMessage(`[{"type":"text","text":"call the tool"}]`),
				})
				if status != http.StatusOK {
					t.Fatalf("tool input answered %d: %s", status, body)
				}
				orchestrationtest.PooledWait(t, "tool-call step reached ClientLink", 60*time.Second, func() bool {
					return liveToolBoundaryIndex(viewer.Frames(), len(frames)) >= 0
				})
				toolEmitted := false
				for _, chunk := range world.LLM.EmittedChunks() {
					if call, ok := chunk.(*content.ToolUseChunk); ok && call.Name == toolName {
						toolEmitted = true
					}
				}
				if !toolEmitted {
					t.Fatal("the model never emitted the scripted tool call")
				}
				toolFrames := viewer.Frames()
				for _, frame := range toolFrames[len(frames) : liveToolBoundaryIndex(toolFrames, len(frames))+1] {
					kind, _ := sessionwire.SessionRecordTypeOf(frame)
					if kind == sessionwire.SessionRecordTypeEphemeralPublication {
						t.Fatalf("tool-call step reached an ephemeral frame: %s", frame)
					}
				}
				close(continuation)
				orchestrationtest.PooledWait(t, "tool turn reached ClientLink", 60*time.Second, func() bool { return liveTerminalCount(viewer.Frames()) >= 2 })
			}
		})
	}
}

func hostLinkSessionFrames(t *testing.T, tap *orchestrationtest.HostLinkTap) [][]byte {
	t.Helper()
	var frames [][]byte
	for _, conn := range tap.Conns() {
		if faults := conn.Faults(); len(faults) != 0 {
			t.Fatalf("HostLink tap faults: %v", faults)
		}
		for _, message := range conn.Messages() {
			if !message.FromHost {
				continue
			}
			var push struct {
				Push struct {
					Pub struct {
						Data json.RawMessage `json:"data"`
					} `json:"pub"`
				} `json:"push"`
			}
			if json.Unmarshal(message.Payload, &push) != nil || len(push.Push.Pub.Data) == 0 {
				continue
			}
			if _, err := sessionwire.SessionRecordTypeOf(push.Push.Pub.Data); err == nil {
				frames = append(frames, push.Push.Pub.Data)
			}
		}
	}
	return frames
}

func waitLiveText(t *testing.T, v *orchestrationtest.PooledViewer, want int) {
	t.Helper()
	orchestrationtest.PooledWait(t, fmt.Sprintf("%d live text frames", want), 30*time.Second, func() bool {
		count := 0
		for _, frame := range v.Frames() {
			kind, _ := sessionwire.SessionRecordTypeOf(frame)
			if kind == sessionwire.SessionRecordTypeEphemeralPublication {
				count++
			}
		}
		return count >= want
	})
}

func liveStepIndex(frames [][]byte) int {
	for i, frame := range frames {
		var p sessionwire.EnduringPublication
		if p.UnmarshalJSON(frame) == nil && bytes.Contains(p.Body, []byte(`"type":"StepDone"`)) {
			return i
		}
	}
	return -1
}

func liveTerminalCount(frames [][]byte) int {
	count := 0
	for _, frame := range frames {
		var p sessionwire.EnduringPublication
		if p.UnmarshalJSON(frame) != nil {
			continue
		}
		var body struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(p.Body, &body) == nil && (body.Type == "TurnDone" || body.Type == "TurnFailed") {
			count++
		}
	}
	return count
}

func liveToolBoundaryIndex(frames [][]byte, from int) int {
	for i := from; i < len(frames); i++ {
		var p sessionwire.EnduringPublication
		if p.UnmarshalJSON(frames[i]) != nil {
			continue
		}
		var body struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(p.Body, &body) == nil && (body.Type == "StepDone" || body.Type == "TurnFailed") {
			return i
		}
	}
	return -1
}

func stepText(done event.StepDone) string {
	var text strings.Builder
	for _, message := range done.Messages {
		if ai, ok := message.(*content.AIMessage); ok {
			for _, block := range ai.Blocks {
				if part, ok := block.(*content.TextBlock); ok {
					text.WriteString(part.Text)
				}
			}
		}
	}
	return text.String()
}

func publicStepText(t *testing.T, body json.RawMessage) string {
	t.Helper()
	var step struct {
		Messages []struct {
			Role   string `json:"role"`
			Blocks []struct {
				Type string `json:"type"`
				Text string `json:"Text"`
			} `json:"blocks"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &step); err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	for _, message := range step.Messages {
		if message.Role != "assistant" {
			continue
		}
		for _, block := range message.Blocks {
			if block.Type == "text" {
				text.WriteString(block.Text)
			}
		}
	}
	return text.String()
}

// TestFactoryHostLiveTextSlowSubscriber checks that a stopped TCP reader cannot
// turn transient preview loss into a durable coverage claim. Its browser uses
// the same journal repair path as the other slow ClientLink lanes.
func TestFactoryHostLiveTextSlowSubscriber(t *testing.T) {
	ctx := placementContext(t)
	const tenant = orchestrationtest.PooledTenantA
	const session = sessionwire.SessionID("live-text-slow-session")
	const command = sessionwire.CommandID("live-text-slow-create")
	world := orchestrationtest.NewPooledWorld(t, ctx, orchestrationtest.PooledWorldOptions{
		Tenants:  []sessionwire.TenantID{tenant},
		LiveText: &host.LiveTextOptions{RateBytesPerSecond: 1, BurstBytes: 4096},
	})
	hold, first, rest := make(chan struct{}), make(chan struct{}), make(chan struct{})
	chunks := make([]content.Chunk, 0, 1201)
	chunks = append(chunks, &content.TextChunk{Text: "first "})
	var streamed strings.Builder
	streamed.WriteString("first ")
	for i := 0; i < 1200; i++ {
		word := fmt.Sprintf("word%d ", i)
		streamed.WriteString(word)
		chunks = append(chunks, &content.TextChunk{Text: word})
	}
	world.LLM.Script(orchestrationtest.PooledTurn{Hold: hold, Chunks: chunks, BeforeChunk: []<-chan struct{}{first, rest}})
	tap := orchestrationtest.NewHostLinkTap()
	h := orchestrationtest.StartTappedPooledHost(t, ctx, world, "live-text-slow-host", 4, tap)
	orchestrationtest.AwaitAdvertised(t, world, h.ID)
	factory := orchestrationtest.StartPooledFactory(t, ctx, world, "live-text-slow-factory", nil)
	browser := orchestrationtest.OpenPooledBrowser(t, ctx, factory, tenant, orchestrationtest.PooledBrowserOptions{Stallable: true, ReadBufferBytes: 1024})
	if err := browser.Watch(t, ctx, session, 0); err != nil {
		t.Fatal(err)
	}
	status, body := factory.Post(t, ctx, tenant, "/v1/sessions", sessionwire.CreateRequest{
		CommandEnvelope: orchestrationtest.PooledEnvelope(string(command)), SessionID: session,
		AgentID: orchestrationtest.PooledAgent, Blocks: json.RawMessage(`[{"type":"text","text":"hello"}]`),
	})
	if status != http.StatusCreated {
		t.Fatalf("create answered %d: %s", status, body)
	}
	orchestrationtest.PooledWait(t, "slow browser's HostLink subscription", 30*time.Second, func() bool {
		for _, link := range orchestrationtest.TappedLinks(t, tap) {
			if len(link.Subscribes) != 0 {
				return true
			}
		}
		return false
	})
	close(hold)
	orchestrationtest.PooledWait(t, "slow turn reached the scripted model", 30*time.Second, func() bool {
		return len(world.LLM.Requests()) >= 1
	})
	orchestrationtest.PooledWait(t, "browser covered the initial durable events", 30*time.Second, func() bool {
		seqs := world.PublicJournalSeqs(t, ctx, tenant, session)
		if len(seqs) == 0 {
			return false
		}
		_, position := browser.Settle(t, ctx)
		return position >= seqs[len(seqs)-1]
	})
	_, before := browser.Settle(t, ctx)
	close(first)
	orchestrationtest.PooledWait(t, "first preview before the stall", 30*time.Second, func() bool {
		for _, entry := range browser.Log() {
			if entry.Kind == "X" {
				return true
			}
		}
		return false
	})
	if _, afterPreview := browser.Settle(t, ctx); afterPreview != before {
		t.Fatalf("ephemeral advanced browser coverage from %d to %d", before, afterPreview)
	}
	browser.Stall()
	close(rest)
	runtimeID := world.RuntimeSessionID(t, ctx, tenant, session)
	orchestrationtest.PooledWait(t, "slow stream's durable StepDone", 60*time.Second, func() bool {
		return len(orchestrationtest.JournalEvents[event.StepDone](t, world, tenant, runtimeID)) > 0
	})
	if got := stepText(orchestrationtest.JournalEvents[event.StepDone](t, world, tenant, runtimeID)[0]); got != streamed.String() {
		t.Fatalf("slow stream's durable text has %d bytes, want %d", len(got), streamed.Len())
	}
	if _, position := browser.Settle(t, ctx); position != before {
		t.Fatalf("stalled subscriber coverage advanced from %d to %d", before, position)
	}
	browser.Release()
	orchestrationtest.PooledWait(t, "HostLink completed the slow turn", 60*time.Second, func() bool {
		return liveTerminalCount(hostLinkSessionFrames(t, tap)) >= 1
	})
	forwarded := 0
	for _, conn := range tap.Conns() {
		for _, message := range conn.Messages() {
			if !message.FromHost {
				continue
			}
			var push struct {
				Push struct {
					Pub struct {
						Data struct {
							Type string `json:"type"`
							Body struct {
								Chunk struct {
									Text string `json:"text"`
								} `json:"chunk"`
							} `json:"body"`
						} `json:"data"`
					} `json:"pub"`
				} `json:"push"`
			}
			if json.Unmarshal(message.Payload, &push) == nil && push.Push.Pub.Data.Type == "ephemeral_publication" {
				forwarded += len(push.Push.Pub.Data.Body.Chunk.Text)
			}
		}
	}
	if forwarded == 0 || forwarded >= streamed.Len() {
		t.Fatalf("HostLink forwarded %d of %d preview bytes, want some but not all to prove drops", forwarded, streamed.Len())
	}
	var held []orchestrationtest.PooledCommitted
	orchestrationtest.PooledWait(t, "slow browser caught up with durable journal", 60*time.Second, func() bool {
		var position uint64
		held, position = browser.Settle(t, ctx)
		seqs := world.PublicJournalSeqs(t, ctx, tenant, session)
		return len(seqs) > 0 && position >= seqs[len(seqs)-1]
	})
	seqs := world.PublicJournalSeqs(t, ctx, tenant, session)
	if len(held) != len(seqs) {
		t.Fatalf("slow browser holds %d enduring records, journal has %d: held=%v seqs=%v", len(held), len(seqs), held, seqs)
	}
	for i, seq := range seqs {
		if held[i].JournalSeq != seq {
			t.Fatalf("enduring record %d has seq %d, want %d", i, held[i].JournalSeq, seq)
		}
	}
	if len(browser.Log()) == 0 {
		t.Fatal("slow browser received no publications")
	}
}
