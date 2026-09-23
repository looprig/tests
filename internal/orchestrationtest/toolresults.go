//go:build integration

package orchestrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/harness/pkg/loop"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/inference"
	"github.com/looprig/sessionstore"
	"github.com/looprig/tests/internal/toolresultobjects"
	"github.com/looprig/tools"
)

// This file is the I2.2 composition: a world whose agent runs REAL Bash under
// readable tool-result retention, pages retained output back with the REAL
// read_tool_result, and whose every Factory serves the retained bytes through
// its object route under the production evidence policy.
//
// What is composed is exactly what a product composes, in three places:
//
//   - the RIG (defineRig): tools.Bash and tools.ReadToolResultDefinition,
//     loop.WithToolLimits with a finite ResultBytes, and
//     rig.WithToolResultObjects over the SAME harness store the rig journals
//     into, with a per-rig spill base outside the workspace region;
//   - the FACTORY (objectRouteOptions): toolresultobjects.FactoryOptions --
//     the evidence policy over each tenant's harness runtime store, the
//     runtime-addressed SessionObjectStoreResolver, the object limits, and the
//     D7 ceiling check that refuses to compose a capture ceiling Factory could
//     never verify;
//   - the HOST: nothing. Host builds no rig and carries no object plane; its
//     department only launches the product's rig.

// ToolResultRetention is the retention configuration a ToolResults world
// composes. Zero fields take the documented defaults.
type ToolResultRetention struct {
	// ResultBytes is the model preview budget (loop.ToolLimits.ResultBytes).
	// It MUST be finite: with zero nothing is ever elided, so nothing is ever
	// retained. Zero here takes DefaultToolResultPreviewBytes.
	ResultBytes int
	// CaptureBytes is the per-result retention ceiling
	// (loop.ToolLimits.CaptureBytes); zero is harness's 8 MiB default.
	CaptureBytes int
	// Iterations bounds tool rounds per turn (loop.ToolLimits.Iterations).
	// Paging a capture back costs one round per page. Zero takes 64.
	Iterations int
	// Limits are the Factory object limits; zero takes
	// factory.DefaultObjectLimits (1 MiB pages, 64 MiB verification).
	Limits factory.ObjectLimits
}

// DefaultToolResultPreviewBytes is the kit's model preview budget.
const DefaultToolResultPreviewBytes = 8192

func (r ToolResultRetention) previewBytes() int {
	if r.ResultBytes == 0 {
		return DefaultToolResultPreviewBytes
	}
	return r.ResultBytes
}

// ObjectLimits is the Factory object limits this world composes.
func (r ToolResultRetention) ObjectLimits() factory.ObjectLimits {
	if r.Limits == (factory.ObjectLimits{}) {
		return factory.DefaultObjectLimits()
	}
	return r.Limits
}

func (r ToolResultRetention) limits() loop.ToolLimits {
	iterations := r.Iterations
	if iterations == 0 {
		iterations = 64
	}
	return loop.ToolLimits{Iterations: iterations, ResultBytes: r.previewBytes(), CaptureBytes: r.CaptureBytes}
}

func (r ToolResultRetention) definitions() []tool.Definition {
	return []tool.Definition{tools.Bash(), tools.ReadToolResultDefinition()}
}

// spillBase is a fresh owner-only directory OUTSIDE the workspace region, one
// per rig: harness refuses a spill base overlapping the workspace, because a
// checkpoint archives the whole region. The world remembers every one, so a
// case can prove the spill held nothing once its sessions were released.
func (w *PooledWorld) spillBase(tb TB) string {
	tb.Helper()
	dir, err := os.MkdirTemp("", "i22-spill-")
	if err != nil {
		tb.Fatalf("orchestrationtest: creating a capture spill base: %v", err)
		return ""
	}
	tb.Cleanup(func() { _ = os.RemoveAll(dir) })
	w.spillMu.Lock()
	w.spillBases = append(w.spillBases, dir)
	w.spillMu.Unlock()
	return dir
}

// SpillEntries reports, per capture spill base this world created, how many
// session spill roots it still holds. harness creates <base>/<session> for a
// live session and removes it at shutdown.
func (w *PooledWorld) SpillEntries(tb TB) map[string]int {
	tb.Helper()
	w.spillMu.Lock()
	bases := append([]string(nil), w.spillBases...)
	w.spillMu.Unlock()
	entries := map[string]int{}
	for _, base := range bases {
		found, err := os.ReadDir(base)
		if err != nil {
			tb.Fatalf("orchestrationtest: reading spill base %s: %v", base, err)
			return nil
		}
		entries[base] = len(found)
	}
	return entries
}

// ToolResults is the world's retention configuration, nil when not composed.
func (w *PooledWorld) ToolResults() *ToolResultRetention { return w.toolResults }

// ToolResultPreviewBytes is the world's composed model preview budget.
func (w *PooledWorld) ToolResultPreviewBytes() int { return w.toolResults.previewBytes() }

// objectRouteOptions are the object-route options every Factory in this world
// composes. Without ToolResults the route is composed REFUSING (a resolver
// that answers no store and no policy), as every earlier lane composed it.
//
// With ToolResults each Factory opens its OWN read of each tenant's runtime
// store, as a separate Factory process would: a restarted Factory therefore
// starts with a cold evidence cache and must re-derive every grant from the
// journal.
func (w *PooledWorld) objectRouteOptions(tb TB) []factory.Option {
	tb.Helper()
	if w.toolResults == nil {
		return []factory.Option{factory.WithSessionObjectStoreResolver(func(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionstore.SessionBinding) (factory.ObjectReader, error) {
			return nil, ErrObjectNotPermitted
		})}
	}
	evidence := map[sessionwire.TenantID]toolresultobjects.Evidence{}
	objects := map[sessionwire.TenantID]factory.ObjectReader{}
	for _, tenant := range w.tenants {
		store, err := harnessstore.Open(w.journalBackends[tenant], w.journalOpenOptions(tenant)...)
		if err != nil {
			tb.Fatalf("orchestrationtest: opening the Factory's evidence read of %q's runtime store: %v", tenant, err)
			return nil
		}
		evidence[tenant] = store
		objects[tenant] = OpenRuntimeJournal(tb, context.Background(), w.journalBackends[tenant], tenant)
	}
	options, err := toolresultobjects.FactoryOptions(toolresultobjects.Config{
		Binding: toolresultobjects.Binding{StorageBindingID: PooledBinding, BindingVersion: PooledBindingVersion},
		Evidence: func(tenant sessionwire.TenantID) (toolresultobjects.Evidence, bool) {
			e, ok := evidence[tenant]
			return e, ok
		},
		Objects: func(tenant sessionwire.TenantID) (factory.ObjectReader, bool) {
			o, ok := objects[tenant]
			return o, ok
		},
		CaptureBytes: w.toolResults.CaptureBytes,
		Limits:       w.toolResults.ObjectLimits(),
	})
	if err != nil {
		tb.Fatalf("orchestrationtest: composing the object route: %v", err)
		return nil
	}
	return options
}

// ---- the model -------------------------------------------------------------

var (
	// ToolResultMarkerPattern matches harness's retention marker when the
	// loop can page the capture, capturing the capture id.
	ToolResultMarkerPattern = regexp.MustCompile(`\n\[tool output shaped; [^\]]*read_tool_result capture_id="([0-9a-f-]{36})"\]\n$`)
	// ToolResultFooterPattern matches a read_tool_result page's footer:
	// capture id, first byte, last byte, captured total, and next_offset (empty
	// at the end).
	ToolResultFooterPattern = regexp.MustCompile(`\n\[capture ([0-9a-f-]{36}): bytes (\d+)-(\d+) of (\d+) retained[^\]]*; (?:next_offset=(\d+)|end)[^\]]*\]$`)
)

// The user-message commands a ToolResultModel understands.
const (
	// ToolResultRun runs the rest of the message in Bash and stops at the
	// preview.
	ToolResultRun = "run "
	// ToolResultRunAndPage runs the rest in Bash and then pages the whole
	// capture back with read_tool_result, following next_offset to the end.
	ToolResultRunAndPage = "run-and-page "
	// ToolResultRead reads "<capture id> <offset>" and follows next_offset to
	// the end; "<capture id> <offset> once" reads that one page only.
	ToolResultRead = "read "
)

// ToolResultModel is a scripted model that acts on what it is SHOWN, the way a
// real one does: it can only page a capture whose id the retention marker told
// it, and it can only continue from the next_offset a page footer told it.
// Install it with world.LLM.Respond(model.Respond).
//
// It records every tool result it was shown, so a case asserts on exactly the
// text the model saw.
type ToolResultModel struct {
	mu    sync.Mutex
	shown []string
	pages []string
}

// Respond chooses the next turn from the request.
func (m *ToolResultModel) Respond(request inference.Request) PooledTurn {
	if len(request.Messages) == 0 {
		return PooledTurn{Text: "done"}
	}
	instruction := lastUserText(request.Messages)
	last := request.Messages[len(request.Messages)-1]
	switch message := last.(type) {
	case *content.UserMessage:
		text := blocksText(message.Blocks)
		switch {
		case strings.HasPrefix(text, ToolResultRunAndPage):
			return bashTurn(strings.TrimPrefix(text, ToolResultRunAndPage))
		case strings.HasPrefix(text, ToolResultRun):
			return bashTurn(strings.TrimPrefix(text, ToolResultRun))
		case strings.HasPrefix(text, ToolResultRead):
			fields := strings.Fields(strings.TrimPrefix(text, ToolResultRead))
			if len(fields) >= 2 {
				offset, err := strconv.ParseUint(fields[1], 10, 64)
				if err == nil {
					return readTurn(fields[0], offset)
				}
			}
		}
		return PooledTurn{Text: "done"}
	case *content.ToolResultMessage:
		text := blocksText(message.Blocks)
		m.mu.Lock()
		defer m.mu.Unlock()
		m.shown = append(m.shown, text)
		if match := ToolResultFooterPattern.FindStringSubmatch(text); match != nil {
			m.pages = append(m.pages, text)
			if match[5] == "" || strings.HasSuffix(instruction, " once") {
				return PooledTurn{Text: "done"}
			}
			next, _ := strconv.ParseUint(match[5], 10, 64)
			return readTurn(match[1], next)
		}
		if match := ToolResultMarkerPattern.FindStringSubmatch(text); match != nil {
			if strings.HasPrefix(instruction, ToolResultRunAndPage) {
				return readTurn(match[1], 0)
			}
		}
		return PooledTurn{Text: "done"}
	}
	return PooledTurn{Text: "done"}
}

// TakePages returns and forgets every read_tool_result page shown since the
// last call, in order.
func (m *ToolResultModel) TakePages() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	pages := m.pages
	m.pages = nil
	return pages
}

// TakeShown returns and forgets every tool result text shown since the last
// call, in order.
func (m *ToolResultModel) TakeShown() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	shown := m.shown
	m.shown = nil
	return shown
}

func bashTurn(command string) PooledTurn {
	args, _ := json.Marshal(map[string]any{"command": command, "timeout": 120})
	return PooledTurn{ToolName: "Bash", ToolInput: string(args)}
}

func readTurn(captureID string, offset uint64) PooledTurn {
	return PooledTurn{ToolName: loop.ReadToolResultToolName,
		ToolInput: fmt.Sprintf(`{"capture_id":%q,"offset":%d}`, captureID, offset)}
}

func lastUserText(messages []content.Conversation) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if user, ok := messages[i].(*content.UserMessage); ok {
			return blocksText(user.Blocks)
		}
	}
	return ""
}

func blocksText(blocks []content.Block) string {
	var b strings.Builder
	for _, block := range blocks {
		if text, ok := block.(*content.TextBlock); ok {
			b.WriteString(text.Text)
		}
	}
	return b.String()
}

// ReassemblePages concatenates read_tool_result pages, checking that each
// names captureID, starts where the previous one ended, and that the last one
// ends the capture. It returns the reassembled bytes and the captured total
// the footers reported.
func ReassemblePages(tb TB, captureID string, pages []string) ([]byte, uint64) {
	tb.Helper()
	var out []byte
	var total uint64
	for i, page := range pages {
		loc := ToolResultFooterPattern.FindStringSubmatchIndex(page)
		if loc == nil {
			tb.Fatalf("orchestrationtest: page %d has no footer: %.200q", i, page)
			return nil, 0
		}
		if got := page[loc[2]:loc[3]]; got != captureID {
			tb.Fatalf("orchestrationtest: page %d names capture %s, want %s", i, got, captureID)
			return nil, 0
		}
		first, _ := strconv.ParseUint(page[loc[4]:loc[5]], 10, 64)
		if first != uint64(len(out)) {
			tb.Fatalf("orchestrationtest: page %d starts at %d, want %d", i, first, len(out))
			return nil, 0
		}
		total, _ = strconv.ParseUint(page[loc[8]:loc[9]], 10, 64)
		out = append(out, page[:loc[0]]...)
		if i == len(pages)-1 && loc[10] != -1 {
			tb.Fatalf("orchestrationtest: the last page reports next_offset %s, not the end", page[loc[10]:loc[11]])
			return nil, 0
		}
	}
	return out, total
}

// ---- the object route, as a client reads it ---------------------------------

// ObjectResponse is one answer from Factory's object route.
type ObjectResponse struct {
	Status int
	Header http.Header
	Body   []byte
}

// GetObject reads /v1/sessions/{s}/objects/{objectID} (or its /metadata) as
// tenant, with an optional Range header.
func (f *PooledFactory) GetObject(tb TB, ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID, objectID string, metadata bool, rangeHeader string) ObjectResponse {
	tb.Helper()
	path := "/v1/sessions/" + string(s) + "/objects/" + objectID
	if metadata {
		path += "/metadata"
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.BaseURL+path, nil)
	if err != nil {
		tb.Fatalf("orchestrationtest: building GET %s: %v", path, err)
		return ObjectResponse{}
	}
	request.Header.Set("Authorization", "Bearer "+PooledBearers[tenant])
	if rangeHeader != "" {
		request.Header.Set("Range", rangeHeader)
	}
	response, err := f.client.Do(request)
	if err != nil {
		tb.Fatalf("orchestrationtest: GET %s as %s: %v", path, tenant, err)
		return ObjectResponse{}
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		tb.Fatalf("orchestrationtest: reading GET %s: %v", path, err)
	}
	return ObjectResponse{Status: response.StatusCode, Header: response.Header, Body: body}
}

// ReadObjectPaged reads a whole object through Factory's object route the way
// a browser must once it is larger than one page: the metadata route first,
// then exact Range pages of at most pageBytes. It fails the case on any
// non-success answer and returns the bytes and the metadata.
func (f *PooledFactory) ReadObjectPaged(tb TB, ctx context.Context, tenant sessionwire.TenantID, s sessionwire.SessionID, objectID string, pageBytes uint64) ([]byte, sessionwire.ObjectMetadata) {
	tb.Helper()
	answer := f.GetObject(tb, ctx, tenant, s, objectID, true, "")
	if answer.Status != http.StatusOK {
		tb.Fatalf("orchestrationtest: the metadata read of %s answered %d: %s", objectID, answer.Status, answer.Body)
		return nil, sessionwire.ObjectMetadata{}
	}
	var metadata sessionwire.ObjectMetadata
	if err := metadata.UnmarshalJSON(answer.Body); err != nil {
		tb.Fatalf("orchestrationtest: the metadata of %s is not Core ObjectMetadata (%s): %v", objectID, answer.Body, err)
		return nil, sessionwire.ObjectMetadata{}
	}
	out := make([]byte, 0, metadata.SizeBytes)
	for start := uint64(0); start < metadata.SizeBytes; start += pageBytes {
		end := min(start+pageBytes, metadata.SizeBytes) - 1
		page := f.GetObject(tb, ctx, tenant, s, objectID, false, fmt.Sprintf("bytes=%d-%d", start, end))
		if page.Status != http.StatusPartialContent {
			tb.Fatalf("orchestrationtest: page %d-%d of %s answered %d: %.300s", start, end, objectID, page.Status, page.Body)
			return nil, sessionwire.ObjectMetadata{}
		}
		if got := page.Header.Get("X-Object-Digest"); got != metadata.Digest {
			tb.Fatalf("orchestrationtest: page %d-%d of %s carries digest %q, metadata says %q", start, end, objectID, got, metadata.Digest)
			return nil, sessionwire.ObjectMetadata{}
		}
		out = append(out, page.Body...)
	}
	return out, metadata
}

// ToolResultObjectKeys lists every tool-result object blob in one runtime
// session's scope of a tenant's journal backend -- the ground truth for "no
// object was written", which no store API enumerates. The key layout is
// SessionStore's legacy single-tenant one, which harness's journal store uses:
// sessions/<runtime session>/blobs/v1/tool-result/<digest>/<generation>.
func (w *PooledWorld) ToolResultObjectKeys(tb TB, ctx context.Context, tenant sessionwire.TenantID, runtime string) []string {
	tb.Helper()
	backend := w.journalBackends[tenant]
	if backend == nil {
		tb.Fatalf("orchestrationtest: no journal backend for tenant %q", tenant)
		return nil
	}
	keys, err := backend.Blobs.List(ctx, "sessions/"+runtime+"/blobs/v1/"+string(sessionstore.ObjectKindToolResult)+"/")
	if err != nil {
		tb.Fatalf("orchestrationtest: listing %s's tool-result blobs: %v", runtime, err)
		return nil
	}
	return keys
}
