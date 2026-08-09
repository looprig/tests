package tests

import (
	"bytes"
	"context"
	"crypto/x509"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/credentials"
	"github.com/looprig/credentials/httpauth"
	"github.com/looprig/inference"
	"github.com/looprig/inference/codec"
	"github.com/looprig/inference/codec/anthropicapi"
	"github.com/looprig/inference/codec/openairesponses"
	"github.com/looprig/inference/failure"
	"github.com/looprig/inference/gateway"
	"github.com/looprig/inference/model"
	"github.com/looprig/inference/stream"
	"github.com/looprig/llm"
	"github.com/looprig/llm/auto"
	anthropicsubscription "github.com/looprig/llm/providers/anthropic/subscription"
	openaisubscription "github.com/looprig/llm/providers/openai/subscription"
	"github.com/looprig/secrets"
)

// The fixtures intentionally contain only provider-neutral request material.
// In particular, they do not contain API keys, OAuth codes, refresh tokens,
// provider account identifiers, or provider-native IDs.
//
//go:embed testdata/subscription/*.json
var credentialGatewayFixtures embed.FS

const credentialGatewayToken = "fixture-gateway-token"

// The always-running cases below use a real public OpenAI transport client
// against a bounded local TLS server. They certify credentials.Source ->
// llm/auto -> inference transport -> gateway/codec plumbing, not provider
// subscription sanction.
func TestCredentialGatewayRealOpenAIThroughAnthropicInvoke(t *testing.T) {
	fixture := newRealOpenAIGateway(t, false)
	defer fixture.close()
	req := fixtureRequest(t, "/v1/messages", fixtureBytes(t, "testdata/subscription/anthropic-invoke.json"), context.Background())
	recording := httptest.NewRecorder()
	fixture.handler.ServeHTTP(recording, req)
	if recording.Code != http.StatusOK {
		t.Fatalf("Anthropic ingress through OpenAI status = %d, want 200; body: %s", recording.Code, recording.Body.String())
	}
	if !strings.Contains(recording.Body.String(), `"stop_reason":"tool_use"`) || !strings.Contains(recording.Body.String(), `"input_tokens":17`) {
		t.Fatalf("cross-harness response lost stop/usage fields: %s", recording.Body.String())
	}
	if fixture.source.acquireCount() != 1 || fixture.source.invalidateCount() != 0 {
		t.Fatalf("source accounting = acquires %d/invalidate %d, want 1/0", fixture.source.acquireCount(), fixture.source.invalidateCount())
	}
	descriptor := fixture.source.Descriptor()
	if descriptor.Provider != "openai" || descriptor.Transport != "responses" || descriptor.Scheme != credentials.SchemeAPIKey || descriptor.Audience == "" {
		t.Fatalf("source descriptor = %#v, want exact OpenAI Responses API-key origin binding", descriptor)
	}
	if body := fixture.wire.body(0); !strings.Contains(body, `"tools"`) || !strings.Contains(body, `"reasoning"`) || !strings.Contains(body, `"input_image"`) {
		t.Fatalf("real OpenAI request lost tools/reasoning/image fields: %s", body)
	}
	fixture.assertWire(t, 1, "Authorization", "Bearer fixture-openai-key")
}

func TestCredentialGatewayRealOpenAIThroughResponsesStream(t *testing.T) {
	fixture := newRealOpenAIGateway(t, false)
	defer fixture.close()
	req := fixtureRequest(t, "/v1/responses", fixtureBytes(t, "testdata/subscription/openai-responses-stream.json"), context.Background())
	recording := httptest.NewRecorder()
	fixture.handler.ServeHTTP(recording, req)
	if recording.Code != http.StatusOK {
		t.Fatalf("Responses ingress through OpenAI status = %d, want 200; body: %s", recording.Code, recording.Body.String())
	}
	body := recording.Body.String()
	for _, marker := range []string{"event: response.output_text.delta", "event: response.reasoning_summary_text.delta", "event: response.function_call_arguments.delta", `"input_tokens":17`, `"output_tokens":9`} {
		if !strings.Contains(body, marker) {
			t.Errorf("cross-harness stream missing %q: %s", marker, body)
		}
	}
	if fixture.source.acquireCount() != 1 || fixture.source.invalidateCount() != 0 {
		t.Fatalf("source accounting = acquires %d/invalidate %d, want 1/0", fixture.source.acquireCount(), fixture.source.invalidateCount())
	}
	if body := fixture.wire.body(0); !strings.Contains(body, `"tools"`) || !strings.Contains(body, `"reasoning"`) || !strings.Contains(body, `"input_image"`) || !strings.Contains(body, `"json_schema"`) {
		t.Fatalf("real OpenAI stream request lost tools/reasoning/image/structured fields: %s", body)
	}
	fixture.assertWire(t, 1, "Authorization", "Bearer fixture-openai-key")
}

func TestCredentialGatewayRealAuthRecoveryRotatesLeaseOnce(t *testing.T) {
	fixture := newRealOpenAIGateway(t, true)
	defer fixture.close()
	req := fixtureRequest(t, "/v1/messages", fixtureBytes(t, "testdata/subscription/anthropic-invoke.json"), context.Background())
	recording := httptest.NewRecorder()
	fixture.handler.ServeHTTP(recording, req)
	if recording.Code != http.StatusOK {
		t.Fatalf("recovered Anthropic ingress status = %d, want 200; body: %s", recording.Code, recording.Body.String())
	}
	if fixture.source.acquireCount() != 2 || fixture.source.invalidateCount() != 1 {
		t.Fatalf("source accounting = acquires %d/invalidate %d, want 2/1", fixture.source.acquireCount(), fixture.source.invalidateCount())
	}
	fixture.assertWire(t, 2, "Authorization", "Bearer fixture-openai-key", "Bearer fixture-openai-key-rotated")
}

func TestCredentialGatewayFixtureInvokeCrossHarness(t *testing.T) {
	fixture := newCredentialGatewayFixture(t, nil)
	req := fixtureRequest(t, "/v1/messages", fixtureBytes(t, "testdata/subscription/anthropic-invoke.json"), context.Background())
	recording := httptest.NewRecorder()
	fixture.handler.ServeHTTP(recording, req)
	if recording.Code != http.StatusOK {
		t.Fatalf("Anthropic ingress status = %d, want 200; body: %s", recording.Code, recording.Body.String())
	}

	var response struct {
		Model      string `json:"model"`
		StopReason string `json:"stop_reason"`
		Content    []struct {
			Type      string          `json:"type"`
			Text      string          `json:"text"`
			Thinking  string          `json:"thinking"`
			ID        string          `json:"id"`
			Name      string          `json:"name"`
			Signature string          `json:"signature"`
			Input     json.RawMessage `json:"input"`
		} `json:"content"`
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(recording.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode Anthropic response: %v; body: %s", err, recording.Body.String())
	}
	if response.Model != "fixture-alias" {
		t.Errorf("response model = %q, want ingress alias fixture-alias", response.Model)
	}
	if response.StopReason != "tool_use" {
		t.Errorf("stop_reason = %q, want tool_use", response.StopReason)
	}
	if response.Usage.InputTokens != 17 || response.Usage.OutputTokens != 9 {
		t.Errorf("usage = %+v, want input=17 output=9", response.Usage)
	}
	if !containsAnthropicBlock(response.Content, "thinking") || !containsAnthropicBlock(response.Content, "tool_use") {
		t.Errorf("response content = %#v, want thinking and tool_use blocks", response.Content)
	}

	decoded := fixture.client.lastRequest(t)
	if decoded.System != "Be concise and explain the tool result." {
		t.Errorf("decoded system = %q", decoded.System)
	}
	if len(decoded.Tools) != 1 || decoded.Tools[0].Name != "get_weather" {
		t.Errorf("decoded tools = %#v, want get_weather", decoded.Tools)
	}
	if decoded.ToolChoice != inference.ToolChoiceRequired {
		t.Errorf("decoded tool choice = %v, want required", decoded.ToolChoice)
	}
	if decoded.Override == nil || decoded.Override.Effort != model.EffortHigh {
		t.Errorf("decoded effort = %#v, want high", decoded.Override)
	}
	if !messagesContainImage(decoded.Messages) {
		t.Fatal("decoded request lost the inline image")
	}
	if got := fixture.client.callCount(); got != 1 {
		t.Fatalf("credential-backed fixture call count = %d, want 1", got)
	}
}

func TestCredentialGatewayFixtureStreamCrossHarness(t *testing.T) {
	fixture := newCredentialGatewayFixture(t, nil)
	req := fixtureRequest(t, "/v1/responses", fixtureBytes(t, "testdata/subscription/openai-responses-stream.json"), context.Background())
	recording := httptest.NewRecorder()
	fixture.handler.ServeHTTP(recording, req)
	if recording.Code != http.StatusOK {
		t.Fatalf("Responses ingress status = %d, want 200; body: %s", recording.Code, recording.Body.String())
	}
	body := recording.Body.String()
	for _, marker := range []string{
		"event: response.output_text.delta",
		"event: response.reasoning_summary_text.delta",
		"event: response.function_call_arguments.delta",
		"event: response.completed",
		`"input_tokens":17`,
		`"output_tokens":9`,
	} {
		if !strings.Contains(body, marker) {
			t.Errorf("stream body missing %q: %s", marker, body)
		}
	}

	decoded := fixture.client.lastRequest(t)
	if decoded.Output == nil || decoded.Output.Name != "fixture_answer" || !decoded.Output.Strict {
		t.Fatalf("decoded structured output = %#v, want fixture_answer/strict", decoded.Output)
	}
	if len(decoded.Tools) != 1 || decoded.Tools[0].Name != "lookup_fixture" {
		t.Errorf("decoded tools = %#v, want lookup_fixture", decoded.Tools)
	}
	if decoded.ToolChoice != inference.ToolChoiceRequired {
		t.Errorf("decoded tool choice = %v, want required", decoded.ToolChoice)
	}
	if decoded.Override == nil || decoded.Override.Effort != model.EffortHigh {
		t.Errorf("decoded reasoning effort = %#v, want high", decoded.Override)
	}
	if !messagesContainImage(decoded.Messages) {
		t.Fatal("decoded Responses request lost the inline image")
	}
}

func TestCredentialGatewayQuotaPropagatesWithoutAlternateSelection(t *testing.T) {
	primary := newRecordingCredentialClient()
	primary.quota = true
	alternate := newRecordingCredentialClient()
	handler := newFixtureGatewayRoutes(t, map[string]inference.Client{"primary": primary, "alternate": alternate})
	body := strings.Replace(string(fixtureBytes(t, "testdata/subscription/anthropic-invoke.json")), `"model": "fixture-alias"`, `"model": "primary"`, 1)
	req := fixtureRequest(t, "/v1/messages", []byte(body), context.Background())
	recording := httptest.NewRecorder()
	handler.ServeHTTP(recording, req)
	if recording.Code != http.StatusTooManyRequests {
		t.Fatalf("quota status = %d, want 429; body: %s", recording.Code, recording.Body.String())
	}
	if !strings.Contains(recording.Body.String(), "rate_limit_error") && !strings.Contains(recording.Body.String(), "quota") {
		t.Errorf("quota response does not identify a quota/rate-limit failure: %s", recording.Body.String())
	}
	if got := primary.callCount(); got != 1 {
		t.Fatalf("configured deployment call count = %d, want one; quota must not route elsewhere", got)
	}
	if got := alternate.callCount(); got != 0 {
		t.Fatalf("alternate deployment call count = %d, want 0", got)
	}
}

func TestCredentialGatewayCancellationClosesStream(t *testing.T) {
	inner := newRecordingCredentialClient()
	inner.blockStream = true
	fixture := newCredentialGatewayFixture(t, inner)
	ctx, cancel := context.WithCancel(context.Background())
	req := fixtureRequest(t, "/v1/responses", fixtureBytes(t, "testdata/subscription/openai-responses-stream.json"), ctx)
	recording := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		fixture.handler.ServeHTTP(recording, req)
		close(done)
	}()

	select {
	case <-inner.streamStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled gateway stream did not return")
	}
	select {
	case <-inner.streamClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("gateway cancellation did not close the provider stream reader")
	}
}

// TestCredentialGatewayChildExecEnvMechanics checks only that an explicitly
// constructed exec.Cmd environment contains gateway metadata and no provider
// variables. CodeRig's ACP composition/e2e tests are the authority for the
// production child launch path; this test does not claim to invoke CodeRig.
func TestCredentialGatewayChildExecEnvMechanics(t *testing.T) {
	if os.Getenv("LOOPRIG_CREDENTIAL_GATEWAY_CHILD") == "1" {
		if got := os.Getenv("LOOPRIG_GATEWAY_BASE_URL"); got != "http://127.0.0.1:43123" {
			t.Fatalf("child gateway base URL = %q, want loopback URL", got)
		}
		if got := os.Getenv("LOOPRIG_GATEWAY_TOKEN"); got != "child-only-gateway-token" {
			t.Fatalf("child gateway token = %q, want child-only token", got)
		}
		for _, key := range []string{
			"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_ACCESS_TOKEN", "ANTHROPIC_AUTH_TOKEN",
			"OPENAI_REFRESH_TOKEN", "ANTHROPIC_REFRESH_TOKEN", "LOOPRIG_PROVIDER_CREDENTIAL",
		} {
			if _, ok := os.LookupEnv(key); ok {
				t.Fatalf("child inherited provider credential environment variable %s", key)
			}
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestCredentialGatewayChildExecEnvMechanics$")
	// An explicit environment exercises exec.Env mechanics only.
	cmd.Env = []string{
		"LOOPRIG_CREDENTIAL_GATEWAY_CHILD=1",
		"LOOPRIG_GATEWAY_BASE_URL=http://127.0.0.1:43123",
		"LOOPRIG_GATEWAY_TOKEN=child-only-gateway-token",
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("isolated child exited with %v: %s", err, output)
	}
}

func TestCredentialGatewayFixturesContainNoCredentialMaterial(t *testing.T) {
	entries, err := credentialGatewayFixtures.ReadDir("testdata/subscription")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := credentialGatewayFixtures.ReadFile("testdata/subscription/" + entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		lower := strings.ToLower(string(data))
		for _, forbidden := range []string{"sk-", "bearer ", "access_token", "refresh_token", "client_secret", "authorization"} {
			if strings.Contains(lower, forbidden) {
				t.Errorf("fixture %s contains forbidden credential marker %q", entry.Name(), forbidden)
			}
		}
	}
}

func TestCredentialGatewayFixturesStaySmall(t *testing.T) {
	entries, err := credentialGatewayFixtures.ReadDir("testdata/subscription")
	if err != nil {
		t.Fatal(err)
	}
	const maxFixtureBytes = 16 << 10
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := credentialGatewayFixtures.ReadFile("testdata/subscription/" + entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if len(data) > maxFixtureBytes {
			t.Fatalf("fixture %s is %d bytes, want <= %d", entry.Name(), len(data), maxFixtureBytes)
		}
	}
}

func TestCredentialGatewayProviderSubscriptionGates(t *testing.T) {
	cases := []struct {
		name  string
		check func() error
		gate  func(error) bool
	}{
		{
			name:  "openai",
			check: func() error { return openaisubscription.OpenAIRegistration().Require() },
			gate: func(err error) bool {
				var typed *openaisubscription.UnsupportedRegistrationError
				return errors.As(err, &typed)
			},
		},
		{
			name:  "anthropic",
			check: func() error { return anthropicsubscription.AnthropicRegistration().Require() },
			gate: func(err error) bool {
				var typed *anthropicsubscription.UnsupportedRegistrationError
				return errors.As(err, &typed)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.check()
			if err == nil {
				t.Fatal("subscription registration gate is open; add a real provider-sanctioned fixture before enabling this case")
			}
			if !tc.gate(err) {
				t.Fatalf("gate returned unrecognized error type %T: %v", err, err)
			}
			t.Skipf("provider subscription case skipped by typed registration gate %T: %v", err, err)
		})
	}
}

func TestCredentialGatewayOpenAISubscriptionThroughAnthropic(t *testing.T) {
	err := openaisubscription.OpenAIRegistration().Require()
	if err == nil {
		t.Fatal("OpenAI subscription gate is open; this case must add the sanctioned client before constructing a provider client")
	}
	var typed *openaisubscription.UnsupportedRegistrationError
	if !errors.As(err, &typed) {
		t.Fatalf("OpenAI gate returned %T, want typed unsupported registration: %v", err, err)
	}
	t.Skipf("OpenAI subscription through Anthropic ingress skipped by typed gate %T: %v", err, err)
}

func TestCredentialGatewayAnthropicSubscriptionThroughResponses(t *testing.T) {
	err := anthropicsubscription.AnthropicRegistration().Require()
	if err == nil {
		t.Fatal("Anthropic subscription gate is open; this case must add the sanctioned client before constructing a provider client")
	}
	var typed *anthropicsubscription.UnsupportedRegistrationError
	if !errors.As(err, &typed) {
		t.Fatalf("Anthropic gate returned %T, want typed unsupported registration: %v", err, err)
	}
	t.Skipf("Anthropic subscription through Responses ingress skipped by typed gate %T: %v", err, err)
}

// credentialGatewayFixture composes real ingress codecs and gateway routing
// around a provider-neutral, credential-bound fixture client. Provider-native
// subscription clients are deliberately not constructed in this fixture.
type credentialGatewayFixture struct {
	handler *gateway.Handler
	client  *recordingCredentialClient
}

func newCredentialGatewayFixture(t *testing.T, client inference.Client) *credentialGatewayFixture {
	t.Helper()
	recording, ok := client.(*recordingCredentialClient)
	if client == nil {
		recording = newRecordingCredentialClient()
		client = recording
	} else if !ok {
		// Recovery wrappers still expose the same recording delegate through the
		// fixture's client field only when callers need call-count assertions.
		recording = nil
	}
	targetModel := model.CustomModel(
		model.ProviderName("fixture-provider"),
		model.APIFormatOpenAIResponses,
		"https://fixture.invalid/v1",
		"fixture-deployment",
		model.WithTools(),
		model.WithImages(),
		model.WithThinking(),
		model.WithStructuredOutputWithTools(),
	)
	mux, err := gateway.NewMux(gateway.Mux{
		Routes: map[gateway.RouteKey]gateway.Target{
			{Ingress: model.APIFormatAnthropic, Model: "fixture-alias"}: {
				ID:     "fixture-deployment",
				Client: client,
				Model:  targetModel,
			},
			{Ingress: model.APIFormatOpenAIResponses, Model: "fixture-alias"}: {
				ID:     "fixture-deployment",
				Client: client,
				Model:  targetModel,
			},
		},
	})
	if err != nil {
		t.Fatalf("gateway.NewMux: %v", err)
	}
	handler, err := gateway.New(gateway.Config{
		Resolver: mux,
		Codecs: map[model.APIFormat]codec.ServerCodec{
			model.APIFormatAnthropic:       anthropicapi.Codec{},
			model.APIFormatOpenAIResponses: openairesponses.Codec{},
		},
		Authenticate:  gateway.StaticToken(credentialGatewayToken),
		MaxConcurrent: 4,
	})
	if err != nil {
		t.Fatalf("gateway.New: %v", err)
	}
	return &credentialGatewayFixture{handler: handler, client: recording}
}

func newFixtureGatewayRoutes(t *testing.T, clients map[string]inference.Client) *gateway.Handler {
	t.Helper()
	targetModel := model.CustomModel(model.ProviderName("fixture-provider"), model.APIFormatOpenAIResponses, "https://fixture.invalid/v1", "fixture-deployment", model.WithTools(), model.WithImages(), model.WithThinking(), model.WithStructuredOutputWithTools())
	routes := make(map[gateway.RouteKey]gateway.Target, len(clients))
	for alias, client := range clients {
		routes[gateway.RouteKey{Ingress: model.APIFormatAnthropic, Model: alias}] = gateway.Target{ID: alias, Client: client, Model: targetModel}
	}
	mux, err := gateway.NewMux(gateway.Mux{Routes: routes})
	if err != nil {
		t.Fatalf("gateway.NewMux(routes): %v", err)
	}
	handler, err := gateway.New(gateway.Config{Resolver: mux, Codecs: map[model.APIFormat]codec.ServerCodec{model.APIFormatAnthropic: anthropicapi.Codec{}}, Authenticate: gateway.StaticToken(credentialGatewayToken), MaxConcurrent: 4})
	if err != nil {
		t.Fatalf("gateway.New(routes): %v", err)
	}
	return handler
}

func fixtureBytes(t *testing.T, name string) []byte {
	t.Helper()
	data, err := credentialGatewayFixtures.ReadFile(name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func fixtureRequest(t *testing.T, path string, body []byte, ctx context.Context) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body))).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+credentialGatewayToken)
	return req
}

type recordingCredentialClient struct {
	mu sync.Mutex

	requests    []inference.Request
	quota       bool
	blockStream bool

	streamStarted chan struct{}
	streamClosed  chan struct{}
	startOnce     sync.Once
	closeOnce     sync.Once
}

func newRecordingCredentialClient() *recordingCredentialClient {
	return &recordingCredentialClient{
		streamStarted: make(chan struct{}),
		streamClosed:  make(chan struct{}),
	}
}

func (c *recordingCredentialClient) Invoke(ctx context.Context, req inference.Request) (*inference.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.requests = append(c.requests, req)
	quota := c.quota
	c.mu.Unlock()
	if quota {
		return nil, failure.NewAPIError(http.StatusTooManyRequests, "quota_exceeded", "fixture-request", 0)
	}
	return fixtureResponse(), nil
}

func (c *recordingCredentialClient) Stream(ctx context.Context, req inference.Request) (*stream.StreamReader[content.Chunk], error) {
	c.mu.Lock()
	c.requests = append(c.requests, req)
	blocked := c.blockStream
	c.mu.Unlock()
	if blocked {
		c.startOnce.Do(func() { close(c.streamStarted) })
		return stream.NewStreamReader(
			func() (content.Chunk, error) {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-c.streamClosed:
					return nil, io.EOF
				}
			},
			func() error {
				c.closeOnce.Do(func() { close(c.streamClosed) })
				return nil
			},
		), nil
	}
	chunks := []content.Chunk{
		&content.ThinkingChunk{Thinking: "fixture reasoning"},
		&content.TextChunk{Text: "fixture answer"},
		&content.ToolUseChunk{Index: 0, ID: "fixture-call", Name: "lookup_fixture", InputJSON: `{"key":"value"}`},
	}
	index := 0
	usage := content.Usage{InputTokens: 17, OutputTokens: 9, ReasoningTokens: 2}
	return stream.NewStreamReaderWithResult(
		func() (content.Chunk, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if index >= len(chunks) {
				return nil, io.EOF
			}
			chunk := chunks[index]
			index++
			return chunk, nil
		},
		func() error { return nil },
		func() (stream.StreamResult, bool, error) {
			return stream.StreamResult{Usage: &usage, Model: "fixture-deployment", FinishReason: stream.FinishReasonStop}, true, nil
		},
	), nil
}

func (c *recordingCredentialClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.requests)
}

func (c *recordingCredentialClient) lastRequest(t *testing.T) inference.Request {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.requests) == 0 {
		t.Fatal("fixture client received no request")
	}
	return c.requests[len(c.requests)-1]
}

func fixtureResponse() *inference.Response {
	usage := content.Usage{InputTokens: 17, OutputTokens: 9, ReasoningTokens: 2}
	return &inference.Response{
		Message: &content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{
			&content.ThinkingBlock{Thinking: "fixture reasoning"},
			&content.TextBlock{Text: "fixture answer"},
			&content.ToolUseBlock{ID: "fixture-call", Name: "get_weather", Input: json.RawMessage(`{"city":"fixture"}`)},
		}}},
		Usage:        &usage,
		Model:        "fixture-deployment",
		FinishReason: stream.FinishReasonToolUse,
	}
}

func containsAnthropicBlock(blocks []struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Thinking  string          `json:"thinking"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Signature string          `json:"signature"`
	Input     json.RawMessage `json:"input"`
}, want string) bool {
	for _, block := range blocks {
		if block.Type == want {
			return true
		}
	}
	return false
}

func messagesContainImage(messages content.AgenticMessages) bool {
	for _, conversation := range messages {
		var blocks []content.Block
		switch message := conversation.(type) {
		case *content.UserMessage:
			blocks = message.Blocks
		case *content.AIMessage:
			blocks = message.Blocks
		case *content.SystemMessage:
			blocks = message.Blocks
		case *content.ToolResultMessage:
			blocks = message.Blocks
		}
		for _, block := range blocks {
			if _, ok := block.(*content.ImageBlock); ok {
				return true
			}
		}
	}
	return false
}

var _ inference.Client = (*recordingCredentialClient)(nil)

// realGatewayFixture binds a real provider client through llm/auto to a real
// gateway handler. The test source is only the credential capability: it
// rotates immutable leases and never performs provider recovery itself.
type realGatewayFixture struct {
	handler *gateway.Handler
	server  *httptest.Server
	source  *rotatingSource
	wire    *wireLog
}

func newRealOpenAIGateway(t *testing.T, recoverFirst bool) *realGatewayFixture {
	t.Helper()
	wire := &wireLog{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		wire.record(r)
		if recoverFirst && wire.count() == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"message":"invalid credential","type":"invalid_request_error","code":"invalid_api_key"}}`)
			return
		}
		if got := r.URL.Path; got != "/v1/responses" {
			http.Error(w, fmt.Sprintf("path = %q, want /v1/responses", got), http.StatusBadRequest)
			return
		}
		if bytes.Contains(body, []byte(`"stream":true`)) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, openAIStreamFixtureResponse)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, openAIInvokeFixtureResponse)
	}))

	selected := model.CustomModel(model.ProviderName(llm.ProviderOpenAI), model.APIFormatOpenAIResponses, server.URL+"/v1", "fixture-deployment", model.WithTools(), model.WithImages(), model.WithThinking(), model.WithStructuredOutputWithTools())
	source := newRotatingSource(t, selected, "Bearer", []string{"fixture-openai-key", "fixture-openai-key-rotated"})
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	client, err := auto.NewWithAuth(selected, source, auto.WithTLSRootCAs(roots))
	if err != nil {
		t.Fatalf("auto.NewWithAuth(openai): %v", err)
	}
	handler := newRealGatewayHandler(t, model.APIFormatAnthropic, "fixture-alias", client, selected)
	return &realGatewayFixture{handler: handler, server: server, source: source, wire: wire}
}

func newRealGatewayHandler(t *testing.T, ingress model.APIFormat, alias string, client inference.Client, selected model.Model) *gateway.Handler {
	t.Helper()
	routes := map[gateway.RouteKey]gateway.Target{{Ingress: ingress, Model: alias}: {ID: "fixture-deployment", Client: client, Model: selected}}
	if ingress == model.APIFormatAnthropic {
		routes[gateway.RouteKey{Ingress: model.APIFormatOpenAIResponses, Model: alias}] = routes[gateway.RouteKey{Ingress: ingress, Model: alias}]
	}
	mux, err := gateway.NewMux(gateway.Mux{Routes: routes})
	if err != nil {
		t.Fatalf("gateway.NewMux(real): %v", err)
	}
	codecs := map[model.APIFormat]codec.ServerCodec{model.APIFormatAnthropic: anthropicapi.Codec{}, model.APIFormatOpenAIResponses: openairesponses.Codec{}}
	handler, err := gateway.New(gateway.Config{Resolver: mux, Codecs: codecs, Authenticate: gateway.StaticToken(credentialGatewayToken), MaxConcurrent: 4})
	if err != nil {
		t.Fatalf("gateway.New(real): %v", err)
	}
	return handler
}

func (f *realGatewayFixture) close() {
	if f != nil && f.server != nil {
		f.server.Close()
	}
}

func (f *realGatewayFixture) assertWire(t *testing.T, count int, header string, expected ...string) {
	t.Helper()
	requests := f.wire.requests()
	if len(requests) != count {
		t.Fatalf("wire requests = %d, want %d", len(requests), count)
	}
	for i, want := range expected {
		if got := requests[i].Header.Get(header); got != want {
			t.Errorf("wire %d %s = %q, want %q", i+1, header, got, want)
		}
	}
}

type wireLog struct {
	mu      sync.Mutex
	records []*http.Request
	bodies  [][]byte
}

func (l *wireLog) record(r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(body))
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, r.Clone(r.Context()))
	l.bodies = append(l.bodies, body)
}

func (l *wireLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.records)
}

func (l *wireLog) requests() []*http.Request {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]*http.Request(nil), l.records...)
}

func (l *wireLog) body(index int) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index < 0 || index >= len(l.bodies) {
		return ""
	}
	return string(l.bodies[index])
}

type rotatingSource struct {
	descriptor credentials.Descriptor
	ref        credentials.Reference
	authorizer func(secrets.Secret) (httpauth.Authorizer, error)
	keys       []string

	mu          sync.Mutex
	current     int
	acquires    int
	invalidates int
}

func newRotatingSource(t *testing.T, selected model.Model, header string, keys []string) *rotatingSource {
	t.Helper()
	policy, err := llm.AuthPolicyForModel(selected)
	if err != nil {
		t.Fatalf("AuthPolicyForModel: %v", err)
	}
	descriptor, err := policy.Accepted[0].Descriptor()
	if err != nil {
		t.Fatalf("auth descriptor: %v", err)
	}
	ref, err := credentials.NewReference(descriptor.Provider, "fixture")
	if err != nil {
		t.Fatalf("credential reference: %v", err)
	}
	authorizer := func(value secrets.Secret) (httpauth.Authorizer, error) {
		if header == "Bearer" {
			return httpauth.Bearer(value)
		}
		return httpauth.Header(header, value)
	}
	return &rotatingSource{descriptor: descriptor, ref: ref, authorizer: authorizer, keys: keys}
}

func (s *rotatingSource) Reference() credentials.Reference    { return s.ref }
func (s *rotatingSource) Descriptor() credentials.Descriptor  { return s.descriptor }
func (s *rotatingSource) CanRecover(credentials.Failure) bool { return len(s.keys) > 1 }
func (s *rotatingSource) Acquire(context.Context) (credentials.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.keys[s.current]
	s.acquires++
	secret, err := secrets.New([]byte(key))
	if err != nil {
		return nil, err
	}
	authorizer, err := s.authorizer(secret)
	if err != nil {
		return nil, err
	}
	generation, err := credentials.NewGeneration(fmt.Sprintf("fixture-generation-%d", s.current))
	if err != nil {
		return nil, err
	}
	return rotatingLease{descriptor: s.descriptor, generation: generation, authorizer: authorizer}, nil
}
func (s *rotatingSource) Invalidate(_ context.Context, generation credentials.Generation, _ credentials.Failure) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	want, err := credentials.NewGeneration(fmt.Sprintf("fixture-generation-%d", s.current))
	if err != nil || generation != want {
		return fmt.Errorf("unexpected generation invalidation")
	}
	if s.current+1 >= len(s.keys) {
		return fmt.Errorf("fixture source exhausted")
	}
	s.current++
	s.invalidates++
	return nil
}
func (s *rotatingSource) Close() error      { return nil }
func (s *rotatingSource) acquireCount() int { s.mu.Lock(); defer s.mu.Unlock(); return s.acquires }
func (s *rotatingSource) invalidateCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.invalidates
}

type rotatingLease struct {
	descriptor credentials.Descriptor
	generation credentials.Generation
	authorizer httpauth.Authorizer
}

func (l rotatingLease) Generation() credentials.Generation { return l.generation }
func (l rotatingLease) Descriptor() credentials.Descriptor { return l.descriptor }
func (l rotatingLease) ExpiresAt() time.Time               { return time.Time{} }
func (l rotatingLease) Authorizer() httpauth.Authorizer    { return l.authorizer }

const openAIInvokeFixtureResponse = `{"id":"resp_1","status":"completed","model":"fixture-deployment","output":[{"type":"reasoning","summary":[{"type":"summary_text","text":"fixture reasoning"}]},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"fixture answer"}]},{"type":"function_call","call_id":"fixture-call","name":"get_weather","arguments":"{\"city\":\"fixture\"}"}],"usage":{"input_tokens":17,"input_tokens_details":{"cached_tokens":0},"output_tokens":9,"output_tokens_details":{"reasoning_tokens":2}}}`

const openAIStreamFixtureResponse = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"fixture answer\"}\n\n" +
	"data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"fixture reasoning\"}\n\n" +
	"data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"{\\\"key\\\":\\\"fixture\\\"}\"}\n\n" +
	"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"model\":\"fixture-deployment\",\"output\":[],\"usage\":{\"input_tokens\":17,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens\":9,\"output_tokens_details\":{\"reasoning_tokens\":2}}}}\n\n"
