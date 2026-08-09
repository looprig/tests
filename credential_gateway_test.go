package tests

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
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
	"github.com/looprig/inference"
	"github.com/looprig/inference/codec"
	"github.com/looprig/inference/codec/anthropicapi"
	"github.com/looprig/inference/codec/openairesponses"
	"github.com/looprig/inference/failure"
	"github.com/looprig/inference/gateway"
	"github.com/looprig/inference/model"
	"github.com/looprig/inference/stream"
	anthropicsubscription "github.com/looprig/llm/providers/anthropic/subscription"
	openaisubscription "github.com/looprig/llm/providers/openai/subscription"
)

// The fixtures intentionally contain only provider-neutral request material.
// In particular, they do not contain API keys, OAuth codes, refresh tokens,
// provider account identifiers, or provider-native IDs.
//
//go:embed testdata/subscription/*.json
var credentialGatewayFixtures embed.FS

const credentialGatewayToken = "fixture-gateway-token"

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

func TestCredentialGatewayOneAuthRecoveryFixture(t *testing.T) {
	inner := newRecordingCredentialClient()
	recovering := &oneAuthRecoveryFixtureClient{inner: inner}
	fixture := newCredentialGatewayFixture(t, recovering)
	req := fixtureRequest(t, "/v1/messages", fixtureBytes(t, "testdata/subscription/anthropic-invoke.json"), context.Background())
	recording := httptest.NewRecorder()
	fixture.handler.ServeHTTP(recording, req)
	if recording.Code != http.StatusOK {
		t.Fatalf("recovered request status = %d, want 200; body: %s", recording.Code, recording.Body.String())
	}
	if got := recovering.recoveryCount(); got != 1 {
		t.Fatalf("auth recovery count = %d, want exactly one", got)
	}
	if got := recovering.wireAttemptCount(); got != 2 {
		t.Fatalf("auth recovery wire attempts = %d, want initial failure plus one recovery attempt", got)
	}
	if got := inner.callCount(); got != 1 {
		t.Fatalf("post-recovery fixture calls = %d, want one successful wire call", got)
	}
}

func TestCredentialGatewayQuotaPropagatesWithoutAlternateSelection(t *testing.T) {
	inner := newRecordingCredentialClient()
	inner.quota = true
	fixture := newCredentialGatewayFixture(t, inner)
	req := fixtureRequest(t, "/v1/messages", fixtureBytes(t, "testdata/subscription/anthropic-invoke.json"), context.Background())
	recording := httptest.NewRecorder()
	fixture.handler.ServeHTTP(recording, req)
	if recording.Code != http.StatusTooManyRequests {
		t.Fatalf("quota status = %d, want 429; body: %s", recording.Code, recording.Body.String())
	}
	if !strings.Contains(recording.Body.String(), "rate_limit_error") && !strings.Contains(recording.Body.String(), "quota") {
		t.Errorf("quota response does not identify a quota/rate-limit failure: %s", recording.Body.String())
	}
	if got := inner.callCount(); got != 1 {
		t.Fatalf("configured deployment call count = %d, want one; quota must not route elsewhere", got)
	}
	if got := inner.alternateCallCount(); got != 0 {
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

func TestCredentialGatewayChildEnvironmentIsolation(t *testing.T) {
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

	cmd := exec.Command(os.Args[0], "-test.run=^TestCredentialGatewayChildEnvironmentIsolation$")
	// An explicit environment models the gateway-backed ACP child contract:
	// only loopback gateway authority crosses the process boundary.
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
	altCalls      int
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

func (c *recordingCredentialClient) alternateCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.altCalls
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

type oneAuthRecoveryFixtureClient struct {
	inner    *recordingCredentialClient
	mu       sync.Mutex
	used     bool
	count    int
	attempts int
}

func (c *oneAuthRecoveryFixtureClient) Invoke(ctx context.Context, req inference.Request) (*inference.Response, error) {
	c.mu.Lock()
	first := !c.used
	c.attempts++
	if first {
		c.used = true
	}
	c.mu.Unlock()
	if first {
		err := failure.NewAPIError(http.StatusUnauthorized, "invalid_api_key", "fixture-auth", 0)
		if recoverableFixtureAuthFailure(err) {
			c.mu.Lock()
			c.count++
			c.attempts++
			c.mu.Unlock()
			// This provider-neutral fixture stands in for the llm credential
			// adapter: one recoverable auth rejection permits exactly one
			// reacquire/send attempt before the gateway sees success.
			return c.inner.Invoke(ctx, req)
		}
		return nil, err
	}
	return c.inner.Invoke(ctx, req)
}

func (c *oneAuthRecoveryFixtureClient) Stream(ctx context.Context, req inference.Request) (*stream.StreamReader[content.Chunk], error) {
	return c.inner.Stream(ctx, req)
}

func (c *oneAuthRecoveryFixtureClient) recoveryCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func (c *oneAuthRecoveryFixtureClient) wireAttemptCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attempts
}

func recoverableFixtureAuthFailure(err error) bool {
	var apiErr *failure.APIError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusUnauthorized && apiErr.Code == "invalid_api_key"
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
var _ inference.Client = (*oneAuthRecoveryFixtureClient)(nil)
