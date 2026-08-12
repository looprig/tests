//go:build integration

package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/eval"
	"github.com/looprig/eval/evaltest"
	"github.com/looprig/eval/exact"
	"github.com/looprig/eval/judge"
	"github.com/looprig/eval/rubric"
	"github.com/looprig/inference"
	"github.com/looprig/inference/model"
	"github.com/looprig/inference/stream"
)

// This integration proof shows that the reusable Eval module can express the
// deterministic Contains and model Judge examples formerly owned by Harness.
// It belongs here because neither Harness production code nor Harness tests need
// to import Eval to prove a cross-module migration contract.

const evalMigrationRevision eval.Revision = "v1"

func evalMigrationUserText(value string) *content.UserMessage {
	return &content.UserMessage{Message: content.Message{
		Role:   content.RoleUser,
		Blocks: []content.Block{&content.TextBlock{Text: value}},
	}}
}

func evalMigrationAIText(value string) *content.AIMessage {
	return &content.AIMessage{Message: content.Message{
		Role:   content.RoleAssistant,
		Blocks: []content.Block{&content.TextBlock{Text: value}},
	}}
}

type evalMigrationTarget struct {
	answer string
}

func (evalMigrationTarget) Name() string { return "migration-fake-agent" }

func (target evalMigrationTarget) Observe(_ context.Context, scenario eval.Scenario) (eval.Observation, error) {
	conversation := make(content.AgenticMessages, 0, len(scenario.Input)+1)
	conversation = append(conversation, scenario.Input...)
	conversation = append(conversation, evalMigrationAIText(target.answer))
	return eval.Observation{
		Conversation: conversation,
		Scope:        eval.ScopeCase,
		Subject: eval.Subject{
			ID:       "agent-under-eval",
			Kind:     eval.SubjectAgent,
			Name:     scenario.Name,
			Revision: scenario.Revision,
		},
	}, nil
}

type evalMigrationJudgeClient struct {
	scoreJSON string
}

func (client evalMigrationJudgeClient) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return &inference.Response{
		Message:      evalMigrationAIText(client.scoreJSON),
		Usage:        &content.Usage{InputTokens: 32, OutputTokens: 8},
		Model:        "migration-judge-model",
		FinishReason: stream.FinishReasonStop,
	}, nil
}

func (evalMigrationJudgeClient) Stream(context.Context, inference.Request) (*stream.StreamReader[content.Chunk], error) {
	return nil, errors.New("stream is not used by the rubric judge")
}

func evalMigrationStructuredModel() model.Model {
	return model.CustomModel(
		"test",
		"test",
		"",
		"migration-judge-model",
		model.WithStructuredOutput(),
	)
}

func TestEvalMigrationContainsAndJudge(t *testing.T) {
	t.Parallel()

	scenario := eval.Scenario{
		ID:       "capital-of-france",
		Name:     "migration-agent",
		Revision: evalMigrationRevision,
		Input:    content.AgenticMessages{evalMigrationUserText("What is the capital of France?")},
	}
	target := evalMigrationTarget{answer: "The capital of France is Paris."}
	contains := exact.RequiredText("Paris")
	judgeClient := evalMigrationJudgeClient{
		scoreJSON: `{"score":0.9,"reason":"the response directly answers the question","evidence":[]}`,
	}
	relevance := judge.New(
		rubric.AnswerRelevanceV1,
		judgeClient,
		inference.Request{Model: evalMigrationStructuredModel()},
	)

	report := evaltest.RunScenario(t, scenario, target, contains, relevance)
	evaltest.RequirePass(t, report)
	if len(report.Samples) != 1 {
		t.Fatalf("got %d samples, want 1", len(report.Samples))
	}
	if got := len(report.Samples[0].Assessments); got != 2 {
		t.Fatalf("got %d assessments, want 2", got)
	}
}
