//go:build integration

package orchestrationtest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/present"
	"github.com/looprig/host/department"
)

func TestCountingPresenterFramesOnlyStampedInputAndCountsEachPresentation(t *testing.T) {
	p := NewCountingPresenter()
	alex := &sessionwire.Principal{Tenant: PooledTenantA, Subject: "user_alex", Kind: sessionwire.PrincipalKindActor}
	blocks := []content.Block{&content.TextBlock{Text: "add milk"}}

	frame, err := p.Present(context.Background(), present.Input{
		Principal: alex, Metadata: sessionwire.MessageMetadata{"space": "family"}, Blocks: blocks,
	})
	if err != nil {
		t.Fatalf("Present: %v", err)
	}
	if len(frame.Prefix) != 1 || len(frame.Suffix) != 0 {
		t.Fatalf("frame = %+v, want one prefix block", frame)
	}
	if got := frame.Prefix[0].(*content.TextBlock).Text; got != "[from: user_alex · space: family]" {
		t.Fatalf("prefix = %q", got)
	}
	empty, err := p.Present(context.Background(), present.Input{Blocks: blocks})
	if err != nil || len(empty.Prefix)+len(empty.Suffix) != 0 {
		t.Fatalf("an unstamped input must get an empty frame, got %+v, %v", empty, err)
	}
	if got := p.Count("add milk"); got != 2 {
		t.Fatalf("Count(add milk) = %d, want 2", got)
	}
}

func TestPooledSessionCopiesPrincipalAndMetadataIntoTheAdmittedCommand(t *testing.T) {
	cmd := departmentCommandFixture(t)
	admitted, err := admittedFor(cmd, 7)
	if err != nil {
		t.Fatalf("admittedFor: %v", err)
	}
	if admitted.Principal == nil || *admitted.Principal != *cmd.Principal {
		t.Fatalf("principal dropped: %+v", admitted.Principal)
	}
	if admitted.Metadata["space"] != "family" {
		t.Fatalf("metadata dropped: %+v", admitted.Metadata)
	}
}

func departmentCommandFixture(t *testing.T) department.RuntimeCommand {
	t.Helper()
	request := sessionwire.InputRequest{
		CommandEnvelope: PooledEnvelope("cmd-1"),
		SessionID:       "session-1",
		Blocks:          json.RawMessage(`[{"type":"text","text":"add milk"}]`),
	}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return department.RuntimeCommand{
		Kind:             PooledKindInput,
		CommandID:        "cmd-1",
		RuntimeCommandID: uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		AttemptID:        "attempt-1",
		Payload:          payload,
		Principal:        &sessionwire.Principal{Tenant: PooledTenantA, Subject: "user_alex", Kind: sessionwire.PrincipalKindActor},
		Metadata:         sessionwire.MessageMetadata{"space": "family"},
	}
}
