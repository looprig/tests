//go:build integration && (darwin || (linux && !android))

package tests

import (
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"
)

func TestForeignloopAcceptedOrderRejectsSwappedQueuedRequests(t *testing.T) {
	loopID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	bID := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	cID := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	otherID := uuid.MustParse("44444444-4444-4444-8444-444444444444")

	for name, acceptedIDs := range map[string][]uuid.UUID{
		"swapped":    {cID, bID},
		"mismatched": {bID, otherID},
	} {
		t.Run(name, func(t *testing.T) {
			events := []event.Event{
				foreignloopCancelledEvent(loopID, bID, "B"),
				foreignloopCancelledEvent(loopID, cID, "C"),
				foreignloopAcceptedEvent(loopID, acceptedIDs[0]),
				foreignloopAcceptedEvent(loopID, acceptedIDs[1]),
			}

			if err := foreignloopAcceptedOrderError(events, loopID, bID.String(), cID.String()); err == nil {
				t.Fatal("invalid DelegateRequestAccepted IDs passed the queued FIFO assertion")
			}
		})
	}
}

func foreignloopCancelledEvent(loopID, commandID uuid.UUID, message string) event.InputCancelled {
	return event.InputCancelled{
		Header: event.Header{
			Coordinates: identity.Coordinates{LoopID: loopID},
			Cause:       identity.Cause{CommandID: commandID},
		},
		Reason: event.CancelTurnFailed,
		Message: &content.UserMessage{Message: content.Message{
			Role:   content.RoleUser,
			Blocks: []content.Block{&content.TextBlock{Text: message}},
		}},
	}
}

func foreignloopAcceptedEvent(loopID, commandID uuid.UUID) event.DelegateRequestAccepted {
	return event.DelegateRequestAccepted{Header: event.Header{
		Coordinates: identity.Coordinates{LoopID: loopID},
		Cause:       identity.Cause{CommandID: commandID},
	}}
}
