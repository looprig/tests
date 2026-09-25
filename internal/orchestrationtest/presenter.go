//go:build integration

package orchestrationtest

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/looprig/core/content"
	"github.com/looprig/harness/pkg/present"
)

// CountingPresenter renders a deterministic source label for stamped inputs
// and counts calls by the original message text. Unstamped inputs get no frame.
type CountingPresenter struct {
	mu    sync.Mutex
	calls map[string]int
}

func NewCountingPresenter() *CountingPresenter {
	return &CountingPresenter{calls: map[string]int{}}
}

func (p *CountingPresenter) Present(_ context.Context, in present.Input) (present.Frame, error) {
	p.mu.Lock()
	p.calls[presentedText(in.Blocks)]++
	p.mu.Unlock()
	if in.Principal == nil {
		return present.Frame{}, nil
	}
	return present.Frame{Prefix: []content.Block{&content.TextBlock{
		Text: fmt.Sprintf("[from: %s · space: %s]", in.Principal.Subject, in.Metadata["space"]),
	}}}, nil
}

func (p *CountingPresenter) Count(text string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[text]
}

func presentedText(blocks []content.Block) string {
	var parts []string
	for _, block := range blocks {
		if text, ok := block.(*content.TextBlock); ok {
			parts = append(parts, text.Text)
		}
	}
	return strings.Join(parts, "\n")
}
