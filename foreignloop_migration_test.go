//go:build migration

package tests

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloop/driver/claude"
	legacy "github.com/looprig/harness/pkg/foreignloop"
)

func TestForeignloopTranscriptParity(t *testing.T) {
	t.Parallel()

	fixtures := map[string]string{
		"empty": "",
		"happy": `{"type":"user","message":{"content":"hi there"}}
{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"let me think","signature":"sig"},{"type":"text","text":"Working"},{"type":"tool_use","id":"toolu_9","name":"Read","input":{"path":"/x"}}]}}
{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_9","content":"contents"}]}}
{"type":"assistant","message":{"content":[{"type":"text","text":"Done"}]}}
`,
		"soft-degradation": `{"type":"user","message":{"content":"hi"}}
{"type":"assistant","isSidechain":true,"message":{"content":[{"type":"text","text":"skip"}]}}
not-json
{"type":"progress","message":{"content":"ignore"}}
{"type":"assistant","message":{"content":[{"type":"text","text":"recovered"}]}}
`,
	}

	for name, fixture := range fixtures {
		name, fixture := name, fixture
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := writeForeignloopTranscriptFixture(t, fixture)

			oldSteps, err := legacy.DecodeTranscriptForMigration(path)
			if err != nil {
				t.Fatalf("legacy decoder: %v", err)
			}
			newSteps, err := claude.DecodeTranscriptForMigration(path)
			if err != nil {
				t.Fatalf("extracted decoder: %v", err)
			}

			oldJSON := canonicalForeignloopJSON(t, oldSteps)
			newJSON := canonicalForeignloopJSON(t, newSteps)
			if !reflect.DeepEqual(newSteps, oldSteps) {
				t.Errorf("complete decoded values differ\nlegacy: %#v\nextracted: %#v", oldSteps, newSteps)
			}
			if !bytes.Equal(newJSON, oldJSON) {
				t.Errorf("canonical JSON differs\nlegacy:\n%s\nextracted:\n%s", oldJSON, newJSON)
			}
		})
	}
}

func writeForeignloopTranscriptFixture(t *testing.T, fixture string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "transcript.jsonl")
	if err := os.WriteFile(path, []byte(fixture), 0o600); err != nil {
		t.Fatalf("write transcript fixture: %v", err)
	}
	return path
}

func canonicalForeignloopJSON(t *testing.T, value []content.AgenticMessages) []byte {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("marshal canonical JSON: %v", err)
	}
	return append(data, '\n')
}
