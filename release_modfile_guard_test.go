package tests_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCanonicalGoModHasNoLocalReplacements(t *testing.T) {
	t.Parallel()

	contents, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read canonical go.mod: %v", err)
	}
	for _, line := range strings.Split(string(contents), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "replace ") || trimmed == "replace" || trimmed == "replace (" {
			t.Fatalf("canonical go.mod still contains a replace directive: %q", trimmed)
		}
	}
}

func TestReleaseCheckUsesCanonicalGoMod(t *testing.T) {
	t.Parallel()

	contents, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	makefile := string(contents)
	if strings.Contains(makefile, "RELEASE_MODFILE") {
		t.Fatal("release-check must not accept an alternate release modfile")
	}
	if !strings.Contains(makefile, `scripts/check-release-modfile.sh go.mod`) {
		t.Fatal("release-check must validate the canonical go.mod")
	}
}

func TestCanonicalModuleGuardRejectsLocalReplacements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		modfile    string
		wantErr    string
		notWantErr string
	}{
		{
			name: "tagged release modules",
			modfile: `module github.com/looprig/tests

go 1.26.4

require (
	github.com/looprig/harness v0.13.0
	github.com/looprig/foreignloops v0.1.0
)
`,
		},
		{
			name: "versioned module replacement",
			modfile: `module github.com/looprig/tests

go 1.26.4

replace github.com/looprig/harness v0.13.0 => example.com/harness v0.13.1
`,
		},
		{
			name: "relative local replacement",
			modfile: `module github.com/looprig/tests

go 1.26.4

replace github.com/looprig/harness => ../harness
`,
			wantErr: "local filesystem replacement",
		},
		{
			name: "absolute local replacement in block",
			modfile: `module github.com/looprig/tests

go 1.26.4

replace (
	github.com/looprig/foreignloops => /private/tmp/foreignloop
)
`,
			wantErr:    "local filesystem replacement",
			notWantErr: "unterminated replace block",
		},
		{
			name: "file URL replacement",
			modfile: `module github.com/looprig/tests

go 1.26.4

replace github.com/looprig/harness => file:///private/tmp/harness
`,
			wantErr: "local filesystem replacement",
		},
		{
			name: "unversioned replacement target",
			modfile: `module github.com/looprig/tests

go 1.26.4

replace github.com/looprig/foreignloops => local-foreignloop
`,
			wantErr: "local filesystem replacement",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			modfile := filepath.Join(t.TempDir(), "go.mod")
			if err := os.WriteFile(modfile, []byte(tt.modfile), 0o600); err != nil {
				t.Fatalf("write temporary modfile: %v", err)
			}

			cmd := exec.Command("sh", "scripts/check-release-modfile.sh", modfile)
			output, err := cmd.CombinedOutput()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("guard rejected canonical module file: %v\n%s", err, output)
				}
				return
			}

			if err == nil {
				t.Fatalf("guard accepted forbidden modfile")
			}
			if !strings.Contains(string(output), tt.wantErr) {
				t.Fatalf("guard output %q does not contain %q", output, tt.wantErr)
			}
			if tt.notWantErr != "" && strings.Contains(string(output), tt.notWantErr) {
				t.Fatalf("guard output %q contains misleading error %q", output, tt.notWantErr)
			}
		})
	}
}
