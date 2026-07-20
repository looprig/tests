package tests_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseModfileGuard(t *testing.T) {
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

			modfile := filepath.Join(t.TempDir(), "go.release.mod")
			if err := os.WriteFile(modfile, []byte(tt.modfile), 0o600); err != nil {
				t.Fatalf("write temporary modfile: %v", err)
			}

			cmd := exec.Command("sh", "scripts/check-release-modfile.sh", modfile)
			output, err := cmd.CombinedOutput()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("guard rejected release modfile: %v\n%s", err, output)
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

func TestReleaseCheckTargetFailsClosedBeforeGo(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	marker := filepath.Join(tempDir, "go-invoked")
	fakeGo := filepath.Join(tempDir, "fake-go")
	fakeGoBody := "#!/bin/sh\nprintf '%s\\n' \"$*\" > \"$RELEASE_GO_MARKER\"\n"
	if err := os.WriteFile(fakeGo, []byte(fakeGoBody), 0o700); err != nil {
		t.Fatalf("write fake Go command: %v", err)
	}

	run := func(t *testing.T, modfile string) ([]byte, error) {
		t.Helper()
		cmd := exec.Command("make", "release-check", "RELEASE_MODFILE="+modfile, "RELEASE_GO="+fakeGo)
		cmd.Env = append(os.Environ(), "RELEASE_GO_MARKER="+marker)
		return cmd.CombinedOutput()
	}

	t.Run("absent modfile", func(t *testing.T) {
		modfile := filepath.Join(tempDir, "absent.mod")
		output, err := run(t, modfile)
		if err == nil {
			t.Fatal("release-check accepted an absent release modfile")
		}
		if !strings.Contains(string(output), "not prepared") {
			t.Fatalf("release-check output %q does not explain absent modfile", output)
		}
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Fatalf("Go command invoked before absent-modfile check: %v", err)
		}
	})

	t.Run("local replace", func(t *testing.T) {
		modfile := filepath.Join(tempDir, "local.mod")
		contents := "module github.com/looprig/tests\n\ngo 1.26.4\n\nreplace github.com/looprig/harness => ../harness\n"
		if err := os.WriteFile(modfile, []byte(contents), 0o600); err != nil {
			t.Fatalf("write local-replace modfile: %v", err)
		}
		output, err := run(t, modfile)
		if err == nil {
			t.Fatal("release-check accepted a local replacement")
		}
		if !strings.Contains(string(output), "local filesystem replacement") {
			t.Fatalf("release-check output %q does not explain local replacement", output)
		}
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Fatalf("Go command invoked before local-replace check: %v", err)
		}
	})

	t.Run("tagged release modfile", func(t *testing.T) {
		modfile := filepath.Join(tempDir, "tagged.mod")
		contents := "module github.com/looprig/tests\n\ngo 1.26.4\n\nrequire github.com/looprig/harness v0.13.0\n"
		if err := os.WriteFile(modfile, []byte(contents), 0o600); err != nil {
			t.Fatalf("write tagged modfile: %v", err)
		}
		output, err := run(t, modfile)
		if err != nil {
			t.Fatalf("release-check rejected tagged modfile: %v\n%s", err, output)
		}
		invocation, err := os.ReadFile(marker)
		if err != nil {
			t.Fatalf("read Go invocation marker: %v", err)
		}
		if !strings.Contains(string(invocation), "test -modfile="+modfile) {
			t.Fatalf("Go invocation %q did not use release modfile", invocation)
		}
	})
}
