package tests

import (
	"os"
	"strings"
	"testing"
)

// TestTheTestTargetsCannotReplayACachedResult is the reader the `-count=1`
// in this Makefile's `go test` recipes would otherwise have none of.
//
// A workspace-wide audit found `go test -race ./...` with no `-count=1` in
// every component's test recipe, this module included, and Factory measured
// the consequence directly: a case failing 8 of 14 clean runs sat behind
// `ok … (cached)` in a green `make check`, because `go test` REPLAYS a cached
// pass for any package whose inputs are unchanged. This module has FOUR such
// recipes rather than one -- `test` (the one `check` itself depends on),
// `dependency-boundary`, `root-layout` and `release-check` (the `-tags
// integration` one release-check shares with `test`) -- and a partial fix
// leaves the same hole open in whichever recipe is skipped: CI calls
// `dependency-boundary` and `root-layout` as their own gates, independent of
// `check`, so each needs the flag on its own line, not inherited from
// another target's fix. `live-network` already carries `-count=1` and is
// checked here too, so a future edit cannot silently drop it.
//
// No behavioural test can catch this, because what goes wrong is that a test
// is NOT EXECUTED, and the thing that would have reported it is the thing
// being skipped. The recipe is read from the Makefile rather than restated,
// so moving the flag or the command still passes and removing it does not.
func TestTheTestTargetsCannotReplayACachedResult(t *testing.T) {
	t.Parallel()

	for _, target := range []string{"test", "live-network", "dependency-boundary", "root-layout", "release-check"} {
		t.Run(target, func(t *testing.T) {
			recipe := makefileRecipe(t, target)
			if !strings.Contains(recipe, "go test") {
				t.Fatalf("the %q recipe does not run `go test`:\n%s", target, recipe)
			}
			if !strings.Contains(recipe, "-count=1") {
				t.Errorf("the %q recipe does not pass -count=1, so it may replay a cached pass instead of running the suite:\n%s", target, recipe)
			}
		})
	}
}

// makefileRecipe returns the command lines of one target.
//
// A recipe line is a line beginning with a TAB, which is make's own rule; the
// recipe ends at the first line that is neither a tab line nor blank. Reading
// the real file is the point -- a copy of the command here would agree with
// itself forever.
func makefileRecipe(t *testing.T, target string) string {
	t.Helper()

	content, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	lines := strings.Split(string(content), "\n")
	start := -1
	for i, line := range lines {
		if strings.HasPrefix(line, target+":") {
			start = i + 1
			break
		}
	}
	if start < 0 {
		t.Fatalf("the Makefile declares no target %q", target)
	}
	var recipe []string
	for _, line := range lines[start:] {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if !strings.HasPrefix(line, "\t") {
			break
		}
		recipe = append(recipe, line)
	}
	if len(recipe) == 0 {
		t.Fatalf("target %q has an empty recipe", target)
	}
	return strings.Join(recipe, "\n")
}
