package tests

// This file adds a cross-module CI check to the guards already living
// alongside dependency_boundary_test.go and release_modfile_guard_test.go:
// a lightweight root-layout consistency check across every sibling
// repository in this ecosystem (Task 26 of the permission-classifier
// implementation plan, "Add repository root-layout check to cross-module
// CI").
//
// Scope, deliberately minimal: this checks only that each of the four
// sibling repositories (harness, classifiers, carbon, and this tests
// module itself) has the small set of top-level marker files every one of
// them ALREADY carries today (go.mod, Makefile, LICENSE, CONTRIBUTING.md) —
// the survey in Task 26 confirmed this exact set is the true common
// baseline (harness and carbon additionally carry CLAUDE.md/AGENTS.md,
// classifiers and carbon lack a top-level README.md, so those are NOT
// included). This does not duplicate classifiers' own
// internal/buildtest/layout_test.go (which checks that ONE repo's own
// internal source-tree shape, e.g. "no root .go files"/"module path is
// exact") or this module's own developmentModuleSourceViolations (which
// checks go.mod REPLACE directives point at the right sibling directory).
// It exists so a future consumer or CI runner that walks this repository
// collection can rely on every sibling exposing the same minimal
// discoverable structure, independent of whether that sibling happens to be
// a Go module dependency of this one (carbon is not: it consumes these
// modules as a downstream product and never appears in go.mod).
//
// carbon is checked by convention, not by go.mod REPLACE discovery (unlike
// harness/classifiers, this module has no dependency edge to carbon to
// walk): its accepted sibling directory names mirror the existing
// harness/harness-permission-classifier duality developmentModuleSources
// already tolerates, for the same reason (a permission-classifier feature
// branch checks out ../carbon-permission-classifier, not ../carbon).

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// rootLayoutMarkerFiles is the minimal set of top-level files every sibling
// repository in this ecosystem is expected to carry, per the Task 26
// survey. Keep this list to files ALL FOUR repos already have — anything
// narrower (e.g. README.md, CLAUDE.md) is not a true cross-repo invariant
// today and does not belong here.
var rootLayoutMarkerFiles = []string{"go.mod", "Makefile", "LICENSE", "CONTRIBUTING.md"}

// rootLayoutRepositories is the closed set of sibling repositories this
// check covers, in report order. directories lists every sibling directory
// name accepted for that repository (canonical name first), mirroring
// developmentModuleSourceViolations' existing harness/
// harness-permission-classifier duality.
var rootLayoutRepositories = []struct {
	label       string
	directories []string
}{
	{label: "harness", directories: []string{"harness", "harness-permission-classifier"}},
	{label: "classifiers", directories: []string{"classifiers"}},
	{label: "carbon", directories: []string{"carbon", "carbon-permission-classifier"}},
	{label: "tests", directories: []string{"tests", "tests-permission-classifier"}},
}

// siblingRootLayoutViolations checks every repository in rootLayoutRepositories
// against collectionRoot (the directory containing every sibling checkout,
// i.e. this module's own parent directory) and returns one human-readable
// violation string per problem found: a repository whose directory is
// missing entirely, or one that is missing a required marker file. A nil
// slice means every known repository has the full expected marker set.
func siblingRootLayoutViolations(collectionRoot string) ([]string, error) {
	var violations []string
	for _, repo := range rootLayoutRepositories {
		root, found, err := findFirstExistingDirectory(collectionRoot, repo.directories)
		if err != nil {
			return nil, err
		}
		if !found {
			violations = append(violations, repo.label+": no sibling directory found (tried "+strings.Join(repo.directories, ", ")+")")
			continue
		}
		for _, marker := range rootLayoutMarkerFiles {
			info, statErr := os.Stat(filepath.Join(root, marker))
			switch {
			case statErr == nil && info.IsDir():
				violations = append(violations, repo.label+": "+marker+" is a directory, want a file")
			case statErr != nil:
				violations = append(violations, repo.label+": missing root-layout marker "+marker)
			}
		}
	}
	sort.Strings(violations)
	return violations, nil
}

// findFirstExistingDirectory returns the first of directories (each
// resolved relative to collectionRoot) that exists and is itself a
// directory, along with whether any candidate matched.
func findFirstExistingDirectory(collectionRoot string, directories []string) (string, bool, error) {
	for _, directory := range directories {
		candidate := filepath.Join(collectionRoot, directory)
		info, err := os.Stat(candidate)
		if err == nil && info.IsDir() {
			return candidate, true, nil
		}
		if err != nil && !os.IsNotExist(err) {
			return "", false, err
		}
	}
	return "", false, nil
}

// writeRootLayoutFixture creates directory/name under collectionRoot and
// populates it with exactly the given marker file names (empty contents —
// this check only cares about presence, matching every real marker file's
// role here: a real go.mod's exact content is already covered by
// findModuleRoot/developmentModuleSourceViolations, not this check).
func writeRootLayoutFixture(t *testing.T, collectionRoot, directory string, markers []string) {
	t.Helper()
	root := filepath.Join(collectionRoot, directory)
	if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatal(err)
	}
	for _, marker := range markers {
		if err := os.WriteFile(filepath.Join(root, marker), []byte("placeholder\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func writeFullRootLayoutFixtureSet(t *testing.T, collectionRoot string) {
	t.Helper()
	writeRootLayoutFixture(t, collectionRoot, "harness-permission-classifier", rootLayoutMarkerFiles)
	writeRootLayoutFixture(t, collectionRoot, "classifiers", rootLayoutMarkerFiles)
	writeRootLayoutFixture(t, collectionRoot, "carbon-permission-classifier", rootLayoutMarkerFiles)
	writeRootLayoutFixture(t, collectionRoot, "tests-permission-classifier", rootLayoutMarkerFiles)
}

func TestSiblingRootLayoutAcceptsAFullMarkerSet(t *testing.T) {
	collectionRoot := t.TempDir()
	writeFullRootLayoutFixtureSet(t, collectionRoot)

	violations, err := siblingRootLayoutViolations(collectionRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("siblingRootLayoutViolations() = %v, want none", violations)
	}
}

func TestSiblingRootLayoutAcceptsCanonicalDirectoryNames(t *testing.T) {
	collectionRoot := t.TempDir()
	writeRootLayoutFixture(t, collectionRoot, "harness", rootLayoutMarkerFiles)
	writeRootLayoutFixture(t, collectionRoot, "classifiers", rootLayoutMarkerFiles)
	writeRootLayoutFixture(t, collectionRoot, "carbon", rootLayoutMarkerFiles)
	writeRootLayoutFixture(t, collectionRoot, "tests", rootLayoutMarkerFiles)

	violations, err := siblingRootLayoutViolations(collectionRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("siblingRootLayoutViolations() = %v, want none", violations)
	}
}

func TestSiblingRootLayoutReportsMissingMarkerFile(t *testing.T) {
	collectionRoot := t.TempDir()
	writeFullRootLayoutFixtureSet(t, collectionRoot)

	if err := os.Remove(filepath.Join(collectionRoot, "classifiers", "LICENSE")); err != nil {
		t.Fatal(err)
	}

	violations, err := siblingRootLayoutViolations(collectionRoot)
	if err != nil {
		t.Fatal(err)
	}
	want := "classifiers: missing root-layout marker LICENSE"
	found := false
	for _, violation := range violations {
		if violation == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("siblingRootLayoutViolations() = %v, want to contain %q", violations, want)
	}
}

func TestSiblingRootLayoutReportsMarkerFileThatIsActuallyADirectory(t *testing.T) {
	collectionRoot := t.TempDir()
	writeFullRootLayoutFixtureSet(t, collectionRoot)

	if err := os.Remove(filepath.Join(collectionRoot, "harness-permission-classifier", "Makefile")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(collectionRoot, "harness-permission-classifier", "Makefile"), 0o750); err != nil {
		t.Fatal(err)
	}

	violations, err := siblingRootLayoutViolations(collectionRoot)
	if err != nil {
		t.Fatal(err)
	}
	want := "harness: Makefile is a directory, want a file"
	found := false
	for _, violation := range violations {
		if violation == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("siblingRootLayoutViolations() = %v, want to contain %q", violations, want)
	}
}

func TestSiblingRootLayoutReportsMissingRepositoryDirectory(t *testing.T) {
	collectionRoot := t.TempDir()
	writeRootLayoutFixture(t, collectionRoot, "classifiers", rootLayoutMarkerFiles)
	writeRootLayoutFixture(t, collectionRoot, "carbon-permission-classifier", rootLayoutMarkerFiles)
	writeRootLayoutFixture(t, collectionRoot, "tests-permission-classifier", rootLayoutMarkerFiles)
	// harness deliberately absent.

	violations, err := siblingRootLayoutViolations(collectionRoot)
	if err != nil {
		t.Fatal(err)
	}
	want := "harness: no sibling directory found (tried harness, harness-permission-classifier)"
	found := false
	for _, violation := range violations {
		if violation == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("siblingRootLayoutViolations() = %v, want to contain %q", violations, want)
	}
}

// TestRepositoryRootLayoutMatchesEcosystemConvention is the live guard: it
// runs siblingRootLayoutViolations against the real, currently checked-out
// sibling repository collection (this module's own parent directory), the
// same way TestDevelopmentModuleSources runs its own violation-detection
// function against the real go.mod. A future repository that drops one of
// these marker files, or a collection missing one of the four repositories
// entirely, fails this test.
func TestRepositoryRootLayoutMatchesEcosystemConvention(t *testing.T) {
	moduleRoot, err := findModuleRoot(".", testsModulePath)
	if err != nil {
		t.Fatal(err)
	}
	collectionRoot := filepath.Dir(moduleRoot)

	violations, err := siblingRootLayoutViolations(collectionRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, violation := range violations {
		t.Error(violation)
	}
}
