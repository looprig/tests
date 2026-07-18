package tests

import (
	"bufio"
	"errors"
	"fmt"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	testsModulePath       = "github.com/looprig/tests"
	harnessModulePath     = "github.com/looprig/harness"
	foreignloopModulePath = "github.com/looprig/foreignloop"
)

func TestCrossModuleOwnershipScannerRejectsOnlyDualIntegrationOwners(t *testing.T) {
	root := t.TempDir()
	writeModuleFixture(t, root, "tests", "module github.com/looprig/tests\n", map[string]string{
		"integration_test.go": "package tests\nimport (\n_ \"github.com/looprig/harness/pkg/rig\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n",
	})
	writeModuleFixture(t, root, "product", `module github.com/example/product

require (
	github.com/looprig/harness v0.0.0
	github.com/looprig/foreignloop v0.0.0
)

replace github.com/looprig/harness => ../harness
replace github.com/looprig/foreignloop => ../foreignloop
`, map[string]string{
		"main.go":            "package product\nimport (\n_ \"github.com/looprig/harness/pkg/rig\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n",
		"not_integration.go": "//go:build !integration\n\npackage product\nimport (\n_ \"github.com/looprig/harness/pkg/rig\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n",
	})
	writeModuleFixture(t, root, "bad-test", "module github.com/example/bad-test\n", map[string]string{
		"nested/owner_test.go": "package nested\nimport (\n_ \"github.com/looprig/harness/pkg/session\"\n_ \"github.com/looprig/foreignloop/driver/claude\"\n)\n",
	})
	writeModuleFixture(t, root, "bad-integration-tag", "module github.com/example/bad-integration-tag\n", map[string]string{
		"nested/owner.go": "//go:build plan9 && integration\n\npackage nested\nimport (\n_ \"github.com/looprig/harness/pkg/session\"\n_ \"github.com/looprig/foreignloop/driver/claude\"\n)\n",
	})
	writeModuleFixture(t, root, "bad-migration-tag", "module github.com/example/bad-migration-tag\n", map[string]string{
		"owner.go": "//go:build migration\n\npackage owner\nimport (\n_ \"github.com/looprig/harness/pkg/session\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n",
	})
	writeModuleFixture(t, root, "bad-split-tests", "module github.com/example/bad-split-tests\n", map[string]string{
		"harness_test.go":     "package owner\nimport _ \"github.com/looprig/harness/pkg/session\"\n",
		"foreignloop_test.go": "package owner\nimport _ \"github.com/looprig/foreignloop/backend\"\n",
	})

	violations, err := crossModuleOwnershipViolations(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"github.com/example/bad-integration-tag",
		"github.com/example/bad-migration-tag",
		"github.com/example/bad-split-tests",
		"github.com/example/bad-test",
	}
	if strings.Join(violations, "\n") != strings.Join(want, "\n") {
		t.Fatalf("crossModuleOwnershipViolations() = %v, want %v", violations, want)
	}
}

func TestCrossModuleOwnershipScannerSkipsNestedModulesAndSimilarNames(t *testing.T) {
	root := t.TempDir()
	writeModuleFixture(t, root, "consumer", `module github.com/example/consumer

require github.com/looprig/harnessed v0.0.0
`, map[string]string{
		"consumer.go":   "package consumer\nimport _ \"github.com/looprig/foreignloopish/backend\"\n",
		"nested/go.mod": "module github.com/example/nested\n",
		"nested/bad.go": "package nested\nimport (\n_ \"github.com/looprig/harness/pkg/rig\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n",
		"vendor/bad.go": "package vendor\nimport (\n_ \"github.com/looprig/harness/pkg/rig\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n",
	})

	violations, err := crossModuleOwnershipViolations(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("crossModuleOwnershipViolations() = %v, want none", violations)
	}
}

func TestCrossModuleOwnershipScannerFollowsImmediateSiblingModuleSymlink(t *testing.T) {
	collectionRoot := t.TempDir()
	targetRoot := t.TempDir()
	writeModuleFixture(t, targetRoot, "linked-module", "module github.com/example/linked\n", map[string]string{
		"ownership_test.go": "package linked\nimport (\n_ \"github.com/looprig/harness/pkg/rig\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n",
	})
	if err := os.Symlink(filepath.Join(targetRoot, "linked-module"), filepath.Join(collectionRoot, "linked")); err != nil {
		t.Fatal(err)
	}

	violations, err := crossModuleOwnershipViolations(collectionRoot)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"github.com/example/linked"}
	if strings.Join(violations, "\n") != strings.Join(want, "\n") {
		t.Fatalf("crossModuleOwnershipViolations() = %v, want %v", violations, want)
	}
}

func TestCrossModuleOwnershipScannerDeduplicatesRealAndSymlinkRoots(t *testing.T) {
	collectionRoot := t.TempDir()
	writeModuleFixture(t, collectionRoot, "real", "module github.com/example/real\n", map[string]string{
		"ownership_test.go": "package real\nimport (\n_ \"github.com/looprig/harness/pkg/rig\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n",
	})
	if err := os.Symlink(filepath.Join(collectionRoot, "real"), filepath.Join(collectionRoot, "alias")); err != nil {
		t.Fatal(err)
	}

	violations, err := crossModuleOwnershipViolations(collectionRoot)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"github.com/example/real"}
	if strings.Join(violations, "\n") != strings.Join(want, "\n") {
		t.Fatalf("crossModuleOwnershipViolations() = %v, want one canonical scan %v", violations, want)
	}
}

func TestCrossModuleOwnershipScannerRejectsBrokenImmediateSiblingSymlink(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{
			name: "dangling",
			setup: func(t *testing.T, root string) {
				t.Helper()
				if err := os.Symlink(filepath.Join(root, "missing"), filepath.Join(root, "linked")); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "cycle",
			setup: func(t *testing.T, root string) {
				t.Helper()
				if err := os.Symlink(filepath.Join(root, "second"), filepath.Join(root, "first")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(root, "first"), filepath.Join(root, "second")); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			tt.setup(t, root)
			_, err := crossModuleOwnershipViolations(root)
			if err == nil || !strings.Contains(err.Error(), "resolve sibling module symlink") {
				t.Fatalf("crossModuleOwnershipViolations() error = %v, want clear sibling symlink resolution failure", err)
			}
		})
	}
}

func TestCrossModuleOwnershipScannerRejectsOwnedGoSourceSymlinks(t *testing.T) {
	collectionRoot := t.TempDir()
	writeModuleFixture(t, collectionRoot, "test-link", "module github.com/example/test-link\n", nil)
	writeModuleFixture(t, collectionRoot, "integration-link", "module github.com/example/integration-link\n", nil)
	writeModuleFixture(t, collectionRoot, "migration-link", "module github.com/example/migration-link\n", nil)
	externalRoot := t.TempDir()
	testSource := filepath.Join(externalRoot, "external-test-source")
	if err := os.WriteFile(testSource, []byte("package external\nimport (\n_ \"github.com/looprig/harness/pkg/rig\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	integrationSource := filepath.Join(externalRoot, "external-integration-source")
	if err := os.WriteFile(integrationSource, []byte("//go:build plan9 && integration\n\npackage external\nimport (\n_ \"github.com/looprig/harness/pkg/rig\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	migrationSource := filepath.Join(externalRoot, "external-migration-source")
	if err := os.WriteFile(migrationSource, []byte("//go:build migration\n\npackage external\nimport (\n_ \"github.com/looprig/harness/pkg/rig\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(testSource, filepath.Join(collectionRoot, "test-link", "linked_test.go")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(integrationSource, filepath.Join(collectionRoot, "integration-link", "linked.go")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(migrationSource, filepath.Join(collectionRoot, "migration-link", "linked.go")); err != nil {
		t.Fatal(err)
	}

	violations, err := crossModuleOwnershipViolations(collectionRoot)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"github.com/example/integration-link",
		"github.com/example/migration-link",
		"github.com/example/test-link",
	}
	if strings.Join(violations, "\n") != strings.Join(want, "\n") {
		t.Fatalf("crossModuleOwnershipViolations() = %v, want %v", violations, want)
	}
}

func TestCrossModuleOwnershipScannerRejectsBrokenGoSourceSymlinks(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{
			name: "dangling",
			setup: func(t *testing.T, root string) {
				t.Helper()
				if err := os.Symlink(filepath.Join(root, "missing"), filepath.Join(root, "linked_test.go")); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "cycle",
			setup: func(t *testing.T, root string) {
				t.Helper()
				if err := os.Symlink(filepath.Join(root, "second.go"), filepath.Join(root, "first.go")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(root, "first.go"), filepath.Join(root, "second.go")); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collectionRoot := t.TempDir()
			writeModuleFixture(t, collectionRoot, "module", "module github.com/example/module\n", nil)
			tt.setup(t, filepath.Join(collectionRoot, "module"))
			_, err := crossModuleOwnershipViolations(collectionRoot)
			if err == nil || !strings.Contains(err.Error(), "resolve Go source symlink") {
				t.Fatalf("crossModuleOwnershipViolations() error = %v, want clear Go source symlink resolution failure", err)
			}
		})
	}
}

func TestCrossModuleOwnershipScannerDoesNotTraverseSymlinkedDirectories(t *testing.T) {
	collectionRoot := t.TempDir()
	writeModuleFixture(t, collectionRoot, "module", "module github.com/example/module\n", nil)
	externalRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(externalRoot, "ownership_test.go"), []byte("package external\nimport (\n_ \"github.com/looprig/harness/pkg/rig\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	moduleRoot := filepath.Join(collectionRoot, "module")
	if err := os.Symlink(externalRoot, filepath.Join(moduleRoot, "linked")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(externalRoot, "ownership_test.go"), filepath.Join(moduleRoot, "ignored.txt")); err != nil {
		t.Fatal(err)
	}

	violations, err := crossModuleOwnershipViolations(collectionRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("crossModuleOwnershipViolations() = %v, want directory and non-Go symlinks ignored", violations)
	}
}

func TestCrossModuleOwnershipBoundary(t *testing.T) {
	moduleRoot, err := findModuleRoot(".", testsModulePath)
	if err != nil {
		t.Fatal(err)
	}
	violations, err := crossModuleOwnershipViolations(filepath.Dir(moduleRoot))
	if err != nil {
		t.Fatal(err)
	}
	for _, violation := range violations {
		t.Errorf("module %s imports both Harness and foreignloop; cross-module integration belongs only to %s", violation, testsModulePath)
	}
}

func TestDevelopmentModuleSourcesAcceptSiblingLayouts(t *testing.T) {
	collectionRoot := t.TempDir()
	modules := map[string]string{
		"core":        "github.com/looprig/core",
		"foreignloop": "github.com/looprig/foreignloop",
		"fsstore":     "github.com/looprig/fsstore",
		"harness":     "github.com/looprig/harness",
		"inference":   "github.com/looprig/inference",
		"mcp":         "github.com/looprig/mcp",
		"storage":     "github.com/looprig/storage",
	}
	for directory, modulePath := range modules {
		writeModuleFixture(t, collectionRoot, directory, "module "+modulePath+"\n", nil)
	}
	writeModuleFixture(t, collectionRoot, "tests", `module github.com/looprig/tests

replace (
	github.com/looprig/core => ../core
	github.com/looprig/harness => ../harness
	github.com/looprig/foreignloop => ../foreignloop
	github.com/looprig/fsstore => ../fsstore
	github.com/looprig/inference => ../inference
	github.com/looprig/mcp => ../mcp
	github.com/looprig/storage => ../storage
)
`, nil)

	violations, err := developmentModuleSourceViolations(filepath.Join(collectionRoot, "tests"))
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("developmentModuleSourceViolations() = %v, want none", violations)
	}
}

func TestDevelopmentModuleSourcesRejectWrongAndMissingLocalSources(t *testing.T) {
	collectionRoot := t.TempDir()
	writeModuleFixture(t, collectionRoot, "harness", "module github.com/example/wrong\n", nil)
	writeModuleFixture(t, collectionRoot, "foreignloop", "module github.com/looprig/foreignloop\n", nil)
	writeModuleFixture(t, collectionRoot, "tests", `module github.com/looprig/tests

replace github.com/looprig/harness => ../harness
replace github.com/looprig/foreignloop => ../../deep/foreignloop
`, nil)

	violations, err := developmentModuleSourceViolations(filepath.Join(collectionRoot, "tests"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"github.com/looprig/core has no local development replacement",
		"github.com/looprig/foreignloop replacement must use ../foreignloop, got ../../deep/foreignloop",
		"github.com/looprig/fsstore has no local development replacement",
		"github.com/looprig/harness replacement resolves to module github.com/example/wrong",
		"github.com/looprig/inference has no local development replacement",
		"github.com/looprig/mcp has no local development replacement",
		"github.com/looprig/storage has no local development replacement",
	}
	if strings.Join(violations, "\n") != strings.Join(want, "\n") {
		t.Fatalf("developmentModuleSourceViolations() = %v, want %v", violations, want)
	}
}

func TestDevelopmentModuleSources(t *testing.T) {
	moduleRoot, err := findModuleRoot(".", testsModulePath)
	if err != nil {
		t.Fatal(err)
	}
	violations, err := developmentModuleSourceViolations(moduleRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, violation := range violations {
		t.Error(violation)
	}
}

type integrationSubjects struct {
	harness     bool
	foreignloop bool
}

func crossModuleOwnershipViolations(collectionRoot string) ([]string, error) {
	moduleRoots, err := siblingModuleRoots(collectionRoot)
	if err != nil {
		return nil, err
	}
	var violations []string
	for _, moduleRoot := range moduleRoots {
		goModPath := filepath.Join(moduleRoot, "go.mod")
		goMod, err := os.ReadFile(goModPath)
		if err != nil {
			return nil, fmt.Errorf("read sibling manifest %s: %w", goModPath, err)
		}
		modulePath, _, err := manifestIntegrationSubjects(goMod)
		if err != nil {
			return nil, fmt.Errorf("parse sibling manifest %s: %w", goModPath, err)
		}
		subjects, err := integrationTestSubjects(moduleRoot, modulePath)
		if err != nil {
			return nil, fmt.Errorf("scan sibling module %s: %w", modulePath, err)
		}
		if modulePath != testsModulePath && subjects.harness && subjects.foreignloop {
			violations = append(violations, modulePath)
		}
	}
	sort.Strings(violations)
	return violations, nil
}

func siblingModuleRoots(collectionRoot string) ([]string, error) {
	entries, err := os.ReadDir(collectionRoot)
	if err != nil {
		return nil, fmt.Errorf("read sibling module collection %s: %w", collectionRoot, err)
	}
	seen := make(map[string]struct{})
	var roots []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		candidate := filepath.Join(collectionRoot, entry.Name())
		isSymlink := entry.Type()&os.ModeSymlink != 0
		if !entry.IsDir() && !isSymlink {
			continue
		}
		moduleRoot := candidate
		if isSymlink {
			moduleRoot, err = filepath.EvalSymlinks(candidate)
			if err != nil {
				return nil, fmt.Errorf("resolve sibling module symlink %s: %w", candidate, err)
			}
			info, err := os.Stat(moduleRoot)
			if err != nil {
				return nil, fmt.Errorf("inspect sibling module symlink %s: %w", candidate, err)
			}
			if !info.IsDir() {
				return nil, fmt.Errorf("inspect sibling module symlink %s: target is not a directory", candidate)
			}
		}
		_, err := os.Stat(filepath.Join(moduleRoot, "go.mod"))
		switch {
		case err == nil:
		case errors.Is(err, os.ErrNotExist):
			if isSymlink {
				return nil, fmt.Errorf("inspect sibling module symlink %s: target has no go.mod", candidate)
			}
			continue
		default:
			if isSymlink {
				return nil, fmt.Errorf("inspect sibling module symlink %s: %w", candidate, err)
			}
			return nil, fmt.Errorf("inspect sibling module %s: %w", candidate, err)
		}
		canonical, err := filepath.EvalSymlinks(moduleRoot)
		if err != nil {
			return nil, fmt.Errorf("canonicalize sibling module root %s: %w", moduleRoot, err)
		}
		canonical, err = filepath.Abs(canonical)
		if err != nil {
			return nil, fmt.Errorf("canonicalize sibling module root %s: %w", moduleRoot, err)
		}
		canonical = filepath.Clean(canonical)
		if _, ok := seen[canonical]; ok {
			continue
		}
		seen[canonical] = struct{}{}
		roots = append(roots, canonical)
	}
	sort.Strings(roots)
	return roots, nil
}

func developmentModuleSourceViolations(moduleRoot string) ([]string, error) {
	contents, err := os.ReadFile(filepath.Join(moduleRoot, "go.mod"))
	if err != nil {
		return nil, fmt.Errorf("read development manifest: %w", err)
	}
	replacements, err := localReplacementTargets(contents)
	if err != nil {
		return nil, fmt.Errorf("parse development replacements: %w", err)
	}
	developmentModules := []struct {
		modulePath string
		directory  string
	}{
		{modulePath: "github.com/looprig/core", directory: "core"},
		{modulePath: foreignloopModulePath, directory: "foreignloop"},
		{modulePath: "github.com/looprig/fsstore", directory: "fsstore"},
		{modulePath: harnessModulePath, directory: "harness"},
		{modulePath: "github.com/looprig/inference", directory: "inference"},
		{modulePath: "github.com/looprig/mcp", directory: "mcp"},
		{modulePath: "github.com/looprig/storage", directory: "storage"},
	}
	var violations []string
	for _, dependency := range developmentModules {
		target, ok := replacements[dependency.modulePath]
		if !ok || (!filepath.IsAbs(target) && !strings.HasPrefix(target, ".")) {
			violations = append(violations, dependency.modulePath+" has no local development replacement")
			continue
		}
		expectedTarget := "../" + dependency.directory
		cleanTarget := filepath.ToSlash(filepath.Clean(filepath.FromSlash(target)))
		if cleanTarget != expectedTarget {
			violations = append(violations, fmt.Sprintf("%s replacement must use %s, got %s", dependency.modulePath, expectedTarget, target))
			continue
		}
		target = filepath.Join(moduleRoot, filepath.FromSlash(target))
		targetManifest, err := os.ReadFile(filepath.Join(filepath.Clean(target), "go.mod"))
		if err != nil {
			return nil, fmt.Errorf("read %s replacement manifest: %w", dependency.modulePath, err)
		}
		resolvedModule, _, err := manifestIntegrationSubjects(targetManifest)
		if err != nil {
			return nil, fmt.Errorf("parse %s replacement manifest: %w", dependency.modulePath, err)
		}
		if resolvedModule != dependency.modulePath {
			violations = append(violations, fmt.Sprintf("%s replacement resolves to module %s", dependency.modulePath, resolvedModule))
		}
	}
	sort.Strings(violations)
	return violations, nil
}

func localReplacementTargets(contents []byte) (map[string]string, error) {
	targets := make(map[string]string)
	var block bool
	scanner := bufio.NewScanner(strings.NewReader(string(contents)))
	for scanner.Scan() {
		line, _, _ := strings.Cut(scanner.Text(), "//")
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) == 1 && fields[0] == ")" {
			block = false
			continue
		}
		if len(fields) == 2 && fields[0] == "replace" && fields[1] == "(" {
			block = true
			continue
		}
		values := fields
		if !block {
			if fields[0] != "replace" {
				continue
			}
			values = fields[1:]
		}
		arrow := -1
		for index, value := range values {
			if value == "=>" {
				arrow = index
				break
			}
		}
		if arrow < 1 || arrow+1 >= len(values) {
			return nil, fmt.Errorf("malformed replace directive %q", strings.TrimSpace(line))
		}
		targets[unquoteModuleToken(values[0])] = unquoteModuleToken(values[arrow+1])
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return targets, nil
}

func manifestIntegrationSubjects(contents []byte) (string, integrationSubjects, error) {
	var modulePath string
	var subjects integrationSubjects
	var block string
	scanner := bufio.NewScanner(strings.NewReader(string(contents)))
	for scanner.Scan() {
		line, _, _ := strings.Cut(scanner.Text(), "//")
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) == 1 && fields[0] == ")" {
			block = ""
			continue
		}
		if len(fields) == 2 && fields[1] == "(" && (fields[0] == "require" || fields[0] == "replace") {
			block = fields[0]
			continue
		}
		directive := fields[0]
		values := fields[1:]
		if block != "" {
			directive = block
			values = fields
		}
		switch directive {
		case "module":
			if len(values) != 1 {
				return "", integrationSubjects{}, fmt.Errorf("module directive has %d values, want 1", len(values))
			}
			modulePath = unquoteModuleToken(values[0])
		case "require", "replace":
			if len(values) > 0 {
				subjects.addImportPath(unquoteModuleToken(values[0]))
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return "", integrationSubjects{}, err
	}
	if modulePath == "" {
		return "", integrationSubjects{}, errors.New("missing module directive")
	}
	return modulePath, subjects, nil
}

func unquoteModuleToken(token string) string {
	value, err := strconv.Unquote(token)
	if err == nil {
		return value
	}
	return token
}

// integrationTestSubjects counts imports only in ownership-test sources:
// every _test.go file, plus non-test Go files whose build constraints contain a
// positive integration or migration tag. Production composition is deliberately
// outside this guard even when it imports both modules.
func integrationTestSubjects(moduleRoot, modulePath string) (integrationSubjects, error) {
	var subjects integrationSubjects
	fset := token.NewFileSet()
	err := filepath.WalkDir(moduleRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		sourcePath := path
		if path != moduleRoot && entry.Type()&os.ModeSymlink != 0 {
			if filepath.Ext(path) != ".go" {
				return nil
			}
			resolved, err := filepath.EvalSymlinks(path)
			if err != nil {
				return fmt.Errorf("resolve Go source symlink %s: %w", path, err)
			}
			info, err := os.Stat(resolved)
			if err != nil {
				return fmt.Errorf("inspect Go source symlink %s: %w", path, err)
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("inspect Go source symlink %s: target is not a regular file", path)
			}
			sourcePath = resolved
		}
		if entry.IsDir() {
			if path == moduleRoot {
				return nil
			}
			if skipOwnershipDirectory(entry.Name()) {
				return fs.SkipDir
			}
			_, err := os.Stat(filepath.Join(path, "go.mod"))
			switch {
			case err == nil:
				return fs.SkipDir
			case errors.Is(err, os.ErrNotExist):
				return nil
			default:
				return err
			}
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		owned, err := integrationOwnershipSource(path, sourcePath)
		if err != nil {
			return err
		}
		if !owned {
			return nil
		}
		parsed, err := parser.ParseFile(fset, sourcePath, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range parsed.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			if hasModulePathPrefix(importPath, modulePath) {
				continue
			}
			subjects.addImportPath(importPath)
		}
		return nil
	})
	return subjects, err
}

func integrationOwnershipSource(logicalPath, sourcePath string) (bool, error) {
	if strings.HasSuffix(logicalPath, "_test.go") {
		return true, nil
	}
	contents, err := os.ReadFile(sourcePath)
	if err != nil {
		return false, err
	}
	scanner := bufio.NewScanner(strings.NewReader(string(contents)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "package ") {
			break
		}
		if !strings.HasPrefix(line, "//go:build ") && !strings.HasPrefix(line, "// +build ") {
			continue
		}
		expression, err := constraint.Parse(line)
		if err != nil {
			return false, fmt.Errorf("parse build constraint in %s: %w", logicalPath, err)
		}
		if hasPositiveOwnershipTag(expression, false) {
			return true, nil
		}
	}
	return false, scanner.Err()
}

func hasPositiveOwnershipTag(expression constraint.Expr, negated bool) bool {
	switch typed := expression.(type) {
	case *constraint.TagExpr:
		return !negated && (typed.Tag == "integration" || typed.Tag == "migration")
	case *constraint.NotExpr:
		return hasPositiveOwnershipTag(typed.X, !negated)
	case *constraint.AndExpr:
		return hasPositiveOwnershipTag(typed.X, negated) || hasPositiveOwnershipTag(typed.Y, negated)
	case *constraint.OrExpr:
		return hasPositiveOwnershipTag(typed.X, negated) || hasPositiveOwnershipTag(typed.Y, negated)
	default:
		return false
	}
}

func (subjects *integrationSubjects) addImportPath(importPath string) {
	subjects.harness = subjects.harness || hasModulePathPrefix(importPath, harnessModulePath)
	subjects.foreignloop = subjects.foreignloop || hasModulePathPrefix(importPath, foreignloopModulePath)
}

func hasModulePathPrefix(importPath, modulePath string) bool {
	return importPath == modulePath || strings.HasPrefix(importPath, modulePath+"/")
}

func skipOwnershipDirectory(name string) bool {
	return name == "CVS" || name == "vendor" || name == "worktrees" || strings.HasPrefix(name, ".")
}

func findModuleRoot(start, wantModule string) (string, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", err
	}
	if info, err := os.Stat(dir); err == nil && !info.IsDir() {
		dir = filepath.Dir(dir)
	}
	for {
		contents, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err == nil {
			modulePath, _, parseErr := manifestIntegrationSubjects(contents)
			if parseErr != nil {
				return "", parseErr
			}
			if modulePath == wantModule {
				return dir, nil
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("find module %s from %s: no matching go.mod", wantModule, start)
		}
		dir = parent
	}
}

func writeModuleFixture(t *testing.T, collectionRoot, name, goMod string, files map[string]string) {
	t.Helper()
	moduleRoot := filepath.Join(collectionRoot, name)
	if err := os.MkdirAll(moduleRoot, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(moduleRoot, "go.mod"), []byte(goMod), 0o600); err != nil {
		t.Fatal(err)
	}
	for relative, contents := range files {
		path := filepath.Join(moduleRoot, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
