package tests

import (
	"bufio"
	"errors"
	"fmt"
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

func TestCrossModuleOwnershipScannerRejectsNonTestsOwners(t *testing.T) {
	root := t.TempDir()
	writeModuleFixture(t, root, "tests", "module github.com/looprig/tests\n", map[string]string{
		"integration_test.go": "package tests\nimport (\n_ \"github.com/looprig/harness/pkg/rig\"\n_ \"github.com/looprig/foreignloop/backend\"\n)\n",
	})
	writeModuleFixture(t, root, "bad-source", "module github.com/example/bad-source\n", map[string]string{
		"nested/owner_plan9_test.go": "//go:build plan9\n\npackage nested\nimport (\n_ \"github.com/looprig/harness/pkg/session\"\n_ \"github.com/looprig/foreignloop/driver/claude\"\n)\n",
	})
	writeModuleFixture(t, root, "bad-replace", `module github.com/example/bad-replace

replace github.com/looprig/harness => ../harness
replace github.com/looprig/foreignloop => ../foreignloop
`, nil)

	violations, err := crossModuleOwnershipViolations(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"github.com/example/bad-replace",
		"github.com/example/bad-source",
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
	writeModuleFixture(t, collectionRoot, "harness", "module github.com/looprig/harness\n", nil)
	writeModuleFixture(t, collectionRoot, "foreignloop", "module github.com/looprig/foreignloop\n", nil)
	writeModuleFixture(t, collectionRoot, "tests", `module github.com/looprig/tests

replace (
	github.com/looprig/harness => ../harness
	github.com/looprig/foreignloop => ../foreignloop
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
	writeModuleFixture(t, collectionRoot, "wrong", "module github.com/example/wrong\n", nil)
	writeModuleFixture(t, collectionRoot, "tests", `module github.com/looprig/tests

replace github.com/looprig/harness => ../wrong
`, nil)

	violations, err := developmentModuleSourceViolations(filepath.Join(collectionRoot, "tests"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"github.com/looprig/foreignloop has no local development replacement",
		"github.com/looprig/harness replacement resolves to module github.com/example/wrong",
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
	entries, err := os.ReadDir(collectionRoot)
	if err != nil {
		return nil, fmt.Errorf("read sibling module collection %s: %w", collectionRoot, err)
	}
	var violations []string
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		moduleRoot := filepath.Join(collectionRoot, entry.Name())
		goModPath := filepath.Join(moduleRoot, "go.mod")
		goMod, err := os.ReadFile(goModPath)
		switch {
		case err == nil:
		case errors.Is(err, os.ErrNotExist):
			continue
		default:
			return nil, fmt.Errorf("read sibling manifest %s: %w", goModPath, err)
		}
		modulePath, subjects, err := manifestIntegrationSubjects(goMod)
		if err != nil {
			return nil, fmt.Errorf("parse sibling manifest %s: %w", goModPath, err)
		}
		sourceSubjects, err := sourceIntegrationSubjects(moduleRoot, modulePath)
		if err != nil {
			return nil, fmt.Errorf("scan sibling module %s: %w", modulePath, err)
		}
		subjects.harness = subjects.harness || sourceSubjects.harness
		subjects.foreignloop = subjects.foreignloop || sourceSubjects.foreignloop
		if modulePath == harnessModulePath {
			subjects.harness = false
		}
		if modulePath == foreignloopModulePath {
			subjects.foreignloop = false
		}
		if modulePath != testsModulePath && subjects.harness && subjects.foreignloop {
			violations = append(violations, modulePath)
		}
	}
	sort.Strings(violations)
	return violations, nil
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
	var violations []string
	for _, subject := range []string{foreignloopModulePath, harnessModulePath} {
		target, ok := replacements[subject]
		if !ok || (!filepath.IsAbs(target) && !strings.HasPrefix(target, ".")) {
			violations = append(violations, subject+" has no local development replacement")
			continue
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(moduleRoot, filepath.FromSlash(target))
		}
		targetManifest, err := os.ReadFile(filepath.Join(filepath.Clean(target), "go.mod"))
		if err != nil {
			return nil, fmt.Errorf("read %s replacement manifest: %w", subject, err)
		}
		resolvedModule, _, err := manifestIntegrationSubjects(targetManifest)
		if err != nil {
			return nil, fmt.Errorf("parse %s replacement manifest: %w", subject, err)
		}
		if resolvedModule != subject {
			violations = append(violations, fmt.Sprintf("%s replacement resolves to module %s", subject, resolvedModule))
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

func sourceIntegrationSubjects(moduleRoot, modulePath string) (integrationSubjects, error) {
	var subjects integrationSubjects
	fset := token.NewFileSet()
	err := filepath.WalkDir(moduleRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
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
		parsed, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
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
