//go:build integration && orchestration

package orchestrationtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// This file widens the Host trip-wires from a SPELLING to a CAPABILITY.
//
// The earlier wires reflected over `*host.Host`'s method set. That is the right
// SUBJECT -- the dependency, not this kit's fixture -- and it is the correction
// this lane already had to make once. But it is one syntactic form of the
// subject, and a review proved the gap by building the likely shape: a separate
// exported `Runtime` type carrying Serve/Start/StartDrain/ObserveDrain, plus a
// package-level `func host.Serve(...)`. Both wires stayed green.
//
// That shape is not a corner case, it is the PROBABLE one. `host.Host` is
// documented as an immutable configuration value, so a runtime surface almost
// certainly will not arrive as a method on it. A trip-wire that fires only on
// the unlikely spelling is a trip-wire that will not fire.
//
// So the scan below reads the module's whole EXPORTED DECLARATION SURFACE --
// every importable package, every package-level function, every method on every
// exported type, and every method declared in an exported interface -- and
// matches on the runtime and drain vocabulary wherever it appears. It does not
// care which type carries the verb, or whether anything carries it at all.

// runtimeVerbs are names whose appearance anywhere on Host's exported surface
// would mean the module has grown something that RUNS.
//
// They are the verbs a caller uses to start, hold or stop a process, plus the
// two HTTP entry points. Launch vocabulary is deliberately absent: department's
// Create, Restore, NewSession and RestoreSession are how a runtime is made, not
// how the Host is run, and every one of them exists today.
var runtimeVerbs = []string{
	"Attach", "Close", "Handler", "Listen", "Run", "Serve", "ServeHTTP",
	"Shutdown", "Start", "Stop",
}

// drainVerbs are names whose appearance would mean Host can be drained.
var drainVerbs = []string{
	"BeginDrain", "Drain", "DrainHost", "DrainSession", "ObserveDrain",
	"RequestDrain", "StartDrain",
}

// hostExportedNames returns every exported declaration name on the host
// module's importable packages, with the file and kind it was found at.
//
// It resolves the module by asking the toolchain rather than guessing a path,
// so it follows the workspace and would follow a released pin just as well.
func hostExportedNames(tb TB) map[string]string {
	tb.Helper()
	out, err := exec.Command("go", "list", "-f", "{{.Dir}}",
		"github.com/looprig/host", "github.com/looprig/host/department").Output()
	if err != nil {
		tb.Fatalf("orchestrationtest: locating the host module's packages: %v", err)
		return nil
	}
	dirs := strings.Fields(strings.TrimSpace(string(out)))
	if len(dirs) == 0 {
		tb.Fatalf("orchestrationtest: the host module resolved to no package directories; " +
			"this scan is vacuous and would report an absence it never looked for")
		return nil
	}
	found := make(map[string]string)
	scanned := 0
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			tb.Fatalf("orchestrationtest: reading %s: %v", dir, err)
			return nil
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(dir, name)
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				tb.Fatalf("orchestrationtest: parsing %s: %v", path, err)
				return nil
			}
			scanned++
			collectExported(file, name, found)
		}
	}
	// A scan that read no files would report "no runtime surface" forever. It
	// is the vacuity guard this kind of absence assertion always needs.
	if scanned == 0 {
		tb.Fatalf("orchestrationtest: parsed 0 host source files; the surface scan is vacuous")
		return nil
	}
	return found
}

func collectExported(file *ast.File, where string, found map[string]string) {
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			// Package-level functions AND methods. A method's receiver type is
			// not consulted: a verb on any exported type is the capability.
			if d.Name.IsExported() {
				found[d.Name.Name] = where
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok || !typeSpec.Name.IsExported() {
					continue
				}
				iface, ok := typeSpec.Type.(*ast.InterfaceType)
				if !ok || iface.Methods == nil {
					continue
				}
				for _, method := range iface.Methods.List {
					for _, name := range method.Names {
						if name.IsExported() {
							found[name.Name] = where
						}
					}
				}
			}
		}
	}
}

func matchedVerbs(surface map[string]string, verbs []string) []string {
	hits := make([]string, 0, len(verbs))
	for _, verb := range verbs {
		if where, ok := surface[verb]; ok {
			hits = append(hits, verb+" ("+where+")")
		}
	}
	sort.Strings(hits)
	return hits
}

// AssertHostExposesNoRuntimeCapability is runbook 07 I1.1 cases 3-4 and I1.4's
// trip-wire, widened from `*host.Host`'s method set to the module's whole
// exported surface.
//
// It fires on a runtime verb wherever it appears -- a method on any exported
// type, a package-level function, or a method declared in an exported interface
// -- so a `Runtime` type with `Serve`, or a bare `func host.Serve`, trips it
// just as a method on `Host` would.
func AssertHostExposesNoRuntimeCapability(tb TB) {
	tb.Helper()
	hits := matchedVerbs(hostExportedNames(tb), runtimeVerbs)
	if len(hits) == 0 {
		return
	}
	tb.Fatalf("orchestrationtest: the host module now exports %v. Host has grown something that RUNS, "+
		"so a session channel can have a publisher: runbook 07 I1.1 cases 3-4 and I1.4 are no longer "+
		"blocked. Drive the live tail for real and delete this trip-wire", hits)
}

// AssertHostExposesNoDrainCapability is runbook 07 I2.3's trip-wire, widened the
// same way and kept separate from the runtime wire because the two unblock
// different tasks and a merged message would mis-attribute whichever fired.
func AssertHostExposesNoDrainCapability(tb TB) {
	tb.Helper()
	hits := matchedVerbs(hostExportedNames(tb), drainVerbs)
	if len(hits) == 0 {
		return
	}
	tb.Fatalf("orchestrationtest: the host module now exports %v. Host has grown a drain surface, "+
		"so runbook 07 I2.3 is no longer blocked: drive drain ordering for real and delete this "+
		"trip-wire", hits)
}
