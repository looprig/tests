//go:build integration

// This file is runbook 07 task I1.3 CASE 5: the deployment the reconciliation
// cases run against contains no cache, notifier or broker -- and those cases,
// unchanged, are the "same tests pass" half.
//
// # What is derived and what is named
//
// The package graph is DERIVED, with `go list -deps`, from the package every
// I1.3 case composes its deployment through (internal/orchestrationtest: real
// Factory replicas, a real pooled Host, the real SessionStore over memstore)
// plus the I1.3 test files' own imports. Only a DENYLIST of client families is
// named, and it is checked against the derived graph rather than against a
// list of packages someone expected.
//
// # Why "no broker package in the graph" is not the assertion
//
// It is false, and measured so: Centrifuge -- the library Factory's ClientLink
// and every Host's HostLink are built on -- bundles an OPTIONAL Redis broker
// and imports github.com/redis/rueidis to provide it. That package is compiled
// in whether or not anything constructs it. So the assertion is the one that
// is both true and meaningful:
//
//  1. no Looprig package, and no package the I1.3 cases import, imports a
//     broker/cache client DIRECTLY -- every path to one runs through the
//     third-party library that bundles it; and
//  2. no Looprig package's production source CONFIGURES one: none of them
//     names Centrifuge's broker or presence-manager seams, so every node keeps
//     its in-process memory broker.
//
// Both halves carry a positive control, so a scanner that silently matched
// nothing cannot pass.

package tests

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// brokerFamilies are the client families a brokerless deployment must not
// wire. It is a denylist of FAMILIES, matched against import paths, not a list
// of packages this case expects to find.
var brokerFamilies = regexp.MustCompile(`(?i)(redis|rueidis|nats-io|/nats\b|natsstore|kafka|sarama|amqp|rabbitmq|memcache|groupcache|ristretto|bigcache|freecache|pulsar|nsqio|zeromq|/zmq|etcd|consul|/nsq\b)`)

// brokerSeams are the identifiers through which a Centrifuge node is given a
// broker or presence manager. Naming none of them keeps the in-memory broker.
var brokerSeams = map[string]bool{
	"GetBroker": true, "GetPresenceManager": true, "SetBroker": true, "SetPresenceManager": true,
	"NewRedisBroker": true, "NewRedisPresenceManager": true, "NewRedisShard": true,
}

// reconciliationTestFiles are the I1.3 cases whose composition this case
// vouches for.
var reconciliationTestFiles = []string{
	"factory_reconciliation_integration_test.go",
	"factory_reconciliation_pooled_integration_test.go",
	"factory_reconciliation_brokerless_integration_test.go",
}

type listedPackage struct {
	ImportPath string
	Dir        string
	GoFiles    []string
	Imports    []string
	Standard   bool
	Module     *struct{ Path string }
}

func goListDeps(t *testing.T, patterns ...string) map[string]listedPackage {
	t.Helper()
	args := append([]string{"list", "-tags", "integration", "-deps", "-json=ImportPath,Dir,GoFiles,Imports,Standard,Module"}, patterns...)
	cmd := exec.Command("go", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}
	packages := map[string]listedPackage{}
	decoder := json.NewDecoder(bytes.NewReader(out))
	for {
		var pkg listedPackage
		if err := decoder.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decoding go list output: %v", err)
		}
		packages[pkg.ImportPath] = pkg
	}
	return packages
}

func isLooprig(path string) bool { return strings.HasPrefix(path, "github.com/looprig/") }

// TestReconciliationDeploymentIsBrokerless is I1.3 case 5.
func TestReconciliationDeploymentIsBrokerless(t *testing.T) {
	// The roots: the kit that composes every I1.3 deployment, and whatever the
	// I1.3 files themselves import.
	roots := map[string]bool{"github.com/looprig/tests/internal/orchestrationtest": true}
	fset := token.NewFileSet()
	for _, name := range reconciliationTestFiles {
		file, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		for _, spec := range file.Imports {
			path, _ := strconv.Unquote(spec.Path.Value)
			if brokerFamilies.MatchString(path) {
				t.Fatalf("%s imports %s directly; the I1.3 cases must compose no broker", name, path)
			}
			if !strings.Contains(path, ".") {
				continue // standard library
			}
			roots[path] = true
		}
	}
	var patterns []string
	for root := range roots {
		patterns = append(patterns, root)
	}
	sort.Strings(patterns)
	graph := goListDeps(t, patterns...)
	if _, ok := graph["github.com/looprig/factory"]; !ok {
		t.Fatalf("the derived graph does not contain Factory; the roots %v are not the deployment", patterns)
	}

	t.Run("every path to a broker client runs through the library that bundles it", func(t *testing.T) {
		importers := map[string][]string{}
		for path, pkg := range graph {
			for _, imported := range pkg.Imports {
				importers[imported] = append(importers[imported], path)
			}
		}
		var brokers []string
		for path := range graph {
			if brokerFamilies.MatchString(path) {
				brokers = append(brokers, path)
			}
		}
		sort.Strings(brokers)
		// POSITIVE CONTROL. Centrifuge's bundled Redis client is in the graph;
		// if the scan found nothing, the denylist or the graph is wrong, and
		// "no broker" would be a statement about a scanner that sees nothing.
		if len(brokers) == 0 {
			t.Fatalf("the denylist matched nothing in a %d-package graph that is known to contain Centrifuge's bundled Redis client", len(graph))
		}
		for _, broker := range brokers {
			for _, importer := range importers[broker] {
				if isLooprig(importer) {
					t.Fatalf("%s imports the broker/cache client %s directly", importer, broker)
				}
				if !brokerFamilies.MatchString(importer) && !strings.HasPrefix(importer, "github.com/centrifugal/") {
					t.Fatalf("%s reaches the broker/cache client %s from outside the library that bundles it", importer, broker)
				}
			}
		}
		t.Logf("case 5: %d broker-family packages in a %d-package graph, all reached only through Centrifuge: %v", len(brokers), len(graph), brokers)
	})

	t.Run("no Looprig package configures a broker or presence manager", func(t *testing.T) {
		scan := func(pkg listedPackage) []string {
			var hits []string
			for _, name := range pkg.GoFiles {
				path := filepath.Join(pkg.Dir, name)
				src, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("reading %s: %v", path, err)
				}
				file, err := parser.ParseFile(token.NewFileSet(), path, src, parser.SkipObjectResolution)
				if err != nil {
					t.Fatalf("parsing %s: %v", path, err)
				}
				ast.Inspect(file, func(node ast.Node) bool {
					if ident, ok := node.(*ast.Ident); ok && brokerSeams[ident.Name] {
						hits = append(hits, path+": "+ident.Name)
					}
					return true
				})
			}
			return hits
		}
		// POSITIVE CONTROL: the seams exist, in Centrifuge's own source, so a
		// scanner that finds none in Looprig is looking in the right way.
		centrifuge, ok := graph["github.com/centrifugal/centrifuge"]
		if !ok {
			t.Fatalf("Centrifuge is not in the derived graph")
		}
		if len(scan(centrifuge)) == 0 {
			t.Fatalf("the seam scanner found no broker seam in Centrifuge's own source; it cannot see one in Looprig's")
		}
		scanned := 0
		for path, pkg := range graph {
			if !isLooprig(path) || pkg.Standard {
				continue
			}
			scanned++
			if hits := scan(pkg); len(hits) != 0 {
				t.Fatalf("a Looprig package configures a broker or presence manager: %v", hits)
			}
		}
		if scanned == 0 {
			t.Fatalf("no Looprig package was scanned")
		}
		t.Logf("case 5: %d Looprig packages scanned; none names a broker or presence-manager seam", scanned)
	})
}
