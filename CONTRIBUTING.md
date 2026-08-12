# Contributing to looprig/tests

`looprig/tests` is the cross-module integration-test harness for the looprig
project. It is a standalone Go module (`github.com/looprig/tests`) — not a
library other repos import — that sits alongside sibling checkouts of
`core`, `harness`, `foreignloops`, `fsstore`, `inference`, `mcp`, `sandbox`,
and `storage`, and exercises them together the way a real consumer would:
real rig/session lifecycles over a real `fsstore`, real MCP server bindings,
real foreignloop primary/subagent wiring, real sandbox executors, and
restore-from-journal behavior across all of the above. Individual modules
own their own unit tests; this repo owns the contracts that only show up
when two or more modules are wired together.

Beyond exercising those cross-module contracts, this repo also guards the
module boundary itself:

- it verifies that no other module in the sibling checkout tree reaches
  into both Harness and foreignloops at once (that kind of cross-module
  integration belongs here, not in a leaf module);
- it verifies that its canonical `go.mod` contains only published module
  versions and no workspace-only `replace` directives, so every invocation
  exercises the same graph a real consumer resolves.

## Build, test, and secure

All Go invocations in this repo are run with `GOWORK=off` so the module
resolves entirely through its own canonical `go.mod` rather than any
enclosing Go workspace.

- `make test` — runs the full integration suite (`-tags integration`) with
  the race detector against the published module graph. `LOOPRIG_LIVE_NETWORK=0`
  keeps the suite from making real outbound network calls; tests that need
  external network access skip themselves closed under this setting.
- `make live-network` — runs only the opted-in
  `TestSandboxBroadNetworkGrantCarriesDNS` test with
  `LOOPRIG_LIVE_NETWORK=1`, exercising a sandboxed process's real outbound
  HTTPS/DNS path. This is intentionally excluded from `make test` and from
  `make check` because it depends on live network access.
- `make fmt` — formats every package directory in this module in place with
  `gofmt`.
- `make fmt-check` — fails if any tracked file is not `gofmt`-clean, without
  modifying anything.
- `make vet` — runs `go vet` over the integration-tagged build.
- `make staticcheck` — runs `staticcheck` over the integration-tagged build.
- `make lint` — `fmt-check`, `vet`, `staticcheck`, then `gosec` scoped to
  this module's own package directories (gosec's `./...` is a plain
  filesystem walk, not module-aware, so it must be pointed only at this
  module's own dirs rather than the sibling checkouts next to it).
- `make vuln` — `go mod verify` followed by `govulncheck` over the module.
- `make secure` — `lint` followed by `vuln`.
- `make dependency-boundary` — runs `TestCrossModuleOwnership*`, which scans
  the sibling module checkouts next to this repo and fails if any of them
  imports both Harness and foreignloops packages from integration-tagged
  (or otherwise real, non-test) source — that combination is reserved for
  this repo.
- `make mod-check` — verifies the canonical `go.mod` has no local filesystem
  replacements, is tidy under `GOWORK=off`, and has verified module sums.
- `make check` — the fast-feedback composition: `fmt-check`, `vet`,
  `dependency-boundary`, `root-layout`, then `test`. It deliberately
  leaves out `secure` (staticcheck/gosec/govulncheck are slower and
  `govulncheck` needs network access to the vulnerability database) and
  leaves out `live-network`.
- `make release-check` — validates the canonical `go.mod` with
  `scripts/check-release-modfile.sh`, checks tidy/module sums, and runs the
  full integration suite against that exact published graph. It has no
  alternate release modfile and cannot silently fall back to workspace source.

## Pull requests

Keep PRs focused and include a clear description of the change and its
motivation. Make sure `make check` passes locally, and run `make secure`
for anything touching dependencies or security-sensitive surface. Add or
update tests for any behavior change. If you are adding new integration
coverage for a contract between two or more looprig modules, it belongs
here rather than in the individual module repos — that is what this repo
is for.

## Code of conduct

Be respectful and constructive. Assume good faith, keep discussion focused
on the technical merits, and be considerate of other contributors' time.

## License

By contributing, you agree that your contributions will be licensed under
the Apache License, Version 2.0 (see [LICENSE](LICENSE)).
