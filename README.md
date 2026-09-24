# tests

`tests` is the cross-module integration-test repository for Looprig. It is a
standalone Go module (`github.com/looprig/tests`) that nothing imports: a single
test package at the root (plus test-support packages under `internal/`) that
wires the **released** Looprig modules together the way a real deployment does
and checks the contracts that only exist between two or more of them. Each module
owns its own unit tests; this repository owns the seams.

It resolves every Looprig dependency as a published version from its own
`go.mod` (no `replace` directives, no vendoring), so a green run is evidence
about the exact graph a consumer builds.

## What it covers

- **Factory and Host orchestration.** Real Factory replicas placing sessions on
  real composed Hosts running real harness rigs over real SessionStore journals:
  placement, admission and crash recovery, the first message of a create, gates
  and gate failover, reconciliation, reconnect, link backpressure, cold reads
  with every Host stopped, pooled-Host lifecycle, lease takeover, stale writers,
  draining, and workspace restore (`factory_*_integration_test.go`,
  `host_*_integration_test.go`).
- **Wire compatibility.** HostLink and ClientLink frames captured off a real
  WebSocket are frozen as fixtures under `testdata/sessionwire` and compared on
  every run (`factory_host_wire_integration_test.go`).
- **Tool-result retention.** Large tool results captured by
  `rig.WithToolResultObjects` are readable afterwards by the model and by an
  authorized client, and by no one else (`tool_result_retention_integration_test.go`).
- **SessionStore over concrete providers**, and harness journals read through
  SessionStore (`sessionstore_provider_integration_test.go`,
  `sessionstore_harness_evidence_integration_test.go`).
- **Harness runtime integrations**: rig lifecycle and restore, gates open at
  shutdown, MCP adapters and restore across a changed MCP catalog,
  foreignloops primaries and subagents, the permission classifier, sandbox
  executors (native on macOS and Linux), eval, and the credential gateway.
- **Boundary guards** (untagged): the canonical `go.mod` holds only published
  versions with no local `replace` (`release_modfile_guard_test.go`), no sibling
  module imports both harness and foreignloops (`dependency_boundary_test.go`),
  and sibling repositories share the same top-level marker files
  (`root_layout_test.go`).

`internal/orchestrationtest` is the shared kit for the Factory/Host lanes,
`internal/toolresultobjects` the object-read policy the tool-result lane
installs, and `internal/kindlane` the two product binaries (`cmd/kindhost`,
`cmd/kindcontrol`) and Dockerfile for the Kubernetes lane.

## Requirements

- Go 1.26.8. Every command runs with `GOWORK=off`.
- **Sibling checkouts.** The suite must run from a checkout where the other
  Looprig repositories sit beside this one: the MCP adapter cases build fixture
  servers from `../mcp`, and the boundary and layout guards walk the parent
  directory. An isolated clone fails those cases for that reason alone.
- The sandbox cases use the platform sandbox (`/usr/bin/sandbox-exec` on macOS;
  user/mount/network namespaces, or Landlock v4 plus seccomp, on Linux) and skip
  when it is unavailable.

## Lanes and build tags

| Lane | Build tags | Gate | Infrastructure |
|---|---|---|---|
| Default suite | `integration` | none | none; every service runs locally on loopback |
| Live network | `integration` | `LOOPRIG_LIVE_NETWORK=1` | outbound DNS/HTTPS |
| Cloud composition | `integration cloud` | `LOOPRIG_CLOUD=1` | Docker (local PostgreSQL, PgBouncer, MinIO) |
| Kubernetes (D3.1) | `integration kind` | `LOOPRIG_KIND=1` | Docker, kind, kubectl |
| 5,000-ClientLink soak | `integration soak` | `LOOPRIG_SOAK=1` | a high file-descriptor limit; tens of minutes |

Most test files carry `//go:build integration`, so a plain `go test ./...` runs
only the untagged guards.

### Default suite

```sh
make test   # LOOPRIG_LIVE_NETWORK=0 GOWORK=off go test -count=1 -tags integration -race ./...
```

It includes `TestFactoryHostRaceStress`, the lower-count race stress over two
Factory replicas and two pooled Hosts. Its size and seed are set with
`LOOPRIG_STRESS_SEED`, `LOOPRIG_STRESS_SESSIONS`, `LOOPRIG_STRESS_HOSTS`,
`LOOPRIG_STRESS_REPLICAS`, `LOOPRIG_STRESS_OPS`, `LOOPRIG_STRESS_VIEWERS` and
`LOOPRIG_STRESS_SETTLE`; `LOOPRIG_STRESS_LOGDIR` keeps each replica's log.

### Live network

```sh
make live-network   # runs TestSandboxBroadNetworkGrantCarriesDNS with LOOPRIG_LIVE_NETWORK=1
```

It is excluded from `make test` and `make check` because it needs real outbound
network access.

### Cloud composition

The released `pgstore` (Ledger, Leaser, KV, OrderedIndex) and `s3store` (Blobs)
as one composite under a real Factory and pooled Hosts, including provider
restarts and tenant isolation. `scripts/cloud-up.sh` starts local,
digest-pinned PostgreSQL, PgBouncer (transaction mode) and MinIO (TLS, static KMS
key) containers and writes the `LOOPRIG_CLOUD_*` environment file;
`scripts/cloud-down.sh` removes them. Nothing contacts a real cloud.

```sh
make cloud           # cloud-up, the TestCloud*/TestSessionStore* cases, cloud-down
make cloud-bouncer   # the same with every pgstore pool routed through PgBouncer
```

### Kubernetes (kind)

The controller's dedicated placement and drain against a real cluster, with the
released controller, Factory and Host, and SessionStore over a real NATS
JetStream server.

```sh
scripts/kind-d31.sh up       # local registry + kind cluster
scripts/kind-d31.sh images   # build and push the lane images; prints KIND_HOST_IMAGE / KIND_CONTROL_IMAGE
LOOPRIG_KIND=1 KIND_HOST_IMAGE=... KIND_CONTROL_IMAGE=... \
  GOWORK=off go test -tags 'integration kind' -run '^TestDedicated' -v -timeout 30m .
scripts/kind-d31.sh down
```

Each case runs in its own disposable namespace, which it deletes afterwards.

### Soak

Two real pooled Hosts and two Factory replicas serve 5,000 ClientLink
connections from a child process. It is a resource measurement, so it must not
run under `-race`:

```sh
ulimit -n 65536
LOOPRIG_SOAK=1 GOWORK=off GOTOOLCHAIN=go1.26.8 go test -tags 'integration soak' \
  -run '^TestFactoryHostClientLinkSoak5000$' -count=1 -timeout=60m .
```

Results are written under `LOOPRIG_SOAK_OUT`; the other knobs are documented at
the top of `factory_host_soak_test.go`.

## Other make targets

- `make check`: `fmt-check`, `vet`, staticcheck, gosec, govulncheck, `test`, `build`.
- `make lint`, `make vuln`, `make secure`: the lint and security tools, run
  through the module's `go tool` directives.
- `make dependency-boundary`, `make root-layout`: the boundary guards on their own.
- `make mod-check`: no local `replace`, tidy, and verified sums.
- `make release-check`: `mod-check` plus the full integration suite.
- `make sessionwire-goldens`: regenerate the `testdata/sessionwire` fixtures from
  a live run. Use it only for an intended wire change; it fails if the fixtures
  differ from what is committed.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## License

Apache License 2.0; see [LICENSE](LICENSE).
