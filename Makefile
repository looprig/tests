.PHONY: cloud cloud-bouncer test live-network sessionwire-goldens fmt fmt-check vet staticcheck lint vuln secure dependency-boundary root-layout check mod-check release-check

# Module's own package dirs. This module is a single package at its root, but
# go list is used (rather than hardcoding ".") to match the go-list idiom the
# rest of this Makefile relies on. -tags integration matches vet/test below so
# the dirs list picks up integration-tagged files too.
GO_DIRS := $(shell GOWORK=off go list -f '{{.Dir}}' -tags integration ./...)

# THIS SUITE CANNOT BE VERIFIED FROM AN ISOLATED CLONE OF THIS REPOSITORY.
# It needs the SIBLING CHECKOUTS to be present beside it: ../mcp (the MCP
# adapter cases resolve it on disk), and the collection root itself, which
# root_layout_test.go and dependency_boundary_test.go walk. Run it from a full
# workspace checkout. A clone on its own fails ~22 cases, and every one of those
# failures is the missing siblings rather than a property of this module -- it
# has been mistaken for one before, in a release gate.
test:
	LOOPRIG_LIVE_NETWORK=0 GOWORK=off go test -count=1 -tags integration -race ./...

live-network:
	LOOPRIG_LIVE_NETWORK=1 GOWORK=off go test -tags integration -race -count=1 -run '^TestSandboxBroadNetworkGrantCarriesDNS$$' .

# Format the whole module in place.
fmt:
	gofmt -w $(GO_DIRS)

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

vet:
	GOWORK=off go vet -tags integration ./...

staticcheck:
	GOWORK=off go tool staticcheck -tags integration ./...

lint: fmt-check vet staticcheck
	# gosec is NOT module-aware: its ./... is a filesystem walk that would descend
	# into sibling checkouts alongside this module rather than stopping at module
	# boundaries the way go vet and staticcheck do. Scope it to THIS module's
	# package dirs via GO_DIRS (the same go-list idiom fmt/fmt-check use).
	GOWORK=off go tool gosec $(GO_DIRS)

vuln:
	GOWORK=off go mod verify
	GOWORK=off go tool govulncheck ./...

secure: lint vuln

dependency-boundary:
	GOWORK=off go test -count=1 -race -run '^TestCrossModuleOwnership' ./...

# Every sibling repository in this ecosystem (harness, classifiers, carbon,
# and this tests module) carries the same minimal top-level marker set
# (go.mod, Makefile, LICENSE, CONTRIBUTING.md). See root_layout_test.go.
root-layout:
	GOWORK=off go test -count=1 -race -run '^(TestSiblingRootLayout|TestRepositoryRootLayoutMatchesEcosystemConvention)' ./...


# Regenerate the frozen HostLink/ClientLink fixtures under testdata/sessionwire
# from a live run of the released Factory, Host and controller drain client
# (factory_host_wire_integration_test.go). Only for an INTENDED wire change: it
# then fails if git sees any diff, so the new fixtures are reviewed and
# committed rather than slipping in. An ordinary `make test` compares against
# the committed fixtures and fails on any drift.
SESSIONWIRE_PRODUCERS := ^(TestFactoryHostWireGoldens|TestHostLinkHostAnswersGoldens|TestClientLinkRefusalGoldens)$$

sessionwire-goldens:
	LOOPRIG_UPDATE_SESSIONWIRE=1 LOOPRIG_LIVE_NETWORK=0 GOWORK=off go test -count=1 -tags integration -run '$(SESSIONWIRE_PRODUCERS)' .
	LOOPRIG_LIVE_NETWORK=0 GOWORK=off go test -count=1 -tags integration -run '^(TestSessionwireGolden|TestHostAnswersFactoryGoldensUnchanged|TestFactoryAcceptsHostGoldensUnchanged)' .
	git diff --exit-code -- testdata/sessionwire
	@test -z "$$(git status --porcelain -- testdata/sessionwire)" || (git status --porcelain -- testdata/sessionwire; echo 'testdata/sessionwire has uncommitted fixtures' >&2; exit 1)

mod-check:
	@sh scripts/check-release-modfile.sh go.mod
	@sh scripts/check-release-modfile.sh oldhostlane/go.mod
	@test -z "$$(GOWORK=off go mod tidy -diff)" || (echo 'go.mod is not tidy' >&2; GOWORK=off go mod tidy -diff; exit 1)
	GOWORK=off go mod verify
	(cd oldhostlane && GOWORK=off go mod verify)

# The P3.1 cloud composition lane (runbook 07): released pgstore + s3store
# under a real Factory and pooled Hosts, against LOCAL digest-pinned
# PostgreSQL, PgBouncer and MinIO containers that scripts/cloud-up.sh starts
# and scripts/cloud-down.sh removes on exit. Needs Docker; never contacts a
# cloud. `cloud-bouncer` routes every pgstore pool through PgBouncer in
# transaction mode.
CLOUD_TEST = GOWORK=off go test -count=1 -tags 'integration cloud' -race -timeout 40m -run '^(TestCloud|TestSessionStore)' .

cloud:
	@env_file=$$(sh scripts/cloud-up.sh) && trap 'sh scripts/cloud-down.sh' EXIT && . "$$env_file" && $(CLOUD_TEST)

cloud-bouncer:
	@env_file=$$(sh scripts/cloud-up.sh) && trap 'sh scripts/cloud-down.sh' EXIT && . "$$env_file" && LOOPRIG_CLOUD_PG_VIA=bouncer $(CLOUD_TEST)

release-check:
	$(MAKE) mod-check
	GOWORK=off go test -count=1 -tags integration -race ./...

# --- standardized check surface -------------------------------------------
# One target, the same set of checks, in every module. CI calls exactly this,
# so a check can no longer pass locally and be silently absent in CI (or the
# reverse). The lint/security tools are pinned by this module's go.mod tool directives.
#
# CHECK_GO_DIRS scopes gosec: gosec is NOT module-aware, so a bare ./... is a
# filesystem walk that descends into nested .worktrees/ checkouts, which are
# separate modules. go vet and staticcheck are module-aware and need no scope.
CHECK_GO_DIRS = $(shell GOWORK=off go list -f '{{.Dir}}' ./...)
# CHECK_GO_FILES is what gofmt gets. Never hand it CHECK_GO_DIRS: gofmt RECURSES
# into directory operands, so for a module with a root package it would walk the
# whole tree, nested .worktrees/ checkouts included.
CHECK_GO_FILES = $(foreach dir,$(CHECK_GO_DIRS),$(wildcard $(dir)/*.go))

check-staticcheck:
	GOWORK=off go tool staticcheck ./...

check-gosec:
	GOWORK=off go tool gosec -quiet $(CHECK_GO_DIRS)

check-vuln:
	GOWORK=off go mod verify
	GOWORK=off go tool govulncheck ./...

build:
	GOWORK=off go build ./...

check: fmt-check vet check-staticcheck check-gosec check-vuln test build

.PHONY: check check-staticcheck check-gosec check-vuln fmt fmt-check vet test build
