.PHONY: test live-network fmt fmt-check vet staticcheck lint vuln secure dependency-boundary root-layout check mod-check release-check

# Module's own package dirs. This module is a single package at its root, but
# go list is used (rather than hardcoding ".") to match the go-list idiom the
# rest of this Makefile relies on. -tags integration matches vet/test below so
# the dirs list picks up integration-tagged files too.
GO_DIRS := $(shell GOWORK=off go list -f '{{.Dir}}' -tags integration ./...)

test:
	LOOPRIG_LIVE_NETWORK=0 GOWORK=off go test -tags integration -race ./...

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
	GOWORK=off go test -race -run '^TestCrossModuleOwnership' ./...

# Every sibling repository in this ecosystem (harness, classifiers, carbon,
# and this tests module) carries the same minimal top-level marker set
# (go.mod, Makefile, LICENSE, CONTRIBUTING.md). See root_layout_test.go.
root-layout:
	GOWORK=off go test -race -run '^(TestSiblingRootLayout|TestRepositoryRootLayoutMatchesEcosystemConvention)' ./...

check: fmt-check vet dependency-boundary root-layout test

mod-check:
	@sh scripts/check-release-modfile.sh go.mod
	@test -z "$$(GOWORK=off go mod tidy -diff)" || (echo 'go.mod is not tidy' >&2; GOWORK=off go mod tidy -diff; exit 1)
	GOWORK=off go mod verify

release-check:
	$(MAKE) mod-check
	GOWORK=off go test -tags integration -race ./...
