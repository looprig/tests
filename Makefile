.PHONY: test fmt-check vet dependency-boundary local-source-check check release-check

RELEASE_MODFILE ?= go.release.mod
RELEASE_GO ?= go

test:
	GOWORK=off go test -tags integration -race ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

vet:
	GOWORK=off go vet -tags integration ./...

dependency-boundary:
	GOWORK=off go test -race -run '^TestCrossModuleOwnership' ./...

local-source-check:
	GOWORK=off go test -race -run '^TestDevelopmentModuleSources' ./...

check: fmt-check vet dependency-boundary local-source-check test

release-check:
	@test -f "$(RELEASE_MODFILE)" || (echo "$(RELEASE_MODFILE) is not prepared" >&2; exit 1)
	@sh scripts/check-release-modfile.sh "$(RELEASE_MODFILE)"
	GOWORK=off $(RELEASE_GO) test -modfile="$(RELEASE_MODFILE)" -tags integration -race ./...
