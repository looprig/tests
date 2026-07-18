.PHONY: test fmt-check vet dependency-boundary local-source-check check release-check

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
	@test -f go.release.mod || (echo "go.release.mod is not prepared" >&2; exit 1)
	@! grep -Eq '^[[:space:]]*replace([[:space:]]|$$)' go.release.mod || (echo "go.release.mod contains local replace directives" >&2; exit 1)
	GOWORK=off go test -modfile=go.release.mod -tags integration -race ./...
