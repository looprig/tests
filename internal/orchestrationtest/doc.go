// Package orchestrationtest is the black-box service test kit for the Factory
// and Host orchestration program.
//
// # Why this file carries no build constraint and every other file does
//
// The kit composes real `github.com/looprig/factory` and `github.com/looprig/host`
// objects. Neither module has a tag yet, so neither can appear in this module's
// `go.mod`, and the kit therefore only builds inside the workspace `go.work`.
// Runbook 07's I0.1 amendment accepts the kit in two halves for exactly that
// reason: (a) under `go.work` now, (b) standalone once `host v0.1.0` and
// `factory v0.1.0` exist.
//
// Every other file in this package is constrained `integration && orchestration`
// so that the module's existing standalone gate — `GOWORK=off go test -tags
// integration -race ./...`, which is what the Makefile runs — keeps passing
// today. Without this untagged file the directory would have no buildable Go
// files under that gate and `go build ./...` would fail with "build constraints
// exclude all Go files". With it, the package builds empty standalone and whole
// under `go.work -tags integration,orchestration`.
//
// The `orchestration` tag is a DEVIATION from the runbook's step 6 command,
// which names `-tags integration` alone. It is removed in half (b): once the two
// tags exist the imports resolve standalone and the extra constraint has nothing
// left to protect.
//
// `go mod tidy` is still red and cannot be made green here. Tidy considers every
// build configuration, so it sees the host/factory imports regardless of tags and
// resolves them to PSEUDO-VERSIONS from their remotes. A pseudo-version is not a
// release. Do not run `make mod-check` against this module until half (b);
// do not let tidy write those pins; and never add a `replace`.
package orchestrationtest
