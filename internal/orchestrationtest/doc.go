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
// today. That tag is load-bearing, not cosmetic: retag these files to bare
// `integration` and `GOWORK=off go build -tags integration ./...` fails to
// resolve `factory` and `host`.
//
// This file is untagged for a NARROWER reason than was first written down here.
// The original claim was that without it `go build ./...` errors with "build
// constraints exclude all Go files". It does not -- a package with no buildable
// files is skipped by a `./...` pattern, as `GOWORK=off go build ./...`,
// `go vet ./...` and `go test -tags integration ./...` each confirm by exiting
// 0. What DOES fail is naming the package explicitly:
// `GOWORK=off go build ./internal/orchestrationtest` reports "build constraints
// exclude all Go files". That is the constraint that actually binds, and it is
// why the untagged file stays.
//
// The `orchestration` tag is a DEVIATION from the runbook's step 6 command,
// which names `-tags integration` alone. It is removed in half (b): once the two
// tags exist the imports resolve standalone and the extra constraint has nothing
// left to protect.
//
// DO NOT RUN `go mod tidy` IN THIS MODULE UNTIL HALF (b), AND KNOW WHY.
//
// It does not fail. It EXITS 0, and it silently rewrites `go.mod` and `go.sum`
// with PSEUDO-VERSION pins for `host` and `factory` -- measured here as
// go.mod +8/-6 and go.sum +60/-12, landing
// `github.com/looprig/factory v0.0.0-20260913080515-4b2b3deb649b` and
// `github.com/looprig/host v0.0.0-20260913072111-5b6b63245d7d` -- because
// tidy considers every build configuration and so sees these imports whatever
// their tags say. A pseudo-version is not a release. A command that goes red is
// a warning; this one goes green and moves your pins, which is the more
// dangerous shape and the reason this paragraph is emphatic.
//
// What IS red is the no-diff gate over it, `make mod-check`. Do not run that
// against this module until half (b) either -- and never add a `replace`.
package orchestrationtest
