module github.com/looprig/tests

go 1.26.4

require (
	github.com/looprig/core v0.2.0
	github.com/looprig/foreignloop v0.0.0
	github.com/looprig/fsstore v0.2.0
	github.com/looprig/harness v0.10.0
	github.com/looprig/inference v0.3.0
)

require github.com/looprig/storage v0.2.0

replace github.com/looprig/core => ../core

replace github.com/looprig/inference => ../inference

replace github.com/looprig/storage => ../storage

// Development-only extraction mappings. release-check consumes a separate
// go.release.mod with tagged modules and no local replacements.
replace github.com/looprig/harness => ../harness

replace github.com/looprig/foreignloop => ../foreignloop

replace github.com/looprig/fsstore => ../fsstore

require github.com/looprig/mcp v0.0.0

require (
	github.com/google/jsonschema-go v0.4.3 // indirect
	github.com/modelcontextprotocol/go-sdk v1.6.1 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/looprig/mcp => ../mcp
