module github.com/looprig/tests

go 1.26.4

require (
	github.com/looprig/core v0.2.0
	github.com/looprig/foreignloops v0.0.0
	github.com/looprig/fsstore v0.2.0
	github.com/looprig/harness v0.10.0
	github.com/looprig/inference v0.3.1-0.20260718005749-13e4d7f173b3
	github.com/looprig/sandbox v0.0.0
)

require github.com/looprig/storage v0.2.0

replace github.com/looprig/core => ../core

replace github.com/looprig/inference => ../inference

replace github.com/looprig/storage => ../storage

// Development-only extraction mappings. release-check consumes a separate
// go.release.mod with tagged modules and no local replacements.
replace github.com/looprig/harness => ../harness

replace github.com/looprig/foreignloops => ../foreignloops

replace github.com/looprig/fsstore => ../fsstore

replace github.com/looprig/sandbox => ../sandbox

require github.com/looprig/mcp v0.0.0

require (
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/jsonschema-go v0.4.3 // indirect
	github.com/google/nftables v0.3.0 // indirect
	github.com/landlock-lsm/go-landlock v0.9.0 // indirect
	github.com/mdlayher/netlink v1.7.3-0.20250113171957-fbb4dce95f42 // indirect
	github.com/mdlayher/socket v0.5.0 // indirect
	github.com/modelcontextprotocol/go-sdk v1.6.1 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	kernel.org/pub/linux/libs/security/libcap/psx v1.2.77 // indirect
)

replace github.com/looprig/mcp => ../mcp
