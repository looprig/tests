module github.com/looprig/tests

go 1.26.4

require (
	github.com/looprig/core v0.2.0
	github.com/looprig/fsstore v0.2.0
	github.com/looprig/harness v0.10.0
	github.com/looprig/inference v0.3.0
)

require github.com/looprig/storage v0.2.0

replace github.com/looprig/core => ../core

replace github.com/looprig/inference => ../inference

replace github.com/looprig/storage => ../storage

replace github.com/looprig/harness => ../harness

replace github.com/looprig/fsstore => ../fsstore
