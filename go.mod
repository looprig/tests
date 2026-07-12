module github.com/looprig/tests

go 1.26.4

require (
	github.com/looprig/core v0.1.0
	github.com/looprig/fsstore v0.1.0
	github.com/looprig/harness v0.5.1
	github.com/looprig/inference v0.1.0
)

require (
	github.com/looprig/storage v0.1.0
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/text v0.37.0 // indirect
)

replace github.com/looprig/core => ../core

replace github.com/looprig/inference => ../inference

replace github.com/looprig/storage => ../storage

replace github.com/looprig/harness => ../harness

replace github.com/looprig/fsstore => ../fsstore
