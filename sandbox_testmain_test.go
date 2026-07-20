//go:build integration

package tests

import (
	"os"
	"testing"

	"github.com/looprig/sandbox"
)

// TestMain dispatches the Sandbox Linux re-exec helper before the test suite.
// Init is a no-op on every other platform.
func TestMain(m *testing.M) {
	sandbox.Init()
	os.Exit(m.Run())
}
