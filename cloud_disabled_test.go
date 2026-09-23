//go:build integration && !cloud

package tests

import "testing"

// cloudSessionStoreProviders is empty unless the `cloud` build tag compiles the
// P3.1 lane in; see cloud_sessionstore_integration_test.go.
func cloudSessionStoreProviders(*testing.T) []sessionStoreProvider { return nil }

// cloudRequiredSessionStoreProviders names no extra backend without the lane.
func cloudRequiredSessionStoreProviders() []string { return nil }
