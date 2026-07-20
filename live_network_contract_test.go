package tests

import (
	"os"
	"strings"
	"testing"
)

func TestLiveNetworkSuiteIsExplicitAndFailClosed(t *testing.T) {
	sourceBytes, err := os.ReadFile("sandbox_integration_test.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	for _, required := range []string{
		`os.Getenv("LOOPRIG_LIVE_NETWORK") != "1"`,
		`set LOOPRIG_LIVE_NETWORK=1`,
	} {
		if !strings.Contains(source, required) {
			t.Errorf("sandbox integration source missing %q", required)
		}
	}
	helperStart := strings.Index(source, "func requireExternalHTTPS")
	helperEnd := strings.Index(source, "func shellQuote")
	if helperStart < 0 || helperEnd <= helperStart {
		t.Fatal("cannot locate requireExternalHTTPS helper")
	}
	helper := source[helperStart:helperEnd]
	if strings.Contains(helper, "t.Skip") {
		t.Error("opted-in external HTTPS prerequisite helper still skips")
	}
	if !strings.Contains(helper, "t.Fatal") {
		t.Error("opted-in external HTTPS prerequisite helper is not fail-closed")
	}

	makefileBytes, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatal(err)
	}
	makefile := string(makefileBytes)
	for _, required := range []string{
		"test:\n\tLOOPRIG_LIVE_NETWORK=0",
		"live-network:",
		"LOOPRIG_LIVE_NETWORK=1",
		"TestSandboxBroadNetworkGrantCarriesDNS",
	} {
		if !strings.Contains(makefile, required) {
			t.Errorf("Makefile missing %q", required)
		}
	}
}
