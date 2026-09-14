//go:build integration && orchestration

package orchestrationtest

import (
	"reflect"
	"sort"

	"github.com/looprig/host"
)

// This file holds the kit's BLOCKED-LANE TRIP-WIRES.
//
// Each one records, in executable form, a premise that a runbook 07 task is
// blocked on. They exist for the reason AssertHostExposesNoRuntimeSurface and
// AssertFactoryComposesNoLinkPlane already exist in this kit: a task deferred in
// prose disappears, and an absence claim nobody can falsify is as wide as an
// unverified presence claim. Every assertion here FAILS on the day its blocker
// lifts, and its message names the task it unblocks.
//
// None of them is a substitute for the task. A trip-wire says "the dependency
// this case needs does not exist yet"; it says nothing about the behaviour the
// case would assert. Do not read a green here as coverage of I1.3, I1.4 or I2.3.

// hostDrainSurfaceNames are method names whose appearance on *host.Host would
// mean Host has grown the drain surface runbook 07 I2.3 needs.
//
// The list is drain-shaped rather than generic on purpose: I2.3 asks a Host to
// stop accepting, settle its residents, and report what it settled. Every name
// here is one spelling of "make that happen" or "observe it happening", and the
// set is deliberately wider than Host's internal vocabulary (its Drainer spells
// them StartDrain/ObserveDrain, and its CapacityPublisher spells one BeginDrain)
// so that renaming the internal type does not silently disarm this.
var hostDrainSurfaceNames = []string{
	"BeginDrain", "Drain", "DrainHost", "DrainSession", "ObserveDrain",
	"RequestDrain", "StartDrain",
}

// AssertHostExposesNoDrainSurface is runbook 07 I2.3's trip-wire.
//
// I2.3 asks for drain ORDERING: un-rank before admission stops, residents
// settled or typed-failed within a deadline, leases and registry entries cleared
// afterwards, a forced cancellation recorded without private payloads. Every one
// of those is an assertion about what a RUNNING Host does, and this module
// cannot make a Host run: the Drainer is host/internal/lifecycle, it is reached
// only through host/internal/realtime/hostlink's drain RPC, and both are
// unreachable from another module by the Go compiler rather than by policy.
// *host.Host itself is a validated configuration value with accessors and
// nothing else.
//
// The generic binary is not a way round it. host/cmd/host's main runs with an
// unconfiguredBootstrap whose Store returns errNoBootstrap immediately, and a
// product cannot supply its own Bootstrap because two of its five methods name
// internal/ types.
//
// So this assertion records the premise and names what lifts it. It is paired
// with AssertHostExposesNoRuntimeSurface rather than folded into it because the
// two unblock different tasks and a merged message would mis-attribute whichever
// fired.
func AssertHostExposesNoDrainSurface(tb TB) {
	tb.Helper()
	hostType := reflect.TypeOf(&host.Host{})
	found := make([]string, 0, len(hostDrainSurfaceNames))
	for _, name := range hostDrainSurfaceNames {
		if _, ok := hostType.MethodByName(name); ok {
			found = append(found, name)
		}
	}
	if len(found) == 0 {
		return
	}
	sort.Strings(found)
	tb.Fatalf("orchestrationtest: *host.Host now exports %v. Host has grown a drain surface, "+
		"so runbook 07 I2.3 is no longer blocked: drive drain ordering for real and delete this trip-wire", found)
}

// The four trip-wires that used to live below this line are DELETED, not
// inverted, because each one's blocker lifted in factory A9.1 stage 2 and each
// fired on the day it did:
//
//   - AssertFactoryExposesNoReconciler  -- *factory.Server grew Start.
//   - AssertFactoryComposesNoAdmissionPlane -- the control routes left 503.
//   - AssertFactoryComposesNoObjectPlane -- WithObjectPolicy arrived. This one
//     did NOT fire, and that is a finding rather than a reprieve: it probed the
//     KIT's composition, which still omitted the new option, so its answer never
//     changed. A trip-wire written against one's own fixture goes blind instead
//     of firing.
//   - AssertFactoryAdvertisesNoLaunchTargets -- WithDepartment arrived, same
//     blindness, same reason.
//
// AssertFactoryComposesNoLinkPlane is deleted from here too; it lived in
// metrics.go and fired on /v1/realtime leaving the 501 table.
//
// What remains is the pair above, which is still true: host v0.1.0 exposes no
// composition surface and no drain surface.
