//go:build integration && orchestration

package orchestrationtest

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

// The narrow drain trip-wire lived here and is REPLACED by
// AssertHostExposesNoDrainCapability in hostsurface.go, for the reason given in
// host.go: it read one syntactic form of the right subject.
//
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
