//go:build integration

package orchestrationtest

// This file holds the kit's BLOCKED-LANE TRIP-WIRES.
//
// Each one records, in executable form, a premise that a runbook 07 task is
// blocked on. They exist for the reason the kit's Host and Factory trip-wires
// always have: a task deferred in
// prose disappears, and an absence claim nobody can falsify is as wide as an
// unverified presence claim. Every assertion here FAILS on the day its blocker
// lifts, and its message names the task it unblocks.
//
// None of them is a substitute for the task. A trip-wire says "the dependency
// this case needs does not exist yet"; it says nothing about the behaviour the
// case would assert. Do not read a green here as coverage of I1.3, I1.4 or I2.3.

// The narrow drain trip-wire lived here, was widened into
// AssertHostExposesNoDrainCapability in hostsurface.go, and FIRED on the host
// v0.2.1 pin together with its runtime twin: host now exports Compose, Service
// and Run, and advertises hostlink.drain over HostLink. Both are deleted; the
// Host trip-wire is now AssertHostCapabilities, which pins the capability set a
// running Host declares on the wire (hostsurface.go says why that and not a
// name scan).
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
// Nothing of the old Host pair remains: host v0.2.1 has both a composition
// surface and a drain surface. The I1.1 cases 3-4 / I1.4 blocker MOVED to
// Factory, which relays no Host publication; see
// AssertFactorySubscribesToNoHostChannel.
