//go:build integration && orchestration

package orchestrationtest

import (
	"context"
	"net/http"
	"reflect"
	"sort"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
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

// factoryReconcilerSurfaceNames are method names whose appearance on
// *factory.Server would mean Factory has grown a runnable reconciliation sweep.
var factoryReconcilerSurfaceNames = []string{
	"Reconcile", "ReconcileGates", "ReconcileOnce", "Start", "Sweep",
	"SweepDue", "SweepGates", "SweepOnce",
}

// AssertFactoryExposesNoReconciler is runbook 07 I1.3 cases 2-4's trip-wire.
//
// Those three cases are assertions about a SWEEP: that a fixed shard rotation
// visits every shard in bounded pages independent of tenant count, that terminal
// not_due commands never consume a due page and that two replicas do not create
// duplicate placements, and that gate deadline intents retire while open gates
// are left alone. Factory has both sweepers -- internal/admission.Reconciler and
// internal/reconcile's gate sweep -- and factory.New constructs NEITHER, so
// nothing in a composed deployment ever calls one. Both packages are internal,
// so this module cannot construct one either.
//
// Reimplementing the rotation here would not discharge the task: it would assert
// that the test's own sweep is bounded, which is a fact about the test.
//
// The probe deliberately does not stop at reflection. Reflection can only see
// the names Factory exports today, and a sweep wired to a TIMER inside Serve
// would export nothing at all -- which is why the paired case drives every route
// with PanicCommands composed, so a sweep that reaches the durable command plane
// by any route announces itself.
func AssertFactoryExposesNoReconciler(tb TB) {
	tb.Helper()
	serverType := reflect.TypeOf(&factory.Server{})
	found := make([]string, 0, len(factoryReconcilerSurfaceNames))
	for _, name := range factoryReconcilerSurfaceNames {
		if _, ok := serverType.MethodByName(name); ok {
			found = append(found, name)
		}
	}
	if len(found) == 0 {
		return
	}
	sort.Strings(found)
	tb.Fatalf("orchestrationtest: *factory.Server now exports %v. Factory has grown a runnable sweep, "+
		"so runbook 07 I1.3 cases 2-4 are no longer blocked: drive the shard and gate sweeps for real "+
		"and delete this trip-wire", found)
}

// ControlRoute is one of Factory's POST control routes and the status it answers
// in a composition whose admission service is nil.
type ControlRoute struct {
	// Suffix is appended to /v1/sessions/{sid}.
	Suffix string
	// Body is a syntactically valid request body. It is sent so that a 503 is
	// known to come from the nil admission service rather than from a body the
	// chain refused first.
	Body string
}

// NotComposedControlRoutes are the four control routes that answer 503 because
// factory.New composes no admission service.
//
// The kit's older NotComposedRoutes map could not hold them: it is probed with
// the GET helper, these routes are POST-only, and a 405 from a GET would make
// the row pass for the wrong reason. FactoryFixture.Post exists to close that.
var NotComposedControlRoutes = []ControlRoute{
	{Suffix: "/input", Body: `{"text":"hello"}`},
	{Suffix: "/interrupt", Body: `{}`},
	{Suffix: "/restore", Body: `{}`},
	{Suffix: "/gates/gate-1", Body: `{"answer":"allow"}`},
}

// AssertFactoryComposesNoAdmissionPlane records, in executable form, that no
// control command can be admitted through a composed Factory.
//
// It is the blocker behind runbook 07 I1.2 and I1.3 case 1's accept/apply leg,
// and behind I2.3's "new admission" half. A 503 here is Factory saying the
// DEPLOYMENT cannot carry the command out, not that the caller's command was
// refused -- and that distinction is the reason this must be probed rather than
// assumed: a 501, a 400 or a 404 would all mean something else about the route.
func AssertFactoryComposesNoAdmissionPlane(tb TB, ctx context.Context, f *FactoryFixture, session sessionwire.SessionID) {
	tb.Helper()
	for _, route := range NotComposedControlRoutes {
		path := SessionPath(session, route.Suffix)
		status, body := f.Post(tb, ctx, path, []byte(route.Body))
		if status != http.StatusServiceUnavailable {
			tb.Fatalf("orchestrationtest: POST %s answered %d, want 503. Factory has composed an admission "+
				"service, so runbook 07 I1.2, I1.3 case 1 and I2.3's admission half are no longer blocked: "+
				"admit commands for real and delete this trip-wire (body %s)", path, status, truncate(body))
			return
		}
	}
}

// AssertFactoryComposesNoObjectPlane records that no authorized object read can
// be served by a composed Factory.
//
// This is the leg of the carried I1.1-hostgone criterion that CANNOT be closed
// here, and the reason is worth stating exactly. The criterion is "a tool
// capture whose Host and workspace are already gone is still readable, and the
// read issues no Host request". factory.New composes no ObjectPolicy and no
// ResolveObjectStore, and internal/httpapi/objects.go checks the nil policy
// BEFORE the catalog summary, before authorization and before any reader. So the
// object route answers 503 for every session in every state -- gone Host or
// live Host, existing object or not.
//
// A case that read 503 here and called it "the Host is gone" would be asserting
// a constant. That is the exact defect U3.2 recorded against this criterion when
// it first deferred it: "no Host request was issued" becomes a type-level
// tautology rather than a probe. It is recorded as still owed, with a trip-wire,
// rather than closed with a row that cannot discriminate.
// notComposedObjectSuffixes are the two object routes probed above. It is a
// package variable rather than a literal so the kit's own positive control can
// stand a SERVED route in for one and prove the assertion reads the answer.
var notComposedObjectSuffixes = []string{"/objects/obj-1", "/objects/obj-1/metadata"}

func AssertFactoryComposesNoObjectPlane(tb TB, ctx context.Context, f *FactoryFixture, session sessionwire.SessionID) {
	tb.Helper()
	for _, suffix := range notComposedObjectSuffixes {
		path := SessionPath(session, suffix)
		status, body := f.Get(tb, ctx, path)
		if status != http.StatusServiceUnavailable {
			tb.Fatalf("orchestrationtest: GET %s answered %d, want 503. Factory has composed an object "+
				"plane, so the object-read leg of I1.1-hostgone is no longer blocked: read a retained "+
				"capture with its Host gone and delete this trip-wire (body %s)", path, status, truncate(body))
			return
		}
	}
}

// AssertFactoryAdvertisesNoLaunchTargets records that /v1/agents is structurally
// empty in every composition this module can build.
//
// Runbook 07 I1.1 case 1 asks for agents to be paged with every Host stopped.
// The route is served and it is real, but its answer is an aggregate over the
// deployment's configured launch templates, and factory.New exposes no option
// that supplies one: httpapi.RouterConfig.Department is an internal type and
// composeRouter passes no value for it. With no templates there is no target to
// probe, so the Directory is never consulted and the answer is the empty list
// whether every Host is running or none is.
//
// So the agents leg of case 1 is reachable but not DISCRIMINATING, and this
// records which of the two it is. It fires the day a composition can advertise a
// target -- at which point the agents leg becomes a real cold-read case.
// emptyAgentsBody is the answer a deployment with no launch templates gives. It
// is a variable for notComposedObjectSuffixes' reason.
var emptyAgentsBody = `{"agents":[]}`

func AssertFactoryAdvertisesNoLaunchTargets(tb TB, ctx context.Context, f *FactoryFixture) {
	tb.Helper()
	want := emptyAgentsBody
	status, body := f.Get(tb, ctx, "/v1/agents")
	if status != http.StatusOK {
		tb.Fatalf("orchestrationtest: GET /v1/agents answered %d, want 200 (body %s)", status, truncate(body))
		return
	}
	if truncate(body) != want {
		tb.Fatalf("orchestrationtest: GET /v1/agents answered %s, want %s. Factory can now advertise a "+
			"launch target, so the agents leg of runbook 07 I1.1 case 1 is discriminating: assert it "+
			"against the target directory and delete this trip-wire", truncate(body), want)
	}
}
