//go:build integration && orchestration

package orchestrationtest

import (
	"context"
	"errors"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/host/department"
)

// ErrRigRefused is what a FakeRig returns when it is configured to refuse.
var ErrRigRefused = errors.New("orchestrationtest: the rig refused to launch")

// ErrUnframedCommand is what a FakeRuntime returns for a command the real
// adapter would never hand it. See FakeRuntime.ApplyCommand.
var ErrUnframedCommand = errors.New("orchestrationtest: runtime command carries no runtime command id")

// FakeRig is the kit's one embedded Department launch target, per runbook 07
// I0.1 step 2. It is a fake of a PRODUCT seam -- department.Rig is the
// interface a product implements to plug Harness in -- not a fake of a service
// or of a link, so it does not stand between two things the kit could have
// driven for real.
//
// It is faithful to what department.NewRigTarget's adapter actually does:
// the adapter type-asserts the returned RigSession for each of six segregated
// capabilities and reports every missing one in an *IncapableRuntimeError, so
// "a session missing capability X" must be a DISTINCT GO TYPE. Go method sets
// are not conditional, so a single struct with a nil func field would be a
// fake that is looser than the dependency: it would satisfy every assertion
// and the adapter's discovery would never be exercised.
type FakeRig struct {
	Session    department.RigSession
	CreateErr  error
	RestoreErr error

	mu       sync.Mutex
	creates  []department.RigCreateRequest
	restores []department.RigRestoreRequest
}

// NewSession satisfies department.Rig.
func (r *FakeRig) NewSession(_ context.Context, req department.RigCreateRequest) (department.RigSession, error) {
	r.mu.Lock()
	r.creates = append(r.creates, req)
	r.mu.Unlock()
	if r.CreateErr != nil {
		return nil, r.CreateErr
	}
	return r.Session, nil
}

// RestoreSession satisfies department.Rig.
func (r *FakeRig) RestoreSession(_ context.Context, _ uuid.UUID, req department.RigRestoreRequest) (department.RigSession, error) {
	r.mu.Lock()
	r.restores = append(r.restores, req)
	r.mu.Unlock()
	if r.RestoreErr != nil {
		return nil, r.RestoreErr
	}
	return r.Session, nil
}

// Creates reports every create the rig was asked for.
func (r *FakeRig) Creates() []department.RigCreateRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]department.RigCreateRequest(nil), r.creates...)
}

// Restores reports every restore the rig was asked for.
func (r *FakeRig) Restores() []department.RigRestoreRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]department.RigRestoreRequest(nil), r.restores...)
}

// Launches reports the total number of launches.
func (r *FakeRig) Launches() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.creates) + len(r.restores)
}

// FakeRuntime is a RigSession carrying ALL six segregated capabilities.
type FakeRuntime struct {
	id uuid.UUID

	mu         sync.Mutex
	applied    []department.RuntimeCommand
	released   int
	subscribed []sessionwire.EventID
	epoch      uint64
	held       bool

	done *Signal
}

// NewFakeRuntime returns a fully capable runtime session.
func NewFakeRuntime(id uuid.UUID) *FakeRuntime {
	return &FakeRuntime{id: id, epoch: 1, held: true, done: NewSignal()}
}

// ID satisfies department.RigSession.
func (r *FakeRuntime) ID() uuid.UUID { return r.id }

// WaitIdle satisfies department.IdleWaiter.
func (r *FakeRuntime) WaitIdle(ctx context.Context) error { return ctx.Err() }

// Done satisfies department.Liveness.
func (r *FakeRuntime) Done() <-chan struct{} { return r.done.ch }

// Stop closes the liveness channel.
func (r *FakeRuntime) Stop() { r.done.Fire() }

// ReleaseResidency satisfies department.Releaser.
func (r *FakeRuntime) ReleaseResidency(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released++
	return nil
}

// Released reports how many times residency was released.
func (r *FakeRuntime) Released() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.released
}

// SubscribeCommitted satisfies department.PublicationSubscriber.
func (r *FakeRuntime) SubscribeCommitted(_ context.Context, after sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	r.mu.Lock()
	r.subscribed = append(r.subscribed, after)
	r.mu.Unlock()
	ch := make(chan sessionwire.EnduringPublication)
	close(ch)
	return ch, nil
}

// SubscribedAfter reports every subscription position asked for.
func (r *FakeRuntime) SubscribedAfter() []sessionwire.EventID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sessionwire.EventID(nil), r.subscribed...)
}

// ApplyCommand satisfies department.CommandApplier.
//
// It REFUSES a command whose RuntimeCommandID is the zero UUID. That refusal
// is the fake's loud failure and it is the point of this method: the real
// Host mints a runtime UUID for every command it applies and correlates it
// with the public CommandID, so a zero id means the case stopped exercising
// the correlation it claims to exercise. A fake that accepted it would let
// the case keep passing while proving nothing -- which is strictly worse than
// having no fake, because the green is now evidence of nothing.
func (r *FakeRuntime) ApplyCommand(_ context.Context, cmd department.RuntimeCommand) error {
	if cmd.RuntimeCommandID.IsZero() {
		return ErrUnframedCommand
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = append(r.applied, cmd)
	return nil
}

// Applied reports every command the runtime accepted.
func (r *FakeRuntime) Applied() []department.RuntimeCommand {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]department.RuntimeCommand(nil), r.applied...)
}

// LeaseEpoch satisfies department.LeaseEpochReporter.
func (r *FakeRuntime) LeaseEpoch() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.epoch, r.held
}

// SetLeaseEpoch moves the reported epoch.
func (r *FakeRuntime) SetLeaseEpoch(epoch uint64, held bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.epoch, r.held = epoch, held
}

// BareRuntime is a RigSession with IDENTITY ONLY -- a distinct type, missing
// every segregated capability, used to prove the adapter's capability
// discovery actually discovers.
type BareRuntime struct{ id uuid.UUID }

// NewBareRuntime returns an identity-only session.
func NewBareRuntime(id uuid.UUID) *BareRuntime { return &BareRuntime{id: id} }

// ID satisfies department.RigSession.
func (r *BareRuntime) ID() uuid.UUID { return r.id }

// KitCapabilities is the minimal Capabilities a kit target declares.
//
// CaptureSafety is streaming rather than the zero value because
// department.Capabilities.Validate refuses a pooled-supporting target whose
// capture safety does not permit pooling; the zero value is "unknown" and is
// refused. Do not relax this to make a composition build.
func KitCapabilities() department.Capabilities {
	return department.Capabilities{
		SupportsPooled:    true,
		SupportsDedicated: true,
		AdmissionWeight:   1,
		CaptureSafety:     department.CaptureSafetyStreaming,
	}
}

// NewDepartment builds a REAL *department.Department around one agent served
// by rig. Only the Rig is a fake; NewRigTarget, the capability adapter and the
// registry are the product's own code.
func NewDepartment(tb TB, agent sessionwire.AgentID, compatibility department.CompatibilityID, rig department.Rig) *department.Department {
	tb.Helper()
	target, err := department.NewRigTarget(rig, compatibility, KitCapabilities())
	if err != nil {
		tb.Fatalf("orchestrationtest: building a rig target: %v", err)
		return nil
	}
	dept, err := department.New([]department.Registration{{AgentID: agent, Target: target}})
	if err != nil {
		tb.Fatalf("orchestrationtest: building a department: %v", err)
		return nil
	}
	return dept
}
