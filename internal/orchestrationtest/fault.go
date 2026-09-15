//go:build integration

package orchestrationtest

import (
	"context"
	"errors"
	"io"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// ErrInjectedStoreFault is the fault the kit injects into the durable plane.
var ErrInjectedStoreFault = errors.New("orchestrationtest: injected store fault")

// FaultyReader wraps the REAL SessionReader and fails the chosen operation.
//
// It wraps rather than replaces for a reason: a replacement returns zero values
// on the happy path, so a case that exercises both the faulted and unfaulted
// operation is silently reading a fake for the second one. Here the unfaulted
// path is the store's own answer, byte for byte.
//
// Arm/Fired is the loud half. A fault that is armed and never reached means the
// case stopped exercising the operation it names -- the request is being served
// from somewhere else, or the route changed -- and AssertFired turns that into
// a failure instead of a green test that proves nothing about faults.
type FaultyReader struct {
	Inner interface {
		ListSessions(context.Context, sessionstore.ListSessionsRequest) (sessionstore.SessionPage, error)
		GetCatalogEntry(context.Context, sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error)
		ReadPublicJournal(context.Context, sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error)
		ReadGates(context.Context, sessionstore.ReadGatesRequest) (sessionwire.GatePage, error)
		GetObject(context.Context, sessionstore.GetObjectRequest) (io.ReadCloser, error)
		GetObjectMetadata(context.Context, sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error)
	}

	mu    sync.Mutex
	armed map[string]bool
	fired map[string]int
}

// NewFaultyReader wraps inner.
func NewFaultyReader(inner *sessionstore.Store) *FaultyReader {
	return &FaultyReader{Inner: inner, armed: make(map[string]bool), fired: make(map[string]int)}
}

// Arm makes the named operation fail until Disarm.
func (r *FaultyReader) Arm(operation string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.armed[operation] = true
}

// Disarm restores the named operation.
func (r *FaultyReader) Disarm(operation string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.armed, operation)
}

// Fired reports how many times the named operation was faulted.
func (r *FaultyReader) Fired(operation string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fired[operation]
}

// AssertFired fails unless the named operation was faulted at least once.
func (r *FaultyReader) AssertFired(tb TB, operation string) {
	tb.Helper()
	if r.Fired(operation) == 0 {
		tb.Fatalf("orchestrationtest: fault %q was armed and never reached; the case is no longer exercising it", operation)
	}
}

func (r *FaultyReader) faulted(operation string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.armed[operation] {
		return false
	}
	r.fired[operation]++
	return true
}

// ListSessions satisfies factory.SessionReader.
func (r *FaultyReader) ListSessions(ctx context.Context, req sessionstore.ListSessionsRequest) (sessionstore.SessionPage, error) {
	if r.faulted("ListSessions") {
		return sessionstore.SessionPage{}, ErrInjectedStoreFault
	}
	return r.Inner.ListSessions(ctx, req)
}

// GetCatalogEntry satisfies factory.SessionReader.
func (r *FaultyReader) GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	if r.faulted("GetCatalogEntry") {
		return sessionstore.CatalogEntry{}, ErrInjectedStoreFault
	}
	return r.Inner.GetCatalogEntry(ctx, req)
}

// ReadPublicJournal satisfies factory.SessionReader.
func (r *FaultyReader) ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	if r.faulted("ReadPublicJournal") {
		return sessionwire.JournalPage{}, ErrInjectedStoreFault
	}
	return r.Inner.ReadPublicJournal(ctx, req)
}

// ReadGates satisfies factory.SessionReader.
func (r *FaultyReader) ReadGates(ctx context.Context, req sessionstore.ReadGatesRequest) (sessionwire.GatePage, error) {
	if r.faulted("ReadGates") {
		return sessionwire.GatePage{}, ErrInjectedStoreFault
	}
	return r.Inner.ReadGates(ctx, req)
}

// GetObject satisfies factory.SessionReader.
func (r *FaultyReader) GetObject(ctx context.Context, req sessionstore.GetObjectRequest) (io.ReadCloser, error) {
	if r.faulted("GetObject") {
		return nil, ErrInjectedStoreFault
	}
	return r.Inner.GetObject(ctx, req)
}

// GetObjectMetadata satisfies factory.SessionReader.
func (r *FaultyReader) GetObjectMetadata(ctx context.Context, req sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error) {
	if r.faulted("GetObjectMetadata") {
		return sessionwire.ObjectMetadata{}, ErrInjectedStoreFault
	}
	return r.Inner.GetObjectMetadata(ctx, req)
}
