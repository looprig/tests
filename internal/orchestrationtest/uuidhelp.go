//go:build integration && orchestration

package orchestrationtest

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/looprig/core/uuid"
)

// mustKitUUID is ONE package-level UUID handed to every FakeRuntime, which is
// M12's exact shape: a constant that collapses two things into one.
//
// It is inert TODAY only because no assertion reads RigSession.ID(), so no
// comparison can collapse. The first case that correlates two runtimes by id
// must mint distinct ones instead -- nothing here guards that, and this comment
// is the only warning.
func mustKitUUID() uuid.UUID {
	return uuid.MustParse("0f1e2d3c-4b5a-4998-8776-655443322110")
}

// hashForPath maps a (tenant, session) key to a distinct directory name.
//
// Distinctness is the whole contract: a constant here would give every tenant
// and every session one shared workspace directory. It is rowed by the kit's
// own seam-driving case, without which a constant survives mutation.
func hashForPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:8])
}
