//go:build integration && orchestration

package orchestrationtest

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/looprig/core/uuid"
)

func mustKitUUID() uuid.UUID {
	return uuid.MustParse("0f1e2d3c-4b5a-4998-8776-655443322110")
}

func hashForPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:8])
}
