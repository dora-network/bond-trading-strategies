package wiring

import (
	"context"
	"log/slog"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/config"
)

// validTestKey is a 32-byte stand-in for the hex-decoded ENCRYPTION_KEY.
var validTestKey = make([]byte, 32)

// TestWireRejectsNilPool pins the fail-fast contract: Wire never
// constructs anything when the pool is missing.
func TestWireRejectsNilPool(t *testing.T) {
	if _, err := Wire(context.Background(), nil, validTestKey, testConfig(t), slog.Default()); err == nil {
		t.Fatal("expected error for nil pool")
	}
}

// TestWireRejectsShortEncryptionKey pins the fail-fast contract on the
// key length before any DB-touching construction runs.
func TestWireRejectsShortEncryptionKey(t *testing.T) {
	if _, err := Wire(context.Background(), nil, []byte("short"), testConfig(t), slog.Default()); err == nil {
		t.Fatal("expected error for short encryption key")
	}
}

// testConfig builds the minimal valid Config. DoraBaseURL is required
// by Validate; the rest ride on Load's defaults.
func testConfig(t *testing.T) (c config.Config) {
	t.Helper()
	t.Setenv("DORA_BASE_URL", "https://api.dora.co")
	c, err := config.Load()
	if err != nil {
		t.Fatalf("config load: %v", err)
	}
	return c
}
