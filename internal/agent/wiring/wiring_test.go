package wiring

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/config"
	"github.com/dora-network/bond-trading-strategies/internal/agent/secrets"
	"github.com/dora-network/bond-trading-strategies/internal/agent/users"
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

// TestEnsurePrincipal_CreatesUserRowIfMissing pins the regression that
// POST /v1/agent/provider-config returned 500 with no agent.users row
// to satisfy the FK. The host's agentPrincipalBridge calls
// EnsurePrincipal on every authenticated request so the FK target
// exists before the per-user write handler runs.
func TestEnsurePrincipal_CreatesUserRowIfMissing(t *testing.T) {
	pool := agenttest.StartPostgres(t)
	ctx := t.Context()

	usersStore, err := users.New(ctx, pool)
	if err != nil {
		t.Fatalf("users.New: %v", err)
	}
	sealer := secrets.NewSealer(bytes.Repeat([]byte{0xAB}, 32))

	const userID = "00000000-0000-0000-0000-000000000099"
	_, _ = pool.Exec(ctx, `delete from agent.users where dora_user_id = $1`, userID)

	rt := &Runtime{users: usersStore, sealer: sealer}
	if err := rt.EnsurePrincipal(ctx, userID, "tenant-T"); err != nil {
		t.Fatalf("EnsurePrincipal: %v", err)
	}

	var tenant string
	var roles []string
	if err := pool.QueryRow(ctx,
		`select tenant_id, roles from agent.users where dora_user_id = $1`, userID,
	).Scan(&tenant, &roles); err != nil {
		t.Fatalf("user not found after EnsurePrincipal: %v", err)
	}
	if tenant != "tenant-T" {
		t.Errorf("tenant: want %q, got %q", "tenant-T", tenant)
	}
	// Placeholder roles; see TODO in EnsurePrincipal. When role
	// gating returns, these will flow in from the host auth context.
	wantRoles := map[string]bool{"TRADER": true, "ADMIN": true}
	if len(roles) != 2 || !wantRoles[roles[0]] || !wantRoles[roles[1]] {
		t.Errorf("roles: want [TRADER ADMIN] in any order, got %v", roles)
	}
}

// TestEnsurePrincipal_Idempotent confirms a second call updates
// tenant/roles but does not error — the bridge calls it on every
// request.
func TestEnsurePrincipal_Idempotent(t *testing.T) {
	pool := agenttest.StartPostgres(t)
	ctx := t.Context()
	usersStore, _ := users.New(ctx, pool)
	sealer := secrets.NewSealer(bytes.Repeat([]byte{0xAB}, 32))

	const userID = "00000000-0000-0000-0000-000000000098"
	_, _ = pool.Exec(ctx, `delete from agent.users where dora_user_id = $1`, userID)

	rt := &Runtime{users: usersStore, sealer: sealer}
	if err := rt.EnsurePrincipal(ctx, userID, "tenant-A"); err != nil {
		t.Fatalf("first EnsurePrincipal: %v", err)
	}
	if err := rt.EnsurePrincipal(ctx, userID, "tenant-B"); err != nil {
		t.Fatalf("second EnsurePrincipal: %v", err)
	}
	var tenant string
	if err := pool.QueryRow(ctx,
		`select tenant_id from agent.users where dora_user_id = $1`, userID,
	).Scan(&tenant); err != nil {
		t.Fatalf("user not found: %v", err)
	}
	if tenant != "tenant-B" {
		t.Errorf("tenant: want tenant-B after second call, got %q", tenant)
	}
}

// TestStorePrincipalKey_RoundTrip confirms the stored api_key decrypts
// to the original value via the same Sealer. This is the live-deployment
// crash-recovery path.
func TestStorePrincipalKey_RoundTrip(t *testing.T) {
	pool := agenttest.StartPostgres(t)
	ctx := t.Context()
	usersStore, _ := users.New(ctx, pool)
	sealer := secrets.NewSealer(bytes.Repeat([]byte{0xAB}, 32))

	const userID = "00000000-0000-0000-0000-000000000097"
	const apiKey = "dora.sk-test-roundtrip-1234567890"
	_, _ = pool.Exec(ctx, `delete from agent.users where dora_user_id = $1`, userID)
	if err := usersStore.Ensure(ctx, userID, "t", []string{"TRADER"}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	rt := &Runtime{users: usersStore, sealer: sealer}
	if err := rt.StorePrincipalKey(ctx, userID, apiKey); err != nil {
		t.Fatalf("StorePrincipalKey: %v", err)
	}

	sealed, err := usersStore.GetEncryptedKey(ctx, userID)
	if err != nil {
		t.Fatalf("GetEncryptedKey: %v", err)
	}
	if bytes.Equal(sealed, []byte(apiKey)) {
		t.Fatal("api_key stored as plaintext; expected sealed ciphertext")
	}
	plain, err := sealer.Open(sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if string(plain) != apiKey {
		t.Errorf("Open: want %q, got %q", apiKey, string(plain))
	}
}
