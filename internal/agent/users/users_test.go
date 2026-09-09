package users_test

import (
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/users"
)

const testUserID = "11111111-1111-4111-8111-111111111111"

func TestEnsure_InsertsNewUser(t *testing.T) {
	pool := agenttest.StartPostgres(t)
	ctx := t.Context()
	s, err := users.New(ctx, pool)
	if err != nil {
		t.Fatalf("users.New: %v", err)
	}
	if err := s.Ensure(ctx, testUserID, "tenant-A", []string{"TRADER"}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	var got string
	if err := pool.QueryRow(ctx, `select tenant_id from agent.users where dora_user_id = $1`, testUserID).Scan(&got); err != nil {
		t.Fatalf("select: %v", err)
	}
	if got != "tenant-A" {
		t.Errorf("tenant_id: want tenant-A, got %q", got)
	}
}

func TestEnsure_UpdatesRolesOnConflict(t *testing.T) {
	pool := agenttest.StartPostgres(t)
	ctx := t.Context()
	s, _ := users.New(ctx, pool)
	if err := s.Ensure(ctx, testUserID, "tenant-A", []string{"TRADER"}); err != nil {
		t.Fatalf("first Ensure: %v", err)
	}
	if err := s.Ensure(ctx, testUserID, "tenant-A", []string{"TRADER", "ADMIN"}); err != nil {
		t.Fatalf("second Ensure: %v", err)
	}
	var roles []string
	if err := pool.QueryRow(ctx, `select roles from agent.users where dora_user_id = $1`, testUserID).Scan(&roles); err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(roles) != 2 || roles[0] != "TRADER" || roles[1] != "ADMIN" {
		t.Errorf("roles after update: want [TRADER ADMIN], got %v", roles)
	}
}

func TestStoreKey_RoundTrip(t *testing.T) {
	pool := agenttest.StartPostgres(t)
	ctx := t.Context()
	s, _ := users.New(ctx, pool)
	if err := s.Ensure(ctx, testUserID, "tenant-A", []string{"TRADER"}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	key := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if err := s.StoreKey(ctx, testUserID, key); err != nil {
		t.Fatalf("StoreKey: %v", err)
	}
	got, err := s.GetEncryptedKey(ctx, testUserID)
	if err != nil {
		t.Fatalf("GetEncryptedKey: %v", err)
	}
	if string(got) != string(key) {
		t.Errorf("key round-trip: want %v, got %v", key, got)
	}
}

func TestEnsure_RejectsEmptyUserID(t *testing.T) {
	pool := agenttest.StartPostgres(t)
	s, _ := users.New(t.Context(), pool)
	err := s.Ensure(t.Context(), "", "tenant-A", []string{"TRADER"})
	if err == nil {
		t.Fatal("want error for empty DoraUserID")
	}
}

func TestNew_RejectsNilPool(t *testing.T) {
	_, err := users.New(t.Context(), nil)
	if err == nil {
		t.Fatal("want error for nil pool")
	}
}
