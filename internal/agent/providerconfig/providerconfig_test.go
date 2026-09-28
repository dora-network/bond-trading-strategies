package providerconfig

import (
	"bytes"
	"errors"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
	agentsecrets "github.com/dora-network/bond-trading-strategies/internal/agent/secrets"
)

const (
	userA  = "00000000-0000-0000-0000-000000000001"
	userB  = "00000000-0000-0000-0000-000000000002"
	nobody = "00000000-0000-0000-0000-000000000099"
)

// seedUsers inserts parent rows in agent.users for userA/userB so the FK
// from agent.provider_configs.dora_user_id -> agent.users.dora_user_id is
// satisfied. Idempotent via ON CONFLICT DO NOTHING.
func seedUsers(t *testing.T, s *Store) {
	t.Helper()
	ctx := t.Context()
	rows := []struct {
		id, tenant string
	}{
		{userA, "t-1"},
		{userB, "t-1"},
	}
	for _, u := range rows {
		if _, err := s.Pool.Exec(ctx, `
			insert into agent.users (dora_user_id, tenant_id, roles)
			values ($1, $2, ARRAY['TRADER'])
			on conflict (dora_user_id) do nothing`,
			u.id, u.tenant); err != nil {
			t.Fatalf("seed user %s: %v", u.id, err)
		}
	}
}

func testStore(t *testing.T) *Store {
	t.Helper()
	sealer := agentsecrets.NewSealer(bytes.Repeat([]byte{0xAB}, 32))
	s := New(agenttest.StartPostgres(t), sealer)
	seedUsers(t, s)
	ctx := t.Context()
	// Per-test cleanup so re-runs against a shared dev DB are repeatable.
	t.Cleanup(func() {
		_, _ = s.Pool.Exec(ctx, `delete from agent.provider_configs where dora_user_id in ($1, $2)`, userA, userB)
	})
	return s
}

// TestSealOpen_MutatedCiphertextFails pins the Sealer rewrite: flipping
// one byte of the sealed value must make Open fail (AES-GCM auth tag).
func TestSealOpen_MutatedCiphertextFails(t *testing.T) {
	sealer := agentsecrets.NewSealer(bytes.Repeat([]byte{0xCD}, 32))
	sealed, err := sealer.Seal([]byte("sk-live-1234567890"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	sealed[len(sealed)-1] ^= 0xFF
	if _, err := sealer.Open(sealed); err == nil {
		t.Fatal("Open on mutated ciphertext: want error, got nil")
	}
}

func TestSetAndGetDecrypted_RoundTrip(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()

	if err := s.Set(ctx, userA, "openai", "sk-live-12345", "gpt-4o-mini", ""); err != nil {
		t.Fatalf("Set: %v", err)
	}
	key, model, baseURL, err := s.GetDecrypted(ctx, userA, "openai")
	if err != nil {
		t.Fatalf("GetDecrypted: %v", err)
	}
	if key != "sk-live-12345" {
		t.Errorf("key: want sk-live-12345, got %s", key)
	}
	if model != "gpt-4o-mini" {
		t.Errorf("model: %s", model)
	}
	if baseURL != "" {
		t.Errorf("baseURL: want empty, got %s", baseURL)
	}
}

func TestSet_WithBaseURL(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	if err := s.Set(ctx, userA, "openai", "k", "m", "https://openrouter.ai/api/v1"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	_, _, baseURL, err := s.GetDecrypted(ctx, userA, "openai")
	if err != nil {
		t.Fatal(err)
	}
	if baseURL != "https://openrouter.ai/api/v1" {
		t.Errorf("baseURL: %s", baseURL)
	}
}

func TestGetDecrypted_NotFound(t *testing.T) {
	s := testStore(t)
	_, err := s.Get(t.Context(), nobody, "openai")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("want ErrNotFound, got %v", err)
	}
}

func TestSet_Overwrites(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	_ = s.Set(ctx, userA, "openai", "old", "m1", "")
	_ = s.Set(ctx, userA, "openai", "new", "m2", "")
	key, model, _, _ := s.GetDecrypted(ctx, userA, "openai")
	if key != "new" || model != "m2" {
		t.Errorf("after overwrite: key=%s model=%s", key, model)
	}
}

func TestList_MasksKeysAndScopesByUser(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	_ = s.Set(ctx, userA, "openai", "sk-live-1234567890ABCD", "gpt-4o-mini", "")
	_ = s.Set(ctx, userA, "anthropic", "sk-ant-abcdefghij1234", "claude-haiku-4-5", "https://api.anthropic.com")
	_ = s.Set(ctx, userB, "openai", "sk-other-zzzz9999", "gpt-4o", "")

	got, err := s.List(ctx, userA)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len: want 2 for userA, got %d (%+v)", len(got), got)
	}
	// Sorted by provider name -> anthropic before openai.
	if got[0].Provider != "anthropic" {
		t.Errorf("got[0].Provider: want anthropic, got %s", got[0].Provider)
	}
	if got[0].DefaultModel != "claude-haiku-4-5" {
		t.Errorf("got[0].DefaultModel: %s", got[0].DefaultModel)
	}
	if got[0].BaseURL != "https://api.anthropic.com" {
		t.Errorf("got[0].BaseURL: %s", got[0].BaseURL)
	}
	if got[0].APIKeyMasked != "sk-...1234" {
		t.Errorf("got[0].APIKeyMasked: want sk-...1234, got %s", got[0].APIKeyMasked)
	}
	if got[1].Provider != "openai" {
		t.Errorf("got[1].Provider: want openai, got %s", got[1].Provider)
	}
	if got[1].APIKeyMasked != "sk-...ABCD" {
		t.Errorf("got[1].APIKeyMasked: want sk-...ABCD, got %s", got[1].APIKeyMasked)
	}
	if got[1].BaseURL != "" {
		t.Errorf("got[1].BaseURL: want empty, got %s", got[1].BaseURL)
	}
}

func TestList_Empty(t *testing.T) {
	s := testStore(t)
	got, err := s.List(t.Context(), nobody)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len: want 0, got %d", len(got))
	}
}

func TestMaskAPIKey(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"sk-live-1234567890ABCD", "sk-...ABCD"},
		{"abcdefgh", "abc...efgh"},
		{"short", "***"},
		{"", "***"},
		{"12345678", "123...5678"},
	}
	for _, c := range cases {
		if got := maskAPIKey(c.in); got != c.want {
			t.Errorf("maskAPIKey(%q): want %q, got %q", c.in, c.want, got)
		}
	}
}

func TestDelete(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	_ = s.Set(ctx, userA, "openai", "k", "m", "")
	if err := s.Delete(ctx, userA, "openai"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := s.Get(ctx, userA, "openai")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("after delete: want ErrNotFound, got %v", err)
	}
}
