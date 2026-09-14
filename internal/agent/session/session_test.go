package session

import (
	"errors"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
)

const (
	userA = "00000000-0000-0000-0000-000000000001"
	userB = "00000000-0000-0000-0000-000000000002"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s := New(agenttest.StartPostgres(t))
	seedUsers(t, s, userA, userB)
	return s
}

// seedUsers inserts the given dora_user_id values into agent.users so the
// FK on agent.sessions(dora_user_id) is satisfied. Idempotent via on
// conflict do nothing.
func seedUsers(t *testing.T, s *Store, ids ...string) {
	t.Helper()
	ctx := t.Context()
	const q = `
		insert into agent.users (dora_user_id, tenant_id, roles)
		values ($1, $2, ARRAY['TRADER'])
		on conflict (dora_user_id) do nothing`
	for _, id := range ids {
		if _, err := s.Pool.Exec(ctx, q, id, "test-tenant"); err != nil {
			t.Fatalf("seed user %s: %v", id, err)
		}
	}
}

func TestCreateAndGetSession_OwnershipEnforced(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()

	created, err := s.CreateSession(ctx, userA, "openai", "gpt-4o-mini", "title")
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	// Owner reads it.
	_, _, err = s.GetSession(ctx, userA, created.ID)
	if err != nil {
		t.Fatalf("owner GetSession: %v", err)
	}
	// Non-owner cannot.
	_, _, err = s.GetSession(ctx, userB, created.ID)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("non-owner GetSession: want ErrNotFound, got %v", err)
	}
}

func TestAppendMessage_SequenceAndHistory(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	sess, _ := s.CreateSession(ctx, userA, "openai", "gpt-4o-mini", "")

	if err := s.AppendMessage(ctx, sess.ID, "user", "hello"); err != nil {
		t.Fatalf("AppendMessage 1: %v", err)
	}
	if err := s.AppendMessage(ctx, sess.ID, "assistant", "hi there"); err != nil {
		t.Fatalf("AppendMessage 2: %v", err)
	}

	_, msgs, err := s.GetSession(ctx, userA, sess.ID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("messages: want 2, got %d", len(msgs))
	}
	if msgs[0].Seq != 1 || msgs[0].Role != "user" || msgs[0].Content != "hello" {
		t.Errorf("msg[0] = %+v", msgs[0])
	}
	if msgs[1].Seq != 2 || msgs[1].Role != "assistant" {
		t.Errorf("msg[1] = %+v", msgs[1])
	}
}

func TestListSessions_ScopedByUser(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	// Clear any sessions left from previous runs so the count assertion is deterministic.
	if _, err := s.Pool.Exec(ctx, `delete from agent.sessions where dora_user_id in ($1, $2)`, userA, userB); err != nil {
		t.Fatalf("clean sessions: %v", err)
	}
	_, _ = s.CreateSession(ctx, userA, "openai", "gpt-4o-mini", "a1")
	_, _ = s.CreateSession(ctx, userB, "openai", "gpt-4o-mini", "b1")

	a, _ := s.ListSessions(ctx, userA)
	b, _ := s.ListSessions(ctx, userB)

	if len(a) != 1 {
		t.Errorf("user-A list: want exactly 1 session, got %d (%+v)", len(a), a)
	} else if a[0].Title.String != "a1" {
		t.Errorf("user-A first session: want title %q, got %q", "a1", a[0].Title.String)
	}
	if len(b) != 1 {
		t.Errorf("user-B list: want exactly 1 session, got %d (%+v)", len(b), b)
	} else if b[0].Title.String != "b1" {
		t.Errorf("user-B first session: want title %q, got %q", "b1", b[0].Title.String)
	}
}

func TestDeleteSession_CascadesMessages(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	sess, _ := s.CreateSession(ctx, userA, "openai", "gpt-4o-mini", "")
	_ = s.AppendMessage(ctx, sess.ID, "user", "x")

	if err := s.DeleteSession(ctx, userA, sess.ID); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	_, _, err := s.GetSession(ctx, userA, sess.ID)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("after delete: want ErrNotFound, got %v", err)
	}
}
