// Package session owns the agent.sessions and agent.messages tables via pgx.
package session

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a session does not exist or is not owned by
// the requesting user (ownership is enforced by filtering on user_id).
var ErrNotFound = errors.New("session: not found")

// Session is one conversation.
type Session struct {
	ID        string
	UserID    string
	Title     sql.NullString
	Provider  string
	Model     string
	Summary   sql.NullString // compressed conversation state from last turn
	CreatedAt time.Time
	UpdatedAt time.Time
}

// StoredMessage is one persisted message row.
type StoredMessage struct {
	ID         int64
	SessionID  string
	Seq        int
	Role       string
	Content    string
	ToolCallID sql.NullString
	ToolCalls  []byte // raw JSON, nil if absent
	CreatedAt  time.Time
}

// Store reads and writes sessions/messages. The caller owns the pool
// lifecycle; session does not Close it.
type Store struct {
	Pool *pgxpool.Pool
}

// New constructs a Store backed by the given pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{Pool: pool}
}

// CreateSession inserts a new session owned by userID.
func (s *Store) CreateSession(ctx context.Context, userID, provider, model, title string) (Session, error) {
	var sess Session
	var titleArg any
	if title != "" {
		titleArg = title
	}
	err := s.Pool.QueryRow(ctx, `
		insert into agent.sessions (id, dora_user_id, title, provider, model, summary)
		values ($1, $2, $3, $4, $5, null)
		returning id, dora_user_id, title, provider, model, summary, created_at, updated_at`,
		uuid.Must(uuid.NewV7()).String(), userID, titleArg, provider, model,
	).Scan(&sess.ID, &sess.UserID, &sess.Title, &sess.Provider, &sess.Model, &sess.Summary, &sess.CreatedAt, &sess.UpdatedAt)
	return sess, err
}

// ListSessions lists sessions for userID, newest first.
func (s *Store) ListSessions(ctx context.Context, userID string) ([]Session, error) {
	rows, err := s.Pool.Query(ctx, `
		select id, dora_user_id, title, provider, model, summary, created_at, updated_at
		from agent.sessions where dora_user_id = $1
		order by created_at desc`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Session
	for rows.Next() {
		var sess Session
		if err := rows.Scan(
			&sess.ID, &sess.UserID, &sess.Title, &sess.Provider, &sess.Model,
			&sess.Summary, &sess.CreatedAt, &sess.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, sess)
	}
	return out, rows.Err()
}

// GetSession returns the session and its messages, scoped by userID.
func (s *Store) GetSession(ctx context.Context, userID, sessionID string) (Session, []StoredMessage, error) {
	var sess Session
	err := s.Pool.QueryRow(ctx, `
		select id, dora_user_id, title, provider, model, summary, created_at, updated_at
		from agent.sessions where id = $1 and dora_user_id = $2`,
		sessionID, userID,
	).Scan(&sess.ID, &sess.UserID, &sess.Title, &sess.Provider, &sess.Model, &sess.Summary, &sess.CreatedAt, &sess.UpdatedAt)
	if err != nil {
		return Session{}, nil, ErrNotFound
	}
	rows, err := s.Pool.Query(ctx, `
		select id, session_id, seq, role, content, tool_call_id, tool_calls, created_at
		from agent.messages where session_id = $1 order by seq`, sessionID)
	if err != nil {
		return Session{}, nil, err
	}
	defer rows.Close()
	var msgs []StoredMessage
	for rows.Next() {
		var m StoredMessage
		if err := rows.Scan(&m.ID, &m.SessionID, &m.Seq, &m.Role, &m.Content, &m.ToolCallID, &m.ToolCalls, &m.CreatedAt); err != nil {
			return Session{}, nil, err
		}
		msgs = append(msgs, m)
	}
	return sess, msgs, rows.Err()
}

// UpdateSummary saves a compressed conversation summary for the session.
func (s *Store) UpdateSummary(ctx context.Context, sessionID, summary string) error {
	_, err := s.Pool.Exec(ctx, `update agent.sessions set summary = $1, updated_at = now() where id = $2`, summary, sessionID)
	return err
}

// DeleteSession deletes a session owned by userID (cascades to messages).
func (s *Store) DeleteSession(ctx context.Context, userID, sessionID string) error {
	tag, err := s.Pool.Exec(ctx, `delete from agent.sessions where id = $1 and dora_user_id = $2`, sessionID, userID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// AppendMessage appends a message with the next per-session sequence number.
// The sequence is computed atomically within the single INSERT.
func (s *Store) AppendMessage(ctx context.Context, sessionID, role, content string) error {
	_, err := s.Pool.Exec(ctx, `
		insert into agent.messages (session_id, seq, role, content)
		values ($1, (select coalesce(max(seq), 0) + 1 from agent.messages where session_id = $1), $2, $3)`,
		sessionID, role, content)
	return err
}
