// Package users persists the local mirror of authenticated Dora identities.
// Dora is the source of truth for identity; this package keeps a minimal
// projection so downstream tables (sessions, messages, strategies) can FK
// against a stable UUID. The auth flow calls Ensure immediately after a
// successful Validate, so the projection is always present for any
// authenticated user.
package users

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store persists the local user mirror.
type Store struct {
	pool *pgxpool.Pool
}

// New constructs a Store backed by the given pool. The caller owns the
// pool lifecycle; users does not Close it.
func New(ctx context.Context, pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, errors.New("users: nil pool")
	}
	return &Store{pool: pool}, nil
}

// Ensure upserts the local projection. Idempotent — safe to call on every
// authenticated request. ON CONFLICT updates the mutable fields so tenant
// or role changes made in Dora propagate without an explicit sync.
// last_seen_at is bumped on every call for observability.
func (s *Store) Ensure(ctx context.Context, userID, tenantID string, roles []string) error {
	if userID == "" {
		return errors.New("users: empty DoraUserID")
	}
	_, err := s.pool.Exec(ctx, `
		insert into agent.users (dora_user_id, tenant_id, roles)
		values ($1, $2, $3)
		on conflict (dora_user_id) do update set
			tenant_id = excluded.tenant_id,
			roles = excluded.roles,
			last_seen_at = now()`,
		userID, tenantID, roles)
	return err
}

// StoreKey persists the encrypted Dora API key for the user. Called by
// the auth middleware after a successful Ensure so crash recovery can
// decrypt the key without an HTTP request. Idempotent — overwrites on
// each auth.
func (s *Store) StoreKey(ctx context.Context, userID string, encryptedKey []byte) error {
	_, err := s.pool.Exec(ctx, `
		update agent.users set api_key = $2
		where dora_user_id = $1`,
		userID, encryptedKey)
	return err
}

// GetEncryptedKey returns the stored encrypted Dora API key for the
// user. Returns a nil slice if no key has been stored.
func (s *Store) GetEncryptedKey(ctx context.Context, userID string) (encryptedKey []byte, err error) {
	err = s.pool.QueryRow(
		ctx, `
		select api_key
		from agent.users where dora_user_id = $1`, userID,
	).Scan(&encryptedKey)
	return encryptedKey, err
}
