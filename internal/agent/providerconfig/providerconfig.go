// Package providerconfig stores per-user LLM provider credentials,
// encrypted at rest via the agent Sealer (AES-256-GCM with the
// service-wide ENCRYPTION_KEY).
package providerconfig

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	agentsecrets "github.com/dora-network/bond-trading-strategies/internal/agent/secrets"
)

// ErrNotFound is returned when no config exists for the (user, provider).
var ErrNotFound = errors.New("providerconfig: not found")

// Config is a stored (still-encrypted) provider config row.
type Config struct {
	Provider     string
	EncryptedKey []byte
	DefaultModel string
	BaseURL      sql.NullString
}

// Store reads/writes provider configs. The caller owns the pool
// lifecycle; providerconfig does not Close it.
type Store struct {
	Pool   *pgxpool.Pool
	sealer *agentsecrets.Sealer
}

// New constructs a Store backed by the given pool and sealer.
func New(pool *pgxpool.Pool, sealer *agentsecrets.Sealer) *Store {
	return &Store{Pool: pool, sealer: sealer}
}

// Set encrypts apiKey and upserts the (userID, provider) config.
func (s *Store) Set(ctx context.Context, userID, provider, apiKey, defaultModel, baseURL string) error {
	sealed, err := s.sealer.Seal([]byte(apiKey))
	if err != nil {
		return err
	}
	var baseURLArg any
	if baseURL != "" {
		baseURLArg = baseURL
	}
	_, err = s.Pool.Exec(ctx, `
		insert into agent.provider_configs
			(dora_user_id, provider, api_key, default_model, base_url, updated_at)
		values ($1, $2, $3, $4, $5, now())
		on conflict (dora_user_id, provider) do update set
			api_key = excluded.api_key,
			default_model = excluded.default_model,
			base_url = excluded.base_url,
			updated_at = now()`,
		userID, provider, sealed, defaultModel, baseURLArg)
	return err
}

// GetDecrypted returns the plaintext API key, default model, and base URL.
func (s *Store) GetDecrypted(
	ctx context.Context, userID, provider string,
) (apiKey, defaultModel, baseURL string, err error) {
	var c Config
	c, err = s.get(ctx, userID, provider)
	if err != nil {
		return "", "", "", err
	}
	pt, err := s.sealer.Open(c.EncryptedKey)
	if err != nil {
		return "", "", "", err
	}
	baseURL = ""
	if c.BaseURL.Valid {
		baseURL = c.BaseURL.String
	}
	return string(pt), c.DefaultModel, baseURL, nil
}

// Get returns the encrypted row (used by the handler to mask the key).
func (s *Store) Get(ctx context.Context, userID, provider string) (Config, error) {
	return s.get(ctx, userID, provider)
}

func (s *Store) get(ctx context.Context, userID, provider string) (Config, error) {
	var c Config
	err := s.Pool.QueryRow(
		ctx, `
		select provider, api_key, default_model, base_url
		from agent.provider_configs where dora_user_id = $1 and provider = $2`,
		userID, provider,
	).Scan(&c.Provider, &c.EncryptedKey, &c.DefaultModel, &c.BaseURL)
	if err != nil {
		return Config{}, ErrNotFound
	}
	return c, nil
}

// Delete removes the (userID, provider) config.
func (s *Store) Delete(ctx context.Context, userID, provider string) error {
	tag, err := s.Pool.Exec(ctx, `
		delete from agent.provider_configs where dora_user_id = $1 and provider = $2`,
		userID, provider)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListEntry is one row in the user's provider_configs list, with the
// api_key already decrypted and masked so the handler can ship it
// straight to the wire.
type ListEntry struct {
	Provider     string
	DefaultModel string
	BaseURL      string
	APIKeyMasked string
}

// List returns every configured provider for the user, each with a
// masked api_key (sk-...abcd form). The store owns the Sealer call so
// the handler layer stays encryption-agnostic. Order is by provider
// name so the response is deterministic for tests.
func (s *Store) List(ctx context.Context, userID string) ([]ListEntry, error) {
	rows, err := s.Pool.Query(ctx, `
		select provider, api_key, default_model, base_url
		from agent.provider_configs where dora_user_id = $1
		order by provider`,
		userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ListEntry, 0)
	for rows.Next() {
		var c Config
		if err := rows.Scan(&c.Provider, &c.EncryptedKey, &c.DefaultModel, &c.BaseURL); err != nil {
			return nil, err
		}
		pt, err := s.sealer.Open(c.EncryptedKey)
		if err != nil {
			return nil, err
		}
		baseURL := ""
		if c.BaseURL.Valid {
			baseURL = c.BaseURL.String
		}
		out = append(out, ListEntry{
			Provider:     c.Provider,
			DefaultModel: c.DefaultModel,
			BaseURL:      baseURL,
			APIKeyMasked: maskAPIKey(string(pt)),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// MinMaskableKeyLen is the shortest api_key that maskAPIKey will
// try to redact. Anything shorter is rendered as "***" so we never
// leak meaningful content from short test or placeholder keys.
const MinMaskableKeyLen = 8

// maskAPIKey returns a redacted form of an LLM api_key. The
// canonical shape is first 3 chars + "..." + last 4 chars
// (e.g. "sk-...abcd"). Keys shorter than MinMaskableKeyLen chars
// return "***" to avoid leaking meaningful content.
func maskAPIKey(key string) string {
	if len(key) < MinMaskableKeyLen {
		return "***"
	}
	return key[:3] + "..." + key[len(key)-4:]
}
