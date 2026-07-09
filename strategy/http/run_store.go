package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RunStore interface {
	LoadRuns(ctx context.Context) ([]*RunDetail, error)
	SaveRun(ctx context.Context, detail *RunDetail) error
	// CheckRunExists reports whether a run with the given id exists and
	// is owned by doraUserID. The decisions handler uses it to enforce
	// ownership before serving rows from strategy_decisions. A nil error
	// with a false result means the run does not exist OR it belongs to
	// another user; the handler maps both to 404.
	CheckRunExists(ctx context.Context, id uuid.UUID, doraUserID string) (bool, error)
	// LookupRunByID returns the full RunDetail for the given run id, or
	// (nil, nil) if no row matches. cmd/strategy-server/main.go wraps this
	// into a RunLookupFunc closure that the notifications/orderupdates
	// package consumes — package-internal callers do not import strategy/http.
	//
	// Errors are reserved for transport / decode failures; "no such run"
	// is reported as (nil, nil) because the filter's hot path treats
	// "missing" and "DB blip" differently.
	LookupRunByID(ctx context.Context, id uuid.UUID) (*RunDetail, error)
}

type PGRunStore struct {
	pool *pgxpool.Pool
}

func NewPGRunStore(pool *pgxpool.Pool) *PGRunStore {
	return &PGRunStore{pool: pool}
}

func (s *PGRunStore) LoadRuns(ctx context.Context) ([]*RunDetail, error) {
	const q = `
		SELECT id, dora_user_id, strategy_type, status, config, created_at, updated_at, stopped_at, error, encrypted_api_key
		FROM strategy_runs
		WHERE status != 'stopped'
	`

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query strategy runs: %w", err)
	}
	defer rows.Close()

	runs := make([]*RunDetail, 0)
	for rows.Next() {
		var detail RunDetail
		if err := rows.Scan(
			&detail.ID,
			&detail.DORAUserID,
			&detail.StrategyType,
			&detail.Status,
			&detail.Config,
			&detail.CreatedAt,
			&detail.UpdatedAt,
			&detail.StoppedAt,
			&detail.Error,
			&detail.EncryptedAPIKey,
		); err != nil {
			return nil, fmt.Errorf("scan strategy run: %w", err)
		}
		runs = append(runs, &detail)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate strategy runs: %w", err)
	}
	return runs, nil
}

func (s *PGRunStore) SaveRun(ctx context.Context, detail *RunDetail) error {
	const q = `
		INSERT INTO strategy_runs (id, dora_user_id, strategy_type, status, config, created_at, updated_at, stopped_at, error, encrypted_api_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id)
		DO UPDATE SET
			dora_user_id       = EXCLUDED.dora_user_id,
			strategy_type      = EXCLUDED.strategy_type,
			status             = EXCLUDED.status,
			config             = EXCLUDED.config,
			created_at         = EXCLUDED.created_at,
			updated_at         = EXCLUDED.updated_at,
			stopped_at         = EXCLUDED.stopped_at,
			error              = EXCLUDED.error,
			encrypted_api_key  = EXCLUDED.encrypted_api_key
	`

	if _, err := s.pool.Exec(
		ctx,
		q,
		detail.ID,
		detail.DORAUserID,
		detail.StrategyType,
		detail.Status,
		detail.Config,
		detail.CreatedAt,
		detail.UpdatedAt,
		detail.StoppedAt,
		detail.Error,
		detail.EncryptedAPIKey,
	); err != nil {
		return fmt.Errorf("save strategy run %s: %w", detail.ID, err)
	}

	return nil
}

// CheckRunExists returns true iff a run with id exists for doraUserID.
// The single-row SELECT EXISTS short-circuits on the first match and
// uses the strategy_runs primary key on id. The dora_user_id filter
// is a residual predicate; no separate index is required.
func (s *PGRunStore) CheckRunExists(ctx context.Context, id uuid.UUID, doraUserID string) (bool, error) {
	const q = `SELECT EXISTS(SELECT 1 FROM strategy_runs WHERE id = $1 AND dora_user_id = $2)`
	var ok bool
	if err := s.pool.QueryRow(ctx, q, id, doraUserID).Scan(&ok); err != nil {
		return false, fmt.Errorf("check strategy run exists %s: %w", id, err)
	}
	return ok, nil
}

// LookupRunByID satisfies RunStore.LookupRunByID. The query descends
// the strategy_runs primary key on id. (nil, nil) on pgx.ErrNoRows so
// the consumer can treat "no such run" as a drop rather than an error.
func (s *PGRunStore) LookupRunByID(ctx context.Context, id uuid.UUID) (*RunDetail, error) {
	const q = `
		SELECT id, dora_user_id, strategy_type, status, config,
		       created_at, updated_at, stopped_at, error, encrypted_api_key
		FROM strategy_runs
		WHERE id = $1
	`
	var d RunDetail
	var encKey []byte
	err := s.pool.QueryRow(ctx, q, id).Scan(
		&d.ID, &d.DORAUserID, &d.StrategyType, &d.Status, &d.Config,
		&d.CreatedAt, &d.UpdatedAt, &d.StoppedAt, &d.Error, &encKey,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("lookup strategy run %s: %w", id, err)
	}
	if encKey != nil {
		d.EncryptedAPIKey = encKey
	}
	return &d, nil
}
