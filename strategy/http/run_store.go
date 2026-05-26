package http

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type RunStore interface {
	LoadRuns(ctx context.Context) ([]*RunDetail, error)
	SaveRun(ctx context.Context, detail *RunDetail) error
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
