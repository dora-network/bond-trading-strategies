package http

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type BacktestStore interface {
	LoadBacktests(ctx context.Context) ([]*BacktestDetail, error)
	LoadBacktestResult(ctx context.Context, id uuid.UUID) (*BacktestResult, error)
	SaveBacktest(ctx context.Context, detail *BacktestDetail) error
	GetBacktestTrades(ctx context.Context, id uuid.UUID, page, limit int) ([]TradeRecord, error)
	GetBacktestClosedTrades(ctx context.Context, id uuid.UUID, page, limit int) ([]ClosedTrade, error)
}

type PGBacktestStore struct {
	pool *pgxpool.Pool
}

func NewPGBacktestStore(pool *pgxpool.Pool) *PGBacktestStore {
	return &PGBacktestStore{pool: pool}
}

func (s *PGBacktestStore) LoadBacktests(ctx context.Context) ([]*BacktestDetail, error) {
	const q = `
		SELECT id, dora_user_id, strategy_type, status, config, start, "end", created_at, completed_at, error
		FROM strategy_backtests
	`

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query strategy backtests: %w", err)
	}
	defer rows.Close()

	backtests := make([]*BacktestDetail, 0)
	for rows.Next() {
		var detail BacktestDetail
		if err := rows.Scan(
			&detail.ID,
			&detail.DORAUserID,
			&detail.StrategyType,
			&detail.Status,
			&detail.Config,
			&detail.Start,
			&detail.End,
			&detail.CreatedAt,
			&detail.CompletedAt,
			&detail.Error,
		); err != nil {
			return nil, fmt.Errorf("scan strategy backtest: %w", err)
		}
		backtests = append(backtests, &detail)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate strategy backtests: %w", err)
	}
	return backtests, nil
}

func (s *PGBacktestStore) LoadBacktestResult(ctx context.Context, id uuid.UUID) (*BacktestResult, error) {
	const q = `
		SELECT result
		FROM strategy_backtests
		WHERE id = $1
	`

	var resultJSON *string
	if err := s.pool.QueryRow(ctx, q, id).Scan(&resultJSON); err != nil {
		return nil, fmt.Errorf("query strategy backtest result: %w", err)
	}

	if resultJSON == nil || *resultJSON == "" {
		return nil, nil
	}

	var result BacktestResult
	if err := json.Unmarshal([]byte(*resultJSON), &result); err != nil {
		return nil, fmt.Errorf("unmarshal backtest result: %w", err)
	}

	return &result, nil
}

func (s *PGBacktestStore) SaveBacktest(ctx context.Context, detail *BacktestDetail) error {
	var resultJSON *string
	if detail.Result != nil {
		b, err := json.Marshal(detail.Result)
		if err != nil {
			return fmt.Errorf("marshal backtest result: %w", err)
		}
		s := string(b)
		resultJSON = &s
	}

	const q = `
		INSERT INTO strategy_backtests (id, dora_user_id, strategy_type, status, config, start, "end", created_at, completed_at, error, result)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (id)
		DO UPDATE SET
			dora_user_id = EXCLUDED.dora_user_id,
			strategy_type = EXCLUDED.strategy_type,
			status = EXCLUDED.status,
			config = EXCLUDED.config,
			start = EXCLUDED.start,
			"end" = EXCLUDED."end",
			created_at = EXCLUDED.created_at,
			completed_at = EXCLUDED.completed_at,
			error = EXCLUDED.error,
			result = EXCLUDED.result
	`

	if _, err := s.pool.Exec(
		ctx,
		q,
		detail.ID,
		detail.DORAUserID,
		detail.StrategyType,
		detail.Status,
		detail.Config,
		detail.Start,
		detail.End,
		detail.CreatedAt,
		detail.CompletedAt,
		detail.Error,
		resultJSON,
	); err != nil {
		return fmt.Errorf("save strategy backtest %s: %w", detail.ID, err)
	}

	return nil
}

func (s *PGBacktestStore) GetBacktestTrades(ctx context.Context, id uuid.UUID, page, limit int) ([]TradeRecord, error) {
	result, err := s.LoadBacktestResult(ctx, id)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return []TradeRecord{}, nil
	}
	return paginate(result.TradeRecords, page, limit), nil
}

func (s *PGBacktestStore) GetBacktestClosedTrades(ctx context.Context, id uuid.UUID, page, limit int) ([]ClosedTrade, error) {
	result, err := s.LoadBacktestResult(ctx, id)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return []ClosedTrade{}, nil
	}
	return paginate(result.ClosedTrades, page, limit), nil
}

func paginate[T any](items []T, page, limit int) []T {
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = defaultPaginationLimit
	}
	if limit > maxPaginationLimit {
		limit = maxPaginationLimit
	}

	start := (page - 1) * limit
	if start >= len(items) {
		return []T{}
	}

	end := start + limit
	if end > len(items) {
		end = len(items)
	}

	return items[start:end]
}
