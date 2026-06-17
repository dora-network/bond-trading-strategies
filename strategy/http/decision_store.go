package http

import (
	"context"
	"fmt"
	"time"

	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGDecisionStore is the postgres-backed implementation of
// strategy.DecisionRecorder — the same interface the live strategy
// loop (meanreversion, copytrading) depends on.  *PGDecisionStore
// satisfies strategy.DecisionRecorder structurally, so callers should
// keep the consumer-package interface (strategy.DecisionRecorder)
// as the canonical name; the producer package does not declare a
// duplicate.
//
// It is safe for concurrent use because *pgxpool.Pool is.
type PGDecisionStore struct {
	pool *pgxpool.Pool
}

// NewPGDecisionStore returns a *PGDecisionStore backed by the given
// pool.  The pool is not owned; the caller is responsible for closing
// it.  The returned value is intended to be passed straight into
// strategyhttp.WithDecisionStore, whose parameter type is
// strategy.DecisionRecorder.
func NewPGDecisionStore(pool *pgxpool.Pool) *PGDecisionStore {
	return &PGDecisionStore{pool: pool}
}

// SaveDecision inserts a single decision row into strategy_decisions.
// On a primary-key conflict (run_id, seq) the call returns an error
// rather than upserting — a duplicate seq is always a bug because
// strategies assign seqs atomically per run.
func (s *PGDecisionStore) SaveDecision(ctx context.Context, d strategycore.Decision) error {
	const q = `
		INSERT INTO strategy_decisions (
			run_id, seq, strategy_type, order_book_id, asset,
			side, signal, kind, quantity, price, leverage, inverse_leverage,
			from_global_position, reason, reason_detail, created_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10, $11, $12,
			$13, $14, $15, $16
		)
	`

	createdAt := d.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	_, err := s.pool.Exec(ctx, q,
		d.RunID,
		d.Seq,
		d.StrategyType,
		d.OrderBookID,
		d.Asset,
		d.Side,
		d.Signal,
		string(d.Kind),
		nullableDecimal(d.Quantity),
		nullableDecimal(d.Price),
		nullableDecimal(d.Leverage),
		nullableDecimal(d.InverseLeverage),
		d.FromGlobalPosition,
		d.Reason,
		d.ReasonDetail,
		createdAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("save strategy decision run=%s seq=%d: %w", d.RunID, d.Seq, err)
	}
	return nil
}
