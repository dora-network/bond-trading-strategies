package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/google/uuid"
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
			from_global_position, reason, reason_detail, client_order_id, created_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10, $11, $12,
			$13, $14, $15, $16, $17
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
		d.Quantity.String(),
		d.Price.String(),
		d.Leverage.String(),
		d.InverseLeverage.String(),
		d.FromGlobalPosition,
		d.Reason,
		d.ReasonDetail,
		d.ClientOrderID,
		createdAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("save strategy decision run=%s seq=%d: %w", d.RunID, d.Seq, err)
	}
	return nil
}

// MaxSeq returns the highest seq already persisted for runID, or 0 if
// the run has no decisions yet. Called once at strategy start so the
// in-memory counter can resume past the DB frontier. A unique-row
// index on (run_id, seq) makes this an O(1) read.
func (s *PGDecisionStore) MaxSeq(ctx context.Context, runID uuid.UUID) (int64, error) {
	const q = `SELECT COALESCE(MAX(seq), 0) FROM strategy_decisions WHERE run_id = $1`
	var maxSeq int64
	if err := s.pool.QueryRow(ctx, q, runID).Scan(&maxSeq); err != nil {
		return 0, fmt.Errorf("max seq for run %s: %w", runID, err)
	}
	return maxSeq, nil
}

// when more rows are available. The wire format is versioned so the
// encoding can evolve without breaking in-flight clients; clients MUST
// NOT parse the underlying payload.
type Cursor struct {
	// Time is the created_at of the last row on the previous page.
	Time time.Time
	// Seq is the seq of the last row on the previous page.
	Seq int64
	// Version is the codec version. Decoding an unknown version is an
	// error. Bump it when changing the on-wire format.
	Version byte
}

const cursorVersion byte = 1

// cursorPayload is the on-wire shape of a Cursor. The version byte
// is part of the payload (not a struct-level constant) so a future
// change to the encoding can bump V and old clients will see a
// clear "unsupported cursor version" error rather than a decode
// failure on a missing field.
type cursorPayload struct {
	V byte  `json:"v"`
	T int64 `json:"t"`
	S int64 `json:"s"`
}

// Encode returns the base64(JSON({v, t, s})) form of c.
func (c *Cursor) Encode() string {
	b, _ := json.Marshal(cursorPayload{V: c.Version, T: c.Time.UnixNano(), S: c.Seq})
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor parses a value produced by Cursor.Encode. Returns an
// error for empty input, bad base64, bad JSON, or unknown version.
func DecodeCursor(raw string) (*Cursor, error) {
	if raw == "" {
		return nil, fmt.Errorf("cursor is empty")
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("decode cursor: %w", err)
	}
	var payload cursorPayload
	if err := json.Unmarshal(b, &payload); err != nil {
		return nil, fmt.Errorf("parse cursor: %w", err)
	}
	if payload.V != cursorVersion {
		return nil, fmt.Errorf("unsupported cursor version %d", payload.V)
	}
	return &Cursor{
		Time:    time.Unix(0, payload.T).UTC(),
		Seq:     payload.S,
		Version: payload.V,
	}, nil
}

// ListDecisionsParams controls the cursor-paginated query in
// PGDecisionStore.ListDecisions. From and To are inclusive; nil means
// unbounded. AfterTime and AfterSeq together form the "give me the page
// strictly older than this point" predicate and must be set together
// (or both nil) — the handler enforces this by only constructing a
// ListDecisionsParams with a cursor after DecodeCursor succeeds.
type ListDecisionsParams struct {
	// RunID filters by run_id. Required.
	RunID uuid.UUID
	// From is the inclusive lower bound on created_at. nil = no lower bound.
	From *time.Time
	// To is the inclusive upper bound on created_at. nil = no upper bound.
	To *time.Time
	// AfterTime is the cursor's created_at. nil = first page.
	AfterTime *time.Time
	// AfterSeq is the cursor's seq. nil = first page; must be set iff AfterTime is set.
	AfterSeq *int64
	// Limit is the maximum number of items to return (1..200). The store
	// fetches Limit+1 rows so it can detect whether a next page exists.
	Limit int
}

// DecisionReader is the read-side counterpart to strategy.DecisionRecorder.
// The handler holds it as a separate field (decisionReader) so the read
// path can be tested without a Postgres fixture and so the write-side
// interface consumed by the strategy loops is not polluted with read
// concerns (cursor, pagination). *PGDecisionStore satisfies both
// interfaces — they are decoupled only at the handler boundary.
//
// Implementations MUST be safe to call concurrently from multiple
// goroutines; the production implementation is backed by *pgxpool.Pool.
type DecisionReader interface {
	ListDecisions(ctx context.Context, p ListDecisionsParams) ([]strategycore.Decision, *Cursor, error)
}

// ListDecisions returns one page of decisions for runID, newest first.
// The returned cursor is non-nil iff a next page exists; the handler
// encodes it into the response body's next_cursor field.
func (s *PGDecisionStore) ListDecisions(ctx context.Context, p ListDecisionsParams) ([]strategycore.Decision, *Cursor, error) {
	if p.Limit <= 0 {
		return nil, nil, fmt.Errorf("list decisions: limit must be positive, got %d", p.Limit)
	}
	if (p.AfterTime == nil) != (p.AfterSeq == nil) {
		return nil, nil, errors.New("list decisions: after_time and after_seq must be set together")
	}

	const q = `
		SELECT run_id, seq, strategy_type, order_book_id, asset,
		       side, signal, kind, quantity, price, leverage, inverse_leverage,
		       from_global_position, reason, reason_detail, client_order_id, created_at
		FROM strategy_decisions
		WHERE run_id = $1
		  AND ($2::TIMESTAMP IS NULL OR created_at >= $2)
		  AND ($3::TIMESTAMP IS NULL OR created_at <= $3)
		  AND ($4::TIMESTAMP IS NULL
		       OR created_at < $4
		       OR (created_at = $4 AND seq < $5))
		ORDER BY created_at DESC, seq DESC
		LIMIT $6
	`

	rows, err := s.pool.Query(ctx, q,
		p.RunID,
		p.From,
		p.To,
		p.AfterTime,
		p.AfterSeq,
		p.Limit+1,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("list strategy decisions run=%s: %w", p.RunID, err)
	}
	defer rows.Close()

	out := make([]strategycore.Decision, 0, p.Limit)
	for rows.Next() {
		var d strategycore.Decision
		if err := rows.Scan(
			&d.RunID,
			&d.Seq,
			&d.StrategyType,
			&d.OrderBookID,
			&d.Asset,
			&d.Side,
			&d.Signal,
			&d.Kind,
			&d.Quantity,
			&d.Price,
			&d.Leverage,
			&d.InverseLeverage,
			&d.FromGlobalPosition,
			&d.Reason,
			&d.ReasonDetail,
			&d.ClientOrderID,
			&d.CreatedAt,
		); err != nil {
			return nil, nil, fmt.Errorf("scan strategy decision: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate strategy decisions: %w", err)
	}

	if len(out) > p.Limit {
		last := out[p.Limit-1] // the last data row
		return out[:p.Limit], &Cursor{
			Time:    last.CreatedAt,
			Seq:     last.Seq,
			Version: cursorVersion,
		}, nil
	}
	return out, nil, nil
}
