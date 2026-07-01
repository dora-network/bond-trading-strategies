package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

// Cursor is the opaque continuation token returned by ListDecisions
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
