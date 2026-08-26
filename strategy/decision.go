package strategy

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// DecisionKind classifies why a decision row was recorded.  The set is
// small on purpose — every row must unambiguously describe whether the
// row corresponds to opening, extending, or closing a position.  New
// kinds are added by appending a constant here and handling the new
// constant in any UI/API surface that lists decisions.
type DecisionKind string

const (
	// DecisionKindOpen records a new position opened by the strategy.
	DecisionKindOpen DecisionKind = "open"
	// DecisionKindClose records a position closed by the strategy.
	DecisionKindClose DecisionKind = "close"
	// DecisionKindExtend records an addition to an existing position
	// (e.g. scaling-in or copy-trading a follow-up buy/sell of the
	// same asset on the same side as the existing position).
	DecisionKindExtend DecisionKind = "extend"
)

// Decision is the per-order record persisted by the live run path
// every time a trading decision triggers a market order.  The record
// is intentionally a snapshot of the inputs that produced the order,
// not a denormalised join — the row is the source of truth for
// "what was the strategy thinking when it placed this order?".
//
// JSON tags are part of the public trading-decisions API contract.
// The marshal test in strategy/http/decision_test.go locks the field
// names; do not rename a tag without bumping the OpenAPI version.
type Decision struct {
	// RunID is the strategy_runs row that owns this decision.
	RunID uuid.UUID `json:"run_id"`
	// Seq is a monotonically increasing per-run counter assigned by the
	// strategy.  Combined with RunID it forms the primary key.
	Seq int64 `json:"seq"`
	// StrategyType is the strategy name ("mean_reversion" or
	// "copy_trading") that produced the decision.
	StrategyType string `json:"strategy_type"`
	// OrderBookID is the DORA order book the order was placed on.
	OrderBookID uuid.UUID `json:"order_book_id"`
	// Asset is the traded bond/asset UUID.
	Asset uuid.UUID `json:"asset"`
	// Side is "buy" or "sell" — the DORA side that was sent.
	Side string `json:"side"`
	// Signal is the strategy's signal at decision time ("buy" or
	// "sell").  For mean-reversion it is the z-score signal; for
	// copy-trading it is the side the followed trader executed.
	Signal string `json:"signal"`
	// Quantity is the order size in bond units that was submitted.
	Quantity decimal.Decimal `json:"quantity"`
	// Price is the bond price at decision time, used for sizing.
	Price decimal.Decimal `json:"price"`
	// Leverage is the leverage sent to DORA for this specific order.
	// For opens and extends of either strategy, equals the strategy's
	// configured Leverage. For closes, both strategies force 1.0. You
	// cannot borrow in order to close a position.
	Leverage decimal.Decimal `json:"leverage"`
	// InverseLeverage is the value sent to DORA for this order, equal
	// to 1 / Leverage. For opens and extends it is 1 / cfg.Leverage
	// when cfg.Leverage > 1, otherwise 1.0. For closes it is always
	// 1.0 regardless of cfg.Leverage, because the close's Leverage is
	// forced to 1 (see Leverage)
	InverseLeverage decimal.Decimal `json:"inverse_leverage"`
	// FromGlobalPosition mirrors the DORA flag controlling which
	// account the order draws from. Both strategies hardcode this to
	// false: every order (open, extend, close) goes through the
	// bond's isolated margin account. The field is preserved for
	// audit completeness and to keep the JSON contract stable for
	// downstream consumers.
	FromGlobalPosition bool `json:"from_global_position"`
	// Kind is one of DecisionKind{Open,Close,Extend}.
	Kind DecisionKind `json:"kind"`
	// Reason is a short machine-readable code (e.g. "z_score_entry",
	// "take_profit", "stop_loss", "follow_trade") so that consumers
	// can group decisions without parsing ReasonDetail.
	Reason string `json:"reason"`
	// ReasonDetail is a free-form human-readable explanation of why
	// the decision fired; safe to surface in the UI.
	ReasonDetail string `json:"reason_detail"`
	// CreatedAt is the wall-clock time at which the order was placed
	// (UTC).
	CreatedAt time.Time `json:"created_at"`
	// ClientOrderID is the per-order identifier sent to DORA in the
	// CreateOrderRequest.  Format: <strategy_name>.<run_id>.<uuidv7>.
	// Set on every successful live order and persisted alongside the
	// rest of the decision so audit consumers can correlate the row
	// with the order in DORA.
	ClientOrderID string `json:"client_order_id"`
}

// BuildClientOrderID returns the client_order_id string the live
// run uses when submitting an order to DORA.  The format is
// <strategy_name>.<run_id>.<uuidv7>.  A fresh uuidv7 is generated on
// every call, so the returned string is unique per order and the
// uuidv7 prefix is monotonically time-sortable.
//
// The string is intended to be generated immediately before the
// CreateMarketOrder call so the same value flows into the DORA
// request and the recorded Decision row.
func BuildClientOrderID(strategyName string, runID uuid.UUID) string {
	return fmt.Sprintf("%s.%s.%s", strategyName, runID, uuid.Must(uuid.NewV7()))
}

// BuildClientOrderIDPrefix returns the prefix shared by every
// client_order_id the live run will generate, e.g.
// "twap.<run_id>." — the trailing dot ensures only this run's
// orders match. Used by execution strategies to list DORA orders
// on restart (the PlaceOrder/SaveState crash-window recovery).
func BuildClientOrderIDPrefix(strategyName string, runID uuid.UUID) string {
	return fmt.Sprintf("%s.%s.", strategyName, runID)
}

// DecisionRecorder is the minimal interface the live strategy loop uses
// to persist a Decision row.  It is satisfied by *http.PGDecisionStore
// in production and by a fake in tests.  The interface lives in this
// package to avoid an import cycle (the strategies depend on
// strategy/Decision but not on strategy/http).
//
// Implementations MUST be safe to call concurrently from multiple
// strategies and from within a strategy's run loop.  Returning an
// error from SaveDecision must NOT roll back the order that triggered
// the call; the live run is the source of truth, and a failed
// persistence is a degraded-but-correct outcome.  Callers should
// log the error and continue.
type DecisionRecorder interface {
	SaveDecision(ctx context.Context, d Decision) error
	// MaxSeq returns the highest seq already persisted for runID, or 0
	// if no decisions exist. Called once at strategy start (after a
	// server restart, after a resume) so the in-memory counter can
	// resume past the DB frontier; if MaxSeq fails, strategies
	// continue with seq starting at 1 and the first duplicate-key
	// collision surfaces as a save error, not a panic.
	MaxSeq(ctx context.Context, runID uuid.UUID) (int64, error)
}

// StateStore persists and loads per-run strategy state as opaque JSON.
// Execution strategies (e.g. TWAP) use it to checkpoint progress so
// they can recover and rebalance after a server restart without
// over-executing. Satisfied by *http.PGRunStore in production.
//
// Implementations MUST be safe for concurrent use. A failed SaveState
// is degraded-but-correct: the strategy continues, and the next
// checkpoint overwrites the stale row. LoadState returns (nil, nil)
// when no state has been persisted for the run.
type StateStore interface {
	SaveState(ctx context.Context, runID uuid.UUID, state []byte) error
	LoadState(ctx context.Context, runID uuid.UUID) ([]byte, error)
}
