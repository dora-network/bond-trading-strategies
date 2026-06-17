package strategy

import (
	"context"
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
type Decision struct {
	// RunID is the strategy_runs row that owns this decision.
	RunID uuid.UUID
	// Seq is a monotonically increasing per-run counter assigned by the
	// strategy.  Combined with RunID it forms the primary key.
	Seq int64
	// StrategyType is the strategy name ("mean_reversion" or
	// "copy_trading") that produced the decision.
	StrategyType string
	// OrderBookID is the DORA order book the order was placed on.
	OrderBookID uuid.UUID
	// Asset is the traded bond/asset UUID.
	Asset uuid.UUID
	// Side is "buy" or "sell" — the DORA side that was sent.
	Side string
	// Signal is the strategy's signal at decision time ("buy" or
	// "sell").  For mean-reversion it is the z-score signal; for
	// copy-trading it is the side the followed trader executed.
	Signal string
	// Quantity is the order size in bond units that was submitted.
	Quantity decimal.Decimal
	// Price is the bond price at decision time, used for sizing.
	Price decimal.Decimal
	// Leverage is the leverage value that was used to derive
	// InverseLeverage for this specific order.  This may differ from
	// the strategy's configured Leverage for close orders, which the
	// copy-trading strategy forces to 1.0 (DORA rejects leveraged
	// closes).  For mean-reversion opens and closes it equals the
	// configured Leverage.
	Leverage decimal.Decimal
	// InverseLeverage is the value sent to DORA for this order
	// (1 / Leverage, with Leverage=1 mapping to 1).  Consumers that
	// need to reconstruct the order can derive Leverage back as
	// 1 / InverseLeverage, but should prefer the recorded Leverage
	// field to avoid rounding artefacts.
	InverseLeverage decimal.Decimal
	// FromGlobalPosition mirrors the DORA flag controlling which
	// account the order draws from.
	FromGlobalPosition bool
	// Kind is one of DecisionKind{Open,Close,Extend}.
	Kind DecisionKind
	// Reason is a short machine-readable code (e.g. "z_score_entry",
	// "take_profit", "stop_loss", "follow_trade") so that consumers
	// can group decisions without parsing ReasonDetail.
	Reason string
	// ReasonDetail is a free-form human-readable explanation of why
	// the decision fired; safe to surface in the UI.
	ReasonDetail string
	// CreatedAt is the wall-clock time at which the order was placed
	// (UTC).
	CreatedAt time.Time
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
}
