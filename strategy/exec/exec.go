// Package exec provides shared types and helpers for execution-style
// strategies (TWAP, VWAP).
//
// Shared stateless primitives:
//   - DORA order-status constants
//   - OrderEntry, OrderFillEvent types
//   - IsTerminal, DoraclientSide, MustParseUUID helpers
//
// Shared I/O and state-update logic (Executor + RunStateView):
//   - Each execution strategy's RunState type implements RunStateView
//     so the Executor can mutate state and persist it without knowing
//     each strategy's concrete type.
//   - Executor wraps the shared deps (logger, market client, decision
//     recorder, state store, run ID) and exposes PlaceOrder,
//     ProcessOrderUpdate, ReconcilePendingOrders, RecordDecision,
//     SaveState, LoadState — the operations that were duplicated
//     across TWAP and VWAP.
package exec

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/dora-client-go/doraclient"
)

// DORA order statuses observed on the order update stream.
const (
	OrderStatusOpen        = "OPEN"
	OrderStatusFilled      = "FILLED"
	OrderStatusPartialFill = "PARTIAL_FILL"
	OrderStatusCancelled   = "CANCELLED"
)

// OrderEntry is the per-run record of a single chunk/bucket order.
type OrderEntry struct {
	OrderID           string          `json:"order_id"`
	ClientOrderID     string          `json:"client_order_id"`
	RequestedQuantity decimal.Decimal `json:"requested_quantity"`
	FilledQuantity    decimal.Decimal `json:"filled_quantity"`
	Status            string          `json:"status"`
}

// OrderFillEvent is the simplified order update payload.
type OrderFillEvent struct {
	ClientOrderID  string
	Status         string
	FilledQuantity decimal.Decimal
}

// IsTerminal reports whether the status indicates the order has
// received its final fill. For TWAP/VWAP market orders, any status
// other than OPEN is terminal.
func IsTerminal(status string) bool {
	return status != OrderStatusOpen
}

// DoraclientSide converts a strategy signal to a DORA order side.
func DoraclientSide(signal types.Signal) doraclient.Side {
	if signal == types.SignalSell {
		return doraclient.SIDE_SELL
	}
	return doraclient.SIDE_BUY
}

// MustParseUUID parses a UUID string, returning uuid.Nil on failure.
func MustParseUUID(s string) uuid.UUID {
	u, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil
	}
	return u
}

// RunStateView is the subset of an execution strategy's RunState that
// the shared Executor operates on. Each strategy's RunState
// implements these methods so the Executor can mutate state without
// knowing the concrete type.
type RunStateView interface {
	AppendOrder(o OrderEntry) int
	FindOrderByClientID(clientID string) (OrderEntry, bool)
	SetOrder(idx int, filledQty decimal.Decimal, status string)
	RemoveOrder(idx int)
	AddTotalSubmitted(d decimal.Decimal)
	AddTotalFilled(d decimal.Decimal)
	ForEachOrders(fn func(o OrderEntry, idx int))
	Marshal() ([]byte, error)
}

// Executor encapsulates the shared I/O and state-update logic for
// execution strategies. Each strategy constructs one with its own
// strategy name (used in log messages and the Decision.Reason code).
type Executor struct {
	Name    string // e.g. "twap", "vwap"
	Log     *slog.Logger
	RunID   uuid.UUID
	Market  strategy.MarketAPIClient
	Records strategy.DecisionRecorder
	Store   strategy.StateStore
}

// PlaceOrder creates a market order on DORA and appends it to the
// run state. Returns the assigned order ID and generated
// client_order_id. The caller increments its own decision sequence
// counter and calls RecordDecision with it.
func (e *Executor) PlaceOrder(
	ctx context.Context,
	state RunStateView,
	bondID string,
	signal types.Signal,
	quantity decimal.Decimal,
) (orderID, clientOrderID string, err error) {
	clientOrderID = strategy.BuildClientOrderID(e.Name, e.RunID)
	orderID, err = e.Market.CreateMarketOrder(
		ctx,
		bondID,
		DoraclientSide(signal),
		quantity,
		decimal.One,
		false,
		clientOrderID,
	)
	if err != nil {
		return "", "", err
	}
	state.AppendOrder(OrderEntry{
		OrderID:           orderID,
		ClientOrderID:     clientOrderID,
		RequestedQuantity: quantity,
		FilledQuantity:    decimal.Zero,
		Status:            OrderStatusOpen,
	})
	state.AddTotalSubmitted(quantity)
	return orderID, clientOrderID, nil
}

// ProcessOrderUpdate matches an order update event to a pending
// order and updates the run state. Replaces FilledQuantity and
// Status with the event values, and on the terminal transition
// increments TotalFilled once (then removes the order). Persists
// state via SaveState.
func (e *Executor) ProcessOrderUpdate(ctx context.Context, state RunStateView, evt OrderFillEvent) {
	if _, ok := state.FindOrderByClientID(evt.ClientOrderID); !ok {
		e.Log.Debug(
			"order update for unknown client_order_id, ignoring",
			"strategy", e.Name,
			"runID", e.RunID,
			"client_order_id", evt.ClientOrderID,
		)
		return
	}
	wasTerminal := false
	idx := -1
	state.ForEachOrders(func(o OrderEntry, i int) {
		if o.ClientOrderID == evt.ClientOrderID {
			wasTerminal = IsTerminal(o.Status)
			idx = i
		}
	})
	if idx == -1 {
		return
	}
	state.SetOrder(idx, evt.FilledQuantity, evt.Status)
	nowTerminal := IsTerminal(evt.Status)
	if nowTerminal && !wasTerminal {
		state.AddTotalFilled(evt.FilledQuantity)
	}
	if nowTerminal {
		e.Log.Info(
			"order reached terminal status",
			"strategy", e.Name,
			"runID", e.RunID,
			"client_order_id", evt.ClientOrderID,
			"status", evt.Status,
			"filled", evt.FilledQuantity,
		)
	}
	e.SaveState(ctx, state)
}

// RecordDecision persists a strategy.Decision row. No-op if Records
// is nil.
func (e *Executor) RecordDecision(
	ctx context.Context,
	signal types.Signal,
	bondID string,
	quantity decimal.Decimal,
	clientOrderID string,
	at time.Time,
	seq int64,
) {
	if e.Records == nil {
		return
	}
	d := strategy.Decision{
		RunID:         e.RunID,
		Seq:           seq,
		StrategyType:  e.Name,
		OrderBookID:   MustParseUUID(bondID),
		Asset:         MustParseUUID(bondID),
		Side:          signal.String(),
		Signal:        signal.String(),
		Quantity:      quantity,
		Price:         decimal.Zero,
		Kind:          strategy.DecisionKindOpen,
		Reason:        e.Name + "_execution",
		CreatedAt:     at,
		ClientOrderID: clientOrderID,
	}
	if err := e.Records.SaveDecision(ctx, d); err != nil {
		e.Log.Error(
			"save strategy decision",
			"strategy", e.Name,
			"err", err,
			"run_id", d.RunID,
		)
	}
}

// ReconcilePendingOrders queries DORA for each non-terminal order in
// persisted state and updates it with the actual fill status. The
// getFilled callback is supplied by the caller (typically a thin
// wrapper over MarketAPIClient.GetOrderFilledStatus).
func (e *Executor) ReconcilePendingOrders(
	ctx context.Context,
	state RunStateView,
	getFilled func(ctx context.Context, orderID string) (status string, filledQty decimal.Decimal, err error),
) {
	if e.Market == nil {
		return
	}
	state.ForEachOrders(func(o OrderEntry, idx int) {
		if IsTerminal(o.Status) {
			return
		}
		status, filledQty, err := getFilled(ctx, o.OrderID)
		if err != nil {
			e.Log.Error(
				"reconcile order failed",
				"strategy", e.Name,
				"runID", e.RunID,
				"orderID", o.OrderID,
				"err", err,
			)
			return
		}
		wasTerminal := IsTerminal(o.Status)
		state.SetOrder(idx, filledQty, status)
		if !wasTerminal && IsTerminal(status) {
			state.AddTotalFilled(filledQty)
		}
	})
	e.SaveState(ctx, state)
}

// SaveState serialises state and persists it. Non-fatal on failure.
// No-op when Store is nil.
func (e *Executor) SaveState(ctx context.Context, state RunStateView) {
	if e.Store == nil {
		return
	}
	raw, err := state.Marshal()
	if err != nil {
		e.Log.Error("marshal run state", "strategy", e.Name, "err", err)
		return
	}
	if err := e.Store.SaveState(ctx, e.RunID, raw); err != nil {
		e.Log.Error("save run state", "strategy", e.Name, "err", err, "runID", e.RunID)
	}
}

// LoadState reads the persisted state bytes. Returns (nil, nil) if
// no state has been persisted or Store is nil.
func (e *Executor) LoadState(ctx context.Context) ([]byte, error) {
	if e.Store == nil {
		return nil, nil
	}
	return e.Store.LoadState(ctx, e.RunID)
}
