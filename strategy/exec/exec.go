// Package exec provides shared types and helpers for execution-style
// strategies (TWAP, VWAP).
//
// Shared stateless primitives:
//   - DORA order-status constants
//   - OrderEntry, OrderFillEvent, RunState types
//   - IsTerminal, DoraclientSide, MustParseUUID helpers
//
// Shared I/O and state-update logic (Executor + RunStateView):
//   - Each execution strategy's RunState alias satisfies RunStateView
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
	"encoding/json"
	"fmt"
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
	OrderStatusPending     = "PENDING"
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
// reached a settled outcome. PENDING (DORA pre-open) and in-flight
// states (OPEN, PARTIAL_FILL) are not terminal; only FILLED and
// CANCELLED are. TotalFilled only counts towards the running tally
// once an event reaches a terminal status.
func IsTerminal(status string) bool {
	return status == OrderStatusFilled || status == OrderStatusCancelled
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

// RunState is the persisted checkpoint for an execution run. TWAP and
// VWAP re-export it via a type alias so callers continue to use the
// local name; both strategies persist the same JSON shape.
type RunState struct {
	TotalFilled     decimal.Decimal   `json:"total_filled"`
	TotalSubmitted  decimal.Decimal   `json:"total_submitted"`
	ChunksProcessed int               `json:"chunks_processed"`
	Orders          []OrderEntry      `json:"orders"`
	Buckets         []decimal.Decimal `json:"buckets,omitempty"`
}

// RunStateView is the subset of RunState that the shared Executor
// operates on.
type RunStateView interface {
	AppendOrder(o OrderEntry) int
	FindOrderByClientID(clientID string) (OrderEntry, bool)
	SetOrder(idx int, filledQty decimal.Decimal, status string)
	AddTotalSubmitted(d decimal.Decimal)
	AddTotalFilled(d decimal.Decimal)
	ForEachOrders(fn func(o OrderEntry, idx int))
	Marshal() ([]byte, error)
}

// Compile-time assertion that *RunState satisfies RunStateView.
var _ RunStateView = (*RunState)(nil)

// Marshal serialises the state to JSON for persistence.
func (s *RunState) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

// UnmarshalState deserialises state from persisted JSON. Returns a
// zero-value RunState if the input is nil or empty.
func UnmarshalState(raw []byte) (RunState, error) {
	if len(raw) == 0 {
		return RunState{}, nil
	}
	var s RunState
	if err := json.Unmarshal(raw, &s); err != nil {
		return RunState{}, err
	}
	return s, nil
}

// --- RunStateView implementation ---

func (s *RunState) AppendOrder(o OrderEntry) int {
	s.Orders = append(s.Orders, o)
	return len(s.Orders) - 1
}

func (s *RunState) FindOrderByClientID(clientID string) (OrderEntry, bool) {
	for _, o := range s.Orders {
		if o.ClientOrderID == clientID {
			return o, true
		}
	}
	return OrderEntry{}, false
}

func (s *RunState) SetOrder(idx int, filledQty decimal.Decimal, status string) {
	if idx < 0 || idx >= len(s.Orders) {
		return
	}
	s.Orders[idx].FilledQuantity = filledQty
	s.Orders[idx].Status = status
}

func (s *RunState) AddTotalSubmitted(d decimal.Decimal) {
	s.TotalSubmitted, _ = s.TotalSubmitted.Add(d)
}

func (s *RunState) AddTotalFilled(d decimal.Decimal) {
	s.TotalFilled, _ = s.TotalFilled.Add(d)
}

func (s *RunState) ForEachOrders(fn func(o OrderEntry, idx int)) {
	for i, o := range s.Orders {
		fn(o, i)
	}
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
// counter and calls RecordDecision with it. Returns an error if DORA
// returns successfully without an order ID — the caller treats the
// empty ID as an unreconcilable zombie rather than silently adding
// it to state.
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
	if orderID == "" {
		return "", "", fmt.Errorf("%s: dora returned empty order id for client_order_id %s", e.Name, clientOrderID)
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
// Status with the event values; increments TotalFilled by
// evt.FilledQuantity on the in-flight → terminal transition (the
// PARTIAL_FILL → FILLED case the reviewer flagged). Late duplicate
// events for an already-terminal order carrying a higher fill
// quantity add the delta so the counter matches the latest truth.
// Persists state via SaveState.
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
	var prevFilled decimal.Decimal
	wasTerminal := false
	idx := -1
	state.ForEachOrders(func(o OrderEntry, i int) {
		if o.ClientOrderID == evt.ClientOrderID {
			prevFilled = o.FilledQuantity
			wasTerminal = IsTerminal(o.Status)
			idx = i
		}
	})
	if idx == -1 {
		return
	}
	state.SetOrder(idx, evt.FilledQuantity, evt.Status)
	if IsTerminal(evt.Status) {
		switch {
		case !wasTerminal:
			// PARTIAL_FILL → FILLED/CANCELLED: add the full final fill
			// to TotalFilled.
			state.AddTotalFilled(evt.FilledQuantity)
		default:
			// Already-terminal: DORA occasionally restates the final
			// fill quantity after settlement adjustments. Only add
			// the delta when the fill grew.
			if delta, _ := evt.FilledQuantity.Sub(prevFilled); delta.Sign() > 0 {
				state.AddTotalFilled(delta)
			}
		}
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
// wrapper over MarketAPIClient.GetOrderFilledStatus). On the
// in-flight → terminal transition adds the full final fill;
// already-terminal orders add the delta on growth.
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
		var prevFilled decimal.Decimal
		wasTerminal := false
		state.ForEachOrders(func(prev OrderEntry, i int) {
			if i == idx {
				prevFilled = prev.FilledQuantity
				wasTerminal = IsTerminal(prev.Status)
			}
		})
		state.SetOrder(idx, filledQty, status)
		if IsTerminal(status) {
			if !wasTerminal {
				state.AddTotalFilled(filledQty)
			} else if delta, _ := filledQty.Sub(prevFilled); delta.Sign() > 0 {
				state.AddTotalFilled(delta)
			}
			e.Log.Info(
				"reconciled order to terminal status",
				"strategy", e.Name,
				"runID", e.RunID,
				"orderID", o.OrderID,
				"status", status,
				"filled", filledQty,
			)
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
