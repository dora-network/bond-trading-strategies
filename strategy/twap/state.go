package twap

import (
	"encoding/json"

	"github.com/govalues/decimal"
)

// RunState is the persisted checkpoint for a TWAP run. It is written
// to strategy_runs.state after every chunk and read on restart so the
// strategy can resume without over-executing.
type RunState struct {
	// TotalFilled is the cumulative filled quantity across orders
	// that have reached a terminal status.
	TotalFilled decimal.Decimal `json:"total_filled"`
	// TotalSubmitted is the cumulative quantity submitted to DORA
	// across all orders (open, partial, filled, or cancelled). Used
	// for the rebalance formula so in-flight and failed orders are
	// not re-placed: nextChunkSize = (TotalAmount - TotalSubmitted)
	// / (NumChunks - ChunksProcessed).
	TotalSubmitted decimal.Decimal `json:"total_submitted"`
	// ChunksProcessed is the number of chunk time-slots consumed,
	// whether the order succeeded, failed, or was skipped.
	ChunksProcessed int          `json:"chunks_processed"`
	Orders          []OrderEntry `json:"orders"`
}

// OrderEntry tracks a single chunk order placed with DORA. Its
// FilledQuantity and Status are updated as fill events arrive.
type OrderEntry struct {
	OrderID           string          `json:"order_id"`
	ClientOrderID     string          `json:"client_order_id"`
	RequestedQuantity decimal.Decimal `json:"requested_quantity"`
	FilledQuantity    decimal.Decimal `json:"filled_quantity"`
	Status            string          `json:"status"`
}

// Marshal serialises the state to JSON for persistence.
func (s RunState) Marshal() ([]byte, error) {
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

// OrderFillEvent is the simplified order update consumed by the TWAP
// run loop. The handler (Phase 5) adapts the raw notifications.Event
// payload from DORA's order update stream into this type so the TWAP
// package has no dependency on the notifications package.
type OrderFillEvent struct {
	ClientOrderID  string
	Status         string
	FilledQuantity decimal.Decimal
}

// Order statuses from the DORA API that are relevant to TWAP market
// orders. A market order transitions OPEN → terminal. PENDING exists
// in the enum but applies only to conditional orders (stop-loss,
// take-profit) and will never appear for a TWAP market order.
const (
	statusOpen        = "OPEN"
	statusFilled      = "FILLED"
	statusPartialFill = "PARTIAL_FILL"
	statusCancelled   = "CANCELLED"
)

// isTerminal reports whether the order has reached its final state.
// For TWAP market orders, any status other than OPEN is terminal.
func isTerminal(status string) bool {
	return status != statusOpen
}
