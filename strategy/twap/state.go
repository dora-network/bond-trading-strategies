package twap

import (
	"encoding/json"

	"github.com/govalues/decimal"
)

// RunState is the persisted checkpoint for a TWAP run. It is written
// to strategy_runs.state after every chunk and read on restart so the
// strategy can resume without over-executing.
type RunState struct {
	// ExecutedAmount is the total quantity confirmed filled across all
	// chunks so far. Updated from order update events (FILLED /
	// PARTIAL_FILL), not from order placement success.
	ExecutedAmount decimal.Decimal `json:"executed_amount"`
	// ChunksProcessed is the number of chunk time-slots consumed,
	// whether the order succeeded, failed, or was skipped. Drives the
	// rebalance formula: nextChunkSize = (TotalAmount - ExecutedAmount)
	// / (NumChunks - ChunksProcessed).
	ChunksProcessed int `json:"chunks_processed"`
	// PendingOrders tracks orders submitted to DORA but not yet
	// confirmed filled. On restart, these are reconciled against the
	// order update stream to determine actual fill quantities.
	PendingOrders []PendingOrder `json:"pending_orders,omitempty"`
}

// PendingOrder tracks a submitted-but-unconfirmed chunk order.
type PendingOrder struct {
	ClientOrderID     string          `json:"client_order_id"`
	RequestedQuantity decimal.Decimal `json:"requested_quantity"`
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
