package vwap

import (
	"encoding/json"

	"github.com/govalues/decimal"

	"github.com/dora-network/bond-trading-strategies/strategy/exec"
)

// RunState is the persisted checkpoint for a VWAP run. Implements
// exec.RunStateView so the shared Executor can mutate it.
type RunState struct {
	TotalFilled     decimal.Decimal   `json:"total_filled"`
	TotalSubmitted  decimal.Decimal   `json:"total_submitted"`
	ChunksProcessed int               `json:"chunks_processed"`
	Orders          []exec.OrderEntry `json:"orders"`
}

// OrderFillEvent aliases exec.OrderFillEvent.
type OrderFillEvent = exec.OrderFillEvent

// Compile-time assertion that RunState satisfies exec.RunStateView.
var _ exec.RunStateView = (*RunState)(nil)

// Marshal serialises the state to JSON for persistence.
func (s *RunState) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

// --- exec.RunStateView implementation ---

func (s *RunState) AppendOrder(o exec.OrderEntry) int {
	s.Orders = append(s.Orders, o)
	return len(s.Orders) - 1
}

func (s *RunState) FindOrderByClientID(clientID string) (exec.OrderEntry, bool) {
	for _, o := range s.Orders {
		if o.ClientOrderID == clientID {
			return o, true
		}
	}
	return exec.OrderEntry{}, false
}

func (s *RunState) SetOrder(idx int, filledQty decimal.Decimal, status string) {
	if idx < 0 || idx >= len(s.Orders) {
		return
	}
	s.Orders[idx].FilledQuantity = filledQty
	s.Orders[idx].Status = status
}

func (s *RunState) RemoveOrder(idx int) {
	if idx < 0 || idx >= len(s.Orders) {
		return
	}
	s.Orders = append(s.Orders[:idx], s.Orders[idx+1:]...)
}

func (s *RunState) AddTotalSubmitted(d decimal.Decimal) {
	s.TotalSubmitted, _ = s.TotalSubmitted.Add(d)
}

func (s *RunState) AddTotalFilled(d decimal.Decimal) {
	s.TotalFilled, _ = s.TotalFilled.Add(d)
}

func (s *RunState) ForEachOrders(fn func(o exec.OrderEntry, idx int)) {
	for i, o := range s.Orders {
		fn(o, i)
	}
}

// UnmarshalState deserialises state from persisted JSON.
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
