package vwap

import (
	"encoding/json"

	"github.com/dora-network/bond-trading-strategies/strategy/exec"
)

// RunState is the persisted checkpoint for a VWAP run. Re-exported as
// a type alias so strategy/exec.RunState satisfies the strategy
// RunState contract while callers continue to use the local name.
//
// Ponytail: previously duplicated as a struct identical to
// strategy/exec.RunState. The shared type already implements all
// RunStateView methods (Marshal/AppendOrder/FindOrderByClientID/
// SetOrder/AddTotalSubmitted/AddTotalFilled/ForEachOrders), so
// aliasing is the smallest correct cutover — the existing VWAP
// `state.Buckets` field is preserved on exec.RunState.
type RunState = exec.RunState

// Compile-time alias-satisfaction check.
var _ exec.RunStateView = (*RunState)(nil)

// OrderFillEvent aliases exec.OrderFillEvent.
type OrderFillEvent = exec.OrderFillEvent

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
