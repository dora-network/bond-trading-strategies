package vwap

import (
	"testing"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy/exec"
)

func TestRunState_RoundTrip(t *testing.T) {
	t.Parallel()
	original := RunState{
		TotalFilled:     decimal.MustNew(5000, 0),
		TotalSubmitted:  decimal.MustNew(8000, 0),
		ChunksProcessed: 3,
		Orders: []exec.OrderEntry{
			{
				OrderID:           "01900000-0000-0000-0000-000000000001",
				ClientOrderID:     "vwap.run1.uuid1",
				RequestedQuantity: decimal.MustNew(2000, 0),
				FilledQuantity:    decimal.MustNew(2000, 0),
				Status:            "FILLED",
			},
			{
				OrderID:           "01900000-0000-0000-0000-000000000002",
				ClientOrderID:     "vwap.run1.uuid2",
				RequestedQuantity: decimal.MustNew(3000, 0),
				FilledQuantity:    decimal.MustNew(1500, 0),
				Status:            "OPEN",
			},
		},
	}

	raw, err := original.Marshal()
	require.NoError(t, err)

	decoded, err := UnmarshalState(raw)
	require.NoError(t, err)
	require.Equal(t, original.TotalFilled.String(), decoded.TotalFilled.String())
	require.Equal(t, original.TotalSubmitted.String(), decoded.TotalSubmitted.String())
	require.Equal(t, original.ChunksProcessed, decoded.ChunksProcessed)
	require.Len(t, decoded.Orders, 2)
}

func TestRunState_EmptyUnmarshal(t *testing.T) {
	t.Parallel()
	s, err := UnmarshalState(nil)
	require.NoError(t, err)
	require.Equal(t, RunState{}, s)

	s, err = UnmarshalState([]byte{})
	require.NoError(t, err)
	require.Equal(t, RunState{}, s)
}
