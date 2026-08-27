package twap

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
				ClientOrderID:     "twap.run1.uuid1",
				RequestedQuantity: decimal.MustNew(2000, 0),
				FilledQuantity:    decimal.MustNew(2000, 0),
				Status:            "FILLED",
			},
			{
				OrderID:           "01900000-0000-0000-0000-000000000002",
				ClientOrderID:     "twap.run1.uuid2",
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

	for i, want := range original.Orders {
		got := decoded.Orders[i]
		require.Equal(t, want.OrderID, got.OrderID)
		require.Equal(t, want.ClientOrderID, got.ClientOrderID)
		require.Equal(t, want.RequestedQuantity.String(), got.RequestedQuantity.String())
		require.Equal(t, want.FilledQuantity.String(), got.FilledQuantity.String())
		require.Equal(t, want.Status, got.Status)
	}
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

func TestIsTerminal(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status string
		want   bool
	}{
		{"OPEN", false},
		{"FILLED", true},
		{"PARTIAL_FILL", false},
		{"CANCELLED", true},
		{"PENDING", false},
		{"UNKNOWN", false},
	}
	for _, c := range cases {
		t.Run(c.status, func(t *testing.T) {
			require.Equal(t, c.want, exec.IsTerminal(c.status))
		})
	}
}
