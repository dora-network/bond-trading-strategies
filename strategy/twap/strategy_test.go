package twap

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy/exec"
)

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func validConfig() Config {
	return Config{
		OrderBookID:     "01900000-0000-0000-0000-000000000001",
		TotalAmount:     decimal.MustNew(1000, 0),
		Side:            "buy",
		StartTime:       time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC),
		EndTime:         time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
		IntervalSeconds: 300,
	}
}

func TestComputeChunkSize_FreshRun(t *testing.T) {
	t.Parallel()
	s := &Strategy{cfg: validConfig()}
	got, err := s.computeChunkSize(10, decimal.Zero, decimal.Zero, 0)
	require.NoError(t, err)
	require.Equal(t, "100", got.String())
}

func TestComputeChunkSize_FilledSoFar(t *testing.T) {
	t.Parallel()
	s := &Strategy{cfg: validConfig()}
	// 1000 total, 500 filled, 5 of 10 chunks processed. Remaining
	// 500 / 5 remaining chunks = 100 each.
	got, err := s.computeChunkSize(10, decimal.MustNew(500, 0), decimal.MustNew(500, 0), 5)
	require.NoError(t, err)
	require.Equal(t, "100", got.String())
}

func TestComputeChunkSize_FilledExceedsTotal(t *testing.T) {
	t.Parallel()
	s := &Strategy{cfg: validConfig()}
	// Defensive: filled > total implies double-count. Clamp to 0.
	got, err := s.computeChunkSize(7, decimal.MustNew(1200, 0), decimal.MustNew(1200, 0), 3)
	require.NoError(t, err)
	require.True(t, got.IsZero())
}

// TestComputeChunkSize_PartialFillDoesNotReissue is the regression for
// the under-execution bug: when a chunk requested 200 but only
// filled 120 (TotalSubmitted 200, TotalFilled 120), the rebalance
// must base on TotalFilled so the already-filled 120 is not
// re-issued. Under the old TotalSubmitted base the next chunk
// would be sized to (1000-200)/7=114.29 and over-submit by 120.
func TestComputeChunkSize_PartialFillDoesNotReissue(t *testing.T) {
	t.Parallel()
	s := &Strategy{cfg: validConfig()}
	// TotalAmount 1000, 3 of 10 chunks processed. A previous chunk
	// was requested 200 but only filled 120 (partial). TotalSubmitted
	// = 200, TotalFilled = 120. Next chunk must be sized off
	// TotalFilled (1000 - 120) so the already-filled 120 is not
	// re-issued. Under the old TotalSubmitted base the next chunk
	// would be sized to (1000 - 200) / 7 = 114.29 and over-submit by 120.
	got, err := s.computeChunkSize(10, decimal.MustNew(200, 0), decimal.MustNew(120, 0), 3)
	require.NoError(t, err)
	require.Equal(t, "125.7142857142857143", got.String(),
		"partial-fill rebalance must base on TotalFilled (120), not TotalSubmitted (200)")
	// Sanity: if the formula accidentally used TotalSubmitted, the
	// result would be 114.29 — not what we want.
	require.NotEqual(t, "114.2857142857142857", got.String(),
		"if this matches, the rebased-to-TotalFilled fix has regressed")
}

func TestComputeChunkSize_AllSubmitted(t *testing.T) {
	t.Parallel()
	s := &Strategy{cfg: validConfig()}
	got, err := s.computeChunkSize(10, decimal.MustNew(1000, 0), decimal.MustNew(1000, 0), 10)
	require.NoError(t, err)
	require.True(t, got.IsZero())
}

func TestComputeChunkSize_NoRemaining(t *testing.T) {
	t.Parallel()
	s := &Strategy{cfg: validConfig()}
	got, err := s.computeChunkSize(10, decimal.MustNew(500, 0), decimal.MustNew(500, 0), 11)
	require.NoError(t, err)
	require.True(t, got.IsZero())
}

func TestProcessOrderUpdate_PartialFillTracksQty(t *testing.T) {
	t.Parallel()
	s := &Strategy{
		cfg:  validConfig(),
		log:  silentLogger(),
		exec: &exec.Executor{Name: StrategyType, Log: silentLogger()},
		mu:   sync.RWMutex{},
		runState: RunState{
			Orders: []exec.OrderEntry{
				{
					OrderID:           "ord1",
					ClientOrderID:     "twap.run1.uuid1",
					RequestedQuantity: decimal.MustNew(1000, 0),
					FilledQuantity:    decimal.Zero,
					Status:            "OPEN",
				},
			},
		},
	}
	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "twap.run1.uuid1",
		Status:         "OPEN",
		FilledQuantity: decimal.MustNew(300, 0),
	})
	require.Equal(t, "300", s.runState.Orders[0].FilledQuantity.String())
	require.Equal(t, "0", s.runState.TotalFilled.String())

	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "twap.run1.uuid1",
		Status:         "FILLED",
		FilledQuantity: decimal.MustNew(1000, 0),
	})
	require.Equal(t, "1000", s.runState.Orders[0].FilledQuantity.String())
	require.Equal(t, "1000", s.runState.TotalFilled.String())
}

// TestProcessOrderUpdate_TerminalTransitionsAddOnce verifies that
// PARTIAL_FILL events while an order is in-flight do not advance
// TotalFilled (the counter tracks settled fills); the
// PARTIAL_FILL -> FILLED transition adds the final quantity once.
func TestProcessOrderUpdate_TerminalTransitionsAddOnce(t *testing.T) {
	t.Parallel()
	s := &Strategy{
		cfg:  validConfig(),
		log:  silentLogger(),
		exec: &exec.Executor{Name: StrategyType, Log: silentLogger()},
		mu:   sync.RWMutex{},
		runState: RunState{
			Orders: []exec.OrderEntry{
				{
					OrderID:           "ord1",
					ClientOrderID:     "twap.run1.uuid1",
					RequestedQuantity: decimal.MustNew(500, 0),
					FilledQuantity:    decimal.Zero,
					Status:            "OPEN",
				},
			},
		},
	}
	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "twap.run1.uuid1",
		Status:         "PARTIAL_FILL",
		FilledQuantity: decimal.MustNew(300, 0),
	})
	require.Equal(t, "0", s.runState.TotalFilled.String(),
		"PARTIAL_FILL while in-flight must not advance TotalFilled")

	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "twap.run1.uuid1",
		Status:         "FILLED",
		FilledQuantity: decimal.MustNew(300, 0),
	})
	require.Equal(t, "300", s.runState.TotalFilled.String(),
		"transition to terminal adds the final fill once")
}

func TestProcessOrderUpdate_UnknownClientOrderID(t *testing.T) {
	t.Parallel()
	s := &Strategy{
		cfg:      validConfig(),
		log:      silentLogger(),
		exec:     &exec.Executor{Name: StrategyType, Log: silentLogger()},
		mu:       sync.RWMutex{},
		runState: RunState{},
	}
	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "unknown",
		Status:         "FILLED",
		FilledQuantity: decimal.MustNew(100, 0),
	})
	require.Equal(t, "0", s.runState.TotalFilled.String())
	require.Empty(t, s.runState.Orders)
}
