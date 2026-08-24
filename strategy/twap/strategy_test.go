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
	got, err := s.computeChunkSize(10, decimal.Zero, 0)
	require.NoError(t, err)
	require.Equal(t, "100", got.String())
}

func TestComputeChunkSize_RebalanceOnPartialFill(t *testing.T) {
	t.Parallel()
	s := &Strategy{cfg: validConfig()}
	got, err := s.computeChunkSize(10, decimal.MustNew(500, 0), 5)
	require.NoError(t, err)
	require.Equal(t, "100", got.String())
}

func TestComputeChunkSize_AfterCancellation(t *testing.T) {
	t.Parallel()
	s := &Strategy{cfg: validConfig()}
	got, err := s.computeChunkSize(7, decimal.MustNew(1200, 0), 3)
	require.NoError(t, err)
	// 1000 total - 1200 submitted (clamped to 0) = 0 remaining. 0/4 = 0.
	require.True(t, got.IsZero())
}

func TestComputeChunkSize_AllSubmitted(t *testing.T) {
	t.Parallel()
	s := &Strategy{cfg: validConfig()}
	got, err := s.computeChunkSize(10, decimal.MustNew(1000, 0), 10)
	require.NoError(t, err)
	require.True(t, got.IsZero())
}

func TestComputeChunkSize_NoRemaining(t *testing.T) {
	t.Parallel()
	s := &Strategy{cfg: validConfig()}
	got, err := s.computeChunkSize(10, decimal.MustNew(500, 0), 11)
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
