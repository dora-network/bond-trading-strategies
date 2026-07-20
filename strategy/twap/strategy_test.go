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
)

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func validConfig() Config {
	return Config{
		OrderBookID: "01900000-0000-0000-0000-000000000001",
		TotalAmount: decimal.MustNew(1000, 0),
		Side:        "buy",
		StartTime:   time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC),
		EndTime:     time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
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
	// 1000 total, 5 of 10 chunks processed (submitted=500, filled=300).
	// Remaining: 5 chunks, remaining qty: 1000-500=500, so each=100.
	// This proves we use TotalSubmitted not TotalFilled: if we used
	// TotalFilled we'd get 700/5=140 (wrong).
	s := &Strategy{cfg: validConfig()}
	got, err := s.computeChunkSize(10, decimal.MustNew(500, 0), 5)
	require.NoError(t, err)
	require.Equal(t, "100", got.String())
}

func TestComputeChunkSize_AfterCancellation(t *testing.T) {
	t.Parallel()
	// 1000 total, 3 chunks placed (each 200), but only 100 actually filled
	// (the third was cancelled mid-fill). Submitted=600, filled=100.
	// Remaining: 7 chunks, remaining qty: 1000-600=400 → 400/7 ≈ 57.
	// Using TotalFilled=100 would give 900/7 ≈ 128 (wrong — would re-place
	// the cancelled quantity).
	s := &Strategy{cfg: validConfig()}
	got, err := s.computeChunkSize(10, decimal.MustNew(600, 0), 3)
	require.NoError(t, err)
	expected, err := decimal.MustNew(400, 0).Quo(decimal.MustNew(7, 0))
	require.NoError(t, err)
	require.Equal(t, expected.String(), got.String())
}

func TestComputeChunkSize_AllSubmitted(t *testing.T) {
	t.Parallel()
	// Edge case: all chunks submitted. Should return zero.
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
		log: silentLogger(),
		mu:  sync.RWMutex{},
		runState: RunState{
			Orders: []OrderEntry{
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

	// First partial fill: 300 of 1000.
	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "twap.run1.uuid1",
		Status:         "OPEN",
		FilledQuantity: decimal.MustNew(300, 0),
	})
	require.Equal(t, "300", s.runState.Orders[0].FilledQuantity.String())
	// OPEN → OPEN is not a terminal transition, so TotalFilled stays at 0.
	require.Equal(t, "0", s.runState.TotalFilled.String())

	// Full fill (OPEN→FILLED): total now 1000.
	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "twap.run1.uuid1",
		Status:         "FILLED",
		FilledQuantity: decimal.MustNew(1000, 0),
	})
	require.Equal(t, "1000", s.runState.Orders[0].FilledQuantity.String())
	require.Equal(t, "FILLED", s.runState.Orders[0].Status)
	// FILLED is terminal — TotalFilled jumps to 1000 (full qty).
	require.Equal(t, "1000", s.runState.TotalFilled.String())
}

func TestProcessOrderUpdate_TerminalTransitionsAddOnce(t *testing.T) {
	t.Parallel()
	s := &Strategy{
		log: silentLogger(),
		mu:  sync.RWMutex{},
		runState: RunState{
			Orders: []OrderEntry{
				{
					OrderID:           "ord1",
					ClientOrderID:     "twap.run1.uuid1",
					RequestedQuantity: decimal.MustNew(500, 0),
					FilledQuantity:    decimal.MustNew(0, 0),
					Status:            "OPEN",
				},
			},
		},
	}

	// Partial fill 200 (still OPEN, not terminal).
	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "twap.run1.uuid1",
		Status:         "OPEN",
		FilledQuantity: decimal.MustNew(200, 0),
	})
	require.Equal(t, "0", s.runState.TotalFilled.String())

	// Partial fill 300 cumulative (still OPEN).
	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "twap.run1.uuid1",
		Status:         "OPEN",
		FilledQuantity: decimal.MustNew(300, 0),
	})
	require.Equal(t, "0", s.runState.TotalFilled.String())

	// Terminal: partial fill closed at 300. Add to TotalFilled once.
	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "twap.run1.uuid1",
		Status:         "PARTIAL_FILL",
		FilledQuantity: decimal.MustNew(300, 0),
	})
	require.Equal(t, "300", s.runState.TotalFilled.String())

	// Duplicate terminal event — TotalFilled must NOT double-count.
	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "twap.run1.uuid1",
		Status:         "PARTIAL_FILL",
		FilledQuantity: decimal.MustNew(300, 0),
	})
	require.Equal(t, "300", s.runState.TotalFilled.String())
}

func TestProcessOrderUpdate_UnknownClientOrderID(t *testing.T) {
	t.Parallel()
	s := &Strategy{
		log:      silentLogger(),
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
