package vwap

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

// strPtrEven returns a strategy with 5 evenly-distributed buckets of
// 200 each (total 1000). The even case is the simplest sanity check.
func strPtrEven() *Strategy {
	return &Strategy{
		exec: &exec.Executor{Name: StrategyType, Log: silentLogger()},
		log:  silentLogger(),
		mu:   sync.RWMutex{},
		cfg: Config{
			TotalAmount: decimal.MustNew(1000, 0),
			Side:        "buy",
			StartTime:   time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC),
			EndTime:     time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
		},
		schedule: Schedule{
			Buckets: []decimal.Decimal{
				decimal.MustNew(200, 0),
				decimal.MustNew(200, 0),
				decimal.MustNew(200, 0),
				decimal.MustNew(200, 0),
				decimal.MustNew(200, 0),
			},
		},
	}
}

// strPtrWeighted returns a strategy with a non-uniform schedule
// representing a 1-hour VWAP where the middle has higher ADV:
// 100, 300, 600 = 1000 total.
func strPtrWeighted() *Strategy {
	return &Strategy{
		exec: &exec.Executor{Name: StrategyType, Log: silentLogger()},
		log:  silentLogger(),
		mu:   sync.RWMutex{},
		cfg: Config{
			TotalAmount: decimal.MustNew(1000, 0),
			Side:        "buy",
			StartTime:   time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC),
			EndTime:     time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
		},
		schedule: Schedule{
			Buckets: []decimal.Decimal{
				decimal.MustNew(100, 0),
				decimal.MustNew(300, 0),
				decimal.MustNew(600, 0),
			},
		},
	}
}

func TestComputeBucketSize_FreshRunEven(t *testing.T) {
	t.Parallel()
	s := strPtrEven()
	got, err := s.computeBucketSize(5, 0, decimal.Zero, 0)
	require.NoError(t, err)
	require.Equal(t, "200", got.String())
}

func TestComputeBucketSize_RebalanceUsesScheduleProportion(t *testing.T) {
	t.Parallel()
	// 5 buckets of 200, 2 processed with 200 submitted.
	// For bucket 2: size = 200 * (1000-200) / (200+200+200) ≈ 266.67.
	// The failed/cancelled quantity is absorbed into the remaining
	// buckets proportionally to their schedule weights.
	s := strPtrEven()
	got, err := s.computeBucketSize(5, 2, decimal.MustNew(200, 0), 2)
	require.NoError(t, err)
	expected, err := decimal.MustNew(1600, 0).Quo(decimal.MustNew(6, 0))
	require.NoError(t, err)
	require.Equal(t, expected.String(), got.String())
}

func TestComputeBucketSize_WeightedSchedule(t *testing.T) {
	t.Parallel()
	// Schedule [100, 300, 600], 1000 total, 0 submitted.
	// Bucket 0: 100 * 1000 / 1000 = 100
	// Bucket 1: 300 * 1000 / 1000 = 300
	// Bucket 2: 600 * 1000 / 1000 = 600
	s := strPtrWeighted()
	got0, err := s.computeBucketSize(3, 0, decimal.Zero, 0)
	require.NoError(t, err)
	require.Equal(t, "100", got0.String())

	got1, err := s.computeBucketSize(3, 1, decimal.Zero, 0)
	require.NoError(t, err)
	require.Equal(t, "300", got1.String())

	got2, err := s.computeBucketSize(3, 2, decimal.Zero, 0)
	require.NoError(t, err)
	require.Equal(t, "600", got2.String())
}

func TestComputeBucketSize_WeightedRebalanceAbsorbsFailed(t *testing.T) {
	t.Parallel()
	// Schedule [100, 300, 600], bucket 0 placed but failed (100
	// submitted, 0 filled). Remaining scheduled = 900. Remaining
	// qty = 1000 - 100 = 900.
	// Bucket 0 is already past, bucket 1 size = 300 * 900/900 = 300.
	// Bucket 2 size = 600 * 900/900 = 600.
	// Total: 300 + 600 = 900, equals totalAmount - submitted. The
	// failed bucket 0 quantity is absorbed into later buckets
	// proportionally.
	s := strPtrWeighted()
	got1, err := s.computeBucketSize(3, 1, decimal.MustNew(100, 0), 1)
	require.NoError(t, err)
	require.Equal(t, "300", got1.String())

	got2, err := s.computeBucketSize(3, 2, decimal.MustNew(100, 0), 1)
	require.NoError(t, err)
	require.Equal(t, "600", got2.String())
}

func TestComputeBucketSize_NoRemaining(t *testing.T) {
	t.Parallel()
	s := strPtrEven()
	got, err := s.computeBucketSize(5, 4, decimal.MustNew(1000, 0), 5)
	require.NoError(t, err)
	require.True(t, got.IsZero())
}

func TestComputeBucketSize_AllSubmitted(t *testing.T) {
	t.Parallel()
	// schedule [200x5], bucket 2 size with all 1000 submitted
	// = 200 * 0 / 600 = 0.
	s := strPtrEven()
	got, err := s.computeBucketSize(5, 2, decimal.MustNew(1000, 0), 5)
	require.NoError(t, err)
	require.True(t, got.IsZero())
}

func TestComputeBucketSize_BucketOutOfRange(t *testing.T) {
	t.Parallel()
	s := strPtrEven()
	// bucketIdx beyond schedule length
	got, err := s.computeBucketSize(5, 5, decimal.Zero, 0)
	require.NoError(t, err)
	require.True(t, got.IsZero())
}

func TestProcessOrderUpdate_PartialFillTracksQty(t *testing.T) {
	t.Parallel()
	s := &Strategy{
		cfg: Config{
			TotalAmount: decimal.MustNew(1000, 0),
			Side:        "buy",
		},
		exec: &exec.Executor{Name: StrategyType, Log: silentLogger()},
		log:  silentLogger(),
		mu:   sync.RWMutex{},
		state: RunState{
			Orders: []exec.OrderEntry{
				{
					OrderID:           "ord1",
					ClientOrderID:     "vwap.run1.uuid1",
					RequestedQuantity: decimal.MustNew(1000, 0),
					FilledQuantity:    decimal.Zero,
					Status:            "OPEN",
				},
			},
		},
	}
	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "vwap.run1.uuid1",
		Status:         "OPEN",
		FilledQuantity: decimal.MustNew(300, 0),
	})
	require.Equal(t, "300", s.state.Orders[0].FilledQuantity.String())
	require.Equal(t, "0", s.state.TotalFilled.String())

	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "vwap.run1.uuid1",
		Status:         "FILLED",
		FilledQuantity: decimal.MustNew(1000, 0),
	})
	require.Equal(t, "1000", s.state.Orders[0].FilledQuantity.String())
	require.Equal(t, "1000", s.state.TotalFilled.String())
}

func TestProcessOrderUpdate_TerminalTransitionsAddOnce(t *testing.T) {
	t.Parallel()
	s := &Strategy{
		cfg: Config{
			TotalAmount: decimal.MustNew(1000, 0),
			Side:        "buy",
		},
		exec: &exec.Executor{Name: StrategyType, Log: silentLogger()},
		log:  silentLogger(),
		mu:   sync.RWMutex{},
		state: RunState{
			Orders: []exec.OrderEntry{
				{
					OrderID:           "ord1",
					ClientOrderID:     "vwap.run1.uuid1",
					RequestedQuantity: decimal.MustNew(500, 0),
					FilledQuantity:    decimal.Zero,
					Status:            "OPEN",
				},
			},
		},
	}
	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "vwap.run1.uuid1",
		Status:         "PARTIAL_FILL",
		FilledQuantity: decimal.MustNew(300, 0),
	})
	require.Equal(t, "300", s.state.TotalFilled.String())

	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "vwap.run1.uuid1",
		Status:         "PARTIAL_FILL",
		FilledQuantity: decimal.MustNew(300, 0),
	})
	require.Equal(t, "300", s.state.TotalFilled.String())
}

func TestProcessOrderUpdate_UnknownClientOrderID(t *testing.T) {
	t.Parallel()
	s := &Strategy{
		cfg:   Config{TotalAmount: decimal.MustNew(1000, 0)},
		exec:  &exec.Executor{Name: StrategyType, Log: silentLogger()},
		log:   silentLogger(),
		mu:    sync.RWMutex{},
		state: RunState{},
	}
	s.processOrderUpdate(context.TODO(), OrderFillEvent{
		ClientOrderID:  "unknown",
		Status:         "FILLED",
		FilledQuantity: decimal.MustNew(100, 0),
	})
	require.Equal(t, "0", s.state.TotalFilled.String())
	require.Empty(t, s.state.Orders)
}
