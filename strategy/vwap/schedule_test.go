package vwap

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/govalues/decimal"

	"github.com/dora-network/bond-trading-strategies/strategy/breakout"
)

// fakeStore implements TradeVolumeStore for tests.
type fakeStore struct {
	trades []breakout.Trade
	err    error
}

func (f *fakeStore) StreamTrades(_ context.Context, _ uuid.UUID, _, _ time.Time) (<-chan breakout.Trade, <-chan error) {
	tCh := make(chan breakout.Trade, len(f.trades))
	eCh := make(chan error, 1)
	for _, t := range f.trades {
		tCh <- t
	}
	if f.err != nil {
		eCh <- f.err
	}
	close(tCh)
	close(eCh)
	return tCh, eCh
}

func TestBuildSchedule_NoHistoryFallsBackToEven(t *testing.T) {
	t.Parallel()
	cfg := validConfig() // 1000 total, 12 buckets at 5 min → ~83.33 per bucket
	sched, err := BuildSchedule(context.TODO(), &fakeStore{}, uuid.New(), cfg)
	require.NoError(t, err)
	require.Len(t, sched.Buckets, 12)
	t.Logf("buckets=%v total=%s", sched.Buckets, sched.Total().String())
	require.Equal(t, "1000", sched.Total().String())
	for i := 0; i < len(sched.Buckets)-1; i++ {
		require.Equal(t, "83", sched.Buckets[i].String())
	}
}

func TestBuildSchedule_AllocatesProportionallyToADV(t *testing.T) {
	t.Parallel()
	cfg := validConfig()
	obID := uuid.New()
	// Two trades in the same minute bucket (09:00-09:05), each qty 100.
	// Sum = 200, mean daily = 200/30 = 6.67. Only that one bucket
	// has ADV. Scale = 1000 / 6.67 ≈ 150. First bucket ≈ 1000.5; the
	// last bucket absorbs the rounding remainder so the total equals
	// 1000 exactly.
	store := &fakeStore{trades: []breakout.Trade{
		{Time: time.Date(2025, 1, 1, 9, 2, 0, 0, time.UTC), Quantity: decimal.MustNew(100, 0)},
		{Time: time.Date(2025, 1, 2, 9, 3, 0, 0, time.UTC), Quantity: decimal.MustNew(100, 0)},
	}}
	sched, err := BuildSchedule(context.TODO(), store, obID, cfg)
	require.NoError(t, err)
	require.Len(t, sched.Buckets, 12)
	require.Equal(t, "1000", sched.Total().String())
	// First bucket gets a non-zero allocation.
	require.True(t, sched.Buckets[0].IsPos())
	// Buckets 1..10 are zero (no ADV in their time-of-day buckets).
	for i := 1; i <= 10; i++ {
		require.True(t, sched.Buckets[i].IsZero(), "bucket %d should be zero", i)
	}
	// First + last = TotalAmount exactly.
	sum := decimal.Zero
	sum, _ = sum.Add(sched.Buckets[0])
	sum, _ = sum.Add(sched.Buckets[11])
	require.Equal(t, "1000", sum.String())
}

func TestBuildSchedule_NoBucketsReturnsError(t *testing.T) {
	t.Parallel()
	cfg := validConfig()
	cfg.EndTime = cfg.StartTime
	_, err := BuildSchedule(context.TODO(), &fakeStore{}, uuid.New(), cfg)
	require.Error(t, err)
}
