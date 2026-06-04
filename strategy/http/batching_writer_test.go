package http

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBatchStore counts bulk-insert calls. It implements BacktestStore
// just well enough to back a BatchingBacktestWriter; the read-side
// methods are unused.
type fakeBatchStore struct {
	mu            sync.Mutex
	tradeBatches  []int
	closedBatches []int
	trades        []stats.TradeRecordInsert
	closedTrades  []stats.ClosedTradeInsert
}

func (f *fakeBatchStore) LoadBacktests(_ context.Context) ([]*BacktestDetail, error) {
	return nil, nil
}

func (f *fakeBatchStore) LoadBacktestResult(_ context.Context, _ uuid.UUID) (json.RawMessage, error) {
	return nil, nil
}

func (f *fakeBatchStore) SaveBacktest(_ context.Context, _ *BacktestDetail) error { return nil }

func (f *fakeBatchStore) GetBacktestTrades(_ context.Context, _ uuid.UUID, _ string, _, _ int) (json.RawMessage, error) {
	return nil, nil
}

func (f *fakeBatchStore) GetBacktestClosedTrades(_ context.Context, _ uuid.UUID, _ string, _, _ int) (json.RawMessage, error) {
	return nil, nil
}

func (f *fakeBatchStore) WriteTradeRecord(_ context.Context, rec stats.TradeRecordInsert) error {
	return nil
}

func (f *fakeBatchStore) WriteClosedTrade(_ context.Context, _ stats.ClosedTradeInsert) error {
	return nil
}

func (f *fakeBatchStore) WriteTradeRecordsBatch(_ context.Context, recs []stats.TradeRecordInsert) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tradeBatches = append(f.tradeBatches, len(recs))
	f.trades = append(f.trades, recs...)
	return nil
}

func (f *fakeBatchStore) WriteClosedTradesBatch(_ context.Context, trades []stats.ClosedTradeInsert) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closedBatches = append(f.closedBatches, len(trades))
	f.closedTrades = append(f.closedTrades, trades...)
	return nil
}

func (f *fakeBatchStore) Flush(_ context.Context) error { return nil }

func TestBatchingWriter_FlushesAtBatchSize(t *testing.T) {
	t.Parallel()
	store := &fakeBatchStore{}
	b := NewBatchingBacktestWriter(store, 10, 10*time.Second)
	t.Cleanup(func() { _ = b.Close() })

	for i := 0; i < 25; i++ {
		require.NoError(t, b.WriteTradeRecord(context.Background(), stats.TradeRecordInsert{
			BacktestID: uuid.Nil,
			Time:       time.Unix(int64(i), 0),
			Signal:     "BUY",
		}))
	}

	// 25 rows in batches of 10 → 2 full batches flushed by WriteTradeRecord
	// plus the trailing 15 still buffered. Force a final flush.
	require.NoError(t, b.Flush(context.Background()))

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Len(t, store.tradeBatches, 3, "two auto-flushes at 10 + one Flush()")
	assert.Equal(t, 10, store.tradeBatches[0])
	assert.Equal(t, 10, store.tradeBatches[1])
	assert.Equal(t, 5, store.tradeBatches[2])
	assert.Len(t, store.trades, 25)
}

func TestBatchingWriter_FlushesOnTime(t *testing.T) {
	t.Parallel()
	store := &fakeBatchStore{}
	b := NewBatchingBacktestWriter(store, 1000, 30*time.Millisecond)
	t.Cleanup(func() { _ = b.Close() })

	require.NoError(t, b.WriteTradeRecord(context.Background(), stats.TradeRecordInsert{
		BacktestID: uuid.Nil,
		Time:       time.Now(),
		Signal:     "BUY",
	}))
	require.NoError(t, b.WriteClosedTrade(context.Background(), stats.ClosedTradeInsert{
		BacktestID: uuid.Nil,
		OpenTime:   time.Now(),
		CloseTime:  time.Now(),
		OpenSignal: "BUY", CloseSignal: "SELL",
	}))

	// Wait for at least one ticker cycle.
	time.Sleep(80 * time.Millisecond)

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.NotEmpty(t, store.tradeBatches, "time-based flush should have fired")
	assert.NotEmpty(t, store.closedBatches, "time-based flush should have fired")
}

func TestBatchingWriter_CloseFlushesTrailing(t *testing.T) {
	t.Parallel()
	store := &fakeBatchStore{}
	b := NewBatchingBacktestWriter(store, 1000, time.Hour) // long flush interval

	for i := 0; i < 3; i++ {
		require.NoError(t, b.WriteTradeRecord(context.Background(), stats.TradeRecordInsert{
			BacktestID: uuid.Nil,
			Time:       time.Unix(int64(i), 0),
			Signal:     "BUY",
		}))
	}

	require.NoError(t, b.Close())

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Len(t, store.tradeBatches, 1)
	assert.Equal(t, 3, store.tradeBatches[0])
}

func TestBatchingWriter_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	store := &fakeBatchStore{}
	b := NewBatchingBacktestWriter(store, 100, time.Second)
	require.NoError(t, b.Close())
	// Second close should be safe.
	require.NoError(t, b.Close())
}

func TestBatchingWriter_ClosedTradesShareBuffer(t *testing.T) {
	t.Parallel()
	store := &fakeBatchStore{}
	b := NewBatchingBacktestWriter(store, 5, time.Hour)
	t.Cleanup(func() { _ = b.Close() })

	// Mix trade records and closed trades; both end up in the same flush.
	for i := 0; i < 3; i++ {
		require.NoError(t, b.WriteTradeRecord(context.Background(), stats.TradeRecordInsert{
			BacktestID: uuid.Nil, Time: time.Unix(int64(i), 0), Signal: "BUY",
		}))
		require.NoError(t, b.WriteClosedTrade(context.Background(), stats.ClosedTradeInsert{
			BacktestID: uuid.Nil, OpenTime: time.Unix(int64(i), 0), CloseTime: time.Unix(int64(i), 0),
			OpenSignal: "BUY", CloseSignal: "SELL",
		}))
	}

	require.NoError(t, b.Flush(context.Background()))

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 3, store.tradeBatches[0])
	assert.Equal(t, 3, store.closedBatches[0])
}
