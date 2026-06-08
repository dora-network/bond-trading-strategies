package http

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
)

// BatchingBacktestWriter buffers WriteTradeRecord / WriteClosedTrade calls
// and flushes them to an inner BacktestTradeWriter either when the
// buffer reaches the configured batch size, or when the configured
// flush interval elapses, whichever happens first. The default values
// (1000 rows, 1s) match the documented behaviour for production
// backtests; the in-memory test fixture uses the same interface but
// ignores the batching.
//
// Why batching: a per-row pgx INSERT runs in ~25ms over the network.
// For a 20-day backtest emitting ~170k trade rows that's >70 minutes.
// A single pgx.CopyFrom over 1000 rows is sub-second; the same 170k
// rows finish in a few minutes.
type BatchingBacktestWriter struct {
	inner      BacktestStore // store, not raw writer — we use the batch methods
	batchSize  int
	flushAfter time.Duration

	mu           sync.Mutex
	trades       []stats.TradeRecordInsert
	closedTrades []stats.ClosedTradeInsert
	lastFlush    time.Time

	// flushMu serialises flushes against new writes. The background
	// ticker and explicit Flush calls both take it; WriteXxx takes it
	// briefly to append.
	flushMu sync.Mutex

	// stopCh signals the background ticker to exit. close() sends
	// here; the ticker goroutine selects on it alongside the tick.
	stopCh chan struct{}
	doneCh chan struct{}
}

// NewBatchingBacktestWriter constructs and starts a batching writer.
// The background ticker begins immediately; call Close to stop it and
// flush any remaining rows.
func NewBatchingBacktestWriter(
	store BacktestStore,
	batchSize int,
	flushAfter time.Duration,
) *BatchingBacktestWriter {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if flushAfter <= 0 {
		flushAfter = time.Second
	}
	b := &BatchingBacktestWriter{
		inner:      store,
		batchSize:  batchSize,
		flushAfter: flushAfter,
		lastFlush:  time.Now(),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
	go b.runTicker()
	return b
}

func (b *BatchingBacktestWriter) WriteTradeRecord(ctx context.Context, rec stats.TradeRecordInsert) error {
	b.mu.Lock()
	b.trades = append(b.trades, rec)
	shouldFlush := len(b.trades) >= b.batchSize
	b.mu.Unlock()
	if shouldFlush {
		return b.Flush(ctx)
	}
	return nil
}

func (b *BatchingBacktestWriter) WriteClosedTrade(ctx context.Context, trade stats.ClosedTradeInsert) error {
	b.mu.Lock()
	b.closedTrades = append(b.closedTrades, trade)
	shouldFlush := len(b.closedTrades) >= b.batchSize
	b.mu.Unlock()
	if shouldFlush {
		return b.Flush(ctx)
	}
	return nil
}

// Flush drains the trade and closed-trade buffers to the underlying
// store via bulk-insert methods. Safe to call from any goroutine. The
// backtest engine must call Flush at the end of its simulation to
// persist trailing rows before the backtest status flips to completed.
func (b *BatchingBacktestWriter) Flush(ctx context.Context) error {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	b.mu.Lock()
	trades := b.trades
	closedTrades := b.closedTrades
	b.trades = nil
	b.closedTrades = nil
	b.lastFlush = time.Now()
	b.mu.Unlock()

	var firstErr error
	if len(trades) > 0 {
		if err := b.inner.WriteTradeRecordsBatch(ctx, trades); err != nil {
			firstErr = err
		}
	}
	if len(closedTrades) > 0 {
		if err := b.inner.WriteClosedTradesBatch(ctx, closedTrades); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Close stops the background ticker and flushes any remaining rows.
// Idempotent: subsequent calls are safe no-ops.
func (b *BatchingBacktestWriter) Close() error {
	select {
	case <-b.stopCh:
		// already closed
	default:
		close(b.stopCh)
		<-b.doneCh
	}
	return b.Flush(context.Background())
}

func (b *BatchingBacktestWriter) runTicker() {
	defer close(b.doneCh)
	ticker := time.NewTicker(b.flushAfter)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.mu.Lock()
			hasWork := len(b.trades) > 0 || len(b.closedTrades) > 0
			b.mu.Unlock()
			if !hasWork {
				continue
			}
			if err := b.Flush(context.Background()); err != nil {
				slog.Error("batching backtest writer flush", "err", err)
			}
		}
	}
}
