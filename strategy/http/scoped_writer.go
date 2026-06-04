package http

import (
	"context"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/google/uuid"
)

// scopedBacktestWriter wraps a BacktestTradeWriter to tag every row with
// a fixed backtest_id. The handler generates the backtest id before
// building the strategy, so it can pass the id to both the writer and
// service.RunBacktest.
type scopedBacktestWriter struct {
	backtestID uuid.UUID
	inner      stats.BacktestTradeWriter
}

func newScopedBacktestWriter(id uuid.UUID, inner stats.BacktestTradeWriter) *scopedBacktestWriter {
	return &scopedBacktestWriter{backtestID: id, inner: inner}
}

func (w *scopedBacktestWriter) WriteTradeRecord(ctx context.Context, rec stats.TradeRecordInsert) error {
	rec.BacktestID = w.backtestID
	return w.inner.WriteTradeRecord(ctx, rec)
}

func (w *scopedBacktestWriter) WriteClosedTrade(ctx context.Context, trade stats.ClosedTradeInsert) error {
	trade.BacktestID = w.backtestID
	return w.inner.WriteClosedTrade(ctx, trade)
}
