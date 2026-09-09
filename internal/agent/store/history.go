// Package store holds the persistence seams shared across the agent
// packages. The concrete *HistoryStore (pgxpool, public schema) lives
// in history_store.go.
package store

import (
	"context"
	"time"
)

// Candle is one row of agent.candles_history, alias-free. The
// framework's dorastrategy.Candle is a separate type; the backtest/live
// code converts. Shapes mirror dora-agent's internal/history.
type Candle struct {
	OrderBookID    string
	StartTimestamp time.Time
	Open           string
	High           string
	Low            string
	Close          string
	OpenYtm        string
	HighYtm        string
	LowYtm         string
	CloseYtm       string
	Volume         string
}

// Trade is one row of agent.trades_history, alias-free.
type Trade struct {
	TransactionID      string
	OrderBookID        string
	OrderID            string
	OrderSeq           int64
	UserID             string
	Asset0             string
	Price              string
	Quantity0          string
	Side               string
	AggressorIndicator bool
	CreatedAt          time.Time
}

// Price is one row of agent.price_history.
type Price struct {
	AssetID   string
	Price     string
	YTM       string
	Timestamp time.Time
}

// HistoryFetcher is the fetch surface of the history store. Consumers
// (backtest starter, live host) hold this interface so tests can
// substitute a fake; the concrete *HistoryStore (L7) satisfies it.
// Cursor is an opaque keyset cursor; "" is the first page.
type HistoryFetcher interface {
	FetchCandles(ctx context.Context, orderBookID string, start, end time.Time,
		resolution string, cursor string, batchSize int) ([]Candle, string, error)
	FetchTrades(ctx context.Context, orderBookID string, start, end time.Time,
		cursor string, batchSize int) ([]Trade, string, error)
	FetchPrices(ctx context.Context, assetID string, start, end time.Time,
		cursor string, batchSize int) ([]Price, string, error)
}

// NopHistoryFetcher returns no rows for every fetch. Tests that don't
// exercise the history path use it (or a nil fetcher — WasmStarter
// treats nil as the legacy REST path).
type NopHistoryFetcher struct{}

func (NopHistoryFetcher) FetchCandles(context.Context, string, time.Time, time.Time, string, string, int) ([]Candle, string, error) {
	return nil, "", nil
}

func (NopHistoryFetcher) FetchTrades(context.Context, string, time.Time, time.Time, string, int) ([]Trade, string, error) {
	return nil, "", nil
}

func (NopHistoryFetcher) FetchPrices(context.Context, string, time.Time, time.Time, string, int) ([]Price, string, error) {
	return nil, "", nil
}
