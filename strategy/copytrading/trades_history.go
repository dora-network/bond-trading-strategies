package copytrading

import (
	"context"
	"time"

	"github.com/govalues/decimal"
)

// Trade is the in-memory representation of a single row in trades_history.
// It is intentionally decoupled from doraclient.Trade so the simulation
// does not depend on the DORA client's generated types.
type Trade struct {
	TransactionID      string
	OrderID            string
	OrderSeq           int64
	OrderBookID        string
	UserID             string
	Asset0             string
	Quantity0          decimal.Decimal
	Price              decimal.Decimal
	Side               string // "BUY" or "SELL", matching the DORA Side enum
	AggressorIndicator bool
	CreatedAt          time.Time
}

// tradesHistoryStore is the backtest's read-only data source for a
// followed trader's persisted trade history.
type tradesHistoryStore interface {
	StreamTrades(ctx context.Context, userID string, start, end time.Time) (<-chan Trade, <-chan error)
	TradeBounds(ctx context.Context, userID string) (min, max time.Time, count int, err error)
}

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o copytradingfakes/fake_trades_history_store.go . tradesHistoryStore
