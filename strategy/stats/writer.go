package stats

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// BacktestTradeWriter is the interface a backtest engine uses to persist
// individual trade records and closed trades as they are produced by the
// simulation. The strategy-server streams each row to the store instead of
// accumulating them in one large JSONB column on strategy_backtests (which
// crashed the DB for backtests exceeding ~50MB of combined trade data).
type BacktestTradeWriter interface {
	WriteTradeRecord(ctx context.Context, rec TradeRecordInsert) error
	WriteClosedTrade(ctx context.Context, trade ClosedTradeInsert) error
}

// TradeRecordInsert is the row shape for strategy_backtest_trades. Strategy
// engines convert their own TradeRecord types into this struct before
// calling WriteTradeRecord. Fields that don't apply to a given strategy
// are left as their zero value (uuid.Nil, decimal.Zero, "").
type TradeRecordInsert struct {
	BacktestID   uuid.UUID
	Time         time.Time
	BondID       string
	Signal       string
	Price        decimal.Decimal
	Quantity     decimal.Decimal
	EntryBalance decimal.Decimal

	// copytrading-only fields. Zero if not a copytrading trade.
	OrderSize    decimal.Decimal
	Cash         decimal.Decimal
	OpenPosition decimal.Decimal
	TradeID      uuid.UUID

	// meanreversion-only fields. Zero if not a meanreversion trade.
	Spread       decimal.Decimal
	PositionSize decimal.Decimal
	ZScore       decimal.Decimal
}

// ClosedTradeInsert is the row shape for strategy_backtest_closed_trades.
// Strategy engines convert their own ClosedTrade types into this struct
// before calling WriteClosedTrade. Fields that don't apply to a given
// strategy are left as their zero value.
type ClosedTradeInsert struct {
	BacktestID   uuid.UUID
	OpenTime     time.Time
	CloseTime    time.Time
	BondID       string
	OpenSignal   string
	CloseSignal  string
	Quantity     decimal.Decimal
	EntryPrice   decimal.Decimal
	ExitPrice    decimal.Decimal
	PnL          decimal.Decimal
	EntryBalance decimal.Decimal

	// copytrading-only fields. Zero if not a copytrading trade.
	OpenTradeID  uuid.UUID
	CloseTradeID uuid.UUID

	// meanreversion-only fields. Zero if not a meanreversion trade.
	EntrySpread  decimal.Decimal
	ExitSpread   decimal.Decimal
	EntryZScore  decimal.Decimal
	ExitZScore   decimal.Decimal
	PositionSize decimal.Decimal
	ExitReason   string
}
