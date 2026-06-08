package copytrading

import (
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

type TradeRecord struct {
	Time         time.Time
	BondID       string
	Signal       types.Signal
	Price        decimal.Decimal
	Quantity     decimal.Decimal
	OrderSize    decimal.Decimal
	Cash         decimal.Decimal
	OpenPosition decimal.Decimal
	TradeID      uuid.UUID
}

type ClosedTrade struct {
	OpenTime     time.Time
	CloseTime    time.Time
	BondID       string
	OpenSignal   types.Signal
	CloseSignal  types.Signal
	Quantity     decimal.Decimal
	EntryPrice   decimal.Decimal
	ExitPrice    decimal.Decimal
	PnL          decimal.Decimal
	EntryBalance decimal.Decimal
	OpenTradeID  uuid.UUID
	CloseTradeID uuid.UUID
}

type BacktestResult struct {
	TradeRecords []TradeRecord
	ClosedTrades []ClosedTrade
	TotalPnL     decimal.Decimal
	WinCount     int
	LossCount    int
	MaxDrawdown  decimal.Decimal
	SharpeRatio  decimal.Decimal
}

func (r BacktestResult) GetTotalPnL() decimal.Decimal    { return r.TotalPnL }
func (r BacktestResult) GetWinCount() int                { return r.WinCount }
func (r BacktestResult) GetLossCount() int               { return r.LossCount }
func (r BacktestResult) GetMaxDrawdown() decimal.Decimal { return r.MaxDrawdown }
func (r BacktestResult) GetSharpeRatio() decimal.Decimal { return r.SharpeRatio }
func (r BacktestResult) GetTradeRecords() any            { return r.TradeRecords }
func (r BacktestResult) GetClosedTrades() any            { return r.ClosedTrades }

var _ types.BacktestResult = BacktestResult{}
