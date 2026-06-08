package types

import "github.com/govalues/decimal"

// BacktestResult is the interface implemented by every strategy's backtest
// output. The strategy server reads the result through this interface and is
// therefore decoupled from strategy-specific record shapes; the concrete trade
// and closed-trade slices are exposed as any and decoded per-strategy at the
// API boundary.
//
// Method names use the Get prefix to avoid collision with the exported field
// names on the implementing structs (Go disallows a field and a method with the
// same name on the same type).
type BacktestResult interface {
	GetTotalPnL() decimal.Decimal
	GetWinCount() int
	GetLossCount() int
	GetMaxDrawdown() decimal.Decimal
	GetSharpeRatio() decimal.Decimal
	// GetTradeRecords returns the strategy-specific trade records (e.g.
	// []meanreversion.TradeRecord or []copytrading.TradeRecord).
	GetTradeRecords() any
	// GetClosedTrades returns the strategy-specific closed trades.
	GetClosedTrades() any
}

// ErrorResult is the BacktestResult the service sends on the result channel
// when Strategy.Backtest returns an error. All numeric accessors return zero
// values; the wrapped error is read via the Err field.
type ErrorResult struct {
	Err error
}

func (ErrorResult) GetTotalPnL() decimal.Decimal    { return decimal.Zero }
func (ErrorResult) GetWinCount() int                { return 0 }
func (ErrorResult) GetLossCount() int               { return 0 }
func (ErrorResult) GetMaxDrawdown() decimal.Decimal { return decimal.Zero }
func (ErrorResult) GetSharpeRatio() decimal.Decimal { return decimal.Zero }
func (ErrorResult) GetTradeRecords() any            { return nil }
func (ErrorResult) GetClosedTrades() any            { return nil }
