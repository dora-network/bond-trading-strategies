package http

import "time"

// MomentumBacktestResult is the JSON shape returned by the HTTP API
// for momentum / trend strategy backtests. Per-trade and per-closed-
// trade rows are persisted to strategy_backtest_trades and
// strategy_backtest_closed_trades by the backtest engine via
// stats.BacktestTradeWriter; this struct carries only aggregate
// metrics and the in-memory trade record slice.
type MomentumBacktestResult struct {
	ClosedTrades []MomentumClosedTrade `json:"closed_trades"`
	TradeRecords []MomentumTradeRecord `json:"trade_records"`
	TotalPnL     string                `json:"total_pnl"` //nolint:tagliatelle
	WinCount     int                   `json:"win_count"`
	LossCount    int                   `json:"loss_count"`
	MaxDrawdown  string                `json:"max_drawdown"`
	SharpeRatio  string                `json:"sharpe_ratio"`
}

type MomentumClosedTrade struct {
	BondID       string    `json:"bond_id"`
	OpenTime     time.Time `json:"open_time"`
	CloseTime    time.Time `json:"close_time"`
	Signal       string    `json:"signal"`
	ExitSignal   string    `json:"exit_signal"`
	EntryPrice   string    `json:"entry_price"`
	ExitPrice    string    `json:"exit_price"`
	Quantity     string    `json:"quantity"`
	PositionSize string    `json:"position_size"`
	PnL          string    `json:"pnl"` //nolint:tagliatelle
	ExitReason   string    `json:"exit_reason"`
	// EntryATR intentionally omitted: strategy_backtest_closed_trades
	// has no entry_atr column (only strategy_backtest_trades does, per
	// migration 011). TradeRecord-level entry_atr is preserved on
	// the matching entry row.
}

type MomentumTradeRecord struct {
	Time         time.Time `json:"time"`
	BondID       string    `json:"bond_id"`
	Signal       string    `json:"signal"`
	Price        string    `json:"price"`
	Quantity     string    `json:"quantity"`
	PositionSize string    `json:"position_size"`
	FastMA       string    `json:"fast_ma"`
	SlowMA       string    `json:"slow_ma"`
	EntryATR     string    `json:"entry_atr"`
}
