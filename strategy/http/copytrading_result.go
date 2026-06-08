package http

import "time"

type CopyTradingBacktestResult struct {
	ClosedTrades []CopyTradingClosedTrade `json:"closed_trades"`
	TradeRecords []CopyTradingTradeRecord `json:"trade_records"`
	TotalPnL     string                   `json:"total_pnl"` //nolint:tagliatelle
	WinCount     int                      `json:"win_count"`
	LossCount    int                      `json:"loss_count"`
	MaxDrawdown  string                   `json:"max_drawdown"`
	SharpeRatio  string                   `json:"sharpe_ratio"`
}

type CopyTradingClosedTrade struct {
	OpenTime     time.Time `json:"open_time"`
	CloseTime    time.Time `json:"close_time"`
	BondID       string    `json:"bond_id"`
	OpenSignal   string    `json:"open_signal"`
	CloseSignal  string    `json:"close_signal"`
	Quantity     string    `json:"quantity"`
	EntryPrice   string    `json:"entry_price"`
	ExitPrice    string    `json:"exit_price"`
	PnL          string    `json:"pnl"` //nolint:tagliatelle
	EntryBalance string    `json:"entry_balance,omitempty"`
	OpenTradeID  string    `json:"open_trade_id"`
	CloseTradeID string    `json:"close_trade_id"`
}

type CopyTradingTradeRecord struct {
	Time         time.Time `json:"time"`
	BondID       string    `json:"bond_id"`
	Signal       string    `json:"signal"`
	Price        string    `json:"price"`
	Quantity     string    `json:"quantity"`
	OrderSize    string    `json:"order_size"`
	Cash         string    `json:"cash"`
	OpenPosition string    `json:"open_position"`
	TradeID      string    `json:"trade_id"`
}
