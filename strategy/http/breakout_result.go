package http

import "time"

type BreakoutBacktestResult struct {
	ClosedTrades []BreakoutClosedTrade `json:"closed_trades"`
	TradeRecords []BreakoutTradeRecord `json:"trade_records"`
	TotalPnL     string                `json:"total_pnl"` //nolint:tagliatelle
	WinCount     int                   `json:"win_count"`
	LossCount    int                   `json:"loss_count"`
	MaxDrawdown  string                `json:"max_drawdown"`
	SharpeRatio  string                `json:"sharpe_ratio"`
}

type BreakoutClosedTrade struct {
	BondID                string    `json:"bond_id"`
	OpenTime              time.Time `json:"open_time"`
	CloseTime             time.Time `json:"close_time"`
	Signal                string    `json:"signal"`
	ExitSignal            string    `json:"exit_signal"`
	EntryPrice            string    `json:"entry_price"`
	ExitPrice             string    `json:"exit_price"`
	Quantity              string    `json:"quantity"`
	PositionSize          string    `json:"position_size"`
	PnL                   string    `json:"pnl"` //nolint:tagliatelle
	ExitReason            string    `json:"exit_reason"`
	EntryCompressionRatio string    `json:"entry_compression_ratio"`
	ExitCompressionRatio  string    `json:"exit_compression_ratio"`
}

type BreakoutTradeRecord struct {
	Time             time.Time `json:"time"`
	BondID           string    `json:"bond_id"`
	Signal           string    `json:"signal"`
	Price            string    `json:"price"`
	Quantity         string    `json:"quantity"`
	PositionSize     string    `json:"position_size"`
	CompressionRatio string    `json:"compression_ratio"`
	EntryATR         string    `json:"entry_atr"`
}
