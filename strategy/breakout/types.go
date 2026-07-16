package breakout

import (
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/govalues/decimal"
)

// StrategyType is the value written to strategy.Decision.StrategyType
// when a live breakout run places an order. Exported so cmd/strategy-
// server can pass it into the orderupdates.Filter alongside the other
// strategy type constants, matching the pattern PR #20 established
// for mean_reversion and copy_trading.
const StrategyType = "breakout"

// Decision reason codes surfaced through the types.Decision.Reason()
// accessor. Persisted in strategy_decisions.reason by the live run loop.
const (
	DecisionReasonNoSignalYet      = "no_signal_yet"
	DecisionReasonWarmingUp        = "warming_up"
	DecisionReasonVolTooLow        = "vol_too_low"
	DecisionReasonCompressionEntry = "compression_breakout"
	DecisionReasonReversal         = "breakout_reversal"
)

// Exit reason constants for ClosedTrade.
const (
	ExitReasonTakeProfit   = "take_profit"
	ExitReasonStopLoss     = "stop_loss"
	ExitReasonStrategyExit = "strategy_exit"
	ExitReasonReversal     = "reversal" // opposite-signal exit
)

// Decision is the in-process evaluation output of breakout.Strategy.Update.
// It implements the types.Decision interface via the seven accessor methods
// below; strategy-specific fields (ShortVol, LongVol, CompressionRatio, ATR,
// BreakoutLevel, CompressionArmed, BarsAboveTrigger) stay exported because
// they are read directly by the breakout backtest.
//
// Fields that collide with interface methods (Time, BondID, Price, Signal,
// PositionSize, Reason) are unexported — Go forbids a method and a field
// sharing a name on the same type. Same convention as meanreversion.Decision.
type Decision struct {
	time         time.Time
	bondID       string
	price        decimal.Decimal
	signal       types.Signal
	positionSize decimal.Decimal
	reason       string

	// Compression / breakout context (read by the breakout backtest).
	ShortVol         decimal.Decimal // σ(price, ShortVolWindow)
	LongVol          decimal.Decimal // σ(price, LongVolWindow)
	CompressionRatio decimal.Decimal // ShortVol / LongVol at the current tick
	ATR              decimal.Decimal // ATR(ATRWindow) of absolute price diffs
	BreakoutLevel    decimal.Decimal // mid ± k·ATR; 0 if no breakout in progress
	CompressionArmed bool            // true once compressionRatio crossed threshold
	BarsAboveTrigger int             // count of consecutive closes beyond BreakoutLevel
	// ArmedCompressionRatio is the ratio at the tick that armed the
	// strategy (crossed below CompressionThreshold). Stable through the
	// confirmation window; the backtester records this in
	// TradeRecord.CompressionRatio for signal verification.
	ArmedCompressionRatio decimal.Decimal
}

// Accessor methods satisfying types.Decision. Bare names match the interface
// contract; struct fields above are unexported to avoid the Go name collision.
func (d Decision) Time() time.Time               { return d.time }
func (d Decision) BondID() string                { return d.bondID }
func (d Decision) Price() decimal.Decimal        { return d.price }
func (d Decision) Signal() types.Signal          { return d.signal }
func (d Decision) PositionSize() decimal.Decimal { return d.positionSize }
func (d Decision) StrategyType() string          { return StrategyType }
func (d Decision) Reason() string                { return d.reason }

// TradeRecord captures a single simulated trade event in the backtest.
type TradeRecord struct {
	Time             time.Time
	BondID           string
	Signal           types.Signal
	Price            decimal.Decimal
	Quantity         decimal.Decimal
	PositionSize     decimal.Decimal
	CompressionRatio decimal.Decimal
	// EntryATR is the ATR computed at the trade's open tick. The
	// Backtester uses this for stop-loss / take-profit distance
	// calculations rather than the current bar's ATR, so the
	// threshold is stable for the lifetime of the position.
	EntryATR decimal.Decimal
}

// ClosedTrade records a completed round-trip trade and its PnL.
type ClosedTrade struct {
	BondID                string
	OpenTime              time.Time
	CloseTime             time.Time
	Signal                types.Signal
	ExitSignal            types.Signal
	EntryPrice            decimal.Decimal
	ExitPrice             decimal.Decimal
	Quantity              decimal.Decimal
	PositionSize          decimal.Decimal
	PnL                   decimal.Decimal
	ExitReason            string
	EntryCompressionRatio decimal.Decimal
	ExitCompressionRatio  decimal.Decimal
}

// BacktestResult summarises a full backtest run and implements
// types.BacktestResult so the strategy-server can read it without
// depending on breakout's concrete record shapes.
type BacktestResult struct {
	ClosedTrades []ClosedTrade
	TradeRecords []TradeRecord
	TotalPnL     decimal.Decimal
	WinCount     int
	LossCount    int
	MaxDrawdown  decimal.Decimal
	SharpeRatio  decimal.Decimal // annualised, assumes daily observations
}

func (r BacktestResult) GetTotalPnL() decimal.Decimal    { return r.TotalPnL }
func (r BacktestResult) GetWinCount() int                { return r.WinCount }
func (r BacktestResult) GetLossCount() int               { return r.LossCount }
func (r BacktestResult) GetMaxDrawdown() decimal.Decimal { return r.MaxDrawdown }
func (r BacktestResult) GetSharpeRatio() decimal.Decimal { return r.SharpeRatio }
func (r BacktestResult) GetTradeRecords() any            { return r.TradeRecords }
func (r BacktestResult) GetClosedTrades() any            { return r.ClosedTrades }

var _ types.BacktestResult = BacktestResult{}
