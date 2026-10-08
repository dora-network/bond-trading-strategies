package momentum

import (
	"time"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/strategy/config"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// StrategyType is the strategy.Decision.StrategyType value and the
// client_order_id prefix used by the live run loop.
const StrategyType = "momentum"

// Signal source constants (Config.SignalSource).
const (
	SignalSourcePrice  = "price"
	SignalSourceYTM    = "ytm"
	SignalSourceSpread = "spread"
)

// Exit reason constants for ClosedTrade.
const (
	ExitReasonStopLoss     = "stop_loss"
	ExitReasonTakeProfit   = "take_profit"
	ExitReasonReversal     = "reversal"
	ExitReasonStrategyExit = "strategy_exit"
)

// Decision reason codes surfaced through types.Decision.Reason().
const (
	DecisionReasonWarmingUp          = "warming_up"
	DecisionReasonFlat               = "flat"
	DecisionReasonMACrossoverUp      = "ma_crossover_up"
	DecisionReasonMACrossoverDown    = "ma_crossover_down"
	DecisionReasonVolumeNotConfirmed = "volume_not_confirmed"
)

// Config holds all tunable parameters for the momentum strategy.
type Config struct {
	config.Config

	// SignalSource selects which series the MA crossover runs on.
	// One of SignalSourcePrice, SignalSourceYTM, SignalSourceSpread.
	SignalSource string

	// FastWindow / SlowWindow are the fast/slow MA bar windows.
	FastWindow int
	SlowWindow int

	// Resolution is the candle resolution of the bar series (candles
	// enum: 1m, 5m, 15m, 1h, 4h, 1d).
	Resolution candles.Resolution

	// ATRWindow is the true-range ATR window used for exits.
	ATRWindow int

	// VolumeAvgWindow is the rolling mean-volume window confirming
	// entries; 0 disables the volume gate.
	VolumeAvgWindow int

	// VolumeRatioThreshold: an entry requires bar volume >= this × the
	// rolling mean volume.
	VolumeRatioThreshold decimal.Decimal

	// StopLossATR / TakeProfitATR are exit distances in ATR units.
	// 0 disables each.
	StopLossATR   decimal.Decimal
	TakeProfitATR decimal.Decimal

	// MinOrderSize skips opening when the computed quantity is below it
	// (0 disables). MaxOrderSize clamps the quantity down (0 disables).
	// Decimal because Dora is a fractionalized market.
	MinOrderSize decimal.Decimal
	MaxOrderSize decimal.Decimal

	// MaxPositionSize caps the fraction of capital per trade (0,1].
	MaxPositionSize decimal.Decimal

	// OrderBookID is the DORA order book to place orders on.
	OrderBookID uuid.UUID

	// Tenor is required when SignalSource == spread.
	Tenor string

	// InitialBalance is the starting capital. Leverage scales it.
	InitialBalance decimal.Decimal
	Leverage       decimal.Decimal
}

// DefaultConfig returns sensible defaults for live deployment and tests.
// Windows are calibrated for 15m bars: fast 8 bars ≈ 2h, slow 96 ≈ 24h,
// ATR 24 bars ≈ 6h.
func DefaultConfig() Config {
	return Config{
		SignalSource:         SignalSourcePrice,
		Resolution:           candles.Resolution15m,
		FastWindow:           8,
		SlowWindow:           96,
		ATRWindow:            24,
		VolumeAvgWindow:      20,
		VolumeRatioThreshold: decimal.One,
		StopLossATR:          decimal.MustNew(20, 0), //nolint:mnd
		TakeProfitATR:        decimal.Zero,
		MinOrderSize:         decimal.Zero,
		MaxOrderSize:         decimal.Zero,
		MaxPositionSize:      decimal.One,
		InitialBalance:       decimal.One,
		Leverage:             decimal.One,
	}
}

// Decision is the per-tick evaluation output of Strategy.Update. It
// implements types.Decision via the seven accessor methods. Fields that
// collide with interface method names are unexported.
type Decision struct {
	time         time.Time
	bondID       string
	price        decimal.Decimal
	signal       types.Signal
	positionSize decimal.Decimal
	reason       string

	// Exported: read directly by the backtester.
	FastMA      decimal.Decimal
	SlowMA      decimal.Decimal
	ATR         decimal.Decimal
	SeriesValue decimal.Decimal
	Trend       string // "up", "down", or ""
}

func (d Decision) Time() time.Time               { return d.time }
func (d Decision) BondID() string                { return d.bondID }
func (d Decision) Price() decimal.Decimal        { return d.price }
func (d Decision) Signal() types.Signal          { return d.signal }
func (d Decision) PositionSize() decimal.Decimal { return d.positionSize }
func (d Decision) StrategyType() string          { return StrategyType }
func (d Decision) Reason() string                { return d.reason }

// TradeRecord captures a single simulated trade event (entry or exit).
type TradeRecord struct {
	Time         time.Time
	BondID       string
	Signal       types.Signal
	Price        decimal.Decimal
	Quantity     decimal.Decimal
	PositionSize decimal.Decimal
	FastMA       decimal.Decimal
	SlowMA       decimal.Decimal
	// EntryATR is the ATR at open; stable for the position's life so
	// stop/take thresholds don't drift. Set on the opening record and
	// mirrored onto the matching exit row (uniform persisted rows).
	EntryATR decimal.Decimal
}

// ClosedTrade records a completed round-trip and its PnL (price terms).
type ClosedTrade struct {
	BondID       string
	OpenTime     time.Time
	CloseTime    time.Time
	Signal       types.Signal // opening direction
	ExitSignal   types.Signal // strategy signal at exit
	EntryPrice   decimal.Decimal
	ExitPrice    decimal.Decimal
	EntryATR     decimal.Decimal
	Quantity     decimal.Decimal
	PositionSize decimal.Decimal
	PnL          decimal.Decimal
	ExitReason   string
}

// BacktestResult summarises a full backtest and implements types.BacktestResult.
type BacktestResult struct {
	ClosedTrades []ClosedTrade
	TradeRecords []TradeRecord
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
