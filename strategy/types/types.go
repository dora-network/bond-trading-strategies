// Package types defines the shared domain types that the strategy framework
// and every strategy implementation agree on (signal vocabulary, observation
// input, and the per-tick decision).
//
// Strategy-specific result shapes (BacktestResult, TradeRecord, ClosedTrade)
// live with the strategy that produces them so the framework can stay
// decoupled from per-strategy record definitions. The shared BacktestResult
// interface in result.go is the only contract the framework reads.
package types

import (
	"time"

	"github.com/govalues/decimal"
)

// Signal is the trading action the strategy recommends for a bond.
type Signal int

const (
	SignalHold Signal = iota // within the neutral band — do nothing
	SignalBuy                // spread too wide — bond is cheap, expect convergence
	SignalSell               // spread too tight — bond is rich, expect reversion
)

func (s Signal) String() string {
	switch s {
	case SignalBuy:
		return "BUY"
	case SignalSell:
		return "SELL"
	default:
		return "HOLD"
	}
}

// YieldObservation is a single timestamped yield-spread reading for one bond.
// The spread is the bond's YTM minus the risk-free benchmark yield (e.g. the
// equivalent-maturity Treasury yield).
type YieldObservation struct {
	Time   time.Time
	BondID string
	// YTM is the bond's current yield-to-maturity expressed as a decimal
	// (e.g. 0.055 for 5.5 %).
	YTM decimal.Decimal
	// BenchmarkYield is the risk-free benchmark yield for the same tenor.
	BenchmarkYield decimal.Decimal
	// Price is the bond's clean price at observation time, used for position
	// sizing (converting dollar budgets into bond quantities).
	Price decimal.Decimal
}

// Spread returns YTM − BenchmarkYield.  A positive spread means the bond
// yields more than the benchmark (it is "cheap"); a negative spread means it
// yields less (it is "rich").
func (o YieldObservation) Spread() (decimal.Decimal, error) {
	return o.YTM.Sub(o.BenchmarkYield)
}

// Decision is the per-tick evaluation output produced by a strategy. Concrete
// implementations live in each strategy package (e.g. meanreversion.Decision,
// breakout.Decision) and satisfy this interface via seven accessor methods.
//
// The framework only depends on these accessors. Strategy-specific fields
// (e.g. meanreversion.Decision.ZScore) live on the concrete struct and are
// read directly by the strategy's own run loop and backtest — they are not
// part of the shared contract.
//
// Go disallows a field and a method with the same name on the same type, so
// any field on a concrete Decision that an interface method needs to expose
// (Time, BondID, Price, Signal, PositionSize, Reason) must be unexported on
// the implementing struct; the method becomes the public accessor.
type Decision interface {
	Time() time.Time
	BondID() string
	Price() decimal.Decimal
	Signal() Signal
	PositionSize() decimal.Decimal
	// StrategyType is the strategy name written to the persisted Decision
	// row (e.g. "mean_reversion", "breakout"). Implementations return a
	// constant — not a per-call field.
	StrategyType() string
	// Reason is a machine-readable code (e.g. "z_score_entry", "warming_up",
	// "compression_breakout") that consumers (HTTP handler, persistence) use
	// to interpret the decision without depending on a specific strategy.
	Reason() string
}
