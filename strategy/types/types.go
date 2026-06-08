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

// Decision is the full output of one strategy evaluation step.
type Decision struct {
	Time   time.Time
	BondID string

	// YTM is the bond's current yield-to-maturity.
	YTM decimal.Decimal
	// BenchmarkYield is the risk-free benchmark yield for the same tenor.
	BenchmarkYield decimal.Decimal
	// Spread is YTM − BenchmarkYield.
	Spread decimal.Decimal
	// RollingMean is the rolling mean of the spread over the lookback window.
	RollingMean decimal.Decimal
	// RollingStdDev is the rolling standard deviation of the spread over the
	// lookback window.
	RollingStdDev decimal.Decimal
	// ZScore is (Spread − RollingMean) / RollingStdDev.
	ZScore decimal.Decimal

	// Price is the bond price at decision time, used for position sizing.
	Price decimal.Decimal

	// Signal is the recommended action.
	Signal Signal
	// PositionSize is a fraction of capital in [0, MaxPositionSize] suggested
	// for the trade. It is proportional to the absolute z-score above the
	// entry threshold, capped at MaxPositionSize.
	PositionSize decimal.Decimal
}
