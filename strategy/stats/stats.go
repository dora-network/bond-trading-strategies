// Package stats provides shared statistical functions used by the strategy
// backtest pipelines (copytrading, meanreversion, etc.).
package stats

import "github.com/govalues/decimal"

// AnnualTradingDays is the number of trading days per year used to
// annualise the Sharpe ratio. DORA order books can trade 24/7, so this
// is 365.25 days (one calendar year) rather than the 252 trading-day
// convention used for equity markets.
const AnnualTradingDays = 365.25

// Sharpe returns the annualised Sharpe ratio of pnls, computed as
// mean(pnls) / sample-stddev(pnls) * sqrt(AnnualTradingDays). It assumes
// pnls are equally-spaced daily observations.
//
// Returns 0 with a nil error if pnls has fewer than two observations.
// Returns the underlying decimal error if any arithmetic operation fails.
func Sharpe(pnls []decimal.Decimal) (decimal.Decimal, error) {
	if len(pnls) < 2 { //nolint:mnd
		return decimal.Zero, nil
	}

	sum := decimal.Zero
	for _, p := range pnls {
		var err error
		sum, err = sum.Add(p)
		if err != nil {
			return decimal.Zero, err
		}
	}
	n := decimal.MustNew(int64(len(pnls)), 0)
	mean, err := sum.Quo(n)
	if err != nil {
		return decimal.Zero, err
	}

	variance := decimal.Zero
	for _, p := range pnls {
		d, err := p.Sub(mean)
		if err != nil {
			return decimal.Zero, err
		}
		sq, err := d.Mul(d)
		if err != nil {
			return decimal.Zero, err
		}
		variance, err = variance.Add(sq)
		if err != nil {
			return decimal.Zero, err
		}
	}
	nMinus1 := decimal.MustNew(int64(len(pnls)-1), 0)
	variance, err = variance.Quo(nMinus1)
	if err != nil {
		return decimal.Zero, err
	}

	sd, err := variance.Sqrt()
	if err != nil {
		return decimal.Zero, err
	}
	if sd.IsZero() {
		return decimal.Zero, nil
	}

	tradingDays, err := decimal.NewFromFloat64(AnnualTradingDays)
	if err != nil {
		return decimal.Zero, err
	}
	sqrtDays, err := tradingDays.Sqrt()
	if err != nil {
		return decimal.Zero, err
	}
	ratio, err := mean.Quo(sd)
	if err != nil {
		return decimal.Zero, err
	}
	return ratio.Mul(sqrtDays)
}
