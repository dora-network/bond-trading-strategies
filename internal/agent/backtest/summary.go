package backtest

import "math"

// defaultInitialEquity is the fallback starting capital when Compute is given
// a non-positive initialEquity. v1 callers pass it explicitly; the default
// keeps the formulas self-consistent for ad-hoc tests.
const defaultInitialEquity = 1000.0

// tradingDaysPerYear is the annualization scale for Sharpe. We treat the
// input fill series as a daily-equivalent return stream regardless of the
// underlying resolution (ponytail: simplifies the v1 formula; revisit if a
// non-daily backtest needs a different scale).
const tradingDaysPerYear = 252

// Compute turns a chronological slice of fills into the v1 summary stats.
// Fills are applied in slice order: a buy subtracts price*quantity from the
// running equity, a sell adds it. Returns are snapshotted at every fill
// boundary (so the first fill contributes to the Sharpe series); Sharpe is
// annualized with sqrt(tradingDaysPerYear).
func Compute(fills []Fill, initialEquity float64) *Summary {
	if initialEquity <= 0 {
		initialEquity = defaultInitialEquity
	}
	equity := initialEquity
	peak := equity
	maxDD := 0.0
	wins := 0
	buys := 0
	returns := make([]float64, 0, len(fills))
	var prev Fill // zero value's Side == "" guards the i==0 look-back below.

	for _, f := range fills {
		switch f.Side {
		case "buy":
			equity -= f.Price * f.Quantity
			buys++
		case "sell":
			equity += f.Price * f.Quantity
			// Win = sell above the price of the most recent preceding buy.
			// Ponytail: this is a one-step look-back; the runner feeds fills
			// already grouped by trade in chronological order, so a deeper
			// average-cost walk would be premature.
			if prev.Side == "buy" && f.Price > prev.Price {
				wins++
			}
		}
		prev = f
		if equity > peak {
			peak = equity
		}
		if peak > 0 {
			if dd := (peak - equity) / peak; dd > maxDD {
				maxDD = dd
			}
		}
		returns = append(returns, (equity-initialEquity)/initialEquity)
	}

	var sharpe float64
	if len(returns) > 1 {
		mean, std := meanStd(returns)
		if std > 0 {
			sharpe = mean / std * math.Sqrt(tradingDaysPerYear)
		}
	}

	var winRate float64
	if buys > 0 {
		winRate = float64(wins) / float64(buys)
	}

	return &Summary{
		TotalReturn: (equity - initialEquity) / initialEquity,
		Sharpe:      sharpe,
		MaxDrawdown: maxDD,
		TradeCount:  len(fills),
		WinRate:     winRate,
		StartEquity: initialEquity,
		EndEquity:   equity,
	}
}

// meanStd returns the arithmetic mean and the population standard deviation
// in one pass. Returns (0, 0) on an empty input — the Sharpe guard in
// Compute treats that as "insufficient data" and skips the ratio.
func meanStd(v []float64) (mean, std float64) {
	if len(v) == 0 {
		return 0, 0
	}
	var sum float64
	for _, x := range v {
		sum += x
	}
	mean = sum / float64(len(v))
	var sqSum float64
	for _, x := range v {
		d := x - mean
		sqSum += d * d
	}
	std = math.Sqrt(sqSum / float64(len(v)))
	return mean, std
}
