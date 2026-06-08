package stats

import (
	"fmt"
	"time"

	"github.com/govalues/decimal"
)

// hoursPerDay is the truncation step for daily-PnL bucketing. DORA trades
// 24/7, so a "day" is exactly 24 hours starting at UTC midnight.
const hoursPerDay = 24

// PnLPoint is the minimum information Summarise needs from each closed
// trade: the realised PnL and the time it was closed. Strategy packages
// convert their own ClosedTrade types into []PnLPoint.
type PnLPoint struct {
	PnL       decimal.Decimal
	CloseTime time.Time
}

// Summary is the aggregated output of a backtest run. Both copytrading
// and meanreversion backtests embed these fields in their public Result
// types.
type Summary struct {
	TotalPnL    decimal.Decimal
	WinCount    int
	LossCount   int
	MaxDrawdown decimal.Decimal
	SharpeRatio decimal.Decimal
}

// Summarise aggregates trades into a Summary.
//
// Win/loss rule: a trade with PnL > 0 is a win, PnL < 0 is a loss, and
// PnL = 0 is neither (and is excluded from both counts).
//
// Sharpe window: the daily-PnL series spans the full [start, end] window
// inclusive of both endpoints (truncated to the day). Days with no closed
// trades contribute 0, so the ratio's denominator reflects the backtest's
// actual coverage. This is the backtest window, not the span of trades.
//
// Returns a zero Summary and nil error if trades is empty. Returns an
// error if start or end is zero, or if start > end.
func Summarise(trades []PnLPoint, start, end time.Time) (Summary, error) {
	if start.IsZero() || end.IsZero() {
		return Summary{}, fmt.Errorf("summarise: start and end must be non-zero")
	}
	if end.Before(start) {
		return Summary{}, fmt.Errorf("summarise: end %s is before start %s", end, start)
	}

	res := Summary{}
	if len(trades) == 0 {
		return res, nil
	}

	equity := decimal.Zero
	peak := decimal.Zero

	dailyPnLMap := make(map[string]decimal.Decimal)
	for _, t := range trades {
		var err error
		res.TotalPnL, err = res.TotalPnL.Add(t.PnL)
		if err != nil {
			return Summary{}, err
		}

		dateStr := t.CloseTime.Format("2006-01-02")
		current, ok := dailyPnLMap[dateStr]
		if !ok {
			current = decimal.Zero
		}
		dailyPnLMap[dateStr], err = current.Add(t.PnL)
		if err != nil {
			return Summary{}, err
		}

		switch {
		case t.PnL.IsPos():
			res.WinCount++
		case t.PnL.IsNeg():
			res.LossCount++
		}

		equity, err = equity.Add(t.PnL)
		if err != nil {
			return Summary{}, err
		}
		if equity.Cmp(peak) > 0 {
			peak = equity
		}
		dd, err := peak.Sub(equity)
		if err != nil {
			return Summary{}, err
		}
		if dd.Cmp(res.MaxDrawdown) > 0 {
			res.MaxDrawdown = dd
		}
	}

	startDay := start.Truncate(hoursPerDay * time.Hour)
	endDay := end.Truncate(hoursPerDay * time.Hour)
	dailyPnLs := make([]decimal.Decimal, 0, int(endDay.Sub(startDay)/(hoursPerDay*time.Hour))+1)
	for d := startDay; !d.After(endDay); d = d.Add(hoursPerDay * time.Hour) {
		dateStr := d.Format("2006-01-02")
		pnl, ok := dailyPnLMap[dateStr]
		if !ok {
			pnl = decimal.Zero
		}
		dailyPnLs = append(dailyPnLs, pnl)
	}

	ratio, err := Sharpe(dailyPnLs)
	if err != nil {
		return Summary{}, err
	}
	res.SharpeRatio = ratio
	return res, nil
}
