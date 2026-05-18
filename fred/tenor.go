package fred

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/govalues/decimal"
)

// Tenor represents a standard Treasury maturity expressed as a number of years.
// Sub-year tenors use fractional years: 1 month ≈ 1/12, 3 months ≈ 0.25, etc.
type Tenor float64

// Standard tenors matching the available FRED CMT series.
const (
	Tenor1Month Tenor = 1.0 / 12
	Tenor3Month Tenor = 3.0 / 12
	Tenor6Month Tenor = 6.0 / 12
	Tenor1Year  Tenor = 1.0
	Tenor2Year  Tenor = 2.0
	Tenor3Year  Tenor = 3.0
	Tenor5Year  Tenor = 5.0
	Tenor7Year  Tenor = 7.0
	Tenor10Year Tenor = 10.0
	Tenor20Year Tenor = 20.0
	Tenor30Year Tenor = 30.0
)

// tenorToSeries maps each standard tenor to its FRED series ID.
//
//nolint:gochecknoglobals // Package-level lookup table, immutable after init
var tenorToSeries = map[Tenor]SeriesID{
	Tenor1Month: Series1Month,
	Tenor3Month: Series3Month,
	Tenor6Month: Series6Month,
	Tenor1Year:  Series1Year,
	Tenor2Year:  Series2Year,
	Tenor3Year:  Series3Year,
	Tenor5Year:  Series5Year,
	Tenor7Year:  Series7Year,
	Tenor10Year: Series10Year,
	Tenor20Year: Series20Year,
	Tenor30Year: Series30Year,
}

// SeriesForTenor returns the FRED series ID for an exact standard tenor.
// Returns an error if the tenor does not map to a known series.
func SeriesForTenor(t Tenor) (SeriesID, error) {
	if s, ok := tenorToSeries[t]; ok {
		return s, nil
	}
	return "", fmt.Errorf("fred: no exact series for tenor %.4f years; use InterpolateYield instead", float64(t))
}

// TenorFromMaturity returns the Tenor (in years) for a bond whose maturity
// date is mat, evaluated as of the reference date ref.  The value is a
// continuous floating-point number of years, not snapped to standard tenors.
func TenorFromMaturity(ref, mat time.Time) Tenor {
	days := mat.Sub(ref).Hours() / 24.0
	return Tenor(days / 365.25) //nolint:mnd
}

// YieldCurve is a snapshot of the Treasury yield curve on a single date,
// holding yields for a set of standard tenors.
type YieldCurve struct {
	Date   time.Time
	Points []CurvePoint // sorted ascending by Tenor
}

// CurvePoint is one tenor/yield pair on the curve.
type CurvePoint struct {
	Tenor Tenor
	Yield decimal.Decimal // decimal, e.g. 0.0425
}

// InterpolateYield returns the linearly interpolated yield for an arbitrary
// tenor (in years) from the curve.
//
// If the requested tenor is below the shortest available tenor, the shortest
// yield is returned (flat extrapolation).  Similarly, at the long end.
// This is consistent with the standard "flat extrapolation, linear interpolation"
// used in many fixed-income systems.
func (yc YieldCurve) InterpolateYield(tenor Tenor) (decimal.Decimal, error) {
	if len(yc.Points) == 0 {
		return decimal.Zero, nil
	}
	if tenor <= yc.Points[0].Tenor {
		return yc.Points[0].Yield, nil
	}
	last := yc.Points[len(yc.Points)-1]
	if tenor >= last.Tenor {
		return last.Yield, nil
	}

	// Binary search for the bracketing interval.
	i := sort.Search(len(yc.Points), func(i int) bool {
		return yc.Points[i].Tenor >= tenor
	})

	lo := yc.Points[i-1]
	hi := yc.Points[i]

	// Linear interpolation.
	t, err := decimal.NewFromFloat64(float64(tenor-lo.Tenor) / float64(hi.Tenor-lo.Tenor))
	if err != nil {
		return decimal.Zero, err
	}
	spread, err := hi.Yield.Sub(lo.Yield)
	if err != nil {
		return decimal.Zero, err
	}
	interpolated, err := t.Mul(spread)
	if err != nil {
		return decimal.Zero, err
	}

	interpolated, err = lo.Yield.Add(interpolated)
	if err != nil {
		return decimal.Zero, err
	}

	return interpolated, nil
}

// BenchmarkYield is a convenience wrapper: given a bond's maturity date and a
// reference date, it linearly interpolates the appropriate Treasury benchmark
// yield from the curve.
func (yc YieldCurve) BenchmarkYield(ref, maturity time.Time) (decimal.Decimal, error) {
	return yc.InterpolateYield(TenorFromMaturity(ref, maturity))
}

// FetchYieldCurve retrieves the full Treasury yield curve for the given date
// by fetching each standard tenor series from FRED and returning the last
// available observation on or before that date.
//
// This makes one HTTP request per tenor (11 requests for the full curve).
// For production use, consider caching results or using FetchSeries over a
// date range instead.
func (c *Client) FetchYieldCurve(ctx context.Context, date time.Time) (YieldCurve, error) {
	curve := YieldCurve{Date: date}

	for tenor, series := range tenorToSeries {
		// Request a small trailing window ending on the target date so that we
		// get the most recent trading-day value on or before the requested date.
		start := date.AddDate(0, 0, -10)
		obs, err := c.FetchSeries(ctx, series, start, date)
		if err != nil {
			return YieldCurve{}, fmt.Errorf("fred: fetch %s: %w", series, err)
		}
		if len(obs) == 0 {
			// No data for this tenor on this date window — skip rather than fail.
			continue
		}
		// The last element is the most recent observation ≤ date.
		curve.Points = append(curve.Points, CurvePoint{
			Tenor: tenor,
			Yield: obs[len(obs)-1].Yield,
		})
	}

	// Sort by tenor so InterpolateYield's binary search works correctly.
	sort.Slice(curve.Points, func(i, j int) bool {
		return curve.Points[i].Tenor < curve.Points[j].Tenor
	})

	return curve, nil
}

// FetchHistoricalYields retrieves a time series of yields for a single tenor,
// useful for building the historical spread series fed into the mean-reversion
// strategy backtester.
func (c *Client) FetchHistoricalYields(
	ctx context.Context,
	tenor Tenor,
	start, end time.Time,
) ([]Observation, error) {
	series, err := SeriesForTenor(tenor)
	if err != nil {
		return nil, err
	}
	return c.FetchSeries(ctx, series, start, end)
}
