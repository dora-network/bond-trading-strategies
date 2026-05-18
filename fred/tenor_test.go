package fred_test

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/testutils"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/fred"
)

// ---- SeriesForTenor ----

func TestSeriesForTenor_KnownTenors(t *testing.T) {
	cases := []struct {
		tenor    fred.Tenor
		expected fred.SeriesID
	}{
		{fred.Tenor1Month, fred.Series1Month},
		{fred.Tenor3Month, fred.Series3Month},
		{fred.Tenor6Month, fred.Series6Month},
		{fred.Tenor1Year, fred.Series1Year},
		{fred.Tenor2Year, fred.Series2Year},
		{fred.Tenor3Year, fred.Series3Year},
		{fred.Tenor5Year, fred.Series5Year},
		{fred.Tenor7Year, fred.Series7Year},
		{fred.Tenor10Year, fred.Series10Year},
		{fred.Tenor20Year, fred.Series20Year},
		{fred.Tenor30Year, fred.Series30Year},
	}
	for _, tc := range cases {
		s, err := fred.SeriesForTenor(tc.tenor)
		require.NoError(t, err)
		assert.Equal(t, tc.expected, s)
	}
}

func TestSeriesForTenor_UnknownTenor(t *testing.T) {
	_, err := fred.SeriesForTenor(fred.Tenor(4.0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no exact series")
}

// ---- TenorFromMaturity ----

func TestTenorFromMaturity_OneYear(t *testing.T) {
	ref := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mat := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tenor := fred.TenorFromMaturity(ref, mat)
	// 366 days in 2024 (leap year), so 366/365.25 ≈ 1.00205
	assert.InDelta(t, 366.0/365.25, float64(tenor), 0.001)
}

func TestTenorFromMaturity_ThreeMonths(t *testing.T) {
	ref := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mat := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)
	tenor := fred.TenorFromMaturity(ref, mat)
	// 91 days / 365.25 ≈ 0.249
	assert.InDelta(t, 91.0/365.25, float64(tenor), 0.001)
}

func TestTenorFromMaturity_SameDay(t *testing.T) {
	ref := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	tenor := fred.TenorFromMaturity(ref, ref)
	assert.InDelta(t, 0.0, float64(tenor), 1e-10)
}

// ---- YieldCurve.InterpolateYield ----

func TestInterpolateYield_ExactMatch(t *testing.T) {
	yc := fred.YieldCurve{
		Points: []fred.CurvePoint{
			{Tenor: fred.Tenor2Year, Yield: decimal.MustNew(4, 2)},
			{Tenor: fred.Tenor5Year, Yield: decimal.MustNew(45, 2)},
			{Tenor: fred.Tenor10Year, Yield: decimal.MustNew(5, 2)},
		},
	}
	yield, err := yc.InterpolateYield(fred.Tenor5Year)
	assert.NoError(t, err)
	want := decimal.MustNew(45, 2)
	assert.True(t, yield.Equal(want))
}

func TestInterpolateYield_Midpoint(t *testing.T) {
	yc := fred.YieldCurve{
		Points: []fred.CurvePoint{
			{Tenor: fred.Tenor2Year, Yield: decimal.MustNew(4, 2)},
			{Tenor: fred.Tenor10Year, Yield: decimal.MustNew(5, 2)},
		},
	}
	// tenor = 6yr, halfway between 2yr and 10yr → 0.04 + 0.5*(0.05-0.04) = 0.045
	result, err := yc.InterpolateYield(fred.Tenor(6.0))
	assert.NoError(t, err)
	want := decimal.MustNew(45, 3)
	assert.True(t, result.Equal(want))
}

func TestInterpolateYield_LinearBetweenPoints(t *testing.T) {
	yc := fred.YieldCurve{
		Points: []fred.CurvePoint{
			{Tenor: fred.Tenor(0.0), Yield: decimal.MustNew(3, 2)},
			{Tenor: fred.Tenor(10.0), Yield: decimal.MustNew(5, 2)},
		},
	}
	// At tenor=2.5 (25% of the way from 0 to 10): 0.03 + 0.25*0.02 = 0.035
	result, err := yc.InterpolateYield(fred.Tenor(2.5))
	assert.NoError(t, err)
	want := decimal.MustNew(35, 3)
	assert.True(t, result.Equal(want))
}

func TestInterpolateYield_BelowShortestTenor(t *testing.T) {
	yc := fred.YieldCurve{
		Points: []fred.CurvePoint{
			{Tenor: fred.Tenor1Year, Yield: decimal.MustNew(4, 2)},
			{Tenor: fred.Tenor10Year, Yield: decimal.MustNew(5, 2)},
		},
	}
	// Flat extrapolation at the short end.
	result, err := yc.InterpolateYield(fred.Tenor(0.1))
	assert.NoError(t, err)
	want := decimal.MustNew(4, 2)
	assert.True(t, result.Equal(want))
}

func TestInterpolateYield_AboveLongestTenor(t *testing.T) {
	yc := fred.YieldCurve{
		Points: []fred.CurvePoint{
			{Tenor: fred.Tenor1Year, Yield: decimal.MustNew(4, 2)},
			{Tenor: fred.Tenor10Year, Yield: decimal.MustNew(5, 2)},
		},
	}
	// Flat extrapolation at the long end.
	result, err := yc.InterpolateYield(fred.Tenor(30.0))
	assert.NoError(t, err)
	want := decimal.MustNew(5, 2)
	assert.True(t, result.Equal(want))
}

func TestInterpolateYield_EmptyCurve(t *testing.T) {
	yc := fred.YieldCurve{}
	result, err := yc.InterpolateYield(fred.Tenor10Year)
	assert.NoError(t, err)
	assert.Equal(t, decimal.Zero, result)
}

func TestInterpolateYield_SinglePoint(t *testing.T) {
	yc := fred.YieldCurve{
		Points: []fred.CurvePoint{
			{Tenor: fred.Tenor10Year, Yield: decimal.MustNew(475, 4)},
		},
	}
	yield5Year, err := yc.InterpolateYield(fred.Tenor5Year)
	assert.NoError(t, err)
	want := decimal.MustNew(475, 4)
	assert.True(t, yield5Year.Equal(want))

	yield10Year, err := yc.InterpolateYield(fred.Tenor10Year)
	assert.NoError(t, err)
	assert.True(t, yield10Year.Equal(want))

	yield30Year, err := yc.InterpolateYield(fred.Tenor30Year)
	assert.NoError(t, err)
	assert.True(t, yield30Year.Equal(want))
}

// ---- YieldCurve.BenchmarkYield ----

func TestBenchmarkYield_UsesMaturityDate(t *testing.T) {
	yc := fred.YieldCurve{
		Points: []fred.CurvePoint{
			{Tenor: fred.Tenor2Year, Yield: decimal.MustNew(4, 2)},
			{Tenor: fred.Tenor10Year, Yield: decimal.MustNew(5, 2)},
		},
	}
	ref := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	// Maturity ~2 years out.
	mat := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	result, err := yc.BenchmarkYield(ref, mat)
	assert.NoError(t, err)
	// Should be very close to the 2-year yield since the tenor ≈ 2.
	want := decimal.MustNew(4, 2)
	assert.True(t, testutils.InDelta(t, result, want, decimal.MustNew(1, 4)))
}

// ---- FetchYieldCurve ----

func TestFetchYieldCurve_BuildsCurve(t *testing.T) {
	// Serve a single observation body for every tenor request.
	// We vary the yield by series_id so we can verify each tenor was fetched.
	seriesYields := map[string]string{
		"DGS1MO": "4.00",
		"DGS3MO": "4.10",
		"DGS6MO": "4.20",
		"DGS1":   "4.30",
		"DGS2":   "4.40",
		"DGS3":   "4.50",
		"DGS5":   "4.60",
		"DGS7":   "4.70",
		"DGS10":  "4.80",
		"DGS20":  "4.90",
		"DGS30":  "5.00",
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seriesID := r.URL.Query().Get("series_id")
		yieldStr, ok := seriesYields[seriesID]
		if !ok {
			http.Error(w, "unknown series", http.StatusBadRequest)
			return
		}
		body := fredResponse([]map[string]string{
			{"date": "2024-01-05", "value": yieldStr},
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	client := fred.NewClient("key", fred.WithBaseURL(srv.URL), fred.WithHTTPClient(srv.Client()))

	date := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)
	curve, err := client.FetchYieldCurve(context.Background(), date)
	require.NoError(t, err)
	assert.Equal(t, date, curve.Date)
	assert.Len(t, curve.Points, 11, "all 11 standard tenors should be present")

	// Curve should be sorted ascending by tenor.
	for i := 1; i < len(curve.Points); i++ {
		assert.True(t, curve.Points[i].Tenor > curve.Points[i-1].Tenor,
			"points should be sorted ascending by tenor")
	}
}

func TestFetchYieldCurve_SkipsMissingTenors(t *testing.T) {
	// Only return data for two tenors; the rest respond with all missing values.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seriesID := r.URL.Query().Get("series_id")
		var body []byte
		switch seriesID {
		case "DGS2":
			body = fredResponse([]map[string]string{{"date": "2024-01-05", "value": "4.40"}})
		case "DGS10":
			body = fredResponse([]map[string]string{{"date": "2024-01-05", "value": "4.80"}})
		default:
			body = fredResponse(nil) // no observations
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	client := fred.NewClient("key", fred.WithBaseURL(srv.URL), fred.WithHTTPClient(srv.Client()))

	curve, err := client.FetchYieldCurve(context.Background(), time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	assert.Len(t, curve.Points, 2)
}

// ---- FetchHistoricalYields ----

func TestFetchHistoricalYields_KnownTenor(t *testing.T) {
	body := fredResponse([]map[string]string{
		{"date": "2024-01-02", "value": "4.50"},
		{"date": "2024-01-03", "value": "4.52"},
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DGS10", r.URL.Query().Get("series_id"))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	client := fred.NewClient("key", fred.WithBaseURL(srv.URL), fred.WithHTTPClient(srv.Client()))

	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)

	obs, err := client.FetchHistoricalYields(context.Background(), fred.Tenor10Year, start, end)
	require.NoError(t, err)
	require.Len(t, obs, 2)
	want := decimal.MustNew(45, 3)
	assert.True(t, obs[0].Yield.Equal(want))
	want = decimal.MustNew(452, 4)
	assert.True(t, obs[1].Yield.Equal(want))
}

func TestFetchHistoricalYields_UnknownTenor(t *testing.T) {
	// Non-standard tenor should return an error without making any HTTP call.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("HTTP request should not have been made for unknown tenor")
	}))
	t.Cleanup(srv.Close)

	client := fred.NewClient("key", fred.WithBaseURL(srv.URL), fred.WithHTTPClient(srv.Client()))

	_, err := client.FetchHistoricalYields(
		context.Background(),
		fred.Tenor(4.0), // not a standard tenor
		time.Time{},
		time.Time{},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no exact series")
}

// ---- Numeric precision ----

func TestYieldPrecision_NoNaN(t *testing.T) {
	// Guard against any NaN or Inf creeping in via arithmetic.
	body := fredResponse([]map[string]string{
		{"date": "2024-01-02", "value": "0.01"},
	})
	client := newTestClient(t, http.StatusOK, body)

	obs, err := client.FetchSeries(context.Background(), fred.Series1Month, time.Time{}, time.Time{})
	require.NoError(t, err)
	require.Len(t, obs, 1)
	f, ok := obs[0].Yield.Float64()
	assert.True(t, ok)
	assert.False(t, math.IsNaN(f))
	assert.False(t, math.IsInf(f, 0))
}
