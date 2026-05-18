package fred_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/fred"
)

// fredResponse is a helper that builds the FRED observations JSON envelope.
func fredResponse(observations []map[string]string) []byte {
	type obs struct {
		Date  string `json:"date"`
		Value string `json:"value"`
	}
	type envelope struct {
		Observations []obs `json:"observations"`
	}
	e := envelope{}
	for _, o := range observations {
		e.Observations = append(e.Observations, obs{Date: o["date"], Value: o["value"]})
	}
	b, _ := json.Marshal(e)
	return b
}

// newTestServer returns a test HTTP server that always responds with the given
// body and status code, plus the FRED Client configured to use it.
func newTestClient(t *testing.T, status int, body []byte) *fred.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	return fred.NewClient("test-api-key",
		fred.WithBaseURL(srv.URL),
		fred.WithHTTPClient(srv.Client()),
	)
}

// ---- FetchSeries ----

func TestFetchSeries_ReturnsObservations(t *testing.T) {
	body := fredResponse([]map[string]string{
		{"date": "2024-01-02", "value": "4.25"},
		{"date": "2024-01-03", "value": "4.30"},
		{"date": "2024-01-04", "value": "4.20"},
	})
	client := newTestClient(t, http.StatusOK, body)

	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC)

	obs, err := client.FetchSeries(context.Background(), fred.Series10Year, start, end)
	require.NoError(t, err)
	require.Len(t, obs, 3)

	assert.Equal(t, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), obs[0].Date)
	want := decimal.MustNew(425, 4)
	assert.True(t, obs[0].Yield.Equal(want))

	assert.Equal(t, time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC), obs[1].Date)
	want = decimal.MustNew(43, 3)
	assert.True(t, obs[1].Yield.Equal(want))

	assert.Equal(t, time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC), obs[2].Date)
	want = decimal.MustNew(42, 3)
	assert.True(t, obs[2].Yield.Equal(want))
}

func TestFetchSeries_FiltersMissingValues(t *testing.T) {
	body := fredResponse([]map[string]string{
		{"date": "2024-01-01", "value": "."}, // holiday — should be filtered
		{"date": "2024-01-02", "value": "4.50"},
		{"date": "2024-01-06", "value": "."}, // weekend — should be filtered
		{"date": "2024-01-08", "value": "4.55"},
	})
	client := newTestClient(t, http.StatusOK, body)

	obs, err := client.FetchSeries(context.Background(), fred.Series10Year, time.Time{}, time.Time{})
	require.NoError(t, err)
	require.Len(t, obs, 2, "missing-value observations should be filtered out")

	want := decimal.MustNew(45, 3)
	assert.True(t, obs[0].Yield.Equal(want))
	want = decimal.MustNew(455, 4)
	assert.True(t, obs[1].Yield.Equal(want))
}

func TestFetchSeries_EmptyObservations(t *testing.T) {
	body := fredResponse(nil)
	client := newTestClient(t, http.StatusOK, body)

	obs, err := client.FetchSeries(context.Background(), fred.Series10Year, time.Time{}, time.Time{})
	require.NoError(t, err)
	assert.Empty(t, obs)
}

func TestFetchSeries_AllMissingValues(t *testing.T) {
	body := fredResponse([]map[string]string{
		{"date": "2024-01-06", "value": "."},
		{"date": "2024-01-07", "value": "."},
	})
	client := newTestClient(t, http.StatusOK, body)

	obs, err := client.FetchSeries(context.Background(), fred.Series10Year, time.Time{}, time.Time{})
	require.NoError(t, err)
	assert.Empty(t, obs)
}

func TestFetchSeries_NonOKStatus(t *testing.T) {
	client := newTestClient(t, http.StatusBadRequest, []byte(`{"error_code":400,"error_message":"Bad Request"}`))

	_, err := client.FetchSeries(context.Background(), fred.Series10Year, time.Time{}, time.Time{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "400")
}

func TestFetchSeries_InvalidJSON(t *testing.T) {
	client := newTestClient(t, http.StatusOK, []byte(`not json`))

	_, err := client.FetchSeries(context.Background(), fred.Series10Year, time.Time{}, time.Time{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode response")
}

func TestFetchSeries_InvalidDate(t *testing.T) {
	// Malformed date that cannot be parsed.
	body := fredResponse([]map[string]string{
		{"date": "not-a-date", "value": "4.00"},
	})
	client := newTestClient(t, http.StatusOK, body)

	_, err := client.FetchSeries(context.Background(), fred.Series10Year, time.Time{}, time.Time{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse date")
}

func TestFetchSeries_InvalidYieldValue(t *testing.T) {
	body := fredResponse([]map[string]string{
		{"date": "2024-01-02", "value": "not-a-number"},
	})
	client := newTestClient(t, http.StatusOK, body)

	_, err := client.FetchSeries(context.Background(), fred.Series10Year, time.Time{}, time.Time{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse value")
}

func TestFetchSeries_YieldConversionPercent(t *testing.T) {
	// Verify that a 5.00 % value is stored as 0.05.
	body := fredResponse([]map[string]string{
		{"date": "2024-06-01", "value": "5.00"},
	})
	client := newTestClient(t, http.StatusOK, body)

	obs, err := client.FetchSeries(context.Background(), fred.Series5Year, time.Time{}, time.Time{})
	require.NoError(t, err)
	require.Len(t, obs, 1)
	want := decimal.MustNew(5, 2)
	assert.True(t, obs[0].Yield.Equal(want))
}

func TestFetchSeries_ZeroDateRange(t *testing.T) {
	// Passing zero times should not add query params; the server still responds.
	body := fredResponse([]map[string]string{
		{"date": "2024-01-02", "value": "3.80"},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Ensure observation_start / observation_end are absent.
		assert.Empty(t, r.URL.Query().Get("observation_start"), "start should not be sent for zero time")
		assert.Empty(t, r.URL.Query().Get("observation_end"), "end should not be sent for zero time")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	client := fred.NewClient("key", fred.WithBaseURL(srv.URL), fred.WithHTTPClient(srv.Client()))
	obs, err := client.FetchSeries(context.Background(), fred.Series2Year, time.Time{}, time.Time{})
	require.NoError(t, err)
	require.Len(t, obs, 1)
}

// ---- FetchLatest ----

func TestFetchLatest_ReturnsMostRecent(t *testing.T) {
	// FRED returns in descending order when sort_order=desc; the most recent
	// valid value should be the first non-"." entry.
	body := fredResponse([]map[string]string{
		{"date": "2024-03-15", "value": "4.60"},
		{"date": "2024-03-14", "value": "4.58"},
		{"date": "2024-03-13", "value": "4.55"},
	})
	client := newTestClient(t, http.StatusOK, body)

	obs, err := client.FetchLatest(context.Background(), fred.Series10Year)
	require.NoError(t, err)
	want := decimal.MustNew(46, 3)
	assert.Equal(t, time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC), obs.Date)
	assert.True(t, obs.Yield.Equal(want))
}

func TestFetchLatest_SkipsLeadingMissingValues(t *testing.T) {
	// The most recent days are weekends/holidays; the first valid value is
	// further back.
	body := fredResponse([]map[string]string{
		{"date": "2024-03-17", "value": "."},
		{"date": "2024-03-16", "value": "."},
		{"date": "2024-03-15", "value": "4.62"},
	})
	client := newTestClient(t, http.StatusOK, body)

	obs, err := client.FetchLatest(context.Background(), fred.Series10Year)
	require.NoError(t, err)

	want := decimal.MustNew(462, 4)
	assert.Equal(t, time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC), obs.Date)
	assert.True(t, obs.Yield.Equal(want))
}

func TestFetchLatest_NoValidObservations(t *testing.T) {
	body := fredResponse([]map[string]string{
		{"date": "2024-03-17", "value": "."},
		{"date": "2024-03-16", "value": "."},
	})
	client := newTestClient(t, http.StatusOK, body)

	_, err := client.FetchLatest(context.Background(), fred.Series10Year)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no valid observations")
}

func TestFetchLatest_NonOKStatus(t *testing.T) {
	client := newTestClient(t, http.StatusUnauthorized, []byte(`{"error_code":401,"error_message":"Bad API key"}`))

	_, err := client.FetchLatest(context.Background(), fred.Series10Year)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

func TestFetchLatest_RequestParams(t *testing.T) {
	body := fredResponse([]map[string]string{
		{"date": "2024-01-05", "value": "4.10"},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		assert.Equal(t, "desc", q.Get("sort_order"), "FetchLatest must request descending order")
		assert.Equal(t, "10", q.Get("limit"), "FetchLatest must request limit=10")
		assert.Equal(t, "DGS10", q.Get("series_id"))
		assert.Equal(t, "test-key", q.Get("api_key"))
		assert.Equal(t, "json", q.Get("file_type"))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	client := fred.NewClient("test-key", fred.WithBaseURL(srv.URL), fred.WithHTTPClient(srv.Client()))
	_, err := client.FetchLatest(context.Background(), fred.Series10Year)
	require.NoError(t, err)
}
