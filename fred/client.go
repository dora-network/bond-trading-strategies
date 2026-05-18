// Package fred provides a client for the FRED (Federal Reserve Economic Data)
// API, focused on retrieving US Treasury constant-maturity yields for use as
// benchmark rates in bond trading strategies.
//
// FRED API reference: https://fred.stlouisfed.org/docs/api/fred/
//
// A free API key is required. Register at:
// https://fred.stlouisfed.org/docs/api/api_key.html
//
// # Treasury series IDs
//
// FRED publishes daily constant-maturity Treasury (CMT) yields under the
// following series IDs:
//
//	DGS1MO  — 1-month
//	DGS3MO  — 3-month
//	DGS6MO  — 6-month
//	DGS1    — 1-year
//	DGS2    — 2-year
//	DGS3    — 3-year
//	DGS5    — 5-year
//	DGS7    — 7-year
//	DGS10   — 10-year
//	DGS20   — 20-year
//	DGS30   — 30-year
//
// Yields are published as percentages (e.g. 4.25 = 4.25 %).  All values
// returned by this package are converted to decimals (e.g. 0.0425).
package fred

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/govalues/decimal"
)

const (
	defaultBaseURL = "https://api.stlouisfed.org/fred"
	dateLayout     = "2006-01-02"

	// MissingValue is the sentinel FRED uses when no observation exists for a
	// date (e.g. weekends, holidays).
	MissingValue = "."
)

// SeriesID is a FRED series identifier string.
type SeriesID string

// Standard US Treasury constant-maturity series IDs.
const (
	Series1Month SeriesID = "DGS1MO"
	Series3Month SeriesID = "DGS3MO"
	Series6Month SeriesID = "DGS6MO"
	Series1Year  SeriesID = "DGS1"
	Series2Year  SeriesID = "DGS2"
	Series3Year  SeriesID = "DGS3"
	Series5Year  SeriesID = "DGS5"
	Series7Year  SeriesID = "DGS7"
	Series10Year SeriesID = "DGS10"
	Series20Year SeriesID = "DGS20"
	Series30Year SeriesID = "DGS30"
)

// Observation is a single dated yield reading returned by FRED.
// Yield is expressed as a decimal fraction (e.g. 0.0425 for 4.25 %).
type Observation struct {
	Date  time.Time
	Yield decimal.Decimal // decimal, e.g. 0.0425
}

// observationsResponse mirrors the JSON envelope returned by
// GET /fred/series/observations.
type observationsResponse struct {
	Observations []struct {
		Date  string `json:"date"`
		Value string `json:"value"`
	} `json:"observations"`
}

// Client is a FRED API client.  Use NewClient to construct one.
type Client struct {
	apiKey  string
	baseURL string
	http    *http.Client
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithHTTPClient replaces the default HTTP client.  Useful for injecting a
// mock transport in tests.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) { c.http = hc }
}

// WithBaseURL overrides the FRED API base URL.  Useful for tests that spin up
// a local HTTP server.
func WithBaseURL(u string) ClientOption {
	return func(c *Client) { c.baseURL = u }
}

// NewClient creates a Client authenticated with the given FRED API key.
func NewClient(apiKey string, opts ...ClientOption) *Client {
	c := &Client{
		apiKey:  apiKey,
		baseURL: defaultBaseURL,
		http:    &http.Client{Timeout: 15 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// FetchSeries retrieves all available daily observations for a FRED series
// within the given date range (inclusive).  Pass a zero time.Time for start
// or end to use FRED's defaults (earliest / latest available).
//
// Observations for non-trading days (weekends, holidays) are omitted because
// FRED returns "." for their values.
func (c *Client) FetchSeries(
	ctx context.Context,
	series SeriesID,
	start, end time.Time,
) ([]Observation, error) {
	params := url.Values{}
	params.Set("series_id", string(series))
	params.Set("api_key", c.apiKey)
	params.Set("file_type", "json")
	params.Set("sort_order", "asc")
	if !start.IsZero() {
		params.Set("observation_start", start.Format(dateLayout))
	}
	if !end.IsZero() {
		params.Set("observation_end", end.Format(dateLayout))
	}

	reqURL := fmt.Sprintf("%s/series/observations?%s", c.baseURL, params.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("fred: build request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fred: http request: %w", err)
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Printf("fred: failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		var body []byte
		body, err = io.ReadAll(io.LimitReader(resp.Body, 512)) //nolint:mnd
		if err != nil {
			return nil, fmt.Errorf("fred: unexpected status %d: read response: %w", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("fred: unexpected status %d: %s", resp.StatusCode, body)
	}

	var envelope observationsResponse
	if err = json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("fred: decode response: %w", err)
	}

	return parseObservations(envelope)
}

// FetchLatest retrieves the single most-recent observation for the series.
// Returns an error if no valid observation is found.
func (c *Client) FetchLatest(ctx context.Context, series SeriesID) (Observation, error) {
	params := url.Values{}
	params.Set("series_id", string(series))
	params.Set("api_key", c.apiKey)
	params.Set("file_type", "json")
	params.Set("sort_order", "desc")
	params.Set("limit", "10") // fetch a small window in case the most recent days are "."

	reqURL := fmt.Sprintf("%s/series/observations?%s", c.baseURL, params.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return Observation{}, fmt.Errorf("fred: build request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return Observation{}, fmt.Errorf("fred: http request: %w", err)
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Printf("fred: failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		var body []byte
		body, err = io.ReadAll(io.LimitReader(resp.Body, 512)) //nolint:mnd
		if err != nil {
			return Observation{}, fmt.Errorf("fred: unexpected status %d: read response: %w", resp.StatusCode, err)
		}

		return Observation{}, fmt.Errorf("fred: unexpected status %d: %s", resp.StatusCode, body)
	}

	var envelope observationsResponse
	if err = json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return Observation{}, fmt.Errorf("fred: decode response: %w", err)
	}

	obs, err := parseObservations(envelope)
	if err != nil {
		return Observation{}, err
	}
	if len(obs) == 0 {
		return Observation{}, fmt.Errorf("fred: no valid observations returned for %s", series)
	}
	// parseObservations returns in ascending order; we requested desc, so the
	// first element after parsing is the most recent valid value.
	return obs[0], nil
}

// parseObservations converts raw FRED JSON observations into typed Observations,
// filtering out missing values (".").  Yields are converted from percent to
// decimal fractions.
func parseObservations(env observationsResponse) ([]Observation, error) {
	out := make([]Observation, 0, len(env.Observations))
	for _, raw := range env.Observations {
		if raw.Value == MissingValue || raw.Value == "" {
			continue
		}
		t, err := time.Parse(dateLayout, raw.Date)
		if err != nil {
			return nil, fmt.Errorf("fred: parse date %q: %w", raw.Date, err)
		}
		pct, err := decimal.Parse(raw.Value)
		if err != nil {
			return nil, fmt.Errorf("fred: parse value %q on %s: %w", raw.Value, raw.Date, err)
		}
		yield, err := pct.Quo(decimal.MustNew(100, 0)) //nolint:mnd
		if err != nil {
			return nil, fmt.Errorf("fred: convert percent to decimal: %w", err)
		}
		out = append(out, Observation{
			Date:  t,
			Yield: yield, // percent → decimal
		})
	}
	return out, nil
}
