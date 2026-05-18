package mcpserver

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/dora-network/bond-trading-strategies/fred"
)

const dateLayout = "2006-01-02"

// validSeriesIDs lists all supported FRED series identifiers.
//
//nolint:gochecknoglobals // Package-level constant list of known FRED series IDs
var validSeriesIDs = []string{
	"DGS1MO", "DGS3MO", "DGS6MO",
	"DGS1", "DGS2", "DGS3", "DGS5", "DGS7",
	"DGS10", "DGS20", "DGS30",
}

// standardTenors lists the standard tenor values (years) accepted by tenor-based tools.

// fredHandler holds server-level state (the API key and optional base URL
// override) and is the receiver for all FRED tool handler methods.
type fredHandler struct {
	apiKey  string
	baseURL string // empty → use fred package default (real FRED API)
}

// fredClient builds a FRED client; returns an error result if apiKey is empty.
func (h *fredHandler) client() (*fred.Client, *mcp.CallToolResult) {
	if h.apiKey == "" {
		return nil, mcp.NewToolResultError(
			"FRED API key is not configured. Start the server with --fred-api-key or set FRED_API_KEY.",
		)
	}
	opts := []fred.ClientOption{}
	if h.baseURL != "" {
		opts = append(opts, fred.WithBaseURL(h.baseURL))
	}
	return fred.NewClient(h.apiKey, opts...), nil
}

// ---- arg structs ----

type fetchSeriesArgs struct {
	SeriesID  string `json:"series_id"`
	StartDate string `json:"start_date"` // YYYY-MM-DD, optional
	EndDate   string `json:"end_date"`   // YYYY-MM-DD, optional
}

type fetchLatestArgs struct {
	SeriesID string `json:"series_id"`
}

type fetchYieldCurveArgs struct {
	Date string `json:"date"` // YYYY-MM-DD
}

type fetchHistoricalYieldsArgs struct {
	Tenor     float64 `json:"tenor"`      // years, must be a standard tenor
	StartDate string  `json:"start_date"` // YYYY-MM-DD
	EndDate   string  `json:"end_date"`   // YYYY-MM-DD, optional
}

type interpolateYieldArgs struct {
	Curve fred.YieldCurve `json:"curve"`
	Tenor float64         `json:"tenor"` // years
}

type benchmarkYieldArgs struct {
	Curve   fred.YieldCurve `json:"curve"`
	RefDate string          `json:"ref_date"` // YYYY-MM-DD
	//nolint:tagliatelle
	MatDate string `json:"maturity_date"` // YYYY-MM-DD
}

// ---- date helpers ----

func parseOptionalDate(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(dateLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid date %q: expected YYYY-MM-DD", s)
	}
	return t, nil
}

func parseRequiredDate(s, field string) (time.Time, *mcp.CallToolResult) {
	if s == "" {
		return time.Time{}, mcp.NewToolResultError(fmt.Sprintf("%s is required (YYYY-MM-DD)", field))
	}
	t, err := time.Parse(dateLayout, s)
	if err != nil {
		return time.Time{}, mcp.NewToolResultError(fmt.Sprintf("invalid %s %q: expected YYYY-MM-DD", field, s))
	}
	return t, nil
}

// ---- registration ----

//nolint:funlen // registration function with many tool definitions
func registerFREDTools(s *server.MCPServer, apiKey, baseURL string) {
	h := &fredHandler{apiKey: apiKey, baseURL: baseURL}

	// fred_fetch_series
	s.AddTool(
		mcp.NewTool("fred_fetch_series",
			mcp.WithDescription(
				"Fetch daily US Treasury CMT yield observations from FRED for a given series "+
					"and optional date range. Yields are returned as decimal fractions (e.g. 0.0425 "+
					"for 4.25%). Missing-value dates (weekends, holidays) are automatically filtered out.",
			),
			mcp.WithString("series_id",
				mcp.Required(),
				mcp.Description("FRED series ID."),
				mcp.Enum(validSeriesIDs...),
			),
			mcp.WithString("start_date",
				mcp.Description("Start of date range inclusive (YYYY-MM-DD). Omit for earliest available."),
			),
			mcp.WithString("end_date",
				mcp.Description("End of date range inclusive (YYYY-MM-DD). Omit for latest available."),
			),
		),
		mcp.NewTypedToolHandler(h.handleFetchSeries),
	)

	// fred_fetch_latest
	s.AddTool(
		mcp.NewTool("fred_fetch_latest",
			mcp.WithDescription(
				"Fetch the single most-recent valid yield observation for a FRED series. "+
					"Skips trailing missing-value dates (weekends, holidays).",
			),
			mcp.WithString("series_id",
				mcp.Required(),
				mcp.Description("FRED series ID."),
				mcp.Enum(validSeriesIDs...),
			),
		),
		mcp.NewTypedToolHandler(h.handleFetchLatest),
	)

	// fred_fetch_yield_curve
	s.AddTool(
		mcp.NewTool("fred_fetch_yield_curve",
			mcp.WithDescription(
				"Fetch the full US Treasury yield curve (all 11 standard tenors) for a given date. "+
					"Uses the most-recent trading-day value on or before the requested date for each tenor. "+
					"Returns a YieldCurve object with points sorted ascending by tenor (years). "+
					"Makes 11 HTTP requests to FRED — cache the result if calling repeatedly.",
			),
			mcp.WithString("date",
				mcp.Required(),
				mcp.Description("The reference date for the yield curve snapshot (YYYY-MM-DD)."),
			),
		),
		mcp.NewTypedToolHandler(h.handleFetchYieldCurve),
	)

	// fred_fetch_historical_yields
	s.AddTool(
		mcp.NewTool("fred_fetch_historical_yields",
			mcp.WithDescription(
				"Fetch a historical time-series of yields for a single standard tenor. "+
					"The result is suitable for feeding directly into strategy_backtest as the "+
					"benchmark_yield for each observation. tenor must be one of the standard values: "+
					"0.0833 (1m), 0.25 (3m), 0.5 (6m), 1, 2, 3, 5, 7, 10, 20, 30.",
			),
			mcp.WithNumber("tenor",
				mcp.Required(),
				mcp.Description("Tenor in fractional years. Must be a standard tenor (see description)."),
			),
			mcp.WithString("start_date",
				mcp.Required(),
				mcp.Description("Start of date range inclusive (YYYY-MM-DD)."),
			),
			mcp.WithString("end_date",
				mcp.Description("End of date range inclusive (YYYY-MM-DD). Omit for latest available."),
			),
		),
		mcp.NewTypedToolHandler(h.handleFetchHistoricalYields),
	)

	// fred_interpolate_yield  — pure computation, no network
	s.AddTool(
		mcp.NewTool("fred_interpolate_yield",
			mcp.WithDescription(
				"Linearly interpolate a yield from a YieldCurve object (as returned by "+
					"fred_fetch_yield_curve) for an arbitrary tenor in years. "+
					"Applies flat extrapolation beyond the shortest/longest known tenor. "+
					"No network call — pass in the curve object obtained from fred_fetch_yield_curve.",
			),
			mcp.WithObject("curve",
				mcp.Required(),
				mcp.Description("YieldCurve object as returned by fred_fetch_yield_curve."),
			),
			mcp.WithNumber("tenor",
				mcp.Required(),
				mcp.Description("Tenor to interpolate, in fractional years (e.g. 4.5 for 4.5-year)."),
				mcp.Min(0),
			),
		),
		mcp.NewTypedToolHandler(handleInterpolateYield),
	)

	// fred_benchmark_yield — pure computation, no network
	s.AddTool(
		mcp.NewTool("fred_benchmark_yield",
			mcp.WithDescription(
				"Given a YieldCurve, a reference date, and a bond's maturity date, "+
					"return the interpolated benchmark yield for that bond's remaining tenor. "+
					"No network call — pass in the curve obtained from fred_fetch_yield_curve. "+
					"This value can be used directly as benchmark_yield in strategy_update or strategy_backtest.",
			),
			mcp.WithObject("curve",
				mcp.Required(),
				mcp.Description("YieldCurve object as returned by fred_fetch_yield_curve."),
			),
			mcp.WithString("ref_date",
				mcp.Required(),
				mcp.Description("Reference (settlement) date (YYYY-MM-DD)."),
			),
			mcp.WithString("maturity_date",
				mcp.Required(),
				mcp.Description("Bond maturity date (YYYY-MM-DD)."),
			),
		),
		mcp.NewTypedToolHandler(handleBenchmarkYield),
	)
}

// ---- handlers ----

func (h *fredHandler) handleFetchSeries(ctx context.Context, _ mcp.CallToolRequest, args fetchSeriesArgs) (*mcp.CallToolResult, error) {
	c, errResult := h.client()
	if errResult != nil {
		return errResult, nil
	}

	start, err := parseOptionalDate(args.StartDate)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	end, err := parseOptionalDate(args.EndDate)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	obs, err := c.FetchSeries(ctx, fred.SeriesID(args.SeriesID), start, end)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("FRED error: %v", err)), nil
	}
	return jsonText(obs)
}

func (h *fredHandler) handleFetchLatest(ctx context.Context, _ mcp.CallToolRequest, args fetchLatestArgs) (*mcp.CallToolResult, error) {
	c, errResult := h.client()
	if errResult != nil {
		return errResult, nil
	}

	obs, err := c.FetchLatest(ctx, fred.SeriesID(args.SeriesID))
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("FRED error: %v", err)), nil
	}
	return jsonText(obs)
}

func (h *fredHandler) handleFetchYieldCurve(ctx context.Context, _ mcp.CallToolRequest, args fetchYieldCurveArgs) (*mcp.CallToolResult, error) { //nolint:lll
	c, errResult := h.client()
	if errResult != nil {
		return errResult, nil
	}

	date, errResult := parseRequiredDate(args.Date, "date")
	if errResult != nil {
		return errResult, nil
	}

	curve, err := c.FetchYieldCurve(ctx, date)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("FRED error: %v", err)), nil
	}
	return jsonText(curve)
}

func (h *fredHandler) handleFetchHistoricalYields(ctx context.Context, _ mcp.CallToolRequest, args fetchHistoricalYieldsArgs) (*mcp.CallToolResult, error) { //nolint:lll
	c, errResult := h.client()
	if errResult != nil {
		return errResult, nil
	}

	start, errResult := parseRequiredDate(args.StartDate, "start_date")
	if errResult != nil {
		return errResult, nil
	}
	end, err := parseOptionalDate(args.EndDate)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	obs, err := c.FetchHistoricalYields(ctx, fred.Tenor(args.Tenor), start, end)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("FRED error: %v", err)), nil
	}
	return jsonText(obs)
}

func handleInterpolateYield(_ context.Context, _ mcp.CallToolRequest, args interpolateYieldArgs) (*mcp.CallToolResult, error) {
	yield, err := args.Curve.InterpolateYield(fred.Tenor(args.Tenor))
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("FRED error: %v", err)), nil
	}
	return jsonText(map[string]any{
		"tenor": args.Tenor,
		"yield": yield,
	})
}

func handleBenchmarkYield(_ context.Context, _ mcp.CallToolRequest, args benchmarkYieldArgs) (*mcp.CallToolResult, error) {
	ref, errResult := parseRequiredDate(args.RefDate, "ref_date")
	if errResult != nil {
		return errResult, nil
	}
	mat, errResult := parseRequiredDate(args.MatDate, "maturity_date")
	if errResult != nil {
		return errResult, nil
	}

	tenor := fred.TenorFromMaturity(ref, mat)
	yield, err := args.Curve.BenchmarkYield(ref, mat)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("FRED error: %v", err)), nil
	}
	return jsonText(map[string]any{
		"tenor_years":     float64(tenor),
		"benchmark_yield": yield,
	})
}
