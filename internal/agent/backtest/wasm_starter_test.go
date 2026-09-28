package backtest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tetratelabs/wazero/api"

	doraclient "github.com/dora-network/dora-client-go/doraclient"
	dorastrategy "github.com/dora-network/dora-strategy-wasm/dorastrategy"

	agentstore "github.com/dora-network/bond-trading-strategies/internal/agent/store"
)

func candleFixture() dorastrategy.Candle {
	return dorastrategy.Candle{
		OrderBookID:    "OB-1",
		StartTimestamp: time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		Open:           "100",
		High:           "110",
		Low:            "95",
		Close:          "105",
		OpenYtm:        "0.05",
		CloseYtm:       "0.051",
		HighYtm:        "0.052",
		LowYtm:         "0.049",
		Volume:         "1000",
	}
}

func newTestState(candles ...dorastrategy.Candle) *jobState {
	// Point lastCandle at the last candle in the variadic list (matches
	// the "host_next_candle has delivered this candle" precondition the
	// simulateFill tests need). When no candles are passed, lastCandle
	// stays nil so simulateFill can verify the no-candle error path.
	var last *dorastrategy.Candle
	if len(candles) > 0 {
		c := candles[len(candles)-1]
		last = &c
	}
	return &jobState{lastCandle: last, logger: slog.New(slog.DiscardHandler)}
}

func TestSimulateFill_Market(t *testing.T) {
	state := newTestState(candleFixture())

	fill, err := simulateFill(state, dorastrategy.OrderIntent{Side: "buy", Quantity: "10", Type: "market"})
	require.NoError(t, err)
	require.Equal(t, "105", fill.Price)
	require.Equal(t, "10", fill.Quantity)
	require.Equal(t, "sim-0001", fill.OrderID)
	require.True(t, fill.Simulated)
	require.Equal(t, "buy", state.currentSide)
}

func TestSimulateFill_LimitWithinRange(t *testing.T) {
	state := newTestState(candleFixture())

	fill, err := simulateFill(state, dorastrategy.OrderIntent{Side: "sell", Quantity: "5", Type: "limit", Price: "100"})
	require.NoError(t, err)
	require.Equal(t, "100", fill.Price)
	require.Equal(t, "5", fill.Quantity)
	require.Equal(t, "sell", state.currentSide)
}

func TestSimulateFill_LimitOutsideRange(t *testing.T) {
	state := newTestState(candleFixture())

	_, err := simulateFill(state, dorastrategy.OrderIntent{Side: "buy", Quantity: "1", Type: "limit", Price: "90"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "limit price 90 not within candle range 95-110")
}

func TestSimulateFill_NoCurrentCandle(t *testing.T) {
	state := newTestState()

	_, err := simulateFill(state, dorastrategy.OrderIntent{Side: "buy", Quantity: "1", Type: "market"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no current candle")
}

func TestFrameworkFillToBacktest(t *testing.T) {
	fw := dorastrategy.Fill{OrderID: "sim-0001", Price: "105.5", Quantity: "10", Simulated: true}
	bt, err := frameworkFillToBacktest(fw, "buy")
	require.NoError(t, err)
	require.Equal(t, "buy", bt.Side)
	require.Equal(t, 105.5, bt.Price)
	require.Equal(t, 10.0, bt.Quantity)
	require.Equal(t, "sim-0001", bt.OrderID)
	require.True(t, bt.SimulatedAt.Equal(bt.Timestamp))
}

func TestSDKCandlesToFramework(t *testing.T) {
	ts := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	c := doraclient.NewCandleWithDefaults()
	c.SetOrderBookId("OB-2")
	c.SetStartTimestamp(ts)
	c.SetOpen("1")
	c.SetHigh("2")
	c.SetLow("3")
	c.SetClose("4")
	c.SetOpenYtm("0.1")
	c.SetCloseYtm("0.2")
	c.SetHighYtm("0.3")
	c.SetLowYtm("0.4")
	c.SetVolume("500")

	out := sdkCandlesToFramework([]doraclient.Candle{*c})
	require.Len(t, out, 1)
	got := out[0]
	require.Equal(t, "OB-2", got.OrderBookID)
	require.Equal(t, ts.Format(time.RFC3339Nano), got.StartTimestamp)
	require.Equal(t, "1", got.Open)
	require.Equal(t, "2", got.High)
	require.Equal(t, "3", got.Low)
	require.Equal(t, "4", got.Close)
	require.Equal(t, "0.1", got.OpenYtm)
	require.Equal(t, "0.2", got.CloseYtm)
	require.Equal(t, "0.3", got.HighYtm)
	require.Equal(t, "0.4", got.LowYtm)
	require.Equal(t, "500", got.Volume)
}

func TestLimitWithinRange_InvalidNumber(t *testing.T) {
	within, err := limitWithinRange("abc", "1", "2")
	require.Error(t, err)
	require.False(t, within)
}

func TestResolutionDuration(t *testing.T) {
	cases := map[string]time.Duration{
		"1m":  time.Minute,
		"5m":  5 * time.Minute,
		"15m": 15 * time.Minute,
		"1h":  time.Hour,
		"4h":  4 * time.Hour,
		"1d":  24 * time.Hour,
	}
	for res, want := range cases {
		require.Equal(t, want, resolutionDuration(res), "res=%s", res)
	}
	require.Equal(t, time.Duration(0), resolutionDuration("unknown"))
}

func TestFetchCandles_PaginatesAndSorts(t *testing.T) {
	// 1m candles over a 4h window would normally be 240 candles —
	// well under the cap. To exercise pagination, ask for a 5000m
	// (~3.5 day) window at 1m resolution, which is 5000 candles and
	// spans ~4 chunks at 1500 candles/chunk. The fake server records
	// each request's start/end and returns a single candle at the
	// chunk midpoint with a descending timestamp so we can verify
	// the sort-by-oldest-first behaviour.
	const chunkCount = 4
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	chunkSpan := time.Duration(candlesPerChunk) * time.Minute
	end := start.Add(chunkSpan * time.Duration(chunkCount))

	var mu sync.Mutex
	requests := []struct{ Start, End time.Time }{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		s, _ := time.Parse(time.RFC3339, q.Get("start"))
		e, _ := time.Parse(time.RFC3339, q.Get("end"))
		mu.Lock()
		requests = append(requests, struct{ Start, End time.Time }{s, e})
		// Return ONE candle per request at the midpoint, descending
		// timestamp so the final sort is observable.
		mid := s.Add(e.Sub(s) / 2)
		mu.Unlock()
		resp := doraclient.ListCandlesResponseEnvelope{}
		resp.Data = []doraclient.Candle{
			*makeSDKCandle("OB-1", mid.Format(time.RFC3339Nano), "1", "2", "1", "1", "0", "0", "0", "0", "100"),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	cfg := doraclient.NewConfiguration()
	cfg.Servers = []doraclient.ServerConfiguration{{URL: srv.URL}}
	api := doraclient.NewAPIClient(cfg)

	w := &WasmStarter{
		api:    api,
		logger: slog.New(slog.DiscardHandler),
	}
	b := &Backtest{
		OrderBookID: "OB-1",
		WindowStart: start,
		WindowEnd:   end,
		Resolution:  "1m",
	}

	got, err := w.fetchCandles(t.Context(), b)
	require.NoError(t, err)
	require.Len(t, got, chunkCount, "one candle per chunk")

	// Requests must be exactly chunkCount, each with [start+i*span, start+(i+1)*span]
	// (last clamped to b.WindowEnd).
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, requests, chunkCount)
	for i, req := range requests {
		wantStart := start.Add(chunkSpan * time.Duration(i))
		wantEnd := wantStart.Add(chunkSpan)
		if wantEnd.After(end) {
			wantEnd = end
		}
		require.Equal(t, wantStart, req.Start, "chunk %d start", i)
		require.Equal(t, wantEnd, req.End, "chunk %d end", i)
	}
}

func TestFetchCandles_FallsBackOnUnrecognisedResolution(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		resp := doraclient.ListCandlesResponseEnvelope{Data: []doraclient.Candle{}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	cfg := doraclient.NewConfiguration()
	cfg.Servers = []doraclient.ServerConfiguration{{URL: srv.URL}}
	api := doraclient.NewAPIClient(cfg)
	w := &WasmStarter{api: api, logger: slog.New(slog.DiscardHandler)}
	b := &Backtest{
		OrderBookID: "OB-1",
		WindowStart: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		WindowEnd:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		Resolution:  "3m", // not in the resolutionDuration table
	}

	_, err := w.fetchCandles(t.Context(), b)
	require.NoError(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&hits), "unrecognised resolution must fall back to a single-shot fetch")
}

func makeSDKCandle(obID, ts, open, high, low, close, openYTM, closeYTM, highYTM, lowYTM, vol string) *doraclient.Candle {
	c := doraclient.NewCandle(obID, mustParseTime(ts), open, high, low, close, openYTM, closeYTM, highYTM, lowYTM, vol)
	return c
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

// TestWarmupStart verifies the warmup-window math: warmupStart shifts
// windowStart back by WarmupCandles * resolutionDuration. This is the
// core logic Task 12 adds; Task 14 wires it into fetchCandles with a
// fakeHistory so the captured start can be asserted end-to-end.
func TestWarmupStart(t *testing.T) {
	windowStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name          string
		warmupCandles int
		resolution    string
		want          time.Time
	}{
		{"zero warmup returns windowStart unchanged", 0, "1h", windowStart},
		{"negative warmup returns windowStart unchanged", -3, "1h", windowStart},
		{"five 1h candles shifts back 5h", 5, "1h", windowStart.Add(-5 * time.Hour)},
		{"ten 1m candles shifts back 10m", 10, "1m", windowStart.Add(-10 * time.Minute)},
		{"three 1d candles shifts back 3d", 3, "1d", windowStart.Add(-3 * 24 * time.Hour)},
		{"unrecognised resolution returns windowStart unchanged", 5, "7m", windowStart},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := warmupStart(windowStart, tc.warmupCandles, tc.resolution)
			require.True(t, got.Equal(tc.want), "warmupStart(%v, %d, %q) = %v, want %v",
				windowStart, tc.warmupCandles, tc.resolution, got, tc.want)
		})
	}
}

// TestWarmupStart_MatchesBacktestField exercises the full path from the
// Backtest row: the warmup window for a Backtest with WarmupCandles set
// should start earlier than its WindowStart. Task 14 replaces this
// direct-call with a fakeHistory that captures the actual fetch start.
func TestWarmupStart_MatchesBacktestField(t *testing.T) {
	b := &Backtest{
		WindowStart:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		WindowEnd:     time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		Resolution:    "1h",
		WarmupCandles: 5,
	}
	want := b.WindowStart.Add(-5 * time.Hour)
	got := warmupStart(b.WindowStart, b.WarmupCandles, b.Resolution)
	require.True(t, got.Equal(want), "warmupStart = %v, want %v", got, want)
	require.True(t, got.Before(b.WindowStart), "warmup start must precede window start")
}

// mkHistoryCandle builds a agentstore.Candle for split tests.
func mkHistoryCandle(ts time.Time, close string) agentstore.Candle {
	return agentstore.Candle{
		OrderBookID:    "OB-1",
		StartTimestamp: ts,
		Open:           close,
		High:           close,
		Low:            close,
		Close:          close,
		Volume:         "1",
	}
}

// onePage wraps a single page of rows as a pageFetcher that serves
// the page once, then an empty page (pageStream's done signal).
func onePage[T any](rows []T) pageFetcher[T] {
	served := false
	return func(context.Context, string) ([]T, string, error) {
		if served {
			return nil, "", nil
		}
		served = true
		return rows, "", nil
	}
}

// callNextEvent invokes the 2-arg host_next_event ABI on the fake
// module, returning the envelope bytes written (nil when the result
// is 0/negative).
func callNextEvent(t *testing.T, fn api.GoModuleFunc, mod *fetchTestModule) ([]byte, int32) {
	t.Helper()
	params := []uint64{0, 65536}
	fn(t.Context(), mod, params)
	n := int32(params[0]) //nolint:gosec // bounded by the buffer
	if n <= 0 {
		return nil, n
	}
	return mod.mem.buf[:n], n
}

// TestHostNextEvent_LegacyRestFallbackStaysEager guards the legacy
// REST path (w.history == nil): hostNextEvent walks the pre-loaded
// replayCandles slice eagerly, emitting candle envelopes in time
// order and then the 0 done signal. The legacy walker updates
// state.lastCandle on each step so simulateFill sees the current
// delivered candle.
func TestHostNextEvent_LegacyRestFallbackStaysEager(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	c1 := dorastrategy.Candle{OrderBookID: "OB-1", StartTimestamp: base.Format(time.RFC3339Nano), Close: "c1"}
	c2 := dorastrategy.Candle{OrderBookID: "OB-1", StartTimestamp: base.Add(time.Hour).Format(time.RFC3339Nano), Close: "c2"}
	state := newTestState(c1, c2)
	state.replayCandles = []dorastrategy.Candle{c1, c2}
	state.lastCandle = nil // the legacy walker will repopulate it as it advances
	fn := hostNextEvent(state)
	mod := &fetchTestModule{mem: &fetchTestMemory{buf: make([]byte, 4096)}}

	var seen []string
	for {
		raw, n := callNextEvent(t, fn, mod)
		if n == 0 {
			break
		}
		require.NotNil(t, raw)
		var env eventEnvelope
		require.NoError(t, json.Unmarshal(raw, &env))
		require.Equal(t, "candle", env.Type, "legacy path emits only candle envelopes")
		var c dorastrategy.Candle
		require.NoError(t, json.Unmarshal(env.Data, &c))
		seen = append(seen, c.Close)
	}
	require.Equal(t, []string{"c1", "c2"}, seen, "candles must arrive in time order")
	require.Equal(t, 2, state.eventCandleIdx)
	// After both candles are walked, lastCandle points at c2 (the
	// last delivered) so simulateFill would price against c2.
	require.NotNil(t, state.lastCandle)
	require.Equal(t, "c2", state.lastCandle.Close)
}

// TestHostNextEvent_StreamsTaggedEnvelopesInTimeOrder covers the
// internal/history path: hostNextEvent drives off the mergeStream and
// emits interleaved tagged envelopes in timestamp order, then the 0
// done signal. Candle events update state.lastCandle so
// simulateFill keeps working on the streaming path.
func TestHostNextEvent_StreamsTaggedEnvelopesInTimeOrder(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	// onePage serves rows once, then an empty page (the stream's
	// done signal — pageStream only treats an empty page as done).
	fetchCandles := onePage([]candleRow{
		{ts: base.Add(2 * time.Hour), payload: dorastrategy.Candle{OrderBookID: "OB-1", Close: "c2"}},
		{ts: base.Add(5 * time.Hour), payload: dorastrategy.Candle{OrderBookID: "OB-1", Close: "c5"}},
	})
	fetchTrades := onePage([]tradeRow{
		{ts: base.Add(time.Hour), payload: dorastrategy.Trade{TransactionID: "tx1"}},
		{ts: base.Add(3 * time.Hour), payload: dorastrategy.Trade{TransactionID: "tx3"}},
	})
	fetchPrices := onePage([]priceRow{
		{ts: base, payload: dorastrategy.Price{AssetID: "p1", Price: "100"}},
		{ts: base.Add(4 * time.Hour), payload: dorastrategy.Price{AssetID: "p4", Price: "104"}},
	})
	state := newTestState()
	state.replay = newMergeStream(t.Context(), 10, 10, 10, fetchCandles, fetchTrades, fetchPrices)
	defer state.replay.close()

	fn := hostNextEvent(state)
	mod := &fetchTestModule{mem: &fetchTestMemory{buf: make([]byte, 8192)}}

	var got []string
	for {
		raw, n := callNextEvent(t, fn, mod)
		if n == 0 {
			break
		}
		require.NotNil(t, raw)
		var env eventEnvelope
		require.NoError(t, json.Unmarshal(raw, &env))
		switch env.Type {
		case "candle":
			var c dorastrategy.Candle
			require.NoError(t, json.Unmarshal(env.Data, &c))
			got = append(got, "candle:"+c.Close)
		case "trade":
			var tr dorastrategy.Trade
			require.NoError(t, json.Unmarshal(env.Data, &tr))
			got = append(got, "trade:"+tr.TransactionID)
		case "price":
			var p dorastrategy.Price
			require.NoError(t, json.Unmarshal(env.Data, &p))
			got = append(got, "price:"+p.AssetID)
		default:
			t.Fatalf("unexpected envelope type %q", env.Type)
		}
	}
	want := []string{"price:p1", "trade:tx1", "candle:c2", "trade:tx3", "price:p4", "candle:c5"}
	require.Equal(t, want, got, "events must be interleaved by timestamp")
	// On the streaming path, only the last delivered candle is
	// retained (bounded memory, independent of window length).
	require.NotNil(t, state.lastCandle)
	require.Equal(t, "c5", state.lastCandle.Close)
}

// TestHostNextEvent_EmptyLegacyState returns done immediately when
// the legacy path has no replay candles.
func TestHostNextEvent_EmptyLegacyState(t *testing.T) {
	state := newTestState()
	fn := hostNextEvent(state)
	mod := &fetchTestModule{mem: &fetchTestMemory{buf: make([]byte, 4096)}}
	_, n := callNextEvent(t, fn, mod)
	require.Equal(t, int32(0), n, "empty state must signal done")
}

// TestWasmStarter_BacktestUses5mBucketedCandles verifies that when
// b.Resolution = "5m", the warmupStart math uses 5m buckets. This
// exercises the resolution → duration mapping that drives the
// server-side SQL bucketing.
func TestWasmStarter_BacktestUses5mBucketedCandles(t *testing.T) {
	windowStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// 5m resolution with 12 warmup candles = 1h of warmup.
	got := warmupStart(windowStart, 12, "5m")
	want := windowStart.Add(-1 * time.Hour)
	require.True(t, got.Equal(want), "warmupStart with 5m resolution = %v, want %v", got, want)

	// Verify the resolution is correctly mapped (5m = 5 min duration).
	require.Equal(t, 5*time.Minute, resolutionDuration("5m"))
}

// TestHistoryCandleToFramework verifies the time.Time → RFC3339
// string conversion that the host functions use when marshalling.
func TestHistoryCandleToFramework(t *testing.T) {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	hc := agentstore.Candle{
		OrderBookID:    "OB-1",
		StartTimestamp: ts,
		Close:          "105",
	}
	fw := historyCandleToFramework(hc)
	require.Equal(t, "OB-1", fw.OrderBookID)
	require.Equal(t, ts.Format(time.RFC3339Nano), fw.StartTimestamp)
	require.Equal(t, "105", fw.Close)
}

// TestHistoryTradeToFramework verifies the trade conversion.
func TestHistoryTradeToFramework(t *testing.T) {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ht := agentstore.Trade{
		TransactionID: "tx1",
		OrderBookID:   "OB-1",
		Price:         "100",
		CreatedAt:     ts,
	}
	fw := historyTradeToFramework(ht)
	require.Equal(t, "tx1", fw.TransactionID)
	require.Equal(t, "OB-1", fw.OrderBookID)
	require.Equal(t, "100", fw.Price)
	require.Equal(t, ts.Format(time.RFC3339Nano), fw.CreatedAt)
}

// TestHistoryPriceToFramework verifies the price conversion.
func TestHistoryPriceToFramework(t *testing.T) {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	hp := agentstore.Price{
		AssetID:   "a1",
		Price:     "100",
		Timestamp: ts,
	}
	fw := historyPriceToFramework(hp)
	require.Equal(t, "a1", fw.AssetID)
	require.Equal(t, "100", fw.Price)
	require.Equal(t, ts.Format(time.RFC3339Nano), fw.Time)
}

// TestFetchAll_CursorPagination verifies the backtest history-store
// fetch drains every page: a store whose first call returns a page
// plus a cursor, and whose second call returns the final page, must
// yield both pages' rows in one slice. Before the fix, Start fetched
// once with cursor="" and discarded the next-cursor, so windows
// larger than one LIMIT page replayed only the first page.
func TestFetchAll_CursorPagination(t *testing.T) {
	t.Parallel()
	page1 := []agentstore.Candle{mkHistoryCandle(time.Now(), "1"), mkHistoryCandle(time.Now(), "2")}
	page2 := []agentstore.Candle{mkHistoryCandle(time.Now(), "3")}
	calls := 0
	all, err := fetchAll(func(cursor string) ([]agentstore.Candle, string, error) {
		calls++
		if cursor == "" {
			return page1, "cur-1", nil
		}
		if cursor != "cur-1" {
			return nil, "", fmt.Errorf("unexpected cursor %q", cursor)
		}
		return page2, "", nil
	})
	require.NoError(t, err)
	require.Len(t, all, len(page1)+len(page2), "must merge both pages")
	require.Equal(t, 2, calls, "must consume both pages")
	require.Equal(t, page2[len(page2)-1], all[len(all)-1], "page-2 rows must come last")
}

// recordingPriceFetcher captures the assetID passed to FetchPrices.
type recordingPriceFetcher struct {
	calls   int
	assetID string
}

func (r *recordingPriceFetcher) FetchCandles(_ context.Context, _ string, _, _ time.Time,
	_ string, _ string, _ int,
) ([]agentstore.Candle, string, error) {
	return nil, "", nil
}

func (r *recordingPriceFetcher) FetchTrades(_ context.Context, _ string, _, _ time.Time,
	_ string, _ int,
) ([]agentstore.Trade, string, error) {
	return nil, "", nil
}

func (r *recordingPriceFetcher) FetchPrices(_ context.Context, assetID string, _, _ time.Time,
	_ string, _ int,
) ([]agentstore.Price, string, error) {
	r.calls++
	r.assetID = assetID
	return nil, "", nil
}

func TestFetchHistoryWindow_LookupAssetIDFlowsToFetchPrices(t *testing.T) {
	fetcher := &recordingPriceFetcher{}
	w := &WasmStarter{
		history: fetcher,
		logger:  slog.New(slog.DiscardHandler),
		apiLookup: func(context.Context, *Backtest, string) string {
			return "asset-123"
		},
	}
	b := &Backtest{OrderBookID: "OB-1", Resolution: "1h"}
	_, err := w.fetchHistoryWindow(t.Context(), b, "test-key")
	require.NoError(t, err)
	require.Equal(t, "asset-123", fetcher.assetID)
}

// TestFetchHistoryWindow_NoLookupSkipsPriceFetch guards the real
// fallback behavior: when apiLookup returns "", FetchPrices is
// skipped (not called with ""). Passing "" to *Store.FetchPrices
// would bind an empty string into a UUID column and fail at the
// pgx layer with "invalid input syntax for type uuid" (re-introducing
// P2 #13). The strategy should run without price history, not fail.
func TestFetchHistoryWindow_NoLookupSkipsPriceFetch(t *testing.T) {
	fetcher := &recordingPriceFetcher{}
	w := &WasmStarter{
		history: fetcher,
		logger:  slog.New(slog.DiscardHandler),
		apiLookup: func(context.Context, *Backtest, string) string {
			// Production failure mode: lookupAssetID returns "" on
			// any error path (HTTP failure, nil resp, nil data,
			// empty BaseAssetId). The inner if-assetID != ""
			// guard in fetchHistoryWindow must skip FetchPrices
			// entirely — passing "" would bind to a UUID column
			// and fail at the pgx layer ("invalid input syntax
			// for type uuid"), re-introducing P2 #13.
			return ""
		},
	}
	b := &Backtest{OrderBookID: "OB-1", Resolution: "1h"}
	hw, err := w.fetchHistoryWindow(t.Context(), b, "test-key")
	require.NoError(t, err)
	require.Equal(t, 0, fetcher.calls, "FetchPrices must not be called when assetID lookup returns empty")
	require.Nil(t, hw.preamblePrices, "preamblePrices must be nil when price fetch is skipped")
}

// fetchTestModule is a fetch-ABI fake (mirrors the livehost tests):
// offset 0 is the shared read/write buffer origin.
type fetchTestModule struct {
	api.Module
	mem *fetchTestMemory
}

func (m *fetchTestModule) Memory() api.Memory { return m.mem }

type fetchTestMemory struct {
	api.Memory
	buf []byte
}

func (m *fetchTestMemory) Read(offset, length uint32) ([]byte, bool) {
	if offset != 0 || uint32(len(m.buf)) < length { //nolint:gosec // test
		return nil, false
	}
	return m.buf[:length], true
}

func (m *fetchTestMemory) Write(_ uint32, val []byte) bool {
	m.buf = append(m.buf[:0], val...)
	return true
}

// callFetch invokes a fetch host function with the fetchReq JSON in
// the shared buffer, returning the raw batch JSON written back.
func callFetch(t *testing.T, fn api.GoModuleFunc, mod *fetchTestModule, req string) (string, int32) {
	t.Helper()
	mod.mem.buf = append(mod.mem.buf[:0], req...)
	params := []uint64{0, uint64(len(req)), 0, 65536}
	fn(t.Context(), mod, params)
	n := int32(params[0]) //nolint:gosec // bounded by the buffer
	if n <= 0 {
		return "", n
	}
	return string(mod.mem.buf[:n]), n
}

func TestWasmStarter_HostFetchCandlesHonorsCursor(t *testing.T) {
	candles := make([]dorastrategy.Candle, 5)
	for i := range candles {
		candles[i] = candleFixture()
		candles[i].Open = strconv.Itoa(100 + i)
	}
	state := newTestState()
	state.preambleCandles = candles
	fn := hostFetchCandles(state)
	mod := &fetchTestModule{mem: &fetchTestMemory{buf: make([]byte, 4096)}}

	// First page: cursor empty, batch_size 2.
	b1, n1 := callFetch(t, fn, mod, `{"batch_size":2}`)
	require.Greater(t, n1, int32(0))
	var batch1 dorastrategy.CandleBatch
	require.NoError(t, json.Unmarshal([]byte(b1), &batch1))
	require.Len(t, batch1.Items, 2)
	require.Equal(t, "100", batch1.Items[0].Open)
	require.Equal(t, "101", batch1.Items[1].Open)
	require.False(t, batch1.Done)
	require.Equal(t, "2", batch1.Cursor)

	// Second page from the returned cursor.
	b2, _ := callFetch(t, fn, mod, `{"batch_size":2,"cursor":"2"}`)
	var batch2 dorastrategy.CandleBatch
	require.NoError(t, json.Unmarshal([]byte(b2), &batch2))
	require.Len(t, batch2.Items, 2)
	require.False(t, batch2.Done)
	require.Equal(t, "4", batch2.Cursor)

	// Final page: one item left, Done=true, cursor cleared.
	b3, _ := callFetch(t, fn, mod, `{"batch_size":2,"cursor":"4"}`)
	var batch3 dorastrategy.CandleBatch
	require.NoError(t, json.Unmarshal([]byte(b3), &batch3))
	require.Len(t, batch3.Items, 1)
	require.Equal(t, "104", batch3.Items[0].Open)
	require.True(t, batch3.Done)
	require.Empty(t, batch3.Cursor)

	// Unset batch_size serves the whole (small) preamble in one page.
	bDef, _ := callFetch(t, fn, mod, `{}`)
	var batchDef dorastrategy.CandleBatch
	require.NoError(t, json.Unmarshal([]byte(bDef), &batchDef))
	require.Len(t, batchDef.Items, 5)
	require.True(t, batchDef.Done)

	// Out-of-range cursor is an error result.
	_, nBad := callFetch(t, fn, mod, `{"cursor":"99"}`)
	require.Equal(t, int32(-1), nBad)
}

func TestWasmStarter_HostFetchTradesHonorsCursor(t *testing.T) {
	trades := make([]dorastrategy.Trade, 3)
	for i := range trades {
		trades[i] = dorastrategy.Trade{TransactionID: fmt.Sprintf("tx-%d", i), Price: "100"}
	}
	state := newTestState()
	state.preambleTrades = trades
	fn := hostFetchTrades(state)
	mod := &fetchTestModule{mem: &fetchTestMemory{buf: make([]byte, 4096)}}

	b1, _ := callFetch(t, fn, mod, `{"batch_size":2}`)
	var batch1 dorastrategy.TradeBatch
	require.NoError(t, json.Unmarshal([]byte(b1), &batch1))
	require.Len(t, batch1.Items, 2)
	require.False(t, batch1.Done)
	require.Equal(t, "2", batch1.Cursor)

	b2, _ := callFetch(t, fn, mod, `{"batch_size":2,"cursor":"2"}`)
	var batch2 dorastrategy.TradeBatch
	require.NoError(t, json.Unmarshal([]byte(b2), &batch2))
	require.Len(t, batch2.Items, 1)
	require.Equal(t, "tx-2", batch2.Items[0].TransactionID)
	require.True(t, batch2.Done)
	require.Empty(t, batch2.Cursor)
}

func TestWasmStarter_HostFetchPricesHonorsCursor(t *testing.T) {
	prices := make([]dorastrategy.Price, 3)
	for i := range prices {
		prices[i] = dorastrategy.Price{AssetID: "A-1", Price: fmt.Sprintf("%d", 100+i)}
	}
	state := newTestState()
	state.preamblePrices = prices
	fn := hostFetchPrices(state)
	mod := &fetchTestModule{mem: &fetchTestMemory{buf: make([]byte, 4096)}}

	b1, _ := callFetch(t, fn, mod, `{"batch_size":1}`)
	var batch1 dorastrategy.PriceBatch
	require.NoError(t, json.Unmarshal([]byte(b1), &batch1))
	require.Len(t, batch1.Items, 1)
	require.Equal(t, "100", batch1.Items[0].Price)
	require.False(t, batch1.Done)
	require.Equal(t, "1", batch1.Cursor)

	b2, _ := callFetch(t, fn, mod, `{"cursor":"1"}`)
	var batch2 dorastrategy.PriceBatch
	require.NoError(t, json.Unmarshal([]byte(b2), &batch2))
	require.Len(t, batch2.Items, 2)
	require.True(t, batch2.Done)
	require.Empty(t, batch2.Cursor)
}
