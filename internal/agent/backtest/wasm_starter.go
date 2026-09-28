package backtest

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"strconv"
	"sync"
	"time"

	doraclient "github.com/dora-network/dora-client-go/doraclient"
	dorastrategy "github.com/dora-network/dora-strategy-wasm/dorastrategy"
	"github.com/dora-network/dora-strategy-wasm/dorastrategy/host"

	agentstore "github.com/dora-network/bond-trading-strategies/internal/agent/store"
	registry "github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// WasmStarter runs a WASM strategy backtest in-process. It does not
// implement ProcessStarter; it is a separate runner used by the HTTP
// handler for strategy_versions.target == "go-wasm".
type WasmStarter struct {
	reg     WasmRuntime
	store   WasmArtifactStore
	btStore Store
	api     *doraclient.APIClient
	// buildHostModule is the wasmruntime host-module seam. L5 wiring
	// assigns registry.BuildHostModule here; until then it is nil and
	// Start fails fast with a clear panic rather than a silent wrong
	// host.
	buildHostModule func(rt wazero.Runtime, impl func(name string) api.GoModuleFunc) wazero.HostModuleBuilder
	history         agentstore.HistoryFetcher
	logger          *slog.Logger

	// apiLookup resolves the order book's base asset_id via the Dora
	// API. Function-value seam so tests can fake it without an APIClient.
	apiLookup func(ctx context.Context, b *Backtest, doraAPIKey string) string
}

// NewWasmStarter constructs an in-process WASM backtest runner.
func NewWasmStarter(
	reg WasmRuntime,
	st WasmArtifactStore,
	btStore Store,
	api *doraclient.APIClient,
	hist agentstore.HistoryFetcher,
	logger *slog.Logger,
) *WasmStarter {
	w := &WasmStarter{
		reg:     reg,
		store:   st,
		btStore: btStore,
		api:     api,
		history: hist,
		logger:  logger,
	}
	w.apiLookup = w.lookupAssetID
	return w
}

// Start fetches candles, loads the .wasm, runs the backtest loop, and
// persists the results directly to the backtest store.
func (w *WasmStarter) Start(ctx context.Context, b *Backtest, wasmRef, manifestHash, doraAPIKey string) error {
	startedAt := time.Now().UTC()
	if err := w.btStore.UpdateStatus(ctx, b.ID, StatusRunning, &startedAt, nil, ""); err != nil {
		return fmt.Errorf("wasm starter: mark running: %w", err)
	}

	var (
		preambleCandles []dorastrategy.Candle
		preambleTrades  []dorastrategy.Trade
		preamblePrices  []dorastrategy.Price
		replayCandles   []dorastrategy.Candle
		replay          *mergeStream
	)

	if w.history != nil {
		win, err := w.fetchHistoryWindow(ctx, b, doraAPIKey)
		if err != nil {
			return err
		}
		preambleCandles, preambleTrades, preamblePrices = win.preambleCandles, win.preambleTrades, win.preamblePrices
		// Replay streams page-by-page from the mergeStream; no eager
		// replay slices on this path.
		replay = w.newReplayStream(ctx, b, doraAPIKey)
	} else {
		// Legacy REST path: no history store configured. Fetch
		// candles via the Dora REST API; trades and prices are
		// unavailable. This path stays eager — the SDK REST trades
		// endpoint is paged at 100 rows per call, so streaming it
		// would amplify, not reduce, long-window cost.
		candleCtx := withAPIKey(ctx, doraAPIKey)
		candles, err := w.fetchCandles(candleCtx, b)
		if err != nil {
			w.fail(ctx, b.ID, fmt.Sprintf("candle fetch: %v", err))
			return fmt.Errorf("wasm starter: fetch candles: %w", err)
		}
		replayCandles = candles
	}

	configJSON, err := w.buildConfigJSON(b)
	if err != nil {
		w.fail(ctx, b.ID, fmt.Sprintf("build config: %v", err))
		return fmt.Errorf("wasm starter: build config: %w", err)
	}

	inst, err := w.reg.Load(ctx, w.store, wasmRef, manifestHash)
	if err != nil {
		w.fail(ctx, b.ID, fmt.Sprintf("load wasm: %v", err))
		return fmt.Errorf("wasm starter: load wasm: %w", err)
	}

	state := &jobState{
		preambleCandles: preambleCandles,
		preambleTrades:  preambleTrades,
		preamblePrices:  preamblePrices,
		replayCandles:   replayCandles,
		replay:          replay,
		configJSON:      configJSON,
		logger:          w.logger,
	}

	var hostMod api.Module
	buildHost := func(rt wazero.Runtime) error {
		var err error
		hostMod, err = w.newBacktestHostModule(ctx, rt, state)
		return err
	}

	mod, err := w.reg.InstantiateWithHost(ctx, inst, buildHost)
	if err != nil {
		w.fail(ctx, b.ID, fmt.Sprintf("run wasm: %v", err))
		return fmt.Errorf("wasm starter: instantiate/run: %w", err)
	}
	defer func() {
		if replay != nil {
			replay.close()
		}
		_ = mod.Close(ctx)
		if hostMod != nil {
			_ = hostMod.Close(ctx)
		}
		// Instance owns the per-call wazero Runtime. Close it
		// last so the host + guest module Closes above release
		// into a still-open store.
		inst.Close(ctx)
	}()
	if state.errored {
		w.fail(ctx, b.ID, state.errorMsg)
		return errors.New(state.errorMsg)
	}

	// Honor cancellation that fired during _start: mergeStream.next()
	// returned ctx.Err(), the plugin's runEvents unwound, and we're
	// about to land. The DB row is already cancelled: CancelIfRunning
	// (postgres.go) flipped it atomically before the goroutine's ctx
	// was cancelled, so there's no second DB write to do. Issuing one
	// here would only race the now-dead ctx and log a spurious error.
	// Returning ctx.Err() skips persistSuccess, which is the load-bearing
	// part: InsertFills + SetSummary would otherwise overwrite the
	// cancelled status as succeeded and persist stray fills.
	if err := ctx.Err(); err != nil {
		return err
	}

	return w.persistSuccess(ctx, b, state.fills)
}

func (w *WasmStarter) fail(ctx context.Context, id, msg string) {
	now := time.Now().UTC()
	if err := w.btStore.UpdateStatus(ctx, id, StatusFailed, nil, &now, msg); err != nil {
		w.logger.Error("wasm starter: failed to mark job failed", "backtest_id", id, "error", err)
	}
}

func (w *WasmStarter) persistSuccess(ctx context.Context, b *Backtest, fills []Fill) error {
	if err := w.btStore.InsertFills(ctx, b.ID, fills); err != nil {
		w.fail(ctx, b.ID, fmt.Sprintf("persist fills: %v", err))
		return fmt.Errorf("wasm starter: insert fills: %w", err)
	}

	initialEquity := defaultInitialEquity
	if v, ok := b.Params["initial_capital"]; ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			initialEquity = f
		}
	}
	summary := Compute(fills, initialEquity)

	if err := w.btStore.SetSummary(ctx, b.ID, summary, len(fills)); err != nil {
		w.fail(ctx, b.ID, fmt.Sprintf("persist summary: %v", err))
		return fmt.Errorf("wasm starter: set summary: %w", err)
	}
	return nil
}

func (w *WasmStarter) fetchCandles(ctx context.Context, b *Backtest) ([]dorastrategy.Candle, error) {
	res := b.Resolution
	chunkDur := resolutionDuration(res)
	if chunkDur == 0 {
		// Unrecognised resolution — fall back to a single-shot fetch.
		return w.fetchCandlesChunk(ctx, b, b.WindowStart, b.WindowEnd)
	}
	chunkSpan := time.Duration(candlesPerChunk) * chunkDur
	var out []dorastrategy.Candle
	for chunkStart := b.WindowStart; chunkStart.Before(b.WindowEnd); chunkStart = chunkStart.Add(chunkSpan) {
		// Honour cancellation between chunks so a long backtest can abort mid-fetch.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chunkEnd := chunkStart.Add(chunkSpan)
		if chunkEnd.After(b.WindowEnd) {
			chunkEnd = b.WindowEnd
		}
		chunk, err := w.fetchCandlesChunk(ctx, b, chunkStart, chunkEnd)
		if err != nil {
			return nil, fmt.Errorf("fetch candles chunk %s..%s: %w",
				chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339), err)
		}
		out = append(out, chunk...)
	}
	// The charting endpoint returns candles in reverse time order
	// (most recent first) for chart rendering; sort oldest-first so
	// the backtest replay sees the strategy's intended input order.
	// ponytail: sorting N candles is cheap relative to the network
	// round-trip we just did; the cost is negligible.
	sort.Slice(out, func(i, j int) bool {
		return out[i].StartTimestamp < out[j].StartTimestamp
	})
	return out, nil
}

// candlesPerChunk is the target number of candles per paginated
// request. The dora candles endpoint caps the response at ~1900
// candles regardless of the requested window, so longer windows must
// be split into sub-windows. 1500 leaves headroom for the API to ever
// tighten the limit without breaking us. ponytail: this is a heuristic;
// if the API limit changes, bump this. Mirrors the docker-based
// strategy framework's defaultFetchCandles pagination.
const candlesPerChunk = 1500

// Per-source warmup sizes for the preamble fetches. The plugin's
// OnPreamble may want far more ticks (trades, prices) than candles
// for high-frequency strategies. The starter derives them from the
// warmup_candles knob:
//   - candles: existing per-fetch chunk size (candlesPerChunk).
//   - trades: max(warmupCandles * 60, 1000) — ~60 trades per 1h
//     candle on a busy book, floor 1000.
//   - prices: max(warmupCandles * 100, 1000) — ~100 ticks per 1h
//     candle (one every ~36s), floor 1000.
//
// The plugin can override per-call via the batchSize arg in
// FetchTrades / FetchPrices.
const (
	warmupTradesPerCandle = 60
	warmupPricesPerCandle = 100
	warmupMinTicks        = 1000
	// Replay page sizes (used by the streaming mergeStream).
	warmupTradesReplay = 1000
	warmupPricesReplay = 1000
)

// resolutionDuration returns the time.Duration of one candle for
// the given resolution string ("1m", "5m", "15m", "1h", "4h", "1d").
// Returns 0 for an unrecognised resolution — the caller
// falls back to a single-shot fetch in that case. Matches the
// doraclient.CandleResolution enum values.
func resolutionDuration(res string) time.Duration {
	switch res {
	case "1m":
		return time.Minute
	case "5m":
		return 5 * time.Minute //nolint:mnd // resolution enum multiplier
	case "15m":
		return 15 * time.Minute //nolint:mnd // resolution enum multiplier
	case "1h":
		return time.Hour
	case "4h":
		return 4 * time.Hour //nolint:mnd // resolution enum multiplier
	case "1d":
		return 24 * time.Hour //nolint:mnd // resolution enum multiplier
	}
	return 0
}

// warmupStart returns windowStart shifted back by warmupCandles*resolution.
// Returns windowStart unchanged when warmupCandles <= 0 or the resolution is
// unrecognised (resolutionDuration == 0), so callers without warmup behave
// exactly as before. Task 14 plumbs this into fetchCandles.
func warmupStart(windowStart time.Time, warmupCandles int, resolution string) time.Time {
	if warmupCandles <= 0 {
		return windowStart
	}
	d := resolutionDuration(resolution)
	if d == 0 {
		return windowStart
	}
	return windowStart.Add(-time.Duration(warmupCandles) * d)
}

// historyWindow is the eager preamble slice for one backtest. The
// replay window is NOT part of this snapshot — it streams page-by-page
// via the mergeStream built by newReplayStream.
type historyWindow struct {
	preambleCandles []dorastrategy.Candle
	preambleTrades  []dorastrategy.Trade
	preamblePrices  []dorastrategy.Price
}

// fetchHistoryWindow eagerly fetches the preamble slice (the warmup
// window [warmupStart, b.WindowStart)) for the plugin's OnPreamble.
// The replay window [b.WindowStart, b.WindowEnd) is streamed by
// newReplayStream's mergeStream instead — host_next_event pulls from
// it page-by-page.
func (w *WasmStarter) fetchHistoryWindow(ctx context.Context, b *Backtest, doraAPIKey string) (historyWindow, error) {
	ws := warmupStart(b.WindowStart, b.WarmupCandles, b.Resolution)
	warmupTrades := max(b.WarmupCandles*warmupTradesPerCandle, warmupMinTicks)
	warmupPrices := max(b.WarmupCandles*warmupPricesPerCandle, warmupMinTicks)
	hcandles, err := fetchAll(func(cursor string) ([]agentstore.Candle, string, error) {
		return w.history.FetchCandles(ctx, b.OrderBookID, ws, b.WindowStart, b.Resolution, cursor, candlesPerChunk)
	})
	if err != nil {
		w.fail(ctx, b.ID, fmt.Sprintf("preamble candle fetch: %v", err))
		return historyWindow{}, fmt.Errorf("wasm starter: preamble candles: %w", err)
	}
	htrades, err := fetchAll(func(cursor string) ([]agentstore.Trade, string, error) {
		return w.history.FetchTrades(ctx, b.OrderBookID, ws, b.WindowStart, cursor, warmupTrades)
	})
	if err != nil {
		w.fail(ctx, b.ID, fmt.Sprintf("preamble trade fetch: %v", err))
		return historyWindow{}, fmt.Errorf("wasm starter: preamble trades: %w", err)
	}
	// Skip the price fetch when the asset_id couldn't be resolved.
	// FetchPrices binds assetID straight into a UUID column
	// (WHERE asset_id = $1), so passing "" is a SQL error, not an
	// empty result — we have to skip the call entirely. The
	// preamble price slice stays nil and the strategy runs without
	// price history (the live side does the same when the manifest
	// declares no price channel). lookupAssetID already logged a
	// Warn explaining the failure.
	var hprices []agentstore.Price
	if w.apiLookup != nil {
		if assetID := w.apiLookup(ctx, b, doraAPIKey); assetID != "" {
			prices, err := fetchAll(func(cursor string) ([]agentstore.Price, string, error) {
				return w.history.FetchPrices(ctx, assetID, ws, b.WindowStart, cursor, warmupPrices)
			})
			if err != nil {
				w.fail(ctx, b.ID, fmt.Sprintf("preamble price fetch: %v", err))
				return historyWindow{}, fmt.Errorf("wasm starter: preamble prices: %w", err)
			}
			hprices = prices
		}
	}
	return historyWindow{
		preambleCandles: historyCandlesToFramework(hcandles),
		preambleTrades:  historyTradesToFramework(htrades),
		preamblePrices:  historyPricesToFramework(hprices),
	}, nil
}

// newReplayStream starts the three replay-window prefetchers. Each
// fetcher walks the [b.WindowStart, b.WindowEnd) window page-by-page
// under the per-job context; the mergeStream interleaves them in time
// order for host_next_event.
func (w *WasmStarter) newReplayStream(ctx context.Context, b *Backtest, doraAPIKey string) *mergeStream {
	fetchCandles := func(ctx context.Context, cursor string) ([]candleRow, string, error) {
		hc, next, err := w.history.FetchCandles(ctx, b.OrderBookID, b.WindowStart, b.WindowEnd, b.Resolution, cursor, candlesPerChunk)
		if err != nil {
			return nil, "", err
		}
		rows := make([]candleRow, len(hc))
		for i, c := range hc {
			rows[i] = candleRow{ts: c.StartTimestamp, payload: historyCandleToFramework(c)}
		}
		return rows, next, nil
	}
	fetchTrades := func(ctx context.Context, cursor string) ([]tradeRow, string, error) {
		ht, next, err := w.history.FetchTrades(ctx, b.OrderBookID, b.WindowStart, b.WindowEnd, cursor, warmupTradesReplay)
		if err != nil {
			return nil, "", err
		}
		rows := make([]tradeRow, len(ht))
		for i, t := range ht {
			rows[i] = tradeRow{ts: t.CreatedAt, payload: historyTradeToFramework(t)}
		}
		return rows, next, nil
	}
	// Resolve the asset_id once up front; an empty id serves an empty
	// price stream (same degraded-but-not-fatal skip as the preamble).
	var assetID string
	if w.apiLookup != nil {
		assetID = w.apiLookup(ctx, b, doraAPIKey)
	}
	fetchPrices := func(ctx context.Context, cursor string) ([]priceRow, string, error) {
		if assetID == "" {
			return nil, "", nil
		}
		hp, next, err := w.history.FetchPrices(ctx, assetID, b.WindowStart, b.WindowEnd, cursor, warmupPricesReplay)
		if err != nil {
			return nil, "", err
		}
		rows := make([]priceRow, len(hp))
		for i, p := range hp {
			rows[i] = priceRow{ts: p.Timestamp, payload: historyPriceToFramework(p)}
		}
		return rows, next, nil
	}
	return newMergeStream(ctx, candlesPerChunk, warmupTradesReplay, warmupPricesReplay, fetchCandles, fetchTrades, fetchPrices)
}

// fetchAll drains a cursor-paginated history fetch into one slice.
// page must return (rows, nextCursor, err); an empty nextCursor
// ends the walk.
func fetchAll[T any](page func(cursor string) ([]T, string, error)) ([]T, error) {
	var all []T
	for cursor := ""; ; {
		rows, next, err := page(cursor)
		if err != nil {
			return nil, err
		}
		all = append(all, rows...)
		if next == "" {
			return all, nil
		}
		cursor = next
	}
}

// historyCandleToFramework converts a agentstore.Candle (time.Time
// timestamp) into a dorastrategy.Candle (RFC3339 string timestamp)
// for JSON marshalling to the plugin.
func historyCandleToFramework(c agentstore.Candle) dorastrategy.Candle {
	return dorastrategy.Candle{
		OrderBookID:    c.OrderBookID,
		StartTimestamp: c.StartTimestamp.Format(time.RFC3339Nano),
		Open:           c.Open,
		High:           c.High,
		Low:            c.Low,
		Close:          c.Close,
		OpenYtm:        c.OpenYtm,
		HighYtm:        c.HighYtm,
		LowYtm:         c.LowYtm,
		CloseYtm:       c.CloseYtm,
		Volume:         c.Volume,
	}
}

// historyTradeToFramework converts a agentstore.Trade (time.Time
// CreatedAt) into a dorastrategy.Trade (RFC3339 string CreatedAt).
func historyTradeToFramework(t agentstore.Trade) dorastrategy.Trade {
	return dorastrategy.Trade{
		TransactionID:      t.TransactionID,
		OrderBookID:        t.OrderBookID,
		OrderID:            t.OrderID,
		OrderSeq:           t.OrderSeq,
		UserID:             t.UserID,
		Asset0:             t.Asset0,
		Price:              t.Price,
		Quantity0:          t.Quantity0,
		Side:               t.Side,
		AggressorIndicator: t.AggressorIndicator,
		CreatedAt:          t.CreatedAt.Format(time.RFC3339Nano),
	}
}

func historyCandlesToFramework(in []agentstore.Candle) []dorastrategy.Candle {
	if len(in) == 0 {
		return nil
	}
	out := make([]dorastrategy.Candle, len(in))
	for i, c := range in {
		out[i] = historyCandleToFramework(c)
	}
	return out
}

func historyTradesToFramework(in []agentstore.Trade) []dorastrategy.Trade {
	if len(in) == 0 {
		return nil
	}
	out := make([]dorastrategy.Trade, len(in))
	for i, t := range in {
		out[i] = historyTradeToFramework(t)
	}
	return out
}

func historyPricesToFramework(in []agentstore.Price) []dorastrategy.Price {
	if len(in) == 0 {
		return nil
	}
	out := make([]dorastrategy.Price, len(in))
	for i, p := range in {
		out[i] = historyPriceToFramework(p)
	}
	return out
}

// historyPriceToFramework converts a agentstore.Price (time.Time
// Timestamp) into a dorastrategy.Price (RFC3339 string Time).
func historyPriceToFramework(p agentstore.Price) dorastrategy.Price {
	return dorastrategy.Price{
		AssetID: p.AssetID,
		Price:   p.Price,
		YTM:     p.YTM,
		Time:    p.Timestamp.Format(time.RFC3339Nano),
	}
}

// lookupAssetID resolves the order book's base asset_id via the Dora
// API. It is used as the price_history key for FetchPrices. Returns ""
// on failure with a warning log — the price fetch then returns zero
// rows (a degraded backtest, not a fail-fast).
func (w *WasmStarter) lookupAssetID(ctx context.Context, b *Backtest, doraAPIKey string) string {
	apiCtx := withAPIKey(ctx, doraAPIKey)
	resp, httpResp, err := w.api.DefaultAPI.GetOrderbookById(apiCtx, b.OrderBookID).Execute()
	if httpResp != nil {
		defer httpResp.Body.Close()
	}
	if err != nil {
		w.logger.Warn("wasm_starter: lookup asset_id failed; price fetch will be empty",
			"order_book_id", b.OrderBookID, "error", err)
		return ""
	}
	if resp == nil || resp.Data == nil {
		w.logger.Warn("wasm_starter: lookup asset_id returned no data; price fetch will be empty",
			"order_book_id", b.OrderBookID)
		return ""
	}
	return resp.Data.GetBaseAssetId()
}

// fetchCandlesChunk fetches one candle window for the given
// [chunkStart, chunkEnd) range. ponytail: the SDK's GetCandleData
// returns *http.Response but the SDK already drains the body; we
// rely on the SDK's normal Execute() call and close the response
// via the execute-returned *http.Response.
func (w *WasmStarter) fetchCandlesChunk(ctx context.Context, b *Backtest, chunkStart, chunkEnd time.Time) ([]dorastrategy.Candle, error) {
	resp, httpResp, err := w.api.DefaultAPI.GetCandleData(ctx, b.OrderBookID).
		Start(chunkStart).
		End(chunkEnd).
		Resolution(doraclient.CandleResolution(b.Resolution)).
		Execute()
	if httpResp != nil {
		defer httpResp.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errors.New("empty candle response")
	}
	return sdkCandlesToFramework(resp.GetData()), nil
}

func sdkCandlesToFramework(in []doraclient.Candle) []dorastrategy.Candle {
	out := make([]dorastrategy.Candle, len(in))
	for i, c := range in {
		out[i] = dorastrategy.Candle{
			OrderBookID:    c.GetOrderBookId(),
			StartTimestamp: c.GetStartTimestamp().Format(time.RFC3339Nano),
			Open:           c.GetOpen(),
			High:           c.GetHigh(),
			Low:            c.GetLow(),
			Close:          c.GetClose(),
			OpenYtm:        c.GetOpenYtm(),
			CloseYtm:       c.GetCloseYtm(),
			HighYtm:        c.GetHighYtm(),
			LowYtm:         c.GetLowYtm(),
			Volume:         c.GetVolume(),
		}
	}
	return out
}

func (w *WasmStarter) buildConfigJSON(b *Backtest) ([]byte, error) {
	cfg := dorastrategy.Config{
		Mode:        dorastrategy.ModeBacktest,
		OrderBookID: b.OrderBookID,
		Start:       b.WindowStart,
		End:         b.WindowEnd,
		Resolution:  b.Resolution,
		Params:      b.Params,
	}
	return json.Marshal(cfg)
}

// jobState is the per-backtest mutable state captured by the host module closures.
type jobState struct {
	mu sync.Mutex
	// lastCandle is the most-recently-delivered candle (regardless of
	// path). simulateFill reads this for fill simulation; host_next_candle
	// reads this once per call. nil when no candle has been delivered yet.
	// Holding a single pointer (not a slice) bounds per-job candle memory
	// to a fixed amount regardless of window length — the streaming
	// merge delivers one candle at a time.
	lastCandle *dorastrategy.Candle
	// lastCandleDelivered is true after the host_next_candle ABI has
	// consumed the current lastCandle (legacy plugins used the
	// host_next_candle ABI to iterate; v3 plugins use host_next_event
	// and never touch this flag). Cleared on each new candle delivery.
	lastCandleDelivered bool
	// Preamble slices: pre-fetched warmup-window data served by
	// host_fetch_candles/trades/prices to the plugin's OnPreamble.
	preambleCandles []dorastrategy.Candle
	preambleTrades  []dorastrategy.Trade
	preamblePrices  []dorastrategy.Price

	// replay is the streaming source for hostNextEvent (the
	// internal/history path). nil on the legacy REST path.
	replay *mergeStream
	// replayCandles is the legacy REST fallback's eager candle
	// slice, walked via eventCandleIdx. The legacy walker updates
	// lastCandle on each step so simulateFill sees the current candle.
	replayCandles  []dorastrategy.Candle
	eventCandleIdx int
	configJSON     []byte
	fills          []Fill
	fillSeq        int
	currentSide    string
	errored        bool
	errorMsg       string
	logger         *slog.Logger
}

func (w *WasmStarter) newBacktestHostModule(ctx context.Context, rt wazero.Runtime, state *jobState) (api.Module, error) {
	build := w.buildHostModule
	if build == nil {
		// L5 default; L8 wiring may override via the field.
		build = registry.BuildHostModuleFn
	}
	hm := build(rt, func(name string) api.GoModuleFunc {
		switch name {
		case "host_get_config":
			return hostGetConfig(state)
		case "host_next_candle":
			return hostNextCandle(state)
		case "host_fetch_candles":
			return hostFetchCandles(state)
		case "host_fetch_trades":
			return hostFetchTrades(state)
		case "host_fetch_prices":
			return hostFetchPrices(state)
		case "host_next_event":
			return hostNextEvent(state)
		case "host_submit_order":
			return hostSubmitOrder(state)
		case "host_record_fill":
			return hostRecordFill(state)
		case "host_log":
			return hostLog(state)
		case "host_backtest_error":
			return hostBacktestError(state)
		default:
			return nil // stub (host_next_live_candle, host_cancel_order, etc.)
		}
	})
	return hm.Instantiate(ctx)
}

func hostGetConfig(state *jobState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		bufPtr := uint32(params[0]) //nolint:gosec // wazero ABI: i32 param
		bufLen := uint32(params[1]) //nolint:gosec // wazero ABI: i32 param

		if uint32(len(state.configJSON)) > bufLen { //nolint:gosec // len fits in u32
			writeError(mod, bufPtr, bufLen, "config JSON exceeds buffer")
			setResult(params, -1)
			return
		}
		mod.Memory().Write(bufPtr, state.configJSON)
		setResult(params, int32(len(state.configJSON))) //nolint:gosec // result fits in i32
	}
}

func hostNextCandle(state *jobState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		bufPtr := uint32(params[0]) //nolint:gosec // wazero ABI: i32 param
		bufLen := uint32(params[1]) //nolint:gosec // wazero ABI: i32 param

		state.mu.Lock()
		// Legacy REST path (state.replay == nil): v2 plugins iterate
		// replayCandles via this ABI. Stream each candle once, then done.
		if state.replay == nil {
			if state.eventCandleIdx >= len(state.replayCandles) {
				state.mu.Unlock()
				setResult(params, 0)
				return
			}
			c := state.replayCandles[state.eventCandleIdx]
			cc := c
			state.lastCandle = &cc
			state.lastCandleDelivered = false
			state.eventCandleIdx++
			b, err := json.Marshal(c)
			if err != nil {
				state.mu.Unlock()
				writeError(mod, bufPtr, bufLen, fmt.Sprintf("marshal candle: %v", err))
				setResult(params, -1)
				return
			}
			if uint32(len(b)) > bufLen { //nolint:gosec // len(b) bounded by bufLen
				state.mu.Unlock()
				writeError(mod, bufPtr, bufLen, "candle JSON exceeds buffer")
				setResult(params, -1)
				return
			}
			state.mu.Unlock()
			mod.Memory().Write(bufPtr, b)
			setResult(params, int32(len(b))) //nolint:gosec // result fits in i32
			return
		}
		// Streaming path (state.replay != nil): v3 plugins use
		// host_next_event. host_next_candle returns the current
		// lastCandle once; if no candle has streamed yet, return done.
		c := state.lastCandle
		if c == nil || state.lastCandleDelivered {
			state.mu.Unlock()
			setResult(params, 0)
			return
		}
		state.lastCandleDelivered = true
		state.mu.Unlock()

		b, err := json.Marshal(c)
		if err != nil {
			writeError(mod, bufPtr, bufLen, fmt.Sprintf("marshal candle: %v", err))
			setResult(params, -1)
			return
		}
		if uint32(len(b)) > bufLen { //nolint:gosec // len(b) bounded by bufLen
			writeError(mod, bufPtr, bufLen, "candle JSON exceeds buffer")
			setResult(params, -1)
			return
		}
		mod.Memory().Write(bufPtr, b)
		setResult(params, int32(len(b))) //nolint:gosec // result fits in i32
	}
}

func hostSubmitOrder(state *jobState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		inPtr := uint32(params[0])  //nolint:gosec // wazero ABI: i32 param
		inLen := uint32(params[1])  //nolint:gosec // wazero ABI: i32 param
		outPtr := uint32(params[2]) //nolint:gosec // wazero ABI: i32 param
		outLen := uint32(params[3]) //nolint:gosec // wazero ABI: i32 param

		inBytes, ok := mod.Memory().Read(inPtr, inLen)
		if !ok {
			writeError(mod, outPtr, outLen, "invalid input memory")
			setResult(params, -1)
			return
		}
		var intent dorastrategy.OrderIntent
		if err := json.Unmarshal(inBytes, &intent); err != nil {
			writeError(mod, outPtr, outLen, fmt.Sprintf("decode intent: %v", err))
			setResult(params, -1)
			return
		}

		fill, err := simulateFill(state, intent)
		if err != nil {
			writeError(mod, outPtr, outLen, err.Error())
			setResult(params, -1)
			return
		}

		b, err := json.Marshal(fill)
		if err != nil {
			writeError(mod, outPtr, outLen, fmt.Sprintf("marshal fill: %v", err))
			setResult(params, -1)
			return
		}
		if uint32(len(b)) > outLen { //nolint:gosec // len(b) bounded by outLen
			writeError(mod, outPtr, outLen, "fill JSON exceeds buffer")
			setResult(params, -1)
			return
		}
		mod.Memory().Write(outPtr, b)
		setResult(params, int32(len(b))) //nolint:gosec // result fits in i32
	}
}

func hostRecordFill(state *jobState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		bufPtr := uint32(params[0]) //nolint:gosec // wazero ABI: i32 param
		bufLen := uint32(params[1]) //nolint:gosec // wazero ABI: i32 param

		b, ok := mod.Memory().Read(bufPtr, bufLen)
		if !ok {
			state.logger.Error("wasm starter: host_record_fill invalid memory", "bufPtr", bufPtr, "bufLen", bufLen)
			return
		}
		var fwFill dorastrategy.Fill
		if err := json.Unmarshal(b, &fwFill); err != nil {
			state.logger.Error("wasm starter: host_record_fill decode failed", "error", err)
			return
		}

		btFill, err := frameworkFillToBacktest(fwFill, state.currentSide)
		if err != nil {
			state.logger.Error("wasm starter: convert fill failed", "error", err)
			return
		}

		state.mu.Lock()
		state.fills = append(state.fills, btFill)
		state.mu.Unlock()
	}
}

func hostLog(state *jobState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		level := uint32(params[0])  //nolint:gosec // wazero ABI: i32 param
		bufPtr := uint32(params[1]) //nolint:gosec // wazero ABI: i32 param
		bufLen := uint32(params[2]) //nolint:gosec // wazero ABI: i32 param

		msg, ok := readString(mod, bufPtr, bufLen)
		if !ok {
			return
		}
		// host_log level values match the framework's host.Level: debug/info/warn/error.
		// Named constants keep the golangci-lint mnd rule happy.
		const (
			hostLogLevelDebug = 0
			hostLogLevelInfo  = 1
			hostLogLevelWarn  = 2
			hostLogLevelError = 3
		)
		switch level {
		case hostLogLevelDebug:
			state.logger.Debug("wasm plugin", "msg", msg)
		case hostLogLevelWarn:
			state.logger.Warn("wasm plugin", "msg", msg)
		case hostLogLevelError:
			state.logger.Error("wasm plugin", "msg", msg)
		default:
			state.logger.Info("wasm plugin", "msg", msg)
		}
	}
}

func hostBacktestError(state *jobState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		bufPtr := uint32(params[0]) //nolint:gosec // wazero ABI: i32 param
		bufLen := uint32(params[1]) //nolint:gosec // wazero ABI: i32 param

		msg, ok := readString(mod, bufPtr, bufLen)
		if !ok {
			msg = "unknown wasm error"
		}
		state.mu.Lock()
		state.errored = true
		state.errorMsg = msg
		state.mu.Unlock()
	}
}

// hostFetchCandles serves preamble candles to the plugin's
// OnPreamble, honoring the plugin's fetchReq cursor + batch_size
// (spec §4.6 step 5): each call returns the next page of
// state.preambleCandles with Done=false and a cursor until the
// slice is exhausted. The cursor is the index of the next unserved
// item, encoded as a decimal string.
// wazero executes host functions single-threaded, so no locking.
func hostFetchCandles(state *jobState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		outPtr := uint32(params[2]) //nolint:gosec // wazero ABI: i32 param
		outLen := uint32(params[3]) //nolint:gosec // wazero ABI: i32 param

		batch, err := preamblePage(state.preambleCandles, mod, params)
		if err != nil {
			writeError(mod, outPtr, outLen, err.Error())
			setResult(params, -1)
			return
		}
		writeBatchJSON(mod, params, outPtr, outLen,
			dorastrategy.CandleBatch{Items: batch.items, Done: batch.done, Cursor: batch.cursor})
	}
}

// hostFetchTrades serves preamble trades via the 4-arg fetch ABI,
// paginated by the plugin's fetchReq cursor + batch_size.
func hostFetchTrades(state *jobState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		outPtr := uint32(params[2]) //nolint:gosec // wazero ABI: i32 param
		outLen := uint32(params[3]) //nolint:gosec // wazero ABI: i32 param

		batch, err := preamblePage(state.preambleTrades, mod, params)
		if err != nil {
			writeError(mod, outPtr, outLen, err.Error())
			setResult(params, -1)
			return
		}
		writeBatchJSON(mod, params, outPtr, outLen,
			dorastrategy.TradeBatch{Items: batch.items, Done: batch.done, Cursor: batch.cursor})
	}
}

// hostFetchPrices serves preamble prices via the 4-arg fetch ABI,
// paginated by the plugin's fetchReq cursor + batch_size.
func hostFetchPrices(state *jobState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		outPtr := uint32(params[2]) //nolint:gosec // wazero ABI: i32 param
		outLen := uint32(params[3]) //nolint:gosec // wazero ABI: i32 param

		batch, err := preamblePage(state.preamblePrices, mod, params)
		if err != nil {
			writeError(mod, outPtr, outLen, err.Error())
			setResult(params, -1)
			return
		}
		writeBatchJSON(mod, params, outPtr, outLen,
			dorastrategy.PriceBatch{Items: batch.items, Done: batch.done, Cursor: batch.cursor})
	}
}

// pageOf is one cursor-driven page over a preamble slice.
type pageOf[T any] struct {
	items  []T
	done   bool
	cursor string
}

// preambleBatchSize caps a single preamble fetch call. The plugin
// walks the cursor until done; a modest batch keeps each call's JSON
// within the wazero buffer. Mirrors internal/orchestrator/livehost.go.
const preambleBatchSize = 500

// maxFetchBatchSize bounds a plugin-requested batch size so a
// runaway plugin can't OOM the agent with one giant page.
const maxFetchBatchSize = 5000

// clampFetchBatchSize bounds req.BatchSize: <=0 falls back to the
// default preamble batch; anything above the max is capped.
func clampFetchBatchSize(n int) int {
	if n <= 0 {
		return preambleBatchSize
	}
	return min(n, maxFetchBatchSize)
}

// decodeFetchReq reads the plugin's fetchReq JSON from the input
// buffer (inPtr, inLen per the 4-arg fetch ABI, spec §4.3).
func decodeFetchReq(mod api.Module, params []uint64) (host.FetchReq, error) {
	var req host.FetchReq
	buf, ok := mod.Memory().Read(uint32(params[0]), uint32(params[1])) //nolint:gosec // wazero ABI: i32 params
	if !ok {
		return req, errors.New("read fetch request buffer")
	}
	if err := json.Unmarshal(buf, &req); err != nil {
		return req, fmt.Errorf("decode fetch request: %w", err)
	}
	return req, nil
}

// preamblePage decodes the fetchReq, resolves the cursor to a slice
// index, and returns the next page of items. The cursor is the index
// of the next unserved item; empty cursor starts at 0.
func preamblePage[T any](items []T, mod api.Module, params []uint64) (pageOf[T], error) {
	var page pageOf[T]
	req, err := decodeFetchReq(mod, params)
	if err != nil {
		return page, err
	}
	start := 0
	if req.Cursor != "" {
		start, err = strconv.Atoi(req.Cursor)
		if err != nil || start < 0 || start > len(items) {
			return page, fmt.Errorf("invalid fetch cursor %q", req.Cursor)
		}
	}
	end := min(start+clampFetchBatchSize(req.BatchSize), len(items))
	page.items = items[start:end]
	page.done = end >= len(items)
	if !page.done {
		page.cursor = strconv.Itoa(end)
	}
	return page, nil
}

// writeBatchJSON marshals a fetch batch envelope and writes it to the
// plugin buffer, setting the -1 error result on failure.
func writeBatchJSON(mod api.Module, params []uint64, outPtr, outLen uint32, batch any) {
	b, err := json.Marshal(batch)
	if err != nil {
		writeError(mod, outPtr, outLen, fmt.Sprintf("marshal batch: %v", err))
		setResult(params, -1)
		return
	}
	if uint32(len(b)) > outLen { //nolint:gosec // len(b) bounded by outLen
		writeError(mod, outPtr, outLen, "batch JSON exceeds buffer")
		setResult(params, -1)
		return
	}
	mod.Memory().Write(outPtr, b)
	setResult(params, int32(len(b))) //nolint:gosec // result fits in i32
}

// hostNextEvent returns the next event from the time-ordered replay
// stream. On the internal/history path it drives off the mergeStream
// (three pageStream prefetchers interleaved by timestamp). On the
// legacy REST path (state.replay == nil) it walks the eager
// replayCandles slice, emitting only candle envelopes (trades and
// prices are unavailable on that path). It writes a tagged
// {"type":"...","data":{...}} envelope; returns 0 when exhausted,
// signalling the plugin loop to stop. The 2-arg ABI matches
// host_next_candle: outPtr, outLen → int32 bytes-written.
func hostNextEvent(state *jobState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		outPtr := uint32(params[0]) //nolint:gosec // wazero ABI: i32 param
		outLen := uint32(params[1]) //nolint:gosec // wazero ABI: i32 param

		var envelope eventEnvelope
		if state.replay == nil {
			// Legacy REST fallback: walk the pre-loaded candle slice.
			state.mu.Lock()
			if state.eventCandleIdx >= len(state.replayCandles) {
				state.mu.Unlock()
				setResult(params, 0) // done
				return
			}
			c := state.replayCandles[state.eventCandleIdx]
			state.eventCandleIdx++
			// Copy the payload so simulateFill and host_next_candle see a
			// stable row pointer that survives the closure exiting.
			cc := c
			state.lastCandle = &cc
			state.lastCandleDelivered = false
			state.mu.Unlock()
			data, err := json.Marshal(c)
			if err != nil {
				writeError(mod, outPtr, outLen, fmt.Sprintf("marshal candle: %v", err))
				setResult(params, -1)
				return
			}
			envelope = eventEnvelope{Type: "candle", Data: data}
		} else {
			ev, ok, err := state.replay.next()
			if err != nil {
				writeError(mod, outPtr, outLen, err.Error())
				setResult(params, -1)
				return
			}
			if !ok {
				setResult(params, 0) // done
				return
			}
			var err2 error
			envelope, err2 = streamEventEnvelope(state, ev)
			if err2 != nil {
				writeError(mod, outPtr, outLen, err2.Error())
				setResult(params, -1)
				return
			}
		}

		b, err := json.Marshal(envelope)
		if err != nil {
			writeError(mod, outPtr, outLen, fmt.Sprintf("marshal event: %v", err))
			setResult(params, -1)
			return
		}
		if uint32(len(b)) > outLen { //nolint:gosec // len(b) bounded by outLen
			writeError(mod, outPtr, outLen, "event JSON exceeds buffer")
			setResult(params, -1)
			return
		}
		mod.Memory().Write(outPtr, b)
		setResult(params, int32(len(b))) //nolint:gosec // result fits in i32
	}
}

// eventEnvelope is the tagged JSON written by hostNextEvent. The
// plugin's host.NextEvent decodes it into host.EventEnvelope.
type eventEnvelope struct {
	Type string          `json:"type"` // "candle" | "trade" | "price"
	Data json.RawMessage `json:"data"`
}

// streamEventEnvelope converts one mergeStream event into the tagged
// envelope. Candle events update state.lastCandle (single pointer, not
// a slice) so simulateFill and host_next_candle keep seeing the current
// candle without growing per-job memory with the window.
func streamEventEnvelope(state *jobState, ev event) (eventEnvelope, error) {
	switch ev.kind {
	case kindCandle:
		row, ok := ev.row.(candleRow)
		if !ok {
			return eventEnvelope{}, fmt.Errorf("event kind candle carried %T", ev.row)
		}
		// Copy the payload off the merge's parked page so the plugin
		// sees a stable row even after the merge advances.
		c := row.payload
		state.mu.Lock()
		state.lastCandle = &c
		state.lastCandleDelivered = false
		state.mu.Unlock()
		data, err := json.Marshal(row.payload)
		if err != nil {
			return eventEnvelope{}, fmt.Errorf("marshal candle: %w", err)
		}
		return eventEnvelope{Type: "candle", Data: data}, nil
	case kindTrade:
		row, ok := ev.row.(tradeRow)
		if !ok {
			return eventEnvelope{}, fmt.Errorf("event kind trade carried %T", ev.row)
		}
		data, err := json.Marshal(row.payload)
		if err != nil {
			return eventEnvelope{}, fmt.Errorf("marshal trade: %w", err)
		}
		return eventEnvelope{Type: "trade", Data: data}, nil
	case kindPrice:
		row, ok := ev.row.(priceRow)
		if !ok {
			return eventEnvelope{}, fmt.Errorf("event kind price carried %T", ev.row)
		}
		data, err := json.Marshal(row.payload)
		if err != nil {
			return eventEnvelope{}, fmt.Errorf("marshal price: %w", err)
		}
		return eventEnvelope{Type: "price", Data: data}, nil
	}
	return eventEnvelope{}, fmt.Errorf("unknown event kind %d", ev.kind)
}

// simulateFill computes the simulated fill for an order intent against the
// last candle delivered by host_next_candle.
func simulateFill(state *jobState, intent dorastrategy.OrderIntent) (dorastrategy.Fill, error) {
	state.mu.Lock()
	if state.lastCandle == nil {
		state.mu.Unlock()
		return dorastrategy.Fill{}, errors.New("no current candle for fill simulation")
	}
	candle := *state.lastCandle
	state.currentSide = intent.Side
	state.fillSeq++
	seq := state.fillSeq
	state.mu.Unlock()

	var price string
	switch intent.Type {
	case "market":
		price = candle.Close
	case "limit":
		within, err := limitWithinRange(intent.Price, candle.Low, candle.High)
		if err != nil {
			return dorastrategy.Fill{}, err
		}
		if !within {
			return dorastrategy.Fill{}, fmt.Errorf("limit price %s not within candle range %s-%s", intent.Price, candle.Low, candle.High)
		}
		price = intent.Price
	default:
		return dorastrategy.Fill{}, fmt.Errorf("unsupported order type %q", intent.Type)
	}

	return dorastrategy.Fill{
		OrderID:   fmt.Sprintf("sim-%04d", seq),
		Price:     price,
		Quantity:  intent.Quantity,
		Simulated: true,
	}, nil
}

func limitWithinRange(price, low, high string) (bool, error) {
	p, ok1 := new(big.Rat).SetString(price)
	l, ok2 := new(big.Rat).SetString(low)
	h, ok3 := new(big.Rat).SetString(high)
	if !ok1 || !ok2 || !ok3 {
		return false, errors.New("non-numeric price/low/high")
	}
	return p.Cmp(l) >= 0 && p.Cmp(h) <= 0, nil
}

func frameworkFillToBacktest(fw dorastrategy.Fill, side string) (Fill, error) {
	price, err := strconv.ParseFloat(fw.Price, 64)
	if err != nil {
		return Fill{}, fmt.Errorf("parse fill price: %w", err)
	}
	qty, err := strconv.ParseFloat(fw.Quantity, 64)
	if err != nil {
		return Fill{}, fmt.Errorf("parse fill quantity: %w", err)
	}
	now := time.Now().UTC()
	return Fill{
		Timestamp:   now,
		Side:        side,
		Quantity:    qty,
		Price:       price,
		OrderID:     fw.OrderID,
		SimulatedAt: now,
	}, nil
}

// errLenPrefixBytes is the 4-byte little-endian length prefix the
// writeError helper writes ahead of the error JSON payload.
const errLenPrefixBytes = 4

func writeError(mod api.Module, bufPtr, bufLen uint32, msg string) {
	errJSON := []byte(fmt.Sprintf(`{"error":%q}`, msg))
	total := uint32(errLenPrefixBytes + len(errJSON)) //nolint:gosec // length-prefix fits in u32
	if total > bufLen {
		// Buffer too small even for the error payload; nothing useful we can do.
		return
	}
	buf := make([]byte, total)
	binary.LittleEndian.PutUint32(buf, uint32(len(errJSON))) //nolint:gosec // length-prefix fits in u32
	copy(buf[errLenPrefixBytes:], errJSON)
	mod.Memory().Write(bufPtr, buf)
}

func readString(mod api.Module, ptr, length uint32) (string, bool) {
	b, ok := mod.Memory().Read(ptr, length)
	if !ok {
		return "", false
	}
	return string(b), true
}

func setResult(params []uint64, v int32) {
	// wazero uses the low 32 bits for i32 results.
	params[0] = uint64(uint32(v)) //nolint:gosec // wazero ABI: i32 result
}
