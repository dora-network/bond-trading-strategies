package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"

	dorastrategy "github.com/dora-network/dora-strategy-wasm/dorastrategy"
	"github.com/dora-network/dora-strategy-wasm/dorastrategy/host"

	"github.com/dora-network/bond-trading-strategies/internal/agent/audit"
	"github.com/dora-network/bond-trading-strategies/internal/agent/deployment"
	"github.com/dora-network/bond-trading-strategies/internal/agent/orderbroker"
	"github.com/dora-network/bond-trading-strategies/internal/agent/safety"
	history "github.com/dora-network/bond-trading-strategies/internal/agent/store"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wsbroker"
)

// liveHostState is the per-deployment mutable state captured by the
// host module closures. One per running deployment.
// The frame channel is fed by the orchestrator goroutine that reads
// from the wsbroker subscription. host_next_live_candle blocks on
// it; closing the channel (via wsbroker.Unsubscribe) causes the
// host function to return 0 (done) and the plugin's live loop to
// exit cleanly.

type liveHostState struct {
	userID       string
	strategyID   string
	deployment   string
	orderBook    string
	apiKey       string
	candleCh     <-chan wsbroker.Frame
	cfgJSON      []byte
	kernel       *safety.Kernel
	orders       *orderbroker.Broker
	audit        auditHook
	logger       *slog.Logger
	candleMu     sync.Mutex
	candleCount  int64
	lastCandleAt time.Time
	lastCandle   string // JSON of the most recent candle delivered
	// Decision tracking — updated on every host_submit_order call.
	lastDecisionAt time.Time
	lastDecision   string // JSON of the most recent order intent
	buyCount       int64
	sellCount      int64
	// Persistence plumbing for the candle count. The livehost
	// flushes state.candleCount to the deployments row on a coarse
	// cadence (candlePersistInterval candles OR 30s, whichever
	// first) so Stats() reports the count since the strategy was
	// first deployed, not since the current goroutine started.
	// See persistCandleCount in lifecycle.go.
	deployStore           deployment.Store
	candlePersistInterval int64
	lastCandlePersist     time.Time

	// Framework v3 event-loop plumbing. The three channels carry
	// wsbroker candle/trade/price frames; hostNextEvent selects across
	// them and writes a tagged envelope. history serves preamble
	// fetches (hostFetchCandles/Trades/Prices); assetID keys price
	// fetches + subscriptions. preambleInProgress gates the fetch
	// functions and the order-submit ban — true during OnPreamble,
	// flipped false by the first hostNextEvent call (the framework's
	// transition from preamble to the event loop).
	tradeCh            <-chan wsbroker.Frame
	priceCh            <-chan wsbroker.Frame
	history            history.HistoryFetcher
	assetID            string
	resolution         string
	warmupCandles      int
	preambleMu         sync.Mutex
	preambleInProgress bool
}

// candlePreviewLen caps the candle JSON shown in info-level logs.
const candlePreviewLen = 200

// auditHook is the audit-log surface the live host needs. The
// concrete sink is supplied by main.go; tests substitute a
// recording function. Mirrors internal/wasmruntime/hostimpl's
// AuditHook, kept here so the orchestrator doesn't depend on
// hostimpl.
type auditHook interface {
	Record(ctx context.Context, action, detail string) error
}

type noopAudit struct{}

func (noopAudit) Record(_ context.Context, _, _ string) error { return nil }

// sinkAudit wires an auditHook to a closure-shaped audit.Insert
// caller. main.go builds one of these per deployment when the
// agent has a pool.
type sinkAudit struct {
	insert func(ctx context.Context, doraUserID, action string, detail []byte) error
	userID string
}

func (s sinkAudit) Record(ctx context.Context, action, detail string) error {
	return s.insert(ctx, s.userID, action, []byte(detail))
}

// buildLiveHostModule builds the wazero host module for a live
// deployment. The closures capture the liveHostState, providing the
// channel-backed host_next_event (select across candle/trade/price
// frames), the preamble fetch functions (hostFetchCandles/Trades/
// Prices), and real host_submit_order/host_cancel_order wired to the
// safety kernel and the order broker. Same wazero NewHostModuleBuilder
// pattern as internal/backtest/wasm_starter.go's newBacktestHostModule.
func buildLiveHostModule(ctx context.Context, rt wazero.Runtime, state *liveHostState) (api.Module, error) {
	hm := registry.BuildHostModule(rt, func(name string) api.GoModuleFunc {
		switch name {
		case "host_get_config":
			return hostGetConfig(state)
		case "host_next_event":
			return hostNextEvent(state)
		case "host_fetch_candles":
			return hostFetchCandles(state)
		case "host_fetch_trades":
			return hostFetchTrades(state)
		case "host_fetch_prices":
			return hostFetchPrices(state)
		case "host_submit_order":
			return hostSubmitOrder(state)
		case "host_cancel_order":
			return hostCancelOrder(state)
		case "host_log":
			return hostLog(state)
		case "host_now":
			return hostNow(state)
		case "host_random":
			return hostRandom(state)
		default:
			return nil // stub (host_next_candle, host_record_fill, etc.)
		}
	})
	return hm.Instantiate(ctx)
}

func hostGetConfig(state *liveHostState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		bufPtr := uint32(params[0]) //nolint:gosec // wazero ABI: i32 param
		bufLen := uint32(params[1]) //nolint:gosec // wazero ABI: i32 param

		if uint32(len(state.cfgJSON)) > bufLen { //nolint:gosec // len fits in u32
			writeError(mod, bufPtr, bufLen, "config JSON exceeds buffer")
			setResult(params, -1)
			return
		}
		mod.Memory().Write(bufPtr, state.cfgJSON)
		setResult(params, int32(len(state.cfgJSON))) //nolint:gosec // result fits in i32
	}
}

// hostNextEvent is the framework-v3 event-loop host function. It
// selects across the candle, trade, and price channels, writes a
// tagged {"type":"...","data":{...}} envelope to the plugin buffer,
// and returns the byte count. Returns 0 (done) when every channel
// has closed (Unsubscribe).
//
// The first call marks the preamble phase complete: the framework's
// Run dispatch calls OnPreamble (which drives hostFetchCandles/Trades/
// Prices) and then enters the host_next_event loop. That first event
// request is the host's signal that the preamble is over — so the
// order-submit ban lifts and the fetch functions stop serving.
func hostNextEvent(state *liveHostState) api.GoModuleFunc {
	return func(ctx context.Context, mod api.Module, params []uint64) {
		bufPtr := uint32(params[0]) //nolint:gosec // wazero ABI: i32 param
		bufLen := uint32(params[1]) //nolint:gosec // wazero ABI: i32 param

		// First event request ends the preamble. This is the only
		// host-side signal that OnPreamble returned; the fetch
		// functions and the order ban read this flag.
		state.preambleMu.Lock()
		state.preambleInProgress = false
		state.preambleMu.Unlock()

		ev, ok := nextEvent(ctx, state)
		if !ok {
			// All channels closed or context cancelled: done.
			setResult(params, 0)
			return
		}
		b, err := json.Marshal(ev)
		if err != nil {
			writeError(mod, bufPtr, bufLen, fmt.Sprintf("marshal event: %v", err))
			setResult(params, -1)
			return
		}
		if uint32(len(b)) > bufLen { //nolint:gosec // bounded by bufLen
			writeError(mod, bufPtr, bufLen, "event JSON exceeds buffer")
			setResult(params, -1)
			return
		}
		mod.Memory().Write(bufPtr, b)
		setResult(params, int32(len(b))) //nolint:gosec // result fits in i32

		if ev.Type == "candle" {
			state.recordCandleDelivery(ev.Data)
		}
	}
}

// liveEvent is the tagged envelope written by hostNextEvent, mirroring
// internal/backtest/wasm_starter.go's eventEnvelope.
type liveEvent struct {
	Type string          `json:"type"` // "candle" | "trade" | "price"
	Data json.RawMessage `json:"data"`
}

// nextEvent selects across the three channels until one yields a
// usable frame (or all close). Extracted from hostNextEvent so tests
// can exercise the routing without a wazero runtime. Returns the
// tagged envelope plus ok=false when every channel is closed or the
// context is cancelled.
func nextEvent(ctx context.Context, state *liveHostState) (liveEvent, bool) {
	// If no channels are live (never subscribed, or all closed),
	// there is nothing to wait on — return done immediately rather
	// than blocking forever on ctx.Done().
	if allClosed(state) {
		return liveEvent{}, false
	}
	for {
		// nil channels are excluded from the select (a nil channel
		// in a case blocks forever). When no subscription exists for
		// a frame type, that case simply never fires.
		select {
		case <-ctx.Done():
			return liveEvent{}, false
		case f, open := <-state.candleCh:
			if !open {
				state.candleCh = nil
				if allClosed(state) {
					return liveEvent{}, false
				}
				continue
			}
			if f.Type != "candle" {
				continue
			}
			b, err := frameCandleJSON(f)
			if err != nil {
				continue
			}
			return liveEvent{Type: "candle", Data: b}, true
		case f, open := <-state.tradeCh:
			if !open {
				state.tradeCh = nil
				if allClosed(state) {
					return liveEvent{}, false
				}
				continue
			}
			b, err := frameEventJSON(f)
			if err != nil {
				continue
			}
			return liveEvent{Type: "trade", Data: b}, true
		case f, open := <-state.priceCh:
			if !open {
				state.priceCh = nil
				if allClosed(state) {
					return liveEvent{}, false
				}
				continue
			}
			b, err := frameEventJSON(f)
			if err != nil {
				continue
			}
			return liveEvent{Type: "price", Data: b}, true
		}
	}
}

// allClosed reports whether every event channel has been closed (or
// was never subscribed). Once all are closed, hostNextEvent returns
// done so the plugin's live loop exits cleanly.
func allClosed(state *liveHostState) bool {
	return state.candleCh == nil && state.tradeCh == nil && state.priceCh == nil
}

// frameEventJSON unwraps a trade/price frame's {"data":...} envelope,
// falling back to the raw JSON (same shape as frameCandleJSON).
func frameEventJSON(f wsbroker.Frame) ([]byte, error) {
	var wrapped struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(f.Raw, &wrapped); err == nil && len(wrapped.Data) > 0 && string(wrapped.Data) != "null" {
		return []byte(wrapped.Data), nil
	}
	if !json.Valid(f.Raw) {
		return nil, errors.New("frame raw is not valid JSON")
	}
	return []byte(f.Raw), nil
}

// recordCandleDelivery bumps the candle counter, logs, and throttles
// the deployments-row persist. Lifted out of the old hostNextLiveCandle
// so hostNextEvent can call it only for candle events.
func (state *liveHostState) recordCandleDelivery(b []byte) {
	state.candleMu.Lock()
	state.candleCount++
	state.lastCandleAt = time.Now()
	state.lastCandle = string(b)
	count := state.candleCount
	state.candleMu.Unlock()

	// Throttle persistence: flush the count to the deployments row
	// on a coarse cadence so a process restart doesn't lose more
	// than ~30s of candle history. The throttle fires when EITHER
	// the per-deploy count hits a hard floor (the candle-persist
	// interval) OR 30s has elapsed since the last flush.
	if state.deployStore != nil && state.candlePersistInterval > 0 {
		pump := count%state.candlePersistInterval == 0 ||
			(!state.lastCandlePersist.IsZero() && time.Since(state.lastCandlePersist) >= 30*time.Second)
		if pump {
			state.lastCandlePersist = time.Now()
			go func(state *liveHostState, n int64) {
				bg := context.Background()
				if err := state.deployStore.SetCandleCount(bg, state.deployment, state.userID, n); err != nil && state.logger != nil {
					state.logger.Debug("wasm plugin: persist candle count failed",
						"deployment", state.deployment, "error", err)
				}
			}(state, count)
		}
	}
	// Log every candle at debug; milestone every 100 at info.
	if state.logger != nil {
		if count%100 == 1 {
			state.logger.Info(
				"wasm plugin: candle delivered",
				"candle_preview", truncateStr(string(b), candlePreviewLen),
				"count", count,
				"order_book", state.orderBook,
			)
		} else {
			state.logger.Debug(
				"wasm plugin: candle delivered",
				"deployment", state.deployment,
				"count", count,
			)
		}
	}
}

// preambleActive reports whether the plugin is still in the OnPreamble
// phase. Guarded by preambleMu because hostNextEvent (writer) and the
// fetch/order functions (readers) all run on the wazero call stack;
// the mutex keeps the transition race-free under any future scheduler.
func (state *liveHostState) preambleActive() bool {
	state.preambleMu.Lock()
	defer state.preambleMu.Unlock()
	return state.preambleInProgress
}

// hostErrPreambleDone is the error string the fetch functions return
// when called outside the preamble. The plugin should only call them
// from OnPreamble; a call after the preamble is a framework contract
// violation.
const hostErrPreambleDone = "preamble_done"

// hostErrPreambleOrdersForbidden is the error string the order
// functions return when called during the preamble. OnPreamble is
// read-only; the host bans host_submit_order / host_cancel_order
// until the first host_next_event call ends the preamble.
const hostErrPreambleOrdersForbidden = "preamble_no_orders"

// fetchWindow computes the preamble fetch window from the deployment's
// resolution + warmup-candle count. Returns (zero, zero) when the
// window can't be computed (no resolution or no warmup), signalling
// the fetch function to return an empty done batch.
func (state *liveHostState) fetchWindow(now time.Time) (time.Time, time.Time) {
	d := resolutionDuration(state.resolution)
	if d == 0 || state.warmupCandles <= 0 {
		return time.Time{}, now
	}
	return now.Add(-time.Duration(state.warmupCandles) * d), now
}

// hostFetchCandles serves preamble candles via the 4-arg fetch ABI
// (inPtr, inLen, outPtr, outLen → i32 bytes-written). Only active
// during the preamble; once the event loop starts (hostNextEvent),
// calls return preamble_done. The history store does the bucketing
// at the deployment's resolution — no client-side aggregation.
func hostFetchCandles(state *liveHostState) api.GoModuleFunc {
	return func(ctx context.Context, mod api.Module, params []uint64) {
		outPtr := uint32(params[2]) //nolint:gosec // wazero ABI: i32 param
		outLen := uint32(params[3]) //nolint:gosec // wazero ABI: i32 param
		writeFetchBatch(mod, params, outPtr, outLen, func() (any, error) {
			if !state.preambleActive() {
				return nil, errors.New(hostErrPreambleDone)
			}
			if state.history == nil {
				return dorastrategy.CandleBatch{Done: true}, nil
			}
			if state.warmupCandles <= 0 {
				return dorastrategy.CandleBatch{Done: true}, nil
			}
			req, err := decodeFetchReq(mod, params)
			if err != nil {
				return nil, err
			}
			start, end := state.fetchWindow(time.Now())
			rows, cursor, err := state.history.FetchCandles(ctx, state.orderBook,
				start, end, state.resolution, req.Cursor, clampBatchSize(req.BatchSize))
			if err != nil {
				return nil, fmt.Errorf("history fetch candles: %w", err)
			}
			return dorastrategy.CandleBatch{Items: historyCandles(rows), Done: cursor == "", Cursor: cursor}, nil
		})
	}
}

// hostFetchTrades serves preamble trades via the 4-arg fetch ABI.
func hostFetchTrades(state *liveHostState) api.GoModuleFunc {
	return func(ctx context.Context, mod api.Module, params []uint64) {
		outPtr := uint32(params[2]) //nolint:gosec // wazero ABI: i32 param
		outLen := uint32(params[3]) //nolint:gosec // wazero ABI: i32 param
		writeFetchBatch(mod, params, outPtr, outLen, func() (any, error) {
			if !state.preambleActive() {
				return nil, errors.New(hostErrPreambleDone)
			}
			if state.history == nil {
				return dorastrategy.TradeBatch{Done: true}, nil
			}
			if state.warmupCandles <= 0 {
				return dorastrategy.TradeBatch{Done: true}, nil
			}
			req, err := decodeFetchReq(mod, params)
			if err != nil {
				return nil, err
			}
			start, end := state.fetchWindow(time.Now())
			rows, cursor, err := state.history.FetchTrades(ctx, state.orderBook, start, end, req.Cursor, clampBatchSize(req.BatchSize))
			if err != nil {
				return nil, fmt.Errorf("history fetch trades: %w", err)
			}
			return dorastrategy.TradeBatch{Items: historyTrades(rows), Done: cursor == "", Cursor: cursor}, nil
		})
	}
}

// hostFetchPrices serves preamble prices via the 4-arg fetch ABI.
func hostFetchPrices(state *liveHostState) api.GoModuleFunc {
	return func(ctx context.Context, mod api.Module, params []uint64) {
		outPtr := uint32(params[2]) //nolint:gosec // wazero ABI: i32 param
		outLen := uint32(params[3]) //nolint:gosec // wazero ABI: i32 param
		writeFetchBatch(mod, params, outPtr, outLen, func() (any, error) {
			if !state.preambleActive() {
				return nil, errors.New(hostErrPreambleDone)
			}
			if state.history == nil {
				return dorastrategy.PriceBatch{Done: true}, nil
			}
			if state.warmupCandles <= 0 {
				return dorastrategy.PriceBatch{Done: true}, nil
			}
			req, err := decodeFetchReq(mod, params)
			if err != nil {
				return nil, err
			}
			start, end := state.fetchWindow(time.Now())
			rows, cursor, err := state.history.FetchPrices(ctx, state.assetID, start, end, req.Cursor, clampBatchSize(req.BatchSize))
			if err != nil {
				return nil, fmt.Errorf("history fetch prices: %w", err)
			}
			return dorastrategy.PriceBatch{Items: historyPrices(rows), Done: cursor == "", Cursor: cursor}, nil
		})
	}
}

// preambleBatchSize caps a single preamble fetch call. The plugin
// walks the cursor until done; a modest batch keeps each call's JSON
// within the wazero buffer.
const preambleBatchSize = 500

// maxFetchBatchSize bounds a plugin-requested batch size so a
// runaway plugin can't OOM the agent with one giant page.
const maxFetchBatchSize = 5000

// clampBatchSize bounds req.BatchSize: <=0 falls back to the
// default preamble batch; anything above the max is capped.
func clampBatchSize(n int) int {
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

// writeFetchBatch runs the fetch closure and writes the resulting
// batch JSON (CandleBatch/TradeBatch/PriceBatch) to the plugin buffer.
// On error, writes the {"error":...} payload and sets -1.
func writeFetchBatch(mod api.Module, params []uint64, outPtr, outLen uint32, fetch func() (any, error)) {
	batch, err := fetch()
	if err != nil {
		writeError(mod, outPtr, outLen, err.Error())
		setResult(params, -1)
		return
	}
	b, err := json.Marshal(batch)
	if err != nil {
		writeError(mod, outPtr, outLen, fmt.Sprintf("marshal fetch batch: %v", err))
		setResult(params, -1)
		return
	}
	if uint32(len(b)) > outLen { //nolint:gosec // bounded by outLen
		writeError(mod, outPtr, outLen, "fetch batch JSON exceeds buffer")
		setResult(params, -1)
		return
	}
	mod.Memory().Write(outPtr, b)
	setResult(params, int32(len(b))) //nolint:gosec // result fits in i32
}

// historyCandles converts internal/history.Candle rows to the
// framework's dorastrategy.Candle JSON shape the plugin expects.
func historyCandles(rows []history.Candle) []dorastrategy.Candle {
	out := make([]dorastrategy.Candle, len(rows))
	for i, r := range rows {
		out[i] = dorastrategy.Candle{
			OrderBookID:    r.OrderBookID,
			StartTimestamp: r.StartTimestamp.Format(time.RFC3339Nano),
			Open:           r.Open,
			High:           r.High,
			Low:            r.Low,
			Close:          r.Close,
			OpenYtm:        r.OpenYtm,
			HighYtm:        r.HighYtm,
			LowYtm:         r.LowYtm,
			CloseYtm:       r.CloseYtm,
			Volume:         r.Volume,
		}
	}
	return out
}

// historyTrades converts internal/history.Trade rows to the
// framework's dorastrategy.Trade JSON shape.
func historyTrades(rows []history.Trade) []dorastrategy.Trade {
	out := make([]dorastrategy.Trade, len(rows))
	for i, r := range rows {
		out[i] = dorastrategy.Trade{
			TransactionID:      r.TransactionID,
			OrderBookID:        r.OrderBookID,
			OrderID:            r.OrderID,
			OrderSeq:           r.OrderSeq,
			UserID:             r.UserID,
			Asset0:             r.Asset0,
			Price:              r.Price,
			Quantity0:          r.Quantity0,
			Side:               r.Side,
			AggressorIndicator: r.AggressorIndicator,
			CreatedAt:          r.CreatedAt.Format(time.RFC3339Nano),
		}
	}
	return out
}

// historyPrices converts internal/history.Price rows to the
// framework's dorastrategy.Price JSON shape.
func historyPrices(rows []history.Price) []dorastrategy.Price {
	out := make([]dorastrategy.Price, len(rows))
	for i, r := range rows {
		out[i] = dorastrategy.Price{
			AssetID: r.AssetID,
			Price:   r.Price,
			YTM:     r.YTM,
			Time:    r.Timestamp.Format(time.RFC3339Nano),
		}
	}
	return out
}

// normalizeResolution converts a Dora resolution shorthand into a
// Go time.ParseDuration string. Supports 1m, 5m, 1h, 4h, 1d, 1w.
func normalizeResolution(res string) string {
	if res == "" {
		return ""
	}
	const hoursPerDay, hoursPerWeek = 24, 168
	last := res[len(res)-1]
	num := res[:len(res)-1]
	switch last {
	case 'd':
		n, err := strconv.Atoi(num)
		if err != nil {
			return res
		}
		return strconv.Itoa(n*hoursPerDay) + "h"
	case 'w':
		n, err := strconv.Atoi(num)
		if err != nil {
			return res
		}
		return strconv.Itoa(n*hoursPerWeek) + "h"
	default:
		return res
	}
}

// resolutionDuration maps a resolution string ("1m", "5m", "1h" …) to
// a time.Duration. Returns 0 for unknown / empty resolutions.
func resolutionDuration(res string) time.Duration {
	d, err := time.ParseDuration(normalizeResolution(res))
	if err != nil {
		return 0
	}
	return d
}

// frameCandleJSON returns the JSON the plugin will receive. Dora
// frames are typically wrapped payloads — {"type":"candle",
// "data":{...}} — and the plugin wants the candle struct
// directly. Try the wrapped form first; if that fails, pass the
// raw frame through (covers brokers that publish the bare
// candle).
func frameCandleJSON(f wsbroker.Frame) ([]byte, error) {
	var wrapped struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(f.Raw, &wrapped); err == nil && len(wrapped.Data) > 0 && string(wrapped.Data) != "null" {
		return []byte(wrapped.Data), nil
	}
	if !json.Valid(f.Raw) {
		return nil, errors.New("frame raw is not valid JSON")
	}
	return []byte(f.Raw), nil
}

func hostSubmitOrder(state *liveHostState) api.GoModuleFunc {
	return func(ctx context.Context, mod api.Module, params []uint64) {
		inPtr := uint32(params[0])  //nolint:gosec // wazero ABI: i32 param
		inLen := uint32(params[1])  //nolint:gosec // wazero ABI: i32 param
		outPtr := uint32(params[2]) //nolint:gosec // wazero ABI: i32 param
		outLen := uint32(params[3]) //nolint:gosec // wazero ABI: i32 param

		fill, err := submitOrderLive(ctx, state, mod, inPtr, inLen)
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
		if uint32(len(b)) > outLen { //nolint:gosec // bounded by outLen
			writeError(mod, outPtr, outLen, "fill JSON exceeds buffer")
			setResult(params, -1)
			return
		}
		mod.Memory().Write(outPtr, b)
		setResult(params, int32(len(b))) //nolint:gosec // result fits in i32
	}
}

// submitOrderLive reads an OrderIntent from the plugin buffer,
// runs it through the safety kernel, and (if allowed) submits via
// the order broker. Returns the fill on success or an error
// describing the denial. Extracted so tests can exercise the
// flow without a wazero Module.
func submitOrderLive(ctx context.Context, state *liveHostState, mod api.Module, inPtr, inLen uint32) (dorastrategy.Fill, error) {
	inBytes, ok := mod.Memory().Read(inPtr, inLen)
	if !ok {
		return dorastrategy.Fill{}, errors.New("invalid input memory")
	}
	var intent dorastrategy.OrderIntent
	if err := json.Unmarshal(inBytes, &intent); err != nil {
		return dorastrategy.Fill{}, fmt.Errorf("decode intent: %w", err)
	}

	// Preamble order ban: the framework's OnPreamble is read-only.
	// A plugin that tries to submit during the preamble gets a
	// structured denial + an order.denied audit entry so the
	// operator can see the attempted early trade. The ban is
	// symmetric with host_cancel_order (no order exists to cancel
	// yet, but the rule is uniform).
	if state.preambleActive() {
		_ = state.audit.Record(ctx, string(audit.ActionOrderDenied),
			fmt.Sprintf(`{"reason":%q,"intent":%s}`, hostErrPreambleOrdersForbidden, string(inBytes)))
		return dorastrategy.Fill{}, errors.New(hostErrPreambleOrdersForbidden)
	}

	// Safety check first; denials are reported before any Dora call.
	openOrders, _ := state.kernel.OrdersInLastMinute(ctx, state.userID)
	allowed, reason, err := state.kernel.CheckOrder(ctx, state.userID, safety.OrderCheck{
		Quantity:   parseDecimal(intent.Quantity),
		Price:      parseDecimal(intent.Price),
		OpenOrders: openOrders,
	})
	if err != nil {
		_ = state.audit.Record(ctx, string(audit.ActionPluginValidateFailed), fmt.Sprintf(`{"reason":%q}`, err.Error()))
		return dorastrategy.Fill{}, fmt.Errorf("safety: %w", err)
	}
	if !allowed {
		_ = state.audit.Record(ctx, string(audit.ActionOrderDenied), fmt.Sprintf(`{"reason":%q,"intent":%s}`, reason, string(inBytes)))
		return dorastrategy.Fill{}, fmt.Errorf("safety: %s", reason)
	}

	if state.orders == nil {
		return dorastrategy.Fill{}, errors.New("no order broker wired")
	}
	res, err := state.orders.SubmitOrder(ctx, state.userID, state.strategyID, state.apiKey, orderbroker.Intent{
		OrderBookID:        state.orderBook,
		Side:               intent.Side,
		Quantity:           intent.Quantity,
		Type:               intent.Type,
		Price:              intent.Price,
		InverseLeverage:    intent.InverseLeverage,
		FromGlobalPosition: intent.FromGlobalPosition,
	})
	if err != nil {
		_ = state.audit.Record(ctx, string(audit.ActionOrderDenied), fmt.Sprintf(`{"reason":%q,"intent":%s}`, err.Error(), string(inBytes)))
		return dorastrategy.Fill{}, fmt.Errorf("broker: %w", err)
	}
	_ = state.audit.Record(
		ctx, string(audit.ActionOrderSubmitted),
		fmt.Sprintf(`{"order_id":%q,"client_order_id":%q}`, res.OrderID, res.ClientOrderID),
	)

	// Record the strategy's decision for operational visibility.
	state.candleMu.Lock()
	state.lastDecisionAt = time.Now()
	state.lastDecision = string(inBytes)
	if strings.EqualFold(intent.Side, "buy") {
		state.buyCount++
	} else if strings.EqualFold(intent.Side, "sell") {
		state.sellCount++
	}
	buys, sells := state.buyCount, state.sellCount
	state.candleMu.Unlock()

	if state.logger != nil {
		state.logger.Info(
			"wasm plugin: order submitted",
			"deployment", state.deployment,
			"side", intent.Side,
			"quantity", intent.Quantity,
			"type", intent.Type,
			"order_id", res.OrderID,
			"buys", buys,
			"sells", sells,
		)
	}

	return dorastrategy.Fill{
		OrderID:   res.OrderID,
		Price:     intent.Price,
		Quantity:  intent.Quantity,
		Simulated: false,
	}, nil
}

func hostCancelOrder(state *liveHostState) api.GoModuleFunc {
	return func(ctx context.Context, mod api.Module, params []uint64) {
		inPtr := uint32(params[0])  //nolint:gosec // wazero ABI: i32 param
		inLen := uint32(params[1])  //nolint:gosec // wazero ABI: i32 param
		outPtr := uint32(params[2]) //nolint:gosec // wazero ABI: i32 param
		outLen := uint32(params[3]) //nolint:gosec // wazero ABI: i32 param

		orderID, ok := readString(mod, inPtr, inLen)
		if !ok {
			writeError(mod, outPtr, outLen, "invalid input memory")
			setResult(params, -1)
			return
		}
		// Symmetric with the submit ban: no order exists to cancel
		// during the preamble, but the rule is uniform.
		if state.preambleActive() {
			writeError(mod, outPtr, outLen, hostErrPreambleOrdersForbidden)
			setResult(params, -1)
			return
		}
		if state.orders == nil {
			writeError(mod, outPtr, outLen, "no order broker wired")
			setResult(params, -1)
			return
		}
		if err := state.orders.CancelOrder(ctx, state.userID, state.apiKey, orderID); err != nil {
			writeError(mod, outPtr, outLen, err.Error())
			setResult(params, -1)
			return
		}
		_ = state.audit.Record(ctx, "order.cancelled", fmt.Sprintf(`{"order_id":%q}`, orderID))
		setResult(params, 0)
	}
}

func hostLog(state *liveHostState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		level := uint32(params[0])  //nolint:gosec // wazero ABI: i32 param
		bufPtr := uint32(params[1]) //nolint:gosec // wazero ABI: i32 param
		bufLen := uint32(params[2]) //nolint:gosec // wazero ABI: i32 param

		msg, ok := readString(mod, bufPtr, bufLen)
		if !ok {
			return
		}
		// host_log level values match the framework's host.Level.
		// Named constants keep the golangci-lint mnd rule happy.
		const (
			hostLogLevelDebug = 0
			hostLogLevelInfo  = 1
			hostLogLevelWarn  = 2
			hostLogLevelError = 3
		)
		switch level {
		case hostLogLevelDebug:
			state.logger.Debug("wasm plugin", "deployment", state.deployment, "msg", msg)
		case hostLogLevelWarn:
			state.logger.Warn("wasm plugin", "deployment", state.deployment, "msg", msg)
		case hostLogLevelError:
			state.logger.Error("wasm plugin", "deployment", state.deployment, "msg", msg)
		default:
			state.logger.Info("wasm plugin", "deployment", state.deployment, "msg", msg)
		}
	}
}

// hostNow returns Unix-millis. The framework wants an int64
// (unix-millis overflows int32 in ~24 days); the spec lists
// host_now as required and the hostimpl.Host.Now also returns
// time.Time, so a real plugin would marshal the value via a
// two-int32 split. We follow the i64 result convention here.
func hostNow(_ *liveHostState) api.GoModuleFunc {
	return func(_ context.Context, _ api.Module, params []uint64) {
		params[0] = uint64(time.Now().UnixMilli())
	}
}

func hostRandom(_ *liveHostState) api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		bufPtr := uint32(params[0]) //nolint:gosec // wazero ABI: i32 param
		bufLen := uint32(params[1]) //nolint:gosec // wazero ABI: i32 param

		if bufLen == 0 {
			setResult(params, 0)
			return
		}
		buf := make([]byte, bufLen)
		if _, err := rand.Read(buf); err != nil {
			writeError(mod, bufPtr, bufLen, "rand read failed")
			setResult(params, -1)
			return
		}
		mod.Memory().Write(bufPtr, buf)
		setResult(params, int32(bufLen)) //nolint:gosec // result fits in i32
	}
}

// readString reads a (ptr, length) range from the module's memory
// and returns it as a string. Returns ok=false on bad ranges.
// Same helper signature as internal/backtest/wasm_starter.go.
func readString(mod api.Module, ptr, length uint32) (string, bool) {
	b, ok := mod.Memory().Read(ptr, length)
	if !ok {
		return "", false
	}
	return string(b), true
}

// writeError writes a length-prefixed {"error": "..."} payload to
// the module's memory. Same protocol as the backtest host module —
// the plugin reads the 4-byte length, then that many bytes of
// JSON. Returns silently if the buffer is too small for the
// error; there is nothing useful the plugin can do in that case.
func writeError(mod api.Module, bufPtr, bufLen uint32, msg string) {
	errJSON := []byte(fmt.Sprintf(`{"error":%q}`, msg))
	const lenPrefix = 4
	total := uint32(lenPrefix + len(errJSON)) //nolint:gosec // length-prefix fits in u32
	if total > bufLen {
		return
	}
	buf := make([]byte, total)
	binary.LittleEndian.PutUint32(buf, uint32(len(errJSON))) //nolint:gosec // length-prefix fits in u32
	copy(buf[lenPrefix:], errJSON)
	mod.Memory().Write(bufPtr, buf)
}

// setResult writes a single i32 result back into the wazero
// params slice. wazero's ABI uses the low 32 bits of params[0]
// for the return value.
func setResult(params []uint64, v int32) {
	// wazero uses the low 32 bits for i32 results.
	params[0] = uint64(uint32(v)) //nolint:gosec // wazero ABI: i32 result
}

// parseDecimal is a best-effort float64 parse for safety notional
// math. Returns 0 on failure; the safety kernel's caps comparison
// treats 0 as the safe default.
func parseDecimal(s string) float64 {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return 0
	}
	f, _ := r.Float64()
	return f
}

// truncateStr caps s at maxLen characters, appending "…" if truncated.
func truncateStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "…"
}
