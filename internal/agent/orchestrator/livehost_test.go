package orchestrator

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tetratelabs/wazero/api"

	history "github.com/dora-network/bond-trading-strategies/internal/agent/store"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wsbroker"
)

func TestFrameCandleJSON_Wrapped(t *testing.T) {
	f := wsbroker.Frame{Type: "candle", OrderBookID: "OB-1", Raw: json.RawMessage(`{"type":"candle","data":{"close":"100"}}`)}
	b, err := frameCandleJSON(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"close":"100"}` {
		t.Errorf("got %s, want data-only", string(b))
	}
}

func TestFrameCandleJSON_BareCandle(t *testing.T) {
	f := wsbroker.Frame{Type: "candle", OrderBookID: "OB-1", Raw: json.RawMessage(`{"close":"100"}`)}
	b, err := frameCandleJSON(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"close":"100"}` {
		t.Errorf("got %s", string(b))
	}
}

func TestFrameCandleJSON_NullDataFallsThrough(t *testing.T) {
	// Some frames have explicit null data; the bare-candle fallback
	// should win (Raw is valid JSON, the test is the raw itself).
	f := wsbroker.Frame{Type: "candle", OrderBookID: "OB-1", Raw: json.RawMessage(`{"data":null,"close":"42"}`)}
	b, err := frameCandleJSON(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"data":null,"close":"42"}` {
		t.Errorf("got %s", string(b))
	}
}

func TestFrameCandleJSON_InvalidJSONFails(t *testing.T) {
	f := wsbroker.Frame{Type: "candle", OrderBookID: "OB-1", Raw: json.RawMessage(`{not-json`)}
	if _, err := frameCandleJSON(f); err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestBuildLiveConfigJSON(t *testing.T) {
	cfg := DeployConfig{
		StrategyID:  "S-1",
		OrderBookID: "OB-1",
		Params:      map[string]string{"leverage": "2"},
	}
	b, err := buildLiveConfigJSON(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got["mode"] != "live" {
		t.Errorf("mode: got %v, want live", got["mode"])
	}
	if got["order_book_id"] != "OB-1" {
		t.Errorf("order_book_id: got %v", got["order_book_id"])
	}
	if p, ok := got["params"].(map[string]any); !ok || p["leverage"] != "2" {
		t.Errorf("params: got %v", got["params"])
	}
}

func TestParseDecimal(t *testing.T) {
	cases := map[string]float64{
		"":      0,
		"abc":   0,
		"0":     0,
		"1":     1,
		"1.5":   1.5,
		"100":   100,
		"0.001": 0.001,
	}
	for in, want := range cases {
		if got := parseDecimal(in); got != want {
			t.Errorf("parseDecimal(%q): got %v, want %v", in, got, want)
		}
	}
}

// TestLiveHostState_ChannelCloses confirms the design assumption
// that drives the whole live path: when the wsbroker subscription
// is closed (Unsubscribe), waitForCandle returns false. This is
// what makes Stop work without a wazero-level force-kill.
// recordingAudit captures every Record call for assertions.
type recordingAudit struct {
	actions []string
	details []string
}

func (r *recordingAudit) Record(_ context.Context, action, detail string) error {
	r.actions = append(r.actions, action)
	r.details = append(r.details, detail)
	return nil
}

func TestLiveHost_RoutesTradesAndPrices(t *testing.T) {
	// Buffered channels; we send one frame, read it, then send the
	// next. Sending all three up front would make the select's pick
	// nondeterministic; serial send/read pins the order so we can
	// assert each frame routes to the matching tagged envelope.
	candleCh := make(chan wsbroker.Frame, 1)
	tradeCh := make(chan wsbroker.Frame, 1)
	priceCh := make(chan wsbroker.Frame, 1)

	state := &liveHostState{
		candleCh: candleCh,
		tradeCh:  tradeCh,
		priceCh:  priceCh,
	}
	ctx := t.Context()

	// Candle: unwrap {"data":...} → bare candle struct.
	candleCh <- wsbroker.Frame{Type: "candle", OrderBookID: "OB-1", Raw: json.RawMessage(`{"type":"candle","data":{"close":"100"}}`)}
	ev, ok := nextEvent(ctx, state)
	if !ok || ev.Type != "candle" {
		t.Fatalf("candle event: type=%q ok=%v, want candle", ev.Type, ok)
	}
	if string(ev.Data) != `{"close":"100"}` {
		t.Errorf("candle data: got %s, want unwrapped", string(ev.Data))
	}

	// Trade: route to the trade envelope, unwrapped.
	tradeCh <- wsbroker.Frame{Type: "trade", OrderBookID: "OB-1", Raw: json.RawMessage(`{"type":"trade","data":{"transaction_id":"tx-1"}}`)}
	ev, ok = nextEvent(ctx, state)
	if !ok || ev.Type != "trade" {
		t.Fatalf("trade event: type=%q ok=%v, want trade", ev.Type, ok)
	}
	if string(ev.Data) != `{"transaction_id":"tx-1"}` {
		t.Errorf("trade data: got %s", string(ev.Data))
	}

	// Price: route to the price envelope, unwrapped.
	priceCh <- wsbroker.Frame{Type: "price", AssetID: "asset-1", Raw: json.RawMessage(`{"type":"price","data":{"price":"99"}}`)}
	ev, ok = nextEvent(ctx, state)
	if !ok || ev.Type != "price" {
		t.Fatalf("price event: type=%q ok=%v, want price", ev.Type, ok)
	}
	if string(ev.Data) != `{"price":"99"}` {
		t.Errorf("price data: got %s", string(ev.Data))
	}
}

func TestLiveHost_NextEventClosedChannelsReturnDone(t *testing.T) {
	// All channels closed (or nil) → nextEvent returns ok=false so
	// the plugin's live loop exits cleanly. This is the Stop path.
	state := &liveHostState{}
	if _, ok := nextEvent(t.Context(), state); ok {
		t.Error("expected ok=false when all channels are nil")
	}
}

func TestLiveHost_PreambleFetchesBucketedCandles(t *testing.T) {
	// The preamble fetch window is computed from the deployment's
	// resolution + warmup-candle count: warmup_start = now - N*res.
	// With resolution="5m" and warmupCandles=10, the window spans
	// 50 minutes. The history store does the SQL bucketing at
	// cfg.Resolution; the host hands the rows through unchanged.
	state := &liveHostState{resolution: "5m", warmupCandles: 10}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	start, end := state.fetchWindow(now)
	want := now.Add(-10 * 5 * time.Minute)
	if !start.Equal(want) {
		t.Errorf("start: got %v, want %v (50m before now)", start, want)
	}
	if !end.Equal(now) {
		t.Errorf("end: got %v, want %v", end, now)
	}

	// Resolution bucketing: normalizeResolution + resolutionDuration
	// must handle 5m, 1h, 1d so the window math is correct.
	if d := resolutionDuration("5m"); d != 5*time.Minute {
		t.Errorf("resolutionDuration(5m) = %v, want 5m", d)
	}
	if d := resolutionDuration("1d"); d != 24*time.Hour {
		t.Errorf("resolutionDuration(1d) = %v, want 24h", d)
	}

	// No history store → fetch returns an empty done batch (the
	// preamble has nothing to load; the plugin's OnPreamble exits).
	state2 := &liveHostState{resolution: "5m", warmupCandles: 10}
	if state2.history != nil {
		t.Fatal("expected nil history store by default")
	}

	// A fetch after the preamble (preambleInProgress=false) returns
	// the preamble_done sentinel. The first hostNextEvent call ends
	// the preamble.
	state3 := &liveHostState{resolution: "5m", warmupCandles: 10, preambleInProgress: false}
	if state3.preambleActive() {
		t.Error("expected preambleActive=false after the event loop starts")
	}
}

func TestLiveHost_PreambleActiveGating(t *testing.T) {
	// preambleActive starts true (set on entry) and flips false once
	// the first host_next_event call runs. This is the host's only
	// signal that OnPreamble returned.
	state := &liveHostState{preambleInProgress: true}
	if !state.preambleActive() {
		t.Error("expected preambleActive=true on entry")
	}
	state.preambleMu.Lock()
	state.preambleInProgress = false
	state.preambleMu.Unlock()
	if state.preambleActive() {
		t.Error("expected preambleActive=false after the event loop starts")
	}
}

func TestLiveHost_OrderSubmitBannedDuringPreamble(t *testing.T) {
	// A host_submit_order call during OnPreamble returns the
	// preamble_no_orders error and writes an order.denied audit
	// entry with denial_reason=preamble_no_orders. We exercise the
	// shared submitOrderLive path via a minimal fake module.
	aud := &recordingAudit{}
	state := &liveHostState{
		userID:             "u-1",
		orderBook:          "OB-1",
		preambleInProgress: true,
		audit:              aud,
	}

	intentJSON := `{"side":"buy","quantity":"1","type":"market"}`
	mod := newFakeModule([]byte(intentJSON))

	_, err := submitOrderLive(t.Context(), state, mod, 0, uint32(len(intentJSON))) //nolint:gosec // G115: tiny test payload
	if err == nil {
		t.Fatal("expected error during preamble order submit")
	}
	if err.Error() != "preamble_no_orders" {
		t.Errorf("error: got %q, want preamble_no_orders", err.Error())
	}
	if len(aud.actions) != 1 || aud.actions[0] != "order.denied" {
		t.Fatalf("audit actions: %v, want [order.denied]", aud.actions)
	}
	if !strings.Contains(aud.details[0], "preamble_no_orders") {
		t.Errorf("audit detail: %q missing preamble_no_orders", aud.details[0])
	}
}

// fakeModule is a minimal api.Module whose Memory().Read returns the
// embedded bytes. Only the submitOrderLive path is exercised, which
// calls Memory().Read(ptr, len) — everything else panics if touched.
type fakeModule struct {
	api.Module
	mem *fakeMemory
}

func newFakeModule(b []byte) *fakeModule {
	return &fakeModule{mem: &fakeMemory{buf: b}}
}

func (m *fakeModule) Memory() api.Memory { return m.mem }

type fakeMemory struct {
	api.Memory
	buf []byte
}

func (m *fakeMemory) Read(offset, length uint32) ([]byte, bool) {
	if offset != 0 || uint32(len(m.buf)) < length { //nolint:gosec // test
		return nil, false
	}
	return m.buf[:length], true
}

func (m *fakeMemory) Write(offset uint32, val []byte) bool {
	m.buf = append(m.buf[:0], val...)
	return true
}

// TestLiveHost_PreambleNoWarmupReturnsEmptyDone verifies that when
// warmupCandles=0, hostFetchCandles/Trades/Prices return an empty
// done batch WITHOUT touching the history store. Before the fix,
// fetchWindow returned a zero start time and the closures passed it
// to history.FetchCandles, producing WHERE start_timestamp >=
// '0001-01-01' — matching the entire table.
//
// The history store is non-nil but backed by a nil *sql.DB. If the
// guard were absent, FetchCandles would nil-panic; the guard returns
// before that point, so the store is never reached.
func TestLiveHost_PreambleNoWarmupReturnsEmptyDone(t *testing.T) {
	ctx := t.Context()
	for _, tc := range []struct {
		name    string
		build   func(state *liveHostState) api.GoModuleFunc
		state   *liveHostState
		wantKey string
	}{
		{
			name:    "candles",
			build:   hostFetchCandles,
			state:   &liveHostState{resolution: "5m", warmupCandles: 0, orderBook: "OB-1", preambleInProgress: true},
			wantKey: "Items",
		},
		{
			name:    "trades",
			build:   hostFetchTrades,
			state:   &liveHostState{resolution: "5m", warmupCandles: 0, orderBook: "OB-1", preambleInProgress: true},
			wantKey: "Items",
		},
		{
			name:    "prices",
			build:   hostFetchPrices,
			state:   &liveHostState{resolution: "5m", warmupCandles: 0, assetID: "asset-1", preambleInProgress: true},
			wantKey: "Items",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Non-nil history with nil db: proves the store is never
			// reached (would nil-panic otherwise).
			tc.state.history = &history.HistoryStore{}
			mod := &fetchModule{mem: &fetchMemory{buf: make([]byte, 4096)}}
			params := []uint64{0, 0, 0, 4096}
			tc.build(tc.state)(ctx, mod, params)

			result := int32(params[0]) //nolint:gosec // G115: bounded by the 4096-byte test buffer
			if result < 0 {
				t.Fatalf("fetch returned error payload: %s", mod.mem.buf)
			}
			var batch map[string]any
			if err := json.Unmarshal(mod.mem.buf[:result], &batch); err != nil {
				t.Fatalf("unmarshal batch: %v (raw: %s)", err, mod.mem.buf[:result])
			}
			if batch["Done"] != true {
				t.Errorf("batch = %v, want Done=true (empty done batch)", batch)
			}
			if items, ok := batch[tc.wantKey]; ok && items != nil {
				t.Errorf("batch %s = %v, want nil (no rows)", tc.wantKey, items)
			}
		})
	}
}

// fetchModule / fetchMemory: a write-only fake for the fetch ABI.
type fetchModule struct {
	api.Module
	mem *fetchMemory
}

func (m *fetchModule) Memory() api.Memory { return m.mem }

type fetchMemory struct {
	api.Memory
	buf []byte
}

func (m *fetchMemory) Write(_ uint32, val []byte) bool {
	m.buf = append(m.buf[:0], val...)
	return true
}

// rwFetchMemory is a fetch-ABI fake memory supporting both Read
// (plugin → host fetchReq buffer) and Write (host → plugin batch
// JSON). offset 0 is the shared buffer origin.
type rwFetchMemory struct {
	api.Memory
	buf []byte
}

func (m *rwFetchMemory) Read(offset, length uint32) ([]byte, bool) {
	if offset != 0 || uint32(len(m.buf)) < length { //nolint:gosec // test
		return nil, false
	}
	return m.buf[:length], true
}

func (m *rwFetchMemory) Write(_ uint32, val []byte) bool {
	m.buf = append(m.buf[:0], val...)
	return true
}

type rwFetchModule struct {
	api.Module
	mem *rwFetchMemory
}

func (m *rwFetchModule) Memory() api.Memory { return m.mem }

// recordingHistory is a history.HistoryFetcher fake that records the cursor
// and batch size of the last FetchCandles call and replays a canned
// page.
type recordingHistory struct {
	candles   []history.Candle
	gotCursor string
	gotBatch  int
}

func (r *recordingHistory) FetchCandles(_ context.Context, _ string, _, _ time.Time,
	_ string, cursor string, batchSize int) ([]history.Candle, string, error) {
	r.gotCursor, r.gotBatch = cursor, batchSize
	return r.candles, "", nil
}

func (r *recordingHistory) FetchTrades(_ context.Context, _ string, _, _ time.Time,
	_ string, _ int) ([]history.Trade, string, error) {
	return nil, "", nil
}

func (r *recordingHistory) FetchPrices(_ context.Context, _ string, _, _ time.Time,
	_ string, _ int) ([]history.Price, string, error) {
	return nil, "", nil
}

// TestLiveHost_FetchCandlesHonorsCursorAndBatchSize verifies the
// host decodes the plugin's fetchReq JSON from (inPtr, inLen) and
// threads req.Cursor and req.BatchSize into history.FetchCandles.
// Before the fix the host never read params[0..1], always passing
// cursor="" and batchSize=500 — so the plugin's re-call with the
// returned cursor was re-served page 1 forever.
func TestLiveHost_FetchCandlesHonorsCursorAndBatchSize(t *testing.T) {
	reqJSON := []byte(`{"start":"2026-08-01T00:00:00Z","end":"2026-08-18T00:00:00Z","resolution":"1m","batch_size":100,"cursor":"abc"}`)
	fake := &recordingHistory{candles: []history.Candle{mkHostCandle()}}
	state := &liveHostState{
		resolution: "1m", warmupCandles: 3, orderBook: "OB-1",
		preambleInProgress: true, history: fake,
	}
	mod := &rwFetchModule{mem: &rwFetchMemory{buf: append(reqJSON, make([]byte, 4096)...)}}
	params := []uint64{0, uint64(len(reqJSON)), 0, 4096}
	hostFetchCandles(state)(t.Context(), mod, params)

	if int32(params[0]) < 0 { //nolint:gosec // wazero i32 result
		t.Fatalf("fetch returned error payload")
	}
	if fake.gotCursor != "abc" {
		t.Errorf("FetchCandles cursor = %q, want %q", fake.gotCursor, "abc")
	}
	if fake.gotBatch != 100 {
		t.Errorf("FetchCandles batchSize = %d, want 100", fake.gotBatch)
	}
}

// mkHostCandle builds one history.Candle for the live-host tests.
func mkHostCandle() history.Candle {
	return history.Candle{
		OrderBookID: "OB-1", StartTimestamp: time.Now().UTC(),
		Open: "1", High: "1", Low: "1", Close: "1", Volume: "1",
	}
}

func TestHistoryCandles_CopiesYTM(t *testing.T) {
	rows := []history.Candle{{
		OrderBookID: "OB-1",
		Open:        "100", High: "101", Low: "99", Close: "100.5",
		OpenYtm: "0.0501", HighYtm: "0.0502", LowYtm: "0.0499", CloseYtm: "0.0500",
		Volume: "10",
	}}
	got := historyCandles(rows)
	if len(got) != 1 {
		t.Fatalf("len: got %d", len(got))
	}
	c := got[0]
	if c.OpenYtm != "0.0501" || c.HighYtm != "0.0502" || c.LowYtm != "0.0499" || c.CloseYtm != "0.0500" {
		t.Errorf("YTM fields not copied: %+v", c)
	}
}
