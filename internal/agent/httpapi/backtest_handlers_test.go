package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

// --- fakes ---

// fakeBacktestStore is the in-memory backtest.Store used by handler tests.
// It only implements the methods the handlers reach; unexercised methods
// hit the embedded interface and panic. cancelled tracks CancelIfRunning
// invocations for assertion.
type fakeBacktestStore struct {
	backtest.Store
	rows       map[string]*backtest.Backtest // id -> row
	fills      map[string][]backtest.Fill    // id -> fills
	createErr  error
	listErr    error
	getErr     error
	fillsErr   error
	cancelErr  error
	cancelled  []string // backtest IDs passed to CancelIfRunning
	createSeen *backtest.Backtest
}

func newFakeBacktestStore() *fakeBacktestStore {
	return &fakeBacktestStore{
		rows:  map[string]*backtest.Backtest{},
		fills: map[string][]backtest.Fill{},
	}
}

func (f *fakeBacktestStore) Create(_ context.Context, b *backtest.Backtest) (string, error) {
	if f.createErr != nil {
		return "", f.createErr
	}
	if b.ID == "" {
		b.ID = "bt-" + b.StrategyID
	}
	cp := *b
	f.rows[b.ID] = &cp
	f.createSeen = &cp
	return b.ID, nil
}

func (f *fakeBacktestStore) Get(_ context.Context, id string) (*backtest.Backtest, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	bt, ok := f.rows[id]
	if !ok {
		return nil, backtest.ErrNotFound
	}
	cp := *bt
	return &cp, nil
}

func (f *fakeBacktestStore) List(_ context.Context, strategyID string, limit int) ([]*backtest.Backtest, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]*backtest.Backtest, 0, len(f.rows))
	for _, bt := range f.rows {
		if bt.StrategyID != strategyID {
			continue
		}
		cp := *bt
		out = append(out, &cp)
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeBacktestStore) GetFills(_ context.Context, backtestID string) ([]backtest.Fill, error) {
	if f.fillsErr != nil {
		return nil, f.fillsErr
	}
	return f.fills[backtestID], nil
}

func (f *fakeBacktestStore) CancelIfRunning(_ context.Context, id string) (bool, error) {
	if f.cancelErr != nil {
		return false, f.cancelErr
	}
	f.cancelled = append(f.cancelled, id)
	return true, nil
}

// UpdateStatus / SetContainerID / SetSummary / InsertFills / FailOrphaned are
// stubbed to swallow calls so the runner goroutine (kicked off by Submit)
// doesn't panic when it tries to reconcile a row that doesn't exist or that
// the test never started in earnest. The handler-level tests don't observe
// these state transitions.
func (f *fakeBacktestStore) UpdateStatus(_ context.Context, _ string, _ backtest.Status,
	_, _ *time.Time, _ string,
) error {
	return nil
}
func (f *fakeBacktestStore) SetContainerID(_ context.Context, _, _ string) error { return nil }
func (f *fakeBacktestStore) SetSummary(_ context.Context, _ string, _ *backtest.Summary, _ int) error {
	return nil
}

func (f *fakeBacktestStore) InsertFills(_ context.Context, _ string, _ []backtest.Fill) error {
	return nil
}
func (f *fakeBacktestStore) FailOrphaned(_ context.Context) (int, error) { return 0, nil }
func (f *fakeBacktestStore) ListBacktests(_ context.Context, _ string, _ *string, _ *backtest.Status, _ int) ([]*backtest.Backtest, error) {
	return nil, nil
}

// seedVersionWithWasm inserts a go-wasm version under testStrategyID with
// the given wasm_ref / manifest_hash.
func seedVersionWithWasm(strats *fakeStrategies) {
	rev := "rev-1"
	wasmRef := "rev-1-wasm"
	manifestHash := "rev-1-mh"
	seedVersion(strats, rev, "", strategies.Meta{}, nil)
	v := strats.versionsByRev[rev]
	v.Target = "go-wasm"
	v.WasmRef = wasmRef
	v.ManifestHash = manifestHash
	strats.versionsByRev[rev] = v
}

// newBacktestServer wires a Server with the strategy fake (testUserID owns
// testStrategyID) and a fresh backtest store + wasm-only orchestrator.
func newBacktestServer(t *testing.T) (*Server, *fakeStrategies, *fakeBacktestStore) {
	t.Helper()
	strats := newFakeStrategies()
	seedStrategy(strats, testStrategyID, "rev-1")
	bs := newFakeBacktestStore()
	// Wire a zero-value WasmStarter so Submit's nil-Wasm guard
	// doesn't shadow the test's intended assertion (e.g. 200 vs 429
	// vs 400). The goroutine Submit spawns will panic when it
	// dereferences WasmStarter.btStore, but the HTTP response is
	// already written by then; the panic is unrelated to the
	// assertion under test.
	orch := &backtest.Orchestrator{Store: bs, Wasm: &backtest.WasmStarter{}}
	srv := &Server{
		sessions:      &fakeSessionStore{},
		strategies:    strats,
		backtestOrch:  orch,
		backtestStore: bs,
	}
	return srv, strats, bs
}

// --- POST /v1/strategies/{id}/versions/{revision}/backtest ---

func TestBacktest_Success(t *testing.T) {
	srv, strats, bs := newBacktestServer(t)
	seedVersionWithWasm(strats)
	body := `{"start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","resolution":"1h","order_book_id":"ob-1"}`
	rec := invoke(t, srv.handleBacktest, http.MethodPost,
		"/v1/strategies/s1/versions/rev-1/backtest", body,
		map[string]string{"id": testStrategyID, "revision": "rev-1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp backtest.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.BacktestID == "" {
		t.Errorf("backtest_id: want non-empty, got empty")
	}
	if bs.createSeen == nil {
		t.Fatal("store.Create was not called")
	}
	if bs.createSeen.VersionID != "rev-1" {
		t.Errorf("version_id: want rev-1, got %q", bs.createSeen.VersionID)
	}
	if bs.createSeen.UserID != testUserID {
		t.Errorf("user_id: want %q, got %q", testUserID, bs.createSeen.UserID)
	}
}

// TestBacktest_WarmupCandles_Passthrough guards P1 #6: the backtest
// request's warmup_candles must reach the persisted Backtest row. The
// go-wasm runner reads b.WarmupCandles to shift the fetch window back
// so OnPreamble sees history; before this fix every backtest ran with 0.
func TestBacktest_WarmupCandles_Passthrough(t *testing.T) {
	srv, strats, bs := newBacktestServer(t)
	seedVersionWithWasm(strats)
	body := `{"start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","resolution":"1h","order_book_id":"ob-1","warmup_candles":200}`
	rec := invoke(t, srv.handleBacktest, http.MethodPost,
		"/v1/strategies/s1/versions/rev-1/backtest", body,
		map[string]string{"id": testStrategyID, "revision": "rev-1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if bs.createSeen == nil {
		t.Fatal("store.Create was not called")
	}
	if bs.createSeen.WarmupCandles != 200 {
		t.Errorf("WarmupCandles: want 200, got %d", bs.createSeen.WarmupCandles)
	}
}

// TestBacktest_WarmupCandles_OutOfRange_400: negative or > int32
// warmup_candles is rejected before any store write.
func TestBacktest_WarmupCandles_OutOfRange_400(t *testing.T) {
	srv, strats, _ := newBacktestServer(t)
	seedVersionWithWasm(strats)
	for _, w := range []string{"-1", "2147483648"} {
		body := `{"start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","resolution":"1h","order_book_id":"ob-1","warmup_candles":` + w + `}`
		rec := invoke(t, srv.handleBacktest, http.MethodPost,
			"/v1/strategies/s1/versions/rev-1/backtest", body,
			map[string]string{"id": testStrategyID, "revision": "rev-1"})
		if rec.Code != http.StatusBadRequest {
			t.Errorf("warmup_candles=%s: want 400, got %d body=%s", w, rec.Code, rec.Body.String())
		}
	}
}

func TestBacktest_StoreUnavailable_503(t *testing.T) {
	srv := &Server{} // no orchestrator, no store
	rec := invoke(t, srv.handleBacktest, http.MethodPost,
		"/v1/strategies/s1/versions/r/backtest", "{}",
		map[string]string{"id": "s1", "revision": "r"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("code: want 503, got %d", rec.Code)
	}
}

func TestBacktest_StrategyNotFound_404(t *testing.T) {
	srv, _, _ := newBacktestServer(t)
	rec := invoke(t, srv.handleBacktest, http.MethodPost,
		"/v1/strategies/missing/versions/r/backtest", "{}",
		map[string]string{"id": "missing", "revision": "r"})
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
}

func TestBacktest_VersionNotFound_404(t *testing.T) {
	srv, _, _ := newBacktestServer(t)
	rec := invoke(t, srv.handleBacktest, http.MethodPost,
		"/v1/strategies/s1/versions/missing/backtest", "{}",
		map[string]string{"id": testStrategyID, "revision": "missing"})
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
}

func TestBacktest_NoImageRef_409(t *testing.T) {
	srv, strats, _ := newBacktestServer(t)
	// Docker-era target (""): the wasm-only handler must refuse it.
	seedVersion(strats, "rev-noimg", "", strategies.Meta{}, nil)
	rec := invoke(t, srv.handleBacktest, http.MethodPost,
		"/v1/strategies/s1/versions/rev-noimg/backtest", "{}",
		map[string]string{"id": testStrategyID, "revision": "rev-noimg"})
	if rec.Code != http.StatusConflict {
		t.Errorf("code: want 409, got %d", rec.Code)
	}
}

func TestBacktest_InvalidJSON_400(t *testing.T) {
	srv, strats, _ := newBacktestServer(t)
	seedVersionWithWasm(strats)
	rec := invoke(t, srv.handleBacktest, http.MethodPost,
		"/v1/strategies/s1/versions/rev-1/backtest", "{not json",
		map[string]string{"id": testStrategyID, "revision": "rev-1"})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("code: want 400, got %d", rec.Code)
	}
}

func TestBacktest_SingleInflight_429(t *testing.T) {
	srv, strats, bs := newBacktestServer(t)
	seedVersionWithWasm(strats)
	bs.createErr = backtest.ErrSingleInflight
	rec := invoke(t, srv.handleBacktest, http.MethodPost,
		"/v1/strategies/s1/versions/rev-1/backtest",
		`{"start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","resolution":"1h","order_book_id":"ob-1"}`,
		map[string]string{"id": testStrategyID, "revision": "rev-1"})
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("code: want 429, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestBacktest_InvalidRequestWrapped_400(t *testing.T) {
	srv, strats, bs := newBacktestServer(t)
	seedVersionWithWasm(strats)
	bs.createErr = errors.Join(backtest.ErrInvalidRequest, errors.New("start after end"))
	rec := invoke(t, srv.handleBacktest, http.MethodPost,
		"/v1/strategies/s1/versions/rev-1/backtest",
		`{"start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","resolution":"1h","order_book_id":"ob-1"}`,
		map[string]string{"id": testStrategyID, "revision": "rev-1"})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("code: want 400, got %d", rec.Code)
	}
}

// --- GET /v1/strategies/{id}/backtests ---

func TestListBacktests_FiltersByOwner(t *testing.T) {
	srv, _, bs := newBacktestServer(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bs.rows["bt-1"] = &backtest.Backtest{ID: "bt-1", StrategyID: testStrategyID, UserID: testUserID, RequestedAt: now}
	bs.rows["bt-2"] = &backtest.Backtest{ID: "bt-2", StrategyID: testStrategyID, UserID: "other-user", RequestedAt: now}

	rec := invoke(t, srv.handleListBacktests, http.MethodGet,
		"/v1/strategies/s1/backtests", "",
		map[string]string{"id": testStrategyID})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	var got []backtest.Backtest
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("rows: want 1 (only owner's), got %d", len(got))
	}
	if got[0].ID != "bt-1" {
		t.Errorf("id: want bt-1, got %q", got[0].ID)
	}
}

func TestListBacktests_StrategyNotFound_404(t *testing.T) {
	srv, _, _ := newBacktestServer(t)
	rec := invoke(t, srv.handleListBacktests, http.MethodGet,
		"/v1/strategies/missing/backtests", "",
		map[string]string{"id": "missing"})
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
}

func TestListBacktests_StoreUnavailable_503(t *testing.T) {
	srv := &Server{strategies: newFakeStrategies()} // backtestStore nil
	rec := invoke(t, srv.handleListBacktests, http.MethodGet,
		"/v1/strategies/s1/backtests", "",
		map[string]string{"id": testStrategyID})
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("code: want 503, got %d", rec.Code)
	}
}

// --- GET /v1/strategies/{id}/backtests/{backtest_id} ---

func TestGetBacktest_Success(t *testing.T) {
	srv, _, bs := newBacktestServer(t)
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	bs.rows["bt-1"] = &backtest.Backtest{
		ID: "bt-1", StrategyID: testStrategyID, UserID: testUserID,
		Status: backtest.StatusSucceeded, RequestedAt: now,
		WindowStart: now.Add(-24 * time.Hour), WindowEnd: now, Resolution: "1h",
	}
	bs.fills["bt-1"] = []backtest.Fill{
		{Timestamp: now.Add(-time.Hour), Side: "buy", Quantity: 1, Price: 100, OrderID: "o-1", SimulatedAt: now},
	}

	rec := invoke(t, srv.handleGetBacktest, http.MethodGet,
		"/v1/strategies/s1/backtests/bt-1", "",
		map[string]string{"id": testStrategyID, "backtest_id": "bt-1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var got struct {
		Backtest backtest.Backtest `json:"backtest"`
		Fills    []backtest.Fill   `json:"fills"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Backtest.ID != "bt-1" {
		t.Errorf("id: want bt-1, got %q", got.Backtest.ID)
	}
	if len(got.Fills) != 1 || got.Fills[0].OrderID != "o-1" {
		t.Errorf("fills: want 1 fill with order o-1, got %+v", got.Fills)
	}
}

func TestGetBacktest_OwnershipNotFound_404(t *testing.T) {
	srv, _, bs := newBacktestServer(t)
	bs.rows["bt-1"] = &backtest.Backtest{ID: "bt-1", StrategyID: testStrategyID, UserID: "other-user"}
	rec := invoke(t, srv.handleGetBacktest, http.MethodGet,
		"/v1/strategies/s1/backtests/bt-1", "",
		map[string]string{"id": testStrategyID, "backtest_id": "bt-1"})
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
}

func TestGetBacktest_NotFound_404(t *testing.T) {
	srv, _, _ := newBacktestServer(t)
	rec := invoke(t, srv.handleGetBacktest, http.MethodGet,
		"/v1/strategies/s1/backtests/missing", "",
		map[string]string{"id": testStrategyID, "backtest_id": "missing"})
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
}

func TestGetBacktest_NoFills_ReturnsEmptyArray(t *testing.T) {
	srv, _, bs := newBacktestServer(t)
	bs.rows["bt-1"] = &backtest.Backtest{ID: "bt-1", StrategyID: testStrategyID, UserID: testUserID}
	rec := invoke(t, srv.handleGetBacktest, http.MethodGet,
		"/v1/strategies/s1/backtests/bt-1", "",
		map[string]string{"id": testStrategyID, "backtest_id": "bt-1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !contains(body, `"fills":[]`) {
		t.Errorf("body: want empty fills array, got %s", body)
	}
}

// --- POST /v1/strategies/{id}/backtests/{backtest_id}/cancel ---

func TestCancelBacktest_Success(t *testing.T) {
	srv, _, bs := newBacktestServer(t)
	bs.rows["bt-1"] = &backtest.Backtest{
		ID: "bt-1", StrategyID: testStrategyID, UserID: testUserID,
		Status: backtest.StatusRunning,
	}
	rec := invoke(t, srv.handleCancelBacktest, http.MethodPost,
		"/v1/strategies/s1/backtests/bt-1/cancel", "",
		map[string]string{"id": testStrategyID, "backtest_id": "bt-1"})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code: want 204, got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(bs.cancelled) != 1 || bs.cancelled[0] != "bt-1" {
		t.Errorf("cancel not invoked: got %v", bs.cancelled)
	}
}

func TestCancelBacktest_OwnershipNotFound_404(t *testing.T) {
	srv, _, bs := newBacktestServer(t)
	bs.rows["bt-1"] = &backtest.Backtest{ID: "bt-1", StrategyID: testStrategyID, UserID: "other-user"}
	rec := invoke(t, srv.handleCancelBacktest, http.MethodPost,
		"/v1/strategies/s1/backtests/bt-1/cancel", "",
		map[string]string{"id": testStrategyID, "backtest_id": "bt-1"})
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
	if len(bs.cancelled) != 0 {
		t.Errorf("cancel must not run for wrong owner: got %v", bs.cancelled)
	}
}

func TestCancelBacktest_NotFound_404(t *testing.T) {
	srv, _, _ := newBacktestServer(t)
	rec := invoke(t, srv.handleCancelBacktest, http.MethodPost,
		"/v1/strategies/s1/backtests/missing/cancel", "",
		map[string]string{"id": testStrategyID, "backtest_id": "missing"})
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
}

// contains is a tiny substring helper used to avoid pulling strings.Contains
// into this file for one assertion.
func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
