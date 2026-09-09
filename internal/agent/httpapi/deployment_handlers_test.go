package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/backtest"

	"github.com/dora-network/bond-trading-strategies/internal/agent/deployment"
	"github.com/dora-network/bond-trading-strategies/internal/agent/orchestrator"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

// --- fakes ---

// fakeDeploymentStore is the in-memory deployment.Store used by handler
// tests. It only implements the methods the handlers reach; unexercised
// methods hit the embedded interface and panic.
type fakeDeploymentStore struct {
	deployment.Store
	rows          map[string]*deployment.Deployment
	activeByStrat map[string]bool
	created       []*deployment.Deployment
	createErr     error
	getErr        error
	listErr       error
	hasActiveErr  error
	updateErr     error
}

func newFakeDeploymentStore() *fakeDeploymentStore {
	return &fakeDeploymentStore{
		rows:          map[string]*deployment.Deployment{},
		activeByStrat: map[string]bool{},
	}
}

func (f *fakeDeploymentStore) Create(_ context.Context, d deployment.Deployment) error {
	if f.createErr != nil {
		return f.createErr
	}
	cp := d
	f.rows[d.ID] = &cp
	f.created = append(f.created, &cp)
	if d.Status == deployment.StatusRunning {
		f.activeByStrat[d.StrategyID] = true
	}
	return nil
}

func (f *fakeDeploymentStore) Get(_ context.Context, id, userID string) (deployment.Deployment, error) {
	if f.getErr != nil {
		return deployment.Deployment{}, f.getErr
	}
	row, ok := f.rows[id]
	if !ok || row.UserID != userID {
		return deployment.Deployment{}, deployment.ErrNotFound
	}
	return *row, nil
}

func (f *fakeDeploymentStore) List(_ context.Context, strategyID, userID string, p deployment.Page) ([]deployment.Deployment, string, error) {
	if f.listErr != nil {
		return nil, "", f.listErr
	}
	limit := p.EffectiveLimit()
	out := make([]deployment.Deployment, 0, len(f.rows))
	for _, d := range f.rows {
		if d.StrategyID != strategyID || d.UserID != userID {
			continue
		}
		out = append(out, *d)
	}
	sortByCreatedAtDesc(out)
	next := ""
	if limit > 0 && len(out) > limit {
		out = out[:limit]
		next = "cursor-next"
	}
	return out, next, nil
}

// sortByCreatedAtDesc orders rows newest-first by CreatedAt (descending).
func sortByCreatedAtDesc(out []deployment.Deployment) {
	for i := range len(out) {
		for j := i + 1; j < len(out); j++ {
			if out[j].CreatedAt.After(out[i].CreatedAt) {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
}

func (f *fakeDeploymentStore) HasActive(_ context.Context, strategyID string) (bool, error) {
	if f.hasActiveErr != nil {
		return false, f.hasActiveErr
	}
	return f.activeByStrat[strategyID], nil
}

func (f *fakeDeploymentStore) UpdateStatus(_ context.Context, id, userID string, status deployment.Status, _ string) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	row, ok := f.rows[id]
	if !ok || row.UserID != userID {
		return deployment.ErrNotFound
	}
	row.Status = status
	if status != deployment.StatusRunning {
		delete(f.activeByStrat, row.StrategyID)
	}
	return nil
}

func (f *fakeDeploymentStore) SetInstance(_ context.Context, _, _, _ string) error { return nil }
func (f *fakeDeploymentStore) HotSwap(_ context.Context, _, _, _ string) error     { return nil }
func (f *fakeDeploymentStore) IncRestart(_ context.Context, _, _ string) error     { return nil }
func (f *fakeDeploymentStore) ListRunning(_ context.Context) ([]deployment.Deployment, error) {
	return nil, nil
}

// fakeOrchestrator is a no-op LiveOrchestrator that records calls.
type fakeOrchestrator struct {
	LiveOrchestrator
	deployErr  error
	stopErr    error
	resumeErr  error
	hotSwapErr error
	deployed   []orchestrator.DeployConfig
	stopped    []string
	resumed    []string
	hotSwapped []string
}

func (f *fakeOrchestrator) Deploy(_ context.Context, cfg orchestrator.DeployConfig) error {
	f.deployed = append(f.deployed, cfg)
	return f.deployErr
}

func (f *fakeOrchestrator) Stop(_ context.Context, deploymentID, _ string) error {
	f.stopped = append(f.stopped, deploymentID)
	return f.stopErr
}

func (f *fakeOrchestrator) Resume(_ context.Context, deploymentID, _, _ string) error {
	f.resumed = append(f.resumed, deploymentID)
	return f.resumeErr
}

func (f *fakeOrchestrator) HotSwap(_ context.Context, deploymentID, _, newRevision, _, _, _, _ string) error {
	f.hotSwapped = append(f.hotSwapped, deploymentID+":"+newRevision)
	return f.hotSwapErr
}

// seedDeploymentVersion inserts a go-wasm version under testStrategyID.
// TODO: make rev parameterised to remove the unparam warning — currently
// every call site passes "rev-1" so the linter flags rev as unused
// flexibility. Tracked in slice-X-followups.
//
//nolint:unparam
func seedDeploymentVersion(strats *fakeStrategies, rev, wasmRef, manifestHash string) {
	seedVersion(strats, rev, "", strategies.Meta{}, nil)
	v := strats.versionsByRev[rev]
	v.Target = "go-wasm"
	v.WasmRef = wasmRef
	v.ManifestHash = manifestHash
	strats.versionsByRev[rev] = v
}

// newDeploymentServer wires a Server with the strategy fake, a
// deployment fake, and a fake live orchestrator.
func newDeploymentServer(t *testing.T) (*Server, *fakeStrategies, *fakeDeploymentStore) {
	t.Helper()
	strats := newFakeStrategies()
	seedStrategy(strats, testStrategyID, "rev-1")
	ds := newFakeDeploymentStore()
	srv := &Server{
		sessions:    &fakeSessionStore{},
		strategies:  strats,
		deployStore: ds,
		liveOrch:    &fakeOrchestrator{},
	}
	return srv, strats, ds
}

func TestHandleDeploy_HappyPath(t *testing.T) {
	srv, strats, ds := newDeploymentServer(t)
	seedDeploymentVersion(strats, "rev-1", "wasm-ref", "manifest-ref")

	rec := invoke(t, srv.handleDeploy, http.MethodPost,
		"/v1/strategies/s1/versions/rev-1/deploy",
		`{"order_book_id":"OB-1","resolution":"1m","params":{"k":"v"},"warmup_candles":120}`,
		map[string]string{"id": testStrategyID, "revision": "rev-1"})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("code: want 202, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		DeploymentID string `json:"deployment_id"`
		Status       string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.DeploymentID == "" {
		t.Errorf("deployment_id: want non-empty")
	}
	if resp.Status != string(deployment.StatusRunning) {
		t.Errorf("status: want running, got %q", resp.Status)
	}
	if len(ds.created) != 1 {
		t.Fatalf("store.Create: want 1, got %d", len(ds.created))
	}
	if ds.created[0].StrategyID != testStrategyID {
		t.Errorf("strategy_id: want %q, got %q", testStrategyID, ds.created[0].StrategyID)
	}
	if ds.created[0].UserID != testUserID {
		t.Errorf("user_id: want %q, got %q", testUserID, ds.created[0].UserID)
	}
	if ds.created[0].Params["k"] != "v" {
		t.Errorf("params: want k=v, got %+v", ds.created[0].Params)
	}
	if ds.created[0].WarmupCandles != 120 {
		t.Errorf("warmup_candles: want 120, got %d", ds.created[0].WarmupCandles)
	}
}

func TestHandleDeploy_AlreadyRunning(t *testing.T) {
	srv, strats, ds := newDeploymentServer(t)
	seedDeploymentVersion(strats, "rev-1", "wasm-ref", "manifest-ref")
	ds.activeByStrat[testStrategyID] = true

	rec := invoke(t, srv.handleDeploy, http.MethodPost,
		"/v1/strategies/s1/versions/rev-1/deploy",
		`{"order_book_id":"OB-1","resolution":"1m"}`,
		map[string]string{"id": testStrategyID, "revision": "rev-1"})

	if rec.Code != http.StatusConflict {
		t.Fatalf("code: want 409, got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(ds.created) != 0 {
		t.Errorf("Create must not run when already active: got %d", len(ds.created))
	}
}

func TestHandleDeploy_NoWasmRef(t *testing.T) {
	srv, strats, ds := newDeploymentServer(t)
	seedDeploymentVersion(strats, "rev-1", "", "")

	rec := invoke(t, srv.handleDeploy, http.MethodPost,
		"/v1/strategies/s1/versions/rev-1/deploy", "",
		map[string]string{"id": testStrategyID, "revision": "rev-1"})

	if rec.Code != http.StatusConflict {
		t.Errorf("code: want 409, got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(ds.created) != 0 {
		t.Errorf("Create must not run without wasm: got %d", len(ds.created))
	}
}

func TestHandleDeploy_StrategyNotFound(t *testing.T) {
	srv, _, _ := newDeploymentServer(t)
	rec := invoke(t, srv.handleDeploy, http.MethodPost,
		"/v1/strategies/missing/versions/r/deploy", "",
		map[string]string{"id": "missing", "revision": "r"})
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
}

func TestHandleGetDeployment_NotFound(t *testing.T) {
	srv, _, _ := newDeploymentServer(t)
	rec := invoke(t, srv.handleGetDeployment, http.MethodGet,
		"/v1/strategies/s1/deployments/missing", "",
		map[string]string{"id": testStrategyID, "deployment_id": "missing"})
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
}

func TestHandleGetDeployment_WrongUser(t *testing.T) {
	srv, _, ds := newDeploymentServer(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ds.rows["d-1"] = &deployment.Deployment{
		ID: "d-1", StrategyID: testStrategyID, Revision: "rev-1",
		UserID: "other-user", Status: deployment.StatusRunning,
		CreatedAt: now, UpdatedAt: now,
	}
	rec := invoke(t, srv.handleGetDeployment, http.MethodGet,
		"/v1/strategies/s1/deployments/d-1", "",
		map[string]string{"id": testStrategyID, "deployment_id": "d-1"})
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
}

func TestHandleGetDeployment_Success(t *testing.T) {
	srv, _, ds := newDeploymentServer(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ds.rows["d-1"] = &deployment.Deployment{
		ID: "d-1", StrategyID: testStrategyID, Revision: "rev-1",
		UserID: testUserID, Status: deployment.StatusRunning,
		OrderBookID: "OB-1", Resolution: "1m", CandleCount: 42,
		CreatedAt: now, UpdatedAt: now,
	}
	rec := invoke(t, srv.handleGetDeployment, http.MethodGet,
		"/v1/strategies/s1/deployments/d-1", "",
		map[string]string{"id": testStrategyID, "deployment_id": "d-1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["deployment_id"] != "d-1" {
		t.Errorf("deployment_id: want d-1, got %v", got["deployment_id"])
	}
	if got["status"] != string(deployment.StatusRunning) {
		t.Errorf("status: want running, got %v", got["status"])
	}
	if got["order_book_id"] != "OB-1" {
		t.Errorf("order_book_id: want OB-1, got %v", got["order_book_id"])
	}
	if got["resolution"] != "1m" {
		t.Errorf("resolution: want 1m, got %v", got["resolution"])
	}
	if got["candle_count"] != float64(42) {
		t.Errorf("candle_count: want 42, got %v", got["candle_count"])
	}
	if _, ok := got["user_id"]; ok {
		t.Errorf("user_id must not be exposed, got %v", got["user_id"])
	}
	if params, ok := got["params"].(map[string]any); !ok || len(params) != 0 {
		t.Errorf("params: want empty map {}, got %#v", got["params"])
	}
	if got["stopped_reason"] != nil {
		t.Errorf("stopped_reason: want nil, got %v", got["stopped_reason"])
	}
	if got["hotswapped_from_rev"] != nil {
		t.Errorf("hotswapped_from_rev: want nil, got %v", got["hotswapped_from_rev"])
	}
}

func TestHandleListDeployments(t *testing.T) {
	srv, _, ds := newDeploymentServer(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ds.rows["d-1"] = &deployment.Deployment{
		ID: "d-1", StrategyID: testStrategyID, UserID: testUserID,
		Status: deployment.StatusRunning, OrderBookID: "OB-1",
		Resolution: "1m", CandleCount: 42,
		CreatedAt: now, UpdatedAt: now,
	}
	ds.rows["d-2"] = &deployment.Deployment{
		ID: "d-2", StrategyID: testStrategyID, UserID: testUserID,
		Status: deployment.StatusStopped, CreatedAt: now.Add(time.Second), UpdatedAt: now,
	}
	ds.rows["d-3"] = &deployment.Deployment{
		ID: "d-3", StrategyID: testStrategyID, UserID: "other-user",
		Status: deployment.StatusRunning, CreatedAt: now, UpdatedAt: now,
	}

	rec := invoke(t, srv.handleListDeployments, http.MethodGet,
		"/v1/strategies/s1/deployments", "",
		map[string]string{"id": testStrategyID})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var got struct {
		Deployments []map[string]any `json:"deployments"`
		NextCursor  string           `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Deployments) != 2 {
		t.Fatalf("rows: want 2 (only owner's), got %d", len(got.Deployments))
	}
	// Newest-first: d-2 (CreatedAt +1s) is first.
	first := got.Deployments[0]
	if first["deployment_id"] != "d-2" {
		t.Errorf("first deployment_id: want d-2, got %v", first["deployment_id"])
	}
	// The d-1 row carries the seeded stats; assert via its map entry.
	second := got.Deployments[1]
	if second["order_book_id"] != "OB-1" {
		t.Errorf("order_book_id: want OB-1, got %v", second["order_book_id"])
	}
	if second["resolution"] != "1m" {
		t.Errorf("resolution: want 1m, got %v", second["resolution"])
	}
	if second["candle_count"] != float64(42) {
		t.Errorf("candle_count: want 42, got %v", second["candle_count"])
	}
	for _, d := range got.Deployments {
		if _, ok := d["user_id"]; ok {
			t.Errorf("user_id must not be exposed: got %v", d["user_id"])
		}
	}
}

func TestHandleStopDeployment(t *testing.T) {
	srv, _, ds := newDeploymentServer(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ds.rows["d-1"] = &deployment.Deployment{
		ID: "d-1", StrategyID: testStrategyID, UserID: testUserID,
		Status: deployment.StatusRunning, CreatedAt: now, UpdatedAt: now,
	}
	orch, _ := srv.liveOrch.(*fakeOrchestrator)
	rec := invoke(t, srv.handleStopDeployment, http.MethodPost,
		"/v1/strategies/s1/deployments/d-1/stop", "",
		map[string]string{"id": testStrategyID, "deployment_id": "d-1"})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code: want 204, got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(orch.stopped) != 1 || orch.stopped[0] != "d-1" {
		t.Errorf("orchestrator.Stop: want [d-1], got %v", orch.stopped)
	}
}

func TestHandleDeployment_NilStore(t *testing.T) {
	srv := &Server{strategies: newFakeStrategies()}
	cases := []struct {
		name     string
		method   string
		target   string
		handler  func(http.ResponseWriter, *http.Request)
		pathVals map[string]string
	}{
		{"deploy", http.MethodPost, "/v1/strategies/s1/versions/r/deploy", srv.handleDeploy,
			map[string]string{"id": testStrategyID, "revision": "r"}},
		{"list_deployments", http.MethodGet, "/v1/strategies/s1/deployments", srv.handleListDeployments,
			map[string]string{"id": testStrategyID}},
		{"get_deployment", http.MethodGet, "/v1/strategies/s1/deployments/d-1", srv.handleGetDeployment,
			map[string]string{"id": testStrategyID, "deployment_id": "d-1"}},
		{"stop_deployment", http.MethodPost, "/v1/strategies/s1/deployments/d-1/stop", srv.handleStopDeployment,
			map[string]string{"id": testStrategyID, "deployment_id": "d-1"}},
		{"deployment_logs", http.MethodGet, "/v1/strategies/s1/deployments/d-1/logs", srv.handleDeploymentLogs,
			map[string]string{"id": testStrategyID, "deployment_id": "d-1"}},
		{"resume", http.MethodPost, "/v1/strategies/s1/deployments/d-1/resume", srv.handleResumeDeployment,
			map[string]string{"id": testStrategyID, "deployment_id": "d-1"}},
		{"restart", http.MethodPost, "/v1/strategies/s1/deployments/d-1/restart", srv.handleRestartDeployment,
			map[string]string{"id": testStrategyID, "deployment_id": "d-1"}},
		{"hotswap", http.MethodPost, "/v1/strategies/s1/deployments/d-1/hotswap", srv.handleHotSwapDeployment,
			map[string]string{"id": testStrategyID, "deployment_id": "d-1"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := invoke(t, c.handler, c.method, c.target, "", c.pathVals)
			if rec.Code != http.StatusServiceUnavailable {
				t.Errorf("%s: want 503, got %d body=%s", c.name, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestHandleDeploymentLogs_NotFound(t *testing.T) {
	srv, _, _ := newDeploymentServer(t)
	rec := invoke(t, srv.handleDeploymentLogs, http.MethodGet,
		"/v1/strategies/s1/deployments/missing", "",
		map[string]string{"id": testStrategyID, "deployment_id": "missing"})
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
}

func TestHandleDeploymentLogs_Success(t *testing.T) {
	srv, _, ds := newDeploymentServer(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ds.rows["d-1"] = &deployment.Deployment{
		ID: "d-1", StrategyID: testStrategyID, UserID: testUserID,
		Status: deployment.StatusRunning, CreatedAt: now, UpdatedAt: now,
	}
	rec := invoke(t, srv.handleDeploymentLogs, http.MethodGet,
		"/v1/strategies/s1/deployments/d-1", "",
		map[string]string{"id": testStrategyID, "deployment_id": "d-1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var got struct {
		Logs []map[string]any `json:"logs"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Logs == nil {
		t.Errorf("logs: want [], got nil")
	}
}

func TestHandleGetDeployment_StoreUnavailable_503(t *testing.T) {
	srv := &Server{strategies: newFakeStrategies()}
	rec := invoke(t, srv.handleGetDeployment, http.MethodGet,
		"/v1/strategies/s1/deployments/d-1", "",
		map[string]string{"id": testStrategyID, "deployment_id": "d-1"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("code: want 503, got %d", rec.Code)
	}
}

func TestHandleResumeDeployment_Success(t *testing.T) {
	srv, _, _ := newDeploymentServer(t)
	orch, _ := srv.liveOrch.(*fakeOrchestrator)
	rec := invoke(t, srv.handleResumeDeployment, http.MethodPost,
		"/v1/strategies/s1/deployments/d-1/resume", "",
		map[string]string{"id": testStrategyID, "deployment_id": "d-1"})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code: want 204, got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(orch.resumed) != 1 || orch.resumed[0] != "d-1" {
		t.Errorf("orchestrator.Resume: want [d-1], got %v", orch.resumed)
	}
}

func TestHandleRestartDeployment_Success(t *testing.T) {
	srv, _, _ := newDeploymentServer(t)
	orch, _ := srv.liveOrch.(*fakeOrchestrator)
	rec := invoke(t, srv.handleRestartDeployment, http.MethodPost,
		"/v1/strategies/s1/deployments/d-1/restart", "",
		map[string]string{"id": testStrategyID, "deployment_id": "d-1"})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code: want 204, got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(orch.resumed) != 1 || orch.resumed[0] != "d-1" {
		t.Errorf("orchestrator.Resume: want [d-1], got %v", orch.resumed)
	}
}

func TestHandleHotSwapDeployment_RequiresRevision(t *testing.T) {
	srv, _, _ := newDeploymentServer(t)
	rec := invoke(t, srv.handleHotSwapDeployment, http.MethodPost,
		"/v1/strategies/s1/deployments/d-1/hotswap", `{}`,
		map[string]string{"id": testStrategyID, "deployment_id": "d-1"})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("code: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleHotSwapDeployment_Success(t *testing.T) {
	srv, _, _ := newDeploymentServer(t)
	orch, _ := srv.liveOrch.(*fakeOrchestrator)
	rec := invoke(t, srv.handleHotSwapDeployment, http.MethodPost,
		"/v1/strategies/s1/deployments/d-1/hotswap", `{"revision_id":"v2"}`,
		map[string]string{"id": testStrategyID, "deployment_id": "d-1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(orch.hotSwapped) != 1 || orch.hotSwapped[0] != "d-1:v2" {
		t.Errorf("orchestrator.HotSwap: want [d-1:v2], got %v", orch.hotSwapped)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["revision"] != "v2" {
		t.Errorf("revision: want v2, got %v", got["revision"])
	}
	if got["status"] != string(deployment.StatusRunning) {
		t.Errorf("status: want running, got %v", got["status"])
	}
}

func TestHandleStopDeployment_OrchestratorError_500(t *testing.T) {
	srv, _, ds := newDeploymentServer(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ds.rows["d-1"] = &deployment.Deployment{
		ID: "d-1", StrategyID: testStrategyID, UserID: testUserID,
		Status: deployment.StatusRunning, CreatedAt: now, UpdatedAt: now,
	}
	srv.liveOrch = &fakeOrchestrator{stopErr: context.Canceled}
	rec := invoke(t, srv.handleStopDeployment, http.MethodPost,
		"/v1/strategies/s1/deployments/d-1/stop", "",
		map[string]string{"id": testStrategyID, "deployment_id": "d-1"})
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("code: want 500, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleDeploy_RejectsEmptyOrderBookID(t *testing.T) {
	srv, strats, ds := newDeploymentServer(t)
	seedDeploymentVersion(strats, "rev-1", "wasm-ref", "manifest-ref")

	rec := invoke(t, srv.handleDeploy, http.MethodPost,
		"/v1/strategies/s1/versions/rev-1/deploy",
		`{"resolution":"1m"}`,
		map[string]string{"id": testStrategyID, "revision": "rev-1"})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "order_book_id is required") {
		t.Errorf("body: want order_book_id error, got %q", rec.Body.String())
	}
	if len(ds.created) != 0 {
		t.Errorf("Create must not run on invalid input: got %d", len(ds.created))
	}
}

func TestHandleDeploy_RejectsEmptyResolution(t *testing.T) {
	srv, strats, ds := newDeploymentServer(t)
	seedDeploymentVersion(strats, "rev-1", "wasm-ref", "manifest-ref")

	rec := invoke(t, srv.handleDeploy, http.MethodPost,
		"/v1/strategies/s1/versions/rev-1/deploy",
		`{"order_book_id":"OB-1"}`,
		map[string]string{"id": testStrategyID, "revision": "rev-1"})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "resolution is required") {
		t.Errorf("body: want resolution error, got %q", rec.Body.String())
	}
	if len(ds.created) != 0 {
		t.Errorf("Create must not run on invalid input: got %d", len(ds.created))
	}
}

// TestHandleDeploy_DeployError_SurfacesCauseInBody guards the
// response-body contract: when the orchestrator's Deploy returns
// an error (e.g. "orchestrator: nil WsBroker" when the live broker
// isn't initialised), the handler must surface the cause in the
// HTTP response body so the LLM (and through it, the user) gets
// a specific reason. Pre-fix the body was just "failed to start
// deployment" with no cause — the LLM could only relay a generic
// message and the operator (who can't see the LLM conversation)
// had no way to diagnose why the deploy failed.
//
// Operator visibility (the slog.Error line) is the orchestrator's
// responsibility: internal/orchestrator/lifecycle.go's Deploy
// method logs the same error at ERROR level with the deployment
// context, so every caller (this handler, the deploy_strategy
// LLM tool, Recover) gets the log for free. The test for that
// contract lives in internal/orchestrator/lifecycle_test.go.
func TestHandleDeploy_DeployError_SurfacesCauseInBody(t *testing.T) {
	srv, strats, _ := newDeploymentServer(t)
	seedDeploymentVersion(strats, "rev-1", "wasm-ref", "manifest-ref")

	wantErr := errors.New("orchestrator: nil WsBroker")
	if f, ok := srv.liveOrch.(*fakeOrchestrator); ok {
		f.deployErr = wantErr
	} else {
		t.Fatal("expected fakeOrchestrator")
	}

	rec := invoke(t, srv.handleDeploy, http.MethodPost,
		"/v1/strategies/s1/versions/rev-1/deploy",
		`{"order_book_id":"OB-1","resolution":"1m"}`,
		map[string]string{"id": testStrategyID, "revision": "rev-1"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("code: want 500, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), wantErr.Error()) {
		t.Errorf("response body should include cause %q; got %q", wantErr.Error(), rec.Body.String())
	}
	if !strings.HasPrefix(rec.Body.String(), "failed to start deployment: ") {
		t.Errorf("response body should start with the standard prefix; got %q", rec.Body.String())
	}
}

func TestHandleDeploy_RejectsWarmupCandlesOutOfRange(t *testing.T) {
	for _, wc := range []int{-1, backtest.MaxWarmupCandles + 1} {
		srv, strats, ds := newDeploymentServer(t)
		seedDeploymentVersion(strats, "rev-1", "wasm-ref", "manifest-ref")

		rec := invoke(t, srv.handleDeploy, http.MethodPost,
			"/v1/strategies/s1/versions/rev-1/deploy",
			fmt.Sprintf(`{"order_book_id":"OB-1","resolution":"1m","warmup_candles":%d}`, wc),
			map[string]string{"id": testStrategyID, "revision": "rev-1"})

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("warmup_candles=%d: want 400, got %d body=%s", wc, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), fmt.Sprintf("warmup_candles must be between 0 and %d", backtest.MaxWarmupCandles)) {
			t.Errorf("warmup_candles=%d: body=%q", wc, rec.Body.String())
		}
		if len(ds.created) != 0 {
			t.Errorf("warmup_candles=%d: Create must not run on invalid input", wc)
		}
	}
}
