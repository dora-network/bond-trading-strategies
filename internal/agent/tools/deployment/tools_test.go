package deployment_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/internal/agent/backtest"

	"github.com/dora-network/bond-trading-strategies/internal/agent/deployment"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/migration"
	"github.com/dora-network/bond-trading-strategies/internal/agent/orchestrator"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies/servertest"
	deploymenttool "github.com/dora-network/bond-trading-strategies/internal/agent/tools/deployment"
)

// fakeDeploymentStore is an in-memory deployment.Store for tool-level
// tests. The deployment_pgstore tests cover the SQL layer; here we
// only need to validate the tool's input parsing, ownership gates,
// and JSON envelope shape.
type fakeDeploymentStore struct {
	mu      sync.Mutex
	byID    map[string]deployment.Deployment
	byStrat map[string][]string // strategyID -> deploymentIDs (insertion order)
	active  map[string]bool     // strategyID -> has running
}

func newFakeDeploymentStore() *fakeDeploymentStore {
	return &fakeDeploymentStore{
		byID:    map[string]deployment.Deployment{},
		byStrat: map[string][]string{},
		active:  map[string]bool{},
	}
}

func (f *fakeDeploymentStore) Create(_ context.Context, d deployment.Deployment) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.byID[d.ID]; exists {
		return errors.New("fake: duplicate id")
	}
	f.byID[d.ID] = d
	f.byStrat[d.StrategyID] = append(f.byStrat[d.StrategyID], d.ID)
	if d.Status == deployment.StatusRunning {
		f.active[d.StrategyID] = true
	}
	return nil
}

func (f *fakeDeploymentStore) Get(_ context.Context, id, userID string) (deployment.Deployment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.byID[id]
	if !ok || d.UserID != userID {
		return deployment.Deployment{}, deployment.ErrNotFound
	}
	return d, nil
}

func (f *fakeDeploymentStore) List(_ context.Context, strategyID, userID string, _ deployment.Page) ([]deployment.Deployment, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []deployment.Deployment
	if strategyID == "" {
		for _, d := range f.byID {
			if d.UserID == userID {
				out = append(out, d)
			}
		}
	} else {
		for _, id := range f.byStrat[strategyID] {
			d := f.byID[id]
			if d.UserID == userID {
				out = append(out, d)
			}
		}
	}
	return out, "", nil
}

func (f *fakeDeploymentStore) UpdateStatus(_ context.Context, id, userID string, status deployment.Status, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.byID[id]
	if !ok || d.UserID != userID {
		return deployment.ErrNotFound
	}
	if d.Status == deployment.StatusRunning && status != deployment.StatusRunning {
		delete(f.active, d.StrategyID)
	}
	d.Status = status
	f.byID[id] = d
	return nil
}

func (f *fakeDeploymentStore) SetInstance(_ context.Context, id, userID, instanceID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.byID[id]
	if !ok || d.UserID != userID {
		return deployment.ErrNotFound
	}
	d.InstanceID = instanceID
	f.byID[id] = d
	return nil
}

func (f *fakeDeploymentStore) HotSwap(_ context.Context, id, userID, newRevision string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.byID[id]
	if !ok || d.UserID != userID {
		return deployment.ErrNotFound
	}
	d.HotswappedAt = nil
	d.HotswappedFromRev = d.Revision
	d.Revision = newRevision
	f.byID[id] = d
	return nil
}

func (f *fakeDeploymentStore) IncRestart(_ context.Context, id, userID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.byID[id]
	if !ok || d.UserID != userID {
		return deployment.ErrNotFound
	}
	d.RestartCount++
	f.byID[id] = d
	return nil
}

func (f *fakeDeploymentStore) SetCandleCount(_ context.Context, id, userID string, n int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.byID[id]
	if !ok || d.UserID != userID {
		return deployment.ErrNotFound
	}
	d.CandleCount = n
	f.byID[id] = d
	return nil
}

func (f *fakeDeploymentStore) UpdateWarmupCandles(_ context.Context, id, userID string, warmupCandles int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.byID[id]
	if !ok || d.UserID != userID {
		return deployment.ErrNotFound
	}
	d.WarmupCandles = warmupCandles
	f.byID[id] = d
	return nil
}

func (f *fakeDeploymentStore) HasActive(_ context.Context, strategyID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active[strategyID], nil
}

func (f *fakeDeploymentStore) ListRunning(_ context.Context) ([]deployment.Deployment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []deployment.Deployment
	for _, d := range f.byID {
		if d.Status == deployment.StatusRunning {
			out = append(out, d)
		}
	}
	return out, nil
}

// fakeOrchestrator records the calls made to the live runtime so the
// tests can assert the tool wires the right args through. The real
// *orchestrator.Orchestrator satisfies the same shape via the
// tools.Orchestrator interface.
type fakeOrchestrator struct {
	mu sync.Mutex

	deployErr    error
	deployCalls  []deployCall
	stopErr      error
	stopCalls    []stopCall
	resumeErr    error
	resumeCalls  []resumeCall
	hotSwapErr   error
	hotSwapCalls []hotSwapCall
}

type (
	deployCall  struct{ DeploymentID, StrategyID, Revision, UserID, OrderBookID, WasmRef, ManifestHash string }
	stopCall    struct{ DeploymentID, UserID string }
	resumeCall  struct{ DeploymentID, UserID, DoraAPIKey string }
	hotSwapCall struct {
		DeploymentID, UserID, NewRevision, NewWasmRef, NewManifestHash, DoraAPIKey, OrderBookID string
	}
)

func newFakeOrchestrator() *fakeOrchestrator { return &fakeOrchestrator{} }
func (f *fakeOrchestrator) Deploy(_ context.Context, cfg orchestrator.DeployConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deployCalls = append(f.deployCalls, deployCall{
		DeploymentID: cfg.DeploymentID, StrategyID: cfg.StrategyID,
		Revision: cfg.Revision, UserID: cfg.UserID,
		OrderBookID: cfg.OrderBookID, WasmRef: cfg.WasmRef, ManifestHash: cfg.ManifestHash,
	})
	return f.deployErr
}

// fakeDeployConfig mirrors orchestrator.DeployConfig's field set so we
// don't import the package (which would couple the test to a private
// type signature).
func (f *fakeOrchestrator) Stop(_ context.Context, id, userID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopCalls = append(f.stopCalls, stopCall{DeploymentID: id, UserID: userID})
	return f.stopErr
}

func (f *fakeOrchestrator) Resume(_ context.Context, id, userID, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumeCalls = append(f.resumeCalls, resumeCall{DeploymentID: id, UserID: userID, DoraAPIKey: key})
	return f.resumeErr
}

func (f *fakeOrchestrator) HotSwap(_ context.Context, id, userID, newRev, newWasm, newManifest, key, obID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hotSwapCalls = append(f.hotSwapCalls, hotSwapCall{
		DeploymentID: id, UserID: userID, NewRevision: newRev,
		NewWasmRef: newWasm, NewManifestHash: newManifest,
		DoraAPIKey: key, OrderBookID: obID,
	})
	return f.hotSwapErr
}

func (f *fakeOrchestrator) Stats(_ string) (orchestrator.DeploymentStats, bool) {
	return orchestrator.DeploymentStats{}, false
}

// --- tests ---

// testUserID is the user the tool calls are bound to. Tests run all
// calls under this id; the wrong-owner test seeds a row under a
// different user.
const testUserID = "user-A"

func TestTools_SpecCountAndNames(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	specs, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	if got, want := len(specs), 8; got != want {
		t.Fatalf("specs: got %d want %d", got, want)
	}
	if got, want := len(handlers), 8; got != want {
		t.Fatalf("handlers: got %d want %d", got, want)
	}
	want := []string{
		"deploy_strategy", "list_deployments", "get_deployment_status",
		"stop_deployment", "resume_deployment", "restart_deployment",
		"hotswap_deployment", "get_deployment_logs",
	}
	for i, name := range want {
		if specs[i].Name != name {
			t.Errorf("specs[%d].Name: got %q want %q", i, specs[i].Name, name)
		}
		if _, ok := handlers[name]; !ok {
			t.Errorf("handlers missing %q", name)
		}
	}
}

func TestTools_AllSchemasAreObjectsWithRequired(t *testing.T) {
	specs, _ := deploymenttool.Tools(newFakeDeploymentStore(), newFakeOrchestrator(), newFakeVersions(), testUserID, "k", migration.New())
	for _, s := range specs {
		// Each schema must parse as a JSON object with a "required" array.
		// LLM tool dispatch fails loudly if a schema is malformed; this
		// guards against hand-edits that drop the "type":"object" tag.
		var v struct {
			Type     string   `json:"type"`
			Required []string `json:"required"`
		}
		if err := json.Unmarshal(s.JSONSchema, &v); err != nil {
			t.Errorf("%s: schema JSON invalid: %v", s.Name, err)
			continue
		}
		if v.Type != "object" {
			t.Errorf("%s: schema.type: got %q want %q", s.Name, v.Type, "object")
		}
		if v.Required == nil {
			t.Errorf("%s: schema.required must be present (even if empty)", s.Name)
		}
	}
}

func TestDeploy_HappyPath(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	vs.addVersion("strat-1", "rev-1", "wasm-hash", "manifest-hash")
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())

	in := map[string]any{
		"strategy_id": "strat-1", "revision_id": "rev-1", "order_book_id": "ob-1",
		"resolution": "1m", "warmup_candles": 90,
		"params": map[string]string{"stop_loss_pct": "0.05"},
	}
	raw, _ := json.Marshal(in)
	out, err := handlers["deploy_strategy"](t.Context(), "deploy_strategy", raw)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	var env struct {
		DeploymentID string `json:"deployment_id"`
		Status       string `json:"status"`
	}
	if err := json.Unmarshal(out, &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Status != "running" {
		t.Errorf("status: got %q want running", env.Status)
	}
	if env.DeploymentID == "" {
		t.Errorf("deployment_id is empty")
	}
	// The fake store should now have the row + the active flag set.
	row, err := store.Get(t.Context(), env.DeploymentID, testUserID)
	if err != nil {
		t.Errorf("created row not retrievable: %v", err)
	}
	if row.WarmupCandles != 90 {
		t.Errorf("warmup_candles: want 90, got %d", row.WarmupCandles)
	}
	active, _ := store.HasActive(t.Context(), "strat-1")
	if !active {
		t.Errorf("strategy should be marked active")
	}
	if len(orch.deployCalls) != 1 {
		t.Fatalf("orchestrator.Deploy calls: got %d want 1", len(orch.deployCalls))
	}
	if orch.deployCalls[0].StrategyID != "strat-1" {
		t.Errorf("Deploy strategy_id: got %q", orch.deployCalls[0].StrategyID)
	}
}

func TestDeploy_AlreadyRunningError(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	vs.addVersion("strat-1", "rev-1", "wasm-hash", "manifest-hash")
	// Pre-seed a running row.
	_ = store.Create(t.Context(), deployment.Deployment{
		ID: "existing", StrategyID: "strat-1", Revision: "rev-0", UserID: testUserID,
		Status: deployment.StatusRunning,
	})
	_ = store.UpdateStatus(t.Context(), "existing", testUserID, deployment.StatusRunning, "")

	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	raw, _ := json.Marshal(map[string]any{
		"strategy_id": "strat-1", "revision_id": "rev-1", "order_book_id": "ob-1",
		"resolution": "1m",
	})
	_, err := handlers["deploy_strategy"](t.Context(), "deploy_strategy", raw)
	if err == nil {
		t.Fatal("expected error for already-running strategy")
	}
	if !errors.Is(err, deployment.ErrAlreadyRunning) {
		t.Errorf("expected ErrAlreadyRunning, got %v", err)
	}
}

func TestDeploy_UnknownStrategyHintsGetStrategy(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	raw, _ := json.Marshal(map[string]any{
		"strategy_id": "missing", "revision_id": "rev-1", "order_book_id": "ob-1",
		"resolution": "1m",
	})
	_, err := handlers["deploy_strategy"](t.Context(), "deploy_strategy", raw)
	if err == nil {
		t.Fatal("expected error for unknown strategy")
	}
	// The tool should surface a recovery hint pointing at get_strategy
	// so the LLM doesn't retry the same dead id. We assert the hint
	// string is in the error message rather than asserting the concrete
	// llm.RecoveryError type (the tools package shouldn't leak the
	// llm error sentinel into test contracts).
	if !strings.Contains(err.Error(), "get_strategy") {
		t.Errorf("error should hint get_strategy, got %q", err.Error())
	}
}

func TestDeploy_MissingOrderBookID(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	vs.addVersion("strat-1", "rev-1", "wasm-hash", "manifest-hash")
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	raw, _ := json.Marshal(map[string]any{
		"strategy_id": "strat-1", "revision_id": "rev-1",
	})
	_, err := handlers["deploy_strategy"](t.Context(), "deploy_strategy", raw)
	if err == nil || !strings.Contains(err.Error(), "order_book_id") {
		t.Fatalf("expected order_book_id error, got %v", err)
	}
}

func TestList_AllAndFiltered(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	_ = store.Create(t.Context(), deployment.Deployment{
		ID: "a", StrategyID: "s1", UserID: testUserID, Status: deployment.StatusStopped,
	})
	_ = store.Create(t.Context(), deployment.Deployment{
		ID: "b", StrategyID: "s2", UserID: testUserID, Status: deployment.StatusRunning,
	})
	_ = store.Create(t.Context(), deployment.Deployment{
		ID: "c", StrategyID: "s1", UserID: "other-user", Status: deployment.StatusStopped,
	})

	// Filtered by strategy_id.
	raw, _ := json.Marshal(map[string]any{"strategy_id": "s1"})
	out, err := handlers["list_deployments"](t.Context(), "list_deployments", raw)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var env struct {
		Deployments []map[string]any `json:"deployments"`
	}
	_ = json.Unmarshal(out, &env)
	if len(env.Deployments) != 1 {
		t.Errorf("filtered list: got %d want 1 (owner's only)", len(env.Deployments))
	}

	// Unfiltered — owner gate still applies, other-user rows are filtered.
	raw, _ = json.Marshal(map[string]any{})
	out, err = handlers["list_deployments"](t.Context(), "list_deployments", raw)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	env.Deployments = nil
	_ = json.Unmarshal(out, &env)
	if len(env.Deployments) != 2 {
		t.Errorf("unfiltered list: got %d want 2", len(env.Deployments))
	}
}

func TestGetStatus_HappyPath(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	_ = store.Create(t.Context(), deployment.Deployment{
		ID: "a", StrategyID: "s1", Revision: "rev-1", UserID: testUserID,
		Status: deployment.StatusRunning, RestartCount: 2,
	})
	raw, _ := json.Marshal(map[string]any{"deployment_id": "a"})
	out, err := handlers["get_deployment_status"](t.Context(), "get_deployment_status", raw)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	var env struct {
		DeploymentID string   `json:"deployment_id"`
		Status       string   `json:"status"`
		RestartCount int      `json:"restart_count"`
		RecentEvents []string `json:"recent_events"`
	}
	if err := json.Unmarshal(out, &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Status != "running" || env.RestartCount != 2 {
		t.Errorf("envelope mismatch: %+v", env)
	}
	// recent_events must always be a JSON array (never null) so the
	// LLM can iterate without a nil check.
	if env.RecentEvents == nil {
		t.Errorf("recent_events must be an empty array, not null")
	}
}

func TestGetStatus_NotFound(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	raw, _ := json.Marshal(map[string]any{"deployment_id": "missing"})
	_, err := handlers["get_deployment_status"](t.Context(), "get_deployment_status", raw)
	if err == nil || !errors.Is(err, deployment.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestGetStatus_WrongOwner(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	_ = store.Create(t.Context(), deployment.Deployment{
		ID: "a", StrategyID: "s1", UserID: "other-user", Status: deployment.StatusRunning,
	})
	raw, _ := json.Marshal(map[string]any{"deployment_id": "a"})
	_, err := handlers["get_deployment_status"](t.Context(), "get_deployment_status", raw)
	if err == nil || !errors.Is(err, deployment.ErrNotFound) {
		t.Errorf("expected ErrNotFound for wrong-owner, got %v", err)
	}
}

func TestStop_PassesThrough(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	_ = store.Create(t.Context(), deployment.Deployment{
		ID: "a", StrategyID: "s1", UserID: testUserID, Status: deployment.StatusRunning,
	})
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	raw, _ := json.Marshal(map[string]any{"deployment_id": "a"})
	out, err := handlers["stop_deployment"](t.Context(), "stop_deployment", raw)
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
	if len(orch.stopCalls) != 1 || orch.stopCalls[0].DeploymentID != "a" {
		t.Errorf("stop not routed to orchestrator: %+v", orch.stopCalls)
	}
	var env struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(out, &env)
	if env.Status != "stopped" {
		t.Errorf("status: got %q want stopped", env.Status)
	}
}

func TestResumeAndRestart_PassThrough(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())

	for _, name := range []string{"resume_deployment", "restart_deployment"} {
		raw, _ := json.Marshal(map[string]any{"deployment_id": "a"})
		if _, err := handlers[name](t.Context(), name, raw); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if len(orch.resumeCalls) != 2 {
		t.Errorf("resume calls: got %d want 2 (one per tool)", len(orch.resumeCalls))
	}
	for _, c := range orch.resumeCalls {
		if c.DoraAPIKey != "dora-key" {
			t.Errorf("dora API key not threaded: got %q", c.DoraAPIKey)
		}
	}
}

func TestHotSwap_PassThrough(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	vs.addVersion("s1", "rev-2", "wasm-2", "manifest-2")
	_ = store.Create(t.Context(), deployment.Deployment{
		ID: "a", StrategyID: "s1", Revision: "rev-1", UserID: testUserID, Status: deployment.StatusRunning,
	})
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	raw, _ := json.Marshal(map[string]any{"deployment_id": "a", "revision_id": "rev-2"})
	out, err := handlers["hotswap_deployment"](t.Context(), "hotswap_deployment", raw)
	if err != nil {
		t.Fatalf("hotswap: %v", err)
	}
	if len(orch.hotSwapCalls) != 1 {
		t.Fatalf("hotswap calls: got %d want 1", len(orch.hotSwapCalls))
	}
	if orch.hotSwapCalls[0].NewRevision != "rev-2" || orch.hotSwapCalls[0].NewWasmRef != "wasm-2" {
		t.Errorf("hotswap args: %+v", orch.hotSwapCalls[0])
	}
	var env struct {
		Revision string `json:"revision"`
		Status   string `json:"status"`
	}
	_ = json.Unmarshal(out, &env)
	if env.Revision != "rev-2" || env.Status != "running" {
		t.Errorf("hotswap envelope: %+v", env)
	}
}

func TestLogs_OwnershipGate(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	_ = store.Create(t.Context(), deployment.Deployment{
		ID: "a", StrategyID: "s1", UserID: testUserID, Status: deployment.StatusRunning,
	})
	raw, _ := json.Marshal(map[string]any{"deployment_id": "a"})
	out, err := handlers["get_deployment_logs"](t.Context(), "get_deployment_logs", raw)
	if err != nil {
		t.Fatalf("logs: %v", err)
	}
	var env struct {
		Logs []string `json:"logs"`
	}
	if err := json.Unmarshal(out, &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Logs == nil {
		t.Errorf("logs must be an empty array, not null")
	}

	// Wrong-owner must surface as not-found.
	raw, _ = json.Marshal(map[string]any{"deployment_id": "other"})
	_, err = handlers["get_deployment_logs"](t.Context(), "get_deployment_logs", raw)
	if err == nil || !errors.Is(err, deployment.ErrNotFound) {
		t.Errorf("expected ErrNotFound for wrong-owner logs, got %v", err)
	}
}

func TestDeploy_MissingResolution(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	vs.addVersion("strat-1", "rev-1", "wasm-hash", "manifest-hash")
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	raw, _ := json.Marshal(map[string]any{
		"strategy_id": "strat-1", "revision_id": "rev-1", "order_book_id": "ob-1",
	})
	_, err := handlers["deploy_strategy"](t.Context(), "deploy_strategy", raw)
	if err == nil || !strings.Contains(err.Error(), "resolution is required") {
		t.Fatalf("expected resolution-required error, got %v", err)
	}
}

func TestDeploy_WarmupCandlesOutOfRange(t *testing.T) {
	store := newFakeDeploymentStore()
	orch := newFakeOrchestrator()
	vs := newFakeVersions()
	vs.addVersion("strat-1", "rev-1", "wasm-hash", "manifest-hash")
	_, handlers := deploymenttool.Tools(store, orch, vs, testUserID, "dora-key", migration.New())
	for _, wc := range []int{-1, backtest.MaxWarmupCandles + 1} {
		raw, _ := json.Marshal(map[string]any{
			"strategy_id": "strat-1", "revision_id": "rev-1", "order_book_id": "ob-1",
			"resolution": "1m", "warmup_candles": wc,
		})
		_, err := handlers["deploy_strategy"](t.Context(), "deploy_strategy", raw)
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("warmup_candles must be between 0 and %d", backtest.MaxWarmupCandles)) {
			t.Fatalf("warmup_candles=%d: expected bounds error, got %v", wc, err)
		}
	}
}

// seedDeployFixtures provisions a Postgres, the owner user, and a strategy
// (no version) for the stale-framework recovery test.
func seedDeployFixtures(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	pool := servertest.StartPostgres(t)
	seedDeployUser(t, pool, deployOwnerID)
	strategyID := uuid.NewString()
	_, err := pool.Exec(t.Context(), `
		insert into agent.strategies (id, dora_user_id, name, source_session_id)
		values ($1, $2, 'alpha', $3)`,
		strategyID, deployOwnerID, uuid.NewString())
	require.NoError(t, err)
	return pool, strategyID
}

const deployOwnerID = "00000000-0000-0000-0000-0000000000e3"

func seedDeployUser(t *testing.T, pool *pgxpool.Pool, userID string) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `
		insert into agent.users (dora_user_id, tenant_id, roles)
		values ($1, 'test', ARRAY['TRADER'])
		on conflict (dora_user_id) do nothing`,
		userID)
	require.NoError(t, err)
}

// insertLegacyDeployVersion inserts a strategy_version row with the
// pre-WASM-migration shape (target='go-docker', no wasm_ref).
func insertLegacyDeployVersion(t *testing.T, pool *pgxpool.Pool, strategyID string) string {
	t.Helper()
	rev := uuid.NewString()
	_, err := pool.Exec(t.Context(), `
		insert into agent.strategy_versions (revision, strategy_id, provider, model,
		    module_name, summary, rationale, validation, target)
		values ($1, $2, 'openai', 'gpt', 'legacy', 's', 'r', '{}'::jsonb, 'go-docker')`,
		rev, strategyID)
	require.NoError(t, err)
	return rev
}

// TestDeployStrategy_StaleFramework_Recovery: deploying a legacy (go-docker)
// version must surface a RecoveryError pointing at generate_strategy, with the
// structured hint carrying strategy_id + current_revision_id.
func TestDeployStrategy_StaleFramework_Recovery(t *testing.T) {
	t.Parallel()
	pool, strategyID := seedDeployFixtures(t)
	legacyRev := insertLegacyDeployVersion(t, pool, strategyID)
	versions := strategies.NewPgStore(pool)
	deployStore := deployment.NewPgStore(pool)

	_, handlers := deploymenttool.Tools(deployStore, nil, versions, deployOwnerID, "dora-key", migration.New())
	handle := handlers["deploy_strategy"]
	_, err := handle(t.Context(), "deploy_strategy", json.RawMessage(fmt.Sprintf(
		`{"strategy_id":%q,"revision_id":%q,"order_book_id":"ob","resolution":"1m"}`,
		strategyID, legacyRev,
	)))
	var rec llm.RecoveryError
	require.ErrorAs(t, err, &rec)
	require.Equal(t, "generate_strategy", rec.RecoveryTool())
	require.NotNil(t, rec.Hint())
}
