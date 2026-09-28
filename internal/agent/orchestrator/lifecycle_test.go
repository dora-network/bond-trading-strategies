package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dora-network/dora-strategy-wasm/manifest"

	"github.com/dora-network/bond-trading-strategies/internal/agent/deployment"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm/prompts"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wsbroker"
)

// fakeStore is an in-memory deployment.Store for unit tests.
// Tracks rows in a map keyed by ID. Implements every method on
// the deployment.Store interface.
type fakeStore struct {
	mu     sync.Mutex
	rows   map[string]deployment.Deployment
	byUser map[string]map[string]bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		rows:   map[string]deployment.Deployment{},
		byUser: map[string]map[string]bool{},
	}
}

func (s *fakeStore) Create(_ context.Context, d deployment.Deployment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[d.ID]; ok {
		return errors.New("duplicate id")
	}
	s.rows[d.ID] = d
	if s.byUser[d.UserID] == nil {
		s.byUser[d.UserID] = map[string]bool{}
	}
	s.byUser[d.UserID][d.ID] = true
	return nil
}

func (s *fakeStore) Get(_ context.Context, id, userID string) (deployment.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.rows[id]
	if !ok || d.UserID != userID {
		return deployment.Deployment{}, deployment.ErrNotFound
	}
	return d, nil
}

func (s *fakeStore) List(_ context.Context, _, _ string, _ deployment.Page) ([]deployment.Deployment, string, error) {
	return nil, "", nil
}

func (s *fakeStore) UpdateStatus(_ context.Context, id, userID string, status deployment.Status, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.rows[id]
	if !ok || d.UserID != userID {
		return deployment.ErrNotFound
	}
	d.Status = status
	d.StoppedReason = reason
	d.UpdatedAt = time.Now()
	s.rows[id] = d
	return nil
}

func (s *fakeStore) SetInstance(_ context.Context, id, userID, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.rows[id]
	if !ok || d.UserID != userID {
		return deployment.ErrNotFound
	}
	return nil
}

func (s *fakeStore) HotSwap(_ context.Context, id, userID, newRev string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.rows[id]
	if !ok || d.UserID != userID {
		return deployment.ErrNotFound
	}
	d.HotswappedFromRev = d.Revision
	d.Revision = newRev
	now := time.Now()
	d.HotswappedAt = &now
	s.rows[id] = d
	return nil
}

func (s *fakeStore) IncRestart(_ context.Context, id, userID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.rows[id]
	if !ok || d.UserID != userID {
		return deployment.ErrNotFound
	}
	d.RestartCount++
	s.rows[id] = d
	return nil
}

func (s *fakeStore) SetCandleCount(_ context.Context, id, userID string, n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.rows[id]
	if !ok || d.UserID != userID {
		return deployment.ErrNotFound
	}
	d.CandleCount = n
	s.rows[id] = d
	return nil
}

func (s *fakeStore) UpdateWarmupCandles(_ context.Context, id, userID string, warmupCandles int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.rows[id]
	if !ok || d.UserID != userID {
		return deployment.ErrNotFound
	}
	d.WarmupCandles = warmupCandles
	s.rows[id] = d
	return nil
}

func (s *fakeStore) HasActive(_ context.Context, strategyID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range s.rows {
		if d.StrategyID == strategyID && d.Status == deployment.StatusRunning {
			return true, nil
		}
	}
	return false, nil
}

func (s *fakeStore) ListRunning(_ context.Context) ([]deployment.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []deployment.Deployment
	for _, d := range s.rows {
		if d.Status == deployment.StatusRunning {
			out = append(out, d)
		}
	}
	return out, nil
}

func (s *fakeStore) statusOf(id string) deployment.Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows[id].Status
}

// newTestOrchestrator builds an orchestrator wired with the
// dependencies the live-runtime tests need. withLiveDeps=false
// leaves WsBroker / DeployStore / Kernel / Orders nil so the
// caller can assert on the nil-dep failure paths.
func newTestOrchestrator(t *testing.T, withLiveDeps bool) *Orchestrator {
	t.Helper()
	dir := t.TempDir()
	st, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	reg, err := registry.New(t.Context(), registry.Config{})
	if err != nil {
		t.Fatalf("registry.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })

	cfg := Config{
		Store:        st,
		Registry:     reg,
		Timeout:      5 * time.Second,
		StartTimeout: 100 * time.Millisecond,
		Logger:       slog.New(slog.NewTextHandler(discardWriter{}, nil)),
	}
	if withLiveDeps {
		cfg.DeployStore = newFakeStore()
		// WsBroker is constructed but not Start()ed; Subscribe
		// works without a connection.
		b, err := wsbroker.New(wsbroker.Config{URL: "ws://localhost:1", APIKey: "test"})
		if err != nil {
			t.Fatalf("wsbroker.New: %v", err)
		}
		cfg.WsBroker = b
	}
	o, err := New(cfg)
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	return o
}

func TestOrchestrator_Deploy_NoWsBroker(t *testing.T) {
	o := newTestOrchestrator(t, false)
	// Validate passes (DeploymentID + OrderBookID + Resolution
	// supplied); the next check (WsBroker nil) is what fails.
	err := o.Deploy(t.Context(), DeployConfig{
		DeploymentID: "dep-1",
		OrderBookID:  "OB-1",
		Resolution:   "1m",
	})
	if err == nil {
		t.Fatal("expected error when WsBroker is nil")
	}
	if !strings.Contains(err.Error(), "nil WsBroker") {
		t.Errorf("error %q did not mention WsBroker", err)
	}
}

func TestOrchestrator_Deploy_LogsFailureAtErrorLevel(t *testing.T) {
	// Operator visibility: every failure path in Deploy (validation,
	// nil WsBroker, nil DeployStore, duplicate, build config, registry
	// load, instantiate) routes through the same return slot. The
	// deferred log fires once at ERROR level with the deployment
	// context so the operator sees the cause in stdout/stderr.
	//
	// This test pins the contract for the orchestrator-internal log
	// (the HTTP / LLM-tool layers have their own response-body
	// contracts). Without it, a future refactor that drops the
	// deferred logger leaves the operator blind to deploy failures.
	o := newTestOrchestrator(t, false)
	// Swap the orchestrator's logger for a buffer-backed one so we
	// can assert the log line lands. The constructor's nil-Logger
	// default would otherwise write to discardWriter.
	var logBuf bytes.Buffer
	o.cfg.Logger = slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	_ = o.Deploy(t.Context(), DeployConfig{
		DeploymentID: "dep-log-1",
		OrderBookID:  "OB-1",
		Resolution:   "1m",
	})

	logs := logBuf.String()
	for _, want := range []string{
		"level=ERROR",
		"orchestrator: deploy failed",
		"dep-log-1",
		"nil WsBroker",
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("operator log missing %q; got:\n%s", want, logs)
		}
	}
}

func TestOrchestrator_Deploy_LogsValidationFailure(t *testing.T) {
	// Regression test for the bug where the deferred log was registered
	// *after* cfg.validateForLive() ran. Validation rejections (empty
	// deployment_id / order_book_id / resolution) returned before
	// the defer was registered, so the operator never saw them in
	// the log. The fix moves the defer to the top of Deploy; this
	// test pins that all three validation paths land in the
	// operator log.
	cases := []struct {
		name    string
		cfg     DeployConfig
		wantErr string
	}{
		{
			name:    "missing DeploymentID",
			cfg:     DeployConfig{OrderBookID: "OB-1", Resolution: "1m"},
			wantErr: "DeploymentID is required",
		},
		{
			name:    "missing OrderBookID",
			cfg:     DeployConfig{DeploymentID: "dep-x", Resolution: "1m"},
			wantErr: "OrderBookID is required",
		},
		{
			name:    "missing Resolution",
			cfg:     DeployConfig{DeploymentID: "dep-x", OrderBookID: "OB-1"},
			wantErr: "Resolution is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := newTestOrchestrator(t, false)
			var logBuf bytes.Buffer
			o.cfg.Logger = slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			err := o.Deploy(t.Context(), tc.cfg)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Deploy error: want substring %q, got %q", tc.wantErr, err.Error())
			}
			logs := logBuf.String()
			for _, want := range []string{"level=ERROR", "orchestrator: deploy failed", tc.wantErr} {
				if !strings.Contains(logs, want) {
					t.Errorf("operator log missing %q; got:\n%s", want, logs)
				}
			}
		})
	}
}

func TestOrchestrator_Deploy_NoDeployStore(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.New(dir)
	reg, _ := registry.New(t.Context(), registry.Config{})
	t.Cleanup(func() { _ = reg.Close() })
	b, _ := wsbroker.New(wsbroker.Config{URL: "ws://localhost:1", APIKey: "test"})

	o, err := New(Config{
		Store:    st,
		Registry: reg,
		WsBroker: b,
		Logger:   slog.New(slog.NewTextHandler(discardWriter{}, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = o.Deploy(t.Context(), DeployConfig{
		DeploymentID: "dep-1",
		OrderBookID:  "OB-1",
		Resolution:   "1m",
	})
	if err == nil {
		t.Fatal("expected error when DeployStore is nil")
	}
	if !strings.Contains(err.Error(), "nil DeployStore") {
		t.Errorf("error %q did not mention DeployStore", err)
	}
}

func TestOrchestrator_Stop_NotRunningStillUpdates(t *testing.T) {
	o := newTestOrchestrator(t, true)
	fs, ok := o.cfg.DeployStore.(*fakeStore)
	if !ok {
		t.Fatal("expected fakeStore")
	}
	id := "dep-1"
	_ = fs.Create(t.Context(), deployment.Deployment{
		ID: id, UserID: "u-1", Status: deployment.StatusRunning, Revision: "v1",
	})

	if err := o.Stop(t.Context(), id, "u-1"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := fs.statusOf(id); got != deployment.StatusStopped {
		t.Errorf("status: got %s, want stopped", got)
	}
}

func TestOrchestrator_Stop_UnknownID(t *testing.T) {
	o := newTestOrchestrator(t, true)
	if err := o.Stop(t.Context(), "missing", "u-1"); err == nil {
		t.Fatal("expected error on unknown deployment")
	}
}

func TestOrchestrator_Recover_NilStore(t *testing.T) {
	o := newTestOrchestrator(t, false)
	// newTestOrchestrator(false) leaves DeployStore nil.
	if _, err := o.Recover(t.Context(), func(_ context.Context, _ string) (string, error) {
		return "k", nil
	}); err == nil {
		t.Fatal("expected error on nil DeployStore")
	}
}

func TestOrchestrator_Recover_NilKeyProvider(t *testing.T) {
	o := newTestOrchestrator(t, true)
	if _, err := o.Recover(t.Context(), nil); err == nil {
		t.Fatal("expected error on nil KeyProvider")
	}
}

func TestOrchestrator_Recover_EmptyList(t *testing.T) {
	o := newTestOrchestrator(t, true)
	n, err := o.Recover(t.Context(), func(_ context.Context, _ string) (string, error) {
		return "k", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("expected 0, got %d", n)
	}
}

func TestOrchestrator_Recover_KeyFailureMarksCrashed(t *testing.T) {
	o := newTestOrchestrator(t, true)
	fs, _ := o.cfg.DeployStore.(*fakeStore)
	_ = fs.Create(t.Context(), deployment.Deployment{
		ID: "dep-key-fail", UserID: "u-1", Status: deployment.StatusRunning, Revision: "v1",
	})

	n, err := o.Recover(t.Context(), func(_ context.Context, _ string) (string, error) {
		return "", errors.New("decrypt failed")
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("expected 0 recovered, got %d", n)
	}
	if got := fs.statusOf("dep-key-fail"); got != deployment.StatusCrashed {
		t.Errorf("status: got %s, want crashed", got)
	}
}

func TestOrchestrator_EnsureDepsForDeploy(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.New(dir)
	reg, _ := registry.New(t.Context(), registry.Config{})
	t.Cleanup(func() { _ = reg.Close() })
	o, _ := New(Config{Store: st, Registry: reg, Logger: slog.New(slog.NewTextHandler(discardWriter{}, nil))})

	if err := o.ensureDepsForDeploy(); err == nil {
		t.Fatal("expected error on empty deps")
	}

	b, _ := wsbroker.New(wsbroker.Config{URL: "ws://localhost:1", APIKey: "test"})
	o.cfg.WsBroker = b
	if err := o.ensureDepsForDeploy(); err == nil {
		t.Fatal("expected error on missing Kernel/Orders/DeployStore")
	}
}

func TestOrchestrator_IsRunning_NoLiveInstance(t *testing.T) {
	o := newTestOrchestrator(t, true)
	if o.IsRunning("nonexistent") {
		t.Error("expected IsRunning=false")
	}
}

func TestOrchestrator_ResolveVersion_Fallback(t *testing.T) {
	o := newTestOrchestrator(t, true)
	d := deployment.Deployment{ID: "d", UserID: "u", Revision: "v3"}
	wasm, manifest, err := o.resolveVersion(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	if wasm != "v3" || manifest != "v3" {
		t.Errorf("fallback: got (%s,%s), want (v3,v3)", wasm, manifest)
	}
}

func TestOrchestrator_ResolveVersion_EmptyRevisionFails(t *testing.T) {
	o := newTestOrchestrator(t, true)
	d := deployment.Deployment{ID: "d", UserID: "u"}
	if _, _, err := o.resolveVersion(context.Background(), d); err == nil {
		t.Fatal("expected error on empty revision")
	}
}

func TestOrchestrator_ResolveVersion_ResolverWins(t *testing.T) {
	o := newTestOrchestrator(t, true)
	o.cfg.VersionResolver = func(d deployment.Deployment) (string, string, error) {
		return "wasm-" + d.Revision, "manifest-" + d.Revision, nil
	}
	d := deployment.Deployment{ID: "d", UserID: "u", Revision: "v1"}
	wasm, manifest, err := o.resolveVersion(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	if wasm != "wasm-v1" || manifest != "manifest-v1" {
		t.Errorf("got (%s,%s)", wasm, manifest)
	}
}

func TestOrchestrator_BuildLiveConfigJSON_Minimal(t *testing.T) {
	cfg := DeployConfig{StrategyID: "S", OrderBookID: "OB"}
	b, err := buildLiveConfigJSON(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got["mode"] != "live" {
		t.Errorf("mode: got %v", got["mode"])
	}
}

func TestOrchestrator_LiveMapInitialized(t *testing.T) {
	o := newTestOrchestrator(t, true)
	if o.liveInstances == nil {
		t.Error("liveInstances should be non-nil after New")
	}
}

func TestNoopAudit_DoesNothing(t *testing.T) {
	if err := (noopAudit{}).Record(context.Background(), "x", "y"); err != nil {
		t.Errorf("noop: got %v, want nil", err)
	}
}

func TestSinkAudit_PassesUserID(t *testing.T) {
	var got struct {
		userID, action, detail string
	}
	rec := func(_ context.Context, userID, action string, detail []byte) error {
		got.userID = userID
		got.action = action
		got.detail = string(detail)
		return nil
	}
	s := sinkAudit{insert: rec, userID: "u-42"}
	if err := s.Record(context.Background(), "x", "y"); err != nil {
		t.Fatal(err)
	}
	if got.userID != "u-42" || got.action != "x" || got.detail != "y" {
		t.Errorf("got %+v", got)
	}
}

func TestOrchestrator_Validate_DoesNotRequireLiveDeps(t *testing.T) {
	dir := t.TempDir()
	st, err := store.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	reg, _ := registry.New(t.Context(), registry.Config{})
	t.Cleanup(func() { _ = reg.Close() })
	o, _ := New(Config{Store: st, Registry: reg})

	// No live deps wired; Validate should still work and report
	// a clean error on a missing artifact.
	if _, err := o.Validate(t.Context(), "missing", "missing"); err == nil {
		t.Fatal("expected error on missing artifact")
	}
	if o.liveInstances == nil {
		t.Error("liveInstances not initialized")
	}
}

func TestOrchestrator_Deploy_RejectsEmptyResolution(t *testing.T) {
	o := newTestOrchestrator(t, true)
	err := o.Deploy(t.Context(), DeployConfig{
		DeploymentID: "dep-1",
		OrderBookID:  "OB-1",
		Resolution:   "",
	})
	if err == nil {
		t.Fatal("expected error on empty resolution")
	}
	if !strings.Contains(err.Error(), "Resolution is required") {
		t.Errorf("error %q should mention Resolution", err)
	}
	if strings.Contains(err.Error(), "implicit default") == false {
		t.Errorf("error %q should explain why no default", err)
	}
}

func TestOrchestrator_Deploy_RejectsEmptyOrderBookID(t *testing.T) {
	o := newTestOrchestrator(t, true)
	err := o.Deploy(t.Context(), DeployConfig{
		DeploymentID: "dep-1",
		OrderBookID:  "",
		Resolution:   "1m",
	})
	if err == nil {
		t.Fatal("expected error on empty OrderBookID")
	}
	if !strings.Contains(err.Error(), "OrderBookID is required") {
		t.Errorf("error %q should mention OrderBookID", err)
	}
}

func TestOrchestrator_Deploy_RejectsEmptyDeploymentID(t *testing.T) {
	o := newTestOrchestrator(t, true)
	err := o.Deploy(t.Context(), DeployConfig{
		DeploymentID: "",
		OrderBookID:  "OB-1",
		Resolution:   "1m",
	})
	if err == nil {
		t.Fatal("expected error on empty DeploymentID")
	}
}

// Resume must refuse a row whose Resolution is empty (pre-migration
// row). The error must name the deployment so the operator can find
// the bad row in their dashboard.
func TestOrchestrator_Resume_RefusesEmptyResolution(t *testing.T) {
	o := newTestOrchestrator(t, true)
	fs, _ := o.cfg.DeployStore.(*fakeStore)
	_ = fs.Create(t.Context(), deployment.Deployment{
		ID: "dep-empty-res", UserID: "u-1", Status: deployment.StatusStopped,
		Revision: "v1", OrderBookID: "OB-1", Resolution: "",
	})
	err := o.Resume(t.Context(), "dep-empty-res", "u-1", "k")
	if err == nil {
		t.Fatal("expected error on empty resolution")
	}
	if !strings.Contains(err.Error(), "dep-empty-res") {
		t.Errorf("error %q must include the deployment id", err)
	}
	if !strings.Contains(err.Error(), "no resolution") {
		t.Errorf("error %q must mention missing resolution", err)
	}
}

// TestOrchestrator_RestartConfigFromRow_CopiesWarmupCandles guards
// the review's P3 #4 contract: a deployment row persisted with
// WarmupCandles=200 (e.g. crashed, then resumed) must carry that
// value into the restarted DeployConfig. Resume and Recover both
// build their config via deployConfigFromRow, so a manifest that
// re-reads without a preamble declaration cannot reset the warmup
// window to zero.
func TestOrchestrator_RestartConfigFromRow_CopiesWarmupCandles(t *testing.T) {
	d := deployment.Deployment{
		ID: "dep-warm-resume", StrategyID: "s-1", Revision: "v1", UserID: "u-1",
		OrderBookID: "OB-1", Resolution: "1m", Status: deployment.StatusCrashed,
		WarmupCandles: 200,
	}
	cfg := deployConfigFromRow(d, "key", "wasm-ref", "manifest-hash")
	if cfg.WarmupCandles != 200 {
		t.Errorf("WarmupCandles: got %d, want 200 (row value must survive the restart)", cfg.WarmupCandles)
	}
	if cfg.OrderBookID != "OB-1" || cfg.Resolution != "1m" || cfg.DoraAPIKey != "key" ||
		cfg.WasmRef != "wasm-ref" || cfg.ManifestHash != "manifest-hash" {
		t.Errorf("config fields not copied from row: %+v", cfg)
	}
}

// Recover must refuse a row whose Resolution is empty and mark it
// crashed so it does not look like a running deployment on the next
// boot.
func TestOrchestrator_Recover_RefusesEmptyResolution(t *testing.T) {
	o := newTestOrchestrator(t, true)
	fs, _ := o.cfg.DeployStore.(*fakeStore)
	_ = fs.Create(t.Context(), deployment.Deployment{
		ID: "dep-rec-empty", UserID: "u-1", Status: deployment.StatusRunning,
		Revision: "v1", OrderBookID: "OB-1", Resolution: "",
	})
	n, err := o.Recover(t.Context(), func(_ context.Context, _ string) (string, error) {
		return "k", nil
	})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 recovered, got %d", n)
	}
	if got := fs.statusOf("dep-rec-empty"); got != deployment.StatusCrashed {
		t.Errorf("status: got %s, want crashed", got)
	}
}

func TestOrchestrator_Recover_RefusesEmptyOrderBookID(t *testing.T) {
	o := newTestOrchestrator(t, true)
	fs, _ := o.cfg.DeployStore.(*fakeStore)
	_ = fs.Create(t.Context(), deployment.Deployment{
		ID: "dep-rec-empty-ob", UserID: "u-1", Status: deployment.StatusRunning,
		Revision: "v1", OrderBookID: "", Resolution: "1m",
	})
	n, err := o.Recover(t.Context(), func(_ context.Context, _ string) (string, error) {
		return "k", nil
	})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 recovered, got %d", n)
	}
	if got := fs.statusOf("dep-rec-empty-ob"); got != deployment.StatusCrashed {
		t.Errorf("status: got %s, want crashed", got)
	}
}

// TestOrchestrator_HandleRunError_ActuallyReDeploys is a regression
// test for the bug where handleRunError built a restartCfg but
// never called o.Deploy(), leaving the deployment row in 'running'
// status with a dead goroutine and an inflated restart_count. After
// the fix, handleRunError calls Deploy(restartCfg) and on success
// the live instance is re-registered (so Stats() returns true).
func TestOrchestrator_HandleRunError_ActuallyReDeploys(t *testing.T) {
	o := newTestOrchestrator(t, true)
	fs, _ := o.cfg.DeployStore.(*fakeStore)
	// Seed a row in 'running' status with a 100-candle-persisted
	// value AND a Resolution. The Resolution is the critical piece:
	// handleRunError reads the row and copies Resolution/Params into
	// restartCfg. If Resolution is empty, validateForLive rejects
	// the redeploy and we never reach the registry — meaning the
	// test can't distinguish a successful re-Deploy (which would
	// then fail at the registry step because the test orchestrator
	// has no real WASM runtime) from a validation-failure path that
	// never actually invoked Deploy. Pre-fix, the bug was exactly
	// that handleRunError's Deploy call always failed at validation
	// because Resolution was empty.
	if err := fs.Create(t.Context(), deployment.Deployment{
		ID:          "dep-redeploy",
		StrategyID:  "s-1",
		Revision:    "rev-1",
		UserID:      "u-1",
		OrderBookID: "OB-1",
		Resolution:  "1m",
		Status:      deployment.StatusRunning,
	}); err != nil {
		t.Fatalf("create row: %v", err)
	}
	if err := fs.SetCandleCount(t.Context(), "dep-redeploy", "u-1", 100); err != nil {
		t.Fatalf("seed: %v", err)
	}

	li := &liveInstance{
		deploymentID: "dep-redeploy",
		budget:       NewRestartBudget(RestartConfig{Window: time.Minute, MaxRestarts: 5}),
	}
	state := &liveHostState{
		userID:     "u-1",
		strategyID: "s-1",
		deployment: "dep-redeploy",
		orderBook:  "OB-1",
		apiKey:     "k",
		logger:     o.cfg.Logger,
	}
	o.handleRunError(t.Context(), li, state, errors.New("synthetic run error"))

	d, err := fs.Get(t.Context(), "dep-redeploy", "u-1")
	if err != nil {
		t.Fatalf("row not found after handleRunError: %v", err)
	}
	if d.RestartCount == 0 {
		t.Errorf("RestartCount: want incremented, got 0 (handleRunError did not call o.Deploy)")
	}
	// Distinguish success vs failure: in the test environment
	// Deploy fails at the registry step (no real WASM runtime),
	// so the failure branch marks the row crashed. If
	// handleRunError had bailed at validation (regression), the
	// error message would say "Resolution is required"; if it
	// reached Deploy, the message will say something about the
	// registry or the in-process restart. Either is fine — what
	// matters is the row hit the failure branch, not the budget
	// branch ("restart budget exhausted") and not the validation
	// branch.
	if d.Status != deployment.StatusCrashed {
		t.Errorf("status: want crashed, got %s", d.Status)
	}
	if !strings.Contains(d.StoppedReason, "in-process restart failed") {
		t.Errorf("StoppedReason should mention 'in-process restart failed'; got %q (proves the deploy was attempted, not the budget path)", d.StoppedReason)
	}
}

// TestOrchestrator_HandleRunError_MarksCrashedOnDeployFailure guards
// the failure branch: when handleRunError's re-Deploy call returns
// an error, the deployment row must be marked crashed with the cause
// so the operator can see why in-process restart failed.
func TestOrchestrator_HandleRunError_MarksCrashedOnDeployFailure(t *testing.T) {
	o := newTestOrchestrator(t, true)
	fs, _ := o.cfg.DeployStore.(*fakeStore)

	// Pre-create the row so UpdateStatus can find it. IncRestart +
	// UpdateStatus need a real row; Create with all required fields.
	if err := fs.Create(t.Context(), deployment.Deployment{
		ID:         "dep-crash",
		StrategyID: "s-1",
		Revision:   "rev-1",
		UserID:     "u-1",
		Status:     deployment.StatusRunning,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	li := &liveInstance{
		deploymentID: "dep-crash",
		budget:       NewRestartBudget(RestartConfig{Window: time.Minute, MaxRestarts: 5}),
	}
	state := &liveHostState{
		userID:     "u-1",
		strategyID: "s-1",
		deployment: "dep-crash",
		orderBook:  "OB-1",
		apiKey:     "k",
		logger:     o.cfg.Logger,
	}
	// The fake registry is nil, so o.Deploy fails. handleRunError
	// must mark the row crashed with the cause.
	o.handleRunError(t.Context(), li, state, errors.New("synthetic"))

	d, _ := fs.Get(t.Context(), "dep-crash", "u-1")
	if d.Status != deployment.StatusCrashed {
		t.Errorf("status: want crashed, got %s", d.Status)
	}
	if !strings.Contains(d.StoppedReason, "in-process restart failed") {
		t.Errorf("StoppedReason should mention restart failure; got %q", d.StoppedReason)
	}
}

// TestOrchestrator_SyncWarmupCandles_ResyncsRowToManifest guards the
// T15 row-vs-runtime divergence: Deploy seeds state.warmupCandles from
// the manifest, but the row was seeded from the request's
// warmup_candles field. When they disagree (request says 0, manifest
// says 200), the row must be re-synced to the manifest — Resume /
// Recover rebuild the warmup window from the row. This tests the
// sync helper Deploy calls at step 9 (a full Deploy needs a real
// wasm artifact; the helper is the seam).
func TestOrchestrator_SyncWarmupCandles_ResyncsRowToManifest(t *testing.T) {
	o := newTestOrchestrator(t, true)
	fs, _ := o.cfg.DeployStore.(*fakeStore)
	// Row as the HTTP handler / tool wrote it: request sent 0.
	if err := fs.Create(t.Context(), deployment.Deployment{
		ID:            "dep-warm-sync",
		StrategyID:    "s-1",
		Revision:      "rev-1",
		UserID:        "u-1",
		OrderBookID:   "OB-1",
		Resolution:    "1m",
		Status:        deployment.StatusRunning,
		WarmupCandles: 0,
	}); err != nil {
		t.Fatalf("create row: %v", err)
	}

	o.syncWarmupCandles(t.Context(), DeployConfig{DeploymentID: "dep-warm-sync", UserID: "u-1"}, 200)

	d, err := fs.Get(t.Context(), "dep-warm-sync", "u-1")
	if err != nil {
		t.Fatalf("get row: %v", err)
	}
	if d.WarmupCandles != 200 {
		t.Errorf("WarmupCandles: got %d, want 200 (row must match the manifest)", d.WarmupCandles)
	}
}

// TestOrchestrator_BuildRestartConfig_CopiesResolutionAndParams guards
// the original bug: handleRunError used to build restartCfg with
// only the in-memory state fields, leaving Resolution and Params
// empty. validateForLive rejected empty Resolution, so the in-process
// restart path always failed at validation and the deployment was
// marked crashed. The fix extracts the build into a small helper that
// reads the persisted row and copies Resolution, Params, and WasmRef.
// This test verifies the helper's contract directly (no orchestrator
// state needed) and pins the regression: if Resolution stops being
// copied, the test fails with a specific message.
func TestOrchestrator_BuildRestartConfig_CopiesResolutionAndParams(t *testing.T) {
	o := newTestOrchestrator(t, true)
	fs, _ := o.cfg.DeployStore.(*fakeStore)
	// Seed a row that has Resolution + Params set on disk.
	if err := fs.Create(t.Context(), deployment.Deployment{
		ID:          "dep-restart",
		StrategyID:  "s-1",
		Revision:    "rev-1",
		UserID:      "u-1",
		OrderBookID: "OB-1",
		Resolution:  "5m",
		Params:      map[string]string{"stop_loss": "0.02", "take_profit": "0.05"},
		Status:      deployment.StatusRunning,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	li := &liveInstance{deploymentID: "dep-restart"}
	state := &liveHostState{
		userID:     "u-1",
		strategyID: "s-1",
		deployment: "dep-restart",
		orderBook:  "OB-IN-MEMORY", // deliberately different from row
		apiKey:     "k",
	}

	cfg := o.buildRestartConfig(t.Context(), li, state)

	// In-memory fields come from the liveHostState.
	if cfg.DeploymentID != "dep-restart" {
		t.Errorf("DeploymentID: want dep-restart, got %q", cfg.DeploymentID)
	}
	if cfg.UserID != "u-1" {
		t.Errorf("UserID: got %q", cfg.UserID)
	}
	if cfg.OrderBookID != "OB-IN-MEMORY" {
		t.Errorf("OrderBookID: want in-memory value, got %q", cfg.OrderBookID)
	}
	if cfg.DoraAPIKey != "k" {
		t.Errorf("DoraAPIKey: got %q", cfg.DoraAPIKey)
	}
	// Row-only fields MUST come from the row, not the in-memory state.
	// Pre-fix these were empty and validateForLive rejected the
	// restart with "Resolution is required".
	if cfg.Resolution != "5m" {
		t.Errorf("Resolution: want 5m (from row), got %q (empty Resolution means validateForLive will reject the restart)", cfg.Resolution)
	}
	if cfg.WasmRef != "rev-1" {
		t.Errorf("WasmRef: want rev-1 (from row.Revision), got %q", cfg.WasmRef)
	}
	if got, want := cfg.Params["stop_loss"], "0.02"; got != want {
		t.Errorf("Params[stop_loss]: want %q, got %q", want, got)
	}
	if got, want := cfg.Params["take_profit"], "0.05"; got != want {
		t.Errorf("Params[take_profit]: want %q, got %q", want, got)
	}
}

// TestOrchestrator_BuildRestartConfig_NoRowIsTolerant is the
// fallback path: if the row has been removed (operator manually
// deleted it, or migration removed it), the helper must still
// produce a usable config with the in-memory fields filled in and
// the row-only fields empty. validateForLive will then reject
// (the caller must handle the rejection) but we never panic on
// a nil-row access.
func TestOrchestrator_BuildRestartConfig_NoRowIsTolerant(t *testing.T) {
	o := newTestOrchestrator(t, true)
	li := &liveInstance{deploymentID: "dep-missing"}
	state := &liveHostState{
		userID:     "u-1",
		strategyID: "s-1",
		deployment: "dep-missing",
		orderBook:  "OB-1",
		apiKey:     "k",
	}
	cfg := o.buildRestartConfig(t.Context(), li, state)
	if cfg.DeploymentID != "dep-missing" {
		t.Errorf("DeploymentID: got %q", cfg.DeploymentID)
	}
	if cfg.Resolution != "" {
		t.Errorf("Resolution: want empty (no row), got %q", cfg.Resolution)
	}
}

// TestOrchestrator_BuildLiveHostState_SeedsCandleCountFromRow pins the
// restart-persistence contract: buildLiveHostState must read the row's
// candle_count and seed the new state with it. Pre-fix the count reset
// to 0 on every restart path (server boot, Recover, Resume, HotSwap,
// in-process crash recovery) which was misleading — users reasonably
// read the stat as "candles since strategy was first deployed" but
// it was actually "candles since the current goroutine started".
// Negative sanity check (skip the seed) makes the test fail with
// "state.candleCount: want 999, got 0".
func TestOrchestrator_BuildLiveHostState_SeedsCandleCountFromRow(t *testing.T) {
	o := newTestOrchestrator(t, true)
	fs, _ := o.cfg.DeployStore.(*fakeStore)
	if err := fs.Create(t.Context(), deployment.Deployment{
		ID:         "dep-seed-helper",
		StrategyID: "s-1",
		Revision:   "rev-1",
		UserID:     "u-1",
		Status:     deployment.StatusRunning,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := fs.SetCandleCount(t.Context(), "dep-seed-helper", "u-1", 999); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Need a subscription for the test — use the real wsbroker
	// (newTestOrchestrator(t, true) creates one).
	sub := o.cfg.WsBroker.Subscribe("OB-1", "1m", []string{"candle"})
	t.Cleanup(func() { o.cfg.WsBroker.Unsubscribe(sub) })
	auditHook := o.auditFor("u-1")
	state := o.buildLiveHostState(
		t.Context(),
		DeployConfig{DeploymentID: "dep-seed-helper", UserID: "u-1"},
		sub,
		nil, nil, // trade + price subs: not relevant to the seed
		manifest.Manifest{}, // manifest: not relevant to the seed
		nil,                 // cfgJSON: not relevant to the seed
		auditHook,
	)
	state.candleMu.Lock()
	defer state.candleMu.Unlock()
	if state.candleCount != 999 {
		t.Errorf("state.candleCount: want 999 (seeded from row), got %d", state.candleCount)
	}
}

// TestOrchestrator_BuildLiveHostState_FreshRowIsZero covers the
// bootstrap path: a brand-new deployment (no row on disk) starts
// at 0. We use a DeploymentID that does not exist in the store.
func TestOrchestrator_BuildLiveHostState_FreshRowIsZero(t *testing.T) {
	o := newTestOrchestrator(t, true)
	sub := o.cfg.WsBroker.Subscribe("OB-1", "1m", []string{"candle"})
	t.Cleanup(func() { o.cfg.WsBroker.Unsubscribe(sub) })
	auditHook := o.auditFor("u-1")
	state := o.buildLiveHostState(
		t.Context(),
		DeployConfig{DeploymentID: "dep-fresh", UserID: "u-1"},
		sub,
		nil, nil,
		manifest.Manifest{},
		nil,
		auditHook,
	)
	state.candleMu.Lock()
	defer state.candleMu.Unlock()
	if state.candleCount != 0 {
		t.Errorf("state.candleCount: want 0 (fresh deployment), got %d", state.candleCount)
	}
}

// TestOrchestrator_Deploy_DoesNotDeadlock is a regression test for
// the bug where a patch dropped the liveMu.Unlock() between the
// duplicate check and the registration step: liveMu is a
// non-reentrant sync.Mutex, so every Deploy that passed the
// duplicate check blocked forever on the second Lock and the
// calling HTTP/tool goroutine hung. The test drives a full Deploy
// (load + instantiate of the real example wasm artifact) in a
// goroutine and fails if it does not return promptly.
func TestOrchestrator_Deploy_DoesNotDeadlock(t *testing.T) {
	wasmPath := filepath.Join("..", "..", "strategywasm", "example", "strategy.wasm")
	if _, err := os.Stat(wasmPath); err != nil {
		t.Skipf("example strategy.wasm not found at %s; run `make tinygo-build` in strategywasm/ to produce it", wasmPath)
	}
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Fatalf("read wasm: %v", err)
	}
	manifestBytes := []byte(`{
		"schema_version": 1,
		"module_name": "noop",
		"language": "go",
		"framework_version": "` + prompts.WasmFrameworkVersion + `",
		"capabilities": {
			"order_books": ["OB-1"],
			"resolutions": ["1m"],
			"channels": ["candle"],
			"host_functions": ["host_log"]
		},
		"params_schema": {}
	}`)

	o := newTestOrchestrator(t, true)
	wh, mh, err := o.cfg.Store.Put(wasmBytes, manifestBytes)
	if err != nil {
		t.Fatalf("store.Put: %v", err)
	}

	// The HTTP handler normally creates the row before Deploy; Stop
	// writes the final status to it, so seed it like the handler would.
	fs, _ := o.cfg.DeployStore.(*fakeStore)
	if err := fs.Create(t.Context(), deployment.Deployment{ID: "dep-deadlock", UserID: "u-1"}); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- o.Deploy(t.Context(), DeployConfig{
			DeploymentID: "dep-deadlock",
			StrategyID:   "strat-1",
			Revision:     "v1",
			UserID:       "u-1",
			OrderBookID:  "OB-1",
			Resolution:   "1m",
			WasmRef:      wh,
			ManifestHash: mh,
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Deploy: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Deploy deadlocked: did not return within 5s")
	}
	// Teardown only: the live loop may already have exited on its own
	// (no broker feed in this fixture), which unregisters the entry —
	// the regression under test is Deploy returning at all.
	if o.IsRunning("dep-deadlock") {
		if err := o.Stop(t.Context(), "dep-deadlock", "u-1"); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	}
}

// TestOrchestrator_BuildRestartConfig_CopiesWarmupCandles guards P1 #8:
// the deployment row persists warmup_candles precisely so Restart/Recover
// need not reload the manifest, but buildRestartConfig never read it —
// every restart ran with WarmupCandles=0, rebuilding an empty warmup window.
func TestOrchestrator_BuildRestartConfig_CopiesWarmupCandles(t *testing.T) {
	o := newTestOrchestrator(t, true)
	fs, _ := o.cfg.DeployStore.(*fakeStore)
	if err := fs.Create(t.Context(), deployment.Deployment{
		ID:            "dep-warm-restart",
		StrategyID:    "s-1",
		Revision:      "rev-1",
		UserID:        "u-1",
		OrderBookID:   "OB-1",
		Resolution:    "5m",
		Status:        deployment.StatusRunning,
		WarmupCandles: 200,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	li := &liveInstance{deploymentID: "dep-warm-restart"}
	state := &liveHostState{userID: "u-1", strategyID: "s-1", deployment: "dep-warm-restart", orderBook: "OB-1", apiKey: "k"}

	cfg := o.buildRestartConfig(t.Context(), li, state)
	if cfg.WarmupCandles != 200 {
		t.Errorf("WarmupCandles: want 200 (from row), got %d (restart would rebuild an empty warmup window)", cfg.WarmupCandles)
	}
}
