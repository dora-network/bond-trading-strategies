package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/deployment"
	"github.com/dora-network/bond-trading-strategies/internal/agent/orchestrator"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

// deployment/job handlers.
//
// Live deployments handle the deployment lifecycle: deploy, list, get,
// stop, resume, restart, hotswap, and logs. Stop/Resume/HotSwap delegate
// to the orchestrator; the row writes go to the deployment store. Backtests
// are below.

// --- deployments (live trading) ---

// deploymentListLimit caps the page size for handleListDeployments. Matches
// the deployment store's MaxPageSize (100); the Store.List cap is enforced
// separately inside deployment.Page.EffectiveLimit.
const deploymentListLimit = deployment.MaxPageSize

// requireDeployment returns false (and writes 503) when the deployment
// store or live orchestrator is unwired. Live runtime endpoints require
// both: the store owns the row, the orchestrator owns the goroutine.
func (s *Server) requireDeployment(w http.ResponseWriter) bool {
	if s.deployStore == nil || s.liveOrch == nil {
		http.Error(w, "deployment runtime unavailable", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// requireDeploymentRead returns false (and writes 503) when the read-side
// deployment store is unwired. Read endpoints don't need the orchestrator.
func (s *Server) requireDeploymentRead(w http.ResponseWriter) bool {
	if s.deployStore == nil {
		http.Error(w, "deployment service unavailable", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// deployRequest is the optional body for POST .../deploy. Empty body is
// fine (params defaults to empty). We don't 400 on unknown fields — the
// orchestrator is the source of truth on what params the plugin accepts.
type deployRequest struct {
	Params        map[string]string `json:"params,omitempty"`
	OrderBookID   string            `json:"order_book_id,omitempty"`
	Resolution    string            `json:"resolution,omitempty"`
	WarmupCandles int               `json:"warmup_candles,omitempty"` // persisted on the row for Resume/Recover
}

// deployResponse is the 202 payload returned to the caller.
type deployResponse struct {
	DeploymentID string `json:"deployment_id"`
	Status       string `json:"status"`
}

// handleDeploy: POST /v1/strategies/{id}/versions/{revision}/deploy
//
// Validates the strategy + version (ownership via GetStrategy, target +
// built wasm_ref), refuses if the strategy already has an active
// deployment, inserts a deployment row, and asks the orchestrator to
// start the live runtime. Returns 202 with the deployment id so the
// caller can poll /v1/strategies/{id}/deployments/{deployment_id}.
func (s *Server) handleDeploy(w http.ResponseWriter, r *http.Request) {
	if !s.requireDeployment(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	strategyID := r.PathValue("id")
	if _, err := s.strategies.GetStrategy(r.Context(), p.UserID, strategyID); err != nil {
		if errors.Is(err, strategies.ErrNotFound) {
			http.Error(w, "strategy not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get strategy", http.StatusInternalServerError)
		return
	}
	revision := strategies.Revision(r.PathValue("revision"))
	v, err := s.strategies.GetVersion(r.Context(), strategyID, revision)
	if err != nil {
		if errors.Is(err, strategies.ErrNotFound) {
			http.Error(w, "version not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get version", http.StatusInternalServerError)
		return
	}
	if v.Target != "go-wasm" || v.WasmRef == "" {
		http.Error(w, "version has no compiled wasm", http.StatusConflict)
		return
	}
	// Pre-parse the body so an obvious JSON error short-circuits before we
	// touch the store. Empty body => zero-value deployRequest (no params).
	var req deployRequest
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
	}
	if req.OrderBookID == "" {
		http.Error(w, "order_book_id is required", http.StatusBadRequest)
		return
	}
	if req.Resolution == "" {
		// Resolution must be explicit. Do not default to "1m" —
		// a strategy that expects 5m bars would silently receive
		// 1m bars on a market it was not designed for.
		http.Error(w, "resolution is required (e.g. 1m, 5m, 15m, 1h, 4h, 1d, 7d)", http.StatusBadRequest)
		return
	}
	// warmup_candles lands in a Postgres INTEGER column (migration 014);
	// out-of-range values overflow it. Mirror the backtest path's
	// errWarmupOutOfRange bounds (internal/backtest/validate.go).
	if req.WarmupCandles < 0 || req.WarmupCandles > backtest.MaxWarmupCandles {
		http.Error(w, fmt.Sprintf("warmup_candles must be between 0 and %d", backtest.MaxWarmupCandles), http.StatusBadRequest)
		return
	}
	active, err := s.deployStore.HasActive(r.Context(), strategyID)
	if err != nil {
		http.Error(w, "failed to check active deployments", http.StatusInternalServerError)
		return
	}
	if active {
		http.Error(w, "strategy already has an active deployment", http.StatusConflict)
		return
	}
	deploymentID := uuid.NewString()
	d := deployment.Deployment{
		ID:            deploymentID,
		StrategyID:    strategyID,
		Revision:      string(v.Revision),
		UserID:        p.UserID,
		OrderBookID:   req.OrderBookID,
		Resolution:    req.Resolution,
		Params:        req.Params,
		Status:        deployment.StatusRunning,
		WarmupCandles: req.WarmupCandles,
	}
	if err := s.deployStore.Create(r.Context(), d); err != nil {
		http.Error(w, "failed to create deployment", http.StatusInternalServerError)
		return
	}
	cfg := orchestratorDeployConfig(d, v, req.OrderBookID, req.Resolution, DoraAPIKeyFromCtx(r.Context()))
	if err := s.liveOrch.Deploy(r.Context(), cfg); err != nil {
		// Best-effort status flip. The goroutine may still try to write
		// crashed later; a transient Deploy error surfaces as crashed.
		//
		// The orchestrator's Deploy method already logs the error at
		// ERROR level with the deployment context (deployment_id,
		// strategy_id, user, order_book, resolution, error) — see
		// internal/orchestrator/lifecycle.go. We only need to
		// surface the cause in the response body so the LLM (and
		// through it, the user) sees a specific reason, and write
		// the cause to the deploy row's stopped_reason.
		_ = s.deployStore.UpdateStatus(r.Context(), deploymentID, p.UserID, deployment.StatusCrashed, err.Error())
		http.Error(w, "failed to start deployment: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusAccepted, deployResponse{DeploymentID: deploymentID, Status: string(deployment.StatusRunning)})
}

// orchestratorDeployConfig maps a deployment row + version into the
// orchestrator's DeployConfig. Resolution is taken from the
// deployment row (populated by the deploy input).
func orchestratorDeployConfig(
	d deployment.Deployment, v strategies.Version,
	orderBookID, resolution, doraAPIKey string,
) orchestrator.DeployConfig {
	return orchestrator.DeployConfig{
		DeploymentID: d.ID,
		StrategyID:   d.StrategyID,
		Revision:     d.Revision,
		UserID:       d.UserID,
		OrderBookID:  orderBookID,
		Resolution:   resolution,
		DoraAPIKey:   doraAPIKey,
		WasmRef:      v.WasmRef,
		ManifestHash: v.ManifestHash,
		Params:       d.Params,
	}
}

func (s *Server) handleListDeployments(w http.ResponseWriter, r *http.Request) {
	if !s.requireDeploymentRead(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	strategyID := r.PathValue("id")
	if _, err := s.strategies.GetStrategy(r.Context(), p.UserID, strategyID); err != nil {
		if errors.Is(err, strategies.ErrNotFound) {
			http.Error(w, "strategy not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get strategy", http.StatusInternalServerError)
		return
	}
	limit := deploymentListLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= deploymentListLimit {
			limit = n
		}
	}
	cursor := r.URL.Query().Get("cursor")
	rows, nextCursor, err := s.deployStore.List(r.Context(), strategyID, p.UserID, deployment.Page{Limit: limit, Cursor: cursor})
	if err != nil {
		http.Error(w, "failed to list deployments", http.StatusInternalServerError)
		return
	}
	if rows == nil {
		rows = []deployment.Deployment{}
	}
	out := make([]deploymentResp, len(rows))
	for i, d := range rows {
		out[i] = deploymentDTO(d)
	}
	writeJSON(w, http.StatusOK, map[string]any{"deployments": out, "next_cursor": nextCursor})
}

// handleGetDeployment: GET /v1/strategies/{id}/deployments/{deployment_id}
//
// Loads the row. Ownership is enforced inside the Store (Get matches
// id + userID), so a mismatched owner returns 404 alongside missing
// rows — we don't leak existence.
func (s *Server) handleGetDeployment(w http.ResponseWriter, r *http.Request) {
	if !s.requireDeploymentRead(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	d, err := s.deployStore.Get(r.Context(), r.PathValue("deployment_id"), p.UserID)
	if err != nil {
		if errors.Is(err, deployment.ErrNotFound) {
			http.Error(w, "deployment not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get deployment", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, deploymentDTO(d))
}

// deploymentResp renders a deployment row in the snake_case JSON shape
// the spec mandates. Status + time fields are rendered as plain strings
// to match the rest of the API surface. user_id is intentionally absent;
// stopped_reason is a pointer so an absent reason serializes as null
// (never an empty string), matching the approved contract.
type deploymentResp struct {
	DeploymentID      string            `json:"deployment_id"`
	StrategyID        string            `json:"strategy_id"`
	Revision          string            `json:"revision"`
	Status            string            `json:"status"`
	InstanceID        string            `json:"instance_id,omitempty"`
	OrderBookID       string            `json:"order_book_id"`
	Resolution        string            `json:"resolution"`
	StartedAt         *time.Time        `json:"started_at"`
	StoppedAt         *time.Time        `json:"stopped_at"`
	StoppedReason     *string           `json:"stopped_reason"`
	RestartCount      int               `json:"restart_count"`
	HotswappedAt      *time.Time        `json:"hotswapped_at"`
	HotswappedFromRev *string           `json:"hotswapped_from_rev"`
	Params            map[string]string `json:"params"`
	CandleCount       int64             `json:"candle_count"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

// deploymentDTO maps a deployment row into the wire shape. nil params are
// normalized to an empty object so callers always see {} (never null); an
// absent stopped_reason and hotswapped_from_rev serialize as JSON null via
// their pointer fields (matching the approved uuid-or-null contract).
func deploymentDTO(d deployment.Deployment) deploymentResp {
	params := d.Params
	if params == nil {
		params = map[string]string{}
	}
	var reason *string
	if d.StoppedReason != "" {
		r := d.StoppedReason
		reason = &r
	}
	var fromRev *string
	if d.HotswappedFromRev != "" {
		r := d.HotswappedFromRev
		fromRev = &r
	}
	return deploymentResp{
		DeploymentID:      d.ID,
		StrategyID:        d.StrategyID,
		Revision:          d.Revision,
		Status:            string(d.Status),
		InstanceID:        d.InstanceID,
		OrderBookID:       d.OrderBookID,
		Resolution:        d.Resolution,
		StartedAt:         d.StartedAt,
		StoppedAt:         d.StoppedAt,
		StoppedReason:     reason,
		RestartCount:      d.RestartCount,
		HotswappedAt:      d.HotswappedAt,
		HotswappedFromRev: fromRev,
		Params:            params,
		CandleCount:       d.CandleCount,
		CreatedAt:         d.CreatedAt,
		UpdatedAt:         d.UpdatedAt,
	}
}

// handleStopDeployment: POST /v1/strategies/{id}/deployments/{deployment_id}/stop
//
// Idempotent: any non-error response is 204. The orchestrator handles
// row status updates internally (running -> stopped); the optional
// body reason is currently ignored — Plan 5 stores it in the row.
func (s *Server) handleStopDeployment(w http.ResponseWriter, r *http.Request) {
	if !s.requireDeployment(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	if r.ContentLength != 0 {
		// Drain so the connection can be reused; ignore decode errors.
		var body struct {
			Reason string `json:"reason"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	if err := s.liveOrch.Stop(r.Context(), r.PathValue("deployment_id"), p.UserID); err != nil {
		http.Error(w, "failed to stop deployment", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleResumeDeployment: POST /v1/strategies/{id}/deployments/{deployment_id}/resume
//
// Restarts a previously stopped deployment. The orchestrator owns the
// resume path (re-resolve wasm artifacts + re-run Deploy). The Dora
// API key is pulled from the per-request context (set by auth mw).
func (s *Server) handleResumeDeployment(w http.ResponseWriter, r *http.Request) {
	if !s.requireDeployment(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	if err := s.liveOrch.Resume(r.Context(), r.PathValue("deployment_id"), p.UserID, DoraAPIKeyFromCtx(r.Context())); err != nil {
		http.Error(w, "failed to resume deployment", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleRestartDeployment: POST /v1/strategies/{id}/deployments/{deployment_id}/restart
//
// Restart and Resume both re-run Deploy against the existing row; the
// orchestrator's restart budget is reset on the live path. We model
// the two as the same call for now (Plan 5 may split them if a
// user-visible budget-clear appears in the API).
func (s *Server) handleRestartDeployment(w http.ResponseWriter, r *http.Request) {
	if !s.requireDeployment(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	if err := s.liveOrch.Resume(r.Context(), r.PathValue("deployment_id"), p.UserID, DoraAPIKeyFromCtx(r.Context())); err != nil {
		http.Error(w, "failed to restart deployment", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleHotSwapDeployment: POST /v1/strategies/{id}/deployments/{deployment_id}/hotswap
//
// Body: {"revision_id": "v2"}. The orchestrator tears down the current
// module and starts the new revision. Plan 5 resolves WasmRef +
// ManifestHash + OrderBookID from the strategies.Store; for now we
// pass empty values and the orchestrator's resolveVersion fallback
// treats deployment.Revision as the wasm ref (worst-effort).
func (s *Server) handleHotSwapDeployment(w http.ResponseWriter, r *http.Request) {
	if !s.requireDeployment(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	var req struct {
		RevisionID string `json:"revision_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RevisionID == "" {
		http.Error(w, "revision_id is required", http.StatusBadRequest)
		return
	}
	deploymentID := r.PathValue("deployment_id")
	if err := s.liveOrch.HotSwap(
		r.Context(), deploymentID, p.UserID, req.RevisionID,
		"", "", DoraAPIKeyFromCtx(r.Context()), "",
	); err != nil {
		http.Error(w, "failed to hotswap deployment", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"deployment_id": deploymentID,
		"status":        string(deployment.StatusRunning),
		"revision":      req.RevisionID,
	})
}

// handleDeploymentLogs: GET /v1/strategies/{id}/deployments/{deployment_id}/logs
//
// ponytail: audit query wired when audit reader is added. For now we
// verify ownership via Store.Get and return an empty log array. The
// endpoint is functional (not 501) so downstream callers can ship UI
// against the shape before the query is plumbed.
func (s *Server) handleDeploymentLogs(w http.ResponseWriter, r *http.Request) {
	if !s.requireDeploymentRead(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	if _, err := s.deployStore.Get(r.Context(), r.PathValue("deployment_id"), p.UserID); err != nil {
		if errors.Is(err, deployment.ErrNotFound) {
			http.Error(w, "deployment not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get deployment", http.StatusInternalServerError)
		return
	}
	// ?limit= is accepted for forward compatibility; the value is
	// ignored until the audit query is wired.
	writeJSON(w, http.StatusOK, map[string]any{"logs": []any{}})
}

// requireBacktest returns false (and writes 503) when the store is unwired
// or no runner is available for any target.
func (s *Server) requireBacktest(w http.ResponseWriter) bool {
	if s.backtestStore == nil || (s.backtestOrch == nil && s.wasmStarter == nil) {
		http.Error(w, "backtest service unavailable", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// requireBacktestRead returns false (and writes 503) when the read-side
// store is unwired. The POST handler additionally needs the orchestrator;
// read endpoints only need the store.
func (s *Server) requireBacktestRead(w http.ResponseWriter) bool {
	if s.backtestStore == nil {
		http.Error(w, "backtest service unavailable", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// --- backtests ---

// backtestListLimit caps the page size for handleListBacktests. Matches the
// strategies defaultPageSize; the Store.List cap is enforced separately.
const backtestListLimit = 50

// handleBacktest: POST /v1/strategies/{id}/versions/{revision}/backtest
//
// Validates the strategy + version (ownership via GetStrategy, presence of
// a built image_ref), then asks the orchestrator to Submit the job. The
// single-inflight partial unique index surfaces as ErrSingleInflight → 429;
// validation errors wrap ErrInvalidRequest → 409 (kept semantically distinct
// from 400 because the orchestrator owns validation and surfaces 409 for
// already-active state; the OpenAPI spec documents this).
func (s *Server) handleBacktest(w http.ResponseWriter, r *http.Request) {
	if !s.requireBacktest(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	strategyID := r.PathValue("id")
	if _, err := s.strategies.GetStrategy(r.Context(), p.UserID, strategyID); err != nil {
		if errors.Is(err, strategies.ErrNotFound) {
			http.Error(w, "strategy not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get strategy", http.StatusInternalServerError)
		return
	}
	revision := strategies.Revision(r.PathValue("revision"))
	v, err := s.strategies.GetVersion(r.Context(), strategyID, revision)
	if err != nil {
		if errors.Is(err, strategies.ErrNotFound) {
			http.Error(w, "version not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get version", http.StatusInternalServerError)
		return
	}
	var req backtest.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	switch v.Target {
	case "go-wasm":
		id, err := s.backtestOrch.Submit(r.Context(), p.UserID, strategyID, string(v.Revision),
			v.WasmRef, v.ManifestHash, DoraAPIKeyFromCtx(r.Context()), &req)
		if err != nil {
			switch {
			case errors.Is(err, backtest.ErrVersionNotBuilt):
				http.Error(w, "version has no compiled wasm", http.StatusConflict)
				return
			case errors.Is(err, backtest.ErrWasmUnavailable):
				http.Error(w, "wasm runtime unavailable", http.StatusServiceUnavailable)
				return
			case errors.Is(err, backtest.ErrInvalidRequest):
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			case errors.Is(err, backtest.ErrSingleInflight):
				http.Error(w, "user already has an active backtest", http.StatusTooManyRequests)
				return
			default:
				http.Error(w, "failed to submit backtest", http.StatusInternalServerError)
				return
			}
		}
		writeJSON(w, http.StatusOK, backtest.Response{BacktestID: id})
	default:
		http.Error(w, "unsupported version target", http.StatusConflict)
		return
	}
}

// handleListBacktests: GET /v1/strategies/{id}/backtests
//
// Returns the caller's backtests for a strategy, newest first. The Store
// returns all backtests for the strategy regardless of owner; we filter by
// user_id in the handler (Ponytail: avoid a second query when one will do).
// Limit is read from ?limit= with a hardcoded cap (backtestListLimit).
func (s *Server) handleListBacktests(w http.ResponseWriter, r *http.Request) {
	if !s.requireBacktestRead(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	strategyID := r.PathValue("id")
	if _, err := s.strategies.GetStrategy(r.Context(), p.UserID, strategyID); err != nil {
		if errors.Is(err, strategies.ErrNotFound) {
			http.Error(w, "strategy not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get strategy", http.StatusInternalServerError)
		return
	}
	limit := backtestListLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= backtestListLimit {
			limit = n
		}
	}
	rows, err := s.backtestStore.List(r.Context(), strategyID, limit)
	if err != nil {
		http.Error(w, "failed to list backtests", http.StatusInternalServerError)
		return
	}
	out := make([]backtest.Backtest, 0, len(rows))
	for _, b := range rows {
		if b.UserID != p.UserID {
			continue
		}
		out = append(out, *b)
	}
	writeJSON(w, http.StatusOK, out)
}

// handleGetBacktest: GET /v1/strategies/{id}/backtests/{backtest_id}
//
// Loads the row and its fills. Ownership is enforced by comparing the row's
// user_id to the principal — a mismatched owner returns 404 (not 403) so we
// don't leak the row's existence. Backtest is wrapped with fills under a
// top-level "fills" key to keep the wire payload stable across the lifecycle.
func (s *Server) handleGetBacktest(w http.ResponseWriter, r *http.Request) {
	if !s.requireBacktestRead(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	btID := r.PathValue("backtest_id")
	bt, err := s.backtestStore.Get(r.Context(), btID)
	if err != nil {
		if errors.Is(err, backtest.ErrNotFound) {
			http.Error(w, "backtest not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get backtest", http.StatusInternalServerError)
		return
	}
	if bt.UserID != p.UserID {
		http.Error(w, "backtest not found", http.StatusNotFound)
		return
	}
	fills, err := s.backtestStore.GetFills(r.Context(), btID)
	if err != nil {
		http.Error(w, "failed to load fills", http.StatusInternalServerError)
		return
	}
	if fills == nil {
		fills = []backtest.Fill{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"backtest": bt, "fills": fills})
}

// handleCancelBacktest: POST /v1/strategies/{id}/backtests/{backtest_id}/cancel
//
// Idempotent: any successful response is 204 regardless of whether the row
// was actually flipped. Missing rows and rows owned by other users both
// collapse to 404. After the DB flip, the orchestrator is signalled so
// the runner goroutine + its docker process unwind via the per-job
// context (no-op if the job already finished).
func (s *Server) handleCancelBacktest(w http.ResponseWriter, r *http.Request) {
	if !s.requireBacktest(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	btID := r.PathValue("backtest_id")
	bt, err := s.backtestStore.Get(r.Context(), btID)
	if err != nil {
		if errors.Is(err, backtest.ErrNotFound) {
			http.Error(w, "backtest not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get backtest", http.StatusInternalServerError)
		return
	}
	if bt.UserID != p.UserID {
		http.Error(w, "backtest not found", http.StatusNotFound)
		return
	}
	if _, err := s.backtestStore.CancelIfRunning(r.Context(), btID); err != nil {
		http.Error(w, "failed to cancel backtest", http.StatusInternalServerError)
		return
	}
	// Also signal the runner goroutine + its docker process. The DB
	// flip is the source of truth for status; the orchestrator cancel
	// is best-effort — it's a no-op if the job already finished.
	s.backtestOrch.Cancel(btID)
	w.WriteHeader(http.StatusNoContent)
}
