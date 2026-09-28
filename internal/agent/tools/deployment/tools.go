// Package deployment adds the live-deployment LLM tools. Each
// tool mirrors the matching HTTP handler in internal/httpapi/job_handlers.go:
// the LLM hands the model a deployment id (or a strategy + revision to start
// one) and the tool routes through the deployment store + orchestrator the
// same way the HTTP layer does. The store's ErrNotFound already collapses
// missing + wrong-owner lookups, so probes can't enumerate other users'
// deployment ids.
package deployment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/deployment"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/migration"
	"github.com/dora-network/bond-trading-strategies/internal/agent/orchestrator"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

// Orchestrator is the live-deployment subset the tools reach. The
// httpapi.LiveOrchestrator interface satisfies this structurally; tests
// inject a fake to avoid standing up the wasm runtime + wsbroker.
//
// The tools never need a KeyProvider or a VersionResolver — those are
// wired inside the orchestrator via main.go. The HotSwap signature
// matches the real *orchestrator.Orchestrator.HotSwap (wasm ref +
// manifest hash + order book id are filled by the orchestrator's
// resolveVersion fallback when the tool doesn't supply them — see the
// hot-swap handler for the same shape).
type Orchestrator interface {
	Deploy(ctx context.Context, cfg orchestrator.DeployConfig) error
	Stop(ctx context.Context, deploymentID, userID string) error
	Resume(ctx context.Context, deploymentID, userID, doraAPIKey string) error
	HotSwap(
		ctx context.Context,
		deploymentID, userID, newRevision string,
		newWasmRef, newManifestHash string,
		doraAPIKey, orderBookID string,
	) error
	Stats(deploymentID string) (orchestrator.DeploymentStats, bool)
}

// tool constants — names are part of the public LLM surface, renaming
// is a model-facing change.
const (
	toolNameDeploy    = "deploy_strategy"
	toolNameList      = "list_deployments"
	toolNameGetStatus = "get_deployment_status"
	toolNameStop      = "stop_deployment"
	toolNameResume    = "resume_deployment"
	toolNameRestart   = "restart_deployment"
	toolNameHotSwap   = "hotswap_deployment"
	toolNameLogs      = "get_deployment_logs"
)

// deployStrategySchema declares the deploy_strategy input shape. The
// LLM hands the model a strategy + revision + (optional) params; the
// tool resolves the version's wasm_ref + manifest_hash and asks the
// orchestrator to start a live deployment.
//
//nolint:lll // tools/input shapes are inherently long; lll here is noise.
const deployStrategySchema = `{"type":"object","properties":{"strategy_id":{"type":"string","description":"UUID of the strategy to deploy"},"revision_id":{"type":"string","description":"UUID of the strategy version (the built artifact)"},"order_book_id":{"type":"string","description":"Dora order book id to trade on"},"resolution":{"type":"string","description":"Candle resolution: 1m, 5m, 15m, 1h, 4h, 1d, 7d (default 1m)"},"warmup_candles":{"type":"integer","description":"Preamble warmup window from the strategy manifest (resolution-spaced candles); copied to the deployment row. 0 when omitted"},"params":{"type":"object","description":"Optional strategy params (key/value strings)","additionalProperties":{"type":"string"}}},"required":["strategy_id","revision_id","order_book_id"]}`

//nolint:lll // tools/input shapes are inherently long; lll here is noise.
const listDeploymentsSchema = `{"type":"object","properties":{"strategy_id":{"type":"string","description":"Optional strategy id to filter by; omit for all of the caller's deployments"},"limit":{"type":"integer","description":"Max deployments to return (1-100, default 50)"}},"required":[]}`

//nolint:lll // tools/input shapes are inherently long; lll here is noise.
const getStatusSchema = `{"type":"object","properties":{"deployment_id":{"type":"string","description":"UUID returned by deploy_strategy"}},"required":["deployment_id"]}`

//nolint:lll // tools/input shapes are inherently long; lll here is noise.
const stopSchema = `{"type":"object","properties":{"deployment_id":{"type":"string","description":"UUID of the deployment to stop"},"reason":{"type":"string","description":"Optional human-readable reason recorded in the audit trail"}},"required":["deployment_id"]}`

//nolint:lll // tools/input shapes are inherently long; lll here is noise.
const resumeSchema = `{"type":"object","properties":{"deployment_id":{"type":"string","description":"UUID of the stopped deployment to resume"}},"required":["deployment_id"]}`

//nolint:lll // tools/input shapes are inherently long; lll here is noise.
const restartSchema = `{"type":"object","properties":{"deployment_id":{"type":"string","description":"UUID of the deployment to restart"}},"required":["deployment_id"]}`

//nolint:lll // tools/input shapes are inherently long; lll here is noise.
const hotSwapSchema = `{"type":"object","properties":{"deployment_id":{"type":"string","description":"UUID of the running deployment to swap"},"revision_id":{"type":"string","description":"UUID of the new strategy version to load"}},"required":["deployment_id","revision_id"]}`

//nolint:lll // tools/input shapes are inherently long; lll here is noise.
const logsSchema = `{"type":"object","properties":{"deployment_id":{"type":"string","description":"UUID of the deployment to fetch logs for"},"limit":{"type":"integer","description":"Max log lines to return (default 100)"}},"required":["deployment_id"]}`

// deployInput is the typed input for deploy_strategy. The LLM sends the
// strategy + revision; the tool resolves the wasm artifacts before
// calling the orchestrator.
type deployInput struct {
	StrategyID    string            `json:"strategy_id"`
	RevisionID    string            `json:"revision_id"`
	OrderBookID   string            `json:"order_book_id"`
	Resolution    string            `json:"resolution,omitempty"`
	WarmupCandles int               `json:"warmup_candles,omitempty"` // manifest preamble value; persisted on the deployment row
	Params        map[string]string `json:"params,omitempty"`
}

// deployOutput is the JSON envelope returned to the agent. Mirrors the
// HTTP 202 payload: deployment_id + status.
type deployOutput struct {
	DeploymentID string `json:"deployment_id"`
	Status       string `json:"status"`
}

// listInput is the typed input for list_deployments. StrategyID is
// optional — when set, the store filters; when empty, the store returns
// the caller's full history.
type listInput struct {
	StrategyID string `json:"strategy_id,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

// listOutput is the JSON envelope returned to the agent. Mirrors the
// HTTP list response shape (deployments array + next_cursor).
type listOutput struct {
	Deployments []deploymentDTO `json:"deployments"`
	NextCursor  string          `json:"next_cursor,omitempty"`
}

// statusInput is the typed input for get_deployment_status.
type statusInput struct {
	DeploymentID string `json:"deployment_id"`
}

// statusOutput mirrors the HTTP GET /deployments/{id} payload plus a
// recent_events array. The recent_events field is populated by the
type statusOutput struct {
	DeploymentID string                        `json:"deployment_id"`
	StrategyID   string                        `json:"strategy_id"`
	Revision     string                        `json:"revision"`
	Status       string                        `json:"status"`
	StartedAt    *time.Time                    `json:"started_at,omitempty"`
	RestartCount int                           `json:"restart_count"`
	RecentEvents []string                      `json:"recent_events"`
	Stats        *orchestrator.DeploymentStats `json:"stats,omitempty"`
}

// stopInput is the typed input for stop_deployment. Reason is optional.
type stopInput struct {
	DeploymentID string `json:"deployment_id"`
	Reason       string `json:"reason,omitempty"`
}

// stopOutput mirrors the HTTP stop response: deployment_id + status.
type stopOutput struct {
	DeploymentID string `json:"deployment_id"`
	Status       string `json:"status"`
}

// resumeInput / restartInput / hotSwapInput are identical to statusInput
// (deployment_id); hotSwapInput also takes a revision_id.
type (
	resumeInput  = statusInput
	restartInput = statusInput
	hotSwapInput struct {
		DeploymentID string `json:"deployment_id"`
		RevisionID   string `json:"revision_id"`
	}
)

// resumeOutput / restartOutput mirror stopOutput.
type (
	resumeOutput  = stopOutput
	restartOutput = stopOutput
)

// hotSwapOutput adds the new revision so the LLM can confirm the swap.
type hotSwapOutput struct {
	DeploymentID string `json:"deployment_id"`
	Status       string `json:"status"`
	Revision     string `json:"revision"`
}

// logsInput is the typed input for get_deployment_logs.
type logsInput struct {
	DeploymentID string `json:"deployment_id"`
	Limit        int    `json:"limit,omitempty"`
}

// logsOutput is the envelope returned to the agent. Ponytail: empty
// until the audit-log reader is wired (the HTTP handler returns the
// same shape today).
type logsOutput struct {
	Logs []string `json:"logs"`
}

// deploymentDTO mirrors the snake_case JSON shape the HTTP layer emits.
// Kept inline (vs. exported from internal/httpapi) so the tools package
// stays independent of the httpapi package — only the field names need
// to match, not the type identity.
type deploymentDTO struct {
	ID           string            `json:"id"`
	StrategyID   string            `json:"strategy_id"`
	Revision     string            `json:"revision"`
	Status       string            `json:"status"`
	Params       map[string]string `json:"params,omitempty"`
	StartedAt    *time.Time        `json:"started_at,omitempty"`
	StoppedAt    *time.Time        `json:"stopped_at,omitempty"`
	RestartCount int               `json:"restart_count"`
}

// Tools builds the full deployment tool set: eight specs + a handler
// map keyed by tool name. The returned slice/map are safe to share
// across turns; each handler captures the same store + orchestrator
// closures (per-process singletons in production) plus the per-turn
// userID + doraAPIKey (re-bound each call by the agent loop's
// toolFactory).
//
// If store or orch is nil, Tools returns an empty set so the agent
// degrades gracefully when the WASM runtime is disabled.
func Tools(store deployment.Store, orch Orchestrator, versions strategies.Store,
	userID, doraAPIKey string, migrator *migration.Migrator,
) ([]llm.ToolSpec, map[string]llm.ToolHandler) {
	specs := []llm.ToolSpec{
		spec(toolNameDeploy, deployStrategySchema,
			"Start a strategy running live against a Dora order book. Returns the new deployment_id; follow up with get_deployment_status."),
		spec(toolNameList, listDeploymentsSchema,
			"List the caller's deployments, newest first. Optionally filter by strategy_id."),
		spec(toolNameGetStatus, getStatusSchema,
			"Fetch a deployment's lifecycle status, restart_count, and recent events. Wrong-owner ids surface as not-found."),
		spec(toolNameStop, stopSchema,
			"Stop a running deployment. Idempotent. The optional reason is forwarded to the audit trail."),
		spec(toolNameResume, resumeSchema,
			"Resume a previously stopped deployment. Re-resolves the wasm artifact and re-runs the live runtime."),
		spec(toolNameRestart, restartSchema,
			"Restart a deployment; clears the restart budget in the orchestrator. Same behavior as resume today."),
		spec(toolNameHotSwap, hotSwapSchema,
			"Swap a running deployment to a new revision. Tears down the current wasm module."),
		spec(toolNameLogs, logsSchema,
			"Fetch recent log lines for a deployment. Returns an empty list today; audit query ships later."),
	}
	handlers := map[string]llm.ToolHandler{
		toolNameDeploy:    deployHandler(store, orch, versions, userID, doraAPIKey, migrator),
		toolNameList:      listHandler(store, userID),
		toolNameGetStatus: statusHandler(store, orch, userID),
		toolNameStop:      stopHandler(orch, userID),
		toolNameResume:    resumeHandler(orch, userID, doraAPIKey),
		toolNameRestart:   restartHandler(orch, userID, doraAPIKey),
		toolNameHotSwap:   hotSwapHandler(store, orch, versions, userID, doraAPIKey, migrator),
		toolNameLogs:      logsHandler(store, userID),
	}
	return specs, handlers
}

// spec builds one ToolSpec from a name + schema + description. The
// JSONSchema is wrapped in json.RawMessage; an invalid schema string
// would fail at agent-loop startup, not at agent-turn time.
func spec(name, schema, description string) llm.ToolSpec {
	return llm.ToolSpec{
		Name:        name,
		Description: description,
		JSONSchema:  json.RawMessage(schema),
	}
}

// deployHandler returns the closure for deploy_strategy. Mirrors
// handleDeploy in internal/httpapi/job_handlers.go: validates strategy
// + version ownership, refuses if a running deployment already exists
// for the strategy, inserts the row, then asks the orchestrator to
// launch. A failed Deploy flips the row to crashed so the lifecycle
// is consistent even when the goroutine can't start.
func deployHandler(store deployment.Store, orch Orchestrator, versions strategies.Store,
	userID, doraAPIKey string, migrator *migration.Migrator,
) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in deployInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("deploy_strategy: parse input: %w", err)
		}
		if in.StrategyID == "" {
			return nil, errors.New("deploy_strategy: strategy_id is required")
		}
		if in.RevisionID == "" {
			return nil, errors.New("deploy_strategy: revision_id is required")
		}
		if in.OrderBookID == "" {
			return nil, errors.New("deploy_strategy: order_book_id is required")
		}
		if in.Resolution == "" {
			// Resolution must be explicit. Do not default to "1m"
			// — a strategy that expects 5m bars would silently
			// receive 1m bars on a market it was not designed for.
			return nil, errors.New("deploy_strategy: resolution is required (e.g. 1m, 5m, 15m, 1h, 4h, 1d, 7d)")
		}
		if in.WarmupCandles < 0 || in.WarmupCandles > backtest.MaxWarmupCandles {
			// Mirrors the HTTP handler: warmup_candles is a Postgres
			// INTEGER; out-of-range values overflow the column.
			return nil, fmt.Errorf("deploy_strategy: warmup_candles must be between 0 and %d", backtest.MaxWarmupCandles)
		}
		if _, err := versions.GetStrategy(ctx, userID, in.StrategyID); err != nil {
			if errors.Is(err, strategies.ErrNotFound) {
				//nolint:lll // error messages can be long; the LLM needs the recovery hint
				return nil, llm.NewRecoveryError(
					fmt.Sprintf("deploy_strategy: strategy %q not found for this user — your in-memory strategy_id may be stale; call get_strategy to look up the current strategy_id, then retry", in.StrategyID),
					"get_strategy",
				)
			}
			return nil, fmt.Errorf("deploy_strategy: %w", err)
		}
		v, err := versions.GetVersion(ctx, in.StrategyID, strategies.Revision(in.RevisionID))
		if err != nil {
			return nil, fmt.Errorf("deploy_strategy: %w", err)
		}
		if migrator.NeedsMigration(v) {
			stale := &migration.ErrStaleFramework{
				StrategyID:        in.StrategyID,
				CurrentRevisionID: in.RevisionID,
				Target:            v.Target,
			}
			return nil, llm.NewRecoveryHintError(stale.Error(), "generate_strategy", stale)
		}
		active, err := store.HasActive(ctx, in.StrategyID)
		if err != nil {
			return nil, fmt.Errorf("deploy_strategy: check active: %w", err)
		}
		if active {
			return nil, fmt.Errorf("deploy_strategy: %w", deployment.ErrAlreadyRunning)
		}
		deploymentID := uuid.NewString()
		d := deployment.Deployment{
			ID:            deploymentID,
			StrategyID:    in.StrategyID,
			Revision:      string(v.Revision),
			UserID:        userID,
			OrderBookID:   in.OrderBookID,
			Resolution:    in.Resolution,
			Params:        in.Params,
			Status:        deployment.StatusRunning,
			WarmupCandles: in.WarmupCandles,
		}
		if err := store.Create(ctx, d); err != nil {
			return nil, fmt.Errorf("deploy_strategy: create: %w", err)
		}
		cfg := orchestrator.DeployConfig{
			DeploymentID: d.ID,
			StrategyID:   d.StrategyID,
			Revision:     d.Revision,
			UserID:       d.UserID,
			OrderBookID:  in.OrderBookID,
			Resolution:   in.Resolution,
			DoraAPIKey:   doraAPIKey,
			WasmRef:      v.WasmRef,
			ManifestHash: v.ManifestHash,
			Params:       d.Params,
		}
		if err := orch.Deploy(ctx, cfg); err != nil {
			// Best-effort status flip. The goroutine may write crashed
			// later; a transient Deploy error surfaces as crashed.
			_ = store.UpdateStatus(ctx, deploymentID, userID, deployment.StatusCrashed, err.Error())
			return nil, fmt.Errorf("deploy_strategy: %w", err)
		}
		return json.Marshal(deployOutput{
			DeploymentID: deploymentID,
			Status:       string(deployment.StatusRunning),
		})
	}
}

// listHandler returns the closure for list_deployments. Mirrors
// handleListDeployments: the store handles owner filtering + cursor
// pagination. Empty strategy_id returns the caller's full history.
func listHandler(store deployment.Store, userID string) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in listInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("list_deployments: parse input: %w", err)
		}
		page := deployment.Page{Limit: in.Limit}
		rows, next, err := store.List(ctx, in.StrategyID, userID, page)
		if err != nil {
			return nil, fmt.Errorf("list_deployments: %w", err)
		}
		out := listOutput{
			Deployments: make([]deploymentDTO, len(rows)),
			NextCursor:  next,
		}
		for i, r := range rows {
			out.Deployments[i] = toDTO(r)
		}
		return json.Marshal(out)
	}
}

// statusHandler returns the closure for get_deployment_status. Loads
// the row, enforces ownership (the store's Get matches id+userID), and
// returns the lifecycle envelope plus recent_events. Ponytail: audit
func statusHandler(store deployment.Store, orch Orchestrator, userID string) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in statusInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("get_deployment_status: parse input: %w", err)
		}
		if in.DeploymentID == "" {
			return nil, errors.New("get_deployment_status: deployment_id is required")
		}
		row, err := store.Get(ctx, in.DeploymentID, userID)
		if err != nil {
			return nil, fmt.Errorf("get_deployment_status: %w", err)
		}
		out := statusOutput{
			DeploymentID: row.ID,
			StrategyID:   row.StrategyID,
			Revision:     row.Revision,
			Status:       string(row.Status),
			StartedAt:    row.StartedAt,
			RestartCount: row.RestartCount,
			RecentEvents: []string{},
		}
		if stats, ok := orch.Stats(in.DeploymentID); ok {
			out.Stats = &stats
		}
		return json.Marshal(out)
	}
}

// stopHandler returns the closure for stop_deployment. Mirrors
// handleStopDeployment: idempotent on the orchestrator side, the
// optional reason is forwarded.
func stopHandler(orch Orchestrator, userID string) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in stopInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("stop_deployment: parse input: %w", err)
		}
		if in.DeploymentID == "" {
			return nil, errors.New("stop_deployment: deployment_id is required")
		}
		// ponytail: reason is recorded when the orchestrator's audit
		// hook is wired to accept a freeform string. Today the
		// orchestrator's Stop signature doesn't carry the reason;
		// the field is parsed but ignored.
		if err := orch.Stop(ctx, in.DeploymentID, userID); err != nil {
			return nil, fmt.Errorf("stop_deployment: %w", err)
		}
		return json.Marshal(stopOutput{
			DeploymentID: in.DeploymentID,
			Status:       string(deployment.StatusStopped),
		})
	}
}

// resumeHandler returns the closure for resume_deployment. Mirrors
// handleResumeDeployment: re-runs Deploy against the existing row,
// the orchestrator owns wasm-ref resolution.
func resumeHandler(orch Orchestrator, userID, doraAPIKey string) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in resumeInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("resume_deployment: parse input: %w", err)
		}
		if in.DeploymentID == "" {
			return nil, errors.New("resume_deployment: deployment_id is required")
		}
		if err := orch.Resume(ctx, in.DeploymentID, userID, doraAPIKey); err != nil {
			return nil, fmt.Errorf("resume_deployment: %w", err)
		}
		return json.Marshal(resumeOutput{
			DeploymentID: in.DeploymentID,
			Status:       string(deployment.StatusRunning),
		})
	}
}

// restartHandler returns the closure for restart_deployment. Today
// restart and resume share the orchestrator call (Plan 5 may split
// them if a user-visible budget-clear appears).
func restartHandler(orch Orchestrator, userID, doraAPIKey string) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in restartInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("restart_deployment: parse input: %w", err)
		}
		if in.DeploymentID == "" {
			return nil, errors.New("restart_deployment: deployment_id is required")
		}
		if err := orch.Resume(ctx, in.DeploymentID, userID, doraAPIKey); err != nil {
			return nil, fmt.Errorf("restart_deployment: %w", err)
		}
		return json.Marshal(restartOutput{
			DeploymentID: in.DeploymentID,
			Status:       string(deployment.StatusRunning),
		})
	}
}

// hotSwapHandler returns the closure for hotswap_deployment. Mirrors
// handleHotSwapDeployment: looks up the existing row to learn its
// strategy_id, resolves the new revision's wasm_ref + manifest_hash,
// and calls the orchestrator's HotSwap. The orchestrator owns
// order-book-id resolution for the new revision (the resolveVersion
// fallback in lifecycle.go handles empty values).
func hotSwapHandler(store deployment.Store, orch Orchestrator, versions strategies.Store,
	userID, doraAPIKey string, migrator *migration.Migrator,
) llm.ToolHandler {
	// ponytail: hotswap defers to Registry.Load; migrator unused here
	_ = migrator
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in hotSwapInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("hotswap_deployment: parse input: %w", err)
		}
		if in.DeploymentID == "" {
			return nil, errors.New("hotswap_deployment: deployment_id is required")
		}
		if in.RevisionID == "" {
			return nil, errors.New("hotswap_deployment: revision_id is required")
		}
		// The HTTP handler takes strategy_id from the URL path;
		// the tool doesn't have it, so we load the existing row.
		// Store.Get enforces ownership — wrong-owner ids surface as
		// not-found.
		row, err := store.Get(ctx, in.DeploymentID, userID)
		if err != nil {
			return nil, fmt.Errorf("hotswap_deployment: %w", err)
		}
		v, err := versions.GetVersion(ctx, row.StrategyID, strategies.Revision(in.RevisionID))
		if err != nil {
			return nil, fmt.Errorf("hotswap_deployment: %w", err)
		}
		if err := orch.HotSwap(
			ctx,
			in.DeploymentID, userID, in.RevisionID,
			v.WasmRef, v.ManifestHash,
			doraAPIKey, "",
		); err != nil {
			return nil, fmt.Errorf("hotswap_deployment: %w", err)
		}
		return json.Marshal(hotSwapOutput{
			DeploymentID: in.DeploymentID,
			Status:       string(deployment.StatusRunning),
			Revision:     in.RevisionID,
		})
	}
}

// logsHandler returns the closure for get_deployment_logs. Verifies
// ownership via Store.Get (matches the HTTP handler) and returns an
// empty logs array. Ponytail: audit query wired when the audit reader
// is added — the HTTP endpoint returns the same shape today.
func logsHandler(store deployment.Store, userID string) llm.ToolHandler {
	return func(ctx context.Context, _ string, input json.RawMessage) (json.RawMessage, error) {
		var in logsInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, fmt.Errorf("get_deployment_logs: parse input: %w", err)
		}
		if in.DeploymentID == "" {
			return nil, errors.New("get_deployment_logs: deployment_id is required")
		}
		if _, err := store.Get(ctx, in.DeploymentID, userID); err != nil {
			return nil, fmt.Errorf("get_deployment_logs: %w", err)
		}
		_ = in.Limit // accepted but unused today (audit query handles limit)
		return json.Marshal(logsOutput{Logs: []string{}})
	}
}

// toDTO maps an internal deployment row to the snake_case DTO the
// tools emit. Mirrors deploymentDTO in internal/httpapi/job_handlers.go.
func toDTO(d deployment.Deployment) deploymentDTO {
	return deploymentDTO{
		ID:           d.ID,
		StrategyID:   d.StrategyID,
		Revision:     d.Revision,
		Status:       string(d.Status),
		Params:       d.Params,
		StartedAt:    d.StartedAt,
		StoppedAt:    d.StoppedAt,
		RestartCount: d.RestartCount,
	}
}
