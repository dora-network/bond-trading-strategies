package httpapi

// strategy_runner.go implements the sole production AgentRunner. It composes
// the shared strategy-turn driver (turn.go) and adds version capture: on a
// verified generate_strategy it records a version (Capture); on capture
// failure it stashes the artifact (StashPending) and audits. The capture
// observer also inherits the original runner's source-pattern reject,
// generate failure, and per-repair audit obligations.

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/audit"
	"github.com/dora-network/bond-trading-strategies/internal/agent/config"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/session"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
	"github.com/dora-network/bond-trading-strategies/internal/agent/tools/generate"
)

// StrategyRunner is the sole production AgentRunner. It composes the shared
// strategy turn driver (the four-layer/classify/tool-loop orchestration) and
// adds version capture: on a verified generate_strategy it records a version,
// and on capture failure it stashes the artifact for manual save.
type StrategyRunner struct {
	newProvider   ProviderFactory
	newClassifier ClassifierFactory
	newTools      ToolFactory
	audit         AuditWriter
	versions      strategies.Store
	repairer      *generate.Repairer
	// modelCaps drives the strategy-generation hop's max_tokens budget.
	// Operators edit configs/model_caps.json when models change; no
	// recompile needed.
	modelCaps config.ModelCaps
	// llmTimeout overrides the per-iteration LLM round-trip deadline for
	// the strategy-generation hop (zero = default).
	llmTimeout time.Duration
	// llmMaxIters caps the tool-call loop iterations (zero = agent default).
	llmMaxIters int
}

// NewStrategyRunner builds the production runner. newTools builds the per-turn
// tool set (the eight Dora read tools plus generate_strategy), binding the
// user's per-request Dora API key each turn. versions is the strategy artifact
// Store (Capture/StashPending). repairer drives the generate_strategy repair
// loop; it may be nil when the caller injects a canned handler (tests).
func NewStrategyRunner(
	newProvider ProviderFactory,
	newClassifier ClassifierFactory,
	newTools ToolFactory,
	auditW AuditWriter,
	versions strategies.Store,
	repairer *generate.Repairer,
	modelCaps config.ModelCaps,
	llmTimeout time.Duration,
	llmMaxIters int,
) *StrategyRunner {
	return &StrategyRunner{
		newProvider:   newProvider,
		newClassifier: newClassifier,
		newTools:      newTools,
		audit:         auditW,
		versions:      versions,
		repairer:      repairer,
		modelCaps:     modelCaps,
		llmTimeout:    llmTimeout,
		llmMaxIters:   llmMaxIters,
	}
}

// Run executes one assistant turn. It builds the capture observer (binding the
// per-turn repair audit callback when a repairer is configured), then drives
// the shared strategy turn.
func (r *StrategyRunner) Run(
	ctx context.Context,
	userID, sessionID, provider, model string,
	history []session.StoredMessage,
	prompt string,
	emit func(EventPayload),
) (string, error) {
	obs := &captureObserver{
		audit: r.audit, versions: r.versions,
		userID: userID, sessionID: sessionID, provider: provider, model: model, emit: emit,
	}
	// Attach the per-turn repair audit callback so every failed repair attempt
	// inside Repairer.Run emits a strategy.repair row (spec §10, §16.5). The
	// callback is nil-safe; r.repairer may be nil in tests that inject a canned
	// generate_strategy output.
	if r.repairer != nil {
		r.repairer.OnRepair = obs.onRepair
	}
	return runStrategyTurn(ctx, turnDeps{
		userID: userID, sessionID: sessionID, provider: provider, model: model,
		prompt: prompt, history: history, emit: emit,
		newProvider: r.newProvider, newClassifier: r.newClassifier,
		newTools: r.newTools, doraAPIKey: DoraAPIKeyFromCtx(ctx),
		audit: r.audit, observeGenerate: obs.wrap,
		modelCaps:   r.modelCaps,
		llmTimeout:  r.llmTimeout,
		llmMaxIters: r.llmMaxIters,
	})
}

// Ensure StrategyRunner satisfies the AgentRunner interface at compile time.
var _ AgentRunner = (*StrategyRunner)(nil)

// captureObserver wraps the generate_strategy handler: on a verified artifact
// it records a version (Capture); on failure it stashes (StashPending), audits,
// and emits a non-fatal SSE notice. It never alters the handler's return value.
type captureObserver struct {
	audit    AuditWriter
	versions strategies.Store
	// userID/sessionID identify the turn for capture scoping and audit.
	// provider/model are recorded in the version's Meta.
	userID, sessionID, provider, model string
	emit                               func(EventPayload)
}

// probeFields is the parsed generate_strategy output the observer inspects.
type probeFields struct {
	verified                       bool
	errorReason                    string
	moduleName, summary, rationale string
	files                          map[string]string
	validation                     json.RawMessage
	imageRef                       string
	wasmRef                        string
	manifestHash                   string
}

func (o *captureObserver) wrap(h llm.ToolHandler) llm.ToolHandler {
	return func(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
		out, err := h(ctx, name, input)
		if err != nil {
			_ = o.audit.Insert(ctx, o.userID, audit.ActionStrategyFailed,
				mustMarshalDetail(generateDetail{Reason: errString(err), Verified: false}))
			return out, err
		}
		var probe struct {
			Verified     bool              `json:"verified"`
			Error        string            `json:"error"`
			ModuleName   string            `json:"module_name"`
			Summary      string            `json:"summary"`
			Rationale    string            `json:"rationale"`
			Files        map[string]string `json:"files"`
			Validation   json.RawMessage   `json:"validation"`
			ImageRef     string            `json:"image_ref,omitempty"`
			WasmRef      string            `json:"wasm_ref,omitempty"`
			ManifestHash string            `json:"manifest_hash,omitempty"`
			// StrategyID + RevisionID are populated by the observer after
			// capture so the LLM sees the persisted row's ids in the
			// assistant message body and can call run_backtest directly
			// without a separate get_strategy lookup. Round-tripped
			// via the wrapper so a later observer pass can read them.
			StrategyID string `json:"strategy_id,omitempty"`
			RevisionID string `json:"revision_id,omitempty"`
		}
		if jerr := json.Unmarshal(out, &probe); jerr != nil {
			return out, nil
		}
		if probe.Error == "source_pattern" {
			_ = o.audit.Insert(ctx, o.userID, audit.ActionMessageRejected,
				mustMarshalDetail(rejectDetail{Category: "source_pattern"}))
			return out, nil
		}
		if !probe.Verified {
			_ = o.audit.Insert(ctx, o.userID, audit.ActionStrategyFailed,
				mustMarshalDetail(generateDetail{Verified: false}))
			return out, nil
		}
		ver, err := o.capture(ctx, probeFields{
			verified:     probe.Verified,
			errorReason:  probe.Error,
			moduleName:   probe.ModuleName,
			summary:      probe.Summary,
			rationale:    probe.Rationale,
			files:        probe.Files,
			validation:   probe.Validation,
			imageRef:     probe.ImageRef,
			wasmRef:      probe.WasmRef,
			manifestHash: probe.ManifestHash,
		})
		if err != nil {
			// Stash failure was already audited + SSE-noticed by capture();
			// the LLM sees the original handler bytes (which carry
			// verified:false in the stash case) — no patching needed.
			return out, nil
		}
		// Patch the captured ids back into the LLM-visible bytes so the
		// next turn can call run_backtest(strategy_id, revision_id, ...)
		// without a separate get_strategy lookup, and the chat UI can
		// render a "view files" link for the saved version.
		probe.StrategyID = ver.StrategyID
		probe.RevisionID = string(ver.Revision)
		if patched, err := json.Marshal(probe); err == nil {
			out = patched
		}
		return out, nil
	}
}

// capture records a versioned artifact on success, or stashes + audits on
// failure. The first version (ParentRevision == "") records strategy.created;
// subsequent versions record strategy.version. Returns the captured
// Version on success so the observer can patch strategy_id + revision_id
// into the LLM-visible result; on stash failure returns the zero
// Version and the audit/SSE notification is already done.
func (o *captureObserver) capture(ctx context.Context, p probeFields) (strategies.Version, error) {
	m := strategies.Meta{
		Provider: o.provider, Model: o.model,
		ModuleName: p.moduleName, Summary: p.summary, Rationale: p.rationale, Validation: p.validation,
		ImageRef: p.imageRef,
	}
	var (
		v   strategies.Version
		err error
	)
	if p.wasmRef != "" {
		v, err = o.versions.CaptureWASM(ctx, o.sessionID, o.userID, o.provider, o.model, m, p.files, p.wasmRef, p.manifestHash)
	} else {
		v, err = o.versions.Capture(ctx, o.sessionID, o.userID, o.provider, o.model, m, p.files, p.imageRef)
	}
	if err != nil {
		slog.Warn("strategy capture failed; stashing", "error", err, "session", o.sessionID)
		if p.wasmRef != "" {
			if stashErr := o.versions.StashPending(ctx, o.sessionID, o.userID, o.provider, o.model, m, p.files, p.wasmRef); stashErr != nil {
				slog.Error("strategy capture stash failed", "error", stashErr, "session", o.sessionID)
			}
		} else {
			if stashErr := o.versions.StashPending(ctx, o.sessionID, o.userID, o.provider, o.model, m, p.files, p.imageRef); stashErr != nil {
				slog.Error("strategy capture stash failed", "error", stashErr, "session", o.sessionID)
			}
		}
		_ = o.audit.Insert(ctx, o.userID, audit.ActionStrategyCaptureFailed,
			mustMarshalDetail(generateDetail{Reason: errString(err)}))
		o.emit(ErrorPayload{Message: "strategy generated but not saved; retry from the session"})
		return strategies.Version{}, err
	}
	action := audit.ActionStrategyVersion
	if v.ParentRevision == "" {
		action = audit.ActionStrategyCreated
	}
	_ = o.audit.Insert(ctx, o.userID, action, mustMarshalDetail(generateDetail{Verified: true}))
	// Surface the saved version to the chat UI so it can render a
	// "view files" affordance that fetches the version's source via
	// GET /v1/strategies/{id}/versions/{revision}. The chat UI doesn't
	// otherwise have a link from this turn to the saved strategy.
	o.emit(StrategySavedPayload{
		StrategyID: v.StrategyID,
		Revision:   string(v.Revision),
		ModuleName: p.moduleName,
		Summary:    p.summary,
	})
	return v, nil
}

// onRepair is the Repairer.OnRepair callback. It writes a strategy.repair audit
// row per failed attempt (spec §10): the 0-indexed attempt number, the first
// diagnostic's category, and its message.
func (o *captureObserver) onRepair(attempt int, result generate.Result) {
	category := "no_diagnostic"
	message := ""
	if len(result.Diagnostics) > 0 {
		category = result.Diagnostics[0].Category
		message = result.Diagnostics[0].Message
	}
	_ = o.audit.Insert(context.Background(), o.userID, audit.ActionStrategyRepair,
		mustMarshalDetail(repairDetail{Attempt: attempt, Category: category, Message: message}))
}
