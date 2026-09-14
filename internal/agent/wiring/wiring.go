// Package wiring assembles the full dora-agent runtime against the host's
// shared pgxpool and ENCRYPTION_KEY. It is the single construction site for
// every agent dependency; cmd/strategy-server calls Wire once at startup and
// mounts Runtime.Server at /v1/agent behind its requireAuth chain.
package wiring

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	doraclient "github.com/dora-network/dora-client-go/doraclient"
	"github.com/jackc/pgx/v5/pgxpool"
	anyllm "github.com/mozilla-ai/any-llm-go"
	"github.com/mozilla-ai/any-llm-go/providers/anthropic"
	"github.com/mozilla-ai/any-llm-go/providers/openai"

	agentaudit "github.com/dora-network/bond-trading-strategies/internal/agent/audit"
	agentbacktest "github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/config"
	agentdeployment "github.com/dora-network/bond-trading-strategies/internal/agent/deployment"
	"github.com/dora-network/bond-trading-strategies/internal/agent/httpapi"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	anyllmshim "github.com/dora-network/bond-trading-strategies/internal/agent/llm/anyllm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/migration"
	"github.com/dora-network/bond-trading-strategies/internal/agent/orchestrator"
	"github.com/dora-network/bond-trading-strategies/internal/agent/orderbroker"
	"github.com/dora-network/bond-trading-strategies/internal/agent/providerconfig"
	"github.com/dora-network/bond-trading-strategies/internal/agent/safety"
	"github.com/dora-network/bond-trading-strategies/internal/agent/sanitize"
	"github.com/dora-network/bond-trading-strategies/internal/agent/secrets"
	"github.com/dora-network/bond-trading-strategies/internal/agent/session"
	agentstore "github.com/dora-network/bond-trading-strategies/internal/agent/store"
	agentstrategies "github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
	backtesttool "github.com/dora-network/bond-trading-strategies/internal/agent/tools/backtest"
	deploymenttool "github.com/dora-network/bond-trading-strategies/internal/agent/tools/deployment"
	doratool "github.com/dora-network/bond-trading-strategies/internal/agent/tools/dora"
	generate "github.com/dora-network/bond-trading-strategies/internal/agent/tools/generate"
	"github.com/dora-network/bond-trading-strategies/internal/agent/tools/generate/validate"
	strategiestool "github.com/dora-network/bond-trading-strategies/internal/agent/tools/strategies"
	"github.com/dora-network/bond-trading-strategies/internal/agent/users"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime"
	wasmstore "github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wsbroker"
)

// encryptionKeyLen is the AES-256 key length Wire requires. Named for
// gosec/mnd: keeps the magic number out of the condition.
const encryptionKeyLen = 32

// doraAPIKeyEnv is the host-level Dora API key the wsbroker uses to
// authenticate against DORA's multiplex WebSocket. DORA's multiplex
// endpoint (/plex) requires a Server-role key (it fans out market
// data to all per-user strategies subscribing on the single
// connection). Per-user trading on DORA uses a different scope and
// pulls the key from the request's Authorization header via
// cmd/strategy-server's agentPrincipalBridge — see DoraAPIKeyFromCtx.
//
// Set via deploy.sh: DORA_API_KEY Secrets Manager entry. The earlier
// DORA_ADMIN_API_KEY name was a residue from the standalone dora-agent
// service; the merged wiring now uses the single DORA_API_KEY for
// server-side market-data access.
const doraAPIKeyEnv = "DORA_API_KEY"

// Runtime owns every agent dependency. Close releases the background
// workers (janitors, wsbroker, wasm runtime, in-flight backtests).
type Runtime struct {
	Server         *httpapi.Server
	StrategiesJani *agentstrategies.Janitor
	BtOrch         *agentbacktest.Orchestrator
	WSBroker       *wsbroker.Broker
	LiveOrch       *orchestrator.Orchestrator
	WASMRuntime    *wasmruntime.Runtime
	History        *agentstore.HistoryStore

	users     *users.Store
	sessions  *session.Store
	configs   *providerconfig.Store
	versions  *agentstrategies.PgStore
	deployStr *agentdeployment.PgStore
	btStore   *agentbacktest.PgStore
	kernel    *safety.Kernel
	sealer    *secrets.Sealer
}

// Wire constructs the agent runtime. encryptionKey is the host's
// ENCRYPTION_KEY (32 bytes, hex-decoded by the caller). It returns the
// first error from any sub-construction — no silent fallbacks: a missing
// model-caps file, WASM artifact root, or short encryption key fails
// startup rather than degrading the agent.
//
//nolint:funlen // startup wiring is inherently long
func Wire(
	ctx context.Context,
	pool *pgxpool.Pool,
	encryptionKey []byte,
	cfg config.Config,
	log *slog.Logger,
) (*Runtime, error) {
	if pool == nil {
		return nil, errors.New("agent wiring: nil pool")
	}
	if len(encryptionKey) != encryptionKeyLen {
		return nil, fmt.Errorf("agent wiring: ENCRYPTION_KEY must be 32 bytes, got %d", len(encryptionKey))
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	sealer := secrets.NewSealer(encryptionKey)
	configs := providerconfig.New(pool, sealer)
	sessions := session.New(pool)
	usersStore, err := users.New(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("agent wiring: users: %w", err)
	}

	modelCaps, err := config.LoadModelCaps(cfg.ModelCapsPath)
	if err != nil {
		return nil, fmt.Errorf("agent wiring: model caps: %w", err)
	}

	// Fail closed: a broken WASM runtime is a deployment error, not a
	// silent skip. ctx (signal-bound in main) is the registry's parent
	// so SIGINT aborts in-flight wazero work.
	wasmRT, err := wasmruntime.NewRuntime(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("agent wiring: %w", err)
	}

	// Real history fetcher: the pool-backed *HistoryStore. Flows into the
	// live orchestrator (Config.History) and the backtest WasmStarter below.
	// NopHistoryFetcher (store/history.go) is a test stub — it is never wired
	// here and is not on the runtime path.
	historyStore := agentstore.NewHistoryStore(pool)
	kernel, err := safety.NewKernel(pool)
	if err != nil {
		return nil, fmt.Errorf("agent wiring: safety kernel: %w", err)
	}

	sdkCfg := doraclient.NewConfiguration()
	sdkCfg.Servers = []doraclient.ServerConfiguration{{URL: cfg.DoraBaseURL}}
	doraAPIClient := doraclient.NewAPIClient(sdkCfg)

	// wsbroker is optional: without DORA_API_KEY the broker stays
	// nil, Recover still runs (marks orphaned rows crashed), and Deploy
	// fails at runtime with "WsBroker not configured".
	var broker *wsbroker.Broker
	if apiKey := os.Getenv(doraAPIKeyEnv); apiKey != "" {
		wsURL := cfg.WsBrokerURL
		if wsURL == "" {
			wsURL = strings.TrimRight(cfg.DoraBaseURL, "/") + "/plex"
		}
		broker, err = wsbroker.New(wsbroker.Config{URL: wsURL, APIKey: apiKey})
		if err != nil {
			return nil, fmt.Errorf("agent wiring: wsbroker: %w", err)
		}
	} else {
		log.Warn("DORA_API_KEY not set; live broker disabled")
	}

	deployStore := agentdeployment.NewPgStore(pool)
	versions := agentstrategies.NewPgStore(pool)
	orderBroker := orderbroker.New(doraAPIClient)
	resolveVersion := func(d agentdeployment.Deployment) (string, string, error) {
		v, err := versions.GetVersion(ctx, d.StrategyID, agentstrategies.Revision(d.Revision))
		if err != nil {
			return "", "", err
		}
		return v.WasmRef, v.ManifestHash, nil
	}
	auditSink := func(ctx context.Context, doraUserID, action string, detail []byte) error {
		return agentaudit.Insert(ctx, pool, doraUserID, action, detail)
	}

	liveOrch, err := orchestrator.New(orchestrator.Config{
		Store:           wasmRT.Store(),
		Registry:        wasmRT.Registry(),
		DeployStore:     deployStore,
		WsBroker:        broker,
		Kernel:          kernel,
		Orders:          orderBroker,
		Logger:          log,
		AuditSink:       auditSink,
		History:         historyStore,
		VersionResolver: resolveVersion,
		RestartCfg: orchestrator.RestartConfig{
			Window:      cfg.LiveRestartWindow,
			MaxRestarts: cfg.LiveMaxRestarts,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("agent wiring: orchestrator: %w", err)
	}

	if broker != nil {
		go func() {
			if err := broker.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("agent wsbroker stopped", "error", err)
			}
		}()
	}

	// Recover orphaned running deployments from a previous process.
	// Non-fatal: a failed recover logs and leaves the rows for the next
	// restart.
	kp := func(ctx context.Context, userID string) (string, error) {
		sealed, err := usersStore.GetEncryptedKey(ctx, userID)
		if err != nil {
			return "", fmt.Errorf("get encrypted key: %w", err)
		}
		if sealed == nil {
			return "", errors.New("no stored Dora key for user — re-authenticate to store it")
		}
		plain, err := sealer.Open(sealed)
		if err != nil {
			return "", fmt.Errorf("unseal key: %w", err)
		}
		return string(plain), nil
	}
	recovered, err := liveOrch.Recover(ctx, orchestrator.KeyProvider(kp))
	if err != nil {
		log.Error("agent deployment recovery failed", "error", err)
	} else if recovered > 0 {
		log.Info("agent deployments recovered", "count", recovered)
	}

	btStore := agentbacktest.NewPgStore(pool)
	wasmStarter := agentbacktest.NewWasmStarter(
		wasmRT.Registry(), wasmRT.Store(), btStore, doraAPIClient, historyStore, log,
	)

	// Capture-pending janitor: sweeps stashed failed-capture rows.
	strategiesJani := agentstrategies.NewJanitor(
		versions, cfg.CapturePendingSweepInterval, cfg.CapturePendingRetention, log,
	)
	strategiesJani.Start(ctx)

	runner, btOrch := newAgentRunner(configs, cfg, wasmStarter, btStore, wasmRT.Store(),
		modelCaps, deployStore, liveOrch, versions, log)

	// Startup janitor: flip orphaned queued/running backtest rows. A
	// crashed previous run can't still be executing on a clean start.
	if err := agentbacktest.RunStartupJanitor(ctx, btStore); err != nil {
		log.Error("agent backtest startup janitor failed", "error", err)
	}

	server := httpapi.New(
		sessions, configs,
		httpapi.WithAgentRunner(runner),
		httpapi.WithProviderFactory(providerFactory(configs, cfg.LLMTimeout)),
		httpapi.WithModelCaps(modelCaps),
		httpapi.WithRateLimit(cfg.RateLimitPerMin),
		httpapi.WithStrategies(versions),
		httpapi.WithBacktest(btOrch, btStore),
		httpapi.WithWasmStarter(wasmStarter),
		httpapi.WithDeploymentStore(deployStore),
		httpapi.WithOrchestrator(liveOrch),
		httpapi.WithAuditWriter(httpapi.PoolAuditWriter{Pool: pool}),
		httpapi.WithSafetyKernel(kernel),
	)

	return &Runtime{
		Server:         server,
		StrategiesJani: strategiesJani,
		BtOrch:         btOrch,
		WSBroker:       broker,
		LiveOrch:       liveOrch,
		WASMRuntime:    wasmRT,
		History:        historyStore,
		users:          usersStore,
		sessions:       sessions,
		configs:        configs,
		versions:       versions,
		deployStr:      deployStore,
		btStore:        btStore,
		kernel:         kernel,
		sealer:         sealer,
	}, nil
}

// EnsurePrincipal upserts the local user mirror — the FK target for
// every per-user table (provider_configs, sessions, strategies, etc.).
// Called from the host's agentPrincipalBridge on every authenticated
// request so the mirror stays present without an explicit sync job.
// Matches the original dora-agent AuthMiddleware: failure is fatal
// for the request (FK would reject every downstream write) and the
// bridge maps it to 502 "auth service unavailable".
//
// Roles are hardcoded to {TRADER, ADMIN} as a placeholder for the
// role gating the original agent enforced via AllowedDoraRoles +
// audit.ActionAuthRoleRejected. Role enforcement was intentionally
// dropped in the merger and has not yet been restored. The host's
// strategyhttp.AuthInfo carries no roles today; when role gating
// returns, this constant goes away and roles flow in from the auth
// context.
//
// TODO: when role gating is restored, remove the hardcoded roles
// below and accept the principal's roles from the host auth context.
func (r *Runtime) EnsurePrincipal(ctx context.Context, userID, tenantID string) error {
	if r == nil || r.users == nil {
		return errors.New("agent wiring: EnsurePrincipal called without users store")
	}
	if err := r.users.Ensure(ctx, userID, tenantID, []string{"TRADER", "ADMIN"}); err != nil {
		return fmt.Errorf("ensure user: %w", err)
	}
	return nil
}

// StorePrincipalKey seals the raw Dora API key with the runtime's
// Sealer and persists it in agent.users.api_key so the live
// orchestrator can recover running deployments after a crash without
// prompting the user to re-authenticate. Matches the original
// dora-agent AuthMiddleware: failure is non-fatal — the request still
// works, only crash recovery degrades. Caller logs at warn level
// (matches the original middleware's posture).
func (r *Runtime) StorePrincipalKey(ctx context.Context, userID, apiKey string) error {
	if r == nil || r.sealer == nil || apiKey == "" {
		return nil
	}
	sealed, err := r.sealer.Seal([]byte(apiKey))
	if err != nil {
		return fmt.Errorf("seal api key: %w", err)
	}
	if err := r.users.StoreKey(ctx, userID, sealed); err != nil {
		return fmt.Errorf("store key: %w", err)
	}
	return nil
}

// Close unwinds the runtime's background workers: cancels in-flight
// backtests, stops the capture-pending janitor, the wsbroker, and the
// wazero registry. Idempotent per component.
func (r *Runtime) Close(_ context.Context) error {
	if r == nil {
		return nil
	}
	if r.BtOrch != nil {
		r.BtOrch.CancelAll()
	}
	if r.StrategiesJani != nil {
		r.StrategiesJani.Stop()
	}
	if r.WSBroker != nil {
		_ = r.WSBroker.Stop()
	}
	if r.WASMRuntime != nil {
		_ = r.WASMRuntime.Close()
	}
	return nil
}

// providerFactory builds the any-llm-go provider for a turn: it resolves
// the decrypted API key + base_url + default model for the user's saved
// config, constructs the provider, and wraps it in the shim. Returns the
// resolved model (the request model, or the config default when the
// request omits it).
func providerFactory(configs *providerconfig.Store, llmTimeout time.Duration) httpapi.ProviderFactory {
	return func(ctx context.Context, userID, provider, model string) (llm.Provider, string, error) {
		apiKey, defaultModel, baseURL, err := configs.GetDecrypted(ctx, userID, provider)
		if err != nil {
			return nil, "", fmt.Errorf("provider config: %w", err)
		}
		if model == "" {
			model = defaultModel
		}
		opts := []anyllm.Option{anyllm.WithAPIKey(apiKey), anyllm.WithTimeout(llmTimeout)}
		if baseURL != "" {
			opts = append(opts, anyllm.WithBaseURL(baseURL))
		}
		var p anyllm.Provider
		switch provider {
		case "openai":
			p, err = openai.New(opts...)
		case "anthropic":
			p, err = anthropic.New(opts...)
		case "openrouter":
			if baseURL == "" {
				opts = append(opts, anyllm.WithBaseURL("https://openrouter.ai/api/v1"))
			}
			p, err = openai.New(opts...)
		default:
			return nil, "", fmt.Errorf("unsupported provider %q", provider)
		}
		if err != nil {
			return nil, "", fmt.Errorf("provider init: %w", err)
		}
		return anyllmshim.New(p), model, nil
	}
}

// classifierFactory builds a sanitize.Classifier bound to the provider+model
// the strategy-building hop uses. The factory receives the already-built
// provider from the driver, so there is no second build.
func classifierFactory() httpapi.ClassifierFactory {
	return func(provider llm.Provider, model string) httpapi.Classifier {
		return sanitize.NewClassifier(provider, model)
	}
}

// toolFactory builds the per-turn tool set: the eight Dora read tools
// (omitted when AGENT_DORA_TOOLS_ENABLED=false), get_strategy,
// read_strategy_sources, generate_strategy, run_backtest,
// get_backtest_result, list_backtests, cancel_backtest, and the eight
// deployment tools. The Dora handlers are constructed fresh each turn so
// the API key is scoped to one request; everything else is a per-process
// singleton safe to share across turns.
func toolFactory(
	cfg config.Config,
	genHandler *generate.Handler,
	btStore agentbacktest.Store,
	btOrch *agentbacktest.Orchestrator,
	wasmStarter *agentbacktest.WasmStarter,
	versions *agentstrategies.PgStore,
	deployStore agentdeployment.Store,
	liveOrch *orchestrator.Orchestrator,
) httpapi.ToolFactory {
	return func(doraAPIKey, userID string) ([]llm.ToolSpec, map[string]llm.ToolHandler) {
		var specs []llm.ToolSpec
		handlers := map[string]llm.ToolHandler{}
		if cfg.DoraToolsEnabled {
			doraHandlers := doratool.NewHandlers(cfg.DoraBaseURL, doraAPIKey)
			doraSpecs := doraHandlers.Tools()
			specs = append(specs, doraSpecs...)
			for _, s := range doraSpecs {
				handlers[s.Name] = doraHandlers.Invoke
			}
		}
		specs = append(specs, strategiestool.Spec())
		handlers["get_strategy"] = strategiestool.Handler(versions, userID)
		specs = append(specs, strategiestool.ReadSpec())
		handlers["read_strategy_sources"] = strategiestool.ReadHandler(versions, userID)
		specs = append(specs, genHandler.Tools()...)
		handlers["generate_strategy"] = genHandler.ForUser(userID)
		specs = append(specs, backtesttool.RunSpec())
		handlers["run_backtest"] = backtesttool.RunHandler(
			btOrch, versions, wasmStarter, btStore, userID, doraAPIKey, migration.New(),
		)
		specs = append(specs, backtesttool.Spec())
		handlers["get_backtest_result"] = backtesttool.Handler(btStore, userID)
		specs = append(specs, backtesttool.ListSpec())
		handlers["list_backtests"] = backtesttool.ListHandler(btStore, userID)
		specs = append(specs, backtesttool.CancelSpec())
		handlers["cancel_backtest"] = backtesttool.CancelHandler(btStore, btOrch, userID)
		if liveOrch != nil && deployStore != nil {
			deploySpecs, deployHandlers := deploymenttool.Tools(
				deployStore, liveOrch, versions, userID, doraAPIKey, migration.New(),
			)
			specs = append(specs, deploySpecs...)
			for name, h := range deployHandlers {
				handlers[name] = h
			}
		}
		return specs, handlers
	}
}

// newAgentRunner wires the production runner: the per-turn provider
// factory, the classifier factory, the per-turn tool factory (Dora read
// tools + generate_strategy with a real repairer), the Postgres-backed
// strategy version store, and the pool-backed audit writer. It also
// returns the backtest orchestrator (the cancel-context registry for
// in-flight WASM backtests).
func newAgentRunner(
	configs *providerconfig.Store,
	cfg config.Config,
	wasmStarter *agentbacktest.WasmStarter,
	btStore agentbacktest.Store,
	wasmArtifactStore wasmstore.ArtifactStore,
	modelCaps config.ModelCaps,
	deployStore agentdeployment.Store,
	liveOrch *orchestrator.Orchestrator,
	versions *agentstrategies.PgStore,
	log *slog.Logger,
) (httpapi.AgentRunner, *agentbacktest.Orchestrator) {
	repairer := generate.NewRepairer(
		validate.New(validate.Config{
			GOPROXY:           cfg.Generate.GOPROXY,
			Allowlist:         cfg.Generate.Allowlist,
			WasmArtifactStore: wasmArtifactStore,
		}),
		cfg.Generate.MaxRepairs,
	)
	genHandler := generate.NewHandler(repairer, versions, migration.New(), generate.Config{
		MaxFiles:   cfg.Generate.MaxFiles,
		MaxBytes:   cfg.Generate.MaxBytes,
		MaxRepairs: cfg.Generate.MaxRepairs,
	})
	btOrch := agentbacktest.NewOrchestrator(btStore, wasmStarter)
	runner := httpapi.NewStrategyRunner(
		providerFactory(configs, cfg.LLMTimeout),
		classifierFactory(),
		toolFactory(cfg, genHandler, btStore, btOrch, wasmStarter, versions, deployStore, liveOrch),
		httpapi.PoolAuditWriter{Pool: configs.Pool},
		versions,
		repairer,
		modelCaps,
		cfg.LLMTimeout,
		cfg.LLMMaxIters,
	)
	log.Info("agent runner wired", "strategy_generation_target", "go-wasm")
	return runner, btOrch
}
