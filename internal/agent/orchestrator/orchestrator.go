// Package orchestrator drives the WASM plugin lifecycle. It owns
// the restart budget, the validate-smoke path (in-process wazero
// via the registry, not a separate binary), and the live
// deployment runtime (per-deployment goroutine, wazero host
// module wired to the safety kernel and order broker, crash
// recovery on boot).
package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/deployment"

	history "github.com/dora-network/bond-trading-strategies/internal/agent/store"

	"github.com/dora-network/bond-trading-strategies/internal/agent/orderbroker"
	"github.com/dora-network/bond-trading-strategies/internal/agent/safety"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
	wasmstore "github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wsbroker"
)

// VersionResolver returns the (WasmRef, ManifestHash) for a
// deployment row's strategy + revision. main.go wires the
// strategies.Store lookup; tests inject a fixed pair. Required
// for Resume / HotSwap / Recover; without it the orchestrator
// falls back to deployment.Revision (and emits a 0/0 result for
// rows without a revision).
type VersionResolver func(d deployment.Deployment) (wasmRef, manifestHash string, err error)

// Config is the orchestrator configuration. The first three
// fields (Store, Registry, Timeout) are required for the
// validate smoke path. The remaining fields are required for
// the live deployment runtime (Deploy/Stop/Resume/HotSwap/
// Recover) but optional for tests that only exercise Validate.
type Config struct {
	Store    wasmstore.ArtifactStore
	Registry *registry.Registry
	Timeout  time.Duration // wall-clock budget for Validate

	// Live runtime. Deploy fails if these are unset.
	DeployStore     deployment.Store
	WsBroker        *wsbroker.Broker
	Kernel          *safety.Kernel
	Orders          *orderbroker.Broker
	Logger          *slog.Logger
	AuditSink       func(ctx context.Context, doraUserID, action string, detail []byte) error
	VersionResolver VersionResolver
	History         *history.HistoryStore

	// Restart policy for the live runtime.
	RestartCfg   RestartConfig
	StartTimeout time.Duration // per-plugin _start deadline; default 30s
}

// Orchestrator is the lifecycle owner.
type Orchestrator struct {
	cfg Config

	liveMu        sync.Mutex
	liveInstances map[string]*liveInstance
}

// Lifecycle defaults applied in New when Config leaves them zero.
const (
	defaultTimeout       = 10 * time.Second
	defaultStartTimeout  = 30 * time.Second
	defaultRestartWindow = 60 * time.Second
)

// New constructs an Orchestrator.
func New(cfg Config) (*Orchestrator, error) {
	if cfg.Store == nil {
		return nil, errors.New("orchestrator: nil Store")
	}
	if cfg.Registry == nil {
		return nil, errors.New("orchestrator: nil Registry")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.StartTimeout == 0 {
		cfg.StartTimeout = defaultStartTimeout
	}
	if cfg.RestartCfg.Window == 0 {
		cfg.RestartCfg.Window = defaultRestartWindow
	}
	if cfg.RestartCfg.MaxRestarts == 0 {
		cfg.RestartCfg.MaxRestarts = 3
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(discardWriter{}, nil))
	}
	return &Orchestrator{
		cfg:           cfg,
		liveInstances: make(map[string]*liveInstance),
	}, nil
}

// DeploymentStats is the live operational telemetry for a running
// deployment. Read from the in-memory liveHostState — not persisted.
type DeploymentStats struct {
	CandleCount    int64     `json:"candle_count"`
	LastCandleAt   time.Time `json:"last_candle_at,omitempty"`
	LastCandle     string    `json:"last_candle,omitempty"`
	LastDecisionAt time.Time `json:"last_decision_at,omitempty"`
	LastDecision   string    `json:"last_decision,omitempty"`
	BuyCount       int64     `json:"buy_count"`
	SellCount      int64     `json:"sell_count"`
}

// Stats returns the live operational telemetry for a running
// deployment. Returns ok=false if the deployment is not currently
// running in this process.
func (o *Orchestrator) Stats(deploymentID string) (DeploymentStats, bool) {
	o.liveMu.Lock()
	li, ok := o.liveInstances[deploymentID]
	o.liveMu.Unlock()
	if !ok || li.state == nil {
		return DeploymentStats{}, false
	}
	li.state.candleMu.Lock()
	defer li.state.candleMu.Unlock()
	return DeploymentStats{
		CandleCount:    li.state.candleCount,
		LastCandleAt:   li.state.lastCandleAt,
		LastCandle:     li.state.lastCandle,
		LastDecisionAt: li.state.lastDecisionAt,
		LastDecision:   li.state.lastDecision,
		BuyCount:       li.state.buyCount,
		SellCount:      li.state.sellCount,
	}, true
}

// ValidateResult is the smoke result.
type ValidateResult struct {
	Passed   bool
	Stderr   string
	Duration time.Duration
}

// Validate runs the in-process wazero smoke for a (.wasm, manifest)
// pair. It loads the module via the registry and calls Init. This
// is the same code path the validator's validateWasm uses — there
// is no separate wasm-smoke binary.
func (o *Orchestrator) Validate(ctx context.Context, wasmHash, manifestHash string) (ValidateResult, error) {
	ctx, cancel := context.WithTimeout(ctx, o.cfg.Timeout)
	defer cancel()

	start := time.Now()
	inst, err := o.cfg.Registry.Load(ctx, o.cfg.Store, wasmHash, manifestHash)
	if err != nil {
		return ValidateResult{Passed: false, Stderr: err.Error(), Duration: time.Since(start)}, err
	}
	defer inst.Close(ctx)

	if err := o.cfg.Registry.Init(ctx, inst); err != nil {
		return ValidateResult{Passed: false, Stderr: err.Error(), Duration: time.Since(start)}, err
	}
	return ValidateResult{Passed: true, Duration: time.Since(start)}, nil
}

// discardWriter is the io.Writer slog uses when no logger is
// configured. We avoid pulling io.Discard into the orchestrator
// imports — the type is small and the orchestrator never logs
// anything in production.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// ensureDepsForDeploy returns a typed error if any live-runtime
// dependency is missing. Called at the top of Deploy.
func (o *Orchestrator) ensureDepsForDeploy() error {
	switch {
	case o.cfg.WsBroker == nil:
		return errors.New("orchestrator: WsBroker not configured")
	case o.cfg.Kernel == nil:
		return errors.New("orchestrator: Kernel not configured")
	case o.cfg.Orders == nil:
		return errors.New("orchestrator: Orders not configured")
	case o.cfg.DeployStore == nil:
		return errors.New("orchestrator: DeployStore not configured")
	}
	return nil
}
