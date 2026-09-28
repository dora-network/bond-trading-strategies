package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"

	dorastrategy "github.com/dora-network/dora-strategy-wasm/dorastrategy"
	"github.com/dora-network/dora-strategy-wasm/manifest"

	"github.com/dora-network/bond-trading-strategies/internal/agent/audit"
	"github.com/dora-network/bond-trading-strategies/internal/agent/deployment"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wsbroker"
)

// liveInstance is the in-memory state of one running deployment.
// The orchestrator keeps a map of these guarded by liveMu. The
// cancel func is invoked on Stop / Halt / HotSwap / Recover. The
// goroutine reads from sub and writes into state.frameCh; closing
// sub (via Unsubscribe) terminates the host function's read on
// the frame channel, which lets the WASM module's _start return.
type liveInstance struct {
	deploymentID string
	cancel       context.CancelFunc
	budget       *RestartBudget
	sub          *wsbroker.Subscription
	subTrades    *wsbroker.Subscription
	subPrices    *wsbroker.Subscription
	done         chan struct{}
	state        *liveHostState
}

// DeployConfig is the per-call input to Deploy. The caller supplies
// the wasm artifact references, the user context, and the
// subscription parameters. Revision is the strategy version's
// revision string (e.g. "v3"); the orchestrator stores it on the
// deployment row.
type DeployConfig struct {
	DeploymentID  string
	StrategyID    string
	Revision      string
	UserID        string
	OrderBookID   string
	DoraAPIKey    string
	WasmRef       string
	Resolution    string
	ManifestHash  string
	WarmupCandles int // preamble fetch window (resolution-spaced candles)
	Params        map[string]string
}

// defaultCandlePersistInterval is the cadence at which the live
// host flushes the in-memory candleCount to the deployments row.
// Chosen as 30s so a 1m-resolution deployment flushes ~once per
// 30 candles (loses at most 29 in a crash) and a 15m deployment
// flushes ~per-candle. The exact value is also used as a count
// floor in the hostNextLiveCandle throttle — the first
// persisted write happens at this many candles so even a
// short-lived deployment that never reaches the time interval
// still has its count on disk.
const defaultCandlePersistInterval = 30

// validateForLive rejects configurations that would start a live
// deployment against the wrong candle stream. Both OrderBookID and
// Resolution are required: an empty OrderBookID would subscribe to
// no books, and an empty Resolution would silently match every
// resolution on the subscribed book, feeding the strategy bars
// from timeframes it was not designed for. Callers (HTTP handler,
// LLM tool, Resume, Recover) MUST surface these errors rather
// than fall back to a default — see the wsbroker Subscription.matches
// rules and the deploy_strategy schema.
func (c DeployConfig) validateForLive() error {
	if c.DeploymentID == "" {
		return errors.New("orchestrator: DeploymentID is required")
	}
	if c.OrderBookID == "" {
		return fmt.Errorf("orchestrator: deployment %s: OrderBookID is required", c.DeploymentID)
	}
	if c.Resolution == "" {
		return fmt.Errorf("orchestrator: deployment %s: Resolution is required; refusing to start with implicit default", c.DeploymentID)
	}
	return nil
}

// KeyProvider returns the per-user decrypted Dora API key. main.go
// supplies this so the orchestrator never touches the encrypted
// key material directly. The recovered Deploy calls use the
// returned key exactly once per deployment.
type KeyProvider func(ctx context.Context, userID string) (string, error)

// Deploy starts a new live deployment: subscribe to the wsbroker
// for the order book, load the .wasm module, build the live host
// module, instantiate, and run the module's _start in a goroutine.
// A liveInstance entry is added to the orchestrator's map before
// the goroutine starts and removed by the goroutine on exit.
//
// On a duplicate Deploy (same deploymentID already in the map),
// Deploy is a no-op error so the HTTP handler can return 409.
// The caller is expected to have already validated the deployment
// row (idempotency / HasActive).
// buildLiveHostState constructs the per-deployment liveHostState and
// seeds state.candleCount from the persisted deployments row so the
// count survives every restart path (server boot, Recover, Resume,
// HotSwap, in-process crash recovery). Pre-existing row -> use the
// persisted value. Fresh deployment -> 0.
//
// sub is the candle subscription (required). subTrades / subPrices are
// the trade / price subscriptions (may be nil when the manifest
// declares no trade / price channels). manifest provides the assetID
// and warmup-candle count for the preamble fetch window.
func (o *Orchestrator) buildLiveHostState(
	ctx context.Context,
	cfg DeployConfig,
	sub, subTrades, subPrices *wsbroker.Subscription,
	manifest manifest.Manifest,
	cfgJSON []byte,
	auditHook auditHook,
) *liveHostState {
	state := &liveHostState{
		userID:                cfg.UserID,
		strategyID:            cfg.StrategyID,
		deployment:            cfg.DeploymentID,
		orderBook:             cfg.OrderBookID,
		apiKey:                cfg.DoraAPIKey,
		candleCh:              sub.Chan(),
		cfgJSON:               cfgJSON,
		kernel:                o.cfg.Kernel,
		orders:                o.cfg.Orders,
		audit:                 auditHook,
		logger:                o.cfg.Logger,
		deployStore:           o.cfg.DeployStore,
		candlePersistInterval: defaultCandlePersistInterval,
		history:               o.cfg.History,
		resolution:            cfg.Resolution,
		warmupCandles:         cfg.WarmupCandles,
		preambleInProgress:    true,
	}
	if subTrades != nil {
		state.tradeCh = subTrades.Chan()
	}
	if subPrices != nil {
		state.priceCh = subPrices.Chan()
	}
	if len(manifest.Capabilities.AssetIDs) > 0 {
		state.assetID = manifest.Capabilities.AssetIDs[0]
	}
	if o.cfg.DeployStore != nil {
		if d, derr := o.cfg.DeployStore.Get(ctx, cfg.DeploymentID, cfg.UserID); derr == nil {
			state.candleCount = d.CandleCount
		}
	}
	return state
}

func (o *Orchestrator) Deploy(ctx context.Context, cfg DeployConfig) (err error) { //nolint:funlen // deadlock fix added a line
	// Operator visibility: every failure path in Deploy (validation,
	// nil WsBroker, nil DeployStore, duplicate, build config, registry
	// load, instantiate) goes through this single return slot. The
	// deferred log fires once with the same error that callers
	// (`handleDeploy`, `deployHandler`, `Recover`, etc.) propagate to
	// their response bodies. Pre-fix the error only reached the LLM
	// via the response body, so the operator (who can't see the LLM
	// conversation) had no way to diagnose why a deploy failed.
	//
	// The defer is registered FIRST, before cfg.validateForLive() runs,
	// so validation rejections (empty deployment_id / order_book_id /
	// resolution) also land in the operator log. The earlier placement
	// (after validateForLive) missed that case — the LLM tool path's
	// "validation error" was the original user-reported bug.
	defer func() {
		if err == nil {
			return
		}
		o.cfg.Logger.Error(
			"orchestrator: deploy failed",
			"deployment", cfg.DeploymentID,
			"strategy", cfg.StrategyID,
			"user", cfg.UserID,
			"order_book", cfg.OrderBookID,
			"resolution", cfg.Resolution,
			"error", err,
		)
	}()
	if err := cfg.validateForLive(); err != nil {
		return err
	}

	if o.cfg.WsBroker == nil {
		return errors.New("orchestrator: nil WsBroker")
	}
	if o.cfg.DeployStore == nil {
		return errors.New("orchestrator: nil DeployStore")
	}

	o.liveMu.Lock()
	if _, exists := o.liveInstances[cfg.DeploymentID]; exists {
		o.liveMu.Unlock()
		return fmt.Errorf("orchestrator: deployment %s already running", cfg.DeploymentID)
	}
	o.liveMu.Unlock()
	// 1. Subscribe to the wsbroker. The candle subscription's channel
	//    feeds host_next_event's candle case; closing it (via
	//    Unsubscribe) signals the live loop to exit. Trade and price
	//    subscriptions are created after the manifest is loaded (they
	//    depend on the manifest's channels + asset_ids).
	sub := o.cfg.WsBroker.Subscribe(cfg.OrderBookID, cfg.Resolution, []string{"candle"})

	// 2. Build the live config JSON for the plugin.
	cfgJSON, err := buildLiveConfigJSON(cfg)
	if err != nil {
		o.cfg.WsBroker.Unsubscribe(sub)
		return fmt.Errorf("orchestrator: build config: %w", err)
	}

	// 3. Load the compiled module (carries the validated manifest).
	inst, err := o.cfg.Registry.Load(ctx, o.cfg.Store, cfg.WasmRef, cfg.ManifestHash)
	if err != nil {
		o.cfg.WsBroker.Unsubscribe(sub)
		return fmt.Errorf("orchestrator: load wasm: %w", err)
	}

	// 3b. Subscribe to trades + prices per the manifest's channels.
	//     Trades are order-book-keyed; prices are asset-keyed. Both
	//     are nil when the manifest declares no trade / price channel.
	var subTrades, subPrices *wsbroker.Subscription
	if hasChannel(inst.Manifest, "trade") {
		subTrades = o.cfg.WsBroker.SubscribeTrades(cfg.OrderBookID)
	}
	if hasChannel(inst.Manifest, "price") && len(inst.Manifest.Capabilities.AssetIDs) > 0 {
		subPrices = o.cfg.WsBroker.SubscribePrices(inst.Manifest.Capabilities.AssetIDs[0])
	}

	// 4. Build the host state. The audit hook falls back to a noop
	//    if the orchestrator was built without a sink.
	auditHook := o.auditFor(cfg.UserID)

	deployCfg := cfg
	if inst.Manifest.Preamble.WarmupCandles > 0 {
		// The manifest is authoritative when it declares a preamble
		// window; otherwise keep the row's persisted value (Resume /
		// Recover copied it in) so a manifest without preamble does
		// not silently reset the warmup window to zero.
		deployCfg.WarmupCandles = inst.Manifest.Preamble.WarmupCandles
	}
	state := o.buildLiveHostState(ctx, deployCfg, sub, subTrades, subPrices, inst.Manifest, cfgJSON, auditHook)

	// 5. Cancelable context for the goroutine.
	runCtx, cancel := context.WithCancel(context.Background())

	// 6. Build host + instantiate the guest module.
	var hostMod api.Module
	buildHost := func(rt wazero.Runtime) error {
		var err error
		hostMod, err = buildLiveHostModule(runCtx, rt, state)
		return err
	}
	mod, err := o.cfg.Registry.InstantiateWithHostDeferred(runCtx, inst, buildHost)
	if err != nil {
		cancel()
		inst.Close(ctx)
		o.cfg.WsBroker.Unsubscribe(sub)
		o.cfg.WsBroker.Unsubscribe(subTrades)
		o.cfg.WsBroker.Unsubscribe(subPrices)
		return fmt.Errorf("orchestrator: instantiate: %w", err)
	}

	// 7. Register the live instance.
	li := &liveInstance{
		deploymentID: cfg.DeploymentID,
		cancel:       cancel,
		budget:       NewRestartBudget(o.cfg.RestartCfg),
		sub:          sub,
		subTrades:    subTrades,
		subPrices:    subPrices,
		done:         make(chan struct{}),
		state:        state,
	}
	o.liveMu.Lock()
	o.liveInstances[cfg.DeploymentID] = li
	o.liveMu.Unlock()

	// 8. Spawn the goroutine. The goroutine owns the lifecycle of
	//    the wazero module + host module; when _start returns or
	//    the context is cancelled, the defer cleans up.
	go o.runLive(runCtx, li, inst, mod, hostMod, state) //nolint:gosec // intentional: deployments outlive HTTP requests

	// 9. Re-sync the row's warmup_candles to the manifest's value.
	//    The handler / tool seeded the row from the request's field,
	//    which can be stale or zero; the manifest is authoritative
	//    (Resume / Recover rebuild the warmup window from the row).
	o.syncWarmupCandles(ctx, cfg, inst.Manifest.Preamble.WarmupCandles)

	o.cfg.Logger.Info(
		"orchestrator: deploy started",
		"deployment", cfg.DeploymentID,
		"strategy", cfg.StrategyID,
		"order_book", cfg.OrderBookID,
	)
	return nil
}

// runLive drives the WASM module's _start export. _start blocks
// running the framework's runLive loop, which calls
// host_next_live_candle (and friends) via the host module. The
// goroutine:
//
//   - recovers from panics inside _start and either restarts (if
//     the restart budget allows) or marks the deployment crashed.
//   - feeds candles from the wsbroker subscription into
//     state.frameCh — but in this design the subscription's channel
//     IS state.frameCh, so no extra goroutine is needed: the
//     host function reads directly from the subscription.
//   - cleans up the wazero module and instance on exit.
//
// The frame-routing is one hop: the wsbroker's read goroutine
// fans frames into sub.Chan(), and the host function reads from
// state.frameCh (== sub.Chan()) on the wazero call stack. wazero
// is single-threaded, so host_next_live_candle blocks the module
// until a frame arrives — exactly the design.
func (o *Orchestrator) runLive(
	ctx context.Context,
	li *liveInstance,
	inst *registry.Instance,
	mod api.Module,
	hostMod api.Module,
	state *liveHostState,
) {
	defer close(li.done)
	defer o.teardownLive(li, inst, mod, hostMod)

	if err := o.invokeStart(ctx, mod); err != nil {
		// exit_code(0) is a clean module exit (channel closed →
		// host_next_live_candle returned done → runLive returned
		// nil → _start returned). Not an error.
		if strings.Contains(err.Error(), "exit_code(0)") {
			_ = o.cfg.DeployStore.UpdateStatus(context.Background(), li.deploymentID, state.userID, deployment.StatusStopped, "clean exit")
			o.cfg.Logger.Info("orchestrator: live loop exited cleanly", "deployment", li.deploymentID)
			return
		}
		o.handleRunError(ctx, li, state, err)
		return
	}

	// _start returned without error — clean exit.
	o.cfg.Logger.Info("orchestrator: live loop exited cleanly", "deployment", li.deploymentID)
}

// invokeStart calls the module's _start export. For live deployments
// _start blocks indefinitely in the candle loop; it only returns
// when the deployment context is cancelled (Stop) or the frame
// channel closes. No timeout — the deployment context is the
// lifecycle bound.
func (o *Orchestrator) invokeStart(ctx context.Context, mod api.Module) error {
	start := mod.ExportedFunction("_start")
	if start == nil {
		return errors.New("module has no _start export")
	}
	_, err := start.Call(ctx)
	return err
}

// handleRunError audits the failure and, if the restart budget
// allows, re-deploys. Otherwise marks the deployment crashed.
// persistCandleCount flushes the in-memory candleCount to the
// deployments row. Used by the livehost throttle and by
// handleRunError before tearing down the failed goroutine. Best
// effort: a failed write is logged at debug and dropped (the
// count will simply lose a few ticks on the next crash). The
// call holds state.candleMu only for the read; the DB call is
// made without the lock so a slow Postgres round trip does not
// backpressure the candle channel.
func (o *Orchestrator) persistCandleCount(ctx context.Context, state *liveHostState) {
	if o.cfg.DeployStore == nil || state == nil {
		return
	}
	state.candleMu.Lock()
	n := state.candleCount
	state.candleMu.Unlock()
	if n < 0 {
		n = 0
	}
	if err := o.cfg.DeployStore.SetCandleCount(ctx, state.deployment, state.userID, n); err != nil {
		if state.logger != nil {
			state.logger.Debug("orchestrator: persist candle count failed",
				"deployment", state.deployment, "error", err)
		}
	}
}

// syncWarmupCandles re-syncs the deployment row's warmup_candles to
// the manifest's value after Deploy has started the runtime. The
// request's warmup_candles field (which seeded the row) is not
// authoritative — if it disagrees with the manifest, Resume /
// Recover would rebuild the warmup window from the wrong count. Best
// effort: a failed write is logged at debug and dropped; the next
// Deploy (restart / recover) re-syncs again.
func (o *Orchestrator) syncWarmupCandles(ctx context.Context, cfg DeployConfig, warmupCandles int) {
	if warmupCandles <= 0 {
		// Manifest declares no preamble; leave the row's persisted
		// value alone (see Deploy).
		return
	}
	if o.cfg.DeployStore == nil {
		return
	}
	if err := o.cfg.DeployStore.UpdateWarmupCandles(ctx, cfg.DeploymentID, cfg.UserID, warmupCandles); err != nil {
		o.cfg.Logger.Debug("orchestrator: sync warmup_candles failed",
			"deployment", cfg.DeploymentID, "error", err)
	}
}

// buildRestartConfig constructs the DeployConfig used by
// handleRunError to re-Deploy after a failed live loop. Critical:
// Resolution and Params are read from the persisted deployments
// row, NOT from the in-memory liveHostState. Without that, a
// restart where the in-memory state has been cleared (or never
// populated) would fail validateForLive and crash instead of
// restart. WasmRef is reloaded from the row's revision (full
// manifest_hash reload is wired in Plan 5).
func (o *Orchestrator) buildRestartConfig(ctx context.Context, li *liveInstance, state *liveHostState) DeployConfig {
	cfg := DeployConfig{
		DeploymentID: li.deploymentID,
		StrategyID:   state.strategyID,
		UserID:       state.userID,
		OrderBookID:  state.orderBook,
		DoraAPIKey:   state.apiKey,
	}
	if d, derr := o.cfg.DeployStore.Get(ctx, li.deploymentID, state.userID); derr == nil {
		cfg.WasmRef = d.Revision // for back-compat; full reload wired in Plan 5
		cfg.Resolution = d.Resolution
		cfg.Params = d.Params
		cfg.WarmupCandles = d.WarmupCandles
	}
	return cfg
}

func (o *Orchestrator) handleRunError(ctx context.Context, li *liveInstance, state *liveHostState, err error) {
	o.cfg.Logger.Error(
		"orchestrator: live loop exited with error",
		"deployment", li.deploymentID,
		"error", err,
	)
	_ = o.auditFor(state.userID).Record(
		ctx, string(audit.ActionPluginPanic),
		fmt.Sprintf(
			`{"deployment":%q,"error":%q}`,
			li.deploymentID, err.Error(),
		),
	)

	if !li.budget.Allow() {
		_ = o.cfg.DeployStore.UpdateStatus(ctx, li.deploymentID, state.userID, deployment.StatusCrashed, "restart budget exhausted")
		return
	}
	// Flush the current candle count to the row before tearing
	// down the goroutine. The teardown happens via the deferred
	// runLive cleanup; this explicit flush guarantees the
	// in-memory count is on disk before Deploy reads it back.
	// (Deploy seeds the new goroutine from the persisted count,
	// so the freshly-deployed goroutine continues from where the
	// crashed one left off.)
	o.persistCandleCount(ctx, state)

	// Best-effort restart: re-Deploy with the same params. The
	// new Deploy seeds state.candleCount from the row we just
	// persisted, so the count survives the goroutine teardown.
	// Errors here mean we cannot restart; mark crashed and let
	// the operator intervene.
	restartCfg := o.buildRestartConfig(ctx, li, state)
	if derr := o.Deploy(ctx, restartCfg); derr != nil {
		_ = o.cfg.DeployStore.IncRestart(ctx, li.deploymentID, state.userID)
		_ = o.cfg.DeployStore.UpdateStatus(
			ctx, li.deploymentID, state.userID,
			deployment.StatusCrashed,
			"in-process restart failed: "+derr.Error(),
		)
		return
	}
	_ = o.cfg.DeployStore.IncRestart(ctx, li.deploymentID, state.userID)
}

// teardownLive closes the wazero module + instance and the host
// module, then unsubscribes from the wsbroker and removes the
// live instance from the orchestrator's map. Idempotent — safe
// to call from defer chains.
func (o *Orchestrator) teardownLive(li *liveInstance, inst *registry.Instance, mod api.Module, hostMod api.Module) {
	if hostMod != nil {
		_ = hostMod.Close(context.Background())
	}
	if mod != nil {
		_ = mod.Close(context.Background())
	}
	if inst != nil {
		inst.Close(context.Background())
	}
	if li.sub != nil {
		o.cfg.WsBroker.Unsubscribe(li.sub)
	}
	o.cfg.WsBroker.Unsubscribe(li.subTrades)
	o.cfg.WsBroker.Unsubscribe(li.subPrices)
	o.liveMu.Lock()
	delete(o.liveInstances, li.deploymentID)
	o.liveMu.Unlock()
}

// Stop signals a running deployment to exit. It cancels the
// goroutine (which closes the frame channel via Unsubscribe and
// waits for the module to exit) and updates the deployment row
// to stopped. The HTTP handler should call this on user-initiated
// stops.
func (o *Orchestrator) Stop(ctx context.Context, deploymentID, userID string) error {
	o.liveMu.Lock()
	li, ok := o.liveInstances[deploymentID]
	o.liveMu.Unlock()
	if !ok {
		// Not running locally. Still mark the row stopped so the
		// state stays consistent across restarts.
		return o.cfg.DeployStore.UpdateStatus(ctx, deploymentID, userID, deployment.StatusStopped, "not running")
	}
	// Cancel the goroutine. The teardown defers close the wazero
	// module, the instance, the subscription, and remove the map
	// entry; we just have to wait.
	li.cancel()
	select {
	case <-li.done:
	case <-ctx.Done():
		return fmt.Errorf("orchestrator: stop timed out: %w", ctx.Err())
	}
	return o.cfg.DeployStore.UpdateStatus(context.Background(), deploymentID, userID, deployment.StatusStopped, "user stop")
}

// Resume restarts a previously stopped deployment. Looks up the
// deployment row, requires the caller to provide the API key
// (per-request from the HTTP handler), and re-runs Deploy.
func (o *Orchestrator) Resume(ctx context.Context, deploymentID, userID, doraAPIKey string) error {
	d, err := o.cfg.DeployStore.Get(ctx, deploymentID, userID)
	if err != nil {
		return fmt.Errorf("orchestrator: resume lookup: %w", err)
	}
	if d.Resolution == "" {
		// Empty resolution on a pre-migration deployment row.
		// Do NOT default — a silent default to 1m could feed a
		// strategy bars from a different timeframe than it was
		// designed for. Operator must fix the row.
		return fmt.Errorf("orchestrator: resume %s: deployment row has no resolution; refusing to resume with implicit default", d.ID)
	}
	if d.OrderBookID == "" {
		return fmt.Errorf("orchestrator: resume %s: deployment row has no order_book_id", d.ID)
	}
	// Resolve the wasm artifacts from the strategy version. Plan
	// 5 wires a strategies.Store here; for now the caller passes
	// them through DeployConfig. We return an error if the
	// strategy's revision hasn't been loaded with a WasmRef.
	wasmRef, manifestHash, err := o.resolveVersion(ctx, d)
	if err != nil {
		return fmt.Errorf("orchestrator: resume resolve: %w", err)
	}

	if err := o.cfg.DeployStore.UpdateStatus(ctx, deploymentID, userID, deployment.StatusRunning, ""); err != nil {
		return fmt.Errorf("orchestrator: resume mark running: %w", err)
	}
	return o.Deploy(ctx, deployConfigFromRow(d, doraAPIKey, wasmRef, manifestHash))
}

// deployConfigFromRow rebuilds the DeployConfig for a restart path
// (Resume, Recover) from the persisted deployment row. WarmupCandles
// comes from the row so a manifest without a preamble declaration
// does not reset the warmup window to zero on restart.
func deployConfigFromRow(d deployment.Deployment, doraAPIKey, wasmRef, manifestHash string) DeployConfig {
	return DeployConfig{
		DeploymentID:  d.ID,
		StrategyID:    d.StrategyID,
		Revision:      d.Revision,
		UserID:        d.UserID,
		OrderBookID:   d.OrderBookID,
		Resolution:    d.Resolution,
		DoraAPIKey:    doraAPIKey,
		WasmRef:       wasmRef,
		ManifestHash:  manifestHash,
		Params:        d.Params,
		WarmupCandles: d.WarmupCandles,
	}
}

// HotSwap swaps a running deployment to a new revision. The old
// module is torn down; the new one is started with the same
// deployment row updated. For now HotSwap uses the existing
// WasmRef/ManifestHash resolved from the new revision; Plan 5
// wires the strategies.Store lookup.
func (o *Orchestrator) HotSwap(
	ctx context.Context,
	deploymentID, userID, newRevision string,
	newWasmRef, newManifestHash string,
	doraAPIKey, orderBookID string,
) error {
	o.liveMu.Lock()
	li, ok := o.liveInstances[deploymentID]
	o.liveMu.Unlock()
	if ok {
		li.cancel()
		<-li.done
	}
	if err := o.cfg.DeployStore.HotSwap(ctx, deploymentID, userID, newRevision); err != nil {
		return fmt.Errorf("orchestrator: store hotswap: %w", err)
	}
	return o.Deploy(ctx, DeployConfig{
		DeploymentID: deploymentID,
		StrategyID:   "", // Plan 5 wires from strategies.Store
		Revision:     newRevision,
		UserID:       userID,
		OrderBookID:  orderBookID,
		DoraAPIKey:   doraAPIKey,
		WasmRef:      newWasmRef,
		ManifestHash: newManifestHash,
	})
}

// Recover restarts every deployment whose row is still status='running'.
// Called on agent boot. Each row is filtered by the safety kernel
// (halted users are skipped) and re-Deployed.
//
// KeyProvider supplies the per-user decrypted Dora API key.
// Plan 5 wires the real DEK-based decryption in main.go.
func (o *Orchestrator) Recover(ctx context.Context, key KeyProvider) (int, error) {
	if o.cfg.DeployStore == nil {
		return 0, errors.New("orchestrator: nil DeployStore")
	}
	if key == nil {
		return 0, errors.New("orchestrator: nil KeyProvider")
	}
	rows, err := o.cfg.DeployStore.ListRunning(ctx)
	if err != nil {
		return 0, fmt.Errorf("orchestrator: list running: %w", err)
	}

	count := 0
	for _, d := range rows {
		if o.cfg.Kernel != nil {
			if halted, _, _ := o.cfg.Kernel.IsHalted(ctx, d.UserID); halted {
				_ = o.cfg.DeployStore.UpdateStatus(ctx, d.ID, d.UserID, deployment.StatusHalted, "kill switch active on boot")
				continue
			}
		}
		apiKey, err := key(ctx, d.UserID)
		if err != nil {
			o.cfg.Logger.Error("orchestrator: recover key decrypt failed",
				"deployment", d.ID, "user", d.UserID, "error", err)
			_ = o.cfg.DeployStore.UpdateStatus(ctx, d.ID, d.UserID, deployment.StatusCrashed, "key decrypt failed")
			continue
		}
		wasmRef, manifestHash, err := o.resolveVersion(ctx, d)
		if err != nil {
			o.cfg.Logger.Error("orchestrator: recover resolve failed",
				"deployment", d.ID, "revision", d.Revision, "error", err)
			continue
		}
		if d.Resolution == "" {
			// Pre-migration row: no resolution persisted. Refuse
			// to start the strategy — a silent default to 1m
			// could feed the strategy bars from the wrong
			// timeframe.
			o.cfg.Logger.Error("orchestrator: recover refuse: deployment row has no resolution",
				"deployment", d.ID, "user", d.UserID)
			_ = o.cfg.DeployStore.UpdateStatus(ctx, d.ID, d.UserID, deployment.StatusCrashed, "missing resolution on deployment row")
			continue
		}
		if d.OrderBookID == "" {
			o.cfg.Logger.Error("orchestrator: recover refuse: deployment row has no order_book_id",
				"deployment", d.ID, "user", d.UserID)
			_ = o.cfg.DeployStore.UpdateStatus(ctx, d.ID, d.UserID, deployment.StatusCrashed, "missing order_book_id on deployment row")
			continue
		}
		if err := o.Deploy(ctx, deployConfigFromRow(d, apiKey, wasmRef, manifestHash)); err != nil {
			o.cfg.Logger.Error("orchestrator: recover deploy failed",
				"deployment", d.ID, "error", err)
			continue
		}
		count++
	}
	o.cfg.Logger.Info("orchestrator: recover complete", "recovered", count, "total_running", len(rows))
	return count, nil
}

// resolveVersion maps a deployment row's strategy+revision to the
// (WasmRef, ManifestHash) pair. Plan 5 wires the strategies.Store
// lookup; until then we treat deployment.Revision as the WasmRef
// (a worst-effort fallback that the existing slice-C tests can
// still drive). When the strategies.Store is wired, this becomes
// strategiesStore.GetVersion(ctx, strategyID, revision).
func (o *Orchestrator) resolveVersion(_ context.Context, d deployment.Deployment) (string, string, error) {
	if o.cfg.VersionResolver != nil {
		return o.cfg.VersionResolver(d)
	}
	if d.Revision == "" {
		return "", "", errors.New("orchestrator: deployment has no revision; wire VersionResolver")
	}
	// Fallback: use the revision as both refs. Tests that don't
	// care about a real .wasm can pass any non-empty value via
	// VersionResolver.
	return d.Revision, d.Revision, nil
}

// buildLiveConfigJSON marshals the dorastrategy.Config the plugin
// will read via host_get_config. Live mode has no start/end/
// resolution — only the order book, the user's params, and the
// DoraBaseURL/DoraAPIKey placeholders the strategy can use.
//
// Ponytail: the framework's GetConfig decodes the JSON into a
// Config struct; Mode is the dispatch key. We pass mode="live"
// and OrderBookID/Params; the other fields are zero.
func buildLiveConfigJSON(cfg DeployConfig) ([]byte, error) {
	c := dorastrategy.Config{
		Mode:        dorastrategy.ModeLive,
		OrderBookID: cfg.OrderBookID,
		Params:      cfg.Params,
	}
	return json.Marshal(c)
}

// hasChannel reports whether the manifest declares a given channel
// ("candle", "trade", "price") in Capabilities.Channels.
func hasChannel(m manifest.Manifest, want string) bool {
	for _, c := range m.Capabilities.Channels {
		if c == want {
			return true
		}
	}
	return false
}

// auditFor returns the auditHook for a user. If the orchestrator
// has no audit sink configured, returns the noop so host
// functions can call Record unconditionally.
func (o *Orchestrator) auditFor(userID string) auditHook {
	if o.cfg.AuditSink == nil {
		return noopAudit{}
	}
	return sinkAudit{insert: o.cfg.AuditSink, userID: userID}
}

// Convenience for callers (and tests) that need to know if a
// deployment is currently running locally. Returns false when
// the orchestrator is not yet wired.
func (o *Orchestrator) IsRunning(deploymentID string) bool {
	o.liveMu.Lock()
	defer o.liveMu.Unlock()
	_, ok := o.liveInstances[deploymentID]
	return ok
}

// Wait blocks until the given deployment's goroutine exits or
// the context is cancelled. Useful for tests.
func (o *Orchestrator) Wait(ctx context.Context, deploymentID string) error {
	o.liveMu.Lock()
	li, ok := o.liveInstances[deploymentID]
	o.liveMu.Unlock()
	if !ok {
		return nil
	}
	select {
	case <-li.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
