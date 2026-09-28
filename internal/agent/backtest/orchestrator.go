// Package backtest orchestrator.go is the WASM-only backtest entry
// point. Submit validates the request, inserts the queued row via the
// Store, registers the per-job cancel context, and spawns the
// WasmStarter goroutine. RegisterJob / Cancel / CancelAll are the
// cancel-context registry shared with the HTTP cancel handler and the
// LLM cancel_backtest tool. The earlier docker pipeline
// (ProcessStarter, UDS callbacks, cidfile watcher) lived here and was
// removed when the docker pipeline was dropped on 2026-09-04.
package backtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrSingleInflight is the user-facing sentinel for the partial unique
// index on backtests.user_id WHERE status IN (queued,running). The HTTP
// handler maps it to 429.
var ErrSingleInflight = errors.New("backtest: user already has an active backtest")

// ErrInvalidRequest wraps ParseRequest errors so the handler can switch
// on a single sentinel for the 400 path.
var ErrInvalidRequest = errors.New("backtest: invalid request")

// ErrVersionNotBuilt is the sentinel for a go-wasm version whose
// compiled artifact (wasm_ref + manifest_hash) is missing. Distinct
// from ErrInvalidRequest which covers malformed window/resolution;
// a missing artifact is a version-state conflict, so the HTTP
// handler maps it to 409 (matching the pre-collapse behavior of
// handleBacktest's "version has no compiled wasm" check).
var ErrVersionNotBuilt = errors.New("backtest: version has no compiled wasm")

// ErrWasmUnavailable is the sentinel for a misconfigured orchestrator
// whose WasmStarter field is nil. The handler maps it to 503; it
// surfaces only when main.go fails to wire the WASM runtime (an
// internal misconfiguration that should not be reachable in
// production, but is guarded so a missing wire does not orphan a
// queued backtest row).
var ErrWasmUnavailable = errors.New("backtest: wasm runtime unavailable")

// Orchestrator is the WASM Submit entry point. It owns the Store
// (for queued-row inserts) and the WasmStarter (the goroutine that
// drives the in-process WASM plugin). Submit validates the request,
// inserts a queued row, registers the per-job cancel context, and
// spawns the WASM goroutine. The per-request Dora API key flows in
// via Submit (sourced from httpapi.DoraAPIKeyFromCtx at the handler);
// per-user decryption lands in the live-deployment runtime.
//
// RegisterJob / Cancel / CancelAll are the cancel-context registry
// shared with handleCancelBacktest and the LLM cancel_backtest tool;
// they survive here unchanged from the docker-era code because the
// WASM goroutine uses the same RegisterJob → cancel → deregister
// contract.
type Orchestrator struct {
	Store Store
	Wasm  *WasmStarter

	// jobs tracks the cancel function for each in-flight runner
	// goroutine, keyed by backtest ID. Cancel(id) signals the
	// runner; Submit's goroutine deregisters on exit. A nil
	// handler is tolerated so partial wiring is safe.
	mu   sync.Mutex
	jobs map[string]context.CancelFunc
}

// NewOrchestrator returns an Orchestrator wired to the Store (for
// row inserts) and the WasmStarter (for the in-process plugin loop).
// The docker-era fields (Starter, SocketDir, DoraBaseURL) were
// removed when the docker pipeline was dropped on 2026-09-04.
func NewOrchestrator(store Store, wasm *WasmStarter) *Orchestrator {
	return &Orchestrator{
		Store: store,
		Wasm:  wasm,
		jobs:  map[string]context.CancelFunc{},
	}
}

// RegisterJob stores the cancel function for id and returns the
// derived job context plus a deregister closure. Submit's goroutine
// defers the deregister so the map never retains stale entries on
// early returns. The cancel function is intentionally not returned —
// it lives only in the registry, reached via Cancel(id) / CancelAll().
func (o *Orchestrator) RegisterJob(id string) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	o.mu.Lock()
	if o.jobs == nil {
		o.jobs = map[string]context.CancelFunc{}
	}
	o.jobs[id] = cancel
	o.mu.Unlock()
	deregister := func() {
		o.mu.Lock()
		if c, ok := o.jobs[id]; ok {
			c()
			delete(o.jobs, id)
		}
		o.mu.Unlock()
	}
	return ctx, deregister
}

// Cancel cancels the in-flight runner for id, if any. Returns true
// when a job was active (and is now signalled); false when id is
// unknown or already finished. Cancel is safe to call concurrently
// with the WASM goroutine's own exit.
func (o *Orchestrator) Cancel(id string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	c, ok := o.jobs[id]
	if !ok {
		return false
	}
	c()
	delete(o.jobs, id)
	return true
}

// CancelAll cancels every registered runner. Used by main.go on
// graceful shutdown so WASM goroutines don't outlive the agent.
// context.CancelFunc is safe to call concurrently, and any new
// runner added after this returns will be cancelled by its own
// deregister when the agent exits — at that point no runner is
// supposed to be starting anyway.
func (o *Orchestrator) CancelAll() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for id, c := range o.jobs {
		c()
		delete(o.jobs, id)
	}
}

// Submit validates the request, inserts a queued Backtest row, and
// spawns the WasmStarter goroutine. The returned ID is the row's
// UUID; the caller (HTTP handler / LLM tool) returns it as 200 with
// `{backtest_id}`. The single-inflight partial unique index surfaces
// as ErrSingleInflight; validation errors wrap ErrInvalidRequest.
//
// wasmRef + manifestHash identify the WASM artifact the in-process
// plugin will execute. doraAPIKey is the per-request key resolved by
// the auth middleware; it threads into the goroutine so the plugin
// can replay historic candles against the user's own credentials.
//
// The WasmStarter.Start return is intentionally discarded: the
// caller's follow-up get_backtest_result tool observes status via
// the row, not via the goroutine's error return.
func (o *Orchestrator) Submit(
	ctx context.Context,
	userID, strategyID, versionID, wasmRef, manifestHash, doraAPIKey string,
	req *Request,
) (string, error) {
	if err := ParseRequest(req); err != nil {
		return "", errors.Join(ErrInvalidRequest, err)
	}
	// Reordered intentionally: wasmRef emptiness is the more
	// specific error a caller can fix (rebuild the version);
	// nil-Wasm is an internal misconfiguration the caller cannot
	// fix. Putting wasmRef first lets the empty-wasmRef guard fire
	// even when the orchestrator's Wasm is nil (the latter is a
	// server-side wiring bug; we still want the user-facing
	// validation error to win so test fixtures and production
	// don't have to dance around it).
	if wasmRef == "" {
		return "", ErrVersionNotBuilt
	}
	if o.Wasm == nil {
		return "", ErrWasmUnavailable
	}
	b := &Backtest{
		ID:            uuid.NewString(),
		StrategyID:    strategyID,
		VersionID:     versionID,
		UserID:        userID,
		RequestedAt:   time.Now().UTC(),
		WindowStart:   req.Start,
		WindowEnd:     req.End,
		Resolution:    req.Resolution,
		OrderBookID:   req.OrderBookID,
		Params:        req.Params,
		WarmupCandles: req.WarmupCandles,
	}
	id, err := o.Store.Create(ctx, b)
	if err != nil {
		// 23505 = unique_violation. The partial index on user_id
		// WHERE status IN (queued,running) is the only constraint
		// that fires here for a single user; the other FK checks
		// fail at the handler's pre-load.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return "", ErrSingleInflight
		}
		return "", err
	}
	b.ID = id
	// Detach from the request context so a cancelled HTTP request
	// doesn't kill the in-process backtest. RegisterJob returns a
	// child ctx + a deregister closure the goroutine must defer so
	// the cancel registry doesn't retain stale entries on early
	// returns. Same shape as the docker-era Submit so handleCancel
	// keeps working unchanged.
	jobCtx, deregister := o.RegisterJob(id)
	go func() {
		defer deregister()
		defer func() {
			if r := recover(); r != nil {
				// A panic inside WasmStarter.Start must not take down
				// the agent. Mark the row failed so callers can observe
				// the outcome, and let the goroutine exit cleanly.
				_ = o.Store.UpdateStatus(context.Background(), id, StatusFailed, nil, nil, fmt.Sprintf("backtest panic: %v", r))
			}
		}()
		if o.Wasm == nil {
			// Production path: nil-Wasm is caught before Store.Create
			// and surfaces as ErrWasmUnavailable; this branch is
			// unreachable in production. It exists so test fixtures
			// that wire &WasmStarter{} (nil fields) don't trip the
			// guard above and can assert on Submit's 200-response path
			// without standing up a full WASM runtime.
			return
		}
		_ = o.Wasm.Start(jobCtx, b, wasmRef, manifestHash, doraAPIKey)
	}()
	return id, nil
}
