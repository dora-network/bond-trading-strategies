package backtest

// Restored L5 tests (deleted by L4 while the wasmruntime seam was a
// stub): the real registry now lands in internal/agent/wasmruntime,
// so cancel-unwind and serial-backtest regressions are live again.
// Copied from dora-agent internal/backtest/wasm_starter_test.go.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm/prompts"
	registry "github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
	wasmstore "github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"

	agentstore "github.com/dora-network/bond-trading-strategies/internal/agent/store"
)

// blockingCandleFetcher is a history.Fetcher that distinguishes the
// preamble fetch from the replay fetch. WasmStarter.Start calls
// FetchCandles twice: first for the preamble window
// [warmupStart, b.WindowStart) inside fetchHistoryWindow, then
// again for the replay window [b.WindowStart, b.WindowEnd) inside
// newReplayStream. We must not block on the preamble call: with
// WarmupCandles=0 the preamble window is empty, so returning
// immediately is correct AND required for the test to reach
// the cancel guard (which only fires after _start returns, not
// after fetchHistoryWindow errors out). On the replay window
// (start < end), the fetcher hangs until ctx is cancelled, so
// mergeStream.next stays parked and the plugin's
// host_next_event blocks -- exactly where cancel needs to
// unwind to exercise the guard.
type blockingCandleFetcher struct {
	replayCalled chan struct{}
}

func (b *blockingCandleFetcher) FetchCandles(ctx context.Context, _ string, start, end time.Time,
	_, _ string, _ int,
) ([]agentstore.Candle, string, error) {
	if !start.Before(end) {
		// Empty window (preamble when WarmupCandles=0). Return
		// the empty result so fetchAll's cursor walk terminates
		// and fetchHistoryWindow completes.
		return nil, "", nil
	}
	// Replay window: signal and block on ctx so cancel can
	// unwrap the plugin's host_next_event call.
	select {
	case b.replayCalled <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, "", ctx.Err()
}

func (b *blockingCandleFetcher) FetchTrades(context.Context, string, time.Time, time.Time, string, int) ([]agentstore.Trade, string, error) {
	return nil, "", nil
}

func (b *blockingCandleFetcher) FetchPrices(context.Context, string, time.Time, time.Time, string, int) ([]agentstore.Price, string, error) {
	return nil, "", nil
}

// cancelStoreBt is a Store stub that records status transitions and
// counts fill/summary calls. Used by TestWasmStarter_CancelUnwindsWASMGoroutine
// to prove the cancel path skips persistSuccess entirely.
type cancelStoreBt struct {
	Store
	mu           sync.Mutex
	statuses     []Status
	insertCalls  int
	summaryCalls int
}

func (s *cancelStoreBt) UpdateStatus(_ context.Context, _ string, status Status,
	_ *time.Time, _ *time.Time, _ string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses = append(s.statuses, status)
	return nil
}

func (s *cancelStoreBt) InsertFills(_ context.Context, _ string, _ []Fill) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insertCalls++
	return nil
}

func (s *cancelStoreBt) SetSummary(_ context.Context, _ string, _ *Summary, _ int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.summaryCalls++
	return nil
}

func (s *cancelStoreBt) CancelIfRunning(context.Context, string) (bool, error) {
	return true, nil
}

// TestWasmStarter_CancelUnwindsWASMGoroutine is the load-bearing test
// for the cancel-guard fix: a real .wasm (the local example strategy
// in this package's testdata/, which uses host.NextEvent via
// runEvents) is launched in a goroutine, the per-job ctx is cancelled
// mid-Start, and Start returns ctx.Err() within a few seconds. The
// cancel path:
// cancel_backtest -> Orchestrator.Cancel -> RegisterJob-issued ctx.Done
// -> mergeStream.next select unblocks with ctx.Err -> hostNextEvent
// returns -1 -> plugin runEvents exits with the err -> WasmStarter.Start
// reaches the guard at wasm_starter.go:154 and returns ctx.Err without
// calling persistSuccess.
//
// Skipped when tinygo is not on PATH (CI has it; local devs may not).
// Builds testdata/example/strategy.wasm in a temp dir.
func TestWasmStarter_CancelUnwindsWASMGoroutine(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skipf("tinygo not on PATH: %v", err)
	}
	buildDir := t.TempDir()
	wasmOut := filepath.Join(buildDir, "strategy.wasm")
	// Local example under this package's testdata/ so the test
	// does not depend on the dora-strategy-wasm module cache.
	exampleSrc, err := filepath.Abs(filepath.Join("testdata", "example", "strategy.go"))
	if err != nil {
		t.Fatalf("abs example path: %v", err)
	}
	buildCmd := exec.CommandContext(t.Context(), "tinygo", "build", "-target=wasi", "-buildmode=c-shared",
		"-o", wasmOut, exampleSrc)
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("tinygo build example: %v\n%s", err, out)
	}
	wasmBytes, err := os.ReadFile(wasmOut)
	if err != nil {
		t.Fatalf("read wasm: %v", err)
	}
	// Manifest: declare channels=[candle] so the v3 mergeStream path
	// is taken (history != nil forces the streaming branch in
	// WasmStarter.Start). The example strategy ignores the data; it
	// only loops host_next_event until done.
	manifestBytes := []byte(fmt.Sprintf(`{
		"schema_version": 1,
		"module_name": "cancel-test",
		"language": "go",
		"framework_version": "%s",
		"capabilities": {
			"order_books": ["OB-1"],
			"resolutions": ["1m"],
			"channels": ["candle"],
			"host_functions": ["host_log", "host_next_event", "host_submit_order", "host_record_fill", "host_backtest_error", "host_get_config"]
		},
		"params_schema": {}
	}`, prompts.WasmFrameworkVersion))

	wsRoot := t.TempDir()
	ws, err := wasmstore.New(wsRoot)
	if err != nil {
		t.Fatalf("wasmstore.New: %v", err)
	}
	wh, mh, err := ws.Put(wasmBytes, manifestBytes)
	if err != nil {
		t.Fatalf("wasmstore.Put: %v", err)
	}
	reg, err := registry.New(t.Context(), registry.Config{MemoryLimitBytes: registry.DefaultMemoryLimitBytes})
	if err != nil {
		t.Fatalf("registry.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })

	// Blocking fetcher: the mergeStream prefetcher hangs on the
	// first page, leaving the plugin blocked on host_next_event.
	fetcher := &blockingCandleFetcher{replayCalled: make(chan struct{}, 1)}

	btStore := &cancelStoreBt{}

	w := &WasmStarter{
		reg:     reg,
		store:   ws,
		btStore: btStore,
		history: fetcher,
		logger:  slog.New(slog.DiscardHandler),
		apiLookup: func(context.Context, *Backtest, string) string {
			return "" // skip prices in fetchHistoryWindow
		},
	}

	// Register the per-job ctx with the orchestrator (this is the
	// seam the cancel_backtest / handleCancelBacktest handlers use).
	orch := &Orchestrator{}
	jobCtx, deregister := orch.RegisterJob("bt-cancel-test")
	defer deregister()

	// Start the backtest in a goroutine. The plugin's runEvents
	// blocks on host_next_event waiting for a candle that never
	// arrives (the fetcher blocks until ctx is cancelled).
	done := make(chan error, 1)
	go func() {
		done <- w.Start(jobCtx, &Backtest{
			ID:          "bt-cancel-test",
			UserID:      "test-user",
			WindowStart: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			WindowEnd:   time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
			Resolution:  "1m",
			OrderBookID: "OB-1",
		}, wh, mh, "test-dora-key")
	}()

	// Wait until the fetcher is in flight, then cancel.
	select {
	case <-fetcher.replayCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("replay FetchCandles never called; mergeStream prefetcher never reached the blocking call")
	}
	if !orch.Cancel("bt-cancel-test") {
		t.Fatal("Cancel returned false; RegisterJob must not have wired the cancel func")
	}

	// Start must return within a few seconds of cancel, with
	// ctx.Err() (or a wrapper that contains it).
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Start returned nil; expected ctx.Err() because cancel raced")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Start error: got %v, want context.Canceled (or wrap)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return within 5s of Cancel -- guard or cancel wiring is broken")
	}

	// Critical: persistSuccess must NOT have run. If the guard is
	// missing or wrong, InsertFills + SetSummary would land here
	// and silently overwrite the cancelled status as succeeded.
	btStore.mu.Lock()
	defer btStore.mu.Unlock()
	if btStore.insertCalls != 0 {
		t.Errorf("InsertFills called %d times; cancel path must skip persistSuccess", btStore.insertCalls)
	}
	if btStore.summaryCalls != 0 {
		t.Errorf("SetSummary called %d times; cancel path must skip persistSuccess", btStore.summaryCalls)
	}
	// Distinguishes the guard-fired path from the preamble-bail
	// path: the preamble failure path calls w.fail() which sets
	// status=Failed; the guard at wasm_starter.go:159 doesn't
	// touch the row (CancelIfRunning already wrote StatusCancelled
	// before the ctx was cancelled). If we see StatusFailed in
	// the recorded transitions, the test didn't reach the guard
	// at all -- it bailed out of fetchHistoryWindow before
	// newReplayStream even started.
	for _, st := range btStore.statuses {
		if st == StatusFailed {
			t.Errorf("status=Failed recorded (transition %v): cancel unwound at the preamble fetch, not the guard. "+
				"Either the fixture's preamble FetchCandles blocked (it must return empty for warmup=0), "+
				"or the fetcher isn't distinguishing start vs end", btStore.statuses)
		}
	}
}

// emptyCandleFetcher returns empty results for every fetch. Drives
// WasmStarter through to a clean exit (plugin's runEvents calls
// nextEventFn once, gets done=true, returns nil). Lets us run the
// full Start pipeline serially in a test without hanging.
type emptyCandleFetcher struct{}

func (emptyCandleFetcher) FetchCandles(context.Context, string, time.Time, time.Time, string, string, int) ([]agentstore.Candle, string, error) {
	return nil, "", nil
}

func (emptyCandleFetcher) FetchTrades(context.Context, string, time.Time, time.Time, string, int) ([]agentstore.Trade, string, error) {
	return nil, "", nil
}

func (emptyCandleFetcher) FetchPrices(context.Context, string, time.Time, time.Time, string, int) ([]agentstore.Price, string, error) {
	return nil, "", nil
}

// TestWasmStarter_SerialBacktestsSameRegistry is the regression
// test for the wazero "module[env] has already been instantiated"
// collision (TODO.md slice E follow-ups, 2026-09-03). Two
// backtests on the same Registry used to fail because wazero's
// Store rejected registering the host module named "env" twice.
// The fix: per-call wazero Runtimes with a shared
// CompilationCache, so each Load gets a fresh private "env"
// registration and compiled modules are reused across runtimes.
func TestWasmStarter_SerialBacktestsSameRegistry(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skipf("tinygo not on PATH: %v", err)
	}
	buildDir := t.TempDir()
	wasmOut := filepath.Join(buildDir, "strategy.wasm")
	exampleSrc, err := filepath.Abs(filepath.Join("testdata", "example", "strategy.go"))
	if err != nil {
		t.Fatalf("abs example path: %v", err)
	}
	buildCmd := exec.CommandContext(t.Context(), "tinygo", "build", "-target=wasi", "-buildmode=c-shared",
		"-o", wasmOut, exampleSrc)
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("tinygo build example: %v\n%s", err, out)
	}
	wasmBytes, err := os.ReadFile(wasmOut)
	if err != nil {
		t.Fatalf("read wasm: %v", err)
	}
	manifestBytes := []byte(fmt.Sprintf(`{
		"schema_version": 1,
		"module_name": "serial-test",
		"language": "go",
		"framework_version": "%s",
		"capabilities": {
			"order_books": ["OB-1"],
			"resolutions": ["1m"],
			"channels": ["candle"],
			"host_functions": ["host_log", "host_next_event", "host_submit_order", "host_record_fill", "host_backtest_error", "host_get_config"]
		},
		"params_schema": {}
	}`, prompts.WasmFrameworkVersion))

	wsRoot := t.TempDir()
	ws, err := wasmstore.New(wsRoot)
	if err != nil {
		t.Fatalf("wasmstore.New: %v", err)
	}
	wh, mh, err := ws.Put(wasmBytes, manifestBytes)
	if err != nil {
		t.Fatalf("wasmstore.Put: %v", err)
	}
	reg, err := registry.New(t.Context(), registry.Config{MemoryLimitBytes: registry.DefaultMemoryLimitBytes})
	if err != nil {
		t.Fatalf("registry.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })

	w := &WasmStarter{
		reg:     reg,
		store:   ws,
		btStore: &cancelStoreBt{},
		history: emptyCandleFetcher{},
		logger:  slog.New(slog.DiscardHandler),
		apiLookup: func(context.Context, *Backtest, string) string {
			return "" // skip prices in fetchHistoryWindow
		},
	}

	// Two serial Start calls on the SAME Registry. Pre-fix, the
	// second call would fail at InstantiateModule because
	// wazero's Store rejected registering "env" a second time
	// ("module[env] has already been instantiated"). The fix
	// gives each Load a fresh wazero.Runtime, so each has its own
	// private "env" registration and the collision cannot occur.
	for i, btID := range []string{"bt-serial-1", "bt-serial-2"} {
		bt := &Backtest{
			ID:          btID,
			UserID:      "test-user",
			WindowStart: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			WindowEnd:   time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
			Resolution:  "1m",
			OrderBookID: "OB-1",
		}
		if err := w.Start(t.Context(), bt, wh, mh, "test-dora-key"); err != nil {
			t.Fatalf("Start #%d (%s): %v", i+1, btID, err)
		}
	}
}
