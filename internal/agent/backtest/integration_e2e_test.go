package backtest_test

// End-to-end runtime smoke for the agent backtest pipeline: boots the
// full wiring.Wire runtime against a real Postgres (host migrations
// 001-014 + agent 015), compiles the local example strategy with
// tinygo, seeds a WASM strategy version, submits a backtest through
// the real Orchestrator, and waits for the row to reach a terminal
// status with a persisted summary.
//
// Gated: AGENT_E2E=1 (keeps pre-commit/CI hermetic) AND DATABASE_URL
// (agenttest.StartPostgres skips). tinygo must be on PATH.
//
// Run locally:
//
//	AGENT_E2E=1 DATABASE_URL=postgres://postgres:postgres@localhost:5432/bond_trading?sslmode=disable \
//	  go test -count=1 -v -run TestIntegration_BacktestCompletesAgainstRealStore \
//	  ./internal/agent/backtest/... -timeout 120s

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/config"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm/prompts"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
	"github.com/dora-network/bond-trading-strategies/internal/agent/users"
	wasmstore "github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wiring"
)

func TestIntegration_BacktestCompletesAgainstRealStore(t *testing.T) {
	if os.Getenv("AGENT_E2E") != "1" {
		t.Skip("set AGENT_E2E=1 to run; gated to keep CI hermetic")
	}
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skipf("tinygo not on PATH: %v", err)
	}
	pool := agenttest.StartPostgresWithHostMigrations(t)
	ctx := t.Context()

	// The artifact root must be set before Wire (wasmruntime.NewRuntime
	// reads it); the test seeds the artifact into the same root.
	artifactRoot := t.TempDir()
	t.Setenv("AGENT_WASM_ARTIFACT_ROOT", artifactRoot)

	// Resolve repo-root-relative assets from this package's dir.
	modelCaps, err := filepath.Abs(filepath.Join("..", "..", "..", "configs", "model_caps.json"))
	if err != nil {
		t.Fatalf("abs model caps: %v", err)
	}
	exampleSrc, err := filepath.Abs(filepath.Join("testdata", "example", "strategy.go"))
	if err != nil {
		t.Fatalf("abs example path: %v", err)
	}

	// Wire the full runtime: WASM runtime + registry, history store,
	// backtest store + orchestrator, live orchestrator, janitors.
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	cfg := config.Config{
		DoraBaseURL:                 "http://127.0.0.1:1", // unreachable on purpose: price lookup degrades to empty
		ModelCapsPath:               modelCaps,
		CapturePendingSweepInterval: 5 * time.Minute,
		CapturePendingRetention:     24 * time.Hour,
		RateLimitPerMin:             1000,
		LLMTimeout:                  30 * time.Second,
	}
	encryptionKey := make([]byte, 32)
	rt, err := wiring.Wire(ctx, pool, encryptionKey, cfg, log)
	if err != nil {
		t.Fatalf("wiring.Wire: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close(t.Context()) })

	// Seed a user (backtests.user_id FKs agent.users).
	userID := uuid.NewString()
	usersStore, err := users.New(ctx, pool)
	if err != nil {
		t.Fatalf("users.New: %v", err)
	}
	if err := usersStore.Ensure(ctx, userID, "e2e-tenant", []string{"trader"}); err != nil {
		t.Fatalf("users.Ensure: %v", err)
	}

	// Seed the replay window: 60 one-minute candles for a random order
	// book (uuid column) so the history store's pass-through 1m query
	// has real rows to stream through the mergeStream.
	orderBookID := uuid.NewString()
	windowStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := windowStart.Add(time.Hour)
	if _, err := pool.Exec(
		ctx, `
		insert into candles_history (order_book_id, start_timestamp, open, high, low, close, volume)
		select $1, ts, 100, 101, 99, 100, 10
		from generate_series($2::timestamptz, $3::timestamptz - interval '1 minute', interval '1 minute') ts
		on conflict do nothing`,
		orderBookID, windowStart, windowEnd,
	); err != nil {
		t.Fatalf("seed candles_history: %v", err)
	}

	// Compile the example strategy and put the artifact + manifest into
	// the runtime's artifact store.
	wasmOut := filepath.Join(t.TempDir(), "strategy.wasm")
	buildCmd := exec.CommandContext(ctx, "tinygo", "build", "-target=wasi", "-buildmode=c-shared",
		"-o", wasmOut, exampleSrc)
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("tinygo build example: %v\n%s", err, out)
	}
	wasmBytes, err := os.ReadFile(wasmOut)
	if err != nil {
		t.Fatalf("read wasm: %v", err)
	}
	// channels=[candle] forces the v3 mergeStream (streaming) path.
	manifestBytes := []byte(fmt.Sprintf(`{
		"schema_version": 1,
		"module_name": "e2e-example",
		"language": "go",
		"framework_version": "%s",
		"capabilities": {
			"order_books": ["%s"],
			"resolutions": ["1m"],
			"channels": ["candle"],
			"host_functions": ["host_log", "host_next_event", "host_submit_order", "host_record_fill", "host_backtest_error", "host_get_config"]
		},
		"params_schema": {}
	}`, prompts.WasmFrameworkVersion, orderBookID))

	ws, err := wasmstore.New(artifactRoot)
	if err != nil {
		t.Fatalf("wasmstore.New: %v", err)
	}
	wasmRef, manifestHash, err := ws.Put(wasmBytes, manifestBytes)
	if err != nil {
		t.Fatalf("wasmstore.Put: %v", err)
	}

	// Seed the strategy + version (target=go-wasm) via the L4 store.
	src, err := os.ReadFile(exampleSrc)
	if err != nil {
		t.Fatalf("read example source: %v", err)
	}
	versions := strategies.NewPgStore(pool)
	ver, err := versions.CaptureWASM(
		ctx, uuid.NewString(), userID,
		"test-provider", "test-model",
		strategies.Meta{
			ModuleName: "e2e-example", Summary: "e2e noop strategy",
			Validation: json.RawMessage(`{}`),
		},
		map[string]string{"strategy.go": string(src)}, wasmRef, manifestHash,
	)
	if err != nil {
		t.Fatalf("CaptureWASM: %v", err)
	}

	// Submit through the real orchestrator (Submit spawns the
	// WasmStarter goroutine and returns the row id).
	id, err := rt.BtOrch.Submit(
		ctx, userID, ver.StrategyID, string(ver.Revision),
		wasmRef, manifestHash, "e2e-dora-key",
		&backtest.Request{
			Start:       windowStart,
			End:         windowEnd,
			Resolution:  "1m",
			OrderBookID: orderBookID,
		},
	)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Poll the row until terminal status.
	btStore := backtest.NewPgStore(pool)
	deadline := time.Now().Add(90 * time.Second)
	for {
		b, err := btStore.Get(ctx, id)
		if err != nil {
			t.Fatalf("backtest Get(%s): %v", id, err)
		}
		switch b.Status {
		case backtest.StatusSucceeded, backtest.StatusFailed, backtest.StatusCancelled:
			if b.Status != backtest.StatusSucceeded {
				t.Fatalf("backtest %s ended %s: %s", id, b.Status, b.ErrorMessage)
			}
			if b.FinishedAt == nil {
				t.Fatalf("backtest %s succeeded but finished_at is null", id)
			}
			if b.Summary == nil {
				t.Fatalf("backtest %s succeeded but summary is nil", id)
			}
			t.Logf("backtest %s succeeded: fills=%d trade_count=%d end_equity=%.2f",
				id, b.FillCount, b.Summary.TradeCount, b.Summary.EndEquity)
			return
		case backtest.StatusQueued, backtest.StatusRunning:
			// Still in flight; fall through to the deadline + sleep below.
		}
		if time.Now().After(deadline) {
			t.Fatalf("backtest %s did not reach a terminal status within 90s (last: %s)", id, b.Status)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}
