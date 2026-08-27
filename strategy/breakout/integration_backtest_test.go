//go:build integration

package breakout_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy/breakout"
)

// Integration test for DORA-5874 — runs the breakout Backtester against
// a real Postgres with synthetic ticks inserted into price_history, and
// asserts the run produces a non-zero trade count, non-zero metrics, and
// is byte-equal across two runs (reproducibility — no RNG used).
//
// Gated by INTEGRATION=1 (or by building with `-tags integration`).
// Skips fast unit-test runs.
func TestIntegration_BacktestAgainstPriceHistory(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("INTEGRATION=1 not set; skipping")
	}

	dsn := os.Getenv("DATABASE_URL")
	require.NotEmpty(t, dsn, "DATABASE_URL is required for integration tests")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	assetID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	const q = `INSERT INTO price_history
		(asset_id, price, ytm, timestamp)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (asset_id, timestamp) DO NOTHING`

	// Synthetic series:
	// - 30 flat ticks at price=100, ytm=0.05 (fills long window, arms
	//   compression; both ShortVol and LongVol → 0 → ratio → 0 → arms)
	// - 1 jump to 110 (BUY on the breakout)
	// - 10 flat ticks at 110 (holds the position; force-closes at end
	//   of history, recording a strategy_exit ClosedTrade)
	// Total 41 ticks.
	insert := func(t *testing.T, ts time.Time, price int64) {
		t.Helper()
		_, err := pool.Exec(ctx, q,
			assetID,
			decimal.MustNew(price, 0),
			decimal.MustNew(5, 2), // ytm = 0.05
			ts.UTC(),
		)
		require.NoError(t, err)
	}
	start := time.Now().UTC().Add(-2 * time.Hour)
	for i := 0; i < 30; i++ {
		insert(t, start.Add(time.Duration(i)*time.Minute), 100)
	}
	insert(t, start.Add(30*time.Minute), 110)
	for i := 0; i < 10; i++ {
		insert(t, start.Add(time.Duration(31+i)*time.Minute), 110)
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM price_history WHERE asset_id = $1`,
			assetID)
	})

	// Build a strategy and drive Backtester.Run directly so we get the
	// concrete breakout.BacktestResult (with .ClosedTrades / .TotalPnL /
	// .SharpeRatio), not the strategycore.BacktestResult interface.
	cfg := breakout.DefaultConfig()
	cfg.OrderBookID = assetID
	cfg.ShortVolWindow = 5
	cfg.LongVolWindow = 30
	cfg.ATRWindow = 14
	cfg.ConfirmationBars = 1
	cfg.InitialBalance = decimal.MustNew(1000, 0)
	cfg.Leverage = decimal.One

	store := breakout.NewPostgresHistoricalStore(pool)
	obs, err := store.Observations(ctx, assetID.String(),
		start, start.Add(41*time.Minute))
	require.NoError(t, err)
	require.NotEmpty(t, obs, "fixture ticks must be present in price_history")

	run := func() breakout.BacktestResult {
		s := breakout.New(cfg, nil)
		bt := breakout.NewBacktester(s, nil)
		r, err := bt.Run(ctx, obs)
		require.NoError(t, err)
		return r
	}

	r1 := run()
	require.NotEmpty(t, r1.ClosedTrades,
		"expected at least one closed trade on a 30-flat + 1-jump + 10-flat series")
	require.False(t, r1.TotalPnL.IsZero(),
		"TotalPnL must be non-zero on a successful breakout")
	require.False(t, r1.SharpeRatio.IsZero(),
		"SharpeRatio must be non-zero on a successful breakout (sanity)")

	// Reproducibility: run a second time and assert byte-equal output.
	r2 := run()
	b1, _ := json.Marshal(r1)
	b2, _ := json.Marshal(r2)
	require.Equal(t, string(b1), string(b2),
		"Backtester output must be byte-equal across runs (no RNG, deterministic)")
}
