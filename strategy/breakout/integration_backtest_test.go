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

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/strategy/breakout"
)

// Integration test for the bar-driven breakout backtester — seeds
// synthetic 1m bars into candles_history, runs Backtest against them,
// and asserts the run produces a non-zero trade count, non-zero
// metrics, and is byte-equal across two runs (reproducibility — no RNG).
//
// Gated by INTEGRATION=1 (or by building with `-tags integration`).
// Skips fast unit-test runs.
func TestIntegration_BacktestAgainstCandleHistory(t *testing.T) {
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

	obID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	// Truncate to the minute: Postgres timestamps keep microsecond
	// precision, so sub-µs nanos in `start` would make the coverage
	// boundary check fail by a fraction of a nanosecond.
	start := time.Now().UTC().Truncate(time.Minute).Add(-2 * time.Hour)
	const q = `INSERT INTO candles_history
		(order_book_id, start_timestamp, open, high, low, close, volume,
		 open_ytm, high_ytm, low_ytm, close_ytm)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (order_book_id, start_timestamp) DO NOTHING`

	// Synthetic 1m series (O=H=L=C per bar):
	// - 30 flat bars at price=100 (fills the long window, arms
	//   compression; ShortVol=LongVol=0 → ratio=0 → armed)
	// - 1 jump to 110 (BUY on the breakout, ConfirmationBars=1)
	// - 10 flat bars at 110 (holds the position; force-closes at end
	//   of history, recording a strategy_exit ClosedTrade)
	// Total 41 bars.
	insert := func(t *testing.T, i int, price int64) {
		t.Helper()
		p := decimal.MustNew(price, 0)
		ts := start.Add(time.Duration(i) * time.Minute)
		_, err := pool.Exec(ctx, q,
			obID, ts.UTC(),
			p, p, p, p, decimal.Zero,
			decimal.MustNew(5, 2), decimal.MustNew(5, 2), decimal.MustNew(5, 2), decimal.MustNew(5, 2),
		)
		require.NoError(t, err)
	}
	// Warmup: LongVolWindow+1 = 31 bars before start (the coverage
	// check requires them; they also fill the long window).
	for i := -31; i < 0; i++ {
		insert(t, i, 100)
	}
	for i := range 30 {
		insert(t, i, 100)
	}
	insert(t, 30, 110)
	for i := range 10 {
		// drift to 119 so the force-close at end-of-history records
		// a non-zero exit PnL
		insert(t, 31+i, 110+int64(i))
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM candles_history WHERE order_book_id = $1`,
			obID)
	})

	cfg := breakout.DefaultConfig()
	cfg.OrderBookID = obID
	cfg.Resolution = "1m"
	cfg.ShortVolWindow = 5
	cfg.LongVolWindow = 30
	cfg.ATRWindow = 14
	cfg.ConfirmationBars = 1
	cfg.InitialBalance = decimal.MustNew(1000, 0)
	cfg.Leverage = decimal.One

	store := candles.NewPGStore(pool)

	run := func() breakout.BacktestResult {
		s := breakout.New(cfg, nil, breakout.WithCandleHistoryStore(store))
		r, err := s.Backtest(ctx, start, start.Add(41*time.Minute))
		require.NoError(t, err)
		return r.(breakout.BacktestResult)
	}

	r1 := run()
	require.NotEmpty(t, r1.ClosedTrades,
		"expected at least one closed trade on the flat + jump + drift series")
	require.False(t, r1.TotalPnL.IsZero(),
		"TotalPnL must be non-zero on a successful breakout")
	// SharpeRatio is annualised from daily-PnL dispersion; this
	// single-day, single-trade fixture is mathematically Sharpe-0.

	// Reproducibility: run a second time and assert byte-equal output.
	r2 := run()
	b1, _ := json.Marshal(r1)
	b2, _ := json.Marshal(r2)
	require.Equal(t, string(b1), string(b2),
		"Backtester output must be byte-equal across runs (no RNG, deterministic)")
}
