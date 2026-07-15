package breakout

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// PostgresHistoricalStore reads price history from the price_history
// table for backtesting. It implements the package-public
// HistoricalPriceStore interface and is the production wiring for
// cmd/strategy-server's backtest endpoint.
//
// The query is intentionally simple: SELECT in timestamp order
// between @start and @end, filtering on asset_id. The breakout signal
// reads only Price; YTM is exposed on the resulting YieldObservation
// (previously a placeholder when sourced from candles_history) so any
// downstream consumer reading observation.YTM gets a real value.
//
// The "orderBookID" parameter on the HistoricalPriceStore interface is
// treated as the asset id at the SQL layer — breakout's config
// exposes only OrderBookID, and in practice (per the integration test
// fixture) the same UUID is used for both. A future production wire-up
// may want to look up asset_id from order_book_id via the DORA REST
// API before querying price_history; out of scope for v1.
type PostgresHistoricalStore struct {
	pool *pgxpool.Pool
}

// NewPostgresHistoricalStore wires a Postgres-backed implementation of
// HistoricalPriceStore. The pool must already be connected; this
// constructor does NOT call Pool.Ping.
func NewPostgresHistoricalStore(pool *pgxpool.Pool) *PostgresHistoricalStore {
	return &PostgresHistoricalStore{pool: pool}
}

// Observations reads price_history rows for assetID between start and end
// (inclusive of both endpoints). Returns them ordered by timestamp
// ascending. Used by Strategy.Backtest to feed the Backtester.
func (s *PostgresHistoricalStore) Observations(
	ctx context.Context,
	orderBookID string,
	start, end time.Time,
) ([]types.YieldObservation, error) {
	const q = `
		SELECT timestamp, price, ytm
		FROM price_history
		WHERE asset_id = $1
		  AND timestamp >= $2
		  AND timestamp <= $3
		ORDER BY timestamp ASC
	`
	rows, err := s.pool.Query(ctx, q, orderBookID, start.UTC(), end.UTC())
	if err != nil {
		return nil, fmt.Errorf("query price_history: %w", err)
	}
	defer rows.Close()

	obID, err := uuid.Parse(orderBookID)
	if err != nil {
		return nil, fmt.Errorf("invalid asset id %q: %w", orderBookID, err)
	}

	var obs []types.YieldObservation
	for rows.Next() {
		var ts time.Time
		var price, ytm decimal.Decimal
		if err := rows.Scan(&ts, &price, &ytm); err != nil {
			return nil, fmt.Errorf("scan price_history row: %w", err)
		}
		obs = append(obs, types.YieldObservation{
			Time:           ts.UTC(),
			BondID:         obID.String(),
			YTM:            ytm,
			BenchmarkYield: ytm, // breakout doesn't use it; YTM is a reasonable proxy
			Price:          price,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate price_history: %w", err)
	}
	return obs, nil
}

// Compile-time assertion that *PostgresHistoricalStore satisfies the
// HistoricalPriceStore interface. Keeps the implementation honest if
// the interface ever changes.
var _ HistoricalPriceStore = (*PostgresHistoricalStore)(nil)
