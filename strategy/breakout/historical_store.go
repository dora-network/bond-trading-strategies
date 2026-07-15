package breakout

import (
	"context"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// HistoricalPriceStore is the backtest's read-only data source. Production
// wiring is strategy/breakout/postgres_store.go's PostgresHistoricalStore;
// tests use the counterfeiter fake in strategy/breakout/breakoutfakes/.
//
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o breakoutfakes/fake_historical_price_store.go . HistoricalPriceStore
type HistoricalPriceStore interface {
	Observations(ctx context.Context, assetID string, start, end time.Time) ([]types.YieldObservation, error)
}
