package breakout

import (
	"context"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// historicalPriceStore is the backtest's read-only data source.
//
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o breakoutfakes/fake_historical_price_store.go . historicalPriceStore
type historicalPriceStore interface {
	Observations(ctx context.Context, orderBookID string, start, end time.Time) ([]types.YieldObservation, error)
}
