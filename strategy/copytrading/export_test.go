package copytrading

import (
	"context"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/google/uuid"
)

// RunWithTrades runs the strategy's internal run loop with a caller-supplied
// trade channel, bypassing the TradeStream subscription. For unit tests only.
func RunWithTrades(s *Strategy, ctx context.Context, msgs <-chan strategy.Message, tradeCh <-chan streams.TradeEvent) error {
	s.runID = uuid.New()
	return s.runLoop(ctx, msgs, tradeCh)
}

// SetMarketAPI injects a market API client for testing.
func SetMarketAPI(s *Strategy, client marketAPIClient) {
	s.marketAPI = client
}
