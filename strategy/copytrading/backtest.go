package copytrading

import (
	"context"
	"errors"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

type Backtester struct {
	strategy *Strategy
}

func NewBacktester(s *Strategy) *Backtester {
	return &Backtester{strategy: s}
}

func (b *Backtester) Run(ctx context.Context, trades []types.TradeRecord) (types.BacktestResult, error) {
	// TODO: implement the backtester
	return types.BacktestResult{}, errors.New("not implemented")
}
