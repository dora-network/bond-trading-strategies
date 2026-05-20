package copytrading

import (
	"context"
	"errors"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/config"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
)

type Config struct {
	config.Config
	FollowedTrader uuid.UUID
	MinOrderSize   int
	MaxOrderSize   int
	AllowedBonds   []uuid.UUID
}

type Strategy struct {
	cfg Config
}

func New(cfg Config) *Strategy {
	return &Strategy{cfg: cfg}
}

func (s *Strategy) Backtest(ctx context.Context, start, end time.Time) (backtestResult types.BacktestResult, err error) {
	// TODO: implement the backtester
	backtester := NewBacktester(s)
	results, err := backtester.Run(ctx, nil)
	return results, err
}

func (s *Strategy) Run(ctx context.Context, msgCh <-chan strategy.Message, runID uuid.UUID) error {
	// TODO: implement the strategy runner
	return errors.New("not implemented")
}
