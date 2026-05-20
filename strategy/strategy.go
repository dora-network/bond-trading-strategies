package strategy

import (
	"context"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
)

// Strategy is the interface that must be implemented by a trading strategy.
//
//counterfeiter:generate . Strategy
type Strategy interface {
	// Backtest runs the strategy against historical data to evaluate its performance with the given configuration.
	Backtest(ctx context.Context, start, end time.Time) (backtestResult types.BacktestResult, err error)
	// Run starts the strategy in the background.
	Run(ctx context.Context, msgCh <-chan Message, runID uuid.UUID) error
}
