package copytrading

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/config"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/govalues/decimal"
	"github.com/google/uuid"
)

// Config holds the copy trading strategy configuration.
type Config struct {
	config.Config
	FollowedTrader        uuid.UUID
	PercentageOfAvailable decimal.Decimal
	Leverage              decimal.Decimal
	MinOrderSize          int
	MaxOrderSize          int
	DisallowedBonds       []uuid.UUID
}

// Strategy implements the copy trading strategy.
type Strategy struct {
	cfg           Config
	marketAPI     marketAPIClient
	tradesClient  tradesClient
	log           *slog.Logger
	tradeStream   *streams.TradeStream
	subscriberID  uuid.UUID
	runID         uuid.UUID
	disallowedSet map[uuid.UUID]struct{}
}

// New creates a new Strategy with the given configuration and functional options.
func New(cfg Config, opts ...func(*Strategy)) *Strategy {
	s := &Strategy{
		cfg: cfg,
		log: slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.disallowedSet = make(map[uuid.UUID]struct{})
	for _, id := range cfg.DisallowedBonds {
		s.disallowedSet[id] = struct{}{}
	}
	return s
}

// WithMarketAPIClient sets the market API client for the strategy.
func WithMarketAPIClient(client marketAPIClient) func(*Strategy) {
	return func(s *Strategy) {
		s.marketAPI = client
	}
}

// WithTradesClient sets the trades client for backtesting.
func WithTradesClient(client tradesClient) func(*Strategy) {
	return func(s *Strategy) {
		s.tradesClient = client
	}
}

// WithLogger sets the logger for the strategy.
func WithLogger(log *slog.Logger) func(*Strategy) {
	return func(s *Strategy) {
		s.log = log
	}
}

// Backtest runs a backtest simulation for the given time range.
func (s *Strategy) Backtest(ctx context.Context, start, end time.Time) (backtestResult types.BacktestResult, err error) {
	backtester := NewBacktester(s)
	return backtester.Run(ctx, start, end)
}

// Run starts the live copy trading strategy.
func (s *Strategy) Run(ctx context.Context, msgCh <-chan strategy.Message, runID uuid.UUID) error {
	s.runID = runID
	return s.run(ctx, msgCh)
}

func (s *Strategy) run(ctx context.Context, msgCh <-chan strategy.Message) error {
	subscriberID, tradeCh := s.tradeStream.Subscribe(s.cfg.FollowedTrader)
	defer s.tradeStream.Unsubscribe(subscriberID)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgCh:
			if !ok {
				return nil
			}
			switch msg {
			case strategy.Stop:
				return nil
			case strategy.Pause:
				s.log.Info("copy trading paused", "run_id", s.runID)
			case strategy.Resume:
				s.log.Info("copy trading resumed", "run_id", s.runID)
			}
		case trade, ok := <-tradeCh:
			if !ok {
				return nil
			}
			if err := s.handleTrade(ctx, trade); err != nil {
				s.log.Error("failed to handle trade", "err", err, "trade", trade.ExecutionID)
			}
		}
	}
}

func (s *Strategy) handleTrade(ctx context.Context, trade streams.TradeEvent) error {
	// Check disallowed bonds
	if _, disallowed := s.disallowedSet[trade.AssetID]; disallowed {
		s.log.Info("skipping trade for disallowed bond", "asset", trade.AssetID)
		return nil
	}

	// Query DORA for current position
	portfolio, err := s.marketAPI.GetPortfolioV2(ctx)
	if err != nil {
		return fmt.Errorf("get portfolio: %w", err)
	}

	// Calculate available balance
	availableBalance := s.calculateAvailableBalance(portfolio)

	// Calculate order size: availableBalance * percentageOfAvailable * leverage
	orderSize := calculateOrderSize(availableBalance, s.cfg.PercentageOfAvailable, s.cfg.Leverage, s.cfg.MinOrderSize, s.cfg.MaxOrderSize)

	if orderSize.IsZero() || orderSize.IsNeg() {
		s.log.Info("skipping trade: calculated order size is zero or negative", "order_size", orderSize)
		return nil
	}

	// Determine side
	var side doraclient.Side
	if trade.Side == "buy" {
		side = doraclient.Side("buy")
	} else {
		side = doraclient.Side("sell")
	}

	// Place market order
	err = s.marketAPI.CreateMarketOrder(
		ctx,
		trade.OrderBookID.String(),
		side,
		orderSize,
		decimal.One,
		true,
	)
	if err != nil {
		return fmt.Errorf("create market order: %w", err)
	}

	s.log.Info("placed copy trade",
		"order_book", trade.OrderBookID,
		"asset", trade.AssetID,
		"side", trade.Side,
		"quantity", orderSize,
		"followed_trader", trade.TraderID)

	return nil
}

// calculateAvailableBalance sums the available balance across all accounts and assets.
func (s *Strategy) calculateAvailableBalance(portfolio *doraclient.AccountPortfolioV2) decimal.Decimal {
	if portfolio == nil {
		return decimal.Zero
	}

	accounts := portfolio.GetAccounts()
	if len(accounts) == 0 {
		return decimal.Zero
	}

	total := decimal.Zero
	for _, accountMap := range accounts {
		for _, asset := range accountMap {
			available, err := decimal.Parse(asset.Available)
			if err == nil {
				total, _ = total.Add(available)
			}
		}
	}

	return total
}

// calculateOrderSize computes the order size from available balance, percentage,
// leverage, and min/max clamps.
func calculateOrderSize(available, percentage, leverage decimal.Decimal, minOrderSize, maxOrderSize int) decimal.Decimal {
	orderSize, _ := available.Mul(percentage)
	orderSize, _ = orderSize.Mul(leverage)

	if minOrderSize > 0 {
		minSize := decimal.MustNew(int64(minOrderSize), 0)
		if orderSize.Cmp(minSize) < 0 {
			orderSize = minSize
		}
	}
	if maxOrderSize > 0 {
		maxSize := decimal.MustNew(int64(maxOrderSize), 0)
		if orderSize.Cmp(maxSize) > 0 {
			orderSize = maxSize
		}
	}

	return orderSize
}
