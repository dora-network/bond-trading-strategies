package copytrading

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/config"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
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
	// InitialBalance is the starting cash for a backtest simulation. A
	// zero value falls back to a package-level default (see the
	// Backtester for the exact number). Callers that want to use a
	// non-default balance should populate this field explicitly; the
	// HTTP decoder does so when the request sets initial_balance.
	InitialBalance decimal.Decimal
}

// Strategy implements the copy trading strategy.
type Strategy struct {
	cfg           Config
	marketAPI     marketAPIClient
	backtestStore tradesHistoryStore
	tradeWriter   stats.BacktestTradeWriter
	log           *slog.Logger
	tradeStream   *streams.TradeStream
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

// WithBacktestStore sets the trades history store used by the backtester.
func WithBacktestStore(store tradesHistoryStore) func(*Strategy) {
	return func(s *Strategy) {
		s.backtestStore = store
	}
}

// WithBacktestWriter sets the destination for per-trade rows the
// backtester emits as the simulation runs. If unset, trade rows are not
// persisted and the /trades and /closed-trades endpoints return empty.
func WithBacktestWriter(w stats.BacktestTradeWriter) func(*Strategy) {
	return func(s *Strategy) {
		s.tradeWriter = w
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
	if s.backtestStore == nil {
		return backtestResult, errors.New("backtest store not configured: use WithBacktestStore")
	}
	backtester := NewBacktester(s, s.backtestStore, s.tradeWriter)
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

	// Determine side. The trade stream sends lowercase ("buy"/"sell");
	// DORA's API expects the typed uppercase constants.
	var side doraclient.Side
	if trade.Side == "buy" {
		side = doraclient.SIDE_BUY
	} else {
		side = doraclient.SIDE_SELL
	}

	// Fetch the current position on the traded asset. Needed before
	// we can decide from_global_position (and which balance to size
	// the order from).
	available, borrowed, err := s.positionForAsset(ctx, trade.AssetID.String())
	if err != nil {
		return fmt.Errorf("position for asset %s: %w", trade.AssetID, err)
	}
	current := positionForAsset(available, borrowed)

	// DORA's from_global_position flag depends on whether this order
	// closes/reduces an existing position (no leverage needed) or
	// opens/extends a position (leverage rules apply).
	fromGlobal := fromGlobalPosition(side, current, s.cfg.Leverage)

	// Pick which asset's available balance to size the order from.
	// Closes look at the traded bond (we need to know how much we
	// hold); opens/extends look at USD (cash to spend, or collateral
	// to borrow the bond to short).
	quoteAssetID, err := s.marketAPI.QuoteAssetID(ctx, trade.OrderBookID.String())
	if err != nil {
		return fmt.Errorf("quote asset ID for order book %s: %w", trade.OrderBookID, err)
	}
	balanceAsset := balanceAssetFor(side, current, trade.AssetID.String(), quoteAssetID)

	// Now fetch the portfolio and read the right account's available
	// balance for the chosen asset. Account depends on fromGlobal:
	//   - fromGlobal=true: use the global account.
	//   - fromGlobal=false: use the isolated account for the asset,
	//     with fallback to global if no isolated account exists yet
	//     (DORA creates it on first leveraged order).
	portfolio, err := s.marketAPI.GetPortfolioV2(ctx)
	if err != nil {
		return fmt.Errorf("get portfolio: %w", err)
	}
	availableBalance := s.availableBalanceFor(portfolio, balanceAsset, fromGlobal)

	// Calculate order size: availableBalance * percentageOfAvailable * leverage
	orderSize := calculateOrderSize(availableBalance, s.cfg.PercentageOfAvailable, s.cfg.Leverage, s.cfg.MinOrderSize, s.cfg.MaxOrderSize)

	if orderSize.IsZero() || orderSize.IsNeg() {
		s.log.Info("skipping trade: calculated order size is zero or negative", "order_size", orderSize)
		return nil
	}

	// DORA's inverse_leverage is 1/leverage. DORA uses it to verify
	// our balance plus the implied borrow is sufficient to cover the
	// (client-side pre-leveraged) orderSize. When leverage is 1 (no
	// leverage), inverse_leverage is 1.0; when leverage is 0
	// (degenerate), treat as 1.
	inverseLeverage := decimal.One
	if s.cfg.Leverage.IsPos() && s.cfg.Leverage.Cmp(decimal.One) > 0 {
		inverseLeverage, _ = decimal.One.Quo(s.cfg.Leverage)
	}

	// Place market order
	err = s.marketAPI.CreateMarketOrder(
		ctx,
		trade.OrderBookID.String(),
		side,
		orderSize,
		inverseLeverage,
		fromGlobal,
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

// positionForAsset fetches the current available and borrowed amounts
// for the given asset via GetLedgerPositionsSelf. Returns zeros (no
// error) when the bot has no position on the asset.
func (s *Strategy) positionForAsset(ctx context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error) {
	return s.marketAPI.GetAssetPosition(ctx, assetID)
}

// positionDirection is the net direction of the bot's holdings for a given
// asset, derived from the ledger positions endpoint.
type positionDirection int

const (
	positionFlat positionDirection = iota
	positionLong
	positionShort
)

// positionForAsset returns the net position direction for an asset. A
// positive net (Available > Borrowed) is a long, a negative net is a
// short, zero is flat. Sourced from GetLedgerPositionsSelf so the value
// is consistent with the meanreversion strategy's view of "the bot's
// position on this asset".
func positionForAsset(available, borrowed decimal.Decimal) positionDirection {
	net, _ := available.Sub(borrowed)
	if net.IsPos() {
		return positionLong
	}
	if net.IsNeg() {
		return positionShort
	}
	return positionFlat
}

// balanceAssetFor returns the asset whose available balance should be
// used to size the order. The rule:
//
//   - Flat + Buy → quoteAssetID  (open a long; spend cash)
//   - Flat + Sell → quoteAssetID (open a short; USD is collateral
//     for borrowing the bond to short)
//   - Long + Buy → quoteAssetID  (extend the long; spend cash)
//   - Long + Sell → bondAssetID  (close the long; size by what we hold)
//   - Short + Sell → quoteAssetID (extend the short; USD is collateral)
//   - Short + Buy → bondAssetID  (close the short; buy back the borrowed)
//
// Closes look at the traded asset (bond); everything else looks at
// the quote asset (USD).
func balanceAssetFor(side doraclient.Side, current positionDirection, bondAssetID, quoteAssetID string) string {
	closes := (current == positionLong && side == doraclient.SIDE_SELL) ||
		(current == positionShort && side == doraclient.SIDE_BUY)
	if closes {
		return bondAssetID
	}
	return quoteAssetID
}

// fromGlobalPosition reports whether a DORA order with the given side
// and current asset position should use DORA's global (cross-margin)
// position pool.
//
// The flag is true when the order doesn't need DORA's leverage
// mechanism. That happens for closing trades (any side that reduces
// an existing position) and for opens of a long with no strategy-level
// leverage. Shorts and leveraged longs need isolated margin so DORA
// can scope margin to the order.
func fromGlobalPosition(side doraclient.Side, current positionDirection, leverage decimal.Decimal) bool {
	// Closes never need leverage.
	if current == positionLong && side == doraclient.SIDE_SELL {
		return true
	}
	if current == positionShort && side == doraclient.SIDE_BUY {
		return true
	}

	// Opens/extends. Shorts always need leverage; longs only when
	// strategy-level leverage > 1.
	noLeverage := leverage.Cmp(decimal.One) <= 0
	switch side {
	case doraclient.SIDE_BUY:
		return noLeverage
	case doraclient.SIDE_SELL:
		return false
	}
	return false
}

// availableBalanceFor returns the available balance from the
// appropriate account for the traded asset, given the
// fromGlobalPosition rule:
//   - fromGlobal=true:  use the global account (one per asset).
//   - fromGlobal=false: use the isolated account for the asset. If
//     no isolated account exists yet (e.g. the first leveraged
//     order on this asset — DORA creates it on first order), fall
//     back to the global account.
//
// The available balance is the account's Available field for the
// matched asset, summed if multiple matching accounts exist.
func (s *Strategy) availableBalanceFor(portfolio *doraclient.AccountPortfolioV2, assetID string, fromGlobal bool) decimal.Decimal {
	if portfolio == nil {
		return decimal.Zero
	}
	accounts := portfolio.GetAccounts()
	if len(accounts) == 0 {
		return decimal.Zero
	}

	// First pass: try the desired account type.
	if total := sumAvailableByAccountType(accounts, fromGlobal, assetID); !total.IsZero() {
		return total
	}

	// Fallback: if the desired type isn't found and we were looking
	// for an isolated account, use the global account. The isolated
	// one will be created by the first leveraged order.
	if !fromGlobal {
		return sumAvailableByAccountType(accounts, true, assetID)
	}
	return decimal.Zero
}

func sumAvailableByAccountType(
	accounts map[string]map[string]doraclient.AccountV2,
	wantGlobal bool,
	assetID string,
) decimal.Decimal {
	var total decimal.Decimal
	for _, assetMap := range accounts {
		account, ok := assetMap[assetID]
		if !ok {
			continue
		}
		if account.GetIsGlobal() != wantGlobal {
			continue
		}
		available, err := decimal.Parse(account.Available)
		if err != nil {
			continue
		}
		total, _ = total.Add(available)
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
