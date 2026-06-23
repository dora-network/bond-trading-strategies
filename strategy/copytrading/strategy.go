package copytrading

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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
	paused        bool
	mu            sync.Mutex
	// decisionStore is invoked after every successful CreateMarketOrder
	// in the live run loop.  nil disables recording (backtests, unit
	// tests, and any caller that does not opt in).
	decisionStore strategy.DecisionRecorder
	// decisionSeq is a per-run monotonic counter assigned to each
	// recorded decision.  Protected by mu.
	decisionSeq int64
	// TODO: copytrading stop-loss. The current copytrading strategy has
	// no stop-loss branch — exits only happen via the strategy.Stop
	// message on the message channel. When a stop-loss is added to
	// copytrading, mirror the meanreversion pattern: add
	// lastStopZ/lastStopPnL/lastStopTriggered fields (guarded by mu),
	// a recordStopLoss helper, and a LastStopLossTrigger getter, and
	// wire the HTTP handler's observer to call it. The observer in
	// strategy/http/handler.go already handles the absence of the
	// method gracefully (it only polls meanreversion.Strategy today).
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

// WithTradeStream sets the live trade stream for the strategy.
func WithTradeStream(ts *streams.TradeStream) func(*Strategy) {
	return func(s *Strategy) {
		s.tradeStream = ts
	}
}

// WithDecisionStore sets the recorder invoked after every successful
// CreateMarketOrder in the live run loop.  Passing nil disables
// recording.  Backtests do not pass a recorder and therefore never
// write to strategy_decisions.
func WithDecisionStore(store strategy.DecisionRecorder) func(*Strategy) {
	return func(s *Strategy) {
		s.decisionStore = store
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
	if s.tradeStream == nil {
		return errors.New("trade stream not configured: use WithTradeStream")
	}
	subscriberID, tradeCh := s.tradeStream.Subscribe(s.cfg.FollowedTrader)
	defer s.tradeStream.Unsubscribe(subscriberID)

	return s.runLoop(ctx, msgCh, tradeCh)
}

func (s *Strategy) runLoop(ctx context.Context, msgCh <-chan strategy.Message, tradeCh <-chan streams.TradeEvent) error {
	s.log.Info("copy trading run loop started", "run_id", s.runID, "followed_trader", s.cfg.FollowedTrader)

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
				s.mu.Lock()
				s.paused = true
				s.mu.Unlock()
				s.log.Info("copy trading paused", "run_id", s.runID)
			case strategy.Resume:
				s.mu.Lock()
				s.paused = false
				s.mu.Unlock()
				s.log.Info("copy trading resumed", "run_id", s.runID)
			}
		case trade, ok := <-tradeCh:
			if !ok {
				s.log.Info("trade channel closed, exiting run loop", "run_id", s.runID)
				return nil
			}
			s.mu.Lock()
			isPaused := s.paused
			s.mu.Unlock()
			if isPaused {
				s.log.Debug("skipping trade while paused", "run_id", s.runID, "trader", trade.TraderID)
				continue
			}
			s.log.Info("received trade event", "run_id", s.runID, "trader", trade.TraderID, "side", trade.Side, "order_book", trade.OrderBookID)
			if err := s.handleTrade(ctx, trade); err != nil {
				s.log.Error("failed to handle trade", "err", err, "trade", trade.ExecutionID)
			}
		}
	}
}

//nolint:funlen // diagnostic logging — will be refined after debugging
func (s *Strategy) handleTrade(ctx context.Context, trade streams.TradeEvent) error {
	// Check disallowed bonds
	if _, disallowed := s.disallowedSet[trade.AssetID]; disallowed {
		s.log.Info("skipping trade for disallowed bond", "asset", trade.AssetID)
		return nil
	}

	// Determine side. The trade stream sends "buy"/"sell" in varying
	// case; DORA's API expects the typed uppercase constants.
	var side doraclient.Side
	switch strings.ToLower(trade.Side) {
	case "buy":
		side = doraclient.SIDE_BUY
	case "sell":
		side = doraclient.SIDE_SELL
	default:
		return fmt.Errorf("unknown trade side %q", trade.Side)
	}

	// Fetch the portfolio first. We need it for:
	//   1. Position tracking (replaces the broken GetLedgerPositionsSelf
	//      call which only sees the global account).
	//   2. Account selection (global vs isolated for fromGlobal).
	//   3. Available balance sizing.
	portfolio, err := s.marketAPI.GetPortfolioV2(ctx)
	if err != nil {
		return fmt.Errorf("get portfolio: %w", err)
	}

	// Compute position from portfolio data. The portfolio includes
	// ALL accounts (global + isolated), so we correctly track
	// isolated margin positions.
	available, borrowed := positionFromPortfolio(portfolio, trade.AssetID.String())
	current := positionForAsset(available, borrowed)
	s.log.Debug("position state",
		"asset", trade.AssetID,
		"available", available,
		"borrowed", borrowed,
		"current", current,
	)

	positionOnGlobal, hasPosition := positionAccountIsGlobal(portfolio, trade.AssetID.String())

	// DORA's from_global_position flag:
	//   - Closes mirror the IsGlobal of the account holding the
	//     existing position (positionOnGlobal).
	//   - Opens/extends follow the leverage rules: long with no
	//     leverage → true; leveraged long or short → false.
	fromGlobal := fromGlobalPosition(side, current, s.cfg.Leverage, positionOnGlobal)
	s.log.Debug("account selection",
		"has_position", hasPosition,
		"position_on_global", positionOnGlobal,
		"from_global", fromGlobal,
		"leverage", s.cfg.Leverage,
	)

	// Pick which asset's available balance to size the order from.
	// Closes look at the traded bond (we need to know how much we
	// hold); opens/extends look at USD (cash to spend, or collateral
	// to borrow the bond to short).
	quoteAssetID, err := s.marketAPI.QuoteAssetID(ctx, trade.OrderBookID.String())
	if err != nil {
		return fmt.Errorf("quote asset ID for order book %s: %w", trade.OrderBookID, err)
	}
	balanceAsset := balanceAssetFor(side, current, trade.AssetID.String(), quoteAssetID)
	s.log.Debug("balance asset selected",
		"quote_asset", quoteAssetID,
		"balance_asset", balanceAsset,
		"side", side,
	)

	// DORA's inverse_leverage is 1/leverage. DORA rejects leveraged
	// closes ("cannot use leverage while position has available
	// bonds"). For closes, force leverage to 1 (no leverage).
	// For opens/extends, use the configured strategy leverage.
	orderLeverage := s.cfg.Leverage
	if isClose(side, current) {
		orderLeverage = decimal.One
		s.log.Info("closing position, forcing leverage=1", "side", side, "current", current)
	}

	// Read the right account's available balance for the chosen
	// asset. Account depends on fromGlobal:
	//   - fromGlobal=true: use the global account.
	//   - fromGlobal=false: use the isolated account for the bond.
	//     Each bond has its own isolated margin account, so we must
	//     look up the account that holds the bond asset and read the
	//     balanceAsset (USD) from that specific account. Summing
	//     across all isolated accounts overstates the balance DORA
	//     will actually check.
	availableBalance := s.availableBalanceFor(portfolio, balanceAsset, trade.AssetID.String(), fromGlobal)
	s.log.Info("available balance for sizing",
		"balance_asset", balanceAsset,
		"bond_asset", trade.AssetID,
		"available_balance", availableBalance,
		"from_global", fromGlobal,
	)

	// Calculate order size.
	//   - Closes: close the entire position (not a percentage).
	//   - Opens/extends: availableBalance * percentage * leverage.
	var orderSize decimal.Decimal
	if isClose(side, current) {
		if current == positionLong {
			orderSize = available
		} else {
			orderSize = borrowed
		}
		s.log.Info("closing position, using full position size",
			"current", current,
			"available", available,
			"borrowed", borrowed,
			"order_size", orderSize,
		)
	} else {
		orderSize = calculateOrderSize(availableBalance, s.cfg.PercentageOfAvailable, orderLeverage, s.cfg.MinOrderSize, s.cfg.MaxOrderSize)
		s.log.Info("calculated order size",
			"available_balance", availableBalance,
			"percentage", s.cfg.PercentageOfAvailable,
			"leverage", orderLeverage,
			"order_size", orderSize,
			"min_order_size", s.cfg.MinOrderSize,
			"max_order_size", s.cfg.MaxOrderSize,
		)
	}

	if orderSize.IsZero() || orderSize.IsNeg() {
		s.log.Info("skipping trade: calculated order size is zero or negative", "order_size", orderSize)
		return nil
	}

	inverseLeverage := decimal.One
	if orderLeverage.IsPos() && orderLeverage.Cmp(decimal.One) > 0 {
		inverseLeverage, _ = decimal.One.Quo(orderLeverage)
	}
	s.log.Info("placing market order",
		"order_book", trade.OrderBookID,
		"side", side,
		"order_size", orderSize,
		"inverse_leverage", inverseLeverage,
		"from_global", fromGlobal,
	)

	// Build the client_order_id before submitting so the same value
	// flows into the DORA request and the recorded decision row.
	clientOrderID := strategy.BuildClientOrderID(strategyType, s.runID)

	// Place market order
	err = s.marketAPI.CreateMarketOrder(
		ctx,
		trade.OrderBookID.String(),
		side,
		orderSize,
		inverseLeverage,
		fromGlobal,
		clientOrderID,
	)
	if err != nil {
		return fmt.Errorf("create market order: %w", err)
	}

	// Record the live-run decision AFTER a successful order. A failed
	// order must not produce a row — the row is the audit trail of
	// orders that actually reached DORA.
	kind := strategy.DecisionKindOpen
	if isClose(side, current) {
		kind = strategy.DecisionKindClose
	} else if current != positionFlat {
		kind = strategy.DecisionKindExtend
	}
	s.recordDecision(ctx, strategy.Decision{
		OrderBookID:        trade.OrderBookID,
		Asset:              trade.AssetID,
		Side:               string(side),
		Signal:             strings.ToLower(trade.Side),
		Quantity:           orderSize,
		Price:              trade.Price,
		Leverage:           orderLeverage,
		InverseLeverage:    inverseLeverage,
		FromGlobalPosition: fromGlobal,
		Kind:               kind,
		Reason:             "follow_trade",
		ReasonDetail:       fmt.Sprintf("followed trader %s execution %s", trade.TraderID, trade.ExecutionID),
		ClientOrderID:      clientOrderID,
	})

	s.log.Info("placed copy trade",
		"order_book", trade.OrderBookID,
		"asset", trade.AssetID,
		"side", trade.Side,
		"quantity", orderSize,
		"followed_trader", trade.TraderID)

	return nil
}

// positionDirection is the net direction of the bot's holdings for a given
// asset, derived from the ledger positions endpoint.
type positionDirection int

// strategyType is the strategy.Decision.StrategyType value used by
// the live run loop and the client_order_id format.  Keep in sync
// with the string written by recordDecision.
const strategyType = "copy_trading"

const (
	positionFlat positionDirection = iota
	positionLong
	positionShort
)

// positionForAsset returns the net position direction for an asset. A
// positive net (Available > Borrowed) is a long, a negative net is a
// short, zero is flat. Computed from the full portfolio across all
// accounts (global and isolated).
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

// positionFromPortfolio extracts the available and borrowed amounts for
// an asset across all accounts (global and isolated) in the portfolio.
// This correctly tracks isolated margin positions that GetLedgerPositionsSelf
// misses because it only queries the global account.
func positionFromPortfolio(portfolio *doraclient.AccountPortfolioV2, assetID string) (decimal.Decimal, decimal.Decimal) {
	if portfolio == nil {
		return decimal.Zero, decimal.Zero
	}
	accounts := portfolio.GetAccounts()
	if len(accounts) == 0 {
		return decimal.Zero, decimal.Zero
	}

	var totalAvailable, totalBorrowed decimal.Decimal
	for _, assetMap := range accounts {
		account, ok := assetMap[assetID]
		if !ok {
			continue
		}
		avail, err := decimal.Parse(account.Available)
		if err != nil {
			continue
		}
		borrow, err := decimal.Parse(account.Borrowed)
		if err != nil {
			continue
		}
		totalAvailable, _ = totalAvailable.Add(avail)
		totalBorrowed, _ = totalBorrowed.Add(borrow)
	}
	return totalAvailable, totalBorrowed
}

// isClose reports whether an order with the given side would close (or
// reduce) the current position.
func isClose(side doraclient.Side, current positionDirection) bool {
	closesLong := current == positionLong && side == doraclient.SIDE_SELL
	closesShort := current == positionShort && side == doraclient.SIDE_BUY
	return closesLong || closesShort
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
// The rule is two-part:
//
//   - Closes (Long+SELL or Short+BUY): fromGlobal mirrors the
//     IsGlobal flag of the account that holds the existing position
//     (positionOnGlobal). A close on the global account uses the
//     global pool (fromGlobal=true); a close on an isolated account
//     uses isolated margin (fromGlobal=false). The caller resolves
//     positionOnGlobal from the portfolio.
//
//   - Opens/extends: long with no leverage → global (true);
//     leveraged long or any short → isolated (false). This is
//     purely a function of (side, leverage) — the position doesn't
//     matter because there's nothing to close.
func fromGlobalPosition(side doraclient.Side, current positionDirection, leverage decimal.Decimal, positionOnGlobal bool) bool {
	closesLong := current == positionLong && side == doraclient.SIDE_SELL
	closesShort := current == positionShort && side == doraclient.SIDE_BUY
	if closesLong || closesShort {
		return positionOnGlobal
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

// positionAccountIsGlobal reports which account (global vs isolated)
// holds the existing position on the given asset. Returns (isGlobal,
// found):
//   - (true, true)  → the global account has the position
//   - (false, true) → an isolated account has the position
//   - (false, false) → no account has a position (flat)
//
// If the bot has positions on the same asset across both global and
// isolated accounts, the global account wins (closes should be
// against the global pool first, which is also where new positions
// land by default).
func positionAccountIsGlobal(portfolio *doraclient.AccountPortfolioV2, assetID string) (bool, bool) {
	if portfolio == nil {
		return false, false
	}
	hasNonZero := func(avail, borrow string) bool {
		a, _ := decimal.Parse(avail)
		b, _ := decimal.Parse(borrow)
		net, _ := a.Sub(b)
		return !net.IsZero()
	}

	accounts := portfolio.GetAccounts()
	if len(accounts) == 0 {
		return false, false
	}

	// Prefer the global account if it has a position.
	for _, assetMap := range accounts {
		account, ok := assetMap[assetID]
		if !ok {
			continue
		}
		if !account.GetIsGlobal() {
			continue
		}
		if hasNonZero(account.Available, account.Borrowed) {
			return true, true
		}
	}

	// Fall back to an isolated account.
	for _, assetMap := range accounts {
		account, ok := assetMap[assetID]
		if !ok {
			continue
		}
		if account.GetIsGlobal() {
			continue
		}
		if hasNonZero(account.Available, account.Borrowed) {
			return false, true
		}
	}

	return false, false
}

// availableBalanceFor returns the available balance from the
// appropriate account for the traded asset, given the
// fromGlobalPosition rule:
//   - fromGlobal=true:  use the global account (one per asset).
//   - fromGlobal=false: use the isolated account that holds the
//     bond. Each bond has its own isolated margin account, so we
//     find the account containing bondAssetID and read the
//     balanceAsset from that specific account. If no isolated
//     account for the bond exists yet, fall back to the global
//     account.
func (s *Strategy) availableBalanceFor(
	portfolio *doraclient.AccountPortfolioV2,
	balanceAsset string,
	bondAssetID string,
	fromGlobal bool,
) decimal.Decimal {
	if portfolio == nil {
		return decimal.Zero
	}
	accounts := portfolio.GetAccounts()
	if len(accounts) == 0 {
		return decimal.Zero
	}

	if fromGlobal {
		// Global account: sum across all global accounts for the asset.
		return sumAvailableByAccountType(accounts, true, balanceAsset)
	}

	// Isolated account: each bond has its own isolated margin
	// account. Find the account that contains the bond asset and
	// read the balanceAsset (e.g. USD) from that specific account.
	for _, assetMap := range accounts {
		bondAccount, ok := assetMap[bondAssetID]
		if !ok {
			continue
		}
		if bondAccount.GetIsGlobal() {
			continue
		}
		// This is the isolated margin account for the bond.
		balanceAccount, ok := assetMap[balanceAsset]
		if !ok {
			continue
		}
		available, err := decimal.Parse(balanceAccount.Available)
		if err != nil {
			continue
		}
		return available
	}

	// Fallback: no isolated account for this bond yet; use global.
	return sumAvailableByAccountType(accounts, true, balanceAsset)
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

// recordDecision is called by the live run loop immediately after a
// successful CreateMarketOrder.  It assigns the per-run seq, stamps
// CreatedAt, and forwards the row to the configured DecisionRecorder.
//
// The helper never returns an error: a failed persistence is logged and
// the strategy continues.  The live run is the source of truth for
// what orders were placed; a missing decision row is a degraded-but-
// correct outcome, not a failure that should kill the run.
//
// Backtests never opt in to a recorder and therefore never call into
// this code path.
func (s *Strategy) recordDecision(ctx context.Context, d strategy.Decision) {
	if s.decisionStore == nil {
		return
	}
	s.mu.Lock()
	s.decisionSeq++
	seq := s.decisionSeq
	runID := s.runID
	s.mu.Unlock()

	d.RunID = runID
	d.Seq = seq
	d.StrategyType = strategyType
	if d.CreatedAt.IsZero() {
		d.CreatedAt = time.Now().UTC()
	}
	if err := s.decisionStore.SaveDecision(ctx, d); err != nil {
		s.log.Error("save strategy decision",
			"err", err,
			"run_id", d.RunID,
			"seq", d.Seq,
			"side", d.Side,
			"signal", d.Signal,
			"quantity", d.Quantity,
		)
	}
}
