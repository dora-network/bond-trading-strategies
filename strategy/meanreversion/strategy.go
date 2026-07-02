package meanreversion

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/config"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/strategy/window"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// Config holds all tunable parameters for the mean-reversion strategy.
// All fields have sensible defaults via DefaultConfig.
type Config struct {
	config.Config
	// LookbackWindow is the number of observations used for the rolling mean
	// and standard deviation. Typical values: 20-60 trading days.
	LookbackWindow int

	// EntryZScore is the z-score threshold at which the strategy opens a
	// position. A common starting point is 2.0 (2 standard deviations).
	EntryZScore decimal.Decimal

	// ExitZScore is the z-score at which an open position is closed.
	// Should be strictly less than EntryZScore in absolute value.
	// A value of 0.5 means "close when the spread is within half a std-dev of
	// the mean" - i.e. the reversion is mostly complete.
	ExitZScore decimal.Decimal

	// StopLossZScore closes a losing position if the spread moves further
	// against us beyond this z-score magnitude. Set to 0 to disable.
	// A value of 3.5 is a reasonable starting point.
	StopLossZScore decimal.Decimal

	// MinStdDev is the minimum spread standard deviation below which the
	// strategy will not trade (avoids reacting to noise in a very flat market).
	// Expressed in the same units as the spread (decimal yield).
	MinStdDev decimal.Decimal

	// MaxPositionSize caps the fraction of capital allocated per trade.
	// 1.0 = 100 % of capital; 0.5 = at most 50 %.
	MaxPositionSize decimal.Decimal

	// OrderBookID is the ID of the DORA order book to place orders on.
	OrderBookID uuid.UUID

	// Tenor is the tenor to use for the benchmark yield
	Tenor string

	// InitialBalance is the starting capital allocated to this strategy.
	// The actual trade size is derived from InitialBalance × MaxPositionSize.
	InitialBalance decimal.Decimal

	// Leverage to apply when placing orders. Default is 1.0
	Leverage decimal.Decimal
}

func DefaultConfig() Config {
	return Config{
		LookbackWindow:  20,
		EntryZScore:     decimal.Two,
		ExitZScore:      decimal.MustNew(5, 1),  //nolint:mnd
		StopLossZScore:  decimal.MustNew(35, 1), //nolint:mnd
		MinStdDev:       decimal.MustNew(5, 4),  //nolint:mnd
		MaxPositionSize: decimal.One,
		InitialBalance:  decimal.One,
		Leverage:        decimal.One,
	}
}

// Strategy is the mean-reversion trading strategy for a single bond.
// Each bond tracked by the caller should have its own Strategy instance.
//
// The strategy observes a rolling window of yield-spread observations
// (bond YTM - benchmark yield), computes a z-score, and emits buy/sell
// signals when the spread deviates significantly from its mean.
type Strategy struct {
	mu                    sync.RWMutex
	cfg                   Config
	log                   *slog.Logger
	window                *window.Rolling
	cancel                context.CancelFunc
	isRunning             bool
	paused                bool
	pricesHandler         *prices.Handler
	marketAPIClient       marketAPIClient
	historyStore          historicalPriceStore
	benchmarkClient       benchmarkYieldClient
	pricesReqID           uuid.UUID
	benchmarkObservations []fred.Observation
	errs                  []error
	backtestWriter        stats.BacktestTradeWriter

	// Tracked balances, initialised from DORA on Run and updated on trade
	// execution.  Protected by mu.
	balancesInitialized bool
	bondQty             decimal.Decimal // net bond position (+ = long, - = short)
	usdBal              decimal.Decimal // USD available balance

	// lastPrice is the most recent clean bond price observed for the
	// configured asset, used as the recorded Price on close decisions
	// (DORA fills the close at the market price, which is approximately
	// the last observed mid).  Zero until the first price update.  Protected
	// by mu.
	lastPrice decimal.Decimal

	// openSignal records the signal direction of the currently open position.
	// SignalHold means flat (no position). Derived from bondQty in
	// initializeBalances (so restarts correctly see the pre-existing position)
	// and kept in sync by executeDecision and closePosition. Protected by mu.
	openSignal types.Signal
	runID      uuid.UUID

	// collateralWeight is the collateral weight of the base asset, fetched from
	// DORA during Run. Defaults to 1.0 if unavailable. Used to compute effective
	// capital: InitialBalance × collateralWeight × Leverage.
	collateralWeight decimal.Decimal

	// lastStop* record the most recent stop-loss trigger from ShouldExit.
	// The HTTP handler polls LastStopLossTrigger to emit EventRunStopLoss.
	// Protected by mu.
	lastStopZ         decimal.Decimal
	lastStopPnL       decimal.Decimal
	lastStopTriggered bool

	// decisionStore is invoked after every successful CreateMarketOrder
	// in the live run loop.  nil disables recording (backtests, unit
	// tests, and any caller that does not opt in).
	decisionStore strategy.DecisionRecorder
	// decisionSeq is a per-run monotonic counter assigned to each
	// recorded decision.  Protected by mu.
	decisionSeq int64
}

// strategyType is the strategy.Decision.StrategyType value used by
// the live run loop and the client_order_id format.  Keep in sync
// with the string written by recordDecision.
const strategyType = "mean_reversion"

// New creates a new Strategy with the given Config and optional functional options.
// Supported options: WithLogger.

func New(cfg Config, pricesHandler *prices.Handler, opts ...func(*Strategy)) *Strategy {
	if cfg.Leverage.IsZero() {
		cfg.Leverage = decimal.One
	}

	s := &Strategy{
		cfg:              cfg,
		window:           window.NewRollingWindow(cfg.LookbackWindow),
		pricesHandler:    pricesHandler,
		marketAPIClient:  newDoraClient(),
		errs:             make([]error, 0),
		collateralWeight: decimal.One,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// WithLogger sets the logger on a mean-reversion Strategy.
func WithLogger(log *slog.Logger) func(*Strategy) {
	return func(s *Strategy) {
		s.log = log
	}
}

// WithMarketAPIClient sets the market API client on a mean-reversion Strategy.
// Use this to inject a client that authenticates with a specific API key
// (e.g. the user's own key decrypted from storage) instead of the default
// client that reads DORA_API_KEY from the environment.
func WithMarketAPIClient(client marketAPIClient) func(*Strategy) {
	return func(s *Strategy) {
		s.marketAPIClient = client
	}
}

// WithBacktestWriter sets the destination for per-trade rows the
// backtester emits during a backtest. If unset, trade rows are not
// persisted and the /trades endpoints return empty.
func WithBacktestWriter(w stats.BacktestTradeWriter) func(*Strategy) {
	return func(s *Strategy) {
		s.backtestWriter = w
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

func (s *Strategy) logger() *slog.Logger {
	if s.log == nil {
		return slog.Default()
	}
	return s.log
}

func (s *Strategy) Backtest(ctx context.Context, start, end time.Time) (backtestResult types.BacktestResult, err error) {
	if end.UTC().Before(start.UTC()) {
		return backtestResult, fmt.Errorf("end date must be after start date")
	}

	if start.UTC().After(time.Now().UTC()) || end.UTC().After(time.Now().UTC()) {
		return backtestResult, fmt.Errorf("start and end date must be in the past")
	}

	bt := NewBacktester(s, s.backtestWriter)
	obs, err := s.getObservations(ctx, start, end)
	if err != nil {
		return backtestResult, err
	}
	return bt.Run(ctx, obs)
}

func (s *Strategy) Run(ctx context.Context, msgCh <-chan strategy.Message, runID uuid.UUID) error {
	s.mu.Lock()
	s.runID = runID

	if s.isRunning {
		s.mu.Unlock()
		return fmt.Errorf("strategy is already running")
	}
	var runCtx context.Context
	runCtx, s.cancel = context.WithCancel(ctx)

	pricesCh, err := s.subscribePrices()
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to subscribe to prices: %w", err)
	}

	s.isRunning = true
	s.mu.Unlock()

	return s.run(runCtx, msgCh, pricesCh)
}

func (s *Strategy) subscribePrices() (<-chan map[uuid.UUID]prices.AssetPrice, error) {
	s.pricesReqID = uuid.Must(uuid.NewV7())
	pricesCh, err := s.pricesHandler.Subscribe(s.pricesReqID)
	if err != nil {
		s.pricesReqID = uuid.Nil
		return nil, err
	}
	return pricesCh, nil
}

func (s *Strategy) unsubscribePrices() {
	if s.pricesHandler == nil || s.pricesReqID == uuid.Nil {
		return
	}
	if err := s.pricesHandler.Unsubscribe(s.pricesReqID); err != nil {
		s.logger().Error("failed to unsubscribe from prices", "error", err)
	}
}

func (s *Strategy) lookupAssetID(orderBookID uuid.UUID) (string, error) {
	if s.marketAPIClient == nil {
		return "", errors.New("DORA order book lookup client is not configured")
	}
	if orderBookID == uuid.Nil {
		return "", errors.New("order book ID is required")
	}
	assetID, err := s.marketAPIClient.BaseAssetID(context.Background(), orderBookID.String())
	if err != nil {
		return "", err
	}
	if assetID == "" {
		return "", fmt.Errorf("order book %s returned an empty base asset ID", orderBookID)
	}
	return assetID, nil
}

func (s *Strategy) currentPosition(ctx context.Context, assetID string) (decimal.Decimal, error) {
	if s.marketAPIClient == nil {
		return decimal.Zero, errors.New("DORA order book lookup client is not configured")
	}
	if strings.TrimSpace(assetID) == "" {
		return decimal.Zero, errors.New("asset ID is required")
	}
	long, short, err := s.marketAPIClient.AssetPosition(ctx, assetID)
	if err != nil {
		return decimal.Zero, err
	}
	if long.IsZero() && short.IsZero() {
		return decimal.Zero, nil
	}

	if short.IsZero() {
		return long, nil
	}

	return short.Neg(), nil
}

func (s *Strategy) cappedOrderQuantity(positionSize, currentPosition, price decimal.Decimal) (decimal.Decimal, bool, error) {
	if positionSize.IsNeg() {
		return decimal.Zero, false, errors.New("position size must be non-negative")
	}
	if price.IsZero() {
		return decimal.Zero, false, errors.New("price must be non-zero")
	}
	if currentPosition.IsNeg() {
		currentPosition = decimal.Zero
	}

	// Effective capital = initial balance × collateral weight × leverage.
	effectiveCapital, err := s.cfg.InitialBalance.Mul(s.collateralWeight)
	if err != nil {
		return decimal.Zero, false, fmt.Errorf("calculate effective capital (collateral): %w", err)
	}
	effectiveCapital, err = effectiveCapital.Mul(s.cfg.Leverage)
	if err != nil {
		return decimal.Zero, false, fmt.Errorf("calculate effective capital (leverage): %w", err)
	}

	// Dollar budget = effective capital × fraction of remaining balance.
	budget, err := effectiveCapital.Mul(positionSize)
	if err != nil {
		return decimal.Zero, false, fmt.Errorf("calculate budget: %w", err)
	}
	if !budget.IsPos() {
		return decimal.Zero, false, nil
	}

	// Ensure we don't exceed total capital (position value + remaining).
	positionValue, err := currentPosition.Mul(price)
	if err != nil {
		return decimal.Zero, false, fmt.Errorf("calculate position value: %w", err)
	}
	if positionValue.Cmp(effectiveCapital) >= 0 {
		// Already fully invested.
		return decimal.Zero, false, nil
	}
	remainingBudget, err := effectiveCapital.Sub(positionValue)
	if err != nil {
		return decimal.Zero, false, fmt.Errorf("calculate remaining budget: %w", err)
	}
	if budget.Cmp(remainingBudget) > 0 {
		budget = remainingBudget
	}

	// Convert dollar budget to bond quantity.
	// Bond quantity must be a whole number (no fractional bonds).
	quantity, err := budget.Quo(price)
	if err != nil {
		return decimal.Zero, false, fmt.Errorf("calculate quantity from budget: %w", err)
	}
	quantity = quantity.Floor(0)
	if quantity.IsZero() {
		// Budget too small to buy even one bond.
		return decimal.Zero, false, nil
	}
	return quantity, true, nil
}

// initializeBalances fetches the user's bond position and USD balance from
// DORA and stores them as the initial tracked balances.  This is best-effort:
// if any API call fails the error is recorded in s.errs and the strategy
// continues with an uninitialised tracker (falling back to API lookups on
// each trade).
//
// The account used for the USD balance depends on fromGlobalPosition:
//   - true (leverage == 1x):  use the global account's USD balance as InitialBalance.
//   - false (leverage > 1x): use the isolated account for the base asset.
func (s *Strategy) initializeBalances(ctx context.Context, baseAssetID string) {
	quoteAssetID, err := s.marketAPIClient.QuoteAssetID(ctx, s.cfg.OrderBookID.String())
	if err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, fmt.Errorf("initialise balances: get quote asset: %w", err))
		s.mu.Unlock()
		return
	}

	// Try the V2 portfolio API first — it returns accounts with IsGlobal flags
	// so we can pick the right one for the leverage level.
	portfolio, err := s.marketAPIClient.GetPortfolioV2(ctx)
	if err == nil && portfolio != nil {
		initializeBalancesFromPortfolio(s, portfolio, baseAssetID, quoteAssetID, false, s.logger())
		s.mu.Lock()
		s.balancesInitialized = true
		s.mu.Unlock()
		return
	}
	if err != nil {
		s.log.Warn("initialise balances: v2 portfolio unavailable, falling back to legacy path", "err", err)
	}

	// Fallback: use the AssetPosition path. This uses the global
	// account regardless of leverage. The user ID is resolved (and
	// cached) inside AssetPosition.

	// Fetch bond position.
	bondAvailable, bondBorrowed, err := s.marketAPIClient.AssetPosition(ctx, baseAssetID)
	if err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, fmt.Errorf("initialise balances: get bond position: %w", err))
		s.mu.Unlock()
	} else {
		s.mu.Lock()
		if !bondBorrowed.IsZero() {
			s.bondQty = bondBorrowed.Neg()
		} else {
			s.bondQty = bondAvailable
		}
		s.mu.Unlock()
	}

	// Fetch USD balance.
	usdAvailable, _, err := s.marketAPIClient.AssetPosition(ctx, quoteAssetID)
	if err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, fmt.Errorf("initialise balances: get USD balance: %w", err))
		s.mu.Unlock()
	} else {
		s.mu.Lock()
		s.usdBal = usdAvailable
		if !usdAvailable.IsZero() {
			s.cfg.InitialBalance = usdAvailable
		}
		s.mu.Unlock()
	}

	// Mark balances as initialised regardless of partial failures — any fields
	// that failed will stay at zero.
	s.mu.Lock()
	s.balancesInitialized = true
	// Reconstruct the open-position signal from the fetched bond quantity.
	switch {
	case s.bondQty.IsPos():
		s.openSignal = types.SignalBuy
	case s.bondQty.IsNeg():
		s.openSignal = types.SignalSell
	default:
		s.openSignal = types.SignalHold
	}
	s.mu.Unlock()
}

// closePosition closes the entire open position by placing a market order in
// the opposite direction for the full position quantity. It is called by the
// run loop when ShouldExit returns true. It uses the in-memory tracked bondQty
// to close the position. If placing the order fails, it checks the live position
// on DORA and self-heals the tracking state if the actual position is already 0.
func (s *Strategy) closePosition(ctx context.Context, assetID string) error {
	s.mu.RLock()
	qty := s.bondQty
	useTracked := s.balancesInitialized
	s.mu.RUnlock()

	if !useTracked {
		var err error
		qty, err = s.currentPosition(ctx, assetID)
		if err != nil {
			return err
		}
	}

	if qty.IsZero() {
		// Already flat — just clear the signal.
		s.mu.Lock()
		if useTracked {
			s.bondQty = decimal.Zero
		}
		s.openSignal = types.SignalHold
		s.mu.Unlock()
		return nil
	}

	// Close a long by selling; close a short by buying back.
	side := doraclient.SIDE_SELL
	closeQty := qty
	if qty.IsNeg() {
		side = doraclient.SIDE_BUY
		closeQty = qty.Neg()
	}

	inverseLeverage, err := decimal.One.Quo(s.cfg.Leverage)
	if err != nil {
		inverseLeverage = decimal.One
	}

	// Build the client_order_id before submitting so the same value
	// flows into the DORA request and the recorded decision row.
	clientOrderID := strategy.BuildClientOrderID(strategyType, s.runID)

	if err := s.marketAPIClient.CreateMarketOrder(
		ctx, s.cfg.OrderBookID.String(), side, closeQty, inverseLeverage, false, clientOrderID,
	); err != nil {
		// Self-healing: if the order failed, check the live position on the exchange.
		// If the live position is actually already 0, we can self-heal and clear our tracking state.
		if liveQty, liveErr := s.currentPosition(ctx, assetID); liveErr == nil && liveQty.IsZero() {
			s.log.Info("close order failed but live position is already 0, self-healing tracking state", "runID", s.runID)
			s.mu.Lock()
			if useTracked {
				s.bondQty = decimal.Zero
			}
			s.openSignal = types.SignalHold
			s.mu.Unlock()
			return nil
		}
		return err
	}

	// Record the live-run decision AFTER a successful close order.
	closeSignal := types.SignalSell
	if side == doraclient.SIDE_BUY {
		closeSignal = types.SignalBuy
	}
	s.mu.RLock()
	closePrice := s.lastPrice
	s.mu.RUnlock()
	s.recordDecision(ctx, strategy.Decision{
		OrderBookID:        s.cfg.OrderBookID,
		Asset:              mustParseUUID(assetID),
		Side:               string(side),
		Signal:             closeSignal.String(),
		Quantity:           closeQty,
		Price:              closePrice, // last observed mid; DORA fills at the market mid
		Leverage:           s.cfg.Leverage,
		InverseLeverage:    inverseLeverage,
		FromGlobalPosition: false,
		Kind:               strategy.DecisionKindClose,
		Reason:             "z_score_exit",
		ReasonDetail:       "close: spread reverted to mean",
		ClientOrderID:      clientOrderID,
	})

	s.mu.Lock()
	if useTracked {
		s.bondQty = decimal.Zero
	}
	s.openSignal = types.SignalHold
	s.mu.Unlock()
	return nil
}

func (s *Strategy) executeDecision(ctx context.Context, decision types.Decision, assetID string) (bool, error) {
	if decision.Signal != types.SignalBuy && decision.Signal != types.SignalSell {
		return false, nil
	}

	// Use the tracked balance if available, otherwise fall back to the DORA
	// API (expensive, but safe).
	s.mu.RLock()
	position := s.bondQty
	useTracked := s.balancesInitialized
	s.mu.RUnlock()

	if !useTracked {
		s.log.Debug("using DORA API to get current position", "runID", s.runID, "assetID", assetID)
		var err error
		position, err = s.currentPosition(ctx, assetID)
		if err != nil {
			return false, err
		}
	}

	price := decision.Price
	if price.IsZero() {
		return false, errors.New("cannot execute decision: price is zero")
	}
	quantity, ok, err := s.cappedOrderQuantity(decision.PositionSize, position, price)
	if err != nil {
		return false, err
	}
	if !ok || quantity.IsZero() {
		return false, nil
	}
	side := doraclient.SIDE_BUY
	if decision.Signal == types.SignalSell {
		side = doraclient.SIDE_SELL
	}

	inverseLeverage, err := decimal.One.Quo(s.cfg.Leverage)
	if err != nil {
		return false, fmt.Errorf("compute inverse leverage: %w", err)
	}

	// All mean-reversion trading routes through the bond's
	// isolated margin account — strategy leverage is reflected
	// only in inverse_leverage.
	fromGlobalPosition := false

	s.log.Info("opening position", "runID", s.runID, "assetID", assetID, "signal", decision.Signal)
	s.log.Info("creating market order",
		"runID", s.runID,
		"assetID", assetID,
		"side", side,
		"quantity", quantity,
		"price", price,
		"inverseLeverage", inverseLeverage,
		"fromGlobalPosition", fromGlobalPosition,
	)
	// Build the client_order_id before submitting so the same value
	// flows into the DORA request and the recorded decision row.
	clientOrderID := strategy.BuildClientOrderID(strategyType, s.runID)
	if err := s.marketAPIClient.CreateMarketOrder(
		ctx, s.cfg.OrderBookID.String(), side, quantity, inverseLeverage, fromGlobalPosition, clientOrderID,
	); err != nil {
		return false, err
	}

	// Record the live-run decision AFTER a successful order. A failed
	// order must not produce a row — the row is the audit trail of
	// orders that actually reached DORA.
	s.recordDecision(ctx, strategy.Decision{
		OrderBookID:        s.cfg.OrderBookID,
		Asset:              mustParseUUID(assetID),
		Side:               string(side),
		Signal:             decision.Signal.String(),
		Quantity:           quantity,
		Price:              price,
		Leverage:           s.cfg.Leverage,
		InverseLeverage:    inverseLeverage,
		FromGlobalPosition: fromGlobalPosition,
		Kind:               strategy.DecisionKindOpen,
		Reason:             "z_score_entry",
		ReasonDetail:       fmt.Sprintf("z-score entry: z=%s signal=%s", decision.ZScore.String(), decision.Signal),
		ClientOrderID:      clientOrderID,
	})

	// Update tracked balances and openSignal after a successful order.
	// We use the decision price as an approximation — the actual fill may
	// differ slightly.
	s.mu.Lock()
	if useTracked {
		if side == doraclient.SIDE_BUY {
			s.bondQty, _ = s.bondQty.Add(quantity)
			cost, _ := quantity.Mul(price)
			s.usdBal, _ = s.usdBal.Sub(cost)
		} else {
			s.bondQty, _ = s.bondQty.Sub(quantity)
			proceeds, _ := quantity.Mul(price)
			s.usdBal, _ = s.usdBal.Add(proceeds)
		}
		// Derive openSignal from the net position so it always reflects reality.
		switch {
		case s.bondQty.IsPos():
			s.openSignal = types.SignalBuy
		case s.bondQty.IsNeg():
			s.openSignal = types.SignalSell
		default:
			s.openSignal = types.SignalHold
		}
	} else {
		// Balance tracking is unavailable; set openSignal from the decision
		// direction since we cannot derive it from bondQty.
		s.openSignal = decision.Signal
	}
	s.mu.Unlock()
	return true, nil
}

//nolint:funlen // main run loop with setup and teardown
func (s *Strategy) run(ctx context.Context, msgs <-chan strategy.Message, prices <-chan map[uuid.UUID]prices.AssetPrice) error {
	defer func() {
		s.mu.Lock()
		s.isRunning = false
		s.mu.Unlock()
	}()
	defer s.unsubscribePrices()

	assetID, err := s.lookupAssetID(s.cfg.OrderBookID)
	if err != nil {
		return fmt.Errorf("error looking up asset ID: %w", err)
	}

	// Fetch the collateral weight of the base asset from DORA. This is
	// best-effort — if the API is unavailable the default of 1.0 is used.
	collateralWeight, err := s.marketAPIClient.AssetCollateralWeight(ctx, assetID)
	if err != nil {
		s.log.Warn("collateral weight lookup failed, defaulting to 1.0", "assetID", assetID, "err", err)
	} else {
		s.mu.Lock()
		s.collateralWeight = collateralWeight
		s.mu.Unlock()
	}

	// Pre-fill the rolling window with historical data so that signals can
	// be generated immediately on the first price tick.  This is best-effort
	// — if the historical price store or FRED client are unavailable the
	// strategy will still start with an empty window (the existing behaviour).
	s.log.Info("prefilling window with historical data", "runID", s.runID)
	if err := s.prefillWindow(ctx, assetID); err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, fmt.Errorf("prefill window (non-fatal): %w", err))
		s.mu.Unlock()
	}

	// Fetch the user's bond position and USD balance from DORA and track
	// them in-memory for subsequent trade execution, avoiding repeated API
	// calls.  This is best-effort — if the API is unavailable the strategy
	// will look up positions on each trade via the DORA API.
	s.log.Info("initialising balances", "runID", s.runID, "assetID", assetID)
	s.initializeBalances(ctx, assetID)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-msgs:
			s.mu.Lock()
			switch msg {
			case strategy.Pause:
				s.paused = true
			case strategy.Resume:
				s.paused = false
			case strategy.Stop:
				s.cancel()
			}
			s.mu.Unlock()
		case pxs := <-prices:
			for _, px := range pxs {
				s.log.Debug("processing price update", "runID", s.runID, "assetID", px.AssetID, "time", px.Time)
				// if the price update is for a different asset, ignore it
				if px.AssetID != assetID {
					continue
				}
				if px.YTM == nil {
					// no YTM available, ignore price updates
					continue
				}
				// getBenchmarkYield may make a FRED API call and acquire a
				// write lock to update the cache, so we call it without
				// holding s.mu.
				benchmarkYield := s.getBenchmarkYield(ctx, px.Time)
				s.log.Debug("processing price update",
					"runID", s.runID,
					"assetID", px.AssetID,
					"time", px.Time,
					"price", px.Price,
					"ytm", *px.YTM,
					"benchmarkYield", benchmarkYield)

				s.mu.Lock()
				obs := types.YieldObservation{
					Time:           px.Time,
					BondID:         px.AssetID,
					YTM:            *px.YTM,
					BenchmarkYield: benchmarkYield,
					Price:          px.Price,
				}
				// Track the most recent clean price for the configured
				// asset.  Recorded on close decisions as the approximate
				// fill price (DORA fills at the market mid, which is
				// approximately the last observed mid).
				s.lastPrice = px.Price
				// Read window readiness before Update so the guard and the
				// z-score in decision are computed from the same window state.
				// On the tick that makes the window full, decision.ZScore is
				// still 0 (computed from the incomplete pre-add window), so
				// we must not evaluate exit conditions on that tick.
				windowReadyBeforeUpdate := s.window.Ready()
				decision, err := s.Update(obs)
				s.log.Debug("decision generated",
					"runID", s.runID,
					"assetID", px.AssetID,
					"time", px.Time,
					"zScore", decision.ZScore,
					"signal", decision.Signal,
				)
				if err != nil {
					s.log.Error("failed to update strategy", "runID", s.runID, "assetID", px.AssetID, "time", px.Time, "err", err)
					s.mu.Unlock()
					continue
				}

				if s.paused {
					// strategy is paused, ignore decision
					s.log.Debug("strategy is paused, ignoring decision", "runID", s.runID, "assetID", px.AssetID, "time", px.Time)
					s.mu.Unlock()
					continue
				}
				currentOpenSignal := s.openSignal
				s.mu.Unlock()

				if currentOpenSignal != types.SignalHold {
					// A position is open. Only evaluate exit conditions once the
					// rolling window was already full before this tick so that
					// the z-score in decision is statistically meaningful.
					if windowReadyBeforeUpdate {
						if shouldExit, reason := s.ShouldExit(currentOpenSignal, decision.ZScore); shouldExit {
							s.log.Info("exiting position", "reason", reason, "runID", s.runID)
							if err := s.closePosition(ctx, px.AssetID); err != nil {
								s.log.Error("failed to close position", "runID", s.runID, "assetID", px.AssetID, "time", px.Time, "err", err)
								s.mu.Lock()
								s.errs = append(s.errs, err)
								s.mu.Unlock()
							}
						}
					}
					// Whether we just closed or are still holding, do not
					// open a new position on this tick.
					continue
				}

				// No open position — check for a new entry signal.
				if decision.Signal == types.SignalHold {
					continue
				}

				if _, err := s.executeDecision(ctx, decision, px.AssetID); err != nil {
					s.log.Error("failed to execute decision", "runID", s.runID, "assetID", px.AssetID, "time", px.Time, "err", err)
					s.mu.Lock()
					s.errs = append(s.errs, err)
					s.mu.Unlock()
				}
			}
		case <-ticker.C:
			s.mu.RLock()
			if s.paused {
				s.mu.RUnlock()
				continue
			}
			s.mu.RUnlock()
		}
	}
}

func (s *Strategy) getBenchmarkYield(ctx context.Context, ts time.Time) decimal.Decimal {
	// First, check the in-memory cache.
	yield, cachedDate, ok := s.cachedBenchmarkYield(ts)
	normedTS := normalizeDate(ts)
	if ok && !cachedDate.Before(normedTS) {
		return yield
	}

	// Cache miss or stale — fetch from FRED.
	tenor, err := parseBenchmarkTenor(s.cfg.Tenor)
	if err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, fmt.Errorf("get benchmark yield: parse tenor: %w", err))
		s.mu.Unlock()
		if ok {
			return yield
		}
		return decimal.Zero
	}

	client, err := s.getBenchmarkYieldClient()
	if err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, fmt.Errorf("get benchmark yield: get client: %w", err))
		s.mu.Unlock()
		if ok {
			return yield
		}
		return decimal.Zero
	}

	// Request a window ending on the target date so we get the most recent
	// published yield (the exact date may not yet be available from FRED).
	start := normedTS.AddDate(0, 0, -10)
	end := normedTS

	obs, err := client.FetchHistoricalYields(ctx, tenor, start, end)
	if err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, fmt.Errorf("get benchmark yield: fred fetch: %w", err))
		s.mu.Unlock()
		if ok {
			return yield
		}
		return decimal.Zero
	}

	if len(obs) == 0 {
		// FRED returned no data for this window; fall back to stale cache if
		// we have it.
		if ok {
			return yield
		}
		return decimal.Zero
	}

	// Merge new observations into the in-memory cache.
	s.mergeBenchmarkObservations(obs)

	// Return the yield from the in-memory cache (FRED yields are converted to
	// percentage format during merge, consistent with DORA YTM).
	yield, _, ok = s.cachedBenchmarkYield(ts)
	if !ok {
		return decimal.Zero
	}
	return yield
}

// Update ingests a new yield observation and returns the resulting Decision.
//
// The rolling mean and standard deviation are computed from the window state
// BEFORE the new observation is added, so the z-score reflects the deviation
// of the current spread against its historical distribution — no look-ahead
// bias.
//
// If the rolling window is not yet full (not enough history), the signal will
// always be SignalHold.
func (s *Strategy) Update(obs types.YieldObservation) (types.Decision, error) {
	spread, err := obs.Spread()
	if err != nil {
		return types.Decision{}, err
	}

	// Compute rolling statistics from the window state BEFORE adding the
	// current observation. This avoids look-ahead bias: the z-score for each
	// bar measures the current spread against the distribution of past spreads.
	stdDev, err := s.window.StdDev()
	if err != nil {
		return types.Decision{}, err
	}
	rollingMean := s.window.Mean()

	// Compute z-score using the pre-add mean and stddev.
	var zScore decimal.Decimal
	if s.window.Ready() && stdDev.Cmp(s.cfg.MinStdDev) >= 0 {
		num, err := spread.Sub(rollingMean)
		if err != nil {
			return types.Decision{}, err
		}
		zScore, err = num.Quo(stdDev)
		if err != nil {
			return types.Decision{}, err
		}
	}

	// Add the current spread to the window for future observations.
	if err = s.window.Add(spread); err != nil {
		return types.Decision{}, err
	}

	d := types.Decision{
		Time:           obs.Time,
		BondID:         obs.BondID,
		YTM:            obs.YTM,
		BenchmarkYield: obs.BenchmarkYield,
		Spread:         spread,
		RollingMean:    rollingMean,
		RollingStdDev:  stdDev,
		ZScore:         zScore,
		Price:          obs.Price,
		Signal:         types.SignalHold,
	}

	// Signal logic uses the pre-add z-score.
	switch {
	case zScore.Cmp(s.cfg.EntryZScore) >= 0:
		// Spread is abnormally wide -> bond is cheap -> BUY
		d.Signal = types.SignalBuy
	case zScore.Cmp(s.cfg.EntryZScore.Neg()) <= 0:
		// Spread is abnormally tight -> bond is rich -> SELL
		d.Signal = types.SignalSell
	}

	// Position size is proportional to the absolute z-score above the entry
	// threshold, capped at MaxPositionSize.
	if d.Signal != types.SignalHold {
		excess, err := zScore.Abs().Sub(s.cfg.EntryZScore)
		if err != nil {
			return types.Decision{}, err
		}
		// scales 0->1 for each std-dev above entry
		size, err := excess.Quo(s.cfg.EntryZScore)
		if err != nil {
			return types.Decision{}, err
		}
		if size.Cmp(s.cfg.MaxPositionSize) > 0 {
			size = s.cfg.MaxPositionSize
		}
		d.PositionSize = size
	}

	return d, nil
}

// ShouldExit reports whether an open position should be closed given the
// current z-score and the direction in which the position was opened.
//
// A position is exited when:
//   - The spread has reverted to within ExitZScore of the mean (profit-take), OR
//   - The spread has moved further against us beyond StopLossZScore (stop-loss).
//
// When the returned bool is true, the string indicates the reason:
// either "take_profit" or "stop_loss".
func (s *Strategy) ShouldExit(openSignal types.Signal, currentZScore decimal.Decimal) (bool, string) {
	// 1. Profit-take / Mean-reversion exit.
	// We exit when the spread reverts to the mean or crosses it.
	switch openSignal { //nolint:exhaustive // SignalHold → no exit needed, fall through to stop-loss check
	case types.SignalBuy:
		if currentZScore.Cmp(s.cfg.ExitZScore) <= 0 {
			return true, ExitReasonTakeProfit
		}
	case types.SignalSell:
		if currentZScore.Cmp(s.cfg.ExitZScore.Neg()) >= 0 {
			return true, ExitReasonTakeProfit
		}
	}

	// Stop-loss (if enabled).
	if s.cfg.StopLossZScore.IsPos() {
		switch openSignal {
		case types.SignalBuy:
			// We went long because spread was wide (z > +entry).
			// Stop out if spread widens even further (z grows more positive).
			if currentZScore.Cmp(s.cfg.StopLossZScore) >= 0 {
				s.recordStopLoss(currentZScore, decimal.Zero)
				return true, ExitReasonStopLoss
			}
		case types.SignalSell:
			// We went short because spread was tight (z < -entry).
			// Stop out if spread tightens even further (z grows more negative).
			if currentZScore.Cmp(s.cfg.StopLossZScore.Neg()) <= 0 {
				s.recordStopLoss(currentZScore, decimal.Zero)
				return true, ExitReasonStopLoss
			}
		default:
			return false, ""
		}
	}

	return false, ""
}

// recordStopLoss stores the z-score and pnl from a stop-loss trigger so
// the HTTP handler can read them via LastStopLossTrigger and emit a
// notification. PnL is not computed at the point of exit decision and is
// left as zero; the handler emits the recorded z-score and a zero pnl.
func (s *Strategy) recordStopLoss(zScore, pnl decimal.Decimal) {
	s.mu.Lock()
	s.lastStopZ = zScore
	s.lastStopPnL = pnl
	s.lastStopTriggered = true
	s.mu.Unlock()
}

// LastStopLossTrigger returns the z-score and pnl recorded by the most
// recent stop-loss trigger from ShouldExit, along with a flag indicating
// whether a stop-loss has fired. Once set, the flag stays set for the
// lifetime of the Strategy; callers should treat the value as
// edge-triggered and reset the strategy to re-arm.
func (s *Strategy) LastStopLossTrigger() (zScore, pnl decimal.Decimal, triggered bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastStopZ, s.lastStopPnL, s.lastStopTriggered
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

// mustParseUUID converts a non-empty DORA asset/order-book ID string
// (which the upstream API hands us as a string) into a uuid.UUID.
// Empty input is treated as uuid.Nil so the live-run path can record
// a decision even if the asset ID lookup failed earlier; the row
// still preserves run_id + seq for forensics.
func mustParseUUID(s string) uuid.UUID {
	if s == "" {
		return uuid.Nil
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil
	}
	return id
}
