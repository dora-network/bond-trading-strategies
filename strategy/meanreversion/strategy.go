package meanreversion

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/config"
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

	// Tracked balances, initialised from DORA on Run and updated on trade
	// execution.  Protected by mu.
	balancesInitialized bool
	bondQty             decimal.Decimal // net bond position (+ = long, - = short)
	usdBal              decimal.Decimal // USD available balance

	// openSignal records the signal direction of the currently open position.
	// SignalHold means flat (no position). Derived from bondQty in
	// initializeBalances (so restarts correctly see the pre-existing position)
	// and kept in sync by executeDecision and closePosition. Protected by mu.
	openSignal types.Signal
}

// New creates a new Strategy with the given Config.

func New(cfg Config, pricesHandlers ...*prices.Handler) *Strategy {
	var pricesHandler *prices.Handler
	if len(pricesHandlers) > 0 {
		pricesHandler = pricesHandlers[0]
	}
	if cfg.Leverage.IsZero() {
		cfg.Leverage = decimal.One
	}

	return &Strategy{
		cfg:             cfg,
		window:          window.NewRollingWindow(cfg.LookbackWindow),
		pricesHandler:   pricesHandler,
		marketAPIClient: newDoraClient(),
		errs:            make([]error, 0),
	}
}

func (s *Strategy) Backtest(ctx context.Context, start, end time.Time) (backtestResult types.BacktestResult, err error) {
	if end.UTC().Before(start.UTC()) {
		return backtestResult, fmt.Errorf("end date must be after start date")
	}

	if start.UTC().After(time.Now().UTC()) || end.UTC().After(time.Now().UTC()) {
		return backtestResult, fmt.Errorf("start and end date must be in the past")
	}

	bt := NewBacktester(New(s.cfg, s.pricesHandler))
	obs, err := s.getObservations(ctx, start, end)
	if err != nil {
		return backtestResult, err
	}
	return bt.Run(ctx, obs)
}

func (s *Strategy) Run(ctx context.Context, msgCh <-chan strategy.Message) error {
	s.mu.Lock()

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

	s.run(runCtx, msgCh, pricesCh)
	return nil
}

func (s *Strategy) subscribePrices() (<-chan map[uuid.UUID]prices.AssetPrice, error) {
	s.pricesReqID = uuid.Must(uuid.NewV7())
	pricesCh, err := s.pricesHandler.Subscribe(s.pricesReqID)
	if err != nil {
		return nil, err
	}
	return pricesCh, nil
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
	accountID, err := s.marketAPIClient.SelfUserID(ctx)
	if err != nil {
		return decimal.Zero, err
	}
	long, short, err := s.marketAPIClient.AssetPosition(ctx, accountID, assetID)
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

	// Dollar budget = total capital × fraction of remaining balance.
	budget, err := s.cfg.InitialBalance.Mul(positionSize)
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
	if positionValue.Cmp(s.cfg.InitialBalance) >= 0 {
		// Already fully invested.
		return decimal.Zero, false, nil
	}
	remainingBudget, err := s.cfg.InitialBalance.Sub(positionValue)
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
func (s *Strategy) initializeBalances(ctx context.Context, baseAssetID string) {
	quoteAssetID, err := s.marketAPIClient.QuoteAssetID(ctx, s.cfg.OrderBookID.String())
	if err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, fmt.Errorf("initialise balances: get quote asset: %w", err))
		s.mu.Unlock()
		return
	}
	accountID, err := s.marketAPIClient.SelfUserID(ctx)
	if err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, fmt.Errorf("initialise balances: get user ID: %w", err))
		s.mu.Unlock()
		return
	}

	// Fetch bond position.
	bondAvailable, bondBorrowed, err := s.marketAPIClient.AssetPosition(ctx, accountID, baseAssetID)
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
	usdAvailable, _, err := s.marketAPIClient.AssetPosition(ctx, accountID, quoteAssetID)
	if err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, fmt.Errorf("initialise balances: get USD balance: %w", err))
		s.mu.Unlock()
	} else {
		s.mu.Lock()
		s.usdBal = usdAvailable
		s.mu.Unlock()
	}

	// Mark balances as initialised regardless of partial failures — any fields
	// that failed will stay at zero.
	s.mu.Lock()
	s.balancesInitialized = true
	// Reconstruct the open-position signal from the fetched bond quantity.
	// This ensures that after a server restart the strategy immediately knows
	// it already holds a position and will not attempt to open a duplicate
	// entry on the next price tick: positive qty = prior Buy, negative = prior
	// Sell, zero = flat.
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
// run loop when ShouldExit returns true. bondQty (or, when balance tracking
// is unavailable, a fresh DORA lookup) determines the exact quantity to close.
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

	if err := s.marketAPIClient.CreateMarketOrder(ctx, s.cfg.OrderBookID.String(), side, closeQty, inverseLeverage); err != nil {
		return err
	}

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
	if !ok {
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

	if err := s.marketAPIClient.CreateMarketOrder(ctx, s.cfg.OrderBookID.String(), side, quantity, inverseLeverage); err != nil {
		return false, err
	}

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
func (s *Strategy) run(ctx context.Context, msgs <-chan strategy.Message, prices <-chan map[uuid.UUID]prices.AssetPrice) {
	defer func() {
		s.mu.Lock()
		s.isRunning = false
		s.mu.Unlock()
	}()

	assetID, err := s.lookupAssetID(s.cfg.OrderBookID)
	if err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, fmt.Errorf("error looking up asset ID: %w", err))
		s.mu.Unlock()
		return
	}

	// Pre-fill the rolling window with historical data so that signals can
	// be generated immediately on the first price tick.  This is best-effort
	// — if the historical price store or FRED client are unavailable the
	// strategy will still start with an empty window (the existing behaviour).
	if err := s.prefillWindow(ctx, assetID); err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, fmt.Errorf("prefill window (non-fatal): %w", err))
		s.mu.Unlock()
	}

	// Fetch the user's bond position and USD balance from DORA and track
	// them in-memory for subsequent trade execution, avoiding repeated API
	// calls.  This is best-effort — if the API is unavailable the strategy
	// will look up positions on each trade via the DORA API.
	s.initializeBalances(ctx, assetID)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
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

				s.mu.Lock()
				obs := types.YieldObservation{
					Time:           px.Time,
					BondID:         px.AssetID,
					YTM:            *px.YTM,
					BenchmarkYield: benchmarkYield,
					Price:          px.Price,
				}
				// Read window readiness before Update so the guard and the
				// z-score in decision are computed from the same window state.
				// On the tick that makes the window full, decision.ZScore is
				// still 0 (computed from the incomplete pre-add window), so
				// we must not evaluate exit conditions on that tick.
				windowReadyBeforeUpdate := s.window.Ready()
				decision, err := s.Update(obs)
				if err != nil {
					s.mu.Unlock()
					continue
				}

				if s.paused {
					// strategy is paused, ignore decision
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
						if shouldExit, _ := s.ShouldExit(currentOpenSignal, decision.ZScore); shouldExit {
							if err := s.closePosition(ctx, px.AssetID); err != nil {
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

	// Return the yield from the most recent observation in the response.
	return obs[len(obs)-1].Yield
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
			return true, types.ExitReasonTakeProfit
		}
	case types.SignalSell:
		if currentZScore.Cmp(s.cfg.ExitZScore.Neg()) >= 0 {
			return true, types.ExitReasonTakeProfit
		}
	}

	// Stop-loss (if enabled).
	if s.cfg.StopLossZScore.IsPos() {
		switch openSignal {
		case types.SignalBuy:
			// We went long because spread was wide (z > +entry).
			// Stop out if spread widens even further (z grows more positive).
			if currentZScore.Cmp(s.cfg.StopLossZScore) >= 0 {
				return true, types.ExitReasonStopLoss
			}
		case types.SignalSell:
			// We went short because spread was tight (z < -entry).
			// Stop out if spread tightens even further (z grows more negative).
			if currentZScore.Cmp(s.cfg.StopLossZScore.Neg()) <= 0 {
				return true, types.ExitReasonStopLoss
			}
		default:
			return false, ""
		}
	}

	return false, ""
}
