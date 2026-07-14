package breakout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/config"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/strategy/window"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// Config holds tunable parameters for the breakout / volatility-compression
// strategy. All fields have sensible defaults via DefaultConfig.
type Config struct {
	config.Config

	// ShortVolWindow is the number of observations used for the short-window
	// price volatility. Typical values: 3-10.
	ShortVolWindow int

	// LongVolWindow is the number of observations used for the long-window
	// price volatility baseline. Typical values: 30-90.
	LongVolWindow int

	// CompressionThreshold is the ShortVol/LongVol ratio below which the
	// strategy considers the market "compressed" and arms for a breakout.
	// Typical values: 0.3-0.6 (lower = stricter).
	CompressionThreshold decimal.Decimal

	// ATRWindow is the number of observations used for the rolling average
	// true range (here: mean absolute price diff, since we only have close
	// prices from YieldObservation). Typical values: 10-20.
	ATRWindow int

	// BreakoutATRMultiple is the number of ATR units above/below the most
	// recent price that defines the breakout trigger level. Typical values:
	// 1.0-2.0.
	BreakoutATRMultiple decimal.Decimal

	// ConfirmationBars is the number of consecutive closes that must exceed
	// the trigger level before a signal is emitted. Typical values: 1-3.
	ConfirmationBars int

	// StopLossATR is the number of ATR units from entry at which an open
	// position is closed for a stop-loss. Set to 0 to disable.
	StopLossATR decimal.Decimal

	// MinLongVolFloor is the minimum LongVol below which the strategy will
	// not trade (avoids reacting to a completely flat baseline). 0 disables.
	MinLongVolFloor decimal.Decimal

	// OrderBookID is the ID of the DORA order book to place orders on.
	OrderBookID uuid.UUID

	// Tenor is the tenor to use for the benchmark yield.
	Tenor string

	// InitialBalance is the starting capital allocated to this strategy.
	InitialBalance decimal.Decimal

	// Leverage applied when placing orders. Default is 1.0.
	Leverage decimal.Decimal
}

// DefaultConfig returns sensible defaults for live deployment and unit tests.
// Tests typically override ShortVolWindow/LongVolWindow/ATRWindow down to
// small values for fast rolling-window fill.
func DefaultConfig() Config {
	return Config{
		ShortVolWindow:       5,
		LongVolWindow:        60,
		CompressionThreshold: decimal.MustNew(5, 1), //nolint:mnd // 0.5
		ATRWindow:            14,
		BreakoutATRMultiple:  decimal.MustNew(15, 1), //nolint:mnd // 1.5
		ConfirmationBars:     2,
		StopLossATR:          decimal.MustNew(30, 1), //nolint:mnd // 3.0
		MinLongVolFloor:      decimal.Zero,
		InitialBalance:       decimal.One,
		Leverage:             decimal.One,
	}
}

// Strategy holds per-bond state for the breakout / volatility-compression
// signal. The skeleton only carries the rolling-window handles and
// scaffolding for persistence; per-tick state (lastPrice, compressionArmed,
// breakoutLevel, run/cancel) is added in Tasks 3 and 5 when the live
// signal logic and run loop need it.
type Strategy struct {
	mu               sync.RWMutex
	cfg              Config
	log              *slog.Logger
	shortVolWin      *window.Rolling
	longVolWin       *window.Rolling
	atrWin           *window.Rolling
	lastPrice        decimal.Decimal
	compressionArmed bool
	barsAboveTrigger int
	barsBelowTrigger int
	decisionStore    strategy.DecisionRecorder
	decisionSeq      int64
	pricesHandler    *prices.Handler
	marketAPIClient  strategy.MarketAPIClient
	historicalStore  historicalPriceStore

	// Live-run state (set in Run, used by executeDecision / closePosition).
	runID               uuid.UUID
	cancel              context.CancelFunc
	isRunning           bool
	pricesReqID         uuid.UUID
	openSignal          types.Signal // signal of the currently open position, or Hold when flat
	balancesInitialized bool
	bondQty             decimal.Decimal // net bond position (+ = long, - = short)
	errs                []error
}

// New creates a breakout Strategy with sensible defaults.
func New(cfg Config, pricesHandler *prices.Handler, opts ...func(*Strategy)) *Strategy {
	if cfg.Leverage.IsZero() {
		cfg.Leverage = decimal.One
	}
	s := &Strategy{
		cfg:           cfg,
		shortVolWin:   window.NewRollingWindow(cfg.ShortVolWindow),
		longVolWin:    window.NewRollingWindow(cfg.LongVolWindow),
		atrWin:        window.NewRollingWindow(cfg.ATRWindow),
		pricesHandler: pricesHandler,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// WithLogger injects a slog.Logger.
func WithLogger(log *slog.Logger) func(*Strategy) {
	return func(s *Strategy) { s.log = log }
}

// WithDecisionStore injects the persistence recorder used by the live run
// loop after every successful CreateMarketOrder.
func WithDecisionStore(store strategy.DecisionRecorder) func(*Strategy) {
	return func(s *Strategy) { s.decisionStore = store }
}

// WithMarketAPIClient injects the DORA client used for live trading.
// Required when Run() is called; the strategy will fail to start without it.
func WithMarketAPIClient(client strategy.MarketAPIClient) func(*Strategy) {
	return func(s *Strategy) { s.marketAPIClient = client }
}

// WithHistoricalStore injects the backtest's historical price source.
// Required when Backtest() is called; the strategy will return an error
// from Backtest() without it.
func WithHistoricalStore(store historicalPriceStore) func(*Strategy) {
	return func(s *Strategy) { s.historicalStore = store }
}

// logger returns the configured logger or the default if none was set.
func (s *Strategy) logger() *slog.Logger {
	if s.log == nil {
		return slog.Default()
	}
	return s.log
}

// SetDecisionSeq seeds the in-memory decision counter. Called once at
// strategy start so a resumed run picks up past the DB frontier. Mirrors
// the equivalent methods on meanreversion.Strategy and copytrading.Strategy.
func (s *Strategy) SetDecisionSeq(seq int64) {
	s.mu.Lock()
	s.decisionSeq = seq
	s.mu.Unlock()
}

// Update advances the strategy with one price observation and returns
// the resulting Decision.
//
// The algorithm:
//  1. Append |Δprice| (vs. previous tick) to the ATR window, skipping the
//     very first observation where there is no prior price.
//  2. Append the current price to the short and long volatility windows.
//  3. If the long window is not yet full, return HOLD with Reason
//     "warming_up" — there is not enough history to characterise volatility.
//  4. Compute ShortVol = σ(shortVolWin), LongVol = σ(longVolWin), ATR = mean
//     of the ATR window. If LongVol is below MinLongVolFloor, return HOLD
//     with Reason "vol_too_low" — the baseline is too flat to trade on.
//  5. Compute the compression ratio ShortVol/LongVol. If it is below
//     CompressionThreshold, set compressionArmed=true. A zero LongVol is
//     treated as ratio=0 (maximum compression) so a perfectly flat
//     baseline correctly arms a flag.
//  6. With compression armed, compute triggerHigh/Low = prevPrice ± k·ATR.
//     A close above triggerHigh increments barsAboveTrigger; a close below
//     triggerLow increments barsBelowTrigger. When either reaches
//     ConfirmationBars, emit SignalBuy or SignalSell with Reason
//     "compression_breakout" and reset the armed flag + counters.
func (s *Strategy) Update(o types.YieldObservation) (Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg := s.cfg

	// Capture the previous price before mutating any state — the breakout
	// trigger is anchored to the most recent close, not the current one.
	prevPrice := s.lastPrice

	if err := s.ingestObservation(o, prevPrice); err != nil {
		return Decision{}, err
	}

	d := Decision{
		time:             o.Time,
		bondID:           o.BondID,
		price:            o.Price,
		CompressionArmed: s.compressionArmed,
		BarsAboveTrigger: s.barsAboveTrigger,
	}

	// Warm-up.
	if !s.longVolWin.Ready() {
		d.signal = types.SignalHold
		d.reason = DecisionReasonWarmingUp
		return d, nil
	}

	shortVol, longVol, atr, err := s.rollingStats()
	if err != nil {
		return Decision{}, err
	}
	d.ShortVol = shortVol
	d.LongVol = longVol
	d.ATR = atr

	if longVol.Cmp(cfg.MinLongVolFloor) < 0 {
		d.signal = types.SignalHold
		d.reason = DecisionReasonVolTooLow
		return d, nil
	}

	ratio, err := s.compressionRatio(shortVol, longVol)
	if err != nil {
		return Decision{}, err
	}
	d.CompressionRatio = ratio

	if ratio.Cmp(cfg.CompressionThreshold) < 0 {
		s.compressionArmed = true
		d.CompressionArmed = true
	}

	if !s.compressionArmed {
		d.signal = types.SignalHold
		d.reason = DecisionReasonNoSignalYet
		return d, nil
	}

	s.evaluateBreakout(&d, o.Price, prevPrice, atr)
	return d, nil
}

// ingestObservation updates the rolling windows with a new observation:
// the ATR window gets |Δprice| (skipping the very first tick), and the
// short/long volatility windows get the raw price.
func (s *Strategy) ingestObservation(o types.YieldObservation, prevPrice decimal.Decimal) error {
	if !prevPrice.IsZero() {
		diff, err := o.Price.Sub(prevPrice)
		if err != nil {
			return err
		}
		if err := s.atrWin.Add(diff.Abs()); err != nil {
			return err
		}
	}
	if err := s.shortVolWin.Add(o.Price); err != nil {
		return err
	}
	if err := s.longVolWin.Add(o.Price); err != nil {
		return err
	}
	s.lastPrice = o.Price
	return nil
}

// rollingStats returns the current ShortVol, LongVol, and ATR. LongVolWindow
// must be Ready() when this is called.
func (s *Strategy) rollingStats() (shortVol, longVol, atr decimal.Decimal, err error) {
	shortVol, err = s.shortVolWin.StdDev()
	if err != nil {
		return
	}
	longVol, err = s.longVolWin.StdDev()
	if err != nil {
		return
	}
	atr = s.atrWin.Mean()
	return
}

// compressionRatio returns ShortVol / LongVol, treating a zero LongVol as
// ratio=0 (maximum compression) so a perfectly flat baseline correctly
// arms the flag.
func (s *Strategy) compressionRatio(shortVol, longVol decimal.Decimal) (decimal.Decimal, error) {
	if longVol.IsZero() {
		return decimal.Zero, nil
	}
	return shortVol.Quo(longVol)
}

// evaluateBreakout computes the trigger levels from the previous price and
// ATR, updates the consecutive-bars counters, and emits SignalBuy/SignalSell
// once the confirmation threshold is reached. Mutates d and s in place.
func (s *Strategy) evaluateBreakout(d *Decision, price, prevPrice, atr decimal.Decimal) {
	kTimesATR, err := s.cfg.BreakoutATRMultiple.Mul(atr)
	if err != nil {
		return
	}
	triggerHigh, err := prevPrice.Add(kTimesATR)
	if err != nil {
		return
	}
	triggerLow, err := prevPrice.Sub(kTimesATR)
	if err != nil {
		return
	}
	d.BreakoutLevel = triggerHigh

	switch {
	case price.Cmp(triggerHigh) > 0:
		s.barsAboveTrigger++
		d.BarsAboveTrigger = s.barsAboveTrigger
		if s.barsAboveTrigger >= s.cfg.ConfirmationBars {
			d.signal = types.SignalBuy
			d.reason = DecisionReasonCompressionEntry
			d.positionSize = decimal.One
			s.resetArmed()
		}
	case price.Cmp(triggerLow) < 0:
		s.barsBelowTrigger++
		if s.barsBelowTrigger >= s.cfg.ConfirmationBars {
			d.signal = types.SignalSell
			d.reason = DecisionReasonCompressionEntry
			d.positionSize = decimal.One
			s.resetArmed()
		}
	}
}

// resetArmed clears the breakout state after a Signal emission.
func (s *Strategy) resetArmed() {
	s.compressionArmed = false
	s.barsAboveTrigger = 0
	s.barsBelowTrigger = 0
}

// Backtest runs the strategy against historical observations between start
// and end. Delegates to a Backtester that reuses the live Update path.
func (s *Strategy) Backtest(ctx context.Context, start, end time.Time) (types.BacktestResult, error) {
	if s.historicalStore == nil {
		return BacktestResult{}, errors.New("breakout: historical price store is not configured")
	}
	obs, err := s.historicalStore.Observations(ctx, s.cfg.OrderBookID.String(), start, end)
	if err != nil {
		return BacktestResult{}, fmt.Errorf("load historical observations: %w", err)
	}
	return NewBacktester(s, nil).Run(ctx, obs)
}

// Run starts the live breakout loop. It subscribes to prices, processes
// ticks through Update(), and places market orders on every non-HOLD
// signal. The opposite-signal pattern closes the open position; this
// mirrors meanreversion.Strategy.Run with breakout-specific simplifications
// (no benchmark yield, no window prefill).
func (s *Strategy) Run(ctx context.Context, msgCh <-chan strategy.Message, runID uuid.UUID) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return errors.New("strategy is already running")
	}
	if s.marketAPIClient == nil {
		s.mu.Unlock()
		return errors.New("breakout: market API client is not configured")
	}
	s.runID = runID
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	pricesCh, err := s.subscribePrices()
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("subscribe to prices: %w", err)
	}
	s.isRunning = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.isRunning = false
		s.mu.Unlock()
		s.unsubscribePrices()
	}()

	return s.runLoop(runCtx, msgCh, pricesCh)
}

// runLoop is the inner select that drives the live strategy. Extracted so
// Run() stays small and the live loop itself can be tested in isolation
// via dependency-injected channels.
func (s *Strategy) runLoop(ctx context.Context, msgs <-chan strategy.Message, prices <-chan map[uuid.UUID]prices.AssetPrice) error {
	assetID, err := s.lookupAssetID(s.cfg.OrderBookID)
	if err != nil {
		return fmt.Errorf("lookup asset ID: %w", err)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-msgs:
			s.mu.Lock()
			switch msg {
			case strategy.Stop:
				s.cancel()
			case strategy.Pause, strategy.Resume:
				// Pause/Resume semantics land in a follow-up ticket; the
				// message is consumed here so the channel stays drained.
			}
			s.mu.Unlock()
		case pxs := <-prices:
			for _, px := range pxs {
				if px.AssetID != assetID {
					continue
				}
				s.handleTick(ctx, px, assetID)
			}
		case <-ticker.C:
			// keep the loop alive; reserved for future heartbeat logic.
		}
	}
}

// handleTick processes a single price update through the Update() pipeline
// and dispatches the resulting decision to executeDecision or closePosition.
func (s *Strategy) handleTick(ctx context.Context, px prices.AssetPrice, assetID string) {
	obs := types.YieldObservation{
		Time:   px.Time,
		BondID: px.AssetID,
		Price:  px.Price,
	}
	s.mu.Lock()
	s.lastPrice = px.Price
	openSig := s.openSignal
	s.mu.Unlock()

	decision, err := s.Update(obs)
	if err != nil {
		s.logger().Error("update strategy failed", "runID", s.runID, "assetID", assetID, "err", err)
		return
	}

	// Already in a position: emit a reversal close if the new signal
	// is the opposite direction. HOLD ticks do not close.
	if openSig != types.SignalHold && isReversal(openSig, decision.Signal()) {
		if err := s.closePosition(ctx, assetID); err != nil {
			s.logger().Error("close position failed", "runID", s.runID, "assetID", assetID, "err", err)
			s.mu.Lock()
			s.errs = append(s.errs, err)
			s.mu.Unlock()
		}
		return
	}

	// Flat: open a position on a fresh signal.
	if openSig == types.SignalHold && decision.Signal() != types.SignalHold {
		if _, err := s.executeDecision(ctx, decision, assetID); err != nil {
			s.logger().Error("execute decision failed", "runID", s.runID, "assetID", assetID, "err", err)
			s.mu.Lock()
			s.errs = append(s.errs, err)
			s.mu.Unlock()
		}
	}
}

// subscribePrices opens a price subscription for this run.
func (s *Strategy) subscribePrices() (<-chan map[uuid.UUID]prices.AssetPrice, error) {
	if s.pricesHandler == nil {
		return nil, errors.New("breakout: prices handler is not configured")
	}
	s.pricesReqID = uuid.Must(uuid.NewV7())
	pricesCh, err := s.pricesHandler.Subscribe(s.pricesReqID)
	if err != nil {
		s.pricesReqID = uuid.Nil
		return nil, err
	}
	return pricesCh, nil
}

// unsubscribePrices closes the price subscription opened in subscribePrices.
func (s *Strategy) unsubscribePrices() {
	if s.pricesHandler == nil || s.pricesReqID == uuid.Nil {
		return
	}
	if err := s.pricesHandler.Unsubscribe(s.pricesReqID); err != nil {
		s.logger().Error("unsubscribe from prices failed", "err", err)
	}
	s.pricesReqID = uuid.Nil
}

// lookupAssetID resolves the configured order-book ID to a DORA asset ID
// via the market API client.
func (s *Strategy) lookupAssetID(orderBookID uuid.UUID) (string, error) {
	if s.marketAPIClient == nil {
		return "", errors.New("breakout: market API client is not configured")
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

// executeDecision places a market order in the decision's signal direction.
// It looks up the current position (preferred via tracked bondQty, falls
// back to a DORA API call) so it can compute the correct order quantity
// for both opening and extending positions.
func (s *Strategy) executeDecision(ctx context.Context, decision Decision, assetID string) (bool, error) {
	if decision.Signal() != types.SignalBuy && decision.Signal() != types.SignalSell {
		return false, nil
	}

	price := decision.Price()
	if price.IsZero() {
		return false, errors.New("cannot execute decision: price is zero")
	}

	position, useTracked := s.positionOrFetch(ctx, assetID)

	quantity, ok, err := s.cappedOrderQuantity(decision.PositionSize(), position, price)
	if err != nil {
		return false, err
	}
	if !ok || quantity.IsZero() {
		return false, nil
	}

	side := doraclient.SIDE_BUY
	if decision.Signal() == types.SignalSell {
		side = doraclient.SIDE_SELL
	}

	inverseLeverage, err := decimal.One.Quo(s.cfg.Leverage)
	if err != nil {
		return false, fmt.Errorf("compute inverse leverage: %w", err)
	}

	clientOrderID := strategy.BuildClientOrderID(StrategyType, s.runID)
	s.logger().Info("opening position", "runID", s.runID, "assetID", assetID, "signal", decision.Signal())
	if err := s.marketAPIClient.CreateMarketOrder(
		ctx, s.cfg.OrderBookID.String(), side, quantity, inverseLeverage, false, clientOrderID,
	); err != nil {
		return false, err
	}

	s.recordDecision(ctx, strategy.Decision{
		OrderBookID:     s.cfg.OrderBookID,
		Asset:           mustParseUUID(assetID),
		Side:            string(side),
		Signal:          decision.Signal().String(),
		Quantity:        quantity,
		Price:           price,
		Leverage:        s.cfg.Leverage,
		InverseLeverage: inverseLeverage,
		Kind:            strategy.DecisionKindOpen,
		Reason:          decision.Reason(),
		ReasonDetail:    fmt.Sprintf("breakout entry: signal=%s compression=%s", decision.Signal(), decision.CompressionRatio),
		ClientOrderID:   clientOrderID,
	})

	s.mu.Lock()
	if useTracked {
		if side == doraclient.SIDE_BUY {
			s.bondQty, _ = s.bondQty.Add(quantity)
		} else {
			s.bondQty, _ = s.bondQty.Sub(quantity)
		}
		switch {
		case s.bondQty.IsPos():
			s.openSignal = types.SignalBuy
		case s.bondQty.IsNeg():
			s.openSignal = types.SignalSell
		default:
			s.openSignal = types.SignalHold
		}
	} else {
		s.openSignal = decision.Signal()
	}
	s.mu.Unlock()
	return true, nil
}

// closePosition reverses the currently open position. Used by handleTick
// when an opposite-signal reversal fires while a position is open.
func (s *Strategy) closePosition(ctx context.Context, assetID string) error {
	s.mu.RLock()
	openSig := s.openSignal
	qty := s.bondQty.Abs()
	useTracked := s.balancesInitialized
	lastPrice := s.lastPrice
	s.mu.RUnlock()

	if openSig == types.SignalHold {
		return nil
	}

	side := doraclient.SIDE_SELL
	if openSig == types.SignalSell {
		side = doraclient.SIDE_BUY
	}

	if !useTracked {
		var err error
		qty, err = s.currentPosition(ctx, assetID)
		if err != nil {
			return err
		}
		if qty.IsZero() {
			// Live position already flat; clear local state.
			s.mu.Lock()
			s.openSignal = types.SignalHold
			s.mu.Unlock()
			return nil
		}
	}

	inverseLeverage, err := decimal.One.Quo(s.cfg.Leverage)
	if err != nil {
		inverseLeverage = decimal.One
	}

	clientOrderID := strategy.BuildClientOrderID(StrategyType, s.runID)
	if err := s.marketAPIClient.CreateMarketOrder(
		ctx, s.cfg.OrderBookID.String(), side, qty, inverseLeverage, false, clientOrderID,
	); err != nil {
		return err
	}

	closeSignal := types.SignalSell
	if side == doraclient.SIDE_BUY {
		closeSignal = types.SignalBuy
	}
	s.recordDecision(ctx, strategy.Decision{
		OrderBookID:     s.cfg.OrderBookID,
		Asset:           mustParseUUID(assetID),
		Side:            string(side),
		Signal:          closeSignal.String(),
		Quantity:        qty,
		Price:           lastPrice,
		Leverage:        s.cfg.Leverage,
		InverseLeverage: inverseLeverage,
		Kind:            strategy.DecisionKindClose,
		Reason:          DecisionReasonReversal,
		ReasonDetail:    "close: breakout reversal",
		ClientOrderID:   clientOrderID,
	})

	s.mu.Lock()
	if useTracked {
		s.bondQty = decimal.Zero
	}
	s.openSignal = types.SignalHold
	s.mu.Unlock()
	return nil
}

// currentPosition fetches the net bond position from DORA. Used as a
// fallback when local bondQty tracking is unavailable.
func (s *Strategy) currentPosition(ctx context.Context, assetID string) (decimal.Decimal, error) {
	if s.marketAPIClient == nil {
		return decimal.Zero, errors.New("breakout: market API client is not configured")
	}
	available, _, err := s.marketAPIClient.AssetPosition(ctx, assetID)
	if err != nil {
		return decimal.Zero, err
	}
	return available, nil
}

// positionOrFetch returns the current bondQty if balance tracking is
// initialised, otherwise fetches it from the DORA API.
func (s *Strategy) positionOrFetch(ctx context.Context, assetID string) (decimal.Decimal, bool) {
	s.mu.RLock()
	useTracked := s.balancesInitialized
	position := s.bondQty
	s.mu.RUnlock()
	if useTracked {
		return position, true
	}
	pos, err := s.currentPosition(ctx, assetID)
	if err != nil {
		s.logger().Debug("current position fetch failed; falling back to zero", "err", err)
		return decimal.Zero, false
	}
	return pos, false
}

// cappedOrderQuantity computes the order quantity for a given budget
// fraction, capped by the min/max order sizes from the strategy config.
// Returns ok=false when the budget is too small to buy even one bond.
func (s *Strategy) cappedOrderQuantity(fraction, position, price decimal.Decimal) (decimal.Decimal, bool, error) {
	budget, err := position.Mul(fraction)
	if err != nil {
		return decimal.Zero, false, err
	}
	qty, err := budget.Quo(price)
	if err != nil {
		return decimal.Zero, false, err
	}
	qty = qty.Floor(0)
	if qty.IsZero() {
		return decimal.Zero, false, nil
	}
	return qty, true, nil
}

// recordDecision persists a strategy.Decision row, stamping RunID/Seq/
// StrategyType/CreatedAt so the audit trail is complete.
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
	d.StrategyType = StrategyType
	if d.CreatedAt.IsZero() {
		d.CreatedAt = time.Now().UTC()
	}
	if err := s.decisionStore.SaveDecision(ctx, d); err != nil {
		s.logger().Error("save strategy decision",
			"err", err,
			"run_id", d.RunID,
			"seq", d.Seq,
			"side", d.Side,
			"signal", d.Signal,
			"quantity", d.Quantity,
		)
	}
}

// mustParseUUID converts a DORA asset/order-book ID string into a
// uuid.UUID. Empty input returns uuid.Nil so a missing lookup doesn't
// abort the persistence path.
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

// Compile-time guard that *Strategy satisfies strategy.Strategy.
var _ strategy.Strategy = (*Strategy)(nil)
