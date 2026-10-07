package momentum

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/strategy/window"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// Strategy holds per-bond state for the momentum / trend strategy.
type Strategy struct {
	mu  sync.RWMutex
	cfg Config

	fastWin *window.Rolling
	slowWin *window.Rolling
	atrWin  *window.Rolling
	// volWin is the rolling mean-volume window behind the entry
	// volume gate. nil when VolumeAvgWindow == 0 (gate disabled).
	volWin *window.Rolling

	// lastPrice is the previous bar's close (the TrueRange seed).
	// Zero until the first bar. NOT updated by ticks — ticks only
	// drive intrabar exits.
	lastPrice decimal.Decimal

	// sourceSign applies the bond-specific direction mapping: +1 for
	// price, -1 for ytm/spread (yield up = price down).
	sourceSign decimal.Decimal

	// marketAPIClient is used by lookupAssetID to resolve an order
	// book's asset ID. Lazily set by the live run loop; nil for
	// backtests and signal-only callers.
	marketAPIClient strategy.MarketAPIClient

	// candleFeed streams closed bars for the live run loop. Required
	// for Run: the loop fails fast when it is nil.
	candleFeed strategy.CandleFeed

	// baseAssetID is the order book's BASE ASSET UUID — distinct from
	// the order book ID. Resolved once per run/backtest via
	// lookupAssetID and stamped onto every Decision as bondID.
	// Protected by mu.
	baseAssetID string

	// collateralWeight is the collateral weight of the base asset
	// fetched from DORA during the live run. Defaults to 1.0 in
	// backtests and signal-only callers.
	collateralWeight decimal.Decimal

	// candleStore / benchmarkClient are the historical data surfaces
	// used by getBars (spread mode only for the FRED client).
	// Defined in historical_data.go.
	candleStore candleHistoryStore
	// priceHistoryStore reads tick history (price_history) for backtest
	// tick replay. nil self-wires from DATABASE_URL (best-effort).
	priceHistoryStore priceHistorySource
	benchmarkClient   benchmarkYieldClient

	// benchmarkObservations caches FRED yields for spread mode.
	benchmarkObservations []fred.Observation

	// backtestWriter receives per-trade rows from the backtester.
	// nil skips persistence.
	backtestWriter stats.BacktestTradeWriter

	// Live-run state.
	log                  *slog.Logger
	runID                uuid.UUID
	cancel               context.CancelFunc
	isRunning            bool
	paused               bool
	pricesReqID          uuid.UUID
	pricesHandler        *prices.Handler
	balancesInitialized  bool
	bondQty              decimal.Decimal
	usdBal               decimal.Decimal
	lastBenchmarkFetchAt time.Time
	openSignal           types.Signal
	entryPrice           decimal.Decimal
	entryATR             decimal.Decimal
	decisionStore        strategy.DecisionRecorder
	decisionSeq          int64
	errs                 []error
}

// New creates a Strategy with the given Config, prices handler, and
// optional options. The market API client is initialised from the
// DORA_API_KEY environment variable by default; callers that need a
// per-user key should use WithMarketAPIClient.
func New(cfg Config, pricesHandler *prices.Handler, opts ...func(*Strategy)) *Strategy {
	if cfg.Leverage.IsZero() {
		cfg.Leverage = decimal.One
	}
	s := &Strategy{
		cfg:              cfg,
		fastWin:          window.NewRollingWindow(cfg.FastWindow),
		slowWin:          window.NewRollingWindow(cfg.SlowWindow),
		atrWin:           window.NewRollingWindow(cfg.ATRWindow),
		sourceSign:       sourceSign(cfg.SignalSource),
		collateralWeight: decimal.One,
		pricesHandler:    pricesHandler,
		marketAPIClient:  strategy.NewDoraClientWithKey(os.Getenv("DORA_API_KEY")),
		errs:             make([]error, 0),
	}
	if cfg.VolumeAvgWindow > 0 {
		// window.NewRollingWindow clamps sizes below 2, so an explicit
		// 0 (gate disabled) must be represented by a nil window.
		s.volWin = window.NewRollingWindow(cfg.VolumeAvgWindow)
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func sourceSign(source string) decimal.Decimal {
	if source == SignalSourcePrice {
		return decimal.One
	}
	return decimal.MustNew(-1, 0) // ytm, spread: inverted
}

// WithLogger sets the logger on a momentum Strategy.
func WithLogger(log *slog.Logger) func(*Strategy) {
	return func(s *Strategy) { s.log = log }
}

// WithCandleFeed sets the bar source used by the live run loop. Required
// for Run: the loop fails fast when no feed is configured.
func WithCandleFeed(f strategy.CandleFeed) func(*Strategy) {
	return func(s *Strategy) { s.candleFeed = f }
}

// WithCandleHistoryStore sets the candle history store used by backtests.
// When unset, the strategy self-wires from DATABASE_URL.
func WithCandleHistoryStore(store candleHistoryStore) func(*Strategy) {
	return func(s *Strategy) { s.candleStore = store }
}

// WithPriceHistoryStore sets the tick history source used by backtest
// tick replay. When unset, the strategy self-wires from DATABASE_URL.
func WithPriceHistoryStore(store priceHistorySource) func(*Strategy) {
	return func(s *Strategy) {
		s.priceHistoryStore = store
	}
}

// WithMarketAPIClient sets the market API client on a momentum Strategy.
// Use this to inject a client that authenticates with a specific API
// key instead of the default client that reads DORA_API_KEY.
func WithMarketAPIClient(c strategy.MarketAPIClient) func(*Strategy) {
	return func(s *Strategy) { s.marketAPIClient = c }
}

// WithDecisionStore sets the recorder invoked after every successful
// CreateMarketOrder in the live run loop.
func WithDecisionStore(store strategy.DecisionRecorder) func(*Strategy) {
	return func(s *Strategy) { s.decisionStore = store }
}

// WithBacktestWriter sets the destination for per-trade rows the
// backtester emits during a backtest. If unset, trade rows are not
// persisted and the /trades endpoints return empty.
func WithBacktestWriter(w stats.BacktestTradeWriter) func(*Strategy) {
	return func(s *Strategy) { s.backtestWriter = w }
}

// SetDecisionSeq seeds the in-memory decision counter. Called once at
// strategy start (after a server restart, after a resumed run) so the
// counter resumes past the DB frontier.
func (s *Strategy) SetDecisionSeq(seq int64) {
	s.mu.Lock()
	s.decisionSeq = seq
	s.mu.Unlock()
}

// DecisionSeqForTest returns the in-memory decision sequence counter.
// resumePersistedRun's SetDecisionSeq switch must seed this from
// MaxSeq so the first post-restart decision doesn't collide on
// strategy_decisions.
func (s *Strategy) DecisionSeqForTest() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.decisionSeq
}

// logger returns a non-nil logger.
func (s *Strategy) logger() *slog.Logger {
	if s.log != nil {
		return s.log
	}
	return slog.Default()
}

// cappedOrderQuantity computes the order quantity for a given position
// fraction. Applies MinOrderSize / MaxOrderSize (0 disables each).
// Returns ok=false to skip opening when the quantity is below the
// minimum or when the budget/price is zero.  Fractional bonds are
// allowed (no floor) — Dora is a fractionalized market.
func (s *Strategy) cappedOrderQuantity(positionSize, currentPosition, price decimal.Decimal) (decimal.Decimal, bool, error) {
	if positionSize.IsNeg() {
		return decimal.Zero, false, errors.New("position size must be non-negative")
	}
	if price.IsZero() {
		return decimal.Zero, false, errors.New("price is zero")
	}

	effectiveCapital, err := s.cfg.InitialBalance.Mul(s.collateralWeight)
	if err != nil {
		return decimal.Zero, false, err
	}
	effectiveCapital, err = effectiveCapital.Mul(s.cfg.Leverage)
	if err != nil {
		return decimal.Zero, false, err
	}
	budget, err := effectiveCapital.Mul(positionSize)
	if err != nil {
		return decimal.Zero, false, err
	}
	if !budget.IsPos() {
		return decimal.Zero, false, nil
	}

	positionValue, err := currentPosition.Mul(price)
	if err != nil {
		return decimal.Zero, false, err
	}
	if positionValue.IsNeg() {
		positionValue = decimal.Zero
	}
	if positionValue.Cmp(effectiveCapital) >= 0 {
		return decimal.Zero, false, nil
	}
	remainingBudget, err := effectiveCapital.Sub(positionValue)
	if err != nil {
		return decimal.Zero, false, err
	}
	if budget.Cmp(remainingBudget) > 0 {
		budget = remainingBudget
	}

	quantity, err := budget.Quo(price)
	if err != nil {
		return decimal.Zero, false, err
	}
	if s.cfg.MaxOrderSize.IsPos() && quantity.Cmp(s.cfg.MaxOrderSize) > 0 {
		quantity = s.cfg.MaxOrderSize
	}
	if s.cfg.MinOrderSize.IsPos() && quantity.Cmp(s.cfg.MinOrderSize) < 0 {
		return decimal.Zero, false, nil
	}
	if quantity.IsZero() || quantity.IsNeg() {
		return decimal.Zero, false, nil
	}
	return quantity, true, nil
}

// lookupAssetID resolves an order-book UUID to its underlying asset ID.
func (s *Strategy) lookupAssetID(ctx context.Context, orderBookID uuid.UUID) (string, error) {
	return strategy.LookupAssetID(ctx, s.marketAPIClient, orderBookID)
}

// Backtest is the strategy.Strategy entry point for a backtest run.
// Validates the date range, loads the bar window (with slow-window
// warmup) from candle history, and forwards to the backtester.
func (s *Strategy) Backtest(ctx context.Context, start, end time.Time) (types.BacktestResult, error) {
	if end.UTC().Before(start.UTC()) {
		return BacktestResult{}, errors.New("end date must be after start date")
	}
	now := time.Now().UTC()
	if start.UTC().After(now) || end.UTC().After(now) {
		return BacktestResult{}, errors.New("start and end date must be in the past")
	}
	bars, err := s.getBars(ctx, start, end)
	if err != nil {
		return nil, err
	}
	assetID, err := s.lookupAssetID(ctx, s.cfg.OrderBookID)
	if err != nil {
		return BacktestResult{}, fmt.Errorf("backtest requires the order book's base asset: %w", err)
	}
	s.mu.Lock()
	s.baseAssetID = assetID
	s.mu.Unlock()
	bt := NewBacktester(s, s.backtestWriter)
	bt.ticks = s.loadTicks(ctx, assetID, bars)
	return bt.Run(ctx, bars)
}

// NewExitDecision builds a Decision carrying only the fields ShouldExit
// inspects (signal + price). Exported so the external test package can
// construct synthetic decisions for unit tests of ShouldExit.
func NewExitDecision(signal types.Signal, price decimal.Decimal) Decision {
	return Decision{signal: signal, price: price}
}

// bandExit evaluates the entry-anchored stop-loss / take-profit price
// bands for a position. It is shared by ShouldExit (bar-close decisions)
// and the live run loop's intrabar tick check so both use identical
// thresholds. Returns the exit reason and whether to exit.
func (s *Strategy) bandExit(
	openSignal types.Signal,
	entryPrice, entryATR, price decimal.Decimal,
) (string, bool) {
	cfg := s.cfg

	if cfg.StopLossATR.IsPos() && entryATR.IsPos() {
		stopDist, err := cfg.StopLossATR.Mul(entryATR)
		if err == nil {
			switch openSignal { //nolint:exhaustive // SignalHold means flat — no stop check
			case types.SignalBuy:
				threshold, err := entryPrice.Sub(stopDist)
				if err == nil && price.Cmp(threshold) <= 0 {
					return ExitReasonStopLoss, true
				}
			case types.SignalSell:
				threshold, err := entryPrice.Add(stopDist)
				if err == nil && price.Cmp(threshold) >= 0 {
					return ExitReasonStopLoss, true
				}
			}
		}
	}

	if cfg.TakeProfitATR.IsPos() && entryATR.IsPos() {
		tpDist, err := cfg.TakeProfitATR.Mul(entryATR)
		if err == nil {
			switch openSignal { //nolint:exhaustive // SignalHold means flat — no take-profit check
			case types.SignalBuy:
				threshold, err := entryPrice.Add(tpDist)
				if err == nil && price.Cmp(threshold) >= 0 {
					return ExitReasonTakeProfit, true
				}
			case types.SignalSell:
				threshold, err := entryPrice.Sub(tpDist)
				if err == nil && price.Cmp(threshold) <= 0 {
					return ExitReasonTakeProfit, true
				}
			}
		}
	}
	return "", false
}

// ShouldExit reports whether an open position should close, and why.
// Priority: stop_loss > take_profit > reversal. Stop/TP are price-based
// and entry-anchored (stable for the position's life). Reversal fires
// when the current decision's signal opposes the open position.
func (s *Strategy) ShouldExit(openSignal types.Signal, d Decision, entryPrice, entryATR decimal.Decimal) (bool, string) {
	if reason, exit := s.bandExit(openSignal, entryPrice, entryATR, d.Price()); exit {
		return true, reason
	}
	// Reversal: current signal opposes the open position.
	if openSignal == types.SignalBuy && d.Signal() == types.SignalSell {
		return true, ExitReasonReversal
	}
	if openSignal == types.SignalSell && d.Signal() == types.SignalBuy {
		return true, ExitReasonReversal
	}
	return false, ""
}

// seriesValue selects the configured series value from a closed bar.
// ok is false when the bar must be dropped entirely (zero close YTM in
// ytm/spread modes). price mode never drops.
func (s *Strategy) seriesValue(bar types.Bar) (decimal.Decimal, bool, error) {
	switch s.cfg.SignalSource {
	case SignalSourcePrice:
		return bar.Close, true, nil
	case SignalSourceYTM:
		if bar.CloseYTM.IsZero() {
			return decimal.Zero, false, nil
		}
		return bar.CloseYTM, true, nil
	default: // spread
		if bar.CloseYTM.IsZero() {
			return decimal.Zero, false, nil
		}
		spread, err := bar.CloseYTM.Sub(bar.BenchmarkYield)
		return spread, true, err
	}
}

// Update ingests one closed bar and returns the resulting Decision.
func (s *Strategy) Update(bar types.Bar) (Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prevClose := s.lastPrice

	value, ok, err := s.seriesValue(bar)
	if err != nil {
		return Decision{}, err
	}
	if !ok {
		// Drop the bar entirely (no window updates, no signal).
		return Decision{
			time: bar.Time, price: bar.Close,
			signal: types.SignalHold, reason: DecisionReasonWarmingUp,
		}, nil
	}

	// ATR = mean true range, seeded by the previous bar's close.
	tr, err := bar.TrueRange(prevClose)
	if err != nil {
		return Decision{}, err
	}
	if err := s.atrWin.Add(tr); err != nil {
		return Decision{}, err
	}
	if err := s.fastWin.Add(value); err != nil {
		return Decision{}, err
	}
	if err := s.slowWin.Add(value); err != nil {
		return Decision{}, err
	}
	if s.volWin != nil {
		if err := s.volWin.Add(bar.Volume); err != nil {
			return Decision{}, err
		}
	}
	s.lastPrice = bar.Close

	fastMA := s.fastWin.Mean()
	slowMA := s.slowWin.Mean()
	atr := s.atrWin.Mean()

	diff, err := fastMA.Sub(slowMA)
	if err != nil {
		return Decision{}, err
	}
	signed, err := diff.Mul(s.sourceSign)
	if err != nil {
		return Decision{}, err
	}

	d := Decision{
		time: bar.Time, price: bar.Close,
		FastMA: fastMA, SlowMA: slowMA, ATR: atr, SeriesValue: value,
		bondID: s.baseAssetID,
		signal: types.SignalHold, reason: DecisionReasonWarmingUp,
		positionSize: s.cfg.MaxPositionSize,
	}

	switch {
	case !s.fastWin.Ready() || !s.slowWin.Ready():
		d.reason = DecisionReasonWarmingUp
	case signed.Cmp(decimal.Zero) == 0:
		d.reason = DecisionReasonFlat
	case signed.Cmp(decimal.Zero) > 0:
		d.signal = types.SignalBuy
		d.Trend = "up"
		d.reason = DecisionReasonMACrossoverUp
	default:
		d.signal = types.SignalSell
		d.Trend = "down"
		d.reason = DecisionReasonMACrossoverDown
	}

	// Volume gate: a directional signal must be confirmed by bar volume
	// >= VolumeRatioThreshold × the rolling mean volume. Gate disabled
	// (nil window) or not yet ready allows the entry, mirroring the
	// meanreversion imbalance gate's pass-through semantics.
	if d.signal != types.SignalHold && s.volWin != nil && s.volWin.Ready() {
		threshold, err := s.cfg.VolumeRatioThreshold.Mul(s.volWin.Mean())
		if err == nil && bar.Volume.Cmp(threshold) < 0 {
			d.signal = types.SignalHold
			d.Trend = ""
			d.reason = DecisionReasonVolumeNotConfirmed
		} else if err != nil {
			s.logger().Warn("volume gate comparison failed, allowing entry", "err", err)
		}
	}
	return d, nil
}

// Run is the strategy.Strategy entry point for a live run. It
// subscribes to prices, runs the per-tick loop, and respects Pause /
// Resume / Stop control messages.
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

func (s *Strategy) initializeBalances(ctx context.Context, baseAssetID string) {
	quoteAssetID, err := s.marketAPIClient.QuoteAssetID(ctx, s.cfg.OrderBookID.String())
	if err != nil {
		s.recordErr(fmt.Errorf("initialise balances: get quote asset: %w", err))
		return
	}

	portfolio, err := s.marketAPIClient.GetPortfolioV2(ctx)
	if err == nil && portfolio != nil {
		if initializeBalancesFromPortfolio(s, portfolio, baseAssetID, quoteAssetID, false, s.logger()) {
			s.mu.Lock()
			s.balancesInitialized = true
			s.mu.Unlock()
			return
		}
		// Adapter did not converge (parse failure or no matching
		// account): fall through to the legacy AssetPosition path so
		// tracked state reflects the real position instead of staying
		// zero while flagged initialized.
	}
	if err != nil {
		s.logger().Warn("initialise balances: v2 portfolio unavailable, falling back to legacy path", "err", err)
	}

	bondAvailable, bondBorrowed, err := s.marketAPIClient.AssetPosition(ctx, baseAssetID)
	if err != nil {
		s.recordErr(fmt.Errorf("initialise balances: get bond position: %w", err))
	} else {
		s.mu.Lock()
		if !bondBorrowed.IsZero() {
			s.bondQty = bondBorrowed.Neg()
		} else {
			s.bondQty = bondAvailable
		}
		s.mu.Unlock()
	}

	usdAvailable, _, err := s.marketAPIClient.AssetPosition(ctx, quoteAssetID)
	if err != nil {
		s.recordErr(fmt.Errorf("initialise balances: get USD balance: %w", err))
	} else {
		s.mu.Lock()
		s.usdBal = usdAvailable
		// Always sync, including zero: an unfunded account must not
		// keep sizing orders off the config default (synthetic
		// capital).
		s.cfg.InitialBalance = usdAvailable
		s.mu.Unlock()
	}

	s.mu.Lock()
	s.balancesInitialized = true
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

// seedResumeAnchor restores an exit anchor for an INHERITED position —
// one that exists when the run starts (server restart with an open
// position, or a run inheriting an exchange position) and therefore has
// a zero entryPrice. Without it entryPrice and entryATR stay zero and
// ShouldExit's stop-loss/take-profit branches — both gated on
// entryATR.IsPos() — never fire, leaving the position protected only by
// opposite-signal reversal. The anchor is approximate: the last bar
// close and the current true-range ATR mean, not the true entry values,
// which trades exact stop placement for having a stop. A real entry
// always sets entryPrice, so the entryPrice.IsZero() guard keeps the
// per-bar invocation in the run loop from re-anchoring (and drifting) a
// live position's stop bands.
// ponytail: approximate re-derivation; upgrade to StateStore-persisted
// anchors (twap/vwap pattern) if exact entry prices ever matter.
func (s *Strategy) seedResumeAnchor() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.openSignal == types.SignalHold || !s.entryPrice.IsZero() || s.entryATR.IsPos() {
		return
	}
	if s.lastPrice.IsZero() || !s.atrWin.Ready() {
		s.logger().Warn("inherited position has no usable price/ATR history; stop-loss and take-profit stay disabled until the next entry",
			"runID", s.runID, "openSignal", s.openSignal)
		return
	}
	s.entryPrice = s.lastPrice
	s.entryATR = s.atrWin.Mean()
	s.logger().Info("seeded approximate entry anchor for inherited position",
		"runID", s.runID, "openSignal", s.openSignal,
		"entryPrice", s.entryPrice, "entryATR", s.entryATR)
}

func (s *Strategy) recordErr(err error) {
	s.mu.Lock()
	s.errs = append(s.errs, err)
	s.mu.Unlock()
	s.logger().Error("strategy error", "err", err)
}

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
		s.mu.Lock()
		if useTracked {
			s.bondQty = decimal.Zero
		}
		s.openSignal = types.SignalHold
		s.entryPrice = decimal.Zero
		s.entryATR = decimal.Zero
		s.mu.Unlock()
		return nil
	}

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

	clientOrderID := strategy.BuildClientOrderID(StrategyType, s.runID)
	if _, err := s.marketAPIClient.CreateMarketOrder(
		ctx, s.cfg.OrderBookID.String(), side, closeQty, inverseLeverage, false, clientOrderID,
	); err != nil {
		if liveQty, liveErr := s.currentPosition(ctx, assetID); liveErr == nil && liveQty.IsZero() {
			s.logger().Info("close order failed but live position is already 0, self-healing", "runID", s.runID)
			s.mu.Lock()
			if useTracked {
				s.bondQty = decimal.Zero
			}
			s.openSignal = types.SignalHold
			s.entryPrice = decimal.Zero
			s.entryATR = decimal.Zero
			s.mu.Unlock()
			return nil
		}
		return err
	}

	closeSignal := types.SignalSell
	if side == doraclient.SIDE_BUY {
		closeSignal = types.SignalBuy
	}
	s.mu.RLock()
	closePrice := s.lastPrice
	s.mu.RUnlock()
	s.recordDecision(ctx, strategy.Decision{
		OrderBookID:        s.cfg.OrderBookID,
		Asset:              strategy.MustParseUUID(assetID),
		Side:               string(side),
		Signal:             closeSignal.String(),
		Quantity:           closeQty,
		Price:              closePrice,
		Leverage:           s.cfg.Leverage,
		InverseLeverage:    inverseLeverage,
		FromGlobalPosition: false,
		Kind:               strategy.DecisionKindClose,
		Reason:             "trend_exit",
		ReasonDetail:       "close: trend reversal or stop/take hit",
		ClientOrderID:      clientOrderID,
	})

	s.mu.Lock()
	if useTracked {
		s.bondQty = decimal.Zero
	}
	s.openSignal = types.SignalHold
	s.entryPrice = decimal.Zero
	s.entryATR = decimal.Zero
	s.mu.Unlock()
	return nil
}

func (s *Strategy) executeDecision(ctx context.Context, decision Decision, assetID string) (bool, error) {
	if decision.Signal() != types.SignalBuy && decision.Signal() != types.SignalSell {
		return false, nil
	}

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

	price := decision.Price()
	if price.IsZero() {
		return false, errors.New("cannot execute decision: price is zero")
	}
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

	fromGlobalPosition := false
	s.logger().Info("opening position",
		"runID", s.runID, "assetID", assetID, "signal", decision.Signal(),
		"side", side, "quantity", quantity, "price", price)
	clientOrderID := strategy.BuildClientOrderID(StrategyType, s.runID)
	if _, err := s.marketAPIClient.CreateMarketOrder(
		ctx, s.cfg.OrderBookID.String(), side, quantity, inverseLeverage, fromGlobalPosition, clientOrderID,
	); err != nil {
		return false, err
	}

	s.recordDecision(ctx, strategy.Decision{
		OrderBookID:        s.cfg.OrderBookID,
		Asset:              strategy.MustParseUUID(assetID),
		Side:               string(side),
		Signal:             decision.Signal().String(),
		Quantity:           quantity,
		Price:              price,
		Leverage:           s.cfg.Leverage,
		InverseLeverage:    inverseLeverage,
		FromGlobalPosition: fromGlobalPosition,
		Kind:               strategy.DecisionKindOpen,
		Reason:             "ma_crossover_entry",
		ReasonDetail: fmt.Sprintf("ma crossover entry: fastMA=%s slowMA=%s signal=%s",
			decision.FastMA.String(), decision.SlowMA.String(), decision.Signal()),
		ClientOrderID: clientOrderID,
	})

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
	// Anchor entry state for ShouldExit.
	s.entryPrice = price
	s.entryATR = decision.ATR
	s.mu.Unlock()
	return true, nil
}

// run is the live loop. Closed bars are the signal source (the candle
// feed bootstraps history back to `since` for a warm start); ticks only
// update intrabar exits. Spread mode resolves the benchmark at bar
// close time.
//
//nolint:funlen // main run loop with setup and teardown
func (s *Strategy) run(ctx context.Context, msgs <-chan strategy.Message, pricesCh <-chan map[uuid.UUID]prices.AssetPrice) error {
	defer func() {
		s.mu.Lock()
		s.isRunning = false
		s.mu.Unlock()
	}()
	defer s.unsubscribePrices()

	assetID, err := s.lookupAssetID(ctx, s.cfg.OrderBookID)
	if err != nil {
		return fmt.Errorf("error looking up asset ID: %w", err)
	}

	s.mu.Lock()
	s.baseAssetID = assetID
	s.mu.Unlock()
	collateralWeight, err := s.marketAPIClient.AssetCollateralWeight(ctx, assetID)
	if err != nil {
		s.logger().Warn("collateral weight lookup failed, defaulting to 1.0", "assetID", assetID, "err", err)
	} else {
		s.mu.Lock()
		s.collateralWeight = collateralWeight
		s.mu.Unlock()
	}

	// Bars are the signal source: the feed bootstraps history back to
	// `since` (warm start, replacing the old prefillWindow) and then
	// streams closed bars.
	if s.candleFeed == nil {
		return fmt.Errorf("candle feed not configured")
	}
	if err := s.requireCandleCoverage(ctx); err != nil {
		return err
	}
	res := strategy.ResolutionDuration(s.cfg.Resolution)
	warmup := time.Duration(s.cfg.SlowWindow+1) * res
	bars, cancelBars, err := s.candleFeed.SubscribeBars(ctx, s.cfg.OrderBookID, s.cfg.Resolution, time.Now().UTC().Add(-warmup))
	if err != nil {
		return fmt.Errorf("failed to subscribe to bars: %w", err)
	}
	defer cancelBars()

	s.logger().Info("initialising balances", "runID", s.runID, "assetID", assetID)
	s.initializeBalances(ctx, assetID)
	s.seedResumeAnchor()

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
		case bar := <-bars:
			// Resolve the benchmark at the bar's close time. This may
			// make a FRED API call, so it runs without holding s.mu.
			if s.cfg.SignalSource == SignalSourceSpread {
				bench, benchOK := s.getBenchmarkYield(ctx, bar.Time.Add(res))
				if !benchOK {
					// No benchmark available (FRED down, empty cache):
					// skip the bar rather than evaluate a spread against
					// zero. The failed fetch is already logged (throttled).
					continue
				}
				bar.BenchmarkYield = bench
			}
			// Snapshot state under the lock; Update() re-acquires s.mu
			// so we must release before calling it (sync.RWMutex is not
			// reentrant). Same pattern as the tick case.
			s.mu.Lock()
			windowReadyBeforeUpdate := s.fastWin.Ready() && s.slowWin.Ready()
			paused := s.paused
			currentOpenSignal := s.openSignal
			entryPrice := s.entryPrice
			entryATR := s.entryATR
			s.mu.Unlock()

			decision, err := s.Update(bar)
			if err != nil {
				s.logger().Error("failed to update strategy", "runID", s.runID, "err", err)
				continue
			}
			// An inherited position anchors its exit bands once the
			// warm-up bars have filled the windows (no-op once the
			// anchor exists or the strategy is flat).
			s.seedResumeAnchor()
			if paused {
				continue
			}

			if currentOpenSignal != types.SignalHold {
				// On the bar that makes a window full, the MAs are
				// still based on incomplete data. Skip exit evaluation
				// on that bar to avoid acting on a stale signal.
				if windowReadyBeforeUpdate {
					if shouldExit, reason := s.ShouldExit(currentOpenSignal, decision, entryPrice, entryATR); shouldExit {
						s.logger().Info("exiting position", "reason", reason, "runID", s.runID)
						if err := s.closePosition(ctx, assetID); err != nil {
							s.logger().Error("failed to close position", "runID", s.runID, "err", err)
							s.recordErr(err)
						}
					}
				}
				continue
			}

			if decision.Signal() == types.SignalHold {
				continue
			}

			if _, err := s.executeDecision(ctx, decision, assetID); err != nil {
				s.logger().Error("failed to execute decision", "runID", s.runID, "err", err)
				s.recordErr(err)
			}
		case pxs := <-pricesCh:
			// Ticks no longer drive entries or the rolling windows; they
			// only provide intrabar stop-loss / take-profit exits.
			for _, px := range pxs {
				if px.AssetID != assetID {
					continue
				}
				s.mu.RLock()
				open := s.openSignal
				paused := s.paused
				entryPrice := s.entryPrice
				entryATR := s.entryATR
				s.mu.RUnlock()
				if paused || open == types.SignalHold {
					// Paused means hands-off: take no action on ticks,
					// mirroring the bar case.
					continue
				}
				if reason, exit := s.bandExit(open, entryPrice, entryATR, px.Price); exit {
					s.logger().Info("exiting position intra-bar", "reason", reason, "runID", s.runID)
					if err := s.closePosition(ctx, assetID); err != nil {
						s.logger().Error("failed to close position", "runID", s.runID, "err", err)
						s.recordErr(err)
					}
				}
			}
		case <-ticker.C:
			_ = struct{}{} // ticker keeps the select responsive when no ticks arrive
		}
	}
}

// getBenchmarkYield returns the FRED benchmark yield for the configured
// tenor. Returns the cached yield if the cache has data for today
// (UTC), otherwise fetches a fresh window from FRED and merges it in.
// ok=false means no benchmark is available at all (FRED unreachable and
// empty cache) — callers must skip the tick rather than evaluate a
// spread against zero. Spread mode only.
func (s *Strategy) getBenchmarkYield(ctx context.Context, ts time.Time) (decimal.Decimal, bool) {
	normedTS := fred.NormalizeDate(ts)

	// stale-cache fallback: on any FRED / parse / client error we return
	// the cached yield rather than decimal.Zero so spread-mode signals
	// keep tracking the most recent known benchmark instead of degrading
	// to raw YTM (which has the inverted sign convention in spread mode).
	cachedYield, cachedOK := s.cachedBenchmarkYield(ts)
	if cachedOK {
		if latestDate, has := s.latestCachedBenchmarkDate(); has && !latestDate.Before(normedTS) {
			return cachedYield, true
		}
	}

	// Per-tick throttle: if the most recent FRED fetch attempt for this
	// benchmark is still fresh (within today UTC), skip the network call.
	// Without this, intraday ticks where FRED has not yet published
	// today's observation refetch per tick, each failure appends to
	// s.errs unboundedly. A 5-minute window keeps the data close to
	// real-time without hammering the upstream.
	const fetchMinInterval = 5 * time.Minute
	s.mu.RLock()
	lastFetchedAt := s.lastBenchmarkFetchAt
	s.mu.RUnlock()
	if !lastFetchedAt.IsZero() && ts.Sub(lastFetchedAt) < fetchMinInterval {
		if cachedOK {
			return cachedYield, true
		}
		return decimal.Zero, false
	}

	tenor, err := fred.ParseBenchmarkTenor(s.cfg.Tenor)
	if err != nil {
		s.recordErr(fmt.Errorf("get benchmark yield: parse tenor: %w", err))
		s.noteBenchmarkFetch(ts)
		if cachedOK {
			return cachedYield, true
		}
		return decimal.Zero, false
	}

	client, err := s.getBenchmarkYieldClient()
	if err != nil {
		s.recordErr(fmt.Errorf("get benchmark yield: get client: %w", err))
		s.noteBenchmarkFetch(ts)
		if cachedOK {
			return cachedYield, true
		}
		return decimal.Zero, false
	}

	start := normedTS.AddDate(0, 0, -10)
	obs, err := client.FetchHistoricalYields(ctx, tenor, start, normedTS)
	s.noteBenchmarkFetch(ts)
	if err != nil {
		s.recordErr(fmt.Errorf("get benchmark yield: fred fetch: %w", err))
		if cachedOK {
			return cachedYield, true
		}
		return decimal.Zero, false
	}
	if len(obs) == 0 {
		// FRED returned no data; fall back to stale cache if we have it.
		if cachedOK {
			return cachedYield, true
		}
		return decimal.Zero, false
	}
	s.mergeBenchmarkObservations(obs)
	fresh, freshOK := s.cachedBenchmarkYield(ts)
	if freshOK {
		return fresh, true
	}
	if cachedOK {
		return cachedYield, true
	}
	return decimal.Zero, false
}

// noteBenchmarkFetch records the timestamp of the most recent FRED fetch
// attempt so getBenchmarkYield can throttle repeated calls within a
// short window. Lock-free read for the throttle check; writes hold
// s.mu for one assignment.
func (s *Strategy) noteBenchmarkFetch(ts time.Time) {
	s.mu.Lock()
	s.lastBenchmarkFetchAt = ts
	s.mu.Unlock()
}

// recordDecision forwards a strategy.Decision row to the configured
// DecisionRecorder, assigning per-run seq. Never returns an error:
// failed persistence is logged and the run continues.
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
			"err", err, "run_id", d.RunID, "seq", d.Seq,
			"side", d.Side, "signal", d.Signal, "quantity", d.Quantity)
	}
}
