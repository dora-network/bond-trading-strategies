package breakout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/config"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/strategy/window"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/dora-network/bond-trading-strategies/trades"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// Config holds tunable parameters for the breakout / volatility-compression
// strategy. All fields have sensible defaults via DefaultConfig.
type Config struct {
	config.Config

	// ShortVolWindow is the number of closed bars used for the
	// short-window price volatility (σ of closes). At the default 5m
	// resolution, 12 bars ≈ 1 hour.
	ShortVolWindow int

	// LongVolWindow is the number of closed bars used for the
	// long-window price-volatility baseline (σ of closes). At the
	// default 5m resolution, 96 bars ≈ 8 hours. Must be greater than
	// ShortVolWindow.
	LongVolWindow int

	// CompressionThreshold is the ShortVol/LongVol ratio below which the
	// strategy considers the market "compressed" and arms for a breakout.
	// Typical values: 0.3-0.6 (lower = stricter).
	CompressionThreshold decimal.Decimal
	// ATRWindow is the number of closed bars used for the rolling
	// average true range. At the default 5m resolution, 12 bars ≈ 1
	// hour.
	ATRWindow int

	// BreakoutATRMultiple is the number of ATR units above/below the most
	// recent price that defines the breakout trigger level. Typical values:
	// 1.0-2.0.
	BreakoutATRMultiple decimal.Decimal

	// ConfirmationBars is the number of consecutive bar closes that
	// must exceed the trigger level before a signal is emitted.
	// Typical values: 2-5 (a sustained move rather than a single-bar
	// spike). The test fixtures use 1 to make deterministic
	// single-jump triggers explicit.
	ConfirmationBars int

	// StopLossATR is the number of ATR units from entry at which an open
	// position is closed for a stop-loss. Set to 0 to disable.
	StopLossATR decimal.Decimal

	// TakeProfitATR is the number of ATR units from entry at which an open
	// position is closed for a take-profit. Set to 0 to disable.
	TakeProfitATR decimal.Decimal

	// MinLongVolFloor is the minimum LongVol below which the strategy will
	// not trade (avoids reacting to a completely flat baseline). 0 disables.
	MinLongVolFloor decimal.Decimal

	// OBVTrendThreshold is the absolute OBV threshold the breakout
	// signal must clear when OBVWindow > 0. The
	// direction must match: BUY requires OBV > OBVTrendThreshold, SELL
	// requires OBV < -OBVTrendThreshold. Default 0 means any non-zero
	// OBV in the right direction is enough.
	OBVTrendThreshold decimal.Decimal

	// OBVWindow is the number of recent trades to include in the
	// windowed On-Balance Volume used by the volume confirmation
	// filter. OBVWindow = 0 disables the filter ("do not need to
	// verify volume"); OBVWindow > 0 verifies with the windowed OBV
	// (sum of signed quantities in the last OBVWindow trades).
	// Cumulative OBV (the full trade history) is not supported
	// because it reflects long-term positioning rather than recent
	// volume shifts, which gave misleading signals in testing.
	// Recommended: match ShortVolWindow so the volume check uses
	// the same recent-trades scope as the volatility check.
	// OBVWindow is the number of recent trades to include in the
	// windowed On-Balance Volume used by the volume confirmation
	// filter. OBVWindow = 0 disables the filter ("do not need to
	// verify volume"); OBVWindow > 0 verifies with the windowed OBV
	// (sum of signed quantities in the last OBVWindow trades).
	// Cumulative OBV (the full trade history) is not supported
	// because it reflects long-term positioning rather than recent
	// volume shifts, which gave misleading signals in testing.
	OBVWindow int

	// Resolution is the candle resolution the strategy subscribes to.
	// Defaults to 5m.
	Resolution candles.Resolution

	// OrderBookID is the ID of the DORA order book to place orders on.
	OrderBookID uuid.UUID

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
		// Bar defaults at 5m resolution: short ≈ 1h, long ≈ 8h, ATR ≈ 1h.
		ShortVolWindow:       12,
		LongVolWindow:        96,
		CompressionThreshold: decimal.MustNew(3, 1), //nolint:mnd // 0.3
		ATRWindow:            12,
		BreakoutATRMultiple:  decimal.MustNew(15, 1), //nolint:mnd // 1.5
		ConfirmationBars:     3,
		StopLossATR:          decimal.MustNew(20, 0), //nolint:mnd // 20x — wider stop for higher-volatility regimes
		TakeProfitATR:        decimal.Zero,           // disabled by default
		MinLongVolFloor:      decimal.Zero,
		OBVTrendThreshold:    decimal.Zero, // disabled by default
		Resolution:           candles.Resolution5m,
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
	mu                    sync.RWMutex
	cfg                   Config
	log                   *slog.Logger
	shortVolWin           *window.Rolling
	longVolWin            *window.Rolling
	atrWin                *window.Rolling
	lastPrice             decimal.Decimal
	compressionArmed      bool
	armedCompressionRatio decimal.Decimal
	barsAboveTrigger      int
	barsBelowTrigger      int
	decisionStore         strategy.DecisionRecorder
	decisionSeq           int64
	pricesHandler         *prices.Handler
	marketAPIClient       strategy.MarketAPIClient
	candleFeed            strategy.CandleFeed
	candleStore           candleHistoryStore
	// priceHistoryStore reads tick history (price_history) for backtest
	// tick replay. nil self-wires from DATABASE_URL (best-effort).
	priceHistoryStore priceHistorySource
	backtestWriter    stats.BacktestTradeWriter
	tradeStream       *streams.TradeStream
	tradeHistoryStore trades.TradeStore

	// baseAssetID is the order book's BASE ASSET UUID — distinct from
	// the order book ID. Resolved once per run/backtest via
	// lookupAssetID and stamped onto every Decision as bondID.
	// Protected by mu.
	baseAssetID string

	// Live-run state (set in Run, used by executeDecision / closePosition).
	runID               uuid.UUID
	cancel              context.CancelFunc
	isRunning           bool
	pricesReqID         uuid.UUID
	openSignal          types.Signal // signal of the currently open position, or Hold when flat
	balancesInitialized bool
	bondQty             decimal.Decimal // net bond position (+ = long, - = short)
	// entryPrice and entryATR are captured when executeDecision opens a
	// position. closePosition uses them to evaluate stop-loss / take-profit
	// on every tick before checking the opposite-signal reversal.
	entryPrice decimal.Decimal
	entryATR   decimal.Decimal
	// paused is flipped to true when the strategy.Message channel
	// receives strategy.Pause. While set, the run loop drops price
	// updates (no Update / no entry / no SL/TP close) and writes a debug
	// log. strategy.Resume clears it.
	paused bool
	// OBV accumulator (live run only; populated from the trade stream).
	// obvMu serialises updates from the trade-stream goroutine against
	// reads from Update's signal-gate path so they don't fight over
	// the Strategy's main mu.
	//
	// obvSum is the running cumulative OBV (used when OBVWindow == 0).
	// obvWindow is a rolling window of signed (BUY=+, SELL=-)
	// quantities for windowed OBV (used when OBVWindow > 0). The
	// sum of obvWindow gives the running windowed OBV. Reuses the
	// window.Rolling type already used by the volatility windows.
	obvMu        sync.RWMutex
	obvSum       decimal.Decimal // running cumulative OBV
	obvPrevPrice decimal.Decimal // last seen price (cumulative fallback only)
	obvWindow    *window.Rolling // rolling window of signed quantities
	tradeSubID   uuid.UUID
	errs         []error
}

// New creates a breakout Strategy with sensible defaults.
func New(cfg Config, pricesHandler *prices.Handler, opts ...func(*Strategy)) *Strategy {
	if cfg.Leverage.IsZero() {
		cfg.Leverage = decimal.One
	}
	s := &Strategy{
		cfg:             cfg,
		shortVolWin:     window.NewRollingWindow(cfg.ShortVolWindow),
		longVolWin:      window.NewRollingWindow(cfg.LongVolWindow),
		atrWin:          window.NewRollingWindow(cfg.ATRWindow),
		marketAPIClient: strategy.NewDoraClientWithKey(os.Getenv("DORA_API_KEY")),
	}
	// Initialize the OBV ring buffer when windowed mode is requested.
	// A nil obvWindow means cumulative OBV (price-comparison). A
	// non-nil obvWindow means windowed OBV (side-based).
	if cfg.OBVWindow > 0 {
		s.obvWindow = window.NewRollingWindow(cfg.OBVWindow)
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
	return func(s *Strategy) { s.priceHistoryStore = store }
}

// WithTradeStream injects the live trade stream. When set, the Run
// loop subscribes to the configured order book and accumulates OBV
// from the trade events. Required only when OBVWindow > 0 (windowed
// volume confirmation); otherwise the stream is ignored.
func WithTradeStream(ts *streams.TradeStream) func(*Strategy) {
	return func(s *Strategy) { s.tradeStream = ts }
}

// WithBacktestWriter injects the destination for per-trade rows the
// backtest emits (one WriteTradeRecord/WriteClosedTrade call per row).
// Pass nil to skip persistence.
func WithBacktestWriter(w stats.BacktestTradeWriter) func(*Strategy) {
	return func(s *Strategy) { s.backtestWriter = w }
}

// WithTradeHistoryStore injects the historical trade source used by the
// backtester to accumulate OBV for the volume confirmation filter
// (active when OBVWindow > 0). The backtester loads trades from the
// store and interleaves them with the observation stream by timestamp
// so OBV is correct at every signal point. When OBVWindow == 0 the
// store is ignored.
func WithTradeHistoryStore(store trades.TradeStore) func(*Strategy) {
	return func(s *Strategy) { s.tradeHistoryStore = store }
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

// IsPaused reports whether the live run is currently paused (a Pause
// message has been received and not yet matched by a Resume). The
// handler's stopLossObserver goroutine uses this to skip emitting
// stop-loss events for paused strategies; meanreversion's observer
// already gates on its own s.paused flag.
func (s *Strategy) IsPaused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.paused
}

// OBV returns the current On-Balance Volume. When obvWindow is set
// (windowed mode), returns the sum of the rolling window of signed
// quantities. When obvWindow is nil (cumulative mode), returns the
// running cumulative OBV from the start of the trade history.
// Used by the live-run signal gate and by tests; nil-safe (returns
// decimal.Zero when no trades have been observed yet).
func (s *Strategy) OBV() decimal.Decimal {
	s.obvMu.RLock()
	defer s.obvMu.RUnlock()
	if s.obvWindow != nil {
		return s.obvWindow.Sum()
	}
	return s.obvSum
}

// applyTradeEvent integrates a single trade into the running OBV.
// When obvWindow is set (windowed mode), pushes the signed quantity
// (BUY=+, SELL=-) into the rolling window and evicts the oldest
// entry. When obvWindow is nil (cumulative mode), accumulates
// the signed quantity into obvSum; falls back to price-comparison
// OBV (Granville's algorithm) when Side is missing or unrecognized.
// Called from the trade-stream reader in Run and from the backtester
// via ingestTradesUpTo.
func (s *Strategy) applyTradeEvent(ev streams.TradeEvent) {
	s.obvMu.Lock()
	defer s.obvMu.Unlock()

	// Windowed mode: push signed quantity into the rolling buffer.
	// No price-comparison fallback — windowed OBV always uses
	// side-based sign. Trades with missing/unrecognized Side are
	// skipped (no price-comparison context in a window).
	if s.obvWindow != nil {
		switch ev.Side {
		case "BUY":
			_ = s.obvWindow.Add(ev.Quantity)
		case "SELL":
			_ = s.obvWindow.Add(ev.Quantity.Neg())
		}
		return
	}

	// Cumulative mode: accumulate into obvSum.
	var delta decimal.Decimal
	switch ev.Side {
	case "BUY":
		delta = ev.Quantity
	case "SELL":
		delta = ev.Quantity.Neg()
	default:
		// Fallback: price-comparison OBV (Granville's algorithm) for
		// trades with missing or unrecognized side. First trade ever
		// counts full quantity; subsequent trades use the price diff.
		if s.obvPrevPrice.IsZero() {
			delta = ev.Quantity
		} else {
			switch ev.Price.Cmp(s.obvPrevPrice) {
			case 1:
				delta = ev.Quantity
			case -1:
				delta = ev.Quantity.Neg()
			}
		}
	}
	s.obvSum, _ = s.obvSum.Add(delta)
	s.obvPrevPrice = ev.Price
}

// Update advances the strategy with one closed bar and returns the
// resulting Decision.
//
// The algorithm:
//  1. Append the bar's true range — max(H-L, |H-prevClose|, |L-prevClose|),
//     seeded by the previous bar's close (H-L on the first bar) — to the
//     ATR window.
//  2. Append the bar's close to the short and long volatility windows.
//  3. If the long window is not yet full, return HOLD with Reason
//     "warming_up" — there is not enough history to characterise volatility.
//  4. Compute ShortVol = σ(shortVolWin), LongVol = σ(longVolWin), ATR = mean
//     of the ATR window. If LongVol is below MinLongVolFloor, return HOLD
//     with Reason "vol_too_low" — the baseline is too flat to trade on.
//  5. Compute the compression ratio ShortVol/LongVol. If it is below
//     CompressionThreshold, set compressionArmed=true. A zero LongVol is
//     treated as ratio=0 (maximum compression) so a perfectly flat
//     baseline correctly arms a flag.
//  6. With compression armed, compute triggerHigh/Low = prevBarClose ± k·ATR.
//     A close above triggerHigh increments barsAboveTrigger; a close below
//     triggerLow increments barsBelowTrigger. When either reaches
//     ConfirmationBars, emit SignalBuy or SignalSell with Reason
//     "compression_breakout" and reset the armed flag + counters.
//
// Breakout is price-only: bars pass through regardless of CloseYTM.
func (s *Strategy) Update(bar types.Bar) (Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg := s.cfg

	// Capture the previous bar's close before mutating any state — the
	// breakout trigger is anchored to the most recent close, not the
	// current one. Same deliberate ordering the tick path had, now
	// per-bar.
	prevClose := s.lastPrice

	if err := s.ingestObservation(bar, prevClose); err != nil {
		return Decision{}, err
	}

	d := Decision{
		time:             bar.Time,
		bondID:           s.baseAssetID,
		price:            bar.Close,
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
		if !s.compressionArmed {
			s.armedCompressionRatio = ratio
		}
		s.compressionArmed = true
		d.CompressionArmed = true
	}

	if !s.compressionArmed {
		d.signal = types.SignalHold
		d.reason = DecisionReasonNoSignalYet
		return d, nil
	}

	s.evaluateBreakout(&d, bar.Close, prevClose, atr)
	s.applyVolumeFilter(&d)
	return d, nil
}

// applyVolumeFilter suppresses the breakout signal when windowed OBV
// doesn't confirm the trade direction. OBVWindow = 0 means "do not
// need to verify volume" (filter is off, early return); OBVWindow > 0
// means verify with the windowed OBV. OBV is the sum of signed
// quantities in the last OBVWindow trades; threshold is
// OBVTrendThreshold.
func (s *Strategy) applyVolumeFilter(d *Decision) {
	if s.cfg.OBVWindow == 0 {
		return
	}
	obv := s.OBV()
	threshold := s.cfg.OBVTrendThreshold
	switch d.signal {
	case types.SignalBuy:
		if obv.Cmp(threshold) <= 0 {
			s.logger().Debug("OBV filter suppressed BUY", "obv", obv, "threshold", threshold)
			d.signal = types.SignalHold
			d.positionSize = decimal.Zero
		}
	case types.SignalSell:
		// Need OBV < -threshold; i.e. -OBV > threshold.
		negObv := obv.Neg()
		if negObv.Cmp(threshold) <= 0 {
			s.logger().Debug("OBV filter suppressed SELL", "obv", obv, "threshold", threshold)
			d.signal = types.SignalHold
			d.positionSize = decimal.Zero
		}
	default:
		// d.signal is Hold; the filter doesn't touch Hold decisions.
	}
}

// ingestObservation updates the rolling windows with a new closed bar:
// the ATR window gets the bar's true range (seeded by the previous
// bar's close), and the short/long volatility windows get the bar's
// close.
func (s *Strategy) ingestObservation(bar types.Bar, prevClose decimal.Decimal) error {
	tr, err := bar.TrueRange(prevClose)
	if err != nil {
		return err
	}
	if err := s.atrWin.Add(tr); err != nil {
		return err
	}
	if err := s.shortVolWin.Add(bar.Close); err != nil {
		return err
	}
	if err := s.longVolWin.Add(bar.Close); err != nil {
		return err
	}
	s.lastPrice = bar.Close
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
			d.ArmedCompressionRatio = s.armedCompressionRatio
			s.resetArmed()
		}
	case price.Cmp(triggerLow) < 0:
		s.barsBelowTrigger++
		if s.barsBelowTrigger >= s.cfg.ConfirmationBars {
			d.signal = types.SignalSell
			d.reason = DecisionReasonCompressionEntry
			d.positionSize = decimal.One
			d.ArmedCompressionRatio = s.armedCompressionRatio
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

// Backtest is the strategy.Strategy entry point for a backtest run.
// Validates the date range, loads the bar window (with LongVolWindow+1
// bars of warmup) from candle history, and forwards to the backtester.
func (s *Strategy) Backtest(ctx context.Context, start, end time.Time) (types.BacktestResult, error) {
	if end.UTC().Before(start.UTC()) {
		return nil, errors.New("end date must be after start date")
	}
	now := time.Now().UTC()
	if start.UTC().After(now) || end.UTC().After(now) {
		return nil, errors.New("start and end date must be in the past")
	}

	bars, err := s.getBars(ctx, start, end)
	if err != nil {
		return nil, err
	}
	assetID, err := s.lookupAssetID(ctx, s.cfg.OrderBookID)
	if err != nil {
		return nil, fmt.Errorf("backtest requires the order book's base asset: %w", err)
	}
	s.mu.Lock()
	s.baseAssetID = assetID
	s.mu.Unlock()
	bt := NewBacktester(s, s.backtestWriter)
	bt.ticks = s.loadTicks(ctx, assetID, bars)
	// Pin the trading window so warmup bars (the pre-`start` slice
	// getBars returns) only seed indicators; entries and reporting
	// stay restricted to [start, end].
	bt.TradeFrom = start
	bt.TradeTo = end
	return bt.Run(ctx, bars)
}

// Run starts the live breakout loop. Closed bars are the signal source
// (the candle feed bootstraps history back to `since` for a warm
// start); ticks only drive intrabar stop-loss/take-profit exits. The
// opposite-signal pattern closes the open position.
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
		s.unsubscribeTrades()
	}()

	return s.runLoop(runCtx, msgCh, pricesCh, s.subscribeTrades())
}

// runLoop is the inner select that drives the live strategy. Extracted so
// Run() stays small and the live loop itself can be tested in isolation
// via dependency-injected channels.
//
//nolint:funlen // main run loop with setup and teardown
func (s *Strategy) runLoop(
	ctx context.Context,
	msgs <-chan strategy.Message,
	prices <-chan map[uuid.UUID]prices.AssetPrice,
	trades <-chan streams.TradeEvent,
) error {
	assetID, err := s.lookupAssetID(ctx, s.cfg.OrderBookID)
	if err != nil {
		return fmt.Errorf("lookup asset ID: %w", err)
	}

	s.mu.Lock()
	s.baseAssetID = assetID
	s.mu.Unlock()

	// Bars are the signal source: the feed bootstraps history back to
	// `since` (the volatility-baseline warm start, replacing the old
	// cold start) and then streams closed bars.
	if s.candleFeed == nil {
		return errors.New("candle feed not configured")
	}
	if err := s.requireCandleCoverage(ctx); err != nil {
		return err
	}
	res := strategy.ResolutionDuration(s.cfg.Resolution)
	warmup := time.Duration(s.cfg.LongVolWindow+1) * res
	bars, cancelBars, err := s.candleFeed.SubscribeBars(ctx, s.cfg.OrderBookID, s.cfg.Resolution, time.Now().UTC().Add(-warmup))
	if err != nil {
		return fmt.Errorf("failed to subscribe to bars: %w", err)
	}
	defer cancelBars()

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
			case strategy.Pause:
				s.paused = true
				s.logger().Debug("breakout run paused", "runID", s.runID)
			case strategy.Resume:
				s.paused = false
				s.logger().Debug("breakout run resumed", "runID", s.runID)
			}
			s.mu.Unlock()
		case bar := <-bars:
			// Snapshot state under the lock; Update() re-acquires the
			// lock so we must release before calling it.
			s.mu.RLock()
			paused := s.paused
			openSig := s.openSignal
			s.mu.RUnlock()

			decision, err := s.Update(bar)
			if err != nil {
				s.logger().Error("update strategy failed", "runID", s.runID, "assetID", assetID, "err", err)
				continue
			}
			if paused {
				continue
			}

			if openSig != types.SignalHold {
				// Opposite-signal reversal closes the position. (Bar-close
				// SL/TP is not evaluated here — the backtester evaluates
				// exits at bar extremes; live SL/TP runs on ticks below.)
				if isReversal(openSig, decision.Signal()) {
					if err := s.closePosition(ctx, assetID, DecisionReasonReversal); err != nil {
						s.logger().Error("close position failed", "runID", s.runID, "assetID", assetID, "err", err)
						s.recordErr(err)
					}
				}
				continue
			}

			// Flat: open a position on a fresh signal.
			if decision.Signal() != types.SignalHold {
				if _, err := s.executeDecision(ctx, decision, assetID); err != nil {
					s.logger().Error("execute decision failed", "runID", s.runID, "assetID", assetID, "err", err)
					s.recordErr(err)
				}
			}
		case pxs := <-prices:
			// Ticks no longer drive entries or the rolling windows; they
			// only provide intrabar stop-loss / take-profit exits.
			for _, px := range pxs {
				if px.AssetID != assetID {
					continue
				}
				s.mu.RLock()
				openSig := s.openSignal
				paused := s.paused
				entryPrice := s.entryPrice
				entryATR := s.entryATR
				s.mu.RUnlock()
				if paused || openSig == types.SignalHold {
					// Paused means hands-off: take no action on ticks,
					// mirroring the bar case.
					continue
				}
				if reason, ok := liveCheckSLTP(openSig, entryPrice, entryATR, px.Price, s.cfg); ok {
					s.logger().Info("exiting position intra-bar", "reason", reason, "runID", s.runID)
					if err := s.closePosition(ctx, assetID, reason); err != nil {
						s.logger().Error("close position failed", "runID", s.runID, "assetID", assetID, "err", err)
						s.recordErr(err)
					}
				}
			}
		case ev, ok := <-trades:
			if !ok {
				// Trade channel closed; continue without it.
				continue
			}
			s.applyTradeEvent(ev)
		case <-ticker.C:
			// keep the loop alive; reserved for future heartbeat logic.
		}
	}
}

// recordErr appends an error to the run's error log under the lock.
func (s *Strategy) recordErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, err)
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

// subscribeTrades subscribes to the live trade stream for the configured
// order book. Returns a nil channel if volume confirmation is disabled
// or the trade stream wasn't injected — runLoop handles nil channels
// by simply never selecting that case.
func (s *Strategy) subscribeTrades() <-chan streams.TradeEvent {
	if s.cfg.OBVWindow == 0 || s.tradeStream == nil {
		return nil
	}
	subID, ch := s.tradeStream.SubscribeOrderBook(s.cfg.OrderBookID)
	s.obvMu.Lock()
	s.tradeSubID = subID
	s.obvMu.Unlock()
	s.logger().Info("subscribed to trade stream", "runID", s.runID, "order_book", s.cfg.OrderBookID, "subID", subID)
	return ch
}

// unsubscribeTrades closes the trade subscription if one was opened.
func (s *Strategy) unsubscribeTrades() {
	s.obvMu.Lock()
	subID := s.tradeSubID
	s.tradeSubID = uuid.Nil
	s.obvMu.Unlock()
	if subID == uuid.Nil || s.tradeStream == nil {
		return
	}
	s.tradeStream.Unsubscribe(subID)
}

// lookupAssetID resolves the configured order-book ID to a DORA asset ID
// via the shared strategy.LookupAssetID helper.
func (s *Strategy) lookupAssetID(ctx context.Context, orderBookID uuid.UUID) (string, error) {
	return strategy.LookupAssetID(ctx, s.marketAPIClient, orderBookID)
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
	if _, err := s.marketAPIClient.CreateMarketOrder(
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
	// Record entry state so closePosition can evaluate SL / TP later.
	s.entryPrice = price
	s.entryATR = decision.ATR
	s.mu.Unlock()
	return true, nil
}

// closePosition reverses the currently open position. Used by handleTick
// when SL/TP or an opposite-signal reversal fires while a position is
// open. The reason argument is persisted in strategy_decisions.reason.
func (s *Strategy) closePosition(ctx context.Context, assetID, reason string) error {
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
			s.entryPrice = decimal.Zero
			s.entryATR = decimal.Zero
			s.mu.Unlock()
			return nil
		}
	}

	inverseLeverage, err := decimal.One.Quo(s.cfg.Leverage)
	if err != nil {
		inverseLeverage = decimal.One
	}

	clientOrderID := strategy.BuildClientOrderID(StrategyType, s.runID)
	if _, err := s.marketAPIClient.CreateMarketOrder(
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
		Reason:          reason,
		ReasonDetail:    fmt.Sprintf("close: %s", reason),
		ClientOrderID:   clientOrderID,
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

// liveCheckSLTP is the live-loop counterpart of the backtest's band
// checks, sharing bandLevel's entry ± multiplier×ATR math. Returns the
// exit reason and true if the current price has crossed the SL or TP
// band, ("", false) otherwise.
// Stops at the first hit (SL first, then TP) so a single fast move
// records the worse outcome.
func liveCheckSLTP(openSig types.Signal, entryPrice, entryATR, currentPrice decimal.Decimal, cfg Config) (string, bool) {
	if entryATR.IsZero() {
		return "", false
	}
	switch openSig {
	case types.SignalBuy:
		if cfg.StopLossATR.IsPos() {
			if slHit, ok := exitPriceCrosses(entryPrice, entryATR, cfg.StopLossATR, currentPrice, false); ok && slHit {
				return ExitReasonStopLoss, true
			}
		}
		if cfg.TakeProfitATR.IsPos() {
			if tpHit, ok := exitPriceCrosses(entryPrice, entryATR, cfg.TakeProfitATR, currentPrice, true); ok && tpHit {
				return ExitReasonTakeProfit, true
			}
		}
	case types.SignalSell:
		if cfg.StopLossATR.IsPos() {
			if slHit, ok := exitPriceCrosses(entryPrice, entryATR, cfg.StopLossATR, currentPrice, true); ok && slHit {
				return ExitReasonStopLoss, true
			}
		}
		if cfg.TakeProfitATR.IsPos() {
			if tpHit, ok := exitPriceCrosses(entryPrice, entryATR, cfg.TakeProfitATR, currentPrice, false); ok && tpHit {
				return ExitReasonTakeProfit, true
			}
		}
	default:
		// openSig is Hold; unreachable in production (handleTick
		// short-circuits before calling this).
		return "", false
	}
	// Reached when the SignalBuy or SignalSell cases don't fire their
	// inner returns (e.g. multipliers are non-positive or the price
	// hasn't crossed the band).
	return "", false
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
		s.logger().Error(
			"save strategy decision",
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
