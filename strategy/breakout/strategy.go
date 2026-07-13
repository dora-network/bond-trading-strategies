package breakout

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/config"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/strategy/window"
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

// Backtest is implemented in Task 4.
func (s *Strategy) Backtest(_ context.Context, _ time.Time, _ time.Time) (types.BacktestResult, error) {
	return BacktestResult{}, nil
}

// Run is implemented in Task 5.
func (s *Strategy) Run(_ context.Context, _ <-chan strategy.Message, _ uuid.UUID) error {
	return nil
}

// Compile-time guard that *Strategy satisfies strategy.Strategy.
var _ strategy.Strategy = (*Strategy)(nil)
