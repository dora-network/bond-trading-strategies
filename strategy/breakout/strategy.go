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
	mu            sync.RWMutex
	cfg           Config
	log           *slog.Logger
	shortVolWin   *window.Rolling
	longVolWin    *window.Rolling
	atrWin        *window.Rolling
	decisionStore strategy.DecisionRecorder
	decisionSeq   int64
	pricesHandler *prices.Handler
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
// the resulting Decision. Full signal logic is implemented in Task 3;
// this skeleton returns HOLD on every tick so the package compiles.
func (s *Strategy) Update(o types.YieldObservation) (Decision, error) {
	return Decision{
		time:   o.Time,
		bondID: o.BondID,
		price:  o.Price,
		signal: types.SignalHold,
		reason: DecisionReasonNoSignalYet,
	}, nil
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
