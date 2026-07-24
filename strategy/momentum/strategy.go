package momentum

import (
	"sync"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/strategy/window"
	"github.com/govalues/decimal"
)

// Strategy holds per-bond state for the momentum / trend strategy.
type Strategy struct {
	mu  sync.RWMutex
	cfg Config

	fastWin *window.Rolling
	slowWin *window.Rolling
	atrWin  *window.Rolling

	// lastPrice is the previous clean price (for ATR abs-diff). Zero
	// until the second tick.
	lastPrice decimal.Decimal

	// sourceSign applies the bond-specific direction mapping: +1 for
	// price, -1 for ytm/spread (yield up = price down).
	sourceSign decimal.Decimal
}

// New creates a Strategy with the given Config and optional options.
func New(cfg Config, opts ...func(*Strategy)) *Strategy {
	if cfg.Leverage.IsZero() {
		cfg.Leverage = decimal.One
	}
	s := &Strategy{
		cfg:        cfg,
		fastWin:    window.NewRollingWindow(cfg.FastWindow),
		slowWin:    window.NewRollingWindow(cfg.SlowWindow),
		atrWin:     window.NewRollingWindow(cfg.ATRWindow),
		sourceSign: sourceSign(cfg.SignalSource),
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

// seriesValue selects the configured series value from the observation.
// ok is false when the tick must be dropped entirely (zero YTM in
// ytm/spread modes). price mode never drops.
func (s *Strategy) seriesValue(o types.YieldObservation) (decimal.Decimal, bool, error) {
	switch s.cfg.SignalSource {
	case SignalSourcePrice:
		return o.Price, true, nil
	case SignalSourceYTM:
		if o.YTM.IsZero() {
			return decimal.Zero, false, nil
		}
		return o.YTM, true, nil
	default: // spread
		if o.YTM.IsZero() {
			return decimal.Zero, false, nil
		}
		spread, err := o.Spread()
		return spread, true, err
	}
}

// Update ingests one observation and returns the resulting Decision.
func (s *Strategy) Update(obs types.YieldObservation) (Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prevPrice := s.lastPrice
	s.lastPrice = obs.Price

	value, ok, err := s.seriesValue(obs)
	if err != nil {
		return Decision{}, err
	}
	if !ok {
		// Drop the tick entirely (no window updates, no signal).
		return Decision{
			time: obs.Time, bondID: obs.BondID, price: obs.Price,
			signal: types.SignalHold, reason: DecisionReasonWarmingUp,
		}, nil
	}

	// ATR = mean absolute price diff.
	var absDiff decimal.Decimal
	if !prevPrice.IsZero() {
		d, err := obs.Price.Sub(prevPrice)
		if err != nil {
			return Decision{}, err
		}
		absDiff = d.Abs()
	}
	if err := s.atrWin.Add(absDiff); err != nil {
		return Decision{}, err
	}
	if err := s.fastWin.Add(value); err != nil {
		return Decision{}, err
	}
	if err := s.slowWin.Add(value); err != nil {
		return Decision{}, err
	}

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
		time: obs.Time, bondID: obs.BondID, price: obs.Price,
		FastMA: fastMA, SlowMA: slowMA, ATR: atr, SeriesValue: value,
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
	return d, nil
}
