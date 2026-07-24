package momentum

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/strategy/window"
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

	// lastPrice is the previous clean price (for ATR abs-diff). Zero
	// until the second tick.
	lastPrice decimal.Decimal

	// sourceSign applies the bond-specific direction mapping: +1 for
	// price, -1 for ytm/spread (yield up = price down).
	sourceSign decimal.Decimal

	// marketAPIClient is used by lookupAssetID to resolve an order
	// book's asset ID. Lazily set by the live run loop; nil for
	// backtests and signal-only callers.
	marketAPIClient strategy.MarketAPIClient

	// collateralWeight is the collateral weight of the base asset
	// fetched from DORA during the live run. Defaults to 1.0 in
	// backtests and signal-only callers.
	collateralWeight decimal.Decimal

	// historyStore / benchmarkClient are the historical data surfaces
	// used by getObservations / prefillWindow (spread mode only for
	// the FRED client). Defined in historical_data.go.
	historyStore    historicalPriceStore
	benchmarkClient benchmarkYieldClient

	// benchmarkObservations caches FRED yields for spread mode.
	benchmarkObservations []fred.Observation

	// backtestWriter receives per-trade rows from the backtester.
	// nil skips persistence.
	backtestWriter stats.BacktestTradeWriter
}

// New creates a Strategy with the given Config and optional options.
func New(cfg Config, opts ...func(*Strategy)) *Strategy {
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

// WithBacktestWriter sets the destination for per-trade rows the
// backtester emits during a backtest. If unset, trade rows are not
// persisted and the /trades endpoints return empty.
func WithBacktestWriter(w stats.BacktestTradeWriter) func(*Strategy) {
	return func(s *Strategy) { s.backtestWriter = w }
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
func (s *Strategy) lookupAssetID(orderBookID uuid.UUID) (string, error) {
	return strategy.LookupAssetID(context.Background(), s.marketAPIClient, orderBookID)
}

// Backtest is the strategy.Strategy entry point for a backtest run.
// Validates the date range, loads the observation window, and forwards
// to the backtester.
func (s *Strategy) Backtest(ctx context.Context, start, end time.Time) (types.BacktestResult, error) {
	if end.UTC().Before(start.UTC()) {
		return BacktestResult{}, errors.New("end date must be after start date")
	}
	now := time.Now().UTC()
	if start.UTC().After(now) || end.UTC().After(now) {
		return BacktestResult{}, errors.New("start and end date must be in the past")
	}
	obs, err := s.getObservations(ctx, start, end)
	if err != nil {
		return nil, err
	}
	bt := NewBacktester(s, s.backtestWriter)
	return bt.Run(ctx, obs)
}

// NewExitDecision builds a Decision carrying only the fields ShouldExit
// inspects (signal + price). Exported so the external test package can
// construct synthetic decisions for unit tests of ShouldExit.
func NewExitDecision(signal types.Signal, price decimal.Decimal) Decision {
	return Decision{signal: signal, price: price}
}

// ShouldExit reports whether an open position should close, and why.
// Priority: stop_loss > take_profit > reversal. Stop/TP are price-based
// and entry-anchored (stable for the position's life). Reversal fires
// when the current decision's signal opposes the open position.
func (s *Strategy) ShouldExit(openSignal types.Signal, d Decision, entryPrice, entryATR decimal.Decimal) (bool, string) {
	price := d.Price()
	cfg := s.cfg

	if cfg.StopLossATR.IsPos() && entryATR.IsPos() {
		stopDist, err := cfg.StopLossATR.Mul(entryATR)
		if err == nil {
			switch openSignal { //nolint:exhaustive // SignalHold means flat — no stop check
			case types.SignalBuy:
				threshold, err := entryPrice.Sub(stopDist)
				if err == nil && price.Cmp(threshold) <= 0 {
					return true, ExitReasonStopLoss
				}
			case types.SignalSell:
				threshold, err := entryPrice.Add(stopDist)
				if err == nil && price.Cmp(threshold) >= 0 {
					return true, ExitReasonStopLoss
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
					return true, ExitReasonTakeProfit
				}
			case types.SignalSell:
				threshold, err := entryPrice.Sub(tpDist)
				if err == nil && price.Cmp(threshold) <= 0 {
					return true, ExitReasonTakeProfit
				}
			}
		}
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
