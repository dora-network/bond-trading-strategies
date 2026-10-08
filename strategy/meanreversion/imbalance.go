package meanreversion

import (
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// applyTrade folds one tape trade into the signed-quantity imbalance
// window (BUY=+, SELL=-). Trades with a missing or unrecognized Side
// are skipped. Called from the live run loop's trade-stream case and
// from the backtester's interleave loop.
func (s *Strategy) applyTrade(ev streams.TradeEvent) {
	s.imbMu.Lock()
	defer s.imbMu.Unlock()
	if s.imbWin == nil {
		return
	}
	switch ev.Side {
	case "BUY":
		_ = s.imbWin.Add(ev.Quantity)
	case "SELL":
		_ = s.imbWin.Add(ev.Quantity.Neg())
	}
}

// imbalanceAllows reports whether an entry in the given direction
// survives the trade-imbalance gate. The gate is off (ImbalanceWindow
// <= 0): always allowed. A window that is not yet Ready allows the
// entry — insufficient tape data is not adverse tape. Otherwise the
// net signed quantity of the last ImbalanceWindow trades is compared
// against ImbalanceThreshold: a BUY is blocked when net flow is more
// negative than -threshold (net selling), a SELL when net flow is more
// positive than +threshold (net buying).
func (s *Strategy) imbalanceAllows(sig types.Signal) bool {
	if s.cfg.ImbalanceWindow <= 0 {
		return true
	}
	s.imbMu.Lock()
	defer s.imbMu.Unlock()
	if s.imbWin == nil || !s.imbWin.Ready() {
		return true
	}
	net := s.imbWin.Sum()
	switch sig {
	case types.SignalBuy:
		// Blocked when net flow is more negative than -threshold.
		return net.Cmp(s.cfg.ImbalanceThreshold.Neg()) >= 0
	case types.SignalSell:
		// Blocked when net flow is more positive than +threshold.
		return net.Cmp(s.cfg.ImbalanceThreshold) <= 0
	default:
		return true
	}
}

// netImbalance returns the current signed-quantity sum of the
// imbalance window (zero, false when the gate is disabled or the
// window not yet full). Used for the filtered-entry log line and tests.
func (s *Strategy) netImbalance() (decimal.Decimal, bool) {
	s.imbMu.Lock()
	defer s.imbMu.Unlock()
	if s.imbWin == nil || !s.imbWin.Ready() {
		return decimal.Zero, false
	}
	return s.imbWin.Sum(), true
}

// subscribeTrades opens a live trade subscription for the configured
// order book. Returns a nil channel when the imbalance gate is
// disabled or no trade stream was injected — the run loop's select
// never fires on a nil channel.
func (s *Strategy) subscribeTrades() <-chan streams.TradeEvent {
	if s.cfg.ImbalanceWindow <= 0 || s.tradeStream == nil {
		return nil
	}
	subID, ch := s.tradeStream.SubscribeOrderBook(s.cfg.OrderBookID)
	s.imbMu.Lock()
	s.tradeSubID = subID
	s.imbMu.Unlock()
	s.logger().Info("subscribed to trade stream", "runID", s.runID, "order_book", s.cfg.OrderBookID, "subID", subID)
	return ch
}

// unsubscribeTrades closes the trade subscription if one was opened.
func (s *Strategy) unsubscribeTrades() {
	s.imbMu.Lock()
	subID := s.tradeSubID
	s.tradeSubID = uuid.Nil
	s.imbMu.Unlock()
	if subID == uuid.Nil || s.tradeStream == nil {
		return
	}
	s.tradeStream.Unsubscribe(subID)
}
