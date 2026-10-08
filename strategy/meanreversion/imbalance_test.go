package meanreversion_test

import (
	"testing"

	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
)

func newGatedStrategy(window int, threshold decimal.Decimal) *meanreversion.Strategy {
	cfg := defaultConfig()
	cfg.ImbalanceWindow = window
	cfg.ImbalanceThreshold = threshold
	return meanreversion.New(cfg, nil)
}

func sellTrade(qty decimal.Decimal) streams.TradeEvent {
	return streams.TradeEvent{Side: "SELL", Quantity: qty, Price: decimal.MustNew(100, 0)}
}

func TestImbalanceGate_BlocksBuyAgainstNetSelling(t *testing.T) {
	t.Parallel()

	s := newGatedStrategy(3, decimal.Zero)
	for range 3 {
		meanreversion.ApplyTrade(s, sellTrade(decimal.One))
	}

	assert.False(t, meanreversion.ImbalanceAllows(s, types.SignalBuy), "BUY must be blocked on net selling tape")
	assert.True(t, meanreversion.ImbalanceAllows(s, types.SignalSell), "SELL allowed on the same tape")
}

func TestImbalanceGate_ThresholdRespected(t *testing.T) {
	t.Parallel()

	// Net flow −5 with threshold 10: the opposing flow is inside the
	// tolerance, so a BUY survives the gate.
	s := newGatedStrategy(10, decimal.MustNew(10, 0))
	for range 5 {
		meanreversion.ApplyTrade(s, sellTrade(decimal.One))
	}
	assert.True(t, meanreversion.ImbalanceAllows(s, types.SignalBuy))
	assert.True(t, meanreversion.ImbalanceAllows(s, types.SignalSell))

	// Net flow −20 with threshold 10: past the tolerance, blocked.
	s = newGatedStrategy(10, decimal.MustNew(10, 0))
	for range 10 {
		meanreversion.ApplyTrade(s, sellTrade(decimal.Two))
	}
	assert.False(t, meanreversion.ImbalanceAllows(s, types.SignalBuy))
}

func TestImbalanceGate_DisabledAlwaysAllows(t *testing.T) {
	t.Parallel()

	s := newGatedStrategy(0, decimal.Zero)
	for range 5 {
		meanreversion.ApplyTrade(s, sellTrade(decimal.One))
	}
	assert.True(t, meanreversion.ImbalanceAllows(s, types.SignalBuy))
	assert.True(t, meanreversion.ImbalanceAllows(s, types.SignalSell))
}

func TestImbalanceGate_WindowNotReadyAllows(t *testing.T) {
	t.Parallel()

	s := newGatedStrategy(3, decimal.Zero)
	meanreversion.ApplyTrade(s, sellTrade(decimal.One)) // window needs 2+, only 1 trade
	assert.True(t, meanreversion.ImbalanceAllows(s, types.SignalBuy))
}

func TestImbalanceGate_UnknownSideSkipped(t *testing.T) {
	t.Parallel()

	s := newGatedStrategy(3, decimal.Zero)
	for range 3 {
		meanreversion.ApplyTrade(s, streams.TradeEvent{Side: "UNKNOWN", Quantity: decimal.One})
		meanreversion.ApplyTrade(s, sellTrade(decimal.One))
	}
	// Only the three SELL trades count: net −3 blocks a BUY.
	assert.False(t, meanreversion.ImbalanceAllows(s, types.SignalBuy))
}

func TestImbalanceGate_BuyFlowBlocksSell(t *testing.T) {
	t.Parallel()

	s := newGatedStrategy(3, decimal.Zero)
	for range 3 {
		meanreversion.ApplyTrade(s, streams.TradeEvent{Side: "BUY", Quantity: decimal.One})
	}
	assert.False(t, meanreversion.ImbalanceAllows(s, types.SignalSell))
	assert.True(t, meanreversion.ImbalanceAllows(s, types.SignalBuy))
}
