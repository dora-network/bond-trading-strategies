package breakout

import (
	"testing"

	"github.com/dora-network/bond-trading-strategies/prices"
)

// TestNewWiresPricesHandler pins the constructor contract: the
// pricesHandler argument must land on the Strategy. New used to drop it,
// so every live run died in Run's subscribePrices with "prices handler
// is not configured" — while the run-loop tests (which inject channels
// directly) stayed green.
func TestNewWiresPricesHandler(t *testing.T) {
	s := New(DefaultConfig(), prices.New(prices.Config{}))
	if s.pricesHandler == nil {
		t.Fatal("New dropped the pricesHandler argument: live runs would fail at subscribePrices")
	}
}
