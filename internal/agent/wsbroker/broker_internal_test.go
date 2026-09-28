package wsbroker

import "testing"

func TestSubscriptionMatchesPrice(t *testing.T) {
	sub := &Subscription{AssetID: "asset-1", TypeFilter: []string{"price"}}
	if !sub.matches(Frame{Type: "price", AssetID: "asset-1"}) {
		t.Error("expected price match for matching asset_id")
	}
	if sub.matches(Frame{Type: "price", AssetID: "asset-other"}) {
		t.Error("expected no match for different asset_id")
	}
	if sub.matches(Frame{Type: "candle", AssetID: "asset-1"}) {
		t.Error("expected no match for non-price frame")
	}
}

func TestSubscriptionMatchesTrade(t *testing.T) {
	sub := &Subscription{OrderBookID: "OB-1", TypeFilter: []string{"trade"}}
	if !sub.matches(Frame{Type: "trade", OrderBookID: "OB-1"}) {
		t.Error("expected trade match for matching order_book_id")
	}
	if sub.matches(Frame{Type: "trade", OrderBookID: "OB-other"}) {
		t.Error("expected no match for different order_book_id")
	}
	if sub.matches(Frame{Type: "candle", OrderBookID: "OB-1"}) {
		t.Error("expected no match for non-trade frame")
	}
}
