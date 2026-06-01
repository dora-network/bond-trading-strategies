package streams

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestTradeStream_SubscribeAndRoute(t *testing.T) {
	ts := NewTradeStream()

	followedTrader := uuid.New()
	subID, ch := ts.Subscribe(followedTrader)
	defer ts.Unsubscribe(subID)

	tradeData := map[string]any{
		"user_id":        followedTrader.String(),
		"asset_0":        uuid.New().String(),
		"transaction_id": uuid.New().String(),
		"side":           "buy",
		"price":          "100.5",
		"quantity_0":     "10",
	}
	entry := map[string]any{
		"Val":  tradeData,
		"Time": time.Now().UTC().Format(time.RFC3339),
	}
	data, _ := json.Marshal(entry)

	orderBookID := uuid.New()
	ts.routeTrade(data, orderBookID)

	select {
	case event := <-ch:
		require.Equal(t, followedTrader, event.TraderID)
		require.Equal(t, orderBookID, event.OrderBookID)
		require.Equal(t, "buy", event.Side)
		require.Equal(t, "100.5", event.Price.String())
		require.Equal(t, "10", event.Quantity.String())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for trade event")
	}
}

func TestTradeStream_SubscribeNotMatched(t *testing.T) {
	ts := NewTradeStream()

	followedTrader := uuid.New()
	otherTrader := uuid.New()
	subID, ch := ts.Subscribe(followedTrader)
	defer ts.Unsubscribe(subID)

	tradeData := map[string]any{
		"user_id":        otherTrader.String(),
		"asset_0":        uuid.New().String(),
		"transaction_id": uuid.New().String(),
		"side":           "buy",
		"price":          "100.5",
		"quantity_0":     "10",
	}
	entry := map[string]any{
		"Val":  tradeData,
		"Time": time.Now().UTC().Format(time.RFC3339),
	}
	data, _ := json.Marshal(entry)

	orderBookID := uuid.New()
	ts.routeTrade(data, orderBookID)

	select {
	case <-ch:
		t.Fatal("expected no trade event for non-matching trader")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestTradeStream_MultipleSubscribers(t *testing.T) {
	ts := NewTradeStream()

	trader1 := uuid.New()
	trader2 := uuid.New()

	sub1ID, ch1 := ts.Subscribe(trader1)
	sub2ID, ch2 := ts.Subscribe(trader2)
	defer func() {
		ts.Unsubscribe(sub1ID)
		ts.Unsubscribe(sub2ID)
	}()

	tradeData := map[string]any{
		"user_id":        trader1.String(),
		"asset_0":        uuid.New().String(),
		"transaction_id": uuid.New().String(),
		"side":           "buy",
		"price":          "100.5",
		"quantity_0":     "10",
	}
	entry := map[string]any{
		"Val":  tradeData,
		"Time": time.Now().UTC().Format(time.RFC3339),
	}
	data, _ := json.Marshal(entry)

	orderBookID := uuid.New()
	ts.routeTrade(data, orderBookID)

	select {
	case event := <-ch1:
		require.Equal(t, trader1, event.TraderID)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected event in ch1")
	}

	select {
	case <-ch2:
		t.Fatal("ch2 should not receive event for trader1")
	case <-time.After(100 * time.Millisecond):
	}
}
