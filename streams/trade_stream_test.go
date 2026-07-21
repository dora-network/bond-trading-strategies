package streams

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
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

func TestSafeStreamURLRedactsAPIKey(t *testing.T) {
	t.Parallel()

	got := safeStreamURL("wss://example.com/v1/trades/book-123/stream?since=2026-06-24T11%3A26%3A36Z&x-api-key=secret123")

	require.Equal(t, "wss://example.com/v1/trades/book-123/stream?since=2026-06-24T11%3A26%3A36Z&x-api-key=%2A%2A%2A", got)
	require.NotContains(t, got, "secret123")
}

func TestNextDelay_DoublesAndCaps(t *testing.T) {
	t.Parallel()

	// Five doublings from 100ms: 100→200→400→800→1600→3200ms (no cap
	// yet). Then twenty more doublings land at the 5s cap. Jitter is
	// bounded by max/jitterDivisor = 1s.
	d := initialReconnectDelay
	for range 5 {
		d = nextDelay(d)
	}
	require.GreaterOrEqual(t, d, 2*time.Second)

	for range 20 {
		d = nextDelay(d)
	}
	require.LessOrEqual(t, d, 6*time.Second)
}

func TestTradeStream_StartReconnectsOnDialFailure(t *testing.T) {
	t.Parallel()

	ts := NewTradeStream()
	obID := uuid.New()

	// Stub dialer: fail twice, then succeed on the third attempt by
	// returning a channel that is already closed (readLoop returns
	// immediately). The reconnect loop sees readLoop exit and
	// backoffs; we end the test via ctx timeout.
	var attempts atomic.Int32
	ts.tradeDial = func(ctx context.Context, wsURL, apiKey, obStr string, since time.Time) (<-chan []byte, context.CancelFunc, error) {
		n := attempts.Add(1)
		if n <= 2 {
			return nil, nil, errors.New("simulated dial failure")
		}
		ch := make(chan []byte)
		close(ch)
		return ch, func() {}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_ = ts.Start(ctx, "wss://example.invalid", "key", []uuid.UUID{obID})

	// Three dial attempts on the first book: 2 failures + 1 success.
	// Backoff after readLoop exit may produce 1+ more, but those are
	// not guaranteed within the test window.
	require.GreaterOrEqual(t, attempts.Load(), int32(3))
}

func TestTradeStream_StartReconnectsAfterDisconnect(t *testing.T) {
	t.Parallel()

	ts := NewTradeStream()
	obID := uuid.New()

	// First dial returns a real channel that we close from outside
	// to simulate the broker dropping the connection; subsequent
	// dials return errors so the loop keeps trying.
	var attempts atomic.Int32
	liveCh := make(chan []byte)
	ts.tradeDial = func(ctx context.Context, wsURL, apiKey, obStr string, since time.Time) (<-chan []byte, context.CancelFunc, error) {
		n := attempts.Add(1)
		if n == 1 {
			return liveCh, func() {}, nil
		}
		return nil, nil, errors.New("simulated dial failure on retry")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		// Close after the first dial has settled.
		time.Sleep(50 * time.Millisecond)
		close(liveCh)
	}()

	_ = ts.Start(ctx, "wss://example.invalid", "key", []uuid.UUID{obID})

	// First dial + at least one re-attempt after disconnect. The
	// exact count past 2 depends on jittered backoff.
	require.GreaterOrEqual(t, attempts.Load(), int32(2))
}
