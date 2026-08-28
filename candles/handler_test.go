package candles_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/candles/candlesfakes"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_buildURL(t *testing.T) {
	t.Parallel()

	sinceTime := time.Date(2026, 4, 10, 15, 30, 0, 0, time.UTC)

	tests := []struct {
		name        string
		cfg         candles.Config
		orderBookID string
		since       *time.Time
		expected    string
		wantErr     bool
	}{
		{
			name: "base case",
			cfg: candles.Config{
				BaseURL: "wss://example.com",
			},
			orderBookID: "book-123",
			expected:    "wss://example.com/v1/charts/book-123/candle/stream?resolution=1m",
		},
		{
			name: "with api key",
			cfg: candles.Config{
				BaseURL: "wss://example.com",
				APIKey:  "secret123",
			},
			orderBookID: "book-123",
			expected:    "wss://example.com/v1/charts/book-123/candle/stream?api_key=secret123&resolution=1m",
		},
		{
			name: "with since",
			cfg: candles.Config{
				BaseURL: "wss://example.com",
			},
			orderBookID: "book-123",
			since:       &sinceTime,
			expected:    "wss://example.com/v1/charts/book-123/candle/stream?resolution=1m&since=2026-04-10T15%3A30%3A00Z",
		},
		{
			name: "invalid url",
			cfg: candles.Config{
				BaseURL: "::not a url",
			},
			orderBookID: "book-123",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := candles.New(tt.cfg, &candlesfakes.FakeCandleStore{})
			u, err := h.BuildURL(tt.orderBookID, tt.since)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, u)
			}
		})
	}
}

func TestHandler_safeURLRedactsAPIKey(t *testing.T) {
	t.Parallel()

	h := candles.New(candles.Config{}, &candlesfakes.FakeCandleStore{})
	got := h.SafeURL("wss://example.com/v1/charts/book-123/candle/stream?api_key=secret123&resolution=1m")

	assert.Equal(t, "wss://example.com/v1/charts/book-123/candle/stream?api_key=%2A%2A%2A&resolution=1m", got)
	assert.NotContains(t, got, "secret123")
}

func TestHandler_processMessage(t *testing.T) {
	t.Parallel()

	t.Run("valid message", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)
		fakeStore := &candlesfakes.FakeCandleStore{
			SaveCandlesStub: func(ctx context.Context, entries []candles.StreamCandlesEntry) error {
				wg.Done()
				return nil
			},
		}
		h := candles.New(candles.Config{}, fakeStore)
		requestID := uuid.Must(uuid.NewV7())
		subCh, err := h.Subscribe(requestID)
		require.NoError(t, err)
		defer func() { _ = h.Unsubscribe(requestID) }()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case entries, ok := <-subCh:
					if !ok {
						return
					}
					_ = fakeStore.SaveCandles(ctx, entries)
				}
			}
		}()

		payload := []byte(`[
			{
				"Time": "2026-04-10T15:30:00Z",
				"Val": {
					"order_book_id": "book-123",
					"start_timestamp": "2026-04-10T15:30:00Z",
					"open": "100.00",
					"high": "105.00",
					"low": "95.00",
					"close": "102.50",
					"volume": "1000"
				}
			}
		]`)

		err = h.ProcessMessage(context.Background(), "book-123", payload)
		require.NoError(t, err)
		wg.Wait()

		require.Equal(t, 1, fakeStore.SaveCandlesCallCount())
		_, savedCandles := fakeStore.SaveCandlesArgsForCall(0)
		require.Len(t, savedCandles, 1)

		c := savedCandles[0]
		assert.Equal(t, "book-123", c.Val.OrderBookID)

		expectedOpen, _ := decimal.Parse("100.00")
		assert.Equal(t, expectedOpen, c.Val.Open)
	})

	t.Run("valid message with YTM fields", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)
		var captured []candles.Candle
		fakeStore := &candlesfakes.FakeCandleStore{
			SaveCandlesStub: func(ctx context.Context, entries []candles.StreamCandlesEntry) error {
				for _, e := range entries {
					captured = append(captured, e.Val)
				}
				wg.Done()
				return nil
			},
		}
		h := candles.New(candles.Config{}, fakeStore)
		requestID := uuid.Must(uuid.NewV7())
		subCh, err := h.Subscribe(requestID)
		require.NoError(t, err)
		defer func() { _ = h.Unsubscribe(requestID) }()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case entries, ok := <-subCh:
					if !ok {
						return
					}
					_ = fakeStore.SaveCandles(ctx, entries)
				}
			}
		}()

		payload := []byte(`[
			{
				"Time": "2026-04-10T15:30:00Z",
				"Val": {
					"order_book_id": "book-123",
					"start_timestamp": "2026-04-10T15:30:00Z",
					"open": "100.00",
					"high": "105.00",
					"low": "95.00",
					"close": "102.50",
					"volume": "1000",
					"open_ytm": "4.20",
					"high_ytm": "4.30",
					"low_ytm": "4.10",
					"close_ytm": "4.25"
				}
			}
		]`)
		err = h.ProcessMessage(context.Background(), "book-123", payload)
		require.NoError(t, err)
		wg.Wait()

		require.Len(t, captured, 1)
		c := captured[0]
		assert.True(t, c.OpenYTM.Equal(decimal.MustParse("4.20")), "OpenYTM")
		assert.True(t, c.HighYTM.Equal(decimal.MustParse("4.30")), "HighYTM")
		assert.True(t, c.LowYTM.Equal(decimal.MustParse("4.10")), "LowYTM")
		assert.True(t, c.CloseYTM.Equal(decimal.MustParse("4.25")), "CloseYTM")
	})

	t.Run("empty list", func(t *testing.T) {
		fakeStore := &candlesfakes.FakeCandleStore{}
		h := candles.New(candles.Config{}, fakeStore)

		payload := []byte(`[]`)
		err := h.ProcessMessage(context.Background(), "book-123", payload)
		require.NoError(t, err)
		assert.Equal(t, 0, fakeStore.SaveCandlesCallCount())
	})

	t.Run("invalid json", func(t *testing.T) {
		fakeStore := &candlesfakes.FakeCandleStore{}
		h := candles.New(candles.Config{}, fakeStore)

		payload := []byte(`{ not valid json }`)
		err := h.ProcessMessage(context.Background(), "book-123", payload)
		require.ErrorContains(t, err, "unmarshal")
		assert.Equal(t, 0, fakeStore.SaveCandlesCallCount())
	})

	t.Run("subscribes and unsubscribes", func(t *testing.T) {
		fakeStore := &candlesfakes.FakeCandleStore{}
		h := candles.New(candles.Config{}, fakeStore)
		requestID := uuid.Must(uuid.NewV7())

		ch, err := h.Subscribe(requestID)
		require.NoError(t, err)
		require.NotNil(t, ch)

		_, err = h.Subscribe(requestID)
		require.ErrorContains(t, err, "already subscribed")

		err = h.Unsubscribe(requestID)
		require.NoError(t, err)

		_, ok := <-ch
		assert.False(t, ok)

		err = h.Unsubscribe(requestID)
		require.ErrorContains(t, err, "subscriber not found")
	})

	t.Run("subscriber save failure does not bubble up", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)
		fakeStore := &candlesfakes.FakeCandleStore{
			SaveCandlesStub: func(ctx context.Context, entries []candles.StreamCandlesEntry) error {
				wg.Done()
				return assert.AnError
			},
		}
		h := candles.New(candles.Config{}, fakeStore)
		requestID := uuid.Must(uuid.NewV7())
		subCh, err := h.Subscribe(requestID)
		require.NoError(t, err)
		defer func() { _ = h.Unsubscribe(requestID) }()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case entries, ok := <-subCh:
					if !ok {
						return
					}
					_ = fakeStore.SaveCandles(ctx, entries)
				}
			}
		}()

		payload := []byte(`[{
			"Time": "2026-04-10T15:30:00Z",
			"Val": {"order_book_id": "book-123"}}
		]`)

		err = h.ProcessMessage(context.Background(), "book-123", payload)
		require.NoError(t, err)
		wg.Wait()
		assert.Equal(t, 1, fakeStore.SaveCandlesCallCount())
	})
}

func TestHandler_StreamSingle(t *testing.T) {
	t.Parallel()

	t.Run("successful stream", func(t *testing.T) {
		saveDone := make(chan struct{})
		fakeStore := &candlesfakes.FakeCandleStore{
			SaveCandlesStub: func(ctx context.Context, entries []candles.StreamCandlesEntry) error {
				close(saveDone)
				return nil
			},
		}
		wg := sync.WaitGroup{}
		wg.Add(1)

		lastTime := time.Date(2026, 4, 10, 15, 29, 0, 0, time.UTC)
		fakeStore.GetLastTimestampReturns(&lastTime, nil)

		serverReady := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Contains(t, r.URL.RawQuery, "since=2026-04-10T15%3A29%3A00Z")

			c, err := websocket.Accept(w, r, nil)
			require.NoError(t, err)
			defer c.Close(websocket.StatusNormalClosure, "")

			msg := []byte(`[{"Time": "2026-04-10T15:30:00Z", "Val": {"order_book_id": "book-123"}}]`)
			close(serverReady)
			wg.Wait()
			err = c.Write(r.Context(), websocket.MessageText, msg)
			require.NoError(t, err)

			c.Close(websocket.StatusNormalClosure, "done")
		}))
		defer srv.Close()

		wsURL := strings.Replace(srv.URL, "http://", "ws://", 1)

		h := candles.New(candles.Config{BaseURL: wsURL}, fakeStore)

		// Mirror the daemon: a single subscriber drains the fan-out
		// into SaveCandles. Register, then run the consumer in a
		// goroutine until the test's ctx is done.
		subscriber := candles.NewStoreSubscriber(fakeStore, h.Subscribe)
		subCtx, subCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer subCancel()
		go func() {
			_ = subscriber.Start(subCtx)
		}()
		// Wait for the subscriber's goroutine to register before
		// StreamSingle can broadcast to it; otherwise the first batch
		// is dropped (processMessage sees no subscribers).
		deadline := time.Now().Add(2 * time.Second)
		for subscriber.RequestID() == uuid.Nil {
			if time.Now().After(deadline) {
				t.Fatal("subscriber never registered a requestID")
			}
			time.Sleep(time.Millisecond)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		streamErr := make(chan error, 1)
		go func() {
			streamErr <- h.StreamSingle(ctx, "book-123")
		}()
		<-serverReady
		wg.Done()

		err := <-streamErr
		require.Error(t, err)
		var closeErr websocket.CloseError
		require.ErrorAs(t, err, &closeErr)
		assert.Equal(t, websocket.StatusNormalClosure, closeErr.Code)

		<-saveDone
		assert.Equal(t, 1, fakeStore.GetLastTimestampCallCount())
	})

	t.Run("GetLastTimestamp failure", func(t *testing.T) {
		fakeStore := &candlesfakes.FakeCandleStore{}
		fakeStore.GetLastTimestampReturns(nil, assert.AnError)

		h := candles.New(candles.Config{BaseURL: "wss://example.com"}, fakeStore)

		err := h.StreamSingle(context.Background(), "book-123")
		require.ErrorIs(t, err, assert.AnError)
		require.ErrorContains(t, err, "get last timestamp")
	})
}

func TestHandler_Stream(t *testing.T) {
	t.Parallel()

	t.Run("missing store", func(t *testing.T) {
		h := candles.New(candles.Config{}, nil)
		err := h.Stream(context.Background())
		require.ErrorContains(t, err, "missing candle store")
	})

	t.Run("missing order books", func(t *testing.T) {
		h := candles.New(candles.Config{}, &candlesfakes.FakeCandleStore{})
		err := h.Stream(context.Background())
		require.ErrorContains(t, err, "no order books configured")
	})

	t.Run("propagates streamSingle errors", func(t *testing.T) {
		fakeStore := &candlesfakes.FakeCandleStore{}
		fakeStore.GetLastTimestampReturns(nil, assert.AnError)

		h := candles.New(candles.Config{
			OrderBookIDs: []string{"book-1", "book-2"},
		}, fakeStore)

		err := h.Stream(context.Background())
		require.ErrorIs(t, err, assert.AnError)
		assert.GreaterOrEqual(t, fakeStore.GetLastTimestampCallCount(), 1)
	})
}

func TestStoreSubscriber_Start(t *testing.T) {
	t.Parallel()

	t.Run("saves subscribed candle updates", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)
		fakeStore := &candlesfakes.FakeCandleStore{
			SaveCandlesStub: func(ctx context.Context, entries []candles.StreamCandlesEntry) error {
				wg.Done()
				return nil
			},
		}
		h := candles.New(candles.Config{}, fakeStore)
		s := candles.NewStoreSubscriber(fakeStore, h.Subscribe)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		go func() {
			_ = s.Start(ctx)
		}()

		// Wait for the subscriber goroutine to mint its requestID and
		// register before we send a message, otherwise ProcessMessage
		// will see no subscriber and drop the batch.
		deadline := time.Now().Add(time.Second)
		for s.RequestID() == uuid.Nil {
			if time.Now().After(deadline) {
				t.Fatal("subscriber never registered a requestID")
			}
			time.Sleep(5 * time.Millisecond)
		}

		payload := fmt.Appendf(nil, `[
			{
				"Time": "2026-04-10T15:30:00Z",
				"Val": {
					"order_book_id": %q,
					"start_timestamp": "2026-04-10T15:30:00Z",
					"open": "100.00",
					"high": "105.00",
					"low": "95.00",
					"close": "102.50",
					"volume": "1000"
				}
			}
		]`, "book-123")

		err := h.ProcessMessage(context.Background(), "book-123", payload)
		require.NoError(t, err)
		wg.Wait()

		require.Equal(t, 1, fakeStore.SaveCandlesCallCount())
		_, entries := fakeStore.SaveCandlesArgsForCall(0)
		require.Len(t, entries, 1)
		assert.Equal(t, "book-123", entries[0].Val.OrderBookID)
	})
}
