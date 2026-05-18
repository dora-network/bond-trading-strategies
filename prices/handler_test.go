package prices_test

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
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/prices/pricesfakes"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_buildURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      prices.Config
		expected string
		wantErr  bool
	}{
		{
			name: "base case",
			cfg: prices.Config{
				BaseURL: "wss://example.com",
			},
			expected: "wss://example.com/v1/prices/stream",
		},
		{
			name: "with api key",
			cfg: prices.Config{
				BaseURL: "wss://example.com",
				APIKey:  "secret123",
			},
			expected: "wss://example.com/v1/prices/stream?api_key=secret123",
		},
		{
			name: "with asset id",
			cfg: prices.Config{
				BaseURL: "wss://example.com",
				AssetID: "abc-123",
			},
			expected: "wss://example.com/v1/prices/stream?asset_id=abc-123",
		},
		{
			name: "with all parameters",
			cfg: prices.Config{
				BaseURL: "wss://example.com",
				APIKey:  "secret123",
				AssetID: "abc-123",
			},
			expected: "wss://example.com/v1/prices/stream?api_key=secret123&asset_id=abc-123",
		},
		{
			name: "invalid url",
			cfg: prices.Config{
				BaseURL: "::not a url",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := prices.New(tt.cfg)
			u, err := h.BuildURL()

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, u)
			}
		})
	}
}

func TestHandler_processMessage(t *testing.T) {
	t.Parallel()

	t.Run("valid message", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)
		fakeStore := &pricesfakes.FakePriceStore{
			SavePricesStub: func(ctx context.Context, prices map[uuid.UUID]prices.AssetPrice) error {
				wg.Done()
				return nil
			},
		}
		h := prices.New(prices.Config{})

		// store subscriber subscribes for price updates so it can write them to the store
		s := prices.NewStoreSubscriber(fakeStore, h.Subscribe)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()
		go func() {
			_ = s.Start(ctx)
		}()

		assetID := uuid.Must(uuid.NewV7())

		payload := []byte(fmt.Sprintf(`{
			"%s": {
				"asset_id": "%s",
				"price": "100.50",
				"time": "2026-04-10T15:30:00Z"
			}
		}`, assetID.String(), assetID.String()))

		time.Sleep(time.Second)
		err := h.ProcessMessage(context.Background(), payload)
		require.NoError(t, err)
		wg.Wait()

		require.Equal(t, 1, fakeStore.SavePricesCallCount())
		_, savedPrices := fakeStore.SavePricesArgsForCall(0)
		require.Len(t, savedPrices, 1)

		p, ok := savedPrices[assetID]
		require.True(t, ok)
		assert.Equal(t, assetID.String(), p.AssetID)

		expectedPrice, _ := decimal.Parse("100.50")
		assert.Equal(t, expectedPrice, p.Price)
	})

	t.Run("invalid json", func(t *testing.T) {
		fakeStore := &pricesfakes.FakePriceStore{}
		h := prices.New(prices.Config{})

		payload := []byte(`{ not valid json }`)
		err := h.ProcessMessage(context.Background(), payload)
		require.ErrorContains(t, err, "unmarshal")
		assert.Equal(t, 0, fakeStore.SavePricesCallCount())
	})
}

func TestHandler_Stream(t *testing.T) {
	t.Parallel()

	t.Run("successful stream", func(t *testing.T) {
		fakeStore := &pricesfakes.FakePriceStore{}
		wg := sync.WaitGroup{}
		wg.Add(1)

		// Start a mock websocket server
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := websocket.Accept(w, r, nil)
			require.NoError(t, err)
			defer c.Close(websocket.StatusNormalClosure, "")

			// Send one valid message
			assetID := uuid.Must(uuid.NewV7())
			msg := []byte(fmt.Sprintf(`{"%s": {"asset_id": "%s", "price": "100.50", "time": "2026-04-10T15:30:00Z"}}`,
				assetID.String(),
				assetID.String(),
			))
			wg.Wait()
			err = c.Write(r.Context(), websocket.MessageText, msg)
			require.NoError(t, err)

			// Gracefully close to finish the stream
			c.Close(websocket.StatusNormalClosure, "done")
		}))
		defer srv.Close()

		wsURL := strings.Replace(srv.URL, "http://", "ws://", 1)

		h := prices.New(prices.Config{BaseURL: wsURL})

		// Wait briefly or just check the call count immediately. We run Stream synchronously until it exits.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// store subscriber subscribes for price updates so it can write them to the store
		s := prices.NewStoreSubscriber(fakeStore, h.Subscribe)
		go func() {
			_ = s.Start(ctx)
		}()
		// we need to wait for the store subscriber to start before we can start streaming
		time.Sleep(time.Second)
		wg.Done()

		err := h.Stream(ctx)
		// Expected error is standard close because the server closed it
		require.Error(t, err)
		var closeErr websocket.CloseError
		require.ErrorAs(t, err, &closeErr)
		assert.Equal(t, websocket.StatusNormalClosure, closeErr.Code)

		assert.GreaterOrEqual(t, fakeStore.SavePricesCallCount(), 1)
	})
}
