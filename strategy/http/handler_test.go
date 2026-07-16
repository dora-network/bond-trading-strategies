package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/dora-network/bond-trading-strategies/notifications/notificationsfakes"
	"github.com/dora-network/bond-trading-strategies/prices"
	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/copytrading"
	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandlerListsCopyTraders(t *testing.T) {
	t.Parallel()

	trader1 := "11111111-1111-1111-1111-111111111111"
	trader2 := "22222222-2222-2222-2222-222222222222"

	fake := doraClientFunc{
		listCopyTraders: func(_ context.Context) ([]strategyhttp.CopyTrader, error) {
			return []strategyhttp.CopyTrader{
				{UserID: trader1, UserName: "alpha_trader"},
				{UserID: trader2, UserName: "beta_trader"},
			}, nil
		},
	}

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(fake),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/copy-traders", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Items []strategyhttp.CopyTraderSummary `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Items, 2)
	assert.Equal(t, strategyhttp.CopyTraderSummary{ID: trader1, UserName: "alpha_trader"}, body.Items[0])
	assert.Equal(t, strategyhttp.CopyTraderSummary{ID: trader2, UserName: "beta_trader"}, body.Items[1])
}

func TestHandlerListsCopyTradersRequiresAuth(t *testing.T) {
	t.Parallel()

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/copy-traders", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHandlerListsCopyTradersEmpty(t *testing.T) {
	t.Parallel()

	fake := doraClientFunc{
		listCopyTraders: func(_ context.Context) ([]strategyhttp.CopyTrader, error) {
			return nil, nil
		},
	}

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(fake),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/copy-traders", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Items []strategyhttp.CopyTraderSummary `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.NotNil(t, body.Items)
	require.Empty(t, body.Items)
}

func TestHandlerListsCopyTradersDORAError(t *testing.T) {
	t.Parallel()

	fake := doraClientFunc{
		listCopyTraders: func(_ context.Context) ([]strategyhttp.CopyTrader, error) {
			return nil, assert.AnError
		},
	}

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(fake),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/copy-traders", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandlerListsStrategies(t *testing.T) {
	t.Parallel()

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp struct {
		Items []strategyhttp.StrategySummary `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Items, 3)
	byType := map[string]strategyhttp.StrategySummary{}
	for _, it := range resp.Items {
		byType[it.Type] = it
		t.Logf("got strategy type=%q status=%q fields=%d", it.Type, it.Status, len(it.ConfigFields))
	}
	t.Logf("raw body: %s", rec.Body.String())

	breakout, ok := byType["breakout"]
	require.True(t, ok, "breakout should be in the strategies list")
	assert.Equal(t, "available", breakout.Status)
	require.Len(t, breakout.ConfigFields, 15)
	assert.Equal(t, "short_vol_window", breakout.ConfigFields[0].Name)
	assert.Equal(t, "long_vol_window", breakout.ConfigFields[1].Name)
	assert.Equal(t, "compression_threshold", breakout.ConfigFields[2].Name)
	assert.Equal(t, "atr_window", breakout.ConfigFields[3].Name)
	assert.Equal(t, "breakout_atr_multiple", breakout.ConfigFields[4].Name)
	assert.Equal(t, "confirmation_bars", breakout.ConfigFields[5].Name)
	assert.Equal(t, "stop_loss_atr", breakout.ConfigFields[6].Name)
	assert.Equal(t, "take_profit_atr", breakout.ConfigFields[7].Name)
	assert.Equal(t, "min_long_vol_floor", breakout.ConfigFields[8].Name)
	assert.Equal(t, "obv_trend_threshold", breakout.ConfigFields[9].Name)
	assert.Equal(t, "obv_window", breakout.ConfigFields[10].Name)
	assert.Equal(t, "order_book_id", breakout.ConfigFields[11].Name)
	assert.Equal(t, "tenor", breakout.ConfigFields[12].Name)
	assert.Equal(t, "initial_balance", breakout.ConfigFields[13].Name)
	assert.Equal(t, "leverage", breakout.ConfigFields[14].Name)
	assert.True(t, breakout.SupportsRun)
	assert.True(t, breakout.SupportsBacktest)

	copytrading, ok := byType["copytrading"]
	require.True(t, ok, "copytrading should be in the strategies list")
	assert.Equal(t, "available", copytrading.Status)
	require.Len(t, copytrading.ConfigFields, 7)
	assert.Equal(t, "followed_trader", copytrading.ConfigFields[0].Name)
	assert.Equal(t, "initial_balance", copytrading.ConfigFields[6].Name)
	assert.Equal(t, "number", copytrading.ConfigFields[6].Type)
	assert.False(t, copytrading.ConfigFields[6].Required)
	assert.True(t, copytrading.ConfigFields[0].Required)
	assert.Equal(t, "percentage_of_available", copytrading.ConfigFields[1].Name)
	assert.Equal(t, "number", copytrading.ConfigFields[1].Type)
	assert.True(t, copytrading.ConfigFields[1].Required)
	assert.Equal(t, "leverage", copytrading.ConfigFields[2].Name)
	assert.Equal(t, "number", copytrading.ConfigFields[2].Type)
	assert.True(t, copytrading.ConfigFields[2].Required)
	assert.Equal(t, "min_order_size", copytrading.ConfigFields[3].Name)
	assert.False(t, copytrading.ConfigFields[3].Required)
	assert.Equal(t, "max_order_size", copytrading.ConfigFields[4].Name)
	assert.False(t, copytrading.ConfigFields[4].Required)
	assert.Equal(t, "disallowed_bonds", copytrading.ConfigFields[5].Name)
	assert.False(t, copytrading.ConfigFields[5].Required)

	mr, ok := byType["mean_reversion"]
	require.True(t, ok, "mean_reversion should be in the strategies list")
	assert.Equal(t, "available", mr.Status)
	require.Len(t, mr.ConfigFields, 10)
	assert.Equal(t, "lookback_window", mr.ConfigFields[0].Name)
	assert.Equal(t, float64(20), mr.ConfigFields[0].Default)
	assert.Equal(t, "order_book_id", mr.ConfigFields[6].Name)
	assert.Equal(t, "tenor", mr.ConfigFields[7].Name)
	assert.Equal(t, float64(1), mr.ConfigFields[8].Default)
	assert.Equal(t, float64(1), mr.ConfigFields[9].Default)
}

func TestHandlerListsTenors(t *testing.T) {
	t.Parallel()

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/tenors", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp struct {
		Items []strategyhttp.TenorSummary `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Items, 11)
	assert.Equal(t, "1M", resp.Items[0].Code)
	assert.Equal(t, "1 Month Treasury", resp.Items[0].Description)
	assert.Equal(t, "10Y", resp.Items[8].Code)
	assert.Equal(t, "10 Year Treasury", resp.Items[8].Description)
	assert.Equal(t, "30Y", resp.Items[10].Code)
	assert.Equal(t, "30 Year Treasury", resp.Items[10].Description)

	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/tenors", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandlerListsDORAOrderBooks(t *testing.T) {
	t.Parallel()

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(doraClientFunc{
			listOrderBooks: func(ctx context.Context) ([]strategyhttp.DORAOrderBookSummary, error) {
				return []strategyhttp.DORAOrderBookSummary{{
					ID:           "book-1",
					DisplayName:  "UST 10Y / USD",
					BaseAssetID:  "asset-base",
					QuoteAssetID: "asset-quote",
					Status:       "ACTIVE",
				}}, nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/dora/orderbooks", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp struct {
		Items []strategyhttp.DORAOrderBookSummary `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "book-1", resp.Items[0].ID)
	assert.Equal(t, "UST 10Y / USD", resp.Items[0].DisplayName)
	assert.Equal(t, "asset-base", resp.Items[0].BaseAssetID)
	assert.Equal(t, "asset-quote", resp.Items[0].QuoteAssetID)
	assert.Equal(t, "ACTIVE", resp.Items[0].Status)

	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/dora/orderbooks", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandlerReturnsDORAOrderBookError(t *testing.T) {
	t.Parallel()

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(doraClientFunc{
			listOrderBooks: func(ctx context.Context) ([]strategyhttp.DORAOrderBookSummary, error) {
				return nil, fmt.Errorf("boom")
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/dora/orderbooks", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "list DORA order books: boom")
}

func TestHandlerGetsDORAUser(t *testing.T) {
	t.Parallel()

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(ctx context.Context) (string, error) {
				return "user-123", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/dora/user", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp strategyhttp.DORAUserSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "user-123", resp.ID)

	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/dora/user", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandlerReturnsDORAUserError(t *testing.T) {
	t.Parallel()

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(ctx context.Context) (string, error) {
				return "", fmt.Errorf("boom")
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/dora/user", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "unauthorised")
}

func TestHandlerCreateAndGetBacktest(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	resultCh := make(chan types.BacktestResult, 1)
	svc := &strategyfakes.FakeService{
		RunBacktestStub: func(ctx context.Context, _ uuid.UUID, strat strategycore.Strategy, start, end time.Time) (<-chan types.BacktestResult, error) {
			return resultCh, nil
		},
	}
	handler := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithNow(func() time.Time { return now }),
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(context.Context) (string, error) {
				return "user-test-1", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	body := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window":   20,
			"entry_z_score":     2.0,
			"exit_z_score":      0.5,
			"stop_loss_z_score": 3.5,
			"min_std_dev":       0.0005,
			"max_position_size": 1.0,
		},
		"start": now.Add(-24 * time.Hour).Format(time.RFC3339),
		"end":   now.Format(time.RFC3339),
	}

	rec := performJSONRequest(t, handler, "/v1/backtests", body)
	require.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, 1, svc.RunBacktestCallCount())

	var accepted strategyhttp.BacktestDetail
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &accepted))
	assert.JSONEq(t, `{"lookback_window":20,"entry_z_score":2,"exit_z_score":0.5,"stop_loss_z_score":3.5,"min_std_dev":0.0005,"max_position_size":1}`, string(accepted.Config))
	assert.Equal(t, "user-test-1", accepted.DORAUserID)
	backtestID := accepted.ID

	resultCh <- meanreversion.BacktestResult{
		ClosedTrades: []meanreversion.ClosedTrade{{
			BondID:       "BOND-1",
			OpenTime:     now.Add(-2 * time.Hour),
			CloseTime:    now.Add(-time.Hour),
			Signal:       types.SignalBuy,
			ExitSignal:   types.SignalHold,
			EntrySpread:  decimal.MustNew(12, 3),
			ExitSpread:   decimal.MustNew(8, 3),
			EntryZScore:  decimal.MustNew(20, 1),
			ExitZScore:   decimal.MustNew(5, 1),
			PositionSize: decimal.MustNew(5, 1),
			PnL:          decimal.MustNew(2, 3),
			EntryPrice:   decimal.MustNew(100, 0),
			ExitPrice:    decimal.MustNew(102, 0),
			Quantity:     decimal.MustNew(5, 0),
		}},
		TotalPnL:    decimal.MustNew(2, 3),
		WinCount:    1,
		LossCount:   0,
		MaxDrawdown: decimal.Zero,
		SharpeRatio: decimal.MustNew(11, 1),
	}

	require.Eventually(t, func() bool {
		rec = httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests/"+backtestID.String()+"/metadata", nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return false
		}
		var summary strategyhttp.BacktestSummary
		if err := json.Unmarshal(rec.Body.Bytes(), &summary); err != nil {
			return false
		}
		return summary.Status == "completed"
	}, time.Second, 10*time.Millisecond)
}

func TestHandlerFailedBacktestIncludesError(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	resultCh := make(chan types.BacktestResult, 1)
	svc := &strategyfakes.FakeService{
		RunBacktestStub: func(ctx context.Context, _ uuid.UUID, strat strategycore.Strategy, start, end time.Time) (<-chan types.BacktestResult, error) {
			return resultCh, nil
		},
	}
	handler := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithNow(func() time.Time { return now }),
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(context.Context) (string, error) {
				return "user-test-1", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	body := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window":   20,
			"entry_z_score":     2.0,
			"exit_z_score":      0.5,
			"stop_loss_z_score": 3.5,
			"min_std_dev":       0.0005,
			"max_position_size": 1.0,
		},
		"start": now.Add(-24 * time.Hour).Format(time.RFC3339),
		"end":   now.Format(time.RFC3339),
	}

	rec := performJSONRequest(t, handler, "/v1/backtests", body)
	require.Equal(t, http.StatusAccepted, rec.Code)
	var accepted strategyhttp.BacktestDetail
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &accepted))
	backtestID := accepted.ID

	resultCh <- types.ErrorResult{Err: fmt.Errorf("observation load failed")}

	// Full GET endpoint should include the error.
	require.Eventually(t, func() bool {
		rec = httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests/"+backtestID.String(), nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return false
		}
		var result strategyhttp.BacktestResultSummary
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			return false
		}
		return result.Status == "failed" && result.Error == "observation load failed"
	}, time.Second, 10*time.Millisecond)

	// Metadata endpoint should also include the error.
	rec = httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests/"+backtestID.String()+"/metadata", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var summary strategyhttp.BacktestSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &summary))
	assert.Equal(t, "failed", summary.Status)
	assert.Equal(t, "observation load failed", summary.Error)

	// List endpoint should also include the error.
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp struct {
		Items []strategyhttp.BacktestSummary `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Items, 1)
	assert.Equal(t, "failed", listResp.Items[0].Status)
	assert.Equal(t, "observation load failed", listResp.Items[0].Error)
}

func TestHandlerCopyTradingBacktestResultShape(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	openTradeID := uuid.Must(uuid.NewV7())
	closeTradeID := uuid.Must(uuid.NewV7())
	resultCh := make(chan types.BacktestResult, 1)
	svc := &strategyfakes.FakeService{
		RunBacktestStub: func(ctx context.Context, _ uuid.UUID, strat strategycore.Strategy, start, end time.Time) (<-chan types.BacktestResult, error) {
			return resultCh, nil
		},
	}
	store := &memoryBacktestStore{}
	handler := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithNow(func() time.Time { return now }),
		strategyhttp.WithBacktestStore(store),
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(context.Context) (string, error) {
				return "user-test-1", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	followed := uuid.Must(uuid.NewV7())
	body := map[string]any{
		"strategy_type": "copytrading",
		"config": map[string]any{
			"followed_trader":         followed.String(),
			"percentage_of_available": 0.5,
			"leverage":                1.0,
			"min_order_size":          0,
			"max_order_size":          0,
			"disallowed_bonds":        []string{},
		},
		"start": now.Add(-24 * time.Hour).Format(time.RFC3339),
		"end":   now.Format(time.RFC3339),
	}
	rec := performJSONRequest(t, handler, "/v1/backtests", body)
	require.Equal(t, http.StatusAccepted, rec.Code)
	var accepted strategyhttp.BacktestDetail
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &accepted))
	backtestID := accepted.ID

	// Per-trade rows now flow through the writer interface, not the
	// result channel. Push them onto the in-memory store directly so
	// the /trades endpoint has data to return.
	require.NoError(t, store.WriteTradeRecord(context.Background(), stats.TradeRecordInsert{
		BacktestID:   backtestID,
		Time:         now.Add(-2 * time.Hour),
		BondID:       "BOND-1",
		Signal:       "BUY",
		Price:        decimal.MustNew(100, 0),
		Quantity:     decimal.MustNew(10, 0),
		OrderSize:    decimal.MustNew(1000, 0),
		Cash:         decimal.MustNew(0, 0),
		OpenPosition: decimal.MustNew(10, 0),
		TradeID:      openTradeID,
	}))
	require.NoError(t, store.WriteClosedTrade(context.Background(), stats.ClosedTradeInsert{
		BacktestID:   backtestID,
		OpenTime:     now.Add(-2 * time.Hour),
		CloseTime:    now.Add(-time.Hour),
		BondID:       "BOND-1",
		OpenSignal:   "BUY",
		CloseSignal:  "SELL",
		Quantity:     decimal.MustNew(10, 0),
		EntryPrice:   decimal.MustNew(100, 0),
		ExitPrice:    decimal.MustNew(120, 0),
		PnL:          decimal.MustNew(200, 0),
		EntryBalance: decimal.MustNew(9000, 0),
		OpenTradeID:  openTradeID,
		CloseTradeID: closeTradeID,
	}))

	resultCh <- copytrading.BacktestResult{
		TotalPnL:    decimal.MustNew(200, 0),
		WinCount:    1,
		LossCount:   0,
		MaxDrawdown: decimal.Zero,
		SharpeRatio: decimal.MustNew(11, 1),
	}

	var bodyBytes []byte
	require.Eventually(t, func() bool {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests/"+backtestID.String(), nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return false
		}
		bodyBytes = rec.Body.Bytes()
		var summary strategyhttp.BacktestResultSummary
		if err := json.Unmarshal(bodyBytes, &summary); err != nil {
			return false
		}
		return summary.Status == "completed" && summary.SharpeRatio != ""
	}, time.Second, 10*time.Millisecond)

	var summary strategyhttp.BacktestResultSummary
	require.NoError(t, json.Unmarshal(bodyBytes, &summary))
	assert.Equal(t, "completed", summary.Status)
	assert.Equal(t, "copytrading", summary.StrategyType)
	assert.Equal(t, "200", summary.TotalPnL)
	assert.Equal(t, 1, summary.WinCount)
	assert.Equal(t, 0, summary.LossCount)
	assert.Equal(t, "1.1", summary.SharpeRatio)

	rec = httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests/"+backtestID.String()+"/trades?limit=10", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var tradesResp struct {
		Items []strategyhttp.CopyTradingTradeRecord `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tradesResp))
	require.Len(t, tradesResp.Items, 1)
	assert.Equal(t, "BOND-1", tradesResp.Items[0].BondID)
	assert.Equal(t, "10", tradesResp.Items[0].Quantity)
	assert.Equal(t, "1000", tradesResp.Items[0].OrderSize)
	assert.Equal(t, "10", tradesResp.Items[0].OpenPosition)

	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests/"+backtestID.String()+"/closed-trades?limit=10", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var closedResp struct {
		Items []strategyhttp.CopyTradingClosedTrade `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &closedResp))
	require.Len(t, closedResp.Items, 1)
	assert.Equal(t, "BOND-1", closedResp.Items[0].BondID)
	assert.Equal(t, "10", closedResp.Items[0].Quantity)
	assert.Equal(t, "200", closedResp.Items[0].PnL)
	assert.Equal(t, "BUY", closedResp.Items[0].OpenSignal)
	assert.Equal(t, "SELL", closedResp.Items[0].CloseSignal)
}

func TestHandlerRejectsCopyTradingBacktestMissingRequiredFields(t *testing.T) {
	t.Parallel()

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	rec := performJSONRequest(t, handler, "/v1/backtests", map[string]any{
		"strategy_type": "copytrading",
		"config": map[string]any{
			"followed_trader": uuid.Must(uuid.NewV7()).String(),
		},
		"start": time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
		"end":   time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "percentage_of_available")
}

func TestHandlerCancelBacktest(t *testing.T) {
	t.Parallel()

	resultCh := make(chan types.BacktestResult)
	svc := &strategyfakes.FakeService{
		RunBacktestStub: func(ctx context.Context, _ uuid.UUID, strat strategycore.Strategy, start, end time.Time) (<-chan types.BacktestResult, error) {
			return resultCh, nil
		},
	}
	handler := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(context.Context) (string, error) {
				return "user-test-1", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	body := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window":   20,
			"entry_z_score":     2.0,
			"exit_z_score":      0.5,
			"stop_loss_z_score": 3.5,
			"min_std_dev":       0.0005,
			"max_position_size": 1.0,
		},
		"start": time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
		"end":   time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
	}
	rec2 := performJSONRequest(t, handler, "/v1/backtests", body)
	require.Equal(t, http.StatusAccepted, rec2.Code)
	var accepted strategyhttp.BacktestDetail
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &accepted))
	backtestID := accepted.ID

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodDelete, "/v1/backtests/"+backtestID.String(), nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, svc.StopBacktestCallCount())
	var summary strategyhttp.BacktestSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &summary))
	assert.Equal(t, "cancelled", summary.Status)
}

func TestHandlerListBacktests(t *testing.T) {
	t.Parallel()

	resultCh1 := make(chan types.BacktestResult)
	resultCh2 := make(chan types.BacktestResult)
	call := 0
	svc := &strategyfakes.FakeService{
		RunBacktestStub: func(ctx context.Context, _ uuid.UUID, strat strategycore.Strategy, start, end time.Time) (<-chan types.BacktestResult, error) {
			call++
			if call == 1 {
				return resultCh1, nil
			}
			return resultCh2, nil
		},
	}
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	handler := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithNow(func() time.Time {
			now = now.Add(time.Second)
			return now
		}),
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(context.Context) (string, error) {
				return "user-test-1", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	body := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window": 20,
			"entry_z_score":   2.0,
			"exit_z_score":    0.5,
		},
		"start": time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
		"end":   time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
	}
	firstRec := performJSONRequest(t, handler, "/v1/backtests", body)
	secondRec := performJSONRequest(t, handler, "/v1/backtests", body)
	var firstAccepted, secondAccepted strategyhttp.BacktestDetail
	require.NoError(t, json.Unmarshal(firstRec.Body.Bytes(), &firstAccepted))
	require.NoError(t, json.Unmarshal(secondRec.Body.Bytes(), &secondAccepted))
	firstID := firstAccepted.ID
	secondID := secondAccepted.ID

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp struct {
		Items []strategyhttp.BacktestSummary `json:"items"`
		Page  int                            `json:"page"`
		Limit int                            `json:"limit"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Items, 2)
	assert.Equal(t, secondID, resp.Items[0].ID)
	assert.Equal(t, firstID, resp.Items[1].ID)
	assert.Equal(t, 1, resp.Page)
	assert.Equal(t, 10, resp.Limit)
}

func TestHandlerCreateAndControlRun(t *testing.T) {
	t.Parallel()

	runID := uuid.Must(uuid.NewV7())
	svc := &strategyfakes.FakeService{
		RunStrategyStub: func(ctx context.Context, strat strategycore.Strategy) (uuid.UUID, error) {
			return runID, nil
		},
	}
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	handler := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithNow(func() time.Time {
			now = now.Add(time.Second)
			return now
		}),
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	body := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window": 20,
			"entry_z_score":   2.0,
			"exit_z_score":    0.5,
			"order_book_id":   uuid.Must(uuid.NewV7()).String(),
			"tenor":           "10Y",
			"initial_balance": 5.5,
			"leverage":        2.0,
		},
	}
	rec := performJSONRequest(t, handler, "/v1/runs", body)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, svc.RunStrategyCallCount())

	var created strategyhttp.RunDetail
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	assert.Equal(t, "test-user", created.DORAUserID)
	cfg, _ := body["config"].(map[string]any)
	orderBookID, _ := cfg["order_book_id"].(string)
	assert.JSONEq(t, fmt.Sprintf(
		`{"lookback_window":20,"entry_z_score":2,"exit_z_score":0.5,"stop_loss_z_score":3.5,"min_std_dev":0.0005,"max_position_size":1,"order_book_id":%q,"tenor":"10Y","initial_balance":5.5,"leverage":2}`,
		orderBookID,
	), string(created.Config))

	rec = httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/runs/"+runID.String()+"/pause", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, svc.PauseStrategyCallCount())

	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/runs/"+runID.String()+"/resume", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, svc.ResumeStrategyCallCount())

	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodDelete, "/v1/runs/"+runID.String(), nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, svc.StopStrategyCallCount())

	var detail strategyhttp.RunDetail
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &detail))
	assert.Equal(t, "stopped", detail.Status)
	assert.NotNil(t, detail.StoppedAt)
}

func TestHandlerRejectsCopyTradingRunMissingRequiredFields(t *testing.T) {
	t.Parallel()

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	rec := performJSONRequest(t, handler, "/v1/runs", map[string]any{
		"strategy_type": "copytrading",
		"config": map[string]any{
			"followed_trader": uuid.Must(uuid.NewV7()).String(),
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "percentage_of_available")
}

func TestHandlerListRuns(t *testing.T) {
	t.Parallel()

	firstID := uuid.Must(uuid.NewV7())
	secondID := uuid.Must(uuid.NewV7())
	call := 0
	svc := &strategyfakes.FakeService{
		RunStrategyStub: func(ctx context.Context, strat strategycore.Strategy) (uuid.UUID, error) {
			call++
			if call == 1 {
				return firstID, nil
			}
			return secondID, nil
		},
	}
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	handler := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithNow(func() time.Time {
			now = now.Add(time.Second)
			return now
		}),
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	body := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window": 20,
			"entry_z_score":   2.0,
			"exit_z_score":    0.5,
		},
	}
	performJSONRequest(t, handler, "/v1/runs", body)
	performJSONRequest(t, handler, "/v1/runs", body)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/runs", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp struct {
		Items []strategyhttp.RunSummary `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Items, 2)
	assert.Equal(t, secondID, resp.Items[0].ID)
	assert.Equal(t, firstID, resp.Items[1].ID)
}

func TestHandlerRejectsDuplicateOrderBookRun(t *testing.T) {
	t.Parallel()

	runID := uuid.Must(uuid.NewV7())
	orderBookID := uuid.Must(uuid.NewV7()).String()

	svc := &strategyfakes.FakeService{
		RunStrategyStub: func(ctx context.Context, strat strategycore.Strategy) (uuid.UUID, error) {
			return runID, nil
		},
	}
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	handler := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithNow(func() time.Time {
			now = now.Add(time.Second)
			return now
		}),
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	// First run should succeed.
	body := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window": 20,
			"entry_z_score":   2.0,
			"exit_z_score":    0.5,
			"order_book_id":   orderBookID,
			"tenor":           "10Y",
			"initial_balance": 5.5,
			"leverage":        2.0,
		},
	}
	rec := performJSONRequest(t, handler, "/v1/runs", body)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, svc.RunStrategyCallCount())

	// Second run with the same order_book_id should fail with 409.
	rec = performJSONRequest(t, handler, "/v1/runs", body)
	require.Equal(t, http.StatusConflict, rec.Code)
	assert.Equal(t, 1, svc.RunStrategyCallCount()) // service was NOT called
	var errResp strategyhttp.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Contains(t, errResp.Error, "already active")
}

func TestHandlerAllowsDifferentOrderBookRun(t *testing.T) {
	t.Parallel()

	firstID := uuid.Must(uuid.NewV7())
	secondID := uuid.Must(uuid.NewV7())
	call := 0
	svc := &strategyfakes.FakeService{
		RunStrategyStub: func(ctx context.Context, strat strategycore.Strategy) (uuid.UUID, error) {
			call++
			if call == 1 {
				return firstID, nil
			}
			return secondID, nil
		},
	}
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	handler := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithNow(func() time.Time {
			now = now.Add(time.Second)
			return now
		}),
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	// First run with order book A.
	bodyA := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window": 20,
			"entry_z_score":   2.0,
			"exit_z_score":    0.5,
			"order_book_id":   uuid.Must(uuid.NewV7()).String(),
			"tenor":           "10Y",
			"initial_balance": 5.5,
			"leverage":        2.0,
		},
	}
	rec := performJSONRequest(t, handler, "/v1/runs", bodyA)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Second run with a different order book should succeed.
	bodyB := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window": 20,
			"entry_z_score":   2.0,
			"exit_z_score":    0.5,
			"order_book_id":   uuid.Must(uuid.NewV7()).String(),
			"tenor":           "5Y",
			"initial_balance": 5.5,
			"leverage":        2.0,
		},
	}
	rec = performJSONRequest(t, handler, "/v1/runs", bodyB)
	require.Equal(t, http.StatusCreated, rec.Code)
}

func TestHandlerAllowsRunAfterPreviousStopped(t *testing.T) {
	t.Parallel()

	runID := uuid.Must(uuid.NewV7())
	orderBookID := uuid.Must(uuid.NewV7()).String()

	svc := &strategyfakes.FakeService{
		RunStrategyStub: func(ctx context.Context, strat strategycore.Strategy) (uuid.UUID, error) {
			return runID, nil
		},
	}
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	handler := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithNow(func() time.Time {
			now = now.Add(time.Second)
			return now
		}),
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	body := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window": 20,
			"entry_z_score":   2.0,
			"exit_z_score":    0.5,
			"order_book_id":   orderBookID,
			"tenor":           "10Y",
			"initial_balance": 5.5,
			"leverage":        2.0,
		},
	}
	rec := performJSONRequest(t, handler, "/v1/runs", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Stop the run.
	rec = httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodDelete, "/v1/runs/"+runID.String(), nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Now creating a new run with the same order book should succeed.
	rec = performJSONRequest(t, handler, "/v1/runs", body)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 2, svc.RunStrategyCallCount())
}

func TestHandlerRestoreRuns(t *testing.T) {
	t.Parallel()

	runningID := uuid.Must(uuid.NewV7())
	pausedID := uuid.Must(uuid.NewV7())
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	store := &memoryRunStore{
		runs: map[uuid.UUID]*strategyhttp.RunDetail{
			runningID: {
				RunSummary: strategyhttp.RunSummary{
					ID:           runningID,
					DORAUserID:   "test-user",
					StrategyType: "mean_reversion",
					Status:       "running",
					CreatedAt:    now,
					UpdatedAt:    now,
				},
				Config: json.RawMessage(`{"lookback_window":20,"entry_z_score":2,"exit_z_score":0.5,"stop_loss_z_score":3.5,"min_std_dev":0.0005,"max_position_size":1}`),
			},
			pausedID: {
				RunSummary: strategyhttp.RunSummary{
					ID:           pausedID,
					DORAUserID:   "test-user",
					StrategyType: "mean_reversion",
					Status:       "paused",
					CreatedAt:    now.Add(-time.Minute),
					UpdatedAt:    now.Add(-time.Minute),
				},
				Config: json.RawMessage(`{"lookback_window":20,"entry_z_score":2,"exit_z_score":0.5,"stop_loss_z_score":3.5,"min_std_dev":0.0005,"max_position_size":1}`),
			},
		},
	}

	svc := strategycore.NewService()
	pricesHandler := prices.New(prices.Config{})
	handlerAny := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithRunStore(store),
		strategyhttp.WithPricesHandler(pricesHandler),
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	restorer, ok := handlerAny.(interface{ RestoreRuns(context.Context) error })
	require.True(t, ok)
	require.NoError(t, restorer.RestoreRuns(context.Background()))

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/runs", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handlerAny.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Items []strategyhttp.RunSummary `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Items, 2)
	ids := []uuid.UUID{resp.Items[0].ID, resp.Items[1].ID}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	expected := []uuid.UUID{pausedID, runningID}
	sort.Slice(expected, func(i, j int) bool { return expected[i].String() < expected[j].String() })
	assert.Equal(t, expected, ids)

	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/runs/"+pausedID.String()+"/resume", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handlerAny.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var detail strategyhttp.RunDetail
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &detail))
	assert.Equal(t, "running", detail.Status)
	assert.Equal(t, "running", store.runs[pausedID].Status)
}

func TestHandlerRestoreBacktests(t *testing.T) {
	t.Parallel()

	completedID := uuid.Must(uuid.NewV7())
	failedID := uuid.Must(uuid.NewV7())
	otherUserID := "user-other"
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	completedAt := now.Add(30 * time.Second)
	store := &memoryBacktestStore{
		backtests: map[uuid.UUID]*strategyhttp.BacktestDetail{
			completedID: {
				BacktestSummary: strategyhttp.BacktestSummary{
					ID:           completedID,
					DORAUserID:   "user-test-1",
					StrategyType: "mean_reversion",
					Status:       "completed",
					Config:       json.RawMessage(`{"lookback_window":20}`),
					CreatedAt:    now,
					CompletedAt:  &completedAt,
				},
				Start: now.Add(-time.Hour),
				End:   now,
				Result: mustMarshalResult(t, strategyhttp.MeanReversionBacktestResult{
					TotalPnL:    "0.05",
					WinCount:    3,
					LossCount:   1,
					MaxDrawdown: "0.01",
					SharpeRatio: "1.5",
				}),
			},
			failedID: {
				BacktestSummary: strategyhttp.BacktestSummary{
					ID:           failedID,
					DORAUserID:   "user-test-1",
					StrategyType: "mean_reversion",
					Status:       "failed",
					Config:       json.RawMessage(`{"lookback_window":10}`),
					CreatedAt:    now.Add(-time.Minute),
					CompletedAt:  &completedAt,
					Error:        "something went wrong",
				},
				Start: now.Add(-2 * time.Hour),
				End:   now.Add(-time.Hour),
			},
			uuid.Must(uuid.NewV7()): {
				BacktestSummary: strategyhttp.BacktestSummary{
					ID:           uuid.Must(uuid.NewV7()),
					DORAUserID:   otherUserID,
					StrategyType: "mean_reversion",
					Status:       "completed",
					Config:       json.RawMessage(`{"lookback_window":5}`),
					CreatedAt:    now.Add(-2 * time.Hour),
					CompletedAt:  &completedAt,
				},
				Start: now.Add(-3 * time.Hour),
				End:   now.Add(-2 * time.Hour),
			},
		},
	}

	svc := strategycore.NewService()
	pricesHandler := prices.New(prices.Config{})
	handlerAny := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithBacktestStore(store),
		strategyhttp.WithPricesHandler(pricesHandler),
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(context.Context) (string, error) {
				return "user-test-1", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	restorer, ok := handlerAny.(interface{ RestoreBacktests(context.Context) error })
	require.True(t, ok)
	require.NoError(t, restorer.RestoreBacktests(context.Background()))

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handlerAny.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Items []strategyhttp.BacktestSummary `json:"items"`
		Page  int                            `json:"page"`
		Limit int                            `json:"limit"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Items, 2)
	ids := []uuid.UUID{resp.Items[0].ID, resp.Items[1].ID}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	expected := []uuid.UUID{failedID, completedID}
	sort.Slice(expected, func(i, j int) bool { return expected[i].String() < expected[j].String() })
	assert.Equal(t, expected, ids)
	assert.Equal(t, 1, resp.Page)
	assert.Equal(t, 10, resp.Limit)
}

func TestHandlerListBacktestsWithFilters(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	completedAt := now.Add(30 * time.Second)
	id1 := uuid.Must(uuid.NewV7())
	id2 := uuid.Must(uuid.NewV7())
	id3 := uuid.Must(uuid.NewV7())

	store := &memoryBacktestStore{
		backtests: map[uuid.UUID]*strategyhttp.BacktestDetail{
			id1: {
				BacktestSummary: strategyhttp.BacktestSummary{
					ID:           id1,
					DORAUserID:   "user-1",
					StrategyType: "mean_reversion",
					Status:       "completed",
					Config:       json.RawMessage(`{"lookback_window":20}`),
					CreatedAt:    now,
					CompletedAt:  &completedAt,
				},
				Start: now.Add(-time.Hour),
				End:   now,
			},
			id2: {
				BacktestSummary: strategyhttp.BacktestSummary{
					ID:           id2,
					DORAUserID:   "user-1",
					StrategyType: "mean_reversion",
					Status:       "failed",
					Config:       json.RawMessage(`{"lookback_window":10}`),
					CreatedAt:    now.Add(-24 * time.Hour),
					Error:        "error occurred",
				},
				Start: now.Add(-25 * time.Hour),
				End:   now.Add(-24 * time.Hour),
			},
			id3: {
				BacktestSummary: strategyhttp.BacktestSummary{
					ID:           id3,
					DORAUserID:   "user-1",
					StrategyType: "trend_following",
					Status:       "running",
					Config:       json.RawMessage(`{"lookback_window":5}`),
					CreatedAt:    now.Add(-48 * time.Hour),
				},
				Start: now.Add(-49 * time.Hour),
				End:   now.Add(-48 * time.Hour),
			},
		},
	}

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithBacktestStore(store),
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(context.Context) (string, error) {
				return "user-1", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	restorer, ok := handler.(interface{ RestoreBacktests(context.Context) error })
	require.True(t, ok)
	require.NoError(t, restorer.RestoreBacktests(context.Background()))

	t.Run("status filter completed", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests?status=completed", nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.BacktestSummary `json:"items"`
			Page  int                            `json:"page"`
			Limit int                            `json:"limit"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.Items, 1)
		assert.Equal(t, id1, resp.Items[0].ID)
	})

	t.Run("status filter multiple", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests?status=completed,failed", nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.BacktestSummary `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.Items, 2)
	})

	t.Run("from filter", func(t *testing.T) {
		rec := httptest.NewRecorder()
		from := now.Add(-12 * time.Hour).Format(time.RFC3339)
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests?from="+from, nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.BacktestSummary `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.Items, 1)
		assert.Equal(t, id1, resp.Items[0].ID)
	})

	t.Run("to filter", func(t *testing.T) {
		rec := httptest.NewRecorder()
		to := now.Add(-12 * time.Hour).Format(time.RFC3339)
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests?to="+to, nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.BacktestSummary `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.Items, 2)
	})

	t.Run("from and to filter", func(t *testing.T) {
		rec := httptest.NewRecorder()
		from := now.Add(-36 * time.Hour).Format(time.RFC3339)
		to := now.Add(-12 * time.Hour).Format(time.RFC3339)
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests?from="+from+"&to="+to, nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.BacktestSummary `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.Items, 1)
		assert.Equal(t, id2, resp.Items[0].ID)
	})

	t.Run("combined status and date filters", func(t *testing.T) {
		rec := httptest.NewRecorder()
		from := now.Add(-12 * time.Hour).Format(time.RFC3339)
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests?status=completed&from="+from, nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.BacktestSummary `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.Items, 1)
		assert.Equal(t, id1, resp.Items[0].ID)
	})

	t.Run("pagination page 1 limit 1", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests?page=1&limit=1", nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.BacktestSummary `json:"items"`
			Page  int                            `json:"page"`
			Limit int                            `json:"limit"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.Items, 1)
		assert.Equal(t, id1, resp.Items[0].ID) // most recent first
		assert.Equal(t, 1, resp.Page)
		assert.Equal(t, 1, resp.Limit)
	})

	t.Run("pagination page 2 limit 1", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests?page=2&limit=1", nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.BacktestSummary `json:"items"`
			Page  int                            `json:"page"`
			Limit int                            `json:"limit"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.Items, 1)
		assert.Equal(t, id2, resp.Items[0].ID)
		assert.Equal(t, 2, resp.Page)
		assert.Equal(t, 1, resp.Limit)
	})

	t.Run("pagination beyond results returns empty", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests?page=10&limit=1", nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.BacktestSummary `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Empty(t, resp.Items)
	})

	t.Run("limit capped at 50", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests?limit=100", nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.BacktestSummary `json:"items"`
			Limit int                            `json:"limit"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.Items, 3)
		assert.Equal(t, 50, resp.Limit)
	})

	t.Run("from with date-only format", func(t *testing.T) {
		rec := httptest.NewRecorder()
		from := now.Format("2006-01-02")
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests?from="+from, nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.BacktestSummary `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		// id1 is today, id2 is yesterday, id3 is 2 days ago — only id1 passes.
		require.Len(t, resp.Items, 1)
		assert.Equal(t, id1, resp.Items[0].ID)
	})

	t.Run("to with date-only format", func(t *testing.T) {
		rec := httptest.NewRecorder()
		to := now.Add(-24 * time.Hour).Format("2006-01-02")
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests?to="+to, nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.BacktestSummary `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		// to=2026-04-23 means CreatedAt <= 2026-04-23T00:00:00Z,
		// so only id3 (created 2 days ago) is included.
		require.Len(t, resp.Items, 1)
		assert.Equal(t, id3, resp.Items[0].ID)
	})
}

func TestHandlerBacktestOwnership(t *testing.T) {
	t.Parallel()

	btID := uuid.Must(uuid.NewV7())
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	store := &memoryBacktestStore{
		backtests: map[uuid.UUID]*strategyhttp.BacktestDetail{
			btID: {
				BacktestSummary: strategyhttp.BacktestSummary{
					ID:           btID,
					DORAUserID:   "user-alice",
					StrategyType: "mean_reversion",
					Status:       "completed",
					Config:       json.RawMessage(`{"lookback_window":20}`),
					CreatedAt:    now,
				},
				Start: now.Add(-time.Hour),
				End:   now,
			},
		},
	}

	svc := strategycore.NewService()
	pricesHandler := prices.New(prices.Config{})
	handlerAny := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithBacktestStore(store),
		strategyhttp.WithPricesHandler(pricesHandler),
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(context.Context) (string, error) {
				return "user-bob", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	restorer, ok := handlerAny.(interface{ RestoreBacktests(context.Context) error })
	require.True(t, ok)
	require.NoError(t, restorer.RestoreBacktests(context.Background()))

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests/"+btID.String(), nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handlerAny.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)

	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodDelete, "/v1/backtests/"+btID.String(), nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handlerAny.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)

	// /metadata endpoint also enforces ownership.
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests/"+btID.String()+"/metadata", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handlerAny.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestHandlerValidationErrors(t *testing.T) {
	t.Parallel()

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	rec := performJSONRequest(t, handler, "/v1/runs", map[string]any{
		"strategy_type": "unsupported",
		"config":        map[string]any{},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	rec = httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/runs/not-a-uuid", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	rec = performJSONRequest(t, handler, "/v1/runs", map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"initial_balance": 0,
		},
	})
	// Runs allow initial_balance=0 because it's populated from DORA positions.
	require.Equal(t, http.StatusCreated, rec.Code)

	// Backtests must still reject initial_balance <= 0.
	backtestBody := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"initial_balance": 0,
		},
		"start": time.Now().Add(-24 * time.Hour).Format(time.RFC3339),
		"end":   time.Now().Format(time.RFC3339),
	}
	backtestPayload, err := json.Marshal(backtestBody)
	require.NoError(t, err)
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/backtests", bytes.NewReader(backtestPayload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "config.initial_balance must be greater than 0 for backtests")

	// Both runs and backtests reject negative initial_balance.
	rec = performJSONRequest(t, handler, "/v1/runs", map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"initial_balance": -1,
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "config.initial_balance must be non-negative")
}

func performJSONRequest(t *testing.T, handler http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	return rec
}

type memoryRunStore struct {
	runs map[uuid.UUID]*strategyhttp.RunDetail
}

type doraClientFunc struct {
	listOrderBooks  func(context.Context) ([]strategyhttp.DORAOrderBookSummary, error)
	getUserID       func(context.Context) (string, error)
	getAssetByID    func(context.Context, string) (*strategyhttp.AssetInfo, error)
	listCopyTraders func(context.Context) ([]strategyhttp.CopyTrader, error)
}

func (f doraClientFunc) ListOrderBooks(ctx context.Context) ([]strategyhttp.DORAOrderBookSummary, error) {
	if f.listOrderBooks == nil {
		return nil, fmt.Errorf("not implemented")
	}
	return f.listOrderBooks(ctx)
}

func (f doraClientFunc) GetAssetByID(ctx context.Context, id string) (*strategyhttp.AssetInfo, error) {
	if f.getAssetByID == nil {
		return nil, fmt.Errorf("not implemented")
	}
	return f.getAssetByID(ctx, id)
}

func (f doraClientFunc) GetUserID(ctx context.Context) (string, error) {
	if f.getUserID == nil {
		return "test-user", nil
	}
	return f.getUserID(ctx)
}

func (f doraClientFunc) ListCopyTraders(ctx context.Context) ([]strategyhttp.CopyTrader, error) {
	if f.listCopyTraders == nil {
		return nil, fmt.Errorf("not implemented")
	}
	return f.listCopyTraders(ctx)
}

func (s *memoryRunStore) LoadRuns(ctx context.Context) ([]*strategyhttp.RunDetail, error) {
	out := make([]*strategyhttp.RunDetail, 0, len(s.runs))
	for _, run := range s.runs {
		copyRun := *run
		copyRun.Config = append([]byte(nil), run.Config...)
		out = append(out, &copyRun)
	}
	return out, nil
}

func (s *memoryRunStore) SaveRun(ctx context.Context, detail *strategyhttp.RunDetail) error {
	if s.runs == nil {
		s.runs = make(map[uuid.UUID]*strategyhttp.RunDetail)
	}
	copyRun := *detail
	copyRun.Config = append([]byte(nil), detail.Config...)
	s.runs[detail.ID] = &copyRun
	return nil
}

// CheckRunExists satisfies the RunStore interface for the in-memory fake.
// Ownership is inferred from the run map; a run present for a different
// user is reported as not existing, mirroring the 404-collapse semantic
// of the real PGRunStore.
func (s *memoryRunStore) CheckRunExists(_ context.Context, id uuid.UUID, doraUserID string) (bool, error) {
	run, ok := s.runs[id]
	return ok && run.DORAUserID == doraUserID, nil
}

// LookupRunByID satisfies RunStore.LookupRunByID for tests.
func (s *memoryRunStore) LookupRunByID(_ context.Context, id uuid.UUID) (*strategyhttp.RunDetail, error) {
	run, ok := s.runs[id]
	if !ok {
		return nil, nil
	}
	copyRun := *run
	copyRun.Config = append([]byte(nil), run.Config...)
	return &copyRun, nil
}

func (s *memoryRunStore) String() string {
	return fmt.Sprintf("memoryRunStore(%d)", len(s.runs))
}

type memoryBacktestStore struct {
	mu           sync.Mutex
	backtests    map[uuid.UUID]*strategyhttp.BacktestDetail
	trades       map[uuid.UUID][]stats.TradeRecordInsert
	closedTrades map[uuid.UUID][]stats.ClosedTradeInsert
}

func (s *memoryBacktestStore) LoadBacktests(ctx context.Context) ([]*strategyhttp.BacktestDetail, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*strategyhttp.BacktestDetail, 0, len(s.backtests))
	for _, bt := range s.backtests {
		copyBT := *bt
		copyBT.Config = append([]byte(nil), bt.Config...)
		copyBT.Result = nil
		out = append(out, &copyBT)
	}
	return out, nil
}

func (s *memoryBacktestStore) LoadBacktestResult(ctx context.Context, id uuid.UUID) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bt, ok := s.backtests[id]
	if !ok {
		return nil, fmt.Errorf("backtest not found")
	}
	return bt.Result, nil
}

func (s *memoryBacktestStore) SaveBacktest(ctx context.Context, detail *strategyhttp.BacktestDetail) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.backtests == nil {
		s.backtests = make(map[uuid.UUID]*strategyhttp.BacktestDetail)
	}
	copyBT := *detail
	copyBT.Config = append([]byte(nil), detail.Config...)
	s.backtests[detail.ID] = &copyBT
	return nil
}

func (s *memoryBacktestStore) GetBacktestTrades(ctx context.Context, id uuid.UUID, strategyType string, page, limit int) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.trades[id]
	pageItems := paginateInserts(rows, page, limit)
	items := make([]json.RawMessage, 0, len(pageItems))
	for i := range pageItems {
		b, err := tradeRecordInsertToResponse(strategyType, pageItems[i])
		if err != nil {
			return nil, err
		}
		items = append(items, b)
	}
	return marshalItems(items)
}

func (s *memoryBacktestStore) WriteTradeRecord(_ context.Context, rec stats.TradeRecordInsert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trades == nil {
		s.trades = make(map[uuid.UUID][]stats.TradeRecordInsert)
	}
	s.trades[rec.BacktestID] = append(s.trades[rec.BacktestID], rec)
	return nil
}

func (s *memoryBacktestStore) WriteClosedTrade(_ context.Context, trade stats.ClosedTradeInsert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closedTrades == nil {
		s.closedTrades = make(map[uuid.UUID][]stats.ClosedTradeInsert)
	}
	s.closedTrades[trade.BacktestID] = append(s.closedTrades[trade.BacktestID], trade)
	return nil
}

func (s *memoryBacktestStore) WriteTradeRecordsBatch(_ context.Context, recs []stats.TradeRecordInsert) error {
	if len(recs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trades == nil {
		s.trades = make(map[uuid.UUID][]stats.TradeRecordInsert)
	}
	for _, r := range recs {
		s.trades[r.BacktestID] = append(s.trades[r.BacktestID], r)
	}
	return nil
}

func (s *memoryBacktestStore) WriteClosedTradesBatch(_ context.Context, trades []stats.ClosedTradeInsert) error {
	if len(trades) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closedTrades == nil {
		s.closedTrades = make(map[uuid.UUID][]stats.ClosedTradeInsert)
	}
	for _, t := range trades {
		s.closedTrades[t.BacktestID] = append(s.closedTrades[t.BacktestID], t)
	}
	return nil
}

func (s *memoryBacktestStore) Flush(_ context.Context) error { return nil }

func paginateInserts[T any](items []T, page, limit int) []T {
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	start := (page - 1) * limit
	if start >= len(items) {
		return []T{}
	}
	end := start + limit
	if end > len(items) {
		end = len(items)
	}
	return items[start:end]
}

func tradeRecordInsertToResponse(strategyType string, r stats.TradeRecordInsert) (json.RawMessage, error) {
	bondID := r.BondID
	switch strategyType {
	case "copytrading":
		rec := strategyhttp.CopyTradingTradeRecord{
			Time:         r.Time,
			BondID:       bondID,
			Signal:       r.Signal,
			Price:        r.Price.String(),
			Quantity:     r.Quantity.String(),
			OrderSize:    r.OrderSize.String(),
			Cash:         r.Cash.String(),
			OpenPosition: r.OpenPosition.String(),
		}
		if r.TradeID != uuid.Nil {
			rec.TradeID = r.TradeID.String()
		}
		return json.Marshal(rec)
	case "breakout":
		rec := strategyhttp.BreakoutTradeRecord{
			Time:             r.Time,
			BondID:           bondID,
			Signal:           r.Signal,
			Price:            r.Price.String(),
			Quantity:         r.Quantity.String(),
			PositionSize:     r.PositionSize.String(),
			CompressionRatio: r.CompressionRatio.String(),
			EntryATR:         r.EntryATR.String(),
		}
		return json.Marshal(rec)
	default:
		rec := strategyhttp.MeanReversionTradeRecord{
			Time:         r.Time,
			BondID:       bondID,
			Signal:       r.Signal,
			Spread:       r.Spread.String(),
			PositionSize: r.PositionSize.String(),
			ZScore:       r.ZScore.String(),
			Price:        r.Price.String(),
			Quantity:     r.Quantity.String(),
			EntryBalance: r.EntryBalance.String(),
		}
		return json.Marshal(rec)
	}
}

func closedTradeInsertToResponse(strategyType string, r stats.ClosedTradeInsert) (json.RawMessage, error) {
	bondID := r.BondID
	switch strategyType {
	case "copytrading":
		ct := strategyhttp.CopyTradingClosedTrade{
			OpenTime:     r.OpenTime,
			CloseTime:    r.CloseTime,
			BondID:       bondID,
			OpenSignal:   r.OpenSignal,
			CloseSignal:  r.CloseSignal,
			Quantity:     r.Quantity.String(),
			EntryPrice:   r.EntryPrice.String(),
			ExitPrice:    r.ExitPrice.String(),
			PnL:          r.PnL.String(),
			EntryBalance: r.EntryBalance.String(),
		}
		if r.OpenTradeID != uuid.Nil {
			ct.OpenTradeID = r.OpenTradeID.String()
		}
		if r.CloseTradeID != uuid.Nil {
			ct.CloseTradeID = r.CloseTradeID.String()
		}
		return json.Marshal(ct)
	case "breakout":
		ct := strategyhttp.BreakoutClosedTrade{
			BondID:                bondID,
			OpenTime:              r.OpenTime,
			CloseTime:             r.CloseTime,
			Signal:                r.OpenSignal,
			ExitSignal:            r.CloseSignal,
			EntryPrice:            r.EntryPrice.String(),
			ExitPrice:             r.ExitPrice.String(),
			Quantity:              r.Quantity.String(),
			PositionSize:          r.PositionSize.String(),
			PnL:                   r.PnL.String(),
			ExitReason:            r.ExitReason,
			EntryCompressionRatio: r.EntryCompressionRatio.String(),
			ExitCompressionRatio:  r.ExitCompressionRatio.String(),
		}
		return json.Marshal(ct)
	default:
		ct := strategyhttp.MeanReversionClosedTrade{
			BondID:       bondID,
			OpenTime:     r.OpenTime,
			CloseTime:    r.CloseTime,
			Signal:       r.OpenSignal,
			ExitSignal:   r.CloseSignal,
			EntrySpread:  r.EntrySpread.String(),
			ExitSpread:   r.ExitSpread.String(),
			EntryZScore:  r.EntryZScore.String(),
			ExitZScore:   r.ExitZScore.String(),
			PositionSize: r.PositionSize.String(),
			PnL:          r.PnL.String(),
			ExitReason:   r.ExitReason,
			EntryPrice:   r.EntryPrice.String(),
			ExitPrice:    r.ExitPrice.String(),
			Quantity:     r.Quantity.String(),
			EntryBalance: r.EntryBalance.String(),
		}
		return json.Marshal(ct)
	}
}

func (s *memoryBacktestStore) GetBacktestClosedTrades(ctx context.Context, id uuid.UUID, strategyType string, page, limit int) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.closedTrades[id]
	pageItems := paginateInserts(rows, page, limit)
	items := make([]json.RawMessage, 0, len(pageItems))
	for i := range pageItems {
		b, err := closedTradeInsertToResponse(strategyType, pageItems[i])
		if err != nil {
			return nil, err
		}
		items = append(items, b)
	}
	return marshalItems(items)
}

func marshalItems(items []json.RawMessage) (json.RawMessage, error) {
	b, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		return nil, err
	}
	return b, nil
}

func mustMarshalResult(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	return b
}

func TestHandlerRunOwnership(t *testing.T) {
	t.Parallel()

	runID := uuid.Must(uuid.NewV7())
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	store := &memoryRunStore{
		runs: map[uuid.UUID]*strategyhttp.RunDetail{
			runID: {
				RunSummary: strategyhttp.RunSummary{
					ID:           runID,
					DORAUserID:   "user-alice",
					StrategyType: "mean_reversion",
					Status:       "running",
					CreatedAt:    now,
					UpdatedAt:    now,
				},
				Config: json.RawMessage(`{"lookback_window":20,"entry_z_score":2,"exit_z_score":0.5,"stop_loss_z_score":3.5,"min_std_dev":0.0005,"max_position_size":1}`),
			},
		},
	}

	svc := strategycore.NewService()
	pricesHandler := prices.New(prices.Config{})
	handlerAny := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithRunStore(store),
		strategyhttp.WithPricesHandler(pricesHandler),
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(context.Context) (string, error) {
				return "user-bob", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	restorer, ok := handlerAny.(interface{ RestoreRuns(context.Context) error })
	require.True(t, ok)
	require.NoError(t, restorer.RestoreRuns(context.Background()))

	// GET by ID: 403 — bob can tell the run exists but cannot access it
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/runs/"+runID.String(), nil)
	req.Header.Set("Authorization", "ApiKey bob-key")
	handlerAny.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)

	// LIST: bob sees zero items (alice's run is filtered out)
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/runs", nil)
	req.Header.Set("Authorization", "ApiKey bob-key")
	handlerAny.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp struct {
		Items []strategyhttp.RunSummary `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Items)

	// DELETE (stop): 404 — hides existence from wrong user on mutations
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodDelete, "/v1/runs/"+runID.String(), nil)
	req.Header.Set("Authorization", "ApiKey bob-key")
	handlerAny.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)

	// POST pause: 404
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/runs/"+runID.String()+"/pause", nil)
	req.Header.Set("Authorization", "ApiKey bob-key")
	handlerAny.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)

	// POST resume: 404
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/runs/"+runID.String()+"/resume", nil)
	req.Header.Set("Authorization", "ApiKey bob-key")
	handlerAny.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerBacktestSummary(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	backtestID := uuid.Must(uuid.NewV7())

	store := &memoryBacktestStore{
		backtests: map[uuid.UUID]*strategyhttp.BacktestDetail{
			backtestID: {
				BacktestSummary: strategyhttp.BacktestSummary{
					ID:           backtestID,
					DORAUserID:   "user-1",
					StrategyType: "mean_reversion",
					Status:       "completed",
					Config:       json.RawMessage(`{"lookback_window":20}`),
					CreatedAt:    now,
				},
				Start: now.Add(-time.Hour),
				End:   now,
				Result: mustMarshalResult(t, strategyhttp.MeanReversionBacktestResult{
					TotalPnL:    "100.0",
					WinCount:    5,
					LossCount:   2,
					MaxDrawdown: "0.1",
					SharpeRatio: "1.5",
				}),
			},
		},
	}

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithBacktestStore(store),
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(context.Context) (string, error) {
				return "user-1", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	restorer, ok := handler.(interface{ RestoreBacktests(context.Context) error })
	require.True(t, ok)
	require.NoError(t, restorer.RestoreBacktests(context.Background()))

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests/"+backtestID.String(), nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	// Must unmarshal as BacktestResultSummary with P&L fields populated
	// from the store, plus metadata fields from the original detail.
	var resultSummary strategyhttp.BacktestResultSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resultSummary))
	assert.Equal(t, "100.0", resultSummary.TotalPnL)
	assert.Equal(t, "0.1", resultSummary.MaxDrawdown)
	assert.Equal(t, "1.5", resultSummary.SharpeRatio)
	assert.Equal(t, 5, resultSummary.WinCount)
	assert.Equal(t, 2, resultSummary.LossCount)
	assert.Equal(t, "mean_reversion", resultSummary.StrategyType)
	assert.Equal(t, "completed", resultSummary.Status)

	// Metadata endpoint returns BacktestSummary.
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests/"+backtestID.String()+"/metadata", nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var summary strategyhttp.BacktestSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &summary))
	assert.Equal(t, backtestID, summary.ID)
	assert.Equal(t, "completed", summary.Status)
	assert.Equal(t, "mean_reversion", summary.StrategyType)
}

func TestHandlerRequiresAuth(t *testing.T) {
	t.Parallel()

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(doraClientFunc{
			listOrderBooks: func(context.Context) ([]strategyhttp.DORAOrderBookSummary, error) {
				return nil, nil
			},
			getUserID: func(context.Context) (string, error) {
				return "user-x", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	v1Endpoints := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/strategies"},
		{http.MethodGet, "/v1/tenors"},
		{http.MethodGet, "/v1/dora/orderbooks"},
		{http.MethodGet, "/v1/dora/user"},
		{http.MethodGet, "/v1/backtests"},
		{http.MethodGet, "/v1/runs"},
	}

	for _, ep := range v1Endpoints {
		t.Run(ep.method+" "+ep.path+" missing header", func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(context.Background(), ep.method, ep.path, nil)
			handler.ServeHTTP(rec, req)
			require.Equal(t, http.StatusUnauthorized, rec.Code)
			assert.Contains(t, rec.Body.String(), "missing Authorization header")
		})

		t.Run(ep.method+" "+ep.path+" unsupported scheme", func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(context.Background(), ep.method, ep.path, nil)
			req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
			handler.ServeHTTP(rec, req)
			require.Equal(t, http.StatusUnauthorized, rec.Code)
			assert.Contains(t, rec.Body.String(), "unsupported scheme")
		})
	}

	// /healthz must not require authentication.
	t.Run("healthz no auth", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil)
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	// /v1/openapi must not require authentication.
	t.Run("openapi no auth", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/openapi", nil)
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		assert.Contains(t, rec.Body.String(), `"openapi"`)
	})

	// /v1/openapi must reject non-GET methods.
	t.Run("openapi method not allowed", func(t *testing.T) {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(context.Background(), method, "/v1/openapi", nil)
			handler.ServeHTTP(rec, req)
			require.Equalf(t, http.StatusMethodNotAllowed, rec.Code, "method %s should return 405", method)
		}
	})

	// Bearer scheme is accepted.
	t.Run("Bearer scheme accepted", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
		req.Header.Set("Authorization", "Bearer eyJhbGciOiJSUzI1NiJ9.test.sig")
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestHandlerBacktestSubResources(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	backtestID := uuid.Must(uuid.NewV7())

	store := &memoryBacktestStore{
		backtests: map[uuid.UUID]*strategyhttp.BacktestDetail{
			backtestID: {
				BacktestSummary: strategyhttp.BacktestSummary{
					ID:           backtestID,
					DORAUserID:   "user-1",
					StrategyType: "mean_reversion",
					Status:       "completed",
					CreatedAt:    now,
				},
				Result: mustMarshalResult(t, strategyhttp.MeanReversionBacktestResult{
					TotalPnL: "100.0",
				}),
			},
		},
	}
	// Trade records and closed trades are now persisted via the writer
	// interface, not embedded in the result JSON. Add 15 of each so the
	// pagination assertions below have something to page through.
	for i := 0; i < 15; i++ {
		require.NoError(t, store.WriteTradeRecord(context.Background(), stats.TradeRecordInsert{
			BacktestID: backtestID,
			BondID:     fmt.Sprintf("bond-%d", i),
			Signal:     "BUY",
		}))
		require.NoError(t, store.WriteClosedTrade(context.Background(), stats.ClosedTradeInsert{
			BacktestID:  backtestID,
			BondID:      fmt.Sprintf("bond-%d", i),
			OpenSignal:  "BUY",
			CloseSignal: "SELL",
		}))
	}

	handler := strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithBacktestStore(store),
		strategyhttp.WithDORAClient(doraClientFunc{
			getUserID: func(context.Context) (string, error) {
				return "user-1", nil
			},
		}),
		strategyhttp.WithTradesHistoryStore(nil),
	)
	restorer, ok := handler.(interface{ RestoreBacktests(context.Context) error })
	require.True(t, ok)
	require.NoError(t, restorer.RestoreBacktests(context.Background()))

	t.Run("GetTradesPage1", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, fmt.Sprintf("/v1/backtests/%s/trades?page=1&limit=10", backtestID), nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.MeanReversionTradeRecord `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Items, 10)
		assert.Equal(t, "bond-0", resp.Items[0].BondID)
		assert.Equal(t, "bond-9", resp.Items[9].BondID)
	})

	t.Run("GetTradesPage2", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, fmt.Sprintf("/v1/backtests/%s/trades?page=2&limit=10", backtestID), nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.MeanReversionTradeRecord `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Items, 5)
		assert.Equal(t, "bond-10", resp.Items[0].BondID)
		assert.Equal(t, "bond-14", resp.Items[4].BondID)
	})

	t.Run("GetClosedTradesLimitClamping", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, fmt.Sprintf("/v1/backtests/%s/closed-trades?limit=100", backtestID), nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Items []strategyhttp.MeanReversionClosedTrade `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		// Our sample has 15, so it should return all 15 if limit is clamped to 50
		assert.Len(t, resp.Items, 15)
	})

	t.Run("NotFound", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, fmt.Sprintf("/v1/backtests/%s/trades", uuid.New()), nil)
		req.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestHandlerCopyTradingBacktestInitialBalance(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	newHandler := func() http.Handler {
		return strategyhttp.NewHandler(
			&strategyfakes.FakeService{},
			strategyhttp.WithNow(func() time.Time { return now }),
			strategyhttp.WithDORAClient(doraClientFunc{
				getUserID: func(context.Context) (string, error) { return "user-1", nil },
			}),
			strategyhttp.WithTradesHistoryStore(nil),
		)
	}

	// omit returns the map unchanged so we can leave initial_balance out.
	buildBody := func(initial any) map[string]any {
		cfg := map[string]any{
			"followed_trader":         uuid.NewString(),
			"percentage_of_available": 0.5,
			"leverage":                1.0,
			"min_order_size":          0,
			"max_order_size":          0,
			"disallowed_bonds":        []string{},
		}
		if initial != "omit" {
			cfg["initial_balance"] = initial
		}
		return map[string]any{
			"strategy_type": "copytrading",
			"config":        cfg,
			"start":         now.Add(-24 * time.Hour).Format(time.RFC3339),
			"end":           now.Format(time.RFC3339),
		}
	}

	cases := []struct {
		name     string
		initial  any
		wantCode int
		wantBody string
	}{
		{
			name:     "absent falls through to default",
			initial:  "omit",
			wantCode: http.StatusAccepted,
		},
		{
			name:     "explicit zero falls through to default",
			initial:  0.0,
			wantCode: http.StatusAccepted,
		},
		{
			name:     "positive is accepted",
			initial:  25000.0,
			wantCode: http.StatusAccepted,
		},
		{
			name:     "negative is rejected",
			initial:  -1.0,
			wantCode: http.StatusBadRequest,
			wantBody: "config.initial_balance must be non-negative",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := performJSONRequest(t, newHandler(), "/v1/backtests", buildBody(tc.initial))
			require.Equal(t, tc.wantCode, rec.Code)
			if tc.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tc.wantBody)
			}
		})
	}
}

func TestHandler_EmitsRunStartedEvent(t *testing.T) {
	t.Parallel()

	runID := uuid.Must(uuid.NewV7())
	svc := &strategyfakes.FakeService{
		RunStrategyStub: func(_ context.Context, _ strategycore.Strategy) (uuid.UUID, error) {
			return runID, nil
		},
	}
	notifier := &notificationsfakes.FakeNotifier{}
	handler := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
		strategyhttp.WithNotifier(notifier),
	)

	body := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window": 20,
			"entry_z_score":   2.0,
			"exit_z_score":    0.5,
			"order_book_id":   uuid.Must(uuid.NewV7()).String(),
			"tenor":           "10Y",
			"initial_balance": 5.5,
			"leverage":        2.0,
		},
	}
	rec := performJSONRequest(t, handler, "/v1/runs", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	require.Equal(t, 1, notifier.PublishCallCount())
	_, evt := notifier.PublishArgsForCall(0)
	assert.Equal(t, notifications.EventRunStarted, evt.Type)
	assert.Equal(t, "test-user", evt.UserID)
	assert.Equal(t, runID.String(), evt.RunID)
}

func TestHandler_EmitsRunStopLossEvent(t *testing.T) {
	t.Parallel()

	runID := uuid.Must(uuid.NewV7())
	var capturedStrat strategycore.Strategy
	svc := &strategyfakes.FakeService{
		RunStrategyStub: func(_ context.Context, s strategycore.Strategy) (uuid.UUID, error) {
			capturedStrat = s
			return runID, nil
		},
	}
	notifier := &notificationsfakes.FakeNotifier{}
	handler := strategyhttp.NewHandler(
		svc,
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
		strategyhttp.WithNotifier(notifier),
		strategyhttp.WithStopLossObserverInterval(10*time.Millisecond),
	)

	body := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window": 20,
			"entry_z_score":   2.0,
			"exit_z_score":    0.5,
			"order_book_id":   uuid.Must(uuid.NewV7()).String(),
			"tenor":           "10Y",
			"initial_balance": 5.5,
			"leverage":        2.0,
		},
	}
	rec := performJSONRequest(t, handler, "/v1/runs", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	mrStrat, ok := capturedStrat.(*meanreversion.Strategy)
	require.True(t, ok, "expected meanreversion.Strategy, got %T", capturedStrat)
	_, _, triggered := mrStrat.LastStopLossTrigger()
	require.False(t, triggered)

	exit, reason := mrStrat.ShouldExit(types.SignalBuy, decimal.MustNew(36, 1))
	require.True(t, exit)
	require.Equal(t, meanreversion.ExitReasonStopLoss, reason)

	require.Eventually(t, func() bool {
		for i := 0; i < notifier.PublishCallCount(); i++ {
			_, evt := notifier.PublishArgsForCall(i)
			if evt.Type == notifications.EventRunStopLoss {
				assert.Equal(t, runID.String(), evt.RunID)
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "expected EventRunStopLoss to be published")
}

// fakeRunStore is a minimal RunStore for the trading-decisions
// handler tests. Only CheckRunExists is exercised by getRunDecisions;
// LoadRuns and SaveRun are present to satisfy the interface and are
// never called on this path. The calls counter lets tests assert the
// ownership check was (or was not) reached.
type fakeRunStore struct {
	exists bool
	err    error
	calls  int
}

func (s *fakeRunStore) LoadRuns(_ context.Context) ([]*strategyhttp.RunDetail, error) {
	return nil, nil
}

func (s *fakeRunStore) SaveRun(_ context.Context, _ *strategyhttp.RunDetail) error {
	return nil
}

func (s *fakeRunStore) CheckRunExists(_ context.Context, _ uuid.UUID, _ string) (bool, error) {
	s.calls++
	return s.exists, s.err
}

// LookupRunByID satisfies RunStore.LookupRunByID. It is never called on
// the trading-decisions path; the stub exists only to keep the fake
// compiling now that RunStore grew the method.
func (s *fakeRunStore) LookupRunByID(_ context.Context, _ uuid.UUID) (*strategyhttp.RunDetail, error) {
	return nil, nil
}

// fakeDecisionReader is a minimal DecisionReader for the
// trading-decisions handler tests. It returns a canned page of
// decisions plus an optional next cursor; the calls counter lets
// tests assert the reader was (or was not) reached.
type fakeDecisionReader struct {
	items []strategycore.Decision
	next  *strategyhttp.Cursor
	err   error
	calls int
}

func (f *fakeDecisionReader) ListDecisions(_ context.Context, _ strategyhttp.ListDecisionsParams) ([]strategycore.Decision, *strategyhttp.Cursor, error) {
	f.calls++
	return f.items, f.next, f.err
}

// newDecisionsHandler wires a Handler with the inline fakes for the
// trading-decisions tests. The doraClientFunc default resolves the
// caller to "test-user", which the fake run store ignores.
func newDecisionsHandler(t *testing.T, runStore strategyhttp.RunStore, reader strategyhttp.DecisionReader) http.Handler {
	t.Helper()
	return strategyhttp.NewHandler(
		&strategyfakes.FakeService{},
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithRunStore(runStore),
		strategyhttp.WithDecisionReader(reader),
		strategyhttp.WithTradesHistoryStore(nil),
	)
}

func doDecisionsReq(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	req.Header.Set("Authorization", "ApiKey test-key")
	h.ServeHTTP(rec, req)
	return rec
}

func sampleDecision(runID uuid.UUID) strategycore.Decision {
	return strategycore.Decision{
		RunID:           runID,
		Seq:             1,
		StrategyType:    "mean_reversion",
		OrderBookID:     uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		Asset:           uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		Side:            "BUY",
		Signal:          "buy",
		Kind:            strategycore.DecisionKindOpen,
		Quantity:        decimal.MustNew(100, 0),
		Price:           decimal.MustParse("98.5"),
		Leverage:        decimal.MustNew(1, 0),
		InverseLeverage: decimal.MustNew(1, 0),
		Reason:          "z_score_entry",
		ReasonDetail:    "z=-2.4",
		ClientOrderID:   "mean_reversion." + runID.String() + ".0",
		CreatedAt:       time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
	}
}

func TestGetRunDecisions_HappyPath(t *testing.T) {
	t.Parallel()
	runID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	runStore := &fakeRunStore{exists: true}
	reader := &fakeDecisionReader{items: []strategycore.Decision{sampleDecision(runID)}}
	handler := newDecisionsHandler(t, runStore, reader)

	rec := doDecisionsReq(t, handler, "/v1/trading-decisions/"+runID.String())
	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Items      []strategycore.Decision `json:"items"`
		NextCursor string                  `json:"next_cursor"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Items, 1)
	assert.Equal(t, int64(1), body.Items[0].Seq)
	assert.Equal(t, "mean_reversion", body.Items[0].StrategyType)
	assert.Empty(t, body.NextCursor)
	assert.NotContains(t, rec.Body.String(), "next_cursor")
	assert.Equal(t, 1, runStore.calls)
	assert.Equal(t, 1, reader.calls)
}

func TestGetRunDecisions_RunNotFoundIs404(t *testing.T) {
	t.Parallel()
	runID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	runStore := &fakeRunStore{exists: false}
	reader := &fakeDecisionReader{}
	handler := newDecisionsHandler(t, runStore, reader)

	rec := doDecisionsReq(t, handler, "/v1/trading-decisions/"+runID.String())
	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "run not found")
	assert.Equal(t, 1, runStore.calls)
	assert.Equal(t, 0, reader.calls, "reader must not be called when the run is absent")
}

func TestGetRunDecisions_WrongUserIs404(t *testing.T) {
	t.Parallel()
	// The handler must not distinguish "no such run" from "run belongs
	// to another user": CheckRunExists collapses both into false, and
	// the response is the same 404 with "run not found".
	runID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	runStore := &fakeRunStore{exists: false}
	reader := &fakeDecisionReader{}
	handler := newDecisionsHandler(t, runStore, reader)

	rec := doDecisionsReq(t, handler, "/v1/trading-decisions/"+runID.String())
	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "run not found")
	assert.Equal(t, 0, reader.calls, "reader must not be called for a wrong-user run")
}

func TestGetRunDecisions_BadRunIDIs400(t *testing.T) {
	t.Parallel()
	runStore := &fakeRunStore{exists: true}
	reader := &fakeDecisionReader{}
	handler := newDecisionsHandler(t, runStore, reader)

	rec := doDecisionsReq(t, handler, "/v1/trading-decisions/not-a-uuid")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid run_id")
	assert.Equal(t, 0, runStore.calls, "ownership check must not run for an unparseable run_id")
	assert.Equal(t, 0, reader.calls)
}

func TestGetRunDecisions_BadDateIs400(t *testing.T) {
	t.Parallel()
	runID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	runStore := &fakeRunStore{exists: true}
	reader := &fakeDecisionReader{}
	handler := newDecisionsHandler(t, runStore, reader)

	rec := doDecisionsReq(t, handler, "/v1/trading-decisions/"+runID.String()+"?from=yesterday")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, 1, runStore.calls)
	assert.Equal(t, 0, reader.calls, "reader must not be called when the date filter is malformed")
}

func TestGetRunDecisions_BadCursorIs400(t *testing.T) {
	t.Parallel()
	runID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	runStore := &fakeRunStore{exists: true}
	reader := &fakeDecisionReader{}
	handler := newDecisionsHandler(t, runStore, reader)

	rec := doDecisionsReq(t, handler, "/v1/trading-decisions/"+runID.String()+"?cursor=garbage")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, 1, runStore.calls)
	assert.Equal(t, 0, reader.calls, "reader must not be called when the cursor is malformed")
}

func TestGetRunDecisions_LastPageOmitsNextCursor(t *testing.T) {
	t.Parallel()
	runID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	runStore := &fakeRunStore{exists: true}
	items := []strategycore.Decision{
		sampleDecision(runID),
		sampleDecision(runID),
		sampleDecision(runID),
	}
	reader := &fakeDecisionReader{items: items, next: nil}
	handler := newDecisionsHandler(t, runStore, reader)

	rec := doDecisionsReq(t, handler, "/v1/trading-decisions/"+runID.String())
	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Items      []strategycore.Decision `json:"items"`
		NextCursor string                  `json:"next_cursor"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Items, 3)
	assert.Empty(t, body.NextCursor)
	assert.NotContains(t, rec.Body.String(), "next_cursor")
	assert.Equal(t, 1, reader.calls)
}
