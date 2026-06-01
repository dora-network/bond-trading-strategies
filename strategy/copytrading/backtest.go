package copytrading

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/govalues/decimal"
)

// tradesClient fetches historical trades from Dora.
type tradesClient interface {
	GetTrades(ctx context.Context, userID string, start, end time.Time) ([]doraclient.Trade, error)
}

// doraTradesClient implements tradesClient using the Dora SDK.
type doraTradesClient struct {
	client *doraclient.APIClient
	apiKey string
}

// newDoraTradesClient creates a new tradesClient for backtesting.
func newDoraTradesClient(apiKey string) *doraTradesClient {
	cfg := doraclient.NewConfiguration()
	if baseURL := os.Getenv("DORA_BASE_URL"); baseURL != "" {
		cfg.Servers = doraclient.ServerConfigurations{{
			URL:         baseURL,
			Description: "Configured DORA API server",
		}}
	}
	return &doraTradesClient{
		client: doraclient.NewAPIClient(cfg),
		apiKey: apiKey,
	}
}

// GetTrades fetches trades for the given userID within [start, end].
func (c *doraTradesClient) GetTrades(ctx context.Context, userID string, start, end time.Time) ([]doraclient.Trade, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("DORA client is not configured")
	}
	if c.apiKey == "" {
		return nil, errors.New("API_KEY is not configured")
	}
	authCtx := context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {
			Key:    c.apiKey,
			Prefix: apiKeyPrefix,
		},
	})

	var allTrades []doraclient.Trade
	limit := int32(1000)
	page := int32(1)

	for {
		req := c.client.DefaultAPI.GetTrades(authCtx).Limit(limit).Page(page)
		if userID != "" {
			req = req.UserIds([]string{userID})
		}
		if !start.IsZero() {
			req = req.Start(start)
		}
		if !end.IsZero() {
			req = req.End(end)
		}

		resp, _, err := req.Execute()
		if err != nil {
			return nil, fmt.Errorf("get trades: %w", err)
		}

		if resp == nil || resp.Data == nil {
			break
		}

		allTrades = append(allTrades, resp.Data...)

		if len(resp.Data) < int(limit) {
			break
		}

		page++
	}

	return allTrades, nil
}

// Backtester replays historical trades for a followed trader and simulates
// copy trading performance.
type Backtester struct {
	strategy *Strategy
	trades   tradesClient
}

// NewBacktester creates a Backtester wrapping the given Strategy.
func NewBacktester(s *Strategy) *Backtester {
	return &Backtester{strategy: s}
}

// Run fetches historical trades for the followed trader within [start, end]
// and simulates copy trading.
func (b *Backtester) Run(ctx context.Context, start, end time.Time) (types.BacktestResult, error) {
	if b.trades == nil {
		apiKey := os.Getenv("DORA_API_KEY")
		if apiKey == "" {
			return types.BacktestResult{}, errors.New("DORA_API_KEY not set")
		}
		b.trades = newDoraTradesClient(apiKey)
	}

	trades, err := b.trades.GetTrades(ctx, b.strategy.cfg.FollowedTrader.String(), start, end)
	if err != nil {
		return types.BacktestResult{}, fmt.Errorf("fetch historical trades: %w", err)
	}

	return b.simulate(ctx, trades)
}

func (b *Backtester) simulate(ctx context.Context, trades []doraclient.Trade) (types.BacktestResult, error) {
	var (
		closedTrades []types.ClosedTrade
		tradeRecords []types.TradeRecord
	)

	// Use a default initial balance for backtest
	remainingBalance := decimal.MustNew(10000, 0)

	for _, trade := range trades {
		select {
		case <-ctx.Done():
			return types.BacktestResult{}, errors.New("backtest cancelled")
		default:
		}

		orderSize := calculateOrderSize(remainingBalance, b.strategy.cfg.PercentageOfAvailable, b.strategy.cfg.Leverage, b.strategy.cfg.MinOrderSize, b.strategy.cfg.MaxOrderSize)

		if orderSize.IsZero() || orderSize.IsNeg() {
			continue
		}

		price, _ := decimal.Parse(trade.Price)
		quantity, _ := decimal.Parse(trade.Quantity0)

		var signal types.Signal
		if trade.Side == doraclient.SIDE_BUY {
			signal = types.SignalBuy
		} else {
			signal = types.SignalSell
		}

		record := types.TradeRecord{
			Time:         trade.CreatedAt,
			BondID:       trade.Asset0,
			Signal:       signal,
			Spread:       decimal.Zero,
			PositionSize: orderSize,
			ZScore:       decimal.Zero,
			Price:        price,
			Quantity:     quantity,
			EntryBalance: remainingBalance,
		}
		tradeRecords = append(tradeRecords, record)

		remainingBalance, _ = remainingBalance.Sub(orderSize)
	}

	return types.BacktestResult{
		ClosedTrades: closedTrades,
		TradeRecords: tradeRecords,
	}, nil
}
