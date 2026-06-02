package copytrading

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/govalues/decimal"
)

// tradesClient fetches historical trades from Dora.
type tradesClient interface {
	ListOrderBooks(ctx context.Context) ([]string, error)
	GetTrades(ctx context.Context, userID string, orderBookIDs []string, start, end time.Time) ([]doraclient.Trade, error)
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

// ListOrderBooks returns the IDs of all order books.
func (c *doraTradesClient) ListOrderBooks(ctx context.Context) ([]string, error) {
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

	resp, _, err := c.client.DefaultAPI.ListOrderBooks(authCtx).Execute()
	if err != nil {
		return nil, fmt.Errorf("list order books: %w", err)
	}
	if resp == nil {
		return nil, nil
	}
	ids := make([]string, 0, len(resp.Data))
	for _, ob := range resp.Data {
		ids = append(ids, ob.OrderBookId)
	}
	return ids, nil
}

// GetTrades fetches trades for the given userID within [start, end]. If
// orderBookIDs is non-empty, results are scoped to those order books.
func (c *doraTradesClient) GetTrades(
	ctx context.Context,
	userID string,
	orderBookIDs []string,
	start, end time.Time,
) ([]doraclient.Trade, error) {
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
	limit := int32(1000) //nolint:mnd
	page := int32(1)

	for {
		req := c.client.DefaultAPI.GetTrades(authCtx).Limit(limit).Page(page)
		if userID != "" {
			req = req.UserIds([]string{userID})
		}
		if len(orderBookIDs) > 0 {
			req = req.OrderBookIds(orderBookIDs)
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

// Run fetches historical trades for the followed trader across all order
// books within [start, end] and simulates copy trading. Trades are collated
// across order books and sorted by created_at ascending.
func (b *Backtester) Run(ctx context.Context, start, end time.Time) (types.BacktestResult, error) {
	if b.trades == nil {
		apiKey := os.Getenv("DORA_API_KEY")
		if apiKey == "" {
			return types.BacktestResult{}, errors.New("DORA_API_KEY not set")
		}
		b.trades = newDoraTradesClient(apiKey)
	}

	orderBooks, err := b.trades.ListOrderBooks(ctx)
	if err != nil {
		return types.BacktestResult{}, fmt.Errorf("list order books: %w", err)
	}

	followedTrader := b.strategy.cfg.FollowedTrader.String()
	var allTrades []doraclient.Trade
	for _, obID := range orderBooks {
		select {
		case <-ctx.Done():
			return types.BacktestResult{}, errors.New("backtest cancelled")
		default:
		}
		trades, err := b.trades.GetTrades(ctx, followedTrader, []string{obID}, start, end)
		if err != nil {
			return types.BacktestResult{}, fmt.Errorf("get trades for order book %s: %w", obID, err)
		}
		allTrades = append(allTrades, trades...)
	}

	// Defensive filter: DORA should already filter by userID, but ensure only
	// the followed trader's trades are simulated.
	filtered := allTrades[:0]
	for _, tr := range allTrades {
		if tr.UserId == followedTrader {
			filtered = append(filtered, tr)
		}
	}
	allTrades = filtered

	sort.Slice(allTrades, func(i, j int) bool {
		return allTrades[i].CreatedAt.Before(allTrades[j].CreatedAt)
	})

	return b.simulate(ctx, allTrades)
}

func (b *Backtester) simulate(ctx context.Context, trades []doraclient.Trade) (types.BacktestResult, error) {
	var (
		closedTrades []types.ClosedTrade
		tradeRecords []types.TradeRecord
	)

	// Use a default initial balance for backtest
	remainingBalance := decimal.MustNew(10000, 0) //nolint:mnd

	for _, trade := range trades {
		select {
		case <-ctx.Done():
			return types.BacktestResult{}, errors.New("backtest cancelled")
		default:
		}

		orderSize := calculateOrderSize(
			remainingBalance,
			b.strategy.cfg.PercentageOfAvailable,
			b.strategy.cfg.Leverage,
			b.strategy.cfg.MinOrderSize,
			b.strategy.cfg.MaxOrderSize,
		)

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
