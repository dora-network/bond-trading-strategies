package meanreversion

import (
	"context"
	"time"

	pricesPkg "github.com/dora-network/bond-trading-strategies/prices"
	strategyPkg "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/dora-network/bond-trading-strategies/trades"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

func SetLookupClient(s *Strategy, client strategyPkg.MarketAPIClient) {
	s.marketAPIClient = client
}
func SetCandleHistoryStore(s *Strategy, store candleHistoryStore) {
	s.candleStore = store
}

func SetPriceHistoryStore(s *Strategy, store priceHistorySource) {
	s.priceHistoryStore = store
}

// SetBacktestTicks seeds the backtester's tick replay slice. For unit tests only.
func SetBacktestTicks(bt *Backtester, ticks []pricesPkg.AssetPrice) {
	bt.ticks = ticks
}

func SetBenchmarkYieldClient(s *Strategy, client benchmarkYieldClient) {
	s.benchmarkClient = client
}

func LookupAssetID(ctx context.Context, s *Strategy, orderBookID uuid.UUID) (string, error) {
	return s.lookupAssetID(ctx, orderBookID)
}

func GetBars(ctx context.Context, s *Strategy, start, end time.Time) ([]types.Bar, error) {
	return s.getBars(ctx, start, end)
}

func PreflightBacktest(ctx context.Context, s *Strategy, start, end time.Time) error {
	return s.PreflightBacktest(ctx, start, end)
}

func CurrentPosition(ctx context.Context, s *Strategy, assetID string) (decimal.Decimal, error) {
	return s.currentPosition(ctx, assetID)
}

func CappedOrderQuantity(s *Strategy, positionSize, currentPosition, price decimal.Decimal) (decimal.Decimal, bool, error) {
	return s.cappedOrderQuantity(positionSize, currentPosition, price)
}

func BondQty(s *Strategy) decimal.Decimal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bondQty
}

func UsdBal(s *Strategy) decimal.Decimal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.usdBal
}

func InitializeBalances(ctx context.Context, s *Strategy, baseAssetID string) {
	s.initializeBalances(ctx, baseAssetID)
}

func BalancesInitialized(s *Strategy) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.balancesInitialized
}

func OpenSignal(s *Strategy) types.Signal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.openSignal
}

// SetBaseAssetID seeds the resolved base-asset UUID so Update can
// stamp it onto Decision.bondID without a live market API client.
func SetBaseAssetID(s *Strategy, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.baseAssetID = id
}

// GetBaseAssetID exposes the base-asset UUID the run loop resolved.
func GetBaseAssetID(s *Strategy) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.baseAssetID
}

// RunWithPrices runs the strategy's internal run loop with a caller-supplied
// price channel, bypassing the prices.Handler subscription. For unit tests only.
func RunWithPrices(ctx context.Context, s *Strategy, msgs <-chan strategyPkg.Message, priceCh <-chan map[uuid.UUID]pricesPkg.AssetPrice) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return nil
	}
	var runCtx context.Context
	runCtx, s.cancel = context.WithCancel(ctx)
	s.isRunning = true
	s.mu.Unlock()
	return s.run(runCtx, msgs, priceCh, nil)
}

// RunWithPricesAndTrades runs the internal run loop with caller-supplied
// price and trade channels, bypassing both subscriptions. For unit tests only.
func RunWithPricesAndTrades(
	ctx context.Context,
	s *Strategy,
	msgs <-chan strategyPkg.Message,
	priceCh <-chan map[uuid.UUID]pricesPkg.AssetPrice,
	tradeCh <-chan streams.TradeEvent,
) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return nil
	}
	var runCtx context.Context
	runCtx, s.cancel = context.WithCancel(ctx)
	s.isRunning = true
	s.mu.Unlock()
	return s.run(runCtx, msgs, priceCh, tradeCh)
}

// ApplyTrade feeds one trade into the imbalance window. For unit tests only.
func ApplyTrade(s *Strategy, ev streams.TradeEvent) {
	s.applyTrade(ev)
}

// ImbalanceAllows exposes the imbalance entry gate. For unit tests only.
func ImbalanceAllows(s *Strategy, sig types.Signal) bool {
	return s.imbalanceAllows(sig)
}

// SetTradeHistoryStore injects a historical trade source. For unit tests only.
func SetTradeHistoryStore(s *Strategy, store trades.TradeStore) {
	s.tradeHistoryStore = store
}
