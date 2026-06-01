# Copy Trading Strategy Design

## Overview

Implement a copy trading strategy that mirrors trades from a specified Dora trader. When the followed trader executes a trade, the strategy places a market order for the same asset, same direction, with size calculated from the follower's available balance, a configurable percentage, and leverage.

## Config

```go
type Config struct {
    FollowedTrader        uuid.UUID           // UUID of trader to mirror (required)
    PercentageOfAvailable decimal.Decimal     // % of available balance per trade (0-1)
    Leverage              decimal.Decimal     // Leverage multiplier (e.g., 3)
    MinOrderSize          int                 // Min order size (0 = no minimum)
    MaxOrderSize          int                 // Max order size (0 = no maximum)
    DisallowedBonds       []uuid.UUID         // Bonds to skip (empty = none disallowed)
}
```

Order size calculation: `orderSize = availableBalance * percentageOfAvailable * leverage`
Clamped by MinOrderSize/MaxOrderSize if either > 0.

## Architecture

### Files

**New:**
- `streams/trade_stream.go` — Pub/sub TradeStream with multi-tenant filtering
- `strategy/copytrading/strategy.go` — Live strategy implementation
- `strategy/copytrading/backtest.go` — Backtest using Dora's GetTrades API

**Modified:**
- `strategy/http/handler.go` — Update config fields, set SupportsRun/Backtest, implement DecodeConfig
- `strategy/copytrading/strategy_test.go` — Tests

### TradeStream (`streams/trade_stream.go`)

Pub/sub WebSocket wrapper for Dora's trade stream. Manages one WS connection per order book, with multiple subscribers each filtering by their followed trader.

```go
type TradeStream struct {
    streamFunc TradeStreamFunc
    mu         sync.Mutex
    streams    map[uuid.UUID]*activeStream
}

type activeStream struct {
    subscribers  map[uuid.UUID]*subscriber
    cancel       context.CancelFunc
    done         chan struct{}
}

type subscriber struct {
    followedTrader uuid.UUID
    ch             chan TradeEvent
}

type TradeEvent struct {
    TraderID      uuid.UUID
    OrderBookID   uuid.UUID
    AssetID       uuid.UUID
    Side          string
    Quantity      decimal.Decimal
    Price         decimal.Decimal
    Timestamp     time.Time
    ExecutionID   string
}

type TradeStreamFunc func(ctx context.Context, wsURL string, apiKey string, orderBookID string) (<-chan TradeEvent, context.CancelFunc, error)
```

Public API:
- `Subscribe(orderBookID, subscriberID, followedTrader) (<-chan TradeEvent, error)` — creates a new subscriber, opens WS if first for this book
- `Unsubscribe(subscriberID)` — removes subscriber, closes WS if last for that book

### Strategy (`strategy/copytrading/strategy.go`)

```go
type Strategy struct {
    cfg              Config
    marketAPI        marketAPIClient
    tradeStream      *streams.TradeStream
    subscriberID     uuid.UUID
}
```

`Run(ctx, msgCh, runID) error`:
1. List all OPEN order books via Dora SDK
2. Subscribe to TradeStream for each order book with the followed trader filter
3. Main loop: select on trade channels + stop/pause/resume messages
4. On trade:
   - Check if asset is in DisallowedBonds → skip if yes
   - Query DORA API for current position (available balance + bond quantities)
   - Calculate order size
   - Place market order via DORA API (same direction as followed trade)
   - Log success/error

### Backtest (`strategy/copytrading/backtest.go`)

`Backtest(ctx, start, end) (types.BacktestResult, error)`:
1. Call Dora's GetTrades API for followed trader within [start, end]
2. Replay trades chronologically
3. Simulate order placement: calculate size, track positions, compute PnL
4. Return `types.BacktestResult` with TradeRecords, ClosedTrades, summary metrics

### Error Handling

Copy order failures: skip and log. Strategy continues running.

### Testing

- Unit tests for TradeStream pub/sub with mock WS function
- Unit tests for strategy order sizing logic
- Unit tests for backtest simulation
- Counterfeiter-generated fakes for marketAPIClient
