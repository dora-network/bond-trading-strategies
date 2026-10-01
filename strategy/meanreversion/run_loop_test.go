package meanreversion_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion/meanreversionfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBarFeed returns a fake CandleFeed serving the given bars on a buffered
// channel.
func newBarFeed(bars []types.Bar) *strategyfakes.FakeCandleFeed {
	ch := make(chan types.Bar, len(bars)+1)
	for _, b := range bars {
		ch <- b
	}
	feed := &strategyfakes.FakeCandleFeed{}
	feed.SubscribeBarsReturns(ch, func() {}, nil)
	return feed
}

// warmBars returns 10 bars alternating low/high YTM (benchmark 0, spread ==
// YTM) so the rolling window fills with measurable variance.
func warmBars(low, high decimal.Decimal) []types.Bar {
	b := make([]types.Bar, 10)
	for i := range b {
		ytm := low
		if i%2 == 1 {
			ytm = high
		}
		b[i] = types.Bar{
			Time:     epoch.Add(time.Duration(i) * time.Hour),
			Close:    bondPriceFromYTM(ytm),
			CloseYTM: ytm,
		}
	}
	return b
}

func tickUpdate(i int, ytm, price decimal.Decimal) map[uuid.UUID]prices.AssetPrice {
	return map[uuid.UUID]prices.AssetPrice{
		uuid.New(): {AssetID: "bond-id", YTM: &ytm, Price: price, Time: epoch.Add(time.Duration(i) * time.Hour)},
	}
}

func fakeClient(held decimal.Decimal) *meanreversionfakes.FakeMarketAPIClient {
	client := &meanreversionfakes.FakeMarketAPIClient{}
	client.BaseAssetIDReturns("bond-id", nil)
	client.AssetCollateralWeightReturns(decimal.One, nil)
	client.QuoteAssetIDReturns("usd-id", nil)
	client.AssetPositionStub = func(_ context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error) {
		if assetID == "bond-id" {
			return held, decimal.Zero, nil
		}
		return decimal.MustNew(100000, 0), decimal.Zero, nil // USD
	}
	return client
}

func runLoopHarness(
	t *testing.T,
	cfg meanreversion.Config,
	client *meanreversionfakes.FakeMarketAPIClient,
	bars []types.Bar,
	ticks []map[uuid.UUID]prices.AssetPrice,
) (*meanreversion.Strategy, func()) {
	t.Helper()

	feed := newBarFeed(bars)
	s := meanreversion.New(cfg, nil,
		meanreversion.WithLogger(slog.Default()),
		meanreversion.WithCandleFeed(feed),
	)
	meanreversion.SetLookupClient(s, client)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	msgCh := make(chan strategy.Message)
	priceCh := make(chan map[uuid.UUID]prices.AssetPrice, len(ticks)+1)
	// Delay tick delivery so warm-up bars are processed first (select order
	// between ready channels is random).
	go func() {
		time.Sleep(150 * time.Millisecond)
		for _, u := range ticks {
			priceCh <- u
		}
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = meanreversion.RunWithPrices(ctx, s, msgCh, priceCh)
	}()
	stop := func() {
		cancel()
		<-done
		close(msgCh)
	}
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			stop()
		}
	})
	return s, stop
}

func runLoopConfig(orderBookID uuid.UUID) meanreversion.Config {
	cfg := defaultConfig()
	cfg.LookbackWindow = 10
	cfg.OrderBookID = orderBookID
	cfg.InitialBalance = decimal.MustNew(10000, 0)
	return cfg
}

// TestRunLoop_BarDrivenEntryFires verifies a bar whose close crosses
// EntryZScore triggers an entry order via the candle feed.
func TestRunLoop_BarDrivenEntryFires(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	client := fakeClient(decimal.Zero)

	bars := warmBars(decimal.MustNew(4, 2), decimal.MustNew(6, 2))
	// mean ≈ 5 %, stddev ≈ 1 %; 8 % close → z ≈ +3 > entry 2.0 → BUY.
	bars = append(bars, types.Bar{
		Time:     epoch.Add(10 * time.Hour),
		Close:    bondPriceFromYTM(decimal.MustNew(8, 2)),
		CloseYTM: decimal.MustNew(8, 2),
	})

	s, stop := runLoopHarness(t, cfg, client, bars, nil)
	defer stop()

	require.Eventually(t, func() bool {
		return client.CreateMarketOrderCallCount() >= 1
	}, timeout, 10*time.Millisecond, "expected entry order from bar signal")
	_, _, side, ctxID, invLev, fromGlobalPos, clientOrderID := client.CreateMarketOrderArgsForCall(0)
	_ = ctxID
	_ = invLev
	_ = fromGlobalPos
	_ = clientOrderID
	assert.Equal(t, doraclient.SIDE_BUY, side)
	assert.Equal(t, types.SignalBuy, meanreversion.OpenSignal(s))
	// Live path: the run loop must have resolved and stored the order
	// book's base asset (Update stamps it as bondID).
	assert.Equal(t, "bond-id", meanreversion.GetBaseAssetID(s))
}

// TestRunLoop_TickOnlyNoEntry is the cutover guard: ticks alone (no bars
// beyond warm-up) must never produce an entry, however extreme their z.
func TestRunLoop_TickOnlyNoEntry(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	client := fakeClient(decimal.Zero)

	bars := warmBars(decimal.MustNew(4, 2), decimal.MustNew(6, 2))
	var ticks []map[uuid.UUID]prices.AssetPrice
	for i := 10; i < 15; i++ {
		ytm := decimal.MustNew(9, 2) // z ≈ +4, far past entry
		ticks = append(ticks, tickUpdate(i, ytm, bondPriceFromYTM(ytm)))
	}

	s, stop := runLoopHarness(t, cfg, client, bars, ticks)
	defer stop()
	time.Sleep(200 * time.Millisecond)

	assert.Zero(t, client.CreateMarketOrderCallCount(),
		"ticks must not drive entries after the bar cutover")
	assert.Equal(t, types.SignalHold, meanreversion.OpenSignal(s))
}

// TestRunLoop_NoNewEntryWhenPositionOpen verifies that when the strategy
// already holds a position (openSignal != Hold, set from bondQty after
// initializeBalances) it does not place another entry order — even when a
// bar crosses the entry threshold. This is the core restart-safety guarantee.
func TestRunLoop_NoNewEntryWhenPositionOpen(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	client := fakeClient(decimal.MustNew(5, 0)) // existing long

	bars := warmBars(decimal.MustNew(4, 2), decimal.MustNew(6, 2))
	for i := 10; i < 15; i++ {
		bars = append(bars, types.Bar{
			Time:     epoch.Add(time.Duration(i) * time.Hour),
			Close:    bondPriceFromYTM(decimal.MustNew(8, 2)),
			CloseYTM: decimal.MustNew(8, 2),
		})
	}

	_, stop := runLoopHarness(t, cfg, client, bars, nil)
	defer stop()
	time.Sleep(200 * time.Millisecond)

	assert.Zero(t, client.CreateMarketOrderCallCount(),
		"expected no new entry order while a position is already open")
}

// TestRunLoop_ClosesPositionOnShouldExit verifies that the run loop calls
// closePosition (placing an opposing market order) when a closed bar's
// z-score triggers ShouldExit for the current open position.
func TestRunLoop_ClosesPositionOnShouldExit(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	client := fakeClient(decimal.MustNew(5, 0)) // existing long

	bars := warmBars(decimal.MustNew(6, 2), decimal.MustNew(8, 2))
	// mean ≈ 7 %, stddev ≈ 1 %; 4 % close → z ≈ -3 ≤ ExitZScore (0.5).
	bars = append(bars, types.Bar{
		Time:     epoch.Add(10 * time.Hour),
		Close:    bondPriceFromYTM(decimal.MustNew(4, 2)),
		CloseYTM: decimal.MustNew(4, 2),
	})

	s, stop := runLoopHarness(t, cfg, client, bars, nil)
	defer stop()

	require.Eventually(t, func() bool {
		return client.CreateMarketOrderCallCount() >= 1
	}, timeout, 10*time.Millisecond, "expected close order to be placed")

	require.Equal(t, 1, client.CreateMarketOrderCallCount())
	_, _, side, qty, invLev, fromGlobalPos, clientOrderID := client.CreateMarketOrderArgsForCall(0)
	_ = invLev
	_ = fromGlobalPos
	_ = clientOrderID
	assert.Equal(t, doraclient.SIDE_SELL, side)
	assert.True(t, qty.Equal(decimal.MustNew(5, 0)), "should close full position quantity")
	assert.Equal(t, types.SignalHold, meanreversion.OpenSignal(s))
}

// runLoopTradesHarness starts the run loop with a caller-held bar channel
// and a pre-seeded trade tape so the imbalance gate state is deterministic
// before the entry bar arrives.
func runLoopTradesHarness(
	t *testing.T,
	cfg meanreversion.Config,
	client *meanreversionfakes.FakeMarketAPIClient,
	trades []streams.TradeEvent,
) (chan types.Bar, func()) {
	t.Helper()

	barCh := make(chan types.Bar, 32)
	feed := &strategyfakes.FakeCandleFeed{}
	feed.SubscribeBarsReturns(barCh, func() {}, nil)
	s := meanreversion.New(cfg, nil,
		meanreversion.WithLogger(slog.Default()),
		meanreversion.WithCandleFeed(feed),
	)
	meanreversion.SetLookupClient(s, client)

	tradeCh := make(chan streams.TradeEvent, len(trades)+1)
	for _, ev := range trades {
		tradeCh <- ev
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	msgCh := make(chan strategy.Message)
	priceCh := make(chan map[uuid.UUID]prices.AssetPrice)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = meanreversion.RunWithPricesAndTrades(ctx, s, msgCh, priceCh, tradeCh)
	}()
	stop := func() {
		cancel()
		<-done
		close(msgCh)
	}
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			stop()
		}
	})
	return barCh, stop
}

// TestRunLoop_ImbalanceGateSuppressesEntry verifies a bar-driven BUY entry is
// filtered when the trade tape shows net selling (opposing flow).
func TestRunLoop_ImbalanceGateSuppressesEntry(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	cfg.ImbalanceWindow = 3
	client := fakeClient(decimal.Zero)

	tape := []streams.TradeEvent{
		{Side: "SELL", Quantity: decimal.One, Price: decimal.MustNew(100, 0)},
		{Side: "SELL", Quantity: decimal.One, Price: decimal.MustNew(100, 0)},
		{Side: "SELL", Quantity: decimal.One, Price: decimal.MustNew(100, 0)},
	}
	barCh, stop := runLoopTradesHarness(t, cfg, client, tape)
	defer stop()

	for _, b := range warmBars(decimal.MustNew(4, 2), decimal.MustNew(6, 2)) {
		barCh <- b
	}
	// Give the loop time to drain warm-up bars and the tape before the
	// entry bar so the gate window is full and opposing.
	time.Sleep(200 * time.Millisecond)
	barCh <- types.Bar{
		Time:     epoch.Add(10 * time.Hour),
		Close:    bondPriceFromYTM(decimal.MustNew(8, 2)),
		CloseYTM: decimal.MustNew(8, 2),
	}
	time.Sleep(300 * time.Millisecond)

	assert.Zero(t, client.CreateMarketOrderCallCount(),
		"BUY entry must be suppressed against a net-selling tape")
}

// TestRunLoop_ImbalanceGateAllowsAgreeingTape verifies the entry fires when
// the tape agrees with the signal direction.
func TestRunLoop_ImbalanceGateAllowsAgreeingTape(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	cfg.ImbalanceWindow = 3
	client := fakeClient(decimal.Zero)

	tape := []streams.TradeEvent{
		{Side: "BUY", Quantity: decimal.One, Price: decimal.MustNew(100, 0)},
		{Side: "BUY", Quantity: decimal.One, Price: decimal.MustNew(100, 0)},
		{Side: "BUY", Quantity: decimal.One, Price: decimal.MustNew(100, 0)},
	}
	barCh, stop := runLoopTradesHarness(t, cfg, client, tape)
	defer stop()

	for _, b := range warmBars(decimal.MustNew(4, 2), decimal.MustNew(6, 2)) {
		barCh <- b
	}
	time.Sleep(200 * time.Millisecond)
	barCh <- types.Bar{
		Time:     epoch.Add(10 * time.Hour),
		Close:    bondPriceFromYTM(decimal.MustNew(8, 2)),
		CloseYTM: decimal.MustNew(8, 2),
	}

	require.Eventually(t, func() bool {
		return client.CreateMarketOrderCallCount() >= 1
	}, timeout, 10*time.Millisecond, "BUY entry must fire on an agreeing tape")
}

// TestRunLoop_IntrabarStopLossOnTick verifies the tick path: while a
// position is open, a tick whose spread z crosses StopLossZScore triggers
// closePosition without waiting for the next closed bar.
func TestRunLoop_IntrabarStopLossOnTick(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	client := fakeClient(decimal.MustNew(5, 0)) // existing long

	// Warm bars fill the window: mean ≈ 7 %, stddev ≈ 1 %.
	bars := warmBars(decimal.MustNew(6, 2), decimal.MustNew(8, 2))
	// Tick at 11 %: z ≈ +3.8 ≥ StopLossZScore (3.5) for the long → stop.
	ticks := []map[uuid.UUID]prices.AssetPrice{
		tickUpdate(10, decimal.MustNew(11, 2), bondPriceFromYTM(decimal.MustNew(11, 2))),
	}
	_, stop := runLoopHarness(t, cfg, client, bars, ticks)
	defer stop()

	require.Eventually(t, func() bool {
		return client.CreateMarketOrderCallCount() >= 1
	}, timeout, 10*time.Millisecond, "expected intra-bar stop-loss close")
	require.Equal(t, 1, client.CreateMarketOrderCallCount())
	_, _, side, ctxID, invLev, fromGlobalPos, clientOrderID := client.CreateMarketOrderArgsForCall(0)
	_ = ctxID
	_ = invLev
	_ = fromGlobalPos
	_ = clientOrderID
	assert.Equal(t, doraclient.SIDE_SELL, side)
}

func TestRunLoop_NoNewEntryWhenQuantityZero(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	// With leverage 1x the tracked USD balance overrides InitialBalance;
	// keep it at $1 so the capped quantity truncates to 0.
	cfg.InitialBalance = decimal.MustNew(1, 0) // budget $1
	client := fakeClient(decimal.Zero)
	client.AssetPositionStub = func(_ context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error) {
		if assetID == "bond-id" {
			return decimal.Zero, decimal.Zero, nil
		}
		return decimal.One, decimal.Zero, nil // USD $1
	}

	// High close price ($100) > budget → capped quantity truncates to 0.
	bars := warmBars(decimal.MustNew(4, 2), decimal.MustNew(6, 2))
	for i := 10; i < 15; i++ {
		bars = append(bars, types.Bar{
			Time:     epoch.Add(time.Duration(i) * time.Hour),
			Close:    decimal.MustNew(100, 0),
			CloseYTM: decimal.MustNew(8, 2), // z >> entry → SignalBuy
		})
	}

	s, stop := runLoopHarness(t, cfg, client, bars, nil)
	defer stop()
	time.Sleep(200 * time.Millisecond)

	assert.Zero(t, client.CreateMarketOrderCallCount(),
		"expected no market order when quantity to order is 0")
	assert.Equal(t, types.SignalHold, meanreversion.OpenSignal(s),
		"expected open signal to remain Hold when quantity is 0")
}

func TestRunLoop_SelfHealsWhenPositionDoesNotExistOnExchange(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	client := fakeClient(decimal.Zero)

	// First AssetPosition call reports an existing long of 5; later calls
	// report flat (the exchange truth after the failed close attempt).
	var count int
	var mu sync.Mutex
	client.AssetPositionStub = func(_ context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error) {
		mu.Lock()
		defer mu.Unlock()
		if assetID == "bond-id" {
			count++
			if count == 1 {
				return decimal.MustNew(5, 0), decimal.Zero, nil
			}
			return decimal.Zero, decimal.Zero, nil
		}
		return decimal.MustNew(50, 0), decimal.Zero, nil
	}
	client.CreateMarketOrderReturns("", errors.New("insufficient position to close"))

	bars := warmBars(decimal.MustNew(6, 2), decimal.MustNew(8, 2))
	// 4 % close → z ≈ -3 → ShouldExit(Buy) → closePosition attempt fails
	// → self-heal to flat.
	bars = append(bars, types.Bar{
		Time:     epoch.Add(10 * time.Hour),
		Close:    bondPriceFromYTM(decimal.MustNew(4, 2)),
		CloseYTM: decimal.MustNew(4, 2),
	})

	s, stop := runLoopHarness(t, cfg, client, bars, nil)
	defer stop()
	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, 1, client.CreateMarketOrderCallCount(),
		"expected exactly 1 market order attempt")
	assert.Equal(t, types.SignalHold, meanreversion.OpenSignal(s))
	assert.True(t, meanreversion.BondQty(s).IsZero())
}

// TestRunLoop_PausedSuppressesIntrabarExit verifies pause is hands-off: an
// open position is not closed by a stop-loss tick while paused.
func TestRunLoop_PausedSuppressesIntrabarExit(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	client := fakeClient(decimal.MustNew(5, 0)) // existing long

	feed := newBarFeed(warmBars(decimal.MustNew(6, 2), decimal.MustNew(8, 2)))
	s := meanreversion.New(cfg, nil,
		meanreversion.WithLogger(slog.Default()),
		meanreversion.WithCandleFeed(feed),
	)
	meanreversion.SetLookupClient(s, client)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	msgCh := make(chan strategy.Message)
	defer close(msgCh)
	priceCh := make(chan map[uuid.UUID]prices.AssetPrice, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = meanreversion.RunWithPrices(ctx, s, msgCh, priceCh)
	}()

	// Let the warm bars fill the window, then pause.
	time.Sleep(150 * time.Millisecond)
	msgCh <- strategy.Pause
	// Stop-loss tick: z ≈ +3.8 ≥ StopLossZScore (3.5) for the long.
	ytm := decimal.MustNew(11, 2)
	priceCh <- map[uuid.UUID]prices.AssetPrice{
		uuid.New(): {AssetID: "bond-id", YTM: &ytm, Price: bondPriceFromYTM(ytm), Time: epoch.Add(10 * time.Hour)},
	}
	time.Sleep(200 * time.Millisecond)

	assert.Zero(t, client.CreateMarketOrderCallCount(),
		"paused run must not close the position on a tick")
	assert.Equal(t, types.SignalBuy, meanreversion.OpenSignal(s))

	cancel()
	<-done
}

// TestRunLoop_FailsWithoutCandleFeed verifies the fail-fast contract: Run
// returns an error when no candle feed is configured.
func TestRunLoop_FailsWithoutCandleFeed(t *testing.T) {
	t.Parallel()

	cfg := runLoopConfig(uuid.Must(uuid.NewV7()))
	client := fakeClient(decimal.Zero)
	s := meanreversion.New(cfg, nil, meanreversion.WithLogger(slog.Default()))
	meanreversion.SetLookupClient(s, client)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	msgCh := make(chan strategy.Message)
	defer close(msgCh)
	priceCh := make(chan map[uuid.UUID]prices.AssetPrice, 1)

	err := meanreversion.RunWithPrices(ctx, s, msgCh, priceCh)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "candle feed not configured")
}
