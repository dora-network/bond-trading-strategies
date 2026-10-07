package momentum_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/prices"
	strategypkg "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

const runLoopTimeout = 3 * time.Second

var epoch = time.Unix(0, 0).UTC()

// newBarFeed returns a fake CandleFeed serving the given bars on a
// buffered channel.
func newBarFeed(bars []types.Bar) *strategyfakes.FakeCandleFeed {
	ch := make(chan types.Bar, len(bars)+1)
	for _, b := range bars {
		ch <- b
	}
	feed := &strategyfakes.FakeCandleFeed{}
	feed.SubscribeBarsReturns(ch, func() {}, nil)
	return feed
}

// momentumBars builds n rising bars (closes 100..100+n-1) with H=L=C so
// the ATR is |Δclose| = 1 per bar.
func momentumBars(n int) []types.Bar {
	b := make([]types.Bar, n)
	for i := range b {
		p := decimal.MustNew(int64(100+i), 0)
		b[i] = types.Bar{
			Time:   epoch.Add(time.Duration(i) * time.Minute),
			Open:   p,
			High:   p,
			Low:    p,
			Close:  p,
			Volume: decimal.MustNew(10, 0),
		}
	}
	return b
}

func momFakeClient(held int64) *strategyfakes.FakeMarketAPIClient {
	client := &strategyfakes.FakeMarketAPIClient{}
	client.BaseAssetIDReturns("asset-A", nil)
	client.QuoteAssetIDReturns("asset-USD", nil)
	client.AssetCollateralWeightReturns(decimal.One, nil)
	client.AssetPositionStub = func(_ context.Context, assetID string) (decimal.Decimal, decimal.Decimal, error) {
		if assetID == "asset-A" {
			return decimal.MustNew(held, 0), decimal.Zero, nil
		}
		return decimal.MustNew(1000, 0), decimal.Zero, nil // USD
	}
	return client
}

func momRunConfig(orderBookID uuid.UUID) momentum.Config {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourcePrice
	cfg.FastWindow = 3
	cfg.SlowWindow = 5
	cfg.ATRWindow = 3
	cfg.VolumeAvgWindow = 0
	cfg.OrderBookID = orderBookID
	cfg.InitialBalance = decimal.MustNew(1000, 0)
	return cfg
}

// momRunLoopHarness starts the run loop with the given bars pre-loaded
// on the feed and ticks delivered after a short delay (select order
// between ready channels is random; ticks must not race the bars).
func momRunLoopHarness(
	t *testing.T,
	cfg momentum.Config,
	client *strategyfakes.FakeMarketAPIClient,
	bars []types.Bar,
	ticks []map[uuid.UUID]prices.AssetPrice,
) *momentum.Strategy {
	t.Helper()

	feed := newBarFeed(bars)
	s := momentum.New(cfg, nil,
		momentum.WithMarketAPIClient(client),
		momentum.WithCandleFeed(feed),
	)

	ctx, cancel := context.WithTimeout(context.Background(), runLoopTimeout)
	msgCh := make(chan strategypkg.Message, 1)
	priceCh := make(chan map[uuid.UUID]prices.AssetPrice, len(ticks)+1)
	go func() {
		time.Sleep(150 * time.Millisecond)
		for _, u := range ticks {
			priceCh <- u
		}
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = momentum.RunLoop(ctx, s, msgCh, priceCh)
	}()
	stop := func() {
		cancel()
		<-done
	}
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			stop()
		}
	})
	_ = stop
	return s
}

// TestRunLoop_BarDrivenEntryFires: rising bars fill the windows and the
// MA crossover entry fires through the candle feed.
func TestRunLoop_BarDrivenEntryFires(t *testing.T) {
	cfg := momRunConfig(uuid.Must(uuid.NewV7()))
	client := momFakeClient(0)

	s := momRunLoopHarness(t, cfg, client, momentumBars(7), nil)

	deadline := time.Now().Add(runLoopTimeout)
	for time.Now().Before(deadline) {
		if momentum.OpenSignal(s) == types.SignalBuy {
			// Live path: the run loop must have resolved and stored the
			// order book's base asset (Update stamps it as bondID).
			assert.Equal(t, "asset-A", momentum.GetBaseAssetID(s))
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("openSignal should be Buy after 5 rising bars; got %v", momentum.OpenSignal(s))
}

// TestRunLoop_TickOnlyNoEntry is the cutover guard: ticks alone must
// never drive an entry after the bar cutover, however extreme their
// price.
func TestRunLoop_TickOnlyNoEntry(t *testing.T) {
	cfg := momRunConfig(uuid.Must(uuid.NewV7()))
	client := momFakeClient(0)

	// Flat bars (no trend → Hold) so the windows are warm but flat.
	bars := flatBars(6)
	var ticks []map[uuid.UUID]prices.AssetPrice
	for i := 6; i < 12; i++ {
		ticks = append(ticks, map[uuid.UUID]prices.AssetPrice{
			uuid.Must(uuid.NewV7()): {
				Time:    epoch.Add(time.Duration(i) * time.Minute),
				AssetID: "asset-A",
				Price:   decimal.MustNew(int64(1000+i), 0), // extreme uptrend
				YTM:     ptrYTM(),
			},
		})
	}

	s := momRunLoopHarness(t, cfg, client, bars, ticks)
	time.Sleep(400 * time.Millisecond)

	assert.Zero(t, client.CreateMarketOrderCallCount(),
		"ticks must not drive entries after the bar cutover")
	assert.Equal(t, types.SignalHold, momentum.OpenSignal(s))
}

func flatBars(n int) []types.Bar {
	b := make([]types.Bar, n)
	for i := range b {
		p := decimal.MustNew(100, 0)
		b[i] = types.Bar{
			Time: epoch.Add(time.Duration(i) * time.Minute),
			Open: p, High: p, Low: p, Close: p,
			Volume: decimal.MustNew(10, 0),
		}
	}
	return b
}

func ptrYTM() *decimal.Decimal {
	v := decimal.MustNew(5, 2)
	return &v
}

// TestRunLoop_IntrabarStopLossOnTick: while a long is open (from
// initializeBalances), a tick crossing the entry-anchored stop band
// triggers closePosition without waiting for the next closed bar.
func TestRunLoop_IntrabarStopLossOnTick(t *testing.T) {
	cfg := momRunConfig(uuid.Must(uuid.NewV7()))
	cfg.StopLossATR = decimal.One // stop distance = 1 × ATR
	client := momFakeClient(5)    // existing long of 5

	// Flat bars: closes alternate 100/101 → ATR = 1, anchor 101.
	bars := oscBars(6)
	ticks := []map[uuid.UUID]prices.AssetPrice{
		{
			uuid.Must(uuid.NewV7()): {
				Time:    epoch.Add(10 * time.Minute),
				AssetID: "asset-A",
				Price:   decimal.MustNew(95, 0), // well below 101 − 1×1
				YTM:     ptrYTM(),
			},
		},
	}

	_ = momRunLoopHarness(t, cfg, client, bars, ticks)

	require.Eventually(t, func() bool {
		return client.CreateMarketOrderCallCount() >= 1
	}, runLoopTimeout, 10*time.Millisecond, "expected intra-bar stop-loss close")
	require.Equal(t, 1, client.CreateMarketOrderCallCount())
}

// oscBars alternates closes 100/101 so the true-range ATR settles at 1
// while the MAs stay ~flat (no crossover signal).
func oscBars(n int) []types.Bar {
	b := make([]types.Bar, n)
	for i := range b {
		p := decimal.MustNew(100, 0)
		if i%2 == 1 {
			p = decimal.MustNew(101, 0)
		}
		b[i] = types.Bar{
			Time: epoch.Add(time.Duration(i) * time.Minute),
			Open: p, High: p, Low: p, Close: p,
			Volume: decimal.MustNew(10, 0),
		}
	}
	return b
}

// TestRunLoop_PausedSuppressesIntrabarExit mirrors meanreversion's pause
// test: an open position is not closed by a stop-band tick while paused.
func TestRunLoop_PausedSuppressesIntrabarExit(t *testing.T) {
	cfg := momRunConfig(uuid.Must(uuid.NewV7()))
	cfg.StopLossATR = decimal.One
	client := momFakeClient(5)

	feed := newBarFeed(oscBars(6))
	s := momentum.New(cfg, nil,
		momentum.WithMarketAPIClient(client),
		momentum.WithCandleFeed(feed),
	)

	ctx, cancel := context.WithTimeout(context.Background(), runLoopTimeout)
	defer cancel()
	msgCh := make(chan strategypkg.Message, 1)
	defer close(msgCh)
	priceCh := make(chan map[uuid.UUID]prices.AssetPrice, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = momentum.RunLoop(ctx, s, msgCh, priceCh)
	}()

	// Let the warm bars fill the windows and the resume anchor seed,
	// then pause before the stop-crossing tick.
	time.Sleep(200 * time.Millisecond)
	msgCh <- strategypkg.Pause
	priceCh <- map[uuid.UUID]prices.AssetPrice{
		uuid.Must(uuid.NewV7()): {
			Time:    epoch.Add(10 * time.Minute),
			AssetID: "asset-A",
			Price:   decimal.MustNew(95, 0),
			YTM:     ptrYTM(),
		},
	}
	time.Sleep(300 * time.Millisecond)

	assert.Zero(t, client.CreateMarketOrderCallCount(),
		"paused run must not close the position on a tick")
	assert.Equal(t, types.SignalBuy, momentum.OpenSignal(s))

	cancel()
	<-done
}
