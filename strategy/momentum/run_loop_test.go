package momentum_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"

	"github.com/dora-network/bond-trading-strategies/prices"
	strategypkg "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	strategyfakes "github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// TestRunLoop_ProcessesTicksWithoutDeadlock exercises the live run loop
// to prove that s.Update (which takes s.mu) can be called from inside
// the run goroutine without re-entering the same mutex.
//
// Pre-fix, run() acquired s.mu and then called s.Update which also
// locked s.mu — sync.RWMutex is not reentrant, so the first matching
// price tick froze the run goroutine forever. This test would hang
// until the test deadline fired (3s) and fail.
//
// Post-fix, run() snapshots state under a short lock, releases, then
// calls s.Update — same pattern as breakout.handleTick.
func TestRunLoop_ProcessesTicksWithoutDeadlock(t *testing.T) {
	cfg := momentum.DefaultConfig()
	cfg.SignalSource = momentum.SignalSourcePrice
	cfg.FastWindow = 3
	cfg.SlowWindow = 5
	cfg.ATRWindow = 3
	cfg.InitialBalance = decimal.MustNew(1000, 0) // without this, the fake's zero USD position zeroes the budget and cappedOrderQuantity returns (0, false)
	cfg.OrderBookID = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	fake := &strategyfakes.FakeMarketAPIClient{}
	fake.BaseAssetIDStub = func(_ context.Context, _ string) (string, error) {
		return "asset-A", nil
	}
	fake.QuoteAssetIDStub = func(_ context.Context, _ string) (string, error) {
		return "asset-USD", nil
	}
	// Without this, the fake's zero collateral weight zeroes the
	// budget and cappedOrderQuantity returns (0, false). The
	// production path defaults to 1.0 on lookup failure; mirror
	// that by returning 1.0 here.
	fake.AssetCollateralWeightStub = func(_ context.Context, _ string) (decimal.Decimal, error) {
		return decimal.One, nil
	}
	// Return a zero position so the strategy starts in Hold and the
	// poll genuinely observes the MA crossover through s.Update. A
	// non-zero stub would set openSignal to Buy before any tick
	// arrives, masking the regression.
	fake.AssetPositionStub = func(_ context.Context, _ string) (decimal.Decimal, decimal.Decimal, error) {
		return decimal.Zero, decimal.Zero, nil
	}

	s := momentum.New(cfg, nil, momentum.WithMarketAPIClient(fake))

	pricesCh := make(chan map[uuid.UUID]prices.AssetPrice, 4)
	msgs := make(chan strategypkg.Message, 1)

	ctx, cancel := context.WithCancel(context.Background())
	teardown := momentum.RunLoop(s, ctx, msgs, pricesCh)
	defer func() {
		cancel()
		teardown()
	}()

	// Set a non-nil YTM so the run loop does not drop the tick at the
	// nil-YTM contract guard (strategy.go drops nil-YTM ticks before
	// they reach s.Update, which would mask the deadlock regression).
	ytmVal := decimal.MustNew(5, 2)
	assetID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	// Send 6 ticks: slowWin has size 5 so on the 5th tick slowWin is
	// still one short of full. The 6th tick captures windowReady=true
	// AND fastMA > slowMA, so executeDecision runs and openSignal
	// flips to Buy. Pre-fix the run goroutine deadlocks on tick 0
	// and openSignal never flips — the regression test must drive a
	// tick through to Update, not just into the run loop.
	for i := range 6 {
		tick := map[uuid.UUID]prices.AssetPrice{
			assetID: {
				Time:    time.Unix(int64(i), 0).UTC(),
				AssetID: "asset-A",
				Price:   decimal.MustNew(int64(100+i), 0),
				YTM:     &ytmVal,
			},
		}
		select {
		case pricesCh <- tick:
		case <-time.After(time.Second):
			t.Fatalf("timed out sending tick %d", i)
		}
	}

	// Poll openSignal until the strategy reports a Buy, or fail after
	// 3s. Pre-fix the run goroutine deadlocks on tick 0 and openSignal
	// never flips. Post-fix it flips within milliseconds.
	deadline := time.Now().Add(3 * time.Second)
	var got types.Signal
	for time.Now().Before(deadline) {
		got = momentum.OpenSignal(s)
		if got == types.SignalBuy {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	assert.Equal(t, types.SignalBuy, got,
		"openSignal should be Buy after 5 rising ticks; got %v. "+
			"If this test hangs, the run loop is deadlocked on s.mu.",
		got)
}
