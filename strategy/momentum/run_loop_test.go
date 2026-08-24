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
	cfg.OrderBookID = uuid.MustParse("11111111-1111-1111-1111-111111111111")

	fake := &strategyfakes.FakeMarketAPIClient{}
	fake.BaseAssetIDStub = func(_ context.Context, _ string) (string, error) {
		return "asset-A", nil
	}
	fake.AssetPositionStub = func(_ context.Context, _ string) (decimal.Decimal, decimal.Decimal, error) {
		return decimal.MustNew(1, 0), decimal.Zero, nil
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

	// Send 5 rising ticks; after tick 5 both windows are full and the
	// fast MA has crossed above the slow MA, so openSignal flips to Buy.
	assetID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	for i := range 5 {
		tick := map[uuid.UUID]prices.AssetPrice{
			assetID: {
				Time:    time.Unix(int64(i), 0).UTC(),
				AssetID: "asset-A",
				Price:   decimal.MustNew(int64(100+i), 0),
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
