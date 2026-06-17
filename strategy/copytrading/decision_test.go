package copytrading

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDecisionRecorder is a goroutine-safe in-memory DecisionRecorder
// used to assert that the live run loop persists decisions with the
// expected fields.  Backtests do not opt in (WithDecisionStore is not
// called) so the recorder stays empty for those code paths.
type fakeDecisionRecorder struct {
	mu        sync.Mutex
	decisions []strategy.Decision
	saveCalls int   // number of times SaveDecision was entered, regardless of error
	err       error // if non-nil, SaveDecision returns this error
}

func (f *fakeDecisionRecorder) SaveDecision(_ context.Context, d strategy.Decision) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveCalls++
	f.decisions = append(f.decisions, d)
	return f.err
}

func (f *fakeDecisionRecorder) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saveCalls
}

func (f *fakeDecisionRecorder) snapshot() []strategy.Decision {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]strategy.Decision, len(f.decisions))
	copy(out, f.decisions)
	return out
}

// buildStrategyForDecisionTest wires a *Strategy with a fake market
// API and (optionally) a fake decision recorder.  Mirrors the setup
// used by TestRunLoop_PauseSuppressesTradeHandling so that the live
// run loop can resolve asset IDs and place orders without hitting the
// real DORA client.
func buildStrategyForDecisionTest(rec strategy.DecisionRecorder) (*Strategy, *fakeMarketAPI, uuid.UUID, uuid.UUID, uuid.UUID) {
	followed := uuid.New()
	bondID := uuid.New()
	orderBookID := uuid.New()
	usdID := uuid.New().String()

	api := &fakeMarketAPI{}
	api.portfolio = &doraclient.AccountPortfolioV2{
		Accounts: map[string]map[string]doraclient.AccountV2{
			"global": {
				bondID.String(): {AssetId: bondID.String(), IsGlobal: boolPtr(true), Available: "1000"},
				usdID:           {AssetId: usdID, IsGlobal: boolPtr(true), Available: "10000"},
			},
		},
	}
	api.quoteAssetID = usdID

	opts := []func(*Strategy){}
	if rec != nil {
		opts = append(opts, WithDecisionStore(rec))
	}
	cfg := Config{
		FollowedTrader:        followed,
		PercentageOfAvailable: decimal.MustParse("0.5"),
		Leverage:              decimal.MustParse("1.0"),
	}
	s := New(cfg, opts...)
	SetMarketAPI(s, api)
	return s, api, followed, bondID, orderBookID
}

// TestRecordDecision_LiveRunRecordsOnOrder verifies that a successful
// CreateMarketOrder in the live run loop produces exactly one
// strategy_decisions row with the expected side, signal, kind, and
// monotonically-increasing seq.
func TestRecordDecision_LiveRunRecordsOnOrder(t *testing.T) {
	t.Parallel()

	rec := &fakeDecisionRecorder{}
	s, api, followed, bondID, orderBookID := buildStrategyForDecisionTest(rec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh := make(chan strategy.Message)
	tradeCh := make(chan streams.TradeEvent, 4)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = RunWithTrades(s, ctx, msgCh, tradeCh)
	}()

	// Fire two trades so we can also verify seq is monotonic.
	trade := streams.TradeEvent{
		TraderID:    followed,
		OrderBookID: orderBookID,
		AssetID:     bondID,
		Side:        "buy",
		Quantity:    decimal.MustParse("1"),
		Price:       decimal.MustParse("100"),
		ExecutionID: "exec-decision-1",
	}
	tradeCh <- trade
	trade.ExecutionID = "exec-decision-2"
	tradeCh <- trade

	require.Eventually(t, func() bool {
		return len(rec.snapshot()) == 2
	}, 2*time.Second, 20*time.Millisecond, "expected two recorded decisions")

	require.Eventually(t, func() bool {
		return api.createMarketOrderCount() == 2
	}, 2*time.Second, 20*time.Millisecond, "expected two CreateMarketOrder calls")

	decisions := rec.snapshot()
	for i, d := range decisions {
		require.Equal(t, "copy_trading", d.StrategyType)
		// Side is the DORA side string the strategy actually sent:
		// doraclient.SIDE_BUY == "BUY".
		require.Equal(t, "BUY", d.Side)
		// Signal is the strategy's normalised side derived from the
		// followed trade, so it is lowercase.
		require.Equal(t, "buy", d.Signal)
		require.Equal(t, bondID, d.Asset)
		require.Equal(t, orderBookID, d.OrderBookID)
		require.Equal(t, "follow_trade", d.Reason)
		require.Equal(t, strategy.DecisionKindOpen, d.Kind)
		require.True(t, d.CreatedAt.After(time.Time{}), "CreatedAt should be stamped")
		require.Equal(t, s.runID, d.RunID)
		require.Equal(t, int64(i+1), d.Seq, "Seq must be monotonically increasing per run")
		assertValidClientOrderID(t, d.ClientOrderID, "copy_trading", s.runID)
	}

	cancel()
	<-done
}

// TestRecordDecision_FailedOrderNotRecorded verifies that a failed
// CreateMarketOrder does NOT produce a decision row — the row is the
// audit trail of orders that actually reached DORA, not the
// intentions.
func TestRecordDecision_FailedOrderNotRecorded(t *testing.T) {
	t.Parallel()

	rec := &fakeDecisionRecorder{}
	s, api, followed, bondID, orderBookID := buildStrategyForDecisionTest(rec)
	api.orderErr = errOrderFailed

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh := make(chan strategy.Message)
	tradeCh := make(chan streams.TradeEvent, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = RunWithTrades(s, ctx, msgCh, tradeCh)
	}()

	tradeCh <- streams.TradeEvent{
		TraderID:    followed,
		OrderBookID: orderBookID,
		AssetID:     bondID,
		Side:        "buy",
		Quantity:    decimal.MustParse("1"),
		Price:       decimal.MustParse("100"),
		ExecutionID: "exec-decision-fail",
	}

	require.Eventually(t, func() bool {
		return api.createMarketOrderCount() == 1
	}, 2*time.Second, 20*time.Millisecond, "expected the order to be attempted")

	// Deterministic wait: close the trade channel and cancel the
	// context so the run loop drains and exits, then assert on the
	// recorder state.  This is racy-free: by the time the run loop
	// has returned, every trade the loop saw has been fully handled
	// (success or failure) and any recordDecision call it would have
	// made has either fired or will never fire.
	close(tradeCh)
	cancel()
	<-done

	require.Equal(t, 0, rec.callCount(), "no decision must be recorded when the order fails")
	require.Empty(t, rec.snapshot(), "no decision row must exist when the order fails")
}

// TestRecordDecision_NilRecorderIsNoop verifies that the helper is
// structurally a no-op when the handler did not opt in.  This pins
// the contract that the backtest direction can never record
// decisions: the backtest code path does not pass WithDecisionStore,
// so the field is nil and recordDecision short-circuits.
func TestRecordDecision_NilRecorderIsNoop(t *testing.T) {
	t.Parallel()

	s := New(Config{
		FollowedTrader:        uuid.New(),
		PercentageOfAvailable: decimal.MustParse("0.5"),
		Leverage:              decimal.MustParse("1.0"),
	})
	require.Nil(t, s.decisionStore, "decisionStore must be nil when not opted in")

	// recordDecision must be safe and do nothing.
	s.recordDecision(context.Background(), strategy.Decision{
		Side: "buy", Signal: "buy", Reason: "follow_trade",
	})
	require.Nil(t, s.decisionStore)
}

// TestRecordDecision_CloseRecordsActualOrderLeverage pins the contract
// that a close decision records the leverage actually sent to DORA
// (forced to 1.0 for closes, since DORA rejects leveraged closes),
// not the strategy's configured leverage.  Regression guard: prior
// versions recorded s.cfg.Leverage for every order, which would log
// e.g. Leverage=2.0 for a close that was placed at inverse_leverage=1.
func TestRecordDecision_CloseRecordsActualOrderLeverage(t *testing.T) {
	t.Parallel()

	rec := &fakeDecisionRecorder{}
	s, api, followed, bondID, orderBookID := buildStrategyForDecisionTest(rec)

	// Reconfigure with a non-unit configured leverage so the bug would
	// be visible: a close at forced leverage=1 must NOT record 2.0.
	s.cfg.Leverage = decimal.MustParse("2.0")

	// Position the fake portfolio in a Long state (Available > 0,
	// Borrowed = 0) so the subsequent sell is detected as a close by
	// isClose(current=Long, side=Sell).
	api.portfolio = &doraclient.AccountPortfolioV2{
		Accounts: map[string]map[string]doraclient.AccountV2{
			"global": {
				bondID.String(): {
					AssetId:   bondID.String(),
					IsGlobal:  boolPtr(true),
					Available: "1000",
					Borrowed:  "0",
				},
				api.quoteAssetID: {
					AssetId:   api.quoteAssetID,
					IsGlobal:  boolPtr(true),
					Available: "10000",
					Borrowed:  "0",
				},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh := make(chan strategy.Message)
	tradeCh := make(chan streams.TradeEvent, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = RunWithTrades(s, ctx, msgCh, tradeCh)
	}()

	tradeCh <- streams.TradeEvent{
		TraderID:    followed,
		OrderBookID: orderBookID,
		AssetID:     bondID,
		Side:        "sell",
		Quantity:    decimal.MustParse("1"),
		Price:       decimal.MustParse("100"),
		ExecutionID: "exec-decision-close",
	}

	require.Eventually(t, func() bool {
		return len(rec.snapshot()) == 1
	}, 2*time.Second, 20*time.Millisecond, "expected one recorded close decision")
	require.Eventually(t, func() bool {
		return api.createMarketOrderCount() == 1
	}, 2*time.Second, 20*time.Millisecond, "expected one CreateMarketOrder call")

	decisions := rec.snapshot()
	require.Len(t, decisions, 1)
	d := decisions[0]

	require.Equal(t, strategy.DecisionKindClose, d.Kind, "sell-against-Long should be classified as close")
	require.Equal(t, "SELL", d.Side)
	require.Equal(t, decimal.One, d.Leverage, "close must record the forced order leverage, not the configured one")
	require.Equal(t, decimal.One, d.InverseLeverage, "close must be sent at inverse_leverage=1 (no leverage)")
	assertValidClientOrderID(t, d.ClientOrderID, "copy_trading", s.runID)

	cancel()
	<-done
}

// assertValidClientOrderID checks that the recorded ClientOrderID
// follows the live-run contract: <strategy_name>.<run_id>.<uuidv7>.
// Used by both copytrading and meanreversion decision tests.
func assertValidClientOrderID(t *testing.T, got, wantStrategy string, wantRunID uuid.UUID) {
	t.Helper()
	require.NotEmpty(t, got, "ClientOrderID must be set on every live-run decision")

	parts := strings.SplitN(got, ".", 3)
	require.Len(t, parts, 3, "ClientOrderID %q must be <strategy_name>.<run_id>.<uuidv7>", got)
	assert.Equal(t, wantStrategy, parts[0], "strategy name must match")
	assert.Equal(t, wantRunID.String(), parts[1], "run id must match")
	_, err := uuid.Parse(parts[2])
	require.NoError(t, err, "uuidv7 segment must be a valid UUID: %q", parts[2])
}

var errOrderFailed = &orderError{msg: "order rejected by dora"}

type orderError struct{ msg string }

func (e *orderError) Error() string { return e.msg }
