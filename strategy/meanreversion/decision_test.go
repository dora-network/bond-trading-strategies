package meanreversion

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"
)

// silentLogger returns a *slog.Logger that discards all output.  Used
// in unit tests so that recordDecision's error-log path is exercised
// without spamming the test output.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeDecisionRecorder is a goroutine-safe in-memory DecisionRecorder
// used by the mean-reversion unit tests to assert that recordDecision
// stamps the right fields.  It mirrors the fake in copytrading.
type fakeDecisionRecorder struct {
	mu        sync.Mutex
	decisions []strategy.Decision
	err       error
}

func (f *fakeDecisionRecorder) SaveDecision(_ context.Context, d strategy.Decision) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.decisions = append(f.decisions, d)
	return f.err
}

func (f *fakeDecisionRecorder) snapshot() []strategy.Decision {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]strategy.Decision, len(f.decisions))
	copy(out, f.decisions)
	return out
}

// TestRecordDecision_StampsRunIDSequenceAndType verifies the helper
// fills in RunID, Seq, and StrategyType from the strategy state and
// that Seq is monotonically increasing per call.
func TestRecordDecision_StampsRunIDSequenceAndType(t *testing.T) {
	t.Parallel()

	s := &Strategy{runID: uuid.New(), log: silentLogger()}
	rec := &fakeDecisionRecorder{}
	s.decisionStore = rec

	base := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
	orderBookID := uuid.New()
	assetID := uuid.New()

	s.recordDecision(context.Background(), strategy.Decision{
		OrderBookID:        orderBookID,
		Asset:              assetID,
		Side:               "BUY",
		Signal:             "buy",
		Quantity:           decimal.MustParse("100"),
		Price:              decimal.MustParse("99.5"),
		Leverage:           decimal.MustParse("1.0"),
		InverseLeverage:    decimal.One,
		FromGlobalPosition: true,
		Kind:               strategy.DecisionKindOpen,
		Reason:             "z_score_entry",
		ReasonDetail:       "z=2.1",
		CreatedAt:          base,
	})

	s.recordDecision(context.Background(), strategy.Decision{
		OrderBookID:        orderBookID,
		Asset:              assetID,
		Side:               "SELL",
		Signal:             "sell",
		Quantity:           decimal.MustParse("100"),
		Price:              decimal.MustParse("100.5"),
		Leverage:           decimal.MustParse("1.0"),
		InverseLeverage:    decimal.One,
		FromGlobalPosition: true,
		Kind:               strategy.DecisionKindClose,
		Reason:             "z_score_exit",
		ReasonDetail:       "spread reverted",
		CreatedAt:          base.Add(time.Second),
	})

	got := rec.snapshot()
	require.Len(t, got, 2)

	require.Equal(t, s.runID, got[0].RunID)
	require.Equal(t, int64(1), got[0].Seq)
	require.Equal(t, "mean_reversion", got[0].StrategyType)
	require.Equal(t, strategy.DecisionKindOpen, got[0].Kind)
	require.Equal(t, "z_score_entry", got[0].Reason)
	require.Equal(t, base, got[0].CreatedAt, "CreatedAt must be preserved when set by caller")

	require.Equal(t, s.runID, got[1].RunID)
	require.Equal(t, int64(2), got[1].Seq, "Seq must be monotonically increasing per run")
	require.Equal(t, "mean_reversion", got[1].StrategyType)
	require.Equal(t, strategy.DecisionKindClose, got[1].Kind)
	require.Equal(t, "z_score_exit", got[1].Reason)
	require.Equal(t, base.Add(time.Second), got[1].CreatedAt)
}

// TestRecordDecision_StampsCreatedAtWhenZero verifies the helper
// stamps CreatedAt at the moment of the call when the caller does
// not supply a timestamp.
func TestRecordDecision_StampsCreatedAtWhenZero(t *testing.T) {
	t.Parallel()

	s := &Strategy{runID: uuid.New(), log: silentLogger()}
	rec := &fakeDecisionRecorder{}
	s.decisionStore = rec

	before := time.Now().UTC()
	s.recordDecision(context.Background(), strategy.Decision{
		OrderBookID:     uuid.New(),
		Asset:           uuid.New(),
		Side:            "BUY",
		Signal:          "buy",
		Quantity:        decimal.MustParse("10"),
		Price:           decimal.MustParse("100"),
		Leverage:        decimal.One,
		InverseLeverage: decimal.One,
		Kind:            strategy.DecisionKindOpen,
		Reason:          "z_score_entry",
	})
	after := time.Now().UTC()

	got := rec.snapshot()
	require.Len(t, got, 1)
	require.False(t, got[0].CreatedAt.Before(before), "CreatedAt should be >= before")
	require.False(t, got[0].CreatedAt.After(after), "CreatedAt should be <= after")
}

// TestRecordDecision_NilRecorderIsNoop verifies the backtest path
// (no WithDecisionStore) never touches the recorder and never
// panics.
func TestRecordDecision_NilRecorderIsNoop(t *testing.T) {
	t.Parallel()

	s := &Strategy{runID: uuid.New(), log: silentLogger()}
	require.Nil(t, s.decisionStore, "decisionStore must be nil when not opted in")

	// Should not panic.
	s.recordDecision(context.Background(), strategy.Decision{
		Side: "BUY", Signal: "buy", Reason: "z_score_entry",
	})
	require.Equal(t, int64(0), s.decisionSeq, "decisionSeq must not advance when recorder is nil")
}

// TestRecordDecision_RecorderErrorIsLoggedNotPropagated verifies the
// helper does not return an error even when the underlying recorder
// fails — the live run is the source of truth for what orders were
// placed, and a degraded persistence must not kill the run.
func TestRecordDecision_RecorderErrorIsLoggedNotPropagated(t *testing.T) {
	t.Parallel()

	s := &Strategy{runID: uuid.New(), log: silentLogger()}
	rec := &fakeDecisionRecorder{err: errRecorderFailed}
	s.decisionStore = rec

	// recordDecision has no return value, so the assertion is that it
	// returns and that the seq counter still advanced.
	s.recordDecision(context.Background(), strategy.Decision{
		OrderBookID: uuid.New(),
		Asset:       uuid.New(),
		Side:        "BUY", Signal: "buy",
		Quantity: decimal.One, Price: decimal.One,
		Leverage: decimal.One, InverseLeverage: decimal.One,
		Kind: strategy.DecisionKindOpen, Reason: "z_score_entry",
	})
	require.Equal(t, int64(1), s.decisionSeq, "seq must advance even if recorder errors")
}

var errRecorderFailed = &recorderError{msg: "boom"}

type recorderError struct{ msg string }

func (e *recorderError) Error() string { return e.msg }
