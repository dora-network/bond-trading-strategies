package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunExitErrorReportsFailed pins that a strategy whose Run loop
// returns an error is reported "failed" with the error surfaced, not
// "completed": startRun wraps the strategy in runExitRecorder so
// maybePublishNaturalCompletion can tell the two exits apart.
func TestRunExitErrorReportsFailed(t *testing.T) {
	runExitStatus(t, errors.New("subscribe to prices: boom"), "failed", "subscribe to prices: boom")
}

// TestRunCleanExitReportsCompleted is the counterpart: a nil exit stays
// "completed" with no error.
func TestRunCleanExitReportsCompleted(t *testing.T) {
	runExitStatus(t, nil, "completed", "")
}

func runExitStatus(t *testing.T, runErr error, wantStatus, wantErr string) {
	t.Helper()

	runID := uuid.Must(uuid.NewV7())
	svc := &strategyfakes.FakeService{}
	svc.RunStrategyReturns(runID, nil)
	handler := NewHandler(svc, WithNow(func() time.Time {
		return time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	}))
	h, ok := handler.(*Handler)
	require.True(t, ok, "NewHandler must return *Handler")

	fake := &strategyfakes.FakeStrategy{}
	fake.RunReturns(runErr)

	detail := &RunDetail{RunSummary: RunSummary{
		ID:           runID,
		Status:       "running",
		DORAUserID:   "user-test",
		StrategyType: "breakout",
	}}
	h.mu.Lock()
	h.runs[runID] = detail
	h.mu.Unlock()

	id, err := h.startRun(context.Background(), detail, fake)
	require.NoError(t, err)
	require.Equal(t, runID, id)
	require.Equal(t, 1, svc.RunStrategyCallCount())

	// The wrapped strategy is what the service goroutine runs; the
	// service deletes its registry entry once Run returns.
	_, wrapped := svc.RunStrategyArgsForCall(0)
	exitErr := wrapped.Run(context.Background(), make(chan strategy.Message), runID)
	if runErr != nil {
		require.ErrorIs(t, exitErr, runErr)
	} else {
		require.NoError(t, exitErr)
	}

	svc.IsRunActiveReturns(false)
	h.maybePublishNaturalCompletion(context.Background(), runID)

	h.mu.RLock()
	defer h.mu.RUnlock()
	assert.Equal(t, wantStatus, detail.Status)
	assert.Equal(t, wantErr, detail.Error)
	assert.NotNil(t, detail.StoppedAt, "terminal transition must stamp StoppedAt")
}

// TestRunExitClassificationIgnoresPriorState pins the resume semantics:
// classification uses only the current invocation's exit result. A
// resumed run's stale error is cleared at launch (a later clean exit is
// not failed by the old one), and a fast startup failure recorded while
// the status had not yet been flipped to "running" (resumeRun flips it
// only after resumePersistedRun returns) still surfaces.
func TestRunExitClassificationIgnoresPriorState(t *testing.T) {
	runID := uuid.Must(uuid.NewV7())
	svc := &strategyfakes.FakeService{}
	svc.RunStrategyReturns(runID, nil)
	handler := NewHandler(svc, WithNow(func() time.Time {
		return time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	}))
	h, ok := handler.(*Handler)
	require.True(t, ok, "NewHandler must return *Handler")

	fake := &strategyfakes.FakeStrategy{}
	fake.RunReturns(errors.New("subscribe to prices: boom"))

	detail := &RunDetail{RunSummary: RunSummary{
		ID:           runID,
		Status:       "paused",
		DORAUserID:   "user-test",
		StrategyType: "breakout",
	}, Error: "old failure from a previous invocation"}
	h.mu.Lock()
	h.runs[runID] = detail
	h.mu.Unlock()

	id, err := h.startRun(context.Background(), detail, fake)
	require.NoError(t, err)
	require.Equal(t, runID, id)

	// The new invocation starts with a clean slate.
	h.mu.RLock()
	assert.Empty(t, detail.Error, "startRun must clear the prior invocation's error")
	h.mu.RUnlock()

	// Fast startup failure lands while the status is still "paused".
	_, wrapped := svc.RunStrategyArgsForCall(0)
	require.Error(t, wrapped.Run(context.Background(), make(chan strategy.Message), runID))

	// The status flip arrives after the exit, before the watcher polls.
	h.mu.Lock()
	detail.Status = "running"
	h.mu.Unlock()

	svc.IsRunActiveReturns(false)
	h.maybePublishNaturalCompletion(context.Background(), runID)

	h.mu.RLock()
	defer h.mu.RUnlock()
	assert.Equal(t, "failed", detail.Status)
	assert.Equal(t, "subscribe to prices: boom", detail.Error)
}

// TestResumeRunDoesNotResurrectClassifiedExit pins the fast-exit race
// the PR #52 review flagged: when the completion watcher classifies a
// rebuilt run's fast startup failure before resumeRun performs its own
// status transition, that transition must not resurrect the run as
// "running" — the exit result was already consumed and the observers
// have exited, so it would be stranded forever.
func TestResumeRunDoesNotResurrectClassifiedExit(t *testing.T) {
	runID := uuid.Must(uuid.NewV7())
	svc := &strategyfakes.FakeService{}
	svc.RunStrategyReturns(runID, nil)
	svc.IsRunActiveReturns(false)
	handler := NewHandler(svc, WithNow(func() time.Time {
		return time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	}))
	h, ok := handler.(*Handler)
	require.True(t, ok, "NewHandler must return *Handler")

	detail := &RunDetail{RunSummary: RunSummary{
		ID:           runID,
		Status:       "running",
		DORAUserID:   "user-test",
		StrategyType: "breakout",
	}}
	h.mu.Lock()
	h.runs[runID] = detail
	h.mu.Unlock()

	// Replay the race window: the completion watcher classifies the
	// exit while resumeRun is still between resumePersistedRun
	// returning and its own status transition.
	h.mu.Lock()
	h.runExitErrs[runID] = errors.New("subscribe to prices: boom")
	h.mu.Unlock()
	h.maybePublishNaturalCompletion(context.Background(), runID)

	// resumeRun's transition arrives late.
	ctx := context.WithValue(context.Background(), doraUserIDContextKey{}, "user-test")
	w := httptest.NewRecorder()
	h.resumeRun(w, ctx, runID)

	h.mu.RLock()
	defer h.mu.RUnlock()
	assert.Equal(t, "failed", detail.Status)
	assert.Equal(t, "subscribe to prices: boom", detail.Error)
	assert.NotNil(t, detail.StoppedAt, "terminal StoppedAt must survive the resume transition")
	assert.Equal(t, http.StatusOK, w.Code)
}

// stubOrderUpdates records where the resume transition's order-status
// side effect runs relative to h.mu.
type stubOrderUpdates struct {
	onUpdateRunStatus func(runID uuid.UUID, status string)
}

func (s *stubOrderUpdates) EnsureSubscribed(context.Context, string, string, uuid.UUID, string) error {
	return nil
}

func (s *stubOrderUpdates) UpdateRunStatus(runID uuid.UUID, status string) {
	if s.onUpdateRunStatus != nil {
		s.onUpdateRunStatus(runID, status)
	}
}

func (s *stubOrderUpdates) Unsubscribe(string) {}

// TestResumeRunPreservesConcurrentStop pins the PR #53 review finding:
// a stop that lands while resumeRun is between its entry status check
// and its transition must not be overwritten — stopRun writes
// "stopped" under h.mu after StopStrategy cancels the run, so the
// resume transition has to defer to it like any other terminal state.
func TestResumeRunPreservesConcurrentStop(t *testing.T) {
	runID := uuid.Must(uuid.NewV7())
	svc := &strategyfakes.FakeService{}
	svc.RunStrategyReturns(runID, nil)
	handler := NewHandler(svc, WithNow(func() time.Time {
		return time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	}))
	h, ok := handler.(*Handler)
	require.True(t, ok, "NewHandler must return *Handler")

	detail := &RunDetail{RunSummary: RunSummary{
		ID:           runID,
		Status:       "running",
		DORAUserID:   "user-test",
		StrategyType: "breakout",
	}}
	h.mu.Lock()
	h.runs[runID] = detail
	h.mu.Unlock()

	// The service call is the window between resumeRun's entry status
	// check and its transition: a concurrent stop's writes land here.
	svc.ResumeStrategyCalls(func(_ context.Context, _ uuid.UUID) error {
		stoppedAt := time.Date(2026, 10, 14, 12, 0, 1, 0, time.UTC)
		h.mu.Lock()
		detail.Status = "stopped"
		detail.UpdatedAt = stoppedAt
		detail.StoppedAt = &stoppedAt
		h.mu.Unlock()
		return nil
	})

	ctx := context.WithValue(context.Background(), doraUserIDContextKey{}, "user-test")
	w := httptest.NewRecorder()
	h.resumeRun(w, ctx, runID)

	h.mu.RLock()
	defer h.mu.RUnlock()
	assert.Equal(t, "stopped", detail.Status)
	assert.NotNil(t, detail.StoppedAt, "concurrent stop's StoppedAt must survive the resume transition")
	assert.Equal(t, http.StatusOK, w.Code)
}

// TestResumeRunSideEffectsHoldTransitionLock pins the serialization half:
// the resume transition's side effects (order-status update, persist,
// run.resumed event) must run under h.mu — the same lock
// maybePublishNaturalCompletion classifies terminal exits under.
// Otherwise a completion-watcher tick can classify, persist, and publish
// the terminal event between the transition and its effects.
func TestResumeRunSideEffectsHoldTransitionLock(t *testing.T) {
	runID := uuid.Must(uuid.NewV7())
	svc := &strategyfakes.FakeService{}
	svc.RunStrategyReturns(runID, nil)
	handler := NewHandler(svc, WithNow(func() time.Time {
		return time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	}))
	h, ok := handler.(*Handler)
	require.True(t, ok, "NewHandler must return *Handler")

	detail := &RunDetail{RunSummary: RunSummary{
		ID:           runID,
		Status:       "paused",
		DORAUserID:   "user-test",
		StrategyType: "breakout",
	}}
	h.mu.Lock()
	h.runs[runID] = detail
	h.mu.Unlock()

	muHeldDuringUpdate := false
	h.orderUpdates = &stubOrderUpdates{
		onUpdateRunStatus: func(uuid.UUID, string) {
			muHeldDuringUpdate = h.mu.TryLock()
			if muHeldDuringUpdate {
				h.mu.Unlock()
			}
		},
	}

	ctx := context.WithValue(context.Background(), doraUserIDContextKey{}, "user-test")
	w := httptest.NewRecorder()
	h.resumeRun(w, ctx, runID)

	assert.False(t, muHeldDuringUpdate,
		"UpdateRunStatus must run under h.mu so terminal classification cannot interleave")
	assert.Equal(t, "running", detail.Status)
	assert.Equal(t, http.StatusOK, w.Code)
}
