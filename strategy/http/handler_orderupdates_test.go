package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/notifications/notificationsfakes"
	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
)

// orderUpdatesManagerSpy is a hand-rolled fake satisfying the local
// orderUpdatesManager interface. We don't counterfeiter-generate one —
// the interface is unexported (it's a Handler-side seam), and a fake
// per test file is the lighter touch.
type orderUpdatesManagerSpy struct {
	ensureCalls      []ensureCall
	unsubscribeCalls []string
}

type ensureCall struct {
	doraUserID string
	apiKey     string
}

func (s *orderUpdatesManagerSpy) EnsureSubscribed(_ context.Context, doraUserID, apiKey string) error {
	s.ensureCalls = append(s.ensureCalls, ensureCall{doraUserID: doraUserID, apiKey: apiKey})
	return nil
}

func (s *orderUpdatesManagerSpy) Unsubscribe(doraUserID string) {
	s.unsubscribeCalls = append(s.unsubscribeCalls, doraUserID)
}

// TestHandler_OrderUpdatesCallSitesAreIsolatedPerUser verifies the
// Handler-Manager contract: starting a run calls EnsureSubscribed with
// the authenticated user, stopping it calls Unsubscribe with the same
// user, and no other user id appears in either call list.
func TestHandler_OrderUpdatesCallSitesAreIsolatedPerUser(t *testing.T) {
	t.Parallel()

	runID := uuid.Must(uuid.NewV7())
	svc := &strategyfakes.FakeService{
		RunStrategyStub: func(_ context.Context, _ strategycore.Strategy) (uuid.UUID, error) {
			return runID, nil
		},
	}
	notifier := &notificationsfakes.FakeNotifier{}
	spy := &orderUpdatesManagerSpy{}

	handler := strategyhttp.NewHandler(svc,
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
		strategyhttp.WithNotifier(notifier),
		strategyhttp.WithOrderUpdatesManager(spy),
	)

	// 1. Starting a run invokes EnsureSubscribed once with the
	//    authenticated user. The "test-user" default comes from
	//    doraClientFunc.GetUserID (handler_test.go).
	body := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window": 20,
			"entry_z_score":   2.0,
			"exit_z_score":    0.5,
			"order_book_id":   uuid.Must(uuid.NewV7()).String(),
			"tenor":           "10Y",
			"initial_balance": 5.5,
			"leverage":        2.0,
		},
	}
	rec := performJSONRequest(t, handler, "/v1/runs", body)
	require.Equal(t, http.StatusCreated, rec.Code, "create run should not 5xx")

	// 2. Stopping the run invokes Unsubscribe with the same user. The
	//    stop endpoint is DELETE /v1/runs/{id}; performJSONRequest only
	//    issues POST, so the DELETE is issued inline.
	rec = httptest.NewRecorder()
	stopReq := httptest.NewRequestWithContext(context.Background(), http.MethodDelete, "/v1/runs/"+runID.String(), nil)
	stopReq.Header.Set("Authorization", "ApiKey test-key")
	handler.ServeHTTP(rec, stopReq)
	require.Equal(t, http.StatusOK, rec.Code, "unexpected stop status %d", rec.Code)

	// 3. Assert the spy recorded exactly the right pattern: one ensure
	//    for "test-user", one unsubscribe for "test-user", and no calls
	//    for any other user.
	require.Len(t, spy.ensureCalls, 1, "exactly one ensure call")
	assert.Equal(t, "test-user", spy.ensureCalls[0].doraUserID,
		"ensure must use the authenticated user's id, not someone else's")

	require.Len(t, spy.unsubscribeCalls, 1, "exactly one unsubscribe call")
	assert.Equal(t, "test-user", spy.unsubscribeCalls[0],
		"unsubscribe must use the same user that ensure was called for")
}

// TestHandler_OrderUpdatesNilManagerIsNoop verifies the nil-guards
// added when the WithOrderUpdatesManager option is not applied. The
// Handler must continue to handle create/stop runs without panicking
// even when h.orderUpdates is nil (the wire-up step is optional).
func TestHandler_OrderUpdatesNilManagerIsNoop(t *testing.T) {
	t.Parallel()

	runID := uuid.Must(uuid.NewV7())
	svc := &strategyfakes.FakeService{
		RunStrategyStub: func(_ context.Context, _ strategycore.Strategy) (uuid.UUID, error) {
			return runID, nil
		},
	}
	// Note: WithOrderUpdatesManager is intentionally NOT applied.
	handler := strategyhttp.NewHandler(svc,
		strategyhttp.WithDORAClient(doraClientFunc{}),
		strategyhttp.WithTradesHistoryStore(nil),
	)

	body := map[string]any{
		"strategy_type": "mean_reversion",
		"config": map[string]any{
			"lookback_window": 20,
			"entry_z_score":   2.0,
			"exit_z_score":    0.5,
			"order_book_id":   uuid.Must(uuid.NewV7()).String(),
			"tenor":           "10Y",
			"initial_balance": 5.5,
			"leverage":        2.0,
		},
	}

	// createRun and stopRun must both survive a nil orderUpdates field.
	// These calls assert no panic happens on either path.
	assert.NotPanics(t, func() {
		rec := performJSONRequest(t, handler, "/v1/runs", body)
		assert.True(t, rec.Code == http.StatusCreated || rec.Code == http.StatusInternalServerError,
			"createRun without OrderUpdates: status %d (Created or 5xx both OK; nil-manager must not crash the handler)",
			rec.Code)
	}, "createRun must not panic when WithOrderUpdatesManager was not applied")

	assert.NotPanics(t, func() {
		rec := httptest.NewRecorder()
		stopReq := httptest.NewRequestWithContext(context.Background(), http.MethodDelete, "/v1/runs/"+runID.String(), nil)
		stopReq.Header.Set("Authorization", "ApiKey test-key")
		handler.ServeHTTP(rec, stopReq)
	}, "stopRun must not panic when WithOrderUpdatesManager was not applied")
}
