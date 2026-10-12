package notifications_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/docs/openapi"
	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvent_RoundTripJSON(t *testing.T) {
	id := uuid.New().String()
	evt := notifications.Event{
		ID:        id,
		Type:      notifications.EventRunStarted,
		UserID:    "user-1",
		RunID:     "run-1",
		Timestamp: time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC),
		Payload:   map[string]any{"strategy_type": "mean_reversion"},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	var got notifications.Event
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, evt, got)
}

func TestEventType_Values(t *testing.T) {
	cases := []notifications.EventType{
		notifications.EventBacktestStarted,
		notifications.EventBacktestCompleted,
		notifications.EventBacktestFailed,
		notifications.EventRunStarted,
		notifications.EventRunPaused,
		notifications.EventRunResumed,
		notifications.EventRunStopped,
		notifications.EventRunCompleted,
		notifications.EventRunFailed,
		notifications.EventRunStopLoss,
		notifications.EventOrderUpdate,
	}
	assert.Len(t, cases, 11)
}

// TestEventType_OpenAPIEnum asserts every EventType constant the server
// can emit is listed in the published OpenAPI enum, so generated
// clients never reject a real event (and the contract never drifts
// from the implementation).
func TestEventType_OpenAPIEnum(t *testing.T) {
	var spec struct {
		Components struct {
			Schemas map[string]struct {
				Enum []string `json:"enum"`
			} `json:"schemas"`
		} `json:"components"`
	}
	require.NoError(t, json.Unmarshal(openapi.Spec, &spec))
	eventTypeSchema, ok := spec.Components.Schemas["EventType"]
	require.True(t, ok, "docs/openapi/strategy-server.json must define the EventType schema")
	published := make(map[string]bool, len(eventTypeSchema.Enum))
	for _, v := range eventTypeSchema.Enum {
		published[v] = true
	}
	for _, et := range []notifications.EventType{
		notifications.EventBacktestStarted,
		notifications.EventBacktestCompleted,
		notifications.EventBacktestFailed,
		notifications.EventRunStarted,
		notifications.EventRunPaused,
		notifications.EventRunResumed,
		notifications.EventRunStopped,
		notifications.EventRunCompleted,
		notifications.EventRunFailed,
		notifications.EventRunStopLoss,
		notifications.EventOrderUpdate,
	} {
		assert.True(t, published[string(et)],
			"EventType %q is emitted by the server but missing from docs/openapi/strategy-server.json", et)
	}
}

type captureLog struct {
	mu   sync.Mutex
	seen []notifications.Event
}

func (l *captureLog) Insert(_ context.Context, evt notifications.Event) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen = append(l.seen, evt)
	return nil
}

func (l *captureLog) Replay(_ context.Context, _, _ string, _ int) ([]notifications.Event, error) {
	return nil, nil
}

func (l *captureLog) DeleteOlderThan(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

func TestBus_PublishDeliversToSubscribersAndLog(t *testing.T) {
	log := &captureLog{}
	bus := notifications.NewBus(log, notifications.NewHub())
	sub, err := bus.Subscribe(context.Background(), "user-1")
	require.NoError(t, err)
	defer sub.Close()

	evt := notifications.Event{ID: uuid.NewString(), Type: notifications.EventRunStarted, UserID: "user-1", Timestamp: time.Now().UTC()}
	require.NoError(t, bus.Publish(context.Background(), evt))

	select {
	case got := <-sub.Events():
		assert.Equal(t, evt, got)
	case <-time.After(time.Second):
		t.Fatal("did not receive event")
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	require.Len(t, log.seen, 1)
	assert.Equal(t, evt.ID, log.seen[0].ID)
}

func TestBus_PublishContinuesWhenLogFails(t *testing.T) {
	failingLog := notifications.FailingLog{Err: assert.AnError}
	bus := notifications.NewBus(failingLog, notifications.NewHub())
	sub, _ := bus.Subscribe(context.Background(), "user-1")
	defer sub.Close()

	evt := notifications.Event{ID: uuid.NewString(), Type: notifications.EventRunStarted, UserID: "user-1", Timestamp: time.Now().UTC()}
	require.NoError(t, bus.Publish(context.Background(), evt))
	select {
	case <-sub.Events():
	case <-time.After(time.Second):
		t.Fatal("subscriber missed event despite log failure")
	}
}
