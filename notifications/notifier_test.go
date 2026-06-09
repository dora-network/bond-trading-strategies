package notifications_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvent_RoundTripJSON(t *testing.T) {
	id := uuid.New().String()
	evt := notifications.Event{
		ID:         id,
		Type:       notifications.EventRunStarted,
		UserID:     "user-1",
		RunID:      "run-1",
		Timestamp:  time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC),
		Payload:    map[string]any{"strategy_type": "mean_reversion"},
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
		notifications.EventRunStopLoss,
	}
	assert.Len(t, cases, 8)
}
