package notifications_test

import (
	"context"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHub_BroadcastToUser(t *testing.T) {
	h := notifications.NewHub()
	defer h.Close()
	_ = h // exported ctor

	sub, err := h.Subscribe(context.Background(), "user-1")
	require.NoError(t, err)
	defer sub.Close()

	evt := notifications.Event{ID: "1", Type: notifications.EventRunStarted, UserID: "user-1"}
	h.Broadcast(evt)

	select {
	case got := <-sub.Events():
		assert.Equal(t, evt, got)
	case <-time.After(time.Second):
		t.Fatal("did not receive broadcast")
	}
}

func TestHub_IsolatesUsers(t *testing.T) {
	h := notifications.NewHub()
	defer h.Close()
	sub1, _ := h.Subscribe(context.Background(), "user-1")
	sub2, _ := h.Subscribe(context.Background(), "user-2")
	defer sub1.Close()
	defer sub2.Close()

	h.Broadcast(notifications.Event{ID: "1", Type: notifications.EventRunStarted, UserID: "user-1"})

	select {
	case <-sub1.Events():
	case <-time.After(time.Second):
		t.Fatal("sub1 missed its own user's event")
	}
	select {
	case evt := <-sub2.Events():
		t.Fatalf("sub2 received event for another user: %+v", evt)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHub_DropsForSlowSubscriber(t *testing.T) {
	h := notifications.NewHub(notifications.WithHubBuffer(2))
	defer h.Close()
	sub, _ := h.Subscribe(context.Background(), "user-1")
	defer sub.Close()

	for i := 0; i < 5; i++ {
		h.Broadcast(notifications.Event{ID: string(rune('a' + i)), Type: notifications.EventRunStarted, UserID: "user-1"})
	}
	// At least one event must be delivered; with buffer=2 the rest are
	// dropped. We assert the subscriber didn't block.
	timeout := time.After(200 * time.Millisecond)
	count := 0
loop:
	for {
		select {
		case <-sub.Events():
			count++
		case <-timeout:
			break loop
		}
	}
	assert.LessOrEqual(t, count, 2)
}
