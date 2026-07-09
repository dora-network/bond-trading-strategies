package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/dora-network/bond-trading-strategies/notifications/orderupdates"
)

// TestOrderUpdates_AreIsolatedPerUser proves the Manager's
// read→decode→filter→publish pipeline never lets user A's order-update
// event reach user B's bus subscription. It drives the real Bus+Hub and
// a fake per-user ManagerStream, skipping the WebSocket roundtrip (the
// WS auth model is covered by TestNotificationsRouter_*).
func TestOrderUpdates_AreIsolatedPerUser(t *testing.T) {
	t.Parallel()

	bus := notifications.NewBus(notifications.FailingLog{}, notifications.NewHub())

	runA := uuid.New()
	runB := uuid.New()

	lookup := func(_ context.Context, id uuid.UUID) (string, string, bool) {
		switch id {
		case runA:
			return "alice", "running", true
		case runB:
			return "bob", "running", true
		}
		return "", "", false
	}

	stream := newCaptureStream("alice", "bob")

	mgr := orderupdates.NewManager(
		t.Context(),
		bus,
		lookup,
		[]string{"mean_reversion", "copy_trading"},
		stream,
	)
	defer mgr.Close()

	subA, err := bus.Subscribe(t.Context(), "alice")
	require.NoError(t, err)
	defer subA.Close()
	subB, err := bus.Subscribe(t.Context(), "bob")
	require.NoError(t, err)
	defer subB.Close()

	require.NoError(t, mgr.EnsureSubscribed(t.Context(), "alice", "k-alice", runA, "running"))
	require.NoError(t, mgr.EnsureSubscribed(t.Context(), "bob", "k-bob", runB, "running"))

	// Deliver Bob's update to Bob's stream, Alice's to Alice's. Each
	// goroutine only ever reads its own channel, so the only way a
	// cross-user leak can happen is inside Filter+Bus routing.
	stream.deliver("bob", []byte(
		`[{"client_order_id":"copy_trading.`+runB.String()+`.x","status":"placed"}]`))
	stream.deliver("alice", []byte(
		`[{"client_order_id":"mean_reversion.`+runA.String()+`.x","status":"placed"}]`))

	readOne := func(sub notifications.Subscription, wantUser string) notifications.Event {
		select {
		case e := <-sub.Events():
			return e
		case <-time.After(time.Second):
			t.Fatalf("expected event for %s; got none", wantUser)
			return notifications.Event{}
		}
	}

	gotA := readOne(subA, "alice")
	gotB := readOne(subB, "bob")

	assert.Equal(t, notifications.EventOrderUpdate, gotA.Type)
	assert.Equal(t, "alice", gotA.UserID)
	assert.Equal(t, runA.String(), gotA.RunID)

	assert.Equal(t, notifications.EventOrderUpdate, gotB.Type)
	assert.Equal(t, "bob", gotB.UserID)
	assert.Equal(t, runB.String(), gotB.RunID)

	// Cross-user contamination guard: each user must never see the
	// other's run id.
	assert.NotEqual(t, runB.String(), gotA.RunID, "alice must never see bob's run id")
	assert.NotEqual(t, runA.String(), gotB.RunID, "bob must never see alice's run id")

	// Exactly one event each: no second event may arrive.
	assertNoMore(t, subA, "alice")
	assertNoMore(t, subB, "bob")
}

func assertNoMore(t *testing.T, sub notifications.Subscription, user string) {
	t.Helper()
	select {
	case e := <-sub.Events():
		t.Fatalf("%s received unexpected extra event: %+v", user, e)
	case <-time.After(150 * time.Millisecond):
		// good: no extra event.
	}
}

// captureStream satisfies orderupdates.ManagerStream with one buffered
// channel per DORA user. Channels are pre-built so delivery never races
// with the Manager's Read call.
type captureStream struct {
	chs map[string]chan []byte
}

func newCaptureStream(users ...string) *captureStream {
	chs := make(map[string]chan []byte, len(users))
	for _, u := range users {
		chs[u] = make(chan []byte, 8)
	}
	return &captureStream{chs: chs}
}

func (s *captureStream) Read(_ context.Context, doraUserID, _ string) <-chan []byte {
	return s.chs[doraUserID]
}

func (s *captureStream) deliver(doraUserID string, raw []byte) {
	s.chs[doraUserID] <- raw
}
