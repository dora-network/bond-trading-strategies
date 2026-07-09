package orderupdates_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/dora-network/bond-trading-strategies/notifications/orderupdates"
)

const (
	testGraceDelay   = 50 * time.Millisecond
	testDropWindow   = 200 * time.Millisecond
	testWaitTimeout  = 2 * time.Second
	testPollInterval = 5 * time.Millisecond
	testTeardownWait = 150 * time.Millisecond
)

// fakeNotifier records every published Event and signals on got.
type fakeNotifier struct {
	mu     sync.Mutex
	events []notifications.Event
	got    chan struct{}
}

func newFakeNotifier() *fakeNotifier {
	return &fakeNotifier{got: make(chan struct{}, 32)}
}

func (f *fakeNotifier) Publish(_ context.Context, evt notifications.Event) error {
	f.mu.Lock()
	f.events = append(f.events, evt)
	f.mu.Unlock()
	select {
	case f.got <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeNotifier) eventsSnapshot() []notifications.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]notifications.Event, len(f.events))
	copy(out, f.events)
	return out
}

func (f *fakeNotifier) Subscribe(_ context.Context, _ string) (notifications.Subscription, error) {
	return nil, errors.New("orderupdates_test: Subscribe not implemented")
}

// fakeStream implements ManagerStream. It delivers the configured
// batches immediately and keeps the channel open until ctx is
// cancelled, so runSubscription stays alive for the test's assertions.
type fakeStream struct {
	batches [][]byte
	opens   atomic.Int32
}

func (f *fakeStream) Read(ctx context.Context, _, _ string) <-chan []byte {
	f.opens.Add(1)
	out := make(chan []byte, len(f.batches))
	for _, b := range f.batches {
		out <- b
	}
	go func() {
		<-ctx.Done()
		close(out)
	}()
	return out
}

// waitForOpens polls until the stream's open count reaches want, or
// fails the test after testWaitTimeout. runSubscription calls Read
// asynchronously, so opens is not updated before EnsureSubscribed returns.
func waitForOpens(t *testing.T, s *fakeStream, want int32) {
	t.Helper()
	deadline := time.Now().Add(testWaitTimeout)
	for time.Now().Before(deadline) {
		if s.opens.Load() == want {
			return
		}
		time.Sleep(testPollInterval)
	}
	assert.Equal(t, want, s.opens.Load())
}

func TestManager_PublishesForwardedEvent(t *testing.T) {
	t.Parallel()

	runID := uuid.New()
	lookup := staticLookupFunc(map[uuid.UUID]lookupRow{
		runID: {"alice", "running"},
	})
	msg := []byte(`[{"client_order_id":"mean_reversion.` +
		runID.String() + `.abc","order_id":"ord-1","status":"placed"}]`)
	stream := &fakeStream{batches: [][]byte{msg}}
	notifier := newFakeNotifier()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := orderupdates.NewManager(ctx, notifier, lookup,
		[]string{"mean_reversion"}, stream,
		orderupdates.WithLogger(silentLogger()))

	require.NoError(t, m.EnsureSubscribed(ctx, "alice", "key", runID, "running"))

	select {
	case <-notifier.got:
	case <-time.After(testWaitTimeout):
		t.Fatal("no event published within timeout")
	}

	evts := notifier.eventsSnapshot()
	require.Len(t, evts, 1)
	assert.Equal(t, notifications.EventOrderUpdate, evts[0].Type)
	assert.Equal(t, "alice", evts[0].UserID)
	assert.Equal(t, runID.String(), evts[0].RunID)
}

func TestManager_DropsUnknownRun(t *testing.T) {
	t.Parallel()

	// Lookup knows about no runs → every entry is dropped.
	lookup := staticLookupFunc(map[uuid.UUID]lookupRow{})
	msg := []byte(`[{"client_order_id":"mean_reversion.` +
		uuid.New().String() + `.abc","order_id":"ord-9"}]`)
	stream := &fakeStream{batches: [][]byte{msg}}
	notifier := newFakeNotifier()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := orderupdates.NewManager(ctx, notifier, lookup,
		[]string{"mean_reversion"}, stream,
		orderupdates.WithLogger(silentLogger()))

	require.NoError(t, m.EnsureSubscribed(ctx, "alice", "key", uuid.New(), "running"))

	select {
	case <-notifier.got:
		t.Fatal("event should not have been published for unknown run")
	case <-time.After(testDropWindow):
	}

	assert.Empty(t, notifier.eventsSnapshot())
}

func TestManager_RefCount_GraceBeforeTearDown(t *testing.T) {
	t.Parallel()

	runID := uuid.New()
	lookup := staticLookupFunc(map[uuid.UUID]lookupRow{
		runID: {"alice", "running"},
	})
	stream := &fakeStream{batches: [][]byte{
		[]byte(`[{"client_order_id":"mean_reversion.` + runID.String() + `.z"}]`),
	}}
	notifier := newFakeNotifier()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := orderupdates.NewManager(ctx, notifier, lookup,
		[]string{"mean_reversion"}, stream,
		orderupdates.WithGraceDelay(testGraceDelay),
		orderupdates.WithLogger(silentLogger()))

	// Two subscriptions for the same user share one stream (opens == 1).
	require.NoError(t, m.EnsureSubscribed(ctx, "alice", "k", runID, "running"))
	require.NoError(t, m.EnsureSubscribed(ctx, "alice", "k", runID, "running"))
	waitForOpens(t, stream, 1)

	// Drop to one ref → no teardown.
	m.Unsubscribe("alice")
	waitForOpens(t, stream, 1)

	// Drop to zero → grace timer starts.
	m.Unsubscribe("alice")

	// Re-subscribe before the grace timer fires → timer cancelled,
	// existing subscription reused (no new stream open).
	require.NoError(t, m.EnsureSubscribed(ctx, "alice", "k", runID, "running"))
	waitForOpens(t, stream, 1)

	// Drop to zero again and let the grace timer fire.
	m.Unsubscribe("alice")
	time.Sleep(testTeardownWait)

	// Fresh subscribe after teardown opens a second stream.
	require.NoError(t, m.EnsureSubscribed(ctx, "alice", "k", uuid.New(), "running"))
	waitForOpens(t, stream, 2)
}

func TestManager_RejectsEmptyArgs(t *testing.T) {
	t.Parallel()

	m := orderupdates.NewManager(
		context.Background(),
		newFakeNotifier(),
		staticLookupFunc(map[uuid.UUID]lookupRow{}),
		[]string{"mean_reversion"},
		&fakeStream{},
		orderupdates.WithLogger(silentLogger()),
	)

	assert.Error(t, m.EnsureSubscribed(context.Background(), "", "key", uuid.New(), "running"))
	assert.Error(t, m.EnsureSubscribed(context.Background(), "alice", "", uuid.New(), "running"))
}

// fallbackCounter wraps a fallback closure to count invocations and
// let tests decide what to return from it.
type fallbackCounter struct {
	n  atomic.Int32
	fn func(context.Context, uuid.UUID) (string, string, bool)
}

func (c *fallbackCounter) fallback(ctx context.Context, id uuid.UUID) (string, string, bool) {
	c.n.Add(1)
	if c.fn != nil {
		return c.fn(ctx, id)
	}
	return "", "", false
}

func TestManager_EnsureSubscribedPopulatesCacheAndAvoidsFallback(t *testing.T) {
	t.Parallel()
	counter := &fallbackCounter{}

	cache := orderupdates.NewRunCache(counter.fallback)
	mgr := orderupdates.NewManager(
		context.Background(), newFakeNotifier(), counter.fallback,
		[]string{"mean_reversion", "copy_trading"}, &fakeStream{},
		orderupdates.WithLogger(silentLogger()),
		orderupdates.WithRunCache(cache),
	)

	runID := uuid.New()
	require.NoError(t, mgr.EnsureSubscribed(context.Background(), "alice", "k", runID, "running"))

	user, status, found := cache.Lookup(context.Background(), runID)
	require.True(t, found, "EnsureSubscribed must populate the cache")
	assert.Equal(t, "alice", user)
	assert.Equal(t, "running", status)

	require.NoError(t, mgr.EnsureSubscribed(context.Background(), "alice", "k", runID, "running"))
	assert.Equal(t, int32(0), counter.n.Load(),
		"cached path must not invoke the fallback closure")
}

func TestManager_UpdateRunStatusMutatesCache(t *testing.T) {
	t.Parallel()
	cache := orderupdates.NewRunCache(nil)
	mgr := orderupdates.NewManager(
		context.Background(), newFakeNotifier(), nil,
		[]string{"mean_reversion"}, &fakeStream{},
		orderupdates.WithLogger(silentLogger()),
		orderupdates.WithRunCache(cache),
	)
	runID := uuid.New()
	require.NoError(t, mgr.EnsureSubscribed(context.Background(), "alice", "k", runID, "running"))

	mgr.UpdateRunStatus(runID, "paused")
	_, status, found := cache.Lookup(context.Background(), runID)
	require.True(t, found)
	assert.Equal(t, "paused", status)

	mgr.UpdateRunStatus(runID, "running")
	_, status, _ = cache.Lookup(context.Background(), runID)
	assert.Equal(t, "running", status)
}

func TestManager_PollReconcileDropsStaleEntries(t *testing.T) {
	t.Parallel()
	// Fallback simulates the DB returning terminal status for the
	// cached run. The reconcile should drop the entry. We assert via
	// Snapshot (no Lookup, which would cold-miss and re-populate via
	// the fallback when the cache is empty) that the entry was removed.
	fallback := func(_ context.Context, _ uuid.UUID) (string, string, bool) {
		return "alice", "stopped", true
	}
	cache := orderupdates.NewRunCache(fallback)

	runID := uuid.New()
	cache.Set(runID, "alice", "running")
	require.Len(t, cache.Snapshot(), 1, "entry seeded before reconcile")

	cache.SnapshotAndReconcileForTest(context.Background())

	assert.Empty(t, cache.Snapshot(),
		"poll reconcile must drop entries whose DB status is terminal")
}
