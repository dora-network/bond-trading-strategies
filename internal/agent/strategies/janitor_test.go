package strategies

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeSweepStore records the calls SweepCapturePending receives so the
// ticker test can assert the cadence without a real database.
type fakeSweepStore struct {
	mu    sync.Mutex
	calls []time.Duration
	failN int // number of leading calls to return an error for
}

func (f *fakeSweepStore) SweepCapturePending(_ context.Context, olderThan time.Duration) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failN > 0 {
		f.failN--
		return 0, errFake
	}
	f.calls = append(f.calls, olderThan)
	return 0, nil
}

func (f *fakeSweepStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

var errFake = &fakeErr{"fake"}

type fakeErr struct{ s string }

func (e *fakeErr) Error() string { return e.s }

// TestJanitor_TicksAtInterval asserts the ticker drives Sweep at the
// configured interval. Uses a tiny interval so the test is fast.
func TestJanitor_TicksAtInterval(t *testing.T) {
	store := &fakeSweepStore{}
	j := NewJanitor(store, 5*time.Millisecond, time.Hour, nil)
	j.Start(t.Context())

	// Wait for ~4 ticks; the ticker is best-effort so allow a
	// generous slack. 4 ticks at 5ms = 20ms; 200ms is 10x slack.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if store.callCount() >= 4 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	j.Stop()

	if got := store.callCount(); got < 4 {
		t.Errorf("ticker fired %d times in 200ms (interval 5ms), want >= 4", got)
	}
}

// TestJanitor_StopIsIdempotent guards against double-close panics in
// the shutdown path.
func TestJanitor_StopIsIdempotent(t *testing.T) {
	store := &fakeSweepStore{}
	j := NewJanitor(store, time.Hour, time.Hour, nil)
	j.Start(t.Context())
	j.Stop()
	// Second call must not panic.
	j.Stop()
}

// TestJanitor_StopBeforeStartIsSafe guards against the "Stop without
// Start" panic, which can happen if shutdown is reached via a panic
// before Start completes.
func TestJanitor_StopBeforeStartIsSafe(t *testing.T) {
	store := &fakeSweepStore{}
	j := NewJanitor(store, time.Hour, time.Hour, nil)
	j.Stop()
}

// TestJanitor_StopDrainsInFlight asserts that an in-flight sweep
// finishes before Stop returns when the sweep respects context
// cancellation. The blocking fake mimics a real DB call that cancels
// on parent-ctx Done, so this is the production drain shape.
func TestJanitor_StopDrainsInFlight(t *testing.T) {
	entered := make(chan struct{})
	gate := make(chan struct{})
	store := &blockingSweepStore{entered: entered, gate: gate}
	j := NewJanitor(store, 5*time.Millisecond, time.Hour, nil)
	j.Start(t.Context())

	// Wait for the first sweep to enter the fake.
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("sweep never entered the fake store")
	}

	// Cancel via Stop. The in-flight sweep sees ctx.Done() and
	// returns promptly. Stop must complete within a small budget.
	done := make(chan struct{})
	go func() {
		j.Stop()
		close(done)
	}()
	select {
	case <-done:
		// Good — the cancel propagated and Stop returned.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stop did not drain within 500ms after ctx cancel")
	}
	close(gate)
}

type blockingSweepStore struct {
	entered chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func (b *blockingSweepStore) SweepCapturePending(ctx context.Context, _ time.Duration) (int, error) {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.gate:
		return 0, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}
