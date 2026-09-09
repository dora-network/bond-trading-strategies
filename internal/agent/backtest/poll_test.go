package backtest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// fakeStore is a Store implementation tailored for PollForTerminal tests.
// Get is the only method the helper exercises; the rest panic to surface
// accidental coupling early in test failures instead of silently returning
// the zero value.
type fakeStore struct {
	// getFn is called by Get; tests stage the responses in slice order.
	getFn func(ctx context.Context, id string) (*Backtest, error)
	// calls counts how many times Get was invoked; tests assert on it
	// when they want to prove the loop actually polled.
	calls atomic.Int32
}

func (f *fakeStore) Get(ctx context.Context, id string) (*Backtest, error) {
	f.calls.Add(1)
	if f.getFn == nil {
		return nil, errors.New("fakeStore: getFn not set")
	}
	return f.getFn(ctx, id)
}

func (f *fakeStore) Create(context.Context, *Backtest) (string, error) {
	panic("fakeStore.Create: not implemented")
}
func (f *fakeStore) List(context.Context, string, int) ([]*Backtest, error) {
	panic("fakeStore.List: not implemented")
}
func (f *fakeStore) UpdateStatus(context.Context, string, Status, *time.Time, *time.Time, string) error {
	panic("fakeStore.UpdateStatus: not implemented")
}
func (f *fakeStore) SetSummary(context.Context, string, *Summary, int) error {
	panic("fakeStore.SetSummary: not implemented")
}
func (f *fakeStore) InsertFills(context.Context, string, []Fill) error {
	panic("fakeStore.InsertFills: not implemented")
}
func (f *fakeStore) GetFills(context.Context, string) ([]Fill, error) {
	panic("fakeStore.GetFills: not implemented")
}
func (f *fakeStore) CancelIfRunning(context.Context, string) (bool, error) {
	panic("fakeStore.CancelIfRunning: not implemented")
}

func (f *fakeStore) FailOrphaned(context.Context) (int, error) {
	panic("fakeStore.FailOrphaned: not implemented")
}

func (f *fakeStore) ListBacktests(context.Context, string, *string, *Status, int) ([]*Backtest, error) {
	panic("fakeStore.ListBacktests: not implemented")
}

func TestPollForTerminal_AlreadyTerminal(t *testing.T) {
	t.Parallel()
	store := &fakeStore{
		getFn: func(context.Context, string) (*Backtest, error) {
			return &Backtest{ID: "b1", Status: StatusSucceeded}, nil
		},
	}
	b, err := PollForTerminal(t.Context(), store, "b1", 2*time.Second, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("PollForTerminal: %v", err)
	}
	if b.Status != StatusSucceeded {
		t.Errorf("status: got %q, want succeeded", b.Status)
	}
	if got := store.calls.Load(); got != 1 {
		t.Errorf("Get calls: got %d, want 1 (no polling)", got)
	}
}

func TestPollForTerminal_PollsUntilTerminal(t *testing.T) {
	t.Parallel()
	// Stage: first call running, second call running, third call succeeded.
	// Use a sync/atomic counter so the closure survives across calls.
	var counter atomic.Int32
	store := &fakeStore{
		getFn: func(context.Context, string) (*Backtest, error) {
			n := counter.Add(1)
			switch n {
			case 1, 2:
				return &Backtest{ID: "b2", Status: StatusRunning}, nil
			default:
				return &Backtest{ID: "b2", Status: StatusSucceeded}, nil
			}
		},
	}
	b, err := PollForTerminal(t.Context(), store, "b2", 2*time.Second, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("PollForTerminal: %v", err)
	}
	if b.Status != StatusSucceeded {
		t.Errorf("status: got %q, want succeeded", b.Status)
	}
	if got := store.calls.Load(); got < 3 {
		t.Errorf("Get calls: got %d, want >= 3 (polled until terminal)", got)
	}
}

func TestPollForTerminal_BudgetExhausted(t *testing.T) {
	t.Parallel()
	store := &fakeStore{
		getFn: func(context.Context, string) (*Backtest, error) {
			return &Backtest{ID: "b3", Status: StatusRunning}, nil
		},
	}
	// Budget is the same order as the interval so the budget-exhausted
	// branch fires after a single poll: the next sleep would overshoot.
	b, err := PollForTerminal(t.Context(), store, "b3", 20*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("PollForTerminal: %v", err)
	}
	if b.Status != StatusRunning {
		t.Errorf("status: got %q, want running (budget exhausted returns last row)", b.Status)
	}
	if got := store.calls.Load(); got < 1 {
		t.Errorf("Get calls: got %d, want >= 1", got)
	}
}

func TestPollForTerminal_ContextCancelled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	// Cancel the context on the first Get; the loop must surface ctx.Err().
	var once atomic.Bool
	// Bind the base function in a local so the wrapper below doesn't
	// re-invoke itself through the field (which would recurse forever).
	baseGet := func(context.Context, string) (*Backtest, error) {
		return &Backtest{ID: "b4", Status: StatusRunning}, nil
	}
	store := &fakeStore{
		getFn: func(c context.Context, id string) (*Backtest, error) {
			if once.CompareAndSwap(false, true) {
				cancel()
			}
			return baseGet(c, id)
		},
	}

	_, err := PollForTerminal(ctx, store, "b4", 2*time.Second, 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected ctx.Err(), got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
