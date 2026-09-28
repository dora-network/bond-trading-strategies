package backtest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeFetcher pages through a fixed set of rows; records call timing
// and lets the test assert prefetch ordering.
type fakeFetcher[T any] struct {
	mu         sync.Mutex
	pages      [][]T
	cursors    []string // cursor returned alongside page i; "" = last page
	delays     []time.Duration
	calls      []time.Time
	gotCursors []string
}

func (f *fakeFetcher[T]) Fetch(ctx context.Context, cursor string) ([]T, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, time.Now())
	for i, c := range f.cursors {
		// The start cursor "" addresses the first page.
		if c == cursor || (cursor == "" && i == 0) {
			delay := time.Duration(0)
			if i < len(f.delays) {
				delay = f.delays[i]
			}
			time.Sleep(delay)
			f.gotCursors = append(f.gotCursors, cursor)
			page := f.pages[i]
			next := ""
			if i+1 < len(f.cursors) {
				next = f.cursors[i+1]
			}
			return page, next, nil
		}
	}
	return nil, "", nil
}

func TestPageStream_PrefetchOneAhead(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// 3 pages; page 0 has 100ms delay, page 1 has 10ms, page 2 has 10ms.
	pages := [][]int{
		{1, 2, 3},
		{4, 5, 6},
		{7, 8, 9},
	}
	cursors := []string{"c1", "c2", "c3"} // initial cursor "" matches cursors[0]
	delays := []time.Duration{100 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond}
	f := &fakeFetcher[int]{pages: pages, cursors: cursors, delays: delays}

	ps := newPageStream[int](ctx, 3, f.Fetch, "")

	// First row arrives after fetch(0) returns (~100ms).
	r1, ok, err := ps.next()
	if err != nil || !ok || r1 != 1 {
		t.Fatalf("r1: %v %v %v", r1, ok, err)
	}

	// By the time we got r1, prefetch(page 1) has been signaled and must
	// start within a short window (signal is sent on page install, so an
	// immediate check would race the prefetcher's scheduling).
	waitFetchCalls(t, f, 2)

	// Drain page 0; while we drain, prefetch(page 1) finishes and prefetch(page 2) starts.
	r2, _, _ := ps.next()
	if r2 != 2 {
		t.Errorf("r2=%d", r2)
	}
	r3, _, _ := ps.next()
	if r3 != 3 {
		t.Errorf("r3=%d", r3)
	}

	// Page 1 ready, page 2 prefetch in flight.
	waitFetchCalls(t, f, 3)

	cancel()
	ps.close()
}

// waitFetchCalls polls the fake's locked call log until it reaches n.
func waitFetchCalls[T any](t *testing.T, f *fakeFetcher[T], n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		f.mu.Lock()
		calls := len(f.calls)
		f.mu.Unlock()
		if calls >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected >=%d fetch calls, got %d", n, calls)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPageStream_BlocksOnEmpty(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	released := make(chan struct{})
	f := func(ctx context.Context, cursor string) ([]int, string, error) {
		<-released // block until test releases
		return []int{42}, "", nil
	}
	ps := newPageStream[int](ctx, 10, f, "")

	done := make(chan struct{})
	go func() {
		_, _, _ = ps.next()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("next returned before fetcher was released")
	case <-time.After(50 * time.Millisecond):
	}

	close(released)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("next did not return after fetcher release")
	}

	ps.close()
}

func TestPageStream_BackpressureBlocksPrefetcher(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var fetchCount int32
	f := func(ctx context.Context, cursor string) ([]int, string, error) {
		atomic.AddInt32(&fetchCount, 1)
		// Each fetch returns one row; cursor signals more pages.
		return []int{1}, "more", nil
	}
	ps := newPageStream[int](ctx, 1, f, "")

	// Drain 3 rows.
	for range 3 {
		if _, _, err := ps.next(); err != nil {
			t.Fatalf("drain row: %v", err)
		}
	}

	// After 3 rows the prefetcher should have fetched at least 3 pages:
	// page 0 plus the one-ahead prefetches triggered as each page is drained.
	if atomic.LoadInt32(&fetchCount) < 3 {
		t.Errorf("expected >=3 fetches, got %d", fetchCount)
	}

	ps.close()
}

var errBoom = errors.New("boom")

func TestPageStream_PropagatesFetchError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var calls atomic.Int32
	f := func(ctx context.Context, cursor string) ([]int, string, error) {
		if calls.Add(1) == 1 {
			return []int{1}, "next", nil
		}
		return nil, "", errBoom
	}
	ps := newPageStream[int](ctx, 10, f, "")
	// First row OK.
	r, ok, err := ps.next()
	if err != nil || !ok || r != 1 {
		t.Fatalf("r1: %v %v %v", r, ok, err)
	}
	// Second row triggers error from page-2 fetch.
	_, _, err = ps.next()
	if !errors.Is(err, errBoom) {
		t.Errorf("expected errBoom, got %v", err)
	}
	ps.close()
}
func TestPageStream_Done(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f := func(ctx context.Context, cursor string) ([]int, string, error) {
		return nil, "", nil // empty page, no error -> done
	}
	ps := newPageStream[int](ctx, 10, f, "")
	_, ok, err := ps.next()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false on done")
	}
	ps.close()
}
func TestPageStream_CtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	f := func(ctx context.Context, cursor string) ([]int, string, error) {
		<-ctx.Done()
		return nil, "", ctx.Err()
	}
	ps := newPageStream[int](ctx, 10, f, "")
	cancel() // cancel before first next
	_, _, err := ps.next()
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	ps.close()
}
