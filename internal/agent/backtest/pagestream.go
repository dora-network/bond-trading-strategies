package backtest

import (
	"context"
	"sync"
	"time"
)

// pageFetcher is the generic fetch function the stream drives.
type pageFetcher[T any] func(ctx context.Context, cursor string) ([]T, string, error)

// pageStream[T] pulls one page ahead of the consumer. A prefetcher
// goroutine fetches page K+1 while the consumer drains page K; the
// result is parked in nextCh (cap 1). The consumer's reqNextCh
// signal (cap 1) tells the prefetcher to fetch the next page after
// the consumer drains the parked one.
type pageStream[T any] struct {
	ctx      context.Context
	pageSize int
	fetch    pageFetcher[T]

	rows   []T
	idx    int
	done   bool
	err    error
	cursor string

	nextCh    chan streamPage[T]
	reqNextCh chan struct{}
	wakeCh    chan struct{}

	stopCh chan struct{}

	prefetcherDone chan struct{}
	closeOnce      sync.Once
}

type streamPage[T any] struct {
	rows   []T
	cursor string
	done   bool
	err    error
}

func newPageStream[T any](ctx context.Context, pageSize int, fetch pageFetcher[T], startCursor string) *pageStream[T] {
	ps := &pageStream[T]{
		ctx:            ctx,
		pageSize:       pageSize,
		fetch:          fetch,
		nextCh:         make(chan streamPage[T], 1),
		reqNextCh:      make(chan struct{}, 1),
		wakeCh:         make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
		cursor:         startCursor,
		prefetcherDone: make(chan struct{}),
	}
	go ps.prefetch()
	return ps
}

// prefetch is the long-lived goroutine. It fetches page N, parks it
// in nextCh (blocking if the consumer hasn't drained the previous
// page), then waits on reqNextCh before fetching page N+1.
func (ps *pageStream[T]) prefetch() {
	defer close(ps.prefetcherDone)
	cursor := ps.cursor
	for {
		rows, next, err := ps.fetch(ps.ctx, cursor)
		page := streamPage[T]{rows: rows, cursor: next, err: err}
		if err == nil && len(rows) == 0 {
			page.done = true
			page.cursor = ""
		}
		select {
		case ps.nextCh <- page:
		case <-ps.ctx.Done():
			return
		case <-ps.stopCh:
			return
		}
		// Wake any merge blocked on this stream's wakeCh (cap 1; the
		// non-blocking send collapses stale signals).
		select {
		case ps.wakeCh <- struct{}{}:
		default:
		}
		if page.done || page.err != nil {
			return
		}
		cursor = next
		select {
		case <-ps.reqNextCh:
		case <-ps.ctx.Done():
			return
		case <-ps.stopCh:
			return
		}
	}
}

// installPage parks a fetched page as the stream's current state and
// releases the prefetcher to fetch the next page (one-ahead).
func (ps *pageStream[T]) installPage(np streamPage[T]) {
	ps.rows = np.rows
	ps.cursor = np.cursor
	ps.done = np.done
	ps.err = np.err
	ps.idx = 0
	select {
	case ps.reqNextCh <- struct{}{}:
	default:
	}
}

// next returns the next row from the current parked page. When the
// parked page is exhausted, it blocks until the prefetcher parks the
// next page. ok=false when the stream is exhausted; err != nil when
// the prefetcher failed.
func (ps *pageStream[T]) next() (T, bool, error) {
	var zero T
	if ps.err != nil {
		return zero, false, ps.err
	}
	if ps.idx >= len(ps.rows) {
		if ps.done {
			return zero, false, nil
		}
		// Non-blocking peek first; block only if nothing is parked yet.
		var np streamPage[T]
		select {
		case np = <-ps.nextCh:
		default:
			select {
			case np = <-ps.nextCh:
			case <-ps.ctx.Done():
				return zero, false, ps.ctx.Err()
			}
		}
		ps.installPage(np)
		if ps.err != nil {
			return zero, false, ps.err
		}
	}
	if ps.idx < len(ps.rows) {
		row := ps.rows[ps.idx]
		ps.idx++
		// Signal demand per consumed row. The cap-1 non-blocking send
		// collapses signals; the prefetcher's next park (cap-1 nextCh)
		// is what actually bounds pages in flight.
		select {
		case ps.reqNextCh <- struct{}{}:
		default:
		}
		return row, true, nil
	}
	if ps.done {
		return zero, false, nil
	}
	return zero, false, ps.ctx.Err()
}

// close stops the prefetcher and waits for it to exit. Idempotent.
func (ps *pageStream[T]) close() {
	ps.closeOnce.Do(func() {
		close(ps.stopCh)
	})
	<-ps.prefetcherDone
}

// peek returns the head row and its timestamp without consuming it.
// It drains a parked page only when the current page is exhausted;
// it never blocks waiting for a new page. ok=false means "no row
// available right now" — the caller (mergeStream.next) falls back to
// its blocking global select. err is sticky (parked fetch failure).
func (ps *pageStream[T]) peek() (T, time.Time, bool, error) {
	var zero T
	if ps.idx >= len(ps.rows) {
		select {
		case np := <-ps.nextCh:
			ps.installPage(np)
		default:
		}
	}
	if ps.err != nil {
		return zero, time.Time{}, false, ps.err
	}
	if ps.idx < len(ps.rows) {
		row := ps.rows[ps.idx]
		return row, ps.rowTime(row), true, nil
	}
	return zero, time.Time{}, false, nil
}

// rowTime extracts a row's timestamp via the timestamped interface.
// Go generics don't allow method dispatch on type parameters, so the
// runtime type assertion is the standard pattern. Returns the zero
// time for row types that don't implement timestamped.
func (ps *pageStream[T]) rowTime(row T) time.Time {
	if tr, ok := any(row).(timestamped); ok {
		return tr.Timestamp()
	}
	return time.Time{}
}

// consume drops the head row (call after peek returned one). The
// demand signal to the prefetcher already fired in installPage, so
// no reqNextCh send is needed here.
func (ps *pageStream[T]) consume() {
	if ps.idx < len(ps.rows) {
		ps.idx++
	}
}

// isDone reports whether the prefetcher has signaled exhaustion.
// Drains any parked done page first (non-blocking).
func (ps *pageStream[T]) isDone() bool {
	if ps.idx >= len(ps.rows) {
		select {
		case np := <-ps.nextCh:
			ps.installPage(np)
		default:
		}
	}
	return ps.done
}

// parkedRows returns the number of rows currently parked on this
// stream's active page. Used by tests to verify that the per-job
// memory cost is independent of window length: regardless of how
// many rows the prefetcher has delivered, at most one page is
// parked at any moment. Safe to call from the consumer goroutine;
// the prefetcher only mutates ps.rows/ps.idx via installPage, which
// is called from this file's methods (next/peek/isDone) under the
// same single-threaded wazero execution.
func (ps *pageStream[T]) parkedRows() int {
	return len(ps.rows) - ps.idx
}
