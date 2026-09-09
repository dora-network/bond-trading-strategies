package backtest

import (
	"context"
	"time"

	dorastrategy "github.com/dora-network/dora-strategy-wasm/dorastrategy"
)

// timestamped is the common surface the merge uses to compare rows
// across the three heterogeneous streams. Go generics can't compare
// across different instantiations of pageStream[T], so the picker
// sorts by this interface instead.
type timestamped interface {
	Timestamp() time.Time
}

// eventKind tags which stream an event came from. The constant order
// matches the rows in streamOrder (used by mergeStream to iterate
// the three streams in the picker loop).
type eventKind int

const (
	kindCandle eventKind = iota
	kindTrade
	kindPrice
)

// event is one time-ordered replay event: the stream it came from and
// the type-erased row (candleRow / tradeRow / priceRow).
type event struct {
	kind eventKind
	row  timestamped
}

// candleRow / tradeRow / priceRow are the typed wrappers the fetchers
// produce. The merge picks the earliest by ts; the host function
// marshals the typed payload back to JSON for the plugin's
// host.NextEvent (same shape the eager path used).
type candleRow struct {
	ts      time.Time
	payload dorastrategy.Candle
}

func (c candleRow) Timestamp() time.Time { return c.ts }

type tradeRow struct {
	ts      time.Time
	payload dorastrategy.Trade
}

func (t tradeRow) Timestamp() time.Time { return t.ts }

type priceRow struct {
	ts      time.Time
	payload dorastrategy.Price
}

func (p priceRow) Timestamp() time.Time { return p.ts }

// streamHandle is the type-erased wrapper around pageStream[T]. Each
// handle exposes the same surface (peek/advance/isDone/close) plus a
// wakeCh channel the merge's blocking select listens on — the
// pageStream signals wakeCh after parking a page. The kind field
// lets callers refer to a stream by eventKind rather than by an
// opaque numeric index.
type streamHandle struct {
	kind     eventKind
	wakeCh   chan struct{}
	peek     func() (timestamped, time.Time, bool, error)
	advance  func()
	isDoneFn func() bool
	closeFn  func()
	parkedFn func() int // test/debug accessor: count of rows currently parked on the pageStream's current page
}

func (s *streamHandle) isDone() bool { return s.isDoneFn() }

// wrapStream builds a streamHandle around a pageStream[T], erasing
// the row type behind the timestamped interface.
func wrapStream[T any](
	kind eventKind, ctx context.Context, pageSize int, fetch pageFetcher[T],
) *streamHandle {
	ps := newPageStream[T](ctx, pageSize, fetch, "")
	h := &streamHandle{
		kind:   kind,
		wakeCh: ps.wakeCh,
	}
	h.peek = func() (timestamped, time.Time, bool, error) {
		row, ts, ok, err := ps.peek()
		if !ok || err != nil {
			return nil, ts, ok, err
		}
		if tr, isTS := any(row).(timestamped); isTS {
			return tr, ts, ok, nil
		}
		return nil, ts, ok, nil
	}
	h.advance = func() { ps.consume() }
	h.isDoneFn = func() bool { return ps.isDone() }
	h.closeFn = func() { ps.close() }
	h.parkedFn = func() int { return ps.parkedRows() }
	return h
}

// mergeStream interleaves three pageStream instances (candles,
// trades, prices) in time order, implementing the §5.2.3 picker for
// hostNextEvent. The three handles are named (not indexed) so the
// picker and the blocking select both refer to streams by eventKind.
type mergeStream struct {
	ctx    context.Context
	cancel context.CancelFunc

	candle *streamHandle
	trade  *streamHandle
	price  *streamHandle
}

// all returns the three streams as a slice for the picker loop. The
// picker iterates over the slice but stores picks by eventKind (via
// stream.kind), so this never indexes by position.
func (m *mergeStream) all() []*streamHandle {
	return []*streamHandle{m.candle, m.trade, m.price}
}

// parkedRows returns the total count of rows currently parked across
// the three streams. This is the per-job memory cost the streaming
// shape is meant to bound; tests use it to assert independence from
// window length.
func (m *mergeStream) parkedRows() int {
	total := 0
	for _, s := range m.all() {
		total += s.parkedFn()
	}
	return total
}

// newMergeStream starts three prefetchers. Each fetch func returns
// pages of a different concrete row type; the merge compares them via
// the timestamped interface the row wrappers implement.
func newMergeStream(
	parent context.Context,
	pageSizeCandle, pageSizeTrade, pageSizePrice int,
	fetchCandles pageFetcher[candleRow],
	fetchTrades pageFetcher[tradeRow],
	fetchPrices pageFetcher[priceRow],
) *mergeStream {
	ctx, cancel := context.WithCancel(parent)
	return &mergeStream{
		ctx:    ctx,
		cancel: cancel,
		candle: wrapStream(kindCandle, ctx, pageSizeCandle, fetchCandles),
		trade:  wrapStream(kindTrade, ctx, pageSizeTrade, fetchTrades),
		price:  wrapStream(kindPrice, ctx, pageSizePrice, fetchPrices),
	}
}

// close tears down all three prefetchers. Idempotent per stream.
func (m *mergeStream) close() {
	m.cancel()
	for _, s := range m.all() {
		s.closeFn()
	}
}

// next returns the earliest-timestamp event across the three
// streams. ok=false when every stream is exhausted; err non-nil when
// a stream failed and no other stream can still serve rows (errors
// surface lazily — healthy streams keep delivering first). A pick is
// only delivered once every live (not-done, not-errored) stream has a
// ready row — otherwise a lagging stream could hold an earlier event
// than the one picked, breaking the merge's time-order invariant.
// When some live stream isn't ready, next blocks on the global
// select until one parks a page (or the context is cancelled).
func (m *mergeStream) next() (event, bool, error) {
	for {
		var (
			pickKind  eventKind = -1
			pickedRow timestamped
			earliest  time.Time
			firstErr  error
			allFin    = true
			liveWait  bool // a live stream has no ready row yet
		)
		for _, s := range m.all() {
			row, ts, has, err := s.peek()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue // finished-with-error; not eligible for picking
			}
			if has {
				if pickKind == -1 || ts.Before(earliest) {
					pickKind = s.kind
					earliest = ts
					pickedRow = row
				}
			} else if !s.isDone() {
				liveWait = true
			}
			if !s.isDone() {
				allFin = false
			}
		}

		if pickKind != -1 && !liveWait {
			pickStream(m, pickKind).advance()
			return event{kind: pickKind, row: pickedRow}, true, nil
		}

		if allFin {
			if firstErr != nil {
				return event{}, false, firstErr
			}
			return event{}, false, nil
		}

		// Block until any live stream wakes us (its prefetcher parked
		// a page). The select arms reference the named streams
		// directly; no numeric indexing.
		select {
		case <-m.ctx.Done():
			return event{}, false, m.ctx.Err()
		case <-m.candle.wakeCh:
		case <-m.trade.wakeCh:
		case <-m.price.wakeCh:
		}
	}
}

// pickStream returns the named streamHandle for the given eventKind.
// Centralizes the switch so the picker and any future caller refer to
// streams by kind, not by opaque numeric index.
func pickStream(m *mergeStream, k eventKind) *streamHandle {
	switch k {
	case kindCandle:
		return m.candle
	case kindTrade:
		return m.trade
	case kindPrice:
		return m.price
	}
	return nil
}
