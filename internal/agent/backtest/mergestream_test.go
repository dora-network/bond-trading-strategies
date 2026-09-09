package backtest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dorastrategy "github.com/dora-network/dora-strategy-wasm/dorastrategy"
)

// candle / trade / price build typed rows with a small placeholder payload.
// The merge's picker only inspects Timestamp(); the payload field exists for
// future host-side marshaling (Task 5).
func candle(ts time.Time) candleRow { return candleRow{ts: ts, payload: dorastrategy.Candle{}} }
func trade(ts time.Time) tradeRow   { return tradeRow{ts: ts, payload: dorastrategy.Trade{}} }
func price(ts time.Time) priceRow   { return priceRow{ts: ts, payload: dorastrategy.Price{}} }

// scriptedFetcher returns pages in order from `pages`. cursor[i] is the
// cursor to send alongside pages[i]; the empty cursor ends the stream.
type scriptedFetcher[T any] struct {
	mu      sync.Mutex
	pages   [][]T
	cursors []string
	idx     int
	calls   int32
}

func (f *scriptedFetcher[T]) Fetch(_ context.Context, _ string) ([]T, string, error) {
	atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.idx >= len(f.pages) {
		return nil, "", nil
	}
	page := f.pages[f.idx]
	next := ""
	if f.idx+1 < len(f.cursors) {
		next = f.cursors[f.idx+1]
	}
	f.idx++
	return page, next, nil
}

// delayedFetcher is a scriptedFetcher variant that blocks each fetch call
// for `delay` before returning its row. Models a slow prefetcher whose
// first page arrives later than its peers'.
type delayedFetcher[T any] struct {
	mu      sync.Mutex
	pages   [][]T
	cursors []string
	delay   time.Duration
	idx     int
}

func (f *delayedFetcher[T]) Fetch(_ context.Context, _ string) ([]T, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.idx >= len(f.pages) {
		return nil, "", nil
	}
	page := f.pages[f.idx]
	next := ""
	if f.idx+1 < len(f.cursors) {
		next = f.cursors[f.idx+1]
	}
	f.idx++
	time.Sleep(f.delay)
	return page, next, nil
}

func TestMergeStream_StreamsCandlesTradesPricesInTimeOrder(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	candles := [][]candleRow{
		{candle(t0.Add(0 * time.Minute)), candle(t0.Add(2 * time.Minute)), candle(t0.Add(4 * time.Minute))},
	}
	trades := [][]tradeRow{
		{trade(t0.Add(1 * time.Minute)), trade(t0.Add(3 * time.Minute)), trade(t0.Add(5 * time.Minute))},
	}
	prices := [][]priceRow{
		{price(t0.Add(0 * time.Minute).Add(30 * time.Second))},
	}

	m := newMergeStream(
		t.Context(), 100, 100, 100,
		(&scriptedFetcher[candleRow]{pages: candles, cursors: []string{"c2"}}).Fetch,
		(&scriptedFetcher[tradeRow]{pages: trades, cursors: []string{"t2"}}).Fetch,
		(&scriptedFetcher[priceRow]{pages: prices, cursors: []string{"p2"}}).Fetch,
	)
	defer m.close()

	want := []time.Time{
		t0.Add(0 * time.Minute),                       // candle
		t0.Add(0 * time.Minute).Add(30 * time.Second), // price
		t0.Add(1 * time.Minute),                       // trade
		t0.Add(2 * time.Minute),                       // candle
		t0.Add(3 * time.Minute),                       // trade
		t0.Add(4 * time.Minute),                       // candle
		t0.Add(5 * time.Minute),                       // trade
	}
	for i, w := range want {
		ev, ok, err := m.next()
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("step %d: ok=false (stream done)", i)
		}
		if !ev.row.Timestamp().Equal(w) {
			t.Errorf("step %d: got %v want %v", i, ev.row.Timestamp(), w)
		}
	}

	_, ok, err := m.next()
	if err != nil {
		t.Fatalf("final: %v", err)
	}
	if ok {
		t.Fatal("expected done")
	}
}

// TestMergeStream_LaggingStreamDoesNotBreakTimeOrder is a regression for
// the §5.2.3 picker bug fixed in Task 3: a stream whose first page is
// still in flight must block the picker, even if other streams have
// earlier-timestamped events parked. Without the liveWait gate, the
// picker would deliver the candle event (the only "ready" stream) while
// the trades stream's earlier event is still being fetched — violating
// "strictly by timestamp." This test locks the invariant.
func TestMergeStream_LaggingStreamDoesNotBreakTimeOrder(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	// Candle stream parks its first page promptly.
	candlePages := [][]candleRow{
		{candle(t0.Add(2 * time.Minute)), candle(t0.Add(4 * time.Minute))},
	}
	candleCursors := []string{"c2"}

	// Trades stream's first page is delayed by 200ms; its first event
	// has the EARLIEST timestamp of any stream (t+1min) — must win
	// the first delivery.
	tradePages := [][]tradeRow{
		{trade(t0.Add(1 * time.Minute)), trade(t0.Add(3 * time.Minute))},
	}
	tradeCursors := []string{"t2"}

	// Prices stream parks promptly.
	pricePages := [][]priceRow{
		{price(t0.Add(90 * time.Second))},
	}
	priceCursors := []string{"p2"}

	fCandle := &scriptedFetcher[candleRow]{pages: candlePages, cursors: candleCursors}
	fTrade := &delayedFetcher[tradeRow]{pages: tradePages, cursors: tradeCursors, delay: 200 * time.Millisecond}
	fPrice := &scriptedFetcher[priceRow]{pages: pricePages, cursors: priceCursors}

	m := newMergeStream(t.Context(), 100, 100, 100, fCandle.Fetch, fTrade.Fetch, fPrice.Fetch)
	defer m.close()

	// Without the liveWait gate, the picker would deliver the candle at
	// t+2min first. The correct behavior: hold until the trades stream
	// parks (200ms later), then deliver the trades event at t+1min.
	ev, ok, err := m.next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if !ok {
		t.Fatal("expected an event")
	}
	if !ev.row.Timestamp().Equal(t0.Add(1 * time.Minute)) {
		t.Errorf("first event timestamp = %v, want %v (the lagging stream's earlier event must win)",
			ev.row.Timestamp(), t0.Add(1*time.Minute))
	}
	if ev.kind != kindTrade {
		t.Errorf("first event kind = %v, want kindTrade", ev.kind)
	}
}

func TestMergeStream_StallsOnCandleErrorButServesOtherStreams(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	// Candle page 1 returns an error; trades/prices have valid data.
	candleErr := errors.New("candle-fetch-failed")
	candles := [][]candleRow{
		{candle(t0.Add(0 * time.Minute)), candle(t0.Add(1 * time.Minute))},
	}
	trades := [][]tradeRow{
		{trade(t0.Add(0 * time.Minute).Add(30 * time.Second))},
		{trade(t0.Add(2 * time.Minute))},
	}
	prices := [][]priceRow{{price(t0.Add(0 * time.Minute).Add(15 * time.Second))}}

	fCandle := &errAfterOnePageFetcher[candleRow]{firstPage: candles[0], nextCursor: "x", err: candleErr}
	fTrade := &scriptedFetcher[tradeRow]{pages: trades, cursors: []string{"t2", ""}}
	fPrice := &scriptedFetcher[priceRow]{pages: prices, cursors: []string{""}}

	m := newMergeStream(t.Context(), 100, 100, 100, fCandle.Fetch, fTrade.Fetch, fPrice.Fetch)
	defer m.close()

	// Expected sequence: candle@10:00, price@10:00:15, trade@10:00:30,
	// candle@10:01, trade@10:02 (from trade page 2), THEN the candle
	// error surfaces — because no earlier event is left.
	wantSeq := []time.Time{
		t0.Add(0 * time.Minute),
		t0.Add(0 * time.Minute).Add(15 * time.Second),
		t0.Add(0 * time.Minute).Add(30 * time.Second),
		t0.Add(1 * time.Minute),
		t0.Add(2 * time.Minute),
	}
	for i, w := range wantSeq {
		ev, ok, err := m.next()
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("step %d: ok=false", i)
		}
		if !ev.row.Timestamp().Equal(w) {
			t.Errorf("step %d: got %v want %v", i, ev.row.Timestamp(), w)
		}
	}

	// Now the candle error should be the only thing left to deliver.
	_, _, err := m.next()
	if !errors.Is(err, candleErr) {
		t.Errorf("expected candle error after drain, got %v", err)
	}
}

// errAfterOnePageFetcher returns its firstPage once, then errors.
type errAfterOnePageFetcher[T any] struct {
	mu         sync.Mutex
	firstPage  []T
	nextCursor string
	err        error
	served     bool
}

func (f *errAfterOnePageFetcher[T]) Fetch(_ context.Context, _ string) ([]T, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.served {
		f.served = true
		return f.firstPage, f.nextCursor, nil
	}
	return nil, "", f.err
}

func TestMergeStream_BoundedMemory(t *testing.T) {
	// Drive 100k events through small pages and directly assert the
	// merge's parked-row count. Spec §1/§5.2.1: parked rows never
	// exceed the per-stream page sizes, independent of window length.
	// Without this assertion a regression that loses the cap-1 nextCh
	// backpressure would pass (the heap-proxy it replaced could not
	// distinguish "bounded" from "unbounded" at this data size).
	const pageSize = 100
	const totalEvents = 100_000

	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	pages := make([][]candleRow, 0, totalEvents/pageSize+1)
	for start := 0; start < totalEvents; start += pageSize {
		end := start + pageSize
		if end > totalEvents {
			end = totalEvents
		}
		page := make([]candleRow, 0, end-start)
		for k := range end - start {
			page = append(page, candle(t0.Add(time.Duration(start+k)*time.Second)))
		}
		pages = append(pages, page)
	}
	cursors := make([]string, len(pages))
	for i := range cursors {
		cursors[i] = "c"
	}

	fCandle := &scriptedFetcher[candleRow]{pages: pages, cursors: cursors}
	fTrade := &scriptedFetcher[tradeRow]{}
	fPrice := &scriptedFetcher[priceRow]{}

	m := newMergeStream(t.Context(), pageSize, pageSize, pageSize,
		fCandle.Fetch, fTrade.Fetch, fPrice.Fetch)
	defer m.close()

	// Drain on the same goroutine and assert parkedRows after every
	// step. Worst case: one page parked per stream = pageSize * 3.
	for n := range totalEvents {
		_, ok, err := m.next()
		if err != nil {
			t.Fatalf("step %d: %v", n, err)
		}
		if !ok {
			t.Fatalf("step %d: ok=false (stream done after %d of %d events)",
				n, n, totalEvents)
		}
		if parked := m.parkedRows(); parked > pageSize*3 {
			t.Errorf("step %d: parkedRows=%d exceeded cap %d (one page per stream)",
				n, parked, pageSize*3)
		}
	}
	// After draining, all streams are done — parkedRows must be 0.
	if got := m.parkedRows(); got != 0 {
		t.Errorf("after draining %d events, parkedRows=%d (want 0)", totalEvents, got)
	}
}

func TestMergeStream_PreambleThenStream(t *testing.T) {
	// Smoke: mergeStream drains correctly when one stream has many pages
	// and the others have only one. Models the preamble-eager + replay-stream
	// boundary: no double-delivery, clean termination.
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	candles := [][]candleRow{
		{candle(t0), candle(t0.Add(time.Minute))},
		{candle(t0.Add(2 * time.Minute)), candle(t0.Add(3 * time.Minute))},
	}
	trades := [][]tradeRow{{trade(t0.Add(30 * time.Second))}}
	prices := [][]priceRow{{price(t0.Add(15 * time.Second))}}

	m := newMergeStream(t.Context(), 2, 1, 1,
		(&scriptedFetcher[candleRow]{pages: candles, cursors: []string{"c2", ""}}).Fetch,
		(&scriptedFetcher[tradeRow]{pages: trades, cursors: []string{""}}).Fetch,
		(&scriptedFetcher[priceRow]{pages: prices, cursors: []string{""}}).Fetch,
	)
	defer m.close()

	want := []time.Time{
		t0, t0.Add(15 * time.Second), t0.Add(30 * time.Second),
		t0.Add(time.Minute), t0.Add(2 * time.Minute),
		t0.Add(3 * time.Minute),
	}
	for i, w := range want {
		ev, ok, err := m.next()
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("step %d: ok=false", i)
		}
		if !ev.row.Timestamp().Equal(w) {
			t.Errorf("step %d: got %v want %v", i, ev.row.Timestamp(), w)
		}
	}

	_, ok, err := m.next()
	if err != nil {
		t.Fatalf("final: %v", err)
	}
	if ok {
		t.Fatal("expected done")
	}
}

func TestMergeStream_LongWindowSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("long-window smoke")
	}

	// Synthetic 365-day window: 365*24*60 = 525,600 candle rows at 1m
	// resolution; 1000 trades/day and 1000 prices/day = 365,000 each.
	// Total ≈ 1.25M events. Page sizes 1500/1000/1000.
	const pageCandle = 1500
	const pageTrade = 1000
	const pagePrice = 1000

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var candlePages [][]candleRow
	for start := 0; start < 365*24*60; start += pageCandle {
		end := start + pageCandle
		if end > 365*24*60 {
			end = 365 * 24 * 60
		}
		page := make([]candleRow, 0, end-start)
		for k := range end - start {
			page = append(page, candle(t0.Add(time.Duration(start+k)*time.Minute)))
		}
		candlePages = append(candlePages, page)
	}
	candleCursors := make([]string, len(candlePages))
	for i := range candleCursors {
		candleCursors[i] = "c"
	}

	var tradePages [][]tradeRow
	for d := range 365 {
		page := make([]tradeRow, 0, pageTrade)
		for k := range pageTrade {
			ts := t0.AddDate(0, 0, d).Add(time.Duration(k) * time.Second)
			page = append(page, trade(ts))
		}
		tradePages = append(tradePages, page)
	}
	tradeCursors := make([]string, len(tradePages))
	for i := range tradeCursors {
		tradeCursors[i] = "t"
	}

	var pricePages [][]priceRow
	for d := range 365 {
		page := make([]priceRow, 0, pagePrice)
		for k := range pagePrice {
			ts := t0.AddDate(0, 0, d).Add(time.Duration(k) * time.Second)
			page = append(page, price(ts))
		}
		pricePages = append(pricePages, page)
	}
	priceCursors := make([]string, len(pricePages))
	for i := range priceCursors {
		priceCursors[i] = "p"
	}

	m := newMergeStream(t.Context(), pageCandle, pageTrade, pagePrice,
		(&scriptedFetcher[candleRow]{pages: candlePages, cursors: candleCursors}).Fetch,
		(&scriptedFetcher[tradeRow]{pages: tradePages, cursors: tradeCursors}).Fetch,
		(&scriptedFetcher[priceRow]{pages: pricePages, cursors: priceCursors}).Fetch,
	)
	defer m.close()

	start := time.Now()
	count := 0
	for {
		_, ok, err := m.next()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if !ok {
			break
		}
		count++
	}
	dur := time.Since(start)
	t.Logf("drained %d events in %s", count, dur)

	// Soft ceiling: 60s on a reasonable machine.
	if dur > 60*time.Second {
		t.Errorf("drain too slow: %s for %d events", dur, count)
	}
}
