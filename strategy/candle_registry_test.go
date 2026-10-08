package strategy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// callRecorder appends tagged events under a mutex so tests can assert
// relative ordering of callbacks invoked from different goroutines or
// under different code paths.
type callRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *callRecorder) record(tag string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, tag)
}

func (r *callRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	return out
}

// recordingSource wraps a candleSource so its Subscribe call can be
// recorded alongside the registry's StartDaemon invocation.
type recordingSource struct {
	inner candleSource
	rec   *callRecorder
}

func (r *recordingSource) Subscribe(id uuid.UUID) (chan []candles.StreamCandlesEntry, error) {
	ch, err := r.inner.Subscribe(id)
	r.rec.record("Subscribe")
	return ch, err
}

func (r *recordingSource) Unsubscribe(id uuid.UUID) error { return r.inner.Unsubscribe(id) }

type stubSource struct {
	ch          chan []candles.StreamCandlesEntry
	unsubs      []uuid.UUID
	lastReqSeen uuid.UUID
}

func (s *stubSource) Subscribe(requestID uuid.UUID) (chan []candles.StreamCandlesEntry, error) {
	s.lastReqSeen = requestID
	if s.ch == nil {
		s.ch = make(chan []candles.StreamCandlesEntry, 4)
	}
	return s.ch, nil
}

func (s *stubSource) Unsubscribe(requestID uuid.UUID) error {
	s.unsubs = append(s.unsubs, requestID)
	return nil
}

func TestCandleRegistrySharesAndRefCounts(t *testing.T) {
	src := &stubSource{}
	var gotCfg candles.Config
	newCalls := 0
	reg := NewCandleRegistry(CandleRegistryConfig{
		NewHandler: func(cfg candles.Config, _ candles.CandleStore) (candleSource, error) {
			newCalls++
			gotCfg = cfg
			return src, nil
		},
		StartDaemon: func(context.Context, candleSource) {},
	})
	book := uuid.New()
	since := time.Now().Add(-time.Hour).Truncate(time.Second)
	ch1, cancel1, err := reg.SubscribeBars(t.Context(), book, "5m", since)
	require.NoError(t, err)
	require.NotNil(t, ch1)
	require.Equal(t, 1, reg.HandlerCount())
	require.Equal(t, 1, newCalls)
	require.Equal(t, []string{book.String()}, gotCfg.OrderBookIDs)
	require.Equal(t, candles.Resolution5m, gotCfg.Resolution)
	require.Equal(t, since, gotCfg.Since)

	_, cancel2, err := reg.SubscribeBars(t.Context(), book, "5m", since)
	require.NoError(t, err)
	require.Equal(t, 1, reg.HandlerCount(), "same (book,res) shares handler")
	require.Equal(t, 1, newCalls)

	_, _, err = reg.SubscribeBars(t.Context(), book, "15m", time.Time{})
	require.NoError(t, err)
	require.Equal(t, 2, reg.HandlerCount(), "different resolution = new handler")

	_, _, err = reg.SubscribeBars(t.Context(), uuid.New(), "5m", time.Time{})
	require.NoError(t, err)
	require.Equal(t, 3, reg.HandlerCount(), "different book = new handler")

	cancel1()
	require.Equal(t, 3, reg.HandlerCount())
	cancel2()
	require.Equal(t, 2, reg.HandlerCount(), "last unsub of 5m stops it")
	// F8: each entry keeps one shared observer on src, so per-subscriber
	// unsubs no longer fan to it — only the last unsub tears down the entry
	// and unsubscribes the single observer.
	require.Len(t, src.unsubs, 1, "5m entry has one observer, unsubscribed on teardown")

	_, _, err = reg.SubscribeBars(t.Context(), book, "nope", time.Time{})
	require.Error(t, err)
}

func TestCandleRegistryEmitsClosedBars(t *testing.T) {
	src := &stubSource{}
	reg := NewCandleRegistry(CandleRegistryConfig{
		NewHandler:  func(candles.Config, candles.CandleStore) (candleSource, error) { return src, nil },
		StartDaemon: func(context.Context, candleSource) {},
	})
	barCh, unsub, err := reg.SubscribeBars(t.Context(), uuid.New(), "5m", time.Time{})
	require.NoError(t, err)
	defer unsub()

	c1 := candles.Candle{StartTimestamp: time.Now().Truncate(time.Minute)}
	c2 := c1
	c2.StartTimestamp = c1.StartTimestamp.Add(5 * time.Minute)
	src.ch <- []candles.StreamCandlesEntry{{Val: c1}}
	src.ch <- []candles.StreamCandlesEntry{{Val: c2}}

	select {
	case b := <-barCh:
		require.Equal(t, c1.StartTimestamp.UTC(), b.Time)
	case <-time.After(2 * time.Second):
		t.Fatal("no closed bar emitted")
	}
	// c2 is still pending (not superseded) — must not be emitted.
	select {
	case b := <-barCh:
		t.Fatalf("pending bar emitted: %+v", b)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestCandleRegistryUnsubIdempotent(t *testing.T) {
	src := &stubSource{}
	reg := NewCandleRegistry(CandleRegistryConfig{
		NewHandler:  func(candles.Config, candles.CandleStore) (candleSource, error) { return src, nil },
		StartDaemon: func(context.Context, candleSource) {},
	})
	_, unsub, err := reg.SubscribeBars(t.Context(), uuid.New(), "5m", time.Time{})
	require.NoError(t, err)
	require.Equal(t, 1, reg.HandlerCount())
	unsub()
	require.NotPanics(t, unsub)
	require.Equal(t, 0, reg.HandlerCount())
	require.Len(t, src.unsubs, 1, "double unsub must not re-unsubscribe")
}

// TestCandleRegistryNilStoreNopAndSinceThreads pins the warm-start wiring:
// with no store configured the registry must substitute the no-op store
// (no resume cursor) so Config.Since reaches the handler untouched. A real
// candles_history-backed store would move the cursor to ~now and silently
// defeat the warm start (see cmd/strategy-server newCandleRegistry).
func TestCandleRegistryNilStoreNopAndSinceThreads(t *testing.T) {
	src := &stubSource{}
	var gotCfg candles.Config
	var gotStore candles.CandleStore
	reg := NewCandleRegistry(CandleRegistryConfig{
		NewHandler: func(cfg candles.Config, store candles.CandleStore) (candleSource, error) {
			gotCfg = cfg
			gotStore = store
			return src, nil
		},
		StartDaemon: func(context.Context, candleSource) {},
	})
	since := time.Now().Add(-24 * time.Hour).Truncate(time.Second)
	_, unsub, err := reg.SubscribeBars(t.Context(), uuid.New(), "1m", since)
	require.NoError(t, err)
	defer unsub()

	require.IsType(t, nopCandleStore{}, gotStore, "nil Store must become nopCandleStore")
	require.Equal(t, since, gotCfg.Since, "Since must thread through with a nil store")
}

// TestCandleRegistrySubscribeBeforeStartDaemon pins the F7 ordering: for a
// brand-new entry the first subscriber must be registered with src BEFORE
// StartDaemon is called. Otherwise the DORA websocket bootstrap (history
// batch honoring Config.Since) can arrive between StartDaemon and
// Subscribe, fan out to zero subscribers, and silently drop the warm
// start that the first strategy run depends on.
func TestCandleRegistrySubscribeBeforeStartDaemon(t *testing.T) {
	src := &stubSource{}
	rec := &callRecorder{}
	recSrc := &recordingSource{inner: src, rec: rec}
	reg := NewCandleRegistry(CandleRegistryConfig{
		NewHandler: func(candles.Config, candles.CandleStore) (candleSource, error) {
			return recSrc, nil
		},
		StartDaemon: func(context.Context, candleSource) { rec.record("StartDaemon") },
	})
	_, unsub, err := reg.SubscribeBars(t.Context(), uuid.New(), "5m", time.Time{})
	require.NoError(t, err)
	defer unsub()

	require.Equal(t, []string{"Subscribe", "StartDaemon"}, rec.snapshot(),
		"first subscriber must be registered before StartDaemon (F7)")
}

// TestCandleRegistryLateSubscriberReplaysCache pins F8: a second
// SubscribeBars on an existing (book, resolution) entry must receive the
// registry's cached closed bars synchronously (not a fresh empty stream).
// Without the cache, the second strategy run waits a full indicator
// window before its slow windows produce signal.
func TestCandleRegistryLateSubscriberReplaysCache(t *testing.T) {
	src := &stubSource{}
	reg := NewCandleRegistry(CandleRegistryConfig{
		NewHandler:  func(candles.Config, candles.CandleStore) (candleSource, error) { return src, nil },
		StartDaemon: func(context.Context, candleSource) {},
	})
	book := uuid.New()

	ch1, unsub1, err := reg.SubscribeBars(t.Context(), book, "5m", time.Time{})
	require.NoError(t, err)
	defer unsub1()

	// Feed 5 bars: the first four each supersede the prior, emitting four
	// closed bars; the fifth stays pending.
	base := time.Now().UTC().Truncate(time.Hour)
	for i := range 5 {
		src.ch <- []candles.StreamCandlesEntry{{
			Val: candles.Candle{StartTimestamp: base.Add(time.Duration(i) * 5 * time.Minute)},
		}}
	}

	var got []types.Bar
	deadline := time.After(2 * time.Second)
	for len(got) < 4 {
		select {
		case b := <-ch1:
			got = append(got, b)
		case <-deadline:
			t.Fatalf("first subscriber: got %d closed bars, want 4", len(got))
		}
	}
	require.Len(t, got, 4)

	// Late subscriber joins the same (book, resolution) entry.
	ch2, unsub2, err := reg.SubscribeBars(t.Context(), book, "5m", time.Time{})
	require.NoError(t, err)
	defer unsub2()

	// The four cached closed bars must be available immediately on ch2.
	var replayed []types.Bar
	for len(replayed) < 4 {
		select {
		case b := <-ch2:
			replayed = append(replayed, b)
		case <-time.After(time.Second):
			t.Fatalf("late subscriber: got %d replayed bars, want 4", len(replayed))
		}
	}
	require.Len(t, replayed, 4)
	for i := range got {
		require.True(t, got[i].Time.Equal(replayed[i].Time),
			"replay[%d].Time = %s, want %s", i, replayed[i].Time, got[i].Time)
	}

	require.Equal(t, 1, reg.HandlerCount(), "late join must not create a second handler")
}

// TestCandleRegistryLateSubscriberContinuity pins F8's replay→live
// guarantee: the replayed cache and subsequent live bars must form a
// single contiguous ordered sequence — no duplicate bars, no gap.
func TestCandleRegistryLateSubscriberContinuity(t *testing.T) {
	src := &stubSource{}
	reg := NewCandleRegistry(CandleRegistryConfig{
		NewHandler:  func(candles.Config, candles.CandleStore) (candleSource, error) { return src, nil },
		StartDaemon: func(context.Context, candleSource) {},
	})
	book := uuid.New()

	ch1, unsub1, err := reg.SubscribeBars(t.Context(), book, "5m", time.Time{})
	require.NoError(t, err)
	defer unsub1()

	// Three bars: closes two (at t=0 and t=5m), leaves t=10m pending.
	base := time.Now().UTC().Truncate(time.Hour)
	for i := range 3 {
		src.ch <- []candles.StreamCandlesEntry{{
			Val: candles.Candle{StartTimestamp: base.Add(time.Duration(i) * 5 * time.Minute)},
		}}
	}
	// Wait for the two closed bars to arrive on ch1 (so cache is populated).
	for i := range 2 {
		select {
		case <-ch1:
		case <-time.After(2 * time.Second):
			t.Fatalf("first subscriber: missing closed bar #%d", i+1)
		}
	}

	// Late subscriber B joins; must receive the two cached bars.
	ch2, unsub2, err := reg.SubscribeBars(t.Context(), book, "5m", time.Time{})
	require.NoError(t, err)
	defer unsub2()

	var bBars []types.Bar
	for len(bBars) < 2 {
		select {
		case b := <-ch2:
			bBars = append(bBars, b)
		case <-time.After(time.Second):
			t.Fatalf("B: got %d cached bars, want 2", len(bBars))
		}
	}
	require.Len(t, bBars, 2)
	require.True(t, bBars[0].Time.Equal(base), "B cached[0] = %s, want %s", bBars[0].Time, base)
	require.True(t, bBars[1].Time.Equal(base.Add(5*time.Minute)),
		"B cached[1] = %s, want %s", bBars[1].Time, base.Add(5*time.Minute))

	// One more bar closes the pending bar (t=10m) and starts a new pending.
	src.ch <- []candles.StreamCandlesEntry{{
		Val: candles.Candle{StartTimestamp: base.Add(3 * 5 * time.Minute)},
	}}

	// B should now receive exactly one more bar (the just-closed t=10m),
	// not a duplicate of the cached two.
	select {
	case b := <-ch2:
		require.True(t, b.Time.Equal(base.Add(2*5*time.Minute)),
			"B live bar = %s, want %s", b.Time, base.Add(2*5*time.Minute))
	case <-time.After(2 * time.Second):
		t.Fatal("B: no live bar after replay")
	}
	select {
	case b := <-ch2:
		t.Fatalf("B: duplicate/unexpected bar after replay: %+v", b)
	case <-time.After(150 * time.Millisecond):
	}
}

// TestCandleRegistryLateSubscriberCacheBounded pins the cache cap: the
// cached replay window is bounded by closedBarCacheCap, not unbounded.
// Deeper since requests still get only the cached slice.
func TestCandleRegistryLateSubscriberCacheBounded(t *testing.T) {
	src := &stubSource{}
	reg := NewCandleRegistry(CandleRegistryConfig{
		NewHandler:  func(candles.Config, candles.CandleStore) (candleSource, error) { return src, nil },
		StartDaemon: func(context.Context, candleSource) {},
	})
	book := uuid.New()

	ch1, unsub1, err := reg.SubscribeBars(t.Context(), book, "5m", time.Time{})
	require.NoError(t, err)
	defer unsub1()

	// Emit more closed bars than the cache cap. BarCloser emits closed bar
	// N-1 when bar N arrives; sending totalSent bars produces totalSent-1
	// closed bars and leaves the totalSent-th pending. Cache must be capped.
	const totalSent = closedBarCacheCap + 100
	base := time.Now().UTC().Truncate(time.Hour)
	for i := range totalSent {
		select {
		case src.ch <- []candles.StreamCandlesEntry{{
			Val: candles.Candle{StartTimestamp: base.Add(time.Duration(i) * 5 * time.Minute)},
		}}:
		case <-time.After(2 * time.Second):
			t.Fatalf("stuck feeding bar #%d", i)
		}
	}
	// Yield to let the forwarder drain src.ch into the cache. ch1 has
	// buffer 16 so most closes are dropped at ch1 but still land in the
	// cache, where the bounded eviction is exercised.
	for drained := 0; drained < closedBarCacheCap; drained++ {
		select {
		case <-ch1:
		case <-time.After(10 * time.Millisecond):
			drained = closedBarCacheCap
		}
	}

	// Late joiner must receive exactly the cache cap (the most recent),
	// not the unbounded full history.
	ch2, unsub2, err := reg.SubscribeBars(t.Context(), book, "5m", time.Time{})
	require.NoError(t, err)
	defer unsub2()

	replayed := 0
	deadline := time.After(time.Second)
loop:
	for replayed < closedBarCacheCap {
		select {
		case b := <-ch2:
			replayed++
			// Newest cached bar should be the closed bar at totalSent-2
			// (totalSent-1 is pending, totalSent-2 is the last closed).
			if replayed == closedBarCacheCap {
				want := base.Add(time.Duration(totalSent-2) * 5 * time.Minute)
				require.True(t, b.Time.Equal(want),
					"newest cached bar = %s, want %s", b.Time, want)
			}
		case <-deadline:
			break loop
		}
	}
	require.Equal(t, closedBarCacheCap, replayed,
		"replay must be bounded by closedBarCacheCap")
}
