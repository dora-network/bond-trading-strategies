package strategy

import (
	"testing"
	"time"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/candles"
)

func TestBarCloserEmitsOnlyClosedBars(t *testing.T) {
	in := make(chan []candles.StreamCandlesEntry, 4)
	closer := StartBarCloser(in)
	defer closer.Stop()

	dec := decimal.MustNew(1, 0)
	mk := func(ts time.Time) []candles.StreamCandlesEntry {
		return []candles.StreamCandlesEntry{{Val: candles.Candle{
			OrderBookID: "ob", StartTimestamp: ts,
			Open: dec, High: dec, Low: dec, Close: dec,
		}}}
	}
	t0 := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	in <- mk(t0) // first bar: nothing emitted
	in <- mk(t0) // same bar updated: still nothing
	in <- mk(t1) // new bar started: t0 is final now

	select {
	case bar := <-closer.Bars():
		require.True(t, bar.Time.Equal(t0))
	case <-time.After(time.Second):
		t.Fatal("expected closed bar for t0")
	}
	select {
	case <-closer.Bars():
		t.Fatal("t1 must not be emitted before a newer bar appears")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBarCloserDropsReplayedCandlesOlderThanPending(t *testing.T) {
	in := make(chan []candles.StreamCandlesEntry, 8)
	closer := StartBarCloser(in)
	defer closer.Stop()

	mk := func(ts time.Time, val int64) candles.StreamCandlesEntry {
		v := decimal.MustNew(val, 0)
		return candles.StreamCandlesEntry{Val: candles.Candle{
			OrderBookID: "ob", StartTimestamp: ts,
			Open: v, High: v, Low: v, Close: v,
		}}
	}
	t0 := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	t2 := t0.Add(2 * time.Minute)
	t3 := t0.Add(3 * time.Minute)
	t4 := t0.Add(4 * time.Minute)

	// Initial 3 bars, in order.
	in <- []candles.StreamCandlesEntry{mk(t0, 10), mk(t1, 11), mk(t2, 12)}
	// Reconnect replay: 0,1,2 again (no-op cursor store forces DORA to
	// resend the bootstrap). Without F5, pending is overwritten to t0
	// and t1's later arrival emits the already-emitted t0/t1 again.
	in <- []candles.StreamCandlesEntry{mk(t0, 10), mk(t1, 11), mk(t2, 12)}
	// A new bar arrives: pending (=t2) must close exactly once.
	in <- []candles.StreamCandlesEntry{mk(t3, 13)}
	// Another new bar so t3 itself is finalised (a bar only closes
	// when a strictly newer one appears).
	in <- []candles.StreamCandlesEntry{mk(t4, 14)}

	got := make([]time.Time, 0, 4)
	deadline := time.After(2 * time.Second)
	for len(got) < 4 {
		select {
		case bar, ok := <-closer.Bars():
			if !ok {
				t.Fatalf("Bars closed early after %d emissions: %v", len(got), got)
			}
			got = append(got, bar.Time)
		case <-deadline:
			t.Fatalf("timed out after %d emissions: %v", len(got), got)
		}
	}
	require.Equal(t, []time.Time{t0, t1, t2, t3}, got,
		"replayed older candles must not duplicate closed bars")

	// No extra emission pending.
	select {
	case bar, ok := <-closer.Bars():
		if ok {
			t.Fatalf("unexpected extra bar after replay dedup: %v", bar.Time)
		}
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBarCloserSameTimestampUpdatesReplacePending(t *testing.T) {
	// Regression guard for F5: dropping entries strictly older than
	// pending must NOT swallow same-timestamp updates (those are
	// in-progress candle updates and must replace pending so the
	// emitted bar carries the latest values). A regression that
	// mutates the guard to `!After` (treats same-timestamp as
	// "replay") would let this test fail.
	in := make(chan []candles.StreamCandlesEntry, 4)
	closer := StartBarCloser(in)
	defer closer.Stop()

	mk := func(ts time.Time, val int64) candles.StreamCandlesEntry {
		v := decimal.MustNew(val, 0)
		return candles.StreamCandlesEntry{Val: candles.Candle{
			OrderBookID: "ob", StartTimestamp: ts,
			Open: v, High: v, Low: v, Close: v,
		}}
	}
	t0 := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	// In-progress candle: same t0, two different values.
	in <- []candles.StreamCandlesEntry{mk(t0, 10), mk(t0, 99)}
	// Strictly newer candle finalises pending: t0 must be emitted
	// with the LATEST value (99), not the original (10).
	in <- []candles.StreamCandlesEntry{mk(t1, 11)}

	select {
	case bar, ok := <-closer.Bars():
		require.True(t, ok, "Bars closed before t0 was emitted")
		require.True(t, bar.Time.Equal(t0), "emitted bar Time=%s want %s", bar.Time, t0)
		want := decimal.MustNew(99, 0)
		require.True(t, bar.Close.Equal(want),
			"same-timestamp update dropped: t0 Close=%s want %s", bar.Close, want)
	case <-time.After(time.Second):
		t.Fatal("expected t0 emission")
	}
}

func TestBarCloserClosesOutputWhenInputCloses(t *testing.T) {
	in := make(chan []candles.StreamCandlesEntry)
	closer := StartBarCloser(in)
	close(in)
	select {
	case _, ok := <-closer.Bars():
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("Bars must close when input closes")
	}
}

func TestResolutionDuration(t *testing.T) {
	require.Equal(t, time.Hour, ResolutionDuration("1h"))
	require.Equal(t, 15*time.Minute, ResolutionDuration("15m"))
	require.Equal(t, time.Duration(0), ResolutionDuration("nope"))
}
