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
