package strategy

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/candles"
)

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
	require.Len(t, src.unsubs, 2)

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
