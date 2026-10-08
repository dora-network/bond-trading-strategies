package strategy

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// CandleFeed supplies closed bars for one (order book, resolution).
// Live runs use a registry over candles.Handler; backtests and tests
// use fakes.
//
//counterfeiter:generate . CandleFeed
type CandleFeed interface {
	// SubscribeBars returns a channel of closed bars for the book.
	// since requests the stream bootstrap to include history back to
	// that time (warm start); implementations may honour only the first
	// subscriber's since on a shared stream. The returned cancel func unsubscribes.
	SubscribeBars(ctx context.Context, orderBookID uuid.UUID, resolution candles.Resolution, since time.Time) (<-chan types.Bar, func(), error)
}

// StartBarCloser wraps a raw candle-update stream and emits bar N-1
// the first time bar N's start_timestamp appears: a bar is final only
// when superseded. Bars closes when the input closes or Stop is called.
type BarCloser struct {
	in   <-chan []candles.StreamCandlesEntry
	out  chan types.Bar
	done chan struct{}
}

func StartBarCloser(in <-chan []candles.StreamCandlesEntry) *BarCloser {
	bc := &BarCloser{in: in, out: make(chan types.Bar, 16), done: make(chan struct{})} //nolint:mnd
	go bc.loop()
	return bc
}

// Bars is the closed-bar output channel.
func (bc *BarCloser) Bars() <-chan types.Bar { return bc.out }

// Stop terminates the loop and closes Bars.
func (bc *BarCloser) Stop() { close(bc.done) }

func (bc *BarCloser) loop() {
	defer close(bc.out)
	var pending *candles.Candle
	for {
		select {
		case <-bc.done:
			return
		case batch, ok := <-bc.in:
			if !ok {
				return
			}
			for i := range batch {
				e := batch[i]
				// Drop replayed/stale entries strictly older than the
				// current pending candle: the registry's no-op cursor
				// store forces DORA to resend the bootstrap on
				// reconnect, and overwriting pending would let later
				// entries re-emit already-closed bars.
				if pending != nil && e.Val.StartTimestamp.Before(pending.StartTimestamp) {
					continue
				}
				if pending != nil && e.Val.StartTimestamp.After(pending.StartTimestamp) {
					select {
					case bc.out <- BarFromCandle(*pending):
					case <-bc.done:
						return
					}
				}
				c := e.Val
				pending = &c
			}
		}
	}
}

// BarFromCandle projects a streamed candle into a types.Bar.
// BenchmarkYield is left zero; spread-mode strategies fill it per bar.
func BarFromCandle(c candles.Candle) types.Bar {
	return types.Bar{
		Time: c.StartTimestamp.UTC(), Open: c.Open, High: c.High,
		Low: c.Low, Close: c.Close, Volume: c.Volume,
		OpenYTM: c.OpenYTM, HighYTM: c.HighYTM,
		LowYTM: c.LowYTM, CloseYTM: c.CloseYTM,
	}
}

// ResolutionDuration maps a resolution to a time.Duration.
// Unknown/empty resolutions return 0.
func ResolutionDuration(res candles.Resolution) time.Duration {
	switch res {
	case candles.Resolution1m:
		return time.Minute
	case candles.Resolution5m:
		return 5 * time.Minute //nolint:mnd
	case candles.Resolution15m:
		return 15 * time.Minute //nolint:mnd
	case candles.Resolution1h:
		return time.Hour
	case candles.Resolution4h:
		return 4 * time.Hour //nolint:mnd
	case candles.Resolution1d:
		return 24 * time.Hour //nolint:mnd
	default:
		return 0
	}
}
