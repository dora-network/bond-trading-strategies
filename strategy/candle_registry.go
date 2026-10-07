package strategy

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
)

// candleSource is the subset of *candles.Handler the registry needs.
type candleSource interface {
	Subscribe(requestID uuid.UUID) (chan []candles.StreamCandlesEntry, error)
	Unsubscribe(requestID uuid.UUID) error
}

// CandleRegistryConfig wires the registry to DORA and (optionally) the
// candle store used only for the since-resume cursor. The registry never
// persists candles — price-daemon owns ingestion; strategy bars are
// ephemeral.
type CandleRegistryConfig struct {
	// WSBaseURL is the DORA WebSocket API base URL.
	WSBaseURL string
	// APIKey is the DORA API key sent on the WebSocket URL.
	APIKey string
	// Store is the candle persistence backend (GetLastTimestamp/LoadCandles).
	// Nil falls back to nopCandleStore: no resume cursor, Config.Since
	// reaches the handler untouched so the warm-start bootstrap honours it.
	Store candles.CandleStore
	// NewHandler constructs the candle source for a new (book, resolution)
	// entry. nil falls back to candles.New wrapped via the streams daemon.
	NewHandler func(c candles.Config, s candles.CandleStore) (candleSource, error)
	// StartDaemon launches the websocket/stream goroutine for a new
	// (book, resolution) entry. nil falls back to streams.Run on a
	// *candles.Handler.
	StartDaemon func(ctx context.Context, src candleSource)
}

// nopCandleStore lets a registry run without persistence: no resume
// cursor, nothing saved or loaded.
type nopCandleStore struct{}

func (nopCandleStore) GetLastTimestamp(context.Context, string) (*time.Time, error) {
	return nil, nil
}
func (nopCandleStore) SaveCandles(context.Context, []candles.StreamCandlesEntry) error { return nil }
func (nopCandleStore) LoadCandles(context.Context, string, time.Time, time.Time) ([]candles.Candle, error) {
	return nil, nil
}

// ponytail: closedBarCacheCap is the replay ceiling. A 1m-resolution
// first-subscriber since ~3 days ago produces ~4320 closed bars; deeper
// since requests see a partial cache and would need
// candles_history.LoadCandlesBucketed prefill rather than a larger ring.
const closedBarCacheCap = 4096

// subscriberChanBase is the slack above the snapshot length reserved on
// every subscriber channel; absorbs short bursts between snapshots.
const subscriberChanBase = 16

// registryEntry owns one shared observer subscription, one BarCloser,
// and a bounded cache of closed bars replayed to late subscribers.
type registryEntry struct {
	src         candleSource
	cancel      context.CancelFunc
	observerID  uuid.UUID
	closer      *BarCloser
	refs        int
	mu          sync.Mutex
	cache       []types.Bar
	subscribers map[uuid.UUID]chan types.Bar
}

// CandleRegistry owns one candle stream per (order book, resolution),
// started on first subscriber and stopped on last unsubscribe. It
// implements CandleFeed.
type CandleRegistry struct {
	cfg     CandleRegistryConfig
	mu      sync.Mutex
	entries map[string]*registryEntry // key: book|resolution
}

var _ CandleFeed = (*CandleRegistry)(nil)

func NewCandleRegistry(cfg CandleRegistryConfig) *CandleRegistry {
	if cfg.NewHandler == nil {
		cfg.NewHandler = func(c candles.Config, s candles.CandleStore) (candleSource, error) {
			return candles.New(c, s)
		}
	}
	if cfg.StartDaemon == nil {
		cfg.StartDaemon = func(ctx context.Context, src candleSource) {
			h, ok := src.(*candles.Handler)
			if !ok {
				return
			}
			d := streams.New(streams.Config{ReconnectDelay: 5 * time.Second})
			go func() { _ = d.Run(ctx, h.Stream) }()
		}
	}
	if cfg.Store == nil {
		cfg.Store = nopCandleStore{}
	}
	return &CandleRegistry{cfg: cfg, entries: map[string]*registryEntry{}}
}

// HandlerCount reports live streams (tests/observability).
func (r *CandleRegistry) HandlerCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// SubscribeBars returns a channel of closed bars for (book, resolution).
// The stream is started by the first subscriber and stopped when the last
// subscriber unsubscribes. Since is first-subscriber-wins: later
// subscribers joining an existing (book, resolution) entry inherit the
// entry's original warm-up point. Deeper since requests are not honoured
// beyond what the entry's own Config.Since requested at creation; late
// joiners are warmed from the entry's closed-bar cache, bounded by
// closedBarCacheCap (see the ponytail note on the cap).
// The ctx parameter is kept for CandleFeed fidelity but does not scope the
// stream — entry lifetime is registry-managed via refcounting.
func (r *CandleRegistry) SubscribeBars(
	_ context.Context, book uuid.UUID, resolution candles.Resolution, since time.Time,
) (<-chan types.Bar, func(), error) {
	if err := resolution.Validate(); err != nil {
		return nil, nil, err
	}
	key := book.String() + "|" + string(resolution)
	r.mu.Lock()
	e, ok := r.entries[key]
	if !ok {
		src, err := r.cfg.NewHandler(candles.Config{
			BaseURL: r.cfg.WSBaseURL, APIKey: r.cfg.APIKey,
			OrderBookIDs: []string{book.String()},
			Resolution:   resolution, Since: since,
		}, r.cfg.Store)
		if err != nil {
			r.mu.Unlock()
			return nil, nil, err
		}
		observerID := uuid.New()
		raw, err := src.Subscribe(observerID)
		if err != nil {
			r.mu.Unlock()
			return nil, nil, err
		}
		// Background-derived: a shared entry must not die when one
		// subscriber's ctx is cancelled; teardown is refcount-driven.
		streamCtx, cancel := context.WithCancel(context.Background())
		e = &registryEntry{
			src:         src,
			cancel:      cancel,
			observerID:  observerID,
			closer:      StartBarCloser(raw),
			subscribers: map[uuid.UUID]chan types.Bar{},
		}
		r.entries[key] = e
		// F7: start the daemon only after the observer is registered.
		// Otherwise the DORA websocket bootstrap (history batch honoring
		// Config.Since) can be fanned out to zero subscribers and the warm
		// start is silently lost.
		r.cfg.StartDaemon(streamCtx, src)
		startEntryForwarder(e)
	}
	// Snapshot the cache and replay synchronously into the new subscriber's
	// channel. The forwarder takes entry.mu to append+send, so the snapshot
	// is strictly ordered before every subsequent live bar — no gap, no
	// duplication.
	e.mu.Lock()
	snap := make([]types.Bar, len(e.cache))
	copy(snap, e.cache)
	subID := uuid.New()
	ch := make(chan types.Bar, len(snap)+subscriberChanBase)
	for _, b := range snap {
		ch <- b
	}
	e.subscribers[subID] = ch
	e.mu.Unlock()
	e.refs++
	r.mu.Unlock()

	var once sync.Once
	unsub := func() {
		once.Do(func() {
			// Lock-ordering: entry.mu must NEVER be held while acquiring
			// r.mu; the only nested path is SubscribeBars (r.mu → entry.mu).
			e.mu.Lock()
			sub, found := e.subscribers[subID]
			if found {
				delete(e.subscribers, subID)
				close(sub)
			}
			e.mu.Unlock()

			r.mu.Lock()
			defer r.mu.Unlock()
			e.refs--
			if e.refs == 0 {
				e.cancel()
				// Unsubscribe closes the observer's raw channel, which
				// makes the BarCloser loop exit and close closer.Bars().
				// The forwarder drains, closes any remaining subscriber
				// channels, and exits.
				_ = e.src.Unsubscribe(e.observerID)
				e.closer.Stop()
				delete(r.entries, key)
			}
		})
	}
	return ch, unsub, nil
}

// startEntryForwarder launches the per-entry goroutine which drains the
// BarCloser into every subscriber's channel and the bounded cache.
// Lock-ordering invariant: entry.mu is acquired exactly once per bar
// (append+evict+fan-out); non-blocking sends mean a slow subscriber
// causes a drop, not a stall (same degradation as the handler's
// push-timeout drop — a strategy that cannot keep up loses a bar).
func startEntryForwarder(e *registryEntry) {
	go func() {
		for bar := range e.closer.Bars() {
			e.mu.Lock()
			e.cache = append(e.cache, bar)
			if n := len(e.cache); n > closedBarCacheCap {
				// ponytail: O(closedBarCacheCap) memmove on overflow.
				copy(e.cache, e.cache[n-closedBarCacheCap:])
				e.cache = e.cache[:closedBarCacheCap]
			}
			for _, ch := range e.subscribers {
				select {
				case ch <- bar:
				default:
					slog.Warn("candle registry subscriber lagging; dropping bar",
						"subscribers", len(e.subscribers))
				}
			}
			e.mu.Unlock()
		}
		// Stream ended (Unsubscribe on the observer, Stop, or daemon exit).
		// Close every remaining subscriber channel so reads unblock with ok=false.
		e.mu.Lock()
		for _, ch := range e.subscribers {
			close(ch)
		}
		e.subscribers = map[uuid.UUID]chan types.Bar{}
		e.mu.Unlock()
	}()
}
