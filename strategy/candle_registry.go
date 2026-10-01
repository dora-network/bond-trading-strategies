package strategy

import (
	"context"
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
	WSBaseURL string
	APIKey    string
	// Store supplies the stream's resume cursor. Optional: nil swaps in
	// a no-op store so the stream works without persistence (candles.Handler
	// rejects a nil store with "missing candle store").
	Store candles.CandleStore
	// NewHandler is injectable for tests; defaults to candles.New.
	NewHandler func(cfg candles.Config, store candles.CandleStore) (candleSource, error)
	// StartDaemon runs the reconnect loop for a handler; injectable for tests.
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

type registryEntry struct {
	src    candleSource
	cancel context.CancelFunc
	refs   int
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
// entry's original warm-up point; deeper since requests are not honoured.
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
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	if !ok {
		src, err := r.cfg.NewHandler(candles.Config{
			BaseURL: r.cfg.WSBaseURL, APIKey: r.cfg.APIKey,
			OrderBookIDs: []string{book.String()},
			Resolution:   resolution, Since: since,
		}, r.cfg.Store)
		if err != nil {
			return nil, nil, err
		}
		// Background-derived: a shared entry must not die when one
		// subscriber's ctx is cancelled; teardown is refcount-driven.
		streamCtx, cancel := context.WithCancel(context.Background())
		r.cfg.StartDaemon(streamCtx, src)
		e = &registryEntry{src: src, cancel: cancel}
		r.entries[key] = e
	}
	subID := uuid.New()
	raw, err := e.src.Subscribe(subID)
	if err != nil {
		if e.refs == 0 {
			e.cancel()
			delete(r.entries, key)
		}
		return nil, nil, err
	}
	e.refs++
	closer := StartBarCloser(raw)
	var once sync.Once
	unsub := func() {
		once.Do(func() {
			_ = e.src.Unsubscribe(subID)
			closer.Stop()
			r.mu.Lock()
			defer r.mu.Unlock()
			e.refs--
			if e.refs == 0 {
				e.cancel()
				delete(r.entries, key)
			}
		})
	}
	return closer.Bars(), unsub, nil
}
