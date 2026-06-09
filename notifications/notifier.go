//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

// Package notifications owns the per-DORA-user notification stream exposed by
// the strategy-server WebSocket endpoint, plus the in-process bus that
// producers call and the outbound client the mcp-server uses to relay
// events to its own MCP clients.
package notifications

import (
	"context"
	"sync"
	"time"
)

// EventType identifies the kind of lifecycle change an Event represents.
// The dora.* namespace is reserved for v2 events relayed from DORA
// (orders, trades) and is intentionally not enumerated here.
type EventType string

const (
	EventBacktestStarted   EventType = "backtest.started"
	EventBacktestCompleted EventType = "backtest.completed"
	EventBacktestFailed    EventType = "backtest.failed"
	EventRunStarted        EventType = "run.started"
	EventRunPaused         EventType = "run.paused"
	EventRunResumed        EventType = "run.resumed"
	EventRunStopped        EventType = "run.stopped"
	EventRunStopLoss       EventType = "run.stop_loss"
)

// Event is the JSON envelope sent on the WebSocket and persisted in
// notification_log. ID is a UUIDv7; its monotonic property makes
// `id > lastID` a correct replay cursor.
type Event struct {
	ID         string    `json:"id"`
	Type       EventType `json:"type"`
	UserID     string    `json:"user_id"`
	RunID      string    `json:"run_id,omitempty"`
	BacktestID string    `json:"backtest_id,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
	Payload    any       `json:"payload"`
}

// Notifier is the producer-facing surface. Publish is called by the
// strategy HTTP handler and strategy implementations at every lifecycle
// transition. Subscribe is called by the WebSocket handler.
//
//counterfeiter:generate . Notifier
type Notifier interface {
	Publish(ctx context.Context, evt Event) error
	Subscribe(ctx context.Context, userID string) (Subscription, error)
}

// Subscription is the consumer-facing surface. Events returns the live
// channel; Close releases the subscription. The channel is closed when
// Close is called or when the Notifier is shut down.
type Subscription interface {
	Events() <-chan Event
	Close() error
}

// Bus is the in-process Notifier. It writes every event to a Log and
// broadcasts it to the Hub. A Log failure is logged but does not stop
// live delivery; the in-memory replay cache covers the gap.
type Bus struct {
	log     Log
	hub     *Hub
	logf    func(string, ...any)
	mu      sync.Mutex
	replay  map[string][]Event // userID -> most recent events (newest last)
	replayN int
}

// NewBus returns a Bus that writes to log and broadcasts through hub.
// If hub is nil, NewHub() is used.
func NewBus(log Log, hub *Hub, opts ...BusOption) *Bus {
	if hub == nil {
		hub = NewHub()
	}
	b := &Bus{log: log, hub: hub, logf: func(string, ...any) {}, replay: make(map[string][]Event), replayN: 1024}
	for _, o := range opts {
		o(b)
	}
	return b
}

// BusOption configures a Bus.
type BusOption func(*Bus)

// WithLogger routes internal errors through the supplied logger.
func WithLogger(f func(string, ...any)) BusOption { return func(b *Bus) { b.logf = f } }

// WithReplaySize overrides the in-memory replay cache size (default 1024).
func WithReplaySize(n int) BusOption { return func(b *Bus) { b.replayN = n } }

// Publish implements Notifier.
func (b *Bus) Publish(ctx context.Context, evt Event) error {
	if err := b.log.Insert(ctx, evt); err != nil {
		b.logf("notifications: log insert failed: %v", err)
	}
	b.cacheForReplay(evt)
	b.hub.Broadcast(evt)
	return nil
}

// Subscribe implements Notifier.
func (b *Bus) Subscribe(ctx context.Context, userID string) (Subscription, error) {
	return b.hub.Subscribe(ctx, userID)
}

// Replay returns the events with id > afterID for the given user from
// the underlying Log. It is exposed so the WebSocket handler can
// implement Last-Event-ID replay without depending on the concrete
// Log type.
func (b *Bus) Replay(ctx context.Context, userID, afterID string, limit int) ([]Event, error) {
	return b.log.Replay(ctx, userID, afterID, limit)
}

func (b *Bus) cacheForReplay(evt Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ring := b.replay[evt.UserID]
	ring = append(ring, evt)
	if len(ring) > b.replayN {
		ring = ring[len(ring)-b.replayN:]
	}
	b.replay[evt.UserID] = ring
}

// FailingLog is a Log that always fails Insert. Used in tests to assert
// Bus still delivers to live subscribers.
type FailingLog struct{ Err error }

func (f FailingLog) Insert(context.Context, Event) error { return f.Err }
func (FailingLog) Replay(context.Context, string, string, int) ([]Event, error) {
	return nil, nil
}
func (FailingLog) DeleteOlderThan(context.Context, time.Duration) (int64, error) { return 0, nil }
