// Package notifications owns the per-DORA-user notification stream exposed by
// the strategy-server WebSocket endpoint, plus the in-process bus that
// producers call and the outbound client the mcp-server uses to relay
// events to its own MCP clients.
package notifications

import (
	"context"
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
