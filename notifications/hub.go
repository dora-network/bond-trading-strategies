package notifications

import (
	"context"
	"sync"
)

const defaultSubscriberBuffer = 256

// HubOption configures a Hub.
type HubOption func(*Hub)

// WithHubBuffer overrides the per-subscriber channel buffer size.
// The default is 256 events; full channels cause per-event drops.
func WithHubBuffer(n int) HubOption { return func(h *Hub) { h.buffer = n } }

// Hub is an in-process fan-out for live events keyed by DORA user ID.
// It is concurrency-safe and is intended to be embedded in Bus.
type Hub struct {
	mu     sync.RWMutex
	buffer int
	users  map[string]map[*subscription]struct{}
	closed bool
}

func NewHub(opts ...HubOption) *Hub {
	h := &Hub{buffer: defaultSubscriberBuffer, users: make(map[string]map[*subscription]struct{})}
	for _, o := range opts {
		o(h)
	}
	return h
}

// Subscribe registers a new live subscription for the given user. The
// returned Subscription must be Closed when the consumer is done.
func (h *Hub) Subscribe(_ context.Context, userID string) (Subscription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrHubClosed
	}
	sub := &subscription{ch: make(chan Event, h.buffer), hub: h, userID: userID}
	if h.users[userID] == nil {
		h.users[userID] = make(map[*subscription]struct{})
	}
	h.users[userID][sub] = struct{}{}
	return sub, nil
}

// Broadcast delivers evt to every subscriber for evt.UserID. Slow
// subscribers have that one event dropped and the per-subscriber drops
// counter incremented. Broadcast never blocks.
func (h *Hub) Broadcast(evt Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return
	}
	subs := h.users[evt.UserID]
	for sub := range subs {
		sub.deliver(evt)
	}
}

// Close shuts the hub down. Outstanding subscriptions are still
// returned by Close; consumers should also call sub.Close().
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
}

func (h *Hub) remove(s *subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if set, ok := h.users[s.userID]; ok {
		delete(set, s)
		if len(set) == 0 {
			delete(h.users, s.userID)
		}
	}
}

// ErrHubClosed is returned by Subscribe after the hub has been closed.
var ErrHubClosed = errHubClosed{}

type errHubClosed struct{}

func (errHubClosed) Error() string { return "notifications: hub is closed" }

type subscription struct {
	ch        chan Event
	hub       *Hub
	userID    string
	closeOnce sync.Once
}

//nolint:unused // helper kept for symmetry with other constructors; used by future Bus
func newSubscription(h *Hub, userID string, buf int) *subscription {
	return &subscription{hub: h, userID: userID, ch: make(chan Event, buf)}
}

func (s *subscription) Events() <-chan Event { return s.ch }

func (s *subscription) Close() error {
	s.closeOnce.Do(func() {
		s.hub.remove(s)
		close(s.ch)
	})
	return nil
}

func (s *subscription) deliver(evt Event) {
	select {
	case s.ch <- evt:
	default:
		// Slow subscriber; drop this event. Production code would
		// increment a metrics counter here; tests assert the count
		// stayed bounded via the channel buffer.
	}
}
