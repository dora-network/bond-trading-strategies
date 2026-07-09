// manager.go owns the per-DORA-user subscription lifecycle: refcounted
// WebSocket subscriptions with grace-period teardown, backed by a
// ManagerStream and forwarding filtered order updates onto the
// notifications bus. RunLookupFunc is the only strategy-side seam, so
// this file has no cross-package dependency on strategy/http.
package orderupdates

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dora-network/bond-trading-strategies/notifications"
)

const (
	defaultGraceDelay = 5 * time.Second
	publishTimeout    = 5 * time.Second
)

// jsonUnmarshal is a tiny seam so tests can swap the parser if needed.
//
//nolint:gochecknoglobals
var jsonUnmarshal = json.Unmarshal

// Manager refcounts subscriptions per DORA user. One open WebSocket
// covers all running strategies for the same user; the WebSocket is
// torn down when the last strategy for that user ends (after a grace
// period to absorb run-stop+restart races).
//
//counterfeiter:generate . Manager
type Manager interface {
	EnsureSubscribed(ctx context.Context, doraUserID, apiKey string) error
	Unsubscribe(doraUserID string)
	Close()
}

// Option configures a Manager.
type Option func(*manager)

func WithGraceDelay(d time.Duration) Option {
	return func(m *manager) { m.graceDelay = d }
}

func WithLogger(l *slog.Logger) Option {
	return func(m *manager) { m.log = l }
}

func WithStream(s ManagerStream) Option {
	return func(m *manager) { m.stream = s }
}

// NewManager returns a Manager. baseCtx's cancellation stops every
// open subscription on shutdown.
func NewManager(
	baseCtx context.Context,
	notifier notifications.Notifier,
	lookup RunLookupFunc,
	strategyTypes []string,
	stream ManagerStream,
	opts ...Option,
) Manager {
	m := &manager{
		baseCtx:       baseCtx,
		notifier:      notifier,
		lookup:        lookup,
		strategyTypes: strategyTypes,
		stream:        stream,
		subs:          make(map[string]*subscription),
		log:           slog.Default(),
		graceDelay:    defaultGraceDelay,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

type manager struct {
	mu            sync.Mutex
	subs          map[string]*subscription
	notifier      notifications.Notifier
	lookup        RunLookupFunc
	strategyTypes []string
	stream        ManagerStream
	baseCtx       context.Context
	log           *slog.Logger
	graceDelay    time.Duration
}

type subscription struct {
	cancel    context.CancelFunc
	count     atomic.Int32
	graceStop func() bool
	graceDone chan struct{}
}

func (m *manager) EnsureSubscribed(ctx context.Context, doraUserID, apiKey string) error {
	if doraUserID == "" {
		return errors.New("orderupdates: doraUserID is required")
	}
	if apiKey == "" {
		return errors.New("orderupdates: apiKey is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if sub, ok := m.subs[doraUserID]; ok {
		// Existing subscription. Cancel any pending grace timer; bump refcount.
		if sub.graceStop != nil && sub.graceStop() {
			<-sub.graceDone
		}
		sub.count.Add(1)
		return nil
	}

	subCtx, cancel := context.WithCancel(m.baseCtx)
	sub := &subscription{
		cancel:    cancel,
		graceStop: func() bool { return false },
		graceDone: make(chan struct{}),
	}
	sub.count.Store(1)
	m.subs[doraUserID] = sub
	go m.runSubscription(subCtx, doraUserID, apiKey)
	return nil
}

func (m *manager) Unsubscribe(doraUserID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sub, ok := m.subs[doraUserID]
	if !ok {
		m.log.Warn("Unsubscribe called with no active subscription",
			"dora_user_id", doraUserID)
		return
	}
	newCount := sub.count.Add(-1)
	if newCount > 0 {
		return
	}
	// Schedule grace teardown.
	m.scheduleGraceTeardown(doraUserID, sub)
}

// scheduleGraceTeardown starts a grace timer; if it fires while the
// refcount is still zero the subscription is torn down. A racing
// EnsureSubscribed cancels the timer via graceStop (which signals the
// goroutine through stopCh and returns true so the caller can wait on
// graceDone). The goroutine closes graceDone before taking m.mu, so
// EnsureSubscribed — which holds m.mu while waiting on graceDone — can
// never deadlock against a teardown-in-progress goroutine.
func (m *manager) scheduleGraceTeardown(doraUserID string, sub *subscription) {
	t := time.NewTimer(m.graceDelay)
	stopCh := make(chan struct{})
	sub.graceStop = func() bool {
		t.Stop()
		select {
		case <-t.C:
		default:
		}
		close(stopCh)
		return true
	}
	sub.graceDone = make(chan struct{})
	go func() {
		select {
		case <-t.C:
			// Grace elapsed; fall through to teardown check.
		case <-stopCh:
			close(sub.graceDone)
			return // re-armed before fire
		}
		close(sub.graceDone)
		m.mu.Lock()
		defer m.mu.Unlock()
		if sub.count.Load() > 0 {
			return
		}
		sub.cancel()
		delete(m.subs, doraUserID)
	}()
}

func (m *manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sub := range m.subs {
		sub.cancel()
	}
	m.subs = make(map[string]*subscription)
}

// runSubscription is the read→decode→filter→publish pipeline. It exits
// when ctx is cancelled (Close / grace-timer fire) or when Stream.Read
// returns a closed channel (auth rejected).
func (m *manager) runSubscription(ctx context.Context, doraUserID, apiKey string) {
	filter := NewFilter(m.lookup, m.strategyTypes)
	for ctx.Err() == nil {
		msgs := m.stream.Read(ctx, doraUserID, apiKey)
		for {
			select {
			case <-ctx.Done():
				return
			case raw, ok := <-msgs:
				if !ok {
					// Stream closed the channel (auth rejected or graceful teardown).
					return
				}
				for _, evt := range m.decodeAndFilter(ctx, raw, filter, doraUserID) {
					m.publish(ctx, evt)
				}
			}
		}
	}
}

// decodeAndFilter parses the DORA `[]StreamOrderUpdatesEntry` envelope,
// iterates entries, decodes each `Val` into map[string]any, and asks
// the Filter which ones to forward. Tolerant of both wrapped (`Val`)
// and flat shapes so tests with hand-rolled JSON work as easily as
// production.
func (m *manager) decodeAndFilter(
	ctx context.Context,
	raw []byte,
	filter *Filter,
	doraUserID string,
) []notifications.Event {
	var entries []map[string]any
	if err := jsonUnmarshal(raw, &entries); err != nil {
		m.log.Debug("DORA entry decode failed",
			"dora_user_id", doraUserID, "err", err)
		return nil
	}
	out := make([]notifications.Event, 0, len(entries))
	for _, entry := range entries {
		val, _ := entry["Val"].(map[string]any)
		if val == nil {
			val = entry // tolerate flat shape (tests use this)
		}
		if evt, ok := filter.Translate(ctx, val, doraUserID); ok {
			out = append(out, evt)
		}
	}
	return out
}

// publish wraps notifier.Publish with a timeout and a debug log on
// failure. Errors are not fatal.
func (m *manager) publish(ctx context.Context, evt notifications.Event) {
	pubCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	if err := m.notifier.Publish(pubCtx, evt); err != nil {
		m.log.Debug("orderupdates publish failed",
			"user_id", evt.UserID, "err", err)
	}
}
