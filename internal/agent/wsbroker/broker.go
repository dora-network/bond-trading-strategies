// Package wsbroker owns the single Admin-keyed multiplex websocket
// connection to Dora. One goroutine reads frames; the broker fans
// them out to per-strategy subscribers (the read-broker fan-out in
// spec §4.2). The subscriber API is set in Plan 3; this plan
// defines the connection lifecycle, the reconnect logic, and the
// per-frame callback surface.
package wsbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

// defaultFrameBuffer is the per-subscriber send buffer depth. A
// full buffer means the subscriber is too slow to keep up with the
// frame rate; the broker drops the frame for that subscriber with
// a warn rather than backpressuring the read loop.
const defaultFrameBuffer = 64

// SubscribeAllOrderBooks is the wildcard order book ID for a
// Subscription — receives frames for any order book.
const SubscribeAllOrderBooks = "*"

// Subscription is a per-consumer handle for a fan-out channel. The
// zero value is invalid; obtain one via Broker.Subscribe. The
// broker closes the channel when Unsubscribe is called.
type Subscription struct {
	OrderBookID string   // candle + trade matching
	Resolution  string   // candle matching only
	AssetID     string   // price matching only
	TypeFilter  []string // "candle" | "trade" | "price" — what this sub wants
	// Channels is the legacy per-frame type filter used by Subscribe.
	// Subscribe now mirrors it into TypeFilter; matches dispatches on
	// TypeFilter (falling back to Channels when TypeFilter is empty).
	Channels []string
	ch       chan Frame
}

// Chan returns the receive-only channel for this subscription.
// The broker closes the channel when Unsubscribe is called.
func (s *Subscription) Chan() <-chan Frame { return s.ch }

// matches reports whether the frame should be delivered to this
// subscription. Dispatch is by frame Type: candle matches on
// (OrderBookID, Resolution), trade on OrderBookID, price on AssetID.
// A wildcard OrderBookID ("*") matches any order book. The type
// gate uses TypeFilter when set; otherwise it falls back to the
// legacy Channels field; an empty type filter matches any frame.
func (s *Subscription) matches(f Frame) bool {
	if !s.typeAllowed(f.Type) {
		return false
	}
	switch f.Type {
	case "candle":
		if s.OrderBookID != "" && s.OrderBookID != SubscribeAllOrderBooks && s.OrderBookID != f.OrderBookID {
			return false
		}
		if s.Resolution != "" && s.Resolution != f.Resolution {
			return false
		}
		return true
	case "trade":
		if s.OrderBookID != "" && s.OrderBookID != SubscribeAllOrderBooks && s.OrderBookID != f.OrderBookID {
			return false
		}
		return true
	case "price":
		if s.AssetID != "" && s.AssetID != f.AssetID {
			return false
		}
		return true
	}
	return false
}

// typeAllowed reports whether the frame type passes the
// subscription's type gate (TypeFilter, or legacy Channels).
func (s *Subscription) typeAllowed(t string) bool {
	if len(s.TypeFilter) > 0 {
		for _, tf := range s.TypeFilter {
			if tf == t {
				return true
			}
		}
		return false
	}
	if len(s.Channels) > 0 {
		for _, c := range s.Channels {
			if c == t {
				return true
			}
		}
		return false
	}
	return true
}

// dialTimeout is the max time to establish a wsplex connection.
// Named for gosec/mnd: avoids magic-number lint on the dial timeout.
const (
	subscribeWriteTimeout = 5 * time.Second
	dialTimeout           = 10 * time.Second
)

type Frame struct {
	Type        string // "candle" | "trade" | "price"
	OrderBookID string
	Resolution  string          // candle only
	AssetID     string          // price only
	Raw         json.RawMessage // the per-event JSON the plugin sees
}

// Config is the broker configuration.
type Config struct {
	URL              string        // wsplex URL, e.g. wss://staging.dora.co/plex
	APIKey           string        // Admin-role Dora API key
	ReconnectBackoff time.Duration // default 1s; tests use 50ms
}

func (c *Config) defaults() {
	if c.ReconnectBackoff == 0 {
		c.ReconnectBackoff = 1 * time.Second
	}
}

type Broker struct {
	cfg            Config
	mu             sync.Mutex
	frameCb        []func(Frame)
	subs           []*Subscription
	conn           *websocket.Conn
	stop           chan struct{}
	stopped        bool
	subscribedKeys []string
	// pendingCandle caches the most recent in-progress candle per
	// (order book, resolution). Key is orderBookID + ":" + resolution.
	//
	// The wsplex wire format for /charts/candles is:
	//   - On subscribe: a snapshot array of [prev_closed,
	//     curr_in_progress] — two distinct start_timestamps, oldest
	//     first.
	//   - On every subsequent update: a single-element array of the
	//     current in-progress candle.
	// There is no explicit "the in-progress candle just closed"
	// notification. Instead, a new window open is signalled by a
	// single-element array whose start_timestamp differs from the
	// cached in-progress candle. The broker then emits the cached
	// candle (the just-closed one) and replaces the cache with the
	// new in-progress candle.
	pendingCandle map[string]json.RawMessage
	pendingTS     map[string]string
	// lastSentTS records the start_timestamp of the most recent
	// candle emitted per (order book, resolution). Used to suppress
	// duplicate bootstrap emits when the wsplex re-sends a snapshot
	// (e.g., on reconnect) that carries the same closed candle.
	lastSentTS map[string]string
}

// New constructs a Broker. Start must be called before the broker
// emits frames.
func New(cfg Config) (*Broker, error) {
	if cfg.URL == "" {
		return nil, errors.New("wsbroker: URL is required")
	}
	if cfg.APIKey == "" {
		return nil, errors.New("wsbroker: APIKey is required")
	}
	cfg.defaults()
	return &Broker{cfg: cfg, stop: make(chan struct{}),
		pendingCandle: make(map[string]json.RawMessage),
		pendingTS:     make(map[string]string),
		lastSentTS:    make(map[string]string),
	}, nil
}

// OnFrame registers a per-frame callback. The callback runs on the
// broker's read goroutine. The slice is append-only.
func (b *Broker) OnFrame(cb func(Frame)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.frameCb = append(b.frameCb, cb)
}

// Subscribe registers a per-(order book, resolution) consumer.
func (b *Broker) Subscribe(orderBookID, resolution string, channels []string) *Subscription {
	sub := &Subscription{
		OrderBookID: orderBookID,
		Resolution:  resolution,
		Channels:    channels,
		TypeFilter:  channels,
		ch:          make(chan Frame, defaultFrameBuffer),
	}
	b.mu.Lock()
	b.subs = append(b.subs, sub)
	conn := b.conn
	subKey := orderBookID + ":" + resolution
	alreadySubbed := false
	for _, k := range b.subscribedKeys {
		if k == subKey {
			alreadySubbed = true
			break
		}
	}
	if !alreadySubbed && orderBookID != SubscribeAllOrderBooks {
		b.subscribedKeys = append(b.subscribedKeys, subKey)
	}
	b.mu.Unlock()
	if conn != nil && !alreadySubbed && orderBookID != SubscribeAllOrderBooks {
		ctx, cancel := context.WithTimeout(context.Background(), subscribeWriteTimeout)
		defer cancel()
		b.sendCandleSubscribe(ctx, conn, orderBookID, resolution)
	}
	return sub
}

// Unsubscribe removes sub from the fan-out list and closes its
// channel. Idempotent: calling twice is a no-op.
func (b *Broker) Unsubscribe(sub *Subscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, s := range b.subs {
		if s == sub {
			b.subs = append(b.subs[:i], b.subs[i+1:]...)
			close(s.ch)
			return
		}
	}
}

// SubscribeTrades subscribes to /trades for one order book with no
// user filter (the strategy wants all trades on the book). Sends a
// /trades subscribe envelope with order_book_ids=[orderBookID],
// users_all=true. The subscription key "trades:<orderBookID>" is
// recorded so reconnects re-send it.
func (b *Broker) SubscribeTrades(orderBookID string) *Subscription {
	sub := &Subscription{
		OrderBookID: orderBookID,
		TypeFilter:  []string{"trade"},
		ch:          make(chan Frame, defaultFrameBuffer),
	}
	const subKeyPrefix = "trades:"
	subKey := subKeyPrefix + orderBookID
	conn, already := b.registerSub(sub, subKey)
	if conn != nil && !already {
		ctx, cancel := context.WithTimeout(context.Background(), subscribeWriteTimeout)
		defer cancel()
		b.sendTradesSubscribe(ctx, conn, orderBookID)
	}
	return sub
}

// SubscribePrices subscribes to /prices for one asset id. Sends a
// /prices subscribe envelope with subscribe=[assetID]. The
// subscription key "prices:<assetID>" is recorded so reconnects
// re-send it.
func (b *Broker) SubscribePrices(assetID string) *Subscription {
	sub := &Subscription{
		AssetID:    assetID,
		TypeFilter: []string{"price"},
		ch:         make(chan Frame, defaultFrameBuffer),
	}
	const subKeyPrefix = "prices:"
	subKey := subKeyPrefix + assetID
	conn, already := b.registerSub(sub, subKey)
	if conn != nil && !already {
		ctx, cancel := context.WithTimeout(context.Background(), subscribeWriteTimeout)
		defer cancel()
		b.sendPricesSubscribe(ctx, conn, assetID)
	}
	return sub
}

// registerSub appends sub to the fan-out list, records subKey in
// subscribedKeys (for reconnect re-send), and returns the current
// connection plus whether subKey was already subscribed. Callers
// do the wire write outside the lock when conn != nil && !already.
func (b *Broker) registerSub(sub *Subscription, subKey string) (conn *websocket.Conn, already bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs = append(b.subs, sub)
	conn = b.conn
	for _, k := range b.subscribedKeys {
		if k == subKey {
			already = true
			break
		}
	}
	if !already {
		b.subscribedKeys = append(b.subscribedKeys, subKey)
	}
	return conn, already
}

// Start connects to the wsplex and begins the read loop. The
// loop reconnects with backoff on disconnect. Start blocks until
// ctx is done, b.Stop is called, or the loop returns an unrecoverable
// error.
func (b *Broker) Start(ctx context.Context) error {
	backoff := b.cfg.ReconnectBackoff
	for {
		// Non-blocking signal check at the top of each loop
		// iteration. The default branch is intentional: it makes
		// the select a fast-path "is anyone asking me to stop?"
		// probe, so the function proceeds to connectAndRead on
		// every iteration where no signal is pending. Removing
		// the default would block the loop on a select that
		// only ever fires on shutdown — tests that call Start
		// in a goroutine and then expect it to do work would hang.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.stop:
			return nil
		default:
		}
		if err := b.connectAndRead(ctx); err != nil {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			case <-b.stop:
				return nil
			}
			continue
		}
		return nil
	}
}

// connectAndRead opens the connection and reads frames until
// disconnect or ctx cancellation.
func (b *Broker) connectAndRead(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	headers := http.Header{}
	headers.Set("Authorization", "ApiKey "+b.cfg.APIKey)
	headers.Set("User-Agent", "dora-agent/1.0")

	//nolint:bodyclose // coder/websocket docs: "You never need to close resp.Body yourself."
	conn, _, err := websocket.Dial(dialCtx, b.cfg.URL, &websocket.DialOptions{
		HTTPHeader: headers,
	})
	if err != nil {
		return fmt.Errorf("wsbroker: dial: %w", err)
	}

	b.mu.Lock()
	b.conn = conn
	b.mu.Unlock()
	defer func() {
		_ = conn.Close(websocket.StatusNormalClosure, "broker closing")
	}()
	slog.Info("wsbroker: connected", "url", b.cfg.URL)

	// Re-send any pending subscriptions from before the connection
	// was established (or from a reconnect). Keys are:
	//   - candle: "orderBookID:resolution" (no prefix, existing)
	//   - trade:  "trades:<orderBookID>"
	//   - price:  "prices:<assetID>"
	b.mu.Lock()
	pending := append([]string(nil), b.subscribedKeys...)
	b.mu.Unlock()
	for _, key := range pending {
		switch {
		case strings.HasPrefix(key, "trades:"):
			b.sendTradesSubscribe(ctx, conn, strings.TrimPrefix(key, "trades:"))
		case strings.HasPrefix(key, "prices:"):
			b.sendPricesSubscribe(ctx, conn, strings.TrimPrefix(key, "prices:"))
		default:
			// Candle subscription: "orderBookID:resolution".
			parts := strings.SplitN(key, ":", 2) //nolint:mnd // split into 2
			if len(parts) != 2 {                 //nolint:mnd // expected 2 parts
				continue
			}
			b.sendCandleSubscribe(ctx, conn, parts[0], parts[1])
		}
	}

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("wsbroker: read: %w", err)
		}

		var env struct {
			ID   string          `json:"id"`
			Kind string          `json:"kind"`
			Path string          `json:"path"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(data, &env); err != nil {
			slog.Debug("wsbroker: unparseable message", "bytes", len(data))
			continue
		}
		if env.Kind == "response" {
			slog.Info("wsbroker: response", "path", env.Path)
			continue
		}
		if env.Kind != "notification" {
			continue
		}

		switch env.Path {
		case "/charts/candles":
			b.handleCandleNotification(env.Data)
		case "/trades":
			b.handleTradeNotification(env.Data)
		case "/prices":
			b.handlePriceNotification(env.Data)
		}
	}
}

// handleCandleNotification parses a /charts/candles notification and
// routes the most-recently-CLOSED candle to matching subscribers.
//
// The wsplex sends a snapshot on first subscribe (history of the
// current window) and deltas on each update. Each notification
// contains the in-progress candle. We only forward a candle to the
// strategy when its start_timestamp differs from the last one we
// sent — the candle that "just closed" (the one captured before the
// new one) is the one the strategy should see.
func (b *Broker) handleCandleNotification(data json.RawMessage) {
	var notify struct {
		Resolution string                     `json:"resolution"`
		Candles    map[string]json.RawMessage `json:"candles"`
	}
	if err := json.Unmarshal(data, &notify); err != nil {
		slog.Debug("wsbroker: failed to parse candles", "error", err)
		return
	}
	for obID, candleArr := range notify.Candles {
		var candles []json.RawMessage
		if err := json.Unmarshal(candleArr, &candles); err != nil || len(candles) == 0 {
			continue
		}
		// Identify the in-progress candle for this notification.
		// - Snapshot (array length >= 2): the LAST element is the
		//   in-progress candle; the second-to-last is the just-closed
		//   one we should emit as the bootstrap snapshot.
		// - Single-element array: only the in-progress candle.
		// The wsplex does not emit an explicit "previous in-progress
		// candle just closed" notification. We infer the window close
		// by watching the in-progress candle's start_timestamp change.
		var (
			newInProgress json.RawMessage
			bootstrap     json.RawMessage // set only when len(candles) >= 2
		)
		// candleSnapshotSizeThreshold is the minimum array length at
		// which the notification is treated as a snapshot including
		// a just-closed previous candle. wsplex sends a 2-element
		// snapshot on subscribe and 1-element updates thereafter.
		const candleSnapshotSizeThreshold = 2
		if len(candles) >= candleSnapshotSizeThreshold {
			newInProgress = candles[len(candles)-1]
			bootstrap = candles[len(candles)-2]
		} else {
			newInProgress = candles[0]
		}
		var newMeta struct {
			StartTimestamp string `json:"start_timestamp"`
		}
		if err := json.Unmarshal(newInProgress, &newMeta); err != nil {
			slog.Debug("wsbroker: failed to parse in-progress candle timestamp",
				"order_book", obID, "error", err)
			continue
		}
		if newMeta.StartTimestamp == "" {
			slog.Debug("wsbroker: empty start_timestamp on in-progress candle",
				"order_book", obID)
			continue
		}

		// Drive the state machine. The selection of what to emit
		// (and what to update next) is a single locked transaction.
		emitCandle, emitTS := b.advanceCandle(obID, notify.Resolution,
			newInProgress, newMeta.StartTimestamp, bootstrap)

		if emitCandle == nil {
			continue
		}
		f := Frame{
			Type:        "candle",
			OrderBookID: obID,
			Resolution:  notify.Resolution,
			Raw:         emitCandle,
		}
		slog.Info("wsbroker: candle sent to strategy",
			"order_book", obID, "resolution", notify.Resolution,
			"start_timestamp", emitTS)
		b.dispatch(f)
	}
}

// handleTradeNotification parses a /trades notification and
// fans out each trade to matching subscribers. data.trades is
// a JSON array of Trade records (per the wsplex /trades schema).
func (b *Broker) handleTradeNotification(data json.RawMessage) {
	var payload struct {
		Trades []struct {
			TransactionID      string `json:"transaction_id"`
			OrderBookID        string `json:"order_book_id"`
			OrderID            string `json:"order_id"`
			OrderSeq           int64  `json:"order_seq"`
			UserID             string `json:"user_id"`
			Asset0             string `json:"asset_0"` //nolint:tagliatelle // wsplex asyncapi wire name
			Price              string `json:"price"`
			Quantity0          string `json:"quantity_0"` //nolint:tagliatelle // wsplex asyncapi wire name
			Side               string `json:"side"`
			AggressorIndicator bool   `json:"aggressor_indicator"`
			CreatedAt          string `json:"created_at"`
		} `json:"trades"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		slog.Debug("wsbroker: unparseable /trades", "error", err)
		return
	}
	for _, t := range payload.Trades {
		raw, _ := json.Marshal(t)
		b.dispatch(Frame{
			Type:        "trade",
			OrderBookID: t.OrderBookID,
			Raw:         raw,
		})
	}
}

// handlePriceNotification parses a /prices notification. Note
// that data.prices is a MAP keyed by asset_id (not an array —
// the asyncapi doc flags this as a breaking change).
func (b *Broker) handlePriceNotification(data json.RawMessage) {
	var payload struct {
		Prices map[string]struct {
			AssetID string `json:"asset_id"`
			Price   string `json:"price"`
			YTM     string `json:"ytm"`
			Time    string `json:"time"`
		} `json:"prices"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		slog.Debug("wsbroker: unparseable /prices", "error", err)
		return
	}
	for assetID, p := range payload.Prices {
		raw, _ := json.Marshal(p)
		b.dispatch(Frame{
			Type:    "price",
			AssetID: assetID,
			Raw:     raw,
		})
	}
}

// advanceCandle is the candle state machine for one (order book,
// resolution). It owns the lock and decides what (if any) candle
// to emit, then updates the cache.
//
// Rules:
//   - First call for this (ob, res): no cache. If the caller passed
//     a snapshot bootstrap (2-element notification), emit the
//     second-to-last element (the just-closed candle) and start
//     caching the new in-progress. Otherwise (single-element
//     bootstrap — no history available), simply cache the new
//     in-progress and wait for the next window open to emit.
//   - Subsequent calls: the cached candle is the in-progress one
//     from the previous notification. If the new candle's
//     start_timestamp matches the cached one, this is an in-progress
//     tick: refresh the cache, no emit. If it differs, the cached
//     candle just closed: emit it, then refresh the cache with the
//     new in-progress candle (and, if the caller also supplied a
//     snapshot bootstrap, emit that as a second candle — the
//     snapshot's "previous closed" reference).
//
// Returns the candle to emit (or nil to skip) and its
// start_timestamp for logging.
func (b *Broker) advanceCandle(
	obID, resolution string,
	newInProgress json.RawMessage, newTS string,
	bootstrap json.RawMessage, // non-nil only when notification had >= 2 elements
) (json.RawMessage, string) {
	dedupKey := obID + ":" + resolution
	b.mu.Lock()
	defer b.mu.Unlock()

	cached, hasCached := b.pendingCandle[dedupKey]
	cachedTS := b.pendingTS[dedupKey]

	if bootstrap != nil {
		// Snapshot path. The wsplex is explicitly telling us
		// what the just-closed candle is. This happens on
		// initial subscribe and on reconnect (where the broker
		// re-subscribes after a dropped connection and the
		// server sends a fresh snapshot of the previous closed
		// candle + the current in-progress one).
		//
		// The snapshot's bootstrap is always authoritative — it
		// may differ from the cached in-progress candle if the
		// disconnect spanned a window close. Refresh the cache
		// with the new in-progress and emit the bootstrap.
		var bootMeta struct {
			StartTimestamp string `json:"start_timestamp"`
		}
		_ = json.Unmarshal(bootstrap, &bootMeta)
		if hasCached && bootMeta.StartTimestamp != "" && bootMeta.StartTimestamp != cachedTS {
			// Disconnect spanned a window close and we missed
			// the cached candle fully closing. The snapshot
			// gives us the most-recent closed reference; the
			// older one is gone. Log a warning so operators
			// see the gap.
			slog.Warn("wsbroker: snapshot bootstrap differs from cached in-progress; dropping stale cached candle",
				"order_book", obID, "resolution", resolution,
				"cached_start", cachedTS, "snapshot_start", bootMeta.StartTimestamp)
		}
		b.pendingCandle[dedupKey] = newInProgress
		b.pendingTS[dedupKey] = newTS
		// Dedupe: if we already emitted this candle (e.g., the wsplex
		// re-sent the same snapshot on reconnect), skip the emit but
		// still refresh the cache.
		if bootMeta.StartTimestamp != "" && b.lastSentTS[dedupKey] == bootMeta.StartTimestamp {
			slog.Debug("wsbroker: snapshot bootstrap already sent, skip",
				"order_book", obID, "resolution", resolution,
				"start_timestamp", bootMeta.StartTimestamp)
			return nil, ""
		}
		b.lastSentTS[dedupKey] = bootMeta.StartTimestamp
		return bootstrap, bootMeta.StartTimestamp
	}

	// Single-element path: in-progress tick or window-open
	// without an explicit snapshot.
	if !hasCached {
		// First-ever single-element notification for this
		// (ob, res). No prior state — cache the candle and
		// wait for the next window open to emit.
		b.pendingCandle[dedupKey] = newInProgress
		b.pendingTS[dedupKey] = newTS
		return nil, ""
	}
	if cachedTS == newTS {
		// In-progress tick: refresh the cache, no emit.
		b.pendingCandle[dedupKey] = newInProgress
		return nil, ""
	}
	// Different start_timestamp and no snapshot: the cached
	// candle just closed and wsplex is now sending the new
	// in-progress one. Emit the cached candle (with its
	// final OHLC from the last tick), then refresh the cache.
	closedCandle := cached
	closedTS := cachedTS
	b.pendingCandle[dedupKey] = newInProgress
	b.pendingTS[dedupKey] = newTS
	if closedTS != "" && b.lastSentTS[dedupKey] == closedTS {
		slog.Debug("wsbroker: cached candle already sent, skip",
			"order_book", obID, "resolution", resolution,
			"start_timestamp", closedTS)
		return nil, ""
	}
	b.lastSentTS[dedupKey] = closedTS
	return closedCandle, closedTS
}

// dispatch routes a frame to all matching subscribers.
func (b *Broker) dispatch(f Frame) {
	b.mu.Lock()
	subs := b.subs
	b.mu.Unlock()
	for _, sub := range subs {
		if !sub.matches(f) {
			continue
		}
		select {
		case sub.ch <- f:
		default:
			slog.Warn("wsbroker: subscriber buffer full, dropping frame",
				"frame_type", f.Type,
				"order_book", sub.OrderBookID,
				"asset_id", sub.AssetID,
			)
		}
	}
}

// sendCandleSubscribe sends a wsplex subscribe for /charts/candles
func (b *Broker) sendCandleSubscribe(ctx context.Context, conn *websocket.Conn, orderBookID, resolution string) {
	msg, _ := json.Marshal(map[string]any{
		"id":   uuid.NewString(),
		"path": "/charts/candles",
		"data": map[string]any{
			"subscribe": map[string]any{
				"orderbook_ids": []string{orderBookID},
				"resolution":    resolution,
			},
		},
	})
	if err := conn.Write(ctx, websocket.MessageText, msg); err != nil {
		slog.Warn("wsbroker: candle subscribe failed", "order_book", orderBookID, "error", err)
	} else {
		slog.Info("wsbroker: subscribed to candles", "order_book", orderBookID, "resolution", resolution)
	}
}

// sendTradesSubscribe writes a /trades request envelope.
func (b *Broker) sendTradesSubscribe(ctx context.Context, conn *websocket.Conn, orderBookID string) {
	msg, _ := json.Marshal(map[string]any{
		"id":   uuid.NewString(),
		"path": "/trades",
		"data": map[string]any{
			"subscribe": []map[string]any{
				{"order_book_ids": []string{orderBookID}, "users_all": true},
			},
		},
	})
	if err := conn.Write(ctx, websocket.MessageText, msg); err != nil {
		slog.Warn("wsbroker: trades subscribe failed", "order_book", orderBookID, "error", err)
	} else {
		slog.Info("wsbroker: subscribed to trades", "order_book", orderBookID)
	}
}

// sendPricesSubscribe writes a /prices request envelope.
func (b *Broker) sendPricesSubscribe(ctx context.Context, conn *websocket.Conn, assetID string) {
	msg, _ := json.Marshal(map[string]any{
		"id":   uuid.NewString(),
		"path": "/prices",
		"data": map[string]any{
			"subscribe": []string{assetID},
		},
	})
	if err := conn.Write(ctx, websocket.MessageText, msg); err != nil {
		slog.Warn("wsbroker: prices subscribe failed", "asset", assetID, "error", err)
	} else {
		slog.Info("wsbroker: subscribed to prices", "asset", assetID)
	}
}

// Stop signals the read loop to exit. Safe to call multiple times.
func (b *Broker) Stop() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopped {
		return nil
	}
	b.stopped = true
	close(b.stop)
	return nil
}
