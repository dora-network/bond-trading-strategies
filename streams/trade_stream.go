package streams

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// TradeEvent represents a single trade event from the Dora Network trade stream.
type TradeEvent struct {
	TraderID    uuid.UUID
	OrderBookID uuid.UUID
	AssetID     uuid.UUID
	Side        string
	Quantity    decimal.Decimal
	Price       decimal.Decimal
	Timestamp   time.Time
	ExecutionID string
}

// TradeStream connects to the Dora Network trade WebSocket, routes trades to
// subscribers by followedTrader UUID, and blocks until the context is cancelled.
type TradeStream struct {
	mu          sync.Mutex
	subscribers map[uuid.UUID]*subscriber
	bookCancels map[string]context.CancelFunc
	activeBooks map[string]struct{}
	// tradeDial is the per-book dial function. It is injectable so
	// tests can swap in a counter or stub. Defaults to dialTradeStream.
	tradeDial func(ctx context.Context, wsURL, apiKey, orderBookID string, since time.Time) (<-chan []byte, context.CancelFunc, error)
}

const (
	// initialReconnectDelay is the first backoff used by the trade
	// stream's per-book reconnect loop after a connect or read failure.
	initialReconnectDelay = 100 * time.Millisecond
	// maxReconnectDelay caps the exponential backoff to 5s so a
	// persistent outage doesn't cause log flood or thundering-herd.
	maxReconnectDelay = 5 * time.Second
	// jitterDivisor is the fraction of the current delay (1/5 = 20%)
	// used to add jitter. Matches notifications/client.go so the
	// project-wide reconnect behavior stays uniform.
	jitterDivisor = 5
)

// nextDelay doubles the backoff and caps it at maxReconnectDelay, then
// adds ±jitter (uniform random up to delay/jitterDivisor) so a fleet
// of reconnects doesn't synchronize. The cap is a constant rather
// than a parameter because every caller in this package uses the
// same limit.
func nextDelay(d time.Duration) time.Duration {
	next := d * 2
	if next > maxReconnectDelay {
		next = maxReconnectDelay
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	jitterMax := uint64(next / jitterDivisor)
	jitterN := binary.BigEndian.Uint64(b[:]) % jitterMax
	//nolint:gosec // G115: jitterMax < maxReconnectDelay (5s) < math.MaxInt64, conversion is safe
	return next + time.Duration(jitterN)
}

type subscriber struct {
	followedTrader uuid.UUID
	ch             chan TradeEvent
}

func NewTradeStream() *TradeStream {
	dial := func(ctx context.Context, wsURL, apiKey, orderBookID string, since time.Time) (<-chan []byte, context.CancelFunc, error) {
		return dialTradeStream(ctx, wsURL, apiKey, orderBookID, since)
	}
	return &TradeStream{
		subscribers: make(map[uuid.UUID]*subscriber),
		bookCancels: make(map[string]context.CancelFunc),
		activeBooks: make(map[string]struct{}),
		tradeDial:   dial,
	}
}

// Start kicks off a per-book reconnect goroutine for each order book
// and blocks until ctx is cancelled. Unlike the price/candle streams
// which use Daemon.Run (a fixed-delay loop on a single
// StreamFunc), the trade stream runs one reconnect loop per book
// because each book owns its own channel + cancel func and shares
// no bookkeeping state at the dial layer. On connect/read failure
// each loop backs off with initialReconnectDelay, doubling to
// maxReconnectDelay with ±jitter, and resets to initialReconnectDelay
// on the next successful connect. The `since` cursor is reset on
// every reconnect so a reconnecting book doesn't replay the whole
// history a fresh connect would have included anyway.
func (ts *TradeStream) Start(ctx context.Context, wsURL, apiKey string, orderBookIDs []uuid.UUID) error {
	slog.Info("starting trade stream", "order_books", len(orderBookIDs), "ws_url", wsURL)

	wg := sync.WaitGroup{}
	for _, obID := range orderBookIDs {
		obStr := obID.String()
		ts.mu.Lock()
		if _, ok := ts.activeBooks[obStr]; ok {
			ts.mu.Unlock()
			continue
		}
		ts.activeBooks[obStr] = struct{}{}
		ts.mu.Unlock()

		wg.Add(1)
		go func(obID uuid.UUID) {
			defer wg.Done()
			ts.runBookReconnectLoop(ctx, wsURL, apiKey, obID)
		}(obID)
	}

	wg.Wait()
	return nil
}

// runBookReconnectLoop is the per-book connect/read/backoff loop.
// It exits only when ctx is cancelled. On every disconnect it
// reconnects with backoff, retrying until the broker comes back.
func (ts *TradeStream) runBookReconnectLoop(ctx context.Context, wsURL, apiKey string, obID uuid.UUID) {
	obStr := obID.String()
	delay := initialReconnectDelay
	ts.logBookReconnectState(obStr, "starting loop")
	for {
		if err := ctx.Err(); err != nil {
			ts.logBookReconnectState(obStr, "ctx done, exiting")
			return
		}

		since := time.Now().UTC()
		tradeChan, cancel, err := ts.tradeDial(ctx, wsURL, apiKey, obStr, since)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			ts.logBookReconnect(obStr, "dial failed, will retry", err, delay)
			if !ts.sleepWithCtx(ctx, delay) {
				return
			}
			delay = nextDelay(delay)
			continue
		}

		ts.mu.Lock()
		ts.bookCancels[obStr] = cancel
		ts.mu.Unlock()
		ts.logBookReconnectState(obStr, "connected")
		delay = initialReconnectDelay // reset on every successful connect

		ts.readLoop(ctx, tradeChan, obID)
		if cancel != nil {
			cancel()
		}
		ts.mu.Lock()
		delete(ts.bookCancels, obStr)
		ts.mu.Unlock()
		ts.logBookReconnectState(obStr, "readLoop exited, will reconnect")

		if !ts.sleepWithCtx(ctx, delay) {
			return
		}
		delay = nextDelay(delay)
	}
}

func (ts *TradeStream) logBookReconnect(obStr, msg string, err error, delay time.Duration) {
	slog.Warn("trade stream reconnect", "order_book", obStr, "msg", msg, "err", err, "next_delay", delay)
}

func (ts *TradeStream) logBookReconnectState(obStr, msg string) {
	slog.Info("trade stream state", "order_book", obStr, "msg", msg)
}

func (ts *TradeStream) sleepWithCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func dialTradeStream(
	ctx context.Context,
	wsURL, apiKey, orderBookID string,
	since time.Time,
) (<-chan []byte, context.CancelFunc, error) {
	base, err := url.Parse(wsURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse ws url: %w", err)
	}
	base.Path = fmt.Sprintf("/v1/trades/%s/stream", orderBookID)

	q := base.Query()
	q.Set("x-api-key", apiKey)
	q.Set("since", since.Format(time.RFC3339))
	base.RawQuery = q.Encode()

	dialURL := base.String()
	slog.Info("dialing trade stream", "url", safeStreamURL(dialURL))
	conn, _, err := websocket.Dial(ctx, dialURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("dial trade stream: %w", err)
	}

	cancel := func() { _ = conn.CloseNow() }

	ch := make(chan []byte, 1000) //nolint:mnd
	go func() {
		defer close(ch)
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			select {
			case ch <- data:
			default:
				slog.Warn("trade channel full, dropping message")
			}
		}
	}()

	return ch, cancel, nil
}

func safeStreamURL(rawURL string) string {
	base, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}

	q := base.Query()
	if q.Get("x-api-key") != "" {
		q.Set("x-api-key", "***")
	}
	base.RawQuery = q.Encode()

	return base.String()
}

func (ts *TradeStream) readLoop(ctx context.Context, tradeChan <-chan []byte, orderBookID uuid.UUID) {
	slog.Info("trade readLoop started", "order_book", orderBookID)
	defer slog.Info("trade readLoop exiting", "order_book", orderBookID)
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-tradeChan:
			if !ok {
				slog.Warn("trade channel closed", "order_book", orderBookID)
				return
			}
			ts.routeTrade(data, orderBookID)
		}
	}
}

func (ts *TradeStream) routeTrade(data []byte, orderBookID uuid.UUID) {
	//nolint:tagliatelle
	type entryT struct {
		Val  map[string]any `json:"Val"`
		Time string         `json:"Time"`
	}

	var entries []entryT
	if err := json.Unmarshal(data, &entries); err != nil {
		// Fallback: DORA may send a single object instead of an array.
		var single entryT
		if err2 := json.Unmarshal(data, &single); err2 != nil {
			slog.Warn("failed to unmarshal trade", "err", err)
			return
		}
		entries = []entryT{single}
	}

	for _, entry := range entries {
		val := entry.Val
		if val == nil {
			continue
		}

		traderID, err := uuid.Parse(fmt.Sprintf("%v", val["user_id"]))
		if err != nil {
			slog.Warn("failed to parse trader ID", "raw", val["user_id"])
			continue
		}
		assetID, err := uuid.Parse(fmt.Sprintf("%v", val["asset_0"]))
		if err != nil {
			slog.Warn("failed to parse asset ID", "raw", val["asset_0"])
			continue
		}
		executionID, err := uuid.Parse(fmt.Sprintf("%v", val["transaction_id"]))
		if err != nil {
			slog.Warn("failed to parse execution ID", "raw", val["transaction_id"])
			continue
		}
		side, _ := val["side"].(string)
		priceStr, _ := val["price"].(string)
		quantityStr, _ := val["quantity_0"].(string)

		price, err := decimal.Parse(priceStr)
		if err != nil {
			slog.Warn("failed to parse price", "raw", priceStr)
			continue
		}
		quantity, err := decimal.Parse(quantityStr)
		if err != nil {
			slog.Warn("failed to parse quantity", "raw", quantityStr)
			continue
		}

		event := TradeEvent{
			TraderID:    traderID,
			OrderBookID: orderBookID,
			AssetID:     assetID,
			Side:        side,
			Quantity:    quantity,
			Price:       price,
			ExecutionID: executionID.String(),
		}

		ts.mu.Lock()
		matched := false
		for _, sub := range ts.subscribers {
			if sub.followedTrader == traderID {
				matched = true
				select {
				case sub.ch <- event:
				default:
					slog.Warn("subscriber channel full, dropping trade", "subscriber", sub.followedTrader)
				}
			}
		}
		ts.mu.Unlock()

		if matched {
			slog.Info("routed trade to subscriber", "trader", traderID, "order_book", orderBookID, "side", side)
		} else {
			slog.Debug("no subscriber for trade", "trader", traderID, "order_book", orderBookID)
		}
	}
}

// Subscribe registers a subscriber for trades from the given followedTrader UUID.
// Returns a unique subscriber ID and a read-only channel for TradeEvents.
func (ts *TradeStream) Subscribe(followedTrader uuid.UUID) (uuid.UUID, <-chan TradeEvent) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	subscriberID := uuid.New()
	s := &subscriber{
		followedTrader: followedTrader,
		ch:             make(chan TradeEvent, 100), //nolint:mnd
	}
	ts.subscribers[subscriberID] = s
	slog.Info("trade stream subscriber added",
		"subscriber_id", subscriberID,
		"followed_trader", followedTrader,
		"total_subscribers", len(ts.subscribers),
	)
	return subscriberID, s.ch
}

// Unsubscribe removes the subscriber identified by subscriberID and closes its channel.
func (ts *TradeStream) Unsubscribe(subscriberID uuid.UUID) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if sub, ok := ts.subscribers[subscriberID]; ok {
		close(sub.ch)
		delete(ts.subscribers, subscriberID)
	}
}
