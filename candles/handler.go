// Package candles provides a daemon that streams real-time candlestick data from the
// DORA WebSocket API and persists each update to the candles_history Postgres table.
package candles

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"golang.org/x/sync/errgroup"
)

// Candle represents a single candlestick data point.
type Candle struct {
	OrderBookID    string          `json:"order_book_id"`
	StartTimestamp time.Time       `json:"start_timestamp"`
	Open           decimal.Decimal `json:"open"`
	High           decimal.Decimal `json:"high"`
	Low            decimal.Decimal `json:"low"`
	Close          decimal.Decimal `json:"close"`
	Volume         decimal.Decimal `json:"volume"`
}

// StreamCandlesEntry wraps a Candle with its stream timestamp.
//
//nolint:tagliatelle
type StreamCandlesEntry struct {
	Val Candle `json:"Val"`
	//nolint:tagliatelle
	Time time.Time `json:"Time"`
}

// Config holds all settings needed to run the Daemon for candles.
type Config struct {
	// BaseURL is the WebSocket API base URL.
	BaseURL string
	// DBURL is the Postgres connection string.
	DBURL string
	// APIKey is sent as the "api_key" query parameter on the WebSocket URL.
	APIKey string
	// OrderBookIDs is a list of order books to subscribe to.
	OrderBookIDs []string
	// Since optionally requests candles only from this point in time onward.
	Since time.Time
}

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate . CandleStore

// CandleStore defines the interface for querying and persisting candles.
type CandleStore interface {
	GetLastTimestamp(ctx context.Context, orderBookID string) (*time.Time, error)
	SaveCandles(ctx context.Context, entries []StreamCandlesEntry) error
}

type Handler struct {
	mu          sync.RWMutex
	cfg         Config
	store       CandleStore
	subscribers map[uuid.UUID]chan []StreamCandlesEntry
	onMessage   func()
}

// New creates a new candles Handler.
func New(cfg Config, store CandleStore, opts ...func(*Handler)) *Handler {
	h := &Handler{
		cfg:         cfg,
		store:       store,
		subscribers: make(map[uuid.UUID]chan []StreamCandlesEntry),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func WithMessageHook(onMessage func()) func(*Handler) {
	return func(h *Handler) {
		h.onMessage = onMessage
	}
}

// Subscribe creates a new subscription channel for the given request ID and returns it.
// When the handler receives a new candle update, it sends the entries to the channel.
func (h *Handler) Subscribe(requestID uuid.UUID) (chan []StreamCandlesEntry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	slog.Info("subscribing to candle updates", "requestID", requestID)
	if _, ok := h.subscribers[requestID]; ok {
		return nil, fmt.Errorf("already subscribed")
	}

	ch := make(chan []StreamCandlesEntry)
	h.subscribers[requestID] = ch
	return ch, nil
}

// Unsubscribe removes the subscription channel for the given request ID.
func (h *Handler) Unsubscribe(requestID uuid.UUID) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch, ok := h.subscribers[requestID]
	if !ok {
		return fmt.Errorf("subscriber not found for request ID: %s", requestID)
	}

	close(ch)
	delete(h.subscribers, requestID)
	return nil
}

// Stream opens WebSocket connections for each configured order book and reads
// messages until an error or ctx cancellation.
func (h *Handler) Stream(ctx context.Context) error {
	if h.store == nil {
		return fmt.Errorf("missing candle store")
	}

	if len(h.cfg.OrderBookIDs) == 0 {
		return fmt.Errorf("no order books configured for streaming")
	}

	eg, egCtx := errgroup.WithContext(ctx)

	for _, id := range h.cfg.OrderBookIDs {
		orderBookID := id // capture loop variable
		eg.Go(func() error {
			return h.streamSingle(egCtx, orderBookID)
		})
	}

	return eg.Wait()
}

// streamSingle handles the websocket connection and message loop for a single order book.
func (h *Handler) streamSingle(ctx context.Context, orderBookID string) error {
	lastTimestamp, err := h.store.GetLastTimestamp(ctx, orderBookID)
	if err != nil {
		return fmt.Errorf("get last timestamp for %s: %w", orderBookID, err)
	}

	var streamSince *time.Time
	if lastTimestamp != nil {
		streamSince = lastTimestamp
	} else if !h.cfg.Since.IsZero() {
		streamSince = &h.cfg.Since
	}

	wsURL, err := h.buildURL(orderBookID, streamSince)
	if err != nil {
		return fmt.Errorf("build ws url for %s: %w", orderBookID, err)
	}

	slog.Info("connecting to candle stream", "order_book_id", orderBookID, "url", wsURL)

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", orderBookID, err)
	}
	defer func() {
		if err = conn.CloseNow(); err != nil {
			slog.Error("failed to close candle stream", "order_book_id", orderBookID, "error", err)
		}
	}()

	slog.Info("connected to candle stream", "order_book_id", orderBookID)

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read %s: %w", orderBookID, err)
		}

		if err := h.processMessage(ctx, orderBookID, data); err != nil {
			slog.Warn("failed to process candle message", "order_book_id", orderBookID, "err", err)
		}
	}
}

// processMessage parses and saves the candle stream data.
func (h *Handler) processMessage(ctx context.Context, orderBookID string, data []byte) error {
	var entries []StreamCandlesEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	h.mu.RLock()
	slog.Debug("sending candle updates", "order_book_id", orderBookID, "updates", len(entries), "subscribers", len(h.subscribers))
	h.mu.RUnlock()

	for subscriber, subCh := range h.subscribers {
		timeout := 50 * time.Millisecond
		pushCtx, cancel := context.WithTimeout(ctx, timeout)
		select {
		case <-pushCtx.Done():
			slog.Warn("subscriber timed out", "subscriber", subscriber, "order_book_id", orderBookID, "timeout", timeout)
			cancel()
			continue
		case subCh <- entries:
			cancel()
		}
	}

	if h.onMessage != nil {
		h.onMessage()
	}

	return nil
}

// buildURL constructs the full WebSocket URL including query parameters.
func (h *Handler) buildURL(orderBookID string, since *time.Time) (string, error) {
	base, err := url.Parse(h.cfg.BaseURL)
	if err != nil {
		return "", fmt.Errorf("parse ws url: %w", err)
	}
	base.Path = fmt.Sprintf("/v1/charts/%s/candle/stream", orderBookID)

	q := base.Query()
	if h.cfg.APIKey != "" {
		q.Set("api_key", h.cfg.APIKey)
	}
	// The plan specifies 1 minute candles
	q.Set("resolution", "1m")
	if since != nil && !since.IsZero() {
		q.Set("since", since.UTC().Format(time.RFC3339))
	}
	base.RawQuery = q.Encode()

	return base.String(), nil
}
