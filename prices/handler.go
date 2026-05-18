// Package prices provides a daemon that streams real-time asset prices from the
// DORA WebSocket API and persists each update to the price_history Postgres table.
//
// The stream endpoint sends a JSON object on every update whose keys are asset
// UUIDs and whose values are AssetPrice objects. The first message contains the
// full current snapshot; subsequent messages carry only changed prices.
package prices

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
)

// AssetPrice is a single price record received from the stream.
type AssetPrice struct {
	AssetID string           `json:"asset_id"`
	Price   decimal.Decimal  `json:"price"`
	YTM     *decimal.Decimal `json:"ytm"`
	Time    time.Time        `json:"time"`
}

// Config holds all settings needed to run the Daemon.
type Config struct {
	// BaseURL is the WebSocket API base URL.
	BaseURL string
	// DBURL is the Postgres connection string (DSN or URL format).
	DBURL string
	// APIKey is sent as the "api_key" query parameter on the WebSocket URL.
	APIKey string
	// AssetID optionally filters the stream to a single asset UUID.
	AssetID string
}

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate . PriceStore

// PriceStore defines the interface for persisting prices.
type PriceStore interface {
	SavePrices(ctx context.Context, prices map[uuid.UUID]AssetPrice) error
}

type Handler struct {
	mu          sync.RWMutex
	cfg         Config
	subscribers map[uuid.UUID]chan map[uuid.UUID]AssetPrice
	onMessage   func()
}

func New(cfg Config, opts ...func(*Handler)) *Handler {
	h := &Handler{
		cfg:         cfg,
		subscribers: make(map[uuid.UUID]chan map[uuid.UUID]AssetPrice),
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
// When the handler receives a new price update, it sends the updated prices to the channel.
func (h *Handler) Subscribe(requestID uuid.UUID) (chan map[uuid.UUID]AssetPrice, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	slog.Info("subscribing to price updates", "requestID", requestID)
	if _, ok := h.subscribers[requestID]; ok {
		return nil, fmt.Errorf("already subscribed")
	}
	ch := make(chan map[uuid.UUID]AssetPrice)
	h.subscribers[requestID] = ch
	return ch, nil
}

// Unsubscribe removes the subscription channel for the given request ID.
func (h *Handler) Unsubscribe(requestID uuid.UUID) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// find the channel to close
	ch, ok := h.subscribers[requestID]
	if !ok {
		return fmt.Errorf("subscriber not found for request ID: %s", requestID)
	}

	// close the channel
	close(ch)

	// delete the subscriber
	delete(h.subscribers, requestID)
	return nil
}

// Stream opens one WebSocket connection and reads messages until an error or
// ctx cancellation.
func (h *Handler) Stream(ctx context.Context) error {
	wsURL, err := h.buildURL()
	if err != nil {
		return fmt.Errorf("build ws url: %w", err)
	}

	slog.Info("connecting to price stream", "url", h.safeURL())

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() {
		if err = conn.CloseNow(); err != nil {
			slog.Error("failed to close price stream", "error", err)
		}
	}()

	slog.Info("connected to price stream")

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		if err := h.processMessage(ctx, data); err != nil {
			slog.Warn("failed to process price message", "err", err)
		}
	}
}

// processMessage unmarshals and saves the message data.
func (h *Handler) processMessage(ctx context.Context, data []byte) error {
	var prices map[uuid.UUID]AssetPrice
	if err := json.Unmarshal(data, &prices); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	h.mu.RLock()
	slog.Debug("sending price updates", "updates", len(prices), "subscribers", len(h.subscribers))
	h.mu.RUnlock()

	for subscriber, subCh := range h.subscribers {
		timeout := time.Millisecond * 50
		pushCtx, cancel := context.WithTimeout(ctx, timeout)
		select {
		case <-pushCtx.Done():
			slog.Warn("subscriber timed out", "subscriber", subscriber, "timeout", timeout)
			cancel()
			continue
		case subCh <- prices:
			cancel()
		}
	}

	if h.onMessage != nil {
		h.onMessage()
	}

	return nil
}

// buildURL constructs the full WebSocket URL including query parameters.
func (h *Handler) buildURL() (string, error) {
	base, err := url.Parse(h.cfg.BaseURL)
	if err != nil {
		return "", fmt.Errorf("parse ws url: %w", err)
	}
	base.Path = "/v1/prices/stream"

	q := base.Query()
	if h.cfg.APIKey != "" {
		q.Set("api_key", h.cfg.APIKey)
	}
	if h.cfg.AssetID != "" {
		q.Set("asset_id", h.cfg.AssetID)
	}
	base.RawQuery = q.Encode()

	return base.String(), nil
}

// safeURL returns the WebSocket URL with the api_key query parameter
// replaced by "***" so it is safe to include in log output.
func (h *Handler) safeURL() string {
	base, err := url.Parse(h.cfg.BaseURL)
	if err != nil {
		return h.cfg.BaseURL
	}
	base.Path = "/v1/prices/stream"

	q := base.Query()
	if h.cfg.APIKey != "" {
		q.Set("api_key", "***")
	}
	if h.cfg.AssetID != "" {
		q.Set("asset_id", h.cfg.AssetID)
	}
	base.RawQuery = q.Encode()

	return base.String()
}
