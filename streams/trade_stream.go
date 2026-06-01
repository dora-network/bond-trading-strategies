package streams

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
}

type subscriber struct {
	followedTrader uuid.UUID
	ch             chan TradeEvent
}

func NewTradeStream() *TradeStream {
	return &TradeStream{
		subscribers: make(map[uuid.UUID]*subscriber),
		bookCancels: make(map[string]context.CancelFunc),
		activeBooks: make(map[string]struct{}),
	}
}

func (ts *TradeStream) Start(ctx context.Context, wsURL, apiKey string, orderBookIDs []uuid.UUID) error {
	ts.mu.Lock()
	for _, obID := range orderBookIDs {
		obStr := obID.String()
		if _, ok := ts.activeBooks[obStr]; ok {
			continue
		}
		ts.activeBooks[obStr] = struct{}{}
	}
	ts.mu.Unlock()

	for _, obID := range orderBookIDs {
		obStr := obID.String()
		ts.mu.Lock()
		if _, ok := ts.bookCancels[obStr]; ok {
			ts.mu.Unlock()
			continue
		}
		ts.mu.Unlock()

		tradeChan, cancel, err := ts.dialTradeStream(ctx, wsURL, apiKey, obStr)
		if err != nil {
			slog.Error("failed to start trade stream", "order_book", obStr, "err", err)
			continue
		}

		ts.mu.Lock()
		ts.bookCancels[obStr] = cancel
		ts.mu.Unlock()

		go ts.readLoop(ctx, tradeChan, obID)
	}

	<-ctx.Done()
	return nil
}

func (ts *TradeStream) dialTradeStream(ctx context.Context, wsURL, apiKey, orderBookID string) (<-chan []byte, context.CancelFunc, error) {
	base, err := url.Parse(wsURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse ws url: %w", err)
	}
	base.Path = "/v1/trades/stream"

	q := base.Query()
	q.Set("api_key", apiKey)
	q.Set("order_book_id", orderBookID)
	base.RawQuery = q.Encode()

	conn, _, err := websocket.Dial(ctx, base.String(), nil)
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

func (ts *TradeStream) readLoop(ctx context.Context, tradeChan <-chan []byte, orderBookID uuid.UUID) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-tradeChan:
			if !ok {
				return
			}
			ts.routeTrade(data, orderBookID)
		}
	}
}

func (ts *TradeStream) routeTrade(data []byte, orderBookID uuid.UUID) {
	//nolint:tagliatelle
	var entry struct {
		Val map[string]any `json:"Val"`
	}
	if err := json.Unmarshal(data, &entry); err != nil {
		slog.Warn("failed to unmarshal trade", "err", err)
		return
	}

	val := entry.Val
	if val == nil {
		return
	}

	traderID, err := uuid.Parse(fmt.Sprintf("%v", val["user_id"]))
	if err != nil {
		slog.Warn("failed to parse trader ID", "raw", val["user_id"])
	}
	assetID, err := uuid.Parse(fmt.Sprintf("%v", val["asset_0"]))
	if err != nil {
		slog.Warn("failed to parse asset ID", "raw", val["asset_0"])
	}
	executionID, err := uuid.Parse(fmt.Sprintf("%v", val["transaction_id"]))
	if err != nil {
		slog.Warn("failed to parse execution ID", "raw", val["transaction_id"])
	}
	side, _ := val["side"].(string)
	priceStr, _ := val["price"].(string)
	quantityStr, _ := val["quantity_0"].(string)

	price, err := decimal.Parse(priceStr)
	if err != nil {
		slog.Warn("failed to parse price", "raw", priceStr)
	}
	quantity, err := decimal.Parse(quantityStr)
	if err != nil {
		slog.Warn("failed to parse quantity", "raw", quantityStr)
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
	for _, sub := range ts.subscribers {
		if sub.followedTrader == traderID {
			select {
			case sub.ch <- event:
			default:
				slog.Warn("subscriber channel full, dropping trade", "subscriber", sub.followedTrader)
			}
		}
	}
	ts.mu.Unlock()
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
