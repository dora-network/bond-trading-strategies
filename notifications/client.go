package notifications

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

// ClientEventHandler is invoked for every event read from the WS.
type ClientEventHandler func(ctx context.Context, evt Event) error

// Client is an outbound WebSocket subscriber used by the mcp-server to
// receive notifications from the strategy-server. It auto-reconnects
// with exponential backoff and re-invokes resolveAuth to refresh the
// Authorization header between attempts.
type Client struct {
	wsURL        string
	authHeader   string
	resolveAuth  func(ctx context.Context) (string, error)
	onEvent      ClientEventHandler
	initialDelay time.Duration
	maxDelay     time.Duration
	log          *slog.Logger
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// ClientOnEvent sets the per-event callback.
func ClientOnEvent(h ClientEventHandler) ClientOption {
	return func(c *Client) { c.onEvent = h }
}

// WithClientLogger overrides the default slog logger.
func WithClientLogger(l *slog.Logger) ClientOption { return func(c *Client) { c.log = l } }

// NewClient returns a Client. resolveAuth returns a fresh
// `ApiKey <key>` or `Bearer <token>` value to use on every reconnect.
func NewClient(
	wsURL string,
	initialAuthHeader string,
	resolveAuth func(ctx context.Context) (string, error),
	opts ...ClientOption,
) *Client {
	c := &Client{
		wsURL:        wsURL,
		authHeader:   initialAuthHeader,
		resolveAuth:  resolveAuth,
		initialDelay: 100 * time.Millisecond,
		maxDelay:     5 * time.Second,
		log:          slog.Default(),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Run dials and reads events. It returns nil when the server closes
// the connection cleanly, and ctx.Err() when ctx is cancelled. A
// non-nil error from a connection attempt triggers an exponential
// backoff retry. The caller is expected to invoke this in a goroutine.
func (c *Client) Run(ctx context.Context) error {
	delay := c.initialDelay
	for {
		err := c.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			return nil
		}
		c.log.Warn("notifications: ws disconnected, will retry", "err", err, "delay", delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = nextDelay(delay, c.maxDelay)
		if header, err := c.resolveAuth(ctx); err == nil {
			c.authHeader = header
		}
	}
}

func (c *Client) runOnce(ctx context.Context) error {
	header := http.Header{}
	header.Set("Authorization", c.authHeader)
	//nolint:bodyclose // coder/websocket docs: caller never closes resp.Body
	conn, _, err := websocket.Dial(ctx, c.wsURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if isNormalClosure(err) {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}
		evt, err := decodeEvent(data)
		if err != nil {
			c.log.Warn("notifications: malformed frame", "err", err)
			continue
		}
		if c.onEvent != nil {
			if err := c.onEvent(ctx, evt); err != nil {
				return fmt.Errorf("handler: %w", err)
			}
		}
	}
}

func decodeEvent(data []byte) (Event, error) {
	var evt Event
	if err := jsonUnmarshal(data, &evt); err != nil {
		return Event{}, err
	}
	return evt, nil
}

const jitterDivisor = 5

func nextDelay(d, max time.Duration) time.Duration {
	next := d * 2
	if next > max {
		next = max
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	jitterMax := uint64(next / jitterDivisor) //nolint:gosec // next is bounded by max (5s); safe
	jitterN := binary.BigEndian.Uint64(b[:]) % jitterMax
	return next + time.Duration(jitterN) //nolint:gosec // jitterMax < max < math.MaxInt64
}

func isNormalClosure(err error) bool {
	var ce websocket.CloseError
	return errors.As(err, &ce) && ce.Code == websocket.StatusNormalClosure
}
