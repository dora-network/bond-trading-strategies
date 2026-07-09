package orderupdates

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	initialBackoff = 1 * time.Second
	maxBackoff     = 60 * time.Second
	jitterDivisor  = 5
	msgChanBuf     = 32
)

// ErrAuthRejected signals the supplied API key was rejected by DORA.
// Callers must NOT auto-retry; the Stream exits the read loop when this
// is returned.
var ErrAuthRejected = errors.New("orderupdates: DORA rejected auth (401/403)")

// ManagerStream is the dial interface the Manager consumes. Production
// code passes *Stream; tests pass a controllable fake.
type ManagerStream interface {
	Read(ctx context.Context, doraUserID, apiKey string) <-chan []byte
}

// Stream dials DORA's per-user order-updates WebSocket and reconnects
// with exponential backoff on transient failures. Concurrency-safe.
//
// On a successful message, the Stream updates the in-memory `since`
// cursor from each entry's `updated_at`; the next reconnect dials
// with `?since=<that timestamp>`. The cursor is keyed by doraUserID
// so one user's reconnect never carries another user's timestamp.
type Stream struct {
	wsBaseURL string
	log       *slog.Logger
	mu        sync.Mutex
	since     map[string]*time.Time // keyed by doraUserID
}

func NewStream(wsBaseURL string, log *slog.Logger) *Stream {
	if log == nil {
		log = slog.Default()
	}
	return &Stream{wsBaseURL: wsBaseURL, log: log, since: make(map[string]*time.Time)}
}

// Read opens the per-user DORA order-updates stream and returns a
// channel of raw JSON messages (each message is the full array DORA
// sent). The channel closes when:
//   - ctx is cancelled (graceful teardown)
//   - auth is rejected by DORA (no auto-retry)
//   - the WebSocket connection drops and the next reconnect also fails
//
// Read is intended to be called from a single goroutine per (doraUserID,
// apiKey) — i.e. one call per Manager subscription. Concurrent calls
// from different goroutines are safe but each call owns its own
// message channel.
func (s *Stream) Read(ctx context.Context, doraUserID, apiKey string) <-chan []byte {
	out := make(chan []byte, msgChanBuf)
	go s.runReadLoop(ctx, doraUserID, apiKey, out)
	return out
}

func (s *Stream) runReadLoop(ctx context.Context, doraUserID, apiKey string, out chan<- []byte) {
	defer close(out)
	backoff := initialBackoff
	for ctx.Err() == nil {
		msgs, authFail := s.dialOnce(ctx, doraUserID, apiKey)
		if authFail {
			s.log.Warn("DORA order-updates auth rejected",
				"dora_user_id", doraUserID,
				"err", ErrAuthRejected)
			return
		}
		if msgs == nil {
			if !s.sleep(ctx, backoff) {
				return
			}
			backoff = nextDelay(backoff, maxBackoff)
			continue
		}
		backoff = initialBackoff
		s.forwardUntilDone(ctx, doraUserID, msgs, out)
	}
}

// dialOnce opens a single WebSocket and returns the message channel.
// authFail=true short-circuits the backoff loop and exits.
func (s *Stream) dialOnce(ctx context.Context, doraUserID, apiKey string) (<-chan []byte, bool) {
	// DORA's WS auth is the x-api-key query param (per the dora-api
	// skill; verified end-to-end via wscat against dev.dora.co). The
	// Sec-WebSocket-Protocol subprotocol mechanism does NOT work here.
	u := s.wsURL(doraUserID, apiKey)
	conn, resp, err := websocket.Dial(ctx, u, nil)
	if err != nil {
		if resp != nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
				return nil, true
			}
		}
		s.log.Debug("DORA order-updates dial error",
			"dora_user_id", doraUserID, "err", err)
		return nil, false
	}
	// DORA order-update arrays can exceed coder/websocket's 32KiB
	// default per-message limit; SetReadLimit(-1) disables the cap
	// (per the library convention - 0 is treated as "1 byte").
	conn.SetReadLimit(-1)

	msgs := make(chan []byte, msgChanBuf)
	go func() {
		// The goroutine owns the connection lifetime. dialOnce cannot
		// defer close here: it would close the conn before the goroutine
		// got a chance to read, surfacing as "use of closed network
		// connection" in the read loop. (Bug fixed 2026-07.)
		defer conn.Close(websocket.StatusNormalClosure, "")
		defer close(msgs)
		for {
			typ, data, err := conn.Read(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					s.log.Debug("DORA order-updates read error",
						"dora_user_id", doraUserID, "err", err)
				}
				return
			}
			if typ != websocket.MessageText {
				continue
			}
			select {
			case msgs <- append([]byte(nil), data...):
			case <-ctx.Done():
				return
			}
		}
	}()
	return msgs, false
}

func (s *Stream) forwardUntilDone(ctx context.Context, doraUserID string, src <-chan []byte, dst chan<- []byte) {
	for {
		select {
		case <-ctx.Done():
			return
		case raw, ok := <-src:
			if !ok {
				return
			}
			s.extractUpdatedAtAndAdvance(doraUserID, raw)
			select {
			case dst <- raw:
			case <-ctx.Done():
				return
			}
		}
	}
}

// wsURL composes the DORA orders stream URL. The `since` cursor is
// appended when a per-user snapshot exists — keyed by doraUserID so
// one user's reconnect never carries another user's timestamp.
func (s *Stream) wsURL(doraUserID, apiKey string) string {
	base := strings.Replace(s.wsBaseURL, "https://", "wss://", 1)
	base = strings.Replace(base, "http://", "ws://", 1)
	u := base + "/v1/user/" + doraUserID + "/orders/all/updates/stream"
	v := url.Values{}
	v.Set("x-api-key", apiKey)
	q := v.Encode()
	if since := s.snapshotSince(doraUserID); since != nil {
		q += "&since=" + url.QueryEscape(since.UTC().Format(time.RFC3339Nano))
	}
	return u + "?" + q
}

// snapshotSince returns a copy of the current since pointer for the
// given doraUserID, or nil when no cursor has been recorded yet.
func (s *Stream) snapshotSince(doraUserID string) *time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.since[doraUserID]
	if p == nil {
		return nil
	}
	c := *p
	return &c
}

// extractUpdatedAtAndAdvance parses the JSON message (an array of
// entries with a top-level `updated_at` field) and updates the
// in-memory since cursor. Returns false on parse error; the message
// is still forwarded.
func (s *Stream) extractUpdatedAtAndAdvance(doraUserID string, raw []byte) bool {
	type entry struct {
		UpdatedAt time.Time `json:"updated_at"`
	}
	var entries []entry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return false
	}
	var max time.Time
	for _, e := range entries {
		if e.UpdatedAt.After(max) {
			max = e.UpdatedAt
		}
	}
	if max.IsZero() {
		return false
	}
	s.mu.Lock()
	s.since[doraUserID] = &max
	s.mu.Unlock()
	return true
}

func (s *Stream) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextDelay(current, max time.Duration) time.Duration {
	b := current * 2
	if b > max {
		b = max
	}
	var buf [1]byte
	if _, err := cryptorand.Read(buf[:]); err != nil {
		return b
	}
	delta := time.Duration(int(b) / jitterDivisor)
	offset := time.Duration(int(buf[0])) * delta / 256
	if buf[0]&1 == 0 {
		return b - offset
	}
	return b + offset
}
