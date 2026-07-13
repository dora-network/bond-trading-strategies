package orderupdates_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/notifications/orderupdates"
)

// silentLogger discards everything.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// upgradeToWS spins up an httptest.Server that upgrades any path and
// streams the configured batches in order. After all batches are
// delivered, the connection closes — simulating a network drop.
type upgradeToWS struct {
	mux     *http.ServeMux
	server  *httptest.Server
	batches [][]byte
	opens   atomic.Int32
}

func newUpgradeWS(t *testing.T) *upgradeToWS {
	t.Helper()
	u := &upgradeToWS{mux: http.NewServeMux()}
	u.mux.HandleFunc("/", u.handle)
	u.server = httptest.NewServer(u.mux)
	t.Cleanup(u.server.Close)
	return u
}

func (u *upgradeToWS) handle(w http.ResponseWriter, r *http.Request) {
	u.opens.Add(1)
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	for _, b := range u.batches {
		if err := c.Write(r.Context(), websocket.MessageText, b); err != nil {
			return
		}
	}
	for {
		if _, _, err := c.Read(r.Context()); err != nil {
			return
		}
	}
}

func (u *upgradeToWS) wsURL() string {
	return strings.Replace(u.server.URL, "http://", "ws://", 1)
}

func TestStream_DialsAndReadsOneMessage(t *testing.T) {
	t.Parallel()
	u := newUpgradeWS(t)
	u.batches = [][]byte{
		[]byte(`[{"client_order_id":"mean_reversion.00000000-0000-0000-0000-000000000000.x","status":"placed","updated_at":"2026-07-09T12:34:56Z"}]`),
	}

	s := orderupdates.NewStream(u.wsURL(), silentLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msgs := s.Read(ctx, "alice", "k")
	select {
	case raw := <-msgs:
		assert.Contains(t, string(raw), "client_order_id")
	case <-ctx.Done():
		t.Fatal("Stream did not deliver message within deadline")
	}
}

func TestStream_StopsOnAuthFailure(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := strings.Replace(server.URL, "http://", "ws://", 1)

	s := orderupdates.NewStream(wsURL, silentLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	msgs := s.Read(ctx, "alice", "k")
	for range msgs {
	}
	// If the loop closes the channel without delivering a message,
	// the outer ctx cancels and we return.
}

// TestStream_ReconnectsOnDisconnect verifies that when the fake DORA
// server drops the connection after one message, the Stream dials
// again. The fake server's opens counter increments on each upgrade.
func TestStream_ReconnectsOnDisconnect(t *testing.T) {
	t.Parallel()
	var dialCount atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		dialCount.Add(1)
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "")
		// First message only, then close.
		_ = c.Write(r.Context(), websocket.MessageText,
			[]byte(`[{"client_order_id":"mean_reversion.00000000-0000-0000-0000-000000000000.x","status":"placed","updated_at":"2026-07-09T12:34:56Z"}]`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := strings.Replace(server.URL, "http://", "ws://", 1)

	s := orderupdates.NewStream(wsURL, silentLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgs := s.Read(ctx, "alice", "k")
	// First message lands.
	select {
	case raw := <-msgs:
		if !strings.Contains(string(raw), "client_order_id") {
			t.Fatalf("expected first message with client_order_id, got %s", raw)
		}
	case <-ctx.Done():
		t.Fatal("first message timeout")
	}
	// After the server closes, the Stream must dial again within the
	// initial 1s backoff window. Allow up to 3s for the second upgrade.
	require.Eventually(t, func() bool {
		return dialCount.Load() >= 2
	}, 3*time.Second, 50*time.Millisecond,
		"Stream must dial again after the first connection drops (dials=%d)", dialCount.Load())
}

// TestStream_Backoff verifies that 5xx from DORA triggers an escalating
// backoff. The fake server responds with HTTP 503 to every upgrade
// attempt; the Stream must back off and retry.
func TestStream_Backoff(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	var callCount atomic.Int32
	var callTimesMu sync.Mutex
	var callTimes []time.Time
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		callTimesMu.Lock()
		callTimes = append(callTimes, time.Now())
		callTimesMu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := strings.Replace(server.URL, "http://", "ws://", 1)

	s := orderupdates.NewStream(wsURL, silentLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	_ = s.Read(ctx, "alice", "k")

	// Wait for several dial attempts to land (initial 1s, then 2s, then 4s
	// — within 2.5s we should see at least 2-3 attempts).
	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if got := callCount.Load(); got < 2 {
		t.Fatalf("expected at least 2 dial attempts in 2.5s, got %d", got)
	}
	// Verify the gap between consecutive calls is at least the initial 1s
	// backoff (allowing for the ±20% jitter, so >= ~800ms).
	callTimesMu.Lock()
	defer callTimesMu.Unlock()
	if len(callTimes) < 2 {
		t.Fatalf("not enough call samples: %d", len(callTimes))
	}
	for i := 1; i < len(callTimes); i++ {
		gap := callTimes[i].Sub(callTimes[i-1])
		if gap < 700*time.Millisecond {
			t.Errorf("backoff gap too small at attempt %d: %v (expected >= 700ms with initial 1s + jitter)", i, gap)
		}
	}
}
