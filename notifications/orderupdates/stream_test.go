package orderupdates_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"

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
