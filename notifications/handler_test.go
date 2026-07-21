package notifications_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/dora-network/bond-trading-strategies/notifications/notificationsfakes"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_RejectsMissingAuth(t *testing.T) {
	bus := notifications.NewBus(&captureLog{}, notifications.NewHub())
	h := notifications.NewHandler(bus, func(_ context.Context) (string, error) {
		return "user-1", nil
	})
	srv := httptest.NewServer(h)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	wsURL := "ws" + strings.TrimPrefix(u.String(), "http") + "/v1/notifications/ws"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	//nolint:bodyclose // coder/websocket docs: caller never closes resp.Body
	_, _, err := websocket.Dial(ctx, wsURL, nil)
	require.Error(t, err)
}

// publishAndRead publishes evt on the bus and reads it from conn,
// tolerating the upgrade-vs-subscribe race: websocket.Dial returns
// after the server's Accept completes, but the handler's Subscribe
// runs immediately afterwards on the same goroutine. If we publish
// in that window the broadcast fires before our subscriber exists
// and the event is dropped. Retry the publish with a short read
// timeout until either the read succeeds or the overall context
// times out. Deterministic — no sleeps, no protocol changes.
func publishAndRead(t *testing.T, ctx context.Context, bus *notifications.Bus, conn *websocket.Conn, evt notifications.Event) notifications.Event {
	t.Helper()
	deadline, ok := ctx.Deadline()
	require.True(t, ok, "publishAndRead requires a deadline-bearing context")
	for {
		require.NoError(t, bus.Publish(ctx, evt))
		readCtx, cancel := context.WithDeadline(ctx, minTime(deadline, time.Now().Add(500*time.Millisecond)))
		_, data, err := conn.Read(readCtx)
		cancel()
		if err == nil {
			var got notifications.Event
			require.NoError(t, json.Unmarshal(data, &got))
			return got
		}
		// Distinguish a real failure from a windowed timeout. The
		// race manifests as context.DeadlineExceeded on the read
		// ctx (subscribed too late); re-publish until the parent
		// ctx also expires, which the next require.NoError catches.
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("conn.Read: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("publishAndRead: read never succeeded before parent deadline: %v", err)
		}
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func TestHandler_DeliversLiveEvents(t *testing.T) {
	bus := notifications.NewBus(&captureLog{}, notifications.NewHub())
	_ = (*notificationsfakes.FakeNotifier)(nil)
	h := notifications.NewHandler(bus, func(_ context.Context) (string, error) {
		return "user-1", nil
	})
	srv := httptest.NewServer(h)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	wsURL := "ws" + strings.TrimPrefix(u.String(), "http") + "/v1/notifications/ws"

	header := http.Header{}
	header.Set("Authorization", "ApiKey test-key")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//nolint:bodyclose // coder/websocket docs: caller never closes resp.Body
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	defer conn.Close(websocket.StatusNormalClosure, "")

	evt := notifications.Event{ID: uuid.NewString(), Type: notifications.EventRunStarted, UserID: "user-1", Timestamp: time.Now().UTC()}
	got := publishAndRead(t, ctx, bus, conn, evt)
	assert.Equal(t, evt.ID, got.ID)
}

func TestHandler_FiltersByTypes(t *testing.T) {
	bus := notifications.NewBus(&captureLog{}, notifications.NewHub())
	h := notifications.NewHandler(bus, func(_ context.Context) (string, error) {
		return "user-1", nil
	})
	srv := httptest.NewServer(h)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	wsURL := "ws" + strings.TrimPrefix(u.String(), "http") + "/v1/notifications/ws?types=run.started"

	header := http.Header{}
	header.Set("Authorization", "ApiKey test-key")
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	//nolint:bodyclose // coder/websocket docs: caller never closes resp.Body
	conn, _, err := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	defer conn.Close(websocket.StatusNormalClosure, "")
	// Use a long-lived background context for the live session so that
	// short read timeouts do not cancel the server's request context.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filtered := notifications.Event{
		ID:        uuid.NewString(),
		Type:      notifications.EventBacktestStarted,
		UserID:    "user-1",
		Timestamp: time.Now().UTC(),
	}
	require.NoError(t, bus.Publish(ctx, filtered))

	filteredCh := make(chan struct{}, 1)
	go func() {
		_, _, _ = conn.Read(ctx)
		filteredCh <- struct{}{}
	}()
	select {
	case <-filteredCh:
		t.Fatal("client received filtered event")
	case <-time.After(200 * time.Millisecond):
	}

	allowed := notifications.Event{
		ID:        uuid.NewString(),
		Type:      notifications.EventRunStarted,
		UserID:    "user-1",
		Timestamp: time.Now().UTC(),
	}
	require.NoError(t, bus.Publish(ctx, allowed))

	select {
	case <-filteredCh:
	case <-time.After(2 * time.Second):
		t.Fatal("client did not receive allowed event")
	}
}

func TestHandler_AcceptsConfiguredOriginPattern(t *testing.T) {
	bus := notifications.NewBus(&captureLog{}, notifications.NewHub())
	h := notifications.NewHandler(
		bus,
		func(_ context.Context) (string, error) { return "user-1", nil },
		notifications.WithAcceptOptions(websocket.AcceptOptions{
			OriginPatterns: []string{"app.example.com"},
		}),
	)
	srv := httptest.NewServer(h)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	wsURL := "ws" + strings.TrimPrefix(u.String(), "http") + "/v1/notifications/ws"

	header := http.Header{}
	header.Set("Authorization", "ApiKey test-key")
	header.Set("Origin", "https://app.example.com")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//nolint:bodyclose // coder/websocket docs: caller never closes resp.Body
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	defer conn.Close(websocket.StatusNormalClosure, "")

	evt := notifications.Event{ID: uuid.NewString(), Type: notifications.EventRunStarted, UserID: "user-1", Timestamp: time.Now().UTC()}
	got := publishAndRead(t, ctx, bus, conn, evt)
	assert.Equal(t, evt.ID, got.ID)
}

func TestHandler_AcceptsInsecureSkipVerify(t *testing.T) {
	bus := notifications.NewBus(&captureLog{}, notifications.NewHub())
	h := notifications.NewHandler(
		bus,
		func(_ context.Context) (string, error) { return "user-1", nil },
		notifications.WithAcceptOptions(websocket.AcceptOptions{
			InsecureSkipVerify: true,
		}),
	)
	srv := httptest.NewServer(h)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	wsURL := "ws" + strings.TrimPrefix(u.String(), "http") + "/v1/notifications/ws"

	header := http.Header{}
	header.Set("Authorization", "ApiKey test-key")
	header.Set("Origin", "https://anywhere.example.com")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//nolint:bodyclose // coder/websocket docs: caller never closes resp.Body
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	defer conn.Close(websocket.StatusNormalClosure, "")
}
