package notifications_test

import (
	"context"
	"encoding/json"
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	//nolint:bodyclose // coder/websocket docs: caller never closes resp.Body
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	defer conn.Close(websocket.StatusNormalClosure, "")

	evt := notifications.Event{ID: uuid.NewString(), Type: notifications.EventRunStarted, UserID: "user-1", Timestamp: time.Now().UTC()}
	require.NoError(t, bus.Publish(ctx, evt))

	_, data, err := conn.Read(ctx)
	require.NoError(t, err)
	var got notifications.Event
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, evt.ID, got.ID)
}
