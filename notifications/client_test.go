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

func TestClient_DialReceivesEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "")
		evt := notifications.Event{ID: uuid.NewString(), Type: notifications.EventRunStarted, UserID: "u", Timestamp: time.Now().UTC()}
		data, _ := json.Marshal(evt)
		_ = c.Write(r.Context(), websocket.MessageText, data)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	wsURL := "ws" + strings.TrimPrefix(u.String(), "http")

	received := make(chan notifications.Event, 1)
	c := notifications.NewClient(wsURL, "ApiKey test", func(_ context.Context) (string, error) { return "u", nil },
		notifications.ClientOnEvent(func(_ context.Context, evt notifications.Event) error {
			received <- evt
			return nil
		}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, c.Run(ctx))
	select {
	case evt := <-received:
		assert.Equal(t, notifications.EventRunStarted, evt.Type)
	case <-ctx.Done():
		t.Fatal("did not receive event")
	}
}

var _ = (*notificationsfakes.FakeNotifier)(nil) // silence unused-import in tests
