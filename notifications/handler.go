package notifications

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/coder/websocket"
)

// ResolveUserID validates the request and returns the DORA user ID the
// client is subscribing to. The implementation should be the same
// requireAuth path used by the REST handler; this interface is local to
// the package to keep the test hermetic.
type ResolveUserID func(ctx context.Context) (string, error)

// Handler serves GET /v1/notifications/ws.
type Handler struct {
	notifier    Notifier
	resolveUser ResolveUserID
	log         *slog.Logger
}

const replayLimit = 1000

func NewHandler(n Notifier, resolve ResolveUserID, opts ...HandlerOption) *Handler {
	h := &Handler{notifier: n, resolveUser: resolve, log: slog.Default()}
	for _, o := range opts {
		o(h)
	}
	return h
}

type HandlerOption func(*Handler)

func WithHandlerLogger(l *slog.Logger) HandlerOption { return func(h *Handler) { h.log = l } }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "ApiKey ") && !strings.HasPrefix(authHeader, "Bearer ") {
		http.Error(w, "missing or unsupported Authorization header", http.StatusUnauthorized)
		return
	}
	userID, err := h.resolveUser(r.Context())
	if err != nil {
		http.Error(w, "unauthorised", http.StatusUnauthorized)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{})
	if err != nil {
		h.log.Error("websocket accept failed", "err", err, "user_id", userID)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()
	sub, err := h.notifier.Subscribe(ctx, userID)
	if err != nil {
		h.log.Error("subscribe failed", "err", err, "user_id", userID)
		return
	}
	defer sub.Close()

	if last := r.URL.Query().Get("Last-Event-ID"); last != "" {
		if hist, ok := h.notifier.(replayProvider); ok {
			history, err := hist.Replay(ctx, userID, last, replayLimit)
			if err != nil {
				h.log.Warn("replay failed; starting at live tail", "err", err, "user_id", userID)
			} else {
				for _, evt := range history {
					if err := writeEvent(ctx, conn, evt); err != nil {
						return
					}
				}
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-sub.Events():
			if !ok {
				return
			}
			if !typeAllowed(r.URL.Query().Get("types"), evt.Type) {
				continue
			}
			if err := writeEvent(ctx, conn, evt); err != nil {
				return
			}
		}
	}
}

func writeEvent(ctx context.Context, conn *websocket.Conn, evt Event) error {
	payload, err := jsonMarshal(evt)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func typeAllowed(filter string, t EventType) bool {
	if filter == "" {
		return true
	}
	for _, want := range strings.Split(filter, ",") {
		if strings.TrimSpace(want) == string(t) {
			return true
		}
	}
	return false
}

// replayProvider is implemented by Bus so the handler can read history
// without depending on the concrete type. It is internal.
type replayProvider interface {
	Replay(ctx context.Context, userID, afterID string, limit int) ([]Event, error)
}
