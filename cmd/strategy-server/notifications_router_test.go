package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dora-network/bond-trading-strategies/authctx"
	"github.com/dora-network/bond-trading-strategies/cors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type captured struct {
	hadAuthInfo bool
	gotAPIKey   string
}

func TestNotificationsRouter_HeaderTakesPrecedence(t *testing.T) {
	cap := &captured{}
	sub := http.NewServeMux()
	sub.HandleFunc("/v1/notifications/ws", func(w http.ResponseWriter, r *http.Request) {
		if info, ok := authctx.AuthInfoFromContext(r.Context()); ok {
			cap.hadAuthInfo = true
			cap.gotAPIKey = info.APIKey
		}
		w.WriteHeader(http.StatusOK)
	})
	r := notificationsRouter{fallback: http.NewServeMux(), sub: sub}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/notifications/ws?x-api-key=query-key", nil)
	req.Header.Set("Authorization", "ApiKey header-key")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, cap.hadAuthInfo)
	assert.Equal(t, "header-key", cap.gotAPIKey)
}

func TestNotificationsRouter_FallsBackToQueryParam(t *testing.T) {
	cap := &captured{}
	sub := http.NewServeMux()
	sub.HandleFunc("/v1/notifications/ws", func(w http.ResponseWriter, r *http.Request) {
		if info, ok := authctx.AuthInfoFromContext(r.Context()); ok {
			cap.hadAuthInfo = true
			cap.gotAPIKey = info.APIKey
		}
		w.WriteHeader(http.StatusOK)
	})
	r := notificationsRouter{fallback: http.NewServeMux(), sub: sub}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/notifications/ws?x-api-key=query-key", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, cap.hadAuthInfo)
	assert.Equal(t, "query-key", cap.gotAPIKey)
}

func TestNotificationsRouter_NoCredentialsPassesThrough(t *testing.T) {
	called := false
	sub := http.NewServeMux()
	sub.HandleFunc("/v1/notifications/ws", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := authctx.AuthInfoFromContext(r.Context()); ok {
			t.Errorf("expected no auth info in context")
		}
		called = true
		w.WriteHeader(http.StatusOK)
	})
	r := notificationsRouter{fallback: http.NewServeMux(), sub: sub}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/notifications/ws", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, called)
}

func TestNotificationsRouter_CORSPreflightSucceeds(t *testing.T) {
	// Verify that an OPTIONS preflight against /v1/notifications/ws
	// gets the CORS headers needed for a browser to open a WebSocket
	// from an allowed origin. The router is built with CORS applied
	// to the WS handler, mirroring the production wiring in main.go.
	sub := http.NewServeMux()
	wsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The preflight should never reach the real WS handler.
		t.Errorf("real handler called for OPTIONS: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	})
	corsWrap := cors.New("https://app.example.com")
	sub.Handle("/v1/notifications/ws", corsWrap(wsHandler))
	r := notificationsRouter{fallback: http.NewServeMux(), sub: sub}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodOptions, "/v1/notifications/ws", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "Authorization, Sec-WebSocket-Protocol")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusNoContent, rr.Code)
	assert.Equal(t, "https://app.example.com", rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Contains(t, rr.Header().Get("Vary"), "Origin")
	assert.Contains(t, rr.Header().Get("Access-Control-Allow-Methods"), "GET")
	assert.Contains(t, rr.Header().Get("Access-Control-Allow-Headers"), "Sec-WebSocket-Protocol")
	assert.Equal(t, "true", rr.Header().Get("Access-Control-Allow-Credentials"))
}

func TestNotificationsRouter_CORSHeadersOnUpgrade(t *testing.T) {
	// Verify that a real GET against /v1/notifications/ws carries
	// the CORS headers on the response (not just the preflight).
	// The handler itself will 401 because we don't wire real auth,
	// but the CORS headers must be present.
	sub := http.NewServeMux()
	wsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	corsWrap := cors.New("https://app.example.com")
	sub.Handle("/v1/notifications/ws", corsWrap(wsHandler))
	r := notificationsRouter{fallback: http.NewServeMux(), sub: sub}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/notifications/ws?x-api-key=test-key", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Authorization", "ApiKey test-key")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.Equal(t, "https://app.example.com", rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Contains(t, rr.Header().Get("Vary"), "Origin")
	assert.Equal(t, "true", rr.Header().Get("Access-Control-Allow-Credentials"))
}

func TestNotificationsRouter_CORSRejectsDisallowedOrigin(t *testing.T) {
	// A request from an origin not in the allow-list must NOT have
	// Access-Control-Allow-Origin set (the browser will block the
	// response). The server-side handler still runs.
	sub := http.NewServeMux()
	wsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	corsWrap := cors.New("https://app.example.com")
	sub.Handle("/v1/notifications/ws", corsWrap(wsHandler))
	r := notificationsRouter{fallback: http.NewServeMux(), sub: sub}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/notifications/ws", nil)
	req.Header.Set("Origin", "https://attacker.example.com")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.Empty(t, rr.Header().Get("Access-Control-Allow-Origin"))
}
