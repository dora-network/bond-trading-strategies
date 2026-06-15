package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dora-network/bond-trading-strategies/authctx"
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
