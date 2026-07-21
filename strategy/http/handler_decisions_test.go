package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

func TestParseDecisionsDateFilter(t *testing.T) {
	t.Parallel()

	t.Run("RFC3339 accepted", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet,
			"/?from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z", nil)
		from, to, err := strategyhttp.ParseDecisionsDateFilter(r)
		require.NoError(t, err)
		require.NotNil(t, from)
		require.NotNil(t, to)
		assert.Equal(t, 2026, from.Year())
		assert.Equal(t, 2026, to.Year())
	})

	t.Run("YYYY-MM-DD accepted", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/?from=2026-01-01", nil)
		from, _, err := strategyhttp.ParseDecisionsDateFilter(r)
		require.NoError(t, err)
		require.NotNil(t, from)
	})

	t.Run("missing is nil with no error", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		from, to, err := strategyhttp.ParseDecisionsDateFilter(r)
		require.NoError(t, err)
		assert.Nil(t, from)
		assert.Nil(t, to)
	})

	t.Run("garbage returns error", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/?from=yesterday", nil)
		_, _, err := strategyhttp.ParseDecisionsDateFilter(r)
		assert.Error(t, err)
	})
}

func TestParseDecisionCursor(t *testing.T) {
	t.Parallel()

	t.Run("empty returns nil cursor with no error", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		c, err := strategyhttp.ParseDecisionCursor(r)
		require.NoError(t, err)
		assert.Nil(t, c)
	})

	t.Run("valid cursor is decoded", func(t *testing.T) {
		t.Parallel()
		encoded := (&strategyhttp.Cursor{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Seq: 7, Version: 1}).Encode()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/?cursor="+encoded, nil)
		c, err := strategyhttp.ParseDecisionCursor(r)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Equal(t, int64(7), c.Seq)
	})

	t.Run("invalid cursor returns error", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/?cursor=not-base64", nil)
		_, err := strategyhttp.ParseDecisionCursor(r)
		assert.Error(t, err)
	})
}

func TestParseDecisionLimit(t *testing.T) {
	t.Parallel()

	t.Run("default when missing", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		assert.Equal(t, 50, strategyhttp.ParseDecisionLimit(r))
	})

	t.Run("garbage keeps the default", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/?limit=abc", nil)
		assert.Equal(t, 50, strategyhttp.ParseDecisionLimit(r))
	})

	t.Run("non-positive keeps the default", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/?limit=0", nil)
		assert.Equal(t, 50, strategyhttp.ParseDecisionLimit(r))
	})

	t.Run("above max is clamped", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/?limit=9999", nil)
		assert.Equal(t, 200, strategyhttp.ParseDecisionLimit(r))
	})

	t.Run("in-range is honored", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/?limit=75", nil)
		assert.Equal(t, 75, strategyhttp.ParseDecisionLimit(r))
	})
}
