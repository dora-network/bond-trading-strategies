package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthCheckerStatus(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	t.Run("healthy during startup grace", func(t *testing.T) {
		checker := newHealthChecker(now, 10*time.Second, 15*time.Second, false, false, 0, nil)

		status := checker.status(context.Background(), now.Add(5*time.Second))
		assert.True(t, status.OK)
		assert.True(t, status.Grace)
		assert.Empty(t, status.Reasons)
	})

	t.Run("unhealthy after grace without price activity", func(t *testing.T) {
		checker := newHealthChecker(now, 10*time.Second, 15*time.Second, false, false, 0, nil)

		status := checker.status(context.Background(), now.Add(11*time.Second))
		assert.False(t, status.OK)
		assert.Contains(t, status.Reasons, "price stream is stale")
		assert.Contains(t, status.Reasons, "price writes are stale")
	})

	t.Run("healthy with fresh price activity", func(t *testing.T) {
		checker := newHealthChecker(now, 10*time.Second, 15*time.Second, false, false, 0, nil)
		checkTime := now.Add(20 * time.Second)
		checker.markPriceStream(checkTime.Add(-5 * time.Second))
		checker.markPriceWrite(checkTime.Add(-3 * time.Second))

		status := checker.status(context.Background(), checkTime)
		assert.True(t, status.OK)
		assert.False(t, status.Grace)
	})

	t.Run("candles required when enabled", func(t *testing.T) {
		checker := newHealthChecker(now, 10*time.Second, 15*time.Second, true, false, 0, nil)
		checkTime := now.Add(20 * time.Second)
		checker.markPriceStream(checkTime.Add(-5 * time.Second))
		checker.markPriceWrite(checkTime.Add(-5 * time.Second))

		status := checker.status(context.Background(), checkTime)
		assert.False(t, status.OK)
		assert.Contains(t, status.Reasons, "candle stream is stale")
		assert.Contains(t, status.Reasons, "candle writes are stale")
	})

	t.Run("healthy with fresh candle activity", func(t *testing.T) {
		checker := newHealthChecker(now, 10*time.Second, 15*time.Second, true, false, 0, nil)
		checkTime := now.Add(20 * time.Second)
		checker.markPriceStream(checkTime.Add(-5 * time.Second))
		checker.markPriceWrite(checkTime.Add(-4 * time.Second))
		checker.markCandleStream(checkTime.Add(-6 * time.Second))
		checker.markCandleWrite(checkTime.Add(-2 * time.Second))

		status := checker.status(context.Background(), checkTime)
		assert.True(t, status.OK)
	})

	t.Run("db ping failure is unhealthy during grace", func(t *testing.T) {
		checker := newHealthChecker(now, 10*time.Second, 15*time.Second, false, true, time.Second, func(ctx context.Context) error {
			return errors.New("ping failed")
		})

		status := checker.status(context.Background(), now.Add(5*time.Second))
		assert.False(t, status.OK)
		assert.Contains(t, status.Reasons, "database ping failed")
	})

	t.Run("db ping failure is unhealthy after grace", func(t *testing.T) {
		checker := newHealthChecker(now, 10*time.Second, 15*time.Second, false, true, time.Second, func(ctx context.Context) error {
			return errors.New("ping failed")
		})
		checkTime := now.Add(20 * time.Second)
		checker.markPriceStream(checkTime.Add(-2 * time.Second))
		checker.markPriceWrite(checkTime.Add(-2 * time.Second))

		status := checker.status(context.Background(), checkTime)
		assert.False(t, status.OK)
		assert.Contains(t, status.Reasons, "database ping failed")
	})
}

func TestHealthHandler(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	checker := newHealthChecker(now, 10*time.Second, 15*time.Second, false, false, 0, nil)
	checkTime := now.Add(20 * time.Second)
	checker.markPriceStream(checkTime.Add(-5 * time.Second))
	checker.markPriceWrite(checkTime.Add(-5 * time.Second))

	handler := newHealthHandler(checker, func() time.Time {
		return checkTime
	})

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var status healthStatus
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &status))
	assert.True(t, status.OK)
	assert.Equal(t, "ok", status.Status)
}
