package ratelimit_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/ratelimit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMiddlewareExemptsHealthz(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled: true,
		Global:  ratelimit.TierConfig{RPS: 0, Burst: 0}, // zero = every request blocked
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil)
	l.Middleware(next).ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestMiddlewareBlocksWhenGlobalLimitExceeded(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled: true,
		Global:  ratelimit.TierConfig{RPS: 1, Burst: 1},
		IP:      ratelimit.TierConfig{RPS: 100, Burst: 100},
		Read:    ratelimit.TierConfig{RPS: 100, Burst: 100},
		Write:   ratelimit.TierConfig{RPS: 100, Burst: 100},
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// First request succeeds (burst = 1).
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	l.Middleware(next).ServeHTTP(rec1, req1)
	require.Equal(t, http.StatusOK, rec1.Code)

	// Second request is blocked.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	l.Middleware(next).ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusTooManyRequests, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "rate limit exceeded")
	assert.Equal(t, "1", rec2.Header().Get("Retry-After"))
	assert.Equal(t, "0", rec2.Header().Get("RateLimit-Remaining"))
}

func TestMiddlewareBlocksWhenIPLimitExceeded(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled: true,
		Global:  ratelimit.TierConfig{RPS: 100, Burst: 100},
		IP:      ratelimit.TierConfig{RPS: 1, Burst: 1},
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// First request from this IP succeeds.
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	l.Middleware(next).ServeHTTP(rec1, req1)
	require.Equal(t, http.StatusOK, rec1.Code)

	// Second request from same IP is blocked.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	l.Middleware(next).ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusTooManyRequests, rec2.Code)
}

func TestMiddlewareBlocksWhenUserLimitExceeded(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled: true,
		Global:  ratelimit.TierConfig{RPS: 100, Burst: 100},
		IP:      ratelimit.TierConfig{RPS: 100, Burst: 100},
		Read:    ratelimit.TierConfig{RPS: 1, Burst: 1},
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// First request for this user succeeds.
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req1.Header.Set("Authorization", "ApiKey user-a")
	l.Middleware(next).ServeHTTP(rec1, req1)
	require.Equal(t, http.StatusOK, rec1.Code)

	// Second request for same user is blocked.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req2.Header.Set("Authorization", "ApiKey user-a")
	l.Middleware(next).ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusTooManyRequests, rec2.Code)
}

func TestMiddlewareIsolatesUsers(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled: true,
		Global:  ratelimit.TierConfig{RPS: 100, Burst: 100},
		IP:      ratelimit.TierConfig{RPS: 100, Burst: 100},
		Read:    ratelimit.TierConfig{RPS: 1, Burst: 1},
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Exhaust user A's bucket.
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req.Header.Set("Authorization", "ApiKey user-a")
	l.Middleware(next).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// User A is now blocked.
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req.Header.Set("Authorization", "ApiKey user-a")
	l.Middleware(next).ServeHTTP(rec, req)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)

	// User B is unaffected.
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req.Header.Set("Authorization", "ApiKey user-b")
	l.Middleware(next).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestMiddlewareClassifiesTiers(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled: true,
		Global:  ratelimit.TierConfig{RPS: 100, Burst: 100},
		IP:      ratelimit.TierConfig{RPS: 100, Burst: 100},
		Read:    ratelimit.TierConfig{RPS: 100, Burst: 100},
		Write:   ratelimit.TierConfig{RPS: 1, Burst: 1},
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// POST (write tier) first request succeeds.
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/backtests", nil)
	req1.Header.Set("Authorization", "ApiKey user-a")
	l.Middleware(next).ServeHTTP(rec1, req1)
	require.Equal(t, http.StatusOK, rec1.Code)

	// POST second request blocked (write tier exhausted).
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/backtests", nil)
	req2.Header.Set("Authorization", "ApiKey user-a")
	l.Middleware(next).ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusTooManyRequests, rec2.Code)

	// GET (read tier) still succeeds because read bucket is separate.
	rec3 := httptest.NewRecorder()
	req3 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests", nil)
	req3.Header.Set("Authorization", "ApiKey user-a")
	l.Middleware(next).ServeHTTP(rec3, req3)
	require.Equal(t, http.StatusOK, rec3.Code)
}

func TestMiddlewareBurstCapacity(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled: true,
		Global:  ratelimit.TierConfig{RPS: 100, Burst: 100},
		IP:      ratelimit.TierConfig{RPS: 100, Burst: 100},
		Read:    ratelimit.TierConfig{RPS: 1, Burst: 3},
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Burst of 3 allows 3 immediate requests.
	for i := range 3 {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
		req.Header.Set("Authorization", "ApiKey user-burst")
		l.Middleware(next).ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "request %d should succeed", i+1)
	}

	// Fourth request is blocked.
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req.Header.Set("Authorization", "ApiKey user-burst")
	l.Middleware(next).ServeHTTP(rec, req)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
}

func TestExtractIPDirect(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled:    true,
		TrustProxy: false,
		Global:     ratelimit.TierConfig{RPS: 100, Burst: 100},
		IP:         ratelimit.TierConfig{RPS: 1, Burst: 1},
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Request with X-Forwarded-For but TrustProxy=false should use RemoteAddr.
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req1.RemoteAddr = "1.2.3.4:1234"
	req1.Header.Set("X-Forwarded-For", "9.9.9.9")
	l.Middleware(next).ServeHTTP(rec1, req1)
	require.Equal(t, http.StatusOK, rec1.Code)

	// Same RemoteAddr, different X-Forwarded-For -> same IP bucket, blocked.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req2.RemoteAddr = "1.2.3.4:1234"
	req2.Header.Set("X-Forwarded-For", "8.8.8.8")
	l.Middleware(next).ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusTooManyRequests, rec2.Code)
}

func TestExtractIPTrustProxy(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled:    true,
		TrustProxy: true,
		Global:     ratelimit.TierConfig{RPS: 100, Burst: 100},
		IP:         ratelimit.TierConfig{RPS: 1, Burst: 1},
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// First request with X-Forwarded-For=9.9.9.9 succeeds.
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req1.RemoteAddr = "1.2.3.4:1234"
	req1.Header.Set("X-Forwarded-For", "9.9.9.9")
	l.Middleware(next).ServeHTTP(rec1, req1)
	require.Equal(t, http.StatusOK, rec1.Code)

	// Same X-Forwarded-For -> same IP bucket, blocked.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req2.RemoteAddr = "5.6.7.8:5678"
	req2.Header.Set("X-Forwarded-For", "9.9.9.9")
	l.Middleware(next).ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusTooManyRequests, rec2.Code)
}

func TestExtractIPTrustProxyList(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled:    true,
		TrustProxy: true,
		Global:     ratelimit.TierConfig{RPS: 100, Burst: 100},
		IP:         ratelimit.TierConfig{RPS: 1, Burst: 1},
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// X-Forwarded-For with comma-separated list uses first entry.
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req1.RemoteAddr = "1.2.3.4:1234"
	req1.Header.Set("X-Forwarded-For", "7.7.7.7, 9.9.9.9, 10.10.10.10")
	l.Middleware(next).ServeHTTP(rec1, req1)
	require.Equal(t, http.StatusOK, rec1.Code)

	// Same first IP -> blocked.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req2.RemoteAddr = "5.6.7.8:5678"
	req2.Header.Set("X-Forwarded-For", "7.7.7.7, 1.1.1.1")
	l.Middleware(next).ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusTooManyRequests, rec2.Code)
}

func TestEvictionRemovesStaleBuckets(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled: true,
		Global:  ratelimit.TierConfig{RPS: 100, Burst: 100},
		IP:      ratelimit.TierConfig{RPS: 1, Burst: 1},
		Read:    ratelimit.TierConfig{RPS: 100, Burst: 100},
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Create some buckets.
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req1.RemoteAddr = "1.2.3.4:1234"
	req1.Header.Set("Authorization", "ApiKey eviction-user")
	l.Middleware(next).ServeHTTP(rec1, req1)
	require.Equal(t, http.StatusOK, rec1.Code)

	require.Equal(t, 1, l.IPBucketCount())
	require.Equal(t, 1, l.UserBucketCount())

	// Trigger eviction — buckets are fresh so they should remain.
	l.Evict()
	require.Equal(t, 1, l.IPBucketCount())
	require.Equal(t, 1, l.UserBucketCount())
}

func TestMiddlewareDisabled(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled: false,
		Global:  ratelimit.TierConfig{RPS: 0, Burst: 0},
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Even with zero limits, disabled config passes through.
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	l.Middleware(next).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestMiddlewareRefillsOverTime(t *testing.T) {
	t.Parallel()

	cfg := ratelimit.Config{
		Enabled: true,
		Global:  ratelimit.TierConfig{RPS: 100, Burst: 100},
		IP:      ratelimit.TierConfig{RPS: 100, Burst: 100},
		Read:    ratelimit.TierConfig{RPS: 10, Burst: 1},
	}
	l := ratelimit.NewLimiter(cfg, nil)
	defer l.Stop()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Exhaust the burst.
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req.Header.Set("Authorization", "ApiKey refill-user")
	l.Middleware(next).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Immediately blocked.
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req.Header.Set("Authorization", "ApiKey refill-user")
	l.Middleware(next).ServeHTTP(rec, req)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)

	// Wait for token to refill (RPS=10 means 100ms per token).
	time.Sleep(150 * time.Millisecond)

	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	req.Header.Set("Authorization", "ApiKey refill-user")
	l.Middleware(next).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}
