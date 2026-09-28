package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestPrincipalMiddleware_InjectsIdentity(t *testing.T) {
	var got Principal
	var gotKey string
	h := PrincipalMiddleware(Principal{UserID: "u-1", TenantID: "t-1", Roles: []string{"TRADER"}}, "k-1")(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = PrincipalFromCtx(r.Context())
			gotKey = DoraAPIKeyFromCtx(r.Context())
		}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	if got.UserID != "u-1" || got.TenantID != "t-1" || len(got.Roles) != 1 || got.Roles[0] != "TRADER" {
		t.Errorf("principal = %+v", got)
	}
	if gotKey != "k-1" {
		t.Errorf("dora api key = %q, want k-1", gotKey)
	}
}

func TestPrincipalFromCtx_EmptyWhenAbsent(t *testing.T) {
	if p := PrincipalFromCtx(t.Context()); p.UserID != "" || p.TenantID != "" || p.Roles != nil {
		t.Errorf("zero principal expected, got %+v", p)
	}
	if k := DoraAPIKeyFromCtx(t.Context()); k != "" {
		t.Errorf("empty ctx: key = %q, want empty", k)
	}
}

func TestRequestIDMiddleware_AssignsAndForwards(t *testing.T) {
	var seen string
	h := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFromCtx(r.Context())
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	if seen == "" {
		t.Error("request ID missing from ctx")
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("X-Request-ID header missing")
	}
}

func TestRequestIDMiddleware_ForwardsCallerSuppliedID(t *testing.T) {
	h := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", "caller-1")
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got != "caller-1" {
		t.Errorf("X-Request-ID = %q, want caller-1", got)
	}
}

// injectPrincipal is the test stand-in for the host's auth mount: it
// puts a fixed principal on the ctx the way PrincipalMiddleware does,
// for rate-limit tests that drive handlers directly.
func injectPrincipal(userID string) func(http.Handler) http.Handler {
	return PrincipalMiddleware(Principal{UserID: userID}, "k")
}

func TestRateLimitMiddleware_AllowsUnderLimit(t *testing.T) {
	called := 0
	h := injectPrincipal("u-1")(
		RateLimitMiddleware(newRateLimiter(2, time.Minute))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called++ }),
		),
	)
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("req %d: want 200, got %d", i, rec.Code)
		}
	}
	if called != 2 {
		t.Errorf("handler calls: want 2, got %d", called)
	}
}

func TestRateLimitMiddleware_BlocksOverLimit(t *testing.T) {
	h := injectPrincipal("u-1")(
		RateLimitMiddleware(newRateLimiter(1, time.Hour))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		),
	)
	// First allowed.
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	// Second blocked.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	if rec2.Code != http.StatusTooManyRequests {
		t.Errorf("over-limit: want 429, got %d", rec2.Code)
	}
}

func TestRateLimitMiddleware_PerUserIndependent(t *testing.T) {
	h := injectPrincipal("u-1")(
		RateLimitMiddleware(newRateLimiter(1, time.Hour))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		),
	)
	// u-1 exhausts their 1 token.
	doReq := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
		return rec
	}
	_ = doReq() // allowed
	if rec := doReq(); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("u-1 second: want 429, got %d", rec.Code)
	}
	// A different principal (u-2) still has budget.
	h2 := injectPrincipal("u-2")(
		RateLimitMiddleware(newRateLimiter(1, time.Hour))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		),
	)
	rec := httptest.NewRecorder()
	h2.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("u-2 first: want 200, got %d", rec.Code)
	}
}

func TestRateLimitMiddleware_RetryAfterHeader(t *testing.T) {
	// 1 token, 1 hour refill — first request drains the bucket,
	// second must 429 with Retry-After ≈ 3600.
	h := injectPrincipal("u-rl")(
		RateLimitMiddleware(newRateLimiter(1, time.Hour))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		),
	)
	// First request: allowed, no Retry-After.
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	if rec1.Code != http.StatusOK {
		t.Fatalf("first: want 200, got %d", rec1.Code)
	}
	if got := rec1.Header().Get("Retry-After"); got != "" {
		t.Errorf("first: Retry-After should be absent on 200, got %q", got)
	}
	// Second request: 429 with Retry-After set.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second: want 429, got %d", rec2.Code)
	}
	got := rec2.Header().Get("Retry-After")
	if got == "" {
		t.Fatal("second: Retry-After header missing on 429")
	}
	secs, err := strconv.Atoi(got)
	if err != nil {
		t.Fatalf("Retry-After: must be integer seconds, got %q: %v", got, err)
	}
	// 1 hour refill — expect 3600s, allow a tiny drift down to 3599
	// if the test machine is slow.
	if secs < 3599 || secs > 3600 {
		t.Errorf("Retry-After: want 3599-3600, got %d", secs)
	}
}

func TestRateLimitMiddleware_RetryAfterSubSecondRoundsUp(t *testing.T) {
	// 0 tokens, sub-second refill — Retry-After should round up to
	// at least 1 second (the contract says integer seconds; 0
	// would invite a tight retry loop).
	rl := newRateLimiter(1, 100*time.Millisecond)
	// Drain the only token.
	if allowed, _ := rl.allow("u-sub"); !allowed {
		t.Fatal("setup: first allow should succeed")
	}
	// Now the bucket is empty.
	h := injectPrincipal("u-sub")(
		RateLimitMiddleware(rl)(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		),
	)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429, got %d", rec.Code)
	}
	got := rec.Header().Get("Retry-After")
	secs, err := strconv.Atoi(got)
	if err != nil {
		t.Fatalf("Retry-After: must be integer seconds, got %q: %v", got, err)
	}
	if secs < 1 {
		t.Errorf("Retry-After: want >= 1, got %d", secs)
	}
}

func TestRateLimitMiddleware_NoRetryAfterOnUnauthenticated(t *testing.T) {
	// When the principal is missing, the middleware still 429s but
	// does NOT emit a Retry-After — the upstream auth error is what
	// the client should retry on, not the rate limiter.
	h := RateLimitMiddleware(newRateLimiter(100, time.Hour))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
	)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429, got %d", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "" {
		t.Errorf("unauthenticated request: Retry-After should be absent, got %q", got)
	}
}
