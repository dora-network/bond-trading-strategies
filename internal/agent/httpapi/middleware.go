package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

type contextKey string

const (
	principalKey  contextKey = "principal"
	requestIDKey  contextKey = "requestID"
	doraAPIKeyKey contextKey = "doraAPIKey"
)

// Principal is the authenticated caller's identity. The host's
// requireAuth mount chain (cmd/strategy-server) resolves the caller and
// injects the principal via PrincipalMiddleware; this package performs
// no authentication of its own.
type Principal struct {
	UserID   string
	TenantID string
	Roles    []string
}

// PrincipalFromCtx extracts the authenticated principal (set by the
// host's auth mount chain via PrincipalMiddleware).
func PrincipalFromCtx(ctx context.Context) Principal {
	if p, ok := ctx.Value(principalKey).(Principal); ok {
		return p
	}
	return Principal{}
}

// DoraAPIKeyFromCtx extracts the raw Dora API key (set alongside the
// principal) the per-turn Dora read tools bind. Returns "" when no key
// is present (tests).
func DoraAPIKeyFromCtx(ctx context.Context) string {
	if k, ok := ctx.Value(doraAPIKeyKey).(string); ok {
		return k
	}
	return ""
}

// RequestIDFromCtx extracts the request ID (set by RequestIDMiddleware).
func RequestIDFromCtx(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}

// WithPrincipal places p (and apiKey) on ctx. The host's mount adapter
// calls this after its own auth succeeds; PrincipalMiddleware is the
// HTTP-level convenience over it.
func WithPrincipal(ctx context.Context, p Principal, apiKey string) context.Context {
	ctx = context.WithValue(ctx, principalKey, p)
	return context.WithValue(ctx, doraAPIKeyKey, apiKey)
}

// PrincipalMiddleware injects a fixed Principal + Dora API key into the
// request context. The host's mount chain (L8 wiring) and the
// end-to-end tests use it; the httpapi package itself does not
// authenticate.
func PrincipalMiddleware(p Principal, apiKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), p, apiKey)))
		})
	}
}

// RequestIDMiddleware assigns a request ID (v7 — chronological for log
// correlation), sets X-Request-ID, and logs the line.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.Must(uuid.NewV7()).String()
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		start := time.Now()
		lw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(lw, r.WithContext(ctx))
		slog.Info("request",
			"method", r.Method, "path", r.URL.Path,
			"status", lw.status, "duration", time.Since(start), "request_id", id)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Flush passes through to the underlying ResponseWriter. The wrapped writer
// only forwards methods it overrides explicitly, so http.Flusher must be
// re-exposed here for SSE handlers to flush incrementally through the
// RequestIDMiddleware chain.
func (s *statusWriter) Flush() {
	if fl, ok := s.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

// rateLimiter is a per-user token bucket. One token per refill up to capacity.
// ponytail: in-memory, single-instance only — a shared store (Redis) ships
// with HA (spec §9).
type rateLimiter struct {
	mu       sync.Mutex
	users    map[string]*bucket
	capacity int
	refill   time.Duration
}

type bucket struct {
	tokens   int
	lastFill time.Time
}

func newRateLimiter(capacity int, refillPerToken time.Duration) *rateLimiter {
	return &rateLimiter{
		users:    make(map[string]*bucket),
		capacity: capacity,
		refill:   refillPerToken,
	}
}

func (rl *rateLimiter) allow(userID string) (allowed bool, retryAfter time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	b, ok := rl.users[userID]
	if !ok {
		b = &bucket{tokens: rl.capacity, lastFill: now}
		rl.users[userID] = b
	}
	// Refill: add one token per `refill` elapsed, capped at capacity.
	elapsed := now.Sub(b.lastFill)
	if tokens := int(elapsed / rl.refill); tokens > 0 {
		b.tokens += tokens
		if b.tokens > rl.capacity {
			b.tokens = rl.capacity
		}
		b.lastFill = b.lastFill.Add(time.Duration(tokens) * rl.refill)
	}
	if b.tokens <= 0 {
		// Empty bucket: wait one full refill interval for the next
		// token. lastFill already reflects the most recent refill
		// pass, so the next token is due at lastFill + refill.
		next := b.lastFill.Add(rl.refill)
		return false, next.Sub(now)
	}
	b.tokens--
	return true, 0
}

// RateLimitMiddleware returns 429 when the authenticated user has exhausted
// their token bucket. Must be chained inside the auth mount chain.
//
// On 429, sets the Retry-After header (in seconds, rounded up, RFC 9110)
// so clients can back off correctly instead of polling. The header is
// omitted when the rejection is due to an unauthenticated request
// (UserID empty): the upstream auth layer should have already produced
// a 401 in that case, but if we somehow get here, retry-hinting is
// misleading.
func RateLimitMiddleware(rl *rateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p := PrincipalFromCtx(r.Context())
			if p.UserID == "" {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			allowed, retryAfter := rl.allow(p.UserID)
			if !allowed {
				// Round up so a sub-second wait becomes 1s rather
				// than 0; a 0 Retry-After would invite a tight
				// retry loop.
				secs := int64(retryAfter / time.Second)
				if retryAfter%time.Second > 0 {
					secs++
				}
				if secs < 1 {
					secs = 1
				}
				w.Header().Set("Retry-After", strconv.FormatInt(secs, 10))
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
