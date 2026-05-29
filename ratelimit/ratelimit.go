// Package ratelimit provides token-bucket HTTP rate limiting middleware.
// It supports three scope layers (global, per-IP, per-user) and two
// request tiers (read, write).  The /healthz endpoint is exempt.
package ratelimit

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	evictionInterval = 5 * time.Minute
	healthzPath      = "/healthz"
)

// Tier distinguishes cheap read operations from expensive writes.
type Tier int

const (
	TierRead Tier = iota
	TierWrite
)

// Config holds rate-limit parameters for each scope and tier.
type Config struct {
	Enabled    bool
	TrustProxy bool
	Read       TierConfig
	Write      TierConfig
	Global     TierConfig
	IP         TierConfig
}

// TierConfig is a single token-bucket configuration.
type TierConfig struct {
	RPS   float64
	Burst int
}

// Limiter is an HTTP middleware that enforces rate limits.
type Limiter struct {
	config Config
	log    *slog.Logger

	global *rate.Limiter

	ipMu    sync.RWMutex
	ipCache map[string]*bucketEntry

	userMu    sync.RWMutex
	userCache map[string]*bucketEntry

	stopEviction chan struct{}
}

// bucketEntry wraps a rate.Limiter with a last-used timestamp.
type bucketEntry struct {
	limiter  *rate.Limiter
	lastUsed time.Time
}

// NewLimiter builds a Limiter from cfg.  It starts a background
// goroutine that evicts stale buckets every five minutes.
func NewLimiter(cfg Config, log *slog.Logger) *Limiter {
	if log == nil {
		log = slog.Default()
	}

	l := &Limiter{
		config:       cfg,
		log:          log,
		global:       newBucket(cfg.Global),
		ipCache:      make(map[string]*bucketEntry),
		userCache:    make(map[string]*bucketEntry),
		stopEviction: make(chan struct{}),
	}

	go l.evictionLoop()

	return l
}

// Stop halts the background eviction goroutine.  Call on shutdown.
func (l *Limiter) Stop() {
	close(l.stopEviction)
}

// Middleware returns an http.Handler that enforces rate limits.
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !l.config.Enabled || r.URL.Path == healthzPath {
			next.ServeHTTP(w, r)
			return
		}

		tier := tierFromMethod(r.Method)

		// 1. Global
		if !l.global.Allow() {
			l.writeRateLimit(w, tier, "global")
			return
		}

		// 2. Per-IP
		ip := l.extractIP(r)
		if ip != "" {
			bkt := l.ipBucket(ip)
			if !bkt.Allow() {
				l.writeRateLimit(w, tier, "ip")
				return
			}
		}

		// 3. Per-user (keyed by hashed Authorization header so we
		//    rate-limit before running expensive auth).
		auth := r.Header.Get("Authorization")
		if auth != "" {
			userKey := hashKey(auth)
			bkt := l.userBucket(userKey, tier)
			if !bkt.Allow() {
				l.writeRateLimit(w, tier, "user")
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

// ipBucket returns the rate limiter for the given IP address.
func (l *Limiter) ipBucket(ip string) *rate.Limiter {
	l.ipMu.RLock()
	ent, ok := l.ipCache[ip]
	l.ipMu.RUnlock()
	if ok {
		ent.touch()
		return ent.limiter
	}

	l.ipMu.Lock()
	defer l.ipMu.Unlock()
	if ent, ok := l.ipCache[ip]; ok {
		ent.touch()
		return ent.limiter
	}

	ent = &bucketEntry{
		limiter:  newBucket(l.config.IP),
		lastUsed: time.Now(),
	}
	l.ipCache[ip] = ent
	return ent.limiter
}

// userBucket returns the rate limiter for the given user key and tier.
func (l *Limiter) userBucket(key string, tier Tier) *rate.Limiter {
	// Include tier in the cache key so reads and writes get separate buckets.
	cacheKey := key + ":" + tier.String()

	l.userMu.RLock()
	ent, ok := l.userCache[cacheKey]
	l.userMu.RUnlock()
	if ok {
		ent.touch()
		return ent.limiter
	}

	l.userMu.Lock()
	defer l.userMu.Unlock()
	if ent, ok := l.userCache[cacheKey]; ok {
		ent.touch()
		return ent.limiter
	}

	cfg := l.config.Read
	if tier == TierWrite {
		cfg = l.config.Write
	}

	ent = &bucketEntry{
		limiter:  newBucket(cfg),
		lastUsed: time.Now(),
	}
	l.userCache[cacheKey] = ent
	return ent.limiter
}

func (l *Limiter) writeRateLimit(w http.ResponseWriter, tier Tier, scope string) {
	cfg := l.config.Read
	if tier == TierWrite {
		cfg = l.config.Write
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("RateLimit-Limit", fmt.Sprintf("%d", cfg.Burst))
	w.Header().Set("RateLimit-Remaining", "0")
	w.Header().Set("RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(time.Second).Unix()))
	w.Header().Set("Retry-After", "1")
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = fmt.Fprintf(w, `{"error":"rate limit exceeded","retry_after":1}`+"\n")

	l.log.Debug("rate limit exceeded",
		slog.String("scope", scope),
		slog.String("tier", tier.String()),
	)
}

func (l *Limiter) extractIP(r *http.Request) string {
	if l.config.TrustProxy {
		fwd := r.Header.Get("X-Forwarded-For")
		if fwd != "" {
			// X-Forwarded-For can be a comma-separated list; use the first (closest proxy).
			parts := strings.Split(fwd, ",")
			ip := strings.TrimSpace(parts[0])
			if ip != "" {
				return ip
			}
		}
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (l *Limiter) evictionLoop() {
	ticker := time.NewTicker(evictionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.evict()
		case <-l.stopEviction:
			return
		}
	}
}

func (l *Limiter) evict() {
	cutoff := time.Now().Add(-evictionInterval)

	l.ipMu.Lock()
	for k, v := range l.ipCache {
		if v.lastUsed.Before(cutoff) {
			delete(l.ipCache, k)
		}
	}
	ipCount := len(l.ipCache)
	l.ipMu.Unlock()

	l.userMu.Lock()
	for k, v := range l.userCache {
		if v.lastUsed.Before(cutoff) {
			delete(l.userCache, k)
		}
	}
	userCount := len(l.userCache)
	l.userMu.Unlock()

	l.log.Debug("rate limiter eviction completed",
		slog.Int("ip_buckets", ipCount),
		slog.Int("user_buckets", userCount),
	)
}

func tierFromMethod(method string) Tier {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return TierRead
	default:
		return TierWrite
	}
}

func (t Tier) String() string {
	if t == TierWrite {
		return "write"
	}
	return "read"
}

func newBucket(cfg TierConfig) *rate.Limiter {
	return rate.NewLimiter(rate.Limit(cfg.RPS), cfg.Burst)
}

func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (e *bucketEntry) touch() {
	e.lastUsed = time.Now()
}
