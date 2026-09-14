// Package agent — authcache is a tiny TTL cache for `/v1/user/self`
// lookups. The agent's HTTP layer uses this to avoid hitting Dora on
// every inbound `/v1/agent/*` request. The cache is keyed by the raw
// API key string and bounded by DORA_AUTH_CACHE_TTL.
package agent

import (
	"context"
	"sync"
	"time"
)

// Resolver is the upstream Dora /v1/user/self lookup the cache wraps.
// Implemented by the dora-client-go SDK via a small adapter built at
// startup.
type Resolver interface {
	Resolve(ctx context.Context, apiKey string) (userID string, err error)
}

type cacheEntry struct {
	userID    string
	expiresAt time.Time
}

// authCache caches Resolver results for ttl. Entries are evicted
// lazily on access (a hit checks expiry, a miss falls through to the
// resolver).
//
// ponytail: an unbounded map. Resolver keys are the raw API key
// strings callers presented to the agent; under normal traffic the
// cardinality is bounded by the user count (low thousands) and TTL
// is short (default 5m). If the agent ever serves a high-traffic
// public surface, swap in an LRU with explicit capacity.
type authCache struct {
	resolver Resolver
	ttl      time.Duration

	mu      sync.Mutex
	entries map[string]cacheEntry
}

func newAuthCache(r Resolver, ttl time.Duration) *authCache {
	return &authCache{
		resolver: r,
		ttl:      ttl,
		entries:  make(map[string]cacheEntry),
	}
}

// Resolve looks up apiKey in the cache; on hit (and not expired), it
// returns the cached userID without touching the resolver. On miss
// or expiry it calls the resolver and stores the result. Errors
// from the resolver are propagated verbatim and NOT cached.
func (c *authCache) Resolve(ctx context.Context, apiKey string) (string, error) {
	c.mu.Lock()
	if e, ok := c.entries[apiKey]; ok && time.Now().Before(e.expiresAt) {
		c.mu.Unlock()
		return e.userID, nil
	}
	c.mu.Unlock()

	userID, err := c.resolver.Resolve(ctx, apiKey)
	if err != nil {
		return "", err
	}

	c.mu.Lock()
	c.entries[apiKey] = cacheEntry{
		userID:    userID,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()

	return userID, nil
}
