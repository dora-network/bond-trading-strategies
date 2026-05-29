package ratelimit

// Evict triggers a synchronous eviction of stale buckets.
// It is exported for tests only.
func (l *Limiter) Evict() {
	l.evict()
}

// IPBucketCount returns the number of IP buckets currently in memory.
func (l *Limiter) IPBucketCount() int {
	l.ipMu.RLock()
	defer l.ipMu.RUnlock()
	return len(l.ipCache)
}

// UserBucketCount returns the number of user buckets currently in memory.
func (l *Limiter) UserBucketCount() int {
	l.userMu.RLock()
	defer l.userMu.RUnlock()
	return len(l.userCache)
}
