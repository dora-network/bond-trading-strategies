package orderupdates

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

// runSlot is the cached tuple for a single active run.
type runSlot struct {
	doraUserID string
	status     string
}

// RunCache is the in-memory hot-path cache for the order-updates
// Manager. RLock on hit; on miss, RUnlock → fallback → repopulate.
// The Manager's poll loop calls snapshotAndReconcile to detect
// terminal-state entries the lifecycle missed.
type RunCache struct {
	mu       sync.RWMutex
	items    map[uuid.UUID]*runSlot
	fallback RunLookupFunc
}

// NewRunCache constructs an empty cache. fallback may be nil for
// tests; Lookup degrades to "not found" in that case.
func NewRunCache(fallback RunLookupFunc) *RunCache {
	return &RunCache{
		items:    make(map[uuid.UUID]*runSlot),
		fallback: fallback,
	}
}

// Lookup satisfies RunLookupFunc. Hot path: RLock + map read + return.
// Cold path: RUnlock, call fallback, repopulate under a write lock.
func (c *RunCache) Lookup(ctx context.Context, runID uuid.UUID) (string, string, bool) {
	c.mu.RLock()
	if slot, ok := c.items[runID]; ok {
		user, status := slot.doraUserID, slot.status
		c.mu.RUnlock()
		return user, status, true
	}
	c.mu.RUnlock()
	if c.fallback == nil {
		return "", "", false
	}
	user, status, found := c.fallback(ctx, runID)
	if !found {
		return "", "", false
	}
	c.mu.Lock()
	c.items[runID] = &runSlot{doraUserID: user, status: status}
	c.mu.Unlock()
	return user, status, true
}

// Set inserts or replaces an entry. Called from EnsureSubscribed on
// a run starting/resuming.
func (c *RunCache) Set(runID uuid.UUID, doraUserID, status string) {
	c.mu.Lock()
	c.items[runID] = &runSlot{doraUserID: doraUserID, status: status}
	c.mu.Unlock()
}

// UpdateStatus mutates the cached status in place. No-op if the run
// isn't in the cache — the next Lookup falls back to the DB.
func (c *RunCache) UpdateStatus(runID uuid.UUID, status string) {
	c.mu.Lock()
	if slot, ok := c.items[runID]; ok {
		slot.status = status
	}
	c.mu.Unlock()
}

// Remove drops a single entry. Exposed for tests; production
// unsubscribe relies on the poll loop to clean up stale entries.
func (c *RunCache) Remove(runID uuid.UUID) {
	c.mu.Lock()
	delete(c.items, runID)
	c.mu.Unlock()
}

// cachedRun is the immutable projection returned by Snapshot.
type cachedRun struct {
	doraUserID string
	status     string
}

// Snapshot returns a copy of cached entries for the poll loop.
func (c *RunCache) Snapshot() map[uuid.UUID]cachedRun {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[uuid.UUID]cachedRun, len(c.items))
	for id, slot := range c.items {
		out[id] = cachedRun{doraUserID: slot.doraUserID, status: slot.status}
	}
	return out
}

// snapshotAndReconcile is invoked by the Manager's poll loop. It
// re-queries the fallback for each cached entry; entries that come
// back with status ∉ {running, paused} or not found are removed.
// Status or doraUserID drift is corrected by Set.
func (c *RunCache) snapshotAndReconcile(ctx context.Context) {
	if c.fallback == nil {
		return
	}
	snap := c.Snapshot()
	for id, cached := range snap {
		user, status, found := c.fallback(ctx, id)
		switch {
		case !found:
			c.Remove(id)
		case status != "running" && status != "paused":
			c.Remove(id)
		case user != cached.doraUserID || status != cached.status:
			c.Set(id, user, status)
		}
	}
}
