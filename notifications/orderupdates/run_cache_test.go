package orderupdates_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/notifications/orderupdates"
)

type callCounter struct{ n atomic.Int32 }

func (c *callCounter) fallback(_ context.Context, id uuid.UUID) (string, string, bool) {
	c.n.Add(1)
	switch id {
	case uuid.MustParse("11111111-1111-1111-1111-111111111111"):
		return "alice", "running", true
	case uuid.MustParse("22222222-2222-2222-2222-222222222222"):
		return "bob", "paused", true
	}
	return "", "", false
}

func TestRunCache_HitReturnsCachedWithoutFallback(t *testing.T) {
	t.Parallel()
	cc := &callCounter{}
	c := orderupdates.NewRunCache(cc.fallback)
	c.Set(uuid.MustParse("11111111-1111-1111-1111-111111111111"), "alice", "running")

	user, status, found := c.Lookup(context.Background(), uuid.MustParse("11111111-1111-1111-1111-111111111111"))
	require.True(t, found)
	assert.Equal(t, "alice", user)
	assert.Equal(t, "running", status)
	assert.Equal(t, int32(0), cc.n.Load(), "hit must not invoke fallback")
}

func TestRunCache_MissPopulatesCache(t *testing.T) {
	t.Parallel()
	cc := &callCounter{}
	c := orderupdates.NewRunCache(cc.fallback)

	user, status, found := c.Lookup(context.Background(), uuid.MustParse("11111111-1111-1111-1111-111111111111"))
	require.True(t, found)
	assert.Equal(t, "alice", user)
	assert.Equal(t, "running", status)
	assert.Equal(t, int32(1), cc.n.Load(), "miss must invoke fallback once")

	c.Lookup(context.Background(), uuid.MustParse("11111111-1111-1111-1111-111111111111"))
	assert.Equal(t, int32(1), cc.n.Load(), "second lookup must not invoke fallback")
}

func TestRunCache_MissNotFoundDoesNotCache(t *testing.T) {
	t.Parallel()
	cc := &callCounter{}
	c := orderupdates.NewRunCache(cc.fallback)

	_, _, found := c.Lookup(context.Background(), uuid.New())
	require.False(t, found)
	assert.Equal(t, int32(1), cc.n.Load())

	c.Lookup(context.Background(), uuid.New())
	assert.Equal(t, int32(2), cc.n.Load(),
		"unknown id must hit fallback each time (no negative caching)")
}

func TestRunCache_UpdateStatusMutatesInPlace(t *testing.T) {
	t.Parallel()
	c := orderupdates.NewRunCache(nil)
	runID := uuid.New()
	c.Set(runID, "alice", "running")
	c.UpdateStatus(runID, "paused")

	user, status, ok := c.Lookup(context.Background(), runID)
	require.True(t, ok)
	assert.Equal(t, "alice", user)
	assert.Equal(t, "paused", status)
}

func TestRunCache_RemoveClearsEntry(t *testing.T) {
	t.Parallel()
	cc := &callCounter{}
	c := orderupdates.NewRunCache(cc.fallback)
	runID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	c.Set(runID, "alice", "running")
	c.Remove(runID)

	user, status, ok := c.Lookup(context.Background(), runID)
	require.True(t, ok)
	assert.Equal(t, "alice", user)
	assert.Equal(t, "running", status)
	assert.Equal(t, int32(1), cc.n.Load(),
		"after Remove, lookup must re-invoke fallback")
}
