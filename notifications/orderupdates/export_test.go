package orderupdates

import "context"

// ExportRunCache allows tests in the _test package to inject a RunCache
// via WithRunCache and inspect it directly. This is a compile-time
// witness for the unexported type — production code never references
// this alias.
type ExportRunCache = RunCache

// SnapshotAndReconcileForTest lets tests in the _test package drive the
// cache's poll-tick reconcile directly without waiting 30 seconds.
func (c *RunCache) SnapshotAndReconcileForTest(ctx context.Context) {
	c.snapshotAndReconcile(ctx)
}
