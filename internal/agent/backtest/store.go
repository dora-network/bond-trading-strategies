package backtest

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when a backtest row is missing (or, via the
// handler's user_id filter, owned by a different user — 404, not 403).
var ErrNotFound = errors.New("backtest: not found")

// Store is the swappable seam for backtest persistence. PgStore is the only
// implementation today; a future in-memory or git-backed backend can satisfy
// the same interface without changing orchestration or HTTP-handler callers.
type Store interface {
	// Create inserts a new job row in StatusQueued. Returns the assigned
	// backtest ID. The partial unique index will reject the insert if
	// the user already has an active backtest; callers map 23505 to 429.
	Create(ctx context.Context, b *Backtest) (string, error)

	// Get returns a single row by ID. ErrNotFound if absent.
	Get(ctx context.Context, id string) (*Backtest, error)

	// List returns up to limit backtests for a strategy, newest first.
	// Ownership is enforced by the HTTP handler (callers filter on
	// b.UserID); this method has no user_id predicate so it can serve
	// the per-strategy backtest index page without a join.
	List(ctx context.Context, strategyID string, limit int) ([]*Backtest, error)

	// ListBacktests is the user-scoped variant used by the LLM tool
	// (and any other call site that needs ownership in SQL). The
	// userID is mandatory and always applied as a WHERE predicate;
	// strategyID and status are optional filters (nil = "any"). The
	// ownership filter lives in the query, not in Go, so a stray
	// caller can't leak rows by forgetting a second-pass filter.
	// Limit is capped at MaxPageSize; <=0 falls back to defaultPageSize.
	ListBacktests(ctx context.Context, userID string, strategyID *string, status *Status, limit int) ([]*Backtest, error)

	// UpdateStatus moves a row into status; also writes startedAt/finishedAt
	// (both nullable; pass nil to leave alone) and the error message (empty
	// string to clear). Drives the queued -> running -> terminal transitions.
	UpdateStatus(ctx context.Context, id string, status Status,
		startedAt, finishedAt *time.Time, errorMsg string) error

	// SetSummary persists the final stats, records fill_count, and atomically
	// transitions the row to succeeded with finished_at = now(). Called by
	// the runner on receipt of POST /summary over the UDS.
	SetSummary(ctx context.Context, id string, summary *Summary, fillCount int) error

	// InsertFills persists a batch of fills for the given backtest. The
	// caller batches per OnCandle invocation; single-row insert-per-fill
	// would not scale. (Phase 1 transitions to copy-from-stdin for large
	// windows; this loop is fine for the v1 ~hundreds-of-fills scale.)
	InsertFills(ctx context.Context, backtestID string, fills []Fill) error

	// GetFills returns all fills for a backtest ordered by timestamp asc.
	GetFills(ctx context.Context, backtestID string) ([]Fill, error)

	// CancelIfRunning atomically transitions queued|running -> cancelled
	// with finished_at = now(). Returns wasRunning=true iff the row was
	// actually flipped; false if the row was already terminal or absent.
	// 404s collapse to (false, nil) so the cancel endpoint stays idempotent.
	CancelIfRunning(ctx context.Context, id string) (wasRunning bool, err error)

	// FailOrphaned flips every queued|running row to failed with a
	// fixed error message. Called from the startup janitor so a crashed
	// previous run doesn't leave the table stuck in active states. Returns
	// the number of rows touched.
	FailOrphaned(ctx context.Context) (int, error)
}
