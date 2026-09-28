// Package deployment owns the live-deployment lifecycle store. A deployment
// is the user-facing history of a strategy running live; running_live_strategies
// (migration 008) is the orchestrator's active-instance mirror.
package deployment

import (
	"context"
	"errors"
	"time"
)

// Status is the deployment lifecycle state.
type Status string

const (
	StatusRunning Status = "running"
	StatusStopped Status = "stopped"
	StatusCrashed Status = "crashed"
	StatusHalted  Status = "halted"
)

// Page is an opaque keyset cursor over a newest-first list. Cursor "" is the
// first page; Limit 0 uses the default (capped at MaxPageSize).
type Page struct {
	Limit  int
	Cursor string
}

const (
	MaxPageSize     = 100
	defaultPageSize = 50
)

// EffectiveLimit resolves a page limit to a bounded value.
func (p Page) EffectiveLimit() int {
	if p.Limit <= 0 || p.Limit > MaxPageSize {
		return defaultPageSize
	}
	return p.Limit
}

// Deployment is one deploy-lifecycle row.
type Deployment struct {
	ID                string
	StrategyID        string
	Revision          string
	UserID            string
	OrderBookID       string
	Resolution        string
	Params            map[string]string
	Status            Status
	InstanceID        string
	StartedAt         *time.Time
	StoppedAt         *time.Time
	StoppedReason     string
	RestartCount      int
	HotswappedAt      *time.Time
	HotswappedFromRev string
	// CandleCount is the cumulative count of candles delivered to
	// the strategy's host_next_live_candle, persisted so the stat
	// survives process restarts. The livehost throttles writes; on
	// restart the orchestrator seeds state.candleCount with this
	// value before the new goroutine starts, so Stats() reports
	// the total since the strategy was first deployed (modulo
	// any unpersisted ticks since the last throttle).
	CandleCount int64
	// WarmupCandles is the strategy's required warmup candle count,
	// read from the manifest at deploy time and stashed on the row
	// so Resume / Recover don't need to re-load the manifest from
	// the artifact store.
	WarmupCandles int `json:"warmup_candles,omitempty"`
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Errors
var (
	ErrNotFound       = errors.New("deployment: not found")
	ErrAlreadyRunning = errors.New("deployment: strategy already has an active deployment")
)

// Store is the deployment persistence interface.
type Store interface {
	Create(ctx context.Context, d Deployment) error
	Get(ctx context.Context, id, userID string) (Deployment, error)
	List(ctx context.Context, strategyID, userID string, p Page) ([]Deployment, string, error)
	UpdateStatus(ctx context.Context, id, userID string, status Status, reason string) error
	SetInstance(ctx context.Context, id, userID, instanceID string) error
	HotSwap(ctx context.Context, id, userID, newRevision string) error
	IncRestart(ctx context.Context, id, userID string) error
	// SetCandleCount overwrites the persisted candle count for a
	// deployment. Used by the livehost on a throttled interval
	// (e.g. every 30s) and on every restart boundary. Owner-scoped
	// (id + userID) like the other Store methods.
	SetCandleCount(ctx context.Context, id, userID string, n int64) error
	// UpdateWarmupCandles overwrites the persisted warmup_candles for a
	// deployment. Deploy re-syncs the row to the manifest's value after
	// the runtime starts — the request's warmup_candles field (which
	// seeded the row) can be stale or zero, and Resume / Recover rebuild
	// the warmup window from the row.
	UpdateWarmupCandles(ctx context.Context, id, userID string, warmupCandles int) error
	HasActive(ctx context.Context, strategyID string) (bool, error)
	ListRunning(ctx context.Context) ([]Deployment, error)
}
