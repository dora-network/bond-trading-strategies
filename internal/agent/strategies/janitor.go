package strategies

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Sweeper is the minimum subset of Store the Janitor depends on. It
// exists so unit tests can inject a fake without implementing the
// full Store interface (which would pull in pgxpool, providers,
// etc.). Production code passes a *PgStore, which satisfies Sweeper.
type Sweeper interface {
	SweepCapturePending(ctx context.Context, olderThan time.Duration) (int, error)
}

// Janitor periodically sweeps stale strategy_capture_pending rows so a
// sustained burst of capture failures cannot accumulate rows
// indefinitely. The sweep predicate is server-time-based, so concurrent
// Janitor instances (multi-replica) are idempotent — N replicas means
// N-1 wasted DELETEs per tick, harmless.
//
// Lifecycle: call Start to begin ticking under a parent context, then
// Stop to cancel and wait for any in-flight sweep to drain. Stop is
// idempotent and safe to call without Start (no goroutine running,
// nothing to drain).
type Janitor struct {
	store     Sweeper
	interval  time.Duration
	retention time.Duration
	logger    *slog.Logger

	mu      sync.Mutex
	cancel  context.CancelFunc // non-nil only between Start and the run goroutine's exit
	stopped chan struct{}      // closed when the goroutine has exited (or immediately if Start was never called)
}

// NewJanitor builds a Janitor with the given sweep interval and
// retention window. Both must be positive. interval should be much
// smaller than retention so a brief outage doesn't lose stashes.
//
// stopped is pre-closed so Stop without Start is a no-op (the read
// returns immediately). Start swaps in an open channel that the run
// goroutine closes on exit.
func NewJanitor(store Sweeper, interval, retention time.Duration, logger *slog.Logger) *Janitor {
	if logger == nil {
		logger = slog.Default()
	}
	stopped := make(chan struct{})
	close(stopped)
	return &Janitor{
		store:     store,
		interval:  interval,
		retention: retention,
		logger:    logger,
		stopped:   stopped,
	}
}

// Start launches the sweep goroutine under the given parent context.
// Returns immediately; the goroutine runs until Stop is called or the
// parent context is cancelled.
func (j *Janitor) Start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)

	j.mu.Lock()
	j.cancel = cancel
	j.stopped = make(chan struct{})
	j.mu.Unlock()

	go j.run(ctx)
}

// Stop cancels the goroutine and waits for any in-flight sweep to
// drain. Idempotent and safe before Start.
func (j *Janitor) Stop() {
	j.mu.Lock()
	cancel := j.cancel
	stopped := j.stopped
	j.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	<-stopped
}

func (j *Janitor) run(ctx context.Context) {
	defer close(j.stopped)
	t := time.NewTicker(j.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Bound each sweep so a stuck DB doesn't block shutdown.
			// The parent ctx propagates cancellation into pgx, so a
			// in-flight DELETE returns promptly on Stop.
			sweepCtx, sweepCancel := context.WithTimeout(ctx, j.interval)
			n, err := j.store.SweepCapturePending(sweepCtx, j.retention)
			sweepCancel()
			if err != nil {
				// Don't crash the agent on a transient DB blip; the
				// next tick will retry. Logged at warn so it's
				// visible without paging anyone.
				j.logger.Warn("strategy_capture_pending sweep failed",
					"error", err,
					"retention", j.retention.String())
				continue
			}
			if n > 0 {
				j.logger.Info("strategy_capture_pending sweep",
					"deleted", n,
					"retention", j.retention.String())
			}
		}
	}
}
