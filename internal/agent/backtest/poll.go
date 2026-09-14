package backtest

import (
	"context"
	"time"
)

// PollForTerminal blocks until the backtest reaches a terminal status
// (succeeded / failed / cancelled), the budget elapses, or ctx is cancelled.
// The chat tool layer wraps this around `get_backtest_result` so the LLM
// gets a single observable call instead of "no, it's still running — call
// me again" loops. If the budget runs out before the backtest finishes,
// the most recent non-terminal row is returned with a nil error so the
// caller can decide to surface "still running" or extend the budget.
func PollForTerminal(
	ctx context.Context,
	store Store,
	id string,
	budget, interval time.Duration,
) (*Backtest, error) {
	deadline := time.Now().Add(budget)
	for {
		b, err := store.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if isTerminal(b.Status) {
			return b, nil
		}
		// If the next sleep would overshoot the budget, return the
		// current row rather than block past the deadline. time.Now
		// can drift backward across a NTP step — bound on the safe
		// side by sleeping only the remaining time.
		remaining := time.Until(deadline)
		if remaining <= interval {
			return b, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

// isTerminal is the closed-set check used both by PollForTerminal and (in
// later phases) the runner's status-flip guard. Kept as a small helper
// rather than a method on Status so callers don't have to construct a
// Backtest just to test the predicate.
func isTerminal(s Status) bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}
