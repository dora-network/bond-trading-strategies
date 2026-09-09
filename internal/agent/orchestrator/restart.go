package orchestrator

import "time"

// RestartConfig is the per-strategy restart budget.
type RestartConfig struct {
	Window      time.Duration // sliding window
	MaxRestarts int
}

// RestartBudget tracks the per-strategy restart count within a
// sliding window. A strategy whose budget is exhausted is marked
// crashed and not restarted.
type RestartBudget struct {
	cfg    RestartConfig
	resets []time.Time
}

// NewRestartBudget constructs a budget.
func NewRestartBudget(cfg RestartConfig) *RestartBudget {
	return &RestartBudget{cfg: cfg}
}

// Allow returns true if a restart is permitted under the budget.
// It records the current time and evicts resets older than Window.
func (b *RestartBudget) Allow() bool {
	now := time.Now()
	cutoff := now.Add(-b.cfg.Window)
	live := b.resets[:0]
	for _, t := range b.resets {
		if t.After(cutoff) {
			live = append(live, t)
		}
	}
	b.resets = live
	if len(b.resets) >= b.cfg.MaxRestarts {
		return false
	}
	b.resets = append(b.resets, now)
	return true
}

// Reset clears the budget. Used by the manual restart endpoint
// (Plan 4 wires the HTTP handler).
func (b *RestartBudget) Reset() {
	b.resets = nil
}
