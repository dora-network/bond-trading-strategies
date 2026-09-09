package backtest

import (
	"context"
	"fmt"
)

// RunStartupJanitor reconciles DB state with the assumption that the
// agent just started fresh. It flips every queued|running row to failed
// (a crashed previous run can't be in those states on a clean start).
// The socket/cidfile cleanup that previously lived here was removed on
// 2026-09-04 alongside the docker pipeline — the WASM-only world does
// not produce on-disk sockets or cidfiles.
func RunStartupJanitor(ctx context.Context, store Store) error {
	if _, err := store.FailOrphaned(ctx); err != nil {
		return fmt.Errorf("fail orphaned backtests: %w", err)
	}
	return nil
}
