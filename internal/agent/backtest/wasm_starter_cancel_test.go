package backtest

import (
	"testing"
	"time"
)

// TestOrchestrator_RegisterJob_CancelReachesCtx proves that the
// exported RegisterJob wires a cancellable per-job ctx and that
// Cancel(id) actually cancels it. The docker path already exercises
// this via TestOrchestrator_CancelCancelsRunner; the WASM path now
// uses the same seam.
//
// The full end-to-end unwind (mergeStream.next unblocks, plugin's
// host_next_event returns -1, _start exits, the cancel guard at
// wasm_starter.go:159 fires) is covered by
// TestWasmStarter_CancelUnwindsWASMGoroutine in wasm_starter_test.go.
// This test pins just the wiring: ctx gets cancelled, not the full
// guest unwind.
func TestOrchestrator_RegisterJob_CancelReachesCtx(t *testing.T) {
	t.Parallel()
	orch := &Orchestrator{}

	jobCtx, deregister := orch.RegisterJob("bt-test-2")

	if err := jobCtx.Err(); err != nil {
		t.Fatalf("fresh ctx: got err %v, want nil", err)
	}

	if !orch.Cancel("bt-test-2") {
		t.Fatal("Cancel(\"bt-test-2\"): got false, want true (RegisterJob must have wired the cancel func)")
	}
	deregister() // explicit: simulates the runner goroutine's deferred deregister exit

	select {
	case <-jobCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("jobCtx not cancelled within 1s of Cancel(id)")
	}
	if err := jobCtx.Err(); err == nil {
		t.Fatal("jobCtx.Err(): got nil, want non-nil after Cancel")
	}

	// Second cancel on a deregistered job: false (idempotent).
	if orch.Cancel("bt-test-2") {
		t.Fatal("Cancel(\"bt-test-2\") after deregister: got true, want false")
	}
	// Unknown id: false.
	if orch.Cancel("does-not-exist") {
		t.Fatal("Cancel(unknown): got true, want false")
	}
}
