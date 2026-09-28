package orchestrator_test

import (
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/orchestrator"
)

func TestRestartBudget_AllowsFirst(t *testing.T) {
	b := orchestrator.NewRestartBudget(orchestrator.RestartConfig{
		Window: time.Minute, MaxRestarts: 3,
	})
	if !b.Allow() {
		t.Error("first restart should be allowed")
	}
}

func TestRestartBudget_TripsAfterMax(t *testing.T) {
	b := orchestrator.NewRestartBudget(orchestrator.RestartConfig{
		Window: time.Minute, MaxRestarts: 3,
	})
	// First three are allowed.
	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("restart %d should be allowed", i)
		}
	}
	// Fourth is denied.
	if b.Allow() {
		t.Error("fourth restart should be denied")
	}
}

func TestRestartBudget_ResetsAfterWindow(t *testing.T) {
	b := orchestrator.NewRestartBudget(orchestrator.RestartConfig{
		Window: 50 * time.Millisecond, MaxRestarts: 2,
	})
	for i := 0; i < 2; i++ {
		if !b.Allow() {
			t.Fatalf("restart %d should be allowed", i)
		}
	}
	if b.Allow() {
		t.Fatal("third restart should be denied")
	}
	time.Sleep(60 * time.Millisecond)
	if !b.Allow() {
		t.Error("after window, restart should be allowed again")
	}
}
