package migration

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

func TestMigrator_NeedsMigration_TrueForGoDocker(t *testing.T) {
	m := New()
	if !m.NeedsMigration(strategies.Version{Target: "go-docker", WasmRef: "x"}) {
		t.Fatal("expected NeedsMigration(true) for target=go-docker")
	}
}

func TestMigrator_NeedsMigration_FalseForGoWasm(t *testing.T) {
	m := New()
	if m.NeedsMigration(strategies.Version{Target: "go-wasm", WasmRef: "sha256:abc"}) {
		t.Fatal("expected NeedsMigration(false) for target=go-wasm + WasmRef set")
	}
}

func TestMigrator_NeedsMigration_TrueForEmptyWasmRef(t *testing.T) {
	m := New()
	if !m.NeedsMigration(strategies.Version{Target: "go-wasm", WasmRef: ""}) {
		t.Fatal("expected NeedsMigration(true) when target=go-wasm but WasmRef empty (broken capture)")
	}
}

func TestMigrator_ReserveBlocksSecond(t *testing.T) {
	m := New()
	if err := m.Reserve("u1", "s1"); err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	if err := m.Reserve("u1", "s1"); !errors.Is(err, ErrRebuildInFlight) {
		t.Fatalf("second Reserve: want ErrRebuildInFlight, got %v", err)
	}
}

func TestMigrator_ReleaseUnblocks(t *testing.T) {
	m := New()
	_ = m.Reserve("u1", "s1")
	m.Release("u1", "s1")
	if err := m.Reserve("u1", "s1"); err != nil {
		t.Fatalf("Reserve after Release: %v", err)
	}
}

func TestMigrator_ReserveTTLExpiry(t *testing.T) {
	m := &Migrator{inflight: make(map[string]time.Time), ttl: 10 * time.Millisecond}
	_ = m.Reserve("u1", "s1")
	time.Sleep(20 * time.Millisecond)
	if err := m.Reserve("u1", "s1"); err != nil {
		t.Fatalf("Reserve after TTL expiry: %v", err)
	}
}

func TestErrStaleFramework_Error_DockerTargetWording(t *testing.T) {
	// Non-go-wasm target (e.g. go-docker): the message names the
	// legacy target and points at generate_strategy.
	err := &ErrStaleFramework{StrategyID: "sid", CurrentRevisionID: "rev", Target: "go-docker"}
	msg := err.Error()
	if !strings.Contains(msg, `target="go-docker"`) {
		t.Errorf("missing docker target wording: %q", msg)
	}
	if strings.Contains(msg, "no compiled wasm artifact") {
		t.Errorf("docker-target message must NOT borrow empty-artifact wording: %q", msg)
	}
}

func TestErrStaleFramework_Error_EmptyArtifactWording(t *testing.T) {
	// target=go-wasm but WasmRef="" (broken capture): the message
	// must NOT claim the row was built on a non-go-wasm target.
	err := &ErrStaleFramework{StrategyID: "sid", CurrentRevisionID: "rev", Target: "go-wasm"}
	msg := err.Error()
	if strings.Contains(msg, `target="go-wasm"; the current target is go-wasm`) {
		t.Errorf("empty-artifact case must not use the docker-target wording: %q", msg)
	}
	if !strings.Contains(msg, "no compiled wasm artifact") {
		t.Errorf("empty-artifact case must name the missing artifact: %q", msg)
	}
	if !strings.Contains(msg, `target="go-wasm"`) {
		t.Errorf("empty-artifact case must still echo the actual target: %q", msg)
	}
}
