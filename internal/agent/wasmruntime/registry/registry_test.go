package registry_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm/prompts"

	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"
)

func TestRegistry_LoadCompileInstantiate(t *testing.T) {
	dir := t.TempDir()
	root := t.TempDir()
	st, err := store.New(root)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	// Use the example strategy compiled in Plan 1 Task 8. If the
	// artifact is not present, skip — the build is gated on TinyGo
	// being on PATH in CI, and the unit test for the runtime must
	// not require it.
	wasmPath := filepath.Join(dir, "..", "..", "..", "strategywasm", "example", "strategy.wasm")
	if _, err := os.Stat(wasmPath); err != nil {
		t.Skipf("example strategy.wasm not found at %s; run `make tinygo-build` in strategywasm/ to produce it", wasmPath)
	}
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Fatalf("read wasm: %v", err)
	}

	manifestBytes := []byte(`{
		"schema_version": 1,
		"module_name": "noop",
		"language": "go",
		"framework_version": "` + prompts.WasmFrameworkVersion + `",
		"capabilities": {
			"order_books": ["OB-1"],
			"resolutions": ["1m"],
			"channels": ["candle"],
			"host_functions": ["host_log"]
		},
		"params_schema": {}
	}`)

	wh, mh, err := st.Put(wasmBytes, manifestBytes)
	if err != nil {
		t.Fatalf("store.Put: %v", err)
	}

	r, err := registry.New(t.Context(), registry.Config{MemoryLimitBytes: registry.DefaultMemoryLimitBytes})
	if err != nil {
		t.Fatalf("registry.New: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	inst, err := r.Load(t.Context(), st, wh, mh)
	if err != nil {
		t.Fatalf("registry.Load: %v", err)
	}
	if inst == nil {
		t.Fatal("Load returned nil instance")
	}
	inst.Close(t.Context())
}

func TestRegistry_RejectsManifestHashMismatch(t *testing.T) {
	root := t.TempDir()
	st, err := store.New(root)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	wh, _, err := st.Put([]byte("\x00asm\x01\x00\x00\x00"), []byte("{}"))
	if err != nil {
		t.Fatalf("store.Put: %v", err)
	}

	r, err := registry.New(t.Context(), registry.Config{})
	if err != nil {
		t.Fatalf("registry.New: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	// Pass a manifest hash that does not match anything in the store.
	_, err = r.Load(t.Context(), st, wh, "0000000000000000000000000000000000000000000000000000000000000000")
	if err == nil {
		t.Fatal("expected error on manifest hash mismatch")
	}
}

// TestRegistry_Close_WithCancelledParent asserts that Close's
// teardown still runs to completion when the parent's context
// is already cancelled. This is the agent-shutdown case: SIGINT
// arrives, the agent's main context is cancelled, the deferred
// r.Close() runs, and we need it to actually release wazero
// resources. Without the fresh bounded context in Close, the
// wazero teardown calls would return immediately and the
// resources would leak.
func TestRegistry_Close_WithCancelledParent(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	r, err := registry.New(parentCtx, registry.Config{})
	if err != nil {
		t.Fatalf("registry.New: %v", err)
	}

	// Cancel the parent BEFORE Close. The Registry's lifetime
	// context (a child of parentCtx) is also cancelled; that's
	// intentional — it aborts in-flight work. The teardown in
	// Close must still complete.
	cancel()

	if err := r.Close(); err != nil {
		t.Errorf("Close after parent cancel: %v", err)
	}

	// Close is idempotent.
	if err := r.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestInstance_Close_Idempotent asserts that concurrent Close
// calls on the same instance are safe and only one wazero
// Module.Close happens. (Before the sync.Mutex, the
// `if i.closed` check was racy.)
func TestInstance_Close_Idempotent(t *testing.T) {
	root := t.TempDir()
	st, err := store.New(root)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	wh, mh, err := st.Put([]byte("\x00asm\x01\x00\x00\x00"),
		[]byte(`{"schema_version":1,"module_name":"x","language":"go","framework_version":"`+prompts.WasmFrameworkVersion+`","capabilities":{"order_books":["OB-1"],"host_functions":["host_log"]},"params_schema":{}}`))
	if err != nil {
		t.Fatalf("store.Put: %v", err)
	}

	r, err := registry.New(t.Context(), registry.Config{})
	if err != nil {
		t.Fatalf("registry.New: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	inst, err := r.Load(t.Context(), st, wh, mh)
	if err != nil {
		t.Fatalf("registry.Load: %v", err)
	}

	// Two concurrent closes. Both must return without panicking.
	done := make(chan struct{}, 2)
	go func() { inst.Close(t.Context()); done <- struct{}{} }()
	go func() { inst.Close(t.Context()); done <- struct{}{} }()
	<-done
	<-done
}
