package orchestrator_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm/prompts"
	"github.com/dora-network/bond-trading-strategies/internal/agent/orchestrator"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"
)

func TestOrchestrator_Validate_NoArtifact(t *testing.T) {
	dir := t.TempDir()
	st, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	reg, err := registry.New(t.Context(), registry.Config{})
	if err != nil {
		t.Fatalf("registry.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })

	o, err := orchestrator.New(orchestrator.Config{Store: st, Registry: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = o.Validate(t.Context(), "no-such-hash", "no-such-manifest")
	if err == nil {
		t.Fatal("expected error on missing artifact")
	}
}

func TestOrchestrator_Validate_HappyPath(t *testing.T) {
	// Gates on TinyGo having built the example strategy.wasm.
	dir := t.TempDir()
	st, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	wasmPath := filepath.Join("..", "..", "strategywasm", "example", "strategy.wasm")
	if _, err := os.Stat(wasmPath); err != nil {
		t.Skipf("example strategy.wasm not found: %v", err)
	}
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Fatalf("read wasm: %v", err)
	}
	manifest := []byte(`{
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
	wh, mh, err := st.Put(wasmBytes, manifest)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	reg, err := registry.New(t.Context(), registry.Config{})
	if err != nil {
		t.Fatalf("registry.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })

	o, err := orchestrator.New(orchestrator.Config{Store: st, Registry: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Validate(t.Context(), wh, mh)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !res.Passed {
		t.Errorf("expected smoke to pass: %+v", res)
	}
}

func TestOrchestrator_New_RejectsNilRegistry(t *testing.T) {
	dir := t.TempDir()
	st, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	_, err = orchestrator.New(orchestrator.Config{Store: st})
	if err == nil {
		t.Fatal("expected error on nil Registry")
	}
}
