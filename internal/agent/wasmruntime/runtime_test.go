package wasmruntime_test

import (
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm/prompts"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime"
)

// NewRuntime(nil pool) falls back to the FS-only store (logged warning).
// Tests in this file run hermetically without DATABASE_URL — the
// pgstore-backed path is exercised by the package's tests in store/pgstore.

func TestRuntime_Constructs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGENT_WASM_ARTIFACT_ROOT", dir)
	rt, err := wasmruntime.NewRuntime(t.Context(), nil)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if rt.Store() == nil {
		t.Error("runtime should have a non-nil store")
	}
	if rt.Registry() == nil {
		t.Error("runtime should have a non-nil registry")
	}
}

func TestRuntime_RequiresArtifactRoot(t *testing.T) {
	t.Setenv("AGENT_WASM_ARTIFACT_ROOT", "")
	if _, err := wasmruntime.NewRuntime(t.Context(), nil); err == nil {
		t.Error("NewRuntime should fail when AGENT_WASM_ARTIFACT_ROOT is unset")
	}
}

func TestRuntime_LoadPlugin(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGENT_WASM_ARTIFACT_ROOT", dir)

	rt, err := wasmruntime.NewRuntime(t.Context(), nil)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	st := rt.Store()
	wh, mh, err := st.Put(
		[]byte("\x00asm\x01\x00\x00\x00"),
		[]byte(`{"schema_version":1,"module_name":"x","framework_version":"`+prompts.WasmFrameworkVersion+`","language":"go","capabilities":{"order_books":["OB-1"],"host_functions":["host_log"]}}`),
	)
	if err != nil {
		t.Fatalf("store.Put: %v", err)
	}

	inst, err := rt.Registry().Load(t.Context(), st, wh, mh)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	inst.Close(t.Context())
}
