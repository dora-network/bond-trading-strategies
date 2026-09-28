package registry_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm/prompts"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
)

// TestHostModule_BuilderInstantiatesAllSpecs exercises the real
// contract: every AllHostFuncs spec must resolve to a concrete
// (non-nil) host function via the BuildHostModule callback, and the
// resulting wazero builder must actually instantiate.
func TestHostModule_BuilderInstantiatesAllSpecs(t *testing.T) {
	rt := wazero.NewRuntime(t.Context())
	t.Cleanup(func() { _ = rt.Close(t.Context()) })

	hm := registry.NewHostModule(rt)
	mod, err := hm.Builder.Instantiate(t.Context())
	if err != nil {
		t.Fatalf("host module builder failed to instantiate: %v", err)
	}
	t.Cleanup(func() { _ = mod.Close(t.Context()) })

	// A fresh runtime: "env" cannot be instantiated twice on one
	// runtime.
	rt2 := wazero.NewRuntime(t.Context())
	t.Cleanup(func() { _ = rt2.Close(t.Context()) })
	seen := make(map[string]bool, len(registry.AllHostFuncs))
	noop := func(context.Context, api.Module, []uint64) {}
	builder := registry.BuildHostModule(rt2, func(name string) api.GoModuleFunc {
		seen[name] = true
		return noop
	})
	if _, err := builder.Instantiate(t.Context()); err != nil {
		t.Fatalf("builder with full impls failed to instantiate: %v", err)
	}
	for _, spec := range registry.AllHostFuncs {
		if !seen[spec.Name] {
			t.Errorf("BuildHostModule never resolved spec %q to an impl", spec.Name)
		}
	}
}

func TestAllHostFuncsIncludesNewImports(t *testing.T) {
	want := map[string]bool{
		"host_get_config":       true, // existing
		"host_next_candle":      true, // existing
		"host_next_live_candle": true, // existing
		"host_submit_order":     true,
		"host_cancel_order":     true,
		"host_record_fill":      true,
		"host_backtest_error":   true,
		"host_log":              true,
		"host_now":              true,
		"host_random":           true,
		"host_fetch_candles":    true, // NEW
		"host_fetch_trades":     true, // NEW
		"host_fetch_prices":     true, // NEW
		"host_next_event":       true, // NEW
	}
	have := map[string]bool{}
	for _, s := range registry.AllHostFuncs {
		have[s.Name] = true
	}
	for n := range want {
		if !have[n] {
			t.Errorf("AllHostFuncs missing %q", n)
		}
	}
	if len(have) != len(want) {
		t.Errorf("AllHostFuncs has %d entries, want %d (extra? %v)", len(have), len(want), diff(have, want))
	}
}

func diff(a, b map[string]bool) map[string]bool {
	out := map[string]bool{}
	for k := range a {
		if !b[k] {
			out[k] = true
		}
	}
	return out
}

// The host binary has no ldflags injection for FrameworkVersion
// (the strategywasm const is "dev" by default). The host's
// CheckFrameworkVersion compares against the agent's known
// WasmFrameworkVersion const, not the framework var.
func TestCheckFrameworkVersion_OK(t *testing.T) {
	if prompts.WasmFrameworkVersion == "" {
		t.Fatal("prompts.WasmFrameworkVersion must be non-empty")
	}
	if err := registry.CheckFrameworkVersion(prompts.WasmFrameworkVersion, nil); err != nil {
		t.Errorf("exact match should pass, got %v", err)
	}
}

func TestCheckFrameworkVersion_Missing(t *testing.T) {
	err := registry.CheckFrameworkVersion("", nil)
	if err == nil {
		t.Fatal("expected error for empty framework_version")
	}
	var typed *registry.ErrFrameworkVersionMismatch
	if !errors.As(err, &typed) {
		t.Fatalf("expected *ErrFrameworkVersionMismatch, got %T", err)
	}
	if typed.Got != "" {
		t.Errorf("typed.Got = %q, want empty", typed.Got)
	}
	if typed.Expected != prompts.WasmFrameworkVersion {
		t.Errorf("typed.Expected = %q, want %q", typed.Expected, prompts.WasmFrameworkVersion)
	}
}

func TestCheckFrameworkVersion_Mismatch(t *testing.T) {
	stale := prompts.WasmFrameworkVersion + "-stale"
	err := registry.CheckFrameworkVersion(stale, nil)
	var typed *registry.ErrFrameworkVersionMismatch
	if !errors.As(err, &typed) {
		t.Fatalf("expected *ErrFrameworkVersionMismatch, got %T", err)
	}
	if typed.Got != stale {
		t.Errorf("typed.Got = %q, want %q", typed.Got, stale)
	}
	if typed.Expected != prompts.WasmFrameworkVersion {
		t.Errorf("typed.Expected = %q, want %q", typed.Expected, prompts.WasmFrameworkVersion)
	}
}

// TestCheckFrameworkVersion_PopulatesMissingAndExtra proves the typed
// error names which host_* imports the manifest declared that the
// framework doesn't export (Missing) and what the framework offers
// beyond the declaration (Extra).
func TestCheckFrameworkVersion_PopulatesMissingAndExtra(t *testing.T) {
	declared := []string{"host_get_config", "host_nonexistent"}
	err := registry.CheckFrameworkVersion("", declared)
	var typed *registry.ErrFrameworkVersionMismatch
	if !errors.As(err, &typed) {
		t.Fatalf("expected *ErrFrameworkVersionMismatch, got %T", err)
	}
	if !slices.Contains(typed.Missing, "host_nonexistent") {
		t.Errorf("Missing should contain %q, got %v", "host_nonexistent", typed.Missing)
	}
	if slices.Contains(typed.Missing, "host_get_config") {
		t.Errorf("Missing must not contain an exported name, got %v", typed.Missing)
	}
	foundExtra := false
	for _, s := range registry.AllHostFuncs {
		if s.Name == "host_get_config" || s.Name == "host_nonexistent" {
			continue
		}
		if slices.Contains(typed.Extra, s.Name) {
			foundExtra = true
		}
	}
	if !foundExtra {
		t.Errorf("Extra should contain at least one exported name not declared, got %v", typed.Extra)
	}
}
