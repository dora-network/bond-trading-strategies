package prompts_test

import (
	"strings"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm/prompts"
)

// TestStrategyPrompt_DefaultTargetIsWASM guards Plan 4: the WASM path
// is the default target for newly-generated strategies. The prompt must
// name go-wasm, the manifest contract, the host functions, and the
// framework import path under strategywasm/dorastrategy.
func TestStrategyPrompt_DefaultTargetIsWASM(t *testing.T) {
	p := prompts.StrategyGenerationSystemPrompt
	for _, want := range []string{
		"go-wasm",
		"manifest.json",
		"wasm",
		"host.Log",
		"host.SubmitOrder",
	} {
		if !strings.Contains(p, want) {
			t.Errorf("prompt missing %q", want)
		}
	}
}

// TestStrategyPrompt_NamesTheFrameworkPath guards the WASM framework
// import path is surfaced so the model emits the correct module.
func TestStrategyPrompt_NamesTheFrameworkPath(t *testing.T) {
	if !strings.Contains(prompts.StrategyGenerationSystemPrompt, prompts.WasmFrameworkImportPath) {
		t.Errorf("prompt should name the WASM framework import path %q", prompts.WasmFrameworkImportPath)
	}
}
