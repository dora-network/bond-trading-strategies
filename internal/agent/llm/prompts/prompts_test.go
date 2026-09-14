package prompts

import (
	"fmt"
	"strings"
	"testing"
)

func TestStrategyGenerationSystemPrompt_ContainsOffTopicRule(t *testing.T) {
	if !strings.Contains(StrategyGenerationSystemPrompt, "trading strategies for assets on the") {
		t.Errorf("StrategyGenerationSystemPrompt must contain the off-topic refusal rule")
	}
}

func TestStrategyGenerationSystemPrompt_ContainsChecklist(t *testing.T) {
	for _, item := range []string{
		"Target/framework alignment",
		"Instrument/universe",
		"Entry rules",
		"Exit rules",
		"Position sizing",
		"Risk limits",
		"Operating cadence",
		"No in-strategy performance metrics",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, item) {
			t.Errorf("StrategyGenerationSystemPrompt missing checklist item %q", item)
		}
	}
}

func TestStrategyGenerationSystemPrompt_ContainsEightToolNames(t *testing.T) {
	for _, name := range []string{
		"dora_list_assets",
		"dora_list_order_books",
		"dora_get_order_book",
		"dora_get_order_book_stats",
		"dora_get_trades",
		"dora_get_positions",
		"dora_get_candle_data",
		"dora_get_asset_ytm",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, name) {
			t.Errorf("StrategyGenerationSystemPrompt missing read tool %q", name)
		}
	}
}

// TestStrategyGenerationSystemPrompt_Phase4DropsTestRequirement guards the Phase 4
// rewrite: the prompt no longer mentions _test.go as required, and
// instead requires a framework-conforming strategy source file.
func TestStrategyGenerationSystemPrompt_Phase4DropsTestRequirement(t *testing.T) {
	if strings.Contains(StrategyGenerationSystemPrompt, "at least one *_test.go") {
		t.Errorf("Phase 4 prompt must NOT require *_test.go (spec §8)")
	}
	for _, kw := range []string{
		"at least one strategy source file (*.go)",
		FrameworkImportPath, // framework module path appears in prompt
		"STRATEGY CONTRACT:",
		"Init(cfg dorastrategy.Config) error",
		"OnCandle(c dorastrategy.Candle)",
		"dorastrategy.Run(",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, kw) {
			t.Errorf("Phase 4 prompt missing contract keyword %q", kw)
		}
	}
}

// TestStrategyGenerationSystemPrompt_LeadWithLookupFirst guards the
// backtest chain: when the user asks to run a backtest, the LLM must
// call get_strategy first so the existing build is reused. Without
// this rule the LLM defaults to generate_strategy (the prior bug).
func TestStrategyGenerationSystemPrompt_LeadWithLookupFirst(t *testing.T) {
	for _, kw := range []string{
		"get_strategy",
		"run_backtest",
		"head.image_ref",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, kw) {
			t.Errorf("StrategyGenerationSystemPrompt missing workflow keyword %q", kw)
		}
	}
}

// TestStrategyGenerationSystemPrompt_MUSTNotGenerateWhenStrategyExists
// pins the precedence rule: the LLM must NOT call generate_strategy
// when get_strategy returns a populated head. The existing
// "you MUST invoke generate_strategy" line is the fallback path
// (only when no build exists), not the default.
func TestStrategyGenerationSystemPrompt_MUSTNotGenerateWhenStrategyExists(t *testing.T) {
	// Look for an explicit "do not generate when head is populated"
	// rule that outranks the existing MUST-generate line.
	// The exact wording below is the rule we want; tie it to the
	// keyword so the test fails if the wording drifts.
	if !strings.Contains(StrategyGenerationSystemPrompt, "head.image_ref") {
		t.Errorf("StrategyGenerationSystemPrompt must include the head.image_ref check before generating")
	}
	// Regression guard: prior structural-corruption edits dropped the
	// 'ids from the head. Do NOT regenerate.' line that closes the
	// (2) BACKTEST A SAVED STRATEGY instruction. Substring tests for
	// keywords above passed because they didn't check this specific
	// line — assert it explicitly so the next regression fails loudly.
	if !strings.Contains(StrategyGenerationSystemPrompt, "ids from the head. Do NOT regenerate.") {
		t.Errorf("StrategyGenerationSystemPrompt missing the 'Do NOT regenerate.' instruction that closes the backtest workflow")
	}
}

// TestStrategyGenerationSystemPrompt_HasDeploymentBlock guards the
// LIVE DEPLOYMENT guidance: the prompt must surface the
// deployment tool names, the "backtest first" rule, and the kill
// switch advice so the model doesn't skip past a safety rail on
// the live path. Without these, the agent defaults to generate-only
// and loses the runtime context for live runs.
func TestStrategyGenerationSystemPrompt_HasDeploymentBlock(t *testing.T) {
	for _, kw := range []string{
		"LIVE DEPLOYMENT",
		"deploy_strategy",
		"dora_get_positions",
		"dora_get_trades",
		"stop_deployment",
		"hotswap_deployment",
		"backtest",
		"kill switch",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, kw) {
			t.Errorf("StrategyGenerationSystemPrompt missing deployment keyword %q", kw)
		}
	}
}

// TestStrategyGenerationSystemPrompt_SequentialBacktestRule pins the
// one-inflight backtest constraint: when the user wants to compare
// multiple windows / params / order books, the LLM must queue them
// one at a time. Without this rule the agent fires several
// run_backtest calls in parallel and the user sees a 429 storm.
func TestStrategyGenerationSystemPrompt_SequentialBacktestRule(t *testing.T) {
	for _, kw := range []string{
		"at most one backtest per user at a time",
		"sequentially",
		"get_backtest_result",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, kw) {
			t.Errorf("StrategyGenerationSystemPrompt missing sequential-backtest rule keyword %q", kw)
		}
	}
}

// TestFrameworkImportPath_Stable asserts the constant is the canonical
func TestFrameworkImportPath_Stable(t *testing.T) {
	want := "github.com/dora-network/dora-agent-strategy"
	if FrameworkImportPath != want {
		t.Errorf("FrameworkImportPath: want %q, got %q", want, FrameworkImportPath)
	}
}

func TestWasmFrameworkVersion_Interpolated(t *testing.T) {
	// The wasm require line must surface the real wasm version
	// (WasmFrameworkVersion = "v0.1.0") to the LLM, distinct from the
	// docker path's FrameworkVersion. Without this the LLM falls back
	// to hallucinated versions (the v0.0.5-alpha incident), the wasm
	// validator's go mod tidy fails to resolve any tagged release, and
	// the build is rejected with "unknown revision".
	if WasmFrameworkVersion == FrameworkVersion {
		t.Errorf("WasmFrameworkVersion must differ from FrameworkVersion: both are %q", FrameworkVersion)
	}
	if !strings.Contains(StrategyGenerationSystemPrompt, WasmFrameworkVersion) {
		t.Errorf("prompt must interpolate WasmFrameworkVersion=%q", WasmFrameworkVersion)
	}
}

func TestWasmFrameworkABIVersion_Interpolated(t *testing.T) {
	// The manifest's framework_version must match the agent's known
	// release tag at the time the LLM runs. A hardcoded tag here is the
	// v0.0.5-alpha class of bug: the LLM writes a version the host
	// refuses at instantiate time.
	want := fmt.Sprintf("(%q)", WasmFrameworkVersion)
	if !strings.Contains(StrategyGenerationSystemPrompt, want) {
		t.Errorf("prompt must interpolate WasmFrameworkVersion: %q", want)
	}
}

// TestStrategyGenerationSystemPrompt_HasOnPreambleBestPractices guards the
// v3 OnPreamble guidance: the prompt must tell the model OnPreamble runs in
// both modes before any event hook, and that it must not place orders there.
func TestStrategyGenerationSystemPrompt_HasOnPreambleBestPractices(t *testing.T) {
	for _, want := range []string{
		"- ON PREAMBLE",
		"do NOT place orders",
		"p.FetchCandles(ctx, start, end",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, want) {
			t.Errorf("StrategyGenerationSystemPrompt missing OnPreamble guidance %q", want)
		}
	}
}

// TestStrategyGenerationSystemPrompt_HasNoOrdersInPreambleRule pins the
// return-type rule: OnPreamble returns only error, and the safety kernel
// rejects any attempt to emit orders from preamble.
func TestStrategyGenerationSystemPrompt_HasNoOrdersInPreambleRule(t *testing.T) {
	for _, want := range []string{
		"- NO ORDERS IN PREAMBLE",
		"not ([]OrderIntent, error)",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, want) {
			t.Errorf("StrategyGenerationSystemPrompt missing no-orders-in-preamble rule %q", want)
		}
	}
}

// TestStrategyGenerationSystemPrompt_HasPreambleHistorySizing guards the
// warmup window sizing advice so generated preambles don't fetch absurdly
// large candle ranges.
func TestStrategyGenerationSystemPrompt_HasPreambleHistorySizing(t *testing.T) {
	for _, want := range []string{
		"- PREAMBLE HISTORY SIZING",
		"max(warmup_candles * resolution_seconds, expected_indicator_lookback * resolution_seconds)",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, want) {
			t.Errorf("StrategyGenerationSystemPrompt missing preamble history sizing guidance %q", want)
		}
	}
}

// TestStrategyGenerationSystemPrompt_HasOnTradeOnPriceDefault guards the
// default-hook rule: unused OnTrade/OnPrice hooks must return nil, nil.
func TestStrategyGenerationSystemPrompt_HasOnTradeOnPriceDefault(t *testing.T) {
	for _, want := range []string{
		"- ON TRADE / ON PRICE DEFAULT",
		"emit return nil, nil",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, want) {
			t.Errorf("StrategyGenerationSystemPrompt missing OnTrade/OnPrice default rule %q", want)
		}
	}
}

// TestStrategyGenerationSystemPrompt_HasSelfFillAwareness guards the
// self-fill guidance: OnTrade sees the strategy's own fills, and the only
// implementable correlation today is Fill.OrderID <-> Trade.OrderID.
func TestStrategyGenerationSystemPrompt_HasSelfFillAwareness(t *testing.T) {
	for _, want := range []string{
		"- SELF-FILL AWARENESS",
		"Fill.OrderID ↔ Trade.OrderID",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, want) {
			t.Errorf("StrategyGenerationSystemPrompt missing self-fill awareness guidance %q", want)
		}
	}
}

// TestStrategyGenerationSystemPrompt_HasRebuildOnMismatch guards the spec
// 4.8.1 recovery flow: on ErrFrameworkVersionMismatch the agent must
// regenerate via generate_strategy, not patch the old revision.
func TestStrategyGenerationSystemPrompt_HasRebuildOnMismatch(t *testing.T) {
	for _, want := range []string{
		"- REBUILD ON VERSION MISMATCH",
		"call generate_strategy with the same user_id",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, want) {
			t.Errorf("StrategyGenerationSystemPrompt missing rebuild-on-mismatch guidance %q", want)
		}
	}
}

func TestStrategyGenerationSystemPrompt_HasRebuildOnStaleFramework(t *testing.T) {
	for _, want := range []string{
		"- REBUILD ON STALE FRAMEWORK",
		"read_strategy_sources(strategy_id=<sid>",
		"WASM decimal-string types",
		"framework_version = current WasmFrameworkVersion",
		"generate_strategy(strategy_id=<sid>) with the rewritten",
		"rebuild already in flight",
	} {
		if !strings.Contains(StrategyGenerationSystemPrompt, want) {
			t.Errorf("StrategyGenerationSystemPrompt missing %q", want)
		}
	}
}
