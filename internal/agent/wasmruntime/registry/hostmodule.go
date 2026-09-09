package registry

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm/prompts"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// HostFuncSpec describes one framework host function's WASM ABI.
type HostFuncSpec struct {
	Name    string
	Params  []api.ValueType
	Results []api.ValueType
}

// AllHostFuncs is the canonical list of every host_* function the
// framework can import via //go:wasmimport. TinyGo emits all declared
// imports into the .wasm module, so every wazero host module MUST
// export every name in this list or instantiation fails.
//
// Single source of truth: when a new //go:wasmimport is added to
// strategywasm/dorastrategy/host/wasm.go, add it here. Every host
// module builder (validate, backtest, live) reads this list, so no
// builder can miss a function.
//
//nolint:gochecknoglobals // canonical list; single source of truth for all host modules
var AllHostFuncs = []HostFuncSpec{
	{Name: "host_get_config", Params: []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, Results: []api.ValueType{api.ValueTypeI32}},
	{Name: "host_next_candle", Params: []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, Results: []api.ValueType{api.ValueTypeI32}},
	{Name: "host_next_live_candle", Params: []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, Results: []api.ValueType{api.ValueTypeI32}},
	{
		Name:    "host_submit_order",
		Params:  []api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32},
		Results: []api.ValueType{api.ValueTypeI32},
	},
	{
		Name:    "host_cancel_order",
		Params:  []api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32},
		Results: []api.ValueType{api.ValueTypeI32},
	},
	{Name: "host_record_fill", Params: []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, Results: nil},
	{Name: "host_backtest_error", Params: []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, Results: nil},
	{Name: "host_log", Params: []api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32}, Results: nil},
	{Name: "host_now", Params: nil, Results: []api.ValueType{api.ValueTypeI64}},
	{Name: "host_random", Params: []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, Results: []api.ValueType{api.ValueTypeI32}},
	{
		Name:    "host_fetch_candles",
		Params:  []api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32},
		Results: []api.ValueType{api.ValueTypeI32},
	},
	{
		Name:    "host_fetch_trades",
		Params:  []api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32},
		Results: []api.ValueType{api.ValueTypeI32},
	},
	{
		Name:    "host_fetch_prices",
		Params:  []api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32},
		Results: []api.ValueType{api.ValueTypeI32},
	},
	{Name: "host_next_event", Params: []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, Results: []api.ValueType{api.ValueTypeI32}},
}

// noopHostFunc is the default implementation for functions a mode
// doesn't use. TinyGo still emits them as imports, so the host module
// must provide something.
//
//nolint:gochecknoglobals // const-like; never reassigned
var noopHostFunc = api.GoModuleFunc(func(context.Context, api.Module, []uint64) {})

// writeEmptyBatch is the validate-mode stub for the three fetch imports
// (host_fetch_candles, host_fetch_trades, host_fetch_prices). It writes a
// no-data {"items":[],"done":true,"cursor":""} JSON envelope into the guest
// output buffer and returns the byte count, matching the real fetch ABI
// (inPtr, inLen, outPtr, outLen) -> int32 bytes-written. A validate strategy
// that calls a fetch and gets this empty batch should treat it as a hard
// error (no state to trade on); the framework's Run surfaces that via
// host_backtest_error. Keeps the validate smoke network-free (spec §4.3).
func writeEmptyBatch(_ context.Context, mod api.Module, params []uint64) {
	outPtr := uint32(params[2]) //nolint:gosec // wazero ABI: i32 param (outPtr)
	outLen := uint32(params[3]) //nolint:gosec // wazero ABI: i32 param (outLen)
	body := []byte(`{"items":[],"done":true,"cursor":""}`)
	if uint32(len(body)) > outLen { //nolint:gosec // len(body) is a constant
		params[0] = 0 // buffer too small; signal no bytes written
		return
	}
	if !mod.Memory().Write(outPtr, body) {
		params[0] = 0
		return
	}
	params[0] = uint64(len(body))
}

// HostImpl returns the GoModuleFunc for a given host function name, or
// nil to use a no-op stub. Each mode (validate, backtest, live)
// provides its own HostImpl.
type HostImpl func(name string) api.GoModuleFunc

// BuildHostModuleFn adapts BuildHostModule to the unnamed func type
// declared by backtest's WasmStarter.buildHostModule seam, so the seam
// field can be assigned registry.BuildHostModuleFn directly.
func BuildHostModuleFn(rt wazero.Runtime, impl func(name string) api.GoModuleFunc) wazero.HostModuleBuilder {
	return BuildHostModule(rt, impl)
}

// BuildHostModule creates the "env" host module on rt with every
// function in AllHostFuncs. For each name, impl returns the real
// implementation or nil for a no-op stub. The caller must call
// Instantiate on the returned builder.
func BuildHostModule(rt wazero.Runtime, impl HostImpl) wazero.HostModuleBuilder {
	hm := rt.NewHostModuleBuilder("env")
	names := make([]string, 0, len(AllHostFuncs))
	for _, spec := range AllHostFuncs {
		fn := impl(spec.Name)
		if fn == nil {
			fn = noopHostFunc
		}
		hm.NewFunctionBuilder().
			WithGoModuleFunction(fn, spec.Params, spec.Results).
			Export(spec.Name)
		names = append(names, spec.Name)
	}
	slog.Info("registry: host module built", "functions", names)
	return hm
}

// ErrFrameworkVersionMismatch is returned at instantiate time
// when the plugin's manifest.framework_version doesn't match
// the running framework's expected version. The agent's
// generate_strategy tool is supposed to drive a regenerate
// and rebuild on this error.
type ErrFrameworkVersionMismatch struct {
	Expected string
	Got      string
	Missing  []string // host_* names the plugin's manifest declares but the framework doesn't export
	Extra    []string // host_* names the plugin's manifest uses but the framework doesn't declare
}

func (e *ErrFrameworkVersionMismatch) Error() string {
	return fmt.Sprintf("plugin framework_version=%q does not match expected %q "+
		"(missing imports: %v, extra imports: %v); rebuild the strategy against the current framework",
		e.Got, e.Expected, e.Missing, e.Extra)
}

// CheckFrameworkVersion returns nil if the manifest's
// framework_version matches the running framework's expected
// version, or a typed *ErrFrameworkVersionMismatch otherwise.
// The expected version is `prompts.WasmFrameworkVersion` (the
// agent's known release tag), not `dorastrategy.FrameworkVersion`
// (which is a `var` defaulting to "dev" and is overwritten only
// in the .wasm built by the validate path's tinygo invocation).
// The host binary compiles the strategywasm package via a
// `replace` directive and does not get ldflags injection, so
// comparing against the framework var would reject every real
// manifest.
func CheckFrameworkVersion(manifestVersion string, hostFunctions []string) error {
	if manifestVersion == "" {
		return &ErrFrameworkVersionMismatch{
			Expected: prompts.WasmFrameworkVersion,
			Got:      "",
			Missing:  missingHostImports(hostFunctions),
			Extra:    extraHostImports(hostFunctions),
		}
	}
	if manifestVersion != prompts.WasmFrameworkVersion {
		return &ErrFrameworkVersionMismatch{
			Expected: prompts.WasmFrameworkVersion,
			Got:      manifestVersion,
			Missing:  missingHostImports(hostFunctions),
			Extra:    extraHostImports(hostFunctions),
		}
	}
	return nil
}

// missingHostImports returns the host_* names the plugin declares
// in its manifest that the framework does NOT export. Empty when
// the manifest matches.
func missingHostImports(declared []string) []string {
	exported := map[string]bool{}
	for _, s := range AllHostFuncs {
		exported[s.Name] = true
	}
	var missing []string
	for _, name := range declared {
		if !exported[name] {
			missing = append(missing, name)
		}
	}
	return missing
}

// extraHostImports returns the host_* names the framework exports
// that the plugin does NOT declare. Reports what the framework
// offers for diagnostics; not used as a hard rejection.
func extraHostImports(declared []string) []string {
	declaredSet := map[string]bool{}
	for _, name := range declared {
		declaredSet[name] = true
	}
	var extra []string
	for _, s := range AllHostFuncs {
		if !declaredSet[s.Name] {
			extra = append(extra, s.Name)
		}
	}
	return extra
}

// HostModule is retained for backward compatibility with tests that
// assert the function surface without instantiating a guest.
type HostModule struct {
	Builder   wazero.HostModuleBuilder
	Functions map[string]bool
}

// NewHostModule builds a validate-mode host module. It wires real stubs for:
//   - host_get_config: a minimal {"mode":"validate"} config so Run()'s smoke
//     init completes;
//   - host_fetch_candles / host_fetch_trades / host_fetch_prices: writeEmptyBatch,
//     returning {"items":[],"done":true,"cursor":""} (network-free; a validate
//     strategy that fetches and gets empty treats it as a hard error);
//   - host_next_event: noopHostFunc (validate has no event loop).
//
// Every other host_* name falls through to the noopHostFunc default.
func NewHostModule(rt wazero.Runtime) HostModule {
	hm := BuildHostModule(rt, func(name string) api.GoModuleFunc {
		switch name {
		case "host_get_config":
			return newGetConfigStub()
		case "host_fetch_candles", "host_fetch_trades", "host_fetch_prices":
			return writeEmptyBatch // {"items":[],"done":true,"cursor":""}; network-free
		case "host_next_event":
			// Validate has no event loop; never called during the Init smoke.
			return noopHostFunc
		}
		return nil
	})
	funcs := make(map[string]bool, len(AllHostFuncs))
	for _, spec := range AllHostFuncs {
		funcs[spec.Name] = true
	}
	return HostModule{Builder: hm, Functions: funcs}
}

// newGetConfigStub returns a host function that writes a minimal
// validate-mode config JSON into the guest buffer.
func newGetConfigStub() api.GoModuleFunc {
	return func(_ context.Context, mod api.Module, params []uint64) {
		bufPtr := uint32(params[0]) //nolint:gosec // wazero ABI: i32 param
		bufLen := uint32(params[1]) //nolint:gosec // wazero ABI: i32 param
		cfg := `{"mode":"validate"}`
		if len(cfg) > int(bufLen) {
			params[0] = 0
			return
		}
		if !mod.Memory().Write(bufPtr, []byte(cfg)) {
			params[0] = 0
			return
		}
		params[0] = uint64(len(cfg))
	}
}
