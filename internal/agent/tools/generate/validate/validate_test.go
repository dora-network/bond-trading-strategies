package validate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm/prompts"
)

// validFiles is a minimal, allowlist-clean strategy file map.
func validFiles() map[string]string {
	return map[string]string{
		"main.go":      "package main\nfunc main() {}\n",
		"main_test.go": "package main\nimport \"testing\"\nfunc TestOK(t *testing.T) {}\n",
		"go.mod":       "module example\n\ngo 1.26.5\n",
	}
}

func TestRequireModule_RejectsGomodDirectives(t *testing.T) {
	cases := []struct {
		line string
		want bool
	}{
		{"module example.com/strategy", false},
		{"go 1.26.5", false},
		{"toolchain go1.26.5", false},
		{"replace foo v1.0.0 => ./foo", false},
		{"require example/doraclient v1.0.0", true},
		{"example/doraclient v1.0.0", true},
		{"evil/dep v2.0.0", true},
		{"require (", false},
		{")", false},
	}
	for _, tc := range cases {
		_, ok := requireModule(tc.line)
		if ok != tc.want {
			t.Errorf("requireModule(%q): want require=%v, got %v", tc.line, tc.want, ok)
		}
	}
}

// TestIsCompilerOutput pins the markers the validator uses to
// classify a build error as a repairable logic failure vs a terminal
// infra failure. Each row is a real production error string observed
// in the agent log; dropping a marker would silently re-route that
// error class to infra, leaving the LLM with verified:false and no
// diagnostic -- exactly the strategy3 retry loop symptom.
func TestIsCompilerOutput(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want bool
	}{
		// Logic failures: each must route to the repairable branch.
		{"file:line:col marker", "main.go:5:2: undefined: foo", true},
		{"undefined:", "./vwap.go:35:2: declared and not used: executionDuration", true},
		{"cannot find package", "main.go:3:8: cannot find package \"github.com/example/missing\" in any of:\n\t/tmp/agent-validator-xxx (/tmp/agent-validator-xxx)\n", true},
		{"missing go.sum entry", "main.go:5:2: missing go.sum entry for module providing package github.com/example/missing", true},
		{"go.mod requires", "/go.mod requires go 1.26.5 (running go 1.24.0)", true},
		{"# pkg marker", "# example/strategy\n./strategy.go:10:5: undefined: VWAPStrategy", true},
		{"real-world unknown field", "unknown field OrderBookID in struct literal of type dorastrategy.OrderIntent", true},
		// Infra failures: each must NOT be classified as compiler output.
		{"docker daemon", "Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?", false},
		{"OOM", "Error response from daemon: OCI runtime exec failed: container_linux.go:380: starting container process caused: process_linux.go:404: out of memory", false},
		{"pull failure", "docker: Error response from daemon: pull access denied for golang, repository does not exist or may require 'docker login'", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isCompilerOutput(errors.New(tc.msg))
			if got != tc.want {
				t.Errorf("isCompilerOutput(%q) = %v, want %v", tc.msg, got, tc.want)
			}
		})
	}
}

// TestIsCompilerOutput_DockerBuildWithCompileError pins the bug
// the strategy4 session hit: a docker build output that contains
// real compile errors AFTER ~5KB of buildkit progress lines. The
// fix removes the pre-truncation in DockerRuntime.Build so the
// validator's isCompilerOutput sees the full message and routes
// to the repairable logic-failure branch instead of the terminal
// infra branch.
func TestIsCompilerOutput_DockerBuildWithCompileError(t *testing.T) {
	var sb strings.Builder
	// Simulate a typical docker build output: 100 lines of
	// base-image pull / extract progress, then the actual
	// go build step that fails with a real compile error.
	for i := range 100 {
		fmt.Fprintf(&sb, "#%d [internal] load metadata for docker.io/library/golang:1.26.5-bookworm\n", i)
		fmt.Fprintf(&sb, "#%d sha256:c5a4625b533197abb25ea2a32be06c59c984d97c3c2dc9952e0b76f2e81ee0d2 12.58MB / 64.41MB 0.4s\n", i)
	}
	sb.WriteString("#10 [build 4/4] RUN go build -o /strategy .\n")
	sb.WriteString("#10 2.038 strategy/strategy.go:84:7: c.Symbol undefined (type dorastrategy.Candle has no field or method Symbol)\n")
	sb.WriteString("#10 2.038 strategy/strategy.go:114:6: unknown field Symbol in struct literal of type dorastrategy.OrderIntent\n")
	sb.WriteString("#10 ERROR: process \"/bin/sh -c go build -o /strategy .\" did not complete successfully: exit code: 1\n")

	// After the fix the validator sees the full message; the
	// trailing .go:N:M: undefined markers drive isCompilerOutput
	// to true. Pre-fix the dockerBuildOutputCap=4KB pre-truncation
	// (followed by the global 512-rune truncate) clipped out the
	// compile errors and isCompilerOutput returned false.
	if !isCompilerOutput(errors.New(sb.String())) {
		t.Error("isCompilerOutput on full docker build output: want true (compile error at tail), got false")
	}
}

// TestTruncate_KeepsTail pins the bug the strategy4 retry loop
// hit: a docker build output dominated by buildkit progress
// lines at the head and a real compile error at the tail. The
// OLD truncate kept only the first 512 runes (all progress
// lines), clipping the actionable error out. The fix keeps
// both head and tail: the LLM gets context (which step the
// build reached) AND the actionable error.
func TestTruncate_KeepsTail(t *testing.T) {
	var sb strings.Builder
	for i := range 200 {
		fmt.Fprintf(&sb, "#%d [internal] load metadata for docker.io/library/golang:1.26.5-bookworm\n", i)
	}
	sb.WriteString("#10 [build 4/4] RUN go build -o /strategy .\n")
	sb.WriteString("#10 2.038 strategy/strategy.go:84:7: c.Symbol undefined (type dorastrategy.Candle has no field or method Symbol)\n")

	got := truncate(sb.String())
	if !strings.Contains(got, "c.Symbol undefined") {
		t.Error("truncate: want tail to contain the compile error, got head-only clipping")
	}
	if !strings.Contains(got, "load metadata") {
		t.Error("truncate: want head to contain progress context, got tail-only clipping")
	}
}

// TestClassifySmokeFailure pins the framework-vs-strategy category
// decision. The classifier matches on the framework's own
// "dorastrategy:" stderr prefix — anything the framework logs is
// a framework/deployment issue, not a strategy bug. A new
// framework error string doesn't need a per-string allowlist
// update; this test just pins the boundary between framework
// stderr (prefix) and strategy stderr (everything else).
func TestClassifySmokeFailure(t *testing.T) {
	cases := []struct {
		name   string
		stderr string
		want   string
	}{
		{"framework_mode_dispatch", "dorastrategy: mode not implemented", "framework"},
		{"framework_env_var", "dorastrategy: BACKTEST_CALLBACK_URL must include scheme (unix or http)", "framework"},
		{"framework_fetch", "dorastrategy: fetch candles: candles: status 404", "framework"},
		{"framework_with_leading_whitespace", "\ndorastrategy: panic: nil pointer", "framework"},
		{"strategy_init", "init: missing ORDER_BOOK_ID", "output"},
		{"strategy_panic", "panic: runtime error: invalid memory address", "output"},
		{"empty", "", "output"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifySmokeFailure([]byte(tc.stderr)); got != tc.want {
				t.Errorf("classifySmokeFailure(%q) = %q, want %q", tc.stderr, got, tc.want)
			}
		})
	}
}

// TestValidate_WasmTarget_RejectsDisallowedImport guards Plan 4: a
// go-wasm strategy whose go.mod requires the disallowed dora-client-go
// SDK is rejected as a logic failure before the build, with an
// allowlist-tagged diagnostic naming the offending import.
func TestValidate_WasmTarget_RejectsDisallowedImport(t *testing.T) {
	files := validFiles()
	files["go.mod"] = "module example\n\ngo 1.26.5\nrequire $$GST43BCT8DFF:L$$/dora-client-go v0.0.0\n"
	v := &Validator{}
	res, err := v.Validate(t.Context(), files)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if res.BuildOK {
		t.Fatal("go-wasm strategy with disallowed import should not build")
	}
	var found bool
	for _, d := range res.Diagnostics {
		if d.Category == diagnosticCategoryAllowlist && strings.Contains(d.Message, "dora-client-go") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected an allowlist diagnostic naming dora-client-go, got %+v", res.Diagnostics)
	}
}

// TestTinygoDockerfile_IsTinyGo guards Plan 4: the wasm Dockerfile uses
// the TinyGo base image and the tinygo build command (not go build).
func TestTinygoDockerfile_IsTinyGo(t *testing.T) {
	if !strings.Contains(tinygoDockerfile, "tinygo/tinygo:") {
		t.Errorf("tinygoDockerfile must use a tinygo base image:\n%s", tinygoDockerfile)
	}
	if !strings.Contains(tinygoDockerfile, "tinygo build -target=wasi") {
		t.Errorf("tinygoDockerfile must run tinygo build -target=wasi:\n%s", tinygoDockerfile)
	}
	if strings.Contains(tinygoDockerfile, "RUN go build ") {
		t.Errorf("tinygoDockerfile must not run go build:\n%s", tinygoDockerfile)
	}
}

// fakeTinyGoRunner writes a canned wasm blob instead of invoking tinygo.
type fakeTinyGoRunner struct {
	wasm []byte
}

func (f *fakeTinyGoRunner) Build(_ context.Context, _, out string) error {
	return os.WriteFile(out, f.wasm, filePerm)
}

// minimalWasm is a valid WebAssembly module that exports an empty init
// function and imports nothing, suitable for the in-process smoke path.
var minimalWasm = []byte{
	0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // magic + version
	0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // type section: () -> ()
	0x03, 0x02, 0x01, 0x00, // func section: 1 function of type 0
	0x07, 0x08, 0x01, 0x04, 0x69, 0x6e, 0x69, 0x74, 0x00, 0x00, // export "init"
	0x0a, 0x04, 0x01, 0x02, 0x00, 0x0b, // code section: empty body
}

// TestValidate_WasmTarget_AllowlistedImportProceeds confirms a go-wasm
// strategy whose go.mod requires only the allowed framework proceeds to
// the in-process build and smoke, populating WasmRef and ManifestHash.
func TestValidate_WasmTarget_AllowlistedImportProceeds(t *testing.T) {
	files := validFiles()
	files["go.mod"] = "module example\n\ngo 1.26.5\nrequire github.com/dora-network/dora-strategy-wasm/dorastrategy v0.0.0\n"
	files["manifest.json"] = `{"schema_version":1,"module_name":"example","framework_version":"` + prompts.WasmFrameworkVersion + `","capabilities":{"order_books":["btcusd"],"host_functions":["host_log"]}}`
	v := &Validator{cfg: Config{
		WasmPrepare:  noopWasmPrepare,
		TinyGoRunner: &fakeTinyGoRunner{wasm: minimalWasm},
	}}
	res, err := v.Validate(t.Context(), files)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !res.BuildOK {
		t.Fatalf("allowed framework import should build, got %+v", res)
	}
	if res.WasmRef == "" {
		t.Errorf("WasmRef should be populated")
	}
	if res.ManifestHash == "" {
		t.Errorf("ManifestHash should be populated")
	}
}

// TestValidate_WasmTarget_ModuleRootRequireAllowed proves the
// allowlist accepts the framework module root in go.mod's `require`
// line — the standard form the LLM emits. The bug was that the
// allowlist only had sub-package prefixes, so the module root
// (e.g. `require github.com/dora-network/dora-strategy-wasm v0.0.0`) was
// rejected even though every sub-package under it was allowed.
func TestValidate_WasmTarget_ModuleRootRequireAllowed(t *testing.T) {
	files := validFiles()
	files["go.mod"] = "module example\n\ngo 1.26.5\nrequire github.com/dora-network/dora-strategy-wasm v0.0.0\n"
	files["manifest.json"] = `{"schema_version":1,"module_name":"example","framework_version":"` + prompts.WasmFrameworkVersion + `","capabilities":{"order_books":["btcusd"],"host_functions":["host_log"]}}`
	v := &Validator{cfg: Config{
		WasmPrepare:  noopWasmPrepare,
		TinyGoRunner: &fakeTinyGoRunner{wasm: minimalWasm},
	}}
	res, err := v.Validate(t.Context(), files)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !res.BuildOK {
		t.Fatalf("module-root require for framework should build, got %+v", res)
	}
	for _, d := range res.Diagnostics {
		t.Errorf("unexpected diagnostic: %s", d.Message)
	}
}

// TestContentHash_Stable confirms the manifest content-hash helper is
// deterministic so the same manifest produces the same reference.
func TestContentHash_Stable(t *testing.T) {
	a := contentHash([]byte(`{"schema_version":1}`))
	b := contentHash([]byte(`{"schema_version":1}`))
	if a != b {
		t.Errorf("contentHash not stable: %q != %q", a, b)
	}
	if c := contentHash([]byte(`{"schema_version":2}`)); c == a {
		t.Errorf("contentHash collided on different inputs: %q", a)
	}
}

// noopWasmPrepare is the test seam for Config.WasmPrepare. The wasm
// validator now requires a real go toolchain + proxy access for its
// prepare step, which unit tests can't provide. This no-op lets the
// wasm path tests run in the same synthetic environment as the docker
// path tests.
func noopWasmPrepare(_ context.Context, _, _ string) error { return nil }
