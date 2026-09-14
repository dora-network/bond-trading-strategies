// Package validate implements the per-build host validator
// (spec §7 and §19). It takes a strategy file map, resolves its
// dependencies against a module proxy in a temp dir, compiles it
// in-process with TinyGo, runs a wazero smoke, and parses the
// JSON result contract.
package validate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm/prompts"

	wasmstore "github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"
)

// Diagnostic result-truncation caps (spec §19.5 step 8). Messages and
// captured stderr share the same 2 KiB ceiling.
const (
	maxDiagnostics  = 10
	maxMessageBytes = 2 * 1024 // 2 KiB per message / stderr snippet
)

// File permission constants (gosec G306 prefers <= 0600 for written files).
const (
	filePerm = 0o600
	dirPerm  = 0o755
)

// minRequireFields is the fewest tokens on a go.mod require line for it to
// name a module (path + version).
const minRequireFields = 2

// diagnosticCategoryAllowlist tags go.mod require lines stripped for being
// outside the curated dependency allowlist.
const diagnosticCategoryAllowlist = "allowlist"

// Config configures a Validator.
type Config struct {
	GOPROXY   string
	Allowlist []string
	// WasmArtifactStore receives the compiled wasm + manifest after a
	// successful go-wasm smoke. The backtest runner later reads the
	// artifact from this store by the returned wasm_ref/manifest_hash.
	// If nil, validateWasm keeps the artifact only in its temp store
	// (useful for tests that do not need on-disk persistence).
	WasmArtifactStore wasmstore.ArtifactStore
	// TinyGoBinary is the tinygo executable name/path. Defaults to "tinygo".
	TinyGoBinary string
	// WasmFrameworkVersion is the framework version the validate
	// path injects into the .wasm via -ldflags. Defaults to
	// prompts.WasmFrameworkVersion (the agent's known framework
	// version, bumped per release). Tests inject "vTEST" or
	// similar; the production wiring leaves it zero and New
	// wires the production default.
	WasmFrameworkVersion string
	// TinyGoRunner abstracts the TinyGo build invocation. Defaults to
	// a real exec.CommandContext runner; tests inject a fake.
	TinyGoRunner TinyGoRunner
	// prepare pins each allowlisted module to its latest version, then runs
	// `go mod tidy && go mod vendor` in the temp dir. It is an unexported
	// seam so unit tests avoid the real go toolchain; New wires the
	// production default. Callers building a Config directly (the production
	// wiring) leave it zero and New sets the default.
	prepare func(ctx context.Context, dir, goproxy string, allowlist []string) error

	// WasmPrepare resolves the wasm framework dependency from the
	// public module proxy (the docker path equivalent for the wasm
	// target). It strips the LLM-authored require, fetches the real
	// tagged version, and runs go mod tidy to populate go.sum.
	// Exposed as a public field so tests can inject a no-op; New
	// wires the production default.
	WasmPrepare func(ctx context.Context, dir, goproxy string) error
}

// Validator runs a per-build build+vet+test pass over a strategy file map.
type Validator struct{ cfg Config }

// New returns a Validator with the production tidy+vendor prepare step.
func New(cfg Config) *Validator {
	if cfg.TinyGoBinary == "" {
		cfg.TinyGoBinary = "tinygo"
	}
	if cfg.prepare == nil {
		cfg.prepare = defaultPrepare
	}
	if cfg.TinyGoRunner == nil {
		version := cfg.WasmFrameworkVersion
		if version == "" {
			version = prompts.WasmFrameworkVersion
		}
		cfg.TinyGoRunner = defaultTinyGoRunner{
			binary:  cfg.TinyGoBinary,
			version: version,
		}
	}
	if cfg.WasmPrepare == nil {
		cfg.WasmPrepare = defaultWasmPrepare
	}
	return &Validator{cfg: cfg}
}

// Result is the JSON-contract outcome of one validation pass (spec §19.3).
// ImageRef is the local-daemon image tag produced by `docker build`
// (Phase 2); empty when no image was built (logic failure) or when
// the build itself returned a tag we could not capture.
type Result struct {
	BuildOK      bool         `json:"build_ok"`
	VetOK        bool         `json:"vet_ok"`
	TestsOK      bool         `json:"tests_ok"`
	GoVersion    string       `json:"go_version"`
	DurationMs   int64        `json:"duration_ms"`
	Diagnostics  []Diagnostic `json:"diagnostics"`
	ImageRef     string       `json:"image_ref,omitempty"`
	WasmRef      string       `json:"wasm_ref,omitempty"`
	ManifestHash string       `json:"manifest_hash,omitempty"`
}

// Diagnostic is one build/vet/finding: a module-relative path, line,
// column, error category, and a truncated message.
type Diagnostic struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	Column   int    `json:"column"`
	Category string `json:"category"`
	Message  string `json:"message"`
}

// ErrInfra signals an infrastructure failure (temp dir, vendoring, image
// build, non-zero container exit). Non-repairable per spec §9.
var ErrInfra = errors.New("validate: infrastructure")

// compilerMarkerRE matches a Go compiler output marker of the form
// `path.go:line:col:`. Both line and column must be present so that
// daemon errors with paths like `container_linux.go:380` (which
// carry only a line number, no column) don't false-positive into
// the logic-failure branch. Compiled once at package load.
var compilerMarkerRE = regexp.MustCompile(`\.go:\d+:\d+:`)

// Validate enforces the wasm allowlist over the go.mod, then runs the
// in-process TinyGo + wazero smoke. Logic failures never surface as
// errors; only infra failures do.
func (v *Validator) Validate(ctx context.Context, files map[string]string) (Result, error) {
	slog.Info("validate: start", "files", len(files), "allowlist", v.cfg.Allowlist, "goproxy", v.cfg.GOPROXY)
	rejected := enforceWasmAllowlist(files)
	if len(rejected) > 0 {
		r := logicFailureCategorized(diagnosticCategoryAllowlist, rejected[0].Message)
		r.Diagnostics = append(r.Diagnostics, rejected[1:]...)
		return r, nil
	}
	return validateWasm(ctx, v.cfg, files)
}

// classifySmokeFailure returns the Diagnostic category for a non-zero
// smoke run. "framework" is returned when the stderr starts with the
// framework's "dorastrategy:" prefix — that's the framework's own
// logger speaking (env-var complaints, mode-dispatch errors, candle
// fetch failures, callback POST failures), not the strategy. Anything
// else is treated as a strategy bug the model can repair.
//
// ponytail: prefix-match on the framework's logger. The previous
// per-string allowlist ("mode not implemented" only) was tight enough
// to misroute the next framework error (BACKTEST_CALLBACK_URL scheme
// check) into the strategy-repair bucket; matching on the prefix
// future-proofs against every new framework error string.
func classifySmokeFailure(stderr []byte) string {
	msg := strings.TrimSpace(string(stderr))
	if strings.HasPrefix(msg, "dorastrategy:") {
		return "framework"
	}
	return "output"
}

// logicFailureCategorized is logicFailure with a non-default Category
// on the synthetic diagnostic. Used by the per-mode smoke loop when
// the failure pattern is recognized as a framework bug rather than
// a strategy bug, so the LLM can skip the repair loop and report
// the framework version instead.
func logicFailureCategorized(category, message string) Result {
	r := Result{BuildOK: false, VetOK: false, TestsOK: false}
	if msg := strings.TrimSpace(message); msg != "" {
		r.Diagnostics = []Diagnostic{{Category: category, Message: truncate(msg)}}
		r.Diagnostics = capDiagnostics(r.Diagnostics)
	}
	return r
}

// isCompilerOutput reports whether the captured build error looks like
// Go compiler/toolchain output so the validator can route it to the
// repairable logic-failure branch instead of the terminal infra
// branch. The file:line:col marker is the strongest signal; the
// others are backup filters that cover module-resolution and
// toolchain errors which never emit a file:line marker. Daemon /
// OOM / pull errors do not match any of these and fall through to
// the infra branch.
//
// New markers added after the strategy3 retry loop showed that a
// "cannot find package" / "missing go.sum entry" / "go.mod requires
// go X" error gets routed to infra because the file:line marker
// wasn't present; the LLM then saw "verified:false, diagnostics:null"
// and concluded it couldn't fix anything. Each addition here came
// from a real production error string observed in the agent log.
func isCompilerOutput(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return compilerMarkerRE.MatchString(msg) ||
		strings.Contains(msg, "undefined:") ||
		strings.Contains(msg, "cannot find package") ||
		strings.Contains(msg, "missing go.sum entry") ||
		strings.Contains(msg, "go.mod requires") ||
		strings.Contains(msg, "unknown field ") || // struct literal mismatch
		strings.Contains(msg, "# ") // '# pkg' marker on a fresh line
}

// logicFailure folds an arbitrary error message into a repairable
// Result: all flags false, one synthetic diagnostic carrying the
// captured output. The handler hands the diagnostics to the model.
func logicFailure(message string) Result {
	r := Result{BuildOK: false, VetOK: false, TestsOK: false}
	if msg := strings.TrimSpace(message); msg != "" {
		r.Diagnostics = []Diagnostic{{Category: "output", Message: truncate(msg)}}
		r.Diagnostics = capDiagnostics(r.Diagnostics)
	}
	return r
}

// writeFiles writes the file map into dir. Paths are module-relative and
// were validated upstream (Task 18); safeJoin is a defense-in-depth guard
// against any path escaping the root.
func writeFiles(dir string, files map[string]string) error {
	for name, content := range files {
		clean := safeJoin(dir, name)
		if mkerr := os.MkdirAll(filepath.Dir(clean), dirPerm); mkerr != nil {
			return fmt.Errorf("mkdir %q: %w", name, mkerr)
		}
		if werr := os.WriteFile(clean, []byte(content), filePerm); werr != nil {
			return fmt.Errorf("write %q: %w", name, werr)
		}
	}
	return nil
}

// requireModule parses a require line and returns the module path and true
// if the line is a require entry (either `require mod ver` single-line or a
// block entry `mod ver`). It returns ("", false) otherwise. A bare token
// pair is only a require entry when its first token is not a go.mod
// structural directive (module, go, toolchain, require, replace, exclude,
// retract, use, tool): real module paths never equal those keywords, and
// excluding them stops `module example` / `go 1.26.5` from being misread as
// require entries and stripped by enforceAllowlist.
func requireModule(line string) (string, bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "//") {
		return "", false
	}
	var rest string
	if strings.HasPrefix(line, "require ") {
		rest = strings.TrimSpace(strings.TrimPrefix(line, "require "))
	} else {
		rest = line
	}
	if rest == "(" || rest == ")" {
		return "", false
	}
	// first whitespace-separated token is the module path.
	fields := strings.Fields(rest)
	if len(fields) < minRequireFields {
		return "", false
	}
	// A bare token pair is only a require entry when its first token is not
	// a go.mod structural directive: real module paths never equal these
	// keywords, and excluding them stops `module example` / `go 1.26.5`
	// from being misread as require entries and stripped by
	// enforceAllowlist.
	switch fields[0] {
	case "module", "go", "toolchain", "require", "replace", "exclude", "retract", "use", "tool":
		return "", false
	}
	return fields[0], true
}

// capDiagnostics caps the slice to maxDiagnostics entries and truncates
// each message to maxMessageBytes (spec §19.5 step 8).
func capDiagnostics(in []Diagnostic) []Diagnostic {
	if len(in) > maxDiagnostics {
		in = in[:maxDiagnostics]
	}
	for i := range in {
		in[i].Message = truncate(in[i].Message)
	}
	return in
}

// truncate keeps both the head and tail of s when the full message
// is too long for the LLM-facing diagnostic. The OLD behavior kept
// only the head; for docker build output the head is dominated
// by buildkit progress lines ("#1 [internal] load metadata...")
// and the actual compile error at the tail was clipped out. The
// LLM then saw only progress noise and classified a real compile
// error as an infra failure. The head gives the LLM context
// (which step the build reached); the tail carries the actionable
// error. The middle is dropped; both halves are bounded by
// maxMessageBytes/4 runes each.
func truncate(s string) string {
	if len(s) <= maxMessageBytes {
		return s
	}
	// ponytail: rune-safe rough clip; exact byte boundary is not load-bearing
	// for a truncated diagnostic message. head + tail.
	const headTailQuarter = maxMessageBytes / 4 // head and tail each get a quarter
	half := headTailQuarter
	runes := []rune(s)
	if len(runes) <= 2*half {
		return s
	}
	head := string(runes[:half])
	tail := string(runes[len(runes)-half:])
	return head + "\n... (truncated middle) ...\n" + tail
}

// defaultPrepare pins each allowlisted module to its latest version, then runs
// `go mod tidy && go mod vendor` in dir against the configured GOPROXY
// (spec §19.5 step 4). The model authors go.mod but cannot know the right
// dependency version, so each allowlisted require is repinned to @latest
// before resolution.
func defaultPrepare(ctx context.Context, dir, goproxy string, allowlist []string) error {
	// Strip the $$NAME:HASH$$ secret-redaction wrapper that the agent
	// uses for module paths in logs and LLM output. Real module paths
	// are like 'github.com/foo/bar'; the wrapper is presentation, not
	// identity, and confuses `go get` and `go mod tidy`.
	for _, raw := range allowlist {
		m := unwrapModulePath(raw)
		// Drop any version the model authored before resolving @latest: go get
		// @latest loads the current module graph, which fails if an existing
		// require pins a version that does not exist (the model cannot know it).
		//nolint:gosec // allowlist entries are operator-configured (env/config), not user input
		drop := exec.CommandContext(ctx, "go", "mod", "edit", "-droprequire="+m)
		drop.Dir = dir
		drop.Env = append(os.Environ(), "GOPROXY="+goproxy)
		if out, err := drop.CombinedOutput(); err != nil {
			return fmt.Errorf("droprequire %s: %w\n%s", m, err, truncate(string(out)))
		}
		//nolint:gosec // allowlist entries are operator-configured (env/config), not user input
		cmd := exec.CommandContext(ctx, "go", "get", m+"@latest")
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOPROXY="+goproxy)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("get latest %s: %w\n%s", m, err, truncate(string(out)))
		}
	}
	for _, args := range [][]string{
		{"mod", "tidy"},
		{"mod", "vendor"},
	} {
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOFLAGS=-mod=vendor", "GOPROXY="+goproxy)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("%s: %w\n%s", strings.Join(args, " "), err, truncate(string(out)))
		}
	}
	return nil
}

// allowlist entries are the real paths; the wrapper is only a log/
// LLM-output convention. A path that does not start with the
// wrapper is returned unchanged.
// --- small path helpers (kept unexported so tests can reuse them) ---

func unwrapModulePath(s string) string {
	const wrapper = "$$"
	idx := strings.Index(s, wrapper)
	if idx < 0 {
		return s
	}
	end := strings.Index(s[idx+len(wrapper):], wrapper)
	if end < 0 {
		return s
	}
	return s[idx+len(wrapper) : idx+len(wrapper)+end]
}

// safeJoin joins name under base, forcing the result under base regardless
// of any ".." segments (defense-in-depth on top of upstream validation).
func safeJoin(base, name string) string {
	cleaned := filepath.Clean("/" + name) // force under root
	return filepath.Join(base, cleaned)
}
