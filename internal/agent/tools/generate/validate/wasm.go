package validate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"

	"github.com/dora-network/dora-strategy-wasm/allowlist"
)

// tinygoDockerfile is the Dockerfile used by the wasm docker path.
// It is retained for compatibility with the Plan 4 test surface.
const tinygoDockerfile = `FROM tinygo/tinygo:0.35.0 AS build
WORKDIR /src
COPY . .
RUN tinygo build -target=wasi -o plugin.wasm .

FROM gcr.io/distroless/base-debian12 AS runtime
COPY --from=build /src/plugin.wasm /plugin.wasm
ENTRYPOINT ["/plugin.wasm"]
`

// TinyGoRunner abstracts the TinyGo invocation so tests can fake the
// wasm build without installing tinygo.
type TinyGoRunner interface {
	// Build compiles the Go package in dir to a wasm binary at out.
	Build(ctx context.Context, dir, out string) error
}

// wasmFrameworkModulePath is the module path of the strategywasm
// framework package whose FrameworkVersion the agent's validate
// path injects at build time. The ldflags target is the import
// path used by generated strategy code.
const wasmFrameworkModulePath = "github.com/dora-network/dora-strategy-wasm/dorastrategy"

// defaultTinyGoRunner shells out to the tinygo binary.
type defaultTinyGoRunner struct {
	binary  string
	version string // framework version to inject via -ldflags
}

// Build runs `tinygo build -o <out> -target wasi .` in dir with
// -ldflags "-X <wasmFrameworkModulePath>.FrameworkVersion=<version>"
// so the compiled .wasm records the framework version the agent
// expects. The version is prompts.WasmFrameworkVersion; without the
// injection the framework var keeps its "dev" default and the wasm
// would write "dev" into its manifest's framework_version field,
// which the host's CheckFrameworkVersion (comparing against
// prompts.WasmFrameworkVersion) would reject at load time.
func (r defaultTinyGoRunner) Build(ctx context.Context, dir, out string) error {
	ldflags := fmt.Sprintf("-X %s.FrameworkVersion=%s", wasmFrameworkModulePath, r.version)
	//nolint:gosec // G204: args come from operator config, not user input
	cmd := exec.CommandContext(ctx, r.binary, "build", "-o", out, "-target", "wasi", "-ldflags", ldflags, ".")
	cmd.Dir = dir
	outBytes, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("tinygo build: %w\n%s", err, outBytes)
	}
	return nil
}

// wasmFrameworkModuleRoot is the wasm framework's module root, used to
// resolve the framework dependency from the public module proxy. The
// strategywasm repo at the WasmFrameworkVersion tag (defined in
// internal/llm/prompts) is a real published module; the LLM's go.mod
// may pin a hallucinated version (e.g. v0.0.5-alpha on the docker
// path), which go mod tidy then fails to resolve with "unknown
// revision". The prep step strips the LLM's require line, fetches the
// real @latest, and re-locks go.sum so the tinygo build has a real
// module.
const wasmFrameworkModuleRoot = "github.com/dora-network/dora-strategy-wasm"

// defaultWasmPrepare is the production wasm prepare step. It strips
// the LLM's require for the wasm framework, fetches the real @latest
// from the public proxy, and runs go mod tidy to lock the version
// into go.sum. The module path constant carries the $$NAME:HASH$$
// log-redaction wrapper; unwrapModulePath strips it before passing
// to the Go toolchain (which doesn't understand the wrapper).
//
// Tests inject a no-op via Config.WasmPrepare so they don't need
// a real go toolchain or proxy access.
func defaultWasmPrepare(ctx context.Context, dir, goproxy string) error {
	realPath := unwrapModulePath(wasmFrameworkModuleRoot)
	// Drop any existing require for the wasm framework module. The LLM
	// may pin a hallucinated version; the drop+get @latest pattern
	// forces a real resolution from the public proxy.
	//nolint:gosec // module path is a constant, not user input
	drop := exec.CommandContext(ctx, "go", "mod", "edit", "-droprequire="+realPath)
	drop.Dir = dir
	_ = drop.Run()
	//nolint:gosec // same
	cmd := exec.CommandContext(ctx, "go", "get", realPath+"@latest")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOPROXY="+goproxy)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("get latest %s: %w\n%s", realPath, err, truncate(string(out)))
	}
	// go mod tidy locks the resolved version into go.sum so the
	// subsequent tinygo build has a real module path to resolve.
	tidy := exec.CommandContext(ctx, "go", "mod", "tidy")
	tidy.Dir = dir
	tidy.Env = append(os.Environ(), "GOPROXY="+goproxy)
	if out, err := tidy.CombinedOutput(); err != nil {
		return fmt.Errorf("go mod tidy: %w\n%s", err, truncate(string(out)))
	}
	return nil
}

// validateWasm compiles the strategy to wasm and runs the in-process
// wazero smoke. It returns a logic-failure Result on build/init errors
// and an infra error only for filesystem/runtime setup failures.
func validateWasm(ctx context.Context, cfg Config, files map[string]string) (Result, error) {
	if cfg.TinyGoBinary == "" {
		cfg.TinyGoBinary = "tinygo"
	}
	if cfg.TinyGoRunner == nil {
		cfg.TinyGoRunner = defaultTinyGoRunner{binary: cfg.TinyGoBinary}
	}

	tempDir, err := os.MkdirTemp("", "agent-validator-wasm-*")
	if err != nil {
		return Result{}, fmt.Errorf("%w: mkdirtemp: %w", ErrInfra, err)
	}

	defer os.RemoveAll(tempDir)

	if err := writeFiles(tempDir, files); err != nil {
		return Result{}, fmt.Errorf("%w: write files: %w", ErrInfra, err)
	}

	// Resolve the wasm framework dependency from the public proxy so
	// the LLM-authored go.mod (which may pin a hallucinated version)
	// is replaced with the real tagged release before tinygo build.
	slog.Info("validate: wasm prepare start")
	if err := cfg.WasmPrepare(ctx, tempDir, cfg.GOPROXY); err != nil {
		slog.Info("validate: wasm prepare FAILED", "err", err.Error())
		return Result{}, fmt.Errorf("%w: prepare: %w", ErrInfra, err)
	}
	slog.Info("validate: wasm prepare ok")

	manifestContent, ok := files["manifest.json"]
	if !ok {
		return Result{}, fmt.Errorf("%w: manifest.json missing", ErrInfra)
	}

	wasmPath := filepath.Join(tempDir, "main.wasm")
	slog.Info("validate: tinygo build start")
	if err := cfg.TinyGoRunner.Build(ctx, tempDir, wasmPath); err != nil {
		slog.Info("validate: tinygo build FAILED", "err", err.Error())
		return logicFailure(truncate(err.Error())), nil
	}
	slog.Info("validate: tinygo build ok")

	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		slog.Error("validate: read wasm FAILED", "err", err.Error())
		return Result{}, fmt.Errorf("%w: read wasm: %w", ErrInfra, err)
	}

	st, err := store.New(filepath.Join(tempDir, "wasm-store"))
	if err != nil {
		slog.Error("validate: temp wasm store FAILED", "err", err.Error())
		return Result{}, fmt.Errorf("%w: temp wasm store: %w", ErrInfra, err)
	}
	wasmHash, manifestHash, err := st.Put(wasmBytes, []byte(manifestContent))
	if err != nil {
		slog.Error("validate: store put FAILED", "err", err.Error())
		return Result{}, fmt.Errorf("%w: store put: %w", ErrInfra, err)
	}

	reg, err := registry.New(ctx, registry.Config{})
	if err != nil {
		slog.Error("validate: registry new FAILED", "err", err.Error())
		return Result{}, fmt.Errorf("%w: registry new: %w", ErrInfra, err)
	}
	defer reg.Close()

	inst, err := reg.Load(ctx, st, wasmHash, manifestHash)
	if err != nil {
		slog.Info("validate: registry load FAILED (repairable)", "err", err.Error())
		return logicFailure(truncate(err.Error())), nil
	}
	defer inst.Close(ctx)

	slog.Info("validate: wasm init start")
	if err := reg.Init(ctx, inst); err != nil {
		slog.Info("validate: wasm init FAILED", "err", err.Error())
		return logicFailure(truncate(err.Error())), nil
	}
	slog.Info("validate: wasm init ok")

	// Persist the verified artifact to the production store so the
	// backtest runner can load it later by wasm_ref/manifest_hash.
	if cfg.WasmArtifactStore != nil {
		if _, _, err := cfg.WasmArtifactStore.Put(wasmBytes, []byte(manifestContent)); err != nil {
			return Result{}, fmt.Errorf("%w: persist wasm artifact: %w", ErrInfra, err)
		}
	}

	return Result{
		BuildOK:      true,
		VetOK:        true,
		TestsOK:      true,
		GoVersion:    runtime.Version(),
		WasmRef:      wasmHash,
		ManifestHash: manifestHash,
	}, nil
}

// enforceWasmAllowlist checks every require entry in files["go.mod"]
// against the WASM dependency allowlist. It returns one Diagnostic per
// rejected module and does not rewrite the map.
func enforceWasmAllowlist(files map[string]string) []Diagnostic {
	mod, ok := files["go.mod"]
	if !ok {
		return nil
	}
	var out []Diagnostic
	for _, line := range strings.Split(mod, "\n") {
		modPath, isReq := requireModule(line)
		if !isReq {
			continue
		}
		if allowlist.IsAllowed(modPath) {
			continue
		}
		out = append(out, Diagnostic{
			File:     "go.mod",
			Category: diagnosticCategoryAllowlist,
			Message: fmt.Sprintf(
				"module %q is not in the WASM dependency allowlist", modPath,
			),
		})
	}
	return out
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// contentHash returns a deterministic sha256 hex digest of b.
func contentHash(b []byte) string {
	return sha256Hex(b)
}
