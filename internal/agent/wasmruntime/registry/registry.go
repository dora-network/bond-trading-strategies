// Package registry owns the wazero runtime. It loads .wasm blobs
// + manifest.json sidecars from the store, validates the manifest,
// compiles the module (cached by content hash), and instantiates
// per-job plugin instances on FRESH per-call wazero runtimes.
//
// Why per-call runtimes: TinyGo-emitted plugins bind their imports
// to the literal host module name "env" (via //go:wasmimport env
// host_*). wazero's Store rejects a second module registered under
// a name it has already seen — so two backtests on the same
// runtime would collide on "env". The cleanest fix is per-call
// Runtimes: each Load returns an Instance carrying its own
// runtime; each runtime gets its own private "env" registration.
//
// Compilation results are shared across all these Runtimes via a
// single wazero.CompilationCache owned by the Registry. Without
// the cache, a CompiledModule compiled on one Runtime's engine is
// NOT instantiable on a different Runtime's engine (wazero binds
// the compiled module to its compiling engine), so each Load
// would re-compile the .wasm from scratch. With the cache,
// consecutive Loads share the compiled artifact and only the
// per-Runtime InstantiateModule is paid twice. (An alternative is
// `NewCompilationCacheWithDir` for disk persistence across
// process restarts; in-memory is sufficient for the single agent
// process.)
package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/dora-network/dora-strategy-wasm/manifest"
)

// Constants

// shutdownGrace is the hard upper bound on per-instance runtime
// teardown. Independent of the Registry's parent so close completes
// even when the caller's parent is already cancelled (agent shutdown).
const shutdownGrace = 5 * time.Second

// DefaultMemoryLimitBytes is the per-instance memory cap used
// when Config.MemoryLimitBytes is zero. 64 MiB is a defensible
// default for the validation smoke and for early
// per-job backtests; live strategies on the order side may
// need a higher cap and can override via Config. The constant
// is exported so callers (e.g. tests) can reference the same
// value the Registry falls back to.
const DefaultMemoryLimitBytes uint64 = 64 * 1024 * 1024

// wasmPageSize is the WebAssembly page size: 65536 bytes (2^16).
// wazero's WithMemoryLimitPages counts in pages, not bytes.
const wasmPageSize = 64 * 1024

// Variables

// ErrAlreadyClosed is returned by Load after the Registry is closed.
var ErrAlreadyClosed = errors.New("registry: already closed")

// Types

// Config is the runtime configuration.
type Config struct {
	// MemoryLimitBytes is the per-instance memory cap. The default
	// (when zero) is DefaultMemoryLimitBytes (64 MiB).
	MemoryLimitBytes uint64
}

// Registry owns the artifact store, the shared compilation cache,
// and the per-call runtime factory. It does NOT own a long-lived
// wazero.Runtime: every Load spins up a fresh Runtime that shares
// the Registry's compilation cache, the Instance carries the
// Runtime, and Instance.Close tears it down. See package doc.
type Registry struct {
	cfg Config

	// cache is shared across all per-call Runtimes so a
	// CompiledModule compiled on one Runtime's engine can be
	// re-instantiated on another without recompiling. wazero
	// binds compiled modules to their compiling engine, so the
	// only way to share across engines is through this cache.
	cache wazero.CompilationCache

	mu    sync.Mutex
	comps map[string]wazero.CompiledModule

	ctx       context.Context
	ctxCancel context.CancelFunc

	closeOnce sync.Once
	closeErr  error
}

// Instance is a per-job plugin instance. It owns the wazero.Runtime
// the guest was loaded into; Instance.Close closes both the guest
// module and the runtime.
type Instance struct {
	runtime  wazero.Runtime
	module   wazero.CompiledModule
	config   wazero.ModuleConfig
	Manifest manifest.Manifest

	closeMu sync.Mutex
	closed  bool
}

// Functions

// New constructs a Registry. The caller's context is the parent of
// the Registry's lifetime context, used to abort in-flight work on
// shutdown. Allocates a single in-memory wazero.CompilationCache
// shared across every per-call Runtime Load produces.
func New(ctx context.Context, cfg Config) (*Registry, error) {
	if cfg.MemoryLimitBytes == 0 {
		cfg.MemoryLimitBytes = DefaultMemoryLimitBytes
	}
	lifetime, cancel := context.WithCancel(ctx)
	return &Registry{
		cfg:       cfg,
		cache:     wazero.NewCompilationCache(),
		comps:     map[string]wazero.CompiledModule{},
		ctx:       lifetime,
		ctxCancel: cancel,
	}, nil
}

// Close releases compiled modules, the shared compilation cache,
// and signals any in-flight work to stop via the lifetime context.
// Idempotent. Uses a fresh bounded teardown context (not the
// Registry's parent, which is often already cancelled during agent
// shutdown) so teardown actually completes.
func (r *Registry) Close() error {
	r.closeOnce.Do(func() {
		r.ctxCancel()
		teardown, teardownCancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer teardownCancel()
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, c := range r.comps {
			_ = c.Close(teardown)
		}
		r.comps = nil
		if r.cache != nil {
			_ = r.cache.Close(teardown)
		}
	})
	return r.closeErr
}

// newRuntime creates a fresh wazero.Runtime for one Instance. The
// runtime is configured with the Registry's MemoryLimitBytes cap
// and the shared CompilationCache, and has wasi_snapshot_preview1
// instantiated (TinyGo plugins may emit WASI imports). The
// caller's context bounds any work inside wazero; closing the
// Instance closes this runtime.
func (r *Registry) newRuntime(ctx context.Context) (wazero.Runtime, error) {
	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
		//nolint:gosec // G115: MemoryLimitBytes is a configured cap; pages fit uint32.
		WithMemoryLimitPages(uint32(r.cfg.MemoryLimitBytes/wasmPageSize)).
		WithCompilationCache(r.cache))
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		_ = rt.Close(ctx)
		return nil, fmt.Errorf("registry: instantiate wasi_snapshot_preview1: %w", err)
	}
	return rt, nil
}

// Close releases the instance: closes the guest module and the
// per-call Runtime. Idempotent.
func (i *Instance) Close(ctx context.Context) {
	if i == nil {
		return
	}
	i.closeMu.Lock()
	defer i.closeMu.Unlock()
	if i.closed {
		return
	}
	if i.runtime != nil {
		_ = i.runtime.Close(ctx)
	}
	i.closed = true
}

// Init instantiates the loaded module with a no-op host module and
// calls the plugin's `init` export. Used by the validator's wasm
// path (Plan 4) and the cmd/wasm-smoke binary (Plan 3) as the
// single canonical "does this .wasm actually initialize?" check.
//
// The host module registers the framework's required + optional
// functions as no-ops (host_log, host_now, host_random, and the
// optional ones). The validator's wasm path doesn't wire the
// safety kernel or the order broker — it just needs to know
// that init runs cleanly. The orchestrator's live path uses a
// separate host module wired with real deps; this method is the
// smoke path only.
//
// The caller's context bounds the operations. If cancelled, wazero
// returns an error immediately and the module is closed.
func (r *Registry) Init(ctx context.Context, inst *Instance) error {
	if inst == nil {
		return errors.New("registry: nil instance")
	}
	host := NewHostModule(inst.runtime)
	if _, err := host.Builder.Instantiate(ctx); err != nil {
		return fmt.Errorf("registry: instantiate host module: %w", err)
	}
	cfg := inst.config.WithStartFunctions("_start")
	mod, err := inst.runtime.InstantiateModule(ctx, inst.module, cfg)
	if err != nil {
		return fmt.Errorf("registry: instantiate guest module: %w", err)
	}
	_ = mod.Close(ctx)
	return nil
}

// InstantiateWithHost instantiates the loaded module after the caller has
// built a host module (via buildHost) on the Instance's runtime. The host
// module's functions are closures capturing per-job state. Used by the
// backtest WasmStarter.
//
// The caller is responsible for closing the returned api.Module
// (and Instance.Close will close the Runtime + remaining guest
// module too).
func (r *Registry) InstantiateWithHost(ctx context.Context, inst *Instance, buildHost func(rt wazero.Runtime) error) (api.Module, error) {
	if err := buildHost(inst.runtime); err != nil {
		return nil, fmt.Errorf("registry: build host module: %w", err)
	}
	cfg := inst.config.WithStartFunctions("_start")
	mod, err := inst.runtime.InstantiateModule(ctx, inst.module, cfg)
	if err != nil {
		return nil, fmt.Errorf("registry: instantiate guest module: %w", err)
	}
	return mod, nil
}

// InstantiateWithHostDeferred is like InstantiateWithHost but does NOT
// call _start during instantiation. The caller must call _start
// manually (e.g., from a goroutine). Used by the live orchestrator,
// where _start blocks indefinitely in the candle loop.
func (r *Registry) InstantiateWithHostDeferred(
	ctx context.Context, inst *Instance, buildHost func(rt wazero.Runtime) error,
) (api.Module, error) {
	if err := buildHost(inst.runtime); err != nil {
		return nil, fmt.Errorf("registry: build host module: %w", err)
	}
	mod, err := inst.runtime.InstantiateModule(ctx, inst.module, inst.config)
	if err != nil {
		return nil, fmt.Errorf("registry: instantiate guest module: %w", err)
	}
	return mod, nil
}

// ArtifactStore is the store surface Load needs. *store.Store
// satisfies it; declaring it here lets backtest's WasmRuntime seam
// accept any store-like type without importing a concrete one.
type ArtifactStore interface {
	Get(wasmHash, manifestHash string) (wasm, manifest []byte, err error)
}

// Load compiles (or fetches from the cache) the WASM blob and
// returns an Instance backed by a fresh wazero.Runtime that shares
// the Registry's CompilationCache. Each Instance gets its own
// Runtime so consecutive instantiations on the same Registry do
// not collide on TinyGo's literal "env" host imports (see package
// doc). The caller is responsible for calling Instance.Close when
// done.
func (r *Registry) Load(ctx context.Context, st ArtifactStore, wasmHash, manifestHash string) (*Instance, error) {
	_, manifestBytes, err := st.Get(wasmHash, manifestHash)
	if err != nil {
		return nil, fmt.Errorf("registry: load from store: %w", err)
	}

	var m manifest.Manifest
	if err := m.UnmarshalJSON(manifestBytes); err != nil {
		return nil, fmt.Errorf("registry: parse manifest: %w", err)
	}
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("registry: manifest invalid: %w", err)
	}
	if err := CheckFrameworkVersion(m.FrameworkVersion, m.Capabilities.HostFunctions); err != nil {
		return nil, err
	}

	rt, err := r.newRuntime(ctx)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	comp, ok := r.comps[wasmHash]
	r.mu.Unlock()
	if !ok {
		wasmBytes, _, err := st.Get(wasmHash, manifestHash)
		if err != nil {
			_ = rt.Close(ctx)
			return nil, fmt.Errorf("registry: re-read wasm: %w", err)
		}
		comp, err = rt.CompileModule(ctx, wasmBytes)
		if err != nil {
			_ = rt.Close(ctx)
			return nil, fmt.Errorf("registry: compile: %w", err)
		}
		r.mu.Lock()
		// Check again under the lock to avoid overwriting a
		// concurrent compile.
		if existing, ok2 := r.comps[wasmHash]; ok2 {
			_ = comp.Close(ctx)
			comp = existing
		} else {
			r.comps[wasmHash] = comp
		}
		r.mu.Unlock()
	}

	mc := wazero.NewModuleConfig().
		WithName(m.ModuleName).
		WithStartFunctions()

	return &Instance{runtime: rt, module: comp, config: mc, Manifest: m}, nil
}
