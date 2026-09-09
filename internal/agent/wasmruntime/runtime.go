// Package wasmruntime is the top-level wiring for the WASM plugin
// runtime. It exposes a single Runtime type that the server's
// main.go instantiates at startup. The Runtime owns the artifact
// store, the wazero registry, and the read broker, write broker,
// and safety kernel.
package wasmruntime

import (
	"context"
	"fmt"
	"os"

	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"
)

// Runtime is the top-level WASM runtime.
type Runtime struct {
	st  *store.Store
	reg *registry.Registry
}

// NewRuntime constructs a Runtime. The runtime is always enabled; the
// AGENT_WASM_RUNTIME_ENABLED feature flag that gated this construction
// was removed when the docker pipeline was dropped on 2026-09-04.
//
// The caller's context is the parent of the Registry's lifetime
// context. The agent passes its signal-bound context here so that
// SIGINT propagates to in-flight wazero work.
func NewRuntime(ctx context.Context) (*Runtime, error) {
	root := os.Getenv("AGENT_WASM_ARTIFACT_ROOT")
	if root == "" {
		return nil, fmt.Errorf("wasmruntime: AGENT_WASM_ARTIFACT_ROOT is required")
	}
	st, err := store.New(root)
	if err != nil {
		return nil, fmt.Errorf("wasmruntime: artifact store: %w", err)
	}
	reg, err := registry.New(ctx, registry.Config{})
	if err != nil {
		return nil, fmt.Errorf("wasmruntime: registry: %w", err)
	}
	return &Runtime{st: st, reg: reg}, nil
}

// Store returns the artifact store.
func (r *Runtime) Store() *store.Store {
	if r == nil {
		return nil
	}
	return r.st
}

// Registry returns the wazero registry.
func (r *Runtime) Registry() *registry.Registry {
	if r == nil {
		return nil
	}
	return r.reg
}

// Close releases the runtime. Idempotent. The Registry's Close
// uses a fresh bounded context, so this call completes even
// when the runtime's parent context is already cancelled
// (agent shutdown).
func (r *Runtime) Close() error {
	if r == nil || r.reg == nil {
		return nil
	}
	return r.reg.Close()
}
