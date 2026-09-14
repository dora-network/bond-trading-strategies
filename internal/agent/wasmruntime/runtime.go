// Package wasmruntime is the top-level wiring for the WASM plugin
// runtime. It exposes a single Runtime type that the server's
// main.go instantiates at startup. The Runtime owns the artifact
// store, the wazero registry, and the read broker, write broker,
// and safety kernel.
package wasmruntime

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store/pgstore"
)

// Runtime is the top-level WASM runtime.
type Runtime struct {
	st  store.ArtifactStore
	reg *registry.Registry
}

// NewRuntime constructs a Runtime.
//
// The artifact store is the Postgres-backed pgstore, which fronts a
// local on-disk CAS at AGENT_WASM_ARTIFACT_ROOT. The pgstore reads
// durable bytes from agent.wasm_artifacts / agent.wasm_manifests on a
// local-CAS miss (cold Fargate restart) and re-materializes to the
// local cache; hot reads stay on disk.
//
// pool == nil falls back to the plain FS store. This keeps
// hermetic tests (no DATABASE_URL) working without skipping —
// useful for callers that exercise Registry without caring about
// durable artifact persistence. Production wiring always passes a
// non-nil pool (see internal/agent/wiring).
func NewRuntime(ctx context.Context, pool *pgxpool.Pool) (*Runtime, error) {
	root := os.Getenv("AGENT_WASM_ARTIFACT_ROOT")
	if root == "" {
		return nil, fmt.Errorf("wasmruntime: AGENT_WASM_ARTIFACT_ROOT is required")
	}
	fs, err := store.New(root)
	if err != nil {
		return nil, fmt.Errorf("wasmruntime: local CAS: %w", err)
	}
	var st store.ArtifactStore
	if pool == nil {
		// ponytail: dev convenience, not a production path. Logs at
		// warn so accidental nil-pool production wiring is visible.
		slog.Default().Warn("wasmruntime: nil pool — using FS-only store; " +
			"compiled wasm artifacts will not survive process restarts")
		st = fs
	} else {
		st = pgstore.New(pool, fs, slog.Default())
	}
	reg, err := registry.New(ctx, registry.Config{})
	if err != nil {
		return nil, fmt.Errorf("wasmruntime: registry: %w", err)
	}
	return &Runtime{st: st, reg: reg}, nil
}

// Store returns the artifact store. Callers receive the interface so
// they don't depend on the concrete pgstore type — see
// wasmruntime/store.ArtifactStore.
func (r *Runtime) Store() store.ArtifactStore {
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
