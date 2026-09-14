package backtest

import (
	"context"

	registry "github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/registry"
	doraclient "github.com/dora-network/dora-client-go/doraclient"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// WasmRuntime is the wasmruntime seam used by WasmStarter. The
// concrete *registry.Registry (L5) satisfies it; tests can fake it.
type WasmRuntime interface {
	Load(ctx context.Context, st registry.ArtifactStore, wasmHash, manifestHash string) (*registry.Instance, error)
	InstantiateWithHost(ctx context.Context, inst *registry.Instance, buildHost func(rt wazero.Runtime) error) (api.Module, error)
}

// WasmInstance is a loaded WASM artifact bound to its own per-call
// runtime. Alias of registry.Instance; Close releases the runtime.
type WasmInstance = registry.Instance

// WasmArtifactStore is the artifact-store subset WasmRuntime.Load
// needs. Alias of registry.ArtifactStore; *wasmruntime store.Store
// satisfies it.
type WasmArtifactStore = registry.ArtifactStore

// withAPIKey returns a context carrying apiKey in the SDK's
// ContextAPIKeys slot, Authorization: ApiKey <key>. Ported from
// dora-agent internal/tools/dora.WithAPIKey.
func withAPIKey(ctx context.Context, apiKey string) context.Context {
	return context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		"apiKeyAuthHeader": {Key: apiKey, Prefix: "ApiKey"},
	})
}
