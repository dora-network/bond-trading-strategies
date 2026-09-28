package deployment_test

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

// fakeVersions is an in-memory strategies.Store for tool-level tests.
// The strategies_pgstore tests cover the SQL layer; the tools only
// need GetStrategy (ownership gate) + GetVersion (resolve wasm ref).
type fakeVersions struct {
	mu       sync.Mutex
	strats   map[string]strategies.Strategy // key = strategyID, value presence == ownership
	versions map[string]strategies.Version  // key = strategyID+"|"+revision
}

func newFakeVersions() *fakeVersions {
	return &fakeVersions{
		strats:   map[string]strategies.Strategy{},
		versions: map[string]strategies.Version{},
	}
}

// addVersion records a version so GetVersion can return it. Target is
// go-wasm so the tool's "no compiled wasm" gate passes.
func (f *fakeVersions) addVersion(strategyID, revision, wasmRef, manifestHash string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.strats[strategyID] = strategies.Strategy{ID: strategyID}
	f.versions[strategyID+"|"+revision] = strategies.Version{
		StrategyID:   strategyID,
		Revision:     strategies.Revision(revision),
		WasmRef:      wasmRef,
		ManifestHash: manifestHash,
		Target:       "go-wasm",
	}
}

func (f *fakeVersions) GetStrategy(_ context.Context, userID, strategyID string) (strategies.Strategy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.strats[strategyID]
	if !ok {
		return strategies.Strategy{}, strategies.ErrNotFound
	}
	if s.UserID != "" && s.UserID != userID {
		return strategies.Strategy{}, strategies.ErrNotFound
	}
	s.UserID = userID // tag ownership lazily so addVersion stays a one-liner
	f.strats[strategyID] = s
	return s, nil
}

func (f *fakeVersions) GetVersion(_ context.Context, strategyID string, rev strategies.Revision) (strategies.Version, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.versions[strategyID+"|"+string(rev)]
	if !ok {
		return strategies.Version{}, strategies.ErrNotFound
	}
	return v, nil
}

// The remaining Store methods aren't used by the deployment tools;
// returning ErrNotFound makes any accidental call surface loudly
// rather than silently succeed.
func (f *fakeVersions) Capture(context.Context, string, string, string, string, strategies.Meta, map[string]string, string) (strategies.Version, error) {
	return strategies.Version{}, errors.New("fake: Capture not implemented")
}

func (f *fakeVersions) CaptureWASM(context.Context, string, string, string, string, strategies.Meta, map[string]string, string, string) (strategies.Version, error) {
	return strategies.Version{}, errors.New("fake: CaptureWASM not implemented")
}

func (f *fakeVersions) StashPending(context.Context, string, string, string, string, strategies.Meta, map[string]string, string) error {
	return errors.New("fake: StashPending not implemented")
}

func (f *fakeVersions) CapturePending(context.Context, string) (strategies.Version, error) {
	return strategies.Version{}, errors.New("fake: CapturePending not implemented")
}

func (f *fakeVersions) Head(context.Context, string) (strategies.Revision, error) {
	return "", errors.New("fake: Head not implemented")
}

func (f *fakeVersions) SetHead(context.Context, string, strategies.Revision) error {
	return errors.New("fake: SetHead not implemented")
}

func (f *fakeVersions) ListVersions(context.Context, string, strategies.Page) ([]strategies.VersionSummary, string, error) {
	return nil, "", nil
}

func (f *fakeVersions) ListStrategies(context.Context, string, strategies.Page) ([]strategies.Strategy, string, error) {
	return nil, "", nil
}

func (f *fakeVersions) ListBySession(context.Context, string, string) ([]strategies.Strategy, error) {
	return nil, nil
}

func (f *fakeVersions) SweepCapturePending(context.Context, time.Duration) (int, error) {
	return 0, nil
}
