//go:build ignore

// Local example strategy for the wasm_starter cancel-integration
// test. Built by the test via `tinygo build -target=wasi
// -buildmode=c-shared` and loaded as a real .wasm plugin.
//
// The file lives under testdata/ (auto-excluded from `go build
// ./...` via `//go:build ignore`) so the parent module doesn't
// treat it as a regular package — and so the test does not depend
// on the dora-strategy-wasm module cache. Framework packages
// (`dorastrategy` and `dorastrategy/host`) still resolve from the
// pinned module in go.mod; that's by design and orthogonal to
// where the example SOURCE lives.
//
// Implements the v3 Strategy interface (Init + OnCandle +
// OnPreamble) with no-op overrides; OnTrade and OnPrice inherit
// from StrategyBase. Its only purpose is to exercise the
// mergeStream -> host_next_event cancel path in
// TestWasmStarter_CancelUnwindsWASMGoroutine.
package main

import (
	"context"

	"github.com/dora-network/dora-strategy-wasm/dorastrategy"
	"github.com/dora-network/dora-strategy-wasm/dorastrategy/host"
)

type noopStrategy struct {
	dorastrategy.StrategyBase
}

func (s *noopStrategy) Init(dorastrategy.Config) error {
	host.Log(host.LevelInfo, "wasm_starter test strategy init")
	return nil
}

func (s *noopStrategy) OnCandle(c dorastrategy.Candle) ([]dorastrategy.OrderIntent, error) {
	return nil, nil
}

func (s *noopStrategy) OnPreamble(ctx context.Context, p dorastrategy.PreambleContext) error {
	return nil
}

func main() {
	if err := dorastrategy.Run(&noopStrategy{}); err != nil {
		panic(err)
	}
}
