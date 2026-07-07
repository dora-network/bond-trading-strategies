package http_test

import (
	"testing"

	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

// TestPGRunStore_CheckRunExists is a compile-time guard proving that
// *PGRunStore satisfies RunStore, which now includes CheckRunExists.
// If the method is missing or has the wrong signature this file does
// not compile. The runtime DB path is covered by the integration suite;
// this unit test only locks the compile-time contract.
func TestPGRunStore_CheckRunExists(t *testing.T) {
	t.Parallel()
	var _ strategyhttp.RunStore = (*strategyhttp.PGRunStore)(nil)
}
