package http_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

// TestPGRunStore_SatisfiesRunStore is a compile-time guard proving that
// *PGRunStore satisfies RunStore, including its two lookup-shaped methods:
// CheckRunExists (used by the decisions handler) and LookupRunByID
// (added for the order-update notifications feature, consumed via a
// RunLookupFunc closure built in cmd/strategy-server/main.go).
//
// If either method is missing or has the wrong signature this file does
// not compile. The runtime DB path is covered by the integration suite;
// this unit test only locks the compile-time contract.
func TestPGRunStore_SatisfiesRunStore(t *testing.T) {
	t.Parallel()
	var _ strategyhttp.RunStore = (*strategyhttp.PGRunStore)(nil)

	// Also exercise the `memoryRunStore` shim's LookupRunByID — the test
	// fake the Handler tests rely on for the order-update integration
	// test. A nil map dereference here means the shim was added wrong.
	var store strategyhttp.RunStore = &memoryRunStore{
		runs: map[uuid.UUID]*strategyhttp.RunDetail{},
	}
	got, err := store.LookupRunByID(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("LookupRunByID on empty store: %v", err)
	}
	if got != nil {
		t.Fatalf("LookupRunByID on empty store: got %+v, want nil", got)
	}
}
