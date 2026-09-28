package pgstore_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies/servertest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store/pgstore"
)

// wasmBytes / manifestBytes are small test fixtures.
var (
	wasmBytes    = []byte("\x00asm\x01\x00\x00\x00WASM_TEST")
	manifestJSON = []byte(`{"schema_version":1,"module_name":"test"}`)
)

// mustPgStore constructs a Store backed by a fresh test Postgres
// fixture. The repo's servertest helper self-skips when the DB DSN is
// not set. Tests wipe the durable tables before constructing so
// per-test isolation holds even when the same hash reappears across
// tests.
func mustPgStore(t *testing.T) (*pgstore.Store, func()) {
	pool := servertest.StartPostgres(t)
	dir := t.TempDir()
	fs, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	st := pgstore.New(pool, fs, nil)
	if _, err := pool.Exec(context.Background(), `delete from agent.wasm_manifests`); err != nil {
		t.Fatalf("wipe wasm_manifests: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `delete from agent.wasm_artifacts`); err != nil {
		t.Fatalf("wipe wasm_artifacts: %v", err)
	}
	cleanup := func() { pool.Close() }
	return st, cleanup
}

// TestStore_PutAndGet_LocalOnly covers the hot path.
func TestStore_PutAndGet_LocalOnly(t *testing.T) {
	st, done := mustPgStore(t)
	defer done()

	wh, mh, err := st.Put(wasmBytes, manifestJSON)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if wh == "" || mh == "" {
		t.Fatal("Put returned empty hashes")
	}
	w, m, err := st.Get(wh, mh)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(w) != string(wasmBytes) {
		t.Errorf("wasm: want %q, got %q", wasmBytes, w)
	}
	if string(m) != string(manifestJSON) {
		t.Errorf("manifest: want %q, got %q", manifestJSON, m)
	}
}

// TestStore_Put_WritesBothLayers pins the durability contract: after
// Put, both rows are queryable from PG. Without this, Fargate restarts
// would still fail.
func TestStore_Put_WritesBothLayers(t *testing.T) {
	st, done := mustPgStore(t)
	defer done()

	wh, mh, err := st.Put(wasmBytes, manifestJSON)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	pool := servertest.StartPostgres(t)
	var gotWasm []byte
	if err := pool.QueryRow(context.Background(),
		`select bytes from agent.wasm_artifacts where hash = $1`, wh).Scan(&gotWasm); err != nil {
		t.Fatalf("read wasm_artifacts: %v", err)
	}
	if string(gotWasm) != string(wasmBytes) {
		t.Errorf("PG wasm bytes mismatch")
	}
	var gotManifest []byte
	if err := pool.QueryRow(context.Background(),
		`select manifest from agent.wasm_manifests where hash = $1`, mh).Scan(&gotManifest); err != nil {
		t.Fatalf("read wasm_manifests: %v", err)
	}
	if string(gotManifest) != string(manifestJSON) {
		t.Errorf("PG manifest mismatch: want %q, got %q", manifestJSON, gotManifest)
	}
}

// TestStore_Get_ColdMiss_PopulatesLocalCAS is the critical Fargate-restart
// regression test: the local CAS is wiped (simulated), Get rehydrates
// from PG, and WasmPath/ManifestPath are real on disk afterwards.
// Without this behavior, callers that pair Get with WasmPath would
// fail on the first request after a task restart.
func TestStore_Get_ColdMiss_PopulatesLocalCAS(t *testing.T) {
	st, done := mustPgStore(t)
	defer done()

	wh, mh, err := st.Put(wasmBytes, manifestJSON)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	wasmPath := st.WasmPath(wh)
	manifestPath := st.ManifestPath(mh)

	// Confirm both files exist on disk after write-through.
	if _, err := os.Stat(wasmPath); err != nil {
		t.Fatalf("local CAS wasm file missing after Put: %v", err)
	}
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("local CAS manifest file missing after Put: %v", err)
	}

	// Simulate Fargate restart: wipe the local CAS.
	if err := os.Remove(wasmPath); err != nil {
		t.Fatalf("remove local wasm: %v", err)
	}
	if err := os.Remove(manifestPath); err != nil {
		t.Fatalf("remove local manifest: %v", err)
	}
	if _, err := os.Stat(wasmPath); !os.IsNotExist(err) {
		t.Fatalf("expected wasm file removed; got err=%v", err)
	}

	// Get must rehydrate from PG and re-create the local files.
	w, m, err := st.Get(wh, mh)
	if err != nil {
		t.Fatalf("Get on cold miss: %v", err)
	}
	if string(w) != string(wasmBytes) {
		t.Errorf("rehydrated wasm mismatch")
	}
	if string(m) != string(manifestJSON) {
		t.Errorf("rehydrated manifest mismatch")
	}
	if _, err := os.Stat(wasmPath); err != nil {
		t.Fatalf("WasmPath should exist on disk after cold-miss rehydration: %v", err)
	}
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("ManifestPath should exist on disk after cold-miss rehydration: %v", err)
	}

	// Rehydrated file content matches the original.
	gotOnDisk, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Fatalf("read rehydrated file: %v", err)
	}
	if string(gotOnDisk) != string(wasmBytes) {
		t.Errorf("rehydrated file content mismatch")
	}
}

// TestStore_Get_MissingRowIsError pins the not-found contract: when
// neither the local CAS nor PG has the hash, Get returns an error
// rather than a partial / zero-value success. Without this, callers
// could silently deploy a zero-byte .wasm.
func TestStore_Get_MissingRowIsError(t *testing.T) {
	st, done := mustPgStore(t)
	defer done()

	_, _, err := st.Get(
		"000000000000000000000000000000000000000000000000000000000000dead",
		"000000000000000000000000000000000000000000000000000000000000beef",
	)
	if err == nil {
		t.Fatal("Get should fail when neither local nor PG has the hash")
	}
	if !strings.Contains(err.Error(), "not found") &&
		!strings.Contains(err.Error(), "no such file") {
		t.Errorf("error should mention not-found; got: %q", err.Error())
	}
}
