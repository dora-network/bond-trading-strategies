package store_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"
)

func TestStore_PutAndGet(t *testing.T) {
	dir := t.TempDir()
	s, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	wasmBytes := []byte("\x00asm\x01\x00\x00\x00")
	manifestBytes := []byte(`{"schema_version":1,"module_name":"m"}`)

	wasmHash, mHash, err := s.Put(wasmBytes, manifestBytes)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if wasmHash == "" || mHash == "" {
		t.Fatal("Put returned empty hashes")
	}

	gotWasm, gotManifest, err := s.Get(wasmHash, mHash)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(gotWasm) != string(wasmBytes) {
		t.Errorf("WASM round-trip mismatch")
	}
	if string(gotManifest) != string(manifestBytes) {
		t.Errorf("manifest round-trip mismatch")
	}
}

func TestStore_GetMissingReturnsError(t *testing.T) {
	dir := t.TempDir()
	s, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	_, _, err = s.Get("deadbeef", "feedface")
	if err == nil {
		t.Fatal("Get on missing hashes should error")
	}
}

func TestStore_DeterministicHashes(t *testing.T) {
	dir := t.TempDir()
	s, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	wasm := []byte("\x00asm\x01\x00\x00\x00")
	manifest := []byte(`{"schema_version":1}`)

	h1, _, err := s.Put(wasm, manifest)
	if err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	h2, _, err := s.Put(wasm, manifest)
	if err != nil {
		t.Fatalf("Put 2: %v", err)
	}
	if h1 != h2 {
		t.Errorf("Put is not deterministic: %s vs %s", h1, h2)
	}
}

func TestStore_RejectsZeroLength(t *testing.T) {
	dir := t.TempDir()
	s, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	if _, _, err := s.Put(nil, []byte("{}")); err == nil {
		t.Error("Put with empty wasm should error")
	}
	if _, _, err := s.Put([]byte("x"), nil); err == nil {
		t.Error("Put with empty manifest should error")
	}
}

func TestStore_PathLayout(t *testing.T) {
	dir := t.TempDir()
	s, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	wasmHash, mHash, err := s.Put([]byte("x"), []byte("y"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Layout: <root>/<aa>/<full-hash>.wasm and <root>/<aa>/<full-hash>.manifest.json
	wantWasmPath := filepath.Join(dir, wasmHash[:2], wasmHash+".wasm")
	wantManifestPath := filepath.Join(dir, mHash[:2], mHash+".manifest.json")
	if _, err := os.Stat(wantWasmPath); err != nil {
		t.Errorf("expected wasm at %s, got %v", wantWasmPath, err)
	}
	if _, err := os.Stat(wantManifestPath); err != nil {
		t.Errorf("expected manifest at %s, got %v", wantManifestPath, err)
	}
}
