// Package store is the on-disk content-addressed artifact store for
// compiled WASM blobs and their manifest.json sidecars. Local FS for
// POC; the interface is small enough that a future object-store
// implementation can replace this without touching callers.
package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// dirPerm is the permission for content-addressed shard directories.
// Named for gosec/mnd: avoids magic-number and hardcoded-perm lint.
const dirPerm = 0o755

// filePerm is the permission for stored blobs (wasm + manifest).
// Named for gosec/mnd: gosec G306 requires 0600 or less.
const filePerm = 0o600

// Store is a content-addressed blob store.
type Store struct {
	root string
}

// New returns a Store rooted at dir. The directory is created if
// it does not exist. If the directory cannot be created, New returns
// an error so the caller can fail fast instead of discovering the
// problem on the first Put.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("store: create root %q: %w", dir, err)
	}
	return &Store{root: dir}, nil
}

// Put writes the wasm blob and the manifest sidecar, returning the
// sha256 hex hashes of each. The same content always produces the
// same hashes (content-addressed).
func (s *Store) Put(wasm, manifest []byte) (wasmHash, manifestHash string, err error) {
	if len(wasm) == 0 {
		return "", "", errors.New("store: wasm blob is empty")
	}
	if len(manifest) == 0 {
		return "", "", errors.New("store: manifest is empty")
	}
	wh := sha256Hex(wasm)
	mh := sha256Hex(manifest)

	if err := os.MkdirAll(filepath.Dir(s.wasmPath(wh)), dirPerm); err != nil {
		return "", "", fmt.Errorf("store: mkdir wasm shard: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.manifestPath(mh)), dirPerm); err != nil {
		return "", "", fmt.Errorf("store: mkdir manifest shard: %w", err)
	}
	if err := os.WriteFile(s.wasmPath(wh), wasm, filePerm); err != nil {
		return "", "", fmt.Errorf("store: write wasm: %w", err)
	}
	if err := os.WriteFile(s.manifestPath(mh), manifest, filePerm); err != nil {
		return "", "", fmt.Errorf("store: write manifest: %w", err)
	}
	return wh, mh, nil
}

// Get reads a previously-Put wasm + manifest pair by their hashes.
func (s *Store) Get(wasmHash, manifestHash string) (wasm, manifest []byte, err error) {
	w, err := os.ReadFile(s.wasmPath(wasmHash))
	if err != nil {
		return nil, nil, fmt.Errorf("store: read wasm: %w", err)
	}
	m, err := os.ReadFile(s.manifestPath(manifestHash))
	if err != nil {
		return nil, nil, fmt.Errorf("store: read manifest: %w", err)
	}
	return w, m, nil
}

// WasmPath returns the on-disk path for a wasm hash. Useful for
// the subprocess smoke in Plan 3 and for debugging.
func (s *Store) WasmPath(hash string) string {
	return s.wasmPath(hash)
}

// ManifestPath returns the on-disk path for a manifest hash. Used
// by the orchestrator to pass the manifest to the smoke subprocess.
func (s *Store) ManifestPath(hash string) string {
	return s.manifestPath(hash)
}

func (s *Store) wasmPath(hash string) string {
	return filepath.Join(s.root, hash[:2], hash+".wasm")
}

func (s *Store) manifestPath(hash string) string {
	return filepath.Join(s.root, hash[:2], hash+".manifest.json")
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ArtifactStore is the contract every backing store must satisfy.
// Implemented by *Store (FS) and *pgstore.Store (Postgres + FS cache).
// The validate path uses this interface so the storage backend can be
// swapped without touching the validation logic.
type ArtifactStore interface {
	Get(wasmHash, manifestHash string) (wasm, manifest []byte, err error)
	Put(wasm, manifest []byte) (wasmHash, manifestHash string, err error)
	WasmPath(hash string) string
	ManifestPath(hash string) string
}
