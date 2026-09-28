// Package pgstore is the durable, Postgres-backed implementation of the
// wasmruntime ArtifactStore. It pairs a local on-disk CAS (write-through
// cache) with the agent.wasm_artifacts / agent.wasm_manifests tables so
// compiled WASM modules survive Fargate task restarts.
//
// Write path (Put): a single tx writes both the wasm bytes row and the
// manifest jsonb row, then writes the same blobs to the local CAS. If
// the local write fails (disk full, perms), the durable copy is already
// in the DB; we log a warning and return success so the validate step
// that produced these bytes is not blocked by a transient cache issue.
//
// Read path (Get): local CAS first (fast hot path), DB on miss with
// re-materialization into the local CAS. After a Fargate restart the
// local CAS is empty; the first Recover for each running deployment
// downloads from PG, writes to disk, returns bytes — subsequent calls
// hit the fast path.
package pgstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/store"
)

// Store is the Postgres-backed ArtifactStore. fs is the local on-disk
// CAS the pgstore fronts; it is also exposed via WasmPath/ManifestPath
// so callers that need a real filesystem path (today: none in
// production code) get the local cache location regardless of whether
// the bytes are present.
type Store struct {
	pool *pgxpool.Pool
	fs   *store.Store
	log  *slog.Logger
}

// New constructs a Store. fs is required — the pgstore has no path
// layout of its own and always materializes bytes to fs's directory.
func New(pool *pgxpool.Pool, fs *store.Store, log *slog.Logger) *Store {
	if log == nil {
		log = slog.Default()
	}
	return &Store{pool: pool, fs: fs, log: log}
}

// Put writes the wasm + manifest to PG (single tx) and to the local
// CAS. Both writes are atomic in PG; the local write is best-effort
// because the DB copy is the durable layer.
func (s *Store) Put(wasm, manifest []byte) (wasmHash, manifestHash string, err error) {
	if len(wasm) == 0 {
		return "", "", errors.New("pgstore: wasm blob is empty")
	}
	if len(manifest) == 0 {
		return "", "", errors.New("pgstore: manifest is empty")
	}
	wh := sha256Hex(wasm)
	mh := sha256Hex(manifest)

	tx, err := s.pool.Begin(context.Background())
	if err != nil {
		return "", "", fmt.Errorf("pgstore: begin tx: %w", err)
	}
	defer func() {
		// Rollback is a no-op after a successful Commit; safe to defer
		// unconditionally.
		_ = tx.Rollback(context.Background())
	}()

	if _, err := tx.Exec(
		context.Background(), `
		insert into agent.wasm_artifacts (hash, size_bytes, bytes, path)
		values ($1, $2, $3, $4)
		on conflict (hash) do update set
			size_bytes = excluded.size_bytes,
			bytes = excluded.bytes,
			path = excluded.path`,
		wh, len(wasm), wasm, s.fs.WasmPath(wh),
	); err != nil {
		return "", "", fmt.Errorf("pgstore: upsert wasm_artifacts: %w", err)
	}
	if _, err := tx.Exec(
		context.Background(), `
		insert into agent.wasm_manifests (hash, artifact_hash, manifest)
		values ($1, $2, $3)
		on conflict (hash) do update set
			artifact_hash = excluded.artifact_hash,
			manifest = excluded.manifest`,
		mh, wh, manifest,
	); err != nil {
		return "", "", fmt.Errorf("pgstore: upsert wasm_manifests: %w", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		return "", "", fmt.Errorf("pgstore: commit: %w", err)
	}

	// Local CAS write-through. Best-effort: the DB row is already
	// durable; if the disk write fails the next Get will rehydrate.
	if _, _, err := s.fs.Put(wasm, manifest); err != nil {
		s.log.Warn("pgstore: local CAS write failed; relying on DB row",
			"wasm_hash", wh, "manifest_hash", mh, "error", err)
	}
	return wh, mh, nil
}

// Get returns the wasm + manifest bytes. Tries the local CAS first
// (hot path); on miss, fetches from PG and materializes to the local
// CAS so subsequent calls return from disk.
func (s *Store) Get(wasmHash, manifestHash string) (wasm, manifest []byte, err error) {
	w, m, fsErr := s.fs.Get(wasmHash, manifestHash)
	if fsErr == nil {
		return w, m, nil
	}
	// Real fs errors (permission, I/O) propagate. A "not found" miss
	// falls through to the DB.
	if !isMiss(fsErr) {
		return nil, nil, fmt.Errorf("pgstore: local CAS read: %w", fsErr)
	}

	ctx := context.Background()
	wasmBytes, err := fetchBytes(ctx, s.pool, "wasm", wasmHash)
	if err != nil {
		return nil, nil, err
	}
	manifestBytes, err := fetchBytes(ctx, s.pool, "manifest", manifestHash)
	if err != nil {
		return nil, nil, err
	}

	// Materialize to local CAS so the fast path covers future calls.
	// Errors here are warnings, not failures — the bytes are already
	// in hand.
	if _, _, err := s.fs.Put(wasmBytes, manifestBytes); err != nil {
		s.log.Warn("pgstore: local CAS rehydration write failed",
			"wasm_hash", wasmHash, "manifest_hash", manifestHash, "error", err)
	}
	return wasmBytes, manifestBytes, nil
}

// fetchBytes reads either a wasm blob or a manifest from PG based on
// the kind selector. Kept as a tiny helper rather than two near-identical
// Get methods because the two queries share shape.
func fetchBytes(ctx context.Context, pool *pgxpool.Pool, kind, hash string) ([]byte, error) {
	var (
		row  pgx.Row
		data []byte
	)
	switch kind {
	case "wasm":
		row = pool.QueryRow(ctx, `select bytes from agent.wasm_artifacts where hash = $1`, hash)
	case "manifest":
		row = pool.QueryRow(ctx, `select manifest from agent.wasm_manifests where hash = $1`, hash)
	default:
		return nil, fmt.Errorf("pgstore: unknown kind %q", kind)
	}
	if err := row.Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("pgstore: %s %s not found", kind, hash)
		}
		return nil, fmt.Errorf("pgstore: read %s %s: %w", kind, hash, err)
	}
	return data, nil
}

// WasmPath returns the local FS path for the wasm blob. The pgstore
// does not define its own path layout — the local CAS owns that. If
// the local CAS has not been hydrated for this hash (cold miss before
// any Get call), the returned path may not exist on disk; callers
// should pair WasmPath with a Get first. Today's production code
// (Registry.Load) reads bytes via Get and never asks for a path, so
// this is safe in practice.
func (s *Store) WasmPath(hash string) string {
	return s.fs.WasmPath(hash)
}

// ManifestPath returns the local FS path for the manifest sidecar.
// Same caveat as WasmPath: path may not exist on disk until Get has
// populated it.
func (s *Store) ManifestPath(hash string) string {
	return s.fs.ManifestPath(hash)
}

// sha256Hex matches the existing store.Store hash format so the two
// stores are interchangeable. Same shape as the unexported helper in
// the FS store package; duplicated here rather than exported because
// the FS helper is an internal implementation detail.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// isMiss reports whether an error from the FS store is a "not found"
// miss (vs. a real I/O error). os.ReadFile wraps fs.ErrNotExist via
// the standard unwrap chain, so errors.Is matches both syscall.ENOENT
// and *fs.PathError. Recommended idiom per os.IsNotExist's godoc.
func isMiss(err error) bool {
	return errors.Is(err, fs.ErrNotExist)
}
