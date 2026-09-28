// Package strategies owns the versioned strategy artifact store. The Store
// interface is the swappable seam: Postgres implements it today; a git/jujutsu
// backend can implement the same operations later without changing callers.
package strategies

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Revision is an opaque version handle. Today a uuid; a future git backend may
// use a commit sha. Callers never inspect its structure.
type Revision string

// Page is an opaque keyset cursor over a newest-first list. Cursor "" is the
// first page; Limit 0 uses the default (capped at MaxPageSize).
type Page struct {
	Limit  int
	Cursor string
}

// MaxPageSize caps the number of items a list call returns.
const MaxPageSize = 100

const defaultPageSize = 50

// EffectiveLimit resolves a page limit to a bounded value.
func (p Page) EffectiveLimit() int {
	if p.Limit <= 0 || p.Limit > MaxPageSize {
		return defaultPageSize
	}
	return p.Limit
}

type Strategy struct {
	ID, UserID, Name string
	SourceSessionID  string
	HeadRevision     Revision // "" until version 1 lands
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type Meta struct {
	Provider, Model     string
	ModuleName, Summary string
	Rationale           string
	Validation          json.RawMessage
	ImageRef            string
}

type VersionSummary struct {
	Revision, ParentRevision Revision
	CreatedAt                time.Time
	ModuleName, Summary      string
	ImageRef                 string
	Target                   string // "go-docker" | "go-wasm"
	WasmRef                  string
	ManifestHash             string
}

type Version struct {
	StrategyID               string
	Revision, ParentRevision Revision
	CreatedAt                time.Time
	Meta                     Meta
	Files                    map[string]string // path -> content
	// ImageRef is the local-daemon image tag the validator produced
	// during build (Phase 2). Slices D/E `docker run` this tag.
	// Empty for pre-Phase-3 captures.
	ImageRef string
	// WasmRef is the content hash of the .wasm blob; non-empty iff
	// Target == "go-wasm". ManifestHash is the content hash of the
	// manifest.json sidecar. Target selects the runtime pipeline:
	// "go-docker" (legacy, image_ref) or "go-wasm" (wasm_ref +
	// manifest_hash, the Plan 4 default for newly-generated
	// strategies).
	WasmRef      string
	ManifestHash string
	Target       string
}

// ErrNotFound is returned when a strategy or version is absent or not owned.
var ErrNotFound = errors.New("strategies: not found")

// ErrNoPending is returned by CapturePending when the session has no stashed
// artifact to retry.
var ErrNoPending = errors.New("strategies: no pending capture for session")

type Store interface {
	// GetStrategy returns a strategy owned by userID. ErrNotFound if absent or
	// not owned — the ownership gate for strategy-scoped endpoints.
	GetStrategy(ctx context.Context, userID, strategyID string) (Strategy, error)

	// Capture records a version for the strategy bound to sourceSessionID,
	// creating and binding the strategy on first capture (parentRevision = "").
	// It content-addresses files, inserts the version, advances head, and clears
	// any pending stash for the session — all atomic.
	Capture(ctx context.Context, sourceSessionID, userID, provider, model string,
		m Meta, files map[string]string, imageRef string) (Version, error)

	// CaptureWASM persists a WASM artifact + its manifest for a new
	// strategy version. Mirrors Capture for go-wasm targets: it creates
	// and binds the strategy on first capture, content-addresses files,
	// inserts the version with target='go-wasm' + the wasm_ref and
	// manifest_hash, advances head, and clears any pending stash — all
	// atomic. wasmRef/manifestHash are content hashes into the local FS
	// artifact store (POC).
	CaptureWASM(ctx context.Context, sourceSessionID, userID, provider, model string,
		m Meta, files map[string]string, wasmRef, manifestHash string) (Version, error)

	// StashPending saves the artifact from a failed capture for the session
	// (upsert), so the user can retry via CapturePending.
	StashPending(ctx context.Context, sessionID, userID, provider, model string,
		m Meta, files map[string]string, imageRef string) error

	// CapturePending retries the capture from the session's stashed artifact:
	// reads the stash, runs Capture, clears it. ErrNoPending if none.
	// CapturePending captures a previously-stashed artifact. imageRef is
	// the local-daemon image tag recorded by an earlier successful
	// build; empty for legacy stashes.
	CapturePending(ctx context.Context, sessionID string) (Version, error)

	GetVersion(ctx context.Context, strategyID string, revision Revision) (Version, error)
	Head(ctx context.Context, strategyID string) (Revision, error)
	SetHead(ctx context.Context, strategyID string, revision Revision) error

	ListVersions(ctx context.Context, strategyID string, p Page) (items []VersionSummary, nextCursor string, err error)
	ListStrategies(ctx context.Context, userID string, p Page) (items []Strategy, nextCursor string, err error)
	// ListBySession returns every strategy whose source_session_id
	// matches sessionID, scoped to userID for the ownership gate. A
	// session can produce multiple strategies across retries, so the
	// chatui surfaces each one with its head revision. Newest first.
	ListBySession(ctx context.Context, userID, sessionID string) ([]Strategy, error)

	// SweepCapturePending deletes strategy_capture_pending rows whose
	// created_at is older than olderThan. Returns the number of rows
	// deleted so callers can log it. Called by the in-process janitor
	// (strategies.Janitor) on a tick interval; safe to invoke from
	// multiple replicas because the predicate is server-time-based.
	SweepCapturePending(ctx context.Context, olderThan time.Duration) (int, error)
}
