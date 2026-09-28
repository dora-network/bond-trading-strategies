package strategies

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PgStore struct {
	Pool *pgxpool.Pool
}

func NewPgStore(pool *pgxpool.Pool) *PgStore { return &PgStore{Pool: pool} }

var _ Store = (*PgStore)(nil)

func (s *PgStore) GetStrategy(ctx context.Context, userID, strategyID string) (Strategy, error) {
	var out Strategy
	var head *string
	err := s.Pool.QueryRow(
		ctx, `
		select id, dora_user_id, name, head_revision, source_session_id, created_at, updated_at
		from agent.strategies where id=$1 and dora_user_id=$2`, strategyID, userID,
	).Scan(&out.ID, &out.UserID, &out.Name, &head, &out.SourceSessionID, &out.CreatedAt, &out.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Strategy{}, ErrNotFound
	}
	if err != nil {
		return Strategy{}, err
	}
	if head != nil {
		out.HeadRevision = Revision(*head)
	}
	return out, nil
}

func (s *PgStore) Capture(
	ctx context.Context,
	sourceSessionID, userID, provider, model string,
	m Meta,
	files map[string]string,
	imageRef string,
) (Version, error) {
	return s.captureInTx(ctx, func(tx pgx.Tx) (Version, error) {
		return s.captureLocked(ctx, tx, sourceSessionID, userID, provider, model, m, files, imageRef)
	})
}

func (s *PgStore) captureInTx(ctx context.Context, fn func(pgx.Tx) (Version, error)) (Version, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Version{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	out, err := fn(tx)
	if err != nil {
		return Version{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Version{}, err
	}
	return out, nil
}

func (s *PgStore) captureLocked(
	ctx context.Context,
	tx pgx.Tx,
	sourceSessionID, userID, provider, model string,
	m Meta,
	files map[string]string,
	imageRef string,
) (Version, error) {
	strategyID, parent, err := bindStrategy(ctx, tx, sourceSessionID, userID, m.ModuleName)
	if err != nil {
		return Version{}, err
	}
	_, out, err := insertVersion(ctx, tx, strategyID, parent, provider, model, m, files, imageRef)
	if err != nil {
		return Version{}, err
	}
	if err := advanceHead(ctx, tx, strategyID, sourceSessionID, out.Revision); err != nil {
		return Version{}, err
	}
	out.StrategyID = strategyID
	return out, nil
}

// CaptureWASM mirrors Capture for go-wasm targets. It binds the
// strategy on first capture, inserts a version stamped with
// target='go-wasm' + the wasm_ref and manifest_hash, advances head,
// and clears any pending stash — all atomic.
func (s *PgStore) CaptureWASM(
	ctx context.Context,
	sourceSessionID, userID, provider, model string,
	m Meta,
	files map[string]string,
	wasmRef, manifestHash string,
) (Version, error) {
	return s.captureInTx(ctx, func(tx pgx.Tx) (Version, error) {
		return s.captureWASMLocked(ctx, tx, sourceSessionID, userID, provider, model, m, files, wasmRef, manifestHash)
	})
}

func (s *PgStore) captureWASMLocked(
	ctx context.Context,
	tx pgx.Tx,
	sourceSessionID, userID, provider, model string,
	m Meta,
	files map[string]string,
	wasmRef, manifestHash string,
) (Version, error) {
	strategyID, parent, err := bindStrategy(ctx, tx, sourceSessionID, userID, m.ModuleName)
	if err != nil {
		return Version{}, err
	}
	out, err := insertVersionWASM(ctx, tx, strategyID, parent, provider, model, m, files, wasmRef, manifestHash)
	if err != nil {
		return Version{}, err
	}
	if err := advanceHead(ctx, tx, strategyID, sourceSessionID, out.Revision); err != nil {
		return Version{}, err
	}
	out.StrategyID = strategyID
	return out, nil
}

// bindStrategy locks the strategy row bound to sourceSessionID for
// update and returns its id + parent head revision. If no strategy is
// bound yet, it creates one (first capture). Shared by Capture and
// CaptureWASM so the ownership/bind contract has one source of truth.
func bindStrategy(
	ctx context.Context,
	tx pgx.Tx,
	sourceSessionID, userID, moduleName string,
) (strategyID, parent string, err error) {
	var head *string
	err = tx.QueryRow(
		ctx,
		`select id, head_revision from agent.strategies where source_session_id=$1 for update`,
		sourceSessionID,
	).Scan(&strategyID, &head)
	if errors.Is(err, pgx.ErrNoRows) {
		strategyID = uuid.NewString()
		_, err = tx.Exec(ctx, `
			insert into agent.strategies (id, dora_user_id, name, source_session_id)
			values ($1, $2, $3, $4)`, strategyID, userID, moduleName, sourceSessionID)
	}
	if err != nil {
		return "", "", err
	}
	if head != nil {
		parent = *head
	}
	return strategyID, parent, nil
}

// advanceHead moves head_revision to revision and clears any pending
// stash for the session. Shared by Capture and CaptureWASM.
func advanceHead(ctx context.Context, tx pgx.Tx, strategyID, sourceSessionID string, revision Revision) error {
	if _, err := tx.Exec(ctx,
		`update agent.strategies set head_revision=$2, updated_at=now() where id=$1`,
		strategyID, revision); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `delete from agent.strategy_capture_pending where session_id=$1`, sourceSessionID)
	return err
}

func insertVersionWASM(
	ctx context.Context,
	tx pgx.Tx,
	strategyID, parent, provider, model string,
	m Meta,
	files map[string]string,
	wasmRef, manifestHash string,
) (Version, error) {
	revision := uuid.NewString()
	var createdAt time.Time
	err := tx.QueryRow(
		ctx, `
		insert into agent.strategy_versions
			(revision, strategy_id, parent_revision, provider, model, module_name, summary, rationale,
			 validation, target, wasm_ref, manifest_hash)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,'go-wasm',$10,$11)
		returning created_at`,
		revision, strategyID,
		nilOrString(parent),
		provider, model,
		m.ModuleName, m.Summary, m.Rationale,
		[]byte(m.Validation),
		nilOrString(wasmRef), nilOrString(manifestHash),
	).Scan(&createdAt)
	if err != nil {
		return Version{}, err
	}
	for path, content := range files {
		sum := sha256.Sum256([]byte(content))
		if _, err := tx.Exec(ctx, `
			insert into agent.strategy_blobs (strategy_id, sha256, content)
			values ($1,$2,$3) on conflict do nothing`, strategyID, sum[:], []byte(content)); err != nil {
			return Version{}, err
		}
		if _, err := tx.Exec(ctx, `
			insert into agent.strategy_version_files (revision, path, sha256)
			values ($1,$2,$3)`, revision, path, sum[:]); err != nil {
			return Version{}, err
		}
	}
	m.Provider, m.Model = provider, model
	return Version{
		Revision: Revision(revision), ParentRevision: Revision(parent), CreatedAt: createdAt,
		Meta: m, Files: files,
		WasmRef: wasmRef, ManifestHash: manifestHash, Target: "go-wasm",
	}, nil
}

func insertVersion(
	ctx context.Context,
	tx pgx.Tx,
	strategyID, parent, provider, model string,
	m Meta,
	files map[string]string,
	imageRef string,
) (string, Version, error) {
	revision := uuid.NewString()
	var createdAt time.Time
	err := tx.QueryRow(
		ctx, `
		insert into agent.strategy_versions
			(revision, strategy_id, parent_revision, provider, model, module_name, summary, rationale, validation, image_ref)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		returning created_at`,
		revision, strategyID,
		nilOrString(parent),
		provider, model,
		m.ModuleName, m.Summary, m.Rationale,
		[]byte(m.Validation), nilOrString(imageRef),
	).Scan(&createdAt)
	if err != nil {
		return "", Version{}, err
	}
	for path, content := range files {
		sum := sha256.Sum256([]byte(content))
		if _, err := tx.Exec(ctx, `
			insert into agent.strategy_blobs (strategy_id, sha256, content)
			values ($1,$2,$3) on conflict do nothing`, strategyID, sum[:], []byte(content)); err != nil {
			return "", Version{}, err
		}
		if _, err := tx.Exec(ctx, `
			insert into agent.strategy_version_files (revision, path, sha256)
			values ($1,$2,$3)`, revision, path, sum[:]); err != nil {
			return "", Version{}, err
		}
	}
	m.Provider, m.Model = provider, model
	return revision, Version{
		Revision: Revision(revision), ParentRevision: Revision(parent), CreatedAt: createdAt, Meta: m, Files: files, ImageRef: imageRef,
	}, nil
}

func nilOrString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (s *PgStore) Head(ctx context.Context, strategyID string) (Revision, error) {
	var head *string
	err := s.Pool.QueryRow(ctx, `select head_revision from agent.strategies where id=$1`, strategyID).Scan(&head)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil || head == nil {
		return "", err
	}
	return Revision(*head), nil
}

func (s *PgStore) GetVersion(ctx context.Context, strategyID string, revision Revision) (Version, error) {
	var out Version
	var parent, sourceSessionID, imageRef, wasmRef, manifestHash *string
	err := s.Pool.QueryRow(
		ctx, `
		select v.revision, v.parent_revision, v.created_at, v.provider, v.model,
			v.module_name, v.summary, v.rationale, v.validation, v.image_ref,
			s.source_session_id, v.target, v.wasm_ref, v.manifest_hash
		from agent.strategy_versions v
		join agent.strategies s on s.id=v.strategy_id
		where v.revision=$1 and v.strategy_id=$2`, revision, strategyID,
	).Scan(&out.Revision, &parent, &out.CreatedAt, &out.Meta.Provider, &out.Meta.Model,
		&out.Meta.ModuleName, &out.Meta.Summary, &out.Meta.Rationale, &out.Meta.Validation,
		&imageRef, &sourceSessionID, &out.Target, &wasmRef, &manifestHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return Version{}, ErrNotFound
	}
	if err != nil {
		return Version{}, err
	}
	if parent != nil {
		out.ParentRevision = Revision(*parent)
	}
	if imageRef != nil {
		out.ImageRef = *imageRef
	}
	if wasmRef != nil {
		out.WasmRef = *wasmRef
	}
	if manifestHash != nil {
		out.ManifestHash = *manifestHash
	}
	out.Files, err = s.loadFiles(ctx, strategyID, revision)
	return out, err
}

func (s *PgStore) loadFiles(ctx context.Context, strategyID string, revision Revision) (map[string]string, error) {
	rows, err := s.Pool.Query(ctx, `
		select f.path, b.content
		from agent.strategy_version_files f
		join agent.strategy_blobs b on b.strategy_id=$1 and b.sha256=f.sha256
		where f.revision=$2`, strategyID, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	files := make(map[string]string)
	for rows.Next() {
		var path string
		var content []byte
		if err := rows.Scan(&path, &content); err != nil {
			return nil, err
		}
		files[path] = string(content)
	}
	return files, rows.Err()
}

func (s *PgStore) SetHead(ctx context.Context, strategyID string, revision Revision) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
		update agent.strategies set head_revision=$2, updated_at=now()
		where id=$1 and exists (
			select 1 from agent.strategy_versions where revision=$2 and strategy_id=$1
		)`, strategyID, revision)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return tx.Commit(ctx)
}

func (s *PgStore) StashPending(
	ctx context.Context,
	sessionID, userID, provider, model string,
	m Meta,
	files map[string]string,
	imageRef string,
) error {
	encodedFiles, err := json.Marshal(files)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, `
		insert into agent.strategy_capture_pending
			(session_id, dora_user_id, provider, model, module_name, summary, rationale, validation, files, image_ref)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		on conflict (session_id) do update set
			dora_user_id=excluded.dora_user_id, provider=excluded.provider, model=excluded.model,
			module_name=excluded.module_name, summary=excluded.summary, rationale=excluded.rationale,
			validation=excluded.validation, files=excluded.files, image_ref=excluded.image_ref, created_at=now()`,
		sessionID, userID, provider, model, m.ModuleName, m.Summary, m.Rationale, []byte(m.Validation), encodedFiles, imageRef)
	return err
}

func (s *PgStore) CapturePending(ctx context.Context, sessionID string) (Version, error) {
	return s.captureInTx(ctx, func(tx pgx.Tx) (Version, error) {
		var userID, provider, model string
		var m Meta
		var encodedFiles []byte
		var imageRef *string
		err := tx.QueryRow(
			ctx, `
			select dora_user_id, provider, model, module_name, summary, rationale, validation, files, image_ref
			from agent.strategy_capture_pending where session_id=$1 for update`, sessionID,
		).Scan(&userID, &provider, &model, &m.ModuleName, &m.Summary, &m.Rationale, &m.Validation, &encodedFiles, &imageRef)
		if errors.Is(err, pgx.ErrNoRows) {
			return Version{}, ErrNoPending
		}
		if err != nil {
			return Version{}, err
		}
		var files map[string]string
		if err := json.Unmarshal(encodedFiles, &files); err != nil {
			return Version{}, err
		}
		if imageRef != nil {
			m.ImageRef = *imageRef
		}
		return s.captureLocked(ctx, tx, sessionID, userID, provider, model, m, files, m.ImageRef)
	})
}

func (s *PgStore) ListVersions(
	ctx context.Context,
	strategyID string,
	p Page,
) ([]VersionSummary, string, error) {
	limit := p.EffectiveLimit()
	args := []any{strategyID}
	query := `select revision, parent_revision, created_at, module_name, summary, image_ref, target, wasm_ref, manifest_hash ` +
		`from agent.strategy_versions where strategy_id=$1`
	if cursorTime, cursorRevision, ok := decodeCursor(p.Cursor); ok {
		query += ` and (created_at, revision) < ($2,$3)`
		args = append(args, cursorTime, cursorRevision)
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" order by created_at desc, revision desc limit $%d", len(args))
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out := make([]VersionSummary, 0, limit+1)
	for rows.Next() {
		var item VersionSummary
		var parent, imageRef, wasmRef, manifestHash *string
		if err := rows.Scan(
			&item.Revision, &parent, &item.CreatedAt, &item.ModuleName, &item.Summary,
			&imageRef, &item.Target, &wasmRef, &manifestHash,
		); err != nil {
			return nil, "", err
		}
		if parent != nil {
			item.ParentRevision = Revision(*parent)
		}
		if imageRef != nil {
			item.ImageRef = *imageRef
		}
		if wasmRef != nil {
			item.WasmRef = *wasmRef
		}
		if manifestHash != nil {
			item.ManifestHash = *manifestHash
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	items, next := versionPage(out, limit)
	return items, next, nil
}

func versionPage(out []VersionSummary, limit int) ([]VersionSummary, string) {
	if len(out) <= limit {
		return out, ""
	}
	next := encodeCursor(out[limit-1].CreatedAt, string(out[limit-1].Revision))
	return out[:limit], next
}

func (s *PgStore) ListStrategies(ctx context.Context, userID string, p Page) ([]Strategy, string, error) {
	limit := p.EffectiveLimit()
	args := []any{userID}
	query := `
		select id, dora_user_id, name, head_revision, source_session_id, created_at, updated_at
		from agent.strategies where dora_user_id=$1`
	if cursorTime, cursorID, ok := decodeCursor(p.Cursor); ok {
		query += ` and (created_at, id) < ($2,$3)`
		args = append(args, cursorTime, cursorID)
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" order by created_at desc, id desc limit $%d", len(args))
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out := make([]Strategy, 0, limit+1)
	for rows.Next() {
		var item Strategy
		var head *string
		if err := rows.Scan(&item.ID, &item.UserID, &item.Name, &head, &item.SourceSessionID, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, "", err
		}
		if head != nil {
			item.HeadRevision = Revision(*head)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(out) <= limit {
		return out, "", nil
	}
	next := encodeCursor(out[limit-1].CreatedAt, out[limit-1].ID)
	return out[:limit], next, nil
}

func encodeCursor(createdAt time.Time, id string) string {
	value := createdAt.UTC().Format(time.RFC3339Nano) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

// ListBySession returns every strategy owned by userID whose
// source_session_id matches sessionID, newest first. Used by the
// session-detail endpoint so the chatui can render historical
// strategies for a session even when the SSE event was missed (e.g.
// the strategy was captured before the strategy_saved event was
// wired into the SSE stream).
func (s *PgStore) ListBySession(ctx context.Context, userID, sessionID string) ([]Strategy, error) {
	rows, err := s.Pool.Query(ctx, `
		select id, dora_user_id, name, head_revision, source_session_id, created_at, updated_at
		from agent.strategies
		where dora_user_id=$1 and source_session_id=$2
		order by created_at desc, id desc`, userID, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Strategy
	for rows.Next() {
		var item Strategy
		var head *string
		if err := rows.Scan(&item.ID, &item.UserID, &item.Name, &head, &item.SourceSessionID, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		if head != nil {
			item.HeadRevision = Revision(*head)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func decodeCursor(cursor string) (time.Time, string, bool) {
	if cursor == "" {
		return time.Time{}, "", false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", false
	}
	createdAtText, id, ok := strings.Cut(string(decoded), "|")
	if !ok || id == "" {
		return time.Time{}, "", false
	}
	createdAt, err := time.Parse(time.RFC3339Nano, createdAtText)
	if err != nil {
		return time.Time{}, "", false
	}
	return createdAt, id, true
}

// BlobCount reports a strategy's distinct content-addressed blobs for tests.
func (s *PgStore) BlobCount(ctx context.Context, strategyID string) (int, error) {
	var count int
	err := s.Pool.QueryRow(ctx, `select count(*) from agent.strategy_blobs where strategy_id=$1`, strategyID).Scan(&count)
	return count, err
}

// SweepCapturePending deletes strategy_capture_pending rows whose
// created_at is older than olderThan. The predicate uses server
// time (`now() - $1::interval`) so it is idempotent across
// replicas and survives clock skew better than client-supplied
// timestamps. Returns the count for log/metric surfaces.
func (s *PgStore) SweepCapturePending(ctx context.Context, olderThan time.Duration) (int, error) {
	if olderThan <= 0 {
		return 0, fmt.Errorf("strategies: olderThan must be > 0, got %s", olderThan)
	}
	tag, err := s.Pool.Exec(ctx, `
		delete from agent.strategy_capture_pending
		where created_at < now() - $1::interval`,
		olderThan.String())
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
