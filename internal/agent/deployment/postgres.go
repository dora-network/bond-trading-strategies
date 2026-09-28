package deployment

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStore is the Postgres-backed implementation of Store. One row per
// deploy action; lives in the deployments table (migration 011).
type PgStore struct {
	Pool *pgxpool.Pool
}

// NewPgStore wires a PgStore to an existing pool.
func NewPgStore(pool *pgxpool.Pool) *PgStore { return &PgStore{Pool: pool} }

var _ Store = (*PgStore)(nil)

// pgUniqueViolation is Postgres' SQLSTATE for unique_violation. The
// partial unique index deployments_one_active fires this when a
// second running row for the same strategy is inserted.
const pgUniqueViolation = "23505"

// scanner is the minimal Scan shape shared by pgx.Row and pgx.Rows so
// scanDeployment works for both single-row and multi-row queries.
type scanner interface {
	Scan(dest ...any) error
}

const deploymentColumns = `id, strategy_id, revision, order_book_id, resolution, user_id, params, status,
instance_id, started_at, stopped_at, stopped_reason, restart_count,
hotswapped_at, hotswapped_from_revision, created_at, updated_at,
candle_count, warmup_candles`

func (s *PgStore) Create(ctx context.Context, d Deployment) error {
	if d.ID == "" {
		d.ID = uuid.NewString()
	}
	params, err := json.Marshal(d.Params)
	if err != nil {
		return fmt.Errorf("deployment: marshal params: %w", err)
	}
	_, err = s.Pool.Exec(
		ctx, `
		insert into agent.deployments (
			id, strategy_id, revision, order_book_id, resolution, user_id, params, status,
			instance_id, started_at, candle_count, warmup_candles
		) values (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, 0, $11
		)`,
		d.ID, d.StrategyID, d.Revision, d.OrderBookID, d.Resolution, d.UserID, params, string(d.Status),
		nullableString(d.InstanceID), nullableTime(d.StartedAt), d.WarmupCandles,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return ErrAlreadyRunning
		}
		return err
	}
	return nil
}

func (s *PgStore) Get(ctx context.Context, id, userID string) (Deployment, error) {
	var out Deployment
	row := s.Pool.QueryRow(
		ctx,
		`select `+deploymentColumns+` from agent.deployments where id=$1 and user_id=$2`,
		id, userID,
	)
	if err := scanDeployment(row, &out); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Deployment{}, ErrNotFound
		}
		return Deployment{}, err
	}
	return out, nil
}

func (s *PgStore) List(ctx context.Context, strategyID, userID string, p Page) ([]Deployment, string, error) {
	limit := p.EffectiveLimit()
	args := []any{userID}
	query := `select ` + deploymentColumns + ` from agent.deployments where user_id=$1`
	if strategyID != "" {
		args = append(args, strategyID)
		query += fmt.Sprintf(" and strategy_id=$%d", len(args))
	}
	if cursorTime, cursorID, ok := decodeDeploymentCursor(p.Cursor); ok {
		args = append(args, cursorTime, cursorID)
		query += fmt.Sprintf(" and (created_at, id) < ($%d,$%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" order by created_at desc, id desc limit $%d", len(args))
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out := make([]Deployment, 0, limit+1)
	for rows.Next() {
		var item Deployment
		if err := scanDeployment(rows, &item); err != nil {
			return nil, "", err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	items, next := deploymentPage(out, limit)
	return items, next, nil
}

func (s *PgStore) UpdateStatus(ctx context.Context, id, userID string, status Status, reason string) error {
	tag, err := s.Pool.Exec(
		ctx, `
		update agent.deployments
		set status=$3,
		    stopped_reason=$4,
		    stopped_at = case when $3 in ('stopped','crashed') and stopped_at is null
		                      then now() else stopped_at end,
		    updated_at = now()
		where id=$1 and user_id=$2`,
		id, userID, string(status), reason,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PgStore) SetInstance(ctx context.Context, id, userID, instanceID string) error {
	tag, err := s.Pool.Exec(
		ctx, `
		update agent.deployments set instance_id=$3, updated_at=now()
		where id=$1 and user_id=$2`,
		id, userID, instanceID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// HotSwap atomically retargets the running deployment at newRevision,
// capturing the prior revision into hotswapped_from_revision and
// stamping hotswapped_at. The PostgreSQL `revision` reference on the
// right-hand side of the SET reads the row's pre-update value, so a
// single statement does the swap without a round-trip.
func (s *PgStore) HotSwap(ctx context.Context, id, userID, newRevision string) error {
	tag, err := s.Pool.Exec(
		ctx, `
		update agent.deployments
		set revision=$3,
		    hotswapped_from_revision=revision,
		    hotswapped_at=now(),
		    updated_at=now()
		where id=$1 and user_id=$2`,
		id, userID, newRevision,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PgStore) IncRestart(ctx context.Context, id, userID string) error {
	tag, err := s.Pool.Exec(
		ctx, `
		update agent.deployments set restart_count=restart_count+1, updated_at=now()
		where id=$1 and user_id=$2`,
		id, userID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetCandleCount overwrites the persisted candle count for a
// deployment. The livehost throttles writes to a coarse interval
// (default 30s) so the DB write rate stays bounded regardless of
// candle resolution. The orchestrator seeds state.candleCount with
// this value at the top of Deploy so Stats() reports the cumulative
// count since the strategy was first deployed (modulo any
// unpersisted ticks since the last throttle).
func (s *PgStore) SetCandleCount(ctx context.Context, id, userID string, n int64) error {
	tag, err := s.Pool.Exec(
		ctx, `
		update agent.deployments set candle_count=$3, updated_at=now()
		where id=$1 and user_id=$2`,
		id, userID, n,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateWarmupCandles overwrites the persisted warmup_candles for a
// deployment. Deploy re-syncs the row to the manifest's value after
// the runtime starts: the HTTP handler / tool seeds the row from the
// request's warmup_candles field, which can disagree with the
// manifest (e.g. the request says 0 while the manifest declares 200).
// The manifest is authoritative — Resume / Recover rebuild the warmup
// window from the row, so a stale row would rebuild it wrong.
func (s *PgStore) UpdateWarmupCandles(ctx context.Context, id, userID string, warmupCandles int) error {
	tag, err := s.Pool.Exec(
		ctx, `
		update agent.deployments set warmup_candles=$3, updated_at=now()
		where id=$1 and user_id=$2`,
		id, userID, warmupCandles,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PgStore) HasActive(ctx context.Context, strategyID string) (bool, error) {
	var exists bool
	err := s.Pool.QueryRow(
		ctx,
		`select exists(select 1 from agent.deployments where strategy_id=$1 and status='running')`,
		strategyID,
	).Scan(&exists)
	return exists, err
}

// ListRunning returns every running deployment regardless of owner so
// the orchestrator can rebuild its in-memory mirror on boot.
func (s *PgStore) ListRunning(ctx context.Context) ([]Deployment, error) {
	rows, err := s.Pool.Query(
		ctx,
		`select `+deploymentColumns+` from agent.deployments where status='running' order by created_at desc`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Deployment, 0)
	for rows.Next() {
		var item Deployment
		if err := scanDeployment(rows, &item); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// scanDeployment reads a deployment row into d. Nullable columns are
// scanned via pointer intermediaries so a NULL maps to the zero value
// rather than aborting the scan.
func scanDeployment(r scanner, d *Deployment) error {
	var params []byte
	var instanceID, stoppedReason, hotswappedFrom *string
	var startedAt, stoppedAt, hotswappedAt *time.Time
	var status string
	var candleCount int64
	err := r.Scan(
		&d.ID, &d.StrategyID, &d.Revision, &d.OrderBookID, &d.Resolution, &d.UserID, &params, &status,
		&instanceID, &startedAt, &stoppedAt, &stoppedReason, &d.RestartCount,
		&hotswappedAt, &hotswappedFrom, &d.CreatedAt, &d.UpdatedAt,
		&candleCount, &d.WarmupCandles,
	)
	if err != nil {
		return err
	}
	d.Status = Status(status)
	d.CandleCount = candleCount
	if len(params) > 0 {
		if err := json.Unmarshal(params, &d.Params); err != nil {
			return fmt.Errorf("deployment: unmarshal params: %w", err)
		}
	}
	if instanceID != nil {
		d.InstanceID = *instanceID
	}
	if stoppedReason != nil {
		d.StoppedReason = *stoppedReason
	}
	if startedAt != nil {
		d.StartedAt = startedAt
	}
	if stoppedAt != nil {
		d.StoppedAt = stoppedAt
	}
	if hotswappedAt != nil {
		d.HotswappedAt = hotswappedAt
	}
	if hotswappedFrom != nil {
		d.HotswappedFromRev = *hotswappedFrom
	}
	return nil
}

// deploymentPage trims a limit+1 probe slice and emits the next cursor
// when more pages remain. The cursor encodes (created_at, id) so the
// next query can resume with a strict-less-than tuple comparison.
func deploymentPage(out []Deployment, limit int) ([]Deployment, string) {
	if len(out) <= limit {
		return out, ""
	}
	next := encodeDeploymentCursor(out[limit-1].CreatedAt, out[limit-1].ID)
	return out[:limit], next
}

type deploymentCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

func encodeDeploymentCursor(createdAt time.Time, id string) string {
	b, _ := json.Marshal(deploymentCursor{CreatedAt: createdAt, ID: id})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeDeploymentCursor(cursor string) (time.Time, string, bool) {
	if cursor == "" {
		return time.Time{}, "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", false
	}
	var c deploymentCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return time.Time{}, "", false
	}
	return c.CreatedAt, c.ID, true
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}
