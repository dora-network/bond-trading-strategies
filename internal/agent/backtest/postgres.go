package backtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxPageSize caps List requests; matches internal/strategies so the
// handler layer doesn't need per-store knowledge.
const MaxPageSize = 100

const defaultPageSize = 50

// selectBacktestSQL is the column list shared by Get and List.
const selectBacktestSQL = `
	select id, strategy_id, version_id, user_id, status,
	       requested_at, started_at, finished_at, error_message,
	       window_start, window_end, resolution, params, summary,
	       fill_count, container_id, image_ref, order_book_id
	from agent.backtests
`

// scannable is the minimal QueryRow/Row contract; lets Get and List share
// the same scan helper.
type scannable interface {
	Scan(dest ...any) error
}

// PgStore is the pgx-backed Store. The migration owns the schema
// (internal/store/migrations/004_backtesting.sql); this struct just maps
// rows to the typed Backtest API.
type PgStore struct {
	Pool *pgxpool.Pool
}

// NewPgStore wraps an existing pool. The pool is owned by the test fixture
// in tests and by the HTTP server in production; we don't manage its
// lifecycle here.
func NewPgStore(pool *pgxpool.Pool) *PgStore {
	return &PgStore{Pool: pool}
}

var _ Store = (*PgStore)(nil)

// Create assigns an ID and inserts a StatusQueued row. b.Status is
// overwritten with StatusQueued so the caller can't smuggle a terminal
// status past the single-flight index.
func (s *PgStore) Create(ctx context.Context, b *Backtest) (string, error) {
	if b.ID == "" {
		b.ID = uuid.NewString()
	}
	b.Status = StatusQueued
	paramsJSON, err := json.Marshal(b.Params)
	if err != nil {
		return "", fmt.Errorf("marshal params: %w", err)
	}
	if b.Params == nil {
		paramsJSON = []byte("{}")
	}
	_, err = s.Pool.Exec(
		ctx, `
		insert into agent.backtests
		    (id, strategy_id, version_id, user_id, status,
		     window_start, window_end, resolution, params, image_ref,
		     order_book_id)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11)`,
		b.ID, b.StrategyID, b.VersionID, b.UserID, string(b.Status),
		b.WindowStart, b.WindowEnd, b.Resolution, paramsJSON, b.ImageRef,
		b.OrderBookID,
	)
	if err != nil {
		return "", err
	}
	return b.ID, nil
}

// Get loads a row by ID. ErrNotFound if absent.
func (s *PgStore) Get(ctx context.Context, id string) (*Backtest, error) {
	row := s.Pool.QueryRow(ctx, selectBacktestSQL+` where id=$1`, id)
	return scanBacktest(row)
}

// List returns up to limit backtests for the strategy, newest first.
// limit<=0 falls back to the default 50.
func (s *PgStore) List(ctx context.Context, strategyID string, limit int) ([]*Backtest, error) {
	if limit <= 0 || limit > MaxPageSize {
		limit = defaultPageSize
	}
	rows, err := s.Pool.Query(ctx,
		selectBacktestSQL+` where strategy_id=$1 order by requested_at desc, id desc limit $2`,
		strategyID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Backtest
	for rows.Next() {
		b, err := scanBacktest(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ListBacktests is the user-scoped list used by the LLM tool. The
// user_id predicate is mandatory and unconditional; strategy_id and
// status are optional filters (nil = "any"). Building the WHERE
// clause dynamically keeps ownership in SQL so a caller can't leak
// other users' rows by skipping a second-pass filter.
//
// Args:
//
//	userID     - mandatory; rows not owned by this user are filtered out.
//	strategyID - optional *string; nil means "any strategy".
//	status     - optional *Status; nil means "any status".
//	limit      - clamped to [1, MaxPageSize]; <=0 falls back to defaultPageSize.
func (s *PgStore) ListBacktests(ctx context.Context, userID string, strategyID *string, status *Status, limit int) ([]*Backtest, error) {
	if limit <= 0 || limit > MaxPageSize {
		limit = defaultPageSize
	}
	// Build the WHERE clause dynamically. user_id is always $1;
	// strategy_id (if present) takes $2; status (if present) takes
	// the next slot. Order: strategy first because status filter is
	// the most selective in practice (most users have 1-2 running
	// rows but hundreds of succeeded ones), so putting it later
	// keeps the parameter indices predictable for callers building
	// query strings. We keep it simple here: predicate order is
	// always user -> strategy -> status.
	q := selectBacktestSQL + ` where user_id=$1`
	args := []any{userID}
	if strategyID != nil {
		args = append(args, *strategyID)
		q += ` and strategy_id=$` + strconv.Itoa(len(args))
	}
	if status != nil {
		args = append(args, string(*status))
		q += ` and status=$` + strconv.Itoa(len(args))
	}
	q += ` order by requested_at desc, id desc limit $` + strconv.Itoa(len(args)+1)
	args = append(args, limit)

	rows, err := s.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("backtest: list backtests: %w", err)
	}
	defer rows.Close()
	var out []*Backtest
	for rows.Next() {
		b, err := scanBacktest(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// UpdateStatus writes status plus optional timestamps and error message.
// Pass nil for startedAt/finishedAt to leave the column untouched.
func (s *PgStore) UpdateStatus(ctx context.Context, id string, status Status,
	startedAt, finishedAt *time.Time, errorMsg string,
) error {
	_, err := s.Pool.Exec(ctx, `
		update agent.backtests
		set status=$2,
		    started_at=coalesce($3, started_at),
		    finished_at=coalesce($4, finished_at),
		    error_message=nullif($5,'')
		where id=$1`,
		id, string(status), startedAt, finishedAt, errorMsg)
	return err
}

// SetSummary persists the final stats and flips the row to succeeded with
// finished_at = now(). Called from the UDS /summary receiver.
func (s *PgStore) SetSummary(ctx context.Context, id string, summary *Summary, fillCount int) error {
	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		return fmt.Errorf("marshal summary: %w", err)
	}
	_, err = s.Pool.Exec(ctx, `
		update agent.backtests
		set summary=$2::jsonb, fill_count=$3,
		    status='succeeded', finished_at=now()
		where id=$1`, id, summaryJSON, fillCount)
	return err
}

// InsertFills batch-inserts fills via pgx CopyFrom. Ponytail: single
// round-trip beats N individual inserts; phase 1 may add COPY-FROM-STDIN
// tuning if fills-per-job ever pushes past ~10k.
func (s *PgStore) InsertFills(ctx context.Context, backtestID string, fills []Fill) error {
	if len(fills) == 0 {
		return nil
	}
	rows := make([][]any, len(fills))
	for i, f := range fills {
		rows[i] = []any{
			uuid.NewString(), backtestID,
			f.Timestamp, f.Side, f.Quantity, f.Price,
			f.OrderID, f.SimulatedAt,
		}
	}
	_, err := s.Pool.CopyFrom(
		ctx,
		pgx.Identifier{"agent", "backtest_fills"},
		[]string{"id", "backtest_id", "timestamp", "side", "quantity", "price", "order_id", "simulated_at"},
		pgx.CopyFromRows(rows),
	)
	return err
}

// GetFills returns fills ordered by timestamp asc.
func (s *PgStore) GetFills(ctx context.Context, backtestID string) ([]Fill, error) {
	rows, err := s.Pool.Query(ctx, `
		select timestamp, side, quantity, price, order_id, simulated_at
		from agent.backtest_fills
		where backtest_id=$1
		order by timestamp, id`, backtestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Fill
	for rows.Next() {
		var f Fill
		if err := rows.Scan(&f.Timestamp, &f.Side, &f.Quantity, &f.Price, &f.OrderID, &f.SimulatedAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// CancelIfRunning atomically flips queued|running -> cancelled with
// finished_at = now(). wasRunning is true iff the update touched a row;
// the HTTP cancel handler treats both branches as 204 (idempotent).
func (s *PgStore) CancelIfRunning(ctx context.Context, id string) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `
		update agent.backtests
		set status='cancelled', finished_at=now(), error_message=null
		where id=$1 and status in ('queued','running')`, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// FailOrphaned flips every queued|running row to failed with a fixed error
// message. Used by the startup janitor to clean up after a crashed agent.
// Touches the table once and reports the row count.
func (s *PgStore) FailOrphaned(ctx context.Context) (int, error) {
	tag, err := s.Pool.Exec(ctx, `
		update agent.backtests
		set status='failed', finished_at=now(), error_message='agent restarted'
		where status in ('queued','running')`)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func scanBacktest(row scannable) (*Backtest, error) {
	var b Backtest
	var status string
	var startedAt, finishedAt *time.Time
	var errMsg *string
	var paramsRaw, summaryRaw []byte
	var fillCount *int
	var containerID *string
	var orderBookID *string
	if err := row.Scan(
		&b.ID, &b.StrategyID, &b.VersionID, &b.UserID, &status,
		&b.RequestedAt, &startedAt, &finishedAt, &errMsg,
		&b.WindowStart, &b.WindowEnd, &b.Resolution, &paramsRaw, &summaryRaw,
		&fillCount, &containerID, &b.ImageRef, &orderBookID,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if orderBookID != nil {
		b.OrderBookID = *orderBookID
	}
	b.Status = Status(status)
	b.StartedAt = startedAt
	b.FinishedAt = finishedAt
	if errMsg != nil {
		b.ErrorMessage = *errMsg
	}
	if fillCount != nil {
		b.FillCount = *fillCount
	}
	if containerID != nil {
		b.ContainerID = *containerID
	}
	if len(paramsRaw) > 0 {
		params := map[string]string{}
		if err := json.Unmarshal(paramsRaw, &params); err == nil {
			b.Params = params
		}
	}
	if len(summaryRaw) > 0 {
		var sum Summary
		if err := json.Unmarshal(summaryRaw, &sum); err == nil {
			b.Summary = &sum
		}
	}
	return &b, nil
}
