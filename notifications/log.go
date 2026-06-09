package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Log persists Events for Last-Event-ID replay. Implementations must be
// safe for concurrent use. Replay returns events with id > afterID,
// ordered ascending by id, capped at limit.
type Log interface {
	Insert(ctx context.Context, evt Event) error
	Replay(ctx context.Context, userID, afterID string, limit int) ([]Event, error)
	DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error)
}

// PGLog is the production Log backed by Postgres + the notification_log
// table created in migration 009.
type PGLog struct {
	pool *pgxpool.Pool
}

func NewPGLog(pool *pgxpool.Pool) *PGLog { return &PGLog{pool: pool} }

func (l *PGLog) Insert(ctx context.Context, evt Event) error {
	id, err := uuid.Parse(evt.ID)
	if err != nil {
		return fmt.Errorf("notifications: invalid event id %q: %w", evt.ID, err)
	}
	payload, err := json.Marshal(evt.Payload)
	if err != nil {
		return fmt.Errorf("notifications: marshal payload: %w", err)
	}
	var runID, backtestID any
	if evt.RunID != "" {
		v, err := uuid.Parse(evt.RunID)
		if err == nil {
			runID = v
		}
	}
	if evt.BacktestID != "" {
		v, err := uuid.Parse(evt.BacktestID)
		if err == nil {
			backtestID = v
		}
	}
	const q = `
		insert into notification_log (id, user_id, type, run_id, backtest_id, payload, created_at)
		values ($1, $2, $3, $4, $5, $6, $7)
	`
	if _, err := l.pool.Exec(ctx, q,
		id, evt.UserID, string(evt.Type), runID, backtestID, payload, evt.Timestamp.UTC(),
	); err != nil {
		return fmt.Errorf("notifications: insert log: %w", err)
	}
	return nil
}

func (l *PGLog) Replay(ctx context.Context, userID, afterID string, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	var afterUUID any
	if afterID != "" {
		v, err := uuid.Parse(afterID)
		if err != nil {
			return nil, fmt.Errorf("notifications: invalid Last-Event-ID %q: %w", afterID, err)
		}
		afterUUID = v
	}
	const q = `
		select id, user_id, type, run_id, backtest_id, payload, created_at
		from notification_log
		where user_id = $1 and ($2::uuid is null or id > $2)
		order by id
		limit $3
	`
	rows, err := l.pool.Query(ctx, q, userID, afterUUID, limit)
	if err != nil {
		return nil, fmt.Errorf("notifications: replay log: %w", err)
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var (
			id, dbUserID, evtType, payload string
			runID, backtestID              *uuid.UUID
			createdAt                      time.Time
		)
		if err := rows.Scan(&id, &dbUserID, &evtType, &runID, &backtestID, &payload, &createdAt); err != nil {
			return nil, fmt.Errorf("notifications: scan log: %w", err)
		}
		evt := Event{
			ID:        id,
			Type:      EventType(evtType),
			UserID:    dbUserID,
			Timestamp: createdAt,
		}
		if runID != nil {
			evt.RunID = runID.String()
		}
		if backtestID != nil {
			evt.BacktestID = backtestID.String()
		}
		if len(payload) > 0 {
			_ = json.Unmarshal([]byte(payload), &evt.Payload)
		}
		out = append(out, evt)
	}
	return out, rows.Err()
}

func (l *PGLog) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	const q = `delete from notification_log where created_at < now() - $1::interval`
	tag, err := l.pool.Exec(ctx, q, age)
	if err != nil {
		return 0, fmt.Errorf("notifications: delete old log: %w", err)
	}
	return tag.RowsAffected(), nil
}
