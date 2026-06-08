package http

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type BacktestStore interface {
	LoadBacktests(ctx context.Context) ([]*BacktestDetail, error)
	LoadBacktestResult(ctx context.Context, id uuid.UUID) (json.RawMessage, error)
	SaveBacktest(ctx context.Context, detail *BacktestDetail) error
	GetBacktestTrades(ctx context.Context, id uuid.UUID, strategyType string, page, limit int) (json.RawMessage, error)
	GetBacktestClosedTrades(ctx context.Context, id uuid.UUID, strategyType string, page, limit int) (json.RawMessage, error)
	WriteTradeRecord(ctx context.Context, rec stats.TradeRecordInsert) error
	WriteClosedTrade(ctx context.Context, trade stats.ClosedTradeInsert) error
	WriteTradeRecordsBatch(ctx context.Context, recs []stats.TradeRecordInsert) error
	WriteClosedTradesBatch(ctx context.Context, trades []stats.ClosedTradeInsert) error
}

type PGBacktestStore struct {
	pool *pgxpool.Pool
}

func NewPGBacktestStore(pool *pgxpool.Pool) *PGBacktestStore {
	return &PGBacktestStore{pool: pool}
}

func (s *PGBacktestStore) LoadBacktests(ctx context.Context) ([]*BacktestDetail, error) {
	const q = `
		SELECT id, dora_user_id, strategy_type, status, config, start, "end", created_at, completed_at, error
		FROM strategy_backtests
	`

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query strategy backtests: %w", err)
	}
	defer rows.Close()

	backtests := make([]*BacktestDetail, 0)
	for rows.Next() {
		var detail BacktestDetail
		if err := rows.Scan(
			&detail.ID,
			&detail.DORAUserID,
			&detail.StrategyType,
			&detail.Status,
			&detail.Config,
			&detail.Start,
			&detail.End,
			&detail.CreatedAt,
			&detail.CompletedAt,
			&detail.Error,
		); err != nil {
			return nil, fmt.Errorf("scan strategy backtest: %w", err)
		}
		backtests = append(backtests, &detail)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate strategy backtests: %w", err)
	}
	return backtests, nil
}

func (s *PGBacktestStore) LoadBacktestResult(ctx context.Context, id uuid.UUID) (json.RawMessage, error) {
	const q = `
		SELECT result
		FROM strategy_backtests
		WHERE id = $1
	`

	var resultJSON *string
	if err := s.pool.QueryRow(ctx, q, id).Scan(&resultJSON); err != nil {
		return nil, fmt.Errorf("query strategy backtest result: %w", err)
	}

	if resultJSON == nil || *resultJSON == "" {
		return nil, nil
	}

	return json.RawMessage(*resultJSON), nil
}

func (s *PGBacktestStore) SaveBacktest(ctx context.Context, detail *BacktestDetail) error {
	var resultJSON *string
	if len(detail.Result) > 0 {
		s := string(detail.Result)
		resultJSON = &s
	}

	const q = `
		INSERT INTO strategy_backtests (id, dora_user_id, strategy_type, status, config, start, "end", created_at, completed_at, error, result)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (id)
		DO UPDATE SET
			dora_user_id = EXCLUDED.dora_user_id,
			strategy_type = EXCLUDED.strategy_type,
			status = EXCLUDED.status,
			config = EXCLUDED.config,
			start = EXCLUDED.start,
			"end" = EXCLUDED."end",
			created_at = EXCLUDED.created_at,
			completed_at = EXCLUDED.completed_at,
			error = EXCLUDED.error,
			result = EXCLUDED.result
	`

	if _, err := s.pool.Exec(
		ctx,
		q,
		detail.ID,
		detail.DORAUserID,
		detail.StrategyType,
		detail.Status,
		detail.Config,
		detail.Start,
		detail.End,
		detail.CreatedAt,
		detail.CompletedAt,
		detail.Error,
		resultJSON,
	); err != nil {
		return fmt.Errorf("save strategy backtest %s: %w", detail.ID, err)
	}

	return nil
}

func (s *PGBacktestStore) GetBacktestTrades(
	ctx context.Context,
	id uuid.UUID,
	strategyType string,
	page, limit int,
) (json.RawMessage, error) {
	rows, err := s.listTradeRecords(ctx, id, page, limit)
	if err != nil {
		return nil, err
	}
	items := make([]json.RawMessage, 0, len(rows))
	for i := range rows {
		b, err := tradeRecordToResponse(strategyType, &rows[i])
		if err != nil {
			return nil, err
		}
		items = append(items, b)
	}
	return marshalItems(items)
}

func (s *PGBacktestStore) GetBacktestClosedTrades(
	ctx context.Context,
	id uuid.UUID,
	strategyType string,
	page, limit int,
) (json.RawMessage, error) {
	rows, err := s.listClosedTrades(ctx, id, page, limit)
	if err != nil {
		return nil, err
	}
	items := make([]json.RawMessage, 0, len(rows))
	for i := range rows {
		b, err := closedTradeToResponse(strategyType, &rows[i])
		if err != nil {
			return nil, err
		}
		items = append(items, b)
	}
	return marshalItems(items)
}

// WriteTradeRecord appends a single trade record to strategy_backtest_trades.
func (s *PGBacktestStore) WriteTradeRecord(ctx context.Context, rec stats.TradeRecordInsert) error {
	const q = `
		INSERT INTO strategy_backtest_trades (
			backtest_id, time, bond_id, signal, price, quantity, entry_balance,
			order_size, cash, open_position, trade_id,
			spread, position_size, zscore
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11,
			$12, $13, $14
		)
	`
	var bondID, tradeID any
	if rec.BondID != "" {
		bondID, _ = uuid.Parse(rec.BondID)
	}
	if rec.TradeID != uuid.Nil {
		tradeID = rec.TradeID
	}
	_, err := s.pool.Exec(ctx, q,
		rec.BacktestID, rec.Time, bondID, rec.Signal,
		nullableDecimal(rec.Price), nullableDecimal(rec.Quantity), nullableDecimal(rec.EntryBalance),
		nullableDecimal(rec.OrderSize), nullableDecimal(rec.Cash), nullableDecimal(rec.OpenPosition), tradeID,
		nullableDecimal(rec.Spread), nullableDecimal(rec.PositionSize), nullableDecimal(rec.ZScore),
	)
	if err != nil {
		return fmt.Errorf("write trade record for backtest %s: %w", rec.BacktestID, err)
	}
	return nil
}

// WriteClosedTrade appends a single closed trade to strategy_backtest_closed_trades.
func (s *PGBacktestStore) WriteClosedTrade(ctx context.Context, trade stats.ClosedTradeInsert) error {
	const q = `
		INSERT INTO strategy_backtest_closed_trades (
			backtest_id, open_time, close_time, bond_id, open_signal, close_signal,
			quantity, entry_price, exit_price, pnl, entry_balance,
			open_trade_id, close_trade_id,
			entry_spread, exit_spread, entry_zscore, exit_zscore, position_size, exit_reason
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11,
			$12, $13,
			$14, $15, $16, $17, $18, $19
		)
	`
	var bondID, openID, closeID any
	if trade.BondID != "" {
		bondID, _ = uuid.Parse(trade.BondID)
	}
	if trade.OpenTradeID != uuid.Nil {
		openID = trade.OpenTradeID
	}
	if trade.CloseTradeID != uuid.Nil {
		closeID = trade.CloseTradeID
	}
	_, err := s.pool.Exec(ctx, q,
		trade.BacktestID, trade.OpenTime, trade.CloseTime, bondID, trade.OpenSignal, trade.CloseSignal,
		trade.Quantity.String(), nullableDecimal(trade.EntryPrice), nullableDecimal(trade.ExitPrice),
		trade.PnL.String(), nullableDecimal(trade.EntryBalance),
		openID, closeID,
		nullableDecimal(trade.EntrySpread), nullableDecimal(trade.ExitSpread),
		nullableDecimal(trade.EntryZScore), nullableDecimal(trade.ExitZScore),
		nullableDecimal(trade.PositionSize), nullableString(trade.ExitReason),
	)
	if err != nil {
		return fmt.Errorf("write closed trade for backtest %s: %w", trade.BacktestID, err)
	}
	return nil
}

// WriteTradeRecordsBatch uses pgx.CopyFrom to bulk-insert trade rows in a
// single COPY command. For a 20-day backtest emitting ~170k trade rows,
// this turns a multi-minute per-row-INSERT loop into sub-second
// batches.
func (s *PGBacktestStore) WriteTradeRecordsBatch(ctx context.Context, recs []stats.TradeRecordInsert) error {
	if len(recs) == 0 {
		return nil
	}
	rows := make([][]any, len(recs))
	for i, r := range recs {
		rows[i] = tradeRecordInsertRow(r)
	}
	src := pgx.CopyFromSlice(len(recs), func(i int) ([]any, error) { return rows[i], nil })
	_, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"strategy_backtest_trades"},
		tradeRecordCopyColumns,
		src,
	)
	if err != nil {
		return fmt.Errorf("write trade records batch (%d rows) for backtest %s: %w", len(recs), recs[0].BacktestID, err)
	}
	return nil
}

// WriteClosedTradesBatch is the bulk-insert counterpart to
// WriteClosedTrade. See WriteTradeRecordsBatch for rationale.
func (s *PGBacktestStore) WriteClosedTradesBatch(ctx context.Context, trades []stats.ClosedTradeInsert) error {
	if len(trades) == 0 {
		return nil
	}
	rows := make([][]any, len(trades))
	for i, t := range trades {
		rows[i] = closedTradeInsertRow(t)
	}
	src := pgx.CopyFromSlice(len(trades), func(i int) ([]any, error) { return rows[i], nil })
	_, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"strategy_backtest_closed_trades"},
		closedTradeCopyColumns,
		src,
	)
	if err != nil {
		return fmt.Errorf("write closed trades batch (%d rows) for backtest %s: %w", len(trades), trades[0].BacktestID, err)
	}
	return nil
}

// Flush is a no-op for the raw PGBacktestStore: every WriteXxx call
// is already committed synchronously. The batching layer wraps this
// store; its own Flush drains accumulated rows via WriteXxxBatch.
func (s *PGBacktestStore) Flush(_ context.Context) error { return nil }

// tradeRecordInsertRow maps a TradeRecordInsert to the column order
// declared in tradeRecordCopyColumns.
func tradeRecordInsertRow(r stats.TradeRecordInsert) []any {
	var bondID, tradeID any
	if r.BondID != "" {
		bondID, _ = uuid.Parse(r.BondID)
	}
	if r.TradeID != uuid.Nil {
		tradeID = r.TradeID
	}
	return []any{
		r.BacktestID, r.Time, bondID, r.Signal,
		nullableDecimal(r.Price), nullableDecimal(r.Quantity), nullableDecimal(r.EntryBalance),
		nullableDecimal(r.OrderSize), nullableDecimal(r.Cash), nullableDecimal(r.OpenPosition), tradeID,
		nullableDecimal(r.Spread), nullableDecimal(r.PositionSize), nullableDecimal(r.ZScore),
	}
}

// closedTradeInsertRow maps a ClosedTradeInsert to the column order
// declared in closedTradeCopyColumns.
func closedTradeInsertRow(t stats.ClosedTradeInsert) []any {
	var bondID, openID, closeID any
	if t.BondID != "" {
		bondID, _ = uuid.Parse(t.BondID)
	}
	if t.OpenTradeID != uuid.Nil {
		openID = t.OpenTradeID
	}
	if t.CloseTradeID != uuid.Nil {
		closeID = t.CloseTradeID
	}
	return []any{
		t.BacktestID, t.OpenTime, t.CloseTime, bondID, t.OpenSignal, t.CloseSignal,
		t.Quantity.String(), nullableDecimal(t.EntryPrice), nullableDecimal(t.ExitPrice),
		t.PnL.String(), nullableDecimal(t.EntryBalance),
		openID, closeID,
		nullableDecimal(t.EntrySpread), nullableDecimal(t.ExitSpread),
		nullableDecimal(t.EntryZScore), nullableDecimal(t.ExitZScore),
		nullableDecimal(t.PositionSize), nullableString(t.ExitReason),
	}
}

var (
	tradeRecordCopyColumns = []string{ //nolint:gochecknoglobals
		"backtest_id", "time", "bond_id", "signal",
		"price", "quantity", "entry_balance",
		"order_size", "cash", "open_position", "trade_id",
		"spread", "position_size", "zscore",
	}
	closedTradeCopyColumns = []string{ //nolint:gochecknoglobals
		"backtest_id", "open_time", "close_time", "bond_id", "open_signal", "close_signal",
		"quantity", "entry_price", "exit_price", "pnl", "entry_balance",
		"open_trade_id", "close_trade_id",
		"entry_spread", "exit_spread", "entry_zscore", "exit_zscore", "position_size", "exit_reason",
	}
)

// tradeRecordRow is the column shape of strategy_backtest_trades.
type tradeRecordRow struct {
	ID           int64
	BacktestID   uuid.UUID
	Time         time.Time
	BondID       *uuid.UUID
	Signal       string
	Price        *string
	Quantity     *string
	EntryBalance *string
	OrderSize    *string
	Cash         *string
	OpenPosition *string
	TradeID      *uuid.UUID
	Spread       *string
	PositionSize *string
	ZScore       *string
}

func (s *PGBacktestStore) listTradeRecords(
	ctx context.Context,
	id uuid.UUID,
	page, limit int,
) ([]tradeRecordRow, error) {
	if page, limit = clampPagination(page, limit); page == 0 {
		return nil, nil
	}
	const q = `
		SELECT id, backtest_id, time, bond_id, signal, price, quantity, entry_balance,
			order_size, cash, open_position, trade_id,
			spread, position_size, zscore
		FROM strategy_backtest_trades
		WHERE backtest_id = $1
		ORDER BY time, id
		LIMIT $2 OFFSET $3
	`
	rows, err := s.pool.Query(ctx, q, id, limit, (page-1)*limit)
	if err != nil {
		return nil, fmt.Errorf("query trade records: %w", err)
	}
	defer rows.Close()
	var out []tradeRecordRow
	for rows.Next() {
		var r tradeRecordRow
		if err := rows.Scan(
			&r.ID, &r.BacktestID, &r.Time, &r.BondID, &r.Signal, &r.Price, &r.Quantity, &r.EntryBalance,
			&r.OrderSize, &r.Cash, &r.OpenPosition, &r.TradeID,
			&r.Spread, &r.PositionSize, &r.ZScore,
		); err != nil {
			return nil, fmt.Errorf("scan trade record: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// closedTradeRow is the column shape of strategy_backtest_closed_trades.
type closedTradeRow struct {
	ID           int64
	BacktestID   uuid.UUID
	OpenTime     time.Time
	CloseTime    time.Time
	BondID       *uuid.UUID
	OpenSignal   string
	CloseSignal  string
	Quantity     string
	EntryPrice   *string
	ExitPrice    *string
	PnL          string
	EntryBalance *string
	OpenTradeID  *uuid.UUID
	CloseTradeID *uuid.UUID
	EntrySpread  *string
	ExitSpread   *string
	EntryZScore  *string
	ExitZScore   *string
	PositionSize *string
	ExitReason   *string
}

func (s *PGBacktestStore) listClosedTrades(
	ctx context.Context,
	id uuid.UUID,
	page, limit int,
) ([]closedTradeRow, error) {
	if page, limit = clampPagination(page, limit); page == 0 {
		return nil, nil
	}
	const q = `
		SELECT id, backtest_id, open_time, close_time, bond_id, open_signal, close_signal,
			quantity, entry_price, exit_price, pnl, entry_balance,
			open_trade_id, close_trade_id,
			entry_spread, exit_spread, entry_zscore, exit_zscore, position_size, exit_reason
		FROM strategy_backtest_closed_trades
		WHERE backtest_id = $1
		ORDER BY close_time, id
		LIMIT $2 OFFSET $3
	`
	rows, err := s.pool.Query(ctx, q, id, limit, (page-1)*limit)
	if err != nil {
		return nil, fmt.Errorf("query closed trades: %w", err)
	}
	defer rows.Close()
	var out []closedTradeRow
	for rows.Next() {
		var r closedTradeRow
		if err := rows.Scan(
			&r.ID, &r.BacktestID, &r.OpenTime, &r.CloseTime, &r.BondID, &r.OpenSignal, &r.CloseSignal,
			&r.Quantity, &r.EntryPrice, &r.ExitPrice, &r.PnL, &r.EntryBalance,
			&r.OpenTradeID, &r.CloseTradeID,
			&r.EntrySpread, &r.ExitSpread, &r.EntryZScore, &r.ExitZScore, &r.PositionSize, &r.ExitReason,
		); err != nil {
			return nil, fmt.Errorf("scan closed trade: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func tradeRecordToResponse(strategyType string, r *tradeRecordRow) (json.RawMessage, error) {
	bondID := ""
	if r.BondID != nil {
		bondID = r.BondID.String()
	}
	price := derefString(r.Price)
	quantity := derefString(r.Quantity)
	entryBalance := derefString(r.EntryBalance)
	switch strategyType {
	case "copytrading":
		rec := CopyTradingTradeRecord{
			Time:         r.Time,
			BondID:       bondID,
			Signal:       r.Signal,
			Price:        price,
			Quantity:     quantity,
			OrderSize:    derefString(r.OrderSize),
			Cash:         derefString(r.Cash),
			OpenPosition: derefString(r.OpenPosition),
		}
		if r.TradeID != nil {
			rec.TradeID = r.TradeID.String()
		}
		return json.Marshal(rec)
	default:
		rec := MeanReversionTradeRecord{
			Time:         r.Time,
			BondID:       bondID,
			Signal:       r.Signal,
			Spread:       derefString(r.Spread),
			PositionSize: derefString(r.PositionSize),
			ZScore:       derefString(r.ZScore),
			Price:        price,
			Quantity:     quantity,
			EntryBalance: entryBalance,
		}
		return json.Marshal(rec)
	}
}

func closedTradeToResponse(strategyType string, r *closedTradeRow) (json.RawMessage, error) {
	bondID := ""
	if r.BondID != nil {
		bondID = r.BondID.String()
	}
	switch strategyType {
	case "copytrading":
		ct := CopyTradingClosedTrade{
			OpenTime:   r.OpenTime,
			CloseTime:  r.CloseTime,
			BondID:     bondID,
			OpenSignal: r.OpenSignal,
			// CloseSignal: not in ClosedTradeInsert struct as separate field; reuse OpenSignal aliasing in meanreversion
			Quantity:     r.Quantity,
			EntryPrice:   derefString(r.EntryPrice),
			ExitPrice:    derefString(r.ExitPrice),
			PnL:          r.PnL,
			EntryBalance: derefString(r.EntryBalance),
		}
		// copytrading ClosedTrade uses OpenSignal and CloseSignal same way; we used OpenSignal above, need CloseSignal:
		ct.CloseSignal = r.CloseSignal
		if r.OpenTradeID != nil {
			ct.OpenTradeID = r.OpenTradeID.String()
		}
		if r.CloseTradeID != nil {
			ct.CloseTradeID = r.CloseTradeID.String()
		}
		return json.Marshal(ct)
	default:
		ct := MeanReversionClosedTrade{
			BondID:       bondID,
			OpenTime:     r.OpenTime,
			CloseTime:    r.CloseTime,
			Signal:       r.OpenSignal,
			ExitSignal:   r.CloseSignal,
			EntrySpread:  derefString(r.EntrySpread),
			ExitSpread:   derefString(r.ExitSpread),
			EntryZScore:  derefString(r.EntryZScore),
			ExitZScore:   derefString(r.ExitZScore),
			PositionSize: derefString(r.PositionSize),
			PnL:          r.PnL,
			ExitReason:   derefString(r.ExitReason),
			EntryPrice:   derefString(r.EntryPrice),
			ExitPrice:    derefString(r.ExitPrice),
			Quantity:     r.Quantity,
			EntryBalance: derefString(r.EntryBalance),
		}
		return json.Marshal(ct)
	}
}

// derefString returns the empty string if p is nil, otherwise *p.
func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// nullableString returns nil for "" so the column is stored as NULL.
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullableDecimal returns nil for the zero decimal so the column is stored
// as NULL. Used for strategy-specific fields that don't apply to every
// strategy (e.g. OrderSize for meanreversion trades).
func nullableDecimal(d decimal.Decimal) any {
	if d.IsZero() {
		return nil
	}
	return d.String()
}

// clampPagination normalises page/limit to positive values and applies the
// per-page maximum. Returns 0 for page when the result is out of range.
func clampPagination(page, limit int) (int, int) {
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = defaultPaginationLimit
	}
	if limit > maxPaginationLimit {
		limit = maxPaginationLimit
	}
	return page, limit
}

func marshalItems(items []json.RawMessage) (json.RawMessage, error) {
	b, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		return nil, fmt.Errorf("marshal items: %w", err)
	}
	return b, nil
}
