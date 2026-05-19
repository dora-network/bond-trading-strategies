package http

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/jackc/pgx/v5/pgxpool"
)

type BacktestStore interface {
	LoadBacktests(ctx context.Context) ([]*BacktestDetail, error)
	LoadBacktestResult(ctx context.Context, id uuid.UUID) (*BacktestResult, error)
	SaveBacktest(ctx context.Context, detail *BacktestDetail) error
	GetBacktestTrades(ctx context.Context, id uuid.UUID, page, limit int) ([]TradeRecord, error)
	GetBacktestClosedTrades(ctx context.Context, id uuid.UUID, page, limit int) ([]ClosedTrade, error)
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

func (s *PGBacktestStore) LoadBacktestResult(ctx context.Context, id uuid.UUID) (*BacktestResult, error) {
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

	var result BacktestResult
	if err := json.Unmarshal([]byte(*resultJSON), &result); err != nil {
		return nil, fmt.Errorf("unmarshal backtest result: %w", err)
	}

	return &result, nil
}

func (s *PGBacktestStore) SaveBacktest(ctx context.Context, detail *BacktestDetail) error {
	var resultJSON *string
	if detail.Result != nil {
		b, err := json.Marshal(detail.Result)
		if err != nil {
			return fmt.Errorf("marshal backtest result: %w", err)
		}
		s := string(b)
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

func (s *PGBacktestStore) GetBacktestTrades(ctx context.Context, id uuid.UUID, page, limit int) ([]TradeRecord, error) {
	result, err := s.LoadBacktestResult(ctx, id)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return []TradeRecord{}, nil
	}
	return paginate(result.TradeRecords, page, limit), nil
}

func (s *PGBacktestStore) GetBacktestClosedTrades(ctx context.Context, id uuid.UUID, page, limit int) ([]ClosedTrade, error) {
	result, err := s.LoadBacktestResult(ctx, id)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return []ClosedTrade{}, nil
	}
	return paginate(result.ClosedTrades, page, limit), nil
}

func paginate[T any](items []T, page, limit int) []T {
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = defaultPaginationLimit
	}
	if limit > maxPaginationLimit {
		limit = maxPaginationLimit
	}

	start := (page - 1) * limit
	if start >= len(items) {
		return []T{}
	}

	end := start + limit
	if end > len(items) {
		end = len(items)
	}

	return items[start:end]
}
func signalFromString(s string) types.Signal {
	switch s {
	case "BUY":
		return types.SignalBuy
	case "SELL":
		return types.SignalSell
	default:
		return types.SignalHold
	}
}

func tradeToClosedTrade(t ClosedTrade) (types.ClosedTrade, error) {
	entrySpread, err := decimal.Parse(t.EntrySpread)
	if err != nil {
		return types.ClosedTrade{}, fmt.Errorf("parse entry_spread: %w", err)
	}
	exitSpread, err := decimal.Parse(t.ExitSpread)
	if err != nil {
		return types.ClosedTrade{}, fmt.Errorf("parse exit_spread: %w", err)
	}
	entryZScore, err := decimal.Parse(t.EntryZScore)
	if err != nil {
		return types.ClosedTrade{}, fmt.Errorf("parse entry_zscore: %w", err)
	}
	exitZScore, err := decimal.Parse(t.ExitZScore)
	if err != nil {
		return types.ClosedTrade{}, fmt.Errorf("parse exit_zscore: %w", err)
	}
	positionSize, err := decimal.Parse(t.PositionSize)
	if err != nil {
		return types.ClosedTrade{}, fmt.Errorf("parse position_size: %w", err)
	}
	pnl, err := decimal.Parse(t.PnL)
	if err != nil {
		return types.ClosedTrade{}, fmt.Errorf("parse pnl: %w", err)
	}
	entryPrice, err := decimal.Parse(t.EntryPrice)
	if err != nil {
		return types.ClosedTrade{}, fmt.Errorf("parse entry_price: %w", err)
	}
	exitPrice, err := decimal.Parse(t.ExitPrice)
	if err != nil {
		return types.ClosedTrade{}, fmt.Errorf("parse exit_price: %w", err)
	}
	quantity, err := decimal.Parse(t.Quantity)
	if err != nil {
		return types.ClosedTrade{}, fmt.Errorf("parse quantity: %w", err)
	}
	entryBalance, err := decimal.Parse(t.EntryBalance)
	if err != nil {
		return types.ClosedTrade{}, fmt.Errorf("parse entry_balance: %w", err)
	}

	return types.ClosedTrade{
		BondID:       t.BondID,
		OpenTime:     t.OpenTime,
		CloseTime:    t.CloseTime,
		Signal:       signalFromString(t.Signal),
		ExitSignal:   signalFromString(t.ExitSignal),
		EntrySpread:  entrySpread,
		ExitSpread:   exitSpread,
		EntryZScore:  entryZScore,
		ExitZScore:   exitZScore,
		PositionSize: positionSize,
		PnL:          pnl,
		ExitReason:   t.ExitReason,
		EntryPrice:   entryPrice,
		ExitPrice:    exitPrice,
		Quantity:     quantity,
		EntryBalance: entryBalance,
	}, nil
}

func tradeRecordFromHTTP(tr TradeRecord) (types.TradeRecord, error) {
	spread, err := decimal.Parse(tr.Spread)
	if err != nil {
		return types.TradeRecord{}, fmt.Errorf("parse spread: %w", err)
	}
	positionSize, err := decimal.Parse(tr.PositionSize)
	if err != nil {
		return types.TradeRecord{}, fmt.Errorf("parse position_size: %w", err)
	}
	zScore, err := decimal.Parse(tr.ZScore)
	if err != nil {
		return types.TradeRecord{}, fmt.Errorf("parse zscore: %w", err)
	}
	price, err := decimal.Parse(tr.Price)
	if err != nil {
		return types.TradeRecord{}, fmt.Errorf("parse price: %w", err)
	}
	quantity, err := decimal.Parse(tr.Quantity)
	if err != nil {
		return types.TradeRecord{}, fmt.Errorf("parse quantity: %w", err)
	}
	entryBalance, err := decimal.Parse(tr.EntryBalance)
	if err != nil {
		return types.TradeRecord{}, fmt.Errorf("parse entry_balance: %w", err)
	}

	var signal types.Signal
	switch tr.Signal {
	case "BUY":
		signal = types.SignalBuy
	case "SELL":
		signal = types.SignalSell
	default:
		signal = types.SignalHold
	}

	return types.TradeRecord{
		Time:         tr.Time,
		BondID:       tr.BondID,
		Signal:       signal,
		Spread:       spread,
		PositionSize: positionSize,
		ZScore:       zScore,
		Price:        price,
		Quantity:     quantity,
		EntryBalance: entryBalance,
	}, nil
}

func (d *BacktestDetail) ToBacktestResult() (types.BacktestResult, error) {
	if d.Result == nil {
		return types.BacktestResult{}, nil
	}

	totalPnL, err := decimal.Parse(d.Result.TotalPnL)
	if err != nil {
		return types.BacktestResult{}, fmt.Errorf("parse total_pnl: %w", err)
	}
	maxDrawdown, err := decimal.Parse(d.Result.MaxDrawdown)
	if err != nil {
		return types.BacktestResult{}, fmt.Errorf("parse max_drawdown: %w", err)
	}
	sharpeRatio, err := decimal.Parse(d.Result.SharpeRatio)
	if err != nil {
		return types.BacktestResult{}, fmt.Errorf("parse sharpe_ratio: %w", err)
	}

	closedTrades := make([]types.ClosedTrade, 0, len(d.Result.ClosedTrades))
	for _, t := range d.Result.ClosedTrades {
		ct, err := tradeToClosedTrade(t)
		if err != nil {
			return types.BacktestResult{}, err
		}
		closedTrades = append(closedTrades, ct)
	}

	tradeRecords := make([]types.TradeRecord, 0, len(d.Result.TradeRecords))
	for _, t := range d.Result.TradeRecords {
		tr, err := tradeRecordFromHTTP(t)
		if err != nil {
			return types.BacktestResult{}, err
		}
		tradeRecords = append(tradeRecords, tr)
	}

	return types.BacktestResult{
		ClosedTrades: closedTrades,
		TradeRecords: tradeRecords,
		TotalPnL:     totalPnL,
		WinCount:     d.Result.WinCount,
		LossCount:    d.Result.LossCount,
		MaxDrawdown:  maxDrawdown,
		SharpeRatio:  sharpeRatio,
	}, nil
}
