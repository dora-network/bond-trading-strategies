// Package backtest owns the async backtest-job lifecycle: persistence (the
// Store interface) and orchestration (Submit, runner). Phase 0 ships
// types + Store + pgx-backed implementation only.
package backtest

import "time"

// Status is the backtest job state. Transitions: queued -> running ->
// {succeeded,failed,cancelled}. The partial unique index on user_id WHERE
// status IN (queued,running) enforces single-inflight per user; the handler
// maps a 23505 violation on insert to 429.
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// Backtest is the persisted job row. JSON tags follow snake_case per project
// convention (internal/httpapi wire shapes use the same names).
type Backtest struct {
	ID            string            `json:"id"`
	StrategyID    string            `json:"strategy_id"`
	VersionID     string            `json:"version_id"`
	UserID        string            `json:"user_id"`
	Status        Status            `json:"status"`
	RequestedAt   time.Time         `json:"requested_at"`
	StartedAt     *time.Time        `json:"started_at,omitempty"`
	FinishedAt    *time.Time        `json:"finished_at,omitempty"`
	ErrorMessage  string            `json:"error_message,omitempty"`
	WindowStart   time.Time         `json:"window_start"`
	WindowEnd     time.Time         `json:"window_end"`
	Resolution    string            `json:"resolution"`
	OrderBookID   string            `json:"order_book_id,omitempty"`
	Params        map[string]string `json:"params"`
	Summary       *Summary          `json:"summary,omitempty"`
	FillCount     int               `json:"fill_count,omitempty"`
	ContainerID   string            `json:"container_id,omitempty"`
	ImageRef      string            `json:"image_ref,omitempty"`
	WarmupCandles int               `json:"warmup_candles,omitempty"` // 0 = no warmup; shifts fetch window back N candles
}

// Summary holds the v1 stats set returned to the chat. Populated on
// succeeded; null while queued/running. Serialized into the backtests.summary
// jsonb column. Ponytail: formulas live in summary.go (Phase 1), not here.
type Summary struct {
	TotalReturn     float64           `json:"total_return"`
	Sharpe          float64           `json:"sharpe"`
	MaxDrawdown     float64           `json:"max_drawdown"`
	TradeCount      int               `json:"trade_count"`
	WinRate         float64           `json:"win_rate"`
	StartEquity     float64           `json:"start_equity"`
	EndEquity       float64           `json:"end_equity"`
	ParamsEffective map[string]string `json:"params_effective"`
}

// Fill is one simulated order. Persisted in backtest_fills; sourced from the
// strategy's per-candle order intents and POST'd to the UDS callback channel.
type Fill struct {
	Timestamp   time.Time `json:"timestamp"`
	Side        string    `json:"side"`
	Quantity    float64   `json:"quantity"`
	Price       float64   `json:"price"`
	OrderID     string    `json:"order_id"`
	SimulatedAt time.Time `json:"simulated_at"`
}

// Request is the wire shape accepted by POST /strategies/{id}/versions/{rev}/backtest.
// Field names match the JSON shape exactly so tagliatelle (which derives the
// expected key from the Go name under use-field-name: true) shadows the
// wire 1:1. Validation lives in validate.go (Phase 1).
type Request struct {
	Start         time.Time         `json:"start"`
	End           time.Time         `json:"end"`
	Resolution    string            `json:"resolution"`
	OrderBookID   string            `json:"order_book_id"`
	Params        map[string]string `json:"params,omitempty"`
	WarmupCandles int               `json:"warmup_candles,omitempty"` // copied to the row; shifts fetch window back N candles
}

// Response is the wire shape returned by the Submit endpoint. The handler
// translates single-inflight (23505) into 429, so 200 means accepted.
type Response struct {
	BacktestID string `json:"backtest_id"`
}
