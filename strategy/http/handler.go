package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/authctx"
	"github.com/dora-network/bond-trading-strategies/fred"
	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/dora-network/bond-trading-strategies/prices"
	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/breakout"
	"github.com/dora-network/bond-trading-strategies/strategy/copytrading"
	"github.com/dora-network/bond-trading-strategies/strategy/exec"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/momentum"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/twap"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/strategy/vwap"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

const (
	strategyStatusAvailable      = "available"
	strategyStatusNotImplemented = "not_implemented"
	defaultPaginationLimit       = 10
	maxPaginationLimit           = 50
	defaultTradingDecisionsLimit = 50
	// maxTradingDecisionsLimit is the upper bound for the
	// trading-decisions list endpoint.  It is higher than
	// maxPaginationLimit because the cursor-paginated contract has no
	// page*limit offset to protect and the per-row payload is narrow.
	maxTradingDecisionsLimit = 200
	// batchSize is the row count that triggers a flush in the batching
	// backtest trade writer. Tuned with flushAfter so a backtest emitting
	// rows faster than the flush interval still drains in bounded chunks.
	batchSize = 1000
	// flushAfter bounds how long buffered rows can sit before being
	// written, so a slow trickle of trades doesn't leave a tail of
	// un-flushed rows in memory.
	flushAfter = time.Second
	// defaultStopLossObserverInterval is the polling cadence the
	// handler uses to detect a stop-loss trigger on a running strategy.
	// 1s is fine for human-facing notification latency and keeps the
	// observer's per-run cost negligible.
	defaultStopLossObserverInterval = time.Second
)

// orderUpdatesBuffer is the channel buffer size for the per-run
// order-update event channel shared by TWAP and VWAP. 16 events
// cover a reasonable burst of fills from a single run before the
// run loop reads.
const orderUpdatesBuffer = 16

type Handler struct {
	service            strategycore.Service
	now                func() time.Time
	log                *slog.Logger
	strategies         map[string]StrategyDefinition
	prices             *prices.Handler
	doraClient         doraClient
	runStore           RunStore
	backtestStore      BacktestStore
	tradesHistoryStore *copytrading.PGTradesHistoryStore
	// decisionStore is invoked by the live strategy loop (meanreversion
	// and copytrading) after every successful market order to record
	// the decision that triggered it. nil disables recording; backtests
	// never opt in and therefore never write to strategy_decisions.
	decisionStore strategycore.DecisionRecorder
	// stateStore is used by execution strategies (TWAP) to checkpoint
	// progress for crash recovery. nil disables persistence; the
	// strategy runs but won't recover state across restarts.
	stateStore strategycore.StateStore
	// endpoint. The route is registered unconditionally in NewHandler;
	// when decisionReader is nil the handler short-circuits to 503 so
	// the endpoint can be deployed without wiring a reader until the
	// operator opts in. Distinct from decisionStore (the write-side
	// DecisionRecorder) so the read path carries no write concerns.
	decisionReader       DecisionReader
	tradeStream          *streams.TradeStream
	historicalPriceStore breakout.HistoricalPriceStore
	tradeHistoryStore    breakout.TradeHistoryStore
	notifier             notifications.Notifier
	orderUpdates         orderUpdatesManager // nil disables the order-update feature
	encryptionKey        []byte              // 32-byte AES-256 key for encrypting API keys at rest
	mux                  *http.ServeMux
	authedMux            http.Handler
	mu                   sync.RWMutex
	backtests            map[uuid.UUID]*BacktestDetail
	runs                 map[uuid.UUID]*RunDetail
	// runningStrategies maps a live run id to the strategy instance that
	// was started for it, so the stop-loss observer can query the
	// strategy's recorded trigger. Populated in createRun and
	// resumePersistedRun; cleared in stopRun and by the observer itself
	// when the run leaves the running state. Protected by mu.
	runningStrategies map[uuid.UUID]strategycore.Strategy
	// stopLossObservers cancels the per-run observer goroutine. Protected
	// by mu.
	stopLossObservers     map[uuid.UUID]context.CancelFunc
	runCompletionWatchers map[uuid.UUID]context.CancelFunc
	// stopLossObserverInterval is the polling cadence for the stop-loss
	// observer. Defaults to 1s; overridable for tests via
	// WithStopLossObserverInterval.
	stopLossObserverInterval time.Duration
	orderbookCache           map[string]DORAOrderBookSummary
	assetCache               map[string]AssetInfo
	cacheMu                  sync.RWMutex
}

// stopLossObserver is the minimal surface the handler needs from a
// running strategy to detect a stop-loss exit. *meanreversion.Strategy
// implements it; copytrading does not yet (see the TODO in its
// strategy.go).
type stopLossObserver interface {
	LastStopLossTrigger() (zScore, pnl decimal.Decimal, triggered bool)
}

type runStarter interface {
	RunStrategyWithID(ctx context.Context, id uuid.UUID, strategy strategycore.Strategy) error
}

type StrategyDefinition struct {
	Type             string
	Status           string
	Description      string
	ConfigFields     []StrategyConfigField
	SupportsRun      bool
	SupportsBacktest bool
	DecodeConfig     func(json.RawMessage, string, stats.BacktestTradeWriter) (json.RawMessage, strategycore.Strategy, error)
}

type StrategyConfigField struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	Required    bool   `json:"required"`
	Default     any    `json:"default,omitempty"`
}

type CreateBacktestRequest struct {
	StrategyType string          `json:"strategy_type"`
	Config       json.RawMessage `json:"config"`
	Start        time.Time       `json:"start"`
	End          time.Time       `json:"end"`
}

type CreateRunRequest struct {
	StrategyType string          `json:"strategy_type"`
	Config       json.RawMessage `json:"config"`
}

type StrategySummary struct {
	Type             string                `json:"type"`
	Status           string                `json:"status"`
	Description      string                `json:"description,omitempty"`
	ConfigFields     []StrategyConfigField `json:"config_fields,omitempty"`
	SupportsRun      bool                  `json:"supports_run"`
	SupportsBacktest bool                  `json:"supports_backtest"`
}

type TenorSummary struct {
	Code        string `json:"code"`
	Description string `json:"description"`
}

type DORAOrderBookSummary struct {
	ID           string `json:"id"`
	DisplayName  string `json:"display_name"`
	BaseAssetID  string `json:"base_asset_id"`
	QuoteAssetID string `json:"quote_asset_id"`
	Status       string `json:"status"`
}

type DORAUserSummary struct {
	ID string `json:"id"`
}

// CopyTraderSummary is a single entry in the list-copy-traders response.
// The id is the DORA user UUID and matches the `followed_trader` field
// accepted by CopyTradingConfig. user_name is the DORA-registered handle
// surfaced for display in copy-trading run configuration UIs.
type CopyTraderSummary struct {
	ID       string `json:"id"`
	UserName string `json:"user_name"`
}

type AssetInfo struct {
	Name   string `json:"name"`
	Symbol string `json:"symbol"`
}

// listable is implemented by BacktestDetail and RunDetail for generic list operations.
type listable interface {
	GetDORAUserID() string
	GetCreatedAt() time.Time
}

type BacktestResultSummary struct {
	TotalPnL     string          `json:"total_pnl"` //nolint:tagliatelle
	WinCount     int             `json:"win_count"`
	LossCount    int             `json:"loss_count"`
	MaxDrawdown  string          `json:"max_drawdown"`
	SharpeRatio  string          `json:"sharpe_ratio"`
	StrategyType string          `json:"strategy_type"`
	Status       string          `json:"status"`
	Config       json.RawMessage `json:"config"`
	AssetName    string          `json:"asset_name"`
	AssetSymbol  string          `json:"asset_symbol"`
	Error        string          `json:"error,omitempty"`
}

func (h *Handler) summaryResult(ctx context.Context, d *BacktestDetail) BacktestResultSummary {
	s := BacktestResultSummary{
		StrategyType: d.StrategyType,
		Status:       d.Status,
		Config:       d.Config,
		Error:        d.Error,
	}
	if len(d.Result) > 0 {
		var summary struct {
			TotalPnL    string `json:"total_pnl"` //nolint:tagliatelle
			WinCount    int    `json:"win_count"`
			LossCount   int    `json:"loss_count"`
			MaxDrawdown string `json:"max_drawdown"`
			SharpeRatio string `json:"sharpe_ratio"`
		}
		_ = json.Unmarshal(d.Result, &summary)
		s.TotalPnL = summary.TotalPnL
		s.WinCount = summary.WinCount
		s.LossCount = summary.LossCount
		s.MaxDrawdown = summary.MaxDrawdown
		s.SharpeRatio = summary.SharpeRatio
	}

	var cfg orderBookConfig
	if d.Config != nil {
		_ = json.Unmarshal(d.Config, &cfg)
	}
	if cfg.OrderBookID != "" {
		info, err := h.resolveOrderbookAsset(ctx, cfg.OrderBookID)
		if err != nil {
			slog.Warn("resolve orderbook asset", "err", err, "order_book_id", cfg.OrderBookID)
		} else {
			s.AssetName = info.Name
			s.AssetSymbol = info.Symbol
		}
	}
	return s
}

type orderBookConfig struct {
	OrderBookID string `json:"order_book_id"`
}

func (h *Handler) toSummary(ctx context.Context, detail *BacktestDetail) BacktestSummary {
	s := detail.BacktestSummary
	s.AssetName = ""
	s.AssetSymbol = ""

	var cfg orderBookConfig
	if detail.Config != nil {
		_ = json.Unmarshal(detail.Config, &cfg)
	}
	if cfg.OrderBookID != "" {
		info, err := h.resolveOrderbookAsset(ctx, cfg.OrderBookID)
		if err != nil {
			slog.Warn("resolve orderbook asset", "err", err, "order_book_id", cfg.OrderBookID)
		} else {
			s.AssetName = info.Name
			s.AssetSymbol = info.Symbol
		}
	}
	return s
}

func (h *Handler) resolveOrderbookAsset(ctx context.Context, orderBookID string) (AssetInfo, error) {
	h.cacheMu.RLock()
	info, ok := h.assetCache[orderBookID]
	h.cacheMu.RUnlock()
	if ok {
		return info, nil
	}

	client := h.doraClient
	if client == nil {
		client = NewDORAClient()
	}

	orderbooks, err := client.ListOrderBooks(ctx)
	if err != nil {
		return AssetInfo{}, fmt.Errorf("list order books: %w", err)
	}

	h.cacheMu.Lock()
	for _, ob := range orderbooks {
		h.orderbookCache[ob.ID] = ob
	}
	h.cacheMu.Unlock()

	h.cacheMu.RLock()
	ob, ok := h.orderbookCache[orderBookID]
	h.cacheMu.RUnlock()
	if !ok {
		return AssetInfo{}, fmt.Errorf("order book %q not found", orderBookID)
	}

	asset, err := client.GetAssetByID(ctx, ob.BaseAssetID)
	if err != nil {
		return AssetInfo{}, fmt.Errorf("get asset by id: %w", err)
	}

	h.cacheMu.Lock()
	h.assetCache[orderBookID] = *asset
	h.cacheMu.Unlock()

	return *asset, nil
}

type BacktestSummary struct {
	ID           uuid.UUID       `json:"id"`
	DORAUserID   string          `json:"dora_user_id"`
	StrategyType string          `json:"strategy_type"`
	Status       string          `json:"status"`
	Config       json.RawMessage `json:"config"`
	AssetName    string          `json:"asset_name"`
	AssetSymbol  string          `json:"asset_symbol"`
	CreatedAt    time.Time       `json:"created_at"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
	Error        string          `json:"error,omitempty"`
}

type BacktestDetail struct {
	BacktestSummary
	Start  time.Time       `json:"start"`
	End    time.Time       `json:"end"`
	Result json.RawMessage `json:"result,omitempty"`
}

type RunSummary struct {
	ID           uuid.UUID  `json:"id"`
	DORAUserID   string     `json:"dora_user_id"`
	StrategyType string     `json:"strategy_type"`
	Status       string     `json:"status"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	StoppedAt    *time.Time `json:"stopped_at,omitempty"`
}

func (d *BacktestDetail) GetDORAUserID() string   { return d.DORAUserID }
func (d *BacktestDetail) GetCreatedAt() time.Time { return d.CreatedAt }

func (d *RunDetail) GetDORAUserID() string   { return d.DORAUserID }
func (d *RunDetail) GetCreatedAt() time.Time { return d.CreatedAt }

// orderBookIDCfg is a minimal struct for extracting order_book_id from strategy config JSON.
type orderBookIDCfg struct {
	OrderBookID string `json:"order_book_id"`
}

// extractOrderBookID extracts the order_book_id value from a strategy config JSON.
func extractOrderBookID(config json.RawMessage) string {
	var c orderBookIDCfg
	if err := json.Unmarshal(config, &c); err != nil {
		return ""
	}
	return c.OrderBookID
}

// findActiveRunForOrderBook checks whether the user already has a running or paused
// strategy for the given order book. Returns the run ID if found, uuid.Nil otherwise.
func (h *Handler) findActiveRunForOrderBook(doraUserID, orderBookID string) uuid.UUID {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, detail := range h.runs {
		if detail.DORAUserID != doraUserID {
			continue
		}
		if detail.Status != "running" && detail.Status != "paused" {
			continue
		}
		if extractOrderBookID(detail.Config) == orderBookID {
			return detail.ID
		}
	}
	return uuid.Nil
}

// filterAndSort returns items from src filtered by doraUserID, sorted by CreatedAt descending.
func filterAndSort[T listable](src map[uuid.UUID]T, doraUserID string) []T {
	result := make([]T, 0, len(src))
	for _, item := range src {
		if item.GetDORAUserID() == doraUserID {
			result = append(result, item)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].GetCreatedAt().After(result[j].GetCreatedAt())
	})
	return result
}

// listItems is a generic helper that filters, sorts, and writes a list of items.
func listItems[T listable, S any](
	w http.ResponseWriter, r *http.Request,
	src map[uuid.UUID]T,
	extract func(T) S,
	resolveDORAUserID func(context.Context) (string, error),
	mu *sync.RWMutex,
) {
	doraUserID, err := resolveDORAUserID(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}
	mu.RLock()
	filtered := filterAndSort(src, doraUserID)
	mu.RUnlock()
	items := make([]S, len(filtered))
	for i, item := range filtered {
		items[i] = extract(item)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

type RunDetail struct {
	RunSummary
	Config          json.RawMessage `json:"config"`
	EncryptedAPIKey []byte          `json:"-"` // stored in DB, never serialized to JSON
	Error           string          `json:"error,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type MeanReversionBacktestResult struct {
	ClosedTrades []MeanReversionClosedTrade `json:"closed_trades"`
	TradeRecords []MeanReversionTradeRecord `json:"trade_records"`
	TotalPnL     string                     `json:"total_pnl"` //nolint:tagliatelle
	WinCount     int                        `json:"win_count"`
	LossCount    int                        `json:"loss_count"`
	MaxDrawdown  string                     `json:"max_drawdown"`
	SharpeRatio  string                     `json:"sharpe_ratio"`
}

type MeanReversionClosedTrade struct {
	BondID       string    `json:"bond_id"`
	OpenTime     time.Time `json:"open_time"`
	CloseTime    time.Time `json:"close_time"`
	Signal       string    `json:"signal"`
	ExitSignal   string    `json:"exit_signal"`
	EntrySpread  string    `json:"entry_spread"`
	ExitSpread   string    `json:"exit_spread"`
	EntryZScore  string    `json:"entry_zscore"` //nolint:tagliatelle
	ExitZScore   string    `json:"exit_zscore"`  //nolint:tagliatelle
	PositionSize string    `json:"position_size"`
	PnL          string    `json:"pnl"` //nolint:tagliatelle
	ExitReason   string    `json:"exit_reason"`
	EntryPrice   string    `json:"entry_price"`
	ExitPrice    string    `json:"exit_price"`
	Quantity     string    `json:"quantity"`
	EntryBalance string    `json:"entry_balance"`
}

type MeanReversionTradeRecord struct {
	Time         time.Time `json:"time"`
	BondID       string    `json:"bond_id"`
	Signal       string    `json:"signal"`
	Spread       string    `json:"spread"`
	PositionSize string    `json:"position_size"`
	ZScore       string    `json:"zscore"` //nolint:tagliatelle
	Price        string    `json:"price"`
	Quantity     string    `json:"quantity"`
	EntryBalance string    `json:"entry_balance"`
}

func NewHandler(service strategycore.Service, opts ...func(*Handler)) http.Handler {
	h := &Handler{
		service:                  service,
		now:                      time.Now,
		backtests:                make(map[uuid.UUID]*BacktestDetail),
		runs:                     make(map[uuid.UUID]*RunDetail),
		runningStrategies:        make(map[uuid.UUID]strategycore.Strategy),
		stopLossObservers:        make(map[uuid.UUID]context.CancelFunc),
		runCompletionWatchers:    make(map[uuid.UUID]context.CancelFunc),
		stopLossObserverInterval: defaultStopLossObserverInterval,
		orderbookCache:           make(map[string]DORAOrderBookSummary),
		assetCache:               make(map[string]AssetInfo),
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.log == nil {
		h.log = slog.Default()
	}
	if h.strategies == nil {
		h.strategies = defaultStrategies(h.prices, h.tradesHistoryStore, h.tradeStream, h.historicalPriceStore, h.tradeHistoryStore, h.log)
	}

	h.mux = http.NewServeMux()
	h.mux.HandleFunc("/healthz", h.handleHealth)
	h.mux.HandleFunc("/v1/dora/orderbooks", h.handleDORAOrderBooks)
	h.mux.HandleFunc("/v1/dora/user", h.handleDORAUser)
	h.mux.HandleFunc("/v1/copy-traders", h.handleCopyTraders)
	h.mux.HandleFunc("/v1/tenors", h.handleTenors)
	h.mux.HandleFunc("/v1/strategies", h.handleStrategies)
	h.mux.HandleFunc("/v1/backtests", h.handleBacktests)
	h.mux.HandleFunc("/v1/backtests/", h.handleBacktestByID)
	h.mux.HandleFunc("/v1/trading-decisions/", h.handleTradingDecisions)
	h.mux.HandleFunc("/v1/runs", h.handleRuns)
	h.mux.HandleFunc("/v1/runs/", h.handleRunByID)
	h.mux.HandleFunc("/v1/openapi", h.handleOpenAPI)
	h.authedMux = requireAuth(h.resolveDORAUserID, h.mux)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// /healthz and /v1/openapi are exempt from authentication so that
	// health probes and spec discovery work without credentials.
	if r.URL.Path == "/healthz" || r.URL.Path == "/v1/openapi" {
		h.mux.ServeHTTP(w, r)
		return
	}
	h.authedMux.ServeHTTP(w, r)
}

func WithNow(now func() time.Time) func(*Handler) {
	return func(h *Handler) {
		h.now = now
	}
}

func WithStrategies(defs ...StrategyDefinition) func(*Handler) {
	return func(h *Handler) {
		h.strategies = make(map[string]StrategyDefinition, len(defs))
		for _, def := range defs {
			h.strategies[def.Type] = def
		}
	}
}

func WithRunStore(store RunStore) func(*Handler) {
	return func(h *Handler) {
		h.runStore = store
	}
}

// WithBacktestStore sets the store used by the backtest trade writer.
func WithBacktestStore(store BacktestStore) func(*Handler) {
	return func(h *Handler) {
		h.backtestStore = store
	}
}

// WithDecisionStore sets the recorder used by the live strategy loops
// (meanreversion and copytrading) to record the decision that triggered
// every market order.  Passing nil disables recording.  Backtests do
// not pass a recorder and therefore never write to strategy_decisions.
func WithDecisionStore(store strategycore.DecisionRecorder) func(*Handler) {
	return func(h *Handler) {
		h.decisionStore = store
	}
}

// WithDecisionReader sets the reader used by the
// /v1/trading-decisions/{run_id} endpoint. Passing nil leaves the
// endpoint registered but makes it return 503 — useful when the read
// path is deployed without a wired reader. The reader is distinct
// from the write-side recorder (see WithDecisionStore).
func WithDecisionReader(reader DecisionReader) func(*Handler) {
	return func(h *Handler) {
		h.decisionReader = reader
	}
}

// WithStateStore sets the per-run state checkpoint store used by
// execution strategies (TWAP) to persist progress for crash recovery.
// The same *PGRunStore satisfies both RunStore and strategy.StateStore.
func WithStateStore(store strategycore.StateStore) func(*Handler) {
	return func(h *Handler) {
		h.stateStore = store
	}
}

// WithTradesHistoryStore sets the store used by the copy-trading
// backtest to read the followed trader's trade history from the
// trades_history Postgres table.
func WithTradesHistoryStore(store *copytrading.PGTradesHistoryStore) func(*Handler) {
	return func(h *Handler) {
		h.tradesHistoryStore = store
	}
}

// WithTradeStream sets the live trade stream used by copy-trading runs.
func WithTradeStream(ts *streams.TradeStream) func(*Handler) {
	return func(h *Handler) {
		h.tradeStream = ts
	}
}

// WithHistoricalPriceStore wires a breakout backtest data source so
// Strategy.Backtest can read candles_history. May be nil; the
// breakout strategy returns an error from Backtest when no store is
// configured.
func WithHistoricalPriceStore(s breakout.HistoricalPriceStore) func(*Handler) {
	return func(h *Handler) {
		h.historicalPriceStore = s
	}
}

// WithTradeHistoryStore wires a breakout backtest trade source so the
// OBV (On-Balance Volume) filter can be evaluated in backtests when
// OBVWindow > 0. Reads from trades_history. May be nil; the backtester
// simply skips OBV accumulation when no store is configured.
func WithTradeHistoryStore(s breakout.TradeHistoryStore) func(*Handler) {
	return func(h *Handler) {
		h.tradeHistoryStore = s
	}
}

func WithPricesHandler(pricesHandler *prices.Handler) func(*Handler) {
	return func(h *Handler) {
		h.prices = pricesHandler
	}
}

func WithLogger(log *slog.Logger) func(*Handler) {
	return func(h *Handler) {
		h.log = log
	}
}

// WithNotifier sets the notifications.Notifier used to publish lifecycle
// events for backtests and runs. When unset, publishEvent is a no-op.
func WithNotifier(n notifications.Notifier) func(*Handler) {
	return func(h *Handler) {
		h.notifier = n
	}
}

// WithOrderUpdatesManager wires the per-DORA-user order-updates
// subscription manager. When unset, the Handler runs as before
// without subscribing to DORA's order-updates stream.
func WithOrderUpdatesManager(m orderUpdatesManager) func(*Handler) {
	return func(h *Handler) { h.orderUpdates = m }
}

// WithStopLossObserverInterval overrides the stop-loss observer poll
// interval. The default is 1s; tests use a shorter interval to avoid
// waiting.
func WithStopLossObserverInterval(d time.Duration) func(*Handler) {
	return func(h *Handler) {
		h.stopLossObserverInterval = d
	}
}

// orderUpdatesManager is the contract the Handler consumes from the
// notifications/orderupdates package. The concrete type lives outside
// strategy/http; this interface is defined locally to keep the import
// boundary one-way (strategy/http does not import notifications/orderupdates).
type orderUpdatesManager interface {
	EnsureSubscribed(ctx context.Context, doraUserID, apiKey string,
		runID uuid.UUID, status string) error
	UpdateRunStatus(runID uuid.UUID, status string)
	Unsubscribe(doraUserID string)
}

func WithDORAClient(client doraClient) func(*Handler) {
	return func(h *Handler) {
		h.doraClient = client
	}
}

// WithEncryptionKey sets the 32-byte AES-256 key used to encrypt API keys at rest.
// Without this, runs cannot be resumed after a server restart because the
// user's DORA API key is unavailable.
func WithEncryptionKey(key []byte) func(*Handler) {
	return func(h *Handler) {
		h.encryptionKey = key
	}
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (h *Handler) handleStrategies(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}

	items := make([]StrategySummary, 0, len(h.strategies))
	for _, def := range h.strategies {
		items = append(items, StrategySummary{
			Type:             def.Type,
			Status:           def.Status,
			Description:      def.Description,
			ConfigFields:     append([]StrategyConfigField(nil), def.ConfigFields...),
			SupportsRun:      def.SupportsRun,
			SupportsBacktest: def.SupportsBacktest,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Type < items[j].Type
	})
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) handleTenors(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}

	supported := fred.SupportedBenchmarkTenors()
	items := make([]TenorSummary, 0, len(supported))
	for _, tenor := range supported {
		items = append(items, TenorSummary{
			Code:        tenor.Code,
			Description: tenor.Description,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) handleDORAOrderBooks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}

	client := h.doraClient
	if client == nil {
		client = NewDORAClient()
	}
	items, err := client.ListOrderBooks(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("list DORA order books: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) handleDORAUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}

	// The user ID was verified and cached in context by the auth middleware.
	userID, ok := doraUserIDFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusInternalServerError, "user ID not in context")
		return
	}
	writeJSON(w, http.StatusOK, DORAUserSummary{ID: userID})
}

// handleCopyTraders returns the list of traders available to be followed by
// copy-trading runs. The list is sourced from DORA's dedicated
// `GET /v1/user/copy-traders` endpoint, which is server-side filtered to users
// with copy trading enabled. Each entry exposes the user UUID (for use as
// `followed_trader` in CopyTradingConfig) and the DORA-registered user_name.
func (h *Handler) handleCopyTraders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}

	client := h.doraClient
	if client == nil {
		client = NewDORAClient()
	}
	traders, err := client.ListCopyTraders(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("list copy traders: %v", err))
		return
	}

	// `make(..., 0, len(traders))` is required, not `var items []CopyTraderSummary`:
	// when DORA returns no copy traders, the live client returns `nil`, and
	// writeJSON serialises a nil slice as `"items": null`. The spec guarantees
	// `"items": []` for empty results, so the handler must produce a non-nil
	// empty slice explicitly.
	items := make([]CopyTraderSummary, 0, len(traders))
	for _, t := range traders {
		items = append(items, CopyTraderSummary{ID: t.UserID, UserName: t.UserName})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) handleBacktests(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createBacktest(w, r)
	case http.MethodGet:
		h.listBacktests(w, r)
	default:
		writeMethodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

func (h *Handler) handleBacktestByID(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/backtests/")
	if rest == r.URL.Path || rest == "" {
		writeError(w, http.StatusNotFound, "resource not found")
		return
	}
	parts := strings.Split(rest, "/")
	id, err := uuid.Parse(parts[0])
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			h.getBacktest(w, r, id)
		case http.MethodDelete:
			h.cancelBacktest(w, r, id)
		default:
			writeMethodNotAllowed(w, http.MethodGet, http.MethodDelete)
		}
		return
	}

	if len(parts) != 2 { //nolint:mnd
		writeError(w, http.StatusNotFound, "resource not found")
		return
	}

	switch parts[1] {
	case "trades":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		h.getBacktestTrades(w, r, id)
	case "closed-trades":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		h.getBacktestClosedTrades(w, r, id)
	case "metadata":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		h.getBacktestMetadata(w, r, id)
	default:
		writeError(w, http.StatusNotFound, "resource not found")
	}
}

func (h *Handler) handleRuns(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createRun(w, r)
	case http.MethodGet:
		h.listRuns(w, r)
	default:
		writeMethodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

func (h *Handler) handleRunByID(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/runs/")
	if rest == r.URL.Path || rest == "" {
		writeError(w, http.StatusNotFound, "resource not found")
		return
	}
	parts := strings.Split(rest, "/")
	id, err := uuid.Parse(parts[0])
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			h.getRun(w, r.Context(), id)
		case http.MethodDelete:
			h.stopRun(w, r.Context(), id)
		default:
			writeMethodNotAllowed(w, http.MethodGet, http.MethodDelete)
		}
		return
	}

	if len(parts) != 2 { //nolint:mnd
		writeError(w, http.StatusNotFound, "resource not found")
		return
	}
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}

	switch parts[1] {
	case "pause":
		h.pauseRun(w, r.Context(), id)
	case "resume":
		h.resumeRun(w, r.Context(), id)
	default:
		writeError(w, http.StatusNotFound, "resource not found")
	}
}

func (h *Handler) createBacktest(w http.ResponseWriter, r *http.Request) {
	var req CreateBacktestRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	id := uuid.Must(uuid.NewV7())
	var tradeWriter stats.BacktestTradeWriter
	var batcher *BatchingBacktestWriter
	if h.backtestStore != nil {
		batcher = NewBatchingBacktestWriter(h.backtestStore, batchSize, flushAfter)
		tradeWriter = newScopedBacktestWriter(id, batcher)
	}
	def, cfg, strat, statusCode, err := h.resolveStrategy(req.StrategyType, req.Config, capabilityBacktest, tradeWriter)
	if err != nil {
		writeError(w, statusCode, err.Error())
		return
	}
	if req.Start.IsZero() || req.End.IsZero() {
		writeError(w, http.StatusBadRequest, "start and end are required")
		return
	}

	doraUserID, err := h.resolveDORAUserID(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}

	// Inject the user's API key into the strategy so it can authenticate
	// with DORA when resolving the asset ID for the order book.
	info, _ := authctx.AuthInfoFromContext(r.Context())
	var apiKey string
	if info != nil {
		apiKey = info.APIKey
	}
	h.applyUserAPIKey(strat, apiKey)

	resultCh, err := h.service.RunBacktest(r.Context(), id, strat, req.Start, req.End)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("run backtest: %v", err))
		return
	}

	now := h.now().UTC()
	detail := &BacktestDetail{
		BacktestSummary: BacktestSummary{
			ID:           id,
			DORAUserID:   doraUserID,
			StrategyType: def.Type,
			Status:       "running",
			Config:       cfg,
			CreatedAt:    now,
		},
		Start: req.Start.UTC(),
		End:   req.End.UTC(),
	}

	h.mu.Lock()
	h.backtests[id] = detail
	h.mu.Unlock()

	if err := h.saveBacktest(r.Context(), detail); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("save backtest: %v", err))
		return
	}

	if doraUserID, ok := doraUserIDFromContext(r.Context()); ok {
		h.publishEvent(r.Context(), notifications.Event{
			Type:       notifications.EventBacktestStarted,
			UserID:     doraUserID,
			BacktestID: detail.ID.String(),
			Timestamp:  time.Now().UTC(),
			Payload:    map[string]any{"strategy_type": detail.StrategyType},
		})
	}

	go h.awaitBacktestResult(id, resultCh, batcher) //nolint:contextcheck,gosec // backtest outlives the HTTP request context
	writeJSON(w, http.StatusAccepted, detail)
}

//nolint:funlen // 5 strategy result cases; consider a map-based dispatcher in a follow-up
func (h *Handler) awaitBacktestResult(id uuid.UUID, resultCh <-chan types.BacktestResult, batcher *BatchingBacktestWriter) {
	result, ok := <-resultCh
	if !ok {
		result = types.ErrorResult{Err: errors.New("backtest result channel closed")}
	}
	now := h.now().UTC()

	h.mu.Lock()
	detail, exists := h.backtests[id]
	if !exists || detail.Status == "cancelled" {
		h.mu.Unlock()
		return
	}
	completedAt := now
	detail.CompletedAt = &completedAt
	var evtType notifications.EventType
	var evtErr string
	switch r := result.(type) {
	case types.ErrorResult:
		detail.Status = "failed"
		detail.Error = r.Err.Error()
		evtType = notifications.EventBacktestFailed
		evtErr = r.Err.Error()
	case meanreversion.BacktestResult:
		detail.Status = "completed"
		raw, err := newBacktestResult(r)
		if err != nil {
			detail.Status = "failed"
			detail.Error = err.Error()
			evtType = notifications.EventBacktestFailed
			evtErr = err.Error()
			break
		}
		detail.Result = raw
		evtType = notifications.EventBacktestCompleted
	case copytrading.BacktestResult:
		detail.Status = "completed"
		raw, err := newCopyTradingBacktestResult(r)
		if err != nil {
			detail.Status = "failed"
			detail.Error = err.Error()
			evtType = notifications.EventBacktestFailed
			evtErr = err.Error()
			break
		}
		detail.Result = raw
		evtType = notifications.EventBacktestCompleted
	case breakout.BacktestResult:
		detail.Status = "completed"
		raw, err := newBreakoutBacktestResult(r)
		if err != nil {
			detail.Status = "failed"
			detail.Error = err.Error()
			evtType = notifications.EventBacktestFailed
			evtErr = err.Error()
			break
		}
		detail.Result = raw
		evtType = notifications.EventBacktestCompleted
	case momentum.BacktestResult:
		detail.Status = "completed"
		raw, err := newMomentumBacktestResult(r)
		if err != nil {
			detail.Status = "failed"
			detail.Error = err.Error()
			evtType = notifications.EventBacktestFailed
			evtErr = err.Error()
			break
		}
		detail.Result = raw
		evtType = notifications.EventBacktestCompleted
	default:
		detail.Status = "failed"
		detail.Error = fmt.Sprintf("unknown backtest result type %T", result)
		evtType = notifications.EventBacktestFailed
		evtErr = detail.Error
	}
	h.mu.Unlock()

	// TODO: replace context.Background() with a context scoped to the
	// backtest's lifetime. The backtest is launched via
	// service.RunBacktest, which uses s.baseCtx (the signal-cancelled
	// server context set in cmd/strategy-server/main.go). Capturing
	// that ctx at backtest-creation time and threading it through to
	// this goroutine is the right fix; r.Context() is already
	// cancelled by the time the backtest finishes.
	if err := h.saveBacktest(context.Background(), detail); err != nil {
		slog.Error("failed to save backtest result", "err", err, "backtest_id", id)
	}

	if evtType != "" {
		evt := notifications.Event{
			Type:       evtType,
			UserID:     detail.DORAUserID,
			BacktestID: detail.ID.String(),
			Timestamp:  time.Now().UTC(),
		}
		if evtType == notifications.EventBacktestCompleted {
			evt.Payload = map[string]any{"strategy_type": detail.StrategyType}
		} else {
			evt.Payload = map[string]any{"error": evtErr}
		}
		h.publishEvent(h.service.BaseContext(), evt)
	}

	// Stop the batching writer's background ticker. The strategy engine
	// already called writer.Flush() at the end of its simulation, so
	// any remaining rows are persisted; Close drains anything that
	// arrived in the final tick window and stops the goroutine.
	if batcher != nil {
		if err := batcher.Close(); err != nil {
			slog.Error("close batching writer", "err", err, "backtest_id", id)
		}
	}
}

func (h *Handler) listBacktests(w http.ResponseWriter, r *http.Request) {
	doraUserID, err := h.resolveDORAUserID(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}

	statusFilter := parseStatusFilter(r)
	from, to := parseDateFilter(r)
	page, limit := parsePagination(r)

	h.mu.RLock()
	details := filterAndSort(h.backtests, doraUserID)
	items := make([]BacktestSummary, 0, len(details))
	for _, d := range details {
		items = append(items, h.toSummary(r.Context(), d))
	}
	h.mu.RUnlock()

	if len(statusFilter) > 0 {
		statusSet := make(map[string]bool, len(statusFilter))
		for _, s := range statusFilter {
			statusSet[s] = true
		}
		tmp := make([]BacktestSummary, 0, len(items))
		for _, item := range items {
			if statusSet[item.Status] {
				tmp = append(tmp, item)
			}
		}
		items = tmp
	}

	if !from.IsZero() || !to.IsZero() {
		tmp := make([]BacktestSummary, 0, len(items))
		for _, item := range items {
			if !from.IsZero() && item.CreatedAt.Before(from) {
				continue
			}
			if !to.IsZero() && !item.CreatedAt.Before(to) {
				continue
			}
			tmp = append(tmp, item)
		}
		items = tmp
	}

	total := len(items)
	start := min((page-1)*limit, total)
	end := min(start+limit, total)

	writeJSON(w, http.StatusOK, map[string]any{
		"items": items[start:end],
		"page":  page,
		"limit": limit,
	})
}

func parseStatusFilter(r *http.Request) []string {
	raw := r.URL.Query().Get("status")
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}

func parseDateFilter(r *http.Request) (from, to time.Time) {
	if f := r.URL.Query().Get("from"); f != "" {
		if t, err := time.Parse(time.RFC3339, f); err == nil {
			from = t
		} else if t, err := time.Parse("2006-01-02", f); err == nil {
			from = t
		}
	}
	if t := r.URL.Query().Get("to"); t != "" {
		if parsed, err := time.Parse(time.RFC3339, t); err == nil {
			to = parsed
		} else if parsed, err := time.Parse("2006-01-02", t); err == nil {
			to = parsed
		}
	}
	return from, to
}

// ParseDecisionsDateFilter parses the from/to query parameters for the
// trading-decisions endpoint. Unlike parseDateFilter, malformed input
// is rejected with a parse error rather than silently dropped, because
// a typo'd date on a paginated endpoint would silently widen the
// result set across many pages.
func ParseDecisionsDateFilter(r *http.Request) (from, to *time.Time, err error) {
	parse := func(raw string) (*time.Time, error) {
		if raw == "" {
			return nil, nil
		}
		if t, perr := time.Parse(time.RFC3339, raw); perr == nil {
			t = t.UTC() // make sure time is in UTC to match the database
			return &t, nil
		}
		if t, perr := time.Parse("2006-01-02", raw); perr == nil {
			t = t.UTC() // make sure time is in UTC to match the database
			return &t, nil
		}
		return nil, fmt.Errorf("invalid date %q (want RFC3339 or YYYY-MM-DD)", raw)
	}
	from, err = parse(r.URL.Query().Get("from"))
	if err != nil {
		return nil, nil, fmt.Errorf("from: %w", err)
	}
	to, err = parse(r.URL.Query().Get("to"))
	if err != nil {
		return nil, nil, fmt.Errorf("to: %w", err)
	}
	return from, to, nil
}

// ParseDecisionCursor parses the cursor query parameter. Returns nil
// with no error when the parameter is absent. The cursor must be
// opaque to clients; this function decodes the wire format produced
// by Cursor.Encode.
func ParseDecisionCursor(r *http.Request) (*Cursor, error) {
	raw := r.URL.Query().Get("cursor")
	if raw == "" {
		return nil, nil
	}
	return DecodeCursor(raw)
}

// ParseDecisionLimit parses the limit query parameter for the
// trading-decisions endpoint. Default is 50, silently clamped to
// [1, 200]. Garbage / non-positive input keeps the default. The
// behaviour matches parsePagination in this package.
func ParseDecisionLimit(r *http.Request) int {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return defaultTradingDecisionsLimit
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return defaultTradingDecisionsLimit
	}
	if n > maxTradingDecisionsLimit {
		return maxTradingDecisionsLimit
	}
	return n
}

// handleTradingDecisions is the top-level dispatcher for the
// /v1/trading-decisions/{run_id} endpoint. It pulls run_id from the
// path and delegates to getRunDecisions. Non-GET methods are rejected
// with 405.
func (h *Handler) handleTradingDecisions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET required")
		return
	}
	raw := strings.TrimPrefix(r.URL.Path, "/v1/trading-decisions/")
	raw = strings.TrimSuffix(raw, "/")
	if raw == "" {
		writeError(w, http.StatusBadRequest, "run_id is required")
		return
	}
	runID, err := uuid.Parse(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid run_id")
		return
	}
	h.getRunDecisions(w, r, runID)
}

// getRunDecisions serves GET /v1/trading-decisions/{run_id}. The flow
// is: resolve the caller, verify the run exists and belongs to the
// caller, parse the date / cursor / limit parameters, fetch one page
// of decisions, write the JSON response. A nil decisionReader
// short-circuits to 503 — the route is registered unconditionally but
// the reader is optional.
func (h *Handler) getRunDecisions(w http.ResponseWriter, r *http.Request, runID uuid.UUID) {
	ctx := r.Context()

	doraUserID, err := h.resolveDORAUserID(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}

	if h.decisionReader == nil {
		writeError(w, http.StatusServiceUnavailable, "trading decisions endpoint is not configured")
		return
	}

	exists, err := h.runStore.CheckRunExists(ctx, runID, doraUserID)
	if err != nil {
		slog.Error("check run exists", "err", err, "run_id", runID)
		writeError(w, http.StatusInternalServerError, "check run")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}

	from, to, err := ParseDecisionsDateFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cursor, err := ParseDecisionCursor(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	limit := ParseDecisionLimit(r)

	params := ListDecisionsParams{
		RunID: runID,
		From:  from,
		To:    to,
		Limit: limit,
	}
	if cursor != nil {
		t := cursor.Time
		s := cursor.Seq
		params.AfterTime = &t
		params.AfterSeq = &s
	}

	items, next, err := h.decisionReader.ListDecisions(ctx, params)
	if err != nil {
		slog.Error("list decisions", "err", err, "run_id", runID)
		writeError(w, http.StatusInternalServerError, "list decisions")
		return
	}

	resp := struct {
		Items      []strategycore.Decision `json:"items"`
		NextCursor string                  `json:"next_cursor,omitempty"`
	}{Items: items}
	if next != nil {
		resp.NextCursor = next.Encode()
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) getBacktestTrades(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	getBacktestSubResource(h, w, r, id, "trades", h.backtestStore.GetBacktestTrades)
}

func (h *Handler) getBacktestClosedTrades(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	getBacktestSubResource(h, w, r, id, "closed trades", h.backtestStore.GetBacktestClosedTrades)
}

func getBacktestSubResource(
	h *Handler, w http.ResponseWriter, r *http.Request, id uuid.UUID, label string,
	fetch func(context.Context, uuid.UUID, string, int, int) (json.RawMessage, error),
) {
	doraUserID, err := h.resolveDORAUserID(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}

	h.mu.RLock()
	detail, ok := h.backtests[id]
	h.mu.RUnlock()
	if !ok || detail.DORAUserID != doraUserID {
		writeError(w, http.StatusNotFound, "backtest not found")
		return
	}

	page, limit := parsePagination(r)
	raw, err := fetch(r.Context(), id, detail.StrategyType, page, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("get backtest %s: %v", label, err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func parsePagination(r *http.Request) (page, limit int) {
	page = 1
	limit = defaultPaginationLimit

	if p := r.URL.Query().Get("page"); p != "" {
		if val, err := strconv.Atoi(p); err == nil && val > 0 {
			page = val
		}
	}
	if l := r.URL.Query().Get("limit"); l != "" {
		if val, err := strconv.Atoi(l); err == nil && val > 0 {
			limit = min(val, maxPaginationLimit)
		}
	}
	return page, limit
}

func (h *Handler) getBacktest(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	doraUserID, err := h.resolveDORAUserID(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}

	h.mu.RLock()
	detail, ok := h.backtests[id]
	if !ok || detail.DORAUserID != doraUserID {
		h.mu.RUnlock()
		if !ok {
			writeError(w, http.StatusNotFound, "backtest not found")
		} else {
			writeError(w, http.StatusForbidden, "access denied")
		}
		return
	}
	// Copy the detail while holding the lock so subsequent reads
	// are safe from concurrent awaitBacktestResult writes.
	detailCopy := *detail
	detailCopy.Result = nil
	if len(detail.Result) > 0 {
		detailCopy.Result = append(json.RawMessage(nil), detail.Result...)
	}
	h.mu.RUnlock()

	if detailCopy.Status == "completed" && len(detailCopy.Result) == 0 && h.backtestStore != nil {
		result, err := h.backtestStore.LoadBacktestResult(r.Context(), id)
		if err != nil {
			slog.Error("failed to load backtest result", "err", err, "backtest_id", id)
		} else {
			detailCopy.Result = result
			h.mu.Lock()
			detail.Result = result
			h.mu.Unlock()
		}
	}

	writeJSON(w, http.StatusOK, h.summaryResult(r.Context(), &detailCopy))
}

func (h *Handler) getBacktestMetadata(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	doraUserID, err := h.resolveDORAUserID(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}

	h.mu.RLock()
	detail, ok := h.backtests[id]
	if !ok || detail.DORAUserID != doraUserID {
		h.mu.RUnlock()
		if !ok {
			writeError(w, http.StatusNotFound, "backtest not found")
		} else {
			writeError(w, http.StatusForbidden, "access denied")
		}
		return
	}
	summary := h.toSummary(r.Context(), detail)
	h.mu.RUnlock()

	writeJSON(w, http.StatusOK, summary)
}

func (h *Handler) cancelBacktest(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	doraUserID, err := h.resolveDORAUserID(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}

	h.mu.RLock()
	detail, ok := h.backtests[id]
	h.mu.RUnlock()
	if !ok || detail.DORAUserID != doraUserID {
		writeError(w, http.StatusNotFound, "backtest not found")
		return
	}

	if err := h.service.StopBacktest(id); err != nil {
		if errors.Is(err, strategycore.ErrBacktestNotFound) {
			writeError(w, http.StatusNotFound, "backtest not found")
			return
		}
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("stop backtest: %v", err))
		return
	}

	now := h.now().UTC()
	h.mu.Lock()
	if detail, ok := h.backtests[id]; ok {
		detail.Status = "cancelled"
		completedAt := now
		detail.CompletedAt = &completedAt
		detail.Error = "backtest cancelled"
	}
	h.mu.Unlock()

	if detail, ok := h.backtests[id]; ok {
		if err := h.saveBacktest(r.Context(), detail); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("save backtest: %v", err))
			return
		}
		h.publishEvent(r.Context(), notifications.Event{
			Type:       notifications.EventBacktestFailed,
			UserID:     doraUserID,
			BacktestID: detail.ID.String(),
			Timestamp:  time.Now().UTC(),
			Payload:    map[string]any{"error": "cancelled"},
		})
	}

	h.getBacktestMetadata(w, r, id)
}

func (h *Handler) createRun(w http.ResponseWriter, r *http.Request) {
	var req CreateRunRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	def, cfg, strat, statusCode, err := h.resolveStrategy(req.StrategyType, req.Config, capabilityRun, nil)
	if err != nil {
		writeError(w, statusCode, err.Error())
		return
	}

	doraUserID, err := h.resolveDORAUserID(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}

	// A user may only have one running or paused strategy per order book.
	if orderBookID := extractOrderBookID(cfg); orderBookID != "" {
		if existingID := h.findActiveRunForOrderBook(doraUserID, orderBookID); existingID != uuid.Nil {
			writeError(w, http.StatusConflict,
				fmt.Sprintf("a %s strategy is already active for this order book (run %s)", def.Type, existingID))
			return
		}
	}

	// Inject the user's API key into the strategy so it can authenticate with DORA.
	info, _ := authctx.AuthInfoFromContext(r.Context())
	var apiKey string
	if info != nil {
		apiKey = info.APIKey
	}
	h.applyUserAPIKey(strat, apiKey)

	// Attach the per-run decision recorder so every successful live
	// market order is written to strategy_decisions. nil disables
	// recording; the helper is a no-op for unknown strategy types.
	h.attachDecisionStore(strat)
	h.attachStateStore(strat)

	var encryptedAPIKey []byte
	if info != nil && info.APIKey != "" && len(h.encryptionKey) > 0 {
		encryptedAPIKey, err = encryptAPIKey([]byte(info.APIKey), h.encryptionKey)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("encrypt api key: %v", err))
			return
		}
	}

	now := h.now().UTC()
	id := uuid.Must(uuid.NewV7())
	detail := &RunDetail{
		RunSummary: RunSummary{
			ID:           id,
			DORAUserID:   doraUserID,
			StrategyType: def.Type,
			Status:       "running",
			CreatedAt:    now,
			UpdatedAt:    now,
		},
		Config:          cfg,
		EncryptedAPIKey: encryptedAPIKey,
	}
	id, err = h.startRun(r.Context(), detail, strat)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("run strategy: %v", err))
		return
	}
	detail.ID = id
	if err := h.saveRun(r.Context(), detail); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("save run: %v", err))
		return
	}

	h.mu.Lock()
	h.runs[id] = detail
	h.mu.Unlock()
	if h.orderUpdates != nil && doraUserID != "" && info != nil && info.APIKey != "" {
		if err := h.orderUpdates.EnsureSubscribed(r.Context(), doraUserID, info.APIKey, detail.ID, detail.Status); err != nil {
			slog.Warn("EnsureSubscribed failed", "user_id", doraUserID, "err", err)
		}
	}

	h.startOrderUpdater(detail, strat)

	h.startStopLossObserver(detail, strat)
	h.startCompletionWatcher(detail)

	h.publishEvent(r.Context(), notifications.Event{
		Type:      notifications.EventRunStarted,
		UserID:    doraUserID,
		RunID:     detail.ID.String(),
		Timestamp: time.Now().UTC(),
		Payload:   map[string]any{"strategy_type": detail.StrategyType},
	})

	writeJSON(w, http.StatusCreated, detail)
}

func (h *Handler) listRuns(w http.ResponseWriter, r *http.Request) {
	listItems(w, r, h.runs,
		func(d *RunDetail) RunSummary { return d.RunSummary },
		h.resolveDORAUserID, &h.mu)
}

func (h *Handler) getRun(w http.ResponseWriter, ctx context.Context, id uuid.UUID) {
	doraUserID, err := h.resolveDORAUserID(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}

	h.mu.RLock()
	detail, ok := h.runs[id]
	h.mu.RUnlock()
	if !ok {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if detail.DORAUserID != doraUserID {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (h *Handler) stopRun(w http.ResponseWriter, ctx context.Context, id uuid.UUID) {
	detail, ok := h.runDetail(id)
	if !ok {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	doraUserID, err := h.resolveDORAUserID(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}
	if detail.DORAUserID != doraUserID {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if detail.Status != "stopped" {
		if err := h.service.StopStrategy(ctx, id); err != nil {
			if errors.Is(err, strategycore.ErrRunIDNotFound) {
				writeError(w, http.StatusNotFound, "run not found")
				return
			}
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("stop run: %v", err))
			return
		}
	}

	now := h.now().UTC()
	h.mu.Lock()
	if detail, ok := h.runs[id]; ok {
		detail.Status = "stopped"
		detail.UpdatedAt = now
		stoppedAt := now
		detail.StoppedAt = &stoppedAt
	}
	if cancel, ok := h.stopLossObservers[id]; ok {
		cancel()
	}
	if cancel, ok := h.runCompletionWatchers[id]; ok {
		cancel()
	}
	h.mu.Unlock()
	if h.orderUpdates != nil && detail.DORAUserID != "" {
		h.orderUpdates.UpdateRunStatus(detail.ID, "stopped")
		h.orderUpdates.Unsubscribe(detail.DORAUserID)
	}

	detail, _ = h.runDetail(id)
	if err := h.saveRun(ctx, detail); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("save run: %v", err))
		return
	}
	h.publishEvent(ctx, notifications.Event{
		Type:      notifications.EventRunStopped,
		UserID:    doraUserID,
		RunID:     detail.ID.String(),
		Timestamp: time.Now().UTC(),
	})
	h.getRun(w, ctx, id)
}

func (h *Handler) pauseRun(w http.ResponseWriter, ctx context.Context, id uuid.UUID) {
	detail, ok := h.runDetail(id)
	if !ok {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	doraUserID, err := h.resolveDORAUserID(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}
	if detail.DORAUserID != doraUserID {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if detail.Status == "stopped" {
		writeError(w, http.StatusConflict, "run is stopped")
		return
	}
	if err := h.service.PauseStrategy(ctx, id); err != nil {
		if errors.Is(err, strategycore.ErrRunIDNotFound) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("stop run: %v", err))
		return
	}

	now := h.now().UTC()
	h.mu.Lock()
	if detail, ok := h.runs[id]; ok {
		detail.Status = "paused"
		detail.UpdatedAt = now
	}
	h.mu.Unlock()

	if h.orderUpdates != nil {
		h.orderUpdates.UpdateRunStatus(id, "paused")
	}

	detail, _ = h.runDetail(id)
	if err := h.saveRun(ctx, detail); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("save run: %v", err))
		return
	}
	h.publishEvent(ctx, notifications.Event{
		Type:      notifications.EventRunPaused,
		UserID:    doraUserID,
		RunID:     detail.ID.String(),
		Timestamp: time.Now().UTC(),
	})
	h.getRun(w, ctx, id)
}

func (h *Handler) resumeRun(w http.ResponseWriter, ctx context.Context, id uuid.UUID) {
	detail, ok := h.runDetail(id)
	if !ok {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	doraUserID, err := h.resolveDORAUserID(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
		return
	}
	if detail.DORAUserID != doraUserID {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if detail.Status == "stopped" {
		writeError(w, http.StatusConflict, "run is stopped")
		return
	}
	if detail.Status == "paused" {
		if err := h.service.ResumeStrategy(ctx, id); err != nil {
			if !errors.Is(err, strategycore.ErrRunIDNotFound) {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("resume run: %v", err))
				return
			}
			if err := h.resumePersistedRun(ctx, detail); err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("resume run: %v", err))
				return
			}
		}
	} else if err := h.service.ResumeStrategy(ctx, id); err != nil {
		if errors.Is(err, strategycore.ErrRunIDNotFound) {
			if err := h.resumePersistedRun(ctx, detail); err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("resume run: %v", err))
				return
			}
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("resume run: %v", err))
			return
		}
	}

	now := h.now().UTC()
	h.mu.Lock()
	if detail, ok := h.runs[id]; ok {
		detail.Status = "running"
		detail.UpdatedAt = now
		detail.StoppedAt = nil
	}
	h.mu.Unlock()

	if h.orderUpdates != nil {
		h.orderUpdates.UpdateRunStatus(id, "running")
	}

	detail, _ = h.runDetail(id)
	if err := h.saveRun(ctx, detail); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("save run: %v", err))
		return
	}
	h.publishEvent(ctx, notifications.Event{
		Type:      notifications.EventRunResumed,
		UserID:    doraUserID,
		RunID:     detail.ID.String(),
		Timestamp: time.Now().UTC(),
	})
	h.getRun(w, ctx, id)
}

func (h *Handler) RestoreRuns(ctx context.Context) error {
	if h.runStore == nil {
		return nil
	}
	runs, err := h.runStore.LoadRuns(ctx)
	if err != nil {
		return fmt.Errorf("load runs: %w", err)
	}
	h.mu.Lock()
	for _, detail := range runs {
		h.runs[detail.ID] = detail
	}
	h.mu.Unlock()

	for _, detail := range runs {
		if detail.Status != "running" {
			continue
		}
		// Don't relaunch runs whose execution window has already
		// passed — the strategy would only skip every bucket on
		// startup. Mark them completed and publish EventRunCompleted
		// so the client isn't left polling.
		if h.runWindowExpired(detail) {
			h.expireRun(ctx, detail)
			continue
		}
		h.log.Info(
			"resuming run",
			"run_id", detail.ID,
			"created_at", detail.CreatedAt,
			"status", detail.Status,
			"user_id", detail.DORAUserID,
			"strategy_type", detail.StrategyType,
			"config", detail.Config,
		)
		if err := h.resumePersistedRun(ctx, detail); err != nil {
			return fmt.Errorf("restore run %s: %w", detail.ID, err)
		}
	}

	return nil
}

func (h *Handler) RestoreBacktests(ctx context.Context) error {
	if h.backtestStore == nil {
		return nil
	}
	backtests, err := h.backtestStore.LoadBacktests(ctx)
	if err != nil {
		return fmt.Errorf("load backtests: %w", err)
	}
	h.mu.Lock()
	for _, detail := range backtests {
		h.backtests[detail.ID] = detail
	}
	h.mu.Unlock()

	return nil
}

func (h *Handler) resumePersistedRun(ctx context.Context, detail *RunDetail) error {
	_, normalised, strat, _, err := h.resolveStrategy(detail.StrategyType, detail.Config, capabilityRun, nil)
	if err != nil {
		return err
	}
	_ = normalised

	// Decrypt the stored API key and inject it into the strategy so it can
	// authenticate with DORA. Without this, a resumed run would fall back to
	// the server's DORA_API_KEY env var, which may belong to a different user.
	var apiKeyDecrypted []byte
	if len(detail.EncryptedAPIKey) > 0 && len(h.encryptionKey) > 0 {
		var err2 error
		apiKeyDecrypted, err2 = decryptAPIKey(detail.EncryptedAPIKey, h.encryptionKey)
		if err2 != nil {
			return fmt.Errorf("decrypt api key for run %s: %w", detail.ID, err2)
		}
		h.applyUserAPIKey(strat, string(apiKeyDecrypted))
	}

	// Attach the per-run decision recorder. nil disables recording.
	h.attachDecisionStore(strat)
	h.attachStateStore(strat)

	// Seed the in-memory decision counter from the DB frontier so a
	// resumed run (e.g. after a server restart) doesn't re-use seqs
	// already in strategy_decisions. A failure here is degraded but
	// non-fatal: the strategy runs, and the first duplicate-key
	// collision surfaces as a save error rather than corrupting state.
	if h.decisionStore != nil {
		if maxSeq, err := h.decisionStore.MaxSeq(ctx, detail.ID); err != nil {
			slog.Warn("cannot seed decision seq; strategy will start from 1 and may collide",
				"run_id", detail.ID, "err", err)
		} else {
			switch s := strat.(type) {
			case *meanreversion.Strategy:
				s.SetDecisionSeq(maxSeq)
			case *copytrading.Strategy:
				s.SetDecisionSeq(maxSeq)
			case *breakout.Strategy:
				s.SetDecisionSeq(maxSeq)
			case *momentum.Strategy:
				s.SetDecisionSeq(maxSeq)
			case *twap.Strategy:
				s.SetDecisionSeq(maxSeq)
			case *vwap.Strategy:
				s.SetDecisionSeq(maxSeq)
			}
		}
	}
	h.startOrderUpdater(detail, strat)
	if _, err := h.startRun(ctx, detail, strat); err != nil {
		return err
	}
	if h.orderUpdates != nil && detail.DORAUserID != "" && apiKeyDecrypted != nil {
		if err := h.orderUpdates.EnsureSubscribed(ctx, detail.DORAUserID, string(apiKeyDecrypted), detail.ID, detail.Status); err != nil {
			slog.Warn("EnsureSubscribed failed", "user_id", detail.DORAUserID, "err", err)
		}
	}
	h.startStopLossObserver(detail, strat)
	h.startCompletionWatcher(detail)
	return nil
}

// startOrderUpdater spawns a per-run goroutine that subscribes to the
// notifications bus for the run's DORA user, filters events for this
// run, and forwards OrderFillEvents to the strategy's updates
// channel. Supports both execution strategies:
//
//   - *twap.Strategy forwards via SetOrderUpdatesChannel(<-chan OrderFillEvent).
//   - *vwap.Strategy forwards via SetOrderUpdatesChannel(<-chan OrderFillEvent).
//
// Both strategies use the same OrderFillEvent shape (it's a type
// alias of exec.OrderFillEvent), so the parsed event value is
// directly assignable to either channel. No-ops when the strategy
// type is not one of the execution strategies, when h.notifier is
// nil, or when the run has no DORA user (it was created without
// one and will not receive DORA order events).
func (h *Handler) startOrderUpdater(detail *RunDetail, strat strategycore.Strategy) {
	if h.notifier == nil || detail.DORAUserID == "" {
		return
	}
	switch s := strat.(type) {
	case *twap.Strategy:
		c := make(chan twap.OrderFillEvent, orderUpdatesBuffer)
		s.SetOrderUpdatesChannel(c)
		h.runOrderUpdater(detail, func(ctx context.Context, evt twap.OrderFillEvent) error {
			select {
			case c <- evt:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	case *vwap.Strategy:
		c := make(chan vwap.OrderFillEvent, orderUpdatesBuffer)
		s.SetOrderUpdatesChannel(c)
		h.runOrderUpdater(detail, func(ctx context.Context, evt vwap.OrderFillEvent) error {
			select {
			case c <- evt:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	default:
		return
	}
}

// runOrderUpdater subscribes to the notifications bus for the run's
// DORA user, filters events for this run, and forwards each parsed
// OrderFillEvent to the strategy via the supplied callback. The
// callback receives a context derived from h.service.BaseContext()
// (or context.Background if none is configured). Closing the
// callback's channel is the strategy's responsibility; runOrderUpdater
// returns when the context is cancelled or the subscription ends.
func (h *Handler) runOrderUpdater(detail *RunDetail, forward func(ctx context.Context, evt exec.OrderFillEvent) error) {
	parentCtx := context.Background()
	if h.service != nil {
		parentCtx = h.service.BaseContext()
	}
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	h.mu.Lock()
	h.stopLossObservers[detail.ID] = cancel
	h.mu.Unlock()

	sub, err := h.notifier.Subscribe(ctx, detail.DORAUserID)
	if err != nil {
		slog.Error("order updates: subscribe failed",
			"strategy_type", detail.StrategyType,
			"run_id", detail.ID, "err", err)
		return
	}
	defer func() {
		if closer, ok := sub.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-sub.Events():
			if !ok {
				return
			}
			if evt.Type != notifications.EventOrderUpdate {
				continue
			}
			if evt.RunID != detail.ID.String() {
				continue
			}
			payload, _ := evt.Payload.(map[string]any)
			if payload == nil {
				continue
			}
			parsed, perr := parseOrderFillEvent(payload)
			if perr != nil {
				slog.Debug("order updates: skip event (missing fields)",
					"run_id", detail.ID, "err", perr)
				continue
			}
			if err := forward(ctx, parsed); err != nil {
				return
			}
		}
	}
}

// parseOrderFillEvent extracts the TWAP fields from a DORA order
// update event payload. The payload is a map[string]any decoded from
// the notifications bus.
func parseOrderFillEvent(payload map[string]any) (twap.OrderFillEvent, error) {
	clientOrderID, _ := payload["client_order_id"].(string)
	if clientOrderID == "" {
		return twap.OrderFillEvent{}, fmt.Errorf("missing client_order_id")
	}
	status, _ := payload["status"].(string)
	if status == "" {
		return twap.OrderFillEvent{}, fmt.Errorf("missing status")
	}
	filledQty, err := parseDecimalField(payload["filled_quantity"])
	if err != nil {
		return twap.OrderFillEvent{}, fmt.Errorf("parse filled_quantity: %w", err)
	}
	return twap.OrderFillEvent{
		ClientOrderID:  clientOrderID,
		Status:         status,
		FilledQuantity: filledQty,
	}, nil
}

// parseDecimalField extracts a decimal value from a payload field that
// may be a string or a number.
func parseDecimalField(v any) (decimal.Decimal, error) {
	switch x := v.(type) {
	case string:
		return decimal.Parse(x)
	case float64:
		return decimal.NewFromFloat64(x)
	case nil:
		return decimal.Zero, nil
	default:
		return decimal.Zero, fmt.Errorf("unsupported type %T", v)
	}
}

func (h *Handler) startRun(ctx context.Context, detail *RunDetail, strat strategycore.Strategy) (uuid.UUID, error) {
	starter, ok := h.service.(runStarter)
	if ok {
		if err := starter.RunStrategyWithID(ctx, detail.ID, strat); err != nil {
			return uuid.Nil, err
		}
		return detail.ID, nil
	}
	id, err := h.service.RunStrategy(ctx, strat)
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// startCompletionWatcher spawns a per-run goroutine that fires
// EventRunCompleted when the strategy's run loop exits naturally
// (chunks exhausted, end_time elapsed, or skipped buckets consumed).
// Distinct from startStopLossObserver which is a no-op for strategies
// without stop-loss semantics — this runs unconditionally so every
// execution strategy's natural completion is observable.
func (h *Handler) startCompletionWatcher(detail *RunDetail) {
	parent := context.Background()
	if h.service != nil {
		if bc := h.service.BaseContext(); bc != nil {
			parent = bc
		}
	}
	ctx, cancel := context.WithCancel(parent)
	h.mu.Lock()
	h.runCompletionWatchers[detail.ID] = cancel
	h.runningStrategies[detail.ID] = nil // ensure map entry exists
	h.mu.Unlock()
	go func() {
		defer cancel()
		defer func() {
			h.mu.Lock()
			delete(h.runCompletionWatchers, detail.ID)
			h.mu.Unlock()
		}()
		interval := h.stopLossObserverInterval
		if interval <= 0 {
			interval = defaultStopLossObserverInterval
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !h.runIsActive(detail.ID) {
					h.maybePublishNaturalCompletion(ctx, detail)
					return
				}
			}
		}
	}()
}

// startStopLossObserver records strat in runningStrategies and spawns a
// per-run goroutine that polls LastStopLossTrigger until the trigger
// fires (publishing EventRunStopLoss) or the run leaves the running
// state. The goroutine is cancellable via the returned cancel func,
// which is also stored in stopLossObservers so stopRun can cancel it.
func (h *Handler) startStopLossObserver(detail *RunDetail, strat strategycore.Strategy) {
	obs, ok := strat.(stopLossObserver)
	if !ok {
		return
	}
	// h.service.BaseContext() may be nil if a test set up a FakeService
	// without a BaseContextReturns. Fall back to Background() so the
	// observer still runs (graceful shutdown won't propagate, which is
	// acceptable for tests). Production wires the real service whose
	// BaseContext is the signal-cancelled context.
	parent := h.service.BaseContext()
	if parent == nil {
		parent = context.Background()
	}
	obsCtx, cancel := context.WithCancel(parent)
	h.mu.Lock()
	h.runningStrategies[detail.ID] = strat
	h.stopLossObservers[detail.ID] = cancel
	h.mu.Unlock()
	go h.runStopLossObserver(obsCtx, detail, obs, cancel)
}

func (h *Handler) runStopLossObserver(ctx context.Context, detail *RunDetail, obs stopLossObserver, cancel context.CancelFunc) {
	defer cancel()
	defer func() {
		h.mu.Lock()
		delete(h.stopLossObservers, detail.ID)
		delete(h.runningStrategies, detail.ID)
		h.mu.Unlock()
	}()

	interval := h.stopLossObserverInterval
	if interval <= 0 {
		interval = defaultStopLossObserverInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !h.runIsActive(detail.ID) {
				h.maybePublishNaturalCompletion(ctx, detail)
				return
			}
			z, pnl, triggered := obs.LastStopLossTrigger()
			if !triggered {
				continue
			}
			h.publishEvent(ctx, notifications.Event{
				Type:    notifications.EventRunStopLoss,
				UserID:  detail.DORAUserID,
				RunID:   detail.ID.String(),
				Payload: map[string]any{"z_score": z, "pnl": pnl},
			})
			return
		}
	}
}

// runIsActive reports whether the strategy's run goroutine for this
// id is still alive by querying the Service. The Service deletes its
// entry when strategy.Run returns, so this returns false once the run
// naturally ends. The observer uses this to fire
// maybePublishNaturalCompletion. We prefer this over checking
// h.runningStrategies because the Handler's map is only cleaned up
// when the observer goroutine exits — a chicken-and-egg deadlock
// prevented natural-completion detection before.
func (h *Handler) runIsActive(id uuid.UUID) bool {
	if h.service == nil {
		return false
	}
	return h.service.IsRunActive(id)
}

// maybePublishNaturalCompletion publishes EventRunCompleted if the run
// was active one tick ago but isn't now, and the status is still
// "running" — that combination means the strategy's run loop
// returned nil naturally (chunks exhausted, end_time elapsed, or all
// skipped consumed) without going through stopRun. The observer
// picks this up once per run. The status is also flipped to
// "completed" so runIsActive returns false and the public status
// reflects reality.
func (h *Handler) maybePublishNaturalCompletion(ctx context.Context, detail *RunDetail) {
	h.mu.Lock()
	d, ok := h.runs[detail.ID]
	if !ok || d.Status != "running" {
		h.mu.Unlock()
		return
	}
	d.Status = "completed"
	d.UpdatedAt = h.now().UTC()
	d.StoppedAt = &d.UpdatedAt
	userID := d.DORAUserID
	runID := d.ID.String()
	if err := h.saveRun(ctx, d); err != nil {
		slog.Error("twap: save completed run", "err", err, "run_id", runID)
	}
	h.mu.Unlock()

	h.publishEvent(ctx, notifications.Event{
		Type:      notifications.EventRunCompleted,
		UserID:    userID,
		RunID:     runID,
		Timestamp: h.now().UTC(),
	})
}

// runWindowExpired returns true if the run's execution window has
// already passed. Reads end_time out of the persisted config JSON,
// which is the canonical source of truth (RunDetail does not expose
// end_time directly).
func (h *Handler) runWindowExpired(detail *RunDetail) bool {
	var cfg struct {
		EndTime time.Time `json:"end_time"`
	}
	if err := json.Unmarshal(detail.Config, &cfg); err != nil {
		return false
	}
	if cfg.EndTime.IsZero() {
		return false
	}
	return h.now().UTC().After(cfg.EndTime)
}

// expireRun marks the run as completed in the DB and publishes
// EventRunCompleted. Called by RestoreRuns when a run with status
// "running" is found past its end_time — we don't want to relaunch
// a finished run because the strategy would only skip every chunk
// at startup.
func (h *Handler) expireRun(ctx context.Context, detail *RunDetail) {
	h.log.Info(
		"run expired during restart, marking completed",
		"run_id", detail.ID,
		"strategy_type", detail.StrategyType,
	)
	now := h.now().UTC()
	userID := ""
	runIDStr := detail.ID.String()
	h.mu.Lock()
	if d, ok := h.runs[detail.ID]; ok {
		d.Status = "completed"
		d.UpdatedAt = now
		d.StoppedAt = &now
		userID = d.DORAUserID
		if err := h.saveRun(ctx, d); err != nil {
			slog.Error("save expired run", "err", err, "run_id", runIDStr)
		}
	}
	h.mu.Unlock()
	h.publishEvent(ctx, notifications.Event{
		Type:      notifications.EventRunCompleted,
		UserID:    userID,
		RunID:     runIDStr,
		Timestamp: now,
	})
}

func (h *Handler) resolveDORAUserID(ctx context.Context) (string, error) {
	// Fast path: user was already verified by the auth middleware.
	if id, ok := doraUserIDFromContext(ctx); ok {
		return id, nil
	}
	client := h.doraClient
	if client == nil {
		client = NewDORAClient()
	}
	return client.GetUserID(ctx)
}

func (h *Handler) saveRun(ctx context.Context, detail *RunDetail) error {
	if h.runStore == nil {
		return nil
	}
	return h.runStore.SaveRun(ctx, detail)
}

func (h *Handler) saveBacktest(ctx context.Context, detail *BacktestDetail) error {
	if h.backtestStore == nil {
		return nil
	}
	return h.backtestStore.SaveBacktest(ctx, detail)
}

func (h *Handler) runDetail(id uuid.UUID) (*RunDetail, bool) {
	h.mu.RLock()
	detail, ok := h.runs[id]
	h.mu.RUnlock()
	return detail, ok
}

type strategyCapability string

const (
	capabilityRun      strategyCapability = "run"
	capabilityBacktest strategyCapability = "backtest"
)

func (h *Handler) resolveStrategy(strategyType string, config json.RawMessage, capability strategyCapability, tradeWriter stats.BacktestTradeWriter) (StrategyDefinition, json.RawMessage, strategycore.Strategy, int, error) { //nolint:lll
	if strategyType == "" {
		return StrategyDefinition{}, nil, nil, http.StatusBadRequest, fmt.Errorf("strategy_type is required")
	}
	def, ok := h.strategies[strategyType]
	if !ok {
		return StrategyDefinition{}, nil, nil, http.StatusBadRequest, fmt.Errorf("unsupported strategy_type %q", strategyType)
	}
	if def.Status != strategyStatusAvailable {
		return StrategyDefinition{}, nil, nil, http.StatusNotImplemented, fmt.Errorf("strategy_type %q is not implemented", strategyType)
	}
	if capability == capabilityRun && !def.SupportsRun {
		return StrategyDefinition{}, nil, nil, http.StatusNotImplemented, fmt.Errorf("strategy_type %q is not implemented for runs", strategyType)
	}
	if capability == capabilityBacktest && !def.SupportsBacktest {
		return StrategyDefinition{}, nil, nil, http.StatusNotImplemented, fmt.Errorf("strategy_type %q is not implemented for backtests", strategyType) //nolint:lll
	}
	if def.DecodeConfig == nil {
		return StrategyDefinition{}, nil, nil, http.StatusNotImplemented, fmt.Errorf("strategy_type %q has no config decoder", strategyType)
	}

	normalised, strat, err := def.DecodeConfig(config, string(capability), tradeWriter)
	if err != nil {
		return StrategyDefinition{}, nil, nil, http.StatusBadRequest, err
	}
	return def, normalised, strat, http.StatusOK, nil
}

// attachDecisionStore wires the handler's configured DecisionRecorder
// into a freshly-built strategy so the live run loop persists every
// decision that triggers a market order.  It is a no-op when the
// handler has no recorder configured (the default) or when the
// strategy type does not support recording.
func (h *Handler) attachDecisionStore(strat strategycore.Strategy) {
	if h.decisionStore == nil {
		return
	}
	switch s := strat.(type) {
	case *meanreversion.Strategy:
		meanreversion.WithDecisionStore(h.decisionStore)(s)
	case *copytrading.Strategy:
		copytrading.WithDecisionStore(h.decisionStore)(s)
	case *breakout.Strategy:
		breakout.WithDecisionStore(h.decisionStore)(s)
	case *momentum.Strategy:
		momentum.WithDecisionStore(h.decisionStore)(s)
	case *twap.Strategy:
		twap.WithDecisionStore(h.decisionStore)(s)
	case *vwap.Strategy:
		vwap.WithDecisionStore(h.decisionStore)(s)
	}
}

// applyUserAPIKey injects the per-request DORA API key into the
// strategy so it can authenticate against DORA with the caller's
// credentials instead of the server-global key. No-op when key is
// empty. Used by createBacktest, createRun, and resumePersistedRun
// to fold the type switch into a single helper.
func (h *Handler) applyUserAPIKey(strat strategycore.Strategy, apiKey string) {
	if apiKey == "" {
		return
	}
	client := strategycore.NewDoraClientWithKey(apiKey)
	switch s := strat.(type) {
	case *meanreversion.Strategy:
		meanreversion.WithMarketAPIClient(client)(s)
	case *copytrading.Strategy:
		copytrading.WithMarketAPIClient(client)(s)
	case *breakout.Strategy:
		breakout.WithMarketAPIClient(client)(s)
	case *momentum.Strategy:
		momentum.WithMarketAPIClient(client)(s)
	case *twap.Strategy:
		twap.WithMarketAPIClient(client)(s)
	case *vwap.Strategy:
		vwap.WithMarketAPIClient(client)(s)
	}
}

// attachStateStore wires the handler's configured StateStore into a
// freshly-built strategy so the live run loop checkpoints progress for
// crash recovery. Currently only TWAP uses it; no-op for other types
// or when the handler has no store configured.
func (h *Handler) attachStateStore(strat strategycore.Strategy) {
	if h.stateStore == nil {
		return
	}
	if s, ok := strat.(*twap.Strategy); ok {
		twap.WithStateStore(h.stateStore)(s)
	}
	if s, ok := strat.(*vwap.Strategy); ok {
		vwap.WithStateStore(h.stateStore)(s)
	}
}

func defaultStrategies(
	pricesHandler *prices.Handler,
	tradesHistoryStore *copytrading.PGTradesHistoryStore,
	tradeStream *streams.TradeStream,
	historicalPriceStore breakout.HistoricalPriceStore,
	tradeHistoryStore breakout.TradeHistoryStore,
	log *slog.Logger,
) map[string]StrategyDefinition {
	defs := []StrategyDefinition{
		newMeanReversionDefinition(pricesHandler, log),
		newTWAPDefinition(log),
		newCopyTradingDefinition(tradesHistoryStore, tradeStream),
		newBreakoutDefinition(pricesHandler, tradeStream, historicalPriceStore, tradeHistoryStore, log),
		newVWAPDefinition(tradeHistoryStore, log),
		newMomentumDefinition(pricesHandler, log),
	}
	out := make(map[string]StrategyDefinition, len(defs))
	for _, def := range defs {
		out[def.Type] = def
	}
	return out
}

func newMeanReversionDefinition(pricesHandler *prices.Handler, log *slog.Logger) StrategyDefinition {
	defaults := meanreversion.DefaultConfig()
	return StrategyDefinition{
		Type:        "mean_reversion",
		Status:      strategyStatusAvailable,
		Description: "Rolling z-score mean reversion strategy.",
		ConfigFields: []StrategyConfigField{
			{
				Name:        "lookback_window",
				Type:        "integer",
				Description: "Rolling observation window. Must be at least 2.",
				Required:    false,
				Default:     defaults.LookbackWindow,
			},
			{
				Name:        "entry_z_score",
				Type:        "number",
				Description: "Entry threshold for opening positions. Must be greater than 0.",
				Required:    false,
				Default:     mustFloat64(defaults.EntryZScore),
			},
			{
				Name:        "exit_z_score",
				Type:        "number",
				Description: "Exit threshold for closing positions as spreads revert. Must be non-negative.",
				Required:    false,
				Default:     mustFloat64(defaults.ExitZScore),
			},
			{
				Name:        "stop_loss_z_score",
				Type:        "number",
				Description: "Stop-loss threshold for closing losing positions. Must be non-negative.",
				Required:    false,
				Default:     mustFloat64(defaults.StopLossZScore),
			},
			{
				Name:        "min_std_dev",
				Type:        "number",
				Description: "Minimum spread volatility required before trading. Must be non-negative.",
				Required:    false,
				Default:     mustFloat64(defaults.MinStdDev),
			},
			{
				Name:        "max_position_size",
				Type:        "number",
				Description: "Maximum fraction of capital allocated per trade. Must be in (0,1].",
				Required:    false,
				Default:     mustFloat64(defaults.MaxPositionSize),
			},
			{
				Name:        "order_book_id",
				Type:        "string(uuid)",
				Description: "Order book UUID used to locate the traded asset and place orders.",
				Required:    false,
			},
			{
				Name:        "tenor",
				Type:        "string",
				Description: "Benchmark Treasury tenor, for example 1M, 6M, 2Y, 5Y, 10Y, or 30Y.",
				Required:    false,
			},
			{
				Name:        "initial balance",
				Type:        "number",
				Description: "Maximum total position amount. Must be greater than 0.",
				Required:    false,
				Default:     mustFloat64(defaults.InitialBalance),
			},
			{
				Name:        "leverage",
				Type:        "number",
				Description: "Leverage multiplier for live orders. Must be greater than 0.",
				Required:    false,
				Default:     mustFloat64(defaults.Leverage),
			},
		},
		SupportsRun:      true,
		SupportsBacktest: true,
		DecodeConfig: func(
			raw json.RawMessage,
			capability string,
			tradeWriter stats.BacktestTradeWriter,
		) (json.RawMessage, strategycore.Strategy, error) {
			forRun := capability == string(capabilityRun)
			cfg, normalised, err := decodeMeanReversionConfig(raw, forRun)
			if err != nil {
				return nil, nil, err
			}
			opts := []func(*meanreversion.Strategy){
				meanreversion.WithLogger(log),
			}
			if tradeWriter != nil {
				opts = append(opts, meanreversion.WithBacktestWriter(tradeWriter))
			}
			return normalised, meanreversion.New(cfg, pricesHandler, opts...), nil
		},
	}
}

func newTWAPDefinition(log *slog.Logger) StrategyDefinition {
	defaults := twap.DefaultConfig()
	return StrategyDefinition{
		Type:             "twap",
		Status:           strategyStatusAvailable,
		Description:      "Time-weighted average price execution strategy for a single order book.",
		SupportsRun:      true,
		SupportsBacktest: false,
		ConfigFields: []StrategyConfigField{
			{
				Name:        "order_book_id",
				Type:        "string(uuid)",
				Description: "Order book UUID where orders will be placed.",
				Required:    true,
			},
			{
				Name:        "total_amount",
				Type:        "number",
				Description: "Total quantity to trade across the execution window.",
				Required:    true,
			},
			{
				Name:        "side",
				Type:        "string",
				Description: "Trade side: 'buy' or 'sell'.",
				Required:    true,
			},
			{
				Name:        "start_time",
				Type:        "string",
				Description: "ISO 8601 start time for execution window.",
				Required:    true,
			},
			{
				Name:        "end_time",
				Type:        "string",
				Description: "ISO 8601 end time for execution window.",
				Required:    true,
			},
			{
				Name:        "interval_seconds",
				Type:        "number",
				Description: "Time between each chunk order. Default 300 (5 minutes).",
				Required:    false,
				Default:     defaults.IntervalSeconds,
			},
		},
		DecodeConfig: func(
			raw json.RawMessage,
			capability string,
			tradeWriter stats.BacktestTradeWriter,
		) (json.RawMessage, strategycore.Strategy, error) {
			cfg, normalised, err := decodeTWAPConfig(raw)
			if err != nil {
				return nil, nil, err
			}
			return normalised, twap.New(cfg, log), nil
		},
	}
}

func newVWAPDefinition(tradeHistoryStore breakout.TradeHistoryStore, log *slog.Logger) StrategyDefinition {
	defaults := vwap.DefaultConfig()
	return StrategyDefinition{
		Type:             "vwap",
		Status:           strategyStatusAvailable,
		Description:      "Volume-weighted average price execution strategy. Bucket schedule derived from historical trade volume.",
		SupportsRun:      true,
		SupportsBacktest: false,
		ConfigFields: []StrategyConfigField{
			{
				Name:        "order_book_id",
				Type:        "string(uuid)",
				Description: "Order book UUID where orders will be placed.",
				Required:    true,
			},
			{
				Name:        "total_amount",
				Type:        "number",
				Description: "Total quantity to trade across the execution window.",
				Required:    true,
			},
			{
				Name:        "side",
				Type:        "string",
				Description: "Trade side: 'buy' or 'sell'.",
				Required:    true,
			},
			{
				Name:        "start_time",
				Type:        "string",
				Description: "ISO 8601 start time for execution window.",
				Required:    true,
			},
			{
				Name:        "end_time",
				Type:        "string",
				Description: "ISO 8601 end time for execution window.",
				Required:    true,
			},
			{
				Name:        "window_days",
				Type:        "integer",
				Description: "Days of historical trade data used for ADV buckets. Default 30.",
				Required:    false,
				Default:     defaults.WindowDays,
			},
			{
				Name:        "bucket_minutes",
				Type:        "integer",
				Description: "Granularity of each VWAP bucket. Default 5.",
				Required:    false,
				Default:     defaults.BucketMinutes,
			},
		},
		DecodeConfig: func(
			raw json.RawMessage,
			capability string,
			tradeWriter stats.BacktestTradeWriter,
		) (json.RawMessage, strategycore.Strategy, error) {
			cfg, normalised, err := decodeVWAPConfig(raw)
			if err != nil {
				return nil, nil, err
			}
			s := vwap.New(cfg, log)
			if tradeHistoryStore != nil {
				vwap.WithTradeHistoryStore(tradeHistoryStore)(s)
			}
			return normalised, s, nil
		},
	}
}

func newCopyTradingDefinition(tradesHistoryStore *copytrading.PGTradesHistoryStore, tradeStream *streams.TradeStream) StrategyDefinition {
	return StrategyDefinition{
		Type:        "copytrading",
		Status:      strategyStatusAvailable,
		Description: "Copy trades from a followed trader subject to limits.",
		ConfigFields: []StrategyConfigField{
			{
				Name:        "followed_trader",
				Type:        "string(uuid)",
				Description: "Trader UUID to mirror. Required.",
				Required:    true,
			},
			{
				Name:        "percentage_of_available",
				Type:        "number",
				Description: "Percentage of available balance to use per trade (0-1). Must be greater than 0.",
				Required:    true,
			},
			{
				Name:        "leverage",
				Type:        "number",
				Description: "Leverage multiplier for copied orders. Must be greater than 0.",
				Required:    true,
			},
			{
				Name:        "min_order_size",
				Type:        "integer",
				Description: "Minimum copied order size. Must be non-negative.",
				Required:    false,
			},
			{
				Name:        "max_order_size",
				Type:        "integer",
				Description: "Maximum copied order size. Must be greater than or equal to min_order_size.",
				Required:    false,
			},
			{
				Name:        "disallowed_bonds",
				Type:        "array[string(uuid)]",
				Description: "Optional list of bond UUIDs to skip. Empty means no bonds are disallowed.",
				Required:    false,
			},
			{
				Name:        "initial_balance",
				Type:        "number",
				Description: "Starting cash for the backtest. Must be non-negative; omit or set to 0 to use the default of 10000.",
				Required:    false,
			},
		},
		SupportsRun:      true,
		SupportsBacktest: true,
		DecodeConfig: func(
			raw json.RawMessage,
			capability string,
			tradeWriter stats.BacktestTradeWriter,
		) (json.RawMessage, strategycore.Strategy, error) {
			cfg, normalised, err := decodeCopyTradingConfig(raw)
			if err != nil {
				return nil, nil, err
			}
			opts := []func(*copytrading.Strategy){
				copytrading.WithLogger(slog.Default()),
				copytrading.WithBacktestStore(tradesHistoryStore),
			}
			if capability == string(capabilityRun) && tradeStream != nil {
				opts = append(opts, copytrading.WithTradeStream(tradeStream))
			}
			if tradeWriter != nil {
				opts = append(opts, copytrading.WithBacktestWriter(tradeWriter))
			}
			return normalised, copytrading.New(cfg, opts...), nil
		},
	}
}

type meanReversionConfigPayload struct {
	LookbackWindow  int      `json:"lookback_window"`
	EntryZScore     float64  `json:"entry_z_score"`
	ExitZScore      float64  `json:"exit_z_score"`
	StopLossZScore  float64  `json:"stop_loss_z_score"`
	MinStdDev       float64  `json:"min_std_dev"`
	MaxPositionSize float64  `json:"max_position_size"`
	OrderBookID     string   `json:"order_book_id,omitempty"`
	Tenor           string   `json:"tenor,omitempty"`
	InitialBalance  *float64 `json:"initial_balance,omitempty"`
	Leverage        *float64 `json:"leverage,omitempty"`
}

type breakoutConfigPayload struct {
	ShortVolWindow       int     `json:"short_vol_window"`
	LongVolWindow        int     `json:"long_vol_window"`
	CompressionThreshold float64 `json:"compression_threshold"`
	ATRWindow            int     `json:"atr_window"`
	BreakoutATRMultiple  float64 `json:"breakout_atr_multiple"`
	ConfirmationBars     int     `json:"confirmation_bars"`
	StopLossATR          float64 `json:"stop_loss_atr"`
	TakeProfitATR        float64 `json:"take_profit_atr"`
	MinLongVolFloor      float64 `json:"min_long_vol_floor"`

	OBVTrendThreshold float64 `json:"obv_trend_threshold"`
	OBVWindow         int     `json:"obv_window"`
	OrderBookID       string  `json:"order_book_id,omitempty"`

	InitialBalance *float64 `json:"initial_balance,omitempty"`
	Leverage       *float64 `json:"leverage,omitempty"`
}

type momentumConfigPayload struct {
	SignalSource    string   `json:"signal_source"`
	FastWindow      int      `json:"fast_window"`
	SlowWindow      int      `json:"slow_window"`
	ATRWindow       int      `json:"atr_window"`
	StopLossATR     *float64 `json:"stop_loss_atr,omitempty"`
	TakeProfitATR   *float64 `json:"take_profit_atr,omitempty"`
	MinOrderSize    float64  `json:"min_order_size"`
	MaxOrderSize    float64  `json:"max_order_size"`
	MaxPositionSize float64  `json:"max_position_size"`
	Tenor           string   `json:"tenor,omitempty"`
	OrderBookID     string   `json:"order_book_id,omitempty"`

	InitialBalance *float64 `json:"initial_balance,omitempty"`
	Leverage       *float64 `json:"leverage,omitempty"`
}

//nolint:funlen // strategy definition with 12 config fields
func newBreakoutDefinition(
	pricesHandler *prices.Handler,
	tradeStream *streams.TradeStream,
	historicalStore breakout.HistoricalPriceStore,
	tradeHistoryStore breakout.TradeHistoryStore,
	log *slog.Logger,
) StrategyDefinition {
	defaults := breakout.DefaultConfig()
	return StrategyDefinition{
		Type:   breakout.StrategyType,
		Status: strategyStatusAvailable,
		Description: "Volatility-compression / price-breakout strategy. Enters when short-window price volatility drops below a " +
			"threshold of long-window volatility, then a close breaks above (or below) a k·ATR trigger band.",
		ConfigFields: []StrategyConfigField{
			{
				Name:        "short_vol_window",
				Type:        "integer",
				Description: "Short-window price-volatility observation count. Must be at least 2.",
				Required:    false,
				Default:     defaults.ShortVolWindow,
			},
			{
				Name:        "long_vol_window",
				Type:        "integer",
				Description: "Long-window price-volatility observation count. Must be greater than short_vol_window.",
				Required:    false,
				Default:     defaults.LongVolWindow,
			},
			{
				Name:        "compression_threshold",
				Type:        "number",
				Description: "ShortVol/LongVol ratio below which the strategy arms for a breakout. Must be in (0, 1].",
				Required:    false,
				Default:     mustFloat64(defaults.CompressionThreshold),
			},
			{
				Name:        "atr_window",
				Type:        "integer",
				Description: "Rolling-mean window for ATR (mean absolute price diff). Must be at least 2.",
				Required:    false,
				Default:     defaults.ATRWindow,
			},
			{
				Name:        "breakout_atr_multiple",
				Type:        "number",
				Description: "Number of ATR units above/below the most recent close that defines the breakout trigger. Must be non-negative.",
				Required:    false,
				Default:     mustFloat64(defaults.BreakoutATRMultiple),
			},
			{
				Name: "confirmation_bars",
				Type: "integer",
				Description: "Consecutive closes beyond the trigger band required to fire. " +
					"Calibrated for a continuously trading bond market (typical 5-30; min 1).",
				Required: false,
				Default:  defaults.ConfirmationBars,
			},
			{
				Name:        "stop_loss_atr",
				Type:        "number",
				Description: "Stop-loss distance in ATR units. Set to 0 to disable. Mirrored in the live Run loop and the backtester.",
				Required:    false,
				Default:     mustFloat64(defaults.StopLossATR),
			},
			{
				Name:        "take_profit_atr",
				Type:        "number",
				Description: "Take-profit distance in ATR units from entry. Set to 0 to disable. Mirrored in the live Run loop and the backtester.",
				Required:    false,
				Default:     mustFloat64(defaults.TakeProfitATR),
			},
			{
				Name:        "min_long_vol_floor",
				Type:        "number",
				Description: "Minimum LongVol required to trade. Set to 0 to disable. Suppresses entries on a completely flat baseline.",
				Required:    false,
				Default:     mustFloat64(defaults.MinLongVolFloor),
			},
			{
				Name:        "obv_trend_threshold",
				Type:        "number",
				Description: "OBV threshold for the volume confirmation filter. BUY > this, SELL < -this. Default 0 means any non-zero OBV works.",
				Required:    false,
				Default:     mustFloat64(defaults.OBVTrendThreshold),
			},
			{
				Name:        "obv_window",
				Type:        "integer",
				Description: "Recent trades to include in windowed OBV. 0 = no volume verification. >0 = verify with last N trades.",
				Required:    false,
				Default:     defaults.OBVWindow,
			},
			{
				Name:        "order_book_id",
				Type:        "string(uuid)",
				Description: "Order book UUID used to locate the traded asset and place orders.",
				Required:    false,
			},
			{
				Name:        "initial_balance",
				Type:        "number",
				Description: "Starting capital for backtests. Omitted for live runs — obtained from DORA positions.",
				Required:    false,
				Default:     mustFloat64(defaults.InitialBalance),
			},
			{
				Name:        "leverage",
				Type:        "number",
				Description: "Leverage multiplier for live orders. Must be greater than 0.",
				Required:    false,
				Default:     mustFloat64(defaults.Leverage),
			},
		},
		SupportsRun:      true,
		SupportsBacktest: true,
		DecodeConfig: func(
			raw json.RawMessage,
			capability string,
			tradeWriter stats.BacktestTradeWriter,
		) (json.RawMessage, strategycore.Strategy, error) {
			forRun := capability == string(capabilityRun)
			cfg, normalised, err := decodeBreakoutConfig(raw, forRun)
			if err != nil {
				return nil, nil, err
			}
			opts := []func(*breakout.Strategy){
				breakout.WithLogger(log),
			}
			if tradeWriter != nil {
				opts = append(opts, breakout.WithBacktestWriter(tradeWriter))
			}
			// Wire the live trade stream when the volume filter is active
			// (OBVWindow > 0); ignored otherwise. tradeStream is always
			// non-nil at runtime (cmd/strategy-server starts it in main.go).
			if cfg.OBVWindow > 0 && tradeStream != nil {
				opts = append(opts, breakout.WithTradeStream(tradeStream))
			}
			// Wire the historical price store so Strategy.Backtest can
			// read candles_history. May be nil if no DB is configured;
			// Backtest will then return an error.
			if historicalStore != nil {
				opts = append(opts, breakout.WithHistoricalStore(historicalStore))
			}
			// Wire the historical trade store so the backtester can
			// compute OBV for the volume confirmation filter. May be
			// nil if no DB is configured; the backtester simply skips
			// OBV accumulation when no store is wired.
			if tradeHistoryStore != nil {
				opts = append(opts, breakout.WithTradeHistoryStore(tradeHistoryStore))
			}
			return normalised, breakout.New(cfg, pricesHandler, opts...), nil
		},
	}
}

//nolint:funlen // config decoding with validation
func decodeBreakoutConfig(raw json.RawMessage, forRun bool) (breakout.Config, json.RawMessage, error) {
	var payload breakoutConfigPayload
	if err := decodeRawConfig(raw, &payload); err != nil {
		return breakout.Config{}, nil, err
	}
	defaults := breakout.DefaultConfig()

	// Apply defaults and validate windows.
	if payload.ShortVolWindow == 0 {
		payload.ShortVolWindow = defaults.ShortVolWindow
	}
	if payload.LongVolWindow == 0 {
		payload.LongVolWindow = defaults.LongVolWindow
	}
	if payload.ATRWindow == 0 {
		payload.ATRWindow = defaults.ATRWindow
	}
	if payload.ConfirmationBars == 0 {
		payload.ConfirmationBars = defaults.ConfirmationBars
	}
	if payload.ShortVolWindow < 2 { //nolint:mnd
		return breakout.Config{}, nil, fmt.Errorf("config.short_vol_window must be at least 2")
	}
	if payload.LongVolWindow <= payload.ShortVolWindow {
		return breakout.Config{}, nil, fmt.Errorf("config.long_vol_window must be greater than short_vol_window")
	}
	if payload.ATRWindow < 2 { //nolint:mnd
		return breakout.Config{}, nil, fmt.Errorf("config.atr_window must be at least 2")
	}
	if payload.ConfirmationBars < 1 {
		return breakout.Config{}, nil, fmt.Errorf("config.confirmation_bars must be at least 1")
	}

	// Apply defaults and validate thresholds.
	if payload.CompressionThreshold == 0 {
		payload.CompressionThreshold = mustFloat64(defaults.CompressionThreshold)
	}
	if payload.BreakoutATRMultiple == 0 {
		payload.BreakoutATRMultiple = mustFloat64(defaults.BreakoutATRMultiple)
	}
	if payload.StopLossATR == 0 {
		payload.StopLossATR = mustFloat64(defaults.StopLossATR)
	}
	if payload.TakeProfitATR == 0 {
		payload.TakeProfitATR = mustFloat64(defaults.TakeProfitATR)
	}
	if payload.MinLongVolFloor == 0 {
		payload.MinLongVolFloor = mustFloat64(defaults.MinLongVolFloor)
	}
	if payload.OBVTrendThreshold == 0 {
		payload.OBVTrendThreshold = mustFloat64(defaults.OBVTrendThreshold)
	}
	if payload.OBVTrendThreshold < 0 {
		return breakout.Config{}, nil, fmt.Errorf("config.obv_trend_threshold must be non-negative")
	}
	if payload.OBVWindow < 0 {
		return breakout.Config{}, nil, fmt.Errorf("config.obv_window must be non-negative")
	}
	if payload.CompressionThreshold <= 0 || payload.CompressionThreshold > 1 {
		return breakout.Config{}, nil, fmt.Errorf("config.compression_threshold must be in (0, 1]")
	}
	if payload.BreakoutATRMultiple < 0 {
		return breakout.Config{}, nil, fmt.Errorf("config.breakout_atr_multiple must be non-negative")
	}
	if payload.StopLossATR < 0 {
		return breakout.Config{}, nil, fmt.Errorf("config.stop_loss_atr must be non-negative")
	}
	if payload.TakeProfitATR < 0 {
		return breakout.Config{}, nil, fmt.Errorf("config.take_profit_atr must be non-negative")
	}
	if payload.MinLongVolFloor < 0 {
		return breakout.Config{}, nil, fmt.Errorf("config.min_long_vol_floor must be non-negative")
	}

	compression, err := decimal.NewFromFloat64(payload.CompressionThreshold)
	if err != nil {
		return breakout.Config{}, nil, fmt.Errorf("config.compression_threshold: %w", err)
	}
	breakoutATR, err := decimal.NewFromFloat64(payload.BreakoutATRMultiple)
	if err != nil {
		return breakout.Config{}, nil, fmt.Errorf("config.breakout_atr_multiple: %w", err)
	}
	stopLoss, err := decimal.NewFromFloat64(payload.StopLossATR)
	if err != nil {
		return breakout.Config{}, nil, fmt.Errorf("config.stop_loss_atr: %w", err)
	}
	takeProfit, err := decimal.NewFromFloat64(payload.TakeProfitATR)
	if err != nil {
		return breakout.Config{}, nil, fmt.Errorf("config.take_profit_atr: %w", err)
	}
	obvThreshold, err := decimal.NewFromFloat64(payload.OBVTrendThreshold)
	if err != nil {
		return breakout.Config{}, nil, fmt.Errorf("config.obv_trend_threshold: %w", err)
	}
	minFloor, err := decimal.NewFromFloat64(payload.MinLongVolFloor)
	if err != nil {
		return breakout.Config{}, nil, fmt.Errorf("config.min_long_vol_floor: %w", err)
	}

	amount := defaults.InitialBalance
	if payload.InitialBalance != nil {
		if *payload.InitialBalance < 0 {
			return breakout.Config{}, nil, fmt.Errorf("config.initial_balance must be non-negative")
		}
		if *payload.InitialBalance == 0 {
			if !forRun {
				return breakout.Config{}, nil, fmt.Errorf("config.initial_balance must be greater than 0 for backtests")
			}
		} else {
			amount, err = decimal.NewFromFloat64(*payload.InitialBalance)
			if err != nil {
				return breakout.Config{}, nil, fmt.Errorf("config.initial_balance: %w", err)
			}
		}
	}

	leverage := defaults.Leverage
	if payload.Leverage != nil {
		if *payload.Leverage <= 0 {
			return breakout.Config{}, nil, fmt.Errorf("config.leverage must be greater than 0")
		}
		leverage, err = decimal.NewFromFloat64(*payload.Leverage)
		if err != nil {
			return breakout.Config{}, nil, fmt.Errorf("config.leverage: %w", err)
		}
	}

	var orderBookID uuid.UUID
	if payload.OrderBookID != "" {
		orderBookID, err = uuid.Parse(strings.TrimSpace(payload.OrderBookID))
		if err != nil {
			return breakout.Config{}, nil, fmt.Errorf("config.order_book_id: %w", err)
		}
	}

	normalised, err := json.Marshal(payload)
	if err != nil {
		return breakout.Config{}, nil, fmt.Errorf("marshal normalised config: %w", err)
	}

	return breakout.Config{
		ShortVolWindow:       payload.ShortVolWindow,
		LongVolWindow:        payload.LongVolWindow,
		CompressionThreshold: compression,
		ATRWindow:            payload.ATRWindow,
		BreakoutATRMultiple:  breakoutATR,
		ConfirmationBars:     payload.ConfirmationBars,
		StopLossATR:          stopLoss,
		TakeProfitATR:        takeProfit,
		MinLongVolFloor:      minFloor,
		OBVTrendThreshold:    obvThreshold,
		OBVWindow:            payload.OBVWindow,
		OrderBookID:          orderBookID,
		InitialBalance:       amount,
		Leverage:             leverage,
	}, normalised, nil
}

//nolint:funlen // config decoding with validation
func decodeMomentumConfig(raw json.RawMessage, forRun bool) (momentum.Config, json.RawMessage, error) {
	var payload momentumConfigPayload
	if err := decodeRawConfig(raw, &payload); err != nil {
		return momentum.Config{}, nil, err
	}
	defaults := momentum.DefaultConfig()

	if payload.SignalSource == "" {
		payload.SignalSource = defaults.SignalSource
	}
	if payload.FastWindow == 0 {
		payload.FastWindow = defaults.FastWindow
	}
	if payload.SlowWindow == 0 {
		payload.SlowWindow = defaults.SlowWindow
	}
	if payload.ATRWindow == 0 {
		payload.ATRWindow = defaults.ATRWindow
	}
	if payload.MaxPositionSize == 0 {
		payload.MaxPositionSize = mustFloat64(defaults.MaxPositionSize)
	}
	stopLossATR := mustFloat64(defaults.StopLossATR)
	if payload.StopLossATR != nil {
		stopLossATR = *payload.StopLossATR
	}
	takeProfitATR := mustFloat64(defaults.TakeProfitATR)
	if payload.TakeProfitATR != nil {
		takeProfitATR = *payload.TakeProfitATR
	}

	if payload.SignalSource != momentum.SignalSourcePrice &&
		payload.SignalSource != momentum.SignalSourceYTM &&
		payload.SignalSource != momentum.SignalSourceSpread {
		return momentum.Config{}, nil, fmt.Errorf("config.signal_source must be one of price, ytm, spread")
	}
	if payload.FastWindow < 2 { //nolint:mnd
		return momentum.Config{}, nil, fmt.Errorf("config.fast_window must be at least 2")
	}
	if payload.SlowWindow <= payload.FastWindow {
		return momentum.Config{}, nil, fmt.Errorf("config.slow_window must be greater than fast_window")
	}
	if payload.ATRWindow < 2 { //nolint:mnd
		return momentum.Config{}, nil, fmt.Errorf("config.atr_window must be at least 2")
	}
	if stopLossATR < 0 {
		return momentum.Config{}, nil, fmt.Errorf("config.stop_loss_atr must be non-negative")
	}
	if takeProfitATR < 0 {
		return momentum.Config{}, nil, fmt.Errorf("config.take_profit_atr must be non-negative")
	}
	if payload.MinOrderSize < 0 {
		return momentum.Config{}, nil, fmt.Errorf("config.min_order_size must be non-negative")
	}
	if payload.MaxOrderSize < 0 {
		return momentum.Config{}, nil, fmt.Errorf("config.max_order_size must be non-negative")
	}
	if payload.MaxPositionSize <= 0 || payload.MaxPositionSize > 1 {
		return momentum.Config{}, nil, fmt.Errorf("config.max_position_size must be in (0,1]")
	}
	if payload.SignalSource == momentum.SignalSourceSpread && payload.Tenor == "" {
		return momentum.Config{}, nil, fmt.Errorf("config.tenor is required when signal_source is spread")
	}

	stopLoss, err := decimal.NewFromFloat64(stopLossATR)
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("config.stop_loss_atr: %w", err)
	}
	takeProfit, err := decimal.NewFromFloat64(takeProfitATR)
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("config.take_profit_atr: %w", err)
	}
	minOrder, err := decimal.NewFromFloat64(payload.MinOrderSize)
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("config.min_order_size: %w", err)
	}
	maxOrder, err := decimal.NewFromFloat64(payload.MaxOrderSize)
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("config.max_order_size: %w", err)
	}
	maxPosition, err := decimal.NewFromFloat64(payload.MaxPositionSize)
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("config.max_position_size: %w", err)
	}

	amount := defaults.InitialBalance
	if payload.InitialBalance != nil {
		if *payload.InitialBalance < 0 {
			return momentum.Config{}, nil, fmt.Errorf("config.initial_balance must be non-negative")
		}
		if *payload.InitialBalance == 0 {
			// For backtests, initial_balance must be > 0 because it is the
			// seed capital. For runs, 0 means "fetch my USD balance": the live
			// path overrides cfg.InitialBalance from the user's DORA portfolio
			// (strategy/momentum/strategy.go:504). Store the explicit 0 so the
			// persisted config reflects the request; if the portfolio fetch
			// never succeeds the strategy sizes off 0 (opens nothing) rather
			// than off the default.
			if !forRun {
				return momentum.Config{}, nil, fmt.Errorf("config.initial_balance must be greater than 0 for backtests")
			}
			amount = decimal.Zero
		} else {
			amount, err = decimal.NewFromFloat64(*payload.InitialBalance)
			if err != nil {
				return momentum.Config{}, nil, fmt.Errorf("config.initial_balance: %w", err)
			}
		}
	}
	leverage := defaults.Leverage
	if payload.Leverage != nil {
		if *payload.Leverage <= 0 {
			return momentum.Config{}, nil, fmt.Errorf("config.leverage must be greater than 0")
		}
		leverage, err = decimal.NewFromFloat64(*payload.Leverage)
		if err != nil {
			return momentum.Config{}, nil, fmt.Errorf("config.leverage: %w", err)
		}
	}

	// MaxOrderSize >= MinOrderSize when both are positive. Mirror copytrading's
	// cross-check (decodeCopyTradingConfig); design §5 promises it explicitly.
	// Without this, the strategy silently never opens because cappedOrderQuantity
	// (strategy/momentum/strategy.go:208-213) clamps qty to MaxOrderSize then
	// skips when clamped qty < MinOrderSize.
	if payload.MinOrderSize > 0 && payload.MaxOrderSize > 0 && payload.MaxOrderSize < payload.MinOrderSize {
		return momentum.Config{}, nil, fmt.Errorf("config.max_order_size must be greater than or equal to min_order_size")
	}

	if payload.OrderBookID == "" {
		return momentum.Config{}, nil, fmt.Errorf("config.order_book_id is required")
	}
	orderBookID, err := uuid.Parse(payload.OrderBookID)
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("config.order_book_id: %w", err)
	}

	out, err := json.Marshal(map[string]any{
		"signal_source":     payload.SignalSource,
		"fast_window":       payload.FastWindow,
		"slow_window":       payload.SlowWindow,
		"atr_window":        payload.ATRWindow,
		"stop_loss_atr":     mustFloat64(stopLoss),
		"take_profit_atr":   mustFloat64(takeProfit),
		"min_order_size":    mustFloat64(minOrder),
		"max_order_size":    mustFloat64(maxOrder),
		"max_position_size": mustFloat64(maxPosition),
		"tenor":             payload.Tenor,
		"order_book_id":     payload.OrderBookID,
		"initial_balance":   mustFloat64(amount),
		"leverage":          mustFloat64(leverage),
	})
	if err != nil {
		return momentum.Config{}, nil, fmt.Errorf("marshal momentum config: %w", err)
	}

	return momentum.Config{
		SignalSource:    payload.SignalSource,
		FastWindow:      payload.FastWindow,
		SlowWindow:      payload.SlowWindow,
		ATRWindow:       payload.ATRWindow,
		StopLossATR:     stopLoss,
		TakeProfitATR:   takeProfit,
		MinOrderSize:    minOrder,
		MaxOrderSize:    maxOrder,
		MaxPositionSize: maxPosition,
		Tenor:           payload.Tenor,
		OrderBookID:     orderBookID,
		InitialBalance:  amount,
		Leverage:        leverage,
	}, out, nil
}

//nolint:funlen // strategy definition with 13 config fields
func newMomentumDefinition(pricesHandler *prices.Handler, log *slog.Logger) StrategyDefinition {
	defaults := momentum.DefaultConfig()
	return StrategyDefinition{
		Type:             momentum.StrategyType,
		Status:           strategyStatusAvailable,
		Description:      "Momentum / trend-following strategy. Fast/slow MA crossover with ATR-anchored stops and opposite-signal reversal.",
		SupportsRun:      true,
		SupportsBacktest: true,
		ConfigFields: []StrategyConfigField{
			{
				Name:        "signal_source",
				Type:        "string",
				Description: "Series the MA crossover runs on. One of price, ytm, spread.",
				Required:    false,
				Default:     defaults.SignalSource,
			},
			{
				Name:        "fast_window",
				Type:        "integer",
				Description: "Fast MA tick window. Must be at least 2.",
				Required:    false,
				Default:     defaults.FastWindow,
			},
			{
				Name:        "slow_window",
				Type:        "integer",
				Description: "Slow MA tick window. Must be greater than fast_window.",
				Required:    false,
				Default:     defaults.SlowWindow,
			},
			{
				Name:        "atr_window",
				Type:        "integer",
				Description: "Mean absolute price diff window used for stops. Must be at least 2.",
				Required:    false,
				Default:     defaults.ATRWindow,
			},
			{
				Name:        "stop_loss_atr",
				Type:        "number",
				Description: "Stop-loss distance in ATR units from entry. 0 disables.",
				Required:    false,
				Default:     mustFloat64(defaults.StopLossATR),
			},
			{
				Name:        "take_profit_atr",
				Type:        "number",
				Description: "Take-profit distance in ATR units from entry. 0 disables.",
				Required:    false,
				Default:     mustFloat64(defaults.TakeProfitATR),
			},
			{
				Name:        "min_order_size",
				Type:        "number",
				Description: "Minimum order size to open a position. 0 disables.",
				Required:    false,
				Default:     mustFloat64(defaults.MinOrderSize),
			},
			{
				Name:        "max_order_size",
				Type:        "number",
				Description: "Maximum order size to open a position. 0 disables.",
				Required:    false,
				Default:     mustFloat64(defaults.MaxOrderSize),
			},
			{
				Name:        "max_position_size",
				Type:        "number",
				Description: "Maximum fraction of capital per trade. Must be in (0,1].",
				Required:    false,
				Default:     mustFloat64(defaults.MaxPositionSize),
			},
			{
				Name:        "tenor",
				Type:        "string",
				Description: "FRED benchmark tenor. Required when signal_source is spread.",
				Required:    false,
				Default:     "",
			},
			{
				Name:        "order_book_id",
				Type:        "string",
				Description: "DORA order book UUID.",
				Required:    true,
				Default:     "",
			},
			{
				Name:        "initial_balance",
				Type:        "number",
				Description: "Starting capital. Live runs override with the user's USD balance.",
				Required:    false,
				Default:     mustFloat64(defaults.InitialBalance),
			},
			{
				Name:        "leverage",
				Type:        "number",
				Description: "Leverage multiplier on InitialBalance.",
				Required:    false,
				Default:     mustFloat64(defaults.Leverage),
			},
		},
		DecodeConfig: func(
			raw json.RawMessage,
			capability string,
			tradeWriter stats.BacktestTradeWriter,
		) (json.RawMessage, strategycore.Strategy, error) {
			forRun := capability == string(capabilityRun)
			cfg, normalised, err := decodeMomentumConfig(raw, forRun)
			if err != nil {
				return nil, nil, err
			}
			opts := []func(*momentum.Strategy){
				momentum.WithLogger(log),
			}
			if tradeWriter != nil {
				opts = append(opts, momentum.WithBacktestWriter(tradeWriter))
			}
			return normalised, momentum.New(cfg, pricesHandler, opts...), nil
		},
	}
}

//nolint:funlen // config decoding with validation
func decodeMeanReversionConfig(raw json.RawMessage, forRun bool) (meanreversion.Config, json.RawMessage, error) {
	var payload meanReversionConfigPayload
	if err := decodeRawConfig(raw, &payload); err != nil {
		return meanreversion.Config{}, nil, err
	}
	defaults := meanreversion.DefaultConfig()
	if payload.LookbackWindow == 0 {
		payload.LookbackWindow = defaults.LookbackWindow
	}
	if payload.LookbackWindow < 2 { //nolint:mnd
		return meanreversion.Config{}, nil, fmt.Errorf("config.lookback_window must be at least 2")
	}
	if payload.EntryZScore == 0 {
		payload.EntryZScore = mustFloat64(defaults.EntryZScore)
	}
	if payload.ExitZScore == 0 {
		payload.ExitZScore = mustFloat64(defaults.ExitZScore)
	}
	if payload.StopLossZScore == 0 {
		payload.StopLossZScore = mustFloat64(defaults.StopLossZScore)
	}
	if payload.MinStdDev == 0 {
		payload.MinStdDev = mustFloat64(defaults.MinStdDev)
	}
	if payload.MaxPositionSize == 0 {
		payload.MaxPositionSize = mustFloat64(defaults.MaxPositionSize)
	}
	if payload.EntryZScore <= 0 {
		return meanreversion.Config{}, nil, fmt.Errorf("config.entry_z_score must be greater than 0")
	}
	if payload.ExitZScore < 0 {
		return meanreversion.Config{}, nil, fmt.Errorf("config.exit_z_score must be non-negative")
	}
	if payload.StopLossZScore < 0 {
		return meanreversion.Config{}, nil, fmt.Errorf("config.stop_loss_z_score must be non-negative")
	}
	if payload.MinStdDev < 0 {
		return meanreversion.Config{}, nil, fmt.Errorf("config.min_std_dev must be non-negative")
	}
	if payload.MaxPositionSize <= 0 || payload.MaxPositionSize > 1 {
		return meanreversion.Config{}, nil, fmt.Errorf("config.max_position_size must be in (0,1]")
	}

	entry, err := decimal.NewFromFloat64(payload.EntryZScore)
	if err != nil {
		return meanreversion.Config{}, nil, fmt.Errorf("config.entry_z_score: %w", err)
	}
	exit, err := decimal.NewFromFloat64(payload.ExitZScore)
	if err != nil {
		return meanreversion.Config{}, nil, fmt.Errorf("config.exit_z_score: %w", err)
	}
	stopLoss, err := decimal.NewFromFloat64(payload.StopLossZScore)
	if err != nil {
		return meanreversion.Config{}, nil, fmt.Errorf("config.stop_loss_z_score: %w", err)
	}
	minStdDev, err := decimal.NewFromFloat64(payload.MinStdDev)
	if err != nil {
		return meanreversion.Config{}, nil, fmt.Errorf("config.min_std_dev: %w", err)
	}
	maxPositionSize, err := decimal.NewFromFloat64(payload.MaxPositionSize)
	if err != nil {
		return meanreversion.Config{}, nil, fmt.Errorf("config.max_position_size: %w", err)
	}

	amount := defaults.InitialBalance
	if payload.InitialBalance != nil {
		if *payload.InitialBalance < 0 {
			return meanreversion.Config{}, nil, fmt.Errorf("config.initial_balance must be non-negative")
		}
		if *payload.InitialBalance == 0 {
			if !forRun {
				return meanreversion.Config{}, nil, fmt.Errorf("config.initial_balance must be greater than 0 for backtests")
			}
			// For runs, initial_balance is obtained from DORA positions, so 0 is valid.
		} else {
			amount, err = decimal.NewFromFloat64(*payload.InitialBalance)
			if err != nil {
				return meanreversion.Config{}, nil, fmt.Errorf("config.initial_balance: %w", err)
			}
		}
	}

	leverage := defaults.Leverage
	if payload.Leverage != nil {
		if *payload.Leverage <= 0 {
			return meanreversion.Config{}, nil, fmt.Errorf("config.leverage must be greater than 0")
		}
		leverage, err = decimal.NewFromFloat64(*payload.Leverage)
		if err != nil {
			return meanreversion.Config{}, nil, fmt.Errorf("config.leverage: %w", err)
		}
	}

	var orderBookID uuid.UUID
	if payload.OrderBookID != "" {
		orderBookID, err = uuid.Parse(strings.TrimSpace(payload.OrderBookID))
		if err != nil {
			return meanreversion.Config{}, nil, fmt.Errorf("config.order_book_id: %w", err)
		}
	}

	payload.Tenor = strings.TrimSpace(payload.Tenor)

	normalised, err := json.Marshal(payload)
	if err != nil {
		return meanreversion.Config{}, nil, fmt.Errorf("marshal normalised config: %w", err)
	}

	return meanreversion.Config{
		LookbackWindow:  payload.LookbackWindow,
		EntryZScore:     entry,
		ExitZScore:      exit,
		StopLossZScore:  stopLoss,
		MinStdDev:       minStdDev,
		MaxPositionSize: maxPositionSize,
		OrderBookID:     orderBookID,
		Tenor:           payload.Tenor,
		InitialBalance:  amount,
		Leverage:        leverage,
	}, normalised, nil
}

func decodeTWAPConfig(raw json.RawMessage) (twap.Config, json.RawMessage, error) {
	var cfg twap.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return twap.Config{}, nil, fmt.Errorf("decode twap config: %w", err)
	}
	if err := cfg.Validate(time.Now().UTC()); err != nil {
		return twap.Config{}, nil, err
	}
	normalised, err := json.Marshal(cfg)
	if err != nil {
		return twap.Config{}, nil, fmt.Errorf("marshal normalised twap config: %w", err)
	}
	return cfg, normalised, nil
}

func decodeVWAPConfig(raw json.RawMessage) (vwap.Config, json.RawMessage, error) {
	var cfg vwap.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return vwap.Config{}, nil, fmt.Errorf("decode vwap config: %w", err)
	}
	if err := cfg.Validate(time.Now().UTC()); err != nil {
		return vwap.Config{}, nil, err
	}
	normalised, err := json.Marshal(cfg)
	if err != nil {
		return vwap.Config{}, nil, fmt.Errorf("marshal normalised vwap config: %w", err)
	}
	return cfg, normalised, nil
}

type copyTradingConfigPayload struct {
	FollowedTrader        string   `json:"followed_trader"`
	PercentageOfAvailable float64  `json:"percentage_of_available"`
	Leverage              float64  `json:"leverage"`
	MinOrderSize          int      `json:"min_order_size"`
	MaxOrderSize          int      `json:"max_order_size"`
	DisallowedBonds       []string `json:"disallowed_bonds"`
	// InitialBalance is optional. Pointer-to-float64 lets us distinguish
	// "not provided" (nil) from "explicitly 0" (treated as "use the
	// default"). The decoder falls back to the package-level default
	// when the field is absent or zero; only negative values are
	// rejected as an explicit caller error.
	InitialBalance *float64 `json:"initial_balance,omitempty"`
}

func decodeCopyTradingConfig(raw json.RawMessage) (copytrading.Config, json.RawMessage, error) {
	var payload copyTradingConfigPayload
	if err := decodeRawConfig(raw, &payload); err != nil {
		return copytrading.Config{}, nil, err
	}
	if payload.FollowedTrader == "" {
		return copytrading.Config{}, nil, fmt.Errorf("config.followed_trader is required")
	}
	followedTrader, err := uuid.Parse(strings.TrimSpace(payload.FollowedTrader))
	if err != nil {
		return copytrading.Config{}, nil, fmt.Errorf("config.followed_trader: %w", err)
	}
	if payload.PercentageOfAvailable <= 0 || payload.PercentageOfAvailable > 1 {
		return copytrading.Config{}, nil, fmt.Errorf("config.percentage_of_available must be in (0,1]")
	}
	if payload.Leverage <= 0 {
		return copytrading.Config{}, nil, fmt.Errorf("config.leverage must be greater than 0")
	}
	if payload.MinOrderSize < 0 {
		return copytrading.Config{}, nil, fmt.Errorf("config.min_order_size must be non-negative")
	}
	if payload.MaxOrderSize < payload.MinOrderSize {
		return copytrading.Config{}, nil, fmt.Errorf("config.max_order_size must be greater than or equal to min_order_size")
	}
	disallowedBonds := make([]uuid.UUID, 0, len(payload.DisallowedBonds))
	for i, bond := range payload.DisallowedBonds {
		id, err := uuid.Parse(bond)
		if err != nil {
			return copytrading.Config{}, nil, fmt.Errorf("config.disallowed_bonds[%d]: %w", i, err)
		}
		disallowedBonds = append(disallowedBonds, id)
	}

	poa, err := decimal.NewFromFloat64(payload.PercentageOfAvailable)
	if err != nil {
		return copytrading.Config{}, nil, fmt.Errorf("config.percentage_of_available: %w", err)
	}
	lev, err := decimal.NewFromFloat64(payload.Leverage)
	if err != nil {
		return copytrading.Config{}, nil, fmt.Errorf("config.leverage: %w", err)
	}

	// initial_balance: absent or zero both fall through to the
	// package default; only a negative value is an explicit error.
	initialBalance := decimal.Zero
	if payload.InitialBalance != nil {
		if *payload.InitialBalance < 0 {
			return copytrading.Config{}, nil, fmt.Errorf("config.initial_balance must be non-negative")
		}
		if *payload.InitialBalance > 0 {
			initialBalance, err = decimal.NewFromFloat64(*payload.InitialBalance)
			if err != nil {
				return copytrading.Config{}, nil, fmt.Errorf("config.initial_balance: %w", err)
			}
		}
	}

	normalised, err := json.Marshal(payload)
	if err != nil {
		return copytrading.Config{}, nil, fmt.Errorf("marshal normalised config: %w", err)
	}
	return copytrading.Config{
		FollowedTrader:        followedTrader,
		PercentageOfAvailable: poa,
		Leverage:              lev,
		MinOrderSize:          payload.MinOrderSize,
		MaxOrderSize:          payload.MaxOrderSize,
		DisallowedBonds:       disallowedBonds,
		InitialBalance:        initialBalance,
	}, normalised, nil
}

func decodeRawConfig(raw json.RawMessage, dst any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return fmt.Errorf("config is required")
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func newBacktestResult(result meanreversion.BacktestResult) (json.RawMessage, error) {
	// Per-trade and per-closed-trade rows are persisted to
	// strategy_backtest_trades and strategy_backtest_closed_trades by the
	// backtest engine via stats.BacktestTradeWriter. The summary-result
	// JSON only carries aggregate metrics.
	out := MeanReversionBacktestResult{
		TotalPnL:    result.TotalPnL.String(),
		WinCount:    result.WinCount,
		LossCount:   result.LossCount,
		MaxDrawdown: result.MaxDrawdown.String(),
		SharpeRatio: result.SharpeRatio.String(),
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal backtest result: %w", err)
	}
	return b, nil
}

func newCopyTradingBacktestResult(result copytrading.BacktestResult) (json.RawMessage, error) {
	// Per-trade and per-closed-trade rows are persisted to
	// strategy_backtest_trades and strategy_backtest_closed_trades by the
	// backtest engine via stats.BacktestTradeWriter. The summary-result
	// JSON only carries aggregate metrics.
	out := CopyTradingBacktestResult{
		TotalPnL:    result.TotalPnL.String(),
		WinCount:    result.WinCount,
		LossCount:   result.LossCount,
		MaxDrawdown: result.MaxDrawdown.String(),
		SharpeRatio: result.SharpeRatio.String(),
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal copytrading backtest result: %w", err)
	}
	return b, nil
}

func newBreakoutBacktestResult(result breakout.BacktestResult) (json.RawMessage, error) {
	// Per-trade and per-closed-trade rows are persisted to
	// strategy_backtest_trades and strategy_backtest_closed_trades by the
	// backtest engine via stats.BacktestTradeWriter. The summary-result
	// JSON only carries aggregate metrics.
	out := BreakoutBacktestResult{
		TotalPnL:    result.TotalPnL.String(),
		WinCount:    result.WinCount,
		LossCount:   result.LossCount,
		MaxDrawdown: result.MaxDrawdown.String(),
		SharpeRatio: result.SharpeRatio.String(),
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal breakout backtest result: %w", err)
	}
	return b, nil
}

func newMomentumBacktestResult(result momentum.BacktestResult) (json.RawMessage, error) {
	// Per-trade and per-closed-trade rows are persisted to
	// strategy_backtest_trades and strategy_backtest_closed_trades by the
	// backtest engine via stats.BacktestTradeWriter. The summary-result
	// JSON only carries aggregate metrics.
	out := MomentumBacktestResult{
		TotalPnL:    result.TotalPnL.String(),
		WinCount:    result.WinCount,
		LossCount:   result.LossCount,
		MaxDrawdown: result.MaxDrawdown.String(),
		SharpeRatio: result.SharpeRatio.String(),
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal momentum backtest result: %w", err)
	}
	return b, nil
}

func decodeJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, ErrorResponse{Error: message})
	if lw, ok := w.(*LoggingResponseWriter); ok {
		lw.WithError(message)
	}
}

func writeMethodNotAllowed(w http.ResponseWriter, allowed ...string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}

func mustFloat64(d decimal.Decimal) float64 {
	v, _ := d.Float64()
	return v
}
