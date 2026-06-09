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

	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/dora-network/bond-trading-strategies/prices"
	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/copytrading"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/stats"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

const (
	strategyStatusAvailable      = "available"
	strategyStatusNotImplemented = "not_implemented"
	defaultPaginationLimit       = 10
	maxPaginationLimit           = 50
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
	tradeStream        *streams.TradeStream
	notifier           notifications.Notifier
	encryptionKey      []byte // 32-byte AES-256 key for encrypting API keys at rest
	mux                *http.ServeMux
	authedMux          http.Handler
	mu                 sync.RWMutex
	backtests          map[uuid.UUID]*BacktestDetail
	runs               map[uuid.UUID]*RunDetail
	// runningStrategies maps a live run id to the strategy instance that
	// was started for it, so the stop-loss observer can query the
	// strategy's recorded trigger. Populated in createRun and
	// resumePersistedRun; cleared in stopRun and by the observer itself
	// when the run leaves the running state. Protected by mu.
	runningStrategies map[uuid.UUID]strategycore.Strategy
	// stopLossObservers cancels the per-run observer goroutine. Protected
	// by mu.
	stopLossObservers map[uuid.UUID]context.CancelFunc
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
type CopyTraderSummary struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
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
		h.strategies = defaultStrategies(h.prices, h.tradesHistoryStore, h.tradeStream, h.log)
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

func WithBacktestStore(store BacktestStore) func(*Handler) {
	return func(h *Handler) {
		h.backtestStore = store
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

// WithStopLossObserverInterval overrides the stop-loss observer poll
// interval. The default is 1s; tests use a shorter interval to avoid
// waiting.
func WithStopLossObserverInterval(d time.Duration) func(*Handler) {
	return func(h *Handler) {
		h.stopLossObserverInterval = d
	}
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

	supported := meanreversion.SupportedBenchmarkTenors()
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
// copy-trading runs. This is a placeholder that filters DORA users by name
// prefix until DORA exposes a dedicated "list available copy traders" endpoint.
// TODO(remove-placeholder): when DORA ships the new endpoint, swap the body of
// this handler to call it directly. The response shape must stay the same.
func (h *Handler) handleCopyTraders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}

	client := h.doraClient
	if client == nil {
		client = NewDORAClient()
	}
	users, err := client.ListBotUsers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("list copy traders: %v", err))
		return
	}

	items := make([]CopyTraderSummary, 0, len(users))
	for _, u := range users {
		items = append(items, CopyTraderSummary{
			ID:          u.ID,
			DisplayName: strings.TrimSpace(u.FirstName + " " + u.LastName),
		})
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
	info, _ := authFromContext(r.Context())
	if info.APIKey != "" {
		if withClient, ok := strat.(*meanreversion.Strategy); ok {
			withClientOpts := meanreversion.WithMarketAPIClient(meanreversion.NewDoraClientWithKey(info.APIKey))
			withClientOpts(withClient)
		}
	}

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
	start := (page - 1) * limit
	if start > total {
		start = total
	}
	end := start + limit
	if end > total {
		end = total
	}

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
			limit = val
			if limit > maxPaginationLimit {
				limit = maxPaginationLimit
			}
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
	info, _ := authFromContext(r.Context())
	if info.APIKey != "" {
		switch withClient := strat.(type) {
		case *meanreversion.Strategy:
			meanreversion.WithMarketAPIClient(meanreversion.NewDoraClientWithKey(info.APIKey))(withClient)
		case *copytrading.Strategy:
			copytrading.WithMarketAPIClient(copytrading.NewDoraClientWithKey(info.APIKey))(withClient)
		}
	}

	var encryptedAPIKey []byte
	if info.APIKey != "" && len(h.encryptionKey) > 0 {
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

	h.startStopLossObserver(detail, strat)

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
	h.mu.Unlock()

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
		h.log.Info("resuming run",
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
		switch withClient := strat.(type) {
		case *meanreversion.Strategy:
			meanreversion.WithMarketAPIClient(meanreversion.NewDoraClientWithKey(string(apiKeyDecrypted)))(withClient)
		case *copytrading.Strategy:
			copytrading.WithMarketAPIClient(copytrading.NewDoraClientWithKey(string(apiKeyDecrypted)))(withClient)
		}
	}

	if _, err := h.startRun(ctx, detail, strat); err != nil {
		return err
	}
	h.startStopLossObserver(detail, strat)
	return nil
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

// runIsActive reports whether the run is still tracked in h.runs with
// status "running". The observer uses this to exit when the run was
// stopped or completed by some other path.
func (h *Handler) runIsActive(id uuid.UUID) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	detail, ok := h.runs[id]
	if !ok {
		return false
	}
	return detail.Status == "running"
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

func defaultStrategies(
	pricesHandler *prices.Handler,
	tradesHistoryStore *copytrading.PGTradesHistoryStore,
	tradeStream *streams.TradeStream,
	log *slog.Logger,
) map[string]StrategyDefinition {
	defs := []StrategyDefinition{
		newMeanReversionDefinition(pricesHandler, log),
		newCopyTradingDefinition(tradesHistoryStore, tradeStream),
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
}

func writeMethodNotAllowed(w http.ResponseWriter, allowed ...string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}

func mustFloat64(d decimal.Decimal) float64 {
	v, _ := d.Float64()
	return v
}
