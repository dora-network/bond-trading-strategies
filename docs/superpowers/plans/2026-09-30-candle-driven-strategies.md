# Candle-Driven Signal Strategies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate mean-reversion, momentum, and breakout from tick-driven indicators to closed-bar (candle) indicators with per-strategy resolution, plus tick/trade auxiliary gates (intrabar exits, MR trade-imbalance, momentum volume).

**Architecture:** One DORA candle WS connection per (order book, resolution) owned by a refcounted registry in strategy-server; a `BarCloser` adapter emits only closed bars; strategies' `Update` consumes `types.Bar`. Ticks stay for intrabar stop checks and execution pricing; the trade tape stays for OBV (breakout) and the new MR imbalance gate. Backtests replay bucketed `candles_history` (SQL folding copied from `internal/agent/store/history_store.go:276-370`).

**Tech Stack:** Go 1.x, pgx/v5 + pgxpool, govalues/decimal, testify, counterfeiter fakes, slog.

**Spec:** `docs/superpowers/specs/2026-09-30-candle-driven-strategies-design.md`

**Repo methodology (overrides per-task commit steps):** after each task, `git add` the touched files and run `pre-commit run --files <files>`. If checks pass, continue to the next task; do **not** `git commit` — commits happen only at user review gates (GPG + hooks never bypassed).

**Shared facts (all tasks):**
- Resolution enum: `1m, 5m, 15m, 1h, 4h, 1d`.
- Default resolutions: mean-reversion `1h`, momentum `15m`, breakout `5m`.
- `window.Rolling` (`strategy/window/rolling.go`) keeps count semantics — now counting bars.
- All `time.Time` bound to SQL must be UTC (repo TIMESTAMP convention).
- Decimal ops return `(Decimal, error)` — propagate errors; never panic.

---

### Task 1: `candles.Config.Resolution`

**Files:**
- Modify: `candles/handler.go` (Config ~44-56, buildURL ~241-261)
- Test: `candles/handler_test.go`

- [ ] **Step 1: Write failing tests**

Add to `candles/handler_test.go` (same package `candles`):

```go
func TestBuildURLUsesConfiguredResolution(t *testing.T) {
	h := New(Config{BaseURL: "wss://x", APIKey: "k", Resolution: "5m"}, nil)
	raw, err := h.buildURL("ob-1", nil)
	require.NoError(t, err)
	u, err := url.Parse(raw)
	require.NoError(t, err)
	require.Equal(t, "5m", u.Query().Get("resolution"))
}

func TestNewRejectsInvalidResolution(t *testing.T) {
	require.Error(t, validateResolution("7h"))
	require.NoError(t, validateResolution("1m"))
	require.Error(t, validateResolution("7d"), "7d is SQL-bucketing only, not a Dora stream resolution")
}
```

- [ ] **Step 2: Run — expect compile failure**

Run: `go test ./candles/ -run TestBuildURL -v` → FAIL (`Resolution` field / `validateResolution` undefined).

- [ ] **Step 3: Implement**

In `candles/handler.go`, extend `Config` (after `Since`):

```go
// Resolution is the candle resolution to subscribe to. One of
// 1m, 5m, 15m, 1h, 4h, 1d. Empty defaults to 1m (price-daemon's
// ingestion resolution).
Resolution string
```

Add:

```go
// ValidResolutions is the closed set Dora serves on the candle stream.
var ValidResolutions = []string{"1m", "5m", "15m", "1h", "4h", "1d"}

func validateResolution(res string) error {
	for _, r := range ValidResolutions {
		if r == res {
			return nil
		}
	}
	return fmt.Errorf("invalid resolution %q (allowed: %s)", res, strings.Join(ValidResolutions, ", "))
}
```

In `New`, before returning: `if cfg.Resolution == "" { cfg.Resolution = "1m" } else if err := validateResolution(cfg.Resolution); err != nil { return nil, err }` — change `New`'s signature to `(*Handler, error)`. Fix the two callers (`cmd/price-daemon/main.go:205`, tests) to handle the error.

In `buildURL` replace the hardcoded line (~254):

```go
q.Set("resolution", h.cfg.Resolution)
```

- [ ] **Step 4: Run tests**

Run: `go test ./candles/... -v` → PASS.

- [ ] **Step 5: Stage + pre-commit**

```bash
git add candles/handler.go candles/handler_test.go cmd/price-daemon/main.go
pre-commit run --files candles/handler.go candles/handler_test.go cmd/price-daemon/main.go
```

---

### Task 2: `types.Bar` + true range

**Files:**
- Modify: `strategy/types/types.go`
- Test: `strategy/types/types_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestBarTrueRange(t *testing.T) {
	bar := Bar{
		High: decimal.MustNew(1015, 2), Low: decimal.MustNew(9925, 2),
		Close: decimal.MustNew(1000, 2),
	}
	// No prev close: range = H - L = 0.25
	tr, err := bar.TrueRange(decimal.Zero)
	require.NoError(t, err)
	require.True(t, tr.Equal(decimal.MustNew(25, 1)))

	// prevClose 99.50: max(0.25, |101.50-99.50|=2.00, |99.25-99.50|=0.25) = 2.00
	tr, err = bar.TrueRange(decimal.MustNew(9950, 2))
	require.NoError(t, err)
	require.True(t, tr.Equal(decimal.Two))
}
```

- [ ] **Step 2: Run** `go test ./strategy/types/ -run TestBarTrueRange -v` → FAIL (`Bar` undefined).

- [ ] **Step 3: Implement** — append to `strategy/types/types.go`:

```go
// Bar is a closed candlestick observation consumed by the signal
// strategies. Time is the bar's start timestamp (UTC). BenchmarkYield is
// resolved by the strategy (spread modes) at evaluation time, not by the feed.
type Bar struct {
	Time           time.Time
	Open           decimal.Decimal
	High           decimal.Decimal
	Low            decimal.Decimal
	Close          decimal.Decimal
	Volume         decimal.Decimal
	OpenYTM        decimal.Decimal
	HighYTM        decimal.Decimal
	LowYTM         decimal.Decimal
	CloseYTM       decimal.Decimal
	BenchmarkYield decimal.Decimal
}

// TrueRange is max(H-L, |H-prevClose|, |L-prevClose|). A zero prevClose
// (first bar) yields H-L.
func (b Bar) TrueRange(prevClose decimal.Decimal) (decimal.Decimal, error) {
	hl, err := b.High.Sub(b.Low)
	if err != nil {
		return decimal.Zero, fmt.Errorf("true range H-L: %w", err)
	}
	if prevClose.IsZero() {
		return hl, nil
	}
	hp, err := b.High.Sub(prevClose)
	if err != nil {
		return decimal.Zero, fmt.Errorf("true range H-prev: %w", err)
	}
	lp, err := b.Low.Sub(prevClose)
	if err != nil {
		return decimal.Zero, fmt.Errorf("true range L-prev: %w", err)
	}
	m := hp.Abs()
	if lp.Abs().Cmp(m) > 0 {
		m = lp.Abs()
	}
	if m.Cmp(hl) > 0 {
		return m, nil
	}
	return hl, nil
}
```

- [ ] **Step 4: Run** `go test ./strategy/types/... -v` → PASS.

- [ ] **Step 5:** `git add strategy/types/types.go strategy/types/types_test.go && pre-commit run --files strategy/types/types.go strategy/types/types_test.go`

---

### Task 3: `CandleFeed`, `BarCloser`, resolution duration

**Files:**
- Create: `strategy/barfeed.go` (package `strategy`)
- Create: `strategy/barfeed_test.go`
- Regenerate fakes for the new interface (counterfeiter)

- [ ] **Step 1: Write failing tests**

`strategy/barfeed_test.go` (package `strategy`):

```go
func TestBarCloserEmitsOnlyClosedBars(t *testing.T) {
	in := make(chan []candles.StreamCandlesEntry, 4)
	closer := StartBarCloser(in)
	defer closer.Stop()

	mk := func(ts time.Time) []candles.StreamCandlesEntry {
		return []candles.StreamCandlesEntry{{Val: candles.Candle{
			OrderBookID: "ob", StartTimestamp: ts,
			Open: decimal.One, High: decimal.One, Low: decimal.One, Close: decimal.One,
		}}}
	}
	t0 := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	in <- mk(t0)                       // first bar: nothing emitted
	in <- mk(t0)                       // same bar updated: still nothing
	in <- mk(t1)                       // new bar: t0 is final now

	select {
	case bar := <-closer.Bars():
		require.True(t, bar.Time.Equal(t0))
	case <-time.After(time.Second):
		t.Fatal("expected closed bar for t0")
	}
	select {
	case <-closer.Bars():
		t.Fatal("t1 must not be emitted before a newer bar appears")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestResolutionDuration(t *testing.T) {
	require.Equal(t, time.Hour, ResolutionDuration("1h"))
	require.Equal(t, 15*time.Minute, ResolutionDuration("15m"))
	require.Equal(t, time.Duration(0), ResolutionDuration("nope"))
}
```

- [ ] **Step 2: Run** `go test ./strategy/ -run 'TestBarCloser|TestResolutionDuration' -v` → FAIL (undefined).

- [ ] **Step 3: Implement** `strategy/barfeed.go`:

```go
package strategy

// CandleFeed supplies closed bars for one (order book, resolution).
// Live runs use the CandleRegistry; backtests and tests use fakes.
//
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate . CandleFeed
type CandleFeed interface {
	// SubscribeBars returns a channel of closed bars for the book.
	// since requests the stream bootstrap to include history back to
	// that time (warm start). The returned cancel func unsubscribes.
	SubscribeBars(ctx context.Context, orderBookID uuid.UUID, resolution string, since time.Time) (<-chan types.Bar, func(), error)
}

// StartBarCloser wraps a raw candle-update stream and emits bar N-1
// the first time bar N's start_timestamp appears: a bar is final only
// when superseded. Bars channel closes when the input closes or Stop
// is called.
type BarCloser struct {
	in   <-chan []candles.StreamCandlesEntry
	out  chan types.Bar
	done chan struct{}
}

func StartBarCloser(in <-chan []candles.StreamCandlesEntry) *BarCloser {
	bc := &BarCloser{in: in, out: make(chan types.Bar, 16), done: make(chan struct{})}
	go bc.loop()
	return bc
}

// Bars is the closed-bar output channel.
func (bc *BarCloser) Bars() <-chan types.Bar { return bc.out }

// Stop terminates the loop and closes Bars.
func (bc *BarCloser) Stop() { close(bc.done) }

func (bc *BarCloser) loop() {
	defer close(bc.out)
	var pending *candles.Candle
	for {
		select {
		case <-bc.done:
			return
		case batch, ok := <-bc.in:
			if !ok {
				return
			}
			for i := range batch {
				e := batch[i]
				if pending != nil && e.Val.StartTimestamp.After(pending.StartTimestamp) {
					select {
					case bc.out <- BarFromCandle(*pending):
					case <-bc.done:
						return
					}
				}
				c := e.Val
				pending = &c
			}
		}
	}
}

// BarFromCandle projects a persisted/streamed candle into a types.Bar.
// BenchmarkYield is left zero; spread-mode strategies fill it per bar.
func BarFromCandle(c candles.Candle) types.Bar {
	return types.Bar{
		Time: c.StartTimestamp.UTC(), Open: c.Open, High: c.High,
		Low: c.Low, Close: c.Close, Volume: c.Volume,
		OpenYTM: c.OpenYTM, HighYTM: c.HighYTM,
		LowYTM: c.LowYTM, CloseYTM: c.CloseYTM,
	}
}

// ResolutionDuration maps a resolution string to a time.Duration.
// Unknown/empty resolutions return 0.
func ResolutionDuration(res string) time.Duration {
	switch res {
	case "1m":
		return time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "1h":
		return time.Hour
	case "4h":
		return 4 * time.Hour
	case "1d":
		return 24 * time.Hour
	case "7d":
		return 7 * 24 * time.Hour
	default:
		return 0
	}
}
```

(Imports: `context`, `time`, `github.com/google/uuid`, `<module>/strategy/types`, `<module>/candles`.)
- [ ] **Step 4: Run** `go test ./strategy/ -v` → PASS. Then `go generate ./strategy/` to create `strategyfakes/fake_candle_feed.go`.

- [ ] **Step 5:** `git add strategy/barfeed.go strategy/barfeed_test.go strategy/strategyfakes/fake_candle_feed.go && pre-commit run --files strategy/barfeed.go strategy/barfeed_test.go strategy/strategyfakes/fake_candle_feed.go`

---

### Task 4: bucketed loader + coverage query

**Files:**
- Modify: `candles/store.go`
- Test: `candles/store_test.go`

- [ ] **Step 1: Write failing test** (pgxmock, existing patterns in `candles/store_test.go`):

```go
func TestLoadCandlesBucketedPassThrough1m(t *testing.T) {
	mock := pgxmock.NewPool(t) // match existing helper style in the file
	s := NewPGStore(mock)
	since := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	until := since.Add(time.Hour)
	mock.ExpectQuery(`FROM candles_history`).
		WithArgs("ob", since, until, time.Time{}.UTC(), maxCandleRows).
		WillReturnRows(pgxmock.NewRows([]string{
			"order_book_id", "start_timestamp", "open", "high", "low", "close", "volume",
			"open_ytm", "high_ytm", "low_ytm", "close_ytm"}).
			AddRow("ob", since, 1, 1, 1, 1, 0, 0.05, 0.05, 0.05, 0.05))
	bars, err := s.LoadCandlesBucketed(t.Context(), "ob", "1m", since, until)
	require.NoError(t, err)
	require.Len(t, bars, 1)
	require.True(t, bars[0].StartTimestamp.Equal(since))
}

func TestLoadCandlesBucketedRejectsUnknownResolution(t *testing.T) {
	s := NewPGStore(nil)
	_, err := s.LoadCandlesBucketed(t.Context(), "ob", "7h", time.Time{}, time.Time{})
	require.ErrorContains(t, err, "unknown resolution")
}

func TestCandleRange(t *testing.T) {
	mock := pgxmock.NewPool(t)
	s := NewPGStore(mock)
	lo := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	hi := lo.Add(24 * time.Hour)
	mock.ExpectQuery(`SELECT MIN\(start_timestamp\), MAX\(start_timestamp\)`).
		WithArgs("ob").
		WillReturnRows(pgxmock.NewRows([]string{"min", "max"}).AddRow(lo, hi))
	gotLo, gotHi, err := s.CandleRange(t.Context(), "ob")
	require.NoError(t, err)
	require.True(t, gotLo.Equal(lo))
	require.True(t, gotHi.Equal(hi))
}
```

- [ ] **Step 2: Run** `go test ./candles/ -run 'Bucketed|CandleRange' -v` → FAIL.

- [ ] **Step 3: Implement** in `candles/store.go`:

Copy `fetchCandlesSQL`, `resolutionSeconds`, `resolutionToSeconds` verbatim from `internal/agent/store/history_store.go:288-376` (same table, same columns — the agent store stays untouched; note the duplication with a `// ponytail: duplicated from internal/agent/store/history_store.go` comment). Then:

```go
const maxCandleRows = 1500

// ErrNoCandleCoverage names the book's available candle range.
type ErrNoCandleCoverage struct{ OrderBookID string }

func (e *ErrNoCandleCoverage) Error() string { return "no candle coverage" }

// LoadCandlesBucketed returns candles for the book folded to the requested
// resolution, oldest-first, via keyset pagination. "1m" passes through.
func (s *PGStore) LoadCandlesBucketed(ctx context.Context, orderBookID, resolution string, since, until time.Time) ([]Candle, error) {
	if resolutionToSeconds(resolution) == 0 {
		return nil, fmt.Errorf("candles: unknown resolution %q (allowed: 1m, 5m, 15m, 1h, 4h, 1d, 7d)", resolution)
	}
	var out []Candle
	cursor := time.Time{}
	for {
		rows, err := s.queryCandlePage(ctx, orderBookID, resolution, since, until, cursor, maxCandleRows)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return out, nil
		}
		out = append(out, rows...)
		if len(rows) < maxCandleRows {
			return out, nil
		}
		cursor = rows[len(rows)-1].StartTimestamp
	}
}

// CandleRange returns the min/max start_timestamp persisted for the book.
// Either may be nil when no rows exist.
func (s *PGStore) CandleRange(ctx context.Context, orderBookID string) (*time.Time, *time.Time, error) {
	var lo, hi *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT MIN(start_timestamp), MAX(start_timestamp)
		 FROM candles_history WHERE order_book_id = $1`, orderBookID).
		Scan(&lo, &hi)
	if err != nil {
		return nil, nil, fmt.Errorf("candle range: %w", err)
	}
	if lo != nil {
		*lo = lo.UTC()
	}
	if hi != nil {
		*hi = hi.UTC()
	}
	return lo, hi, nil
}
```

`queryCandlePage` (same file):

```go
// queryCandlePage runs one keyset-paginated page of the bucketed query.
func (s *PGStore) queryCandlePage(ctx context.Context, orderBookID, resolution string,
	since, until, cursor time.Time, limit int,
) ([]Candle, error) {
	rows, err := s.pool.Query(ctx, fetchCandlesSQL(resolution),
		orderBookID, since.UTC(), until.UTC(), cursor.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("bucketed candles: %w", err)
	}
	defer rows.Close()
	var out []Candle
	for rows.Next() {
		var c Candle
		if err := rows.Scan(&c.OrderBookID, &c.StartTimestamp, &c.Open, &c.High,
			&c.Low, &c.Close, &c.Volume,
			&c.OpenYTM, &c.HighYTM, &c.LowYTM, &c.CloseYTM); err != nil {
			return nil, fmt.Errorf("scan candle: %w", err)
		}
		c.StartTimestamp = c.StartTimestamp.UTC()
		out = append(out, c)
	}
	return out, rows.Err()
}
```

(Match the receiver field name to `PGStore`'s actual pool field — see the existing `LoadCandles` scan loop at `candles/store.go:43-107`.)

- [ ] **Step 4: Run** `go test ./candles/... -v` → PASS.

- [ ] **Step 5:** `git add candles/store.go candles/store_test.go && pre-commit run --files candles/store.go candles/store_test.go`

---

### Task 5: candle registry + strategy-server wiring

**Files:**
- Create: `strategy/candle_registry.go` (package `strategy`)
- Create: `strategy/candle_registry_test.go`
- Modify: `cmd/strategy-server/main.go`

- [ ] **Step 1: Write failing test**

```go
func TestCandleRegistrySharesHandlersPerKey(t *testing.T) {
	r := NewCandleRegistry(CandleRegistryConfig{WSBaseURL: "wss://x", APIKey: "k"})
	book := uuid.New()
	ch1, cancel1, err := r.SubscribeBars(t.Context(), book, "5m", time.Time{})
	require.NoError(t, err)
	require.NotNil(t, ch1)
	require.Equal(t, 1, r.HandlerCount())

	ch2, cancel2, err := r.SubscribeBars(t.Context(), book, "5m", time.Time{})
	require.NoError(t, err)
	require.Equal(t, 1, r.HandlerCount(), "same (book,res) must share the handler")

	cancel1()
	require.Equal(t, 1, r.HandlerCount(), "still one subscriber left")
	cancel2()
	require.Equal(t, 0, r.HandlerCount(), "last unsubscribe stops the handler")
}
```

- [ ] **Step 2: Run** `go test ./strategy/ -run TestCandleRegistry -v` → FAIL.

- [ ] **Step 3: Implement** `strategy/candle_registry.go`:

```go
package strategy

// CandleRegistryConfig wires the registry to DORA and (optionally) the
// candle store used only for the since-resume cursor. The registry never
// persists candles — price-daemon owns ingestion; strategy bars are
// ephemeral.
type CandleRegistryConfig struct {
	WSBaseURL string
	APIKey    string
	Store     candles.CandleStore // optional; nil disables resume cursors
}

type registryEntry struct {
	handler *candles.Handler
	cancel  context.CancelFunc
	refs    int
}

// CandleRegistry owns one candles.Handler per (order book, resolution),
// started on first subscriber and stopped on last unsubscribe. It
// implements CandleFeed.
type CandleRegistry struct {
	cfg     CandleRegistryConfig
	mu      sync.Mutex
	entries map[string]*registryEntry // key: book|resolution
}

func NewCandleRegistry(cfg CandleRegistryConfig) *CandleRegistry {
	return &CandleRegistry{cfg: cfg, entries: map[string]*registryEntry{}}
}

// HandlerCount reports live handlers (tests/observability).
func (r *CandleRegistry) HandlerCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

func (r *CandleRegistry) SubscribeBars(ctx context.Context, book uuid.UUID, resolution string, since time.Time) (<-chan types.Bar, func(), error) {
	key := book.String() + "|" + resolution
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	if !ok {
		h, err := candles.New(candles.Config{
			BaseURL: r.cfg.WSBaseURL, APIKey: r.cfg.APIKey,
			OrderBookIDs: []string{book.String()},
			Resolution:   resolution, Since: since,
		}, r.cfg.Store)
		if err != nil {
			return nil, nil, err
		}
		streamCtx, cancel := context.WithCancel(ctx)
		go func() { _ = (&streams.Daemon{ReconnectDelay: 5 * time.Second}).Run(streamCtx, h.Stream) }()
		e = &registryEntry{handler: h, cancel: cancel}
		r.entries[key] = e
	}
	subID := uuid.New()
	raw, err := e.handler.Subscribe(subID)
	if err != nil {
		return nil, nil, err
	}
	e.refs++
	closer := StartBarCloser(raw)
	unsub := func() {
		_ = e.handler.Unsubscribe(subID)
		closer.Stop()
		r.mu.Lock()
		defer r.mu.Unlock()
		e.refs--
		if e.refs == 0 {
			e.cancel()
			delete(r.entries, key)
		}
	}
	return closer.Bars(), unsub, nil
}
```

(Note: `candles.New` returns an error as of Task 1; `streams.Daemon{ReconnectDelay: ...}` matches the field used at `cmd/price-daemon/main.go:166-168`. `candles.Handler.Subscribe` takes a caller-generated request UUID — hence `subID := uuid.New()`.)

- [ ] **Step 4: Run** `go test ./strategy/ -v` → PASS.

- [ ] **Step 5: Wire in `cmd/strategy-server/main.go`**

After the trade stream setup (~line 162):

```go
candleReg := strategy.NewCandleRegistry(strategy.CandleRegistryConfig{
	WSBaseURL: *wsURL, APIKey: *apiKey,
	Store: candles.NewPGStore(pool), // nil-safe if pool == nil
})
```

and pass it into the strategy construction options added in Tasks 7-12 (`strategyhttp.WithCandleFeed(candleReg)` — added to `strategy/http/handler.go` alongside `WithPricesHandler`).

`git add` the three files + `pre-commit run --files` them.

---

### Task 6: HTTP payloads — `resolution` + gate fields

**Files:**
- Modify: `strategy/http/handler.go` (payload structs ~2842-2889, decoders ~3048+, `ConfigFields` ~2905-3044, `WithCandleFeed` option near `WithPricesHandler:241`)

- [ ] **Step 1: Write failing tests** — extend the existing handler config-decode tests (`handler_test.go`): decode `{"resolution":"5m", ...}` for each strategy; assert default applied when absent (MR `1h`, momentum `15m`, breakout `5m`); assert `400` on `"7h"`; assert MR `imbalance_window`/`imbalance_threshold` and momentum `volume_avg_window`/`volume_ratio_threshold` decode with defaults (100/0 and 20/1.0).

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

Add to `WithPricesHandler`-adjacent wiring:

```go
// WithCandleFeed injects the bar feed used by signal strategies' live runs.
func WithCandleFeed(feed strategy.CandleFeed) func(*Handler) {
	return func(h *Handler) { h.candleFeed = feed }
}
```

Payload structs: add `Resolution string \`json:"resolution"\`` to each of the three; MR adds `ImbalanceWindow int \`json:"imbalance_window"\``, `ImbalanceThreshold decimal.Decimal \`json:"imbalance_threshold"\``; momentum adds `VolumeAvgWindow int \`json:"volume_avg_window"\``, `VolumeRatioThreshold decimal.Decimal \`json:"volume_ratio_threshold"\``.

Decoders: after applying defaults, `if cfg.Resolution == "" { cfg.Resolution = "<default>" } else if err := candles.ValidateResolution(cfg.Resolution); err != nil { return 400 }` (export `validateResolution` as `ValidateResolution` in Task 1).

`ConfigFields` metadata: append the new fields per strategy with type/description/default, mirroring existing entries (the MCP surface reads this).

- [ ] **Step 4: Run** `go test ./strategy/http/... -v` → PASS.

- [ ] **Step 5:** stage + pre-commit the file(s).

---

### Task 7: mean-reversion — bar-driven core

**Files:**
- Modify: `strategy/meanreversion/strategy.go` (Config ~27-82, Update ~913-1007, run loop ~674-842), `strategy/meanreversion/types.go`
- Test: `strategy/meanreversion/strategy_test.go`, `strategy/meanreversion/run_loop_test.go`

- [ ] **Step 1: Rewrite tests first**

Adapt existing `Update` tests: fixtures build `types.Bar` (Close/CloseYTM carry the values ticks used to). New defaults: `LookbackWindow: 24`, `Resolution: "1h"` in `DefaultConfig`. Add:

```go
func TestUpdateOnBarsIgnoresIntraBarNoise(t *testing.T) {
	// two bars with identical closes but different High/Low must yield
	// identical z-scores: only Close/CloseYTM feed the spread.
}

func TestDefaultConfigResolution(t *testing.T) {
	require.Equal(t, "1h", DefaultConfig().Resolution)
	require.Equal(t, 24, DefaultConfig().LookbackWindow)
}
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

Config: add `Resolution string`; `DefaultConfig` sets `Resolution: "1h", LookbackWindow: 24`.

`Update` signature and body: replace `obs types.YieldObservation` with `bar types.Bar`; `s.lastPrice = bar.Close`; spread:

```go
spread, err := bar.CloseYTM.Sub(bar.BenchmarkYield)
if err != nil {
	return Decision{}, fmt.Errorf("spread: %w", err)
}
```

z-score math (window mean/σ before add) unchanged. Decision fields: `YTM: bar.CloseYTM, BenchmarkYield: bar.BenchmarkYield, Price: bar.Close, time: bar.Time`.

Strategy struct: add `candleFeed strategy.CandleFeed`, option:

```go
func WithCandleFeed(f strategy.CandleFeed) func(*Strategy) {
	return func(s *Strategy) { s.candleFeed = f }
}
```

Run loop (`run`): replace `prices` subscription with bars:

```go
warmup := time.Duration(s.cfg.LookbackWindow+1) * strategy.ResolutionDuration(s.cfg.Resolution)
bars, cancelBars, err := s.candleFeed.SubscribeBars(ctx, s.cfg.OrderBookID, s.cfg.Resolution, time.Now().UTC().Add(-warmup))
if err != nil {
	return fmt.Errorf("subscribe bars: %w", err)
}
defer cancelBars()
```

Bar case (entry path — same decision/execute logic as today's tick path, minus `px.YTM == nil` guard, plus benchmark):

```go
case bar := <-bars:
	bar.BenchmarkYield = s.getBenchmarkYield(ctx, bar.Time.Add(strategy.ResolutionDuration(s.cfg.Resolution)))
	// ... existing windowReadyBeforeUpdate / Update / paused / ShouldExit(z-reversion) /
	// executeDecision sequence from the old price case, operating on the bar decision.
```

Tick case (exits + execution pricing only — signals never):

```go
case pxs := <-prices:
	for _, px := range pxs {
		if px.AssetID != assetID || px.YTM == nil {
			continue
		}
		bench := s.getBenchmarkYield(ctx, px.Time)
		s.mu.Lock()
		s.lastPrice = px.Price
		open := s.openSignal
		s.mu.Unlock()
		if open == types.SignalHold {
			continue
		}
		if z, ok := s.intrabarZ(*px.YTM, bench); ok {
			if shouldExit, _ := s.ShouldExit(open, z); shouldExit {
				// closePosition(ctx, assetID) — existing helper
			}
		}
	}
```

`intrabarZ` (new, RLock on `s.mu`, uses `s.window` stats without mutating):

```go
func (s *Strategy) intrabarZ(ytm, bench decimal.Decimal) (decimal.Decimal, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.window.Ready() {
		return decimal.Zero, false
	}
	stdDev, err := s.window.StdDev()
	if err != nil || stdDev.Cmp(s.cfg.MinStdDev) < 0 {
		return decimal.Zero, false
	}
	spread, err := ytm.Sub(bench)
	if err != nil {
		return decimal.Zero, false
	}
	num, err := spread.Sub(s.window.Mean())
	if err != nil {
		return decimal.Zero, false
	}
	z, err := num.Quo(stdDev)
	if err != nil {
		return decimal.Zero, false
	}
	return z, true
}
```

Delete the old tick-driven signal path (window updates from ticks, entry evaluation from ticks). Keep the `prices` subscription — it now only feeds the tick case above.

- [ ] **Step 4: Run** `go test ./strategy/meanreversion/... -race -v` → PASS.

- [ ] **Step 5:** stage + pre-commit.

---

### Task 8: mean-reversion — prefill removal, backtest on candles, coverage guard

**Files:**
- Modify: `strategy/meanreversion/historical_data.go` (getObservations 38-87, prefillWindow 254-316), `strategy/meanreversion/backtest.go`, `strategy/meanreversion/strategy.go` (Backtest 244-267)
- Test: `strategy/meanreversion/historical_data_test.go`, `strategy/meanreversion/backtest_test.go`

- [ ] **Step 1: Failing tests** — `getObservations` returns `[]types.Bar` from a `candles.CandleStore`-shaped fake (bucketed), merged with FRED benchmark; prefill is gone (`Run` warm-starts via stream `since` from Task 7); backtest returns `ErrNoCandleCoverage`-shaped error when `CandleRange` min/max don't cover `[start-warmup, end]`.

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

- Replace `historicalPriceStore` with:

```go
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate . candleHistoryStore
type candleHistoryStore interface {
	LoadCandlesBucketed(ctx context.Context, orderBookID, resolution string, since, until time.Time) ([]candles.Candle, error)
	CandleRange(ctx context.Context, orderBookID string) (*time.Time, *time.Time, error)
}
```

- `getObservations(ctx, start, end)` → `getBars`: coverage check first (`CandleRange`); if `min == nil || max == nil || min.After(start) || max.Before(end.Add(-res))` return `&candles.ErrNoCandleCoverage{...}` enriched with the available range string; then `LoadCandlesBucketed`, convert via `strategy.BarFromCandle`, fill `BenchmarkYield` per bar from the FRED cache (reuse the existing merge logic at `historical_data.go:211-252`).
- Delete `prefillWindow` (254-316) and its `Run` call (strategy.go:704) — the stream bootstrap covers warm start.
- Backtester (`backtest.go`): replay `[]types.Bar` oldest-first through the bar-Update from Task 7; after each bar, if a position is open evaluate `ShouldExit` at the adverse extreme:

```go
// adverse spread extreme: for long (bought cheap when z high), the hurt is
// spread moving higher still; for short, lower.
var adverse decimal.Decimal
if openSignal == types.SignalBuy {
	adverse, err = bar.HighYTM.Sub(bar.BenchmarkYield)
} else {
	adverse, err = bar.LowYTM.Sub(bar.BenchmarkYield)
}
z := zFrom(adverse, windowStats)
```

- Handler mapping: `strategy/http/handler.go` backtest endpoint returns 400 with the coverage message when `errors.As(&ErrNoCandleCoverage)`.

- [ ] **Step 4: Run** `go test ./strategy/meanreversion/... -v` → PASS.

- [ ] **Step 5:** stage + pre-commit.

---

### Task 9: mean-reversion — imbalance gate

**Files:**
- Create: `strategy/meanreversion/imbalance.go`
- Modify: `strategy/meanreversion/strategy.go` (Config, Strategy struct, Run), `strategy/meanreversion/backtest.go`
- Test: `strategy/meanreversion/imbalance_test.go`

- [ ] **Step 1: Failing test**

```go
func TestImbalanceGateBlocksEntryAgainstTape(t *testing.T) {
	s := New(Config{...ImbalanceWindow: 3, ImbalanceThreshold: zero...}, nil)
	s.applyTradeForImbalance(streams.TradeEvent{Side: "SELL", Quantity: decimal.Ten})
	s.applyTradeForImbalance(streams.TradeEvent{Side: "SELL", Quantity: decimal.One})
	// BUY entries blocked (net flow -11 opposes), SELL entries allowed.
	require.False(t, s.imbalanceAllows(types.SignalBuy))
	require.True(t, s.imbalanceAllows(types.SignalSell))
}
```

Also: disabled (`ImbalanceWindow == 0`) always allows.

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement** `strategy/meanreversion/imbalance.go`:

```go
// applyTradeForImbalance folds one tape trade into the signed-quantity
// window (BUY=+, SELL=-). Missing/unrecognized Side is skipped.
func (s *Strategy) applyTradeForImbalance(ev streams.TradeEvent)

// imbalanceAllows reports whether the entry direction survives the
// imbalance gate: blocked when net signed quantity over the window
// opposes the direction beyond ImbalanceThreshold.
func (s *Strategy) imbalanceAllows(sig types.Signal) bool
```

(Fields on Strategy: `imbWin *window.Rolling` sized `cfg.ImbalanceWindow`; guard `ImbalanceWindow <= 0` → always true. Use the same `window.Rolling` sum semantics as breakout's OBV at `strategy/breakout/strategy.go:323-365`.)

Config: `ImbalanceWindow int`, `ImbalanceThreshold decimal.Decimal` (defaults 100 / 0).

Run loop: add `WithTradeStream(ts *streams.TradeStream)` option (mirror breakout's at `strategy/breakout/strategy.go:251-253`); in `Run`, when configured, `trades, id := ts.SubscribeOrderBook(cfg.OrderBookID)` and a `case ev := <-trades: s.applyTradeForImbalance(ev)` in the select; defer unsubscribe. Gate entry: in the bar case, before `executeDecision`, `if !s.imbalanceAllows(decision.Signal()) { continue }` (log at debug, reason recorded on the decision as `imbalance_filtered`).

Backtest: interleave `trades_history` (reuse `breakout.PGTradeHistoryStore.StreamTrades` by timestamp — the breakout backtester's existing interleave at `strategy/breakout/backtest.go:79-94` is the pattern; import the breakout store rather than duplicating the SQL). Missing trades data → gate passes (log).

Wire `WithTradeStream` in `strategy/http/handler.go` meanreversion construction (it already builds the shared `tradeStream` for copytrading/breakout).

- [ ] **Step 4: Run** `go test ./strategy/meanreversion/... -race -v` → PASS.

- [ ] **Step 5:** stage + pre-commit.

---

### Task 10: momentum — bar-driven core + true-range ATR + volume gate

**Files:**
- Modify: `strategy/momentum/types.go` (Config 41-96), `strategy/momentum/strategy.go` (Update 337-411, run loop 766-901)
- Test: `strategy/momentum/strategy_test.go`, `strategy/momentum/run_loop_test.go`

- [ ] **Step 1: Failing tests**

```go
func TestDefaultConfigBars(t *testing.T) {
	c := DefaultConfig()
	require.Equal(t, "15m", c.Resolution)
	require.Equal(t, 8, c.FastWindow)
	require.Equal(t, 96, c.SlowWindow)
	require.Equal(t, 24, c.ATRWindow)
	require.Equal(t, 20, c.VolumeAvgWindow)
	require.True(t, c.VolumeRatioThreshold.Equal(decimal.One))
}

func TestTrueRangeFeedsATRWindow(t *testing.T) {
	// bars with known H/L/prevClose; assert Decision.ATR equals the mean
	// of true ranges, NOT mean |Δclose|.
}

func TestVolumeGateBlocksThinVolume(t *testing.T) {
	// prime vol window with volume 10; a bar with volume 5 and threshold
	// 1.0 must NOT emit an entry despite MA separation; volume 10+ must.
}
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

Config: add `Resolution string`, `VolumeAvgWindow int`, `VolumeRatioThreshold decimal.Decimal`; defaults `15m / 8 / 96 / 24 / 20 / 1.0`.

Strategy struct: add `candleFeed strategy.CandleFeed`, `volWin *window.Rolling`, `WithCandleFeed` option.

`Update(obs types.Bar)`:

```go
value, ok, err := s.seriesValue(bar) // price: bar.Close; ytm: bar.CloseYTM; spread: CloseYTM−BenchmarkYield
tr, err := bar.TrueRange(s.lastPrice) // lastPrice is the previous bar's close
if err := s.atrWin.Add(tr); err != nil { ... }
_ = s.volWin.Add(bar.Volume)
_ = s.fastWin.Add(value)
_ = s.slowWin.Add(value)
s.lastPrice = bar.Close
// MA crossover + sourceSign logic unchanged
// volume gate: entry signals (non-Hold) require, when VolumeAvgWindow > 0
// and volWin.Ready(): bar.Volume ≥ VolumeRatioThreshold × volWin.Mean();
// otherwise downgrade to Hold with reason "volume_not_confirmed"
```

`seriesValue` switches its input to `types.Bar` (zero `CloseYTM` drops the bar in ytm/spread modes, same contract as today's zero-YTM drop).

Run loop: mirror Task 7's structure — bar case drives Update/entry (with volume gate), tick case drives `liveCheckIntrabar`: stop-loss/take-profit checks against `entryPrice ± StopLossATR × entryATR` using tick prices (reuse `ShouldExit`'s price-band logic at `strategy/momentum/strategy.go:266-314`; extract the band check into a helper both callers share). Warmup: `since = now − (SlowWindow+1)×res`. Delete tick-driven window updates and entries; delete `prefillWindow`/`seedResumeAnchor`'s dependence on tick windows (`seedResumeAnchor` stays but sources `entryATR` from the current true-range ATR window).

- [ ] **Step 4: Run** `go test ./strategy/momentum/... -race -v` → PASS.

- [ ] **Step 5:** stage + pre-commit.

---

### Task 11: momentum — backtest on candles

**Files:**
- Modify: `strategy/momentum/historical_data.go` (getObservations, prefillWindow), `strategy/momentum/backtest.go`
- Test: `strategy/momentum/historical_data_test.go`, `strategy/momentum/backtest_test.go`

Identical shape to Task 8: `candleHistoryStore` (bucketed + range), coverage guard error, bar replay with adverse-extreme exit checks (price-based here: `bar.High`/`bar.Low` vs entry bands), delete prefill. `spread` mode keeps the FRED merge per bar. Stage + pre-commit.

---

### Task 12: breakout — bar-driven core, true-range ATR, prefill, backtest

**Files:**
- Modify: `strategy/breakout/strategy.go` (Config 25-135, Update 367-453, ingestObservation 487-508, runLoop 641-688, handleTick 690-762), `strategy/breakout/postgres_store.go`, `strategy/breakout/backtest.go`
- Test: `strategy/breakout/strategy_test.go`, `strategy/breakout/backtest_test.go`, `strategy/breakout/integration_backtest_test.go`

- [ ] **Step 1: Failing tests**

```go
func TestDefaultConfigBars(t *testing.T) {
	c := DefaultConfig()
	require.Equal(t, "5m", c.Resolution)
	require.Equal(t, 12, c.ShortVolWindow)
	require.Equal(t, 96, c.LongVolWindow)
	require.Equal(t, 12, c.ATRWindow)
	require.Equal(t, 3, c.ConfirmationBars)
}

func TestConfirmationBarsAreBars(t *testing.T) {
	// feed 12+96 warmup bars, arm compression, then 3 consecutive closes
	// beyond trigger → signal fires on the 3rd, not before; ticks in
	// between are ignored for signal purposes.
}
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

Config: add `Resolution string` (default `5m`); new defaults above. Strategy struct: `candleFeed strategy.CandleFeed` + `WithCandleFeed`.

`Update(o types.Bar)` / `ingestObservation(bar, prevBarClose)`: `atrWin.Add(bar.TrueRange(prevBarClose))`; `shortVolWin.Add(bar.Close)`; `longVolWin.Add(bar.Close)`; `s.lastPrice = bar.Close`. `evaluateBreakout` uses `prevBarClose ± BreakoutATRMultiple×ATR` — unchanged shape, now bar-closes. OBV path untouched.

Run loop: bar case drives Update + entries (volume-filtered by OBV as today); tick case keeps `liveCheckSLTP` on tick prices (already price-based, `strategy/breakout/strategy.go:984-1025`) + `lastPrice` for execution. Warm start: `since = now − (LongVolWindow+1)×res` — this replaces the cold start (the "new prefill" from the spec, via stream bootstrap). Delete tick-driven signal path.

`PostgresHistoricalStore.Observations` (`postgres_store.go:44-87`): switch from `price_history` to `LoadCandlesBucketed` (delegate to `candles.PGStore`, injectable for tests); fix the stale "reads candles_history" comment while there. Coverage guard as in Task 8. Backtester: replay bars; exits on bar adverse extreme (`bar.High`/`bar.Low` vs SL/TP bands after each bar); trades interleave for OBV unchanged.

- [ ] **Step 4: Run** `go test ./strategy/breakout/... -race -v` → PASS; update `integration_backtest_test.go` fixtures to seed `candles_history` rows.

- [ ] **Step 5:** stage + pre-commit.

---

### Task 13: cutover guards, docs, final verification

**Files:**
- Test: one per strategy (`strategy/meanreversion`, `strategy/momentum`, `strategy/breakout`)
- Modify: `strategies.md`, `strategy/http/handler.go` (ConfigFields text), `docs/openapi/*` (strategy payloads), MCP tool schema descriptions if duplicated

- [ ] **Step 1: Cutover guard tests**

```go
func TestTicksAloneProduceNoEntries(t *testing.T) {
	// strategy with warm windows (bar fixtures); drive the run loop with
	// ONLY price ticks that would have triggered the old tick engine;
	// assert no decisions/orders recorded.
}
```

- [ ] **Step 2: Docs**

`strategies.md`: update every window field description from ticks to bars with the new defaults; add `resolution` rows to all three field tables; document `imbalance_window`/`imbalance_threshold`, `volume_avg_window`/`volume_ratio_threshold`; delete the "Bar/candle resampling is not supported" v1 limitation (momentum) and the OBV trades-history note stays; describe the new bar/close semantics and intrabar stops. Mirror the same text into `ConfigFields` metadata and the OpenAPI strategy schemas.

- [ ] **Step 3: Full verification**

```bash
go build ./...
go test ./... -race
golangci-lint run --timeout 5m ./...
pre-commit run --all-files
```

Expected: all green. Then stage everything from this task and report for the user review gate.

---

## Self-Review (completed during planning)

- **Spec coverage:** decisions 1-5 → Tasks 1-13 (cutover: 7-12; native stream: 1,3,5; aux signals: 7,9,10,12; backtests: 4,8,11,12; defaults: 6,7,10,12; docs: 13). Bar type/BarCloser/registry/bucketed loader each have a task; gates have tasks; coverage guard in 8/11/12; prefill removal in 7/8 (stream-bootstrap warm start is the spec's "prefill" implemented with less code — noted deviation, same behavior).
- **Placeholders:** none — every code step shows code; the two "adapt to real field names" notes (Daemon config, subID capture) name the exact source lines to copy from.
- **Type consistency:** `types.Bar` (Task 2) used by BarCloser/registry (3,5) and every strategy Update (7,10,12); `LoadCandlesBucketed`/`CandleRange`/`ErrNoCandleCoverage` defined in Task 4 and consumed in 5,8,11,12; `WithCandleFeed` defined per strategy package (7,10,12) and wired in 5,6.
