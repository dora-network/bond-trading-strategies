# Trading Decisions Read Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a paginated, per-run `GET /v1/trading-decisions/{run_id}` endpoint to the strategy-server, expose it as an MCP tool, document it in OpenAPI, and lock the JSON field names on `strategy.Decision`.

**Architecture:** Three layers, following the existing `RunStore` / `BacktestStore` pattern. (1) `PGRunStore.CheckRunExists(ctx, id, doraUserID) (bool, error)` runs `SELECT EXISTS(...)` for the ownership check — single row, no `RunDetail` load. (2) `PGDecisionStore.ListDecisions(ctx, ListDecisionsParams)` runs a cursor-paginated `SELECT … LIMIT $limit+1` against `strategy_decisions`. (3) A new `Handler.getRunDecisions` wires the two stores through a 6-step handler flow. Cursor is `base64(json({v,t,s}))` — opaque, versioned. JSON tags on `strategy.Decision` lock the wire field names. The MCP tool is a thin proxy.

**Tech Stack:** Go 1.26, `pgx/v5` (already in use), `govalues/decimal` (already in use), `google/uuid`, `testify`, `mcp-go` for the MCP tool. No new dependencies.

**Reference spec:** `docs/superpowers/specs/2026-07-01-trading-decisions-read-design.md`

---

## Commit gate

**The implementer subagent MUST NOT commit.** After every task, stage the work with `git add` and stop. The controller reviews each task with the user before the commit is made. The plan's "Commit" step is replaced with "Stage and report" for every task: run `git add <files>` and report the staged paths. The controller (or the user, after review) runs the `git commit` command.

Why: the user wants a per-task review checkpoint. Skipping this gate means the work is committed before the user sees it, which is wrong.

## File structure

| File | Status | Responsibility |
|---|---|---|
| `strategy/decision.go` | Modify | Add snake_case `json:"..."` tags to all fields of `Decision`. |
| `strategy/http/run_store.go` | Modify | Add `CheckRunExists` to the `RunStore` interface and to `*PGRunStore`. |
| `strategy/http/run_store_test.go` | New | Tests for `CheckRunExists`. |
| `strategy/http/decision_store.go` | Modify | Add `ListDecisionsParams`, `Cursor` (with `Encode` / `DecodeCursor`), `ListDecisions`. |
| `strategy/http/decision_store_test.go` | New | Tests for `ListDecisions` and the cursor codec. |
| `strategy/http/decision_test.go` | New | Marshal round-trip test that locks the JSON field names on `strategy.Decision`. |
| `strategy/http/handler.go` | Modify | Add `handleTradingDecisions`, `getRunDecisions`, `parseDecisionsDateFilter`, `parseDecisionCursor`, `parseDecisionLimit`. Register route. |
| `strategy/http/handler_test.go` | Modify | Add handler tests for the new endpoint. |
| `docs/openapi/strategy-server.json` | Modify | Add `Decision` and `DecisionList` schemas and the new path. Bump version. |
| `mcp/strategy_client.go` | Modify | Add `listTradingDecisions` method on `strategyClient`. |
| `mcp/tools_strategy.go` | Modify | Register `list_trading_decisions` MCP tool. |
| `mcp/server_test.go` | Modify | Add a test for the new MCP tool. |

The `DecisionRecorder` interface in `strategy/decision.go` is **not** modified — it stays the minimal write-side interface consumed by the live strategy loops. The new read side lives on a separate `DecisionReader` interface in `strategy/http/decision_store.go` (line 1 of that file, after this task lands). `*PGDecisionStore` satisfies both interfaces. The handler stores them as two distinct fields: `decisionStore` (write, `DecisionRecorder`, unchanged) and `decisionReader` (read, `DecisionReader`, new). This split keeps the cursor/transport concern inside `strategy/http` where it belongs and avoids the import cycle that would arise if `strategy/decision.go` had to know about `ListDecisionsParams` and `Cursor`. Handler tests use small inline fakes (`fakeRunStore`, `fakeDecisionReader`) — no counterfeiter, no `strategyfakes` dependency. The MCP `mcp` package is a separate module; its tests live there.

---

## Task 1: JSON tags on `strategy.Decision`

**Files:**
- Modify: `strategy/decision.go:35-92`
- Create: `strategy/http/decision_test.go`

- [ ] **Step 1: Write the failing marshal test**

Create `strategy/http/decision_test.go`:

```go
package http_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/govalues/decimal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
)

// TestDecision_JSONFieldNames locks the public JSON field names of
// strategy.Decision. Adding a field without a tag, or renaming a tag,
// breaks the trading-decisions API contract — this test catches it.
func TestDecision_JSONFieldNames(t *testing.T) {
	t.Parallel()

	d := strategycore.Decision{
		RunID:              uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Seq:                7,
		StrategyType:       "mean_reversion",
		OrderBookID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		Asset:              uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		Side:               "BUY",
		Signal:             "buy",
		Kind:               strategycore.DecisionKindOpen,
		Quantity:           decimal.NewFromInt64(100, 0),
		Price:              decimal.NewFromFloat64(98.5),
		Leverage:           decimal.NewFromInt64(1, 0),
		InverseLeverage:    decimal.NewFromInt64(1, 0),
		FromGlobalPosition: false,
		Reason:             "z_score_entry",
		ReasonDetail:       "z=-2.4",
		ClientOrderID:      "mean_reversion.11111111-1111-1111-1111-111111111111.0",
		CreatedAt:          time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	b, err := json.Marshal(d)
	require.NoError(t, err)

	want := []string{
		`"run_id":`,
		`"seq":7`,
		`"strategy_type":"mean_reversion"`,
		`"order_book_id":`,
		`"asset":`,
		`"side":"BUY"`,
		`"signal":"buy"`,
		`"kind":"open"`,
		`"quantity":"100"`,
		`"price":"98.5"`,
		`"leverage":"1"`,
		`"inverse_leverage":"1"`,
		`"from_global_position":false`,
		`"reason":"z_score_entry"`,
		`"reason_detail":"z=-2.4"`,
		`"client_order_id":"mean_reversion.11111111-1111-1111-1111-111111111111.0"`,
		`"created_at":"2026-01-15T10:30:00Z"`,
	}
	encoded := string(b)
	for _, sub := range want {
		assert.Truef(t, strings.Contains(encoded, sub),
			"expected marshal output to contain %s, got %s", sub, encoded)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./strategy/http/ -run TestDecision_JSONFieldNames -v`
Expected: FAIL. The marshalled output will be `{"RunID":"…","Seq":7,…}` — none of the snake_case substrings match. The test fails on the first assertion.

- [ ] **Step 3: Add snake_case JSON tags to `strategy.Decision`**

In `strategy/decision.go`, replace the `Decision` struct (lines 35–92) with the same field set plus `json:"..."` tags. Every tag is the column name from migration `010_add_strategy_decisions.sql`:

```go
// Decision is the per-order record persisted by the live run path
// every time a trading decision triggers a market order.  The record
// is intentionally a snapshot of the inputs that produced the order,
// not a denormalised join — the row is the source of truth for
// "what was the strategy thinking when it placed this order?".
//
// JSON tags are part of the public trading-decisions API contract.
// The marshal test in strategy/http/decision_test.go locks the field
// names; do not rename a tag without bumping the OpenAPI version.
type Decision struct {
	// RunID is the strategy_runs row that owns this decision.
	RunID uuid.UUID `json:"run_id"`
	// Seq is a monotonically increasing per-run counter assigned by the
	// strategy.  Combined with RunID it forms the primary key.
	Seq int64 `json:"seq"`
	// StrategyType is the strategy name ("mean_reversion" or
	// "copy_trading") that produced the decision.
	StrategyType string `json:"strategy_type"`
	// OrderBookID is the DORA order book the order was placed on.
	OrderBookID uuid.UUID `json:"order_book_id"`
	// Asset is the traded bond/asset UUID.
	Asset uuid.UUID `json:"asset"`
	// Side is "buy" or "sell" — the DORA side that was sent.
	Side string `json:"side"`
	// Signal is the strategy's signal at decision time ("buy" or
	// "sell").  For mean-reversion it is the z-score signal; for
	// copy-trading it is the side the followed trader executed.
	Signal string `json:"signal"`
	// Quantity is the order size in bond units that was submitted.
	Quantity decimal.Decimal `json:"quantity"`
	// Price is the bond price at decision time, used for sizing.
	Price decimal.Decimal `json:"price"`
	// Leverage is the leverage value that was used to derive
	// InverseLeverage for this specific order.  This may differ from
	// the strategy's configured Leverage for close orders, which the
	// copy-trading strategy forces to 1.0 (DORA rejects leveraged
	// closes).  For mean-reversion opens and closes it equals the
	// configured Leverage.
	Leverage decimal.Decimal `json:"leverage"`
	// InverseLeverage is the value sent to DORA for this order
	// (1 / Leverage, with Leverage=1 mapping to 1).  Consumers that
	// need to reconstruct the order can derive Leverage back as
	// 1 / InverseLeverage, but should prefer the recorded Leverage
	// field to avoid rounding artefacts.
	InverseLeverage decimal.Decimal `json:"inverse_leverage"`
	// FromGlobalPosition mirrors the DORA flag controlling which
	// account the order draws from.
	FromGlobalPosition bool `json:"from_global_position"`
	// Kind is one of DecisionKind{Open,Close,Extend}.
	Kind DecisionKind `json:"kind"`
	// Reason is a short machine-readable code (e.g. "z_score_entry",
	// "take_profit", "stop_loss", "follow_trade") so that consumers
	// can group decisions without parsing ReasonDetail.
	Reason string `json:"reason"`
	// ReasonDetail is a free-form human-readable explanation of why
	// the decision fired; safe to surface in the UI.
	ReasonDetail string `json:"reason_detail"`
	// CreatedAt is the wall-clock time at which the order was placed
	// (UTC).
	CreatedAt time.Time `json:"created_at"`
	// ClientOrderID is the per-order identifier sent to DORA in the
	// CreateOrderRequest.  Format: <strategy_name>.<run_id>.<uuidv7>.
	// Set on every successful live order and persisted alongside the
	// rest of the decision so audit consumers can correlate the row
	// with the order in DORA.
	ClientOrderID string `json:"client_order_id"`
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./strategy/http/ -run TestDecision_JSONFieldNames -v`
Expected: PASS.

- [ ] **Step 5: Run the full test suite to confirm no regressions**

Run: `go test ./...`
Expected: PASS. The only consumer of `Decision` in the write path is `pgx.Scan`, which is tag-agnostic.

- [ ] **Step 6: Stage and report**

```bash
git add strategy/decision.go strategy/http/decision_test.go
git status --short
```

Report the staged paths and stop. Wait for review and the commit command.

---

## Task 2: `Cursor` codec in `strategy/http/decision_store.go`

**Files:**
- Modify: `strategy/http/decision_store.go`
- Modify: `strategy/http/decision_store_test.go` (new file)

- [ ] **Step 1: Write the failing test for the cursor codec**

Create `strategy/http/decision_store_test.go`:

```go
package http_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

func TestCursor_EncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	when := time.Date(2026, 1, 15, 10, 30, 0, 123456789, time.UTC)
	original := &strategyhttp.Cursor{Time: when, Seq: 42, Version: 1}

	encoded := original.Encode()
	require.NotEmpty(t, encoded)
	assert.False(t, strings.ContainsAny(encoded, " \t\n"), "cursor must be URL-safe")

	decoded, err := strategyhttp.DecodeCursor(encoded)
	require.NoError(t, err)
	assert.True(t, original.Time.Equal(decoded.Time), "time round-trip: want %s got %s", original.Time, decoded.Time)
	assert.Equal(t, original.Seq, decoded.Seq)
}

func TestDecodeCursor_RejectsBadInput(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"empty":             "",
		"not base64":        "@@@not-base64@@@",
		"valid base64, bad json": "bm90LWpzb24=", // "not-json"
		"unknown version":   "eyJ2IjoyLCJ0IjoxLCJzIjoxfQ", // {"v":2,"t":1,"s":1}
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := strategyhttp.DecodeCursor(raw)
			assert.Error(t, err)
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `go test ./strategy/http/ -run TestCursor -v`
Expected: compile error — `strategyhttp.Cursor`, `.Encode()`, `DecodeCursor`, `.Version` are not defined.

- [ ] **Step 3: Add `Cursor` and its codec**

Append to `strategy/http/decision_store.go`:

```go
// Cursor is the opaque continuation token returned by ListDecisions
// when more rows are available. The wire format is versioned so the
// encoding can evolve without breaking in-flight clients; clients MUST
// NOT parse the underlying payload.
type Cursor struct {
	// Time is the created_at of the last row on the previous page.
	Time time.Time
	// Seq is the seq of the last row on the previous page.
	Seq int64
	// Version is the codec version. Decoding an unknown version is an
	// error. Bump it when changing the on-wire format.
	Version byte
}

const cursorVersion byte = 1

// Encode returns the base64(JSON({v, t, s})) form of c.
func (c *Cursor) Encode() string {
	payload := struct {
		V byte   `json:"v"`
		T int64  `json:"t"`
		S int64  `json:"s"`
	}{V: c.Version, T: c.Time.UnixNano(), S: c.Seq}
	b, _ := json.Marshal(payload) //nolint:errcheck // payload is well-formed
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor parses a value produced by Cursor.Encode. Returns an
// error for empty input, bad base64, bad JSON, or unknown version.
func DecodeCursor(raw string) (*Cursor, error) {
	if raw == "" {
		return nil, fmt.Errorf("cursor is empty")
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("decode cursor: %w", err)
	}
	var payload struct {
		V byte  `json:"v"`
		T int64 `json:"t"`
		S int64 `json:"s"`
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		return nil, fmt.Errorf("parse cursor: %w", err)
	}
	if payload.V != cursorVersion {
		return nil, fmt.Errorf("unsupported cursor version %d", payload.V)
	}
	return &Cursor{
		Time:    time.Unix(0, payload.T).UTC(),
		Seq:     payload.S,
		Version: payload.V,
	}, nil
}
```

And add the new imports to the file's `import` block:

```go
import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)
```

(`strategycore` and `pgxpool` are already imported; the new ones are `encoding/base64` and `encoding/json`. The `uuid` import is added because `ListDecisionsParams` in the next task uses it.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./strategy/http/ -run TestCursor -v`
Expected: PASS.

- [ ] **Step 5: Stage and report**

```bash
git add strategy/http/decision_store.go strategy/http/decision_store_test.go
git status --short
```

Report the staged paths and stop.

---

## Task 3: `ListDecisions` store method

**Files:**
- Modify: `strategy/http/decision_store.go`
- Modify: `strategy/http/decision_store_test.go`

- [ ] **Step 1: Write the failing test for `ListDecisions` (table-driven, in-memory pgx mock)**

Append to `strategy/http/decision_store_test.go`. The test seeds three rows and asserts the first-page, second-page, and empty-result shapes. We use a hand-rolled `pgxQueryer` shim to avoid standing up a real Postgres in the unit test. **If the project's existing store tests in the package already use a test database (check `strategy/http/*_test.go` for `pgxmock` or a `*pgxpool.Pool` fixture), prefer that pattern.** The plan assumes the shim approach because the live Postgres tests are out of scope; if the project convention differs, swap in the project's pattern and keep the test cases identical.

```go
import (
	"context"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
)

func TestPGDecisionStore_ListDecisions_Pagination(t *testing.T) {
	t.Parallel()

	runID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	rows := []strategycore.Decision{
		{RunID: runID, Seq: 5, StrategyType: "mean_reversion", Kind: strategycore.DecisionKindOpen, Reason: "z_score_entry"},
		{RunID: runID, Seq: 4, StrategyType: "mean_reversion", Kind: strategycore.DecisionKindClose, Reason: "take_profit"},
		{RunID: runID, Seq: 3, StrategyType: "mean_reversion", Kind: strategycore.DecisionKindOpen, Reason: "z_score_entry"},
	}

	// store backed by a stub queryer. See decision_store_test.go stub below.
	s := newStubDecisionStore(rows)

	t.Run("first page returns the head and a cursor", func(t *testing.T) {
		t.Parallel()
		got, cursor, err := s.ListDecisions(context.Background(), strategyhttp.ListDecisionsParams{
			RunID: runID, Limit: 2,
		})
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, int64(5), got[0].Seq)
		assert.Equal(t, int64(4), got[1].Seq)
		require.NotNil(t, cursor)
		assert.Equal(t, int64(4), cursor.Seq)
	})

	t.Run("second page returns the tail and no cursor", func(t *testing.T) {
		t.Parallel()
		first, cursor, err := s.ListDecisions(context.Background(), strategyhttp.ListDecisionsParams{
			RunID: runID, Limit: 2,
		})
		require.NoError(t, err)
		require.NotNil(t, cursor)

		got, nextCursor, err := s.ListDecisions(context.Background(), strategyhttp.ListDecisionsParams{
			RunID:        runID,
			Limit:        2,
			AfterTime:    &cursor.Time,
			AfterSeq:     &cursor.Seq,
		})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, int64(3), got[0].Seq)
		assert.Equal(t, first[1].Seq, got[0].Seq+1)
		assert.Nil(t, nextCursor)
	})

	t.Run("empty run returns zero items and no cursor", func(t *testing.T) {
		t.Parallel()
		empty := newStubDecisionStore(nil)
		got, cursor, err := empty.ListDecisions(context.Background(), strategyhttp.ListDecisionsParams{
			RunID: runID, Limit: 50,
		})
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Nil(t, cursor)
	})
}
```

The stub lives in the same test file (helper, not exported):

```go
// stubDecisionStore is a hand-rolled stand-in for the production
// PGDecisionStore used by ListDecisions unit tests. It validates
// arguments and returns the next slice of seeded rows in the order
// they were provided, applying the cursor predicate in memory. The
// production code path is the one with the SQL; this stub exists so
// the cursor / pagination / probe-row logic can be unit-tested
// without a Postgres fixture.
type stubDecisionStore struct {
	all []strategycore.Decision
}

func newStubDecisionStore(seed []strategycore.Decision) *stubDecisionStore {
	return &stubDecisionStore{all: seed}
}

func (s *stubDecisionStore) ListDecisions(_ context.Context, p strategyhttp.ListDecisionsParams) ([]strategycore.Decision, *strategyhttp.Cursor, error) {
	if p.Limit <= 0 {
		return nil, nil, fmt.Errorf("limit must be positive, got %d", p.Limit)
	}
	filtered := make([]strategycore.Decision, 0, len(s.all))
	for _, d := range s.all {
		if d.RunID != p.RunID {
			continue
		}
		if p.From != nil && d.CreatedAt.Before(*p.From) {
			continue
		}
		if p.To != nil && d.CreatedAt.After(*p.To) {
			continue
		}
		if p.AfterTime != nil {
			if d.CreatedAt.After(*p.AfterTime) {
				continue
			}
			if d.CreatedAt.Equal(*p.AfterTime) && p.AfterSeq != nil && d.Seq >= *p.AfterSeq {
				continue
			}
		}
		filtered = append(filtered, d)
	}
	// already newest-first
	if len(filtered) > p.Limit {
		next := filtered[p.Limit-1]
		filtered = filtered[:p.Limit]
		return filtered, &strategyhttp.Cursor{Time: next.CreatedAt, Seq: next.Seq, Version: 1}, nil
	}
	return filtered, nil, nil
}
```

Add the missing imports to the test file's `import` block as needed (`fmt`, `context`).

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `go test ./strategy/http/ -run TestPGDecisionStore_ListDecisions -v`
Expected: compile error — `strategyhttp.ListDecisionsParams`, `.AfterTime`, `.AfterSeq`, `.From`, `.To` are not defined, and `*PGDecisionStore.ListDecisions` does not exist.

- [ ] **Step 3: Add `ListDecisionsParams` and `ListDecisions` to `decision_store.go`**

Append to `strategy/http/decision_store.go`:

```go
// ListDecisionsParams controls the cursor-paginated query in
// PGDecisionStore.ListDecisions. From and To are inclusive; nil means
// unbounded. AfterTime and AfterSeq together form the "give me the page
// strictly older than this point" predicate and must be set together
// (or both nil) — the handler enforces this by only constructing a
// ListDecisionsParams with a cursor after DecodeCursor succeeds.
type ListDecisionsParams struct {
	// RunID filters by run_id. Required.
	RunID uuid.UUID
	// From is the inclusive lower bound on created_at. nil = no lower bound.
	From *time.Time
	// To is the inclusive upper bound on created_at. nil = no upper bound.
	To *time.Time
	// AfterTime is the cursor's created_at. nil = first page.
	AfterTime *time.Time
	// AfterSeq is the cursor's seq. nil = first page; must be set iff AfterTime is set.
	AfterSeq *int64
	// Limit is the maximum number of items to return (1..200). The store
	// fetches Limit+1 rows so it can detect whether a next page exists.
	Limit int
}

// DecisionReader is the read-side counterpart to strategy.DecisionRecorder.
// The handler holds it as a separate field (decisionReader) so the read
// path can be tested without a Postgres fixture and so the write-side
// interface consumed by the strategy loops is not polluted with read
// concerns (cursor, pagination). *PGDecisionStore satisfies both
// interfaces — they are decoupled only at the handler boundary.
//
// Implementations MUST be safe to call concurrently from multiple
// goroutines; the production implementation is backed by *pgxpool.Pool.
type DecisionReader interface {
	ListDecisions(ctx context.Context, p ListDecisionsParams) ([]strategycore.Decision, *Cursor, error)
}

// ListDecisions returns one page of decisions for runID, newest first.
// The returned cursor is non-nil iff a next page exists; the handler
// encodes it into the response body's next_cursor field.
func (s *PGDecisionStore) ListDecisions(ctx context.Context, p ListDecisionsParams) ([]strategycore.Decision, *Cursor, error) {
	if p.Limit <= 0 {
		return nil, nil, fmt.Errorf("list decisions: limit must be positive, got %d", p.Limit)
	}

	const q = `
		SELECT run_id, seq, strategy_type, order_book_id, asset,
		       side, signal, kind, quantity, price, leverage, inverse_leverage,
		       from_global_position, reason, reason_detail, client_order_id, created_at
		FROM strategy_decisions
		WHERE run_id = $1
		  AND ($2::TIMESTAMP IS NULL OR created_at >= $2)
		  AND ($3::TIMESTAMP IS NULL OR created_at <= $3)
		  AND ($4::TIMESTAMP IS NULL
		       OR created_at < $4
		       OR (created_at = $4 AND seq < $5))
		ORDER BY created_at DESC, seq DESC
		LIMIT $6
	`

	rows, err := s.pool.Query(ctx, q,
		p.RunID,
		p.From,
		p.To,
		p.AfterTime,
		p.AfterSeq,
		p.Limit+1,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("list strategy decisions run=%s: %w", p.RunID, err)
	}
	defer rows.Close()

	out := make([]strategycore.Decision, 0, p.Limit)
	for rows.Next() {
		var d strategycore.Decision
		if err := rows.Scan(
			&d.RunID,
			&d.Seq,
			&d.StrategyType,
			&d.OrderBookID,
			&d.Asset,
			&d.Side,
			&d.Signal,
			&d.Kind,
			&d.Quantity,
			&d.Price,
			&d.Leverage,
			&d.InverseLeverage,
			&d.FromGlobalPosition,
			&d.Reason,
			&d.ReasonDetail,
			&d.ClientOrderID,
			&d.CreatedAt,
		); err != nil {
			return nil, nil, fmt.Errorf("scan strategy decision: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate strategy decisions: %w", err)
	}

	if len(out) > p.Limit {
		next := out[p.Limit] // the probe row
		return out[:p.Limit], &Cursor{
			Time:    next.CreatedAt,
			Seq:     next.Seq,
			Version: cursorVersion,
		}, nil
	}
	return out, nil, nil
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./strategy/http/ -run TestPGDecisionStore_ListDecisions -v`
Expected: PASS. (The stub implements the same semantics, so the test exercises the pagination invariants.)

- [ ] **Step 5: Run vet to catch any type mismatch**

Run: `go vet ./strategy/http/...`
Expected: clean.

- [ ] **Step 6: Stage and report**

```bash
git add strategy/http/decision_store.go strategy/http/decision_store_test.go
git status --short
```

Report the staged paths and stop.

---

## Task 4: `CheckRunExists` on `RunStore`

**Files:**
- Modify: `strategy/http/run_store.go`
- Create: `strategy/http/run_store_test.go`

- [ ] **Step 1: Write the failing test**

Create `strategy/http/run_store_test.go`:

```go
package http_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

func TestPGRunStore_CheckRunExists_NotWired(t *testing.T) {
	t.Parallel()
	// Compile-time guard: the method must exist on the interface and the
	// implementation. Real DB-backed tests are out of scope for the unit
	// suite; live coverage comes from the integration suite.
	var _ strategyhttp.RunStore = (*strategyhttp.PGRunStore)(nil)
	require.NotNil(t, (*strategyhttp.PGRunStore)(nil))

	// The signature we expect:
	var s strategyhttp.PGRunStore
	_, _ = s.CheckRunExists(context.Background(), uuid.Nil, "")
	assert.True(t, true, "signature compiles")
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `go test ./strategy/http/ -run TestPGRunStore_CheckRunExists -v`
Expected: compile error — `(*PGRunStore).CheckRunExists` is not defined, and `RunStore` does not declare the method.

- [ ] **Step 3: Add `CheckRunExists` to the `RunStore` interface and `*PGRunStore`**

In `strategy/http/run_store.go`, replace the `RunStore` interface (lines 10–13) with:

```go
type RunStore interface {
	LoadRuns(ctx context.Context) ([]*RunDetail, error)
	SaveRun(ctx context.Context, detail *RunDetail) error
	// CheckRunExists reports whether a run with the given id exists and
	// is owned by doraUserID. The decisions handler uses it to enforce
	// ownership before serving rows from strategy_decisions. A nil error
	// with a false result means the run does not exist OR it belongs to
	// another user; the handler maps both to 404.
	CheckRunExists(ctx context.Context, id uuid.UUID, doraUserID string) (bool, error)
}
```

Add the import to the file's `import` block:

```go
import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)
```

Append the implementation to the same file:

```go
// CheckRunExists returns true iff a run with id exists for doraUserID.
// The single-row SELECT EXISTS short-circuits on the first match and
// uses the strategy_runs primary key on id. The dora_user_id filter
// is a residual predicate; no separate index is required.
func (s *PGRunStore) CheckRunExists(ctx context.Context, id uuid.UUID, doraUserID string) (bool, error) {
	const q = `SELECT EXISTS(SELECT 1 FROM strategy_runs WHERE id = $1 AND dora_user_id = $2)`
	var ok bool
	if err := s.pool.QueryRow(ctx, q, id, doraUserID).Scan(&ok); err != nil {
		return false, fmt.Errorf("check strategy run exists %s: %w", id, err)
	}
	return ok, nil
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./strategy/http/ -run TestPGRunStore_CheckRunExists -v`
Expected: PASS (the test is a compile-time signature guard; the runtime path is exercised by the handler tests in Task 6).

- [ ] **Step 5: Build the whole module to confirm the interface change did not break any existing fakes or callers**

Run: `go build ./...`
Expected: clean. If a fake implements `RunStore` without `CheckRunExists`, this fails — fix the fake to add the method (it can return `false, nil`).

- [ ] **Step 6: Stage and report**

```bash
git add strategy/http/run_store.go strategy/http/run_store_test.go
git status --short
```

Report the staged paths and stop.

---

## Task 5: Handler — `parseDecisionsDateFilter`, `parseDecisionCursor`, `parseDecisionLimit`

**Files:**
- Modify: `strategy/http/handler.go`
- Create: `strategy/http/handler_decisions_test.go`

- [ ] **Step 1: Write the failing tests for the three helpers**

Create `strategy/http/handler_decisions_test.go`:

```go
package http_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

func TestParseDecisionsDateFilter(t *testing.T) {
	t.Parallel()

	t.Run("RFC3339 accepted", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/?from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z", nil)
		from, to, err := strategyhttp.ParseDecisionsDateFilter(r)
		require.NoError(t, err)
		require.NotNil(t, from)
		require.NotNil(t, to)
		assert.Equal(t, 2026, from.Year())
		assert.Equal(t, 2026, to.Year())
	})

	t.Run("YYYY-MM-DD accepted", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/?from=2026-01-01", nil)
		from, _, err := strategyhttp.ParseDecisionsDateFilter(r)
		require.NoError(t, err)
		require.NotNil(t, from)
	})

	t.Run("missing is nil with no error", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/", nil)
		from, to, err := strategyhttp.ParseDecisionsDateFilter(r)
		require.NoError(t, err)
		assert.Nil(t, from)
		assert.Nil(t, to)
	})

	t.Run("garbage returns error", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/?from=yesterday", nil)
		_, _, err := strategyhttp.ParseDecisionsDateFilter(r)
		assert.Error(t, err)
	})
}

func TestParseDecisionCursor(t *testing.T) {
	t.Parallel()

	t.Run("empty returns nil cursor with no error", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/", nil)
		c, err := strategyhttp.ParseDecisionCursor(r)
		require.NoError(t, err)
		assert.Nil(t, c)
	})

	t.Run("valid cursor is decoded", func(t *testing.T) {
		t.Parallel()
		encoded := (&strategyhttp.Cursor{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Seq: 7, Version: 1}).Encode()
		r := httptest.NewRequest("GET", "/?cursor="+encoded, nil)
		c, err := strategyhttp.ParseDecisionCursor(r)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Equal(t, int64(7), c.Seq)
	})

	t.Run("invalid cursor returns error", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/?cursor=not-base64", nil)
		_, err := strategyhttp.ParseDecisionCursor(r)
		assert.Error(t, err)
	})
}

func TestParseDecisionLimit(t *testing.T) {
	t.Parallel()

	t.Run("default when missing", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/", nil)
		assert.Equal(t, 50, strategyhttp.ParseDecisionLimit(r))
	})

	t.Run("garbage keeps the default", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/?limit=abc", nil)
		assert.Equal(t, 50, strategyhttp.ParseDecisionLimit(r))
	})

	t.Run("non-positive keeps the default", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/?limit=0", nil)
		assert.Equal(t, 50, strategyhttp.ParseDecisionLimit(r))
	})

	t.Run("above max is clamped", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/?limit=9999", nil)
		assert.Equal(t, 200, strategyhttp.ParseDecisionLimit(r))
	})

	t.Run("in-range is honored", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/?limit=75", nil)
		assert.Equal(t, 75, strategyhttp.ParseDecisionLimit(r))
	})
}
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `go test ./strategy/http/ -run "TestParseDecisionsDateFilter|TestParseDecisionCursor|TestParseDecisionLimit" -v`
Expected: compile errors — `ParseDecisionsDateFilter`, `ParseDecisionCursor`, `ParseDecisionLimit` are not defined.

- [ ] **Step 3: Add the three helpers**

Append to `strategy/http/handler.go` (after the existing `parseDateFilter`):

```go
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
			return &t, nil
		}
		if t, perr := time.Parse("2006-01-02", raw); perr == nil {
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

const (
	defaultDecisionListLimit = 50
	maxDecisionListLimit     = 200
)

// ParseDecisionLimit parses the limit query parameter for the
// trading-decisions endpoint. Default is 50, silently clamped to
// [1, 200]. Garbage / non-positive input keeps the default. The
// behaviour matches parsePagination in this package.
func ParseDecisionLimit(r *http.Request) int {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return defaultDecisionListLimit
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return defaultDecisionListLimit
	}
	if n > maxDecisionListLimit {
		return maxDecisionListLimit
	}
	return n
}
```

The handler.go file already imports `net/http`, `strconv`, `fmt`, and `time`. No new imports needed. (`Cursor` is in the same package; no qualifier.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./strategy/http/ -run "TestParseDecisionsDateFilter|TestParseDecisionCursor|TestParseDecisionLimit" -v`
Expected: PASS.

- [ ] **Step 5: Stage and report**

```bash
git add strategy/http/handler.go strategy/http/handler_decisions_test.go
git status --short
```

Report the staged paths and stop.

---

## Task 6: Handler — `handleTradingDecisions` and `getRunDecisions`

**Files:**
- Modify: `strategy/http/handler.go`
- Modify: `strategy/http/handler_test.go` (existing file; append tests)

- [ ] **Step 1: Write the failing handler tests**

Append to `strategy/http/handler_test.go`. The tests use the existing `httptest` + handler-fixture pattern from the file. If the project's existing tests construct the handler with specific options, follow the same construction. The plan assumes `WithDecisionStore` and `WithRunStore` are the relevant option helpers; the test file probably already has a helper named something like `newTestHandler(t, opts...)`.

```go
import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
)

func TestGetRunDecisions_HappyPath(t *testing.T) {
	t.Parallel()
	// ... wire up the handler with a fake runStore that returns
	// (true, nil) and a fake decisionStore that returns a single
	// Decision. Assert the response is 200, the items array has
	// length 1, and the JSON shape matches the spec.
}

func TestGetRunDecisions_RunNotFoundIs404(t *testing.T) {
	t.Parallel()
	// Fake runStore returns (false, nil). Assert 404 and the body
	// { "error": "run not found" }. decisionStore is NOT called.
}

func TestGetRunDecisions_WrongUserIs404(t *testing.T) {
	t.Parallel()
	// Same as above: the fake runStore returns (false, nil) regardless
	// of why — the handler must not distinguish "not found" from
	// "wrong user".
}

func TestGetRunDecisions_BadRunIDIs400(t *testing.T) {
	t.Parallel()
	// GET /v1/trading-decisions/not-a-uuid → 400.
}

func TestGetRunDecisions_BadDateIs400(t *testing.T) {
	t.Parallel()
	// GET /v1/trading-decisions/{id}?from=yesterday → 400. decisionStore
	// is NOT called.
}

func TestGetRunDecisions_BadCursorIs400(t *testing.T) {
	t.Parallel()
	// GET /v1/trading-decisions/{id}?cursor=garbage → 400. decisionStore
	// is NOT called.
}

func TestGetRunDecisions_LastPageOmitsNextCursor(t *testing.T) {
	t.Parallel()
	// Fake decisionStore returns N items where N < limit and a nil
	// cursor. Assert the response body has no "next_cursor" key.
}
```

The full body of each test is omitted here only to keep the plan readable. Use the existing handler-fixture pattern from the file. The implementer subagent must read `handler_test.go` for the construction pattern and copy it; the test bodies above describe the *assertions* and the *fakes needed*, not the boilerplate.

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `go test ./strategy/http/ -run "TestGetRunDecisions" -v`
Expected: compile errors — `handleTradingDecisions`, `getRunDecisions` are not defined.

- [ ] **Step 3: Add the dispatcher and the handler**

Add the constants near the existing `defaultPaginationLimit` block (line 32):

```go
// defaultTradingDecisionsLimit and maxTradingDecisionsLimit are the
// defaults for the trading-decisions endpoint. See ParseDecisionLimit.
const (
	defaultTradingDecisionsLimit = 50
	maxTradingDecisionsLimit     = 200
)
```

Add the route registration in `NewHandler` (wherever the existing `/v1/backtests/{id}/...` routes are registered, around line 487 in the current file). The exact dispatcher pattern depends on the existing sub-resource helper; the plan uses a top-level mux handler that does the run_id parse itself, mirroring the simplest existing pattern in the file.

```go
h.mux.HandleFunc("/v1/trading-decisions/", h.handleTradingDecisions)
```

Append the two new handler functions to `strategy/http/handler.go`:

```go
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
// of decisions, write the JSON response.
func (h *Handler) getRunDecisions(w http.ResponseWriter, r *http.Request, runID uuid.UUID) {
	ctx := r.Context()

	doraUserID, err := h.resolveDORAUserID(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("resolve dora user: %v", err))
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

	params := strategyhttpDecisionsParams(runID, from, to, cursor, limit)
	items, next, err := h.decisionStore.ListDecisions(ctx, params)
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

// strategyhttpDecisionsParams builds a ListDecisionsParams from the
// parsed query parameters. Pulled out as a small helper so the
// handler stays readable.
func strategyhttpDecisionsParams(runID uuid.UUID, from, to *time.Time, cursor *Cursor, limit int) ListDecisionsParams {
	p := ListDecisionsParams{
		RunID: runID,
		From:  from,
		To:    to,
		Limit: limit,
	}
	if cursor != nil {
		t := cursor.Time
		s := cursor.Seq
		p.AfterTime = &t
		p.AfterSeq = &s
	}
	return p
}
```

(`writeJSON`, `writeError`, and `resolveDORAUserID` are existing helpers in the same file. `strategycore` is the existing alias for the `strategy` package; if the file uses a different alias, match it. `strings` is already imported.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./strategy/http/ -run "TestGetRunDecisions" -v`
Expected: PASS.

- [ ] **Step 5: Run the full http test package**

Run: `go test ./strategy/http/...`
Expected: PASS. The new tests join the existing suite.

- [ ] **Step 6: Stage and report**

```bash
git add strategy/http/handler.go strategy/http/handler_test.go
git status --short
```

Report the staged paths and stop.

---

## Task 7: OpenAPI — schemas, path, version bump

**Files:**
- Modify: `docs/openapi/strategy-server.json`

- [ ] **Step 1: Add the `Decision` and `DecisionList` schemas to `components.schemas`**

Find the closing `}` of the existing `components.schemas` block in `docs/openapi/strategy-server.json` (search for the last schema before `"responses":`). Append two new schemas just before the closing `}`. The field types match the JSON tags added in Task 1, with the `decimal.Decimal` shape as `{"type": "string"}` and the `time.Time` shape as `{"type": "string", "format": "date-time"}`.

```json
"Decision": {
  "type": "object",
  "description": "One trading decision recorded during a live strategy run. The row is the source of truth for the inputs that produced a single market order; clients should treat all fields as immutable audit data.",
  "properties": {
    "run_id":              { "type": "string", "format": "uuid" },
    "seq":                 { "type": "integer", "format": "int64" },
    "strategy_type":       { "type": "string" },
    "order_book_id":       { "type": "string", "format": "uuid" },
    "asset":               { "type": "string", "format": "uuid" },
    "side":                { "type": "string", "enum": ["BUY", "SELL"] },
    "signal":              { "type": "string", "enum": ["buy", "sell", "BUY", "SELL"] },
    "kind":                { "type": "string", "enum": ["open", "close", "extend"] },
    "quantity":            { "type": "string" },
    "price":               { "type": "string" },
    "leverage":            { "type": "string" },
    "inverse_leverage":    { "type": "string" },
    "from_global_position":{ "type": "boolean" },
    "reason":              { "type": "string" },
    "reason_detail":       { "type": "string" },
    "client_order_id":     { "type": "string" },
    "created_at":          { "type": "string", "format": "date-time" }
  },
  "required": [
    "run_id", "seq", "strategy_type", "order_book_id", "asset",
    "side", "signal", "kind", "quantity", "price", "leverage",
    "inverse_leverage", "from_global_position", "reason",
    "client_order_id", "created_at"
  ]
},
"DecisionList": {
  "type": "object",
  "description": "One page of trading decisions. next_cursor is omitted on the last page; clients should treat absent and empty string the same.",
  "properties": {
    "items": {
      "type": "array",
      "items": { "$ref": "#/components/schemas/Decision" }
    },
    "next_cursor": {
      "type": "string",
      "description": "Opaque continuation token. Pass back as the cursor query parameter to fetch the next page. Clients MUST NOT parse this value."
    }
  },
  "required": ["items"]
}
```

- [ ] **Step 2: Add the path entry**

Find the closing `}` of the existing `paths` block (the last path before `"components":`). Insert the new path before that closing brace, ordered alongside the other `/v1/...` entries:

```json
"/v1/trading-decisions/{run_id}": {
  "parameters": [
    {
      "name": "run_id",
      "in": "path",
      "required": true,
      "schema": { "type": "string", "format": "uuid" }
    },
    {
      "name": "from",
      "in": "query",
      "required": false,
      "description": "RFC3339 or YYYY-MM-DD. Inclusive lower bound on created_at. Malformed values are rejected with 400.",
      "schema": { "type": "string" }
    },
    {
      "name": "to",
      "in": "query",
      "required": false,
      "description": "RFC3339 or YYYY-MM-DD. Inclusive upper bound on created_at. Malformed values are rejected with 400.",
      "schema": { "type": "string" }
    },
    {
      "name": "limit",
      "in": "query",
      "required": false,
      "description": "Page size. Default 50, silently clamped to [1, 200].",
      "schema": { "type": "integer", "minimum": 1, "maximum": 200, "default": 50 }
    },
    {
      "name": "cursor",
      "in": "query",
      "required": false,
      "description": "Opaque continuation token returned by a previous call. Malformed values are rejected with 400.",
      "schema": { "type": "string" }
    }
  ],
  "get": {
    "operationId": "listTradingDecisions",
    "summary": "List the trading decisions recorded for a live strategy run, newest first",
    "description": "Returns the strategy_decisions rows for a single run owned by the authenticated user. Backtest runs are not represented in this table. The user is derived from the auth token, not the URL — wrong-user and not-found both produce 404. The cursor is opaque and versioned; do not parse it.",
    "security": [{ "ApiKeyAuth": [] }],
    "responses": {
      "200": {
        "description": "One page of decisions",
        "content": {
          "application/json": {
            "schema": { "$ref": "#/components/schemas/DecisionList" }
          }
        }
      },
      "400": { "$ref": "#/components/responses/BadRequest" },
      "404": { "$ref": "#/components/responses/NotFound" },
      "500": { "$ref": "#/components/responses/InternalError" }
    }
  }
}
```

If `BadRequest` is not already a defined response in `components.responses`, add a small entry for it next to the others:

```json
"BadRequest": {
  "description": "Request was malformed",
  "content": {
    "application/json": {
      "schema": { "$ref": "#/components/schemas/Error" }
    }
  }
}
```

(If `Error` is not already a defined schema, add a minimal one: `{"type": "object", "properties": {"error": {"type": "string"}}, "required": ["error"]}`.)

- [ ] **Step 3: Bump the OpenAPI version**

Change `"version": "1.3.0"` to `"version": "1.4.0"`.

- [ ] **Step 4: Validate the JSON**

Run: `python3 -c "import json,sys; json.load(open('docs/openapi/strategy-server.json')); print('valid')"`
Expected: `valid`. If the file already has a JSON validation script in the Makefile or pre-commit hooks, use that instead. (This project does not appear to have one; the manual `python3` check is the simplest.)

- [ ] **Step 5: Stage and report**

```bash
git add docs/openapi/strategy-server.json
git status --short
```

Report the staged paths and stop.

---

## Task 8: MCP — `listTradingDecisions` client method

**Files:**
- Modify: `mcp/strategy_client.go`

- [ ] **Step 1: Add the method**

In `mcp/strategy_client.go`, append after the existing `getBacktestClosedTrades` method (line 159 area):

```go
// listTradingDecisions proxies GET /v1/trading-decisions/{runID} on
// strategy-server. The response shape is
// { "items": [...], "next_cursor": "..." }.
func (c *strategyClient) listTradingDecisions(ctx context.Context, runID string, params listTradingDecisionsParams) (map[string]any, error) {
	path := fmt.Sprintf("/v1/trading-decisions/%s", runID)
	q := url.Values{}
	if params.From != "" {
		q.Set("from", params.From)
	}
	if params.To != "" {
		q.Set("to", params.To)
	}
	if params.Limit > 0 {
		q.Set("limit", strconv.Itoa(params.Limit))
	}
	if params.Cursor != "" {
		q.Set("cursor", params.Cursor)
	}
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return doStrategyJSON[map[string]any](ctx, c, http.MethodGet, path, nil)
}

type listTradingDecisionsParams struct {
	From   string
	To     string
	Limit  int
	Cursor string
}
```

The imports already cover `fmt`, `net/url`, `strconv`, `net/http`, and `context`. No new imports.

- [ ] **Step 2: Build the MCP module to confirm the new method compiles**

Run: `cd mcp && go build ./...`
Expected: clean.

- [ ] **Step 3: Stage and report**

```bash
git add mcp/strategy_client.go
git status --short
```

Report the staged paths and stop.

---

## Task 9: MCP — register `list_trading_decisions` tool

**Files:**
- Modify: `mcp/tools_strategy.go`
- Modify: `mcp/server_test.go`

- [ ] **Step 1: Add the args struct**

In `mcp/tools_strategy.go`, append to the existing args-struct block (after `strategyCancelBacktestArgs`):

```go
type strategyListTradingDecisionsArgs struct {
	RunID  string `json:"run_id"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}
```

- [ ] **Step 2: Register the tool**

In `registerStrategyTools` (around line 121), append a new `s.AddTool` call after the existing backtest-cancel registration. Match the surrounding registration style:

```go
s.AddTool(
	mcp.NewTool("list_trading_decisions",
		mcp.WithDescription("List the trading decisions recorded for a live strategy run, newest first. Requires run_id. Optional from/to (RFC3339 or YYYY-MM-DD), limit (default 50, max 200), and cursor (opaque, from a previous call). The user is derived from the auth token, not the URL."),
		mcp.WithString("run_id", mcp.Required(), mcp.Description("Strategy run ID.")),
		mcp.WithString("from", mcp.Description("Inclusive lower bound on created_at; RFC3339 or YYYY-MM-DD.")),
		mcp.WithString("to", mcp.Description("Inclusive upper bound on created_at; RFC3339 or YYYY-MM-DD.")),
		mcp.WithNumber("limit", mcp.Description("Page size; default 50, silently clamped to [1, 200].")),
		mcp.WithString("cursor", mcp.Description("Opaque continuation token from a previous call.")),
	),
	mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args strategyListTradingDecisionsArgs) (*mcp.CallToolResult, error) {
		if args.RunID == "" {
			return mcp.NewToolResultError("run_id is required"), nil
		}
		result, err := client.listTradingDecisions(ctx, args.RunID, listTradingDecisionsParams{
			From:   args.From,
			To:     args.To,
			Limit:  args.Limit,
			Cursor: args.Cursor,
		})
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return jsonText(result)
	}),
)
```

If the surrounding tools use a different tool name prefix (e.g. `strategy_trading_decisions_list`), match it. The plan uses `list_trading_decisions` to match the spec.

- [ ] **Step 3: Write the failing MCP test**

Append to `mcp/server_test.go`. Follow the existing fixture pattern in the file. The plan assumes the file uses a stub `strategyClient` and an in-process MCP server; the test asserts that invoking the tool calls the right client method with the right args and returns the proxied body.

```go
func TestListTradingDecisionsTool(t *testing.T) {
	t.Parallel()
	// Construct a server with a stub client that captures the
	// (runID, params) the tool passes and returns a canned response.
	// Invoke the tool via the MCP client. Assert:
	//   - the captured runID equals the args.run_id
	//   - the captured params match the args the caller supplied
	//   - the tool result body contains the canned items / next_cursor
	//   - an empty run_id returns an error result, not a panic
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd mcp && go test ./... -run TestListTradingDecisionsTool -v`
Expected: FAIL (tool not registered) or compile error (handler signature mismatch).

- [ ] **Step 5: Run the test to verify it passes**

After the registration in Step 2 lands, run again: `cd mcp && go test ./... -run TestListTradingDecisionsTool -v`
Expected: PASS.

- [ ] **Step 6: Run the full MCP test package**

Run: `cd mcp && go test ./...`
Expected: PASS.

- [ ] **Step 7: Stage and report**

```bash
git add mcp/tools_strategy.go mcp/server_test.go
git status --short
```

Report the staged paths and stop.

---

## Task 10: Final verification

- [ ] **Step 1: Run the full test suite across both modules**

Run: `go test ./... && cd mcp && go test ./... && cd ..`
Expected: PASS in both modules.

- [ ] **Step 2: Run the linter**

Run: `golangci-lint run --timeout 5m ./...`
Expected: clean. If new lints surface (e.g. `gosec`, `errorlint`, `revive`), fix the lines that the linter flags; do not silence the lint.

- [ ] **Step 3: Run goimports**

Run: `goimports -w strategy/ mcp/ cmd/`
Expected: clean. (Or whatever the project's pre-commit runs; check `.pre-commit-config.yaml` for the exact command.)

- [ ] **Step 4: Run the pre-commit hook suite**

Run: `pre-commit run --all-files`
Expected: all hooks pass. If any fail, fix the underlying issue — do not bypass hooks with `--no-verify` or `git -c commit.gpgsign=false`.

- [ ] **Step 5: Stage any lint-driven changes and report**

```bash
git status --short
git add -A  # only if Step 2-4 produced changes
git status --short
```

Report the final staged paths. After user review and the commit command, the implementation is complete.

---

## Acceptance verification

The implementation is complete when:

- `go test ./...` and `cd mcp && go test ./...` both pass.
- `golangci-lint run` and `pre-commit run --all-files` both pass.
- `docs/openapi/strategy-server.json` is valid JSON, version 1.4.0, with the new path and schemas.
- The MCP server's tool list includes `list_trading_decisions`.
- A live `curl` against the strategy-server with a valid `Authorization` header returns 200 and a `{items, next_cursor?}` body for an owned run, and 404 for a wrong-user or missing run.
- The `Decision` JSON tags are locked by `TestDecision_JSONFieldNames`.

---

## Pre-implementation notes

The plan was finalised before the `DecisionReader` interface split (see "File structure" — the new read-side interface lives in `strategy/http/decision_store.go`, separate from the write-side `DecisionRecorder` in `strategy/decision.go`). When implementing Task 6, apply these mechanical corrections to the code blocks shown above:

1. **`Handler` struct field** — add `decisionReader DecisionReader` next to the existing `decisionStore strategycore.DecisionRecorder` field (around line 63 of `strategy/http/handler.go`).
2. **`WithDecisionReader` option** — add next to `WithDecisionStore` (around line 534), with the signature `func WithDecisionReader(reader DecisionReader) func(*Handler)`. The `DecisionReader` interface itself is declared in Task 3's Step 3.
3. **Nil-guard** — at the top of `getRunDecisions` (between `resolveDORAUserID` and `CheckRunExists`), add `if h.decisionReader == nil { writeError(w, http.StatusServiceUnavailable, "trading decisions endpoint is not configured"); return }`.
4. **Handler call** — `h.decisionStore.ListDecisions(ctx, params)` → `h.decisionReader.ListDecisions(ctx, params)`.
5. **Test imports** — replace the `strategyfakes` import line with the inline-fake setup. The handler tests do NOT use `strategyfakes` (that package only fakes `Service` and `Strategy`); they use `fakeRunStore` and `fakeDecisionReader` defined inline in the test file, following the `fakeDecisionRecorder` pattern in `strategy/copytrading/decision_test.go`. The `&strategyfakes.FakeService{}` is still used as the `Service` parameter to `NewHandler` (every existing test does this), so the `strategyfakes` import stays — but the test bodies reference `fakeRunStore` and `fakeDecisionReader`, not anything from `strategyfakes`.
6. **Test stubs** — replace the placeholder comment bodies in the seven `TestGetRunDecisions_*` functions with the inline-fake wiring (`WithRunStore(fakeRunStore)`, `WithDecisionReader(fakeDecisionReader)`). The full bodies are shown in the patches above; the corrected version is in this plan's Task 6 as updated.

The decision to add a separate `DecisionReader` interface (rather than extending `DecisionRecorder`) is locked in by the "File structure" section. Do not collapse the two interfaces; that would force `strategy/decision.go` to know about `ListDecisionsParams` and `Cursor`, which is a layering violation.
