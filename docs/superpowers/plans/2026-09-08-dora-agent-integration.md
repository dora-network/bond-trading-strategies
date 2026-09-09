# dora-agent Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fold the dora-agent service into `bond-trading-strategies` so that strategy-server serves the agent's HTTP API at `/v1/agent/*`, sharing one Postgres, one AES-256-GCM key under `ENCRYPTION_KEY`, and one auth gate (`strategy/http/requireAuth`). Delete the agent's standalone binary, admin CLI, admin listener, envelope encryption, and historical database connection.

**Architecture:** Surgical copy-and-adapt. Each agent `internal/*` package is copied into `internal/agent/*` of bond-trading-strategies. Seams that conflict with strategy-server's existing primitives (auth, secrets, DB pool, env vars, route prefix) are rewritten. `internal/agent/wiring.Wire(pool, encryptionKey, cfg, log)` constructs the agent runtime from the shared pgxpool and returns a `*Runtime` whose `Server.Routes()` is mounted at `/v1/agent/*` inside the existing authed mux. Agent's envelope encryption (DEK + KMS-wrap) is replaced with `strategy/http/crypto.go`'s AES-256-GCM under `ENCRYPTION_KEY`. Agent's `internal/history` package is replaced by `internal/agent/store/history_store.go` operating on the shared pool. Admin listener + `cmd/agent-cli` deleted. One consolidated migration creates the agent's tables in the existing tern migrator.

**Tech Stack:** Go 1.26, `pgx/v5` v5.10.0 (bumped from current 5.9.2), `dora-client-go` (already pinned), `dora-strategy-wasm` v0.3.3, `wazero` v1.12.0, `mozilla-ai/any-llm-go` v0.9.0, `testcontainers-go` + `postgres` module (already in agent's deps; promote to bond-trading-strategies' go.mod).

**Spec:** `docs/superpowers/specs/2026-09-08-dora-agent-integration-design.md`

**Branch:** `tan/feat-integrate-dora-agent`

**Pre-commit gate:** `pre-commit run --all-files` must be green after every task. Do NOT use `--no-verify`. Do NOT disable GPG signing.

**Commit gate:** Stage at end of each task. User reviews and commits.

---

## Implementation status (as of 2026-09-09)

Phases 0-3 + Phase 4 leaf packages + llm + config are landed and committed.
The remaining tasks (4.2 / 4.3 / 4.5 / 4.6 / 4.7 / 4.8 / 5.1 / 5.2 / 6.x / 7.1 / 9.1 / 10.1)
are **deferred - do them as one big semantic commit in a follow-up session**.

The plan's task-by-task ordering was optimistic: the agent packages share
several seams (envelope encryption, pgxpool, migration, schema namespace,
httpapi prefix) that must be rewritten simultaneously. Per-task green builds
are not achievable without doing the rewrites in a different order.

Concrete rewrites needed before the deferred tasks can land:

1. **`internal/agent/providerconfig`** - replace `internal/secrets.KMS/Seal/Unseal`
   with the new `internal/agent/secrets.Sealer` struct.
2. **`internal/agent/wasmruntime/registry`** - promote `dora-strategy-wasm` and
   `wazero` to direct go.mod deps (currently indirect; the importing code now
   exists).
3. **`internal/agent/tools/generate` and `internal/agent/strategies/servertest`** -
   remove references to `internal/migration` (deleted per spec). Replace with
   direct tern calls or remove the test helper entirely.
4. **`internal/agent/history`** - rewrite `New(ctx, dsn)` to accept a
   `*pgxpool.Pool` instead of opening its own `*sql.DB`. Replace
   `database/sql.QueryContext` calls with `pgxpool.Query`.
5. **`internal/agent/httpapi`** - copy the agent's httpapi package, drop its
   `AuthMiddleware`, add `RoutesAt(basePath)` parameter, wire secrets +
   authctx reads.
6. **All agent SQL** - schema-qualify every `INSERT INTO users` /
   `SELECT ... FROM users` etc. to `agent.users`. The migration places the
   tables in the `agent` schema; the code must follow.
7. **`internal/agent/wiring`** - construct the runtime from the shared pgxpool
   + encryption key + agent config + logger; mount at `/v1/agent/*` in
   `cmd/strategy-server/main.go`.

## Phase 0 — Pre-flight

### Task 0.1: Confirm both repos checked out and verify branch

**Files:** none

- [ ] **Step 0.1.1: Confirm working directory and branch**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
git branch --show-current
git status --short
```

Expected: `tan/feat-integrate-dora-agent` and only the staged spec file from brainstorming. If other files appear staged, stop and surface to the user.

- [ ] **Step 0.1.2: Confirm dora-agent repo is reachable**

```bash
ls /home/tanq/code/dora/repos/dora-services/dora-agent/development
```

Expected: AGENTS.md, cmd/, internal/, main.go, etc. If the path doesn't resolve, ask the user where dora-agent lives and adjust imports/paths accordingly.

- [ ] **Step 0.1.3: Confirm pre-commit passes on baseline**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -20
```

Expected: all hooks pass. If anything fails on baseline, stop and fix before proceeding.

### Task 0.2: Bump pgx/v5 dependency

**Files:**
- Modify: `go.mod`
- Modify: `go.sum`

- [ ] **Step 0.2.1: Update pgx/v5 to v5.10.0**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go get github.com/jackc/pgx/v5@v5.10.0
go mod tidy
```

Expected: `go.mod` line 10 changes from `v5.9.2` to `v5.10.0`. `go.sum` regenerated. No other changes.

- [ ] **Step 0.2.2: Verify build still passes**

```bash
go build ./...
```

Expected: exit 0, no errors.

- [ ] **Step 0.2.3: Verify pre-commit still passes**

```bash
pre-commit run --all-files 2>&1 | tail -10
```

Expected: all hooks green.

- [ ] **Step 0.2.4: Stage and commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
git add go.mod go.sum
git diff --cached --stat
```

Expected: only `go.mod` and `go.sum`. If anything else shows up, investigate.

Then prompt the user to commit with message `chore(deps): bump pgx/v5 to v5.10.0 for dora-agent compatibility`. Do not run `git commit` yourself.

### Task 0.3: Promote agent-only dependencies into bond-trading-strategies' go.mod

**Files:**
- Modify: `go.mod`
- Modify: `go.sum`

- [ ] **Step 0.3.1: Add dora-strategy-wasm, wazero, any-llm-go, testcontainers**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go get github.com/dora-network/dora-strategy-wasm@v0.3.3
go get github.com/tetratelabs/wazero@v1.12.0
go get github.com/mozilla-ai/any-llm-go@v0.9.0
go get github.com/testcontainers/testcontainers-go@v0.43.0
go get github.com/testcontainers/testcontainers-go/modules/postgres@v0.43.0
go get github.com/jackc/tern/v2@v2.4.1
go mod tidy
```

Expected: `go.mod` gains the six new direct deps. Indirect deps from the agent's existing go.sum will land in this repo's go.sum as transitive.

- [ ] **Step 0.3.2: Verify build**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./...
```

Expected: exit 0.

- [ ] **Step 0.3.3: Stage and commit**

Stage `go.mod` and `go.sum`. Prompt the user to commit with message `chore(deps): add dora-strategy-wasm, wazero, any-llm-go, testcontainers for dora-agent`.

---

## Phase 1 — Database

### Task 1.1: Build the consolidated agent schema

**Files:**
- Create: `/tmp/agent_schema_dump.sql` (build artifact, not committed)

- [ ] **Step 1.1.1: Start a fresh testcontainers Postgres and run all 15 agent migrations against it**

From any directory, run a one-off Go script that does this. Save as `/tmp/agent_schema_dump.sql` to use in step 2:

```bash
cd /home/tanq/code/dora/repos/dora-services/dora-agent/development
# Verify the agent builds and its migrations can be replayed. If agent's
# main.go doesn't expose a "run migrations" subcommand, write a tiny
# /tmp/agent_migrate.go helper and `go run /tmp/agent_migrate.go`.
go run ./cmd/agent-cli --help 2>&1 | head -20 || echo "agent-cli exists but flag may differ"
ls internal/store/migrations
```

Expected: list of 15 agent migration files. Confirm nothing else is needed.

- [ ] **Step 1.1.2: Use pg_dump to extract the final schema as a single CREATE script**

Use a Postgres of your choice. From a testcontainers Postgres or a local one:

```bash
PGPASSWORD=postgres pg_dump \
  --host=localhost --port=5432 --user=postgres --dbname=postgres \
  --schema-only --no-owner --no-privileges \
  --exclude-table=schema_version \
  -t users -t sessions -t messages -t provider_configs \
  -t strategies -t strategy_versions -t strategy_capture_pending \
  -t deployments -t audit_log -t server_dora_credentials \
  > /tmp/agent_schema_dump.sql
wc -l /tmp/agent_schema_dump.sql
```

Expected: a SQL file with CREATE TABLE statements for all 10 tables.

- [ ] **Step 1.1.3: Audit for table-name collisions**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
grep -E '^CREATE TABLE' /tmp/agent_schema_dump.sql | awk '{print $3}' | sort > /tmp/agent_tables.txt
grep -E '^CREATE TABLE' migrations/*.sql | awk '{print $3}' | sort > /tmp/host_tables.txt
comm -12 /tmp/host_tables.txt /tmp/agent_tables.txt
```

Expected: empty output. If anything collides, edit `/tmp/agent_schema_dump.sql` to rename the agent-side table (e.g. `agent_users`) and update the dump accordingly.

- [ ] **Step 1.1.4: Schema-qualify every reference under the `agent` schema**

The agent's tables are created in a dedicated `agent` schema (not `public`). All CREATE TABLE / ALTER TABLE / CREATE INDEX / FK targets in the consolidated migration use the `agent.` prefix. The migration begins with `create schema if not exists agent;` and `set search_path to agent, public;` so unqualified references resolve correctly during the migration run, but every reference in the file body is schema-qualified.

The agent's `server_dora_credentials.api_key` column must be a single `bytea` column — no `_dek` columns. Edit the dump to remove any `api_key_dek` columns or related DEK artifacts from the agent's `009_server_admin_dora_key.sql`-derived DDL:

```bash
grep -E '(_dek|DEK|wrapped)' /tmp/agent_schema_dump.sql
```

Expected: empty output. If anything appears, drop those columns from the dump.

- [ ] **Step 1.1.5: Stage the dump for review (do not commit yet)**

```bash
cp /tmp/agent_schema_dump.sql /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development/migrations/015_agent_consolidated_schema.sql
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
git diff --cached migrations/015_agent_consolidated_schema.sql | head -100
```

Expected: a CREATE TABLE block per agent table. DEK columns absent.

### Task 1.2: Wire the consolidated migration into tern

**Files:**
- Create: `migrations/015_agent_consolidated_schema.sql` (already staged above)

- [ ] **Step 1.2.1: Run tern against a fresh local Postgres**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
# Bring up a fresh Postgres if not already running. Adjust to your local setup.
docker run --rm -d --name bts-tern -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16-alpine
sleep 2
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres \
  tern migrate --config migrations/tern.conf
```

Expected: tern reports `15 migrations applied` (or the relevant count including existing migrations). Inspect:

```bash
PGPASSWORD=postgres psql -h localhost -U postgres -d postgres -c "\dt"
```

Expected: all 14 existing tables + the 10 new agent tables.

- [ ] **Step 1.2.2: Tear down the test Postgres**

```bash
docker stop bts-tern
```

- [ ] **Step 1.2.3: Prompt user to commit the migration**

Stage `migrations/015_agent_consolidated_schema.sql`. Prompt the user to commit with message `feat(db): add consolidated agent schema under agent schema (sessions, messages, strategies, deployments, audit, server_dora_credentials, wasm, deployments, etc.)`.

---

## Phase 2 — Crypto seam

### Task 2.1: Move strategy/http/crypto.go to internal/secrets/crypto.go

**Files:**
- Create: `internal/secrets/crypto.go`
- Modify: `strategy/http/handler.go` (import path)
- Modify: `strategy/http/run_store.go` (or wherever `encryptAPIKey`/`decryptAPIKey` is called)
- Test: existing test `strategy/http/crypto_test.go` (file path changes)

- [ ] **Step 2.1.1: Find all callers of `encryptAPIKey`/`decryptAPIKey`**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
grep -rn 'encryptAPIKey\|decryptAPIKey' --include='*.go' .
```

Expected output: ~3 hits — the definitions in `strategy/http/crypto.go` and 1-2 callers in `strategy/http/`. Save the caller file paths for step 2.1.3.

- [ ] **Step 2.1.2: Create `internal/secrets/crypto.go` with the renamed functions**

```bash
mkdir -p internal/secrets
```

Create `internal/secrets/crypto.go`:

```go
// Package secrets owns AES-256-GCM helpers used across the service to
// seal sensitive bytes (per-user Dora API keys, the server-held admin
// Dora key, per-user LLM provider credentials) at rest. The same key
// (ENCRYPTION_KEY, 32 bytes hex-decoded) is used for every secret; the
// caller is responsible for binding a key to its use site.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

// Encrypt seals plaintext under key using AES-256-GCM. The returned
// ciphertext is `nonce || ct || tag`. The caller supplies a 32-byte key.
func Encrypt(plaintext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt opens a ciphertext produced by Encrypt. Returns an error if
// the ciphertext is shorter than the GCM nonce size.
func Decrypt(ciphertext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short: got %d bytes, need at least %d", len(ciphertext), nonceSize)
	}
	return gcm.Open(nil, ciphertext[:nonceSize], ciphertext[nonceSize:], nil)
}
```

- [ ] **Step 2.1.3: Update callers in `strategy/http/` to use the new package**

For each file from step 2.1.1 that calls `encryptAPIKey`/`decryptAPIKey`:

```go
import (
    // remove "github.com/dora-network/bond-trading-strategies/strategy/http" if it only existed for the helpers — leave it if it's still needed.
    "github.com/dora-network/bond-trading-strategies/internal/secrets"
)

// Replace call sites:
secrets.Encrypt(plaintext, key)   // was http.encryptAPIKey
secrets.Decrypt(ciphertext, key)  // was http.decryptAPIKey
```

Adjust the import block and call sites per file. Don't change semantics — `Encrypt`/`Decrypt` are byte-for-byte identical to `encryptAPIKey`/`decryptAPIKey` (same code, just renamed and exported).

- [ ] **Step 2.1.4: Delete `strategy/http/crypto.go`**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
git rm strategy/http/crypto.go
```

- [ ] **Step 2.1.5: Move the test file and update its package**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
mkdir -p internal/secrets
# Read strategy/http/crypto_test.go to see what it tests; if it only tests
# encryptAPIKey/decryptAPIKey, move the file:
git mv strategy/http/crypto_test.go internal/secrets/crypto_test.go
```

Update the test file's package declaration:

```go
package secrets
```

Update the test function names if needed:

```go
func TestEncryptDecrypt_RoundTrip(t *testing.T) {
    key := bytes.Repeat([]byte{0x42}, 32)
    plaintext := []byte("hello")
    sealed, err := Encrypt(plaintext, key)
    if err != nil { t.Fatal(err) }
    got, err := Decrypt(sealed, key)
    if err != nil { t.Fatal(err) }
    if !bytes.Equal(got, plaintext) {
        t.Fatalf("round-trip mismatch: got %q want %q", got, plaintext)
    }
}
```

Drop any internal-package test (`package http`) and any test of removed helpers.

- [ ] **Step 2.1.6: Verify build + tests pass**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./...
go test ./internal/secrets/... ./strategy/http/...
```

Expected: all pass.

- [ ] **Step 2.1.7: Verify pre-commit passes**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

Expected: green.

- [ ] **Step 2.1.8: Stage and prompt for commit**

Stage `internal/secrets/crypto.go`, `internal/secrets/crypto_test.go`, `strategy/http/crypto.go` (delete), and any modified `strategy/http/*.go`. Prompt user to commit with message `refactor(crypto): move encryptAPIKey/decryptAPIKey to internal/secrets as Encrypt/Decrypt`.

### Task 2.2: Add internal/agent/secrets/seal.go thin wrappers

**Files:**
- Create: `internal/agent/secrets/seal.go`
- Test: `internal/agent/secrets/seal_test.go`

- [ ] **Step 2.2.1: Write the failing test**

Create `internal/agent/secrets/seal_test.go`:

```go
package secrets

import (
    "bytes"
    "crypto/rand"
    "testing"
)

func TestSealOpen_RoundTrip(t *testing.T) {
    key := make([]byte, 32)
    if _, err := rand.Read(key); err != nil {
        t.Fatal(err)
    }
    plaintext := []byte("a secret dora api key, base64 or similar")

    sealed, err := Seal(plaintext)
    if err != nil {
        t.Fatalf("seal: %v", err)
    }
    if bytes.Equal(sealed, plaintext) {
        t.Fatal("sealed output equals plaintext")
    }
    opened, err := Open(sealed)
    if err != nil {
        t.Fatalf("open: %v", err)
    }
    if !bytes.Equal(opened, plaintext) {
        t.Fatalf("round-trip mismatch")
    }
}

func TestSealOpen_DifferentCiphertextEachTime(t *testing.T) {
    plaintext := []byte("same input")
    s1, _ := Seal(plaintext)
    s2, _ := Seal(plaintext)
    if bytes.Equal(s1, s2) {
        t.Fatal("two seals of the same plaintext produced the same ciphertext (nonce reused?)")
    }
}
```

The test uses a package-level `key []byte` set via `SetKey` — see step 2.2.3.

- [ ] **Step 2.2.2: Run the test to verify it fails**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./internal/agent/secrets/... 2>&1 | tail -10
```

Expected: `package secrets` not found, or `Seal`/`Open` undefined.

- [ ] **Step 2.2.3: Implement seal.go**

Create `internal/agent/secrets/seal.go`:

```go
// Package secrets provides Seal/Open helpers for the agent's at-rest
// secrets (server_dora_credentials.api_key, provider_configs.api_key).
// It binds to the service-wide ENCRYPTION_KEY via SetKey, called once
// at startup from cmd/strategy-server/main.go.
package secrets

import (
    "sync"

    "github.com/dora-network/bond-trading-strategies/internal/secrets"
)

var (
    keyMu sync.RWMutex
    encKey []byte
)

// SetKey installs the service-wide 32-byte AES-256 key. Called once at
// startup; further calls overwrite the key (intentional — supports
// tests, not production hot-swap).
func SetKey(k []byte) {
    keyMu.Lock()
    defer keyMu.Unlock()
    encKey = append([]byte(nil), k...)
}

func getKey() []byte {
    keyMu.RLock()
    defer keyMu.RUnlock()
    return append([]byte(nil), encKey...)
}

// Seal encrypts plaintext using the service-wide key.
func Seal(plaintext []byte) ([]byte, error) {
    return secrets.Encrypt(plaintext, getKey())
}

// Open decrypts a ciphertext produced by Seal.
func Open(sealed []byte) ([]byte, error) {
    return secrets.Decrypt(sealed, getKey())
}
```

- [ ] **Step 2.2.4: Update the test to set the key in TestMain**

Add to `internal/agent/secrets/seal_test.go`:

```go
func TestMain(m *testing.M) {
    key := make([]byte, 32)
    for i := range key {
        key[i] = byte(i)
    }
    SetKey(key)
    m.Run()
}
```

- [ ] **Step 2.2.5: Run the test to verify it passes**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./internal/agent/secrets/...
```

Expected: PASS.

- [ ] **Step 2.2.6: Stage and prompt for commit**

Stage `internal/agent/secrets/seal.go` and `internal/agent/secrets/seal_test.go`. Prompt user to commit with message `feat(agent/secrets): add Seal/Open thin wrappers around internal/secrets`.

---

## Phase 3 — Auth seam

### Task 3.1: Add internal/agent/authcache.go

**Files:**
- Create: `internal/agent/authcache.go`
- Test: `internal/agent/authcache_test.go`

- [ ] **Step 3.1.1: Write the failing test**

Create `internal/agent/authcache_test.go`:

```go
package agent

import (
    "context"
    "errors"
    "testing"
    "time"
)

type stubResolver struct {
    calls int
    user  string
    err   error
}

func (s *stubResolver) Resolve(ctx context.Context, apiKey string) (string, error) {
    s.calls++
    return s.user, s.err
}

func TestAuthCache_HitWithinTTL(t *testing.T) {
    s := &stubResolver{user: "u1"}
    c := newAuthCache(s, 5*time.Minute)

    u, err := c.Resolve(context.Background(), "key1")
    if err != nil || u != "u1" {
        t.Fatalf("first call: got (%q, %v)", u, err)
    }
    u, err = c.Resolve(context.Background(), "key1")
    if err != nil || u != "u1" {
        t.Fatalf("second call: got (%q, %v)", u, err)
    }
    if s.calls != 1 {
        t.Fatalf("expected resolver called once, got %d", s.calls)
    }
}

func TestAuthCache_MissAfterTTL(t *testing.T) {
    s := &stubResolver{user: "u1"}
    c := newAuthCache(s, 1*time.Millisecond)

    _, _ = c.Resolve(context.Background(), "key1")
    time.Sleep(10 * time.Millisecond)
    _, _ = c.Resolve(context.Background(), "key1")
    if s.calls != 2 {
        t.Fatalf("expected resolver called twice after TTL, got %d", s.calls)
    }
}

func TestAuthCache_PropagatesError(t *testing.T) {
    s := &stubResolver{err: errors.New("dora unreachable")}
    c := newAuthCache(s, 5*time.Minute)

    _, err := c.Resolve(context.Background(), "key1")
    if err == nil {
        t.Fatal("expected error from resolver to propagate")
    }
    if s.calls != 1 {
        t.Fatalf("expected resolver called once on error, got %d", s.calls)
    }
}
```

- [ ] **Step 3.1.2: Run test to verify failure**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./internal/agent/... 2>&1 | tail -10
```

Expected: undefined `newAuthCache`.

- [ ] **Step 3.1.3: Implement authcache.go**

Create `internal/agent/authcache.go`:

```go
// Package agent — authcache is a tiny TTL cache for `/v1/user/self`
// lookups. The agent's HTTP layer uses this to avoid hitting Dora on
// every inbound `/v1/agent/*` request. The cache is keyed by the raw
// API key string and bounded by DORA_AUTH_CACHE_TTL.
package agent

import (
    "context"
    "sync"
    "time"
)

// Resolver is the upstream Dora /v1/user/self lookup the cache wraps.
// Implemented by the dora-client-go SDK via a small adapter built at
// startup.
type Resolver interface {
    Resolve(ctx context.Context, apiKey string) (userID string, err error)
}

type cacheEntry struct {
    userID    string
    expiresAt time.Time
}

type authCache struct {
    resolver Resolver
    ttl      time.Duration

    mu      sync.Mutex
    entries map[string]cacheEntry
}

func newAuthCache(r Resolver, ttl time.Duration) *authCache {
    return &authCache{
        resolver: r,
        ttl:      ttl,
        entries:  make(map[string]cacheEntry),
    }
}

func (c *authCache) Resolve(ctx context.Context, apiKey string) (string, error) {
    c.mu.Lock()
    if e, ok := c.entries[apiKey]; ok && time.Now().Before(e.expiresAt) {
        c.mu.Unlock()
        return e.userID, nil
    }
    c.mu.Unlock()

    userID, err := c.resolver.Resolve(ctx, apiKey)
    if err != nil {
        return "", err
    }

    c.mu.Lock()
    c.entries[apiKey] = cacheEntry{
        userID:    userID,
        expiresAt: time.Now().Add(c.ttl),
    }
    c.mu.Unlock()

    return userID, nil
}
```

- [ ] **Step 3.1.4: Run test to verify pass**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test -race ./internal/agent/...
```

Expected: PASS.

- [ ] **Step 3.1.5: Stage and prompt for commit**

Stage `internal/agent/authcache.go` and `internal/agent/authcache_test.go`. Prompt user to commit with message `feat(agent): add TTL auth cache for Dora /v1/user/self lookups`.

---

## Phase 4 — Copy agent packages

The agent has ~20 internal packages. They copy as-is to `internal/agent/*` with one of three treatments:
- **Leaf (no modifications)** — pure copy with package rename.
- **Modified seam (auth/secrets/DB)** — copied with the seam rewritten in the same task.
- **Wiring** — done in Phase 5.

### Task 4.1: Copy leaf packages (no modifications)

**Files:**
- Create: `internal/agent/{audit,orderbroker,sanitize,safety,scan}/<files>.go`

- [ ] **Step 4.1.1: Identify the leaf packages**

```bash
cd /home/tanq/code/dora/repos/dora-services/dora-agent/development
ls internal/audit internal/orderbroker internal/sanitize internal/safety internal/scan
```

These are packages that don't import any of the modified seams (`internal/auth`, `internal/secrets`, `internal/store`, `internal/history`). Verify with:

```bash
cd /home/tanq/code/dora/repos/dora-services/dora-agent/development
for pkg in audit orderbroker sanitize safety scan; do
  echo "=== $pkg ==="
  grep -h '"github.com/dora-network/dora-agent/internal/' "internal/$pkg"/*.go | sort -u
done
```

Expected: none of these import auth, secrets, store, history, serveradmin. If they do, move that package to a later task with seam modifications.

- [ ] **Step 4.1.2: Copy with package-name renames**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
mkdir -p internal/agent
for pkg in audit orderbroker sanitize safety scan; do
  cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/$pkg internal/agent/$pkg
  # Rename package declarations: `package audit` -> `package audit` (keep, it's not `agent`)
  # Actually keep the package name as-is so cross-package imports read naturally.
  # In each .go file under internal/agent/$pkg, replace imports:
  for f in internal/agent/$pkg/*.go; do
    sed -i 's|"github.com/dora-network/dora-agent/internal/|"github.com/dora-network/bond-trading-strategies/internal/agent/|g' "$f"
  done
done
ls internal/agent/audit internal/agent/orderbroker internal/agent/sanitize internal/agent/safety internal/agent/scan
```

Expected: all five directories populated. All inter-agent imports rewritten to point to the new module path.

- [ ] **Step 4.1.3: Build and resolve imports**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./internal/agent/audit/... ./internal/agent/orderbroker/... ./internal/agent/sanitize/... ./internal/agent/safety/... ./internal/agent/scan/...
```

Expected: build fails with import errors. Each error tells you which package still references the old dora-agent module path. Fix by hand (the sed above may have missed comments or string literals).

- [ ] **Step 4.1.4: Iterate until build is clean**

Loop step 4.1.3 until `go build` returns exit 0.

- [ ] **Step 4.1.5: Run their existing tests**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./internal/agent/audit/... ./internal/agent/orderbroker/... ./internal/agent/sanitize/... ./internal/agent/safety/... ./internal/agent/scan/...
```

Expected: all pass.

- [ ] **Step 4.1.6: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

Expected: green.

- [ ] **Step 4.1.7: Stage and prompt for commit**

Stage all of `internal/agent/{audit,orderbroker,sanitize,safety,scan}/`. Prompt user to commit with message `feat(agent): copy leaf packages (audit, orderbroker, sanitize, safety, scan) from dora-agent`.

### Task 4.2: Copy tools packages (dora, backtest, deployment, generate)

**Files:**
- Create: `internal/agent/tools/{dora,backtest,deployment,generate}/<files>.go`

The tools packages import `internal/secrets` and `internal/store` — those seams will be rewritten in place during copy.

- [ ] **Step 4.2.1: Copy each tools subpackage**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
mkdir -p internal/agent/tools
for pkg in dora backtest deployment generate; do
  cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/tools/$pkg internal/agent/tools/$pkg
  for f in internal/agent/tools/$pkg/*.go; do
    sed -i 's|"github.com/dora-network/dora-agent/internal/|"github.com/dora-network/bond-trading-strategies/internal/agent/|g' "$f"
  done
done
ls internal/agent/tools/
```

- [ ] **Step 4.2.2: Rewrite the secrets seam in each tool**

Each tool that previously imported `internal/secrets` (the agent's old envelope package) must now import `internal/agent/secrets`:

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
grep -rln '"github.com/dora-network/dora-agent/internal/secrets"' internal/agent/tools/
```

For each file, replace the import path and call sites:

```go
// old
sealed, wrapped, err := secrets.Seal(ctx, kms, plaintext)
plaintext, err := secrets.Unseal(ctx, kms, sealed, wrapped)

// new (AES-256-GCM under ENCRYPTION_KEY, no DEK)
sealed, err := agentsecrets.Seal(plaintext)
plaintext, err := agentsecrets.Open(sealed)
```

Add the import:

```go
agentsecrets "github.com/dora-network/bond-trading-strategies/internal/agent/secrets"
```

Drop any `secrets.KMS`, `secrets.NewEnvKMS`, or `*kms` field that's no longer used.

- [ ] **Step 4.2.3: Build, fix any leftover imports, iterate until clean**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./internal/agent/tools/... 2>&1 | head -30
```

Iterate fixing remaining `dora-agent/internal/*` paths until exit 0.

- [ ] **Step 4.2.4: Run tests**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./internal/agent/tools/...
```

Expected: pass. If a test relied on the old envelope encryption format, update it to call `agentsecrets.SetKey(...)` in `TestMain` and use `Seal`/`Open` directly.

- [ ] **Step 4.2.5: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 4.2.6: Stage and prompt for commit**

Stage all of `internal/agent/tools/`. Prompt user to commit with message `feat(agent/tools): copy dora, backtest, deployment, generate packages; rewrite secrets seam`.

### Task 4.3: Copy store packages (session, users, providerconfig, strategies, deployment, backtest)

**Files:**
- Create: `internal/agent/store/{session,users,providerconfig,strategies,deployment,backtest}/<files>.go`

These packages implement Postgres-backed stores using their own pool. Replace the pool constructor with one accepting the shared `*pgxpool.Pool`.

- [ ] **Step 4.3.1: Copy each store subpackage**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
mkdir -p internal/agent/store
for pkg in session users providerconfig strategies deployment backtest; do
  cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/$pkg internal/agent/store/$pkg
  for f in internal/agent/store/$pkg/*.go; do
    sed -i 's|"github.com/dora-network/dora-agent/internal/|"github.com/dora-network/bond-trading-strategies/internal/agent/|g' "$f"
  done
done
```

- [ ] **Step 4.3.2: Replace each store's pool constructor signature**

For each store, find its `NewXxxStore` constructor that opens its own `pgxpool.Pool` and rewrite it to accept a `*pgxpool.Pool` argument:

```go
// OLD signature
func NewSessionStore(ctx context.Context, dsn string) (*SessionStore, error)

// NEW signature
func NewSessionStore(pool *pgxpool.Pool) (*SessionStore, error)
```

Apply this pattern across `session`, `users`, `providerconfig`, `strategies`, `deployment`, `backtest`. Drop any internal `pgxpool.New` calls in these packages.

- [ ] **Step 4.3.3: Build, fix imports, iterate**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./internal/agent/store/... 2>&1 | head -30
```

Iterate fixing remaining `dora-agent/internal/*` paths and any caller of `NewSessionStore(ctx, dsn)` etc. until exit 0.

- [ ] **Step 4.3.4: Update store tests for the new signatures**

Tests that constructed stores from a DSN must now construct them from a `*pgxpool.Pool`. Use the existing test pattern: spin up a testcontainers Postgres, build the pool once in `TestMain`, pass to each store.

```go
// internal/agent/store/session/session_test.go (sketch)
func TestMain(m *testing.M) {
    pgC, err := postgres.RunContainer(context.Background(),
        testcontainers.WithImage("postgres:16-alpine"),
        postgres.WithDatabase("postgres"),
        testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections")),
    )
    if err != nil { panic(err) }
    pool, err := pgxpool.New(context.Background(), pgC.ConnectionString)
    if err != nil { panic(err) }
    // Run the consolidated migration against this DB
    if err := store.MigrateConn(context.Background(), /*...*/); err != nil { panic(err) }
    globalPool = pool
    code := m.Run()
    pgC.Terminate(context.Background())
    os.Exit(code)
}

var globalPool *pgxpool.Pool

func TestFoo(t *testing.T) {
    s, err := NewSessionStore(globalPool)
    // ...
}
```

Apply this pattern across the six store packages. Some packages may already have a fixture helper — extend it rather than duplicating.

- [ ] **Step 4.3.5: Run tests**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test -race ./internal/agent/store/...
```

Expected: pass. May be slow (testcontainers).

- [ ] **Step 4.3.6: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 4.3.7: Stage and prompt for commit**

Stage all of `internal/agent/store/`. Prompt user to commit with message `feat(agent/store): copy session/users/providerconfig/strategies/deployment/backtest; switch to shared pgxpool`.

### Task 4.4: Copy LL

M + config

**Files:**
- Create: `internal/agent/llm/<files>.go`
- Create: `internal/agent/config/<files>.go`

The LLM package wraps `mozilla-ai/any-llm-go`. The config package reads agent-specific env vars.

- [ ] **Step 4.4.1: Copy LLM package**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
mkdir -p internal/agent/llm
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/llm/* internal/agent/llm/
for f in internal/agent/llm/*.go; do
  sed -i 's|"github.com/dora-network/dora-agent/internal/|"github.com/dora-network/bond-trading-strategies/internal/agent/|g' "$f"
done
go build ./internal/agent/llm/... 2>&1 | head -20
```

Iterate fixing imports until exit 0.

- [ ] **Step 4.4.2: Copy config package and apply env-var renames**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
mkdir -p internal/agent/config
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/config/* internal/agent/config/
for f in internal/agent/config/*.go; do
  sed -i 's|"github.com/dora-network/dora-agent/internal/|"github.com/dora-network/bond-trading-strategies/internal/agent/|g' "$f"
done
```

Apply the env-var renames from spec §6:

| Old | New |
| --- | --- |
| `AGENT_DORA_BASE_URL` | `DORA_BASE_URL` |
| `AGENT_LOG_LEVEL` | `LOG_LEVEL` |
| `AGENT_CORS_ALLOWED_ORIGINS` | `CORS_ALLOWED_ORIGINS` |
| `AGENT_AUTH_CACHE_TTL` | `DORA_AUTH_CACHE_TTL` |

Find each in `internal/agent/config/config.go` (the agent's `Env*` constants) and rename. Update the corresponding default-key fallback in `Load()`.

Drop these entirely from the agent's `config.go`:
- `EnvPostgresDSN` (`AGENT_POSTGRES_DSN`)
- `EnvMasterKey` (`AGENT_MASTER_KEY`)
- `EnvAdminAddr`, `EnvAdminTokenHash` (`AGENT_ADMIN_ADDR`, `AGENT_ADMIN_TOKEN_HASH`)
- `EnvAllowedDoraRoles` (`AGENT_ALLOWED_DORA_ROLES`)

Keep these (already in spec §6 "Kept as-is"):
- `AGENT_RATE_LIMIT_PER_MIN`, `AGENT_LLM_*`, `AGENT_MAX_PROMPT_BYTES`, `AGENT_MODEL_CAPS_PATH`, `AGENT_DORA_TOOLS_ENABLED`, `AGENT_GENERATE_*`, `AGENT_LIVE_*`, `AGENT_WSBROKER_URL`, `AGENT_CAPTURE_PENDING_*`, `AGENT_WASM_ARTIFACT_ROOT`, `AGENT_ALLOW_HTTP_BASE_URL`.

- [ ] **Step 4.4.3: Build, fix, iterate**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./internal/agent/config/... ./internal/agent/llm/... 2>&1 | head -30
```

Iterate.

- [ ] **Step 4.4.4: Run tests**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./internal/agent/config/... ./internal/agent/llm/...
```

Update tests for renamed env vars and dropped fields.

- [ ] **Step 4.4.5: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 4.4.6: Stage and prompt for commit**

Stage `internal/agent/llm/` and `internal/agent/config/`. Prompt user to commit with message `feat(agent): copy llm + config; apply env-var renames per spec §6`.

### Task 4.5: Copy wasmruntime and wsbroker

**Files:**
- Create: `internal/agent/wasmruntime/<files>.go`
- Create: `internal/agent/wsbroker/<files>.go`

- [ ] **Step 4.5.1: Copy wasmruntime**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
mkdir -p internal/agent/wasmruntime
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/wasmruntime/* internal/agent/wasmruntime/
for f in internal/agent/wasmruntime/*.go; do
  sed -i 's|"github.com/dora-network/dora-agent/internal/|"github.com/dora-network/bond-trading-strategies/internal/agent/|g' "$f"
done
go build ./internal/agent/wasmruntime/... 2>&1 | head -20
```

Iterate until clean.

- [ ] **Step 4.5.2: Copy wsbroker**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
mkdir -p internal/agent/wsbroker
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/wsbroker/* internal/agent/wsbroker/
for f in internal/agent/wsbroker/*.go; do
  sed -i 's|"github.com/dora-network/dora-agent/internal/|"github.com/dora-network/bond-trading-strategies/internal/agent/|g' "$f"
done
go build ./internal/agent/wsbroker/... 2>&1 | head -20
```

Iterate until clean.

- [ ] **Step 4.5.3: Run tests**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./internal/agent/wasmruntime/... ./internal/agent/wsbroker/...
```

Expected: pass.

- [ ] **Step 4.5.4: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 4.5.5: Stage and prompt for commit**

Stage both packages. Prompt user to commit with message `feat(agent): copy wasmruntime and wsbroker`.

### Task 4.6: Copy orchestrator + janitor

**Files:**
- Create: `internal/agent/orchestrator/<files>.go`
- Create: `internal/agent/janitor/<files>.go`

- [ ] **Step 4.6.1: Copy orchestrator**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
mkdir -p internal/agent/orchestrator
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/orchestrator/* internal/agent/orchestrator/
for f in internal/agent/orchestrator/*.go; do
  sed -i 's|"github.com/dora-network/dora-agent/internal/|"github.com/dora-network/bond-trading-strategies/internal/agent/|g' "$f"
done
go build ./internal/agent/orchestrator/... 2>&1 | head -20
```

Iterate.

- [ ] **Step 4.6.2: Copy janitor**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
mkdir -p internal/agent/janitor
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/orchestrator/janitor/*.go internal/agent/janitor/ 2>/dev/null \
  || cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/janitor/*.go internal/agent/janitor/
ls internal/agent/janitor/
```

If the agent's janitor lives inside `orchestrator`, take it from there. Otherwise locate and copy.

For each `.go` file:

```bash
for f in internal/agent/janitor/*.go; do
  sed -i 's|"github.com/dora-network/dora-agent/internal/|"github.com/dora-network/bond-trading-strategies/internal/agent/|g' "$f"
done
go build ./internal/agent/janitor/... 2>&1 | head -20
```

Iterate.

- [ ] **Step 4.6.3: Run tests**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./internal/agent/orchestrator/... ./internal/agent/janitor/...
```

- [ ] **Step 4.6.4: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 4.6.5: Stage and prompt for commit**

Stage `internal/agent/orchestrator/` and `internal/agent/janitor/`. Prompt user to commit with message `feat(agent): copy orchestrator + capture-pending janitor`.

### Task 4.7: Build internal/agent/store/history_store.go

**Files:**
- Create: `internal/agent/store/history_store.go`
- Test: `internal/agent/store/history_store_test.go`

This replaces the agent's `internal/history` package. Operates on the shared `*pgxpool.Pool`.

- [ ] **Step 4.7.1: Copy the agent's history.go into the new location**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
cp /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/history/history.go internal/agent/store/history_store.go
```

- [ ] **Step 4.7.2: Rewrite the file to use the shared pool**

In `internal/agent/store/history_store.go`:

```go
// Package store — history_store provides typed fetch functions for
// candles_history, trades_history, and price_history tables. It is the
// agent's replacement for the standalone dora-agent/internal/history
// package, rewritten to operate on the shared pgxpool.Pool instead of
// its own connection.
package store

import (
    "context"
    "database/sql"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
    db *sql.DB // wraps the pgxpool for compatibility with existing SQL
}

const fetchTimeout = 60 * time.Second

// New opens a database/sql.DB backed by the shared pgxpool. Caller
// passes a fresh pool; the wrapper is used so existing FetchCandles
// SQL can stay verbatim.
func New(ctx context.Context, pool *pgxpool.Pool) (*Store, error) {
    cfg := pool.Config()
    dsn := cfg.ConnConfig.ConnString()
    db, err := sql.Open("pgx", dsn)
    if err != nil {
        return nil, err
    }
    return &Store{db: db}, nil
}

func (s *Store) Close() error {
    if s.db == nil {
        return nil
    }
    return s.db.Close()
}

// ... copy FetchCandles, FetchTrades, FetchPrices verbatim from the
// original history.go, plus fetchCandlesSQL, fetchTradesSQL,
// fetchPricesSQL, encodeCursor, decodeCursor, resolutionSeconds,
// resolutionToSeconds.
```

Apply the rest of `history.go` verbatim. Adjust imports: drop `"database/sql"` if not needed, drop the old `New(ctx, dsn string)` signature's `database/sql.Open` with `pgx` driver registration; replace with the new `New(ctx, pool)` form above. The fetch functions don't change.

If `database/sql` proves awkward, rewrite the fetch functions to use `*pgxpool.Pool` directly. The trade-off is mechanical: `db.QueryContext(...)` becomes `pool.Query(ctx, ...)`, row scanning switches from `rows.Scan(&a, &b)` to pgx's `rows.Scan(&a, &b)` (pgx supports both styles). For mechanical copy, prefer the rewrite to pgx — drop the `*sql.DB` wrapper entirely.

- [ ] **Step 4.7.3: Build**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./internal/agent/store/... 2>&1 | head -30
```

Iterate.

- [ ] **Step 4.7.4: Write the integration test**

Create `internal/agent/store/history_store_test.go`:

```go
package store_test

import (
    "context"
    "os"
    "testing"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/testcontainers/testcontainers-go"
    "github.com/testcontainers/testcontainers-go/modules/postgres"
    "github.com/testcontainers/testcontainers-go/wait"

    "github.com/dora-network/bond-trading-strategies/internal/agent/store"
)

var (
    pool *pgxpool.Pool
    s    *store.Store
)

func TestMain(m *testing.M) {
    ctx := context.Background()
    pgC, err := postgres.RunContainer(ctx,
        testcontainers.WithImage("postgres:16-alpine"),
        postgres.WithDatabase("postgres"),
        testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections")),
    )
    if err != nil { panic(err) }
    defer pgC.Terminate(ctx)
    pool, err = pgxpool.New(ctx, pgC.ConnectionString())
    if err != nil { panic(err) }
    defer pool.Close()

    // Apply the consolidated migration. Use the existing tern migrator or
    // embed the migration files. See Task 1.1 for the dump.
    // Run migrations here:
    applyMigrations(ctx, pool)

    s, err = store.New(ctx, pool)
    if err != nil { panic(err) }
    defer s.Close()

    os.Exit(m.Run())
}

func TestFetchCandles_Basic(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    // Insert a known candle row, fetch it back
    _, err := pool.Exec(ctx,
        `INSERT INTO candles_history (order_book_id, start_timestamp, resolution, open, high, low, close, volume) VALUES ($1, $2, '1m', 100, 101, 99, 100.5, 10)`,
        "ob-1", time.Now().UTC().Truncate(time.Minute))
    if err != nil { t.Fatal(err) }
    rows, cursor, err := s.FetchCandles(ctx, "ob-1", time.Now().Add(-1*time.Hour), time.Now().Add(1*time.Hour), "1m", "", 100)
    if err != nil { t.Fatal(err) }
    if len(rows) == 0 { t.Fatal("expected at least one candle") }
    if rows[0].OrderBookID != "ob-1" { t.Fatalf("wrong order book id: %q", rows[0].OrderBookID) }
    if cursor != "" { t.Fatalf("expected empty cursor on last page, got %q", cursor) }
}
```

Apply analogous tests for `FetchTrades` and `FetchPrices`. Adapt the SQL inserts to the actual column names — verify by reading the consolidated migration.

- [ ] **Step 4.7.5: Run tests**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test -race ./internal/agent/store/
```

Expected: pass.

- [ ] **Step 4.7.6: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 4.7.7: Stage and prompt for commit**

Stage `internal/agent/store/history_store.go` and `internal/agent/store/history_store_test.go`. Prompt user to commit with message `feat(agent/store): add history_store (candles/trades/prices) on shared pgxpool`.

### Task 4.8: Copy httpapi package (with route-prefix + auth + secrets rewrites)

**Files:**
- Create: `internal/agent/httpapi/<files>.go`

This is the largest package. It owns the agent's HTTP server, middleware, and handlers.

- [ ] **Step 4.8.1: Copy httpapi**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
mkdir -p internal/agent/httpapi
cp -r /home/tanq/code/dora/repos/dora-services/dora-agent/development/internal/httpapi/* internal/agent/httpapi/
for f in internal/agent/httpapi/*.go; do
  sed -i 's|"github.com/dora-network/dora-agent/internal/|"github.com/dora-network/bond-trading-strategies/internal/agent/|g' "$f"
done
ls internal/agent/httpapi/
```

Expected: ~25 files (server.go, middleware.go, handlers.go, etc.).

- [ ] **Step 4.8.2: Drop agent-side auth middleware**

Delete `internal/agent/httpapi/middleware.go`'s `AuthMiddleware` function (it re-implements what strategy-server's `requireAuth` does). The agent's `httpapi.Server.Routes()` should no longer wrap with auth; the wiring in Phase 5 wraps the routes with `strategy/http.requireAuth` instead.

For each file in `internal/agent/httpapi/` that referenced `s.auth` (the agent's old authenticator):

```go
// remove references to s.auth, WithAuth, Authenticator interface, AuthMiddleware
```

Replace `s.auth.Validate(ctx, key)` calls with `auth.Principal` lookups that the upstream middleware already populated.

Specifically:
- Drop `WithAuth(a Authenticator) Option` from `server.go`.
- Drop the `AuthMiddleware` from `middleware.go` (or strip its body to a no-op for now; we'll delete the file in step 4.8.3).
- Update `PrincipalFromCtx` and `DoraAPIKeyFromCtx` to read from `authctx` instead of from a context key set by the agent's `AuthMiddleware`:

```go
// internal/agent/httpapi/authctx_bridge.go (new file)
package httpapi

import (
    "github.com/dora-network/bond-trading-strategies/authctx"
)

func principalFromCtx(ctx context.Context) string {
    // The agent's auth model assumed an explicit Principal; for the merged
    // service, the upstream requireAuth has already verified the API key
    // and resolved the user ID. Read it back from authctx if present.
    info, ok := authctx.AuthInfoFromContext(ctx)
    if !ok {
        return ""
    }
    // User ID isn't stored on authctx.AuthInfo today — extend it.
    return info.UserID  // requires adding UserID to authctx.AuthInfo; see Task 4.9
}

func doraAPIKeyFromCtx(ctx context.Context) string {
    info, ok := authctx.AuthInfoFromContext(ctx)
    if !ok {
        return ""
    }
    return info.APIKey
}
```

Step 4.9 adds `UserID` to `authctx.AuthInfo`. Until then, leave `principalFromCtx` returning "" and the per-user agent rate limiter is keyed by API key instead.

- [ ] **Step 4.8.3: Add `basePath` to `Routes()`**

In `internal/agent/httpapi/server.go`, change:

```go
func (s *Server) Routes() http.Handler {
    mux := http.NewServeMux()
    // ...
}

func (s *Server) RoutesAt(basePath string) http.Handler {
    mux := http.NewServeMux()
    mux.HandleFunc("GET "+basePath+"/healthz", ...)
    mux.HandleFunc("POST "+basePath+"/sessions", s.handleCreateSession)
    // ... every pattern prefixed with basePath
}
```

Mechanical: prefix every registered pattern with `basePath`. The strategy-server wiring calls `RoutesAt("/v1/agent")`.

- [ ] **Step 4.8.4: Build, fix imports, iterate**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./internal/agent/httpapi/... 2>&1 | head -30
```

Iterate. Expect errors referencing `s.auth`, `AuthMiddleware`, and the old secrets package.

- [ ] **Step 4.8.5: Update tests**

Tests that constructed the agent's `Authenticator` (a Dora client + cache) and called `WithAuth(...)` must be updated to construct the auth cache and inject it differently. Many agent tests will move to integration tests in Phase 5.

For now, update unit tests to construct the httpapi.Server without `WithAuth`. Mock the `principalFromCtx`/`doraAPIKeyFromCtx` lookups by stuffing authctx values directly into the request context in tests:

```go
ctx := authctx.WithAuthInfo(context.Background(), authctx.AuthInfo{APIKey: "key1", UserID: "u1"})
req := httptest.NewRequest("GET", "/v1/agent/sessions", nil).WithContext(ctx)
```

- [ ] **Step 4.8.6: Run tests; iterate until pass**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test -race ./internal/agent/httpapi/...
```

- [ ] **Step 4.8.7: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 4.8.8: Stage and prompt for commit**

Stage `internal/agent/httpapi/`. Prompt user to commit with message `feat(agent/httpapi): copy httpapi; drop agent AuthMiddleware, add RoutesAt(basePath)`.

### Task 4.9: Extend authctx.AuthInfo with UserID

**Files:**
- Modify: `authctx/authctx.go`
- Test: `authctx/authctx_test.go`

- [ ] **Step 4.9.1: Write the failing test**

In `authctx/authctx_test.go`:

```go
func TestAuthInfo_UserID(t *testing.T) {
    info := authctx.AuthInfo{APIKey: "k", UserID: "u1"}
    ctx := authctx.WithAuthInfo(context.Background(), info)
    got, ok := authctx.AuthInfoFromContext(ctx)
    if !ok { t.Fatal("expected AuthInfo from context") }
    if got.UserID != "u1" {
        t.Fatalf("got %q want %q", got.UserID, "u1")
    }
}
```

- [ ] **Step 4.9.2: Run test to verify failure**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./authctx/...
```

Expected: compile error — `info.UserID` undefined.

- [ ] **Step 4.9.3: Add UserID to AuthInfo**

In `authctx/authctx.go`, modify the struct:

```go
type AuthInfo struct {
    APIKey      string
    BearerToken string
    TenantID    string
    UserID      string  // NEW: verified Dora user UUID, populated by requireAuth after /v1/user/self succeeds.
}
```

- [ ] **Step 4.9.4: Run test to verify pass**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./authctx/...
```

Expected: PASS.

- [ ] **Step 4.9.5: Update strategy-server's requireAuth to populate UserID**

In `strategy/http/auth.go`, modify the `requireAuth` middleware to write the user ID:

```go
// existing code calls resolveUserID(ctx), which returns (userID, err).
// after:
authInfo.UserID = userID  // <-- add this line
ctx := authctx.WithAuthInfo(r.Context(), authInfo)
```

- [ ] **Step 4.9.6: Verify the strategy-server suite still passes**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./strategy/http/... ./authctx/...
```

Expected: pass.

- [ ] **Step 4.9.7: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 4.9.8: Stage and prompt for commit**

Stage `authctx/authctx.go`, `authctx/authctx_test.go`, `strategy/http/auth.go`. Prompt user to commit with message `feat(authctx): add UserID to AuthInfo; populate from requireAuth`.

---

## Phase 5 — Wiring

### Task 5.1: Create internal/agent/wiring package

**Files:**
- Create: `internal/agent/wiring/wiring.go`

- [ ] **Step 5.1.1: Write a smoke test that constructs the runtime**

Create `internal/agent/wiring/wiring_test.go`:

```go
package wiring_test

import (
    "context"
    "os"
    "testing"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/testcontainers/testcontainers-go/modules/postgres"
    "github.com/testcontainers/testcontainers-go/wait"

    agentcfg "github.com/dora-network/bond-trading-strategies/internal/agent/config"
    "github.com/dora-network/bond-trading-strategies/internal/agent/wiring"
    agentsecrets "github.com/dora-network/bond-trading-strategies/internal/agent/secrets"
)

func TestWire_Smoke(t *testing.T) {
    ctx := context.Background()
    pgC, err := postgres.RunContainer(ctx,
        testcontainers.WithImage("postgres:16-alpine"),
        testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections")))
    if err != nil { t.Fatal(err) }
    defer pgC.Terminate(ctx)

    pool, err := pgxpool.New(ctx, pgC.ConnectionString())
    if err != nil { t.Fatal(err) }
    defer pool.Close()
    applyMigrations(ctx, t, pool)

    key := bytes.Repeat([]byte{0x55}, 32)
    agentsecrets.SetKey(key)

    cfg := agentcfg.Config{
        // populate required fields with sensible test defaults
        // (DoraBaseURL set to a test server; LLM/WSBroker disabled)
    }
    log := slog.New(slog.NewTextHandler(os.Stderr, nil))

    rt, err := wiring.Wire(ctx, pool, key, cfg, log)
    if err != nil { t.Fatal(err) }
    defer rt.Close()

    if rt.Server == nil { t.Fatal("Server is nil") }
    if rt.Server.RoutesAt("/v1/agent") == nil { t.Fatal("RoutesAt returned nil") }
}
```

`applyMigrations` is a helper that runs the consolidated migration against the test DB. Copy from the existing `internal/store/migrator.go` pattern.

- [ ] **Step 5.1.2: Run test to verify failure**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./internal/agent/wiring/...
```

Expected: undefined `wiring.Wire`.

- [ ] **Step 5.1.3: Implement wiring.go**

```bash
mkdir -p internal/agent/wiring
```

Create `internal/agent/wiring/wiring.go`:

```go
// Package wiring constructs the agent runtime from the shared pgxpool,
// encryption key, and config. The returned *Runtime owns every agent
// dependency: stores, services, the WASM runtime, wsbroker, the live
// orchestrator, and the httpapi.Server. cmd/strategy-server/main.go
// calls Wire once at startup, then mounts Runtime.Server.RoutesAt on
// /v1/agent/*.
package wiring

import (
    "context"
    "fmt"
    "log/slog"

    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/dora-network/bond-trading-strategies/internal/agent/audit"
    agentcfg "github.com/dora-network/bond-trading-strategies/internal/agent/config"
    "github.com/dora-network/bond-trading-strategies/internal/agent/httpapi"
    "github.com/dora-network/bond-trading-strategies/internal/agent/janitor"
    "github.com/dora-network/bond-trading-strategies/internal/agent/orchestrator"
    agentsecrets "github.com/dora-network/bond-trading-strategies/internal/agent/secrets"
    "github.com/dora-network/bond-trading-strategies/internal/agent/safety"
    "github.com/dora-network/bond-trading-strategies/internal/agent/sanitize"
    "github.com/dora-network/bond-trading-strategies/internal/agent/store"
    backtestpkg "github.com/dora-network/bond-trading-strategies/internal/agent/store/backtest"
    deploystore "github.com/dora-network/bond-trading-strategies/internal/agent/store/deployment"
    pcstore "github.com/dora-network/bond-trading-strategies/internal/agent/store/providerconfig"
    sessionstore "github.com/dora-network/bond-trading-strategies/internal/agent/store/session"
    strstore "github.com/dora-network/bond-trading-strategies/internal/agent/store/strategies"
    userstore "github.com/dora-network/bond-trading-strategies/internal/agent/store/users"
    bthandlers "github.com/dora-network/bond-trading-strategies/internal/agent/tools/backtest"
    deploytools "github.com/dora-network/bond-trading-strategies/internal/agent/tools/deployment"
    gen "github.com/dora-network/bond-trading-strategies/internal/agent/tools/generate"
    "github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime"
    "github.com/dora-network/bond-trading-strategies/internal/agent/wsbroker"
)

type Runtime struct {
    Server      *httpapi.Server
    Janitor     *janitor.Janitor
    WSBroker    *wsbroker.Broker
    LiveOrch    *orchestrator.Orchestrator
    WASMRuntime *wasmruntime.Runtime
    History     *store.Store
    Sessions    *sessionstore.Store
    Users       *userstore.Store
    Providers   *pcstore.Store
    Strategies  *strstore.Store
    Backtests   *backtestpkg.Store
    Deployments *deploystore.Store
}

func Wire(
    ctx context.Context,
    pool *pgxpool.Pool,
    encryptionKey []byte,
    cfg agentcfg.Config,
    log *slog.Logger,
) (*Runtime, error) {
    agentsecrets.SetKey(encryptionKey)

    // 1. Stores
    sessions, err := sessionstore.New(pool)
    if err != nil { return nil, fmt.Errorf("sessions: %w", err) }
    users, err := userstore.New(pool)
    if err != nil { return nil, fmt.Errorf("users: %w", err) }
    providers, err := pcstore.New(pool)
    if err != nil { return nil, fmt.Errorf("providers: %w", err) }
    strategies, err := strstore.New(pool)
    if err != nil { return nil, fmt.Errorf("strategies: %w", err) }
    backtests, err := backtestpkg.New(pool)
    if err != nil { return nil, fmt.Errorf("backtests: %w", err) }
    deployments, err := deploystore.New(pool)
    if err != nil { return nil, fmt.Errorf("deployments: %w", err) }

    // 2. History store (shared pool)
    history, err := store.New(ctx, pool)
    if err != nil { return nil, fmt.Errorf("history: %w", err) }

    // 3. WASM runtime
    wasmRT, err := wasmruntime.New(wasmruntime.Config{ArtifactRoot: cfg.WasmArtifactRoot, MemoryLimit: cfg.LiveMemoryLimit})
    if err != nil { return nil, fmt.Errorf("wasm runtime: %w", err) }

    // 4. WS broker
    wsB := wsbroker.New(wsbroker.Config{DoraBaseURL: cfg.DoraBaseURL, WSBrokerURL: cfg.WsBrokerURL, Logger: log})

    // 5. Audit writer
    aw, err := audit.NewBatchingWriter(ctx, pool, audit.BatchingWriterConfig{})
    if err != nil { return nil, fmt.Errorf("audit: %w", err) }

    // 6. Backtest orchestrator + WASM starter
    btOrch := backtestpkg.NewOrchestrator(pool, backtests, history, aw, log)
    wasmStarter := backtestpkg.NewWasmStarter(wasmRT, history, log)

    // 7. Generate handler
    genHandler := gen.NewHandler(gen.Config{
        WasmRT: wasmRT,
        Strategies: strategies,
        Allowlist: cfg.Generate.Allowlist,
        MaxRepairs: cfg.Generate.MaxRepairs,
        Log: log,
    })

    // 8. Sanitize classifier
    classifier := sanitize.NewClassifier(sanitize.Config{ /* ... */ })

    // 9. Safety kernel
    sk := safety.NewKernel(safety.Config{ /* ... */ })

    // 10. Live orchestrator
    orch, err := orchestrator.New(ctx, orchestrator.Config{ /* ... */ })
    if err != nil { return nil, fmt.Errorf("orchestrator: %w", err) }

    // 11. httpapi.Server
    srv := httpapi.New(sessions, providers, httpapi.WithStrategies(strategies), httpapi.WithBacktest(btOrch, backtests), httpapi.WithWasmStarter(wasmStarter), httpapi.WithDeploymentStore(deployments), httpapi.WithOrchestrator(orch), httpapi.WithSafetyKernel(sk), httpapi.WithAuditWriter(aw), httpapi.WithAgentRunner(/* ... */))

    // 12. Janitor
    j := janitor.New(ctx, janitor.Config{Pool: pool, Retention: cfg.CapturePendingRetention, SweepInterval: cfg.CapturePendingSweepInterval})

    return &Runtime{
        Server: srv, Janitor: j, WSBroker: wsB, LiveOrch: orch,
        WASMRuntime: wasmRT, History: history, Sessions: sessions, Users: users,
        Providers: providers, Strategies: strategies, Backtests: backtests, Deployments: deployments,
    }, nil
}

func (r *Runtime) Close() error {
    if r.Janitor != nil { r.Janitor.Stop() }
    if r.WSBroker != nil { _ = r.WSBroker.Close() }
    if r.History != nil { _ = r.History.Close() }
    return nil
}
```

Specifics: copy the constructor argument lists from agent's `main.go` (which already builds this same runtime). The exact names, types, and option set must match what the agent already passes. If a constructor signature differs from this sketch, adjust.

- [ ] **Step 5.1.4: Run test**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./internal/agent/wiring/...
```

Iterate until pass.

- [ ] **Step 5.1.5: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 5.1.6: Stage and prompt for commit**

Stage `internal/agent/wiring/`. Prompt user to commit with message `feat(agent/wiring): add Wire(ctx, pool, key, cfg, log) runtime constructor`.

### Task 5.2: Mount agent runtime in cmd/strategy-server/main.go

**Files:**
- Modify: `cmd/strategy-server/main.go`

- [ ] **Step 5.2.1: Add flag for new agent-specific env vars**

Add flags near the existing rate-limit flags (around line 90):

```go
agentDoraBaseURL := flag.String("agent-dora-base-url", envOr("DORA_BASE_URL", ""), "Dora REST base URL for agent (default: DORA_BASE_URL)")
agentWSBrokerURL := flag.String("agent-ws-broker-url", envOr("AGENT_WSBROKER_URL", ""), "wsplex endpoint URL (optional)")
agentWasmArtifactRoot := flag.String("agent-wasm-artifact-root", envOr("AGENT_WASM_ARTIFACT_ROOT", "/var/lib/dora-agent/wasm-artifacts"), "writable directory for compiled WASM artifacts")
agentRateLimitPerMin := flag.Int("agent-rate-limit-per-min", envOrInt("AGENT_RATE_LIMIT_PER_MIN", 20), "per-user requests/min cap on /v1/agent/*")
agentLiveMemoryLimit := flag.Int64("agent-live-memory-limit", envOrInt64("AGENT_LIVE_MEMORY_LIMIT", 256*1024*1024), "per-instance wazero memory cap for live plugins")
agentLLMTimeout := flag.Duration("agent-llm-timeout", envOrDuration("AGENT_LLM_TIMEOUT", 10*time.Minute), "per-iteration LLM provider call timeout")
agentLLMMaxIters := flag.Int("agent-llm-max-iters", envOrInt("AGENT_LLM_MAX_ITERS", 50), "max LLM tool-call iterations per turn")
agentMaxPromptBytes := flag.Int("agent-max-prompt-bytes", envOrInt("AGENT_MAX_PROMPT_BYTES", 32*1024), "max bytes accepted in user prompts")
agentDoraToolsEnabled := flag.Bool("agent-dora-tools-enabled", envOrBool("AGENT_DORA_TOOLS_ENABLED", true), "enable the agent's read-only Dora tools")
agentModelCapsPath := flag.String("agent-model-caps-path", envOr("AGENT_MODEL_CAPS_PATH", "configs/model_caps.json"), "path to model capabilities JSON")
agentCapturePendingRetention := flag.Duration("agent-capture-pending-retention", envOrDuration("AGENT_CAPTURE_PENDING_RETENTION", 7*24*time.Hour), "capture-pending retention")
agentCapturePendingSweepInterval := flag.Duration("agent-capture-pending-sweep-interval", envOrDuration("AGENT_CAPTURE_PENDING_SWEEP_INTERVAL", 5*time.Minute), "capture-pending sweep interval")
agentLiveRestartWindow := flag.Duration("agent-live-restart-window", envOrDuration("AGENT_LIVE_RESTART_WINDOW", 60*time.Second), "live restart sliding window")
agentLiveMaxRestarts := flag.Int("agent-live-max-restarts", envOrInt("AGENT_LIVE_MAX_RESTARTS", 3), "live max restarts within window")
```

Add helpers `envOrInt64` and `envOrDuration` if not present:

```go
func envOrInt64(key string, fallback int64) int64 {
    v := os.Getenv(key)
    if v == "" { return fallback }
    n, err := strconv.ParseInt(v, 10, 64)
    if err != nil { return fallback }
    return n
}

func envOrDuration(key string, fallback time.Duration) time.Duration {
    v := os.Getenv(key)
    if v == "" { return fallback }
    d, err := time.ParseDuration(v)
    if err != nil { return fallback }
    return d
}
```

- [ ] **Step 5.2.2: Construct the agent runtime in main()**

After the existing `pgxpool.New` and before the existing strategy-handler chain:

```go
agentLog := slog.With("service", "agent")
agentCfg := agentcfg.Config{
    DoraBaseURL: *agentDoraBaseURL,
    WsBrokerURL: *agentWSBrokerURL,
    WasmArtifactRoot: *agentWasmArtifactRoot,
    RateLimitPerMin: *agentRateLimitPerMin,
    LiveMemoryLimit: uint64(*agentLiveMemoryLimit),
    LLMTimeout: *agentLLMTimeout,
    LLMMaxIters: *agentLLMMaxIters,
    MaxPromptBytes: *agentMaxPromptBytes,
    DoraToolsEnabled: *agentDoraToolsEnabled,
    ModelCapsPath: *agentModelCapsPath,
    CapturePendingRetention: *agentCapturePendingRetention,
    CapturePendingSweepInterval: *agentCapturePendingSweepInterval,
    LiveRestartWindow: *agentLiveRestartWindow,
    LiveMaxRestarts: *agentLiveMaxRestarts,
}
agentRT, err := agentwiring.Wire(ctx, pool, encryptionKey, agentCfg, agentLog)
if err != nil {
    slog.Error("agent wiring failed", "err", err)
    os.Exit(1)
}
defer agentRT.Close()
```

Add the import:

```go
import (
    agentcfg "github.com/dora-network/bond-trading-strategies/internal/agent/config"
    "github.com/dora-network/bond-trading-strategies/internal/agent/wiring"
)
```

- [ ] **Step 5.2.3: Mount agent routes at /v1/agent/* inside the authed mux**

Find the existing `strategyhttp.NewHandler(...)` call and how its result is served. Wrap the result with the agent handler mounted at `/v1/agent/*` inside the same authed subtree:

```go
strategyHandler := strategyhttp.NewHandler(service, /* ... existing opts ... */)

// Mount agent routes at /v1/agent/*. The strategy-server's existing
// CORS, rate-limit, and requireAuth middleware chains apply to both
// /v1/* and /v1/agent/*.
agentHandler := http.StripPrefix("/v1/agent", agentRT.Server.RoutesAt("/v1/agent"))
agentSubMux := http.NewServeMux()
agentSubMux.Handle("/v1/agent/", agentHandler)

// ... existing code that wraps strategyHandler with cors/ratelimit/auth.
// Insert a /v1/agent branch that routes to agentSubMux BEFORE the catchall
// strategyHandler. Pseudocode for the authed subtree:
authed := http.NewServeMux()
authed.Handle("/v1/agent/", agentRT.Server.RoutesAt("/v1/agent"))
authed.Handle("/", strategyHandler)
```

The exact wiring depends on how strategy-server currently composes its handlers. Read `cmd/strategy-server/main.go` lines 240-330 carefully before editing.

- [ ] **Step 5.2.4: Build**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./...
```

Iterate fixing type errors. Most common: missing options on `httpapi.Server` because we didn't wire them all in Task 5.1.

- [ ] **Step 5.2.5: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 5.2.6: Stage and prompt for commit**

Stage `cmd/strategy-server/main.go`. Prompt user to commit with message `feat(strategy-server): wire agent runtime; mount /v1/agent/* in authed subtree`.

---

## Phase 6 — Tests for the merged service

### Task 6.1: Test that /v1/agent/* is behind requireAuth

**Files:**
- Modify or create: `strategy/http/integration_test.go` (or a new file)

- [ ] **Step 6.1.1: Find the existing integration test pattern**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
ls strategy/http/ | grep -i test
head -50 strategy/http/handler_test.go
```

Look for a test that boots the full handler chain with `httptest`. Mirror its setup.

- [ ] **Step 6.1.2: Write the failing test**

Append to `strategy/http/integration_test.go` (or wherever the existing handler tests live):

```go
func TestAgentRoutes_RequireAuth(t *testing.T) {
    h := newTestHandlerWithAgent(/* ... */)
    cases := []struct{
        method, path string
        body io.Reader
    }{
        {"GET", "/v1/agent/sessions", nil},
        {"POST", "/v1/agent/sessions", strings.NewReader("{}")},
        {"GET", "/v1/agent/strategies", nil},
        {"POST", "/v1/agent/strategies/abc/backtests", strings.NewReader("{}")},
    }
    for _, c := range cases {
        req := httptest.NewRequest(c.method, c.path, c.body)
        rr := httptest.NewRecorder()
        h.ServeHTTP(rr, req)
        if rr.Code != http.StatusUnauthorized {
            t.Errorf("%s %s: got %d, want 401", c.method, c.path, rr.Code)
        }
    }
}
```

The fixture `newTestHandlerWithAgent` is built the same way as the existing handler tests, but with the agent runtime stubbed (use a nil-wired `*Runtime` whose `Server` is `httpapi.New(...)` with no stores wired; the test only exercises the auth wall).

- [ ] **Step 6.1.3: Run test to verify failure**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./strategy/http/... -run TestAgentRoutes_RequireAuth -v
```

Expected: either compile error (missing fixture) or test passes because the agent routes happen to 200. Adjust the fixture so the test fails (no auth header → 401).

- [ ] **Step 6.1.4: Implement fixture and re-run**

Build `newTestHandlerWithAgent` that mounts the agent routes inside the same authed chain as the existing test fixture. Run the test, expect 401 for every case.

- [ ] **Step 6.1.5: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 6.1.6: Stage and prompt for commit**

Stage the new test file. Prompt user to commit with message `test: confirm /v1/agent/* routes are behind requireAuth`.

### Task 6.2: Test that bare /v1/* does NOT serve agent routes

**Files:**
- Same as 6.1

- [ ] **Step 6.2.1: Write the failing test**

Append:

```go
func TestAgentRoutes_NotAtBareV1Path(t *testing.T) {
    h := newTestHandlerWithAgent(/* ... */)
    cases := []string{
        "/v1/sessions",
        "/v1/strategies",
        "/v1/provider-config",
    }
    for _, p := range cases {
        req := httptest.NewRequest("GET", p, nil)
        rr := httptest.NewRecorder()
        h.ServeHTTP(rr, req)
        // Bare /v1/<agent-thing> should NOT route to the agent handler. We
        // accept 401 (because strategy-server's bare /v1/* also requires
        // auth) or 404, but NOT 200 from the agent handler.
        if rr.Code == http.StatusOK {
            body, _ := io.ReadAll(rr.Body)
            t.Errorf("GET %s: got 200, expected non-200 (bare /v1/* should not route to agent). body: %s", p, body)
        }
    }
}
```

- [ ] **Step 6.2.2: Run test; expect failure**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./strategy/http/... -run TestAgentRoutes_NotAtBareV1Path -v
```

- [ ] **Step 6.2.3: Fix the wiring if the test fails**

If bare `/v1/sessions` returns 200 from the agent, the mux is wrong. Re-do step 5.2.3: make sure `/v1/agent/` is registered before the catchall, and the bare `/v1/*` does not fall through to the agent.

- [ ] **Step 6.2.4: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 6.2.5: Stage and prompt for commit**

Stage the test additions. Prompt user to commit with message `test: confirm agent routes do not respond at bare /v1/*`.

### Task 6.3: Test that agent's per-user rate limiter returns 429 with Retry-After

**Files:**
- Same as 6.1

- [ ] **Step 6.3.1: Write the failing test**

```go
func TestAgentRoutes_RateLimitReturns429WithRetryAfter(t *testing.T) {
    h := newTestHandlerWithAgent(/* with rate limit set to 1 req/min */)
    // First request: 401 (no auth) — fine, doesn't consume the bucket if auth runs before rate limit
    // Actually the agent's per-user bucket is keyed by user, not by request. Send an authed request to consume.
    // For this test, configure the handler with a fake auth that always returns user "u1".
    req := authedRequest("GET", "/v1/agent/sessions")
    rr1 := httptest.NewRecorder()
    h.ServeHTTP(rr1, req)
    if rr1.Code == http.StatusTooManyRequests {
        t.Skip("rate limit fired on first request; fixture probably configured wrong")
    }
    rr2 := httptest.NewRecorder()
    h.ServeHTTP(rr2, req)
    if rr2.Code != http.StatusTooManyRequests {
        t.Fatalf("expected 429 on second request, got %d", rr2.Code)
    }
    if rr2.Header().Get("Retry-After") == "" {
        t.Fatal("expected Retry-After header on 429")
    }
}

func authedRequest(method, path string) *http.Request {
    req := httptest.NewRequest(method, path, nil)
    req = req.WithContext(authctx.WithAuthInfo(req.Context(), authctx.AuthInfo{APIKey: "k1", UserID: "u1"}))
    return req
}
```

- [ ] **Step 6.3.2: Run test; iterate**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./strategy/http/... -run TestAgentRoutes_RateLimitReturns429WithRetryAfter -v
```

- [ ] **Step 6.3.3: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 6.3.4: Stage and prompt for commit**

Stage test additions. Prompt user to commit with message `test: confirm agent per-user rate limiter returns 429 with Retry-After`.

---

## Phase 7 — OpenAPI merge

### Task 7.1: Merge the agent's openapi spec into strategy-server's

**Files:**
- Modify: `strategy/http/openapi.go` (or wherever the existing spec is embedded)

- [ ] **Step 7.1.1: Find the agent's openapi spec**

```bash
cd /home/tanq/code/dora/repos/dora-services/dora-agent/development
grep -rln 'openapispec' internal/ | head -5
```

The agent likely has `internal/httpapi/openapispec/spec.go` or `internal/openapi/spec.go` with a `var Spec = []byte{...}` byte literal.

- [ ] **Step 7.1.2: Find the host's openapi spec**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
grep -rln 'openapi' strategy/http/ | head -5
ls strategy/http/openapi*
```

- [ ] **Step 7.1.3: Choose merge strategy**

Read both specs. If both are JSON, use `jq` or a Python script to merge:

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
python3 - <<'PY'
import json
with open('/tmp/agent_openapi.json') as f: agent = json.load(f)
with open('strategy/http/openapi.json') as f: host = json.load(f)

# Merge paths: host's paths win on conflict, but agent's paths get /v1/agent prefix.
# This is mechanical but DO NOT do it by hand. Use a script and verify.
merged = host.copy()
merged.setdefault('paths', {})
for p, item in agent.get('paths', {}).items():
    if p == '/healthz' or p == '/v1/openapi':
        continue  # already in host
    new_p = '/v1/agent' + p if not p.startswith('/v1/agent') else p
    if p.startswith('/v1/'):
        new_p = '/v1/agent' + p[len('/v1'):]
    merged['paths'][new_p] = item

with open('strategy/http/openapi_merged.json', 'w') as f:
    json.dump(merged, f, indent=2)
print('paths after merge:', list(merged['paths'].keys()))
PY
```

Verify the merge produces sensible output. Iterate until every agent path is under `/v1/agent/`.

- [ ] **Step 7.1.4: Replace the embedded spec byte literal**

If the host's openapi is embedded as `var Spec = []byte(...)` in a Go file, replace that constant with the merged JSON. If it's served from a file, replace the file.

- [ ] **Step 7.1.5: Build and test**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./...
go test ./strategy/http/...
```

- [ ] **Step 7.1.6: Verify the merged spec is served**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go run ./cmd/strategy-server -d "$DATABASE_URL" -e "$ENCRYPTION_KEY" &
sleep 2
curl -fsS http://localhost:8081/v1/openapi | jq '.paths | keys' | head -30
```

Expected: output contains both strategy paths (`/v1/strategies/*`) and agent paths (`/v1/agent/strategies/*`).

- [ ] **Step 7.1.7: Stop the server**

```bash
pkill -f 'go-build.*strategy-server' || true
```

- [ ] **Step 7.1.8: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 7.1.9: Stage and prompt for commit**

Stage the modified spec file and any helper scripts. Prompt user to commit with message `feat(openapi): merge agent spec under /v1/agent/*`.

---

## Phase 8 — Admin surface removal

### Task 8.1: Delete cmd/agent-cli

**Files:**
- Delete: `cmd/agent-cli/`

- [ ] **Step 8.1.1: Confirm agent-cli exists**

```bash
cd /home/tanq/code/dora/repos/dora-services/dora-agent/development
ls cmd/agent-cli/
```

- [ ] **Step 8.1.2: Delete the directory**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
# It should not exist here yet — we're already in the host repo.
# If we accidentally copy-pasted agent-cli during Phase 4, delete it.
ls cmd/ 2>&1 | grep agent-cli || echo "not present (good)"
```

If `cmd/agent-cli/` exists here, delete it.

- [ ] **Step 8.1.3: Verify build**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./...
```

Expected: exit 0.

- [ ] **Step 8.1.4: Stage and prompt for commit (if anything deleted)**

If we had a copy, stage its deletion. If not, skip this commit — there's nothing to delete in this repo. (The agent-cli lives in the dora-agent repo, which we're not modifying.)

### Task 8.2: Confirm internal/serveradmin is not present

**Files:** none

- [ ] **Step 8.2.1: Verify absence**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
find . -path ./node_modules -prune -o -name 'serveradmin' -print 2>/dev/null
```

Expected: empty output (we never copied serveradmin into this repo).

If it does exist, delete:

```bash
rm -rf internal/agent/serveradmin
git add -u internal/agent/serveradmin
```

- [ ] **Step 8.2.2: Verify build**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./...
```

- [ ] **Step 8.2.3: Stage any deletions and prompt for commit**

If anything deleted, prompt user to commit with message `chore: ensure serveradmin and agent-cli are absent from this repo`.

---

## Phase 9 — Documentation

### Task 9.1: Update README.md and env.example

**Files:**
- Modify: `README.md`
- Modify: `.env.example` (or wherever env is documented)

- [ ] **Step 9.1.1: Document the new agent env vars**

In `.env.example`, add the agent-specific env vars from spec §6:

```
# Agent runtime
AGENT_WASM_ARTIFACT_ROOT=/var/lib/dora-agent/wasm-artifacts
AGENT_RATE_LIMIT_PER_MIN=20
AGENT_LLM_TIMEOUT=10m
AGENT_LLM_MAX_ITERS=50
AGENT_MAX_PROMPT_BYTES=32768
AGENT_DORA_TOOLS_ENABLED=true
AGENT_MODEL_CAPS_PATH=configs/model_caps.json
AGENT_LIVE_MEMORY_LIMIT=268435456
AGENT_LIVE_RESTART_WINDOW=60s
AGENT_LIVE_MAX_RESTARTS=3
AGENT_WSBROKER_URL=
AGENT_CAPTURE_PENDING_RETENTION=168h
AGENT_CAPTURE_PENDING_SWEEP_INTERVAL=5m
AGENT_GENERATE_MAX_REPAIRS=2
AGENT_GENERATE_MAX_FILES=50
AGENT_GENERATE_MAX_BYTES=1048576
AGENT_GENERATE_GOPROXY=https://proxy.golang.org,direct
AGENT_GENERATE_ALLOWLIST=$$DORA_CLIENT$$/dora-client-go,$$DORA_AGENT_STRATEGY$$/dora-agent-strategy
AGENT_ALLOW_HTTP_BASE_URL=
```

(The exact `$$...$$` placeholder format mirrors what's in this repo's existing comments — verify against the actual go.mod path before committing.)

- [ ] **Step 9.1.2: Update README to mention the /v1/agent/* surface**

Add a section:

```
## Agent API

`/v1/agent/*` is the AI agent surface: chat-style strategy generation
(`POST /v1/agent/sessions/{id}/messages`), strategy/version management
(`/v1/agent/strategies`, `/v1/agent/strategies/{id}/versions`), WASM
backtests (`/v1/agent/strategies/{id}/versions/{revision}/backtest`),
and live deployments (`/v1/agent/strategies/{id}/versions/{revision}/deploy`).

All routes require `Authorization: ApiKey <key>` or `Bearer <token>`,
resolved via Dora's `/v1/user/self`. The agent's per-user rate limit
(default 20 req/min) is in addition to strategy-server's broader rate
limits.

The agent's storage uses the same Postgres as the rest of the service.
Admin key rotation is via `DORA_ADMIN_API_KEY` env var at deploy time.
```

- [ ] **Step 9.1.3: Verify pre-commit**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -15
```

- [ ] **Step 9.1.4: Stage and prompt for commit**

Stage `.env.example` and `README.md`. Prompt user to commit with message `docs: document agent env vars and /v1/agent/* surface`.

---

## Phase 10 — Final verification

### Task 10.1: Full pre-commit run + go test ./...

**Files:** none

- [ ] **Step 10.1.1: Run the full pre-commit suite**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
pre-commit run --all-files 2>&1 | tail -30
```

Expected: all green. If golangci-lint complains about imports/order, run `goimports -local github.com/dora-network/bond-trading-strategies -w .`.

- [ ] **Step 10.1.2: Run the full test suite**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go test ./...
go test -race ./...
```

Expected: all pass. Slow tests (testcontainers) may take several minutes.

- [ ] **Step 10.1.3: Smoke-test the binary**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres \
  DORA_BASE_URL=https://dev.dora.co \
  ENCRYPTION_KEY=$(head -c 32 /dev/urandom | xxd -p -c 64) \
  AGENT_WASM_ARTIFACT_ROOT=/tmp/wasm-artifacts \
  go run ./cmd/strategy-server &
sleep 3
curl -fsS -o /dev/null -w '%{http_code}\n' http://localhost:8081/healthz
curl -fsS -o /dev/null -w '%{http_code}\n' http://localhost:8081/v1/agent/sessions  # 401 expected
curl -fsS -o /dev/null -w '%{http_code}\n' http://localhost:8081/v1/sessions  # whatever strategy-server's bare behavior is
curl -fsS http://localhost:8081/v1/openapi | python3 -c 'import json,sys; d=json.load(sys.stdin); print("agent paths:", sum(1 for p in d["paths"] if p.startswith("/v1/agent")))'
pkill -f 'go-build.*strategy-server'
```

Expected:
- `/healthz` returns 200.
- `/v1/agent/sessions` returns 401.
- The OpenAPI spec contains `/v1/agent/*` paths.

- [ ] **Step 10.1.4: Confirm MCP server and price-daemon still build**

```bash
cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development
go build ./cmd/mcp-server/...
go build ./cmd/price-daemon/...
```

Expected: both exit 0.

- [ ] **Step 10.1.5: Stage any final fixes; prompt for final commit**

If pre-commit produced any fixes, stage and prompt user to commit with message `chore: final pre-commit fixes for dora-agent integration`.

---

## Self-review checklist (run before handoff)

- [ ] Every spec section has a corresponding task:
  - §1 auth + secrets → Tasks 2.1, 2.2, 3.1, 4.9
  - §2 database merge → Tasks 1.1, 1.2
  - §3 agent runtime wiring → Tasks 4.1–4.8, 5.1
  - §4 routes + middleware → Tasks 4.8 (RoutesAt), 5.2 (mount), 6.1, 6.2, 6.3
  - §5 openapi → Task 7.1
  - §6 env vars → Task 4.4 (config rename), 5.2 (main.go flags), 9.1 (docs)
  - §7 deleted code → Tasks 8.1, 8.2
  - §8 testing → Tasks 6.1, 6.2, 6.3 (plus Task 4.7 history_store test)
- [ ] No "TBD" / "TODO" / "implement later" / "similar to Task N" placeholders. (Tasks reference other tasks by number, which is allowed.)
- [ ] Type names match across tasks: `*httpapi.Server`, `agentsecrets.Seal`, `wiring.Runtime`, `store.Store`, etc.
- [ ] Every step that touches code shows the code.
- [ ] Every step that runs a test shows the command and the expected output.
- [ ] Every task ends with a commit gate ("Stage and prompt for commit").

---

## Notes for the executor

- **Anchor every shell command to the project root.** Always `cd /home/tanq/code/dora/repos/dora-services/bond-trading-strategies/development` at the start of every bash call. The harness's bash CWD drifts; relative paths are unsafe.
- **Do not bypass the pre-commit gate.** `--no-verify` is forbidden. GPG signing is required.
- **Do not commit on the user's behalf.** Stage at the end of each task; prompt the user to review and commit. The repo's AGENTS.md is explicit about this.
- **Watch for shared-file coupling.** When tasks 4.x modify the same package, sequence them so each commit lands a green tree. If a task would block on another's changes, fold them.
- **The spec is the source of truth.** If a task here disagrees with the spec, the spec wins. Surface to the user.
- **Testcontainers Postgres is slow.** Tasks 4.3, 4.7, 5.1, 6.x may each take 30s–2min. Use `go test -p 1` if parallelism flakes.
- **Worktree hygiene.** This plan runs on `tan/feat-integrate-dora-agent`. If a subagent is dispatched, anchor it to the same branch and verify with `git branch --show-current` before each write.
