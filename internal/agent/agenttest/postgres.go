// Package agenttest provides the shared opt-in Postgres fixture for
// agent-store integration tests. Follows the repo convention of
// prices/store_test.go: tests skip unless DATABASE_URL is set.
package agenttest

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/tern/v2/migrate"
)

func readMigration(t *testing.T) string {
	t.Helper()
	// Resolve the consolidated schema from test binaries at different
	// package depths (internal/agent/x and internal/agent/tools/x).
	for _, p := range []string{
		"../../../migrations/015_agent_consolidated_schema.sql",
		"../../../../migrations/015_agent_consolidated_schema.sql",
	} {
		if b, err := os.ReadFile(p); err == nil {
			return string(b)
		}
	}
	t.Fatal("read migration: 015_agent_consolidated_schema.sql not found")
	return ""
}

// StartPostgres returns a pool with the agent schema applied.
//
// The schema is created at most once per database: under an advisory
// lock the schema is dropped and the consolidated migration replayed
// only when it does not already exist. Tests are idempotent
// (on-conflict seeds, per-test cleanups), so reuse across packages
// within one `go test ./...` run is safe.
func StartPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping agent PG test")
	}
	ctx := t.Context()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `select pg_advisory_lock(hashtext('agent_schema_bootstrap'))`); err != nil {
		t.Fatalf("advisory lock: %v", err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, `select pg_advisory_unlock(hashtext('agent_schema_bootstrap'))`)
	}()

	var exists bool
	if err := conn.QueryRow(ctx, `select exists(
		select 1 from information_schema.schemata where schema_name = 'agent')`).Scan(&exists); err != nil {
		t.Fatalf("schema check: %v", err)
	}
	if exists {
		return pool
	}

	if _, err := conn.Exec(ctx, readMigration(t)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	return pool
}

// StartPostgresWithHostMigrations returns a pool with the host's tern
// migrations 001-014 applied in addition to the agent's 015
// consolidated migration. Use for tests that need the public-schema
// tables (candles_history / trades_history / price_history) the
// agent's history_store queries. Idempotent: tern records applied
// versions in public.schema_version, so repeat calls no-op.
func StartPostgresWithHostMigrations(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := StartPostgres(t)

	// Resolve the migrations dir from test binaries at different
	// package depths (mirrors readMigration above).
	var dir string
	for _, p := range []string{"../../../migrations", "../../../../migrations"} {
		if _, err := os.Stat(p); err == nil {
			dir = p
			break
		}
	}
	if dir == "" {
		t.Fatal("host migrations dir not found")
	}

	ctx := t.Context()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()

	m, err := migrate.NewMigrator(ctx, conn.Conn(), "public.schema_version")
	if err != nil {
		t.Fatalf("tern migrator: %v", err)
	}
	if err := m.LoadMigrations(os.DirFS(dir)); err != nil {
		t.Fatalf("tern load: %v", err)
	}
	// 015 (agent schema) is already applied by StartPostgres; stop
	// just before it.
	const lastHostMigration = 14
	if err := m.MigrateTo(ctx, lastHostMigration); err != nil {
		t.Fatalf("tern migrate: %v", err)
	}
	return pool
}
