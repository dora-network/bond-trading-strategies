// Package servertest provides the Postgres fixture for agent strategy
// tool tests. Kept as an alias of the shared agenttest fixture so the
// upstream dora-agent test call sites compile unchanged.
//
// ponytail: schema apply delegates to agenttest (015 consolidated SQL);
// the dora-agent's on-demand store.MigrateConn path is gone.
package servertest

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
)

// StartPostgres returns a pool with the agent schema applied.
func StartPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return agenttest.StartPostgres(t)
}
