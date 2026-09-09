// Package audit writes rows to the audit_log table. Each notable action in
// the agent lifecycle (provider config change, auth rejection, session
// creation, strategy generation, sanitisation refusal) is recorded with a
// stable action code and a JSON detail blob.
package audit

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Core action codes (see 2026-07-27-dora-agent-core-service-design.md §324).
const (
	ActionProviderConfigSet    = "provider_config.set"
	ActionProviderConfigDelete = "provider_config.delete"
	ActionAuthRoleRejected     = "auth.role_rejected"
	ActionSessionCreate        = "session.create"
)

// Strategy-generation action codes (see 2026-07-29-dora-agent-slice-b-strategy-generation-design.md §16.5).
const (
	ActionMessageRejected   = "message.rejected"
	ActionStrategyRefused   = "strategy.refused"
	ActionStrategyGenerated = "strategy.generated"
	ActionStrategyRepair    = "strategy.repair"
	ActionStrategyFailed    = "strategy.failed"
)

// Versioned-strategy action codes (see
// 2026-07-30-dora-agent-slice-c-strategy-versioning-design.md §17.1).
// These supersede ActionStrategyGenerated: a verified generate_strategy is now
// captured as a versioned artifact, so the audit row distinguishes the first
// version (created) from subsequent ones (version) and records capture failure.
const (
	ActionStrategyCreated       = "strategy.created"
	ActionStrategyVersion       = "strategy.version"
	ActionStrategyRollback      = "strategy.rollback"
	ActionStrategyCaptureFailed = "strategy.capture_failed"
)

// Insert writes a single audit_log row. doraUserID is a UUID string; detail is
// raw JSON (write "{}" or a marshalled object). The error is returned unwrapped
// so callers can decide whether to fail the request or log-and-continue.
func Insert(ctx context.Context, pool *pgxpool.Pool, doraUserID, action string, detail []byte) error {
	_, err := pool.Exec(ctx, `
		insert into audit_log (dora_user_id, action, detail)
		values ($1, $2, $3)`,
		doraUserID, action, detail,
	)
	if err != nil {
		return fmt.Errorf("audit insert %s: %w", action, err)
	}
	return nil
}
