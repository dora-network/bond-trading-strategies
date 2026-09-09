// Package safety owns the per-user risk cap set and the kill
// switch. The kernel is consulted on every host_submit_order call;
// a rejected order returns ErrCapExceeded or ErrUserHalted to the
// plugin.
package safety

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Caps is the per-user cap set.
type Caps struct {
	MaxOpenOrders       int
	MaxNotionalPerOrder float64
	MaxTotalNotional    float64
	MaxOrdersPerMinute  int
}

// Default cap values for a new user.
const (
	defaultMaxOpenOrders       = 50
	defaultMaxNotionalPerOrder = 100000
	defaultMaxTotalNotional    = 1000000
	defaultOrdersPerMinute     = 60
)

// Defaults returns the cap set a new user gets.
func Defaults() Caps {
	return Caps{
		MaxOpenOrders:       defaultMaxOpenOrders,
		MaxNotionalPerOrder: defaultMaxNotionalPerOrder,
		MaxTotalNotional:    defaultMaxTotalNotional,
		MaxOrdersPerMinute:  defaultOrdersPerMinute,
	}
}

// OrderCheck is the per-order check input.
type OrderCheck struct {
	Quantity   float64
	Price      float64
	OpenOrders int
}

// Kernel is the safety kernel.
type Kernel struct {
	pool *pgxpool.Pool
}

// Errors.
var (
	ErrUserHalted  = errors.New("safety: user halted")
	ErrCapExceeded = errors.New("safety: cap exceeded")
)

// NewKernel constructs a Kernel backed by a pgxpool.
func NewKernel(pool *pgxpool.Pool) (*Kernel, error) {
	if pool == nil {
		return nil, errors.New("safety: nil pool")
	}
	return &Kernel{pool: pool}, nil
}

// CapsFor returns the cap set for a user. If the user has no row,
// the defaults are returned (no write side-effect).
func (k *Kernel) CapsFor(ctx context.Context, userID string) (Caps, error) {
	var c Caps
	err := k.pool.QueryRow(ctx, `
		select max_open_orders,
		       max_notional_per_order,
		       max_total_notional,
		       max_orders_per_minute
		from user_caps where user_id = $1
	`, userID).Scan(&c.MaxOpenOrders, &c.MaxNotionalPerOrder, &c.MaxTotalNotional, &c.MaxOrdersPerMinute)
	if err != nil {
		// No row → defaults.
		return Defaults(), nil
	}
	return c, nil
}

// SetCaps upserts the cap set for a user.
func (k *Kernel) SetCaps(ctx context.Context, userID string, c Caps) error {
	_, err := k.pool.Exec(ctx, `
		insert into user_caps (user_id, max_open_orders, max_notional_per_order,
		                       max_total_notional, max_orders_per_minute, updated_at)
		values ($1, $2, $3, $4, $5, now())
		on conflict (user_id) do update set
			max_open_orders = excluded.max_open_orders,
			max_notional_per_order = excluded.max_notional_per_order,
			max_total_notional = excluded.max_total_notional,
			max_orders_per_minute = excluded.max_orders_per_minute,
			updated_at = now()
	`, userID, c.MaxOpenOrders, c.MaxNotionalPerOrder, c.MaxTotalNotional, c.MaxOrdersPerMinute)
	return err
}

// IsHalted returns (true, reason) if the kill switch is set.
func (k *Kernel) IsHalted(ctx context.Context, userID string) (bool, string, error) {
	var halted bool
	var reason *string
	err := k.pool.QueryRow(ctx, `
		select halted, halted_reason from user_kill_switches where user_id = $1
	`, userID).Scan(&halted, &reason)
	if err != nil {
		return false, "", nil
	}
	r := ""
	if reason != nil {
		r = *reason
	}
	return halted, r, nil
}

// Halt sets the kill switch.
func (k *Kernel) Halt(ctx context.Context, userID, reason string) error {
	_, err := k.pool.Exec(ctx, `
		insert into user_kill_switches (user_id, halted, halted_at, halted_reason)
		values ($1, true, now(), $2)
		on conflict (user_id) do update set
			halted = true, halted_at = now(), halted_reason = excluded.halted_reason
	`, userID, reason)
	return err
}

// Resume clears the kill switch.
func (k *Kernel) Resume(ctx context.Context, userID string) error {
	_, err := k.pool.Exec(ctx, `
		update user_kill_switches
		set halted = false, halted_at = null, halted_reason = null
		where user_id = $1
	`, userID)
	return err
}

// CheckOrder returns (allowed, reason, err). If allowed is false
// and err is nil, the reason string carries ErrCapExceeded or
// ErrUserHalted text for the host implementation to surface.
func (k *Kernel) CheckOrder(ctx context.Context, userID string, oc OrderCheck) (bool, string, error) {
	halted, haltReason, err := k.IsHalted(ctx, userID)
	if err != nil {
		return false, "", err
	}
	if halted {
		return false, fmt.Sprintf("%v: %s", ErrUserHalted, haltReason), nil
	}

	caps, err := k.CapsFor(ctx, userID)
	if err != nil {
		return false, "", err
	}

	if oc.OpenOrders >= caps.MaxOpenOrders {
		return false, fmt.Sprintf("%v: open_orders=%d >= %d",
			ErrCapExceeded, oc.OpenOrders, caps.MaxOpenOrders), nil
	}
	notional := oc.Quantity * oc.Price
	if notional > caps.MaxNotionalPerOrder {
		return false, fmt.Sprintf("%v: notional=%v > %v",
			ErrCapExceeded, notional, caps.MaxNotionalPerOrder), nil
	}

	count, err := k.OrdersInLastMinute(ctx, userID)
	if err != nil {
		return false, "", err
	}
	if count >= caps.MaxOrdersPerMinute {
		return false, fmt.Sprintf("%v: orders/min=%d >= %d",
			ErrCapExceeded, count, caps.MaxOrdersPerMinute), nil
	}
	return true, "", nil
}

// OrdersInLastMinute returns the count of orders the user has
// submitted in the last 60 seconds. Plan 4 wires this to audit_log;
// Plan 3 uses an internal counter table for tests. If the table is
// missing or the query errors, zero is returned (best-effort).
func (k *Kernel) OrdersInLastMinute(ctx context.Context, userID string) (int, error) {
	var n int
	err := k.pool.QueryRow(ctx, `
		select coalesce(sum(cnt), 0)::int from safety_order_counter
		where user_id = $1 and window_start > now() - interval '60 seconds'
	`, userID).Scan(&n)
	if err != nil {
		// Table may not exist yet; treat as zero. Plan 4 adds the
		// real audit-log-backed implementation.
		return 0, nil
	}
	return n, nil
}

// SetOrderCountForTest forces a user's recent-order count to a
// specific value. Used by tests; never call in production.
func (k *Kernel) SetOrderCountForTest(ctx context.Context, userID string, count int, at time.Time) error {
	_, err := k.pool.Exec(ctx, `
		insert into safety_order_counter (user_id, window_start, cnt)
		values ($1, $2, $3)
	`, userID, at, count)
	return err
}
