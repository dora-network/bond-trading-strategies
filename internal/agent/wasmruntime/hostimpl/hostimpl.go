// Package hostimpl implements the host-side bodies of every host_*
// function a generated WASM plugin can call. The registry in Plan 2
// wires these to the wazero host module; this package is what runs
// when the plugin calls host_submit_order, host_log, etc.
package hostimpl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/orderbroker"
	"github.com/dora-network/bond-trading-strategies/internal/agent/safety"
)

// Intent is the host-side mirror of the plugin's order intent.
// Quantity and Price are float64 on the host side; the order broker
// consumes decimal strings, so SubmitOrder converts them.
type Intent struct {
	OrderBookID string
	Side        string
	Quantity    float64
	Type        string
	Price       float64
}

// Deps is the host impl's dependencies. Plan 3 wires Kernel and
// Orders; Plan 4 wires Audit to the real audit_log writer.
type Deps struct {
	Kernel     *safety.Kernel
	Orders     *orderbroker.Broker
	Audit      AuditHook // nil-safe; defaults to NoopAudit
	UserID     string
	StrategyID string
	APIKey     string
	Logger     *slog.Logger
}

// AuditHook is the audit-log interface. The Plan 4 implementation
// writes to audit_log; the Plan 3 test uses a no-op.
type AuditHook interface {
	Record(ctx context.Context, action, detail string) error
}

// NoopAudit is the no-op audit hook used in tests.
type NoopAudit struct{}

func (NoopAudit) Record(_ context.Context, _, _ string) error { return nil }

// Host is the host impl.
type Host struct {
	deps Deps
}

// New constructs a Host.
func New(deps Deps) *Host {
	if deps.Audit == nil {
		deps.Audit = NoopAudit{}
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return &Host{deps: deps}
}

// Log emits a structured log line. The plugin calls this with a
// level + message + key-value pairs.
func (h *Host) Log(ctx context.Context, level, msg string, kv ...string) {
	attrs := []slog.Attr{
		slog.String("level", level),
		slog.String("msg", msg),
	}
	for i := 0; i+1 < len(kv); i += 2 {
		attrs = append(attrs, slog.String(kv[i], kv[i+1]))
	}
	h.deps.Logger.LogAttrs(ctx, slog.LevelInfo, "wasm plugin", attrs...)
}

// GetParamString returns a runtime parameter declared in the
// manifest.params_schema. Plan 4 wires the param store; Plan 3
// returns ErrUnsupported (a plugin that hits this in Plan 3 was
// not yet updated for the new param contract).
func (h *Host) GetParamString(_ context.Context, _ string) (string, error) {
	return "", fmt.Errorf("hostimpl.GetParamString: %w (Plan 4)", errors.ErrUnsupported)
}

// SubmitOrder sends an order intent through the safety kernel then
// the order broker. The plugin's host_submit_order import calls this.
func (h *Host) SubmitOrder(ctx context.Context, intent Intent) (string, error) {
	if h.deps.Kernel == nil {
		return "", errors.New("hostimpl: nil kernel")
	}
	allowed, reason, err := h.deps.Kernel.CheckOrder(ctx, h.deps.UserID, safety.OrderCheck{
		Quantity: intent.Quantity, Price: intent.Price,
		OpenOrders: 0, // Plan 4: query actual open count from the ledger.
	})
	if err != nil {
		return "", fmt.Errorf("hostimpl: safety check: %w", err)
	}
	if !allowed {
		_ = h.deps.Audit.Record(ctx, "order.denied", reason)
		return "", fmt.Errorf("hostimpl: order denied: %s", reason)
	}
	_ = h.deps.Audit.Record(ctx, "order.submitted", "")

	res, err := h.deps.Orders.SubmitOrder(ctx, h.deps.UserID, h.deps.StrategyID, h.deps.APIKey, orderbroker.Intent{
		OrderBookID: intent.OrderBookID,
		Side:        intent.Side,
		Quantity:    strconv.FormatFloat(intent.Quantity, 'f', -1, 64),
		Type:        intent.Type,
		Price:       strconv.FormatFloat(intent.Price, 'f', -1, 64),
	})
	if err != nil {
		return "", fmt.Errorf("hostimpl: order broker: %w", err)
	}
	_ = h.deps.Audit.Record(ctx, "order.filled", res.OrderID)
	return res.OrderID, nil
}

// CancelOrder cancels a previously submitted order. The plugin's
// host_cancel_order import calls this. Plan 4 wires the order
// broker's CancelOrder through the host impl.
func (h *Host) CancelOrder(ctx context.Context, orderID string) error {
	if err := h.deps.Orders.CancelOrder(ctx, h.deps.UserID, h.deps.APIKey, orderID); err != nil {
		return fmt.Errorf("hostimpl: cancel order: %w", err)
	}
	return nil
}

// Now returns the host's wall clock.
func (h *Host) Now() time.Time {
	return time.Now()
}
