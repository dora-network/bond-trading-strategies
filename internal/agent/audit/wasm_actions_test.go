package audit_test

import (
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/audit"
)

func TestWASMActions_AllDefined(t *testing.T) {
	required := []audit.Action{
		audit.ActionPluginLoaded,
		audit.ActionPluginValidatePassed,
		audit.ActionPluginValidateFailed,
		audit.ActionPluginPanic,
		audit.ActionPluginFuelExhausted,
		audit.ActionPluginMemoryExhausted,
		audit.ActionStrategyStarted,
		audit.ActionStrategyCrashed,
		audit.ActionStrategyHalted,
		audit.ActionStrategyResumed,
		audit.ActionStrategyHotSwapped,
		audit.ActionOrderSubmitted,
		audit.ActionOrderDenied,
		audit.ActionOrderFilled,
		audit.ActionKillSwitchActivated,
		audit.ActionKillSwitchDeactivated,
		audit.ActionWsplexConnected,
		audit.ActionWsplexDisconnected,
		audit.ActionWsplexReconnected,
	}
	for _, a := range required {
		if string(a) == "" {
			t.Errorf("audit action is empty: %v", a)
		}
	}
}
