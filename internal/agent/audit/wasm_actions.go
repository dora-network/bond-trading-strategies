package audit

// Action is the typed audit_log.action string for the WASM plugin
// path. The existing audit infrastructure treats action as an unconstrained
// string; these constants are the codes the WASM orchestrator + host impl emit.
type Action string

const (
	// Plugin lifecycle.
	ActionPluginLoaded          Action = "plugin.loaded"
	ActionPluginValidatePassed  Action = "plugin.validate_passed"
	ActionPluginValidateFailed  Action = "plugin.validate_failed"
	ActionPluginPanic           Action = "plugin.panic"
	ActionPluginFuelExhausted   Action = "plugin.fuel_exhausted"
	ActionPluginMemoryExhausted Action = "plugin.memory_exhausted"

	// Strategy lifecycle.
	ActionStrategyStarted    Action = "strategy.started"
	ActionStrategyCrashed    Action = "strategy.crashed"
	ActionStrategyHalted     Action = "strategy.halted"
	ActionStrategyResumed    Action = "strategy.resumed"
	ActionStrategyHotSwapped Action = "strategy.hot_swapped"

	// Order flow.
	ActionOrderSubmitted Action = "order.submitted"
	ActionOrderDenied    Action = "order.denied"
	ActionOrderFilled    Action = "order.filled"

	// Kill switch.
	ActionKillSwitchActivated   Action = "kill_switch.activated"
	ActionKillSwitchDeactivated Action = "kill_switch.deactivated"

	// wsplex.
	ActionWsplexConnected    Action = "wsplex.connected"
	ActionWsplexDisconnected Action = "wsplex.disconnected"
	ActionWsplexReconnected  Action = "wsplex.reconnected"
)
