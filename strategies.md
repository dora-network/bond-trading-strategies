# Trading Strategies

The strategy server supports three automated trading strategies. Each strategy
can be run as a **live run** (placing real orders on a DORA order book) or as a
**backtest** (simulating trades on historical data).

## Mean Reversion

A statistical arbitrage strategy that opens positions when a bond's spread (price
minus benchmark yield) deviates significantly from its rolling mean, and closes
when the spread reverts.

**Algorithm**: A rolling window computes the z-score of the current spread
relative to its recent distribution. When the z-score crosses below the entry
threshold (spread is unusually cheap), the strategy buys. When it crosses above
(expensive), it sells. Positions close when the z-score returns within the exit
threshold. An optional stop-loss closes positions that continue to diverge.

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `lookback_window` | int | 20 | Rolling observation window. Must be at least 2. |
| `entry_z_score` | float | 2.0 | Entry z-score threshold. Must be greater than 0. |
| `exit_z_score` | float | 0.0 | Exit z-score threshold. Must be non-negative. |
| `stop_loss_z_score` | float | 0.0 | Stop-loss z-score threshold. Must be non-negative. |
| `min_std_dev` | float | 0.0 | Minimum spread volatility required before trading. Must be non-negative. |
| `max_position_size` | float | 1.0 | Maximum fraction of capital allocated per trade, in (0, 1]. |
| `order_book_id` | uuid | – | DORA order book UUID used to locate the traded asset. |
| `tenor` | string | – | Benchmark Treasury tenor (e.g. `1M`, `6M`, `2Y`, `5Y`, `10Y`, `30Y`). |
| `initial_balance` | float | 0 | Starting capital for backtests. Must be greater than 0 for backtests. |
| `leverage` | float | 1.0 | Leverage multiplier for live orders. Must be greater than 0. |

### How the fields affect the strategy

- **`entry_z_score`** controls how extreme the deviation must be before opening.
  Higher values produce fewer, higher-confidence trades.
- **`exit_z_score`** controls how close to the mean the spread must return before
  closing. Lower values hold positions longer for more profit but risk divergence.
- **`lookback_window`** sets the rolling observation window. Longer windows
  produce more stable z-scores but react more slowly to regime changes.
- **`stop_loss_z_score`** acts as a safety net; positions that diverge past this
  threshold are forcibly closed.

---

## Copy Trading

Mirrors the trades of a followed trader subject to configurable limits.

**Algorithm**: Subscribes to the DORA trade stream across all open order books
and filters for trades by the followed trader. For each trade by the followed
trader — regardless of which order book it occurs on — places a market order in
the same direction, sized as a percentage of the strategy's available balance.
Configurable minimum and maximum order sizes prevent over- or under-sizing.

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `followed_trader` | uuid | – | UUID of the trader to copy. Required. |
| `percentage_of_available` | float | – | Percentage of available balance to use per trade, in (0, 1]. Required. |
| `leverage` | float | 1.0 | Leverage multiplier for copied orders. Must be greater than 0. |
| `min_order_size` | int | 0 | Minimum copied order size. Must be non-negative. |
| `max_order_size` | int | 0 | Maximum copied order size. |
| `disallowed_bonds` | uuid[] | [] | Bond UUIDs to skip. Empty means no bonds are disallowed. |
| `initial_balance` | float | 0 | Starting capital for backtests. Must be non-negative. |

### How the fields affect the strategy

- **`percentage_of_available`** controls position sizing. Lower values are more
  conservative (smaller positions), higher values are more aggressive.
- **`min_order_size`** and **`max_order_size`** clamp the computed order size,
  preventing tiny or oversized orders.
- **`disallowed_bonds`** lets you exclude specific bonds from being traded.

---

## Breakout (Volatility Compression)

A price-action strategy that enters on a volatility compression → price breakout.

**Algorithm**: Computes short-window and long-window price volatility. When the
ratio (ShortVol / LongVol) drops below `compression_threshold`, the market is
considered "compressed" and the strategy arms for a breakout. Once armed, a
breakout fires when `confirmation_bars` consecutive closes exceed the trigger
band (`prev_price ± breakout_atr_multiple × ATR`). ATR (Average True Range)
measures recent price volatility as the rolling mean of absolute price changes;
it adapts the trigger distance to current market conditions. Stop-loss and
take-profit exits are supported in ATR units. An optional OBV (On-Balance
Volume) filter confirms the trade direction using recent trade history — BUY
requires positive OBV (net buying pressure), SELL requires negative OBV (net
selling pressure).

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `short_vol_window` | int | 240 | Short-window price volatility count. Must be at least 2. |
| `long_vol_window` | int | 1440 | Long-window price volatility count. Must be greater than `short_vol_window`. |
| `compression_threshold` | float | 0.3 | ShortVol/LongVol ratio below which the strategy arms for a breakout. Must be in (0, 1]. Lower values are stricter. |
| `atr_window` | int | 240 | Rolling-mean window for ATR (mean absolute price diff). Must be at least 2. |
| `breakout_atr_multiple` | float | 1.5 | Number of ATR units above/below the most recent close that defines the breakout trigger band. Must be non-negative. |
| `confirmation_bars` | int | 5 | Consecutive closes beyond the trigger band required to fire. Must be at least 1. More bars = fewer false breakouts. |
| `stop_loss_atr` | float | 20 | Stop-loss distance in ATR units from entry. 0 disables. |
| `take_profit_atr` | float | 0 | Take-profit distance in ATR units from entry. 0 disables. |
| `min_long_vol_floor` | float | 0 | Minimum LongVol required to trade. Suppresses entries on a completely flat baseline. |
| `obv_window` | int | 0 | Recent trades to include in windowed OBV. 0 = disabled. >0 = verify signal with windowed OBV over last N trades. |
| `obv_trend_threshold` | float | 0 | OBV threshold for the volume confirmation filter. BUY requires OBV > this, SELL requires OBV < -this. 0 means any non-zero OBV in the right direction is enough. |
| `order_book_id` | uuid | – | DORA order book UUID used to locate the traded asset. |
| `initial_balance` | float | 1 | Starting capital for backtests. Must be greater than 0 for backtests. |
| `leverage` | float | 1 | Leverage multiplier for live orders. Must be greater than 0. |

### How the fields affect the strategy

- **`compression_threshold`** is the most impactful parameter. A lower value
  (e.g. 0.2) makes the strategy more selective — it only fires on strong
  compression. A higher value (e.g. 0.7) is less strict but generates more false
  signals.
- **`confirmation_bars`** filters noise. 1 bar fires on a single tick; 5 bars
  requires sustained momentum. More bars = fewer trades but higher quality.
- **`stop_loss_atr`** should be wide enough to let trades develop. Tighter
  stops (e.g. 3–10×ATR) can fire on market noise without giving the position
  room to develop. Wider stops (e.g. 20×ATR) let trends play out but risk
  larger losses if the signal is wrong.
- **`obv_window`** enables volume confirmation when > 0. Set to match
  `short_vol_window` for consistency — the volume check uses the same
  recent-trades scope as the volatility check. Set to 0 to skip volume
  verification.
- **`breakout_atr_multiple`** sets the trigger distance. 1.5 (default) means the
  breakout fires when price moves 1.5×ATR beyond the most recent close.
- **Window sizes** (`short_vol_window`, `long_vol_window`, `atr_window`) should
  be calibrated per asset based on tick frequency. The defaults are for
  continuously-trading markets; adjust down for assets with slower tick rates.

### Known v1 limitations

- Volume confirmation (`obv_window > 0`) requires historical trade data in
  `trades_history`. If no trade data exists for the backtest date range, set
  `obv_window` to 0.
