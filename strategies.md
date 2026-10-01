# Trading Strategies

The strategy server supports three automated trading strategies. Each strategy
can be run as a **live run** (placing real orders on a DORA order book) or as a
**backtest** (simulating trades on historical data).

## Mean Reversion

A statistical arbitrage strategy that opens positions when a bond's spread (price
minus benchmark yield) deviates significantly from its rolling mean, and closes
when the spread reverts.

**Algorithm**: A rolling window computes the z-score of the current spread
relative to its recent distribution, using **closed bars** of `resolution`
(OHLCV + YTM from the DORA candle stream; the server maintains one candle
stream per order book + resolution, shared across runs). Entries and
z-reversion exits are evaluated on bar closes: when the z-score crosses below
the entry threshold (spread is unusually cheap) the strategy buys; above
(expensive) it sells; positions close when the z-score returns within the exit
threshold. The stop-loss z-score is checked intrabar on ticks so divergent
positions exit without waiting for the bar to close. Ticks are used only for
intrabar stop checks and execution pricing. An optional imbalance gate,
computed from the trade tape, blocks entries against strong opposing flow.

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `lookback_window` | int | 24 | Rolling observation window, in bars. Must be at least 2. |
| `entry_z_score` | float | 2.0 | Entry z-score threshold. Must be greater than 0. |
| `exit_z_score` | float | 0.5 | Exit z-score threshold. Must be non-negative. |
| `stop_loss_z_score` | float | 3.5 | Stop-loss z-score threshold, checked intrabar on ticks. Must be non-negative. |
| `min_std_dev` | float | 0.0005 | Minimum spread volatility required before trading. Must be non-negative. |
| `max_position_size` | float | 1.0 | Maximum fraction of capital allocated per trade, in (0, 1]. |
| `order_book_id` | uuid | – | DORA order book UUID used to locate the traded asset. |
| `tenor` | string | – | Benchmark Treasury tenor (e.g. `1M`, `6M`, `2Y`, `5Y`, `10Y`, `30Y`). |
| `initial_balance` | float | 1 | Starting capital for backtests. Must be greater than 0 for backtests. |
| `leverage` | float | 1.0 | Leverage multiplier for live orders. Must be greater than 0. |
| `resolution` | string | `1h` | Candle resolution for the signal series. One of `1m`, `5m`, `15m`, `1h`, `4h`, `1d`. |
| `imbalance_window` | int | 100 | Number of recent tape trades for the imbalance entry gate. 0 disables the gate. |
| `imbalance_threshold` | float | 0 | Net opposing signed quantity (over the last `imbalance_window` trades) that blocks an entry. Must be non-negative; 0 blocks on any net opposing flow. |

### How the fields affect the strategy

- **`entry_z_score`** controls how extreme the deviation must be before opening.
  Higher values produce fewer, higher-confidence trades.
- **`exit_z_score`** controls how close to the mean the spread must return before
  closing. Lower values hold positions longer for more profit but risk divergence.
- **`lookback_window`** sets the rolling observation window, counted in bars of
  `resolution`. Longer windows produce more stable z-scores but react more slowly
  to regime changes.
- **`stop_loss_z_score`** acts as a safety net; positions that diverge past this
  threshold are closed intrabar (on ticks live; backtests replay `price_history` ticks
  intrabar, matching live — bars without tick coverage fall back to the bar extreme).
- **`imbalance_window` / `imbalance_threshold`** gate entries against the trade
  tape: an entry in a given direction is blocked when the net signed quantity of
  the last `imbalance_window` trades in the opposing direction exceeds
  `imbalance_threshold`. With the default threshold 0, any net opposing flow
  blocks the entry. Set `imbalance_window` to 0 to disable the gate.

### Known limitations

- Backtests require `candles_history` coverage for the full lookback window at
  the chosen `resolution`; if coverage is missing the request fails with a 400
  naming the available range.

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

**Algorithm**: All signals are computed on **closed bars** of `resolution`
(OHLCV from the DORA candle stream; one stream per order book + resolution is
shared across runs). Computes short-window and long-window price volatility.
When the ratio (ShortVol / LongVol) drops below `compression_threshold`, the
market is considered "compressed" and the strategy arms for a breakout. Once
armed, a breakout fires when `confirmation_bars` consecutive bar closes exceed
the trigger band (`prev close ± breakout_atr_multiple × ATR`). ATR (Average
True Range) measures recent price volatility over the bar series; it adapts
the trigger distance to current market conditions. Stop-loss and take-profit
exits are anchored at entry and checked intrabar on ticks. Backtests replay
`price_history` ticks intrabar, matching live; bars without tick coverage fall
back to the bar extreme. Ticks are used only for intrabar exits and execution
pricing. An
optional OBV (On-Balance Volume) filter confirms the trade direction using
recent trade history — BUY requires positive OBV (net buying pressure), SELL
requires negative OBV (net selling pressure). Runs warm-start their windows
from historical candles at startup.

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `short_vol_window` | int | 12 | Short-window price volatility count, in bars. Must be at least 2. |
| `long_vol_window` | int | 96 | Long-window price volatility count, in bars. Must be greater than `short_vol_window`. |
| `compression_threshold` | float | 0.3 | ShortVol/LongVol ratio below which the strategy arms for a breakout. Must be in (0, 1]. Lower values are stricter. |
| `atr_window` | int | 12 | Rolling window for ATR (true range) over bars. Must be at least 2. |
| `breakout_atr_multiple` | float | 1.5 | Number of ATR units above/below the most recent close that defines the breakout trigger band. Must be non-negative. |
| `confirmation_bars` | int | 3 | Consecutive bar closes beyond the trigger band required to fire. Must be at least 1. More bars = fewer false breakouts. |
| `stop_loss_atr` | float | 20 | Stop-loss distance in ATR units from entry, checked intrabar on ticks. 0 disables. |
| `take_profit_atr` | float | 0 | Take-profit distance in ATR units from entry, checked intrabar on ticks. 0 disables. |
| `min_long_vol_floor` | float | 0 | Minimum LongVol required to trade. Suppresses entries on a completely flat baseline. |
| `obv_window` | int | 0 | Recent trades to include in windowed OBV. 0 = disabled. >0 = verify signal with windowed OBV over last N trades. |
| `obv_trend_threshold` | float | 0 | OBV threshold for the volume confirmation filter. BUY requires OBV > this, SELL requires OBV < -this. 0 means any non-zero OBV in the right direction is enough. |
| `order_book_id` | uuid | – | DORA order book UUID used to locate the traded asset. |
| `initial_balance` | float | 1 | Starting capital for backtests. Must be greater than 0 for backtests. |
| `leverage` | float | 1 | Leverage multiplier for live orders. Must be greater than 0. |
| `resolution` | string | `5m` | Candle resolution for the signal series. One of `1m`, `5m`, `15m`, `1h`, `4h`, `1d`. |

### How the fields affect the strategy

- **`compression_threshold`** is the most impactful parameter. A lower value
  (e.g. 0.2) makes the strategy more selective — it only fires on strong
  compression. A higher value (e.g. 0.7) is less strict but generates more false
  signals.
- **`confirmation_bars`** filters noise. 1 bar fires on a single bar close; 3
  bars requires sustained momentum. More bars = fewer trades but higher
  quality.
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
- **Window sizes** (`short_vol_window`, `long_vol_window`, `atr_window`) are
  counted in bars of `resolution`. The defaults (12/96/12 at 5m ≈ 1h/8h/1h)
  are for continuously-trading markets; adjust down for slower books or
  choose a coarser `resolution`.

### Known v1 limitations

- Volume confirmation (`obv_window > 0`) requires historical trade data in
  `trades_history`. If no trade data exists for the backtest date range, set
  `obv_window` to 0.
- Backtests require `candles_history` coverage for the long volatility window
  at the chosen `resolution`; if coverage is missing the request fails with a
  400 naming the available range.

## Momentum (Trend Following)

A trend-following strategy that rides sustained moves on the bond's price,
yield, or yield-spread series. Where mean reversion bets on spread
reversion, momentum bets that trends persist — it detects a trend via a
fast/slow moving-average crossover and rides it until the MAs reverse or
an ATR-based protective stop fires.

**Algorithm**: Three rolling windows (`fast_window`, `slow_window`,
`atr_window`) process a configurable series (`signal_source`) computed on
**closed bars** of `resolution` (OHLCV + YTM from the DORA candle stream; one
stream per order book + resolution is shared across runs):

- **`price`** — uses the bond's clean price directly. Rising series → Buy
  (long uptrend).
- **`ytm`** — uses the bond's yield-to-maturity. Rising YTM = falling price
  → Sell (short downtrend). The raw cross direction flips because yield
  moves inverse to price.
- **`spread`** — uses `YTM − benchmark_yield(FRED)`. Requires `tenor`. Like
  `ytm`, the direction is inverted: rising spread = cheapening → Sell.

When the fast MA crosses above (or below, after inversion) the slow MA on a
bar close, the strategy opens a position in the indicated direction. The
position is held until one of three exits fires, in priority order: stop-loss
(price moved against entry by `stop_loss_atr × ATR_at_open`), take-profit
(price moved in favour by `take_profit_atr × ATR_at_open`), or reversal (the
MA crossover flipped against the open position). ATR is a true-range measure
over bars; it's anchored at entry so the thresholds don't drift as volatility
changes. The SL/TP exits are checked intrabar on ticks. Backtests replay
`price_history` ticks intrabar, matching live; bars without tick coverage fall
back to the bar extreme; ticks are used only for intrabar exits and execution
pricing.

Every bar carries a YTM (the price pipeline guarantees it). A bar with a
missing YTM is treated as a data-contract violation in all modes: the run
records the violation and drops the bar, and historical loading fails loudly
rather than backfilling from a corrupt row.

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `signal_source` | string | `price` | Series the MA crossover runs on. One of `price`, `ytm`, `spread`. `spread` requires `tenor`. |
| `fast_window` | int | 8 | Fast-MA window, in bars. Must be at least 2. |
| `slow_window` | int | 96 | Slow-MA window, in bars. Must be greater than `fast_window`. |
| `atr_window` | int | 24 | True-range ATR window (in bars) for exits. Must be at least 2. |
| `stop_loss_atr` | float | 20 | Stop-loss distance in ATR units, anchored at entry. 0 disables. |
| `take_profit_atr` | float | 0 | Take-profit distance in ATR units from entry. 0 disables. |
| `min_order_size` | float | 0 | Skip opening when the computed quantity is below this. 0 disables. Decimal because Dora is a fractionalized market. |
| `max_order_size` | float | 0 | Clamp the order quantity down to this. 0 disables. |
| `max_position_size` | float | 1 | Maximum fraction of capital per trade. Must be in (0, 1]. |
| `order_book_id` | uuid | – | **Required.** DORA order book UUID used to locate the traded asset. |
| `tenor` | string | – | Benchmark Treasury tenor (e.g. `2Y`, `5Y`, `10Y`). Required when `signal_source` is `spread`. |
| `initial_balance` | float | 1 | Starting capital for backtests. Backtests require > 0; live runs may pass 0 and the strategy uses the user's USD balance from DORA. |
| `leverage` | float | 1 | Leverage multiplier for live orders. Must be greater than 0. |
| `resolution` | string | `15m` | Candle resolution for the signal series. One of `1m`, `5m`, `15m`, `1h`, `4h`, `1d`. |
| `volume_avg_window` | int | 20 | Bars in the rolling mean-volume window confirming entries; 0 disables the volume gate. |
| `volume_ratio_threshold` | float | 1.0 | Entry requires bar volume ≥ this × mean volume of the last `volume_avg_window` bars. |

### How the fields affect the strategy

- **`signal_source`** is the most consequential choice. `price` is the
  simplest — best for assets where price moves drive the trade thesis.
  `ytm` and `spread` translate yield-driven views into trades (a
  "bonds cheapen" thesis is a Sell signal in `spread` mode). Switching
  source mid-run isn't supported.
- **`fast_window` / `slow_window`** control trend sensitivity, counted in
  bars of `resolution`. Tight pairs (e.g. 4 / 32) catch trends early but
  whipsaw more; wide pairs (e.g. 8 / 96 at 15m ≈ 2h / 24h) ride bigger
  moves but with late entries.
- **`stop_loss_atr`** anchors at entry so the threshold doesn't drift if
  volatility changes mid-trade. Tight stops (3–10×ATR) cut losses fast but
  risk exiting before the trend develops; wider stops (20×ATR+) let trends
  play out at the cost of larger per-trade risk.
- **`take_profit_atr = 0`** disables take-profit and lets the position run
  until the MA crossover reverses. Set >0 to lock in gains on fast trends.
- **`volume_avg_window` / `volume_ratio_threshold`** gate entries on bar
  volume from the DORA candle stream: an entry requires the entry bar's
  volume to be at least `volume_ratio_threshold` × the mean volume of the
  last `volume_avg_window` bars. Set `volume_avg_window` to 0 to disable
  the gate.
- **`spread` mode** makes a daily FRED API call per missing benchmark date.
  The result is cached in-memory for the lifetime of the run; on a FRED
  outage the last cached yield is used (fetches throttled to one attempt
  per 5 minutes).

### Known v1 limitations

- Flat position sizing — every entry uses `max_position_size` of effective
  capital. No scaling by MA separation or trend strength.
- Backtests require `candles_history` coverage for the slow window at the
  chosen `resolution`; if coverage is missing the request fails with a 400
  naming the available range.
- Intrabar stop/take-profit exits evaluate on ticks in live runs; backtests
  replay `price_history` ticks intrabar, matching live, with bars lacking tick
  coverage falling back to the bar extreme (high/low).
- The volume gate uses DORA bar volume; books with sparse trade flow may
  need `volume_avg_window` = 0.
- A position that exists when the run starts (server restart, or an inherited
  exchange position) gets its stop/take-profit anchor from the startup price
  and current ATR, not the true entry price.
- No whipsaw neutral-band filter; tight MA pairs generate more false
  crossovers.
- Shared `signal_source` decision is per-run; switching requires a new
  run.
