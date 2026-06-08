---
title: Backtesting Considerations for Fixed Income
category: concepts
tags: [backtesting, data-quality, look-ahead-bias, survivorship-bias, transaction-costs]
sources:
  - "[[Corporate Bond Factor Replication Crisis]]"
  - "[[Numerix - Backtesting Systematic FI]]"
  - "[[Imperial - Transaction Costs and Capacity]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Fixed income backtesting has unique pitfalls: Latent Implementation Bias, Look-Ahead Bias, survivorship bias, and unrealistic transaction cost assumptions can make a strategy look profitable when it isn't.
provenance:
  extracted: 0.7
  inferred: 0.2
  ambiguous: 0.1
base_confidence: 0.88
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Backtesting Considerations for Fixed Income

## Critical Biases

### Latent Implementation Bias (LIB)
- **Cause:** Using the same month-end price for signal computation and return measurement
- **Root issue:** TRACE prices record past transactions, not standing offers — a price observed in a backtest may not have been executable
- **Fix:** Time gap between signal and return:
  - **Signal gap:** Compute signal from earlier price (e.g., t−Δ)
  - **Return gap:** Measure return from month-begin price at t+1
- **Impact:** Without correction, reported factor returns can be significantly inflated

### Look-Ahead Bias (LAB)
- **Cause:** Return filtering thresholds computed from the full sample rather than from data available at portfolio formation
- **Example:** Winsorizing returns at percentiles computed from months t+1,...,T — information the investor didn't have at time t
- **Impact:** Ex-post winsorizing inflates Sharpe ratios from 0.5 to above 5 in extreme cases (documented in equity options)
- **Fix:** Ex-ante filtering — thresholds computed using only data through month t

### Survivorship Bias
- **Cause:** Using today's bond universe for historical backtests, excluding defaults
- **Impact:** A high-yield backtest showing no losses in 2001-2002 or 2008-2009 is almost certainly missing default data
- **Fix:** Point-in-time universe data — bonds that later defaulted must be included with recovery values (~40%) at default date

## Data Requirements

| Requirement | Why |
|-------------|-----|
| Point-in-time universe | Exclude bonds not yet issued; include bonds that later defaulted |
| Corporate actions database | Calls, defaults, mergers, CUSIP changes — thousands of events |
| Accurate price/total return series | From reliable providers; avoid stale or interpolated prices |
| Point-in-time ratings/fundamentals | Don't apply future downgrades to past periods |
| Transaction costs (20-50 bps/trade) | Explicitly modeled; varies by liquidity |
| Recovery rate assumptions | ~40% for defaulted bonds |

## Transaction Costs
- **IG corporate bonds:** 20-50 bps per trade (one-way)
- **HY / EM bonds:** Materially wider — must model explicitly, not flat assumption
- **Market impact:** MMI (market microstructure invariance) cost functions are upward-sloping by construction, unlike TRACE-based estimates that show volume discounts
- **Capacity:** Market portfolios have multi-trillion capacity; factor strategies have lower but still substantial capacity

## Credibility Checklist
- [ ] Point-in-time universe at each historical date
- [ ] Defaults reflected as realized losses, not exclusions
- [ ] Transaction costs explicitly modeled
- [ ] Separate in-sample / out-of-sample performance
- [ ] Signal-return price overlap eliminated (LIB correction)
- [ ] Ex-ante return filtering (LAB correction)
- [ ] Walk-forward or rolling-origin validation, not single train/test split
- [ ] Strategy validated on liquid, tradeable universe ($300M+ outstanding)

## Related Concepts

- [[Factor Investing in Corporate Bonds]] — strategies these backtests evaluate
- [[Relative Value Trading]] — RV strategies particularly sensitive to LIB/LAB
- [[Pairs Trading and Statistical Arbitrage]] — walk-forward validation critical
