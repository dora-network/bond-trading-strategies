---
title: Mean Reversion in Fixed Income
category: concepts
tags: [mean-reversion, spread-trading, cointegration, statistical-arbitrage, ornstein-uhlenbeck]
sources:
  - "[[SRMR Model for Credit Spread Dynamics]]"
  - "[[Dimensional - Mean Reversion in FI Premiums]]"
  - "[[Mean-Reverting Portfolio Design]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Mean reversion strategies bet that deviations from a long-term equilibrium will correct. In fixed income, this applies to credit spreads, yield spreads, and yield curve shapes — but the evidence for profitable mean reversion is mixed.
provenance:
  extracted: 0.6
  inferred: 0.3
  ambiguous: 0.1
base_confidence: 0.78
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Mean Reversion in Fixed Income

## Where It Works

### Credit Spread Mean Reversion
- **Spread-return mean-reverting (SRMR) model:** Hybrid of Black-Karasinski (spreads) and Ornstein-Uhlenbeck (returns) processes. Captures spread mean-reversion, heavy tails, and return autocorrelation better than single-process models.
- **CDS spreads** empirically exhibit stationarity of returns, positive autocorrelations, and two-sided heavy-tailed distributions.
- **Cross-sectional mean reversion:** Bonds with spreads wide relative to peers (same rating/maturity) tend to outperform — this is the bond value factor.

### Yield Spread Mean Reversion (Cross-Currency)
- Government bond yield differentials mean-revert around structurally stable equilibria
- Z-score based entry/exit on yield spreads between two economies generates FX trading signals
- Yield curve slope (2s10s) can be used as a regime filter

### Cointegration-Based Pairs Trading
- Two non-stationary bond price/yield series can be cointegrated — their spread is stationary
- **Engle-Granger two-step:** OLS regression for hedge ratio → ADF test on residuals
- **Johansen test:** Symmetric treatment of both legs, more powerful in finite samples
- **Half-life filter:** Pairs with 5-60 day OU half-life are tradable; too noisy (<5d) or too slow (>60d)

## Where It Doesn't Work

### Term Premium Mean Reversion (Does NOT Work)
- Dimensional Fund Advisors study: 18/24 term premium mean reversion strategies delivered **negative** excess returns
- Betting on reversal of past term premiums is unreliable — variable maturity strategies using *current* term spreads consistently outperform
- **Key insight:** Use real-time market information embedded in current yield curves, not past realized premiums

### Credit Premium Mean Reversion (Does NOT Work)
- 13/18 credit premium mean reversion strategies underperformed 50/50 benchmarks
- Variable credit approach (using current credit spreads, not past premiums) consistently outperforms
- The statistical relation between *past* premiums and *future* premiums is much weaker than the relation between *current* spreads and future premiums

## Models

| Model | Application | Strength |
|-------|------------|----------|
| **Ornstein-Uhlenbeck (OU)** | Spread modeling, half-life estimation | Simple, closed-form, captures mean-reversion speed |
| **SRMR (Spread-Return MR)** | CDS spread dynamics | Captures both spread MR and return autocorrelation |
| **Kalman Filter** | Time-varying hedge ratio | Adapts to changing relationships (vs. static OLS) |
| **VAR/VECM** | Multi-asset cointegration | Captures adjustment dynamics, not just spread |

## Related Concepts

- [[Pairs Trading and Statistical Arbitrage]] — cointegration-based strategies
- [[Factor Investing in Corporate Bonds]] — value factor = cross-sectional mean reversion
- [[Relative Value Trading]] — rich/cheap signals based on mean reversion
