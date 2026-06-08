---
title: Relative Value Trading in Fixed Income
category: concepts
tags: [relative-value, basis-trade, swap-spread, rich-cheap, arbitrage]
sources:
  - "[[BIS - Hedge Fund Treasury RV 2025]]"
  - "[[Huggins & Schaller - Fixed Income RV Analysis]]"
  - "[[J.P. Morgan - Systematic Treasury RV]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Strategies that exploit pricing discrepancies between closely related fixed income instruments. Core strategies include cash-futures basis, swap spread, on/off-the-run, and yield curve arbitrage.
provenance:
  extracted: 0.65
  inferred: 0.25
  ambiguous: 0.1
base_confidence: 0.83
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Relative Value Trading in Fixed Income

## Core Strategies

### Cash-Futures Basis Trade
- Long cash Treasury + short Treasury futures
- Exploits the difference between cash bond yields and implied futures yields
- Most well-documented RV strategy; the largest but stagnating since early 2024

### IRS Swap Spread Trade (Growing Rapidly)
- Long cash Treasury + pay-fixed in interest rate swap
- Exploits the discount at which cash USTs trade vs. IRS due to "inconvenience yield"
- **Size:** Grew from $281B (Q1 2024) to $631B (Q2 2025) — more than doubled
- **Risk:** Exposed to rate shocks; contracted 11% during April 2025 turbulence
- **Driver:** Bets on SLR regulatory relief for banks

### On-the-Run / Off-the-Run Arbitrage
- Long off-the-run (cheaper, less liquid) + short on-the-run (richer, more liquid)
- Exploits liquidity premium between most recently auctioned and older bonds

### Yield Curve Arbitrage (Butterfly / Fly)
- Long and short positions at different yield curve points
- Typically PCA-based: neutralize PC1 (level) and PC2 (slope), retain PC3 (curvature) exposure
- Also performed with fitted curve residuals (actual yield − model yield)

## Trading Signal Approaches

### Yield Error / Spread Differentials
- **Yield error:** Difference between actual yield and fitted curve yield for a bond
- **Matched-maturity swap spread switches:** Buy the bond with the cheapest swap spread, sell the richest in the same maturity bucket
- J.P. Morgan research shows these deliver better risk-adjusted returns than outright switches

### PCA-Based Rich/Cheap
- Compute residual = actual yield change − PCA-reconstructed yield change
- Positive residual = too rich (yield didn't rise as much as model predicts)
- Negative residual = too cheap (yield rose more than model predicts)
- Trade the expected reversion of residuals

### Model-Based Fair Value
- Use multi-factor term structure models to compute theoretical bond prices
- Enter butterfly when market price deviates from model value
- Hold until convergence (typically 1-12 months)

## Risk Characteristics

- RV strategies are **not pure arbitrage** — exposed to:
  - **Indirect default risk** (LIBOR/SOFR rates spike during banking stress)
  - **Funding risk** (repo rates, margin calls during volatility)
  - **Basis risk** (curve shape changes, CTD switches in futures)
- Returns are **positively skewed** — strategies generate larger offsetting positive returns than negative ones
- **Leverage is essential** — spreads are tight (often <50 bps), requiring 10-50x leverage for attractive returns

## Related Concepts

- [[Principal Component Analysis in Fixed Income]] — PCA for curve-neutral RV
- [[Swap Spread Analysis]] — theory and practice of swap spread trading
- [[Backtesting Considerations]] — biases that inflate RV strategy returns
