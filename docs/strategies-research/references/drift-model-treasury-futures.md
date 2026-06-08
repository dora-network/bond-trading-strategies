---
title: >-
  DRIFT Model: Statistical Arbitrage on Treasury Futures with PCA Hedging
category: references
tags: [treasury-futures, mean-reversion, pca, statistical-arbitrage, portfolio-optimization]
source_url: "https://github.com/jerryxyx/TreasuryFutureTrading"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Mean-reversion strategy on 5 Treasury futures (TU, FV, TY, US, UB) with PCA-based hedging against parallel shift and log(T) slope. Optimizes weights to maximize distance from historical mean while maintaining insensitivity to curve movements.
provenance:
  extracted: 0.7
  inferred: 0.2
  ambiguous: 0.1
base_confidence: 0.82
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# DRIFT Model: Treasury Futures Statistical Arbitrage

**URL:** https://github.com/jerryxyx/TreasuryFutureTrading

## What It Covers

A portfolio-level mean-reversion strategy on the 5 major Treasury futures (TU, FV, TY, US, UB). The core innovation is optimizing portfolio weights to maximize distance (in standard deviations) from the historical mean while maintaining insensitivity to yield curve level and log(T) slope changes.

## Key Claims

1. **Portfolio of 5 futures is mean-reverting** in certain directions while following random walks in others *(extracted)*
2. **Optimization objective:** Minimize standardized value `(Current Price − MA) / StdDev` subject to two constraints: zero sensitivity to parallel shift (duration) and zero sensitivity to slope-up-by-log(T) *(extracted)*
3. **Two hedging constraints** reduce the weight space from 5D to 3D — enough to find an optimal direction for mean reversion *(extracted)*
4. **Entry/exit logic:** Enter when optimized standardized value < entry threshold × bid-ask spread; exit when value > exit threshold or < stop-loss threshold *(extracted)*
5. **Bond representation:** Each future is represented by the CTD (cheapest-to-deliver), OTR (on-the-run), or a virtual bond with fixed duration and DlogD sensitivity *(extracted)*
6. **Moving average types:** SMA over sliding window (e.g., SMA300) or EMA with equivalent decay factor *(extracted)*
7. **Minimum change quantity** filter prevents position adjustments that are smaller than transaction costs *(extracted)*

## Mathematical Framework

```
Objective:    minimize (V − μ) / σ = Σ(pᵢ·wᵢ) − MA(Σ(pᵢ·wᵢ)) / std(Σ(pᵢ·wᵢ))
Constraints:  Σ(dᵢ·pᵢ·wᵢ) = 0              (parallel shift neutral)
              Σ(dᵢ·ln(dᵢ)·pᵢ·wᵢ) = 0      (slope neutral)
```

## Implementation Parameters

| Parameter | Typical Value | Purpose |
|-----------|--------------|---------|
| SMA window | 300 periods | Mean estimation |
| Entry threshold | ∼2.0 (in σ units) | Signal generation |
| Exit threshold | ∼0.5 (in σ units) | Mean reversion achieved |
| Stop-loss | Custom | Risk control |
| Min change quantity | 200 contracts | Cost filter |

## Limitations

- Only 5 futures instruments — limited diversification
- Static constraint structure assumes stable yield curve dynamics
- Does not address CTD switch risk
- Transaction cost model simplified
