---
title: Pairs Trading and Statistical Arbitrage
category: concepts
tags: [pairs-trading, cointegration, statistical-arbitrage, mean-reversion, market-neutral]
sources:
  - "[[Universal Pairs Trading System]]"
  - "[[Statistical Arbitrage Engine - Kalman Filter]]"
  - "[[Cointegration Pairs Trading - Walk Forward Validation]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  A market-neutral statistical arbitrage strategy that profits from mean reversion of the spread between two cointegrated assets. Applied to equities, bonds, commodities, and FX.
provenance:
  extracted: 0.7
  inferred: 0.2
  ambiguous: 0.1
base_confidence: 0.83
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Pairs Trading and Statistical Arbitrage

## Core Framework

1. **Identify cointegrated pairs:** Two price series are cointegrated if there exists a linear combination that is stationary, even though each series is individually non-stationary.
2. **Compute the spread:** `Spread(t) = P1(t) − β × P2(t)` where β is the hedge ratio.
3. **Standardize to Z-score:** `Z(t) = (Spread(t) − μ) / σ` over a rolling window.
4. **Trade mean reversion:** Enter when spread deviates; exit when it normalizes.

## Cointegration Testing

### Engle-Granger Two-Step
1. OLS regression: `P1(t) = α + β × P2(t) + ε(t)`
2. ADF test on residuals ε̂(t): reject unit root null at p < 0.05
- **Limitation:** Biased toward one leg as dependent variable; Johansen is more powerful

### Johansen Test
- Likelihood-ratio test on VECM
- Symmetric treatment of both legs
- **Best practice:** Require both EG (both orderings) and Johansen to pass

### Half-Life Filter
- Model spread as Ornstein-Uhlenbeck process: `dS(t) = λ(μ − S(t))dt + σdW`
- Half-life = `ln(2)/λ`
- **Tradable range:** 5-60 days (too noisy below, too slow above)

## Trading Rules

| Condition | Action |
|-----------|--------|
| Z < −2.0 | Long spread (long A, short B) |
| Z > +2.0 | Short spread (short A, long B) |
| \|Z\| < 0.5 | Close (mean reversion achieved) |
| \|Z\| > 3.5 | Stop-loss (regime change) |
| Holding > 30 days | Force close |

## Advanced Techniques

### Time-Varying Hedge Ratio (Kalman Filter)
- OLS gives a static β — equity relationships drift over time
- **State-space model:** Observation equation links prices via β(t); transition equation models β as random walk
- **Trade-off:** Higher δ = more responsive β but noisier estimates

### Walk-Forward Validation
- Split data into rolling train/test windows (e.g., 3-year train, 1-year test)
- Estimate β and confirm cointegration in-sample only
- Apply fixed β to out-of-sample period
- Prevents the classic mistake: pairs that look cointegrated in-sample often fail out-of-sample

### Regime Filtering
- HMM regime detector identifies high-volatility / trending regimes
- Suppress entries during unfavorable regimes
- Preliminary results: ~20% reduction in max drawdown at ~12% reduction in trade count

## Statistical Foundation for Bond Pairs

For bonds specifically, cointegration can be applied to:
- **Yield spreads** between two sovereign issuers (cross-currency pairs trading)
- **Credit spreads** between two corporate issuers in the same sector
- **Yield curve points** (butterfly spreads as a form of 3-asset "pairs" trade)

## Key Risks

1. **Decointegration:** Relationships break — walk-forward Sharpe decay signals this
2. **Divergence risk:** Spread may continue diverging before reverting
3. **Transaction costs:** Frequent trading erodes profits; pairs trading requires two trades per entry/exit
4. **Short-side constraints:** Borrow availability, short-sale proceeds not fully credited at retail level
5. **Multiple-testing bias:** Screening C(n,2) pairs produces false positives — apply Benjamini-Hochberg FDR correction

## Related Concepts

- [[Mean Reversion in Fixed Income]] — theoretical foundation
- [[Principal Component Analysis in FI]] — PCA for mean-reverting portfolio design
- [[Backtesting Considerations]] — walk-forward validation protocol
