---
title: Principal Component Analysis in Fixed Income
category: concepts
tags: [pca, yield-curve, risk-management, hedging, butterfly]
sources:
  - "[[Huggins & Schaller - Fixed Income RV Analysis]]"
  - "[[BSIC - PCA Hedging]]"
  - "[[PCA Butterfly Replication - Analytic Musings]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  PCA decomposes yield curve movements into orthogonal factors (level, slope, curvature). Used for risk management, curve-neutral butterfly construction, and relative value identification.
provenance:
  extracted: 0.7
  inferred: 0.2
  ambiguous: 0.1
base_confidence: 0.85
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Principal Component Analysis in Fixed Income

## PCA Decomposition of the Yield Curve

Eigendecomposition of the covariance matrix of yield changes across tenors produces principal components:

| Component | Interpretation | Variance Explained | Loading Pattern |
|-----------|---------------|-------------------|-----------------|
| **PC1** | Level (parallel shift) | 75-81% | All tenors same sign, roughly equal |
| **PC2** | Slope (steepening/flattening) | 15-21% | Short tenors opposite sign to long tenors |
| **PC3** | Curvature (butterfly) | 1-5% | Wings same sign, belly opposite |

First 3 PCs explain ~95-99% of yield curve variance across markets.

## Applications

### 1. Risk Management / Hedging
- Decompose portfolio risk into PC exposures
- Hedge unwanted factor exposures: neutralize PC1 (level) and PC2 (slope), retain desired PC3 (curvature)
- **PCA-based hedging** uses factor loading matrix inversion to compute trade weights:
  - For a butterfly trade with 2Y-5Y-10Y, solve `notionals = f(loadings, DV01s)` to determine how much of each leg to trade

### 2. Curve-Neutral Butterfly Construction
- Use PC3 loadings as weights for butterfly trades
- In theory, these are uncorrelated with level and slope changes
- **Critical caveat:** PCA-weighted butterflies that were stationary in 1989-1999 are NO LONGER stationary on 2012-2022 data — yield curve dynamics have fundamentally changed
- Z-scoring weights over rolling windows can create more stationary spreads, but breaks the theoretical level/slope neutrality

### 3. Relative Value Identification
- PCA-reconstructed yields represent "fair value" given curve dynamics
- **Residual = actual yield change − PCA-reconstructed yield change**
- Large positive residual → bond too rich (yield didn't rise as much as model predicts)
- Large negative residual → bond too cheap
- Trade the expected reversion of residuals

### 4. Historical Plausibility of Yield Curve Scenarios
- **Explanatory power:** What % of historical movement does a scenario represent?
- **Shape plausibility:** Is the shape of the move consistent with historical PC patterns?
- **Magnitude plausibility:** How many standard deviations is the move?
- Used to stress-test portfolios with historically consistent scenarios

## Implementation Considerations

| Parameter | Recommendation |
|-----------|---------------|
| Rolling window | 120 days (trade-off: smoothness vs. responsiveness) |
| Data stationarity | De-trend or use yield changes, not levels |
| Sign flipping | Use cosine similarity to detect and correct eigenvector sign inversions across rolling windows |
| Number of PCs | 3 sufficient for most applications; more for MBS or complex instruments |
| Data frequency | Daily for trading signals; monthly for strategic allocation |

## Limitations
- PC loadings are not stable over time — market dynamics change
- Correlation between PCs can appear in sub-periods (they're only uncorrelated over the full sample)
- PCA assumes linear relationships; non-linear yield curve dynamics exist
- PCA-weighted butterfly trades weakened significantly since the original 2000 Salomon Brothers paper

## Related Concepts

- [[Yield Curve Strategies]] — butterfly trades using PCA weights
- [[Relative Value Trading]] — PCA residuals as RV signals
- [[Backtesting Considerations]] — rolling window biases in PCA estimation
