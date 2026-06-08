---
title: Yield Curve Strategies
category: concepts
tags: [yield-curve, steepener, flattener, butterfly, duration]
sources:
  - "[[CFA Institute - Yield Curve Strategies]]"
  - "[[Duarte et al. - Fixed Income Arbitrage]]"
  - "[[BSIC - PCA Hedging]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Active strategies that position a bond portfolio to capitalize on expected changes in the level, slope, or curvature of the yield curve using cash bonds, futures, and swaps.
provenance:
  extracted: 0.65
  inferred: 0.25
  ambiguous: 0.1
base_confidence: 0.82
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Yield Curve Strategies

## Core Strategies

### Duration Management (Level)
- **Bullish on rates** (expect yields to fall): Increase duration via long bond positions, receive-fixed swaps, long futures
- **Bearish on rates** (expect yields to rise): Reduce duration via short positions, pay-fixed swaps, short futures
- Duration measures linear price-yield relationship; convexity captures second-order effects

### Steepeners and Flatteners (Slope)
- **Steepener:** Long short-dated bonds, short long-dated bonds — profits when 2s10s spread widens
- **Flattener:** Short short-dated bonds, long long-dated bonds — profits when spread narrows
- Must be **DV01-weighted** to ensure the position expresses a curve view, not a directional bias
- DV01 ratios: ZT ≈ $38, ZF ≈ $47, ZN ≈ $65, ZB ≈ $125
- Example: 2s10s steepener requires ~1.7 ZT contracts per 1 ZN contract for DV01 neutrality

### Butterfly Trades (Curvature)
- Three-legged trade: wings (short and long tenor) + belly (medium tenor)
- **Long butterfly (long belly):** Long body, short wings — profits when curvature increases (belly yields fall relative to wings)
- **Short butterfly (short belly):** Short body, long wings — profits when curvature decreases
- Weights can be DV01-based, regression-based (OLS), or PCA-based

## PCA-Based Curve Trading

Principal Component Analysis decomposes yield curve movements into orthogonal factors:
- **PC1 (Level):** ~75-80% of variance — parallel shifts
- **PC2 (Slope):** ~15-20% — steepening/flattening
- **PC3 (Curvature):** ~1-5% — butterfly movements
- First 3 PCs explain ~95-99% of yield curve variance

PCA can be used to:
1. Construct curve-neutral butterfly trades (hedge PC1 and PC2, retain PC3 exposure)
2. Identify relative value: compute residual = actual yield − PCA-reconstructed yield; large residuals signal rich/cheap bonds
3. Assess historical plausibility of yield curve scenarios

**Important caveat:** PCA-weighted butterfly spreads that were stationary in 1989-1999 no longer produce stationary spreads on modern data — yield curve dynamics have changed.

## Related Concepts

- [[Carry and Rolldown]] — baseline return before curve positioning
- [[Duration Management]] — managing level exposure
- [[Principal Component Analysis in Fixed Income]] — PCA methodology
- [[DV01 and Risk Weighting]] — ensuring curve-neutral positions
