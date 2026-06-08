---
title: >-
  Schroers (2026): Nonparametric Factor Structure of Bond Markets
category: references
tags: [pca, factor-model, high-frequency, sofr, mean-reversion]
source_url: "https://medium.com/@jsgastoniriartecabrera/the-bond-market-hides-far-more-than-3-factors-i-built-a-trading-bot-to-prove-it-f291dfc5a176"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Mathematical proof that bond markets have 8-16 factors (not 3), with a nonparametric framework for identifying tradable residual mispricings in SOFR futures using high-frequency PCA.
provenance:
  extracted: 0.65
  inferred: 0.25
  ambiguous: 0.1
base_confidence: 0.78
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Schroers: Nonparametric Factor Structure of Bond Markets

**URL:** Via Medium summary; original paper in Mathematical Finance 2026

**Author:** Dennis Schroers (University of Bonn)

## What It Covers

A nonparametric, infinite-dimensional framework that reveals the true number of random drivers in bond markets. Proves that the classic 3-factor (level/slope/curvature) model systematically misses 15%+ of continuous variation. Applied to SOFR futures for tradable signals.

## Key Claims

1. **More than 8 factors needed to explain 99% of variation** in almost every year 1990-2022; the classic 3-factor model explains 85% at best *(extracted)*
2. **Using 16 factors instead of 3 reduces approximation error** of 30-day bond returns from 0.50 to 0.07 *(extracted)*
3. **2-3 factors missed completely** by classical PCA in most years — the 3-factor model is structurally incomplete *(extracted)*
4. **7 COVID-shock days in 2020 explained as much variation as 243 normal days** — robust jump detection through truncation *(extracted)*
5. **Trading signal:** When a specific maturity contract has an extreme PCA residual (z-score > 2.0), it is temporarily cheap/rich relative to what the factor model predicts *(extracted)*
6. **Raw residuals are NOT directly tradable** — requires regime filters, cost modeling, dynamic k-selection, and per-maturity threshold calibration *(extracted)*
7. **Number of factors dropped from ~14 (early 1990s) to ~8-10 (2010s)** — market is becoming lower-dimensional over time *(inferred)*

## Implementation Requirements

For a tradeable strategy:
1. Regime filter (pause when Hilbert-Schmidt norm is elevated)
2. Per-regime k selection (fewer factors in calm markets)
3. Transaction cost model for SOFR futures spreads
4. Walk-forward PCA validation (expanding window)
5. Maturity-specific entry thresholds
6. Pair/spread expression rather than outright positions

## Limitations

- Trading bot built from the paper showed flat equity curve — signal mechanism is sound but not yet profitable without the enhancements listed
- Requires high-frequency data infrastructure
- SOFR futures specific; generalization to other bond markets unproven
