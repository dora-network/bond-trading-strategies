---
title: >-
  Numerix: How Backtesting Shapes Systematic Fixed Income Performance (2025)
category: references
tags: [backtesting, data-quality, systematic-strategies, transaction-costs, survivorship-bias]
source_url: "https://www.numerix.com/resources/white-paper/testing-strategy-how-backtesting-shapes-systematic-fixed-income-performance"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Comprehensive guide to rigorous backtesting for systematic fixed income, covering data limitations, corporate actions, transaction costs, and common biases that distort historical simulations.
provenance:
  extracted: 0.7
  inferred: 0.2
  ambiguous: 0.1
base_confidence: 0.85
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Numerix: Backtesting Systematic Fixed Income

**URL:** https://www.numerix.com/resources/white-paper/testing-strategy-how-backtesting-shapes-systematic-fixed-income-performance

**Published:** December 2025

## What It Covers

Industry white paper on best practices for systematic fixed income backtesting. Covers the full lifecycle: point-in-time data, corporate actions, default handling, transaction costs, and bias avoidance.

## Key Claims

1. **Transaction costs of 20-50 bps** per bond trade (BondWave Trade Insights, Jan 2025) are the standard estimate — critical to model explicitly *(extracted)*
2. **Survivorship bias** arises when backtests use today's bond universe, excluding defaulted names — makes historical performance look better than achievable *(extracted)*
3. **Point-in-time universe data** — knowing exactly which bonds were outstanding at each historical date — is required mitigation *(extracted)*
4. **Look-ahead bias** from using future information (e.g., 2010 rating downgrade applied to 2008 simulation) overstates returns *(extracted)*
5. **Corporate actions must be modeled:** bond calls, defaults at recovery (~40%), issuer mergers — ignoring these overstates performance for high-coupon bonds *(extracted)*
6. **Liquidity thresholds** ($300M+ outstanding) trade off signal breadth for execution feasibility — strategies validated on liquid universes more accurately represent live performance *(extracted)*
7. **Backtest credibility checklist:** point-in-time universe, defaults as realized losses, explicit transaction costs, separate in-sample/out-of-sample periods *(extracted)*

## Limitations

- Numerix commercial perspective — promotes their analytics platform
- Transaction cost estimates are averages; vary significantly by bond and market regime
- Does not address strategy capacity estimation
