---
title: >-
  The Corporate Bond Factor Replication Crisis (Robotti et al. 2025)
category: references
tags: [backtesting, corporate-bonds, look-ahead-bias, data-quality, trace]
source_url: "https://arxiv.org/html/2604.07880"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Documents systematic measurement errors and biases in corporate bond factor research, introducing Latent Implementation Bias (LIB) and Look-Ahead Bias (LAB) that inflate reported factor returns.
provenance:
  extracted: 0.75
  inferred: 0.15
  ambiguous: 0.1
base_confidence: 0.88
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# The Corporate Bond Factor Replication Crisis

**URL:** https://arxiv.org/html/2604.07880

**Authors:** Dickerson, Robotti, Rossetti, et al. (companion: openbondassetpricing.com)

## What It Covers

Systematic analysis of measurement and implementation biases across 108 corporate bond factors. Introduces formal framework for Latent Implementation Bias (LIB) and Look-Ahead Bias (LAB), with open-source corrected data and PyBondLab software.

## Key Claims

1. **Latent Implementation Bias (LIB):** Using the same month-end price for signal computation and return measurement creates mechanical correlation that inflates returns — arises from TRACE transaction prices being past trades, not executable quotes *(extracted)*
2. **Two gap procedures** break the signal-return link: signal gap (compute signal from earlier price) and return gap (measure return from month-begin price at t+1) *(extracted)*
3. **Look-Ahead Bias (LAB):** Ex-post return filtering (winsorization/trimming with full-sample thresholds) creates infeasible trading strategies — returns that no investor could have achieved in real time *(extracted)*
4. **Transaction costs:** 20-50 bps per trade is standard estimate for IG corporate bonds; can consume significant fraction of gross alpha *(extracted)*
5. **Survivorship bias:** Excluding defaulted bonds from universe dramatically overstates performance — must use point-in-time universes *(extracted)*
6. **Open-source pipeline** (PyBondLab) provides error-corrected daily and monthly TRACE data at openbondassetpricing.com *(extracted)*
7. **Standard backtests** that use signal price as return denominator measure returns that no investor could actually capture *(extracted)*

## Limitations

- Focus on monthly frequency; intraday strategies may have different biases
- Signal gap approach assumes serially uncorrelated measurement errors
- Does not fully address liquidity-driven implementation challenges
