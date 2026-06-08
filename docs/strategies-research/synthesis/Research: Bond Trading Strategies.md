---
title: >-
  Research: Bond Trading Strategies
category: synthesis
tags: [bond-trading, factor-investing, relative-value, yield-curve, systematic-strategies, research]
sources:
  - "[[Common Factors in Corporate Bond Returns]]"
  - "[[BIS - Hedge Fund Treasury RV 2025]]"
  - "[[Fixed Income Relative Value Analysis]]"
  - "[[Corporate Bond Factor Replication Crisis]]"
  - "[[Risk and Return in Fixed Income Arbitrage]]"
  - "[[Vanguard - Tech Pillars]]"
  - "[[State Street SAFI Q1 2026]]"
  - "[[Numerix - Backtesting Systematic FI]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Synthesis of 3-round research on bond trading strategies covering factor investing, relative value, yield curve, mean reversion, PCA, backtesting methodology, and electronic market structure. 20+ sources consulted.
provenance:
  extracted: 0.6
  inferred: 0.3
  ambiguous: 0.1
base_confidence: 0.8
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Research: Bond Trading Strategies

## Overview

Bond trading strategies span a wide spectrum — from traditional yield curve positioning to systematic factor investing, from manual relative value analysis to AI-driven electronic execution. The market is undergoing rapid transformation: electronic trading now covers ~60% of Treasuries and ~50% of IG corporates, systematic fixed income strategies are growing from <2% to mainstream adoption, and AI/ML tools are reshaping price discovery and execution. This research surveys the strategy landscape, identifies proven approaches, and documents the critical implementation pitfalls.

## Key Findings

- **Factor investing works in bonds** — carry, value, momentum, and defensive factors generate statistically significant risk-adjusted returns in corporate bonds with low correlation to equity factors [[Common Factors in Corporate Bond Returns]]
- **Relative value strategies remain the core** of hedge fund fixed income activity — the IRS swap spread trade has more than doubled to $631B and is now the primary driver of Treasury repo leverage [[BIS - Hedge Fund Treasury RV 2025]]
- **Mean reversion in term/credit premiums is NOT reliably profitable** — using current market information (spreads, term structure) consistently outperforms betting on reversal of past premiums [[Dimensional - Mean Reversion in FI Premiums]]
- **Backtesting bias is severe and underappreciated** — Latent Implementation Bias and Look-Ahead Bias can make unprofitable strategies appear profitable. Point-in-time universes, signal-return gaps, and ex-ante filtering are required [[Corporate Bond Factor Replication Crisis]]
- **PCA remains the workhorse statistical tool** for yield curve decomposition, hedging, and relative value identification — but PCA-weighted butterfly strategies have weakened significantly since the 1990s [[Principal Component Analysis in Fixed Income]]
- **Electronic trading and AI** are reshaping execution: MarketAxess CP+ provides AI-generated reference prices, Vanguard reduced ETF basket creation from 2 hours to 10 minutes, and liquidity aggregation processes millions of dealer quotes daily [[Vanguard - Tech Pillars]]
- **Transaction costs are the strategy killer** — 20-50 bps per trade can consume the entire gross alpha of high-turnover strategies. Low-turnover approaches consistently outperform after costs [[Numerix - Backtesting Systematic FI]]

## Core Concepts

- [[Carry and Rolldown]] — The baseline return of any bond position; everything else is a deviation
- [[Yield Curve Strategies]] — Duration management, steepeners/flatteners, butterfly trades using PCA
- [[Factor Investing in Corporate Bonds]] — Carry, value, momentum, defensive: definitions, Sharpe ratios, implementation
- [[Relative Value Trading in Fixed Income]] — Basis trades, swap spreads, on/off-the-run, yield curve arbitrage
- [[Mean Reversion in Fixed Income]] — Where it works (cross-sectional spreads, cointegrated pairs) and where it doesn't (term/credit premiums)
- [[Backtesting Considerations for Fixed Income]] — LIB, LAB, survivorship bias, transaction costs, data requirements
- [[Principal Component Analysis in Fixed Income]] — PCA decomposition, hedging, butterfly construction, RV signals
- [[Pairs Trading and Statistical Arbitrage]] — Cointegration testing, Kalman filter hedge ratios, walk-forward validation

## Entities & Tools

- [[TRACE]] — FINRA's mandatory corporate bond transaction reporting; primary data source requiring careful cleaning
- [[PCA]] — Principal Component Analysis: level/slope/curvature decomposition of yield curves
- [[MarketAxess]] — Largest electronic corporate bond marketplace; CP+ AI reference prices
- [[DV01]] — Dollar Value of a Basis Point; essential for curve-neutral position sizing
- [[Ornstein-Uhlenbeck Process]] — Mean-reverting stochastic model for spreads; half-life estimation
- [[FRED]] — Federal Reserve data; Treasury yields, credit spreads, macro indicators

## Contradictions & Open Questions

### Contradiction 1: Mean Reversion Works — Sometimes
Cross-sectional mean reversion in credit spreads (the value factor) is well-documented and profitable. But time-series mean reversion in term and credit premiums is NOT profitable — using current spreads is consistently better than betting on reversal of past premiums. The distinction between cross-sectional and time-series mean reversion is critical and often conflated.

### Contradiction 2: PCA Butterflies — Then vs. Now
PCA-weighted butterfly spreads that were stationary and tradeable in 1989-1999 are no longer stationary on 2012-2022 data. Yield curve dynamics have fundamentally changed — possibly due to QE, increased electronic trading, or structural shifts in the rate environment. The Salomon Brothers (2000) methodology may need substantial adaptation for modern markets.

### Open Question: Capacity of Systematic Bond Strategies
Estimates range from $4.5T to $11T for broad market portfolios, but factor strategies have lower capacity. The intersection of signal decay, market impact, and strategy crowding is poorly understood for credit factors specifically.

### Open Question: AI/ML Signal Generation
While Vanguard and MarketAxess demonstrate AI-driven price discovery and signal generation, the source of their edge is unclear — is it better data processing (handling unstructured text), nonlinear pattern recognition, or simply faster execution? Independent academic validation is limited.

## Strategy Archetype Summary

| Strategy Type | Approach | Sharpe Potential | Key Risk |
|--------------|----------|-----------------|----------|
| **Carry harvesting** | Long high-spread bonds, fund at short rate | 0.5-0.9 | Spread widening, defaults |
| **Value (cross-sectional MR)** | Long cheap vs. peers; short rich | 0.5-0.7 | Value traps, continued cheapening |
| **Momentum (trend)** | Long winners, short losers (6-12m) | 0.5-0.8 | Reversals, crowding |
| **Defensive (low-risk)** | Long low-leverage, high-profitability issuers | 0.3-0.5 | Flight-from-quality during rallies |
| **Multi-factor** | Equal-risk combination of above | 0.9-1.2 | Factor correlation during crises |
| **Yield curve RV** | Butterfly, steepener/flattener | 0.3-0.6 | Curve regime changes |
| **Basis/swap spread** | Cash-futures, swap spread arbitrage | 0.4-0.7 | Rate shocks, funding stress |
| **Pairs/stat arb** | Cointegrated pairs mean reversion | 0.5-1.0 | Decointegration, transaction costs |
| **Duration timing** | Adjust duration based on rate view | 0.2-0.4 | Rate forecast accuracy |

## Implementation Roadmap (For Strategy Development)

### Phase 1: Data Infrastructure
1. Acquire clean TRACE data (or use PyBondLab open-source pipeline)
2. Build point-in-time universe tracker with corporate actions database
3. Source Treasury CMT data from FRED
4. Implement signal-return gap to eliminate LIB

### Phase 2: Factor Research
1. Replicate known factors (carry, value, momentum, defensive) with LIB/LAB corrections
2. Test factor combinations and weighting schemes
3. Walk-forward validation with realistic transaction costs (20-50 bps)
4. Assess capacity and signal decay

### Phase 3: Strategy Construction
1. Select factor combination with robust out-of-sample performance
2. Implement PCA-based risk decomposition and hedging
3. Add regime filter (HMM or simpler volatility-based) for drawdown control
4. Model transaction costs with MMI-based impact functions

### Phase 4: Live Deployment
1. Connect to electronic trading platform (MarketAxess, Tradeweb, Bloomberg)
2. Implement execution algorithms with RFQ optimization
3. Continuous monitoring: Sharpe decay, factor exposure drift, cost analysis
4. Regular walk-forward re-estimation to detect strategy obsolescence

## Sources Consulted

- [[Common Factors in Corporate Bond Returns]] (Israel, Palhares, Richardson)
- [[BIS - Hedge Fund Treasury RV 2025]]
- [[Fixed Income Relative Value Analysis]] (Huggins & Schaller)
- [[Corporate Bond Factor Replication Crisis]] (Robotti et al.)
- [[Risk and Return in Fixed Income Arbitrage]] (Duarte, Longstaff, Yu)
- [[Vanguard - Tech Pillars]]
- [[State Street SAFI Q1 2026]]
- [[Numerix - Backtesting Systematic FI]]
- BBH: Introducing Systematic Fixed Income (Jorge Aseff, 2026)
- J.P. Morgan: Systematic Treasury RV Trading
- SSRN: 221 Years of Factor Premiums in Government Bonds
- AQR: Value, Momentum, Carry Work in Bonds Too
- Dimensional: Should You Bet on Mean Reversion?
- Quoniam: Factor Investing — Equity vs. Corporate Bonds
- SIFMA: Fixed Income Market Structure Compendium (2025)
- CAIA: Introduction to Carry Strategies
- yieldcurve.pro: Carry and Rolldown
