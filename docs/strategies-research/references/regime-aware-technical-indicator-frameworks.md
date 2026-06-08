---
title: >-
  RegimeFolio & CPO: Regime-Aware Technical Indicator Frameworks (2024-2025)
category: references
tags: [regime-detection, conditional-parameter-optimization, machine-learning, technical-indicators, volatility]
source_url: "https://www.interactivebrokers.com/campus/ibkr-quant-news/conditional-parameter-optimization-adapting-parameters-to-changing-market-regimes/"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Conditional Parameter Optimization (CPO) uses random forest ML to adapt trading parameters to market regimes. RegimeFolio achieves 137% cumulative return with VIX-based regime segmentation + sector-specific ensemble forecasting.
provenance:
  extracted: 0.65
  inferred: 0.25
  ambiguous: 0.1
base_confidence: 0.78
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Regime-Aware Technical Indicator Frameworks

## Conditional Parameter Optimization (CPO)

**Source:** PredictNow.ai / Ernest Chan (Interactive Brokers, 2021)

### What It Covers

A machine learning approach that adapts trading strategy parameters to prevailing market conditions. Uses random forest with boosting to learn from a large feature set capturing market conditions.

### Key Claims

1. **Fixed-parameter optimization fails** when market regimes change — parameters optimized on historical data become suboptimal *(extracted)*
2. **Walk-forward optimization with rolling windows** still cannot respond fast enough to rapid regime changes *(extracted)*
3. **CPO uses supervised ML** to predict strategy outcomes based on market features + parameter values, enabling dynamic parameter selection *(extracted)*
4. **Applied to Bollinger Band mean-reversion** on GLD (gold ETF) — parameters adapt daily to volatility regime *(extracted)*

## RegimeFolio (2025)

### Key Claims

1. **VIX-based regime classification** into low, medium, high volatility — rolling 252-day terciles (~17.8, ~23.1 thresholds) *(extracted)*
2. **Separate ensemble models per regime + sector** — Random Forest + Gradient Boosting trained on regime-specific subsets *(extracted)*
3. **137% cumulative return with Sharpe 1.17** over 2020-2024, outperforming regime-agnostic benchmarks by 15-20% in forecast accuracy *(extracted)*
4. **Technical indicator features** used: RSI, MACD, Bollinger Bands, rolling volatility — standardized separately within each regime *(extracted)*
5. **Regime-stratified cross-validation** prevents hyperparameter contamination across regimes *(extracted)*
6. **Feature attribution (SHAP)** confirms models capture economically meaningful drivers within each regime *(extracted)*

## Yield Curve Regime Classification (TradingView)

Six canonical yield curve regimes from crossing curve direction (steepen/flatten) with yield direction (rise/fall):

| Regime | Curve | Short Yield | Long Yield | Macro Signal |
|--------|-------|-------------|------------|-------------|
| Bull Steepener | Widens | Falls | Falls (faster) | Early easing cycle |
| Bear Steepener | Widens | Rises | Rises (faster) | Inflation / term premium |
| Steepener Twist | Widens | Falls | Rises | Reflation pivot |
| Bull Flattener | Narrows | Falls | Falls (faster) | Flight to quality |
| Bear Flattener | Narrows | Rises | Rises (faster) | Aggressive tightening |
| Flattener Twist | Narrows | Rises | Falls | Stagflation signal |

With Z-score strength scoring, multi-horizon confluence, dwell-time, and rolling frequency statistics.

## Limitations

- CPO demonstrated on equities/ETFs, not directly on individual bonds
- RegimeFolio tested on 34 large-cap equities; bond-specific validation pending
- Yield curve regimes require adaptation for credit/individual bond context
