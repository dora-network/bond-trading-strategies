---
title: Technical Indicators in Continuous Bond Markets
category: concepts
tags: [technical-indicators, macd, rsi, bollinger, moving-averages, adx, regime-adaptation, continuous-markets]
sources:
  - "[[Fong & Wu - Technical Trading Rules]]"
  - "[[Méndez - Technical Analysis Treasury Bonds]]"
  - "[[Regime-Aware Technical Indicator Frameworks]]"
  - "[[DRIFT Model]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Technical indicators are proven effective on bond markets when properly adapted. Multi-indicator combinations with regime awareness and parameter optimization consistently outperform single-indicator approaches.
provenance:
  extracted: 0.6
  inferred: 0.3
  ambiguous: 0.1
base_confidence: 0.8
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Technical Indicators in Continuous Bond Markets

## Proven Effectiveness in Bond Markets

Technical indicators are NOT just for equities and FX. Evidence across 48 sovereign bond markets with 27,000 rule variants confirms predictability, especially in emerging markets (China: 5.2% excess return). Multi-indicator strategies consistently beat single-indicator approaches.

## Core Indicator Families

### Trend-Following Indicators

| Indicator | Bond Adaptation | Best Parameter Range | Signal |
|-----------|----------------|---------------------|--------|
| **SMA Crossover** | On bond price or yield (inverted) | Short: 5-20, Long: 50-200 | Trend direction |
| **MACD** | On bond price or ETF price | (12,26,9) or (12,26,0) | Momentum + trend |
| **ADX** | On yield spread or price | 14-period | Trend strength |
| **Supertrend** | On bond price | (10,3) | Binary trend direction |

**Key insight:** MACD combined with Bollinger Bands + RSI is the most effective Treasury bond strategy tested.

### Momentum Oscillators

| Indicator | Bond Adaptation | Entry/Exit Logic |
|-----------|----------------|-----------------|
| **RSI** | On price (not yield — yield is inverted) | RSI < 30 oversold/buy; > 70 overbought/sell |
| **Stochastic** | On price | %K < 20 oversold; > 80 overbought |
| **ROC (Rate of Change)** | On yield change or price change | Positive → momentum up |
| **CCI** | On price | ±100 thresholds |

**Key insight:** RSI(21,50) centerline crossover outperforms traditional RSI(14,30/70) in multiple markets. Centerline crossover is more robust for bonds.

### Volatility Indicators

| Indicator | Bond Adaptation | Signal |
|-----------|----------------|--------|
| **Bollinger Bands** | On price or spread | Price outside bands → mean reversion |
| **ATR** | On price | Position sizing, stop-loss placement |
| **Volatility compression** | Short/long vol ratio | Compression → impending breakout |

### Volume Indicators (Continuous Market Only)

| Indicator | Bond Adaptation | Signal |
|-----------|----------------|--------|
| **OBV (On-Balance Volume)** | On CLOB trade volume | Accumulation/distribution |
| **MFI (Money Flow Index)** | Price × volume | Capital flow strength |
| **Volume profile** | On CLOB data | Support/resistance zones |

**Key insight:** Volume indicators are currently impractical for OTC bonds but become fully viable in a continuous CLOB market. This is a major new capability unlocked by fractionalization.

## Critical Bond-Specific Adaptations

### 1. Yield vs. Price Direction (THE Critical Adaptation)
**Bond prices RISE when yields FALL.** Technical indicators designed for equities assume "up = good." For bonds, you must decide whether to apply indicators to:
- **Price** (natural: up = profitable long) — preferred for most indicators
- **Yield** (inverted: yield down = bond up) — preferred for macro/curve analysis
- **Inverted yield** (negate yield) — makes indicators directionally consistent

### 2. Duration Scaling
Indicators applied to price are sensitive to duration. A 1% yield move affects:
- SHY (2Y): ~2% price change
- TLT (20Y+): ~17% price change

**Solution:** Duration-normalize the series or apply indicators to duration-hedged returns.

### 3. Maturity-Driven Volatility
Longer-dated bonds have higher price volatility. ATR(14) on TLT is ~5-10× larger than on SHY. **Always volatility-normalize** when comparing signals across maturities.

### 4. Coupon Effects
Bond prices include accrued interest (dirty price) vs. clean price. Use total return (price + coupon) for momentum/trend indicators, clean price for mean-reversion.

## Multi-Indicator Fusion Architectures

### Architecture 1: Signal Fusion (Weighted Voting)
Used by bond-trading-platform with 4 signals:
- Signal 1: Yield curve shape → 35% weight
- Signal 2: Rate momentum → 25% weight
- Signal 3: Credit spread → 20% weight
- Signal 4: Price momentum (SMA crossover + mean reversion) → 20% weight
- **Output:** Composite score (−100 to +100)

### Architecture 2: Confirmation Stack
Used by PairTrade Finder:
- Layer 1: Z-score identifies statistical mispricing
- Layer 2: Technical bias stack evaluates directional conviction
  - Supertrend (trend direction)
  - MACD histogram slope (momentum acceleration)
  - ADX + DI (trend strength)
  - OBV (volume confirmation)
- Only execute when both layers agree

### Architecture 3: Regime-Gated
Used by RegimeFolio:
- Step 1: Classify current regime (VIX-based or yield-curve-based)
- Step 2: Select regime-specific indicator parameters (CPO)
- Step 3: Generate signals using regime-appropriate indicators
- Step 4: Validate with regime-stratified confidence thresholds

## Institutional-Grade Indicator Enhancements

Raw indicators suffer from three problems in bond markets:
1. **Scale dependence** — indicators produce different values for different bonds
2. **Volatility sensitivity** — indicators break during regime shifts
3. **Non-comparability** — cannot rank signals across the universe

### Transformations

| Raw Indicator | Institutional Enhancement | Benefit |
|--------------|--------------------------|---------|
| Supertrend | Normalized Trend Slope (NTS) = z-score of 20D linear slope / ATR(20) | Volatility-adjusted, cross-asset comparable |
| MACD slope | Normalized Momentum Z-Score (NMZ) = z-score of MACD histogram over 252D | Expresses statistical rarity |
| ADX | Volatility-Adjusted Trend Strength (VATS) = ADX × (1 − ATR percentile) | Regime-aware trend strength |
| OBV | Normalized OBV Flow (NOBF) = z-score of OBV over rolling window | Detects unusual flow |
| Bollinger %B | Z-score of price/spread over adaptive window | Captures regime-adaptive deviation |

## Regime-Adaptive Parameter Optimization (CPO)

**Problem:** MACD(12,26,9) or RSI(14) are fixed parameters — they were calibrated decades ago and don't adapt to current market conditions.

**Solution:** Conditional Parameter Optimization
1. Define a feature set capturing market state (volatility, curve shape, spread levels, VIX, correlation)
2. Train a random forest to predict strategy outcome given (features, parameters)
3. At each decision point, select parameters that maximize predicted outcome under current features
4. Re-train periodically with expanding window

**Key finding:** Bollinger Band mean-reversion parameters adapted via CPO significantly outperform static parameters.

## Strategy: DRIFT Model — PCA-Hedged Mean Reversion

A complete example of technical indicators + statistical arbitrage on bonds:

1. Construct portfolio of 5 Treasury futures (TU, FV, TY, US, UB)
2. Optimize weights to maximize distance from historical mean
3. Constrain: zero sensitivity to parallel shift AND log(T) slope
4. Use SMA/EMA for mean estimation (SMA300 typical)
5. Entry: standardized value < −2.0 × bid-ask spread
6. Exit: standardized value > 0.5 or < stop-loss × bid-ask
7. Minimum change quantity filter to avoid micro-adjustments

## Related Concepts

- [[FX Strategies for Continuous Bond Markets]] — how continuous markets enable these strategies
- [[Convergence Trading in Continuous Markets]] — mean reversion dynamics
- [[Factor Investing in Corporate Bonds]] — complementary fundamental approach
- [[Carry and Rolldown]] — baseline return framework
