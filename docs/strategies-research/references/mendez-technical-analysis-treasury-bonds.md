---
title: >-
  Méndez: Trading Strategies for US Treasury Bonds Using Technical Analysis (2024)
category: references
tags: [technical-analysis, treasury-bonds, MACD, RSI, Bollinger, bloomberg]
source_url: "https://revistas.itm.edu.co/index.php/revista-cea/article/view/2634"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Optimal US Treasury bond trading strategies identified via Bloomberg backtesting. MACD + Bollinger Bands + RSI combination outperforms single-indicator strategies. Multi-indicator strategies consistently beat single-indicator approaches.
provenance:
  extracted: 0.7
  inferred: 0.2
  ambiguous: 0.1
base_confidence: 0.82
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Méndez: Technical Analysis for US Treasury Bonds

**URL:** https://revistas.itm.edu.co/index.php/revista-cea/article/view/2634

**Published:** 2024

## What It Covers

Quantitative study using Bloomberg Professional's BT and BTST backtesting functions to identify optimal technical trading strategies for US Treasury bonds. Tests oscillators (MACD, RSI), volatility (Bollinger Bands), and trend indicators.

## Key Claims

1. **MACD combined with Bollinger Bands and RSI** was the most effective strategy — defined by number of successful trades *(extracted)*
2. **Multi-indicator strategies consistently outperform single-indicator ones** — confirmed across all tested combinations *(extracted)*
3. **MACD, RSI, and Bollinger Bands** are the core indicators that professional brokers apply daily at trading desks *(extracted)*
4. **Strategy effectiveness varies by indicator combination** — not all multi-indicator combinations are equally effective; the specific pairing matters *(inferred)*
5. **Bloomberg BT/BTST provides a practical backtesting environment** for rapid strategy development on Treasuries *(extracted)*

## Indicator Definitions Tested

| Indicator | Type | Parameters Tested |
|-----------|------|-------------------|
| MACD | Trend-following momentum | Multiple EMA combinations |
| RSI | Momentum oscillator | Multiple periods (7, 14, 21) |
| Bollinger Bands | Volatility | Standard deviation bands |

## Limitations

- Effectiveness measured by number of successful trades, not risk-adjusted returns
- No transaction cost analysis
- Bloomberg platform-specific — reproducibility requires terminal access
- Specific parameter values not disclosed in detail
