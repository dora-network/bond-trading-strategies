---
title: >-
  Fong & Wu: Predictability in Sovereign Bond Returns Using Technical Trading Rules (BIS/HKMA)
category: references
tags: [technical-analysis, sovereign-bonds, trading-rules, machine-learning, predictability]
source_url: "https://www.bis.org/ifc/publ/ifcb50_20.pdf"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Analysis of 27,000 technical trading rules across 48 sovereign bond markets. Moving average, filtering, support/resistance, and channel breakout rules are profitable, especially in emerging markets and during US tightening/recession.
provenance:
  extracted: 0.75
  inferred: 0.15
  ambiguous: 0.1
base_confidence: 0.88
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Fong & Wu: Technical Trading Rules in Sovereign Bonds

**URL:** https://www.bis.org/ifc/publ/ifcb50_20.pdf

**Authors:** Tom Fong, Gabriel Wu (Hong Kong Monetary Authority)

## What It Covers

Systematic evaluation of 27,000 technical trading rule variants across four popular classes (moving average, filtering, support/resistance, channel breakout) applied to 48 sovereign bond markets. Assesses predictability using excess return over buy-and-hold, with bootstrap significance testing and NBC machine learning optimization.

## Key Claims

1. **Sovereign bond markets ARE predictable** using technical trading rules — most markets show positive excess returns over buy-and-hold *(extracted)*
2. **Emerging Asian markets are significantly MORE predictable** than advanced economies — China (5.2% excess return), Philippines (4.7%), Peru (4.5%) vs. Switzerland (−1.0%), UK (−0.7%) *(extracted)*
3. **Predictability is HIGHER when the US tightens monetary policy or undergoes recession** — spillover effects from US policy are substantial *(extracted)*
4. **Machine learning (NBC algorithm) further improves returns** — two-thirds of markets benefit from ML-based rule selection *(extracted)*
5. **The percentage of unprofitable rules varies dramatically** — from 5.8% (China) to 83.5% (Chile) — rule selection matters enormously *(extracted)*
6. **Moving average rules** generate signals from short-period vs long-period MA crossovers — a trend is considered initiated when the short MA penetrates the long MA *(extracted)*
7. **Lower government effectiveness, lower regulatory quality, and narrower financial openness** are all associated with higher technical predictability *(extracted)*

## Four Rule Classes Tested

| Class | Mechanism |
|-------|-----------|
| Moving Average (MA) | Short-period MA crosses long-period MA |
| Filtering (FL) | Buy/sell when price moves X% from recent low/high |
| Support & Resistance (SR) | Buy/sell at recent high/low breakouts |
| Channel Breakout (CB) | Buy/sell when price breaks above/below a channel |

## Limitations

- Daily frequency only — intraday strategies not tested
- Does not account for transaction costs explicitly
- NBC algorithm is simple; more sophisticated ML (gradient boosting, neural nets) likely improves results further
- Sovereign bond indices, not individual bonds — the diversification benefits within indices may mask strategy performance on single bonds
