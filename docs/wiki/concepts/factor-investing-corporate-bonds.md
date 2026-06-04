---
title: Factor Investing in Corporate Bonds
category: concepts
tags: [factor-investing, corporate-bonds, carry, momentum, value, defensive]
sources:
  - "[[Israel, Palhares, Richardson - Common Factors in Corporate Bond Returns]]"
  - "[[Quant Decoded - Value Momentum Carry in Bonds]]"
  - "[[Quoniam - Factor Investing EQ vs Corp]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Carry, value, momentum, and defensive factors reliably predict cross-sectional variation in corporate bond excess returns, with low correlation to equity factors and strong diversification benefits.
provenance:
  extracted: 0.7
  inferred: 0.2
  ambiguous: 0.1
base_confidence: 0.85
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Factor Investing in Corporate Bonds

## The Four Core Factors

| Factor | Bond Definition | Sharpe (LS) | Economic Rationale |
|--------|----------------|-------------|-------------------|
| **Carry** | Option-adjusted spread per unit of duration | 0.91 | Compensation for bearing credit risk during bad times |
| **Value** | Spread relative to rating/maturity peers; residual from fair-value regression | 0.72 | Behavioral: temporary dislocations from forced selling, index effects |
| **Momentum** | Trailing 6-month excess return over duration-matched Treasuries | 0.84 | Slow diffusion of credit information in OTC markets |
| **Defensive** | Low market leverage, high profitability, low duration | 0.50 | Flight-to-quality premium; low-risk anomaly |

**Combined equal-weight multi-factor portfolio:** Sharpe ratio 1.22, positive in 89% of 10-year rolling periods.

## Key Differentiators from Equity Factors

1. **Bond value ≠ equity value.** Bond value is *relative* (risk-adjusted spread vs. fair value model), while equity value is *absolute* (book-to-market). Equity value behaves more like bond carry in practice.
2. **Equity momentum in bonds** works through a lead-lag relationship — stock prices adjust faster than bond prices, creating a predictable signal for bond returns. This cross-asset signal is unavailable to equity-only strategies.
3. **Bond momentum is ~35% correlated** with equity momentum on the same issuer — ~2/3 of the signal is credit-specific.
4. **Factor definitions naturally differ** because bond factors can model expected loss (default probability × loss given default) with reasonable accuracy, while equity factors ignore future dividend growth estimates.

## Active Long-Only Implementation

- **Net active return:** 2.20% annualized (IR 0.86) after transaction costs
- **Long-only factor-tilted portfolios** overweight top quintile bonds on combined factor score
- **Factor credit ETFs** exist for retail access (e.g., FCTB, IGBH)

## Performance Characteristics

- Value works in most environments; momentum is most powerful during credit weakness
- Factors are complementary — value and momentum frequently offset each other
- Corporate bond factor premia survived multiple credit stress episodes (Yen carry unwind Aug 2024, Liberation Day Apr 2025, Iran War Mar 2026)
- Unlike equity factors, credit factors did not experience a performance drag post-2020

## Related Concepts

- [[Carry and Rolldown]] — related but carry-as-factor includes spread roll-down
- [[Mean Reversion in Spreads]] — value factor exploits mean reversion in spreads
- [[Momentum in Fixed Income]] — credit momentum mechanics
