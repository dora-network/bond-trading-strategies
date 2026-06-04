---
title: Convergence Trading in Continuous Markets
category: concepts
tags: [convergence-trading, arbitrage, limits-to-arbitrage, market-microstructure, continuous-trading]
sources:
  - "[[Kondor - Risk in Dynamic Arbitrage]]"
  - "[[Kambhu - Convergence Trading IRS]]"
  - "[[BIS - Bond ETF Arbitrage]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Paradoxically, making bond markets more liquid and continuous does not guarantee faster convergence of mispricings. More arbitrage capital can slow convergence and increase fragility.
provenance:
  extracted: 0.55
  inferred: 0.35
  ambiguous: 0.1
base_confidence: 0.75
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Convergence Trading in Continuous Markets

## The Paradox

**Intuition:** More liquidity + lower transaction costs + continuous trading = faster convergence of mispricings.

**Reality (Kondor 2009, Kambhu):** More arbitrage capital can paradoxically SLOW convergence and INCREASE market fragility.

## The Kondor Paradox

When bonds trade continuously with many arbitrageurs:

1. **Speed paradox:** As more arbitrage capital enters, the half-life of price gaps INCREASES. Convergence gets SLOWER, not faster.
2. **Martingale convergence:** As capital approaches its theoretical maximum, the gap approaches a martingale — it becomes unpredictable.
3. **Risk without constraints:** Prices can diverge even when NO arbitrageur faces binding capital constraints. Divergence is endogenous to competition.
4. **Losses without shocks:** Arbitrageurs can suffer losses purely from the dynamics of competition, even if the underlying opportunity is fundamentally riskless.

**Why:** Arbitrageurs individually optimize their capital allocation over time. Collectively, they smooth out convergence — stretching the gap over longer periods to extract more total profit. Competition transforms arbitrage into speculation.

## The Kambhu Destabilization

Empirical evidence from swap spread convergence:

1. **Dual role:** Convergence trading BOTH stabilizes and destabilizes.
2. **Normal times:** Converges spreads toward fundamental value — stabilizing.
3. **Stress times:** Forced unwinding amplifies shocks — destabilizing. Repo volume drops, spreads diverge.
4. **Capital sensitivity:** Convergence speed depends on trader capital. Losses → slower convergence → wider spreads → more losses (feedback loop).

## Implications for a Continuous Bond Market

| Feature | Intuitive Expectation | Actual Likely Outcome |
|---------|----------------------|----------------------|
| Lower transaction costs | Faster arbitrage | Slower convergence (Kondor paradox) |
| More participants | Tighter spreads | More fragile during stress (Kambhu) |
| 24/7 trading | Continuous price discovery | More frequent mini-crises from forced unwinding |
| Fractional positions | More precise hedging | More levered positions → larger unwinds |
| Tokenized settlement | Instant finality | Faster cascades during stress (no T+2 buffer) |

## The ETF Lesson

Bond ETFs already provide a real-world laboratory for fractional, continuously-traded bond exposure:

- **Baskets ≠ holdings:** Only <3% of holdings are in any creation/redemption basket
- **Arbitrage is imperfect:** APs prioritize their own inventory management over pure arbitrage
- **Shock absorption:** The decoupling of baskets from holdings actually helps — partial convertibility prevents fire sales
- **4-10 minute correction time** for ETF premiums/discounts — even with continuous secondary trading, price discovery has meaningful latency

## Design Implications

For a well-functioning continuous bond market:

1. **Circuit breakers are essential** — more important, not less, in a 24/7 market
2. **Capital/margin requirements** need to account for the paradox (more capital ≠ more stable)
3. **Diverse participant types** matter more than participant count — heterogeneous strategies reduce correlated unwinding
4. **In-kind redemption mechanisms** (like ETF structure) are stabilizers; pure cash redemption can amplify stress
5. **Convergence speed should be monitored** as a market health indicator — unusually fast convergence may signal insufficient arbitrage capital (paradoxically)

## Related Concepts

- [[FX Strategies for Continuous Bond Markets]] — strategies that become viable
- [[Tokenized Bond Market Structure]] — infrastructure
- [[Limits to Arbitrage in Bond Markets]] — theoretical foundations
- [[Relative Value Trading]] — convergence trading strategies
