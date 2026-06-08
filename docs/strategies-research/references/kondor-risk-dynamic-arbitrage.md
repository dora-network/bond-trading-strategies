---
title: >-
  Kondor (2009): Risk in Dynamic Arbitrage — Price Effects of Convergence Trading
category: references
tags: [convergence-trading, arbitrage, limits-to-arbitrage, market-microstructure]
source_url: "http://personal.lse.ac.uk/kondor/research/kondorjof.pdf"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Equilibrium model showing that as more arbitrage capital enters, convergence speed paradoxically decreases. Prices can diverge even when arbitrageurs are unconstrained, and competition transforms arbitrage into speculation.
provenance:
  extracted: 0.7
  inferred: 0.2
  ambiguous: 0.1
base_confidence: 0.85
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Kondor: Risk in Dynamic Arbitrage

**URL:** http://personal.lse.ac.uk/kondor/research/kondorjof.pdf

**Author:** Peter Kondor (LSE)

## What It Covers

An analytically tractable equilibrium model of convergence trading where arbitrageurs with limited capital face a dynamic arbitrage opportunity. The model examines how competition among arbitrageurs affects price dynamics, convergence speed, and returns.

## Key Claims

1. **As more arbitrage capital enters, convergence speed DECREASES** — the half-life of the price gap increases without bound as capital approaches its theoretical maximum *(extracted)*
2. **Prices can diverge even when arbitrageurs are NOT capital-constrained** — divergence doesn't require binding constraints *(extracted)*
3. **Arbitrageurs can suffer losses in the absence of any shock** — purely from the endogenous dynamics of competition *(extracted)*
4. **Competition transforms arbitrage into speculation** — the gap approaches a martingale as capital increases; the expected change in the gap and expected return on capital both decrease *(extracted)*
5. **Three measures are inversely related to competition:** expected return on capital, expected change in the gap, and speed of convergence — all decrease as v̄₀ increases *(extracted)*
6. **The gap provides a one-sided bet** if arbitrageurs did not trade (it can only converge), but their trading endogenously creates the possibility of divergence *(extracted)*
7. **Sharpe ratio of convergence trading shrinks** as more capital enters — the half-life increases and the market becomes less profitable *(inferred)*

## Implications for Continuous Bond Markets

In a fully liquid, continuous bond market where many participants can arbitrage:
- **Convergence trades become LESS profitable**, not more — paradoxically
- **Gaps persist longer** even though transaction costs are lower
- **Arbitrage opportunities don't disappear** — they transform into longer-duration speculative bets
- **Flash crashes and forced unwinding** risk increases due to the dynamic

## Limitations

- Simplified model with only two identical assets
- Assumes constant hazard rate for window closing
- Does not model HFT or liquidity provision dynamics explicitly
