---
title: Ornstein-Uhlenbeck Process
category: entities
tags: [stochastic-process, mean-reversion, spread-modeling, statistical-arbitrage]
sources:
  - "[[Mean Reversion in Fixed Income]]"
  - "[[Spread-Return Mean-Reverting Model]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  A mean-reverting stochastic process widely used to model spreads in fixed income. Key parameter is mean-reversion speed (λ), which gives the half-life of deviations.
---

# Ornstein-Uhlenbeck (OU) Process

## Definition

`dX(t) = λ(μ − X(t))dt + σdW(t)`

Where:
- **λ:** Mean-reversion speed (higher = faster reversion)
- **μ:** Long-term mean
- **σ:** Volatility
- **W(t):** Wiener process (Brownian motion)

## Key Derived Quantity: Half-Life

`Half-life = ln(2) / λ`

**Interpretation:** Expected time for the process to cover half the distance back to its mean after a deviation.

| Half-Life Range | Trading Assessment |
|----------------|-------------------|
| < 5 days | Too noisy — transaction costs dominate |
| 5-60 days | Tradable range for daily-frequency strategies |
| > 60 days | Too slow — opportunity cost, regime change risk |
| > 252 days | Not practically mean-reverting |

## Estimation
OLS regression of `ΔS(t)` on `S(t−1)`:
- `ΔS(t) = a + b·S(t−1) + ε(t)`
- `λ̂ = −b/Δt`
- Half-life = `ln(2)/λ̂`

## Fixed Income Applications
1. **Credit spread modeling:** OU on spread levels or returns
2. **Pairs trading:** Model cointegrated spread as OU; half-life informs holding period
3. **Yield curve relative value:** Model mispricing decay as OU; entry/exit at σ thresholds
4. **SRMR extension:** OU on spread returns + mean-reversion term on log-spread level — overcomes OU limitations (unbounded variance without mean-reversion on level)

## Limitations
- Constant parameters in basic form — real-world λ can change with regime
- Gaussian increments — real spreads have heavy tails; SRMR adds jumps
- Assumes continuous trading — requires discretization for daily/weekly frequency

## Related Concepts

- [[Mean Reversion in Fixed Income]] — OU as the core model
- [[Pairs Trading and Statistical Arbitrage]] — OU half-life for pair filtering
- [[SRMR Model]] — extension of OU for credit spread dynamics
