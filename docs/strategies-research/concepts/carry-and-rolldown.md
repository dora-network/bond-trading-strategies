---
title: Carry and Rolldown
category: concepts
tags: [carry, rolldown, yield-curve, duration, strategy-baseline]
sources:
  - "[[yieldcurve.pro - Carry and Rolldown]]"
  - "[[Simplify - Efficient Long Duration]]"
  - "[[CAIA - Introduction to Carry Strategies]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  The baseline return of any bond position: carry (yield minus funding cost) plus rolldown (price appreciation from aging down a positively sloped curve). Everything else is a deviation from this baseline.
provenance:
  extracted: 0.7
  inferred: 0.2
  ambiguous: 0.1
base_confidence: 0.85
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Carry and Rolldown

## Definition

**Carry** is the income a bond earns above its funding cost. For a par bond: `carry = yield − funding_rate`. **Rolldown** is the price appreciation a bond gets from aging down a positively sloped yield curve — as a bond's maturity shortens, its yield falls (assuming unchanged curve shape), and its price rises.

Together, carry and rolldown define the total expected return from holding a bond position if the world stays still — no rate surprises, no spread moves, no regime shifts.

## Example (Current US Curve)

- 5Y Treasury at 3.91%, funded at Fed Funds midpoint (3.625%): **carry = +29 bps/year**
- 5Y to 3Y slope = +14 bps over 2 years → ~7 bps/year yield decline
- With ~4.5 modified duration: **rolldown ≈ +32 bps/year**
- **Total expected return ≈ +61 bps/year** before any directional rate bet

## Strategic Implications

1. **Carry and rolldown are not a strategy** — they are the baseline. Every duration position has a carry/rolldown profile. A steepener, flattener, or butterfly is a deviation from this baseline.
2. **The forward rate IS the breakeven yield** — every carry trade bets that realized rates will undershoot what the curve prices in.
3. **Belly of the curve (5Y-10Y)** offers highest absolute carry; front end (2Y) wins on carry-per-unit-of-duration efficiency.
4. **Rolldown only works** with upward-sloping curves; flat or inverted curves kill or reverse rolldown.

## Related Concepts

- [[Duration Management]] — duration exposure determines carry magnitude
- [[Yield Curve Strategies]] — steepeners/flatteners deviate from carry baseline
- [[Term Premium]] — some carry compensates for bearing duration risk
