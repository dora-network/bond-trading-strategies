---
title: >-
  Research: Bond Strategies in a Continuous Fractionalized Marketplace
category: synthesis
tags: [continuous-market, fractional-bonds, fx-strategies, convergence-trading, tokenization, research]
sources:
  - "[[BIS - Tokenisation of Government Bonds]]"
  - "[[ECB - Tokenised Bonds Efficiency and Liquidity]]"
  - "[[Kondor - Risk in Dynamic Arbitrage]]"
  - "[[Kambhu - Convergence Trading IRS]]"
  - "[[BIS - Bond ETF Arbitrage]]"
  - "[[Carry Trade and Momentum in Currency Markets]]"
  - "[[Schroers - Nonparametric Bond Factors]]"
  - "[[Chen & Garriott - HFT Bond Futures]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Synthesis of 2-round research on how existing bond strategies behave and which FX strategies apply in a continuous, fractionalized bond marketplace. Key paradox: more liquidity ≠ faster convergence.
provenance:
  extracted: 0.55
  inferred: 0.35
  ambiguous: 0.1
base_confidence: 0.75
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Research: Bond Strategies in a Continuous Fractionalized Marketplace

## Overview

If bonds traded continuously with fractional ownership — like FX and equities — the strategy landscape would transform. Tokenized bond markets already demonstrate the feasibility: 19 bps bid-ask spreads (vs 30 bps conventional), 40% lower yield spread at issuance, and $110K minimum investments. But the implications for trading strategies are non-obvious. The central finding is paradoxical: **more liquidity and lower transaction costs do not guarantee faster convergence of mispricings.** Increased arbitrage capital can slow convergence and increase systemic fragility. Simultaneously, the entire FX algorithmic trading toolkit — carry trade, momentum, grid trading, breakout, market making, execution algorithms — becomes directly applicable to bonds.

## Key Findings

- **The Kondor Paradox: more arbitrage capital = slower convergence.** As competition increases, the half-life of price gaps lengthens without bound. Arbitrage opportunities transform from short-duration convergence trades into long-duration speculative bets. Prices can diverge even when no arbitrageur faces binding capital constraints [[Kondor - Risk in Dynamic Arbitrage]]
- **Convergence trading is a dual-edged sword.** It stabilizes markets in normal times but amplifies shocks during stress through forced unwinding. Repo volume drops after trader losses, and spreads diverge. Continuous margining in a tokenized market could accelerate cascading selloffs [[Kambhu - Convergence Trading IRS]]
- **FX carry trade translates directly to bonds.** Borrow short, lend long in credit or duration. With fractional bonds, carry can be precisely sized across dozens of instruments. Combined carry+momentum strategies in FX achieve Sharpe 0.98 due to low correlation; the same diversification should apply to bond carry+momentum [[Carry Trade and Momentum in Currency Markets]]
- **Bond markets have 8-16 factors, not 3.** The classic level/slope/curvature decomposition misses 15%+ of systematic variation. A nonparametric framework with higher-order PCA reveals tradable residual mispricings. Using 16 factors instead of 3 reduces 30-day return approximation error from 0.50 to 0.07 [[Schroers - Nonparametric Bond Factors]]
- **HFT can work positively in bond markets.** Canadian bond futures evidence shows HFTs act as liquidity suppliers, not predators. No back-running detected. Increased HFT competition improved institutional execution costs [[Chen & Garriott - HFT Bond Futures]]
- **Bond ETFs already demonstrate fractional trading dynamics.** Creation/redemption baskets contain <3% of holdings; decoupling weakens arbitrage but absorbs shocks. 4-10 minute correction times show that even continuous secondary trading has meaningful latency in price discovery [[BIS - Bond ETF Arbitrage]]
- **Tokenized bonds deliver measurable improvements now.** 19 vs 30 bps bid-ask, 27% tighter spreads, 40% lower issuance yield spread. 24/7 trading, fractional ownership, smart contract automation — all demonstrated at experimental scale [[BIS - Tokenisation of Government Bonds]], [[ECB - Tokenised Bonds Efficiency and Liquidity]]
- **Execution algorithms (10-20% of FX volume) will migrate to bonds.** Slicing orders, aggregating fragmented liquidity, smart order routing — all currently FX-only capabilities that a continuous bond CLOB would enable [[BIS FX Execution Algorithms]]

## Core Concepts

- [[FX Strategies Applicable to Continuous Bond Markets]] — 8 FX strategy families that translate: carry, momentum, grid, breakout, market making, execution algos, cross-asset cascades, multi-factor residuals
- [[Convergence Trading in Continuous Markets]] — The Kondor paradox and Kambhu destabilization: why more liquidity doesn't mean faster convergence
- [[Tokenized Bond Market Structure]] — Current state ($8B market), key features (fractional, 24/7, instant settlement), and strategy implications

## How Existing Bond Strategies Behave

| Strategy | Current OTC Market | Continuous Fractional Market | Change |
|----------|-------------------|------------------------------|--------|
| **Factor investing (carry/value/momentum)** | Monthly rebalancing, $1M lots, 20-50 bps costs | Daily/intraday rebalancing, fractional positions, <10 bps costs | **Significantly improved** — higher frequency, lower costs, more diversification |
| **Relative value / basis trades** | 10-50x leverage, repo-funded, slow convergence | More capital competing → paradoxically slower convergence | **Paradoxically harder** — Kondor effect dominates |
| **Yield curve strategies** | Futures-based, quarterly rolls | Direct curve positioning on cash bonds, real-time PCA | **Better execution** — more instruments, real-time signals |
| **Pairs trading / stat arb** | Few tradeable pairs, illiquidity | Hundreds of pairs, continuous data, CLOB visibility | **Dramatically expanded** — entire equity stat arb toolkit applies |
| **Butterfly trades** | DV01-weighted, monthly rebalancing | PCA-weighted, intraday rebalancing, dynamic k | **More factors = more opportunities** — but requires adaptation |
| **Carry/rolldown harvesting** | Hold-to-maturity, limited rebalancing | Dynamic carry optimization, real-time roll capture | **Moderately improved** — lower costs help, but carry economics unchanged |
| **Swap spread arbitrage** | Balance-sheet constrained, repo-funded | Lower barriers, more participants → Kondor effect | **Harder at scale** — convergence slows with more capital |
| **Convergence / RV arbitrage** | Slow, capital-intensive | Faster trading, more competition → slower convergence | **Paradoxically less profitable per unit of risk** |

## FX Strategies That Translate Directly

| FX Strategy | Bond Equivalent | Feasibility | Key Risk |
|-------------|----------------|-------------|----------|
| Carry trade (borrow low, lend high) | Duration/credit carry on fractional bonds | High | Duration + credit on top of carry |
| Momentum (cross-sectional) | Rank bonds by excess return | High | Reversals, crowding |
| Momentum (time-series) | Long/short based on sign of past return | High | Whipsaws in range-bound markets |
| Grid trading | Ladder orders on yield spreads | Medium | Trending markets destroy grids |
| Breakout / vol compression | Enter on range break after vol squeeze | High | False breakouts |
| Market making (Avellaneda-Stoikov) | Stream two-sided quotes on bond CLOB | Medium | Latency risk, adverse selection |
| Execution algorithms (TWAP/VWAP) | Slice large bond orders | High | Requires CLOB infrastructure |
| Cross-asset cascade | Bonds → FX → commodities → equities | Medium | Broken cascades (signal failure) |
| Latency arbitrage | Front-run slow participants on CLOB | Low | Regulatory risk, Tobin tax |
| Multi-factor residual (PCA) | Trade extreme PCA residuals | Medium | Requires regime filter and cost model |

## Contradictions & Open Questions

### Contradiction 1: More Liquidity = Slower Convergence
The Kondor paradox directly contradicts the intuition that lower transaction costs and more participants speed up arbitrage. This is the central tension in designing continuous bond markets. The resolution may lie in participant diversity: heterogeneous strategies (different time horizons, risk models, capital constraints) may counteract the paradox, while homogeneous arbitrage capital makes it worse.

### Contradiction 2: Continuous Trading Stabilizes vs. Destabilizes
Continuous trading enables faster price discovery and tighter spreads (stabilizing). But continuous margining, 24/7 trading, and instant settlement remove circuit breakers that currently slow cascades (destabilizing). The net effect is unclear and likely regime-dependent.

### Open Question: Optimal Design
What market structure maximizes the benefits (lower costs, better price discovery, strategy diversity) while minimizing the risks (Kondor paradox, faster cascades)? Key design variables:
- Circuit breaker design for 24/7 markets
- Margin/collateral requirements in tokenized markets
- Role of designated market makers vs. open liquidity provision
- In-kind vs. cash creation/redemption mechanisms
- Permissioned vs. public DLT

### Open Question: Strategy Capacity
Current systematic bond strategies have multi-trillion capacity. In a continuous market with lower costs and more participants, capacity may paradoxically decrease if the Kondor effect dominates — more capital chasing the same opportunities at slower convergence speeds.

### Open Question: The Term Premium
In a continuous, liquid bond market where duration can be precisely traded and hedged, does the term premium compress? Evidence from tokenized bonds (40% lower issuance spread) suggests yes — but whether this reflects genuine efficiency gains or temporary novelty premium is unknown.

## Strategy Evolution Roadmap

### Phase 1: Current → ETF-Like Trading (Now)
- Bond ETFs provide continuous secondary trading with fractional shares
- AP arbitrage mechanism (4-10 min correction)
- Factor ETFs already exist (FCTB, IGBH)
- **Available strategies:** ETF-level factor rotation, ETF premium/discount arbitrage, creation/redemption optimization

### Phase 2: ETF-Like → Tokenized Trading (2-5 years)
- Individual bonds trade continuously on DLT platforms
- Fractional ownership reduces minimum investment
- Smart contracts automate corporate actions
- **New strategies:** Individual bond momentum, grid trading on bond CLOB, cross-venue stat arb, multi-factor residual trading

### Phase 3: Tokenized → Fully Continuous (5-10 years)
- Consolidated limit order books across venues
- HFT market making on individual bonds
- Real-time PCA and factor decomposition
- Execution algorithms for institutional bond orders
- **New strategies:** Full FX/equity algo toolkit on bonds, latency arbitrage, CLOB imbalance strategies

## Sources Consulted

- [[BIS - Tokenisation of Government Bonds]] — Tokenized bond empirical analysis
- [[ECB - Tokenised Bonds Efficiency and Liquidity]] — Efficiency and liquidity gains
- [[Kondor - Risk in Dynamic Arbitrage]] — Convergence paradox
- [[Kambhu - Convergence Trading IRS]] — Destabilizing convergence
- [[BIS - Bond ETF Arbitrage]] — ETF arbitrage mechanics
- [[Carry Trade and Momentum in Currency Markets]] — FX strategy survey
- [[Schroers - Nonparametric Bond Factors]] — Higher-order factor structure
- [[Chen & Garriott - HFT Bond Futures]] — HFT in bond markets
- BIS FX Execution Algorithms report — EA mechanics and market impact
- Designing Future Bond Markets (Springer) — Tokenization design principles
- BrokerTec Microstructure (NY Fed) — Treasury CLOB mechanics
- Market Microstructure of Fixed-Income Markets (SEC) — Structural comparison
- Grid Trading System Robot — Grid strategy mechanics
- Volatility Compression in Bonds — Breakout strategy
- Cross-Asset Momentum Cascades — Cascade trading framework
