---
title: Tokenized Bond Market Structure
category: concepts
tags: [tokenization, blockchain, dlt, fractional-ownership, market-structure, continuous-trading]
sources:
  - "[[BIS - Tokenisation of Government Bonds]]"
  - "[[ECB - Tokenised Bonds Efficiency and Liquidity]]"
  - "[[Designing Future Bond Markets - Tokenization]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Tokenized bonds on DLT enable fractional ownership, 24/7 trading, near-instant settlement, and smart contract automation. Current market is $8B but growing rapidly with measurable efficiency gains.
provenance:
  extracted: 0.7
  inferred: 0.2
  ambiguous: 0.1
base_confidence: 0.83
lifecycle: draft
lifecycle_changed: 2026-06-04
---

# Tokenized Bond Market Structure

## Current State

The market for tokenized bonds is nascent but growing rapidly:

| Metric | Value |
|--------|-------|
| Total issuance | ~$8 billion (2025) |
| Bid-ask spread | 19 bps (vs 30 bps conventional) |
| Issuance cost (yield spread) | 0.14pp lower (40% reduction) |
| Minimum investment | $110K avg (vs $185K conventional) |
| 88% of issuances | Last 3 years |
| Primary platforms | Ethereum ($1.2B market value), Polygon |

## Key Features

### Fractional Ownership
- Bonds divided into smaller units, lowering investment thresholds
- Enables retail and smaller institutional participation
- Currently: conventional bonds have $1M round lots; tokenized bonds can be any size
- **Impact on strategies:** Precise position sizing, diversification across more instruments

### 24/7 Continuous Trading
- Unlike conventional bonds (limited trading hours, infrequent trades)
- Smart contracts automate execution
- **Impact on strategies:** Intraday strategies become viable; carry/momentum can be traded at any frequency

### Near-Instant Settlement
- T+0 or near-instant settlement via DLT
- Eliminates counterparty risk during settlement window
- Reduces capital tied up in clearing
- **Impact on strategies:** Faster capital recycling; enables higher turnover strategies without settlement drag

### Smart Contract Automation
- Automated interest payments, principal repayment, corporate actions
- Compliance verification embedded in token logic
- **Impact on strategies:** Reduced operational costs; programmable strategy execution

### Multi-Venue Trading
- Bond tokens can trade across multiple platforms sharing common infrastructure
- Reduced asset specificity (bonds no longer locked to single dealer/venue)
- **Impact on strategies:** Cross-venue arbitrage; smart order routing; best execution

## Design Choices

| Dimension | Options | Strategy Impact |
|-----------|---------|----------------|
| Native vs Tokenized | Pure on-chain vs backed by conventional bond | Native: simpler; Tokenized: leverages existing legal framework |
| Public vs Permissioned | Public blockchain vs private DLT | Public: more liquidity, transparency; Private: more control |
| Custody | Self-custody vs third-party | Self: lower costs; Third-party: regulatory compliance |
| Collateral model | On-platform vs off-platform vs self-managed | On-platform: full programmability; Off-platform: simpler transition |

## Strategy Implications

### Strategies That Benefit Most
1. **Carry trade:** Fractional positions + lower costs = higher net carry
2. **Statistical arbitrage:** Continuous data + order book visibility = richer signal set
3. **Momentum/trend following:** Intraday signals on individual bonds
4. **Market making:** Lower barriers to LP participation
5. **Grid/breakout:** Continuous CLOB enables these FX-style strategies

### Strategies That Change Least
1. **Buy-and-hold / duration targeting:** Core economics unchanged
2. **Fundamental credit analysis:** Still requires issuer-level research
3. **Macro direction bets:** Yield curve level still dominated by macro factors

### New Risks
1. **Smart contract risk:** Bugs, exploits, oracle manipulation
2. **Platform concentration:** Ethereum dominance creates single-point-of-failure
3. **Regulatory uncertainty:** Varies by jurisdiction; may fragment liquidity
4. **Faster cascades:** Instant settlement removes the T+2 buffer during stress

## Related Concepts

- [[Convergence Trading in Continuous Markets]] — how arbitrage dynamics change
- [[FX Strategies for Continuous Bond Markets]] — strategies that become applicable
- [[Bond ETF Arbitrage Mechanism]] — closest existing analog
