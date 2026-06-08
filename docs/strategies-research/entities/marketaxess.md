---
title: MarketAxess
category: entities
tags: [electronic-trading, corporate-bonds, platform, market-data]
sources:
  - "[[Vanguard - Tech Pillars]]"
  - "[[MD2C Platforms and Causal Inference]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Largest electronic trading marketplace for US corporate bonds. Operates Open Trading (all-to-all protocol) and CP+ (AI-generated reference prices for price discovery).
---

# MarketAxess

**Largest electronic trading marketplace** for US institutional corporate bond trading.

## Key Offerings

### Open Trading
- All-to-all trading protocol within RFQ mechanism since 2012
- MarketAxess becomes counterparty to both sides; clears and settles with the platform
- **Impact:** Supplements dealer liquidity, doesn't replace it — dealers remain the most important liquidity providers

### CP+ (Composite+)
- AI/ML-generated reference prices (bid and ask) in real time
- **Training data:** TRACE trade reports + proprietary RFQ data (all dealer responses, executed or not)
- **Model:** Trained nightly; Minimized Absolute Deviation (MAD) objective; doesn't use other asset class features
- **Coverage:** 20,335 corporate bond issues, quotes often every minute
- **Finding:** CP+ is more informative about future trade prices than the last trade; adds most value for moderate-liquidity bonds

### RFQ Protocol
- Clients simultaneously solicit quotes from multiple dealers
- Dealers cannot see each other's prices (competitive)
- **Key drivers of hit probability:** Spread (dominant), DV01 exposure (risk/size), number of dealers (competition)
- Generative models (structural) match LightGBM (discriminative) in ROC-AUC while enforcing spread monotonicity

## Market Share
- Dominant electronic platform for US IG corporate bonds
- ~50% of IG corporate trading now electronic; MarketAxess is the largest venue
