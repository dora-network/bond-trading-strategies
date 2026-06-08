---
title: TRACE (Trade Reporting and Compliance Engine)
category: entities
tags: [data, finra, corporate-bonds, market-data, transparency]
sources:
  - "[[Corporate Bond Factor Replication Crisis]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  FINRA's mandatory post-trade reporting system for US corporate bond transactions. The primary data source for corporate bond research, but requires careful cleaning due to recording errors, stale pricing, and infrequent trading.
---

# TRACE

**Trade Reporting and Compliance Engine** — FINRA's system for mandatory post-trade transparency in the US corporate bond market.

## Data Characteristics

- Covers all broker-dealer corporate bond transactions since 2002
- Reports: price, size, time, side (buy/sell), and whether dealer was principal or agent
- **Enhanced TRACE** (post-2018): includes 144A bonds, more granular timestamps
- TRACE prices are **past transaction records, not standing executable quotes**

## Common Data Issues

1. **Recording errors:** Decimal shifts (10.5 entered as 105.0), cancellations, corrections, reversals
2. **Stale pricing:** Many bonds trade infrequently — month-end "price" may be days or weeks old
3. **Non-executability:** A transaction price at time t was available to the trading parties at time t, but not necessarily to a strategy observing it — creates Latent Implementation Bias
4. **Reporting delays:** 15-minute reporting window for most trades; exceptions for large blocks
5. **Survivorship:** Defaulted bonds leave the dataset — must be tracked through default for unbiased backtests

## Cleaning Pipeline (Standard)
- Dick-Nielsen (2014) filters: remove cancellations, corrections, reversals, agency-side duplicates
- Extended filters: decimal shift corrector, bounce-back filter for transient spikes
- Volume-weighted daily aggregation
- Point-in-time universe tracking

## Related Tools
- **PyBondLab:** Open-source Python pipeline for clean TRACE → monthly factor data (openbondassetpricing.com)
- **WRDS:** Academic access to TRACE via Wharton Research Data Services
- **MarketAxess CP+:** AI-generated reference prices that supplement TRACE for intraday pricing
