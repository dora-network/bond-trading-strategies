---
title: FRED (Federal Reserve Economic Data)
category: entities
tags: [data, federal-reserve, yields, economic-data, api]
sources: []
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Federal Reserve Bank of St. Louis database with 800K+ economic time series. Key source for Treasury yields (CMT), credit spreads (BAA/AAA), and macro indicators used in bond strategy research.
---

# FRED

**Federal Reserve Economic Data** — maintained by the Federal Reserve Bank of St. Louis. Free API and web access to 800,000+ US and international economic time series.

## Key Bond-Related Series

| Series | Description | Strategy Use |
|--------|------------|-------------|
| DGS2, DGS5, DGS10, DGS30 | Constant Maturity Treasury yields | Yield curve construction, carry/rolldown |
| BAA, AAA | Moody's Seasoned Corporate Bond Yields | Credit spread analysis (BAA-AAA spread) |
| T10Y2Y | 10Y-2Y Treasury spread | Curve slope monitoring |
| T10YIE | 10Y Breakeven Inflation Rate | Real yield calculation, inflation expectations |
| DFEDTARU | Federal Funds Target Rate (Upper) | Funding cost for carry calculations |

## API Access
- Free registration at research.stlouisfed.org
- Python: `fredapi` package (`pip install fredapi`)
- Rate limit: 120 requests/minute (free tier)

## Python Usage
```python
from fredapi import Fred
fred = Fred(api_key='YOUR_KEY')
baa = fred.get_series('BAA', observation_start='2016-01-01')
aaa = fred.get_series('AAA', observation_start='2016-01-01')
spread = baa - aaa
```
