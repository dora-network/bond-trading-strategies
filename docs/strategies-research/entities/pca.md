---
title: PCA (Principal Component Analysis)
category: entities
tags: [pca, risk-management, yield-curve, statistical-method]
sources:
  - "[[Principal Component Analysis in Fixed Income]]"
  - "[[BSIC - PCA Hedging]]"
created: 2026-06-04T00:00:00Z
updated: 2026-06-04T00:00:00Z
summary: >-
  Statistical technique for dimensionality reduction. In fixed income, PCA decomposes yield curve movements into orthogonal principal components corresponding to level, slope, and curvature.
---

# PCA in Fixed Income

## Mathematical Foundation

Eigendecomposition of the covariance matrix of yield changes:
- **Inputs:** Yield changes across N tenors over T time periods → N×T matrix
- **Outputs:** Eigenvectors (PC loadings) and eigenvalues (variance explained)
- **First 3 PCs:** Explain 95-99% of yield curve variance

## Fixed Income Interpretation

| PC | Interpretation | Typical Variance |
|----|---------------|-----------------|
| PC1 | Level (parallel shift) | 75-81% |
| PC2 | Slope (steepening/flattening) | 15-21% |
| PC3 | Curvature (butterfly) | 1-5% |

## Input Data
- Constant Maturity Treasury (CMT) yields: 1Y through 30Y, interpolated at regular intervals (e.g., 90-day with cubic spline)
- Swap rates: alternative curve for non-government applications
- Bond yields for specific issuer curves

## Key Tools & Libraries
- **Python:** sklearn.decomposition.PCA, statsmodels
- **R:** prcomp, princomp
- **Bloomberg:** PCA functionality in PORT, MARS
