# NBA Trade Market Dynamics & Team Success

**Type:** Sports Business Analytics · Panel Data · Predictive Modeling  
**Tech:** R, Python, Web Scraping, PCA, Regression, Classification

## Project Overview

NBA front offices constantly balance two goals: staying under complex salary‐cap and luxury‐tax rules while building a roster that can compete for the playoffs. This project asks:

> Do teams that use their salary cap, luxury tax room, and cash more efficiently actually perform better in the following season?

Using 10 seasons of NBA financial and performance data, I built a team–season panel, engineered “financial utilization” metrics, and tested how well they explain next-year performance and playoff qualification.

## Key Business Questions

1. How are teams *actually* using cap space, tax room, and cash relative to the thresholds set by the CBA?
2. Are there patterns in financial behavior (e.g., consistently aggressive tax spenders) over time?
3. Does higher or “smarter” financial utilization predict:
   - A higher composite performance score (efficiency metrics),
   - A higher probability of making the playoffs?

## Data & Sources

- **Seasons:** 2012–13 to 2023–24
- **Performance data:** Team-level advanced metrics from the official NBA Stats API
- **Financial data:** Team salaries, cap, luxury tax, and cash spending scraped from Spotrac
- **Panel size:** 360 team-season rows after cleaning and lagging predictors

All data is merged for each team and season, with franchise name changes reconciled via a custom lookup table.

## Analytics Approach

1. **Data Engineering**
   - Normalized team IDs and season keys across NBA and Spotrac sources
   - Derived 5 financial utilization ratios:
     - Cap utilization
     - Tax utilization
     - Cash utilization
     - Average annual contract value (AAV) utilization
     - Off-season spending utilization
   - Lagged all financial features by one season to avoid leakage and mimic real-world decision making.

2. **Exploratory Analysis**
   - Distribution plots of utilization ratios and composite performance
   - Season-over-season trends in league-wide utilization
   - Correlation analysis between financial metrics and performance index

3. **Dimensionality Reduction**
   - Principal Component Analysis (PCA) on 5 efficiency metrics to create a single **Performance PC1** index
   - PC1 explains ~56% of total variance and serves as the main continuous outcome.

4. **Modeling**
   - **Regression (continuous outcome – Performance PC1)**
     - Pooled OLS
     - Team fixed-effects model
     - LASSO regression for feature selection
     - Random Forest regression as a nonlinear benchmark
   - **Classification (binary outcome – Playoffs vs. No Playoffs)**
     - Logistic regression
     - Random Forest classifier
   - Evaluation metrics:
     - RMSE and R² for regression
     - ROC–AUC for playoff classification
     - Out-of-sample validation using train/test splits

## Key Findings

- Cap and cash utilization show the strongest positive relationship with next-season performance; most other ratios have weaker or noisy effects.
- LASSO keeps only a small subset of utilization metrics, suggesting that more spending is not always better; *where* and *how* teams spend matters more than raw totals.
- Across model families, predictive power for next-season performance is modest, and playoff classification is only slightly better than chance. Financial “savvy” alone cannot explain success—talent evaluation and coaching still dominate.

## Takeaways for Stakeholders

- Front offices can treat these utilization ratios as **early warning indicators**, not hard rules: extreme under-spending or over-spending (relative to peers) rarely translates into outsized performance.
- The framework can be extended by adding player-level data (age, minutes, contract structure) to better understand *who* is being paid, not just *how much*.

## What I Did as a Data Analyst

- Designed business-style questions in collaboration with sports‐economics literature
- Built a reproducible data pipeline (scraping, cleaning, and joining multi-source data)
- Performed EDA and PCA to construct interpretable performance indices
- Implemented and compared several regression and classification models
- Translated model results into concrete recommendations and limitations for decision-makers
