# NBA Trade Market Dynamics & Team Success

Sports business analytics project exploring how teams’ use of cap space, luxury tax room, and cash relates to next-season performance and playoff qualification.

**Type:** Sports Analytics · Panel Data · Predictive Modeling  
**Tech:** R, Python, Web Scraping, PCA, Regression, Classification

---

## Business Problem

NBA front offices operate under strict salary cap and luxury tax rules while trying to assemble playoff-caliber rosters. Decision-makers want to know whether **how** they use cap and cash actually translates into better on-court results.

This project asks:

- How are teams using cap space, tax room, and cash relative to the thresholds set by the CBA?
- Are there recognizable patterns in financial behavior over time (e.g., consistently aggressive tax spenders)?
- Does “smarter” financial utilization help predict next-season performance and playoff appearances?

---

## Data & Scope

- **Seasons:** 2012–13 through 2023–24  
- **Performance data:** Team-level advanced metrics from the official NBA Stats API  
- **Financial data:** Team salaries, cap, luxury tax, and cash spending scraped from Spotrac  
- **Granularity:** Team–season panel (one row per team per season)  
- **Scale:** 360 team-seasons after cleaning and lagging predictors

---

## Approach

- **Data Engineering & Cleaning**
  - Scraped and normalized financial data from Spotrac and aligned it with NBA Stats performance data.
  - Reconciled team name changes and relocations via a custom lookup table to maintain consistent franchise IDs.
  - Engineered five financial utilization ratios (cap, tax, cash, average annual contract value, off-season spending) and lagged them one season to avoid leakage.

- **Analysis & Modeling**
  - Conducted exploratory analysis on distributions and trends of utilization ratios and performance metrics.
  - Used **Principal Component Analysis (PCA)** to reduce multiple performance metrics into a single interpretable index (Performance PC1).
  - Modeled next-season outcomes using:
    - Pooled OLS, fixed-effects regression, LASSO, and Random Forest for the continuous performance index.
    - Logistic regression and Random Forest for playoff vs non-playoff classification.
  - Evaluated models using RMSE, R², and ROC–AUC, with out-of-sample validation.

---

## Key Findings

- Cap and cash utilization show the clearest positive relationship with next-season performance; other ratios have weaker or noisier effects.
- LASSO and fixed-effects models retain only a subset of financial variables, suggesting that **where and how** teams spend matters more than raw totals.
- Predictive power for both performance and playoff qualification is **modest**, indicating that financial behavior alone cannot explain success—talent evaluation, coaching, and injuries remain major drivers.
- The utilization framework is still valuable as an **early warning system** for extreme under- or over-spending relative to league norms.

---

## Skills Demonstrated

- **Data Engineering (R & Python)** – web scraping, normalizing multi-source sports data, building a reproducible team–season panel.
- **Feature Engineering** – creation of meaningful financial utilization ratios and lagged predictors.
- **Statistical Modeling** – PCA, OLS and fixed-effects regression, LASSO regularization, and classification models for playoff prediction.
- **Model Evaluation** – use of RMSE, R², and ROC–AUC with clear comparisons across model families.
- **Business Interpretation** – translating statistical results into guidance on how front offices can use financial metrics as diagnostics rather than one-shot decision rules.
