# Predicting Molecular Biodegradability for ChemsRUs

**Type:** Applied Predictive Analytics · Feature Selection · Model Comparison  
**Tech:** R, caret, glmnet, e1071, tidyverse

## Business Context

ChemsRUs participates in a Codalab challenge to predict whether new chemical compounds are **biodegradable** or **non-biodegradable**. Their internal baseline uses logistic regression with p-value-based feature selection, but performance on the leaderboard suggests that the approach is leaving accuracy on the table.

This project acts as a consulting engagement for ChemsRUs:

> Can we design a more robust modeling and feature-selection pipeline that improves AUC on the challenge test set while identifying which molecular descriptors truly matter?

## Data

- **Source:** ChemsRUs Codalab biodegradability challenge
- **Rows:** 1,055 training compounds + external test set
- **Features:** 168 molecular descriptors (X0–X167)
- **Target:** Binary label  
  - `1` = biodegradable  
  - `-1` = non-biodegradable
- **Split:** 90% train / 10% validation (fixed seed for reproducibility)

## Analytical Approach

1. **Baseline Models (All Features)**
   - Logistic Regression
   - SVM with radial kernel
   - Metrics: balanced accuracy and ROC–AUC on validation

2. **Feature Selection Strategies**
   - **Recursive Feature Elimination (RFE)**
     - SVM-based RFE to identify the top ~40 descriptors
   - **L1-Regularized Logistic Regression (Lasso)**
     - Penalized model to shrink unimportant coefficients to zero

3. **Retraining on Reduced Feature Sets**
   - Re-fit LR and SVM models using:
     - RFE-selected features
     - L1-selected features
   - Compare performance against the 168-feature baseline.

4. **Model Selection & Final Submission**
   - Choose the model that optimizes validation AUC and balanced accuracy.
   - Generate predictions for the Codalab test set and submit as ChemsRUs’ improved entry.

## Key Results

- Both LR and SVM improve when trained on a reduced feature set rather than all 168 descriptors.
- The **SVM + RFE (40 features)** model delivers the best trade-off, achieving:
  - Higher balanced accuracy on the validation set
  - A strong AUC score on the external test set
- L1-regularized models help interpret which descriptors drive biodegradability, even when their predictive performance is slightly lower.

## Insights for ChemsRUs

- Feature selection meaningfully improves model stability and generalization.
- A small subset of descriptors contains most of the signal, which can:
  - Guide future experimental design (which properties to measure)
  - Simplify internal scoring tools for new compounds
- P-value-based selection in the original baseline is not sufficient; modern regularization and RFE yield more reliable results.

## What I Did as a Data Analyst

- Re-framed a Kaggle-style challenge into a client-oriented analytics engagement
- Implemented multiple feature-selection strategies using `caret` and `glmnet`
- Compared models using consistent validation splits and metrics
- Documented trade-offs between interpretability and raw AUC
- Produced a recommendation for ChemsRUs’ “production” model and feature set
