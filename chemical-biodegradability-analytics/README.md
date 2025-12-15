# Predicting Molecular Biodegradability for ChemsRUs

Applied predictive analytics project reframing a Codalab biodegradability competition as a consulting engagement for a fictional client, **ChemsRUs**.

**Type:** Applied Predictive Analytics · Feature Selection · Model Comparison  
**Tech:** R, caret, glmnet, e1071, tidyverse

---

## Business Problem

ChemsRUs needs to predict whether new chemical compounds are **biodegradable** or **non-biodegradable** to prioritize R&D and manage environmental risk. Their internal baseline uses logistic regression with p-value-based feature selection and underperforms on the challenge leaderboard.

This project asks:

- Can we build a more robust modeling and feature-selection pipeline to improve predictive performance?
- Which molecular descriptors actually drive biodegradability, and how many do we need?
- How should ChemsRUs balance interpretability and AUC when choosing a “production” model?

---

## Data & Scope

- **Source:** ChemsRUs Codalab biodegradability challenge dataset  
- **Rows:** 1,055 training compounds + external test set  
- **Features:** 168 molecular descriptors (X0–X167)  
- **Target:** Binary label  
  - `1` = biodegradable  
  - `-1` = non-biodegradable  
- **Split:** 90% train / 10% validation with fixed seed for reproducibility

---

## Approach

- **Baseline Modeling**
  - Trained logistic regression and SVM with radial kernel on all 168 features.
  - Evaluated models using balanced accuracy and ROC–AUC on the validation set.

- **Feature Selection**
  - Applied **Recursive Feature Elimination (RFE)** with an SVM base learner to identify a compact set (~40) of high-signal descriptors.
  - Trained **L1-regularized logistic regression (Lasso)** to shrink uninformative coefficients to zero and compare its selected feature set.

- **Refinement & Model Selection**
  - Re-trained LR and SVM models on:
    - RFE-selected features.
    - L1-selected features.
  - Compared performance to the full-feature baseline and chose the final model for Codalab submission.

---

## Key Findings

- Both logistic regression and SVM perform **better on a reduced feature set** than on all 168 descriptors, demonstrating the value of feature selection.
- The **SVM + RFE (~40 features)** model delivers the best balance of balanced accuracy and AUC on the validation set and is chosen as the final submission.
- L1-regularized models, even when slightly behind in raw AUC, provide a clearer view of which descriptors are most influential for biodegradability.
- P-value-based feature selection used in the original baseline is less stable and less performant than modern regularization and RFE methods.

---

## Skills Demonstrated

- **Statistical Modeling in R** – building and tuning logistic regression and SVM models using `caret` and `e1071`.
- **Feature Selection Techniques** – implementing and comparing RFE and L1 regularization to reduce dimensionality.
- **Model Evaluation** – using balanced accuracy and ROC–AUC with a controlled validation split for fair comparison.
- **Trade-off Analysis** – weighing interpretability versus predictive performance when recommending a “production” model.
- **Client-Oriented Framing** – translating a competition setting into a practical consulting narrative for an R&D stakeholder.
