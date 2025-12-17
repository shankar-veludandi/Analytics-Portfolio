# Shankar’s Analytics Portfolio

Hi, I’m **Shankar Veludandi** – a **Data Analyst** with a strong foundation in Python, SQL, and Power BI.

This portfolio highlights projects where I:

- Start from a clear **business or domain problem**
- Build reproducible **data pipelines** and data models
- Perform **exploratory and statistical analysis**
- Communicate **actionable insights** to non-technical stakeholders

I also draw on my background in machine learning when predictive models add value to the analysis.

## Projects

### 🏠 [Boston & NYC Rental Market Analytics](./rental-market-analytics)

**Type:** End-to-End BI / Market Analytics  
**Tech:** Python, PostgreSQL, Power BI

Compares the rental markets in **Boston** and **New York City** from a renter’s perspective using live listing data.

- Built a cloud-hosted data warehouse with `raw → staging → cleaned → analytics` layers fed by Realtor and Redfin APIs.
- Cleaned, merged, and enriched 20K+ active listings; added geospatial context and outlier handling.
- Designed Power BI dashboards and a “Rental Finder” to answer questions like “Where can I afford to live with my budget?”

See the project folder for the Power BI report, stakeholder slide deck, and full pipeline code.

---

### 🏀 [NBA Trade Market Dynamics & Team Success](./nba-financial-analytics)

**Type:** Sports Business Analytics · Panel Data  
**Tech:** R, Python, web scraping, PCA, regression/classification

Analyzes whether NBA teams that use cap space, luxury-tax room, and cash more efficiently actually perform better in the following season.

- Assembled a 10-season team–season panel from NBA Stats and Spotrac salary data.
- Engineered financial utilization ratios and a PCA-based performance index.
- Built and evaluated regression and classification models for next-season performance and playoff qualification, then translated results into guidance for front-office decision-makers.

---

### 💬 [AskReddit Engagement Analytics](./reddit-engagement-analytics)

**Type:** Social Media / Content Analytics  
**Tech:** Python, PRAW, scikit-learn, spaCy, NLTK

Studies what makes r/AskReddit questions high vs low engagement based on wording, sentiment, and timing.

- Collected and labeled ~13K posts via the Reddit API using comment volume as an engagement proxy.
- Engineered text, sentiment, and temporal features; trained an SVM classifier with cross-validated tuning.
- Showed that linguistic and sentiment features matter more than posting time, giving content teams concrete levers to experiment with.

---

### 🐞 [Invasive Insect Analytics](./invasive-insect-analytics)

**Type:** Environmental / Spatio-Temporal Analytics  
**Tech:** Python, TensorFlow/Keras, GeoPandas

Distinguishes invasive insect species from native look-alikes and forecasts their county-level spread patterns across the Northeastern U.S.

- Pulled 100K iNaturalist observations and built a spatio-temporal dataset (grid cells × time).
- Performed geospatial EDA and then trained a hybrid CNN–LSTM model to forecast future presence intensity.
- Produced geographic error maps and confidence regions that help conservation teams prioritize survey and treatment efforts.

---

### 🧪 [ChemsRUs Biodegradability Analytics](./chemical-biodegradability-analytics)

**Type:** Applied Predictive Analytics · Feature Selection  
**Tech:** R, caret, glmnet, e1071, tidyverse

Consults **ChemsRUs** in a Codalab competition for chemcial biodegradability prediction.

- Explored 168 molecular descriptors for ~1,000 compounds and set up a robust train/validation framework.
- Compared baseline models to approaches using Recursive Feature Elimination and L1-regularized logistic regression.
- Recommended an SVM model on a compact feature subset that improves AUC and highlights the most important descriptors for screening.

---

## 📫 Contact
Email: [shankar.veludandi.02@gmail.com](mailto::shankar.veludandi.02@gmail.com)

LinkedIn: [/shankar-veludandi](https://www.linkedin.com/in/shankar-veludandi-7a5b461b3/)

I am actively seeking **entry-level Data Analyst roles** where I can work end-to-end—from data collection and modeling through to dashboards and stakeholder-ready insights.
