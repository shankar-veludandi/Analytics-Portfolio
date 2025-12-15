# AskReddit Engagement Analytics

Social media analytics project analyzing how the wording and timing of r/AskReddit question titles relate to engagement, measured by comment volume.

**Type:** Social Media Analytics · Text Mining · Classification  
**Tech:** Python, PRAW, scikit-learn, spaCy, NLTK, Matplotlib/Seaborn

---

## Business Problem

Content and community teams often ask: *What makes a post “take off” and drive conversation?* They need a framework that goes beyond gut feeling to understand which aspects of a question—language, sentiment, or timing—most influence engagement.

This project asks:

- Which linguistic patterns in question titles are associated with high comment volume?
- Does sentiment (positive, negative, neutral) impact engagement?
- How much do timing factors (day of week, time of day) contribute compared with wording?

---

## Data & Scope

- **Source:** r/AskReddit via the Reddit API (PRAW)  
- **Scope:** 500 “hot” threads; 13,315 question titles  
- **Features collected:** Comment counts, timestamps, raw title text  
- **Labeling:**  
  - Above-median comment count → **High engagement**  
  - At or below median → **Low engagement**  
- **Split:** 80% train / 20% test

---

## Approach

- **Data Engineering & Cleaning**
  - Built a scraping pipeline using PRAW to collect titles, timestamps, and engagement counts.
  - Cleaned and normalized text (tokenization, case-folding, punctuation removal) and filtered out obviously malformed entries.

- **Analysis & Modeling**
  - Engineered features:
    - TF–IDF over unigrams and bigrams.
    - Structural features (title length, average word length, presence of question marks).
    - Sentiment score using NLTK’s `SentimentIntensityAnalyzer`.
    - Temporal features: day-of-week and hour-of-day buckets.
  - Trained a **Support Vector Machine (RBF kernel)** classifier with `GridSearchCV` for hyperparameter tuning.
  - Evaluated with accuracy, precision/recall/F1 by class, and confusion matrix analysis.

---

## Key Findings

- The model achieves **~82% test accuracy**, with especially high recall (~92%) on low-engagement posts—useful for flagging questions unlikely to generate conversation.
- High-engagement recall (~64%) shows the model can surface many strong candidate titles but still misses some “viral” outliers.
- Linguistic and sentiment features provide more predictive signal than timing alone, suggesting that **how you ask** matters more than **when you ask**.
- The feature framework can be used to pre-score draft titles and support A/B testing of alternative phrasings.

---

## Skills Demonstrated

- **API-Based Data Collection** – scraping Reddit data with PRAW and handling rate limits and pagination.
- **Text Preprocessing & Feature Engineering** – TF–IDF representations, structural features, and sentiment scoring.
- **Supervised Learning** – SVM classifier with cross-validated hyperparameter tuning.
- **Model Evaluation & Diagnostics** – interpreting precision/recall trade-offs for high vs low engagement segments.
- **Business Storytelling** – converting model insights into actionable recommendations for content and community strategy.
