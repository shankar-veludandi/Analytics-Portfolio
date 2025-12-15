# AskReddit Engagement Analytics

**Type:** Social Media Analytics · Text Mining · Classification  
**Tech:** Python, PRAW, scikit-learn, spaCy, NLTK, Matplotlib/Seaborn

## Project Overview

Content teams often ask: *What makes a post “take off” and drive conversation?*  

This project analyzes r/AskReddit posts to understand how the **wording and timing** of a question relate to community engagement. I build a classifier to distinguish **high-engagement** vs **low-engagement** threads and use it to diagnose which features—language, sentiment, or posting time—matter most.

## Business Questions

1. Which linguistic patterns in question titles are associated with high comment volume?
2. Does sentiment (positive, negative, neutral) impact engagement?
3. How much do timing factors (day of week, time of day) contribute compared with wording?

## Data

- **Source:** r/AskReddit subreddit via PRAW (Python Reddit API Wrapper)
- **Scope:** 500 “hot” threads; 13,315 question titles with:
  - Comment counts (engagement proxy)
  - Timestamps
- **Labeling:**
  - Posts with comment counts above the median → **High engagement**
  - Posts at or below median → **Low engagement**
- **Split:** 80% train / 20% test

## Feature Engineering

1. **Text features**
   - TF–IDF vectors over unigrams and bigrams
   - Part-of-Speech tag counts (e.g., interrogatives, pronouns)
   - Readability and structure: title length, average word length, presence of question mark
2. **Sentiment features**
   - Compound score from NLTK’s `SentimentIntensityAnalyzer`
3. **Temporal features**
   - Day of week (Mon–Sun)
   - Hour-of-day bucket (morning / afternoon / evening / night)

## Modeling Approach

- **Model:** Support Vector Machine (RBF kernel) using scikit-learn’s `SVC`
- **Tuning:** `GridSearchCV` over C and gamma
- **Evaluation metrics:**
  - Accuracy
  - Precision, recall, F1 for each engagement class
  - Confusion matrix

## Results & Insights

- Overall test accuracy around **82%**, with strong performance on low-engagement posts.
- High recall on low-engagement threads (≈92%) shows the model is good at flagging questions that are unlikely to spark conversation.
- High-engagement recall (~64%) indicates the model can identify many strong candidates but still misses some “viral” outliers.
- Linguistic and sentiment features contribute more signal than time-of-day alone, suggesting **how you ask** matters more than **when you ask**.

## Practical Applications

- Editorial teams can:
  - Use similar features to score draft question titles before posting.
  - A/B test alternative phrasings while holding timing constant.
  - Build dashboards that track engagement drivers over time.

## What I Did as a Data Analyst

- Designed the engagement labeling strategy (median-based threshold)
- Built a full scraping and cleaning pipeline from the Reddit API
- Engineered and interpreted text, sentiment, and temporal features
- Evaluated and documented model trade-offs, especially precision vs recall for high-engagement posts
- Translated technical findings into simple guidelines for content strategy
