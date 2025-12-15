import praw
import re
import nltk
import spacy
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.sentiment import SentimentIntensityAnalyzer
from spacy import displacy

#load spacy model
nlp = spacy.load("en_core_web_sm")

# Ensure necessary NLTK resources are downloaded
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('vader_lexicon')

# Sentiment analyzer instance
sia =  SentimentIntensityAnalyzer()

def collect_data(subreddit, limit=500):
    print("collect_data")
    reddit = praw.Reddit(
        client_id='XnMzPzoLHzr6SZj3j9cGSQ',
        client_secret='0NCyCy08f94Hka6HxnqcTzgDqMNWog',
        user_agent='script by /u/Dizzy_Rub5734'
    )
    data = []
    for submission in reddit.subreddit(subreddit).hot(limit=limit):
        submission.comments.replace_more(limit=10)
        comments_count = len(submission.comments.list())
        for comment in submission.comments.list()[:100]:
            data.append({
                'title': submission.title,
                'comment_count': comments_count,
                'net_engagement': comment.score,
                'timestamp': comment.created_utc
            })
    return data

def clean_text(text):
    print("clean_text")
    """ Basic text cleaning and tokenization """
    text = re.sub(r'\s+', ' ', text)  # Replace multiple whitespaces with single space
    text = re.sub(r'http\S+', '', text)  # Remove URLs
    words = word_tokenize(text.lower())
    stop_words = set(stopwords.words('english'))
    filtered_words = [word for word in words if word not in stop_words and word.isalnum()]
    return ' '.join(filtered_words)

def add_sentiment_score(data):
    print("add_sentiment_score")
    """ Append sentiment scores to each title """
    for item in data:
        item['sentiment'] = sia.polarity_scores(item['title'])['compound']
    return data

def preprocess_and_filter(data):
    print("preprocess_and_filter")
    """ Clean, filter, and enrich data with sentiment analysis """
    filtered_data = []
    for item in data:
        cleaned_title = clean_text(item['title'])
        
        # Check if text is significantly short to filter out
        if len(cleaned_title.split()) < 5:
            continue

        # Add cleaned and processed information back to the item
        item['cleaned_title'] = cleaned_title

        # Extracting linguistic features from the title
        question_features = is_open_ended_question(cleaned_title)
        item.update(question_features)
        
        filtered_data.append(item)
    
    # Add sentiment analysis after filtering to avoid processing unnecessary data
    filtered_data_with_sentiment = add_sentiment_score(filtered_data)
    return filtered_data_with_sentiment

def linguistic_features(text):
    """ Analyze text for complex linguistic features using Spacy """
    doc = nlp(text)
    num_sub_clauses = sum(1 for token in doc if token.dep_ == 'acl')
    num_passive = sum(1 for token in doc if token.dep_ == 'auxpass')
    return {'num_sub_clauses': num_sub_clauses, 'num_passive': num_passive}

def is_open_ended_question(text):
    wh_words = {'what', 'how', 'why', 'which', 'where', 'who'}
    modal_verbs = {'can', 'could', 'would', 'might', 'should'}
    words = word_tokenize(text.lower())
    linguistic_data = linguistic_features(text)
    features = {
        'contains_wh_word': any(word in wh_words for word in words),
        'contains_modal_verb': any(word in modal_verbs for word in words),
        'question_length': len(words),  # Longer questions tend to be more open-ended
        'num_sub_clauses': linguistic_data['num_sub_clauses'],
        'num_passive': linguistic_data['num_passive']
    }
    return features

def extract_question_features(data):
    print("extract_question_features")
    """ Append features indicating if a title suggests an open-ended question """
    for item in data:
        question_features = is_open_ended_question(item['title'])
        item.update(question_features)
    return data

import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.svm import SVC
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

def extract_features(data):
    print("extract_features")
    """ Extract and return feature vectors for model training """
    vectorizer = TfidfVectorizer(max_features=1000)
    tfidf_titles = vectorizer.fit_transform([item['title'] for item in data])
    
    # Create DataFrame from TF-IDF Sparse Matrix
    features = pd.DataFrame(tfidf_titles.toarray(), columns=vectorizer.get_feature_names_out())

    # Add open-ended question features
    features['contains_wh_word'] = [item['contains_wh_word'] for item in data]
    features['contains_modal_verb'] = [item['contains_modal_verb'] for item in data]
    features['question_length'] = [item['question_length'] for item in data]
    
    # Add engagement and temporal features
    features['net_engagement'] = [item['net_engagement'] for item in data]
    features['sentiment'] = [item['sentiment'] for item in data]
    features['hour_of_day'] = [datetime.fromtimestamp(item['timestamp']).hour for item in data]
    features['day_of_week'] = [datetime.fromtimestamp(item['timestamp']).weekday() for item in data]

    # Engagement rate could be added based on additional context provided (e.g., normalized by views if available)
    return features

def visualize_data(features):
    print("visualize_data")
    print("histogram")
    """ Visualize engagement metrics and temporal features """
    plt.figure(figsize=(10, 6))
    sns.histplot(features['net_engagement'], bins=50, kde=False, color='skyblue')
    plt.title('Distribution of Net Engagement Scores')
    plt.xlabel('Net Engagement')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()

    print("boxplot")
    # Hour of Day Analysis
    days_of_week = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    plt.figure(figsize=(12, 6))
    sns.boxplot(x='day_of_week', y='net_engagement', data=features)
    plt.yscale('log')  # Set y-axis to logarithmic scale
    plt.xticks(ticks=range(len(days_of_week)), labels=days_of_week)  # Set custom x-axis labels
    plt.title('Engagement by Day of Week')
    plt.xlabel('Day of Week')
    plt.ylabel('Net Engagement (log scale)')
    plt.grid(True)
    plt.show()

    print("time series")
    #Time Series Analysis of engagement over time by the average engagement score by hour of the day
    plt.figure(figsize=(12, 6))
    features.groupby('hour_of_day')['net_engagement'].mean().plot(kind='line')
    plt.title('Average Engagement by Hour of Day')
    plt.xlabel('Hour of Day')
    plt.ylabel('Average Net Engagement')
    plt.grid(True)
    plt.show()

    print("scatter plot")
    #Scatter Plots with Trend Lines between sentiment and net engagement
    plt.figure(figsize=(10, 6))
    sns.regplot(x='sentiment', y='net_engagement', data=features)
    plt.title('Sentiment vs. Net Engagement')
    plt.xlabel('Sentiment Score')
    plt.ylabel('Net Engagement')
    plt.show()

    print("violin plot")
    #Violion Plot comparing the distribution of engagement scores across different days of the week
    plt.figure(figsize=(12, 6))
    sns.violinplot(x='day_of_week', y='net_engagement', data=features)
    plt.xticks(ticks=range(len(days_of_week)), labels=days_of_week)
    plt.title('Net Engagement Scores by Day of Week')
    plt.xlabel('Day of Week')
    plt.ylabel('Net Engagement')
    plt.show()




def train_and_evaluate(features, labels):
    print("train_and_evaluate")
    """ Train SVM and evaluate performance """
    X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size=0.2, random_state=42)
    
    params = {'C': [0.1, 1, 10], 'kernel': ['linear', 'rbf']}
    model = GridSearchCV(SVC(), params, cv=5)
    model.fit(X_train, y_train)
    
    print("Best model parameters:", model.best_params_)
    predictions = model.predict(X_test)
    print(classification_report(y_test, predictions))
    print(confusion_matrix(y_test, predictions))

# Prepare labels assuming binary classification based on engagement
enriched_data = extract_question_features(preprocess_and_filter(collect_data('AskReddit')))
features = extract_features(enriched_data)
labels = np.where(features['net_engagement'] > features['net_engagement'].median(), 1, 0)  # Binary classification high/low engagement

visualize_data(features)
train_and_evaluate(features.drop(columns=['net_engagement']), labels)  # Drop net_engagement when training