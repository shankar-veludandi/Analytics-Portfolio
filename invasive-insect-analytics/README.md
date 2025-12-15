# Invasive Insect Spread Analytics

**Type:** Environmental Data Science · Spatio-Temporal Forecasting  
**Tech:** Python, TensorFlow/Keras, GeoPandas, Matplotlib

## Project Overview

Invasive insect species cost the U.S. billions of dollars each year and threaten native ecosystems. Conservation agencies need to know **where these species are likely to appear next** to target monitoring and treatment.

This project builds a data pipeline and forecasting model using iNaturalist observations to:

1. Classify invasive vs native look-alike insects, and  
2. Predict the future spread pattern of key invasive species across the northeastern U.S.

## Data

- **Source:** iNaturalist API
- **Scale:**  
  - ~69k images of target invasive insects  
  - ~34k images of native look-alikes
- **Region:** Northeastern United States
- **Features:**
  - Latitude / longitude of each observation
  - Date (year, month, day) and derived **seasonality index**
  - Region codes (e.g., state / eco-region)

## Analytical Pipeline

1. **Data Acquisition & Cleaning**
   - Fetched image metadata and geolocation via iNaturalist
   - Filtered target vs look-alike species
   - De-duplicated sightings and removed low-quality or ambiguous labels

2. **Exploratory Analysis**
   - Density maps of sightings by species and region
   - Seasonal patterns (e.g., emergence peaks by month)
   - Class imbalance analysis between invasive and native sets

3. **Modeling – Spread Prediction**
   - Aggregated sightings into spatial grids and time steps
   - Built a hybrid **CNN–LSTM** model:
     - CNN encodes spatial structure of observation grids
     - LSTM models temporal dynamics (spread over time)
   - Target: future presence intensity in each grid cell

4. **Evaluation**
   - Train / validation / test split: 60 / 20 / 20
   - Metrics:
     - Mean Squared Error
     - Mean geographic error (km) between predicted and actual centroids
     - 95th-percentile error bounds for latitude and longitude

## Key Findings

- For species with rich observation histories (e.g., Asian Lady Beetle), the model achieves **geographic mean errors on the order of tens of kilometers**, with tight 95th-percentile bounds.
- For sparse species (e.g., Emerald Ash Borer), errors are larger, highlighting where additional monitoring data would most improve forecasts.
- Spatial error maps and confidence ellipses help visualize **where predictions are reliable vs uncertain**, providing actionable guidance to conservation teams.

## Decision-Support Value

- Prioritize survey and treatment resources to high-risk cells where the model predicts imminent spread.
- Identify data-poor regions where additional sampling would most reduce uncertainty.
- Provide a repeatable framework to plug in new species and updated observations.

## What I Did as a Data Analyst

- Translated a broad conservation problem into concrete forecasting questions
- Designed the data schema for spatio-temporal modeling from raw citizen-science data
- Performed geospatial EDA and communicated patterns via maps and plots
- Implemented, tuned, and interpreted a deep learning model in the context of real-world conservation decisions
