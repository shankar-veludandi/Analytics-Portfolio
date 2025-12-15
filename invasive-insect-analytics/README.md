# Invasive Insect Spread Analytics

Environmental data science project using citizen-science observations to forecast the spatial spread of invasive insect species across the northeastern United States.

**Type:** Environmental Analytics · Spatio-Temporal Forecasting  
**Tech:** Python, TensorFlow/Keras, GeoPandas, Matplotlib

---

## Business Problem

Invasive insects cost the U.S. billions of dollars and threaten native ecosystems. Conservation agencies need to know **where these species are likely to appear next** to proactively target monitoring and treatment.

This project asks:

- How are invasive vs native look-alike species distributed across space and time?
- Can we forecast near-term spread patterns to highlight high-risk regions?
- Where is the model confident vs uncertain, and how can that guide data collection and field operations?

---

## Data & Scope

- **Source:** iNaturalist API (citizen-science observations)  
- **Scale:**  
  - ~69k images of invasive target species  
  - ~34k images of native look-alikes  
- **Region:** Northeastern United States  
- **Granularity:** Individual observation with latitude/longitude and timestamp  
- **Derived features:** Seasonality indicators, spatial grid cells, and region codes

---

## Approach

- **Data Engineering & Cleaning**
  - Pulled observation metadata and geolocation from the iNaturalist API.
  - Filtered and labeled target invasive species vs native look-alikes.
  - Removed duplicates and low-quality/ambiguous records; aggregated observations into spatial grids and time steps.

- **Analysis & Modeling**
  - Conducted geospatial EDA: density maps, seasonal patterns, and class imbalance diagnostics.
  - Designed a hybrid **CNN–LSTM** architecture:
    - CNN encodes spatial structure of each grid snapshot.
    - LSTM models temporal dynamics of spread across time steps.
  - Trained and evaluated models with train/validation/test splits, focusing on geographic error metrics.

---

## Key Findings

- For species with rich observation histories, the model achieves **geographic mean errors on the order of tens of kilometers**, with relatively tight 95th-percentile error bounds.
- For sparsely observed species, errors increase, clearly showing where new survey data would most improve forecasts.
- Spatial error maps and confidence regions help distinguish **high-confidence vs low-confidence** areas, making it easier for agencies to prioritize field work.
- The overall pipeline is reusable: new species and updated observations can be plugged into the same framework.

---

## Skills Demonstrated

- **Geospatial Data Handling** – using GeoPandas and spatial grid aggregation to prepare spatio-temporal datasets.
- **Exploratory Geospatial Analysis** – density plots, seasonal trend analysis, and interpretation of class imbalance.
- **Deep Learning for Time & Space** – implementing and tuning a CNN–LSTM model for spatial–temporal prediction.
- **Evaluation Design** – using geographic error metrics and percentile error bounds to assess model usefulness for field decisions.
- **Applied Environmental Analytics** – translating model performance into actionable insights for conservation and monitoring strategy.
