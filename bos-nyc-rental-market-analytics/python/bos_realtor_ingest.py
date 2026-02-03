"""
File: bos_realtor_ingest.py
Author: Shankar Veludandi
Created: 2025-03-01
Last Updated: 2025-12-08

Description:
    End-to-end ETL pipeline for ingesting Boston rental listings from the
    Realtor API via RapidAPI. The pipeline performs the following steps:

    1. Iterates through all Boston ZIP codes
    2. Fetches paginated Realtor rental listings
    3. Normalizes and cleans raw JSON responses
    4. Applies neighborhood enrichment using ZIP mapping
    5. Deduplicates records
    6. Casts all columns to analytics-safe data types
    7. Loads final dataset into PostgreSQL (Supabase)

Target Table:
    raw.bos_realtor_listings_raw

Dependencies:
    - requests
    - pandas
    - sqlalchemy
    - psycopg2

Execution:
    python bos_realtor_ingest.py
"""

# ============================================================
# Standard Library Imports
# ============================================================

import time
from datetime import datetime
import re

# ============================================================
# Third-Party Library Imports
# ============================================================

import requests
import pandas as pd
from sqlalchemy import create_engine, text
from config import settings

# ============================================================
# Boston ZIP → Neighborhood Mapping
# Used for geographic enrichment
# ============================================================

BOS_ZIP_TO_NEIGHBORHOOD = {
    "02108": "Beacon Hill",
    "02109": "Financial District",
    "02110": "Financial District",
    "02111": "Chinatown",
    "02113": "North End",
    "02114": "West End",
    "02115": "Longwood",
    "02116": "Back Bay",
    "02118": "South End",
    "02119": "Roxbury",
    "02120": "Mission Hill",
    "02121": "Dorchester",
    "02122": "Dorchester",
    "02124": "Dorchester",
    "02125": "Dorchester",
    "02126": "Mattapan",
    "02127": "South Boston",
    "02128": "East Boston",
    "02129": "Charlestown",
    "02130": "Jamaica Plain",
    "02131": "Roslindale",
    "02132": "West Roxbury",
    "02134": "Allston",
    "02135": "Brighton",
    "02136": "Hyde Park",
    "02199": "Back Bay",
    "02210": "Seaport",
    "02215": "Fenway"
}

# ============================================================
# Realtor API Configuration (RapidAPI)
# ============================================================

API_KEY = settings.API_KEY
if not API_KEY:
    raise RuntimeError("Missing API_KEY in environment")
BASE_URL = "https://realtor16.p.rapidapi.com/search/forrent"

HEADERS = {
    "x-rapidapi-host": "realtor16.p.rapidapi.com",
    "x-rapidapi-key": API_KEY
}

# ============================================================
# API Fetching Utilities
# ============================================================

def fetch_realtor_listings(zipcode: str, page: int, max_retries: int = 3, backoff_factor: int = 2):
    """
    Fetch a single page of rental listings for a given ZIP code from the Realtor API.

    Implements retry logic with exponential backoff to safely handle
    transient network issues and API gateway timeouts.

    Args:
        zipcode (str): 5-digit ZIP code to query.
        page (int): Page number for paginated results.
        max_retries (int): Number of retry attempts on failure.
        backoff_factor (int): Multiplier for wait time between retries.

    Returns:
        list: List of raw property JSON objects.
    """
    params = {
        "location": zipcode,
        "page": page,
        "limit": 200
    }

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=20)
            response.raise_for_status()
            json_data = response.json()
            return json_data.get("properties", [])

        except requests.exceptions.ReadTimeout:
            wait_time = backoff_factor * attempt
            print(f"Timeout fetching {zipcode} (page {page}). Retrying in {wait_time}s... (Attempt {attempt}/{max_retries})")
            time.sleep(wait_time)

        except requests.exceptions.HTTPError as e:
            if response.status_code == 504:
                wait_time = backoff_factor * attempt
                print(f"504 Gateway Timeout for {zipcode} page {page}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                print(f"HTTP error {response.status_code} for {zipcode}: {e}")
                return []

        except requests.exceptions.RequestException as e:
            print(f"Request failed for {zipcode} (page {page}): {e}")
            return []

    print(f"Max retries exceeded for {zipcode} (page {page}). Skipping.")
    return []

# ============================================================
# Safe JSON Access Utility
# Prevents KeyErrors in nested objects
# ============================================================

def safe_get(obj, *keys):
    """
    Safely traverse nested dictionaries using a variable list of keys.

    Args:
        obj (dict): Source dictionary.
        *keys: Arbitrary depth of nested keys.

    Returns:
        Any: Value at nested path or None if missing.
    """
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return None
    return obj

# ============================================================
# Raw Listing Normalization
# ============================================================

def parse_listing(item: dict) -> dict:
    """
    Normalize a raw Realtor API listing into a flat schema suitable for relational storage.

    Args:
        item (dict): Raw property JSON object.

    Returns:
        dict: Cleaned listing record.
    """
    description = safe_get(item, "description") or {}
    addr = safe_get(item, "location", "address") or {}
    coords = safe_get(addr, "coordinate") or {}
    pet_policy = safe_get(item, "pet_policy") or {}

    return {
        "listing_id": item.get("listing_id"),
        "list_price": item.get("list_price"),
        "beds": description.get("beds"),
        "baths": description.get("baths_consolidated"),
        "sqft": description.get("sqft"),
        "list_date": item.get("list_date"),
        "zip_code": addr.get("postal_code"),
        "latitude": coords.get("lat"),
        "longitude": coords.get("lon"),
        "address_line": addr.get("line"),
        "url": "https://www.realtor.com/rentals/details/" + item.get("permalink", ""),
        "pet_cats": pet_policy.get("cats"),
        "pet_dogs": pet_policy.get("dogs")
    }

# ============================================================
# ZIP-Level Extraction, Cleaning & Enrichment
# ============================================================

def fetch_and_clean_zip(zipcode: str) -> pd.DataFrame:
    """
    Fetches all paginated Realtor listings for a ZIP code,
    normalizes the dataset, and applies neighborhood enrichment.

    Args:
        zipcode (str): Boston ZIP code.

    Returns:
        pd.DataFrame: Cleaned ZIP-level dataset.
    """
    listings = []
    page = 1

    while True:
        time.sleep(0.6)  # Rate limiting to avoid API throttling

        data = fetch_realtor_listings(zipcode, page)
        if not data:
            break

        for item in data:
            listings.append(parse_listing(item))

        print(f"Fetched {len(data)} listings from {zipcode} (page {page})")

        # End pagination if fewer than page limit returned
        if len(data) < 200:
            break

        page += 1

    df = pd.DataFrame(listings)

    # Normalize date fields
    df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce")

    # Enrich listings with Boston neighborhood using ZIP mapping
    df["neighborhood"] = df["zip_code"].map(BOS_ZIP_TO_NEIGHBORHOOD)

    return df

# ============================================================
# Main Execution Block
# ============================================================

if __name__ == "__main__":

    # Aggregate all ZIP-level datasets into a master DataFrame
    master_df = pd.DataFrame()

    for zip_code in BOS_ZIP_TO_NEIGHBORHOOD.keys():
        df_zip = fetch_and_clean_zip(zip_code)
        print(f"{zip_code}: {len(df_zip)} listings fetched")
        master_df = pd.concat([master_df, df_zip], ignore_index=True)

    print(f"🧮 Total fetched listings: {len(master_df)}")

    # Enforce string type for deduplication
    master_df["listing_id"] = master_df["listing_id"].astype(str)

    # Deduplicate listings across ZIP codes
    master_df = master_df.drop_duplicates(subset="listing_id", keep="first")

    # Apply analytics-safe data types
    master_df = master_df.astype({
        "listing_id": "string",
        "list_price": "Int32",
        "beds": "float32",
        "baths": "float32",
        "sqft": "Int32",
        "list_date": "datetime64[ns]",
        "zip_code": "string",
        "latitude": "float32",
        "longitude": "float32",
        "address_line": "string",
        "url": "string",
        "pet_cats": "boolean",
        "pet_dogs": "boolean",
        "neighborhood": "string"
    }, errors="ignore")

    # ========================================================
    # Numeric Field Sanitization
    # Some numeric fields arrive as mixed strings
    # ========================================================

    def clean_numeric_column(series: pd.Series) -> pd.Series:
        """
        Extracts numeric values from mixed string/numeric fields.

        Args:
            series (pd.Series): Raw numeric series.

        Returns:
            pd.Series: Cleaned float values.
        """
        return (
            series.astype(str)
            .str.extract(r'([\d\.]+)')[0]
            .astype(float)
        )

    master_df["beds"] = clean_numeric_column(master_df["beds"])
    master_df["baths"] = clean_numeric_column(master_df["baths"])

    # ========================================================
    # PostgreSQL (Supabase) Load
    # ========================================================

    engine = create_engine(settings.get_sqlalchemy_url())

    # Truncate raw staging table prior to reload
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw.bos_realtor_listings_raw"))
        print("✅ Truncated raw.bos_realtor_listings_raw")

    # Load DataFrame into PostgreSQL
    master_df.to_sql(
        "bos_realtor_listings_raw",
        engine,
        schema="raw",
        if_exists="append",
        index=False
    )

    print("📥 Data loaded successfully into PostgreSQL.")
