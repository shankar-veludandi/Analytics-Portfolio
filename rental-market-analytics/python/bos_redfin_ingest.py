"""
File: bos_redfin_ingest.py
Author: Shankar Veludandi
Created: 2025-03-01
Last Updated: 2025-12-08

Description:
    End-to-end ETL pipeline for ingesting Boston rental listings from the
    Redfin API via RapidAPI. The pipeline performs the following steps:

    1. Iterates through all Boston ZIP codes
    2. Fetches paginated Redfin rental listings
    3. Normalizes nested JSON responses into flat records
    4. Applies ZIP-to-neighborhood enrichment
    5. Deduplicates listings across ZIP codes
    6. Casts all columns to analytics-safe data types
    7. Loads final dataset into PostgreSQL (Supabase)

Target Table:
    raw.bos_redfin_listings_raw

Dependencies:
    - requests
    - pandas
    - sqlalchemy
    - psycopg2

Execution:
    python bos_redfin_ingest.py
"""

# ============================================================
# Standard Library Imports
# ============================================================

import time
from datetime import datetime

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
# Redfin API Configuration (RapidAPI)
# ============================================================

API_KEY = settings.API_KEY
if not API_KEY:
    raise RuntimeError("Missing API_KEY in environment")
BASE_URL = "https://redfin-com-data.p.rapidapi.com/property/search-rent"

HEADERS = {
    "x-rapidapi-host": "redfin-com-data.p.rapidapi.com",
    "x-rapidapi-key": API_KEY
}

# ============================================================
# API Fetching Utilities
# ============================================================

def fetch_redfin_listings(zipcode: str, page: int, max_retries: int = 3, backoff_factor: int = 2):
    """
    Fetch a single page of rental listings from the Redfin API.

    Implements retry logic with exponential backoff to safely recover
    from transient network failures and gateway timeouts.

    Args:
        zipcode (str): 5-digit ZIP code to query.
        page (int): Page number for paginated results.
        max_retries (int): Number of retry attempts on failure.
        backoff_factor (int): Multiplier for wait time between retries.

    Returns:
        tuple:
            - list: List of raw listing JSON objects.
            - int: Total expected result count for the ZIP code.
    """
    params = {
        "location": zipcode,
        "page": page
    }

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
            response.raise_for_status()
            json_data = response.json()
            results = json_data.get("data", [])
            total_count = json_data.get("totalResultCount", 0)
            return results, total_count

        except requests.exceptions.ReadTimeout:
            wait_time = backoff_factor * attempt
            print(
                f"Timeout fetching {zipcode} (page {page}). "
                f"Retrying in {wait_time}s... (Attempt {attempt}/{max_retries})"
            )
            time.sleep(wait_time)

        except requests.exceptions.HTTPError as e:
            if response.status_code == 504:
                wait_time = backoff_factor * attempt
                print(
                    f"504 Gateway Timeout for {zipcode} page {page}. "
                    f"Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
                continue
            else:
                print(f"HTTP error {response.status_code} for {zipcode}: {e}")
                return [], 0

        except requests.exceptions.RequestException as e:
            print(f"Request failed for {zipcode} (page {page}): {e}")
            return [], 0

    print(f"Max retries exceeded for {zipcode} (page {page}). Skipping.")
    return [], 0

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
    Normalize a raw Redfin API listing into a flat relational schema.

    Args:
        item (dict): Raw listing JSON object.

    Returns:
        dict: Cleaned listing record.
    """
    homeData = safe_get(item, "homeData") or {}
    rentalExtension = safe_get(item, "rentalExtension") or {}
    addr = safe_get(homeData, "addressInfo") or {}
    coords = safe_get(addr, "centroid", "centroid") or {}

    return {
        "listing_id": rentalExtension.get("rentalId"),
        "price_min": rentalExtension.get("rentPriceRange", {}).get("min"),
        "price_max": rentalExtension.get("rentPriceRange", {}).get("max"),
        "beds_min": rentalExtension.get("bedRange", {}).get("min"),
        "beds_max": rentalExtension.get("bedRange", {}).get("max"),
        "baths_min": rentalExtension.get("bathRange", {}).get("min"),
        "baths_max": rentalExtension.get("bathRange", {}).get("max"),
        "sqft_min": rentalExtension.get("sqftRange", {}).get("min"),
        "sqft_max": rentalExtension.get("sqftRange", {}).get("max"),
        "zip_code": addr.get("zip"),
        "latitude": coords.get("latitude"),
        "longitude": coords.get("longitude"),
        "address_line": addr.get("formattedStreetLine"),
        "url": "https://www.redfin.com" + homeData.get("url", "")
    }

# ============================================================
# ZIP-Level Extraction, Cleaning & Enrichment
# ============================================================

def fetch_and_clean_zip(zipcode: str) -> pd.DataFrame:
    """
    Fetches all paginated Redfin rental listings for a ZIP code,
    normalizes the dataset, and applies neighborhood enrichment.

    Args:
        zipcode (str): Boston ZIP code.

    Returns:
        pd.DataFrame: Cleaned ZIP-level dataset.
    """
    listings = []
    page = 1
    total_count = None

    while True:
        results, count = fetch_redfin_listings(zipcode, page)

        if total_count is None:
            total_count = count
            print(f"Total expected listings for {zipcode}: {total_count}")

        if not results:
            print(f"No more data returned for {zipcode}. Exiting loop.")
            break

        for item in results:
            listings.append(parse_listing(item))

        print(f"Fetched {len(results)} listings from {zipcode} (page {page})")

        # Exit loop once all known results are fetched
        if len(listings) >= total_count:
            print(f"All {total_count} listings retrieved for {zipcode}.")
            break

        page += 1
        time.sleep(0.2)  # Lightweight rate limiting

    df = pd.DataFrame(listings)

    if df.empty:
        print(f"No data fetched for {zipcode}. Skipping normalization.")
        return df

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
        time.sleep(1)  # Cross-ZIP throttling

    print(f"Total Listings Fetched: {len(master_df)}")

    # Enforce string type for deduplication
    master_df["listing_id"] = master_df["listing_id"].astype(str)

    # Deduplicate listings across ZIP codes
    master_df = master_df.drop_duplicates(subset="listing_id", keep="first")

    # Apply analytics-safe data types
    master_df = master_df.astype({
        "listing_id": "string",
        "price_min": "Int32",
        "price_max": "Int32",
        "beds_min": "float32",
        "beds_max": "float32",
        "baths_min": "float32",
        "baths_max": "float32",
        "sqft_min": "Int32",
        "sqft_max": "Int32",
        "zip_code": "string",
        "latitude": "float32",
        "longitude": "float32",
        "address_line": "string",
        "url": "string",
        "neighborhood": "string"
    }, errors="ignore")

    # ========================================================
    # PostgreSQL (Supabase) Load
    # ========================================================

    engine = create_engine(settings.get_sqlalchemy_url())

    # Truncate raw staging table prior to reload
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw.bos_redfin_listings_raw"))
        print("✅ Truncated raw.bos_redfin_listings_raw")

    # Load DataFrame into PostgreSQL
    master_df.to_sql(
        "bos_redfin_listings_raw",
        engine,
        schema="raw",
        if_exists="append",
        index=False
    )

    print("📥 Data loaded successfully into PostgreSQL.")
