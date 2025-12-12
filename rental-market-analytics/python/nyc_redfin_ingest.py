"""
File: nyc_redfin_ingest.py
Author: Shankar Veludandi
Created: 2025-03-01
Last Updated: 2025-12-08

Description:
    End-to-end ETL pipeline for ingesting New York City rental listings from the
    Redfin API via RapidAPI. The pipeline performs the following steps:

    1. Iterates through all NYC ZIP codes grouped by borough
    2. Fetches paginated Redfin rental listings
    3. Normalizes nested JSON responses into flat records
    4. Applies borough-level enrichment
    5. Deduplicates listings across ZIP codes
    6. Casts all columns to analytics-safe data types
    7. Loads final dataset into PostgreSQL (Supabase)

Target Table:
    raw.nyc_redfin_listings_raw

Dependencies:
    - requests
    - pandas
    - sqlalchemy
    - psycopg2

Execution:
    python nyc_redfin_ingest.py
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
# NYC ZIP Codes Grouped by Borough
# Used for borough-level enrichment
# ============================================================

NYC_ZIPS_BY_BOROUGH = {

    "Manhattan": [
        "10001","10002","10003","10004","10006","10007","10009","10010",
        "10011","10012","10013","10014","10016","10017","10018","10019",
        "10021","10022","10023","10024","10025","10026","10027","10028",
        "10029","10030","10031","10032","10033","10034","10035","10036",
        "10037","10038","10039","10040","10044","10065","10069","10075",
        "10128","10280"
    ],

    "Staten Island": [
        "10301","10304","10305","10306","10308"
    ],

    "Bronx": [
        "10451","10454","10455","10456","10457","10458","10461","10462",
        "10463","10466","10467","10468","10471","10473"
    ],

    "Queens": [
        "11004","11101","11102","11103","11104","11105","11106","11354",
        "11355","11356","11357","11358","11360","11361","11362","11363",
        "11364","11365","11366","11367","11368","11369","11370","11372",
        "11373","11374","11375","11377","11378","11379","11385","11411",
        "11412","11413","11414","11415","11416","11417","11418","11419",
        "11420","11421","11422","11423","11426","11427","11428","11429",
        "11432","11433","11434","11435","11436","11691","11692","11693",
        "11694"
    ],

    "Brooklyn": [
        "11201","11203","11205","11206","11207","11208","11209","11211",
        "11212","11214","11215","11216","11217","11218","11220","11221",
        "11222","11224","11225","11226","11228","11229","11230","11231",
        "11233","11234","11235","11237","11238","11249"
    ]
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
        zipcode (str): 5-digit NYC ZIP code to query.
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
                    f"504 Gateway Timeout for {zipcode} (page {page}). "
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
# ZIP-Level Extraction, Cleaning & Borough Enrichment
# ============================================================

def fetch_and_clean_zip(zipcode: str, borough: str) -> pd.DataFrame:
    """
    Fetches all paginated Redfin rental listings for a ZIP code,
    normalizes the dataset, and applies borough enrichment.

    Args:
        zipcode (str): NYC ZIP code.
        borough (str): Borough name.

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
            rec = parse_listing(item)

            # Sanitize ZIP as 5-digit string
            rec["zip_code"] = str(rec["zip_code"])[:5].strip() if "zip_code" in rec else None

            # Apply borough enrichment
            rec["borough"] = borough
            listings.append(rec)

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

    return df

# ============================================================
# Main Execution Block
# ============================================================

if __name__ == "__main__":

    # Aggregate all ZIP-level datasets into a master DataFrame
    master_df = pd.DataFrame()

    for borough, zip_list in NYC_ZIPS_BY_BOROUGH.items():
        for zip_code in zip_list:
            df_zip = fetch_and_clean_zip(zip_code, borough)
            print(f"{borough} - {zip_code}: {len(df_zip)} listings fetched")
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
        "borough": "string"
    }, errors="ignore")

    # ========================================================
    # PostgreSQL (Supabase) Load
    # ========================================================

    engine = create_engine(settings.get_sqlalchemy_url())

    # Truncate raw staging table prior to reload
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw.nyc_redfin_listings_raw"))
        print("✅ Truncated raw.nyc_redfin_listings_raw")

    # Load DataFrame into PostgreSQL
    master_df.to_sql(
        "nyc_redfin_listings_raw",
        engine,
        schema="raw",
        if_exists="append",
        index=False
    )

    print("📥 Data loaded successfully into PostgreSQL.")
