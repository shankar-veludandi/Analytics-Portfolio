"""
File: nyc_realtor_ingest.py
Author: Shankar Veludandi
Created: 2025-03-01
Last Updated: 2025-12-08

Description:
    End-to-end ETL pipeline for ingesting New York City rental listings from the
    Realtor API via RapidAPI. The pipeline performs the following steps:

    1. Iterates through all NYC ZIP codes by borough
    2. Fetches paginated Realtor rental listings
    3. Normalizes nested JSON responses into flat records
    4. Applies borough-level enrichment
    5. Deduplicates listings across ZIP codes
    6. Casts all columns to analytics-safe data types
    7. Loads final dataset into PostgreSQL (Supabase)

Target Table:
    raw.nyc_realtor_listings_raw

Dependencies:
    - requests
    - pandas
    - sqlalchemy
    - psycopg2

Execution:
    python nyc_realtor_ingest.py
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
from requests.exceptions import HTTPError
from config import settings

# ============================================================
# NYC ZIP Codes Grouped by Borough
# Used for borough enrichment
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
        "10301","10302","10303","10304","10305","10306","10307","10308",
        "10309","10310","10312","10314"
    ],

    "Bronx": [
        "10451","10453","10454","10455","10456","10457","10458","10459",
        "10460","10461","10462","10463","10464","10465","10466","10467",
        "10468","10469","10470","10471","10472","10473","10475"
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
        "11201","11203","11204","11205","11206","11207","11208","11209",
        "11210","11211","11212","11213","11214","11215","11216","11217",
        "11218","11219","11220","11221","11222","11223","11224","11225",
        "11226","11228","11229","11230","11231","11232","11233","11234",
        "11235","11236","11237","11238","11249"
    ]
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

def fetch_realtor_listings(zipcode: str, page: int, max_retries: int = 5, backoff_factor: int = 2):
    """
    Fetch a single page of rental listings from the Realtor API.

    Implements retry logic with exponential backoff to safely recover
    from rate limits, timeouts, and transient network failures.

    Args:
        zipcode (str): 5-digit ZIP code to query.
        page (int): Page number for paginated results.
        max_retries (int): Number of retry attempts on failure.
        backoff_factor (int): Multiplier for wait time between retries.

    Returns:
        list: List of raw listing JSON objects.
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
            print(
                f"⏳ Timeout fetching {zipcode} (page {page}). "
                f"Retrying in {wait_time}s... (Attempt {attempt}/{max_retries})"
            )
            time.sleep(wait_time)

        except requests.exceptions.HTTPError as e:
            status = response.status_code
            if status in [429, 504]:
                wait_time = (2 ** attempt) * 5
                print(
                    f"⚠️  HTTP {status} for {zipcode} (page {page}). "
                    f"Waiting {wait_time}s before retry... (Attempt {attempt}/{max_retries})"
                )
                time.sleep(wait_time)
                continue
            else:
                print(f"❌ HTTP error {status} for {zipcode} (page {page}): {e}")
                return []

        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed for {zipcode} (page {page}): {e}")
            return []

    print(f"❌ Max retries exceeded for {zipcode} (page {page}). Skipping.")
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
    Normalize a raw Realtor API listing into a flat relational schema.

    Args:
        item (dict): Raw listing JSON object.

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
# ZIP-Level Extraction, Cleaning & Borough Enrichment
# ============================================================

def fetch_and_clean_zip(zipcode: str, borough: str) -> pd.DataFrame:
    """
    Fetches all paginated Realtor rental listings for a ZIP code,
    normalizes the dataset, and applies borough enrichment.

    Args:
        zipcode (str): NYC ZIP code.
        borough (str): Borough name.

    Returns:
        pd.DataFrame: Cleaned ZIP-level dataset.
    """
    listings = []
    page = 1

    while True:
        data = fetch_realtor_listings(zipcode, page)
        if not data:
            break

        for item in data:
            rec = parse_listing(item)

            # Sanitize bath values such as "1+"
            rec["baths"] = (
                float(rec["baths"].replace("+", "").strip())
                if rec.get("baths") else None
            )

            rec["borough"] = borough
            listings.append(rec)

        print(f"Fetched {len(data)} listings from {zipcode} (page {page})")

        if len(data) < 200:
            break

        page += 1
        time.sleep(0.5)  # Rate limiting

    if not listings:
        print(f"⚠️  No listings found for {zipcode} ({borough}). Skipping.")
        return pd.DataFrame()

    try:
        df = pd.DataFrame(listings)

        if "list_date" not in df.columns:
            df["list_date"] = None

        df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce")
        return df

    except KeyError as e:
        print(f"⚠️  Skipping {zipcode} ({borough}) due to missing column: {e}")
        return pd.DataFrame()

    except Exception as e:
        print(f"❌ Unexpected error for {zipcode} ({borough}): {e}")
        return pd.DataFrame()

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

    print(f"Total Listings Fetched: {len(master_df)}")

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
        "borough": "string"
    }, errors="ignore")

    # ========================================================
    # PostgreSQL (Supabase) Load
    # ========================================================

    engine = create_engine(settings.get_sqlalchemy_url())

    # Truncate raw staging table prior to reload
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw.nyc_realtor_listings_raw"))
        print("✅ Truncated raw.nyc_realtor_listings_raw")

    # Load DataFrame into PostgreSQL
    master_df.to_sql(
        "nyc_realtor_listings_raw",
        engine,
        schema="raw",
        if_exists="append",
        index=False
    )

    print("📥 Data loaded successfully into PostgreSQL.")
