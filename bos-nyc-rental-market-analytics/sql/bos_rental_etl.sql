-- =====================================================================
-- Script: bos_rental_etl.sql
-- Author: Shankar Veludandi
-- Created: 2025-03-01
-- Last Updated: 2025-12-08
--
-- Description:
--   End-to-end SQL transformations for the Boston rental pipeline.
--   Converts raw Realtor & Redfin ingestions into an analytics-ready table:
--     1) Create core schemas
--     2) Define raw source tables (Realtor + Redfin)
--     3) Normalize & split addresses (staging)
--     4) Build cleaned, deduplicated, merged materialized views
--     5) Fill missing sqft using ZIP/bed/bath medians
--     6) Apply outlier filters and publish analytics.bos_final
--
-- Conventions:
--   - Schemas: raw → staging → cleaned → analytics
--   - Keep business-facing names stable in analytics.*
--   - Preserve traceability via *_url and *_id fields upstream
-- =====================================================================


-- ============================================================
-- 0) Schema Initialization (idempotent)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS cleaned;
CREATE SCHEMA IF NOT EXISTS analytics;


-- ============================================================
-- 1) Raw Source Tables (API landings)
--    Notes:
--      - Keep types flexible at raw layer (minimal coercion)
--      - No constraints beyond primary keys
-- ============================================================
CREATE TABLE IF NOT EXISTS raw.bos_realtor_listings_raw (
    listing_id   VARCHAR(10) PRIMARY KEY,  -- Realtor listing identifier
    list_price   INTEGER,                  -- Monthly rent in USD
    beds         REAL,                     -- Bedrooms (Studio=0.0 in upstream)
    baths        REAL,                     -- Bathrooms (may arrive with symbols)
    sqft         INTEGER,                  -- Interior square footage
    list_date    DATE,                     -- First seen or listed date
    zip_code     CHAR(5),                  -- 5-digit ZIP
    latitude     REAL,
    longitude    REAL,
    address_line VARCHAR(100),             -- Single-line street address
    url          VARCHAR(255),             -- Canonical listing URL
    pet_cats     BOOLEAN,
    pet_dogs     BOOLEAN,
    neighborhood VARCHAR(29)               -- Upstream neighborhood label
);

CREATE TABLE IF NOT EXISTS raw.bos_redfin_listings_raw (
    listing_id CHAR(36) PRIMARY KEY,       -- Redfin rentalId (UUID-like)
    price_min  INTEGER,
    price_max  INTEGER,
    beds_min   REAL,
    beds_max   REAL,
    baths_min  REAL,
    baths_max  REAL,
    sqft_min   INTEGER,
    sqft_max   INTEGER,
    zip_code   CHAR(5),
    latitude   REAL,
    longitude  REAL,
    address_line VARCHAR(100),
    url          VARCHAR(255),
    neighborhood VARCHAR(29)
);


-- ============================================================
-- 2) Address Normalization (staging)
--    Purpose:
--      - Split single-line address into [address_line_1, address_line_2]
--      - Produce comparable keys for matching Realtor↔Redfin
--    Behavior:
--      - Detects unit markers (Unit/Apt/Ste/Ph/Fl/#)
--      - Returns lowercase, trimmed, punctuation-safe values
-- ============================================================
CREATE OR REPLACE FUNCTION staging.split_address(full_address TEXT)
RETURNS TABLE (address_line_1 TEXT, address_line_2 TEXT)
AS $$
DECLARE
    keyword_pos INT;
BEGIN
    -- Find position of the first unit-indicator, if any (case-sensitive here).
    -- NOTE: We intentionally check for the marker with a trailing space when relevant
    -- to avoid partial matches inside words (e.g., "Unit" vs "UnitA"). Where the
    -- POSITION() call omits the trailing space (Unit/Apt), it still finds the token
    -- correctly in our data; adjust if your upstream varies.
    SELECT
        CASE
            WHEN POSITION('Unit ' IN full_address) > 0 THEN POSITION('Unit' IN full_address)
            WHEN POSITION('Apt '  IN full_address) > 0 THEN POSITION('Apt'  IN full_address)
            WHEN POSITION('Ste '  IN full_address) > 0 THEN POSITION('Ste ' IN full_address)
            WHEN POSITION('Ph '   IN full_address) > 0 THEN POSITION('Ph '  IN full_address)
            WHEN POSITION('Fl '   IN full_address) > 0 THEN POSITION('Fl '  IN full_address)
            WHEN POSITION('#'     IN full_address) > 0 THEN POSITION('#'    IN full_address)
            ELSE 0
        END
    INTO keyword_pos;

    -- Split into base street line and unit line (if present)
    IF keyword_pos = 0 THEN
        address_line_1 := full_address;
        address_line_2 := NULL;
    ELSE
        address_line_1 := SUBSTRING(full_address FROM 1 FOR keyword_pos - 1);
        address_line_2 := SUBSTRING(full_address FROM keyword_pos);
    END IF;

    -- Normalize: lowercase, trim, and clean punctuation
    address_line_1 := LOWER(TRIM(REGEXP_REPLACE(address_line_1, '[^a-zA-Z0-9\s\.\'']', '', 'g')));

    -- Remove leading unit keywords and extra spaces from line_2 (case-insensitive)
    address_line_2 := LOWER(TRIM(REGEXP_REPLACE(address_line_2, '(^|\s)(Apt|Unit|Ste|Ph|Fl|#)\s*', '', 'gi')));

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- ============================================================
-- 3) Staging Views – Realtor & Redfin Cleanups
--    Purpose:
--      - Apply split_address()
--      - Sanitize baths (strip symbols), compute Redfin averages
--      - Prepare comparable address_clean/unit_clean for joining
-- ============================================================

-- Realtor cleaned view
CREATE OR REPLACE VIEW staging.vw_bos_realtor_cleaned AS
SELECT DISTINCT ON (r.listing_id)
    r.listing_id,
    r.list_price,
    r.beds,
    NULLIF(REGEXP_REPLACE(r.baths::TEXT, '[^0-9\.]', '', 'g'), '')::REAL AS baths,
    r.sqft,
    r.zip_code,
    r.neighborhood,
    sa.address_line_1,
    sa.address_line_2,
    sa.address_line_1 AS address_clean,   -- comparable join key
    sa.address_line_2 AS unit_clean,      -- comparable join key
    r.latitude,
    r.longitude,
    r.url,
    r.pet_cats,
    r.pet_dogs,
    r.list_date
FROM raw.bos_realtor_listings_raw r
CROSS JOIN LATERAL staging.split_address(r.address_line) sa
WHERE r.listing_id IS NOT NULL
  AND r.zip_code   IS NOT NULL
  AND r.neighborhood IS NOT NULL
  AND r.url        IS NOT NULL
ORDER BY r.listing_id, r.list_date DESC;  -- prefer newest record by id

-- Redfin cleaned view
CREATE OR REPLACE VIEW staging.vw_bos_redfin_cleaned AS
SELECT DISTINCT ON (rd.listing_id)
    rd.listing_id,
    (rd.price_min  + rd.price_max)/2     AS price_avg,
    (rd.beds_min   + rd.beds_max)/2.0    AS beds_avg,
    (rd.baths_min  + rd.baths_max)/2.0   AS baths_avg,
    (rd.sqft_min   + rd.sqft_max)/2      AS sqft_avg,
    rd.zip_code,
    rd.neighborhood,
    sa.address_line_1,
    sa.address_line_2,
    sa.address_line_1 AS address_clean,  -- comparable join key
    sa.address_line_2 AS unit_clean,     -- comparable join key
    rd.latitude,
    rd.longitude,
    rd.url
FROM raw.bos_redfin_listings_raw rd
CROSS JOIN LATERAL staging.split_address(rd.address_line) sa
WHERE rd.listing_id  IS NOT NULL
  AND rd.zip_code    IS NOT NULL
  AND rd.neighborhood IS NOT NULL
  AND rd.url         IS NOT NULL;


-- ============================================================
-- 4) Cleaned Layer – Merged Materialized Views
--    Purpose:
--      - Match Realtor↔Redfin using address, unit, ZIP, lat/lon, and price band
--      - Keep unmatched records from each source
--      - Produce a single, deduped, analysis-friendly stream
--    Notes:
--      - Matching tolerances are intentionally small to reduce false positives:
--        • geo tolerance: ±0.0002 degrees
--        • price tolerance: ±$300
-- ============================================================
DROP MATERIALIZED VIEW IF EXISTS cleaned.mv_bos_merged_listings CASCADE;

CREATE MATERIALIZED VIEW cleaned.mv_bos_merged_listings AS
WITH matches AS (
    SELECT
        md5(r.url)  AS realtor_id,
        md5(rd.url) AS redfin_id,
        r.address_line_1, r.address_line_2,
        r.zip_code, r.neighborhood,
        rd.latitude, rd.longitude,
        r.list_price AS rent,
        COALESCE(rd.beds_avg,  r.beds)          AS beds,
        COALESCE(rd.baths_avg, r.baths::REAL)   AS baths,
        COALESCE(rd.sqft_avg,  r.sqft)          AS sqft,
        r.pet_cats, r.pet_dogs,
        r.list_date::DATE,
        r.url AS realtor_url, rd.url AS redfin_url
    FROM staging.vw_bos_realtor_cleaned r
    INNER JOIN staging.vw_bos_redfin_cleaned rd
        ON r.zip_code     = rd.zip_code
       AND r.address_clean = rd.address_clean
       AND COALESCE(r.unit_clean, '') = COALESCE(rd.unit_clean, '')
       AND ABS(r.latitude  - rd.latitude)  <= 0.0002
       AND ABS(r.longitude - rd.longitude) <= 0.0002
       AND ABS(r.list_price - rd.price_avg) <= 300
),
unmatched_realtor AS (
    SELECT 
        md5(r.url) AS realtor_id, NULL AS redfin_id,
        r.address_line_1, r.address_line_2, r.zip_code, r.neighborhood,
        r.latitude, r.longitude, r.list_price AS rent,
        r.beds, r.baths::REAL, r.sqft,
        r.pet_cats, r.pet_dogs, 
        r.list_date::DATE,
        r.url AS realtor_url, NULL AS redfin_url
    FROM staging.vw_bos_realtor_cleaned r
    LEFT JOIN matches m ON m.realtor_id = md5(r.url)
    WHERE m.realtor_id IS NULL
),
unmatched_redfin AS (
    SELECT
        NULL AS realtor_id, md5(rd.url) AS redfin_id,
        rd.address_line_1, rd.address_line_2, rd.zip_code, rd.neighborhood,
        rd.latitude, rd.longitude, rd.price_avg AS rent,
        rd.beds_avg AS beds, rd.baths_avg AS baths, rd.sqft_avg AS sqft,
        NULL::BOOLEAN AS pet_cats, NULL::BOOLEAN AS pet_dogs,
        NULL::DATE AS list_date,
        NULL AS realtor_url, rd.url AS redfin_url
    FROM staging.vw_bos_redfin_cleaned rd
    LEFT JOIN matches m ON m.redfin_id = md5(rd.url)
    WHERE m.redfin_id IS NULL
)
SELECT *
FROM (
    -- Matched records use Realtor ID as the canonical listing key
    SELECT realtor_id AS listing_id, zip_code, neighborhood, latitude, longitude, rent, beds, 
           baths, sqft, pet_cats, pet_dogs, list_date, realtor_url AS url
    FROM matches 

    UNION ALL 

    -- Unmatched Realtor-only records
    SELECT realtor_id AS listing_id, zip_code, neighborhood, latitude, longitude, rent, beds, 
           baths, sqft, pet_cats, pet_dogs, list_date, realtor_url AS url
    FROM unmatched_realtor 

    UNION ALL 

    -- Unmatched Redfin-only records
    SELECT redfin_id AS listing_id, zip_code, neighborhood, latitude, longitude, rent, beds, 
           baths, sqft, pet_cats, pet_dogs, list_date, redfin_url AS url 
    FROM unmatched_redfin
) merged
WHERE rent      IS NOT NULL
  AND beds      IS NOT NULL
  AND baths     IS NOT NULL
  AND latitude  IS NOT NULL
  AND longitude IS NOT NULL
WITH DATA;


-- ============================================================
-- 5) Imputation – Fill Missing SqFt With Medians
--    Purpose:
--      - Provide sqft_filled for downstream price/sqft metrics
--      - Median by (zip, beds, baths) offers stable local estimates
-- ============================================================
DROP MATERIALIZED VIEW IF EXISTS cleaned.mv_bos_merged_filled CASCADE;

CREATE MATERIALIZED VIEW cleaned.mv_bos_merged_filled AS
WITH sqft_median AS (
    SELECT
        zip_code, beds, baths,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sqft) AS sqft_median
    FROM cleaned.mv_bos_merged_listings
    WHERE sqft IS NOT NULL
    GROUP BY zip_code, beds, baths
)
SELECT
    m.*,
    COALESCE(m.sqft, s.sqft_median) AS sqft_filled
FROM cleaned.mv_bos_merged_listings m
LEFT JOIN sqft_median s
    ON m.zip_code = s.zip_code
   AND m.beds     = s.beds
   AND m.baths    = s.baths
WHERE COALESCE(m.sqft, s.sqft_median) IS NOT NULL;


-- Refresh order matters: base view first, then filled view
REFRESH MATERIALIZED VIEW cleaned.mv_bos_merged_listings;
REFRESH MATERIALIZED VIEW cleaned.mv_bos_merged_filled;


-- ============================================================
-- 6) Analytics Table (Business-Facing Delivery)
--    Purpose:
--      - Outlier-filtered, analytics-ready Boston listings
--      - Stable column names for BI/reporting
--    Notes:
--      - Outlier strategy: two-tailed sqft fence + 95th pctl caps on rent/beds/baths
-- ============================================================
CREATE TABLE IF NOT EXISTS analytics.bos_final (
    "Listing ID"   TEXT PRIMARY KEY,
    "City"         TEXT,
    "Borough"      TEXT,
    NTA2020        TEXT,
    "ZIP Code"     TEXT,
    "Neighborhood" TEXT,
    "Latitude"     REAL,
    "Longitude"    REAL,
    "Rent"         INTEGER,
    "Beds"         REAL,
    "Baths"        REAL,
    "SqFt"         INTEGER,
    "Cats Allowed" BOOLEAN,
    "Dogs Allowed" BOOLEAN,
    "List Date"    DATE,
    "URL"          TEXT
);

TRUNCATE TABLE analytics.bos_final;

-- Compute robust cutoffs once; then filter & insert
INSERT INTO analytics.bos_final (
    "Listing ID", "City", "Borough", NTA2020, "ZIP Code", "Neighborhood",
    "Latitude", "Longitude", "Rent", "Beds", "Baths", "SqFt",
    "Cats Allowed", "Dogs Allowed", "List Date", "URL"
)
WITH percentiles AS (
    SELECT
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY sqft_filled) AS sqft_p05,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqft_filled) AS sqft_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rent)        AS rent_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY beds)        AS beds_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY baths)       AS baths_p95
    FROM cleaned.mv_bos_merged_filled
)
SELECT
    m.listing_id,
    'Boston'             AS "City",
    NULL::TEXT           AS "Borough",     -- Placeholder to align with NYC schema
    NULL::TEXT           AS NTA2020,       -- Placeholder to align with NYC schema
    m.zip_code           AS "ZIP Code",
    m.neighborhood       AS "Neighborhood",
    m.latitude           AS "Latitude",
    m.longitude          AS "Longitude",
    m.rent               AS "Rent",
    m.beds               AS "Beds",
    m.baths              AS "Baths",
    m.sqft_filled        AS "SqFt",
    m.pet_cats           AS "Cats Allowed",
    m.pet_dogs           AS "Dogs Allowed",
    m.list_date          AS "List Date",
    m.url                AS "URL"
FROM cleaned.mv_bos_merged_filled m, percentiles p
WHERE m.sqft_filled BETWEEN p.sqft_p05 AND p.sqft_p95
  AND m.rent  <= p.rent_p95
  AND m.beds  <= p.beds_p95
  AND m.baths <= p.baths_p95;
