-- =====================================================================
-- Script: nyc_rental_etl.sql
-- Author: Shankar Veludandi
-- Created: 2025-03-01
-- Last Updated: 2025-12-08
--
-- Description:
--   End-to-end SQL transformations for the New York City rental pipeline.
--   Converts raw Realtor & Redfin ingestions into an analytics-ready table:
--     1) Create core schemas
--     2) Define raw source tables (Realtor + Redfin)
--     3) Normalize & split addresses (staging)
--     4) Build cleaned, deduplicated, merged materialized views
--     5) Impute missing sqft by (zip, beds, baths) medians
--     6) Trim outliers and prepare geometry
--     7) Spatial join with NYC NTA boundaries (PostGIS)
--     8) Publish analytics.nyc_final with stable, business-facing columns
--
-- Conventions:
--   - Schemas: raw → staging → cleaned → analytics
--   - Stable column names in analytics.* for BI/reporting
--   - Matching tolerances chosen to minimize false positives:
--       • Geo tolerance: ±0.0002 degrees (≈10–20m)
--       • Price tolerance: ±$300
--
-- Requirements:
--   - PostGIS installed and enabled in the database (for ST_* functions)
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
--      - Keep raw types flexible (minimal coercion)
--      - Only PK constraints; cleaning happens downstream
-- ============================================================

-- Realtor raw (one row per API record)
CREATE TABLE IF NOT EXISTS raw.nyc_realtor_listings_raw (
    listing_id   VARCHAR(10) PRIMARY KEY,   -- Realtor listing identifier
    list_price   INTEGER,                   -- Monthly rent in USD
    beds         REAL,                      -- Bedrooms (studio → 0.0 upstream)
    baths        REAL,                      -- Bathrooms (may include symbols)
    sqft         INTEGER,
    list_date    DATE,                      -- Listing publication/seen date
    zip_code     CHAR(5),
    latitude     REAL,
    longitude    REAL,
    address_line VARCHAR(100),
    borough      VARCHAR(25),               -- Upstream borough label
    url          VARCHAR(255),              -- Canonical listing URL
    pet_cats     BOOLEAN,
    pet_dogs     BOOLEAN
);

-- Redfin raw (ranges averaged in staging)
CREATE TABLE IF NOT EXISTS raw.nyc_redfin_listings_raw (
    listing_id   CHAR(36) PRIMARY KEY,      -- Redfin rentalId (UUID-like)
    price_min    INTEGER,
    price_max    INTEGER,
    beds_min     REAL,
    beds_max     REAL,
    baths_min    REAL,
    baths_max    REAL,
    sqft_min     INTEGER,
    sqft_max     INTEGER,
    zip_code     CHAR(5),
    latitude     REAL,
    longitude    REAL,
    address_line VARCHAR(100),
    borough      VARCHAR(25),
    url          VARCHAR(255)
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
    -- Find position of the first unit-indicator, if any.
    -- Adjust markers if your upstream varies (case/spacing).
    SELECT
        CASE
            WHEN POSITION('Unit' IN full_address) > 0 THEN POSITION('Unit' IN full_address)
            WHEN POSITION('Apt'  IN full_address) > 0 THEN POSITION('Apt'  IN full_address)
            WHEN POSITION('Ste ' IN full_address) > 0 THEN POSITION('Ste ' IN full_address)
            WHEN POSITION('Ph '  IN full_address) > 0 THEN POSITION('Ph '  IN full_address)
            WHEN POSITION('Fl '  IN full_address) > 0 THEN POSITION('Fl '  IN full_address)
            WHEN POSITION('#'    IN full_address) > 0 THEN POSITION('#'    IN full_address)
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
    address_line_1 := LOWER(TRIM(REGEXP_REPLACE(address_line_1, '[^a-zA-Z0-9\s]', '', 'g')));

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
DROP VIEW IF EXISTS staging.vw_nyc_realtor_cleaned CASCADE;
CREATE OR REPLACE VIEW staging.vw_nyc_realtor_cleaned AS
SELECT DISTINCT ON (r.listing_id)
    r.listing_id,
    r.list_price,
    r.beds,
    NULLIF(REGEXP_REPLACE(r.baths::TEXT, '[^0-9\.]', '', 'g'), '')::REAL AS baths,
    r.sqft,
    r.zip_code,
    r.borough,
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
FROM raw.nyc_realtor_listings_raw r
CROSS JOIN LATERAL staging.split_address(r.address_line) sa
WHERE r.listing_id IS NOT NULL
  AND r.zip_code   IS NOT NULL
  AND r.url        IS NOT NULL
ORDER BY r.listing_id, r.list_date DESC;  -- prefer newest record by id

-- Redfin cleaned view
DROP VIEW IF EXISTS staging.vw_nyc_redfin_cleaned CASCADE;
CREATE OR REPLACE VIEW staging.vw_nyc_redfin_cleaned AS
SELECT DISTINCT ON (rd.listing_id)
    rd.listing_id,
    (rd.price_min  + rd.price_max)/2     AS price_avg,
    (rd.beds_min   + rd.beds_max)/2.0    AS beds_avg,
    (rd.baths_min  + rd.baths_max)/2.0   AS baths_avg,
    (rd.sqft_min   + rd.sqft_max)/2      AS sqft_avg,
    rd.zip_code,
    rd.borough,
    sa.address_line_1,
    sa.address_line_2,
    sa.address_line_1 AS address_clean,  -- comparable join key
    sa.address_line_2 AS unit_clean,     -- comparable join key
    rd.latitude,
    rd.longitude,
    rd.url
FROM raw.nyc_redfin_listings_raw rd
CROSS JOIN LATERAL staging.split_address(rd.address_line) sa
WHERE rd.listing_id IS NOT NULL
  AND rd.zip_code   IS NOT NULL
  AND rd.url        IS NOT NULL;


-- ============================================================
-- 4) Cleaned Layer – Merged Materialized Views
--    Purpose:
--      - Match Realtor↔Redfin using address, unit, ZIP, lat/lon, and price band
--      - Keep unmatched records from each source
--      - Produce a single, deduped, analysis-friendly stream
-- ============================================================
DROP MATERIALIZED VIEW IF EXISTS cleaned.mv_nyc_merged_listings CASCADE;

CREATE MATERIALIZED VIEW cleaned.mv_nyc_merged_listings AS
WITH matches AS (
    SELECT
        md5(r.url)  AS realtor_id,
        md5(rd.url) AS redfin_id,
        r.address_line_1, r.address_line_2,
        r.zip_code, r.borough,
        rd.latitude, rd.longitude,
        r.list_price AS rent,
        COALESCE(rd.beds_avg,  r.beds)        AS beds,
        COALESCE(rd.baths_avg, r.baths::REAL) AS baths,
        COALESCE(rd.sqft_avg,  r.sqft)        AS sqft,
        r.pet_cats, r.pet_dogs,
        r.list_date::DATE,
        r.url AS realtor_url, rd.url AS redfin_url
    FROM staging.vw_nyc_realtor_cleaned r
    INNER JOIN staging.vw_nyc_redfin_cleaned rd
        ON r.zip_code      = rd.zip_code
       AND r.address_clean = rd.address_clean
       AND COALESCE(r.unit_clean, '') = COALESCE(rd.unit_clean, '')
       AND ABS(r.latitude  - rd.latitude)  <= 0.0002
       AND ABS(r.longitude - rd.longitude) <= 0.0002
       AND ABS(r.list_price - rd.price_avg) <= 300
),
unmatched_realtor AS (
    SELECT 
        md5(r.url) AS realtor_id, NULL AS redfin_id,
        r.address_line_1, r.address_line_2, r.zip_code, r.borough,
        r.latitude, r.longitude, r.list_price AS rent,
        r.beds, r.baths::REAL, r.sqft,
        r.pet_cats, r.pet_dogs, 
        r.list_date::DATE,
        r.url AS realtor_url, NULL AS redfin_url
    FROM staging.vw_nyc_realtor_cleaned r
    LEFT JOIN matches m ON m.realtor_id = md5(r.url)
    WHERE m.realtor_id IS NULL
),
unmatched_redfin AS (
    SELECT
        NULL AS realtor_id, md5(rd.url) AS redfin_id,
        rd.address_line_1, rd.address_line_2, rd.zip_code, rd.borough,
        rd.latitude, rd.longitude, rd.price_avg AS rent,
        rd.beds_avg AS beds, rd.baths_avg AS baths, rd.sqft_avg AS sqft,
        NULL::BOOLEAN AS pet_cats, NULL::BOOLEAN AS pet_dogs,
        NULL::DATE AS list_date,
        NULL AS realtor_url, rd.url AS redfin_url
    FROM staging.vw_nyc_redfin_cleaned rd
    LEFT JOIN matches m ON m.redfin_id = md5(rd.url)
    WHERE m.redfin_id IS NULL
)
SELECT *
FROM (
   -- Matched records use Realtor ID as the canonical listing key
   SELECT realtor_id AS listing_id, zip_code, borough, latitude, longitude, rent, beds, 
          baths, sqft, pet_cats, pet_dogs, list_date, realtor_url AS url
   FROM matches 

   UNION ALL 

   -- Unmatched Realtor-only records
   SELECT realtor_id AS listing_id, zip_code, borough, latitude, longitude, rent, beds, 
          baths, sqft, pet_cats, pet_dogs, list_date, realtor_url AS url
   FROM unmatched_realtor 

   UNION ALL 

   -- Unmatched Redfin-only records
   SELECT redfin_id AS listing_id, zip_code, borough, latitude, longitude, rent, beds, 
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
DROP MATERIALIZED VIEW IF EXISTS cleaned.mv_nyc_merged_filled CASCADE;

CREATE MATERIALIZED VIEW cleaned.mv_nyc_merged_filled AS
WITH sqft_median AS (
    SELECT
        zip_code, beds, baths,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sqft) AS sqft_median
    FROM cleaned.mv_nyc_merged_listings
    WHERE sqft IS NOT NULL
    GROUP BY zip_code, beds, baths
)
SELECT
    m.*,
    COALESCE(m.sqft, s.sqft_median) AS sqft_filled
FROM cleaned.mv_nyc_merged_listings m
LEFT JOIN sqft_median s
    ON m.zip_code = s.zip_code
   AND m.beds     = s.beds
   AND m.baths    = s.baths
WHERE COALESCE(m.sqft, s.sqft_median) IS NOT NULL;


-- ============================================================
-- 6) Outlier Trim (Analytics MV) & Geometry Prep
--    Purpose:
--      - Trim extreme values for robust analytics
--      - Build geometry points for spatial join (PostGIS)
-- ============================================================
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_nyc_merged_trimmed CASCADE;

CREATE MATERIALIZED VIEW analytics.mv_nyc_merged_trimmed AS
WITH percentiles AS (
    SELECT
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY sqft_filled) AS sqft_p05,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqft_filled) AS sqft_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rent)        AS rent_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY beds)        AS beds_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY baths)       AS baths_p95
    FROM cleaned.mv_nyc_merged_filled
)
SELECT m.*
FROM cleaned.mv_nyc_merged_filled m, percentiles p
WHERE m.sqft_filled BETWEEN p.sqft_p05 AND p.sqft_p95
  AND m.rent  <= p.rent_p95
  AND m.beds  <= p.beds_p95
  AND m.baths <= p.baths_p95;


-- ============================================================
-- 7) Spatial Prep – NTA Boundaries & Listing Points
--    Purpose:
--      - Transform NTA boundaries to EPSG:4326
--      - Create point geometry for each listing
-- ============================================================
-- Normalize NTA boundary geometry into WGS84 (EPSG:4326)
DROP TABLE IF EXISTS public.nyc_nta_boundaries_4326 CASCADE;
CREATE TABLE public.nyc_nta_boundaries_4326 AS
SELECT 
    ogc_fid,
    borocode,
    boroname,
    countyfips,
    nta2020,
    ntaname,
    ntaabbrev,
    ntatype,
    cdta2020,
    cdtaname,
    shape_leng,
    shape_area,
    ST_Transform(geom, 4326) AS geom
FROM public.nyc_nta_boundaries;

-- Point table with spatial index for fast spatial joins
DROP TABLE IF EXISTS analytics.tbl_nyc_merged_trimmed CASCADE;
CREATE TABLE analytics.tbl_nyc_merged_trimmed AS
SELECT *, 
       ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom  
FROM analytics.mv_nyc_merged_trimmed
WHERE longitude IS NOT NULL AND latitude IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tbl_nyc_geom ON analytics.tbl_nyc_merged_trimmed USING GIST (geom);


-- ============================================================
-- 8) Spatial Join – Listing → NTA
--    Purpose:
--      - Attach borough & NTA attributes for city analytics
-- ============================================================
DROP VIEW IF EXISTS analytics.vw_nyc_listings_with_nta CASCADE;
CREATE VIEW analytics.vw_nyc_listings_with_nta AS
SELECT
    t.*,
    n.boroname  AS borough_name,
    n.ntaname   AS nta_name,
    n.ntaabbrev AS nta_abbrev,
    n.nta2020   AS NTA2020
FROM analytics.tbl_nyc_merged_trimmed t
INNER JOIN public.nyc_nta_boundaries_4326 n
  ON ST_Within(t.geom, n.geom);


-- ============================================================
-- 9) Final Analytics Table (Business-Facing Delivery)
--    Purpose:
--      - Stable, human-friendly schema for BI/reporting
-- ============================================================
CREATE TABLE IF NOT EXISTS analytics.nyc_final (
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

TRUNCATE TABLE analytics.nyc_final;

INSERT INTO analytics.nyc_final (
    "Listing ID", "City", "Borough", NTA2020, "ZIP Code", "Neighborhood",
    "Latitude", "Longitude", "Rent", "Beds", "Baths", "SqFt",
    "Cats Allowed", "Dogs Allowed", "List Date", "URL"
)
SELECT
    listing_id,
    'New York City'         AS "City",
    borough                 AS "Borough",
    NTA2020,
    zip_code                AS "ZIP Code",
    nta_name                AS "Neighborhood",
    latitude                AS "Latitude",
    longitude               AS "Longitude",
    rent                    AS "Rent",
    beds                    AS "Beds",
    baths                   AS "Baths",
    sqft_filled             AS "SqFt",
    pet_cats                AS "Cats Allowed",
    pet_dogs                AS "Dogs Allowed",
    list_date               AS "List Date",
    url                     AS "URL"
FROM analytics.vw_nyc_listings_with_nta
WHERE sqft_filled IS NOT NULL;
