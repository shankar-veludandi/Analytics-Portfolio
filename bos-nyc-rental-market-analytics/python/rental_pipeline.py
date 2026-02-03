"""
File: rental_pipeline.py
Author: Shankar Veludandi
Created: 2025-03-01
Last Updated: 2025-12-08

Description:
    Master script for the full Boston & NYC rental market data pipeline. 
    This script coordinates the end-to-end workflow:

    1. Executes all raw ETL scripts (Realtor + Redfin for BOS & NYC)
    2. Runs all SQL transformation pipelines
    3. Builds unified analytics tables for Power BI consumption
    4. Logs full execution details
    5. Records pipeline run status in PostgreSQL for monitoring

Execution:
    python rental_pipeline.py

Dependencies:
    - subprocess
    - psycopg2
    - sqlparse
    - logging
    - os
"""

# ============================================================
# Standard Library Imports
# ============================================================

import os
import datetime
import subprocess
import logging
import traceback

# ============================================================
# Third-Party Library Imports
# ============================================================

import psycopg2
import sqlparse
from config import settings

# ============================================================
# Repository & Logging Configuration
# ============================================================

# Absolute path to the root of the repository
REPO_PATH = os.path.dirname(os.path.abspath(__file__))

# Centralized log directory for pipeline runs
LOG_DIR = os.path.join(REPO_PATH, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Daily rotating pipeline log
logfile = os.path.join(
    LOG_DIR,
    f"pipeline_log_{datetime.date.today()}.log"
)

# Configure global logging format
logging.basicConfig(
    filename=logfile,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ============================================================
# PostgreSQL (Supabase) Connection Configuration
# ============================================================

DB_CONFIG = settings.get_psycopg2_kwargs()

# ============================================================
# External Script Execution Utility
# ============================================================

def run_script(label: str, command: list) -> None:
    """
    Executes an external Python ETL script using subprocess
    and logs execution status.

    Args:
        label (str): Human-readable label for logging.
        command (list): Command list passed to subprocess.

    Returns:
        None
    """
    try:
        logging.info(f"Starting {label}...")
        subprocess.run(command, check=True)
        logging.info(f"✅ Completed {label}.")

    except subprocess.CalledProcessError as e:
        logging.error(f"❌ {label} failed: {e}")
        raise

# ============================================================
# SQL Execution Utility (Multi-Statement Safe Runner)
# ============================================================

def execute_sql(sql_path: str, conn) -> None:
    """
    Executes a SQL script file containing one or more statements.
    Safely parses and executes statements sequentially.

    Args:
        sql_path (str): Path to the SQL file.
        conn: Active psycopg2 database connection.

    Returns:
        None
    """
    with open(sql_path, "r", encoding="utf-8") as f:
        raw_sql = f.read()

    # Split multi-statement SQL safely
    statements = sqlparse.split(raw_sql)

    with conn.cursor() as cursor:
        for stmt in statements:
            stmt_clean = stmt.strip()

            if not stmt_clean:
                continue

            try:
                print(f"\n📄 Executing SQL statement:\n{stmt_clean[:120]}...\n")
                cursor.execute(stmt_clean)
                conn.commit()

            except Exception as e:
                conn.rollback()
                print("❌ Error executing SQL statement:")
                print(stmt_clean)
                print(e)
                raise

# ============================================================
# Pipeline Run Status Tracking Utility
# ============================================================

def update_status(status: str, details: str = "") -> None:
    """
    Writes pipeline execution status into the analytics.pipeline_status
    table for monitoring and historical audit tracking.

    Args:
        status (str): Pipeline run status (SUCCESS / FAILURE).
        details (str): Optional error message or details.

    Returns:
        None
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Create status table if it does not exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS analytics.pipeline_status (
                run_timestamp TIMESTAMP PRIMARY KEY DEFAULT NOW(),
                status TEXT,
                details TEXT
            );
        """)

        # Record current pipeline execution status
        cur.execute(
            "INSERT INTO analytics.pipeline_status (status, details) VALUES (%s, %s);",
            (status, details)
        )

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f"Failed to update pipeline status: {e}")

# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    logging.info("==== Weekly Pipeline Start ====")

    try:
        # ----------------------------------------------------
        # Step 1: Execute All Raw Ingestion ETL Scripts
        # ----------------------------------------------------

        run_script("Boston Realtor Ingest", ["python", "bos_realtor_ingest.py"])
        run_script("Boston Redfin Ingest", ["python", "bos_redfin_ingest.py"])
        run_script("NYC Realtor Ingest", ["python", "nyc_realtor_ingest.py"])
        run_script("NYC Redfin Ingest", ["python", "nyc_redfin_ingest.py"])

        # ----------------------------------------------------
        # Step 2: Execute SQL Transformation Pipelines
        # ----------------------------------------------------

        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_session(autocommit=True)

        execute_sql("bos_rental_etl.sql", conn)
        execute_sql("nyc_rental_etl.sql", conn)

        # ----------------------------------------------------
        # Step 3: Build Unified Analytics Table for Power BI
        # ----------------------------------------------------

        with conn.cursor() as cur:
            logging.info("Creating unified listings table...")

            cur.execute("""
                DROP TABLE IF EXISTS analytics.listings;

                CREATE TABLE analytics.listings AS
                SELECT * FROM analytics.bos_final WHERE false;

                TRUNCATE TABLE analytics.listings;

                INSERT INTO analytics.listings
                SELECT * FROM analytics.bos_final
                UNION ALL
                SELECT * FROM analytics.nyc_final;
            """)

            # Add and update a system-level refresh timestamp
            cur.execute("""
                ALTER TABLE analytics.listings
                ADD COLUMN IF NOT EXISTS "Last Updated" TIMESTAMP;
            """)

            cur.execute("""
                UPDATE analytics.listings
                SET "Last Updated" = NOW()::timestamp(0);
            """)

            conn.commit()
            logging.info("✅ Unified listings table successfully created.")

        conn.close()

        # ----------------------------------------------------
        # Step 4: Finalize Pipeline Run
        # ----------------------------------------------------

        logging.info("==== Weekly Pipeline Complete ====")
        update_status("SUCCESS", "Pipeline completed successfully.")

    except Exception as e:
        error_message = traceback.format_exc()
        logging.error(f"❌ Pipeline failed:\n{error_message}")
        update_status("FAILURE", str(e))
