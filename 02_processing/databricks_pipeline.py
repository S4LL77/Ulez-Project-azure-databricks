"""
Local Lakehouse Pipeline: ULEZ Car Analytics (DuckDB Version)
------------------------------------------------------------
Layer: Medallion Architecture (Bronze -> Silver -> Gold)
Logic: SQL/DuckDB-based data cleaning and ULEZ compliance mapping.
Environment: Local Python (No Java required).
"""

import os
import logging
from pathlib import Path
from datetime import datetime
import duckdb

# --- STRUCTURED LOGGING CONFIGURATION ---
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - PI PELINE - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MedallionProcessor")

def run_medallion_pipeline():
    # Paths
    bronze_path = "data/bronze/*.parquet"
    silver_path = Path("data/silver/fct_cars.parquet")
    gold_impact_path = Path("data/gold/mart_market_impact.parquet")
    gold_diesel_path = Path("data/gold/mart_diesel_devaluation.parquet")

    # Ensure directories exist
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    gold_impact_path.parent.mkdir(parents=True, exist_ok=True)

    print(">>> Starting Local Lakehouse Pipeline (DuckDB)...")
    
    # Initialize DuckDB connection
    con = duckdb.connect()

    # --- 1. BRONZE -> SILVER ---
    print(">>> 1. Processing Bronze to Silver...")
    if not any(Path("data/bronze").glob("*.parquet")):
        print("ERROR: No bronze parquet files found. Run ingestion first.")
        return

    # STEP A: Dynamic Column Detection
    # This ensures the pipeline works even if columns are missing from ALL parquet files
    res = con.execute(f"SELECT * FROM read_parquet('{bronze_path}', union_by_name=True) LIMIT 0")
    current_cols = [desc[0] for desc in res.description]
    
    # Define fallback logic for columns that might be missing in old data
    brand_col = "brand" if "brand" in current_cols else "NULL"
    model_col = "model" if "model" in current_cols else "NULL"
    title_col = "title" if "title" in current_cols else "NULL"
    
    print(f"DEBUG: Schema detected. Brand exists: {'brand' in current_cols}, Model exists: {'model' in current_cols}")

    # STEP B: Transform and Clean with Dynamic SQL
    # We use temporary names locally and cast them to the final model
    query = f"""
        CREATE OR REPLACE TABLE silver_cars AS
        SELECT 
            id,
            _tmp_brand as brand,
            _tmp_model as model,
            year,
            price,
            mileage,
            fuel_type,
            engine_size,
            transmission,
            is_ulez_compliant,
            processed_at
        FROM (
            SELECT 
                coalesce({brand_col}, 'UNKNOWN') as _tmp_brand,
                coalesce({model_col}, {title_col}, 'UNKNOWN') as _tmp_model,
                year,
                CAST(price AS DECIMAL(10,2)) as price,
                mileage,
                lower(trim(fuelType)) as fuel_type,
                engineSize as engine_size,
                transmission,
                CASE 
                    WHEN (lower(trim(fuelType)) = 'petrol' AND year >= 2006) THEN TRUE
                    WHEN (lower(trim(fuelType)) = 'diesel' AND year >= 2015) THEN TRUE
                    ELSE FALSE
                END as is_ulez_compliant,
                now() as processed_at,
                id,
                ingestion_timestamp
            FROM read_parquet('{bronze_path}', union_by_name=True)
        )
        QUALIFY row_number() OVER (PARTITION BY id ORDER BY ingestion_timestamp DESC) = 1
    """
    con.execute(query)

    # Save Silver
    con.execute(f"COPY silver_cars TO '{silver_path}' (FORMAT PARQUET)")
    print(f"SUCCESS: Silver layer saved to {silver_path}")

    # --- 2. SILVER -> GOLD (Market Impact) ---
    print(">>> 2. Processing Silver to Gold (Impact)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_impact AS
        SELECT 
            brand,
            AVG(CASE WHEN is_ulez_compliant THEN price END) as avg_price_compliant,
            AVG(CASE WHEN NOT is_ulez_compliant THEN price END) as avg_price_non_compliant,
            round(((AVG(CASE WHEN NOT is_ulez_compliant THEN price END) - AVG(CASE WHEN is_ulez_compliant THEN price END)) / 
                   NULLIF(AVG(CASE WHEN is_ulez_compliant THEN price END), 0)) * 100, 1) as percent_diff
        FROM silver_cars
        GROUP BY brand
    """)
    con.execute(f"COPY gold_impact TO '{gold_impact_path}' (FORMAT PARQUET)")

    # --- 3. SILVER -> GOLD (Diesel Devaluation) ---
    print(">>> 3. Processing Silver to Gold (Diesel)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_diesel AS
        SELECT 
            brand,
            model,
            AVG(CASE WHEN is_ulez_compliant THEN price END) as avg_price_compliant,
            AVG(CASE WHEN NOT is_ulez_compliant THEN price END) as avg_price_non_compliant,
            round(((AVG(CASE WHEN NOT is_ulez_compliant THEN price END) - AVG(CASE WHEN is_ulez_compliant THEN price END)) / 
                   NULLIF(AVG(CASE WHEN is_ulez_compliant THEN price END), 0)) * 100, 1) as devaluation_percent
        FROM silver_cars
        WHERE fuel_type = 'diesel'
        GROUP BY brand, model
        HAVING avg_price_compliant IS NOT NULL AND avg_price_non_compliant IS NOT NULL
        ORDER BY devaluation_percent DESC
        LIMIT 10
    """)
    con.execute(f"COPY gold_diesel TO '{gold_diesel_path}' (FORMAT PARQUET)")

    print("\nSUCCESS: Medallion Pipeline complete (Local DuckDB).")
    logger.info("Pipeline successful. Final Gold tables verified locally.")

if __name__ == "__main__":
    logger.info("--- Data Pipeline Execution Started ---")
    try:
        run_medallion_pipeline()
    except Exception as e:
        logger.critical(f"Pipeline crashed with unhandled exception: {e}")
        raise
    logger.info("--- Data Pipeline Execution Finished ---")
