"""
Automated Data Quality (QA) Suite
---------------------------------
This module implements the automated quality assurance processes required to 
ensure data reliability across the Medallion Architecture.
"""

import logging
import sys
from pathlib import Path
import duckdb

# Setup Quality Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - QUALITY_CHECK - %(levelname)s - %(message)s"
)
logger = logging.getLogger("DataQuality")

SILVER_FILE = "data/silver/fct_cars.parquet"
GOLD_IMPACT_FILE = "data/gold/mart_market_impact.parquet"

def run_quality_checks():
    con = duckdb.connect()
    
    # Check if files exist
    if not Path(SILVER_FILE).exists():
        logger.error(f"FAIL: Silver layer not found at {SILVER_FILE}")
        return False

    logger.info("Starting Data Quality Suite...")
    
    tests_failed = 0

    # --- 1. SILVER LAYER CHECKS ---
    logger.info(">>> Validating SILVER layer...")
    
    # Integrity: Unique IDs
    dup_count = con.execute(f"SELECT COUNT(id) FROM read_parquet('{SILVER_FILE}') GROUP BY id HAVING COUNT(*) > 1").fetchone()
    if dup_count:
        logger.warning(f"FAIL: Found duplicates in Silver ID column.")
        tests_failed += 1
    else:
        logger.info("PASS: Primary Key Integrity (IDs are unique).")

    # Accuracy: No negative or 0 prices
    invalid_prices = con.execute(f"SELECT COUNT(*) FROM read_parquet('{SILVER_FILE}') WHERE price <= 0").fetchone()[0]
    if invalid_prices > 0:
        logger.warning(f"FAIL: Found {invalid_prices} records with zero or negative prices.")
        tests_failed += 1
    else:
        logger.info("PASS: Price Accuracy (All prices > 0).")

    # Business Logic Accuracy: ULEZ Compliance Check
    # Verify that no Diesel before 2015 is marked as compliant
    logic_errors = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{SILVER_FILE}') 
        WHERE fuel_type = 'diesel' AND year < 2015 AND is_ulez_compliant = TRUE
    """).fetchone()[0]
    
    if logic_errors > 0:
        logger.error(f"FAIL: Found {logic_errors} Diesel cars with incorrect ULEZ status.")
        tests_failed += 1
    else:
        logger.info("PASS: ULEZ Compliance Logic Integrity.")

    # --- 2. GOLD LAYER CHECKS ---
    logger.info(">>> Validating GOLD layer...")
    
    if Path(GOLD_IMPACT_FILE).exists():
        null_impacts = con.execute(f"SELECT COUNT(*) FROM read_parquet('{GOLD_IMPACT_FILE}') WHERE percent_diff IS NULL").fetchone()[0]
        if null_impacts > 0:
            logger.warning(f"FAIL: Found NULL values in Gold Impact metrics.")
            tests_failed += 1
        else:
            logger.info("PASS: Gold Metrics Completeness.")

    # --- FINAL REPORT ---
    if tests_failed > 0:
        logger.error(f"QUALITY SUITE FAILED: {tests_failed} test(s) failed.")
        return False
    else:
        logger.info("QUALITY SUITE PASSED: All checks successful.")
        return True

if __name__ == "__main__":
    success = run_quality_checks()
    if not success:
        sys.exit(1)
