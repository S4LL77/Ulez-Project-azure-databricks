"""
Automated Data Quality (QA) Suite
---------------------------------
This module implements the automated quality assurance processes required to 
ensure data reliability across the Medallion Architecture.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import duckdb

# Setup Quality Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - QUALITY_CHECK - %(levelname)s - %(message)s"
)
logger = logging.getLogger("DataQuality")

SILVER_FILE = "data/silver/fct_cars.parquet"
GOLD_IMPACT_FILE = "data/gold/mart_market_impact.parquet"
AUDIT_LOG_FILE = "data/diagnostics/quality_audit.parquet"

def record_audit_result(check_name, layer, status, error_count):
    """
    Persists audit results to a diagnostics parquet file.
    """
    audit_dir = Path("data/diagnostics")
    audit_dir.mkdir(parents=True, exist_ok=True)
    
    new_record = {
        "check_timestamp": datetime.now(),
        "layer": layer,
        "check_name": check_name,
        "status": status,
        "error_count": int(error_count)
    }
    
    df_new = pd.DataFrame([new_record])
    
    if Path(AUDIT_LOG_FILE).exists():
        df_history = pd.read_parquet(AUDIT_LOG_FILE)
        df_combined = pd.concat([df_history, df_new], ignore_index=True)
        # Keep only the last 100 runs to avoid bloat
        df_combined = df_combined.tail(100)
    else:
        df_combined = df_new
        
    df_combined.to_parquet(AUDIT_LOG_FILE, index=False)

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
    dup_row = con.execute(f"SELECT COUNT(id) FROM read_parquet('{SILVER_FILE}') GROUP BY id HAVING COUNT(*) > 1").fetchone()
    dup_count = dup_row[0] if dup_row else 0
    
    if dup_count > 0:
        logger.warning(f"FAIL: Found {dup_count} duplicates in Silver ID column.")
        record_audit_result("Unique IDs", "SILVER", "FAIL", dup_count)
        tests_failed += 1
    else:
        logger.info("PASS: Primary Key Integrity (IDs are unique).")
        record_audit_result("Unique IDs", "SILVER", "PASS", 0)

    # Accuracy: No negative or 0 prices
    invalid_prices = con.execute(f"SELECT COUNT(*) FROM read_parquet('{SILVER_FILE}') WHERE price <= 0").fetchone()[0]
    if invalid_prices > 0:
        logger.warning(f"FAIL: Found {invalid_prices} records with zero or negative prices.")
        record_audit_result("Price Accuracy", "SILVER", "FAIL", invalid_prices)
        tests_failed += 1
    else:
        logger.info("PASS: Price Accuracy (All prices > 0).")
        record_audit_result("Price Accuracy", "SILVER", "PASS", 0)

    # Business Logic Accuracy: ULEZ Compliance Check
    logic_errors = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{SILVER_FILE}') 
        WHERE fuel_type = 'diesel' AND year < 2015 AND is_ulez_compliant = TRUE
    """).fetchone()[0]
    
    if logic_errors > 0:
        logger.error(f"FAIL: Found {logic_errors} Diesel cars with incorrect ULEZ status.")
        record_audit_result("ULEZ Logic Integrity", "SILVER", "FAIL", logic_errors)
        tests_failed += 1
    else:
        logger.info("PASS: ULEZ Compliance Logic Integrity.")
        record_audit_result("ULEZ Logic Integrity", "SILVER", "PASS", 0)

    # --- 2. GOLD LAYER CHECKS ---
    logger.info(">>> Validating GOLD layer...")
    
    if Path(GOLD_IMPACT_FILE).exists():
        null_impacts = con.execute(f"SELECT COUNT(*) FROM read_parquet('{GOLD_IMPACT_FILE}') WHERE percent_diff IS NULL").fetchone()[0]
        if null_impacts > 0:
            logger.warning(f"FAIL: Found NULL values in Gold Impact metrics.")
            record_audit_result("Metrics Completeness", "GOLD", "FAIL", null_impacts)
            tests_failed += 1
        else:
            logger.info("PASS: Gold Metrics Completeness.")
            record_audit_result("Metrics Completeness", "GOLD", "PASS", 0)

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

if __name__ == "__main__":
    success = run_quality_checks()
    if not success:
        sys.exit(1)
