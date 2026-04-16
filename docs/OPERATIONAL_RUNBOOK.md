# 📘 Operational Runbook: ULEZ Analytics Platform

This document provides standardized procedures for operating and troubleshooting the ULEZ data platform.

## 🚨 Incident Response & Troubleshooting

### 1. Ingestion Failure (AutoTrader API)
**Symptoms**: No new data in `BRONZE.AUTOTRADER_RAW`.
**Common Causes**: API Rate limiting, schema changes in GraphQL response.
**Action**:
1. Check `scripts/data_engine.py` logs.
2. Verify API connectivity: `python scripts/autotrader_collector.py --test`.
3. If schema changed, update `BRONZE` table definition in `snowflake/setup/01_databases.sql`.

### 2. dbt Transformation Failure (Silver/Gold)
**Symptoms**: Dashboard data is stale; `dbt run` fails.
**Action**:
1. Run `dbt test` to identify data quality violations.
2. Check `target/run_results.json` for specific SQL errors.
3. Common Fix: If source data is null, update `silver` models to handle coalesce logic.

## 🔄 Routine Maintenance

### Schema Evolution
When adding new vehicle attributes:
1. Update Snowflake `BRONZE` table.
2. Update `dbt` models in the `silver/` layer.
3. Run `dbt run --full-refresh` to rebuild incremental models.

### Access Control Review
Monthly audit of active roles:
1. Run `SHOW GRANTS ON ROLE ULEZ_DEVELOPER`.
2. Ensure `ULEZ_AUDITOR` role usage is logged.

## 🚀 Deployment Process

### Azure Databricks Integration
To deploy the Spark pipeline:
1. Upload `scripts/databricks_pipeline.py` to the Databricks Workspace `/production/` folder.
2. Configure a Job with a 2.1-node cluster (X-Small) for cost efficiency.
3. Schedule to run daily at 02:00 UTC.

---
*Created by Data Engineering Team - For internal use only (Regulated).*
