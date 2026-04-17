# 📘 Operational Runbook: ULEZ Analytics Platform

This document provides standardized procedures for maintaining reliable, well-governed data pipelines and ensuring operational stability.

---

## 🚨 Incident Response & Root-Cause Analysis

### 1. Ingestion Failure (AutoTrader API)
**Symptoms**: No new data detected in the Bronze layer (`data/bronze/`).
**Root-Cause Categories**:
- **External**: API rate limiting (HTTP 429) or service downtime (HTTP 5xx).
- **Internal**: Network configuration or SSL certificate changes.
**Action & Mitigation**:
1. Inspect `logs/pipeline.log` for HTTP status codes and error messages.
2. Validate API connectivity: `python 01_ingestion/data_engine.py`.
3. If API schema changes occur, update the field mapping in `01_ingestion/autotrader_collector.py`.

### 2. Processing Pipeline Failure (Silver/Gold)
**Symptoms**: Pipeline crashes; Gold-layer datasets are missing or stale.
**Root-Cause Categories**:
- **Schema Drift**: New columns in Bronze not handled in the transformation logic.
- **Data Quality**: Zero-row output triggers guardrail abort (ValueError).
- **Resource Exhaustion**: Memory issues during large local Parquet processing.
**Action & Mitigation**:
1. Review the DuckDB execution trace in `logs/pipeline.log`.
2. Run `python 05_quality/quality_checks.py` to identify data quality failures.
3. The dynamic schema detection in `02_processing/databricks_pipeline.py` handles missing columns automatically — check if new required columns need fallback logic.

---

## 🚦 Data Quality & Governance Monitoring

### Automated Quality Checks
- **Script**: `05_quality/quality_checks.py`
- **Checks**: Primary Key integrity, price accuracy (>0), ULEZ compliance logic validation, Gold metrics completeness.
- **Audit Trail**: Results are persisted to `data/diagnostics/quality_audit.parquet` (last 100 runs).

### Security & Access Control
- **Credential Management**: API keys and credentials stored in `.env` (excluded from version control via `.gitignore`).
- **Data Exclusion**: Raw data (`data/`) is excluded from git to prevent accidental exposure of scraped listings.

---

## 🔄 Deployment & Release Process

### Release Checklist
1. **Lint**: Run `ruff check .` to enforce code style.
2. **Unit Tests**: Run `pytest tests/` to validate collector logic.
3. **Quality Gate**: Execute `python 05_quality/quality_checks.py` on current data.
4. **Release**: Push to `main` branch to trigger GitHub Actions CI/CD.

### Pipeline Monitoring
- Review `logs/pipeline.log` for processing times, row counts, and error traces.
- Monitor `data/diagnostics/quality_audit.parquet` via the Streamlit dashboard's Data Trust section.

---

## 📋 Known Limitations & Production Roadmap

### Current Limitations
- **Full Refresh Only**: The pipeline does a complete reload on each run. No incremental/CDC strategy is implemented.
- **Local Scale**: Designed for thousands of records, not petabyte-scale workloads.
- **Single-threaded**: No parallel processing across brands/fuel types.

### Production Migration Path
To deploy this pipeline to a cloud Lakehouse environment:
1. Replace local `data/` paths with `abfss://` (Azure Data Lake Gen2) URIs.
2. Swap DuckDB engine for Spark SQL on Databricks — the SQL logic is portable.
3. Add incremental load strategy using watermark columns (`ingestion_timestamp`).
4. Implement secret management via Azure Key Vault or environment variables.
5. Schedule pipeline runs via Databricks Workflows or Apache Airflow.
