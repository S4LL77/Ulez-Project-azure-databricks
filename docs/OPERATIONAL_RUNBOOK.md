# 📘 Operational Runbook: ULEZ Analytics Platform (Azure Databricks & Synapse Simulation)

This document provides standardized procedures for **Delivery of reliable, scalable and well-governed data pipelines**, ensuring operational stability and platform performance.

---

## 🚨 Incident Response & Root-Cause Analysis

### 1. Ingestion Failure (AutoTrader API)
**Symptoms**: No new data detected in the Bronze layer.
**Root-Cause Categories**:
- **External**: API Rate limiting or Service Downtime.
- **Internal**: Network configuration or SSL certificate changes.
**Action & Mitigation**:
1. Inspect `logs/pipeline.log` for HTTP 429 (Rate Limit) or 500 (Server Error).
2. Validate local connectivity: `python 01_ingestion/data_engine.py --test`.
3. If schema changes occur, update the ingestion mapping in `01_ingestion/autotrader_collector.py`.

### 2. Processing Pipeline Failure (Silver/Gold)
**Symptoms**: Pipeline crashes; Gold-layer datasets are missing or stale.
**Root-Cause Categories**:
- **Schema Drift**: New columns in Bronze not handled in the transformation logic.
- **Resource Exhaustion**: Memory issues during large local Parquet processing.
**Action & Mitigation**:
1. Review the DuckDB execution trace in `logs/pipeline.log`.
2. Run `python 05_quality/quality_checks.py` to identify if the failure was triggered by a Data Quality guardrail.
3. Common Fix: Use the dynamic schema detection logic in `02_processing/databricks_pipeline.py` to ensure input/output alignment.

---

## 🚦 Data Quality & Governance Monitoring

### Adherence to Standards
- **Automated QA**: Daily execution of `05_quality/quality_checks.py` to ensure **trusted, high-quality Gold-layer datasets**.
- **Audit Trails**: All data transformations across Bronze, Silver, and Gold are timestamped and logged for full lineage transparency.

### Security & Access Control
- **Regulated Environment**: Data access is restricted via `.gitignore` and `.env` local configurations to simulate production-level credential management.
- **Data Hardening**: Final Gold Parquet files are optimized for performance in consumers like **Azure Synapse** and **Power BI**.

---

## 🔄 Deployment & Engineering Standards

### release management processes
1. **Validation**: Run all unit tests via `pytest`.
2. **Quality Gate**: Execute the Quality Suite on a sample set.
3. **Release**: Push to `main` branch to trigger GitHub Actions CI/CD workflows.

### Platform Monitoring
- Periodic review of `pipeline.log` to monitor **Service Level Indicators (SLIs)** for data latency and processing time.

---
*Maintained by the Data Platform Engineering Team. In line with Hedyn's Technology Strategy.*
