# 📖 Metadata Catalog & Data Dictionary

This document serves as the **Metadata Repository** for the London ULEZ Market Impact project. It defines the structure, semantic meaning, and data governance rules for each layer of the Medallion Architecture.

---

## 🏗️ Medallion Structure Overview

| Layer | Type | Physical Path | Description |
| :--- | :--- | :--- | :--- |
| **Bronze** | Raw Parquet | `data/bronze/*.parquet` | Immutable source data from AutoTrader API. |
| **Silver** | Cleaned Table | `data/silver/fct_cars.parquet` | Validated, type-casted, and de-duplicated transactional records. |
| **Gold** | Analytics Mart | `data/gold/mart_*.parquet` | Aggregated insights for Market Impact and Diesel Devaluation. |
| **Diagnostics**| Audit Metadata | `data/diagnostics/quality_audit.parquet` | Historical QA results for platform stability monitoring. |

---

## 📋 Data Dictionary (Silver Layer: `fct_cars`)

This is the primary fact table used for analytics and clustering.

| Column | Data Type | Semantic Meaning | Business Rules / Quality Checks |
| :--- | :--- | :--- | :--- |
| `id` | STRING | Unique Advert ID | **Primary Key**: Must be unique (checked in Silver). |
| `brand` | STRING | Vehicle Manufacturer | Standardized to UPPERCASE. |
| `model` | STRING | Vehicle Model | Fallback to `title` if NULL in source. |
| `year` | INTEGER | Manufacturing Year | Must be >= 1900. |
| `price` | DECIMAL | Listing Price (£) | Must be > 0. Validated via `CAST`. |
| `fuel_type` | STRING | Petrol, Diesel, Hybrid, EV | Standardized to lowercase. |
| `is_ulez_compliant` | BOOLEAN | ULEZ Status | Derived column based on Fuel Type and Year. |
| `processed_at` | TIMESTAMP | Processing Date | Audit timestamp for lineage tracking. |

### 🚦 Semantic Rules for `is_ulez_compliant`
- **Petrol**: Compliant if `year >= 2006` (Euro 4).
- **Diesel**: Compliant if `year >= 2015` (Euro 6).
- **Alternative (Hybrid/EV)**: Default to Compliant (subject to further verification).

---

## 🛡️ Data Governance & Security Standards

### Access Control & Security
- **Regulated Environment**: This project simulates compliance within a highly regulated environment. 
- **Data Protection**: Implementation of `.gitignore` and `.env` prevents leak of connection strings or raw staging data.
- **Access Management**: Read-only access for analytical researchers; Read/Write access restricted to the Data Engineering automated service principal.

### Semantic Layer (Power BI Optimization)
To ensure the **availability of trusted, high-quality Gold-layer datasets**, the following semantic optimizations are applied:
- **Star Schema Ready**: Gold marts are designed as flat tables to minimize join complexity in Power BI.
- **Pre-aggregated Metrics**: Key business KPIs (Percent Difference, Average Price) are pre-calculated to reduce DAX overhead.
- **Data Profiling**: Regular profiling is conducted using `05_quality/quality_checks.py`.

---
*Maintained by the Data Platform Engineering Team. For use in Azure Synapse & Power BI.*
