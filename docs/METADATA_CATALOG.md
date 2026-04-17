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
| `id` | STRING | Unique Advert ID | **Primary Key**: Must be unique (validated in QA suite). |
| `brand` | STRING | Vehicle Manufacturer | Fallback to 'UNKNOWN' if missing in source. |
| `model` | STRING | Vehicle Model | Fallback to `title` → 'UNKNOWN' if NULL in source. |
| `year` | INTEGER | Manufacturing Year | Used for ULEZ compliance derivation. |
| `price` | DECIMAL | Listing Price (£) | Must be > 0 (validated in QA suite). |
| `mileage` | INTEGER | Odometer Reading | Extracted from API badge data. |
| `fuel_type` | STRING | Petrol, Diesel, Hybrid, EV | Standardized to lowercase via `lower(trim())`. |
| `engine_size` | FLOAT | Engine displacement (litres) | Extracted via regex from subtitle text. |
| `transmission` | STRING | Manual / Automatic | Heuristic detection from subtitle/grabber text. |
| `is_ulez_compliant` | BOOLEAN | ULEZ Status | Derived column based on Fuel Type and Year. |
| `processed_at` | TIMESTAMP | Processing Date | Audit timestamp for lineage tracking. |

### 🚦 ULEZ Compliance Rules
- **Petrol**: Compliant if `year >= 2006` (Euro 4 standard).
- **Diesel**: Compliant if `year >= 2015` (Euro 6 standard).
- **All Others**: Default to non-compliant (conservative approach).

---

## 📊 Gold Layer: Analytics Marts

### `mart_market_impact.parquet`
| Column | Type | Description |
| :--- | :--- | :--- |
| `brand` | STRING | Vehicle manufacturer |
| `avg_price_compliant` | DECIMAL | Average price of ULEZ-compliant vehicles |
| `avg_price_non_compliant` | DECIMAL | Average price of non-compliant vehicles |
| `percent_diff` | DECIMAL | Price gap percentage between compliant and non-compliant |

### `mart_diesel_devaluation.parquet`
| Column | Type | Description |
| :--- | :--- | :--- |
| `brand` | STRING | Vehicle manufacturer |
| `model` | STRING | Vehicle model |
| `avg_price_compliant` | DECIMAL | Average compliant diesel price |
| `avg_price_non_compliant` | DECIMAL | Average non-compliant diesel price |
| `devaluation_percent` | DECIMAL | Percentage devaluation due to ULEZ non-compliance |

### `mart_market_clusters.parquet`
| Column | Type | Description |
| :--- | :--- | :--- |
| `CLUSTER_ID` | INTEGER | K-Means assigned cluster ID |
| `CLUSTER_NAME` | STRING | Business label (e.g., "Premium Segment (Compliant)") |
| All Silver columns | — | Inherited from `fct_cars` |

---

## 🛡️ Data Governance

### Access Control
- **Credential Management**: `.env` file excluded from version control via `.gitignore`.
- **Data Exclusion**: `data/` directory excluded from git to prevent raw data exposure.
- **Read-only Consumers**: The Streamlit dashboard reads Gold layer via Parquet — no write access.

### Semantic Layer
Gold marts are designed as flat, pre-aggregated tables to minimize downstream processing:
- **Star Schema Ready**: Flat tables minimize join complexity for BI tools.
- **Pre-aggregated KPIs**: Key metrics (percent_diff, devaluation_percent) are pre-calculated.
- **Profiled**: Regular QA checks via `05_quality/quality_checks.py`.
