# Snowflake Configuration & Architecture

This document details the Snowflake environment setup for the London ULEZ project.

## 🏗️ Schema Architecture (Medallion)

We use a Medallion architecture to ensure data quality and lineage:

1.  **BRONZE**: Landing zone for raw data.
    - `AUTOTRADER_RAW`: JSON/Variant data from the API.
    - Data is ingested using `scripts/data_engine.py`.
2.  **SILVER**: Quality-controlled tables.
    - Transformations handled by **dbt**.
    - Type casting, deduplication, and ULEZ compliance logic applied.
3.  **GOLD**: Analytical layer.
    - Aggregated views for the dashboard and ML training.

## 🔐 Role-Based Access Control (RBAC)

To follow security best practices, access is restricted using specific roles:

- `ULEZ_ADMIN`: Full control over the `ULEZ_DB` database and schemas. Used for infrastructure changes.
- `ULEZ_DEVELOPER`: DML/DDL permissions on the schemas. Used by the dbt pipeline and ingestion scripts.
- **Hierarchy**: `ULEZ_DEVELOPER` -> `ULEZ_ADMIN` -> `SYSADMIN`.

## ⚙️ Compute (Warehouses)

- **Warehouse**: `ULEZ_WH`
- **Size**: `X-Small` (to maintain zero-cost tier when possible).
- **Auto-Suspend**: 60 seconds (prevents credit waste after the ingestion/dbt run finishes).

## 🚀 Ingestion Logic

The ingestion engine (`scripts/data_engine.py`) performs the following:

1.  Fetches data from AutoTrader GraphQL API.
2.  Pre-processes basic attributes (Mileage, Engine Size).
3.  Uses Snowflake **Internal Stages** for bulk loading via `PUT` and `COPY INTO`.
4.  Ensures idempotency by checking `advertId` before final insertion.

## 🔄 Applying Analytical Changes
If any script in `snowflake/setup/` (especially those in the `GOLD` layer like `07_mart_diesel_devaluation.sql`) is modified, it must be **re-executed manually** in the Snowflake console to update the corresponding view or table. 

This ensures the **Streamlit Dashboard** always reflects the latest business logic versioned in this repository.
