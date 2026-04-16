# ⛓️ Data Lineage: ULEZ Databricks & Snowflake

This document tracks the flow of data from source extraction to final analytical consumption.

```mermaid
graph TD
    subgraph "External Sources"
        AT[AutoTrader GraphQL API]
    end

    subgraph "Snowflake: BRONZE"
        E1[01_ingestion/data_engine.py] --> B1[(BRONZE.AUTOTRADER_RAW)]
    end

    subgraph "Snowflake: SILVER"
        B1 --> DB[02_processing/databricks_pipeline.py]
        DB --> S1[(SILVER.STG_CARS)]
    end

    subgraph "Snowflake: GOLD"
        S1 --> DBT[03_analytics/dbt_project]
        DBT --> G1[(GOLD.MARKET_IMPACT_FACT)]
    end

    subgraph "Consumption"
        G1 --> APP[04_visualization/app.py]
        G1 --> ML[02_processing/ml_clustering.py]
    end

    style B1 fill:#CD7F32,color:#fff
    style S1 fill:#C0C0C0,color:#fff
    style G1 fill:#FFD700,color:#fff
```

| Folder | Component | Logic Summary | Engine |
| :--- | :--- | :--- | :--- |
| **01_ingestion** | `data_engine.py` | API fetching and direct Snowflake ingestion. | Python |
| **02_processing** | `databricks_pipeline.py` | ULEZ Compliance mapping and data cleaning. | PySpark (AWS) |
| **03_analytics** | `dbt models` | Final Star Schema and Star modeling. | dbt (Snowflake) |
| **04_visualization** | `app.py` | Market impact dashboard. | Streamlit |
