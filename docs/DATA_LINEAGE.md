# ⛓️ Data Lineage: ULEZ Lakehouse Pipeline

This document tracks the flow of data from source extraction to final analytical consumption.

```mermaid
graph TD
    subgraph "External Sources"
        AT[AutoTrader GraphQL API]
    end

    subgraph "Bronze Layer"
        E1[01_ingestion/data_engine.py] --> B1[(data/bronze/*.parquet)]
    end

    subgraph "Silver Layer"
        B1 --> DB[02_processing/databricks_pipeline.py]
        DB --> S1[(data/silver/fct_cars.parquet)]
    end

    subgraph "Gold Layer"
        S1 --> G1[(data/gold/mart_market_impact.parquet)]
        S1 --> G2[(data/gold/mart_diesel_devaluation.parquet)]
        S1 --> ML[02_processing/ml_clustering.py]
        ML --> G3[(data/gold/mart_market_clusters.parquet)]
    end

    subgraph "Consumption"
        G1 --> APP[04_visualization/app.py]
        G2 --> APP
        G3 --> APP
    end

    AT --> E1

    style B1 fill:#CD7F32,color:#fff
    style S1 fill:#C0C0C0,color:#fff
    style G1 fill:#FFD700,color:#fff
    style G2 fill:#FFD700,color:#fff
    style G3 fill:#FFD700,color:#fff
```

| Folder | Component | Logic Summary | Engine |
| :--- | :--- | :--- | :--- |
| **01_ingestion** | `data_engine.py` | API fetching, saves raw Parquet to Bronze layer. | Python + Pandas |
| **02_processing** | `databricks_pipeline.py` | ULEZ compliance mapping, deduplication, cleaning. | DuckDB SQL |
| **02_processing** | `ml_clustering.py` | K-Means market segmentation with silhouette evaluation. | Scikit-Learn |
| **04_visualization** | `app.py` | Market impact dashboard. | Streamlit + Plotly |
| **05_quality** | `quality_checks.py` | Automated QA: PK integrity, price accuracy, ULEZ logic. | DuckDB SQL |
