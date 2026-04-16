-- ==============================================================================
-- 09_ml_orchestration_setup.sql
-- Description: Enterprise orchestration setup for Machine Learning pipelines.
-- Shows how a Data Engineer productionizes the local Python clustering script.
-- ==============================================================================

USE DATABASE ULEZ_ANALYTICS;
USE SCHEMA GOLD;

-- ==============================================================================
-- OPTION 1: Serverless Orchestration (Snowflake Tasks + External Functions or Airflow)
-- ==============================================================================
-- In a standard Modern Data Stack, an external orchestrator (like Apache Airflow,
-- Prefect, or Dagster) would trigger the scripts/ml_clustering.py script 
-- immediately after the dbt transformation layer finishes successfully.

-- However, if we want to orchestrate it directly from Snowflake using purely cloud
-- compute (Snowpark), we would wrap our K-Means logic into a Stored Procedure:

CREATE OR REPLACE PROCEDURE SP_MARKET_CLUSTERING()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  PACKAGES = ('snowflake-snowpark-python', 'pandas', 'scikit-learn')
  HANDLER = 'run_clustering'
AS
$$
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

def run_clustering(session):
    # 1. Read from Silver Layer
    df = session.sql("""
        SELECT brand, model, year, price, mileage, is_ulez_compliant 
        FROM ULEZ_ANALYTICS.SILVER.FCT_CARS 
        WHERE price IS NOT NULL AND mileage IS NOT NULL AND year IS NOT NULL
    """).to_pandas()
    
    if df.empty:
        return "No data to process"

    # 2. Feature Engineering & Scaling
    features = ['PRICE', 'MILEAGE', 'YEAR']
    X = df[features].copy()
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # 3. Model Training
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    df['CLUSTER_ID'] = kmeans.fit_predict(X_scaled)
    
    # ... logic for naming clusters would go here ...
    df['CLUSTER_NAME'] = 'Assigned by Snowpark'
    
    # 4. Write back to Gold Schema
    df_snowpark = session.create_dataframe(df)
    df_snowpark.write.mode("overwrite").save_as_table("ULEZ_ANALYTICS.GOLD.MART_MARKET_CLUSTERS")
    
    return "SUCCESS: Clustered Market Data Updated"
$$;

-- ==============================================================================
-- STEP 2: Automating the Execution (Snowflake Task)
-- ==============================================================================
-- This task runs the Machine Learning model automatically every day at 3:00 AM,
-- ensuring the Streamlit dashboard always reads fresh profiles.

CREATE OR REPLACE TASK ML_CLUSTERING_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 3 * * * UTC'
AS
  CALL SP_MARKET_CLUSTERING();

-- In production, the DE must resume the task to make it active:
-- ALTER TASK ML_CLUSTERING_TASK RESUME;

-- NOTE FOR PORTFOLIO: 
-- You don't need to run this SQL. The Python script (scripts/ml_clustering.py) 
-- does exactly this but runs locally, which is perfect for demonstration without 
-- incurring Snowflake Stored Procedure compute costs.
