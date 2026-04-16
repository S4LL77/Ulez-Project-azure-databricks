-- ⛓️ Snowflake Gold Bridge: External Tables Setup
-- This script creates the integration between Snowflake and the Delta Lake Silver/Gold layers.

-- 1. Create a Storage Integration (Azure ADLS example)
-- Requires Azure Storage Account details
CREATE STORAGE INTEGRATION IF NOT EXISTS azure_lake_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('azure://seu_account.blob.core.windows.net/silver/');

-- 2. Create an External Stage
CREATE STAGE IF NOT EXISTS silver_lake_stage
  URL = 'azure://seu_account.blob.core.windows.net/silver/'
  STORAGE_INTEGRATION = azure_lake_integration;

-- 3. Create External Table (Gold View)
-- This allows Snowflake users to query Databricks-processed data without moving it.
CREATE OR REPLACE EXTERNAL TABLE GOLD.ULEZ_MARKET_IMPACT (
    advert_id VARCHAR AS (VALUE:id::VARCHAR),
    brand VARCHAR AS (VALUE:brand::VARCHAR),
    fuel_type VARCHAR AS (VALUE:fuel_type::VARCHAR),
    year INTEGER AS (VALUE:year::INTEGER),
    is_ulez_compliant BOOLEAN AS (VALUE:is_ulez_compliant::BOOLEAN),
    processed_at TIMESTAMP AS (VALUE:processed_at::TIMESTAMP)
)
WITH LOCATION = @silver_lake_stage
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

-- 4. Governance: Masking & Roles
GRANT SELECT ON EXTERNAL TABLE GOLD.ULEZ_MARKET_IMPACT TO ROLE ULEZ_AUDITOR;
