-- Setup database for ULEZ project
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS ULEZ_DB;

USE DATABASE ULEZ_DB;

CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- Metadata comments
COMMENT ON DATABASE ULEZ_DB IS 'Main database for London ULEZ Car Price Impact Analysis';
COMMENT ON SCHEMA BRONZE IS 'Raw, untouched data from CSV/Kaggle ingestion';
COMMENT ON SCHEMA SILVER IS 'Cleaned, standardized data handled by dbt';
COMMENT ON SCHEMA GOLD IS 'Analytical tables ready for ML and Business Insight';
