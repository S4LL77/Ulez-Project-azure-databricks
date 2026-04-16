-- Snowflake Streaming (Change Data Capture) Setup
-- Using Streams and Tasks for cost-effective ingestion

USE DATABASE ULEZ_DB;
USE SCHEMA BRONZE;

-- 0. Ensure the raw table exists before creating a stream on it
CREATE TABLE IF NOT EXISTS MERC_RAW (
    model VARCHAR,
    year INTEGER,
    price INTEGER,
    transmission VARCHAR,
    mileage INTEGER,
    fuelType VARCHAR,
    tax INTEGER,
    mpg FLOAT,
    engineSize FLOAT
);

-- 1. Create a STREAM on the raw table
CREATE OR REPLACE STREAM mercedes_stream ON TABLE MERC_RAW;

-- 2. Create a TASK to process the stream
-- The task runs every 1 minute BUT only consumes credits
-- IF the stream has new data (CONDITION).
CREATE OR REPLACE TASK ulez_transform_task
  WAREHOUSE = 'ULEZ_WH'
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('mercedes_stream')
AS
  -- This replaces the dbt 'run' momentarily for true CDC simulation
  -- In a production environment, you'd use dbt on top of streams
  -- or Snowflake Dynamic Tables.
  INSERT INTO SILVER.FCT_CARS (
    brand, model, year, price, transmission, mileage, 
    fuel_type, tax, mpg, engine_size, is_ulez_compliant, 
    vehicle_age, price_per_mile
  )
  SELECT 
    'MERCEDES', model, year, price, transmission, mileage, 
    fuelType, tax, mpg, engineSize,
    -- ULEZ logic replicated here for streaming demonstration
    CASE 
      WHEN fuelType = 'Petrol' AND year >= 2006 THEN TRUE
      WHEN fuelType = 'Diesel' AND year >= 2015 THEN TRUE
      WHEN fuelType IN ('Hybrid', 'Electric', 'Other') THEN TRUE
      ELSE FALSE
    END as is_ulez_compliant,
    (2025 - year) as vehicle_age,
    (price / NULLIF(mileage, 0)) as price_per_mile
  FROM mercedes_stream
  WHERE METADATA$ACTION = 'INSERT';

-- 3. Resume the task (Tasks are created in 'SUSPENDED' state)
ALTER TASK ulez_transform_task RESUME;
