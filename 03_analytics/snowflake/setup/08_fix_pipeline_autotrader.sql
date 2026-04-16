-- Fix for AutoTrader Pipeline
USE DATABASE ULEZ_DB;
USE SCHEMA BRONZE;

-- Ensure the brand column exists in the existing table
ALTER TABLE AUTOTRADER_RAW ADD COLUMN IF NOT EXISTS brand VARCHAR;

-- 1. Create a STREAM on the new AutoTrader table
CREATE OR REPLACE STREAM autotrader_stream ON TABLE AUTOTRADER_RAW;

-- 2. Create a TASK to process the AutoTrader stream into SILVER layer
-- Using the same logic as the previous task but for the structured API data
CREATE OR REPLACE TASK autotrader_transform_task
  WAREHOUSE = 'ULEZ_WH'
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('autotrader_stream')
AS
  INSERT INTO SILVER.FCT_CARS (
    brand, model, year, price, transmission, mileage, 
    fuel_type, engine_size, is_ulez_compliant, 
    vehicle_age, price_per_mile
  )
  SELECT 
    brand, 
    -- We take the title as model for now as it's the most descriptive field from API search results
    title as model, 
    year, 
    TRY_CAST(REPLACE(REPLACE(price, '£', ''), ',', '') AS INTEGER), 
    transmission, 
    mileage, 
    fuelType, 
    engineSize,
    CASE 
      WHEN fuelType = 'Petrol' AND year >= 2006 THEN TRUE
      WHEN fuelType = 'Diesel' AND year >= 2015 THEN TRUE
      WHEN fuelType IN ('Hybrid', 'Electric', 'Other') THEN TRUE
      ELSE FALSE
    END as is_ulez_compliant,
    (2025 - year) as vehicle_age,
    (TRY_CAST(REPLACE(REPLACE(price, '£', ''), ',', '') AS INTEGER) / NULLIF(mileage, 0)) as price_per_mile
  FROM autotrader_stream
  WHERE METADATA$ACTION = 'INSERT';

-- 3. Resume the task
ALTER TASK autotrader_transform_task RESUME;

-- 4. MANUAL MIGRATION: Migrate any existing data in AUTOTRADER_RAW right now
INSERT INTO SILVER.FCT_CARS (
    brand, model, year, price, transmission, mileage, 
    fuel_type, engine_size, is_ulez_compliant, 
    vehicle_age, price_per_mile
)
SELECT 
    brand, title, year, 
    TRY_CAST(REPLACE(REPLACE(price, '£', ''), ',', '') AS INTEGER), 
    transmission, mileage, fuelType, engineSize,
    CASE 
      WHEN fuelType = 'Petrol' AND year >= 2006 THEN TRUE
      WHEN fuelType = 'Diesel' AND year >= 2015 THEN TRUE
      WHEN fuelType IN ('Hybrid', 'Electric', 'Other') THEN TRUE
      ELSE FALSE
    END as is_ulez_compliant,
    (2025 - year) as vehicle_age,
    (TRY_CAST(REPLACE(REPLACE(price, '£', ''), ',', '') AS INTEGER) / NULLIF(mileage, 0)) as price_per_mile
FROM AUTOTRADER_RAW
WHERE NOT EXISTS (SELECT 1 FROM SILVER.FCT_CARS WHERE SILVER.FCT_CARS.model = AUTOTRADER_RAW.title);