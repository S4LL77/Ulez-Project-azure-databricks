-- Unified staging model for all car brands (Priority: AutoTrader Live)
{{ config(materialized='view') }}

WITH autotrader_source AS (
    SELECT
        UPPER(SUBSTR(title, 0, INSTR(title, ' ') - 1)) as brand, -- Attempt to extract brand from title
        model,
        year,
        CAST(REPLACE(price, ',', '') AS FLOAT) as price,
        transmission,
        mileage,
        fuelType as fuel_type,
        0 as tax, -- API doesn't always provide tax
        0 as mpg,
        engineSize as engine_size
    FROM {{ source('bronze', 'autotrader_raw') }}
),

-- Previous logic for CSVs (Legacy)
unified_legacy AS (
    -- ... existing logic as fallback or historical ...
    SELECT 
        brand, model, year, price, transmission, mileage, 
        fuel_type, tax, mpg, engine_size 
    FROM autotrader_source -- Temporarily pointing legacy code to new source
)

SELECT * FROM unified_legacy
