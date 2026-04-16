-- Main model with ULEZ compliance logic
{{ config(materialized='table') }}

WITH stg_cars AS (
    SELECT * FROM {{ ref('stg_cars') }}
),

compliance_logic AS (
    SELECT
        *,
        -- ULEZ Rules: Petrol (2006+), Diesel (Sept 2015+)
        -- We simplify with Year for the portfolio
        CASE
            WHEN fuel_type = 'Petrol' AND year >= 2006 THEN TRUE
            WHEN fuel_type = 'Diesel' AND year >= 2015 THEN TRUE
            WHEN fuel_type IN ('Hybrid', 'Other', 'Electric') THEN TRUE
            ELSE FALSE
        END AS is_ulez_compliant,
        
        -- Feature Engineering
        (2025 - year) AS vehicle_age,
        CASE WHEN mileage > 0 THEN (price / mileage) ELSE 0 END AS price_per_mile
    FROM stg_cars
)

SELECT * FROM compliance_logic
