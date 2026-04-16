-- Final analytical mart for dashboarding ULEZ impact
{{ config(materialized='table') }}

WITH fct_cars AS (
    SELECT * FROM {{ ref('fct_cars') }}
),

impact_summary AS (
    SELECT
        brand,
        year,
        fuel_type,
        is_ulez_compliant,
        COUNT(*) as total_listings,
        ROUND(AVG(price), 2) as avg_price,
        ROUND(AVG(mileage), 2) as avg_mileage,
        ROUND(AVG(vehicle_age), 1) as avg_age
    FROM fct_cars
    GROUP BY 1, 2, 3, 4
),

-- Comparison: Compliant vs Non-compliant for the same brand/year/model
-- To highlight the "ULEZ Penalty"
final AS (
    SELECT
        *,
        (avg_price / NULLIF(avg_age, 0)) as price_efficiency
    FROM impact_summary
)

SELECT * FROM final
