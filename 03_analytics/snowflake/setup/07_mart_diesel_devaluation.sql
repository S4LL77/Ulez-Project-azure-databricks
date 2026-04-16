-- Top 30 Diesel Devaluation (Gold Layer)
USE DATABASE ULEZ_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE VIEW MART_DIESEL_TOP_10_DEVALUATION AS
WITH diesel_stats AS (
    SELECT 
        brand,
        model,
        AVG(CASE WHEN is_ulez_compliant = TRUE THEN price END) as avg_price_compliant,
        AVG(CASE WHEN is_ulez_compliant = FALSE THEN price END) as avg_price_non_compliant,
        COUNT(*) as total_listings
    FROM SILVER.FCT_CARS
    WHERE fuel_type = 'Diesel'
    GROUP BY 1, 2
    HAVING COUNT(CASE WHEN is_ulez_compliant = TRUE THEN 1 END) >= 1 
       AND COUNT(CASE WHEN is_ulez_compliant = FALSE THEN 1 END) >= 1
)
SELECT 
    brand,
    model,
    ROUND(avg_price_compliant, 0) as avg_price_compliant,
    ROUND(avg_price_non_compliant, 0) as avg_price_non_compliant,
    ROUND(avg_price_compliant - avg_price_non_compliant, 0) as devaluation_amount,
    ROUND(((avg_price_non_compliant - avg_price_compliant) / NULLIF(avg_price_compliant, 0)) * 100, 1) as devaluation_percent
FROM diesel_stats
ORDER BY devaluation_percent ASC
LIMIT 10;
