-- Update for AutoTrader API schema
USE DATABASE ULEZ_DB;
USE SCHEMA BRONZE;

-- Create table to hold the structured JSON output from AutoTrader
CREATE TABLE IF NOT EXISTS AUTOTRADER_RAW (
    id VARCHAR,
    brand VARCHAR,
    title VARCHAR,
    price VARCHAR, -- Some prices may have text (e.g. £12,000)
    year INTEGER,
    mileage INTEGER,
    fuelType VARCHAR,
    engineSize FLOAT,
    transmission VARCHAR,
    search_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
