-- ==========================================================
-- GOVERNANCE & SECURITY: MEDALLION ACCESS CONTROL
-- ==========================================================
-- Purpose: Implementation of Data Masking and Row-Level Security
-- Targeted for: Financial Services Compliance / Audit Standards
-- ==========================================================

USE ROLE SECURITYADMIN;
USE DATABASE ULEZ_DB;

-- 1. Create a Restricted Audit Role
CREATE ROLE IF NOT EXISTS ULEZ_AUDITOR;
GRANT ROLE ULEZ_AUDITOR TO ROLE ULEZ_ADMIN;

-- 2. Define Masking Policies for Sensitive Data (Price & PII)
-- Even non-PII data like exact Price can be sensitive in some contexts.
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS GOVERNANCE;
USE SCHEMA GOVERNANCE;

CREATE OR REPLACE MASKING POLICY price_mask AS (val NUMBER) RETURNS NUMBER ->
  CASE
    WHEN CURRENT_ROLE() IN ('ULEZ_ADMIN', 'SYSADMIN') THEN val
    ELSE -999.99 -- Mask price for non-admin users
  END;

CREATE OR REPLACE MASKING POLICY text_mask AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('ULEZ_ADMIN', 'SYSADMIN', 'ULEZ_DEVELOPER') THEN val
    ELSE '*********' -- Obfuscate text (e.g., registration or specific titles)
  END;

-- 3. Apply Masking Policies to Gold Layer
-- We apply governance at the Gold (Analytics) layer where users consume data.
USE ROLE ACCOUNTADMIN; -- Need high privilege to apply masking if not owner
ALTER TABLE GOLD.DIESEL_DEVALUATION MODIFY COLUMN listing_price SET MASKING POLICY GOVERNANCE.price_mask;
ALTER TABLE GOLD.DIESEL_DEVALUATION MODIFY COLUMN title SET MASKING POLICY GOVERNANCE.text_mask;

-- 4. Row Access Policy (Regional / Group Filtering)
-- Example: Restrict data based on some attribute (e.g., only show specific Brands to some roles)
CREATE OR REPLACE ROW ACCESS POLICY brand_policy AS (brand_name VARCHAR) RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() IN ('ULEZ_ADMIN', 'SYSADMIN') THEN TRUE
    WHEN CURRENT_ROLE() = 'ULEZ_DEVELOPER' AND brand_name IN ('BMW', 'AUDI') THEN TRUE
    ELSE FALSE
  END;

ALTER TABLE GOLD.DIESEL_DEVALUATION ADD ROW ACCESS POLICY brand_policy ON (brand);

-- 5. Audit Logging Setup (Simulation)
-- Create a schema for audit logs
CREATE SCHEMA IF NOT EXISTS AUDIT;
CREATE TABLE IF NOT EXISTS AUDIT.ACCESS_LOGS (
    event_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    user_name VARCHAR,
    role_name VARCHAR,
    query_text VARCHAR
);

-- Note: In a real enterprise setup, we would use Snowflake's ACCESS_HISTORY view
-- but creating a dedicated table shows intent for auditability.

PRINT 'GOVERNANCE: Security and Audit layers successfully configured.';
