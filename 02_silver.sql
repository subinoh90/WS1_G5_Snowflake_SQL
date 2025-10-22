-- 1) Set context
-- use the team role
USE ROLE WS1_G5;

-- Use the shared warehouse we just created
USE WAREHOUSE group5_Wrk1;

-- Use the team database and schema
USE DATABASE WS1_G5_DB;

-- use silver schema
USE SCHEMA silver;

-- 2) Create Tables 
-- These are empty shells with defined data types
-- Ensure that all attribute names follow a consistent naming convention
CREATE OR REPLACE TABLE customer_products (
    product_id VARCHAR,
    product_type VARCHAR,
    product_name VARCHAR,
    acquisition_date DATE,
    customer_id VARCHAR,
    customer_name VARCHAR,
    total_account_balance DECIMAL(12,2),
    total_liabilities DECIMAL(12,2),
    preferred_channel VARCHAR,
    date_of_birth DATE,
    city VARCHAR,
    gender VARCHAR,
    income DECIMAL(12,2),
    join_date DATE,
    marital_status VARCHAR
);

CREATE OR REPLACE TABLE campaign_interactions (
    customer_id VARCHAR,
    campaign_id VARCHAR,
    promotion_type VARCHAR,
    sent_date DATE,
    click_status VARCHAR,
    click_date DATE
);

CREATE OR REPLACE TABLE campaigns (
    campaign_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    launch_date DATE,
    budget DECIMAL(12,2),
    channel VARCHAR,
    status VARCHAR
);


-- 3) INSERT de-duplicate data from bronze layer into silver layer tables
-- Apply data standardization and cleansing
-- Ensure consistent formatting, handle type conversions, and normalize text values
-- Improve data quality for downstream analysis
INSERT INTO customer_products
SELECT 
    TRIM(ProductID), 
    LOWER(TRIM(productType)) AS productType,
    LOWER(TRIM(productName)) AS productName,
    COALESCE(
        TRY_TO_DATE(acquisitionDate, 'YYYY-MM-DD'),
        TRY_TO_DATE(acquisitionDate, 'YYYY/MM/DD'),
        TRY_TO_DATE(acquisitionDate, 'DD/MM/YYYY'),
        TRY_TO_DATE(acquisitionDate, 'DD-MM-YYYY'),
        TRY_TO_DATE(acquisitionDate, 'MM/DD/YYYY HH24:MI')
    ) AS acquisitionDate, 
    customerId,
    TRIM(customerName) AS customerName, 
    TRY_TO_NUMBER(REPLACE(totalAccountBalance, ',', '')) AS totalAccountBalance,
    TRY_TO_NUMBER(REPLACE(totalLiabilities, ',', '')) AS totalLiabilities,
    LOWER(TRIM(preferredChannel)) AS preferredChannel, 
    COALESCE(
        TRY_TO_DATE(dob, 'YYYY-MM-DD'),
        TRY_TO_DATE(dob, 'YYYY/MM/DD'),
        TRY_TO_DATE(dob, 'DD/MM/YYYY'),
        TRY_TO_DATE(dob, 'DD-MM-YYYY'),
        TRY_TO_DATE(dob, 'MM/DD/YYYY HH24:MI')
    ) AS dob, 
    LOWER(TRIM(city)) AS city, 
    LOWER(TRIM(gender)) AS gender, 
    TRY_TO_NUMBER(REPLACE(income, ',', '')) AS income, 
    COALESCE(
        TRY_TO_DATE(joinDate, 'YYYY-MM-DD'),
        TRY_TO_DATE(joinDate, 'YYYY/MM/DD'),
        TRY_TO_DATE(joinDate, 'DD/MM/YYYY'),
        TRY_TO_DATE(joinDate, 'DD-MM-YYYY'),
        TRY_TO_DATE(joinDate, 'MM/DD/YYYY HH24:MI')
    ) AS join_date, 
    LOWER(TRIM(maritalStatus)) AS maritalStatus
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY CustomerID, ProductID
               ORDER BY AcquisitionDate DESC
           ) AS rn
    FROM bronze.customer_products
) t
WHERE rn = 1;

INSERT INTO campaign_interactions
SELECT 
    TRIM(customer_id),
    UPPER(TRIM(campaign_id)),
    LOWER(TRIM(promotion_type)) AS promotion_type,
    TRY_TO_DATE(sent_date) AS sent_date,
    LOWER(TRIM(click_status)) AS click_status,
    TRY_TO_DATE(click_date) AS click_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id, campaign_id
               ORDER BY sent_date DESC
           ) AS rn
    FROM bronze.campaign_interactions
) t
WHERE rn = 1;

INSERT INTO campaigns
SELECT 
    campaignID,
    TRIM(name) AS name,
    TRY_TO_DATE(launchDate) AS launch_date,
    TRY_TO_NUMBER(REPLACE(budget, ',', '')) AS budget,
    LOWER(TRIM(channel)) AS channel,
    LOWER(TRIM(status)) AS status
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY campaignID
               ORDER BY launchDate DESC
           ) AS rn
    FROM bronze.campaigns
) t
WHERE rn = 1;

-- 4) Clean data in customer_products table
-- See the visualization in Snowflake
SELECT * FROM customer_products;

-- Remove the rows that product ID missing or error
DELETE FROM customer_products
WHERE product_id IS NULL
   OR LOWER(product_id) IN ('undefined', 'nan', 'error')
   OR TRY_TO_NUMBER(product_id) IS NOT NULL;

-- Remove existing columns with missing or invalid data
-- These columns contain inconsistent or unreliable values, so we drop them to ensure data integrity
ALTER TABLE customer_products DROP COLUMN product_type, product_name;

-- Add new columns to replace the dropped ones
ALTER TABLE customer_products
ADD COLUMN product_type VARCHAR, product_name VARCHAR;

-- Populate the new columns using CASE logic based on product_id
-- This ensures each product_id is mapped to its correct type and name, restoring consistency
UPDATE customer_products
SET product_type = CASE product_id
    WHEN 'P001' THEN 'term deposit'
    WHEN 'P002' THEN 'savings account'
    WHEN 'P003' THEN 'term credit card'
    WHEN 'P004' THEN 'term home loan'
    WHEN 'P005' THEN 'term premium account'
END,
product_name = CASE product_id
    WHEN 'P001' THEN 'saver premium'
    WHEN 'P002' THEN 'saver basic'
    WHEN 'P003' THEN 'rewards plus'
    WHEN 'P004' THEN 'variable home loan'
    WHEN 'P005' THEN 'membership premium'
END;

-- Update customer_id to NULL if it doesn't match the pattern 'C' followed by exactly three digits
UPDATE customer_products
SET customer_id = NULL
WHERE customer_id IS NOT NULL
  AND customer_id NOT RLIKE '^C[0-9]{3}$';

-- Update rows with NULL customer_id by matching on customer_name and date_of_birth
UPDATE customer_products t1
SET t1.customer_id = t2.customer_id
FROM customer_products t2
WHERE t1.customer_id IS NULL
AND UPPER(t1.customer_name) = UPPER(t2.customer_name)
AND t1.date_of_birth = t2.date_of_birth
AND t2.customer_id IS NOT NULL;

-- Remove rows that still have NULL customer_id after imputation
DELETE FROM customer_products
WHERE customer_id IS NULL;

-- Replace known invalid customer's infomation values with NULL to improve data quality
-- Create a reference table
CREATE OR REPLACE TEMP TABLE invalid_values (value STRING);
INSERT INTO invalid_values VALUES
  ('nan'), ('999999999'), ('-9999'), ('???'), ('undefined'), ('NaN'), ('unknown'), ('other'), ('error');

-- Use it to update values
UPDATE customer_products
SET customer_name = NULL
WHERE LOWER(customer_name) IN (SELECT value FROM invalid_values);
UPDATE customer_products
SET preferred_channel = NULL
WHERE LOWER(preferred_channel) IN (SELECT value FROM invalid_values);
UPDATE customer_products
SET city = NULL
WHERE LOWER(city) IN (SELECT value FROM invalid_values);
UPDATE customer_products
SET gender = NULL
WHERE LOWER(gender) IN (SELECT value FROM invalid_values);
UPDATE customer_products
SET marital_status = NULL
WHERE LOWER(marital_status) IN (SELECT value FROM invalid_values);

-- Update rows with NULL customer_name by matching on customer_id
UPDATE customer_products t1
SET t1.customer_name = t2.customer_name
FROM customer_products t2
WHERE t1.customer_name IS NULL
AND t1.customer_id = t2.customer_id
AND t2.customer_name IS NOT NULL;

-- Update rows with NULL preferred_channel by matching on customer_id
UPDATE customer_products t1
SET t1.preferred_channel = t2.preferred_channel
FROM customer_products t2
WHERE t1.preferred_channel IS NULL
AND t1.customer_id = t2.customer_id
AND t2.preferred_channel IS NOT NULL;

-- Replace NULL preferred_channel with the most frequent non-null value
UPDATE customer_products
SET preferred_channel = (
  SELECT preferred_channel
  FROM customer_products
  WHERE preferred_channel IS NOT NULL
  GROUP BY preferred_channel
  ORDER BY COUNT(*) DESC
  LIMIT 1
)
WHERE preferred_channel IS NULL;

-- Update rows with NULL city by matching on customer_id
UPDATE customer_products t1
SET t1.city = t2.city
FROM customer_products t2
WHERE t1.city IS NULL
AND t1.customer_id = t2.customer_id
AND t2.city IS NOT NULL;

-- Update rows with NULL gender by matching on customer_id
UPDATE customer_products t1
SET t1.gender = t2.gender
FROM customer_products t2
WHERE t1.gender IS NULL
AND t1.customer_id = t2.customer_id
AND t2.gender IS NOT NULL;

-- Update rows with NULL marital_status by matching on customer_id
UPDATE customer_products t1
SET t1.marital_status = t2.marital_status
FROM customer_products t2
WHERE t1.marital_status IS NULL
AND t1.customer_id = t2.customer_id
AND t2.marital_status IS NOT NULL;

-- Remove rows with invalid total_account_balance
DELETE FROM customer_products
WHERE total_account_balance = -9999;

-- Remove rows with invalid total_liabilities values
DELETE FROM customer_products
WHERE total_liabilities IN (-9999, 999999999);

SELECT * FROM customer_products;

-- 5) Clean data in campaigns table
-- See the visualization in Snowflake
SELECT * FROM campaigns;

-- Fill missing campaign data to preserve completeness for analysis and avoid loss from small sample size.
-- Update launch_date for campaign CC2025
UPDATE campaigns
SET launch_date = '2025-01-05'
WHERE campaign_id = 'CC2025';

-- Update launch_date for campaign SAV2025
UPDATE campaigns
SET launch_date = '2024-12-25'
WHERE campaign_id = 'SAV2025';

-- Update budget and channel for campaign UPG2025
UPDATE campaigns
SET budget = 57839.00,
    channel = 'app, sms'
WHERE campaign_id = 'UPG2025';

-- 6) Clean data in campaign_interactions table
-- See the visualization in Snowflake
SELECT * FROM campaign_interactions;

-- Replace known invalid values with NULL to improve data quality
UPDATE campaign_interactions
SET customer_id = NULL
WHERE LOWER(customer_id) IN (SELECT value FROM invalid_values);
UPDATE campaign_interactions
SET campaign_id = NULL
WHERE LOWER(campaign_id) IN (SELECT value FROM invalid_values);
UPDATE campaign_interactions
SET promotion_type = NULL
WHERE LOWER(promotion_type) IN (SELECT value FROM invalid_values);
UPDATE campaign_interactions
SET click_status = NULL
WHERE LOWER(click_status) IN (SELECT value FROM invalid_values);

-- Remove rows with missing customer_id or campaign_id
DELETE FROM campaign_interactions
WHERE customer_id IS NULL
   OR campaign_id IS NULL;

-- Update NULL promotion_type using the most common value per campaign_id
UPDATE campaign_interactions AS ci
SET promotion_type = mode.promotion_type
FROM (
  SELECT campaign_id, promotion_type
  FROM (
    SELECT campaign_id, promotion_type,
           COUNT(*) AS freq,
           ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY COUNT(*) DESC) AS rn
    FROM campaign_interactions
    WHERE promotion_type IS NOT NULL
    GROUP BY campaign_id, promotion_type
  )
  WHERE rn = 1
) AS mode
WHERE ci.promotion_type IS NULL
  AND ci.campaign_id = mode.campaign_id;
