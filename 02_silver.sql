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
    TRIM(productName) AS productName,
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

SELECT * FROM bronze.customer_products;
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

SELECT * FROM campaign_interactions;

-- 4) Remove outlier data
-- See the visualization in Snowflake to detect the outlier and remove them
SELECT * FROM customer_products;

DELETE FROM customer_products
WHERE total_account_balance = 999999 OR
total_liabilities = 123456 OR
income = 426863;

SELECT * FROM customer_products;

-- 5) Handle missing data
-- 5.1) customer_products table
-- See the visualization in Snowflake to detect missing data
SELECT * FROM customer_products;
-- In customer_product, there are missing data in preferred_channel, city, gender, income, join_date, and marital_status, which are the customer's profile data

-- If the same customer has other rows with a known data, use that value to fill in the missing one
-- preferred_channel
UPDATE customer_products cp
SET preferred_channel = (
    SELECT MAX(preferred_channel)
    FROM customer_products
    WHERE customer_id = cp.customer_id
      AND preferred_channel IS NOT NULL
)
WHERE cp.preferred_channel IS NULL;

-- city
UPDATE customer_products cp
SET city = (
    SELECT MAX(city)
    FROM customer_products
    WHERE customer_id = cp.customer_id
      AND city IS NOT NULL
)
WHERE cp.city IS NULL;

-- gender
UPDATE customer_products cp
SET gender = (
    SELECT MAX(gender)
    FROM customer_products
    WHERE customer_id = cp.customer_id
      AND gender IS NOT NULL
)
WHERE cp.gender IS NULL;

-- income
UPDATE customer_products cp
SET income = (
    SELECT ROUND(AVG(income), 2)
    FROM customer_products
    WHERE customer_id = cp.customer_id
      AND income IS NOT NULL
)
WHERE cp.income IS NULL;

-- join_date
UPDATE customer_products cp
SET join_date = (
    SELECT MAX(join_date)
    FROM customer_products
    WHERE customer_id = cp.customer_id
      AND join_date IS NOT NULL
)
WHERE cp.join_date IS NULL;

-- marital_status
UPDATE customer_products cp
SET marital_status = (
    SELECT MAX(marital_status)
    FROM customer_products
    WHERE customer_id = cp.customer_id
      AND marital_status IS NOT NULL
)
WHERE cp.marital_status IS NULL;

-- The other missing preferred_channel are filled wiht the most common channel
UPDATE customer_products
SET preferred_channel = 'phone call'
WHERE preferred_channel IS NULL;

-- The other missing gender are filled with 'x' (not specified)
UPDATE customer_products
SET gender = 'x'
WHERE gender IS NULL;

--the other missing income are filled with average income of other rows that share the same product_type, product_name, and marital_status.
UPDATE customer_products cp
SET income = (
    SELECT ROUND(AVG(income), 2)
    FROM customer_products
    WHERE product_type = cp.product_type
      AND product_name = cp.product_name
      AND income IS NOT NULL
)
WHERE cp.income IS NULL;

-- The remaining missing values in city, join_date, and marital_status are left as NULL
-- These attributes cannot be reliably imputed using statistical methods
SELECT * FROM customer_products;

-- 5.2) campaign_interactions
-- See the visualization in Snowflake to detect missing data
SELECT * FROM campaign_interactions;
-- In campaign_interactions, there are missing data in click_status

-- Set click_status to 'yes' if click_date is present, 'no' if click_date is missing
UPDATE campaign_interactions
SET click_status = CASE
    WHEN click_date IS NOT NULL THEN 'yes'
    WHEN click_date IS NULL THEN 'no'
END;

-- 5.3) campaigns
-- See the visualization in Snowflake to detect missing data
SELECT * FROM campaigns;
-- In campaigns, there are missing data in budget, but it cannot be reliably imputed using statistical methods
-- so, leave them as NULL
