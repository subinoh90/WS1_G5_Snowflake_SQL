-- STEP 1: Set up context for loading

-- Use team role with access to shared warehouse and database
USE ROLE WS1_G5;

-- Choose the warehouse created by TRAINING_ROLE
USE WAREHOUSE group5_Wrk1;

-- Select the shared team database and Bronze schema
USE DATABASE WS1_G5_DB;
USE SCHEMA BRONZE;

-- Verify context
SELECT 
  CURRENT_ROLE()        AS ROLE,
  CURRENT_WAREHOUSE()   AS WAREHOUSE,
  CURRENT_DATABASE()    AS DATABASE,
  CURRENT_SCHEMA()      AS SCHEMA;

-- STEP 2: Create a stage for file uploads

-- If the stage does not exist yet, create it once
CREATE OR REPLACE STAGE BRONZE_STAGE;

-- Upload CSV files to BRONZE_STAGE 

LIST @BRONZE_STAGE;


-- STEP 3: Create file format definition

CREATE OR REPLACE FILE FORMAT BRONZE.CSV_FMT
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL')
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- STEP 4: Create raw tables

CREATE OR REPLACE TABLE BRONZE.CUSTOMER_PRODUCTS (
  ProductID STRING,
  ProductType STRING,
  ProductName STRING,
  AcquisitionDate STRING,
  CustomerID STRING,
  CustomerName STRING,
  TotalAccountBalance STRING,
  TotalLiabilities STRING,
  PreferredChannel STRING,
  dob STRING,
  city STRING,
  gender STRING,
  income STRING,
  joinDate STRING,
  maritalStatus STRING
);

CREATE OR REPLACE TABLE BRONZE.CAMPAIGNS (
  CampaignID STRING,
  Name STRING,
  LaunchDate STRING,
  Budget STRING,
  Channel STRING,
  Status STRING
);

CREATE OR REPLACE TABLE BRONZE.CAMPAIGN_INTERACTIONS (
  Customer_ID STRING,
  Campaign_ID STRING,
  Promotion_Type STRING,
  Sent_Date STRING,
  Click_Status STRING,
  Click_Date STRING
);

-- STEP 5: Load CSV files into Bronze tables

-- CustomerProducts
COPY INTO BRONZE.CUSTOMER_PRODUCTS
FROM @BRONZE_STAGE/CustomerProducts_raw.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.CSV_FMT)
ON_ERROR = CONTINUE;

-- Campaigns
COPY INTO BRONZE.CAMPAIGNS
FROM @BRONZE_STAGE/Campaigns_raw.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.CSV_FMT)
ON_ERROR = CONTINUE;

-- CampaignInteractions
COPY INTO BRONZE.CAMPAIGN_INTERACTIONS
FROM @BRONZE_STAGE/CampaignInteractions_raw.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.CSV_FMT)
ON_ERROR = CONTINUE;

SELECT * FROM BRONZE.CUSTOMER_PRODUCTS LIMIT 5;



-- STEP 6: Verify data load

SELECT 'CUSTOMER_PRODUCTS' AS table_name, COUNT(*) AS row_count
FROM BRONZE.CUSTOMER_PRODUCTS
UNION ALL
SELECT 'CAMPAIGNS', COUNT(*) 
FROM BRONZE.CAMPAIGNS
UNION ALL
SELECT 'CAMPAIGN_INTERACTIONS', COUNT(*) 
FROM BRONZE.CAMPAIGN_INTERACTIONS;


-- 2. Check column structure 

-- Show column names, types, and their order
SHOW COLUMNS IN TABLE BRONZE.CUSTOMER_PRODUCTS;
SHOW COLUMNS IN TABLE BRONZE.CAMPAIGNS;
SHOW COLUMNS IN TABLE BRONZE.CAMPAIGN_INTERACTIONS;
