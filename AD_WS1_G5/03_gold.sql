USE ROLE WS1_G5;
USE WAREHOUSE group5_Wrk1;
USE DATABASE WS1_G5_DB;
USE SCHEMA gold;


-- create dimension tables

CREATE OR REPLACE TABLE dim_calendar (
    date_key VARCHAR PRIMARY KEY,
    full_date DATE,
    day_of_week INTEGER,
    week_of_year INTEGER,
    date_month INTEGER,
    date_year INTEGER,
    is_weekend BOOLEAN,
    month_name VARCHAR,
    day_name VARCHAR
);

CREATE OR REPLACE TABLE dim_campaign (
    campaign_id      VARCHAR PRIMARY KEY,
    campaign_name    VARCHAR,
    launch_date_key  INTEGER,
    budget           NUMBER(18,2),
    channel          VARCHAR,
    status           VARCHAR
);
 
CREATE OR REPLACE TABLE dim_product (
  product_id VARCHAR PRIMARY KEY,
  product_type VARCHAR,
  product_name VARCHAR
);

CREATE OR REPLACE TABLE dim_customer (
  customer_id VARCHAR PRIMARY KEY,
  customer_name  VARCHAR,
  total_account_balance DECIMAL(12,2),
  total_liabilities DECIMAL(12,2),
  preferred_channel VARCHAR,
  date_of_birth_key VARCHAR,
  city VARCHAR,
  gender VARCHAR,
  income DECIMAL(12,2),
  join_date_key VARCHAR,
  marital_status VARCHAR
);


-- create fact tables

CREATE OR REPLACE TABLE fact_campaign_interaction (
    campaign_id VARCHAR,
    customer_id VARCHAR(10),
    sent_date_key INTEGER,
    click_date_key INTEGER,
    promotion_type VARCHAR(50),
    click_status VARCHAR(5)
);

CREATE OR REPLACE TABLE fact_product_acquisition (
    product_id VARCHAR(10),
    customer_id VARCHAR(10),
    acquisition_date_key INTEGER
);



-- load data from silver layer into dimension tables

-- dim_calendar

TRUNCATE TABLE dim_calendar;

-- insert a row for each day since the earlies date of birth until in 1 year
INSERT INTO dim_calendar (
    date_key,
    full_date,
    day_of_week,
    week_of_year,
    date_month,
    date_year,
    is_weekend,
    month_name,
    day_name
)
-- use min date of birth as min date and date in 1 year as max date
WITH date_bounds AS (
    SELECT
        MIN(date_of_birth) AS min_date,
        DATEADD(year, 1, CURRENT_DATE) AS max_date
    FROM silver.customer_products
),
-- get all days between min and max date
days AS (
    SELECT
        db.min_date,
        SEQ.VALUE AS day_offset
    FROM date_bounds db,
         LATERAL FLATTEN(input => ARRAY_GENERATE_RANGE(0, DATEDIFF(day, db.min_date, db.max_date) + 1)) AS SEQ
)
SELECT
    TO_CHAR(DATEADD(day, day_offset, min_date), 'YYYYMMDD') AS date_key,
    DATEADD(day, day_offset, min_date) AS full_date,
    DAYOFWEEK(DATEADD(day, day_offset, min_date)) AS day_of_week,
    WEEKOFYEAR(DATEADD(day, day_offset, min_date)) AS week_of_year,
    MONTH(DATEADD(day, day_offset, min_date)) AS date_month,
    YEAR(DATEADD(day, day_offset, min_date)) AS date_year,
    CASE WHEN DAYOFWEEK(DATEADD(day, day_offset, min_date)) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
    DECODE(
    DAYOFWEEK(DATEADD(day, day_offset, min_date)),
        0, 'Sunday',
        1, 'Monday',
        2, 'Tuesday',
        3, 'Wednesday',
        4, 'Thursday',
        5, 'Friday',
        6, 'Saturday'
    ) AS day_name,
    TO_CHAR(DATEADD(day, day_offset, min_date), 'MMMM') AS month_name
FROM days;


-- dim_campaign

TRUNCATE TABLE dim_campaign;

INSERT INTO dim_campaign (
    campaign_id,
    campaign_name,
    launch_date_key,
    budget,
    channel,
    status
)
SELECT
    campaign_id,
    name AS campaign_name,
    TO_CHAR(launch_date, 'YYYYMMDD') AS launch_date_key,
    budget,
    INITCAP(LOWER(channel)) AS channel,
    INITCAP(LOWER(status)) AS status
FROM silver.campaigns;


-- dim_product

TRUNCATE TABLE dim_product;

INSERT INTO dim_product (
  product_id,
  product_type,
  product_name
)
WITH product_agg AS (
  SELECT
    INITCAP(LOWER(TRIM(product_id))) AS product_id,
    INITCAP(LOWER(TRIM(product_type))) AS product_type,
    INITCAP(TRIM(product_name)) AS product_name
  FROM silver.customer_products
  GROUP BY
    INITCAP(LOWER(TRIM(product_id))),
    INITCAP(LOWER(TRIM(product_type))),
    INITCAP(TRIM(product_name))
)
SELECT
  product_agg.product_id,
  product_agg.product_type,
  product_agg.product_name
FROM product_agg;


-- dim_customer

TRUNCATE TABLE dim_customer;

INSERT INTO dim_customer (
  customer_id,
  customer_name,
  total_account_balance,
  total_liabilities,
  preferred_channel,
  date_of_birth_key,
  city,
  gender,
  income,
  join_date_key,
  marital_status
)
WITH customer_agg AS (
  SELECT
    INITCAP(LOWER(TRIM(customer_id)))                AS customer_id,
    MAX(INITCAP(TRIM(customer_name)))                AS customer_name,
    ROUND(SUM(total_account_balance), 2)             AS total_account_balance,
    ROUND(SUM(total_liabilities), 2)                 AS total_liabilities,
    MAX(INITCAP(LOWER(TRIM(preferred_channel))))     AS preferred_channel,
    MAX(date_of_birth)                               AS date_of_birth,
    MAX(INITCAP(LOWER(TRIM(city))))                  AS city,
    MAX(INITCAP(LOWER(TRIM(gender))))                AS gender,
    ROUND(AVG(income), 2)                            AS income,
    MAX(join_date)                                   AS join_date,
    MAX(INITCAP(LOWER(TRIM(marital_status))))        AS marital_status
  FROM silver.customer_products
  GROUP BY LOWER(TRIM(customer_id))
)
SELECT
  customer_agg.customer_id,
  customer_agg.customer_name,
  customer_agg.total_account_balance,
  customer_agg.total_liabilities,
  customer_agg.preferred_channel,
  TO_CHAR(customer_agg.date_of_birth, 'YYYYMMDD') AS date_of_birth_key,
  customer_agg.city,
  customer_agg.gender,
  customer_agg.income,
  TO_CHAR(customer_agg.join_date, 'YYYYMMDD')     AS join_date_key,
  customer_agg.marital_status
FROM customer_agg
ORDER BY customer_id;


-- load data from silver layer into fact tables

-- fact_campaign_interaction

TRUNCATE TABLE fact_campaign_interaction;

INSERT INTO fact_campaign_interaction (
    campaign_id,
    customer_id,
    sent_date_key,
    click_date_key,
    promotion_type,
    click_status
)
SELECT
    ci.campaign_id,
    ci.customer_id,
    dcs.date_key AS sent_date_key,
    dcc.date_key AS click_date_key,
    INITCAP(LOWER(TRIM(ci.promotion_type))) AS promotion_type,
    INITCAP(LOWER(TRIM(ci.click_status))) AS click_status
FROM silver.campaign_interactions ci
LEFT JOIN dim_calendar dcs
    ON TO_CHAR(ci.sent_date, 'YYYYMMDD') = dcs.date_key
LEFT JOIN dim_calendar dcc
    ON TO_CHAR(ci.click_date, 'YYYYMMDD') = dcc.date_key;


-- fact_product_acquisition

TRUNCATE TABLE fact_product_acquisition;

INSERT INTO fact_product_acquisition (
    product_id,
    customer_id,
    acquisition_date_key
)
SELECT
    INITCAP(LOWER(TRIM(cp.product_id))) AS product_id,
    INITCAP(LOWER(TRIM(cp.customer_id))) AS customer_id,
    dcl.date_key AS acquisition_date_key
FROM silver.customer_products cp
LEFT JOIN dim_calendar dcl
    ON TO_CHAR(cp.acquisition_date, 'YYYYMMDD') = dcl.date_key;

SELECT * FROM DIM_CALENDAR;

