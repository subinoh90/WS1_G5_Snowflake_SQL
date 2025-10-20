/* =========================
   DASHBOARD SETUP (run once)
   ========================= */
USE ROLE WS1_G5;
USE WAREHOUSE group5_Wrk1;
USE DATABASE WS1_G5_DB;
 
CREATE SCHEMA IF NOT EXISTS GOLD_DASH;
USE SCHEMA GOLD_DASH;

--KPI 1 - Credit Card Promotion

--# of CC Acquired over Year
SELECT
ca.full_date,
cu.CITY
FROM GOLD.FACT_PRODUCT_ACQUISITION a
LEFT JOIN GOLD.DIM_PRODUCT p ON a.product_id = p.product_id
LEFT JOIN GOLD.DIM_CUSTOMER cu ON a.customer_id = cu.customer_id
LEFT JOIN GOLD.DIM_CALENDAR ca ON a.acquisition_date_key = ca.date_key
WHERE PRODUCT_TYPE ='Term Credit Card'

--YTD - CC Acquired
SELECT
count(*)
FROM GOLD.FACT_PRODUCT_ACQUISITION a
LEFT JOIN GOLD.DIM_PRODUCT p ON a.product_id = p.product_id
LEFT JOIN GOLD.DIM_CUSTOMER cu ON a.customer_id = cu.customer_id
LEFT JOIN GOLD.DIM_CALENDAR ca ON a.acquisition_date_key = ca.date_key
WHERE product_type='Term Credit Card'

--Age, Customer and Gender duration

WITH base as (
SELECT
DISTINCT 
CUSTOMER_ID,
FROM GOLD.FACT_PRODUCT_ACQUISITION a
LEFT JOIN GOLD.DIM_PRODUCT p ON a.product_id = p.product_id
WHERE PRODUCT_TYPE ='Term Credit Card'
)
SELECT
b.CUSTOMER_ID,
ROUND((DATEDIFF(year, FULL_DATE, CURRENT_DATE()) 
      - CASE 
          WHEN TO_CHAR(CURRENT_DATE(), 'MMDD') < TO_CHAR(FULL_DATE, 'MMDD') THEN 1 
          ELSE 0 
        END),0) AS AGE,
GENDER,
INCOME,
DATEDIFF(year, FULL_DATE, CURRENT_DATE()) 
      - CASE 
          WHEN TO_CHAR(CURRENT_DATE(), 'MMDD') < TO_CHAR(FULL_DATE, 'MMDD') THEN 1 
          ELSE 0 
        END AS CUSTOMER_DURATION,
MARITAL_STATUS
FROM base b
LEFT JOIN GOLD.DIM_CUSTOMER cu ON b.customer_id = cu.customer_id
LEFT JOIN GOLD.DIM_CALENDAR cal ON cu.DATE_OF_BIRTH_KEY = cal.date_key

--Income and Marital Status Analysis

WITH base as (
SELECT
DISTINCT 
CUSTOMER_ID,
FROM GOLD.FACT_PRODUCT_ACQUISITION a
LEFT JOIN GOLD.DIM_PRODUCT p ON a.product_id = p.product_id
WHERE PRODUCT_TYPE ='Term Credit Card'
)
SELECT
b.CUSTOMER_ID,
ROUND((DATEDIFF(year, FULL_DATE, CURRENT_DATE()) 
      - CASE 
          WHEN TO_CHAR(CURRENT_DATE(), 'MMDD') < TO_CHAR(FULL_DATE, 'MMDD') THEN 1 
          ELSE 0 
        END),0) AS AGE,
GENDER,
INCOME,
DATEDIFF(year, FULL_DATE, CURRENT_DATE()) 
      - CASE 
          WHEN TO_CHAR(CURRENT_DATE(), 'MMDD') < TO_CHAR(FULL_DATE, 'MMDD') THEN 1 
          ELSE 0 
        END AS CUSTOMER_DURATION,
MARITAL_STATUS
FROM base b
LEFT JOIN GOLD.DIM_CUSTOMER cu ON b.customer_id = cu.customer_id
LEFT JOIN GOLD.DIM_CALENDAR cal ON cu.DATE_OF_BIRTH_KEY = cal.date_key

--KPI 2 - Click-through

--Distribution of Click-thought Clients

SELECT
ci.customer_id,
click_status
FROM GOLD.FACT_CAMPAIGN_INTERACTION ci
LEFT JOIN GOLD.DIM_CUSTOMER cu ON ci.customer_id = UPPER(cu.customer_id)
LEFT JOIN GOLD.DIM_CAMPAIGN ca ON ci.campaign_id = ca.campaign_id

--Click-through Ratio by Campaign

WITH base as (
SELECT
COUNT(*) as t_contacts,
campaign_name,
ci.campaign_id
FROM GOLD.FACT_CAMPAIGN_INTERACTION ci
LEFT JOIN GOLD.DIM_CAMPAIGN ca ON ci.campaign_id = ca.campaign_id
GROUP BY campaign_name, ci.campaign_id
), sucessfull as(
SELECT
COUNT(*) as p_contacts,
campaign_name,
ci.campaign_id
FROM GOLD.FACT_CAMPAIGN_INTERACTION ci
LEFT JOIN GOLD.DIM_CAMPAIGN ca ON ci.campaign_id = ca.campaign_id
WHERE CLICK_STATUS = 'Yes'
GROUP BY campaign_name, ci.campaign_id
)
SELECT 
b.campaign_name,
ROUND(((p_contacts/t_contacts)*100), 1) AS click_through_ratio
FROM base b
LEFT JOIN sucessfull s ON b.campaign_id = s.campaign_id
;

--AVG Age vs Customer Duration

WITH base as (
SELECT
DISTINCT 
CUSTOMER_ID,
CAMPAIGN_ID
FROM GOLD.FACT_CAMPAIGN_INTERACTION
WHERE click_status = 'Yes'
)
SELECT
ca.campaign_name,
b.CUSTOMER_ID,
ROUND((DATEDIFF(year, FULL_DATE, CURRENT_DATE()) 
      - CASE 
          WHEN TO_CHAR(CURRENT_DATE(), 'MMDD') < TO_CHAR(FULL_DATE, 'MMDD') THEN 1 
          ELSE 0 
        END),0) AS AGE,
GENDER,
INCOME,
DATEDIFF(year, FULL_DATE, CURRENT_DATE()) 
      - CASE 
          WHEN TO_CHAR(CURRENT_DATE(), 'MMDD') < TO_CHAR(FULL_DATE, 'MMDD') THEN 1 
          ELSE 0 
        END AS CUSTOMER_DURATION,
MARITAL_STATUS
FROM base b
LEFT JOIN GOLD.DIM_CUSTOMER cu ON b.customer_id = cu.customer_id
LEFT JOIN GOLD.DIM_CAMPAIGN ca ON b.campaign_id = ca.campaign_id
LEFT JOIN GOLD.DIM_CALENDAR cal ON cu.DATE_OF_BIRTH_KEY = cal.date_key

--Gender Distribution by Campaign

WITH base as (
SELECT
DISTINCT 
CUSTOMER_ID,
CAMPAIGN_ID
FROM GOLD.FACT_CAMPAIGN_INTERACTION
WHERE click_status = 'Yes'
)
SELECT
ca.campaign_name,
b.CUSTOMER_ID,
ROUND((DATEDIFF(year, FULL_DATE, CURRENT_DATE()) 
      - CASE 
          WHEN TO_CHAR(CURRENT_DATE(), 'MMDD') < TO_CHAR(FULL_DATE, 'MMDD') THEN 1 
          ELSE 0 
        END),0) AS AGE,
GENDER,
INCOME,
DATEDIFF(year, FULL_DATE, CURRENT_DATE()) 
      - CASE 
          WHEN TO_CHAR(CURRENT_DATE(), 'MMDD') < TO_CHAR(FULL_DATE, 'MMDD') THEN 1 
          ELSE 0 
        END AS CUSTOMER_DURATION,
MARITAL_STATUS
FROM base b
LEFT JOIN GOLD.DIM_CUSTOMER cu ON b.customer_id = cu.customer_id
LEFT JOIN GOLD.DIM_CAMPAIGN ca ON b.campaign_id = ca.campaign_id
LEFT JOIN GOLD.DIM_CALENDAR cal ON cu.DATE_OF_BIRTH_KEY = cal.date_key

--KPI 3 - Membership Upgrade

--# of Products Acquired over Year
SELECT
ca.full_date,
cu.CITY
FROM GOLD.FACT_PRODUCT_ACQUISITION a
LEFT JOIN GOLD.DIM_PRODUCT p ON a.product_id = p.product_id
LEFT JOIN GOLD.DIM_CUSTOMER cu ON a.customer_id = cu.customer_id
LEFT JOIN GOLD.DIM_CALENDAR ca ON a.acquisition_date_key = ca.date_key

--# of Clients Upgraded
SELECT 
    DISTINCT s.CUSTOMER_ID
FROM GOLD.FACT_PRODUCT_ACQUISITION s
JOIN GOLD.FACT_PRODUCT_ACQUISITION p
    ON s.CUSTOMER_ID = p.CUSTOMER_ID
WHERE s.PRODUCT_ID = 'P002'
  AND p.PRODUCT_ID = 'P001'

--Demographic Analysis

  WITH base as (
SELECT 
    DISTINCT s.CUSTOMER_ID
FROM GOLD.FACT_PRODUCT_ACQUISITION s
JOIN GOLD.FACT_PRODUCT_ACQUISITION p
    ON s.CUSTOMER_ID = p.CUSTOMER_ID
WHERE s.PRODUCT_ID = 'P002'
  AND p.PRODUCT_ID = 'P001'
)
SELECT
b.CUSTOMER_ID,
ROUND((DATEDIFF(year, FULL_DATE, CURRENT_DATE()) 
      - CASE 
          WHEN TO_CHAR(CURRENT_DATE(), 'MMDD') < TO_CHAR(FULL_DATE, 'MMDD') THEN 1 
          ELSE 0 
        END),0) AS AGE,
GENDER,
INCOME,
CITY,
DATEDIFF(year, FULL_DATE, CURRENT_DATE()) 
      - CASE 
          WHEN TO_CHAR(CURRENT_DATE(), 'MMDD') < TO_CHAR(FULL_DATE, 'MMDD') THEN 1 
          ELSE 0 
        END AS CUSTOMER_DURATION,
MARITAL_STATUS
FROM base b
LEFT JOIN GOLD.DIM_CUSTOMER cu ON b.customer_id = cu.customer_id
LEFT JOIN GOLD.DIM_CALENDAR cal ON cu.DATE_OF_BIRTH_KEY = cal.date_key


--Age by Marital Status & Gender

SELECT
count(*)
FROM GOLD.FACT_PRODUCT_ACQUISITION a
LEFT JOIN GOLD.DIM_PRODUCT p ON a.product_id = p.product_id
LEFT JOIN GOLD.DIM_CUSTOMER cu ON a.customer_id = cu.customer_id
LEFT JOIN GOLD.DIM_CALENDAR ca ON a.acquisition_date_key = ca.date_key
WHERE product_type='Credit Card';


SELECT 
    DISTINCT s.CUSTOMER_ID AS customers_with_both
FROM GOLD.FACT_PRODUCT_ACQUISITION s
JOIN GOLD.FACT_PRODUCT_ACQUISITION p
    ON s.CUSTOMER_ID = p.CUSTOMER_ID
WHERE s.PRODUCT_ID = 'P002'
  AND p.PRODUCT_ID = 'P001'; 


  WITH base as (
SELECT 
    DISTINCT s.CUSTOMER_ID
FROM GOLD.FACT_PRODUCT_ACQUISITION s
JOIN GOLD.FACT_PRODUCT_ACQUISITION p
    ON s.CUSTOMER_ID = p.CUSTOMER_ID
WHERE s.PRODUCT_ID = 'P002'
  AND p.PRODUCT_ID = 'P001'
)
SELECT
b.CUSTOMER_ID,
ROUND((DATEDIFF(year, FULL_DATE, CURRENT_DATE()) 
      - CASE 
          WHEN TO_CHAR(CURRENT_DATE(), 'MMDD') < TO_CHAR(FULL_DATE, 'MMDD') THEN 1 
          ELSE 0 
        END),0) AS AGE,
GENDER,
INCOME,
DATEDIFF(year, FULL_DATE, CURRENT_DATE()) 
      - CASE 
          WHEN TO_CHAR(CURRENT_DATE(), 'MMDD') < TO_CHAR(FULL_DATE, 'MMDD') THEN 1 
          ELSE 0 
        END AS CUSTOMER_DURATION,
MARITAL_STATUS
FROM base b
LEFT JOIN GOLD.DIM_CUSTOMER cu ON b.customer_id = cu.customer_id
LEFT JOIN GOLD.DIM_CALENDAR cal ON cu.DATE_OF_BIRTH_KEY = cal.date_key