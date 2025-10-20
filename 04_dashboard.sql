/* =========================
   DASHBOARD SETUP (run once)
   ========================= */
USE ROLE WS1_G5;
USE WAREHOUSE group5_Wrk1;
USE DATABASE WS1_G5_DB;

CREATE SCHEMA IF NOT EXISTS GOLD_DASH;
USE SCHEMA GOLD_DASH;


/* =========================
   CORE HELPER VIEWS
   ========================= */

/* Daily sends/clicks per campaign/channel */
CREATE OR REPLACE VIEW V_INTERACTIONS_DAILY AS
SELECT
  f.campaign_id,
  dc.full_date,
  dc.date_year,
  dc.week_of_year,
  c.channel,
  COUNT(*)                                                       AS sends,
  COUNT_IF(f.click_date_key IS NOT NULL)                         AS clicks,
  COUNT(DISTINCT f.customer_id)                                  AS uniq_sent_customers,
  COUNT(DISTINCT IFF(f.click_date_key IS NOT NULL, f.customer_id, NULL)) AS uniq_click_customers
FROM WS1_G5_DB.GOLD.FACT_CAMPAIGN_INTERACTION f
JOIN WS1_G5_DB.GOLD.DIM_CALENDAR dc ON dc.date_key = f.SENT_DATE_KEY
LEFT JOIN WS1_G5_DB.GOLD.DIM_CAMPAIGN c ON c.campaign_id = f.campaign_id
GROUP BY 1,2,3,4,5;

/* Responders = distinct clickers per campaign */
CREATE OR REPLACE VIEW V_RESPONDERS_BY_CAMPAIGN AS
SELECT
  campaign_id,
  COUNT(DISTINCT customer_id) AS uniq_responders
FROM WS1_G5_DB.GOLD.FACT_CAMPAIGN_INTERACTION
WHERE CLICK_DATE_KEY IS NOT NULL
GROUP BY 1;

/* Daily acquisitions (for funnel if you want to show the final stage) */
CREATE OR REPLACE VIEW V_ACQUISITIONS_DAILY AS
SELECT
  fpa.customer_id,
  fpa.product_id,
  fpa.acquisition_date_key,
  dc.full_date,
  dc.date_year,
  dc.week_of_year
FROM WS1_G5_DB.GOLD.FACT_PRODUCT_ACQUISITION fpa
JOIN WS1_G5_DB.GOLD.DIM_CALENDAR dc ON dc.date_key = fpa.ACQUISITION_DATE_KEY;

/* Customer with derived demographics (age_group, income_bucket) */
CREATE OR REPLACE VIEW V_CUSTOMER_ENRICHED AS
SELECT
  c.customer_id,
  c.customer_name,
  COALESCE(c.gender, 'unknown') AS gender,
  COALESCE(c.city,   'unknown') AS city,
  c.income,
  c.date_of_birth,
  COALESCE(c.preferred_channel, 'unknown') AS preferred_channel,
  DATEDIFF(year, c.date_of_birth, CURRENT_DATE) AS age,
  CASE
    WHEN DATEDIFF(year, c.date_of_birth, CURRENT_DATE) < 18 THEN '00-17'
    WHEN DATEDIFF(year, c.date_of_birth, CURRENT_DATE) BETWEEN 18 AND 24 THEN '18-24'
    WHEN DATEDIFF(year, c.date_of_birth, CURRENT_DATE) BETWEEN 25 AND 34 THEN '25-34'
    WHEN DATEDIFF(year, c.date_of_birth, CURRENT_DATE) BETWEEN 35 AND 44 THEN '35-44'
    WHEN DATEDIFF(year, c.date_of_birth, CURRENT_DATE) BETWEEN 45 AND 54 THEN '45-54'
    WHEN DATEDIFF(year, c.date_of_birth, CURRENT_DATE) BETWEEN 55 AND 64 THEN '55-64'
    ELSE '65+'
  END AS age_group,
  CASE
    WHEN c.income IS NULL THEN 'Unknown'
    WHEN c.income < 50000 THEN '<50k'
    WHEN c.income < 100000 THEN '50-100k'
    WHEN c.income < 150000 THEN '100-150k'
    ELSE '150k+'
  END AS income_bucket
FROM WS1_G5_DB.GOLD.DIM_CUSTOMER c;

/* Strict scorecard (truth) */
CREATE OR REPLACE VIEW V_CAMPAIGN_SCORECARD_STRICT AS
WITH inter AS (
  SELECT
    f.campaign_id,
    COUNT(*) AS sends,
    COUNT_IF(f.click_date_key IS NOT NULL) AS clicks,
    COUNT(DISTINCT IFF(f.click_date_key IS NOT NULL, f.customer_id, NULL)) AS responders
  FROM WS1_G5_DB.GOLD.FACT_CAMPAIGN_INTERACTION f
  GROUP BY 1
),
base AS (
  SELECT
    dc.campaign_id,
    dc.channel,
    dc.budget,
    COALESCE(i.sends,0)      AS sends,
    COALESCE(i.clicks,0)     AS clicks,
    COALESCE(i.responders,0) AS responders
  FROM WS1_G5_DB.GOLD.DIM_CAMPAIGN dc
  LEFT JOIN inter i ON i.campaign_id = dc.campaign_id
)
SELECT
  campaign_id,
  channel,
  budget,
  sends, clicks, responders,
  (CAST(clicks  AS FLOAT)/NULLIF(sends,0))     AS cvr,
  (CAST(budget AS FLOAT)/NULLIF(responders,0)) AS cac,
  CASE
    WHEN budget IS NULL THEN 'No BUDGET → CAC null'
    WHEN responders = 0 AND sends > 0 THEN 'No responders (no clicks) → CAC null'
    WHEN sends = 0 AND clicks = 0 THEN 'No interactions (no sends/clicks) → CVR null'
    WHEN sends = 0 AND clicks > 0 THEN 'Clicks exist but sends=0 → data mismatch'
    ELSE 'OK / check formula'
  END AS data_quality_reason
FROM base;

/* Presentation-friendly scorecard (for charts) */
CREATE OR REPLACE VIEW V_CAMPAIGN_SCORECARD_PRESENT AS
SELECT
  campaign_id, channel, budget, sends, clicks, responders, cvr, cac, data_quality_reason,
  IFF(sends > 0 AND clicks = 0, 0.0, cvr)                    AS cvr_display,
  IFF(responders = 0 OR budget IS NULL, NULL, cac)           AS cac_display,
  CASE
    WHEN budget IS NULL THEN 'No budget'
    WHEN responders = 0 AND sends > 0 THEN 'No responders'
    WHEN sends = 0 AND clicks = 0 THEN 'No interactions'
    WHEN sends = 0 AND clicks > 0 THEN 'Mismatch: clicks w/o sends'
    ELSE 'OK'
  END AS dq_badge
FROM V_CAMPAIGN_SCORECARD_STRICT;

/* Coverage KPIs (optional but recommended) */
CREATE OR REPLACE VIEW V_KPI_COVERAGE_INTERACTIONS AS
WITH allc AS (SELECT COUNT(*) AS total FROM WS1_G5_DB.GOLD.DIM_CAMPAIGN),
intc AS (
  SELECT COUNT(DISTINCT campaign_id) AS with_interactions
  FROM WS1_G5_DB.GOLD.FACT_CAMPAIGN_INTERACTION
)
SELECT with_interactions, total,
       ROUND(100.0 * with_interactions / NULLIF(total,0), 1) AS pct_with_interactions
FROM intc, allc;

CREATE OR REPLACE VIEW V_KPI_COVERAGE_BUDGET AS
SELECT COUNT(*) AS total,
       COUNT_IF(budget IS NOT NULL) AS with_budget,
       ROUND(100.0 * COUNT_IF(budget IS NOT NULL) / NULLIF(COUNT(*),0), 1) AS pct_with_budget
FROM WS1_G5_DB.GOLD.DIM_CAMPAIGN;

CREATE OR REPLACE VIEW V_KPI_COVERAGE_RESPONDERS AS
WITH resp AS (
  SELECT DISTINCT campaign_id
  FROM WS1_G5_DB.GOLD.FACT_CAMPAIGN_INTERACTION
  WHERE click_date_key IS NOT NULL
),
allc AS (SELECT campaign_id FROM WS1_G5_DB.GOLD.DIM_CAMPAIGN)
SELECT COUNT(*) AS total,
       (SELECT COUNT(*) FROM resp) AS with_responders,
       ROUND(100.0 * (SELECT COUNT(*) FROM resp) / NULLIF(COUNT(*),0), 1) AS pct_with_responders
FROM allc;

/* =========================
   TILE QUERIES (copy each into a Chart → Move to Dashboard)
   ========================= */

/* --- TILE 01: KPI - Average CVR --- */
SELECT AVG(cvr_display) AS avg_cvr
FROM GOLD_DASH.V_CAMPAIGN_SCORECARD_PRESENT
WHERE sends > 0;

/* --- TILE 02: KPI - Average CAC (definable only) --- */
SELECT AVG(cac_display) AS avg_cac
FROM GOLD_DASH.V_CAMPAIGN_SCORECARD_PRESENT
WHERE cac_display IS NOT NULL;

/* --- TILE 03: KPI - Active Campaigns --- */
SELECT COUNT(DISTINCT campaign_id) AS active_campaigns
FROM WS1_G5_DB.GOLD.DIM_CAMPAIGN
WHERE LOWER(status) = 'active';

/* --- TILE 04: KPI - Coverage % (Interactions) --- */
SELECT pct_with_interactions
FROM GOLD_DASH.V_KPI_COVERAGE_INTERACTIONS;

/* --- TILE 05: KPI - Coverage % (Budget present) --- */
SELECT pct_with_budget
FROM GOLD_DASH.V_KPI_COVERAGE_BUDGET;

/* --- TILE 06: KPI - Coverage % (Responders present) --- */
SELECT pct_with_responders
FROM GOLD_DASH.V_KPI_COVERAGE_RESPONDERS;

/* --- TILE 07: Funnel (Sends → Clicks → Acquisitions) --- */
WITH s AS (
  SELECT SUM(sends) AS sends, SUM(clicks) AS clicks
  FROM GOLD_DASH.V_INTERACTIONS_DAILY
),
a AS (
  SELECT COUNT(*) AS acquisitions
  FROM GOLD_DASH.V_ACQUISITIONS_DAILY
)
SELECT 'Sends' AS stage, sends AS value FROM s
UNION ALL
SELECT 'Clicks', clicks FROM s
UNION ALL
SELECT 'Acquisitions', acquisitions FROM a;

/* --- TILE 08: CVR - Daily Trend (line) --- */
SELECT
  full_date,
  SUM(clicks) AS clicks,
  SUM(sends)  AS sends,
  (CAST(SUM(clicks) AS FLOAT) / NULLIF(SUM(sends),0)) AS cvr
FROM GOLD_DASH.V_INTERACTIONS_DAILY
GROUP BY 1
ORDER BY 1;

/* --- TILE 09: Channel - CVR (bar) --- */
SELECT
  channel,
  AVG(cvr_display) AS cvr
FROM GOLD_DASH.V_CAMPAIGN_SCORECARD_PRESENT
WHERE sends > 0
GROUP BY 1
ORDER BY cvr DESC;

/* --- TILE 10: Channel - CAC (bar) --- */
SELECT
  channel,
  AVG(cac_display) AS cac
FROM GOLD_DASH.V_CAMPAIGN_SCORECARD_PRESENT
WHERE cac_display IS NOT NULL
GROUP BY 1
ORDER BY cac;

/* --- TILE 11: Campaigns - CAC vs CVR (scatter; X=CAC, Y=CVR, Size=Budget, Color=Channel) --- */
SELECT
  campaign_id,
  channel,
  budget,
  cac_display AS cac,
  cvr_display AS cvr
FROM GOLD_DASH.V_CAMPAIGN_SCORECARD_PRESENT
WHERE cvr_display IS NOT NULL OR cac_display IS NOT NULL;

/* --- TILE 12: Campaign Table (with QA badge) --- */
SELECT
  campaign_id, channel, budget, sends, clicks, responders,
  cvr, cac, dq_badge AS data_quality
FROM GOLD_DASH.V_CAMPAIGN_SCORECARD_PRESENT
ORDER BY cvr DESC NULLS LAST;

/* --- TILE 13: CVR by Age Group (bar) --- */
WITH clicks AS (
  SELECT DISTINCT f.customer_id, f.campaign_id
  FROM WS1_G5_DB.GOLD.FACT_CAMPAIGN_INTERACTION f
  WHERE f.click_date_key IS NOT NULL
),
sends AS (
  SELECT f.customer_id, f.campaign_id
  FROM WS1_G5_DB.GOLD.FACT_CAMPAIGN_INTERACTION f
)
SELECT
  ce.age_group,
  (CAST(COUNT(DISTINCT clicks.customer_id) AS FLOAT) / NULLIF(COUNT(DISTINCT sends.customer_id),0)) AS cvr
FROM sends
JOIN GOLD_DASH.V_CUSTOMER_ENRICHED ce ON ce.customer_id = sends.customer_id
LEFT JOIN clicks
  ON clicks.customer_id = sends.customer_id
 AND clicks.campaign_id = sends.campaign_id
GROUP BY 1
ORDER BY
  CASE age_group
    WHEN '00-17' THEN 0 WHEN '18-24' THEN 1 WHEN '25-34' THEN 2
    WHEN '35-44' THEN 3 WHEN '45-54' THEN 4 WHEN '55-64' THEN 5 ELSE 6
  END;

/* --- TILE 14: CVR by Gender (bar/donut) --- */
WITH clicks AS (
  SELECT DISTINCT f.customer_id, f.campaign_id
  FROM WS1_G5_DB.GOLD.FACT_CAMPAIGN_INTERACTION f
  WHERE f.click_date_key IS NOT NULL
),
sends AS (
  SELECT f.customer_id, f.campaign_id
  FROM WS1_G5_DB.GOLD.FACT_CAMPAIGN_INTERACTION f
)
SELECT
  ce.gender,
  (CAST(COUNT(DISTINCT clicks.customer_id) AS FLOAT) / NULLIF(COUNT(DISTINCT sends.customer_id),0)) AS cvr
FROM sends
JOIN GOLD_DASH.V_CUSTOMER_ENRICHED ce ON ce.customer_id = sends.customer_id
LEFT JOIN clicks
  ON clicks.customer_id = sends.customer_id
 AND clicks.campaign_id = sends.campaign_id
GROUP BY 1
ORDER BY 1;

/* --- TILE 15: Top Cities by Responders (bar) --- */
SELECT
  ce.city,
  COUNT(DISTINCT f.customer_id) AS responders
FROM WS1_G5_DB.GOLD.FACT_CAMPAIGN_INTERACTION f
JOIN GOLD_DASH.V_CUSTOMER_ENRICHED ce ON ce.customer_id = f.customer_id
WHERE f.click_date_key IS NOT NULL
GROUP BY 1
ORDER BY responders DESC
LIMIT 15;

/* --- TILE 16: Heatmap - Preferred Channel vs Campaign Channel (CVR) --- */
WITH base AS (
  SELECT
    f.campaign_id,
    dc.channel                AS campaign_channel,
    ce.preferred_channel      AS preferred_channel,
    COUNT(*)                  AS sends,
    COUNT_IF(f.click_date_key IS NOT NULL) AS clicks
  FROM WS1_G5_DB.GOLD.FACT_CAMPAIGN_INTERACTION f
  JOIN WS1_G5_DB.GOLD.DIM_CAMPAIGN dc ON dc.campaign_id = f.campaign_id
  JOIN GOLD_DASH.V_CUSTOMER_ENRICHED ce ON ce.customer_id = f.customer_id
  GROUP BY 1,2,3
)
SELECT
  preferred_channel,
  campaign_channel,
  (CAST(SUM(clicks) AS FLOAT) / NULLIF(SUM(sends), 0)) AS cvr
FROM base
GROUP BY 1,2
ORDER BY 1,2;

/* --- TILE 17: CAC by Income Bucket (bar) --- */
WITH responders AS (
  SELECT DISTINCT f.customer_id, f.campaign_id
  FROM WS1_G5_DB.GOLD.FACT_CAMPAIGN_INTERACTION f
  WHERE f.click_date_key IS NOT NULL
),
budget_per_campaign AS (
  SELECT campaign_id, SUM(budget) AS budget
  FROM WS1_G5_DB.GOLD.DIM_CAMPAIGN
  GROUP BY 1
),
joined AS (
  SELECT
    COALESCE(ce.income_bucket, 'Unknown') AS income_bucket,
    r.campaign_id,
    b.budget,
    r.customer_id
  FROM responders r
  JOIN GOLD_DASH.V_CUSTOMER_ENRICHED ce ON ce.customer_id = r.customer_id
  LEFT JOIN budget_per_campaign b ON b.campaign_id = r.campaign_id
)
SELECT
  income_bucket,
  (CAST(SUM(budget) AS FLOAT) / NULLIF(COUNT(DISTINCT customer_id), 0)) AS cac
FROM joined
GROUP BY 1
ORDER BY
  CASE income_bucket
    WHEN '<50k' THEN 1 WHEN '50-100k' THEN 2 WHEN '100-150k' THEN 3
    WHEN '150k+' THEN 4 ELSE 5
  END;
