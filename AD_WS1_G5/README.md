# WS1_G5 Snowflake SQL (Bronze → Silver → Gold → Dashboard)

This repo contains reproducible SQL for our Snowflake Medallion pipeline.

## Files
- `01_bronze.sql` – stage + file format, raw tables (STRING), COPY INTO from stage, load checks.
- `02_silver.sql` – standardize/clean, type casting, dedup, outlier handling, missing-data rules.
- `03_gold.sql` – build star schema (dim_calendar, dim_campaign, dim_product, dim_customer; fact tables) and load.
- `04_dashboard.sql` – helper views + tile queries for Snowsight dashboards.

## How to run
1. Upload CSVs to `@BRONZE_STAGE` in Snowflake.
2. Run `01_bronze.sql` top → bottom.
3. Run `02_silver.sql`.
4. Run `03_gold.sql`.
5. Run `04_dashboard.sql` to create views; use the SELECTs for dashboard tiles.

**Warehouse:** `group5_Wrk1`  
**Database:** `WS1_G5_DB`  
**Schemas:** `BRONZE`, `SILVER`, `GOLD`, `GOLD_DASH`

> Bronze is raw only (no cleaning). Silver cleans; Gold models for analytics.

> This SQL pipeline corresponds to the implementation described in Section 3
> (“Working Prototype Implementation using Snowflake”) of our final project report.