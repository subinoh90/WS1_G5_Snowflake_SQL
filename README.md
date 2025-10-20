# WS1_G5 Snowflake SQL (Bronze → Silver → Gold → Dashboard)

This repo contains reproducible SQL for our Snowflake Medallion pipeline.

## Files
- `01_bronze.sql` – stage + file format, raw tables (STRING), COPY INTO from stage, load checks.
- `02_silver.sql` – standardize/clean, type casting, dedup, outlier handling, missing-data rules.
- `03_gold.sql` – build star schema (dim_calendar, dim_campaign, dim_product, dim_customer; fact tables) and load.
- `04_dashboard.sql` – helper views + tile queries for Snowsight dashboards.

## How to run
1. Upload CSVs to your Snowflake stage: `@BRONZE_STAGE`.
2. Run `01_bronze.sql` (top → bottom).
3. Run `02_silver.sql`.
4. Run `03_gold.sql`.
5. Run `04_dashboard.sql` to create views; use the final SELECTs for dashboard tiles.

**Warehouse:** `group5_Wrk1`  
**Database:** `WS1_G5_DB`  
**Schemas:** `BRONZE`, `SILVER`, `GOLD`, `GOLD_DASH`

> Bronze is raw only (no cleaning). Silver cleans; Gold models for analytics.

> This SQL pipeline corresponds to Section 3 (“Working Prototype Implementation using Snowflake”)
> of our final project report.

---

## 📊 Data Source

This repository includes **synthetic CSV data** used in the Bronze layer for reproducibility.  
All files live in the `/source data` folder and can be uploaded directly to the Snowflake stage `@BRONZE_STAGE`.

### Folder structure
source data/
├── CustomerProducts_raw.csv
├── Campaigns_raw.csv
└── CampaignInteractions_raw.csv

**What each file represents**
- **CustomerProducts_raw.csv** — customer profiles, demographics, and product holdings  
- **Campaigns_raw.csv** — campaign master metadata (name, launch date, budget, channel, status)  
- **CampaignInteractions_raw.csv** — per-customer delivery/response events (sent, click)

> Note: All data is **synthetic** and used for academic demonstration only.  
> The column order matches the schemas created in `01_bronze.sql` (all STRING in Bronze).

---

## Reproducibility checklist
- Use role: `WS1_G5`
- Use warehouse: `group5_Wrk1` (auto-suspend enabled)
- Database: `WS1_G5_DB`
- Stages: `BRONZE_STAGE` (internal)
- Run order: `01_bronze.sql` → `02_silver.sql` → `03_gold.sql` → `04_dashboard.sql`