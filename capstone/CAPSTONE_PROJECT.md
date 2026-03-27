# Databricks Data Engineering Capstone Assignment

## Build a Data Platform for Comcast

---

### Overview

You are a **Data Engineer at Comcast**, one of the largest cable, internet, and media companies in the US. Your team has been asked to build a modern **data lakehouse platform** on Databricks that ingests, transforms, and governs data from four operational systems:

| System             | Dataset               | What it contains                                   |
| ------------------ | --------------------- | -------------------------------------------------- |
| CRM                | `customers.csv`       | Subscriber profiles, plans, regions, billing       |
| Network Operations | `network_events.json` | Node-level signal quality, outages, maintenance    |
| Usage Metering     | `service_usage.json`  | Daily internet/TV/phone consumption per subscriber |
| Help Desk          | `support_tickets.csv` | Customer complaints, resolution times, CSAT scores |

By the end of six sessions you will have built a **complete, production-style Databricks pipeline** covering ingestion, transformation, Delta Lake, a DLT Medallion pipeline, scheduled workflows, and Unity Catalog governance.

---

### Datasets

Download the four files from the `sample_data/` folder and upload them to your Databricks workspace under `dbfs:/comcast/landing/`.

#### customers.csv — Schema

| Column           | Type   | Description                                                                                        |
| ---------------- | ------ | -------------------------------------------------------------------------------------------------- |
| `customer_id`    | String | Unique subscriber ID (`CUST00001`…)                                                                |
| `first_name`     | String | First name                                                                                         |
| `last_name`      | String | Last name                                                                                          |
| `email`          | String | Email address (PII)                                                                                |
| `phone`          | String | Phone number (PII)                                                                                 |
| `region`         | String | One of: Northeast, Southeast, Midwest, West, Southwest                                             |
| `plan`           | String | One of: Internet_100, Internet_400, Internet_Gig, TV_Basic, TV_Premium, Bundle_Basic, Bundle_Elite |
| `status`         | String | Active, Suspended, or Churned                                                                      |
| `signup_date`    | String | ISO date (YYYY-MM-DD)                                                                              |
| `monthly_charge` | Double | Monthly billing amount in USD                                                                      |

#### service_usage.json — Schema

| Column          | Type   | Description                                      |
| --------------- | ------ | ------------------------------------------------ |
| `event_id`      | String | Unique event ID (`EVT100001`…)                   |
| `customer_id`   | String | References `customers.customer_id`               |
| `event_date`    | String | ISO date of usage measurement                    |
| `gb_downloaded` | Double | Internet data downloaded (GB)                    |
| `gb_uploaded`   | Double | Internet data uploaded (GB)                      |
| `tv_hours`      | Double | Hours of TV streamed (0 for Internet-only plans) |
| `phone_minutes` | Double | Phone minutes used (0 for non-voice plans)       |
| `region`        | String | Subscriber's region                              |

#### support_tickets.csv — Schema

| Column            | Type    | Description                                                  |
| ----------------- | ------- | ------------------------------------------------------------ |
| `ticket_id`       | String  | Unique ticket ID (`TKT000001`…)                              |
| `customer_id`     | String  | References `customers.customer_id`                           |
| `category`        | String  | Outage, Billing, Speed Issue, Equipment, Installation, Other |
| `priority`        | String  | Low, Medium, High, or Critical                               |
| `opened_date`     | String  | ISO date ticket was opened                                   |
| `resolution_days` | Integer | Days taken to close the ticket                               |
| `status`          | String  | Resolved, Pending, or Escalated                              |
| `csat_score`      | Integer | Customer satisfaction score 1–5 (may be null)                |
| `region`          | String  | Subscriber's region                                          |

#### network_events.json — Schema

| Column            | Type    | Description                                                |
| ----------------- | ------- | ---------------------------------------------------------- |
| `event_id`        | String  | Unique event ID (`NET000001`…)                             |
| `node_id`         | String  | Network node identifier (e.g., `NODE-NEA-001`)             |
| `node_type`       | String  | CMTS, HFC_Node, DOCSIS, or Fiber_ONT                       |
| `event_type`      | String  | Outage, Degraded, Maintenance, Restored, Warning           |
| `event_timestamp` | String  | ISO timestamp of event                                     |
| `duration_mins`   | Integer | Duration of the event in minutes (0 for non-outage events) |
| `affected_homes`  | Integer | Number of homes impacted                                   |
| `region`          | String  | Region of the node                                         |
| `snr_db`          | Double  | Signal-to-noise ratio in dB                                |

---

### Instructions for Uploading Data

1. In your Databricks workspace, open the **Data** section in the left sidebar.
2. Click **Add Data → Upload Files (DBFS)**.
3. Upload each file to the following DBFS paths:
   - `dbfs:/comcast/landing/customers/` → upload `customers.csv`
   - `dbfs:/comcast/landing/service_usage/` → upload `service_usage.json`
   - `dbfs:/comcast/landing/support_tickets/` → upload `support_tickets.csv`
   - `dbfs:/comcast/landing/network_events/` → upload `network_events.json`

> **Tip:** You can verify the files are uploaded by running:
>
> ```python
> display(dbutils.fs.ls("dbfs:/comcast/landing/"))
> ```

---

## Daily Tasks

---

### Day 1 — Data Ingestion & Exploration

**Objective:** Read data from the landing zone and explore it using Spark DataFrames and SQL.

**Tasks:**

1. **Read each dataset** into a Spark DataFrame. Use `inferSchema=True` for CSV files and `inferSchema=True` for JSON files.

2. **Inspect each DataFrame** using:
   - `.printSchema()` — verify column names and types
   - `.count()` — verify row counts
   - `.describe()` — statistical summary of numeric columns

3. **Explore distributions** by answering these questions with `display()` and `groupBy`:
   - How many customers are in each region?
   - How many subscribers are on each plan type?
   - How many customers are Active vs Suspended vs Churned?

4. **Register temp views** and run at least **3 SQL queries** using `%sql` magic that answer meaningful questions about the data.

5. **Write a short markdown cell** in your notebook summarising one interesting pattern you observed.

**Deliverable:** A single notebook with all steps above, clearly labelled with markdown headers.

**Expected counts:**

- Customers: 30 rows
- Service usage: 60 rows
- Support tickets: 30 rows
- Network events: 20 rows

---

### Day 2 — Transforming Data with Spark

**Objective:** Clean and enrich the raw data using Spark DataFrame transformations.

**Tasks:**

1. **Transform the `customers` DataFrame:**
   - Cast `signup_date` to `DateType`
   - Cast `monthly_charge` to `DoubleType`
   - Derive a new column `product_type` with values `Internet`, `TV`, or `Bundle` based on the `plan` column
   - Derive `is_high_value` (Boolean): `True` if `monthly_charge >= 120`
   - Derive `tenure_days`: number of days from `signup_date` to today
   - Standardise `region` to uppercase
   - Drop the `email` and `phone` columns (PII not needed downstream)

2. **Transform the `service_usage` DataFrame:**
   - Cast `event_date` to `DateType`
   - Add `total_gb = gb_downloaded + gb_uploaded`
   - Write a **UDF** that classifies `total_gb` into a `usage_tier`:
     - `< 5 GB` → Light, `5–30 GB` → Medium, `30–80 GB` → Heavy, `> 80 GB` → Ultra

3. **Transform the `support_tickets` DataFrame:**
   - Cast `opened_date` to `DateType` and `resolution_days` to `IntegerType`
   - Fill null `csat_score` with `0`
   - Add `sla_breached` (Boolean): `True` when priority is High or Critical AND `resolution_days > 5`
   - Add `sla_category`: `Breached`, `Excellent` (≤2 days), `On-Time` (3–5 days), or `Delayed`

4. **Build a Customer 360 view** by joining:
   - Customers (spine)
   - Aggregated usage per customer (total download, avg daily GB, active days)
   - Aggregated ticket metrics (total tickets, SLA breach count, average CSAT)
   - Fill nulls with `0` for customers with no usage or ticket records

5. **Apply window functions** to rank customers within each region by total data downloaded.

6. **Write all transformed DataFrames to Parquet** at `dbfs:/comcast/processed/`.

7. **Answer with SQL** (using `createOrReplaceTempView`):
   - Which plan type has the highest churn rate?
   - What is the average CSAT by region and priority?

**Deliverable:** A single notebook with all transformations, plus proof of Parquet output using `dbutils.fs.ls`.

---

### Day 3 — Managing Data with Delta Lake

**Objective:** Store the cleaned data as Delta tables and practise ACID operations.

**Tasks:**

1. **Create a database** called `comcast_db` and set it as active.

2. **Create four Delta tables** from your processed Parquet files:
   - `comcast_db.customers`
   - `comcast_db.service_usage` (partition by `region`)
   - `comcast_db.support_tickets` (partition by `region`)
   - `comcast_db.network_events`

3. **Perform ACID operations:**
   - **INSERT** two new customers directly via SQL
   - **UPDATE** one existing customer's plan and monthly charge
   - **DELETE** all churned customers with tenure > 730 days

4. **Simulate a CRM batch update using MERGE INTO:**
   - One existing customer has upgraded their plan
   - Two brand-new customers arrive
   - Write the MERGE statement that upserts both cases in one operation

5. **Add a new column** `autopay_enrolled` (Boolean) using `ALTER TABLE` and backfill it with `false`.

6. **Time Travel:**
   - Run `DESCRIBE HISTORY` on the customers table
   - Query the table `VERSION AS OF 0` to see original state
   - Compare a customer's plan in version 0 vs the current version

7. **Set up Autoloader** to stream the `network_events` JSON files into a new Delta table `comcast_db.network_events_stream`. Use a checkpoint path at `dbfs:/comcast/checkpoints/network_events`.

8. **Run `OPTIMIZE ... ZORDER BY (customer_id)`** on the `service_usage` table.

**Deliverable:** A notebook demonstrating all 8 steps, with `DESCRIBE HISTORY` output visible.

---

### Day 4 — Delta Live Tables: Medallion Pipeline

**Objective:** Build a declarative Bronze → Silver → Gold pipeline using Delta Live Tables.

> This notebook must be **configured as a DLT Pipeline** in the Workflows UI, not run interactively.
> Pipeline name: `comcast_medallion_pipeline` | Target schema: `comcast_dlt`

**Tasks:**

**Bronze Layer** — Ingest raw data as-is, adding `_source_file` and `_ingestion_ts` metadata columns:

- `bronze_customers`
- `bronze_service_usage`
- `bronze_support_tickets`
- `bronze_network_events`

**Silver Layer** — Validate and cleanse data. You must add **at least two `@dlt.expect_or_drop` rules per table**:

- `silver_customers` — apply all the transformations from Day 2 (product_type, tenure_days, etc.). Reject rows with null `customer_id` or invalid `status`.
- `silver_service_usage` — cast types, derive `total_gb`, `usage_tier`. Reject null `customer_id` or negative download values.
- `silver_support_tickets` — apply SLA logic from Day 2. Reject null `ticket_id` or invalid `priority`.
- `silver_network_events` — parse timestamps, add `is_outage` flag. Reject null `event_id`.

**Gold Layer** — Build three business-ready aggregation tables:

- `gold_customer_360` — one row per customer with 30-day usage totals, ticket count, SLA breach count, CSAT average, and a `churn_risk_score` column (`High`, `Medium`, or `Low`) based on your own logic
- `gold_regional_performance` — one row per region per date with average GB, ticket count, and outage count
- `gold_sla_compliance` — one row per region + priority + month with total tickets, SLA breach rate, and average CSAT

**Deliverable:** Running DLT pipeline in the UI with no expectation failures visible in the pipeline graph.

---

### Day 5 — Workflows & Orchestration

**Objective:** Automate the full pipeline as a scheduled Databricks Job.

**Tasks:**

1. **Create a parameterised notebook** that accepts two widgets:
   - `run_date` (default: today's date as YYYY-MM-DD)
   - `region_filter` (default: `ALL`)

2. **Task A — Validate landing data:** Read each raw file and verify:
   - Row count > 0
   - No null values in the ID column
   - Raise an exception (fail the task) if validation fails
   - Use `dbutils.jobs.taskValues.set` to pass the validation timestamp downstream

3. **Task B (DLT pipeline task):** Configure in the Workflows UI as a Delta Live Tables pipeline task pointing to `comcast_medallion_pipeline`.

4. **Task C — Gold aggregation refresh:** Re-compute the `gold_customer_360` table and save it. Retrieve the task value from Task A using `dbutils.jobs.taskValues.get`.

5. **Task D — Data quality report:** Query each Delta table, compute null counts, write a summary report to a Delta table called `comcast_db.dq_report_log`. Use `append` mode so historical reports accumulate.

6. **Task E — Alert on failure:** A task that should be configured to run when any upstream task **fails**. Log the failure to `comcast_db.pipeline_alert_log` with an ISO timestamp and severity `HIGH`.

7. **Configure the full Job in the Workflows UI:**
   - Name: `comcast_daily_pipeline`
   - Set task dependencies as: A → B → C → D, with E triggered on any failure
   - Set a schedule: daily at 6 AM UTC

**Deliverable:** Screenshot (or copy of the JSON job definition from the UI) showing all 5 tasks with correct dependencies. Successful manual run of at least Tasks A, C, and D.

---

### Day 6 — SQL Warehouse & Unity Catalog Governance

**Objective:** Build an analytics layer and apply enterprise governance using Unity Catalog.

**Part 1 — SQL Warehouse Analytics**

Connect your notebook compute to a **SQL Warehouse** and write the following queries:

1. **Revenue by region and plan type:** Total MRR, subscriber count, churn rate per region + product type.
2. **High-value churn risk customers:** List customers where `is_high_value = true` and `churn_risk_score` is High or Medium. Include tenure, monthly charge, and ticket count.
3. **Network reliability ranking:** Top 5 worst-performing network nodes by outage count. Include total homes affected and average outage duration.
4. **SLA compliance trend:** Monthly SLA breach rate and average CSAT per region, ordered most recent first.
5. **Executive snapshot:** A single query using CTEs that returns one row per region combining revenue, outage count, ticket count, and average CSAT.

**Part 2 — Unity Catalog Governance**

> Requires a Unity Catalog-enabled workspace.

1. **Create a catalog** called `comcast` with three schemas: `bronze`, `silver`, `gold`.
2. **Register your Gold tables** in `comcast.gold` using `CREATE TABLE ... AS SELECT`.
3. **Add tags** to the `comcast.silver.customers` table:
   - Table-level tag: `domain = customer`, `classification = internal`
   - Column-level tag: `first_name` and `last_name` as `pii = true`
4. **Write a column masking function** that hides all but the first character of PII name fields for non-admin users. Apply it to `first_name` and `last_name`.
5. **Write a row filter function** that restricts access to rows based on region, so only members of the `region_<region_name>` group can see their region's data. Apply it to `comcast.silver.service_usage`.
6. **Write `GRANT` statements** (as comments if you don't have group permissions) for:
   - Analyst group: `SELECT` on all Gold tables
   - Data engineers: `SELECT, MODIFY` on Silver tables
7. **Query `system.access.audit`** (if available) or describe in a markdown cell how you would monitor who accessed the PII data.

**Deliverable:** A notebook with all SQL queries running against SQL Warehouse, plus all Unity Catalog DDL statements documented.

---

## Grading Rubric

| Day | Section             | Points  | Criteria                                                                                                                                            |
| --- | ------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Data Ingestion      | 10      | All 4 datasets read, schema printed, 3+ SQL queries, markdown summary                                                                               |
| 2   | Transformations     | 20      | All 5 transforms correct, UDF works, Customer 360 join correct, window function applied, Parquet written                                            |
| 3   | Delta Lake          | 20      | All 4 Delta tables created, INSERT/UPDATE/DELETE demonstrated, MERGE handles both match and no-match, Time Travel query runs, Autoloader configured |
| 4   | DLT Pipeline        | 20      | Pipeline runs in UI with 0 expectation failures, all 3 Gold tables produced, at least 2 expect rules per Silver table                               |
| 5   | Workflows           | 15      | 5 tasks configured with correct dependencies, `taskValues` used, DQ report written to Delta, alert task on failure                                  |
| 6   | SQL + Unity Catalog | 15      | 5 SQL queries run against Warehouse, catalog/schema created, column masking function written, row filter written, GRANT documented                  |
|     | **Total**           | **100** |                                                                                                                                                     |

### Bonus (up to 10 extra points)

- Add a `churn_risk_score` that uses a window function to look at usage trend (declining downloads over days) — +5
- Parameterise the DLT pipeline to accept a `run_date` configuration — +3
- Write a `RESTORE` command to roll back the customers table to version 0 after the MERGE — +2

---

## Evaluation Tips

- **Be systematic:** run each Day's notebook from top to bottom before moving on.
- **Name things correctly:** use the exact table names and database names specified. Graders will run queries against them.
- **Show your work:** include markdown cells explaining what you did and why. A correct result without explanation earns partial credit.
- **Test edge cases:** what happens if `csat_score` is null? What happens when the MERGE finds no matching row?
- **Use `display()`** instead of `show()` for all output so it renders in the notebook.

---

## Submission Checklist

- [ ] Day 1 notebook exported as `.ipynb` or `.py`
- [ ] Day 2 notebook + `dbutils.fs.ls` output showing Parquet files
- [ ] Day 3 notebook + `DESCRIBE HISTORY` output visible
- [ ] Day 4 DLT pipeline running in UI (share a screenshot of the pipeline graph)
- [ ] Day 5 Job definition JSON or screenshot of workflow with all 5 tasks
- [ ] Day 6 notebook with SQL + Unity Catalog DDL

Submit all notebooks to the shared workspace folder: `Users/<your-email>/comcast_capstone/`
