# Using SQL Warehouse in Databricks

---

## Table of Contents

1. [Introduction to SQL Warehouse](#1-introduction-to-sql-warehouse)
   - 1.1 [What Is a SQL Warehouse?](#11-what-is-a-sql-warehouse)
   - 1.2 [SQL Warehouse vs All-Purpose Cluster vs Job Cluster](#12-sql-warehouse-vs-all-purpose-cluster-vs-job-cluster)
   - 1.3 [SQL Warehouse Architecture](#13-sql-warehouse-architecture)
   - 1.4 [Types of SQL Warehouses](#14-types-of-sql-warehouses)
   - 1.5 [When to Use a SQL Warehouse](#15-when-to-use-a-sql-warehouse)
2. [Starting a SQL Warehouse](#2-starting-a-sql-warehouse)
   - 2.1 [Creating a SQL Warehouse via the UI](#21-creating-a-sql-warehouse-via-the-ui)
   - 2.2 [Warehouse Size and Scaling Options](#22-warehouse-size-and-scaling-options)
   - 2.3 [Auto-Stop Configuration](#23-auto-stop-configuration)
   - 2.4 [Starting and Stopping a Warehouse](#24-starting-and-stopping-a-warehouse)
   - 2.5 [Creating a SQL Warehouse via the REST API](#25-creating-a-sql-warehouse-via-the-rest-api)
3. [Costing in SQL Warehouse](#3-costing-in-sql-warehouse)
   - 3.1 [How SQL Warehouse Billing Works](#31-how-sql-warehouse-billing-works)
   - 3.2 [DBU Consumption by Warehouse Type](#32-dbu-consumption-by-warehouse-type)
   - 3.3 [Cluster Size and Cost Impact](#33-cluster-size-and-cost-impact)
   - 3.4 [Auto-Stop and Scaling for Cost Control](#34-auto-stop-and-scaling-for-cost-control)
   - 3.5 [Query Result Caching](#35-query-result-caching)
   - 3.6 [Cost Monitoring and Budgets](#36-cost-monitoring-and-budgets)
   - 3.7 [Cost Optimization Best Practices](#37-cost-optimization-best-practices)
4. [Integration of SQL Warehouse with Unity Catalog](#4-integration-of-sql-warehouse-with-unity-catalog)
   - 4.1 [Why SQL Warehouse and Unity Catalog Belong Together](#41-why-sql-warehouse-and-unity-catalog-belong-together)
   - 4.2 [Three-Level Namespace in SQL Warehouse Queries](#42-three-level-namespace-in-sql-warehouse-queries)
   - 4.3 [Granting Access to Catalogs, Schemas, and Tables](#43-granting-access-to-catalogs-schemas-and-tables)
   - 4.4 [Row-Level and Column-Level Security](#44-row-level-and-column-level-security)
   - 4.5 [Data Lineage via SQL Warehouse](#45-data-lineage-via-sql-warehouse)
5. [Processing Data in SQL Warehouse](#5-processing-data-in-sql-warehouse)
   - 5.1 [Databricks SQL Editor](#51-databricks-sql-editor)
   - 5.2 [Running Queries and Viewing Results](#52-running-queries-and-viewing-results)
   - 5.3 [Creating and Managing Dashboards](#53-creating-and-managing-dashboards)
   - 5.4 [Alerts on Query Results](#54-alerts-on-query-results)
   - 5.5 [Connecting BI Tools via JDBC/ODBC](#55-connecting-bi-tools-via-jdbcodbc)
   - 5.6 [Using SQL Warehouse from Notebooks](#56-using-sql-warehouse-from-notebooks)
   - 5.7 [Query History and Performance Monitoring](#57-query-history-and-performance-monitoring)
6. [Summary](#6-summary)

---

## 1. Introduction to SQL Warehouse

### 1.1 What Is a SQL Warehouse?

A **SQL Warehouse** (formerly called SQL Endpoint) is a dedicated, serverless-optional compute resource in Databricks designed exclusively for running **SQL queries**. Unlike all-purpose clusters that support PySpark, Scala, R, and notebooks, a SQL Warehouse exposes only a SQL interface — making it ideal for **BI analysts, data analysts, and business users** who work with SQL tools and dashboards.

SQL Warehouses are a core component of the **Databricks Lakehouse Platform**. They sit on top of the same Delta Lake storage layer used by Spark clusters, providing SQL access to the same governed tables registered in Unity Catalog.

```
┌─────────────────────────────────────────────────────────────┐
│               Databricks Lakehouse Platform                 │
│                                                             │
│   Data Scientists      Data Engineers       BI Analysts    │
│   ┌──────────┐         ┌──────────┐        ┌──────────┐   │
│   │ All-Purp │         │  Job     │        │  SQL     │   │
│   │ Cluster  │         │ Cluster  │        │ Warehouse│   │
│   │ (PySpark)│         │ (ETL)    │        │ (SQL)    │   │
│   └────┬─────┘         └────┬─────┘        └────┬─────┘   │
│        │                    │                    │          │
│   ┌────▼────────────────────▼────────────────────▼─────┐   │
│   │              Unity Catalog (Metastore)               │   │
│   │         Tables │ Permissions │ Lineage │ Audit      │   │
│   └─────────────────────────────────────────────────────┘   │
│                              │                              │
│   ┌──────────────────────────▼──────────────────────────┐   │
│   │           Delta Lake on AWS S3                       │   │
│   └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

### 1.2 SQL Warehouse vs All-Purpose Cluster vs Job Cluster

| Aspect                    | All-Purpose Cluster     | Job Cluster             | SQL Warehouse              |
| ------------------------- | ----------------------- | ----------------------- | -------------------------- |
| **Primary Use**           | Interactive development | Scheduled ETL/ML jobs   | SQL queries and dashboards |
| **Languages**             | Python, SQL, Scala, R   | Python, SQL, Scala, R   | SQL only                   |
| **User Interface**        | Notebooks               | Notebooks / scripts     | SQL Editor, Dashboards     |
| **BI Tool Integration**   | Limited                 | None                    | JDBC/ODBC, partner connect |
| **Auto-scaling**          | Manual or standard auto | Single-node or standard | Multi-cluster auto-scaling |
| **Start-up Time**         | 3–8 minutes             | 3–8 minutes             | ~20 seconds (warm pool)    |
| **Billing Unit**          | DBUs (interactive rate) | DBUs (job rate)         | DBUs (SQL rate)            |
| **Query Result Caching**  | None                    | None                    | Built-in (UI cache)        |
| **Unity Catalog Support** | Yes                     | Yes                     | Yes (tightly integrated)   |
| **Photon Engine**         | Optional                | Optional                | Default (Serverless/Pro)   |

---

### 1.3 SQL Warehouse Architecture

When a query is submitted to a SQL Warehouse, it goes through the following layers:

```
User / BI Tool
     │  JDBC/ODBC or HTTP API
     ▼
┌──────────────────────────────────────────────────┐
│             SQL Warehouse Endpoint               │
│   ┌────────────────────────────────────────┐    │
│   │          Query Dispatcher              │    │
│   │  • Parse SQL                           │    │
│   │  • Check Unity Catalog permissions     │    │
│   │  • Route to available cluster          │    │
│   │  • Check result cache                  │    │
│   └───────────────┬────────────────────────┘    │
│                   │                             │
│   ┌───────────────▼────────────────────────┐    │
│   │    Compute Cluster(s) — Photon Engine  │    │
│   │  • Execute query on Delta Lake files   │    │
│   │  • Data skipping / Z-ordering          │    │
│   │  • Return result set                   │    │
│   └────────────────────────────────────────┘    │
└──────────────────────────────────────────────────┘
           │
           ▼
    AWS S3 (Delta Lake files)
```

The **Photon engine** — Databricks' native vectorized query engine written in C++ — accelerates SQL execution and is enabled by default on Pro and Serverless warehouses.

---

### 1.4 Types of SQL Warehouses

Databricks offers three warehouse types:

| Type           | Description                                                                | Best For                                      |
| -------------- | -------------------------------------------------------------------------- | --------------------------------------------- |
| **Classic**    | Standard SQL endpoint running on EC2 clusters you manage                   | Cost-conscious workloads, predictable queries |
| **Pro**        | Classic + Photon engine + enhanced caching + Predictive I/O                | BI dashboards, mixed analytical workloads     |
| **Serverless** | Fully managed by Databricks, instant start (~3 sec), no cluster management | Interactive ad-hoc SQL, variable workloads    |

> **Serverless SQL Warehouse** is the preferred choice for most teams in 2025+. The compute runs in Databricks' cloud account, eliminating the wait for EC2 provisioning.

---

### 1.5 When to Use a SQL Warehouse

Use a SQL Warehouse when:

- **BI analysts** need to query Delta Lake tables with tools like Tableau, Power BI, or Looker.
- You want to expose a **governed SQL endpoint** to users who shouldn't access raw notebooks or clusters.
- You need **fast, interactive SQL queries** with low startup latency.
- You want **built-in dashboards and alerts** within Databricks SQL.
- You need **row/column-level security** enforced at the query layer via Unity Catalog.
- You require **JDBC/ODBC connectivity** for external applications.

---

## 2. Starting a SQL Warehouse

### 2.1 Creating a SQL Warehouse via the UI

**Prerequisites:** You must have the **Can Manage** permission on the workspace, or be a workspace admin.

**Step-by-step:**

1. In the left sidebar, click **SQL** (the diamond icon) → **SQL Warehouses**.
2. Click **Create SQL Warehouse**.
3. Fill in the configuration:

| Field            | Recommended Setting                  | Notes                                      |
| ---------------- | ------------------------------------ | ------------------------------------------ |
| **Name**         | `team-analytics-warehouse`           | Descriptive, include team name             |
| **Cluster size** | `Small (4 workers)`                  | Start small, scale up if slow              |
| **Auto Stop**    | `10 minutes`                         | Prevents idle billing                      |
| **Scaling**      | Min: 1, Max: 3                       | Enables multi-cluster for concurrent users |
| **Type**         | `Serverless` (if available)          | Fastest startup, no infra management       |
| **Photon**       | Enabled (default for Pro/Serverless) | Vectorised engine for fast queries         |
| **Channel**      | `Current`                            | Stable release channel                     |

4. Click **Create**.
5. The warehouse starts automatically on first query or you can click **Start** manually.

---

### 2.2 Warehouse Size and Scaling Options

**Cluster Size** controls the number of worker nodes per cluster:

| Size                | Workers | vCPUs (approx.) | Best For                              |
| ------------------- | ------- | --------------- | ------------------------------------- |
| 2X-Small            | 1       | 4               | Development, low-concurrency queries  |
| X-Small             | 2       | 8               | Small teams, simple dashboards        |
| Small               | 4       | 16              | Typical analytics workload            |
| Medium              | 8       | 32              | Complex queries, moderate concurrency |
| Large               | 16      | 64              | Heavy aggregations, large scans       |
| X-Large             | 32      | 128             | Enterprise-scale workloads            |
| 2X-Large            | 64      | 256             | Very large scans, extreme concurrency |
| 3X-Large / 4X-Large | 128–256 | 512+            | Petabyte-scale, rare use cases        |

**Multi-cluster scaling:** Set a **Min cluster count** and **Max cluster count**. Databricks automatically spins up additional clusters (up to Max) when query queue depth exceeds a threshold, and shuts them down when idle.

```
Example: Min=1, Max=3

Low concurrency (5 users):       High concurrency (50 users):
  ┌── Cluster 1 (active) ──┐       ┌── Cluster 1 ──┐
                             ──▶   ├── Cluster 2 ──┤  (auto-scaled up)
                                   └── Cluster 3 ──┘  (auto-scaled up)
```

---

### 2.3 Auto-Stop Configuration

SQL Warehouses automatically stop after a configurable idle period to avoid billing for unused compute:

| Setting        | Behavior                                                        |
| -------------- | --------------------------------------------------------------- |
| **10 minutes** | Stops 10 min after last query — good for interactive usage      |
| **30 minutes** | Suitable for shared dashboards with periodic refreshes          |
| **Disabled**   | Never auto-stops — only use for always-on production warehouses |

> **Tip:** For Serverless warehouses, auto-stop is nearly free to restart (~3 seconds). For Classic/Pro warehouses on EC2, restart takes 2–5 minutes, so set a longer idle timeout for frequently used warehouses.

---

### 2.4 Starting and Stopping a Warehouse

**Manual start/stop (UI):**

1. Navigate to **SQL Warehouses**.
2. Find the warehouse. The status indicator shows **Running** (green) or **Stopped** (grey).
3. Click the **Start** or **Stop** button on the right.

**Via Databricks CLI:**

```bash
# List warehouses
databricks warehouses list

# Start a warehouse
databricks warehouses start <warehouse-id>

# Stop a warehouse
databricks warehouses stop <warehouse-id>

# Get warehouse details
databricks warehouses get <warehouse-id>
```

**Warehouse states:**

```
Stopped → [Start triggered] → Starting → Running → [Idle timeout] → Stopping → Stopped
                                                  ↑                              │
                                                  └──── [Query arrives] ─────────┘
                                                         (auto-wakes from Stopped)
```

When a query arrives at a stopped warehouse, Databricks automatically wakes it up. The user sees a "Warehouse is starting…" message.

---

### 2.5 Creating a SQL Warehouse via the REST API

```python
import requests

DATABRICKS_HOST = "https://<your-workspace>.azuredatabricks.net"   # or .databricks.com
TOKEN = dbutils.secrets.get(scope="admin", key="databricks-token")

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "name": "team-analytics-warehouse",
    "cluster_size": "Small",
    "min_num_clusters": 1,
    "max_num_clusters": 3,
    "auto_stop_mins": 10,
    "enable_photon": True,
    "warehouse_type": "PRO",
    "channel": {"name": "CHANNEL_NAME_CURRENT"}
}

response = requests.post(
    f"{DATABRICKS_HOST}/api/2.0/sql/warehouses",
    headers=headers,
    json=payload
)

warehouse_id = response.json()["id"]
print(f"Created warehouse: {warehouse_id}")
```

---

## 3. Costing in SQL Warehouse

### 3.1 How SQL Warehouse Billing Works

SQL Warehouse billing combines two cost components:

```
Total Cost = DBU Cost + Cloud Infrastructure Cost

DBU Cost:
  • Databricks charges DBUs (Databricks Units) per cluster-hour
  • DBU rate depends on warehouse type and your contract tier

Cloud Infrastructure Cost:
  • AWS charges for EC2 instances running the warehouse clusters
  • Serverless warehouses: compute is in Databricks' account — you pay a
    higher DBU rate but no direct EC2 cost

Billing starts: when the warehouse moves to "Running" state
Billing stops:  when the warehouse fully stops (after auto-stop or manual stop)
Billing unit:   per second (Classic/Pro) or per second (Serverless)
```

---

### 3.2 DBU Consumption by Warehouse Type

| Warehouse Type | DBUs per Hour per Worker | Notes                             |
| -------------- | ------------------------ | --------------------------------- |
| **Classic**    | 1 DBU                    | Base rate, no Photon              |
| **Pro**        | 1.5 DBUs                 | +50% for Photon, advanced caching |
| **Serverless** | ~2 DBUs                  | Includes full management overhead |

> DBU pricing varies by region and contract. Consult the [Databricks pricing page](https://www.databricks.com/product/pricing) for current rates.

**Example cost calculation (Pro warehouse, Small size, AWS us-east-1):**

```
Warehouse size:   Small = 4 workers
DBU rate:         1.5 DBU/worker/hour
Hours used:       8 hours/day × 20 working days = 160 hours/month

Monthly DBUs:     4 workers × 1.5 DBU/hr × 160 hrs = 960 DBUs/month
DBU price:        ~$0.22/DBU (example enterprise rate)
DBU cost:         960 × $0.22 = $211/month

EC2:              4 × m5.2xlarge ≈ $0.384/hr × 160 hrs = $61/month

Total:            ~$272/month (for sustained 8hr/day usage)
```

> With **Auto-Stop at 10 minutes**, actual cluster-on time drops dramatically for interactive use — the effective cost is often 20–40% of sustained usage.

---

### 3.3 Cluster Size and Cost Impact

Larger cluster sizes multiply DBU consumption linearly:

| Size     | Workers | DBU/hr (Pro) | Cost Multiplier vs 2X-Small |
| -------- | ------- | ------------ | --------------------------- |
| 2X-Small | 1       | 1.5          | 1×                          |
| X-Small  | 2       | 3.0          | 2×                          |
| Small    | 4       | 6.0          | 4×                          |
| Medium   | 8       | 12.0         | 8×                          |
| Large    | 16      | 24.0         | 16×                         |
| X-Large  | 32      | 48.0         | 32×                         |

**Key insight:** Doubling the cluster size doubles cost but does NOT always double query speed — larger sizes help for complex aggregations and large scans but provide diminishing returns for simple lookups.

---

### 3.4 Auto-Stop and Scaling for Cost Control

**Auto-Stop savings example:**

```
Scenario: 20 users, queries clustered in 9–10am and 2–3pm windows

Without Auto-Stop (always-on, Small warehouse):
  24 hrs × $6 DBU/hr × $0.22 = $31.68/day = ~$950/month

With Auto-Stop at 10 minutes:
  Active time ≈ 2.5 hours/day (2 hours query windows + warm-up/cool-down)
  2.5 hrs × $6 × $0.22 = $3.30/day = ~$99/month

Savings: ~90% cost reduction
```

**Scaling strategy:**

```
Pattern 1 — Small, Multi-Cluster (best for concurrent light queries):
  Min=1, Max=5, Size=Small
  → Good for 50+ concurrent BI users running simple dashboard queries

Pattern 2 — Large, Single-Cluster (best for heavy ad-hoc analytics):
  Min=1, Max=1, Size=Large
  → Good for a few data analysts running complex multi-join queries

Pattern 3 — Medium, Multi-Cluster (balanced):
  Min=1, Max=3, Size=Medium
  → Works for mixed workloads (dashboards + ad-hoc analysis)
```

---

### 3.5 Query Result Caching

SQL Warehouse has three layers of caching that reduce both query time and cost:

| Cache Layer          | What Is Cached                             | Duration                 |
| -------------------- | ------------------------------------------ | ------------------------ |
| **Result Cache**     | Full result sets of identical SQL queries  | 24 hours                 |
| **Disk Cache**       | Delta Lake parquet files on local SSD      | Until evicted            |
| **Delta Statistics** | File-level min/max stats for data skipping | Persisted in \_delta_log |

When a cached result is returned, **no compute is consumed** — the DBU clock does not tick for that query.

```sql
-- First execution: reads S3, takes 5 seconds, consumes DBUs
SELECT region, SUM(revenue) FROM sales GROUP BY region;

-- Second execution (identical query): returns from result cache in <100ms, zero DBU cost
SELECT region, SUM(revenue) FROM sales GROUP BY region;
```

---

### 3.6 Cost Monitoring and Budgets

**View usage in the Account Console:**

1. Go to **Account Console** → **Cost Management** → **Usage**.
2. Filter by workspace, cluster tag, or user.
3. Export to CSV for custom analysis.

**Tag SQL Warehouses for cost allocation:**

```json
{
  "custom_tags": {
    "team": "analytics",
    "project": "retail-reporting",
    "environment": "production",
    "cost-center": "CC-1042"
  }
}
```

Tags propagate to AWS Cost Explorer, allowing per-team/per-project cost breakdown.

**Set budget alerts** in Account Console → Budget Policies to receive email/webhook notifications when spend exceeds a threshold.

---

### 3.7 Cost Optimization Best Practices

| Practice                            | Impact                                                           |
| ----------------------------------- | ---------------------------------------------------------------- |
| Enable Auto-Stop (10–30 min)        | Eliminates idle billing — largest single cost saver              |
| Use Serverless for ad-hoc workloads | No minimum cluster billable time, instant scale-to-zero          |
| Right-size cluster for workload     | Small for dashboards; scale up only for heavy analytics          |
| Use multi-cluster scaling           | Handles concurrency spikes without over-provisioning permanently |
| Leverage result caching             | Identical dashboard queries served for free                      |
| Optimize Delta tables (OPTIMIZE)    | Fewer files to scan = less compute per query                     |
| Use Z-ORDER on filter columns       | Data skipping reduces bytes scanned dramatically                 |
| Partition large tables              | Partition pruning eliminates irrelevant data before scanning     |
| Avoid `SELECT *`                    | Column pruning reduces I/O and Photon processing cost            |

---

## 4. Integration of SQL Warehouse with Unity Catalog

### 4.1 Why SQL Warehouse and Unity Catalog Belong Together

SQL Warehouse is designed to work natively with **Unity Catalog** — Databricks' unified governance layer. Every query submitted through a SQL Warehouse is automatically subject to:

- **Authentication:** The user's identity is verified via the workspace's identity provider (Azure AD, Okta, etc.).
- **Authorization:** Unity Catalog checks table/column/row-level permissions for every query.
- **Auditing:** All queries, including who ran them and what data was accessed, are logged to the Unity Catalog audit log.
- **Lineage:** Column-level lineage is tracked automatically across SQL Warehouse queries.

```
User submits SQL query
         │
         ▼
SQL Warehouse Query Dispatcher
         │
         ▼  (check performed before execution)
Unity Catalog Permission Check
  • Does user have SELECT on catalog.schema.table?
  • Are any columns masked for this user?
  • Does a row filter apply to this user's group?
         │
   ┌─────┴──────┐
   │ DENY       │ ALLOW
   ▼            ▼
 Error       Execute on Photon Engine
             → Return filtered result set
```

---

### 4.2 Three-Level Namespace in SQL Warehouse Queries

Unity Catalog uses a **three-level namespace**: `catalog.schema.table`. SQL Warehouse queries must reference this fully qualified name, or set a default catalog/schema to use unqualified names.

```sql
-- Fully qualified (always works):
SELECT * FROM retail_catalog.sales_schema.orders
WHERE order_date >= '2024-01-01';

-- Set default catalog for the session:
USE CATALOG retail_catalog;
USE SCHEMA sales_schema;

-- Now unqualified names resolve to retail_catalog.sales_schema:
SELECT * FROM orders WHERE order_date >= '2024-01-01';

-- Cross-catalog join (analysts often don't realize this is possible):
SELECT
    o.order_id,
    o.total,
    c.email
FROM retail_catalog.sales_schema.orders o
JOIN hr_catalog.customer_schema.customers c
  ON o.customer_id = c.customer_id;
```

---

### 4.3 Granting Access to Catalogs, Schemas, and Tables

Unity Catalog permissions flow **hierarchically** — granting at the catalog level gives access to all schemas and tables within it (subject to more granular grants below).

```sql
-- Grant a user access to use the catalog:
GRANT USE CATALOG ON CATALOG retail_catalog TO `analyst@company.com`;

-- Grant access to a schema:
GRANT USE SCHEMA ON SCHEMA retail_catalog.sales_schema TO `analyst@company.com`;

-- Grant SELECT on a specific table:
GRANT SELECT ON TABLE retail_catalog.sales_schema.orders TO `analyst@company.com`;

-- Grant SELECT on all current and future tables in a schema:
GRANT SELECT ON SCHEMA retail_catalog.sales_schema TO `analyst_group`;

-- Grant a group permission to create tables in a schema:
GRANT CREATE TABLE ON SCHEMA retail_catalog.dev_schema TO `data_engineers`;

-- Revoke a permission:
REVOKE SELECT ON TABLE retail_catalog.sales_schema.orders FROM `analyst@company.com`;

-- View what permissions exist on a table:
SHOW GRANTS ON TABLE retail_catalog.sales_schema.orders;
```

---

### 4.4 Row-Level and Column-Level Security

**Column Masking** — Hides or transforms sensitive column values for users who don't have full access:

```sql
-- Create a masking function that shows full email only to admin group:
CREATE FUNCTION retail_catalog.security.mask_email(email STRING)
  RETURN CASE
    WHEN is_member('admin_group') THEN email
    ELSE CONCAT(LEFT(email, 3), '****@****.com')
  END;

-- Apply mask to the email column on the customers table:
ALTER TABLE retail_catalog.sales_schema.customers
  ALTER COLUMN email
  SET MASK retail_catalog.security.mask_email;

-- Admin user sees:    john.doe@company.com
-- Analyst user sees:  joh****@****.com
```

**Row Filters** — Restricts which rows a user can see:

```sql
-- Create a row filter function: users see only their region's data
CREATE FUNCTION retail_catalog.security.region_filter(region STRING)
  RETURN is_member('global_viewer') OR region = current_user_region();

-- Apply to the sales table:
ALTER TABLE retail_catalog.sales_schema.sales
  SET ROW FILTER retail_catalog.security.region_filter ON (region);

-- APAC analyst running this query only sees APAC rows:
SELECT region, SUM(revenue) FROM retail_catalog.sales_schema.sales GROUP BY region;
```

---

### 4.5 Data Lineage via SQL Warehouse

Unity Catalog automatically captures **column-level lineage** for SQL queries run through SQL Warehouses. No configuration is required.

**Viewing lineage in the UI:**

1. Navigate to **Catalog** (the book icon in the sidebar).
2. Browse to a table (e.g., `retail_catalog.gold_schema.revenue_summary`).
3. Click the **Lineage** tab.
4. View the upstream tables that contributed to this table's data.

**Lineage is tracked for:**

- `INSERT INTO ... SELECT`
- `CREATE TABLE ... AS SELECT`
- `MERGE INTO`
- View definitions
- DLT pipeline outputs

---

## 5. Processing Data in SQL Warehouse

### 5.1 Databricks SQL Editor

The **Databricks SQL Editor** is the primary web-based interface for running SQL against a SQL Warehouse. It provides:

- Multi-tab query editing
- Auto-complete for catalogs, schemas, tables, and columns
- Schema browser (navigate Unity Catalog hierarchy visually)
- Query history (view and re-run past queries)
- Results visualization (table, bar chart, line chart, scatter, etc.)
- Saved queries (personal and shared)

**Keyboard shortcuts:**

| Action        | Mac                 | Windows/Linux          |
| ------------- | ------------------- | ---------------------- |
| Run query     | `⌘ + Enter`         | `Ctrl + Enter`         |
| Run selected  | `⌘ + Shift + Enter` | `Ctrl + Shift + Enter` |
| Format SQL    | `⌘ + Shift + F`     | `Ctrl + Shift + F`     |
| New query tab | `⌘ + T`             | `Ctrl + T`             |

---

### 5.2 Running Queries and Viewing Results

```sql
-- Example: Analyze top customers by revenue
SELECT
    c.customer_id,
    c.name,
    c.region,
    COUNT(o.order_id)     AS total_orders,
    SUM(o.order_amount)   AS total_revenue,
    AVG(o.order_amount)   AS avg_order_value,
    MAX(o.order_date)     AS last_order_date
FROM retail_catalog.sales_schema.orders o
JOIN retail_catalog.sales_schema.customers c
  ON o.customer_id = c.customer_id
WHERE o.order_date >= DATE_SUB(CURRENT_DATE(), 90)
  AND o.status = 'COMPLETED'
GROUP BY c.customer_id, c.name, c.region
ORDER BY total_revenue DESC
LIMIT 100;
```

**Explain a query plan** to understand execution before running:

```sql
EXPLAIN
SELECT region, COUNT(*) FROM retail_catalog.sales_schema.orders GROUP BY region;
```

**Profile query execution:**

```sql
-- Returns Photon metrics, scan statistics, and stage breakdown:
EXPLAIN ANALYZE
SELECT region, COUNT(*) FROM retail_catalog.sales_schema.orders GROUP BY region;
```

---

### 5.3 Creating and Managing Dashboards

Databricks SQL includes a **Lakeview Dashboards** feature (formerly SQL Dashboards) for building and sharing business-facing reports.

**Create a dashboard:**

1. In the left sidebar, click **Dashboards** → **Create Dashboard**.
2. Click **Add Visualization** → select a saved query → choose chart type.
3. Arrange widgets with drag-and-drop.
4. Add filters to make the dashboard interactive.
5. Configure **auto-refresh** (e.g., every 30 minutes for real-time reporting).
6. Click **Publish** and share with a group or the entire workspace.

**Example: Revenue by Region Bar Chart**

```sql
-- Underlying query for a dashboard widget:
SELECT
    region,
    DATE_TRUNC('month', order_date) AS month,
    SUM(order_amount)                AS total_revenue
FROM retail_catalog.sales_schema.orders
WHERE order_date >= DATE_SUB(CURRENT_DATE(), 365)
GROUP BY region, month
ORDER BY month, region;
```

In the dashboard editor:

- **X-axis:** `month`
- **Y-axis:** `total_revenue`
- **Group by:** `region`
- **Chart type:** Stacked Bar Chart

---

### 5.4 Alerts on Query Results

**SQL Alerts** monitor a query on a schedule and notify you when a condition is met (e.g., error rate exceeds threshold, revenue drops below expected value).

**Create an alert:**

1. Open a saved query → click **Create Alert**.
2. Set the **Value column** (e.g., `error_count`), **Operator** (`>`), and **Threshold** (`1000`).
3. Set the **Refresh schedule** (e.g., every 15 minutes).
4. Add **Destinations**: email, Slack webhook, PagerDuty.
5. Click **Save**.

```sql
-- Example alert query: SLA breach detection
SELECT COUNT(*) AS late_orders
FROM retail_catalog.sales_schema.orders
WHERE status = 'PENDING'
  AND order_date < DATE_SUB(CURRENT_DATE(), 2);
-- Alert triggers when late_orders > 50
```

---

### 5.5 Connecting BI Tools via JDBC/ODBC

SQL Warehouses expose a **JDBC/ODBC endpoint** compatible with any standard SQL client or BI tool.

**Connection details (from UI):**

1. Go to **SQL Warehouses** → select your warehouse → **Connection Details** tab.
2. Copy the **Server Hostname** and **HTTP Path**.

**JDBC connection string:**

```
jdbc:databricks://<hostname>:443/default;
  transportMode=http;
  ssl=1;
  httpPath=<http-path>;
  AuthMech=3;
  UID=token;
  PWD=<personal-access-token>
```

**Connect from Python (using databricks-sql-connector):**

```python
from databricks import sql

connection = sql.connect(
    server_hostname="<workspace>.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/<warehouse-id>",
    access_token=dbutils.secrets.get(scope="myapp", key="databricks-token")
)

cursor = connection.cursor()
cursor.execute("""
    SELECT region, SUM(order_amount) AS revenue
    FROM retail_catalog.sales_schema.orders
    GROUP BY region
    ORDER BY revenue DESC
""")
results = cursor.fetchall()
for row in results:
    print(row)

cursor.close()
connection.close()
```

**Supported BI tools (Databricks Partner Connect):**

- Tableau
- Power BI
- Looker
- Qlik
- ThoughtSpot
- Preset

Navigate to **Partner Connect** in the sidebar for guided 1-click integration setups.

---

### 5.6 Using SQL Warehouse from Notebooks

Notebooks can submit queries to a SQL Warehouse (instead of the notebook's attached cluster) using the `%sql` magic or the Databricks SQL Connector. This is useful when a notebook orchestrates SQL transformations that benefit from Photon or need SQL Warehouse permissions.

**Use `spark.sql()` with warehouse context (notebook attached to SQL Warehouse):**

```python
# Attach your notebook to a SQL Warehouse endpoint via the cluster/warehouse selector
# Then use spark.sql() normally — it runs on the warehouse:
df = spark.sql("""
    SELECT * FROM retail_catalog.sales_schema.orders
    WHERE order_date >= '2024-01-01'
""")
df.display()
```

**Programmatic access from a job notebook (using connector):**

```python
from databricks import sql
import os

with sql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"],
    http_path=os.environ["WAREHOUSE_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"]
) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO retail_catalog.gold_schema.revenue_summary
            SELECT region, SUM(amount), CURRENT_DATE()
            FROM retail_catalog.silver_schema.cleaned_orders
            GROUP BY region
        """)
        print(f"Rows affected: {cursor.rowcount}")
```

---

### 5.7 Query History and Performance Monitoring

**Query History** (available under **SQL** → **Query History**) shows:

| Column            | Description                               |
| ----------------- | ----------------------------------------- |
| **Query text**    | Full SQL statement                        |
| **User**          | Who ran the query                         |
| **Status**        | Succeeded / Failed / Cancelled            |
| **Duration**      | Total wall-clock time                     |
| **Bytes scanned** | Data read from Delta Lake files           |
| **Rows returned** | Number of result rows                     |
| **Spill to disk** | Whether query exceeded in-memory capacity |

**Performance optimization from Query History:**

```sql
-- Find the most expensive queries in the last 7 days (via system table):
SELECT
    query_text,
    user_name,
    total_duration_ms / 1000.0    AS duration_seconds,
    bytes_scanned / 1e9           AS gb_scanned,
    rows_produced
FROM system.query.history
WHERE start_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAYS)
  AND status = 'FINISHED'
ORDER BY total_duration_ms DESC
LIMIT 20;
```

> **System tables** (`system.query.history`, `system.access.audit`) require Unity Catalog and are available to workspace admins. These provide the gold standard for query observability.

---

## 6. Summary

SQL Warehouse is the dedicated SQL compute layer of the Databricks Lakehouse, purpose-built for BI analysts and SQL-first workloads.

| Concept            | Key Takeaway                                                                        |
| ------------------ | ----------------------------------------------------------------------------------- |
| **What it is**     | Managed SQL endpoint with Photon, caching, and JDBC/ODBC — SQL only                 |
| **Types**          | Classic (basic), Pro (Photon + caching), Serverless (instant start, no EC2)         |
| **Start/Stop**     | Auto-starts on query arrival; configure Auto-Stop to avoid idle billing             |
| **Costing**        | DBUs per cluster-per-hour + EC2 (Classic/Pro); result caching reduces spend         |
| **Unity Catalog**  | Tight integration — every query is permission-checked, audited, and lineage-tracked |
| **SQL Editor**     | Web-based IDE with schema browser, visualisations, and saved queries                |
| **Dashboards**     | Lakeview Dashboards for business reporting with auto-refresh                        |
| **BI integration** | JDBC/ODBC endpoint connects Tableau, Power BI, Looker, and 20+ other tools          |
| **Cost control**   | Auto-Stop + multi-cluster scaling + Serverless = optimal cost-to-performance        |

**What's next:** Guide 14 covers **Managing Data Access with Unity Catalog** — the governance layer that SQL Warehouse relies on for permissions, lineage, and auditing.

---
