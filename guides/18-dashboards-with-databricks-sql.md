# Dashboards with Databricks SQL

---

## Table of Contents

1. [Introduction to Databricks SQL Dashboards](#1-introduction-to-databricks-sql-dashboards)
   - 1.1 [What Is Databricks SQL?](#11-what-is-databricks-sql)
   - 1.2 [Dashboard Capabilities and Use Cases](#12-dashboard-capabilities-and-use-cases)
   - 1.3 [Dashboard vs. Notebook vs. External BI Tools](#13-dashboard-vs-notebook-vs-external-bi-tools)
   - 1.4 [Databricks SQL Architecture](#14-databricks-sql-architecture)
2. [Creating Interactive Dashboards](#2-creating-interactive-dashboards)
   - 2.1 [The SQL Editor](#21-the-sql-editor)
   - 2.2 [Saved Queries](#22-saved-queries)
   - 2.3 [Visualization Types](#23-visualization-types)
   - 2.4 [Creating a Dashboard and Adding Widgets](#24-creating-a-dashboard-and-adding-widgets)
   - 2.5 [Widget Parameters and Cross-Filtering](#25-widget-parameters-and-cross-filtering)
3. [Building Queries for Dashboards](#3-building-queries-for-dashboards)
   - 3.1 [Parameterized Queries with `{{ }}` Syntax](#31-parameterized-queries-with---syntax)
   - 3.2 [Dropdown and Date Widget Filters](#32-dropdown-and-date-widget-filters)
   - 3.3 [Query Snippets and Reusable Fragments](#33-query-snippets-and-reusable-fragments)
   - 3.4 [Multi-Query Dashboards](#34-multi-query-dashboards)
4. [Automating Updates with Scheduled Queries](#4-automating-updates-with-scheduled-queries)
   - 4.1 [Query Scheduling](#41-query-scheduling)
   - 4.2 [Dashboard Auto-Refresh](#42-dashboard-auto-refresh)
   - 4.3 [Refresh Schedule Options and Cost Considerations](#43-refresh-schedule-options-and-cost-considerations)
   - 4.4 [Triggering Refreshes via the REST API](#44-triggering-refreshes-via-the-rest-api)
5. [Sharing Dashboards with Controlled Access](#5-sharing-dashboards-with-controlled-access)
   - 5.1 [Dashboard Permission Levels](#51-dashboard-permission-levels)
   - 5.2 [Run As Owner vs. Run As Viewer](#52-run-as-owner-vs-run-as-viewer)
   - 5.3 [Sharing via Link vs. Specific Users and Groups](#53-sharing-via-link-vs-specific-users-and-groups)
   - 5.4 [Integration with Unity Catalog Permissions](#54-integration-with-unity-catalog-permissions)
6. [Alerts: Notifications from Query Results](#6-alerts-notifications-from-query-results)
   - 6.1 [Creating Alerts on Saved Queries](#61-creating-alerts-on-saved-queries)
   - 6.2 [Alert Condition Types](#62-alert-condition-types)
   - 6.3 [Alert Destinations](#63-alert-destinations)
   - 6.4 [Alert Muting and Cooldown](#64-alert-muting-and-cooldown)
7. [Best Practices and Real-World Dashboard Patterns](#7-best-practices-and-real-world-dashboard-patterns)
   - 7.1 [Executive KPI Dashboard Pattern](#71-executive-kpi-dashboard-pattern)
   - 7.2 [Operational Monitoring Dashboard Pattern](#72-operational-monitoring-dashboard-pattern)
   - 7.3 [Performance Tips: Warehouse Sizing and Result Caching](#73-performance-tips-warehouse-sizing-and-result-caching)
   - 7.4 [Dashboard Governance and Lifecycle Management](#74-dashboard-governance-and-lifecycle-management)
8. [Summary](#8-summary)

---

## 1. Introduction to Databricks SQL Dashboards

### 1.1 What Is Databricks SQL?

**Databricks SQL** is the SQL-native analytics surface within the Databricks Lakehouse Platform. It provides:

- A **SQL Editor** for authoring, saving, and running queries against Delta tables
- **SQL Warehouses** (serverless or classic) as the isolated compute layer for SQL workloads
- **Dashboards** for composing and publishing visualizations built on saved queries
- **Alerts** that notify teams when query results cross defined thresholds
- **Unity Catalog** integration for fine-grained data access control across all SQL objects

Databricks SQL is designed for **data analysts and BI users** who need SQL-first, interactive analytics without managing Spark clusters. Under the hood, all queries still run on the same Lakehouse data — Delta tables — giving users full access to optimized, versioned, governed data.

---

### 1.2 Dashboard Capabilities and Use Cases

| Capability                     | Description                                                                |
| ------------------------------ | -------------------------------------------------------------------------- |
| **Interactive visualizations** | Bar, line, area, scatter, pie, counter, pivot table, map, box plot         |
| **Query parameters**           | Dynamic filter widgets: text input, dropdowns, date pickers, multi-select  |
| **Cross-dashboard linking**    | Navigate from a summary metric to a detail dashboard via URL parameters    |
| **Scheduled refresh**          | Automatically re-run queries on a cron schedule                            |
| **Fine-grained permissions**   | Share with specific users, groups, or via public link                      |
| **Embedded credentials**       | Run As Owner mode serves data without exposing raw table access to viewers |
| **Threshold alerts**           | Trigger email / Slack / webhook notifications when KPIs breach limits      |
| **AI/BI Dashboards**           | Next-generation drag-and-drop dashboard editor (Databricks Runtime 14.1+)  |

**Common dashboard use cases:**

- Executive weekly business review — revenue, churn, NPS trends
- Operational monitoring — pipeline health, SLA breach alerts, data freshness
- Product analytics — DAU/MAU, funnel conversion, A/B test results
- Data quality reporting — null rates, schema drift. duplicate counts
- Customer success — per-account usage metrics, renewal risk scoring

---

### 1.3 Dashboard vs. Notebook vs. External BI Tools

| Dimension            | Databricks SQL Dashboard              | Databricks Notebook                  | External BI (Tableau, Power BI)          |
| -------------------- | ------------------------------------- | ------------------------------------ | ---------------------------------------- |
| **Target audience**  | BI analysts, business users           | Data engineers, data scientists      | Business analysts, report authors        |
| **Query language**   | SQL                                   | Python, SQL, Scala, R                | Proprietary (DAX, VizQL, MDX)            |
| **Interactivity**    | Parameter widgets, cross-filters      | Widgets (`dbutils.widgets`)          | Rich: slicers, drill-down, tooltips      |
| **Scheduling**       | Native — query-level cron             | Databricks Workflows                 | Tool-specific refresh schedules          |
| **Access control**   | Unity Catalog + dashboard permissions | Unity Catalog + notebook permissions | Connector credentials or DirectQuery     |
| **Data freshness**   | Scheduled query refresh               | On-demand or workflow-triggered      | Connector extract or DirectQuery         |
| **Cost**             | SQL Warehouse DBUs                    | All-purpose cluster DBUs             | External tool licenses + Databricks DBUs |
| **Setup complexity** | Minimal                               | Minimal                              | Moderate (connector setup required)      |
| **Best for**         | Lightweight governed analytics        | Exploratory analysis                 | Complex enterprise reporting             |

---

### 1.4 Databricks SQL Architecture

```
DATABRICKS SQL ARCHITECTURE
──────────────────────────────────────────────────────────────────────
  BROWSER / API CLIENT
  ┌────────────────────────────────────────────────────────┐
  │  SQL Editor │ Dashboard Viewer │ Alert Manager │ API   │
  └────────────────────────┬───────────────────────────────┘
                           │ HTTPS / REST API
  ┌────────────────────────▼───────────────────────────────┐
  │            DATABRICKS CONTROL PLANE                     │
  │  - Query scheduler         - Permission enforcement     │
  │  - Dashboard renderer      - Alert evaluation           │
  │  - Result cache (72-hour)  - Unity Catalog catalog      │
  └────────────────────────┬───────────────────────────────┘
                           │ Internal
  ┌────────────────────────▼───────────────────────────────┐
  │            SQL WAREHOUSE (Compute Plane)                │
  │  Classic Warehouse         Serverless Warehouse         │
  │  - Fixed cluster size      - Auto-scales instantly      │
  │  - Manual start/stop       - Per-query billing          │
  │  - 2–5 min startup        - ~2 sec startup             │
  └────────────────────────┬───────────────────────────────┘
                           │
  ┌────────────────────────▼───────────────────────────────┐
  │            DATA LAYER (Unity Catalog + Delta Lake)      │
  │  catalog.gold.daily_sales   catalog.silver.customers    │
  │  S3 / ADLS (Parquet + Delta transaction log)           │
  └────────────────────────────────────────────────────────┘
```

---

## 2. Creating Interactive Dashboards

### 2.1 The SQL Editor

The **SQL Editor** is the authoring environment for all dashboard queries.

**Key features:**

- **Schema browser** (left panel) — browse Unity Catalog → Schema → Table → Column
- **Query history** — all queries you've run with execution time and row counts
- **Multiple tabs** — work on several queries simultaneously
- **Keyboard shortcuts** — `Ctrl+Enter` / `Cmd+Enter` to run, `Ctrl+Shift+F` to format SQL
- **Query settings** — attach to a specific SQL Warehouse

```sql
-- ── Basic dashboard query example ─────────────────────────────────
SELECT
    DATE_TRUNC('day', order_date)   AS order_day,
    region,
    product_category,
    COUNT(*)                         AS order_count,
    SUM(total_amount)                AS daily_revenue,
    COUNT(DISTINCT customer_id)      AS unique_customers,
    AVG(total_amount)                AS avg_order_value
FROM catalog.gold.daily_sales_summary
WHERE order_date >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY 1, 2, 3
ORDER BY 1 DESC, daily_revenue DESC;
```

---

### 2.2 Saved Queries

Save queries to reference them in dashboards and schedule them for refresh.

**Saving a query:**

1. Write the SQL in the SQL Editor
2. Click **Save** → Enter a name (use a descriptive convention, e.g., `[Dashboard] Revenue by Region - 30d`)
3. Optionally move into a folder (e.g., `/Finance/Revenue Dashboards/`)

> **Naming convention:** Prefix saved queries with the dashboard name they belong to. This makes finding and managing related queries easy as the number grows.

```sql
-- ── Saved query: revenue by region (30-day rolling) ───────────────
-- Name: [Revenue Dashboard] Revenue by Region - 30d
-- Warehouse: Shared SQL Warehouse
-- Schedule: Every hour

SELECT
    region,
    SUM(total_amount)           AS total_revenue,
    COUNT(*)                    AS order_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    AVG(total_amount)           AS avg_order_value,
    MAX(order_date)             AS last_order_date
FROM catalog.gold.orders_summary
WHERE order_date >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY region
ORDER BY total_revenue DESC;
```

---

### 2.3 Visualization Types

Each saved query can have one or more **visualizations** attached to it. Visualizations are created in the **Visualization** tab below the query results.

| Visualization Type    | Best For                                   | Key Configuration                                         |
| --------------------- | ------------------------------------------ | --------------------------------------------------------- |
| **Bar chart**         | Comparing categories                       | X axis (category), Y axis (metric), optional group by     |
| **Line chart**        | Time series trends                         | X axis (date/time), Y axis (metric), optional group by    |
| **Area chart**        | Cumulative trends, part-to-whole over time | Same as line; fill area below line                        |
| **Scatter plot**      | Correlation between two metrics            | X axis (metric 1), Y axis (metric 2), optional size/color |
| **Pie / Donut chart** | Part-to-whole: ≤ 7 slices                  | Slice column, value column                                |
| **Counter**           | Single KPI with optional comparison        | Value column, target value, prefix/suffix                 |
| **Pivot table**       | Cross-tab aggregations                     | Row, column, value fields                                 |
| **Map (Choropleth)**  | Geographic distributions                   | Country/state column, value column                        |
| **Box plot**          | Distribution of a metric                   | Group column, value column                                |
| **Table**             | Detailed row-level data with formatting    | All columns; conditional formatting rules                 |
| **Funnel**            | Conversion rate analysis                   | Stage column, count column                                |

**Creating a visualization:**

1. Run your saved query
2. Click **+ Add visualization** below the results
3. Choose the type, configure axes/columns
4. Give it a descriptive name (e.g., `Revenue by Region - Bar`)
5. Click **Save**

---

### 2.4 Creating a Dashboard and Adding Widgets

**Creating a dashboard:**

1. In the left sidebar, click **Dashboards** → **+ New Dashboard**
2. Enter a name (e.g., `Executive Revenue Dashboard - Q1 2026`)
3. Click **Add widget** → select a saved query and one of its visualizations

**Widget layout:**

- Drag and resize widgets on the grid canvas
- Use **Text Box** widgets for section headers, descriptions, and context notes
- Add **Filter** widgets linked to query parameters to enable global dashboard filters

```
EXAMPLE DASHBOARD LAYOUT
──────────────────────────────────────────────────────────────────
┌────────────────────────────────────────────────────────────────┐
│  Executive Revenue Dashboard — Q1 2026                         │
│  [Filter: Region ▼]  [Filter: Date Range ▼]                   │
├──────────┬──────────┬──────────┬────────────────────────────── │
│ COUNTER  │ COUNTER  │ COUNTER  │  LINE CHART                   │
│ $4.2M    │ 18,432   │ $228     │  Daily Revenue Trend          │
│ Revenue  │ Orders   │ AOV      │  (last 30 days)               │
├──────────┴──────────┴──────────┤                               │
│  BAR CHART                     │                               │
│  Revenue by Region             ├──────────────────────────────┤│
│                                │  TABLE                        ││
│                                │  Top 10 Products by Revenue   ││
├────────────────────────────────┤                               ││
│  MAP                           ├───────────────────────────────┤│
│  Revenue Choropleth by Country │  PIE CHART                    ││
│                                │  Revenue by Category          ││
└────────────────────────────────┴───────────────────────────────┘
```

---

### 2.5 Widget Parameters and Cross-Filtering

**Widget-level parameters** allow a visualization to respond to a filter input. When the same parameter name is used across multiple widgets on a dashboard, they all respond to a single filter control.

**Setting up cross-dashboard filtering:**

1. Open the saved query → add a `{{ date_range }}` parameter
2. In the dashboard, add the query as a widget
3. Open **Dashboard Filters** → map the widget parameter to a dashboard-level filter
4. Multiple widgets using `{{ date_range }}` will all update when the filter changes

```sql
-- ── Query wired to dashboard filter ───────────────────────────────
-- The {{ region }} and {{ start_date }} parameters become filter inputs
SELECT
    order_date,
    SUM(total_amount) AS daily_revenue,
    COUNT(*)          AS order_count
FROM catalog.gold.orders_summary
WHERE
    region     = '{{ region }}'        -- dropdown filter
    AND order_date >= '{{ start_date }}'   -- date picker filter
GROUP BY order_date
ORDER BY order_date;
```

---

## 3. Building Queries for Dashboards

### 3.1 Parameterized Queries with `{{ }}` Syntax

Databricks SQL supports **Handlebars-style** `{{ param_name }}` syntax for query parameters. When a query is executed in the SQL Editor or on a dashboard, parameter inputs appear as interactive controls above the results.

```sql
-- ── Text parameter ─────────────────────────────────────────────────
SELECT *
FROM catalog.silver.customers
WHERE country = '{{ country }}'
  AND status  = '{{ customer_status }}';

-- ── Numeric parameter ─────────────────────────────────────────────
SELECT
    product_id,
    product_name,
    unit_price,
    stock_qty
FROM catalog.silver.products
WHERE unit_price  <= {{ max_price }}
  AND stock_qty   >= {{ min_stock }};

-- ── Date parameter ─────────────────────────────────────────────────
SELECT
    order_date,
    SUM(total_amount)  AS revenue,
    COUNT(*)           AS orders
FROM catalog.gold.daily_sales
WHERE order_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY order_date
ORDER BY order_date;

-- ── Dropdown parameter — using a query-based dropdown ──────────────
-- A separate "Region List" query provides the dropdown values:
--    SELECT DISTINCT region FROM catalog.silver.orders ORDER BY 1;
-- Then this query references the dropdown:
SELECT *
FROM catalog.gold.orders_summary
WHERE region = '{{ region_filter }}';
```

**Parameter types available in Databricks SQL:**

| Parameter Type      | Widget Rendered                 | Example Value              |
| ------------------- | ------------------------------- | -------------------------- |
| **Text**            | Text input box                  | `"North America"`          |
| **Number**          | Numeric input                   | `1000`                     |
| **Dropdown List**   | Static or query-backed dropdown | `"APAC"`                   |
| **Date**            | Date picker                     | `2026-03-01`               |
| **Date and Time**   | Datetime picker                 | `2026-03-01 09:00:00`      |
| **Date Range**      | Two date pickers (start/end)    | `2026-01-01 to 2026-03-31` |
| **Date Time Range** | Two datetime pickers            | Full datetime range        |

---

### 3.2 Dropdown and Date Widget Filters

```sql
-- ── Step 1: Create the dropdown values query ───────────────────────
-- Save as: "[Filters] Region Dropdown Values"
SELECT DISTINCT region AS value, region AS name
FROM catalog.silver.orders
WHERE region IS NOT NULL
ORDER BY region;

-- ── Step 2: Main query referencing the dropdown ───────────────────
-- In the SQL Editor, click the {{ }} icon → Add parameter → choose
-- "Dropdown List" → set the source query to the values query above

SELECT
    product_category,
    COUNT(*)          AS order_count,
    SUM(total_amount) AS revenue,
    AVG(total_amount) AS avg_order_value
FROM catalog.gold.orders_summary
WHERE region     = '{{ region }}'
  AND order_date BETWEEN '{{ date_range.start }}' AND '{{ date_range.end }}'
GROUP BY product_category
ORDER BY revenue DESC;

-- ── Multi-select parameter (SQL IN clause pattern) ─────────────────
-- Note: multi-select values are rendered as comma-separated quoted strings
-- Need to use a workaround for IN clauses:
SELECT *
FROM catalog.silver.orders
WHERE FIND_IN_SET(region, '{{ regions_csv }}') > 0;
-- or with array param syntax (Databricks SQL AI/BI dashboards):
-- WHERE region IN ({{ regions }})
```

---

### 3.3 Query Snippets and Reusable Fragments

**Query snippets** let you define reusable SQL fragments referenced with `{{ snippet:name }}` syntax. Useful for shared date range filters, standard WHERE clauses, or common JOINs.

```sql
-- ── Define a snippet: "Standard Date Filter" ──────────────────────
-- Saved in Settings → Query Snippets → Name: "standard_date_filter"
order_date BETWEEN DATEADD(DAY, -{{ lookback_days }}, CURRENT_DATE()) AND CURRENT_DATE()

-- ── Use the snippet in a query ────────────────────────────────────
SELECT
    region,
    SUM(total_amount) AS revenue
FROM catalog.gold.daily_sales
WHERE {{ snippet:standard_date_filter }}
GROUP BY region;

-- ── Common JOIN snippet ───────────────────────────────────────────
-- Snippet name: "orders_customers_join"
catalog.silver.orders  o
JOIN catalog.silver.customers c ON o.customer_id = c.customer_id

-- Usage:
SELECT
    c.country,
    SUM(o.total_amount) AS revenue
FROM {{ snippet:orders_customers_join }}
WHERE o.order_date >= '{{ start_date }}'
GROUP BY c.country;
```

---

### 3.4 Multi-Query Dashboards

A dashboard can combine results from multiple independent queries into a cohesive view. Each widget on the dashboard is backed by its own saved query.

```sql
-- ── Query 1: Revenue KPI Counter ──────────────────────────────────
SELECT
    ROUND(SUM(total_amount) / 1e6, 2)  AS revenue_m,
    COUNT(*)                            AS order_count,
    ROUND(AVG(total_amount), 2)         AS avg_order_value
FROM catalog.gold.orders_summary
WHERE order_date >= DATEADD(MONTH, -1, CURRENT_DATE())
  AND region = '{{ region }}';

-- ── Query 2: Daily Trend Line ─────────────────────────────────────
SELECT
    order_date,
    SUM(total_amount) AS daily_revenue,
    LAG(SUM(total_amount), 7) OVER (ORDER BY order_date) AS revenue_7d_ago,
    SUM(total_amount)
        - LAG(SUM(total_amount), 7) OVER (ORDER BY order_date) AS wow_delta
FROM catalog.gold.orders_summary
WHERE order_date BETWEEN DATEADD(DAY, -60, CURRENT_DATE()) AND CURRENT_DATE()
  AND region = '{{ region }}'
GROUP BY order_date
ORDER BY order_date;

-- ── Query 3: Top Products Table ───────────────────────────────────
SELECT
    product_id,
    product_name,
    category,
    COUNT(*)          AS order_count,
    SUM(quantity)     AS units_sold,
    SUM(total_amount) AS revenue,
    RANK() OVER (ORDER BY SUM(total_amount) DESC) AS revenue_rank
FROM catalog.silver.order_items  oi
JOIN catalog.silver.products      p  ON oi.product_id = p.product_id
JOIN catalog.silver.orders        o  ON oi.order_id  = o.order_id
WHERE o.order_date >= DATEADD(DAY, -30, CURRENT_DATE())
  AND o.region = '{{ region }}'
GROUP BY product_id, product_name, category
ORDER BY revenue DESC
LIMIT 20;

-- ── Query 4: Regional Map ─────────────────────────────────────────
SELECT
    country_code,                              -- ISO-3166-2 for map
    country_name,
    SUM(total_amount)           AS revenue,
    COUNT(*)                    AS order_count,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM catalog.gold.orders_by_country
WHERE order_date >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY country_code, country_name
ORDER BY revenue DESC;
```

> **Tip:** Use the same parameter name (`{{ region }}`) across all queries on a dashboard. When you add a filter widget and map it to `region`, all four widgets update simultaneously.

---

## 4. Automating Updates with Scheduled Queries

### 4.1 Query Scheduling

Scheduling a query ensures its results are **pre-computed and cached** before users open the dashboard, giving instant load times without warehouse cold starts.

**Steps to schedule a query:**

1. Open the saved query in the SQL Editor
2. Click **Schedule** (next to the Run button)
3. Choose interval: **Never / Minutes / Hours / Days / Weeks / Custom (cron)**
4. Select the SQL Warehouse to use for the schedule
5. Click **Save Schedule**

| Schedule Option          | Cron Equivalent | Use Case                    |
| ------------------------ | --------------- | --------------------------- |
| Every 15 minutes         | `*/15 * * * *`  | Live operational metrics    |
| Every hour               | `0 * * * *`     | Hourly KPI dashboards       |
| Every day at 6 AM        | `0 6 * * *`     | Morning executive reports   |
| Every Monday at 7 AM     | `0 7 * * 1`     | Weekly business review prep |
| 1st of month at midnight | `0 0 1 * *`     | Monthly billing summaries   |

> **Important:** Query scheduling uses SQL Warehouse compute. Schedule aggressively only for dashboards that are actively viewed. Unused scheduled queries waste DBU cost.

---

### 4.2 Dashboard Auto-Refresh

In addition to per-query schedules, dashboards can be configured to **auto-refresh** in the viewer's browser.

**Setting dashboard auto-refresh:**

1. Open the dashboard in view mode
2. Click the **Refresh** icon (top right) → **Auto Refresh**
3. Select interval: 1 min / 5 min / 10 min / 30 min / 1 hr / 2 hr / Custom

> **Note:** Dashboard auto-refresh in the browser triggers the scheduled query to re-run (or uses cached results if the cache is fresh). It does **not** bypass the schedule — a query scheduled hourly will not re-run at 1-minute auto-refresh intervals; the cached result from the last scheduled run is served.

---

### 4.3 Refresh Schedule Options and Cost Considerations

```
REFRESH COST IMPACT MATRIX
────────────────────────────────────────────────────────────────
Schedule        Queries/day    Warehouse time   Monthly cost*
─────────────   ───────────    ──────────────   ────────────
Every 1 min     1,440          ~12 hrs           High
Every 15 min    96             ~0.8 hrs          Low-Medium
Every 1 hour    24             ~0.2 hrs          Low
Every 1 day     1              ~0.01 hrs         Minimal

*Assumes 2 XS SQL Warehouse nodes, 5-second average query time
```

**Cost optimization strategies:**

```sql
-- ── Optimize scheduled queries with pre-aggregated Gold tables ────
-- Instead of scanning raw Silver tables on every refresh,
-- pre-compute aggregates in a nightly Gold table:

-- ────── Nightly Gold table build (run via Workflows at 02:00 UTC) ──
CREATE OR REPLACE TABLE catalog.gold.daily_sales_summary AS
SELECT
    order_date,
    region,
    product_category,
    customer_segment,
    COUNT(*)                    AS order_count,
    SUM(total_amount)           AS revenue,
    COUNT(DISTINCT customer_id) AS unique_customers,
    AVG(total_amount)           AS avg_order_value,
    SUM(quantity)               AS units_sold
FROM catalog.silver.orders    o
JOIN catalog.silver.products  p ON o.product_id  = p.product_id
JOIN catalog.silver.customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= DATEADD(YEAR, -2, CURRENT_DATE())
GROUP BY 1, 2, 3, 4;

-- ────── Dashboard query (fast, reads Gold table only) ──────────────
SELECT
    order_date,
    SUM(revenue)         AS revenue,
    SUM(order_count)     AS orders,
    SUM(unique_customers) AS customers
FROM catalog.gold.daily_sales_summary
WHERE region        = '{{ region }}'
  AND order_date   >= '{{ start_date }}'
GROUP BY order_date
ORDER BY order_date;
```

---

### 4.4 Triggering Refreshes via the REST API

For pipelines that need to trigger a dashboard refresh after a data load completes, use the Databricks SQL REST API.

```bash
# ── Get the query ID from the Databricks SQL UI or API ─────────────
# Queries API format: GET /api/2.0/preview/sql/queries

# ── Trigger a query refresh ────────────────────────────────────────
curl -X POST \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  "https://<workspace_url>/api/2.0/preview/sql/queries/<query_id>/refresh"

# ── List all saved queries ─────────────────────────────────────────
curl -X GET \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  "https://<workspace_url>/api/2.0/preview/sql/queries?page_size=50"
```

```python
# ── Python wrapper for triggering a dashboard query refresh ────────
import requests

def refresh_dashboard_query(workspace_url: str, token: str, query_id: str) -> dict:
    """
    Trigger a refresh for a saved Databricks SQL query.
    Returns the API response.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }
    url = f"{workspace_url}/api/2.0/preview/sql/queries/{query_id}/refresh"
    response = requests.post(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()

# ── Use from a Databricks Workflow notebook ────────────────────────
workspace_url = "https://adb-1234567890.1.azuredatabricks.net"
token         = dbutils.secrets.get(scope="api", key="databricks_token")
query_id      = "abc123-def456-..."     # from SQL UI Query URL

result = refresh_dashboard_query(workspace_url, token, query_id)
print(f"Refresh triggered: {result}")
```

---

## 5. Sharing Dashboards with Controlled Access

### 5.1 Dashboard Permission Levels

Databricks SQL dashboards support three permission levels:

| Permission Level | Can View | Can Edit | Can Manage Permissions | Can Delete |
| ---------------- | -------- | -------- | ---------------------- | ---------- |
| **No Access**    | ✗        | ✗        | ✗                      | ✗          |
| **Can View**     | ✓        | ✗        | ✗                      | ✗          |
| **Can Edit**     | ✓        | ✓        | ✗                      | ✗          |
| **Can Manage**   | ✓        | ✓        | ✓                      | ✓          |

**Granting access:**

1. Open dashboard → Click **Share** (top right)
2. Search for user, group, or service principal
3. Select the permission level from the dropdown
4. Click **Add**
5. Optionally click **Copy link** to share the direct URL

---

### 5.2 Run As Owner vs. Run As Viewer

This is the most important access control decision for shared dashboards.

| Setting           | How Queries Run                                         | Use Case                                                 | Security Consideration                                                           |
| ----------------- | ------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------------------------------- |
| **Run As Owner**  | All viewers see data as if they are the dashboard owner | Serving data to users who don't have direct table access | Owner must have table access; queries run with owner's Unity Catalog permissions |
| **Run As Viewer** | Each viewer's own credentials used to run queries       | Personalized data views; per-user row-level security     | Every viewer must have individual table READ permission                          |

```
RUN AS OWNER — Data access model
────────────────────────────────────────────────────────────────────
 BI Analyst (owner)         Viewer A          Viewer B
 Has: SELECT on all         Has: Only          Has: No table
 catalog.gold tables        workspace access   access at all
      │                          │                  │
      │                          │                  │
      ▼                          ▼                  ▼
 ┌──────────────┐          ┌──────────────────────────┐
 │ Dashboard    │ shared   │ Both viewers see the same │
 │ "Run as      │ ──────▶  │ results using owner's     │
 │  Owner"      │          │ credentials               │
 └──────────────┘          └──────────────────────────┘
```

**Configuring Run As Owner:**

1. Open dashboard → **Share** → **Manage Permissions**
2. Find **Credentials** section
3. Toggle to **Run as owner** (default) or **Run as viewer**

> **Best practice:** Use **Run As Owner** for executive and operational dashboards where you want centralized data access control. Use **Run As Viewer** when Unity Catalog row-level security (`ROW FILTER`) needs to apply per-user.

---

### 5.3 Sharing via Link vs. Specific Users and Groups

```
SHARING OPTIONS
──────────────────────────────────────────────────────────────────
 Option              Access Scope        Authentication Required
 ─────────────────   ─────────────────   ─────────────────────
 Specific user       Named individual    Yes — Databricks login
 Group               All group members   Yes — Databricks login
 Service principal   Automation/API use  Yes — token
 Public link         Anyone with URL     No (if enabled by admin)
 Embedded iframe     External page       Depends on config
```

> **Security warning:** Public link sharing allows unauthenticated access to dashboard results. Enable this only for non-sensitive, publicly intended data. Verify with your organization's data classification policy before enabling public links. Workspace administrators can disable public link sharing globally in **Admin Console → Settings → Dashboard Embedding**.

---

### 5.4 Integration with Unity Catalog Permissions

Dashboard access control works **in conjunction** with Unity Catalog table permissions, not as a replacement for them.

**Permission chain:**

```
User clicks dashboard widget
   ↓
Dashboard "Run As Owner" credential used
   ↓
Unity Catalog checks: does the dashboard OWNER have SELECT on the table?
   ↓ If Yes                     ↓ If No
Results returned              Permission denied error shown

Dashboard "Run As Viewer" credential used
   ↓
Unity Catalog checks: does the VIEWER have SELECT on the table?
   ↓ If Yes                     ↓ If No
Results returned              Permission denied error shown
```

```sql
-- ── Grant table access for a dashboard service account ────────────
GRANT SELECT ON TABLE catalog.gold.daily_sales_summary
    TO `dashboard-service-account@company.com`;

GRANT SELECT ON TABLE catalog.gold.orders_by_country
    TO `dashboard-service-account@company.com`;

-- ── Or grant schema-wide access for all gold tables ───────────────
GRANT SELECT ON SCHEMA catalog.gold TO `dashboard-service-account@company.com`;

-- ── Use row-level security for per-viewer data filtering ──────────
-- Create a row filter:
CREATE FUNCTION catalog.security.region_filter(region STRING)
RETURNS BOOLEAN
RETURN IS_ACCOUNT_GROUP_MEMBER(region);  -- user must be in a group named by the region value

-- Apply row filter to table:
ALTER TABLE catalog.silver.orders
SET ROW FILTER catalog.security.region_filter ON (region);
-- Now "Run As Viewer" ensures each viewer sees only their region's data
```

---

## 6. Alerts: Notifications from Query Results

### 6.1 Creating Alerts on Saved Queries

**Alerts** monitor a saved query's result on a schedule and send a notification when a condition is met. They are ideal for data quality checks, SLA monitoring, and business KPI thresholds.

**Steps to create an alert:**

1. Navigate to **Alerts** in the left sidebar → **+ New Alert**
2. Select the **saved query** to monitor (must return at least one row with a numeric column)
3. Configure the **alert condition** (column, operator, threshold)
4. Set the **notification schedule** (how often to check)
5. Add **alert destinations** (email, Slack, webhook, PagerDuty)
6. Give the alert a name and click **Create Alert**

```sql
-- ── Alert query examples ───────────────────────────────────────────

-- Alert 1: Low inventory threshold
-- Alert when: stock_qty < 10 for any critical product
SELECT
    product_id,
    product_name,
    stock_qty,
    reorder_threshold
FROM catalog.silver.products
WHERE is_active = true
  AND stock_qty < reorder_threshold
ORDER BY stock_qty ASC;

-- Alert 2: Pipeline data freshness check
-- Alert when: max_ingest_time is more than 2 hours old
SELECT
    MAX(UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(_ingest_time)) / 3600
        AS hours_since_last_ingest,
    MAX(_ingest_time)  AS most_recent_ingest
FROM catalog.bronze.orders;

-- Alert 3: Fraud detection rate spike
-- Alert when: fraud_rate > 0.02 (2%)
SELECT
    ROUND(SUM(CASE WHEN is_fraud = true  THEN 1 ELSE 0 END) /
          COUNT(*), 4)  AS fraud_rate,
    COUNT(*)            AS total_transactions
FROM catalog.silver.transactions
WHERE txn_date = CURRENT_DATE();

-- Alert 4: Revenue drop alert
-- Alert when: today's revenue is < 50% of 7-day average
SELECT
    today_rev,
    avg_7d_rev,
    ROUND(today_rev / NULLIF(avg_7d_rev, 0), 3) AS revenue_ratio
FROM (
    SELECT
        SUM(CASE WHEN order_date = CURRENT_DATE() THEN total_amount END)  AS today_rev,
        AVG(total_amount)                                                  AS avg_7d_rev
    FROM catalog.gold.daily_sales_summary
    WHERE order_date >= DATEADD(DAY, -7, CURRENT_DATE())
) t;
```

---

### 6.2 Alert Condition Types

| Condition                 | Operator | Description                                 | Example                      |
| ------------------------- | -------- | ------------------------------------------- | ---------------------------- |
| **Greater than**          | `>`      | Alert when value exceeds threshold          | `fraud_rate > 0.02`          |
| **Greater than or equal** | `>=`     | Alert when value meets or exceeds threshold | `error_count >= 100`         |
| **Less than**             | `<`      | Alert when value drops below threshold      | `revenue_ratio < 0.5`        |
| **Less than or equal**    | `<=`     | Alert when value is at or below threshold   | `stock_qty <= 10`            |
| **Equal to**              | `=`      | Alert when value matches exactly            | `pipeline_status = 'FAILED'` |
| **Not equal to**          | `!=`     | Alert when value differs from expected      | `row_count != 0`             |

**Result conditions:**

| Condition                    | Meaning                                                           |
| ---------------------------- | ----------------------------------------------------------------- |
| **Value is above threshold** | Numeric column value > threshold                                  |
| **Value is below threshold** | Numeric column value < threshold                                  |
| **Value equals threshold**   | Numeric column value = threshold                                  |
| **Query has no results**     | Zero rows returned — good for "check that no bad data exists"     |
| **Query has results**        | One or more rows returned — good for "alert if any anomaly found" |

---

### 6.3 Alert Destinations

Alert destinations define **where** notifications are sent. Configure destinations in **Settings → Alert Destinations**.

**1. Email**

```
Configuration:
  - Subject: [Databricks Alert] {{ alert_name }}
  - Recipients: ops-team@company.com; data-eng@company.com
  - Message body: Optional custom template

Default email body includes:
  - Alert name
  - Alert condition that triggered
  - Query result value
  - Link to dashboard
```

**2. Slack**

```
Configuration:
  - Slack webhook URL (from Slack App → Incoming Webhooks)
  - Channel: #data-alerts
  - Message format: Databricks sends structured attachment

Setup steps:
  1. Create a Slack App at api.slack.com/apps
  2. Enable "Incoming Webhooks"
  3. Add webhook to workspace → copy URL
  4. Paste URL in Databricks Alert Destination
```

**3. Webhook (generic POST)**

```json
{
  "url": "https://hooks.example.com/notify",
  "username": "Databricks Alerts",
  "icon_emoji": ":bell:",
  "template": {
    "text": "Alert: {{ alert.name }} — Value: {{ result.value }}"
  }
}
```

**4. PagerDuty**

```
Configuration:
  - Integration key (from PagerDuty Service → Integrations → Databricks)
  - Severity: critical / error / warning / info
  - Automatically creates and resolves PagerDuty incidents
```

---

### 6.4 Alert Muting and Cooldown

**Cooldown / rearm period** prevents notification fatigue by suppressing repeated alerts for the same condition.

| Rearm Setting                    | Behavior                                                            |
| -------------------------------- | ------------------------------------------------------------------- |
| **Just once**                    | Send one notification, then never again (until manually re-enabled) |
| **Each time alert is evaluated** | Send notification every time the condition is true (noisy)          |
| **When value changes**           | Send when the value transitions from OK → ALERT state               |
| **Every N minutes**              | Re-notify every N minutes while condition remains true              |

> **Recommendation:** Set rearm to **"When value changes"** for most production alerts. This notifies once when the problem starts and once when it resolves, avoiding repetitive noise.

**Muting an alert:**

- Open the alert → Click the **bell icon** to mute
- Muted alerts still evaluate but do not send notifications
- Useful during planned maintenance windows

---

## 7. Best Practices and Real-World Dashboard Patterns

### 7.1 Executive KPI Dashboard Pattern

Executive dashboards need to be **fast, simple, and mobile-friendly.** They should answer "How are we doing?" in under 10 seconds.

```sql
-- ── Query 1: Revenue KPI counters (< 1 sec — Gold table) ──────────
SELECT
    ROUND(SUM(revenue) / 1e6, 2)         AS revenue_mtd_m,
    ROUND(SUM(revenue_prior_month) / 1e6, 2) AS revenue_prior_m,
    ROUND(
        (SUM(revenue) - SUM(revenue_prior_month))
        / NULLIF(SUM(revenue_prior_month), 0) * 100,
        1
    )                                     AS mom_growth_pct,
    SUM(order_count)                      AS orders_mtd,
    ROUND(SUM(revenue) / NULLIF(SUM(order_count), 0), 2) AS aov,
    SUM(unique_customers)                 AS customers_mtd
FROM catalog.gold.monthly_kpis
WHERE year_month = DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM');

-- ── Query 2: 12-month revenue trend ───────────────────────────────
SELECT
    year_month,
    SUM(revenue)   AS monthly_revenue,
    SUM(revenue)
        - LAG(SUM(revenue), 12) OVER (ORDER BY year_month)
                               AS yoy_delta
FROM catalog.gold.monthly_kpis
WHERE year_month >= DATE_FORMAT(DATEADD(MONTH, -12, CURRENT_DATE()), 'yyyy-MM')
GROUP BY year_month
ORDER BY year_month;

-- ── Query 3: Top 5 regions by MTD revenue ─────────────────────────
SELECT
    region,
    SUM(revenue) AS revenue_mtd,
    RANK() OVER (ORDER BY SUM(revenue) DESC) AS rank
FROM catalog.gold.daily_sales_summary
WHERE order_date >= DATE_TRUNC('MONTH', CURRENT_DATE())
GROUP BY region
ORDER BY revenue_mtd DESC
LIMIT 5;
```

**Executive dashboard layout principles:**

- Lead with 3–5 **Counter widgets** at the top (revenue, orders, AOV, churn rate, NPS)
- Follow with one **trend line** showing direction (last 12 months)
- Add a **regional breakdown** (bar or map)
- Keep total widget count to **8–12 maximum**
- Use **text widgets** for last-refreshed timestamps and metric definitions

---

### 7.2 Operational Monitoring Dashboard Pattern

Operational dashboards need to be **refreshed frequently** (every 5–15 min) and **alert immediately** on anomalies.

```sql
-- ── Query 1: Pipeline health overview ─────────────────────────────
SELECT
    pipeline_name,
    last_run_status,
    last_run_time,
    ROUND(
        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(last_run_time)) / 60,
        1
    )                  AS minutes_since_last_run,
    rows_processed,
    error_message
FROM catalog.monitoring.pipeline_runs
WHERE last_run_time >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
ORDER BY last_run_time DESC;

-- ── Query 2: Data freshness by table ──────────────────────────────
SELECT
    table_name,
    catalog_name,
    schema_name,
    MAX(last_modified_time)  AS last_updated,
    ROUND(
        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(last_modified_time))) / 3600,
        2
    )                        AS hours_stale,
    expected_refresh_hours,
    CASE
        WHEN ROUND(
            (UNIX_TIMESTAMP(CURRENT_TIMESTAMP())
             - UNIX_TIMESTAMP(MAX(last_modified_time))) / 3600, 2
        ) > expected_refresh_hours THEN 'STALE'
        ELSE 'FRESH'
    END                       AS freshness_status
FROM catalog.monitoring.table_refresh_log  r
JOIN catalog.monitoring.table_sla_config   s USING (table_name)
GROUP BY table_name, catalog_name, schema_name, expected_refresh_hours
ORDER BY hours_stale DESC;

-- ── Query 3: Error rate over last 4 hours ─────────────────────────
SELECT
    DATE_TRUNC('HOUR', event_time) AS hour_bucket,
    event_source,
    COUNT(*) FILTER (WHERE severity = 'ERROR')   AS error_count,
    COUNT(*) FILTER (WHERE severity = 'WARNING') AS warning_count,
    COUNT(*)                                      AS total_events
FROM catalog.monitoring.application_logs
WHERE event_time >= DATEADD(HOUR, -4, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1 DESC, error_count DESC;

-- ── Query 4: SLA breach alert query ───────────────────────────────
-- (Attach alert: "error_count > 0")
SELECT
    COUNT(*) AS error_count,
    MAX(last_error_time) AS last_error
FROM catalog.monitoring.pipeline_runs
WHERE last_run_status = 'FAILED'
  AND last_run_time   >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP());
```

---

### 7.3 Performance Tips: Warehouse Sizing and Result Caching

**SQL Warehouse sizing:**

| Warehouse Size | vCPUs | RAM    | Best For                                      |
| -------------- | ----- | ------ | --------------------------------------------- |
| **2X-Small**   | 4     | 16 GB  | Light queries, < 1 GB datasets, BI dashboards |
| **X-Small**    | 8     | 32 GB  | Standard dashboard queries, < 10 GB scans     |
| **Small**      | 16    | 64 GB  | Complex joins, 10–100 GB scans                |
| **Medium**     | 32    | 128 GB | Heavy aggregations, 100 GB–1 TB               |
| **Large**      | 64    | 256 GB | Very large datasets, concurrent users         |

> **Recommendation:** Start with **X-Small** or **Small** for most dashboard workloads. Use **Serverless SQL Warehouse** for variable usage patterns — it scales in seconds and you only pay per query second, eliminating idle cluster cost.

**Result caching:**

Databricks SQL caches query results for up to **72 hours**. Dashboard viewers see cached results instantly if the cache is valid.

```sql
-- ── Check if a query uses cached results ──────────────────────────
-- Open the query in SQL Editor → click the "i" icon on the result
-- If the result shows "Query result from cache", no warehouse time was used

-- ── Force a cache refresh ─────────────────────────────────────────
-- Click the circular "Refresh" arrow next to Run — bypasses the cache

-- ── Delta table result caching ────────────────────────────────────
-- Databricks SQL also maintains per-table result cache
-- When the Delta table changes (new commit), the cache is automatically invalidated
-- Queries against unchanged tables always hit cache — very cost-efficient

-- ── Optimize with OPTIMIZE + ZORDER for fast scan queries ────────
OPTIMIZE catalog.gold.daily_sales_summary ZORDER BY (region, order_date);
ANALYZE TABLE catalog.gold.daily_sales_summary COMPUTE STATISTICS FOR ALL COLUMNS;
```

---

### 7.4 Dashboard Governance and Lifecycle Management

```sql
-- ── Audit who viewed a dashboard (Unity Catalog system tables) ────
SELECT
    user_identity.email        AS user_email,
    request_params.dashboard_id,
    action_name,
    event_time
FROM system.access.audit
WHERE action_name   IN ('viewDashboard', 'runQuery')
  AND event_time    >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
ORDER BY event_time DESC;

-- ── Find unused dashboards (not viewed in 60 days) ────────────────
SELECT
    d.id             AS dashboard_id,
    d.name           AS dashboard_name,
    d.created_at,
    d.updated_at,
    MAX(a.event_time) AS last_viewed
FROM system.access.audit   a
RIGHT JOIN information_schema.dashboards d
    ON a.request_params.dashboard_id = d.id
    AND a.action_name = 'viewDashboard'
GROUP BY d.id, d.name, d.created_at, d.updated_at
HAVING MAX(a.event_time) < DATEADD(DAY, -60, CURRENT_DATE())
    OR MAX(a.event_time) IS NULL
ORDER BY last_viewed ASC NULLS FIRST;
```

**Dashboard lifecycle best practices:**

| Practice              | Recommendation                                                                       |
| --------------------- | ------------------------------------------------------------------------------------ |
| **Naming convention** | `[Team] Dashboard Name — Scope` e.g., `[Finance] Revenue KPIs — MTD`                 |
| **Folder structure**  | Organize by team: `/Finance/`, `/Engineering/`, `/Marketing/`                        |
| **Stale dashboards**  | Archive or delete dashboards with no views in 90+ days                               |
| **Query ownership**   | Each saved query has a designated owner responsible for SLA                          |
| **Change management** | Communicate schedule changes to downstream dashboard users                           |
| **Documentation**     | Add a text widget describing data sources, refresh frequency, and metric definitions |
| **Version control**   | Export dashboard JSON via REST API and commit to Git                                 |

```bash
# ── Export a dashboard definition via REST API ─────────────────────
curl -X GET \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  "https://<workspace_url>/api/2.0/preview/sql/dashboards/<dashboard_id>" \
  | python3 -m json.tool > dashboard_revenue_kpi_backup.json

# ── Import a dashboard from JSON ──────────────────────────────────
curl -X POST \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d @dashboard_revenue_kpi_backup.json \
  "https://<workspace_url>/api/2.0/preview/sql/dashboards"
```

---

## 8. Summary

| Section                                       | Key Points                                                                                                                                                                                                                                                                         |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Introduction to Databricks SQL Dashboards** | Databricks SQL is the SQL-native analytics surface with SQL Editor, SQL Warehouses, Dashboards, and Alerts; it integrates with Unity Catalog for governed access; dashboards serve pre-computed results from saved queries                                                         |
| **Creating Interactive Dashboards**           | Build queries in the SQL Editor, attach multiple visualizations (bar, line, counter, map, table), compose dashboards by adding visualization widgets; use the same `{{ param }}` name across widgets for cross-filtering                                                           |
| **Building Queries for Dashboards**           | Use `{{ param }}` Handlebars syntax for text, number, dropdown, date, and date-range parameters; query snippets enable reusable SQL fragments; multi-query dashboards with a shared parameter update all widgets simultaneously                                                    |
| **Automating Updates with Scheduled Queries** | Schedule individual queries on a cron-like interval; dashboard auto-refresh serves cached results in the browser; use pre-aggregated Gold tables to minimize warehouse compute on scheduled refreshes; trigger refreshes via the REST API from Workflows                           |
| **Sharing Dashboards with Controlled Access** | Three permission levels: Can View / Can Edit / Can Manage; **Run As Owner** serves data using the owner's credentials for viewers without table access; **Run As Viewer** applies per-user Unity Catalog row-level security; avoid public links for sensitive data                 |
| **Alerts**                                    | Alerts monitor a saved query result on a schedule; conditions include >, <, =, !=, has results, no results; destinations: email, Slack, generic webhook, PagerDuty; configure rearm to "When value changes" to avoid notification fatigue                                          |
| **Best Practices**                            | Executive dashboards: 3–5 counters + trend line, max 12 widgets, Gold table backed; operational dashboards: 5–15 min refresh, alert on SLA breach; use Serverless Warehouse for variable load; Delta result cache serves unchanged data instantly; audit via `system.access.audit` |

---
