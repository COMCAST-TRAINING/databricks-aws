# Build Data Pipelines with Delta Live Tables

---

## Table of Contents

1. [Introduction to Delta Live Tables](#1-introduction-to-delta-live-tables)
   - 1.1 [What Is Delta Live Tables?](#11-what-is-delta-live-tables)
   - 1.2 [The Problem DLT Solves](#12-the-problem-dlt-solves)
   - 1.3 [DLT Architecture and Components](#13-dlt-architecture-and-components)
   - 1.4 [DLT Pipeline Lifecycle](#14-dlt-pipeline-lifecycle)
2. [Introduction to Spark Declarative Pipeline](#2-introduction-to-spark-declarative-pipeline)
   - 2.1 [What Is Spark Declarative Pipeline?](#21-what-is-spark-declarative-pipeline)
   - 2.2 [Declarative vs Imperative Comparison](#22-declarative-vs-imperative-comparison)
   - 2.3 [Core Decorators and Annotations](#23-core-decorators-and-annotations)
   - 2.4 [Writing Your First Declarative Pipeline](#24-writing-your-first-declarative-pipeline)
3. [Delta Live Tables vs Spark Declarative Pipeline](#3-delta-live-tables-vs-spark-declarative-pipeline)
   - 3.1 [Feature Comparison Matrix](#31-feature-comparison-matrix)
   - 3.2 [When to Use DLT](#32-when-to-use-dlt)
   - 3.3 [When to Use Spark Declarative Pipeline](#33-when-to-use-spark-declarative-pipeline)
   - 3.4 [Migration Path: DLT → Spark Declarative Pipeline](#34-migration-path-dlt--spark-declarative-pipeline)
4. [Medallion Architecture in Detail](#4-medallion-architecture-in-detail)
   - 4.1 [Bronze Layer — Raw Ingestion](#41-bronze-layer--raw-ingestion)
   - 4.2 [Silver Layer — Validated and Enriched](#42-silver-layer--validated-and-enriched)
   - 4.3 [Gold Layer — Business-Ready Aggregates](#43-gold-layer--business-ready-aggregates)
   - 4.4 [Cross-Layer Design Patterns](#44-cross-layer-design-patterns)
   - 4.5 [Naming Conventions and Catalogue Organisation](#45-naming-conventions-and-catalogue-organisation)
5. [Creating and Managing Delta Live Tables / Declarative Pipelines](#5-creating-and-managing-delta-live-tables--declarative-pipelines)
   - 5.1 [Creating a DLT Pipeline in the UI](#51-creating-a-dlt-pipeline-in-the-ui)
   - 5.2 [Streaming Tables vs Materialised Views](#52-streaming-tables-vs-materialised-views)
   - 5.3 [Reading from External Sources](#53-reading-from-external-sources)
   - 5.4 [Building the Medallion with DLT — Full Example](#54-building-the-medallion-with-dlt--full-example)
   - 5.5 [Data Quality with Expectations](#55-data-quality-with-expectations)
   - 5.6 [Schema Management in DLT](#56-schema-management-in-dlt)
   - 5.7 [Python vs SQL in DLT](#57-python-vs-sql-in-dlt)
6. [Automating Data Processing with Delta Live Tables / Declarative Pipelines](#6-automating-data-processing-with-delta-live-tables--declarative-pipelines)
   - 6.1 [Continuous vs Triggered Pipeline Modes](#61-continuous-vs-triggered-pipeline-modes)
   - 6.2 [Scheduling DLT Pipelines via Workflows](#62-scheduling-dlt-pipelines-via-workflows)
   - 6.3 [Full Refresh vs Incremental Refresh](#63-full-refresh-vs-incremental-refresh)
   - 6.4 [Parameterizing DLT Pipelines](#64-parameterizing-dlt-pipelines)
   - 6.5 [Multi-Environment Deployments (Dev / Staging / Prod)](#65-multi-environment-deployments-dev--staging--prod)
7. [Best Practices for Managing Data Pipelines with Delta Live Tables / Declarative Pipelines](#7-best-practices-for-managing-data-pipelines-with-delta-live-tables--declarative-pipelines)
   - 7.1 [Pipeline Code Organisation](#71-pipeline-code-organisation)
   - 7.2 [Testing DLT Pipelines](#72-testing-dlt-pipelines)
   - 7.3 [Monitoring and Observability](#73-monitoring-and-observability)
   - 7.4 [Performance Optimisation](#74-performance-optimisation)
   - 7.5 [Cost Management](#75-cost-management)
   - 7.6 [Error Recovery Patterns](#76-error-recovery-patterns)
   - 7.7 [Security and Access Control](#77-security-and-access-control)
8. [Summary](#8-summary)

---

## 1. Introduction to Delta Live Tables

### 1.1 What Is Delta Live Tables?

**Delta Live Tables (DLT)** is a declarative ETL framework built on top of Apache Spark and Delta Lake in Databricks. Instead of writing imperative code that specifies _how_ to move data step by step, DLT lets you define _what_ the data should look like — and the framework handles execution, orchestration, error recovery, and data quality automatically.

```
Traditional ETL (Imperative):                   DLT (Declarative):
─────────────────────────────                   ──────────────────
Step 1: Read raw CSV from S3                    Define: "silver_orders is the
Step 2: Cast column types                        raw_orders table, cleaned,
Step 3: Filter nulls                             typed, and deduplicated"
Step 4: Deduplicate by order_id
Step 5: MERGE into silver table                 DLT figures out:
Step 6: Handle schema evolution                  • How to execute it
Step 7: Track checkpoint                         • Whether to stream or batch
Step 8: Retry on failure                         • Ordering of dependencies
                                                 • Schema management
                                                 • Checkpointing
                                                 • Retries
```

DLT notebooks/scripts define a **pipeline** — a DAG of datasets (tables and views) connected by transformation logic. The DLT engine compiles this DAG, determines execution order, and runs it as a managed infrastructure.

---

### 1.2 The Problem DLT Solves

Before DLT, every Databricks ETL pipeline required engineers to manually handle:

| Concern                | Manual Work                         | DLT Handles It                        |
| ---------------------- | ----------------------------------- | ------------------------------------- |
| Dependency ordering    | Manually sequence notebook tasks    | Automatic DAG inference               |
| Incremental processing | Implement checkpoint logic          | Built-in via Streaming Tables         |
| Schema evolution       | Add `.option("mergeSchema","true")` | Automatic                             |
| Data quality           | Write custom assert code            | Declarative `@dlt.expect` constraints |
| Error handling         | Try/except + retry decorators       | Automatic retry + quarantine          |
| Monitoring             | Custom metrics tables               | Built-in lineage graph + event log    |
| Re-processing          | Drop table and re-run               | `full_refresh` flag                   |
| Cluster lifecycle      | Job cluster per task                | Single managed cluster per pipeline   |

---

### 1.3 DLT Architecture and Components

```
┌────────────────────────────────────────────────────────────────────┐
│                    DLT Pipeline                                     │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │   Pipeline Notebooks / Python Files (user-authored)          │  │
│  │   @dlt.table, @dlt.view, @dlt.expect                        │  │
│  └──────────────────────────┬───────────────────────────────────┘  │
│                             │ compiled                              │
│  ┌──────────────────────────▼───────────────────────────────────┐  │
│  │          DLT Execution Engine                                │  │
│  │  ┌────────────────┐  ┌──────────────────┐  ┌─────────────┐  │  │
│  │  │  DAG Compiler  │  │  Schema Manager  │  │  Data Qual  │  │  │
│  │  │  (orders tasks)│  │  (creates tables)│  │  Enforcer   │  │  │
│  │  └────────────────┘  └──────────────────┘  └─────────────┘  │  │
│  │  ┌────────────────┐  ┌──────────────────┐  ┌─────────────┐  │  │
│  │  │  Checkpoint    │  │  Event Log       │  │  Auto Retry │  │  │
│  │  │  Manager       │  │  (audit trail)   │  │  Handler    │  │  │
│  │  └────────────────┘  └──────────────────┘  └─────────────┘  │  │
│  └────────────────────────────────────────────────────────────--┘  │
│                             │ writes                                │
│  ┌──────────────────────────▼───────────────────────────────────┐  │
│  │     Delta Lake Tables (in Unity Catalog or DBFS)             │  │
│  │     pipeline_catalog.bronze.* | .silver.* | .gold.*          │  │
│  └──────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────┘
```

**Key DLT components:**

| Component             | Description                                                              |
| --------------------- | ------------------------------------------------------------------------ |
| **Pipeline**          | A collection of notebooks that define a complete data flow               |
| **Streaming Table**   | An always-growing table fed by an append-only stream (Autoloader, Kafka) |
| **Materialised View** | A derived table that DLT automatically recomputes when upstream changes  |
| **Live View**         | An ephemeral view (not persisted) used within the pipeline only          |
| **Expectation**       | A named data quality rule — defines what to do when rows violate it      |
| **Event Log**         | Built-in table recording DLT operational events for monitoring           |
| **Pipeline Graph**    | Visual DAG showing dataset lineage in the DLT UI                         |

---

### 1.4 DLT Pipeline Lifecycle

```
Pipeline Lifecycle:
  INITIALIZING  → Cluster starts, pipeline code is parsed and compiled
  SETTING_UP    → DLT validates dataset definitions and creates target tables
  RUNNING       → Executing the pipeline (processing data)
       │
       ├── UPDATE_COMPLETE → All datasets are up to date (triggered mode)
       │
       └── WAITING        → Idle, waiting for new data (continuous mode)

Failed states:
  FAILED         → Pipeline encountered an unrecoverable error
  STOPPING       → Clean shutdown in progress

User-initiated:
  RESETTING      → Full refresh requested — all datasets recomputed from scratch
```

---

## 2. Introduction to Spark Declarative Pipeline

### 2.1 What Is Spark Declarative Pipeline?

**Spark Declarative Pipeline** is the evolution and rebranding of DLT announced by Databricks in 2024. It extends the declarative model beyond DLT's managed environment by:

1. **Supporting standard Spark clusters** — not just DLT clusters.
2. **Deepening Unity Catalog integration** — full UC governance by default.
3. **Removing the DLT cluster surcharge** — pipelines run on regular job clusters.
4. **Keeping the same `dlt` Python API** — backward compatible with existing DLT code.

```
DLT (Classic):
  ─ DLT-specific cluster with DLT surcharge
  ─ Optional Unity Catalog integration
  ─ Pipeline-managed storage

Spark Declarative Pipeline (New):
  ─ Standard Databricks cluster (no surcharge)
  ─ Unity Catalog required
  ─ Tables stored as standard Delta Lake tables in a UC schema
  ─ Same dlt Python API
```

The programming model is identical — you write `@dlt.table`, `@dlt.view`, and `@dlt.expect` the same way. The execution environment and cost model differ.

---

### 2.2 Declarative vs Imperative Comparison

```python
# ─── IMPERATIVE APPROACH (Notebook ETL) ─────────────────────────────────────

# Step 1 – Read
raw_df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/checkpoints/orders/schema") \
    .load("s3://bucket/raw/orders/")

# Step 2 – Transform
silver_df = raw_df \
    .withColumn("order_date", to_date(col("order_date"))) \
    .withColumn("quantity",   col("quantity").cast("integer")) \
    .filter("order_id IS NOT NULL") \
    .dropDuplicates(["order_id"])

# Step 3 – Write
silver_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/orders/silver") \
    .option("mergeSchema", "true") \
    .toTable("silver.orders") \
    .awaitTermination()
```

```python
# ─── DECLARATIVE APPROACH (DLT / Spark Declarative Pipeline) ─────────────────

import dlt
from pyspark.sql.functions import col, to_date

# Define the Bronze table (DLT manages the stream, checkpoint, and schema)
@dlt.table(name="raw_orders", comment="Raw orders from S3 — Bronze layer")
def raw_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/pipelines/orders/schema")
            .load("s3://bucket/raw/orders/")
    )

# Define the Silver table — DLT sees the dependency automatically
@dlt.table(name="silver_orders", comment="Cleaned and validated orders — Silver layer")
@dlt.expect("valid_order_id",  "order_id IS NOT NULL")
@dlt.expect("positive_quantity", "quantity > 0")
def silver_orders():
    return (
        dlt.read_stream("raw_orders")
            .withColumn("order_date", to_date(col("order_date")))
            .withColumn("quantity",   col("quantity").cast("integer"))
            .dropDuplicates(["order_id"])
    )
```

The declarative approach is shorter, more readable, and DLT handles the streaming plumbing (checkpoints, schema, retries) automatically.

---

### 2.3 Core Decorators and Annotations

```python
import dlt
from pyspark.sql.functions import col, current_timestamp

# ── @dlt.table ──────────────────────────────────────────────────────────────
# Creates a persisted Delta table managed by DLT/Spark Declarative Pipeline
@dlt.table(
    name        = "orders_bronze",          # table name (defaults to function name)
    comment     = "Raw order events",       # table description (shows in Unity Catalog)
    table_properties = {
        "quality":                     "bronze",
        "delta.autoOptimize.optimizeWrite": "true"
    },
    partition_cols = ["order_date"],        # partition the table
    schema      = "order_id STRING, customer_id STRING, amount DOUBLE, order_date DATE"
)
def orders_bronze():
    # Return a DataFrame or streaming DataFrame
    return spark.readStream.format("cloudFiles") \
                           .option("cloudFiles.format", "json") \
                           .option("cloudFiles.schemaLocation", "/dlt/schema/orders") \
                           .load("s3://bucket/raw/orders/")


# ── @dlt.view ────────────────────────────────────────────────────────────────
# Creates a temporary view (not persisted as a table); only visible within the pipeline
@dlt.view(name="orders_with_tax")
def orders_with_tax():
    return dlt.read("orders_bronze").withColumn("tax", col("amount") * 0.08)


# ── @dlt.expect ──────────────────────────────────────────────────────────────
# Data quality rule — WARN (default): log violations but keep the row
@dlt.table
@dlt.expect("not_null_order_id", "order_id IS NOT NULL")
def orders_silver():
    return dlt.read_stream("orders_bronze")


# ── @dlt.expect_or_drop ──────────────────────────────────────────────────────
# Data quality rule — DROP: remove violating rows and log them
@dlt.table
@dlt.expect_or_drop("positive_amount", "amount > 0")
def orders_silver_strict():
    return dlt.read_stream("orders_bronze")


# ── @dlt.expect_or_fail ──────────────────────────────────────────────────────
# Data quality rule — FAIL: if ANY row violates, stop the pipeline
@dlt.table
@dlt.expect_or_fail("valid_date", "order_date >= '2020-01-01'")
def orders_validated():
    return dlt.read_stream("orders_bronze")


# ── @dlt.expect_all ─────────────────────────────────────────────────────────
# Apply multiple rules with the same action
rules = {
    "not_null_id":     "order_id IS NOT NULL",
    "positive_amount": "amount > 0",
    "valid_status":    "status IN ('shipped', 'pending', 'cancelled')"
}

@dlt.table
@dlt.expect_all(rules)                   # WARN for all
def orders_quality_checked():
    return dlt.read_stream("orders_bronze")


# ── @dlt.expect_all_or_drop ──────────────────────────────────────────────────
@dlt.table
@dlt.expect_all_or_drop(rules)           # DROP rows violating any rule
def orders_clean():
    return dlt.read_stream("orders_bronze")
```

---

### 2.4 Writing Your First Declarative Pipeline

```python
# ──────────────────────────────────────────────────────────────────────────────
# File: pipelines/orders_pipeline.py
# A complete Bronze → Silver → Gold pipeline in ~60 lines
# ──────────────────────────────────────────────────────────────────────────────

import dlt
from pyspark.sql.functions import col, to_date, when, trim, lower, current_timestamp, sum, count

# ── Configuration (injected via Pipeline Configuration) ──
catalog = spark.conf.get("pipeline.catalog",     "dev")
source  = spark.conf.get("pipeline.source_path", "s3://my-bucket/raw/orders/")


# ── BRONZE: Raw ingestion ─────────────────────────────────────────────────────
@dlt.table(
    name    = "orders_bronze",
    comment = "Raw orders — Bronze layer. All raw fields preserved."
)
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",         "csv")
            .option("cloudFiles.schemaLocation", f"/dlt/{catalog}/schema/orders")
            .option("cloudFiles.inferColumnTypes","true")
            .option("header",                    "true")
            .load(source)
            .withColumn("_ingested_at", current_timestamp())
    )


# ── SILVER: Cleaned and validated ────────────────────────────────────────────
@dlt.table(
    name    = "orders_silver",
    comment = "Validated, typed, and deduplicated orders — Silver layer."
)
@dlt.expect_or_drop("valid_order_id",    "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_quantity", "CAST(quantity AS INT) > 0")
@dlt.expect("valid_date",                "order_date IS NOT NULL")
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
            .withColumn("order_id",    trim(col("order_id")))
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("quantity",    col("quantity").cast("integer"))
            .withColumn("unit_price",  col("unit_price").cast("double"))
            .withColumn("order_date",  to_date(col("order_date"), "yyyy-MM-dd"))
            .withColumn("total",       col("quantity") * col("unit_price"))
            .withColumn("status",      lower(trim(col("status"))))
            .withColumn("status",
                when(col("status").isin(["shipped", "dispatched"]), "shipped")
                .when(col("status").isin(["pending", "open"]),       "pending")
                .when(col("status").isin(["cancelled", "canceled"]), "cancelled")
                .otherwise(col("status"))
            )
            .drop("_ingested_at")
    )


# ── GOLD: Daily revenue aggregation ─────────────────────────────────────────
@dlt.table(
    name    = "daily_revenue",
    comment = "Daily revenue by status — Gold layer. Updated on each pipeline run."
)
def daily_revenue():
    return (
        dlt.read("orders_silver")       # batch read (materialised view)
            .filter("status = 'shipped'")
            .groupBy("order_date", "status")
            .agg(
                count("*").alias("order_count"),
                sum("total").alias("revenue")
            )
    )
```

---

## 3. Delta Live Tables vs Spark Declarative Pipeline

### 3.1 Feature Comparison Matrix

| Feature                       | Delta Live Tables (Classic) | Spark Declarative Pipeline  |
| ----------------------------- | :-------------------------: | :-------------------------: |
| **Programming API**           |        `import dlt`         |     `import dlt` (same)     |
| **Python support**            |              ✓              |              ✓              |
| **SQL support**               |              ✓              |              ✓              |
| **Streaming Tables**          |              ✓              |              ✓              |
| **Materialised Views**        |              ✓              |              ✓              |
| **Data Quality Expectations** |              ✓              |              ✓              |
| **Automatic DAG resolution**  |              ✓              |              ✓              |
| **Unity Catalog**             |          Optional           |          Required           |
| **Cluster type**              |    DLT-specific cluster     | Standard Databricks cluster |
| **DLT surcharge**             |    Yes ($0.36/DBU extra)    |             No              |
| **Available on**              |  Standard + Premium tiers   |       Premium tier +        |
| **External data sources**     |              ✓              |              ✓              |
| **Event log**                 |              ✓              |              ✓              |
| **Pipeline UI**               |              ✓              |              ✓              |
| **Repair / partial runs**     |              ✓              |              ✓              |

---

### 3.2 When to Use DLT

Use **classic DLT** when:

- Your workspace uses the **Standard tier** (Spark Declarative Pipeline requires Premium+).
- You need **self-contained pipeline storage** managed by DLT (not Unity Catalog).
- You are migrating from older DLT pipelines and do not want to refactor yet.
- You are using **Serverless DLT** (available on DLT, not yet on all Declarative Pipeline deployments).

---

### 3.3 When to Use Spark Declarative Pipeline

Use **Spark Declarative Pipeline** when:

- Your workspace is on **Premium or above** with Unity Catalog enabled.
- You want **lower cost** (no DLT surcharge; standard DBU pricing).
- You want **full Unity Catalog governance** — column-level security, lineage, audit.
- Your tables need to coexist with other Delta Lake tables in the same UC schema.
- You are building **new** pipelines and want the latest recommended approach.

---

### 3.4 Migration Path: DLT → Spark Declarative Pipeline

The migration is largely mechanical because the `dlt` Python API is identical:

```
1. Enable Unity Catalog on your workspace.

2. Create a new Databricks Pipeline (Pipelines → Create Pipeline) and set:
     Storage mode: Unity Catalog
     Target catalog and schema: <your_catalog>.<your_schema>

3. Point the pipeline to your existing DLT notebook(s).
   No code changes needed for basic pipelines.

4. Optional: remove `dlt.pipeline_target()` calls if using UC schema.

5. Run a Full Refresh on the new pipeline.

6. Decommission the old DLT pipeline after validation.
```

The main changes:

- Tables are now registered as standard UC tables (queryable by all UC-enabled tools).
- No more pipeline-managed storage path — tables live in a standard UC schema.

---

## 4. Medallion Architecture in Detail

### 4.1 Bronze Layer — Raw Ingestion

The **Bronze layer** is the landing zone for all raw data. The golden rule: **preserve everything, transform nothing**.

```
Design Principles for Bronze:
  ✓ Append-only (never update or delete)
  ✓ Store data in its original form (no business logic applied)
  ✓ Add ingestion metadata: _ingested_at, _source_file, _source_system
  ✓ Enforce schema only if a schema is provided by the source
  ✓ Store data that is wrong — quarantine later (in Silver)
  ✓ Partition by ingestion date for efficient time-based queries
```

```python
# DLT Bronze — Autoloader from S3
@dlt.table(
    name              = "orders_raw",
    comment           = "Raw orders from S3 — unmodified Bronze copy",
    partition_cols    = ["_source_date"],
    table_properties  = {"quality": "bronze", "delta.autoOptimize.autoCompact": "true"}
)
def orders_raw():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",         "json")
            .option("cloudFiles.schemaLocation", "/dlt/schema/orders_raw")
            .option("cloudFiles.inferColumnTypes","false")   # strings only in Bronze
            .load("s3://my-bucket/raw/orders/")
            .withColumn("_ingested_at",    current_timestamp())
            .withColumn("_source_file",    col("_metadata.file_path"))
            .withColumn("_source_date",    col("_metadata.file_modification_time").cast("date"))
            .withColumn("_pipeline_name",  lit("orders_medallion"))
    )
```

---

### 4.2 Silver Layer — Validated and Enriched

The **Silver layer** is the single source of truth for operational data. It is:

- Typed and cleaned.
- Deduplicated.
- Joined with reference data (customers, products, geolocation).
- Has business rules applied.
- Free of PII or has PII masked/hashed.

```
Design Principles for Silver:
  ✓ Cast all columns to correct types
  ✓ Normalise string values (trim, lowercase)
  ✓ Remove duplicates using the natural business key
  ✓ Join with slowly-changing reference tables
  ✓ Apply business validation rules as DLT expectations
  ✓ Remove Bronze metadata columns
  ✓ Use MERGE (streaming tables handle this automatically via Spark Streaming)
```

```python
# DLT Silver — cleaned orders joined with customer reference
@dlt.table(
    name             = "orders_silver",
    comment          = "Validated and enriched orders — Silver layer",
    table_properties = {"quality": "silver", "delta.enableChangeDataFeed": "true"}
)
@dlt.expect_all_or_drop({
    "valid_order_id":    "order_id IS NOT NULL",
    "positive_quantity": "quantity > 0",
    "non_negative_total":"total >= 0",
    "valid_status":      "status IN ('shipped','pending','cancelled','returned')"
})
def orders_silver():
    orders  = dlt.read_stream("orders_raw")
    # Join with customer dimension (batch read — always latest)
    customers = dlt.read("customers_dim")

    return (
        orders
            .withColumn("order_id",    trim(col("order_id")))
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("product_id",  upper(trim(col("product_id"))))
            .withColumn("quantity",    col("quantity").cast("integer"))
            .withColumn("unit_price",  col("unit_price").cast("double"))
            .withColumn("order_date",  to_date(col("order_date"), "yyyy-MM-dd"))
            .withColumn("total",       col("quantity") * col("unit_price"))
            .withColumn("status",      lower(trim(col("status"))))
            .join(customers.select("customer_id", "country", "region", "tier"),
                  on="customer_id", how="left")
            .drop("_ingested_at", "_source_file", "_source_date", "_pipeline_name")
    )
```

---

### 4.3 Gold Layer — Business-Ready Aggregates

The **Gold layer** contains pre-aggregated, purpose-built tables optimised for BI tools, dashboards, and ML feature stores.

```
Design Principles for Gold:
  ✓ Build one table per use case (revenue by region, customer LTV, product ranking)
  ✓ Materialised views (DLT recomputes automatically when silver changes)
  ✓ Apply currency conversions, fiscal calendar logic, KPI formulas
  ✓ Optimise for query performance: Z-ORDER on filter columns, OPTIMIZE
  ✓ Expose to BI tools, Databricks SQL, and Data Science teams
```

```python
# DLT Gold — daily revenue KPI
@dlt.table(
    name             = "daily_revenue_by_region",
    comment          = "Daily revenue aggregated by region — Gold KPI table",
    table_properties = {"quality": "gold"}
)
def daily_revenue_by_region():
    return (
        dlt.read("orders_silver")        # materialised view — reads full silver
            .filter("status = 'shipped'")
            .groupBy("order_date", "region", "tier")
            .agg(
                count("order_id").alias("order_count"),
                sum("total").alias("revenue"),
                avg("total").alias("avg_order_value"),
                countDistinct("customer_id").alias("unique_customers")
            )
            .withColumn("_updated_at", current_timestamp())
    )


# DLT Gold — customer lifetime value
@dlt.table(
    name    = "customer_ltv",
    comment = "Customer Lifetime Value — Gold KPI table"
)
def customer_ltv():
    return (
        dlt.read("orders_silver")
            .filter("status != 'cancelled'")
            .groupBy("customer_id", "country", "region", "tier")
            .agg(
                count("order_id").alias("total_orders"),
                sum("total").alias("lifetime_value"),
                min("order_date").alias("first_order"),
                max("order_date").alias("last_order")
            )
    )
```

---

### 4.4 Cross-Layer Design Patterns

**Pattern 1 — Slowly Changing Dimensions (SCD Type 1):**

```python
# The Silver DLT table serves as an SCD1 (always latest snapshot via MERGE):
# Spark Structured Streaming's forEachBatch with MERGE handles this automatically
# in DLT streaming tables.
```

**Pattern 2 — Fan-out (one Bronze → multiple Silver tables):**

```python
# Split events table into separate Silver tables per event type
@dlt.table(name="page_view_silver")
@dlt.expect_or_drop("valid_page", "page_url IS NOT NULL")
def page_view_silver():
    return dlt.read_stream("events_bronze").filter("event_type = 'page_view'")

@dlt.table(name="purchase_silver")
@dlt.expect_or_drop("positive_revenue", "revenue > 0")
def purchase_silver():
    return dlt.read_stream("events_bronze").filter("event_type = 'purchase'")
```

**Pattern 3 — Fan-in (multiple Silver → one Gold table):**

```python
@dlt.table(name="unified_revenue")
def unified_revenue():
    online  = dlt.read("purchase_silver").withColumn("channel", lit("online"))
    offline = dlt.read("pos_transactions_silver").withColumn("channel", lit("offline"))
    return online.union(offline)
```

---

### 4.5 Naming Conventions and Catalogue Organisation

```
Unity Catalog structure for a Medallion pipeline:
  <environment>_catalog
    │
    ├── bronze
    │     ├── orders_raw
    │     ├── customers_raw
    │     └── products_raw
    │
    ├── silver
    │     ├── orders_silver
    │     ├── customers_dim
    │     └── products_dim
    │
    └── gold
          ├── daily_revenue_by_region
          ├── customer_ltv
          └── product_performance

Examples:
  prod_catalog.bronze.orders_raw
  prod_catalog.silver.orders_silver
  prod_catalog.gold.daily_revenue_by_region
```

---

## 5. Creating and Managing Delta Live Tables / Declarative Pipelines

### 5.1 Creating a DLT Pipeline in the UI

**Step-by-step:**

1. In Databricks, navigate to **Pipelines** → **Create Pipeline**.
2. **Pipeline name:** e.g., `Orders Medallion Pipeline`.
3. **Pipeline mode:** `Triggered` (batch-style) or `Continuous` (streaming).
4. **Source code:** Select your pipeline notebook(s) or Python file(s).
5. **Target:**
   - For DLT classic: set a **storage location** on DBFS/S3.
   - For Spark Declarative Pipeline: set **Target catalog** and **Target schema**.
6. **Cluster policy:** Choose an existing policy or configure a new cluster.
7. **Advanced Configuration:** Add key-value pairs (accessible as `spark.conf.get()` within the pipeline).
8. Click **Create**, then **Start** to run.

---

### 5.2 Streaming Tables vs Materialised Views

These are the two fundamental DLT dataset types — understanding the difference is critical:

```
Streaming Table:
  ─ Reads from an append-only streaming source (Autoloader, Kafka, Delta CDC)
  ─ Processes ONLY new data since the last run (incremental)
  ─ Backed by a Delta table with a streaming checkpoint
  ─ Use for: Bronze and Silver layers receiving new data continuously
  ─ Created with: dlt.read_stream() as the input

Materialised View:
  ─ Reads from any source (table, view, or another DLT dataset)
  ─ Recomputes the FULL result set on each pipeline run
  ─ DLT automatically determines what to recompute (change-based)
  ─ Use for: Gold aggregations, joins with slowly-changing dimensions
  ─ Created with: dlt.read() (no stream) as the input
```

```python
# ── Streaming Table ──────────────────────────────────────────────────────────
@dlt.table(name="orders_bronze")
def orders_bronze():
    return spark.readStream.format("cloudFiles")  \
                           .option("cloudFiles.format", "json") \
                           .option("cloudFiles.schemaLocation", "/dlt/schema/orders") \
                           .load("s3://bucket/raw/orders/")
    # dlt knows this is a Streaming Table because readStream is used


# ── Materialised View ────────────────────────────────────────────────────────
@dlt.table(name="orders_gold")
def orders_gold():
    return (
        dlt.read("orders_silver")   # dlt.read() → Materialised View
            .groupBy("order_date")
            .agg(sum("total").alias("daily_revenue"))
    )
```

---

### 5.3 Reading from External Sources

```python
# ── Read from Autoloader (most common for Bronze) ────────────────────────────
@dlt.table(name="events_bronze")
def events_bronze():
    return spark.readStream.format("cloudFiles") \
                           .option("cloudFiles.format", "json") \
                           .option("cloudFiles.schemaLocation", "/dlt/schema/events") \
                           .load("s3://bucket/raw/events/")


# ── Read from Kafka ──────────────────────────────────────────────────────────
@dlt.table(name="orders_kafka_bronze")
def orders_kafka_bronze():
    from pyspark.sql.functions import from_json
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    schema = StructType([
        StructField("order_id", StringType()),
        StructField("amount",   DoubleType())
    ])

    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "orders")
            .option("startingOffsets", "latest")
            .load()
            .select(from_json(col("value").cast("string"), schema).alias("d"))
            .select("d.*")
    )


# ── Read from an existing Delta table (not managed by DLT) ───────────────────
@dlt.table(name="customers_dim")
def customers_dim():
    return spark.read.table("prod_catalog.reference.customers")
    # Not a DLT-managed table — just a regular read


# ── Read from JDBC ────────────────────────────────────────────────────────────
@dlt.table(name="accounts_bronze")
def accounts_bronze():
    return spark.read.jdbc(
        url   = "jdbc:postgresql://prod-db.company.com:5432/finance",
        table = "accounts",
        properties = {
            "user":     spark.conf.get("db.user"),
            "password": spark.conf.get("db.password")
        }
    )
```

---

### 5.4 Building the Medallion with DLT — Full Example

```python
# ──────────────────────────────────────────────────────────────────────────────
# File: pipelines/ecommerce_medallion.py
# Complete Bronze → Silver → Gold pipeline with expectations and enrichment
# ──────────────────────────────────────────────────────────────────────────────

import dlt
from pyspark.sql.functions import (
    col, trim, upper, lower, to_date, when,
    sum, count, avg, min, max, countDistinct,
    current_timestamp, lit
)

CATALOG = spark.conf.get("pipeline.catalog", "dev")
SOURCE  = spark.conf.get("pipeline.orders_source", "s3://ecommerce-bucket/raw/orders/")


# ═══════════════════════════════════════════════════════════════════════════════
# BRONZE LAYER
# ═══════════════════════════════════════════════════════════════════════════════

@dlt.table(name="orders_bronze", comment="Raw order events — Bronze")
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"/dlt/{CATALOG}/schema/orders")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("header", "true")
            .load(SOURCE)
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
    )


@dlt.table(name="products_bronze", comment="Raw product catalogue — Bronze")
def products_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"/dlt/{CATALOG}/schema/products")
            .load("s3://ecommerce-bucket/raw/products/")
            .withColumn("_ingested_at", current_timestamp())
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SILVER LAYER
# ═══════════════════════════════════════════════════════════════════════════════

# Quality rules for orders
ORDER_RULES = {
    "valid_order_id":   "order_id IS NOT NULL AND length(order_id) > 0",
    "valid_customer":   "customer_id IS NOT NULL",
    "positive_qty":     "CAST(quantity AS INT) > 0",
    "non_negative_amt": "CAST(unit_price AS DOUBLE) >= 0",
}

@dlt.table(name="orders_silver", comment="Cleaned and validated orders — Silver")
@dlt.expect_all_or_drop(ORDER_RULES)
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
            .withColumn("order_id",    trim(col("order_id")))
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("product_id",  upper(trim(col("product_id"))))
            .withColumn("quantity",    col("quantity").cast("integer"))
            .withColumn("unit_price",  col("unit_price").cast("double"))
            .withColumn("order_date",  to_date(col("order_date"), "yyyy-MM-dd"))
            .withColumn("total",       col("quantity") * col("unit_price"))
            .withColumn("status",
                when(col("status").isin(["shipped", "dispatched"]), "shipped")
                .when(col("status").isin(["pending", "new"]),         "pending")
                .when(col("status").isin(["cancelled", "canceled"]),  "cancelled")
                .otherwise(lower(trim(col("status"))))
            )
            .drop("_ingested_at", "_source_file")
    )


@dlt.table(name="products_silver", comment="Cleaned product catalogue — Silver")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def products_silver():
    return (
        dlt.read_stream("products_bronze")
            .withColumn("product_id", upper(trim(col("product_id"))))
            .withColumn("price",      col("price").cast("double"))
            .withColumn("category",   lower(trim(col("category"))))
            .dropDuplicates(["product_id"])
            .drop("_ingested_at")
    )


# ═══════════════════════════════════════════════════════════════════════════════
# GOLD LAYER
# ═══════════════════════════════════════════════════════════════════════════════

@dlt.table(name="daily_revenue", comment="Daily revenue summary — Gold")
def daily_revenue():
    return (
        dlt.read("orders_silver")
            .filter("status = 'shipped'")
            .groupBy("order_date")
            .agg(
                count("order_id").alias("orders"),
                sum("total").alias("revenue"),
                avg("total").alias("avg_order_value"),
                countDistinct("customer_id").alias("unique_buyers")
            )
    )


@dlt.table(name="product_sales_summary", comment="Product sales summary — Gold")
def product_sales_summary():
    orders   = dlt.read("orders_silver").filter("status != 'cancelled'")
    products = dlt.read("products_silver")

    return (
        orders
            .join(products.select("product_id", "name", "category", "price"),
                  on="product_id", how="left")
            .groupBy("product_id", "name", "category")
            .agg(
                sum("quantity").alias("units_sold"),
                sum("total").alias("gross_revenue"),
                avg("total").alias("avg_order_amount"),
                count("order_id").alias("order_count")
            )
            .orderBy(col("gross_revenue").desc())
    )
```

---

### 5.5 Data Quality with Expectations

```python
# ── Expectation modes summary ────────────────────────────────────────────────
#
#  @dlt.expect          → WARN: log violation, keep the row
#  @dlt.expect_or_drop  → DROP: remove violating rows, continue pipeline
#  @dlt.expect_or_fail  → FAIL: stop the pipeline if any row violates

# ── Metrics: Where to see expectation results ────────────────────────────────
# Pipeline UI → Dataset node → Expectations panel shows:
#   - Passed rows count
#   - Failed rows count
#   - Pass rate %

# ── Querying expectation metrics programmatically ────────────────────────────
# DLT writes an event log table: <pipeline_id>.event_log

event_log = spark.sql("""
    SELECT
        timestamp,
        details:flow_progress.metrics.num_output_rows         AS output_rows,
        details:flow_progress.data_quality.dropped_records    AS dropped,
        details:flow_progress.data_quality.expectations[0].name    AS rule,
        details:flow_progress.data_quality.expectations[0].failed_records AS violations
    FROM event_log
    WHERE event_type = 'flow_progress'
    ORDER BY timestamp DESC
""")
event_log.show(truncate=False)


# ── Quarantine pattern with DLT ───────────────────────────────────────────────
# Instead of dropping bad rows, route them to a quarantine table

@dlt.view(name="orders_with_quality_flags")
def orders_with_quality_flags():
    from pyspark.sql.functions import expr
    return (
        dlt.read_stream("orders_bronze")
            .withColumn("is_valid",
                col("order_id").isNotNull() &
                (col("quantity").cast("int") > 0) &
                (col("unit_price").cast("double") >= 0)
            )
    )

@dlt.table(name="orders_silver_valid")
@dlt.expect_or_drop("must_be_valid", "is_valid = true")
def orders_silver_valid():
    return dlt.read_stream("orders_with_quality_flags").drop("is_valid")

@dlt.table(name="orders_quarantine")
def orders_quarantine():
    return (
        dlt.read_stream("orders_with_quality_flags")
            .filter("is_valid = false")
            .withColumn("_quarantined_at", current_timestamp())
            .drop("is_valid")
    )
```

---

### 5.6 Schema Management in DLT

```python
# ── Explicit schema (recommended for Bronze) ─────────────────────────────────
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

ORDER_SCHEMA = StructType([
    StructField("order_id",    StringType(),  False),
    StructField("customer_id", StringType(),  True),
    StructField("product_id",  StringType(),  True),
    StructField("quantity",    StringType(),  True),  # string in Bronze, cast in Silver
    StructField("unit_price",  StringType(),  True),
    StructField("order_date",  StringType(),  True),
    StructField("status",      StringType(),  True),
])

@dlt.table(
    name   = "orders_bronze_typed",
    schema = ORDER_SCHEMA
)
def orders_bronze_typed():
    return spark.readStream.format("cloudFiles") \
                           .option("cloudFiles.format", "csv") \
                           .schema(ORDER_SCHEMA) \
                           .load("s3://bucket/raw/orders/")


# ── Schema evolution in DLT ──────────────────────────────────────────────────
# DLT automatically handles schema evolution for streaming tables
# New columns in the source are added to the target table
# This is controlled by the pipeline setting:
#   Configuration key:   "pipelines.schema.autoMerge.enabled"
#   Value:               "true"
```

---

### 5.7 Python vs SQL in DLT

Both Python and SQL are supported in the same DLT pipeline notebook:

```sql
-- SQL DLT table definition
CREATE OR REFRESH STREAMING TABLE orders_bronze
COMMENT "Raw orders — Bronze layer"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
    *,
    current_timestamp() AS _ingested_at,
    _metadata.file_path AS _source_file
FROM STREAM(
    read_files(
        "s3://bucket/raw/orders/",
        format          => "csv",
        header          => true,
        inferSchema     => false,
        schemaLocation  => "/dlt/schema/orders"
    )
);


-- SQL Silver with expectations
CREATE OR REFRESH STREAMING TABLE orders_silver (
    CONSTRAINT valid_order_id   EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT positive_qty     EXPECT (quantity > 0)         ON VIOLATION DROP ROW
)
COMMENT "Validated orders — Silver layer"
AS
SELECT
    TRIM(order_id)                      AS order_id,
    TRIM(customer_id)                   AS customer_id,
    CAST(quantity   AS INT)             AS quantity,
    CAST(unit_price AS DOUBLE)          AS unit_price,
    TO_DATE(order_date, 'yyyy-MM-dd')   AS order_date,
    CAST(quantity AS INT) * CAST(unit_price AS DOUBLE) AS total,
    LOWER(TRIM(status))                 AS status
FROM STREAM(LIVE.orders_bronze);


-- SQL Gold materialised view
CREATE OR REFRESH MATERIALIZED VIEW daily_revenue
COMMENT "Daily revenue — Gold KPI"
AS
SELECT
    order_date,
    COUNT(*)     AS order_count,
    SUM(total)   AS revenue,
    AVG(total)   AS avg_order_value
FROM LIVE.orders_silver
WHERE status = 'shipped'
GROUP BY order_date;
```

**Choosing Python vs SQL:**

| Aspect                   | Python             | SQL                     |
| ------------------------ | ------------------ | ----------------------- |
| Complex logic            | ✓ Better           | Limited                 |
| UDFs                     | ✓                  | Limited                 |
| Readability for analysts | Limited            | ✓ Better                |
| Expectations             | `@dlt.expect`      | `CONSTRAINT ... EXPECT` |
| Schema definition        | `StructType`       | Inline DDL              |
| Parameterisation         | `spark.conf.get()` | `${param_name}` in SQL  |

---

## 6. Automating Data Processing with Delta Live Tables / Declarative Pipelines

### 6.1 Continuous vs Triggered Pipeline Modes

```
TRIGGERED Mode (default):
  ─ Pipeline runs once and stops after processing all available data.
  ─ Ideal for scheduled batch pipelines (run daily, hourly).
  ─ Cluster terminates after each run — cost efficient.
  ─ Configure via Workflows to run on a schedule.

  Timeline:
    [08:00] Cluster starts → processes all new data → cluster terminates [08:18]
    [09:00] Cluster starts → processes all new data → cluster terminates [09:14]

CONTINUOUS Mode:
  ─ Cluster stays running 24/7.
  ─ Processes new data immediately as it arrives.
  ─ Minimal latency (seconds).
  ─ Higher cost (cluster runs even when idle).
  ─ Best for real-time use cases.

  Timeline:
    [Continuous] Cluster running → new file arrives → processed within seconds
```

```json
// Set pipeline mode in the Pipeline configuration
{
  "pipeline_mode": "TRIGGERED" // or "CONTINUOUS"
}
```

---

### 6.2 Scheduling DLT Pipelines via Workflows

The recommended way to schedule a DLT pipeline is as a **Pipeline task inside a Databricks Workflow**:

```json
{
  "name": "Scheduled Orders Medallion",
  "schedule": {
    "quartz_cron_expression": "0 0 */4 * * ?",
    "timezone_id": "UTC"
  },
  "tasks": [
    {
      "task_key": "run_dlt_pipeline",
      "pipeline_task": {
        "pipeline_id": "abc123-dlt-pipeline-id",
        "full_refresh": false
      }
    },
    {
      "task_key": "post_dlt_reporting",
      "depends_on": [{ "task_key": "run_dlt_pipeline" }],
      "notebook_task": {
        "notebook_path": "/reporting/send_dashboard_snapshot"
      },
      "job_cluster_key": "reporting_cluster"
    }
  ]
}
```

**Benefits of using Workflows to schedule DLT:**

- The Workflow handles scheduling; DLT handles the data pipeline itself.
- You can chain post-DLT notebooks (e.g., refresh BI dashboards) in the same Workflow.
- Repair runs in Workflow can re-trigger the DLT pipeline without manually intervening.

---

### 6.3 Full Refresh vs Incremental Refresh

```python
# ── Full Refresh ──────────────────────────────────────────────────────────────
# Drops and recreates ALL tables from scratch — reprocesses all source data.
# Use when:
#   - Pipeline logic changes significantly
#   - Source data was corrected retroactively
#   - Debugging unexpected results

# Via UI: Pipeline → Full Refresh
# Via API:
import requests
requests.post(
    "https://<workspace>/api/2.0/pipelines/<pid>/updates",
    headers = {"Authorization": f"Bearer {TOKEN}"},
    json    = {"full_refresh": True}
)


# ── Incremental Refresh (default) ─────────────────────────────────────────────
# Processes only new data since last run.
# Streaming Tables: process new files/messages.
# Materialised Views: recompute if upstream changed.


# ── Selective Full Refresh ────────────────────────────────────────────────────
# Full refresh only specific tables (leaves others unchanged)
# Via API:
requests.post(
    "https://<workspace>/api/2.0/pipelines/<pid>/updates",
    headers = {"Authorization": f"Bearer {TOKEN}"},
    json    = {
        "full_refresh_selection": ["orders_silver", "daily_revenue"]
        # Only these tables are fully refreshed
    }
)
```

---

### 6.4 Parameterizing DLT Pipelines

```python
# Access pipeline configuration parameters in DLT notebooks
CATALOG = spark.conf.get("pipeline.catalog",      "dev")
ENV     = spark.conf.get("pipeline.environment",  "dev")
SOURCE  = spark.conf.get("pipeline.source_path",  "s3://dev-bucket/raw/")
MAX_FILES = int(spark.conf.get("pipeline.max_files_per_trigger", "1000"))

@dlt.table(name="orders_bronze")
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",             "json")
            .option("cloudFiles.schemaLocation",     f"/dlt/{ENV}/schema/orders")
            .option("cloudFiles.maxFilesPerTrigger", str(MAX_FILES))
            .load(SOURCE)
    )
```

**Set parameters in the Pipeline configuration (JSON):**

```json
{
  "configuration": {
    "pipeline.catalog": "prod",
    "pipeline.environment": "prod",
    "pipeline.source_path": "s3://prod-bucket/raw/",
    "pipeline.max_files_per_trigger": "5000"
  }
}
```

---

### 6.5 Multi-Environment Deployments (Dev / Staging / Prod)

```
Recommended pattern:
  ─ One pipeline definition (notebook) shared across all environments
  ─ Environment-specific pipeline configurations (target catalog, source path)
  ─ Databricks Repos + Git branching for version control

Dev Pipeline Configuration:
  pipeline.catalog:    dev_catalog
  pipeline.source:     s3://dev-bucket/raw/
  pipeline.mode:       TRIGGERED

Prod Pipeline Configuration:
  pipeline.catalog:    prod_catalog
  pipeline.source:     s3://prod-bucket/raw/
  pipeline.mode:       TRIGGERED (scheduled via Workflow)
```

```python
# Automated deployment script using Python SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineCluster, PipelineClusterAutoscale

w = WorkspaceClient()

def deploy_pipeline(env: str, catalog: str, source: str) -> str:
    pipeline = w.pipelines.create(
        name         = f"Orders Medallion ({env.upper()})",
        libraries    = [{"notebook": {"path": "/pipelines/ecommerce_medallion"}}],
        configuration = {
            "pipeline.catalog":     catalog,
            "pipeline.environment": env,
            "pipeline.source_path": source
        },
        clusters     = [
            PipelineCluster(
                label      = "default",
                autoscale  = PipelineClusterAutoscale(min_workers=2, max_workers=8)
            )
        ],
        continuous   = False,
        development  = (env != "prod")
    )
    return pipeline.pipeline_id

dev_id  = deploy_pipeline("dev",  "dev_catalog",  "s3://dev-bucket/raw/")
prod_id = deploy_pipeline("prod", "prod_catalog", "s3://prod-bucket/raw/")
print(f"Dev pipeline:  {dev_id}")
print(f"Prod pipeline: {prod_id}")
```

---

## 7. Best Practices for Managing Data Pipelines with Delta Live Tables / Declarative Pipelines

### 7.1 Pipeline Code Organisation

```
Recommended file structure:
  pipelines/
    ├── bronze/
    │     ├── orders_bronze.py       ← one file per source system
    │     ├── customers_bronze.py
    │     └── products_bronze.py
    ├── silver/
    │     ├── orders_silver.py
    │     └── customers_silver.py
    ├── gold/
    │     ├── revenue_gold.py
    │     └── customer_ltv_gold.py
    └── shared/
          ├── config.py              ← shared configurations
          └── expectations.py        ← reusable expectation dictionaries

# In config.py
CATALOG = spark.conf.get("pipeline.catalog", "dev")
SOURCE  = spark.conf.get("pipeline.source",  "s3://dev-bucket/raw/")

# In expectations.py
ORDER_QUALITY_RULES = {
    "valid_id":    "order_id IS NOT NULL",
    "positive_qty":"quantity > 0",
    "valid_status":"status IN ('shipped','pending','cancelled','returned')"
}

# In orders_silver.py
from shared.config import CATALOG
from shared.expectations import ORDER_QUALITY_RULES
import dlt

@dlt.table
@dlt.expect_all_or_drop(ORDER_QUALITY_RULES)
def orders_silver():
    ...
```

---

### 7.2 Testing DLT Pipelines

```python
# ── Unit Testing pipeline logic (outside DLT) ────────────────────────────────
# Extract your transformation logic into plain functions, testable without DLT

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, lower
import pytest

def transform_orders_silver(df):
    """The transformation logic — pure function, testable outside DLT."""
    return (
        df
        .withColumn("order_id",   trim(col("order_id")))
        .withColumn("quantity",   col("quantity").cast("integer"))
        .withColumn("order_date", to_date(col("order_date")))
        .withColumn("status",     lower(trim(col("status"))))
        .filter("order_id IS NOT NULL")
        .filter("quantity > 0")
    )

# In DLT, delegate to the function
@dlt.table
def orders_silver():
    raw = dlt.read_stream("orders_bronze")
    return transform_orders_silver(raw)


# pytest test file: tests/test_orders_silver.py
def test_transform_orders_silver(spark: SparkSession):
    test_input = spark.createDataFrame([
        ("ORD001", " CUS001 ", "2", "9.99", "2024-01-15", " PENDING "),
        (None,     "CUS002",   "1", "5.00", "2024-01-16", "shipped"),   # bad: null order_id
        ("ORD003", "CUS003",   "0", "3.00", "2024-01-17", "cancelled"), # bad: zero quantity
        ("ORD004", "CUS004",   "3", "7.00", "2024-01-18", "SHIPPED"),   # valid
    ], ["order_id", "customer_id", "quantity", "unit_price", "order_date", "status"])

    result = transform_orders_silver(test_input)

    assert result.count() == 2, "Expected 2 valid rows (ORD001 and ORD004)"
    assert result.filter("order_id = 'ORD001'").select("status").collect()[0][0] == "pending"
    assert result.filter("order_id = 'ORD004'").select("status").collect()[0][0] == "shipped"
```

---

### 7.3 Monitoring and Observability

````python
# ── Query the DLT Event Log ───────────────────────────────────────────────────
# The event log is a Delta table created by DLT for each pipeline

# Option 1: Named via Unity Catalog
event_log_table = f"`{CATALOG}`.`{SCHEMA}`.`event_log`"

# Option 2: By storage path (non-UC pipelines)
event_log_table = "delta.`/Shared/pipelines/<pipeline-id>/system/events`"

# ── Pipeline health dashboard query ──────────────────────────────────────────
spark.sql(f"""
    SELECT
        timestamp,
        event_type,
        level,                                              -- INFO / WARN / ERROR
        details:flow_definition.output_dataset AS dataset, -- which table
        details:flow_progress.status           AS status,
        details:flow_progress.metrics.num_output_rows AS rows_written,
        details:flow_progress.data_quality.dropped_records AS rows_dropped,
        details:flow_progress.data_quality.expectations    AS quality_rules
    FROM {event_log_table}
    WHERE event_type IN ('flow_progress', 'flow_definition')
    ORDER BY timestamp DESC
    LIMIT 100
""").show(truncate=False)


# ── Alert on data quality failures ───────────────────────────────────────────
quality_failures = spark.sql(f"""
    SELECT
        timestamp,
        details:flow_definition.output_dataset AS dataset,
        explode(details:flow_progress.data_quality.expectations) AS rule
    FROM {event_log_table}
    WHERE event_type = 'flow_progress'
    AND   details:flow_progress.data_quality.dropped_records > 0
    AND   timestamp > current_timestamp() - INTERVAL 1 HOUR
""")

if quality_failures.count() > 0:
    # Send alert
    alert_msg = quality_failures.toPandas().to_string()
    send_slack_alert(f":warning: DLT Quality Failures in last 1 hour:\n```{alert_msg}```")
````

---

### 7.4 Performance Optimisation

```python
# ── Tune shuffle partitions for your cluster size ────────────────────────────
spark.conf.set("spark.sql.shuffle.partitions", "200")   # set in pipeline configuration

# ── Enable Adaptive Query Execution ──────────────────────────────────────────
# Already on by default in DBR 10.0+. No action needed.

# ── Prevent shuffle for small reference table joins ──────────────────────────
from pyspark.sql.functions import broadcast

@dlt.table
def orders_enriched():
    orders    = dlt.read_stream("orders_silver")
    countries = dlt.read("countries_dim")   # small reference table (~200 rows)
    return orders.join(broadcast(countries), on="country_code", how="left")


# ── Optimise write performance with optimizeWrite ────────────────────────────
@dlt.table(
    table_properties = {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true"
    }
)
def orders_silver(): ...


# ── Use liquid clustering (Databricks 13.2+) instead of partitioning ─────────
@dlt.table(
    cluster_by        = ["customer_id", "order_date"],  # liquid cluster columns
    table_properties  = {"delta.enableDeletionVectors": "true"}
)
def orders_silver(): ...


# ── Set watermark for streaming deduplication ─────────────────────────────────
from pyspark.sql.functions import col

@dlt.table
def orders_silver_dedup():
    return (
        dlt.read_stream("orders_bronze")
            .withWatermark("event_timestamp", "1 hour")  # late data tolerance
            .dropDuplicates(["order_id", "event_timestamp"])
    )
```

---

### 7.5 Cost Management

```
Cost Levers in DLT:
  1. Pipeline mode: TRIGGERED (process and stop) is cheaper than CONTINUOUS (always-on).
  2. Cluster autoscaling: Set min_workers=1, max_workers=N to scale down when idle.
  3. Spot instances: Use SPOT_WITH_FALLBACK for up to 70% cost reduction.
  4. Photon: Enable Photon engine for ~3× faster SQL queries (same DBU cost).
  5. Serverless DLT: Instant startup, no cluster management, pay-per-use.

Cost anti-patterns to avoid:
  ✗ CONTINUOUS mode for pipelines that only need hourly refresh
  ✗ Fixed large clusters (e.g., 20 workers) for pipelines with variable load
  ✗ Running DLT in development mode (no optimisations) in production
  ✗ Full refresh on every run (reprocesses all data unnecessarily)
```

```json
// Cost-optimised pipeline cluster configuration
{
  "clusters": [
    {
      "label": "default",
      "spark_version": "14.3.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "autoscale": {
        "min_workers": 1,
        "max_workers": 10
      },
      "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",
        "spot_bid_price_percent": 100,
        "first_on_demand": 1
      },
      "enable_photon": true
    }
  ],
  "development": false
}
```

---

### 7.6 Error Recovery Patterns

```python
# ── Pattern 1: Safe null defaults (prevent row drops due to nulls) ────────────
from pyspark.sql.functions import coalesce, lit

@dlt.table
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
            .withColumn("quantity",  coalesce(col("quantity").cast("int"),   lit(0)))
            .withColumn("status",    coalesce(col("status"),                 lit("unknown")))
    )


# ── Pattern 2: Rescued data column ────────────────────────────────────────────
# Autoloader can route unparse-able records to a _rescued_data column
@dlt.table
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format",             "json")
            .option("cloudFiles.schemaLocation",     "/dlt/schema/orders")
            .option("cloudFiles.schemaEvolutionMode","rescue")  # routes bad records to _rescued_data
            .load("s3://bucket/raw/orders/")
    )

# Then surface rescued data as a separate table
@dlt.table(name="orders_rescued", comment="Records that did not match the schema")
@dlt.expect("has_rescued_data", "_rescued_data IS NOT NULL")
def orders_rescued():
    return (
        dlt.read_stream("orders_bronze")
            .filter("_rescued_data IS NOT NULL")
    )


# ── Pattern 3: Pipeline-level retry via Workflow ──────────────────────────────
# In the Workflow task for the DLT pipeline, set:
# {
#   "max_retries": 2,
#   "min_retry_interval_millis": 300000   // 5 minutes
# }
# DLT will checkpoint before each batch, so retry resumes from failure point.
```

---

### 7.7 Security and Access Control

```python
# ── Unity Catalog grants on DLT-managed tables ────────────────────────────────

# Grant access to Gold tables for BI team
spark.sql("GRANT SELECT ON TABLE prod_catalog.gold.daily_revenue TO `bi-team@company.com`")
spark.sql("GRANT SELECT ON TABLE prod_catalog.gold.customer_ltv  TO `data-science@company.com`")

# Grant access to Silver tables for data engineers only
spark.sql("GRANT SELECT, MODIFY ON TABLE prod_catalog.silver.orders_silver TO `data-engineers@company.com`")

# Restrict Bronze to pipeline service principal only
spark.sql("REVOKE ALL ON TABLE prod_catalog.bronze.orders_raw FROM `data-engineers@company.com`")
spark.sql("GRANT SELECT, MODIFY ON TABLE prod_catalog.bronze.orders_raw TO `svc-dlt@company.com`")

# Row-level security example (filter data by user)
spark.sql("""
    CREATE ROW ACCESS POLICY region_filter
    AS (region STRING) RETURNS BOOLEAN
    RETURN is_account_group_member(CONCAT('region-', region))
""")
spark.sql("ALTER TABLE prod_catalog.silver.orders_silver ADD ROW FILTER region_filter ON (region)")

# Column masking for PII (mask email in Silver)
spark.sql("""
    CREATE FUNCTION mask_email(email STRING) RETURNS STRING
    RETURN CASE WHEN is_account_group_member('pii-access') THEN email
                ELSE CONCAT(LEFT(email, 2), '****@****.com')
           END
""")
spark.sql("ALTER TABLE prod_catalog.silver.customers_silver ALTER COLUMN email SET MASK mask_email")
```

```
Security Checklist for DLT Pipelines:
  ✓ Run pipelines with a dedicated service principal (not personal credentials)
  ✓ Store all credentials in Databricks Secrets (never in code)
  ✓ Use Unity Catalog for all table-level and column-level governance
  ✓ Apply row-level security on Bronze/Silver for multi-tenant pipelines
  ✓ Grant minimum necessary permissions to service accounts
  ✓ Enable audit logging via Databricks Audit Log
  ✓ Pin Databricks Runtime version in pipeline cluster configuration
```

---

## 8. Summary

| Topic                           | Key Points                                                                             |
| ------------------------------- | -------------------------------------------------------------------------------------- |
| **Delta Live Tables**           | Declarative ETL framework — define _what_, DLT figures out _how_                       |
| **Spark Declarative Pipeline**  | Evolution of DLT — runs on standard clusters with Unity Catalog; same `dlt` API        |
| **DLT vs Declarative Pipeline** | Declarative Pipeline = no DLT surcharge, requires UC Premium; DLT = wider tier support |
| **Streaming Tables**            | Incremental, append-only; process only new data since last run                         |
| **Materialised Views**          | Full recompute; DLT handles change detection automatically                             |
| **Medallion Architecture**      | Bronze (raw) → Silver (clean) → Gold (aggregated) — three quality tiers                |
| **Expectations**                | `@dlt.expect` (warn), `@dlt.expect_or_drop` (filter), `@dlt.expect_or_fail` (stop)     |
| **Event Log**                   | Built-in Delta table recording all pipeline operations for observability               |
| **Scheduling**                  | Use Workflows to schedule DLT pipelines (Pipeline task type)                           |
| **Full vs Incremental**         | Default is incremental; full refresh on logic changes or corrections                   |
| **Testing**                     | Extract transformation logic into plain functions; unit-test outside DLT               |
| **Cost**                        | Triggered mode + Spot instances + Autoscaling = optimal cost profile                   |
| **Security**                    | Unity Catalog RBAC + row filters + column masks + service principals                   |

---

### Quick Reference: DLT API Cheat Sheet

```python
import dlt

# Table types
@dlt.table                         # Streaming Table (if uses readStream) or Materialised View
@dlt.view                          # Ephemeral view (not persisted)

# Expectations
@dlt.expect("name", "condition")                    # WARN
@dlt.expect_or_drop("name", "condition")            # DROP violating rows
@dlt.expect_or_fail("name", "condition")            # FAIL pipeline
@dlt.expect_all({"name": "condition", ...})         # WARN — multiple rules
@dlt.expect_all_or_drop({"name": "cond", ...})      # DROP — multiple rules
@dlt.expect_all_or_fail({"name": "cond", ...})      # FAIL — multiple rules

# Reading datasets
dlt.read("table_name")             # Read a DLT-managed table (batch)
dlt.read_stream("table_name")      # Read a DLT-managed table as a stream

# Configuration access
spark.conf.get("pipeline.my_key", "default_value")

# SQL equivalents
# CREATE OR REFRESH STREAMING TABLE   (= @dlt.table with readStream)
# CREATE OR REFRESH MATERIALIZED VIEW (= @dlt.table with dlt.read)
# CREATE OR REFRESH LIVE VIEW         (= @dlt.view)
# CONSTRAINT name EXPECT (cond) ON VIOLATION [WARN|DROP ROW|FAIL UPDATE]
```

**Series complete:** You have now covered the full Databricks Data Engineering curriculum — from setting up clusters and writing Spark code, to building production Delta Lake pipelines, orchestrating them with Workflows, and expressing them declaratively with Delta Live Tables and Spark Declarative Pipelines.

---
