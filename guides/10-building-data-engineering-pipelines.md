# Building Data Engineering Pipelines

---

## Table of Contents

1. [Understanding Data Pipelines and Workflows](#1-understanding-data-pipelines-and-workflows)
   - 1.1 [What Is a Data Pipeline?](#11-what-is-a-data-pipeline)
   - 1.2 [Batch vs Streaming Pipelines](#12-batch-vs-streaming-pipelines)
   - 1.3 [The Medallion Architecture](#13-the-medallion-architecture)
   - 1.4 [Pipeline Design Principles](#14-pipeline-design-principles)
2. [Building ETL Pipelines in Databricks](#2-building-etl-pipelines-in-databricks)
   - 2.1 [Extract — Reading from Source Systems](#21-extract--reading-from-source-systems)
   - 2.2 [Transform — Cleaning and Enriching Data](#22-transform--cleaning-and-enriching-data)
   - 2.3 [Load — Writing to Delta Tables](#23-load--writing-to-delta-tables)
   - 2.4 [Building a Bronze Layer Ingestion Pipeline](#24-building-a-bronze-layer-ingestion-pipeline)
   - 2.5 [Building a Silver Layer Transformation Pipeline](#25-building-a-silver-layer-transformation-pipeline)
   - 2.6 [Building a Gold Layer Aggregation Pipeline](#26-building-a-gold-layer-aggregation-pipeline)
   - 2.7 [Incremental Processing with Change Data Capture](#27-incremental-processing-with-change-data-capture)
   - 2.8 [Error Handling and Data Quality Checks](#28-error-handling-and-data-quality-checks)
3. [Scheduling and Automating Data Pipelines](#3-scheduling-and-automating-data-pipelines)
   - 3.1 [Databricks Jobs — Overview](#31-databricks-jobs--overview)
   - 3.2 [Creating a Job from a Notebook](#32-creating-a-job-from-a-notebook)
   - 3.3 [Job Scheduling Options](#33-job-scheduling-options)
   - 3.4 [Multi-Task Jobs and Task Dependencies](#34-multi-task-jobs-and-task-dependencies)
   - 3.5 [Parameterizing Jobs with Widgets](#35-parameterizing-jobs-with-widgets)
   - 3.6 [Job Clusters vs Interactive Clusters](#36-job-clusters-vs-interactive-clusters)
4. [Monitoring and Managing Data Pipelines](#4-monitoring-and-managing-data-pipelines)
   - 4.1 [Job Run Monitoring in the UI](#41-job-run-monitoring-in-the-ui)
   - 4.2 [Structured Logging with Log4j and print](#42-structured-logging-with-log4j-and-print)
   - 4.3 [Alerting on Job Failures](#43-alerting-on-job-failures)
   - 4.4 [Pipeline Metrics with Spark UI](#44-pipeline-metrics-with-spark-ui)
   - 4.5 [Data Quality Monitoring](#45-data-quality-monitoring)
   - 4.6 [Retrying and Recovering Failed Pipelines](#46-retrying-and-recovering-failed-pipelines)
5. [Case Study: Implementing a Complete Data Pipeline](#5-case-study-implementing-a-complete-data-pipeline)
   - 5.1 [Business Scenario](#51-business-scenario)
   - 5.2 [Pipeline Architecture](#52-pipeline-architecture)
   - 5.3 [Implementation: Bronze Layer](#53-implementation-bronze-layer)
   - 5.4 [Implementation: Silver Layer](#54-implementation-silver-layer)
   - 5.5 [Implementation: Gold Layer](#55-implementation-gold-layer)
   - 5.6 [Orchestrating the Pipeline as a Job](#56-orchestrating-the-pipeline-as-a-job)
6. [Summary](#6-summary)

---

## 1. Understanding Data Pipelines and Workflows

### 1.1 What Is a Data Pipeline?

A **data pipeline** is a series of automated processes that move data from one or more **source systems** through a series of **transformations** into a **target store** where it can be analysed.

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA PIPELINE                               │
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │  SOURCE  │───▶│ EXTRACT  │───▶│TRANSFORM │───▶│  LOAD   │  │
│  │          │    │          │    │          │    │          │  │
│  │ • S3     │    │ Read raw │    │ Clean    │    │ Delta    │  │
│  │ • Kafka  │    │ data     │    │ Enrich   │    │ Tables   │  │
│  │ • JDBC   │    │ Validate │    │ Aggregate│    │ Data     │  │
│  │ • API    │    │          │    │ Join     │    │ Warehouse│  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘  │
│                                                                 │
│  ←────────────────── Orchestration & Scheduling ──────────────▶ │
│  ←────────────────── Monitoring & Alerting ───────────────────▶ │
│  ←────────────────── Error Handling & Retry ──────────────────▶ │
└─────────────────────────────────────────────────────────────────┘
```

**Core pipeline components:**

- **Ingestion** — Pulling raw data from sources (files, databases, streams).
- **Transformation** — Cleaning, validating, enriching, and reshaping data.
- **Storage** — Persisting processed data in Delta tables.
- **Orchestration** — Scheduling and managing execution order.
- **Monitoring** — Tracking health, performance, and data quality.

---

### 1.2 Batch vs Streaming Pipelines

| Characteristic      | Batch Pipeline                         | Streaming Pipeline                    |
| ------------------- | -------------------------------------- | ------------------------------------- |
| **Trigger**         | Scheduled (hourly, daily)              | Continuous or micro-batch             |
| **Latency**         | Minutes to hours                       | Seconds to minutes                    |
| **Data Volume**     | Large volumes per run                  | Small, continuous increments          |
| **Complexity**      | Simpler                                | Higher (state management)             |
| **Use Cases**       | Daily reports, end-of-day aggregations | Real-time dashboards, fraud detection |
| **Databricks Tool** | Jobs + Notebooks                       | Structured Streaming + DLT            |

**Typical Databricks approach:** A **Lambda** or **Kappa** hybrid. Autoloader and Structured Streaming handle real-time ingestion, while batch Jobs perform heavy transformations and aggregations on schedule.

---

### 1.3 The Medallion Architecture

The **Medallion Architecture** is the most widely adopted data organization pattern in Databricks. It organises data into three quality tiers:

```
┌─────────────────────────────────────────────────────────────────┐
│                     MEDALLION ARCHITECTURE                       │
│                                                                 │
│  Source          Bronze              Silver              Gold   │
│  ──────          ──────              ──────              ────   │
│  Raw files  ───▶  Raw copy   ──────▶  Cleaned   ──────▶  Agg   │
│  Kafka           Validated           Enriched           Reports │
│  JDBC            No transform        Deduplicated       KPIs    │
│                  Schema enforced     Conformed          ML Sets │
│                                                                 │
│  S3://raw/       delta.bronze        delta.silver        delta.gold
└─────────────────────────────────────────────────────────────────┘
```

| Layer      | Purpose                             | Characteristics                                        |
| ---------- | ----------------------------------- | ------------------------------------------------------ |
| **Bronze** | Raw ingestion — preserve everything | Append-only, no business logic, minimal transformation |
| **Silver** | Cleaned, enriched, conformed        | Deduplicated, NULL handled, joined with reference data |
| **Gold**   | Business-ready aggregates           | KPIs, metrics, data mart tables for BI/ML              |

---

### 1.4 Pipeline Design Principles

1. **Idempotency** — Re-running a pipeline produces the same result. Use `MERGE` or `INSERT OVERWRITE` instead of blind appends.
2. **Checkpointing** — Store progress state so the pipeline can resume from where it left off after a failure.
3. **Immutable Bronze** — Never transform raw data in-place; always copy to a new layer.
4. **Schema-on-Write** — Enforce and document schema at write time using Delta's schema enforcement.
5. **Separation of Concerns** — Each notebook/task does one thing (ingest, transform, or aggregate).
6. **Data Quality Assertions** — Validate row counts, NULL rates, and business rules before promoting data upstream.

---

## 2. Building ETL Pipelines in Databricks

### 2.1 Extract — Reading from Source Systems

**From CSV / JSON / Parquet files on S3:**

```python
# Read CSV with explicit schema
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

schema = StructType([
    StructField("order_id",     StringType(), False),
    StructField("customer_id",  StringType(), True),
    StructField("product_id",   StringType(), True),
    StructField("quantity",     StringType(), True),   # read as string, cast in silver
    StructField("unit_price",   StringType(), True),
    StructField("order_date",   StringType(), True),
    StructField("status",       StringType(), True),
])

raw_orders = spark.read \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .option("badRecordsPath", "/corrupt/orders/") \
    .load("s3://my-bucket/raw/orders/2024/")

# Read JSON files
raw_events = spark.read \
    .format("json") \
    .option("multiLine", "true") \
    .load("s3://my-bucket/raw/events/")

# Read Parquet
raw_products = spark.read.parquet("s3://my-bucket/raw/products/")
```

**From JDBC (relational database):**

```python
jdbc_url  = "jdbc:mysql://prod-db.company.com:3306/sales"
db_props  = {
    "user":     dbutils.secrets.get("db-scope", "username"),
    "password": dbutils.secrets.get("db-scope", "password"),
    "driver":   "com.mysql.cj.jdbc.Driver"
}

# Full table load
customers_raw = spark.read.jdbc(
    url        = jdbc_url,
    table      = "customers",
    properties = db_props
)

# Parallel read with partitioning (for large tables)
orders_raw = spark.read.jdbc(
    url              = jdbc_url,
    table            = "orders",
    column           = "order_id_hash",     # numeric partition column
    lowerBound       = 0,
    upperBound       = 1000000,
    numPartitions    = 20,
    properties       = db_props
)
```

**From Kafka (streaming):**

```python
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "orders-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize JSON payload
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

event_schema = StructType([
    StructField("order_id",   StringType()),
    StructField("amount",     DoubleType()),
    StructField("event_time", StringType()),
])

orders_stream = kafka_stream \
    .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
    .select("data.*")
```

---

### 2.2 Transform — Cleaning and Enriching Data

```python
from pyspark.sql.functions import (
    col, trim, upper, lower, when, to_date, to_timestamp,
    regexp_replace, coalesce, lit, current_timestamp,
    monotonically_increasing_id
)

def transform_silver_orders(bronze_df):
    """
    Silver transformation for orders:
    - Cast types
    - Trim whitespace
    - Normalise status codes
    - Remove duplicates
    - Handle NULLs
    - Add audit columns
    """
    return (
        bronze_df
        # ── Type casting ──
        .withColumn("quantity",     col("quantity").cast("integer"))
        .withColumn("unit_price",   col("unit_price").cast("double"))
        .withColumn("order_date",   to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("total_amount", col("quantity") * col("unit_price"))

        # ── String cleaning ──
        .withColumn("status",       lower(trim(col("status"))))
        .withColumn("customer_id",  trim(col("customer_id")))
        .withColumn("product_id",   upper(trim(col("product_id"))))

        # ── Normalise status values ──
        .withColumn("status",
            when(col("status").isin(["shipped", "ship", "dispatched"]), "shipped")
            .when(col("status").isin(["pending", "open", "new"]),        "pending")
            .when(col("status").isin(["cancelled", "canceled", "void"]), "cancelled")
            .otherwise(col("status"))
        )

        # ── NULL handling ──
        .withColumn("quantity",   coalesce(col("quantity"),   lit(1)))
        .withColumn("unit_price", coalesce(col("unit_price"), lit(0.0)))
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())

        # ── Deduplication ──
        .dropDuplicates(["order_id"])

        # ── Audit columns ──
        .withColumn("_silver_processed_at", current_timestamp())
    )
```

---

### 2.3 Load — Writing to Delta Tables

```python
# Mode 1: OVERWRITE — full refresh
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.orders")

# Mode 2: APPEND — append new records
silver_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("silver.orders")

# Mode 3: MERGE — upsert (idempotent)
from delta.tables import DeltaTable

def upsert_to_silver(new_df, table_name, merge_key):
    if DeltaTable.isDeltaTable(spark, f"spark-warehouse/{table_name}"):
        target = DeltaTable.forName(spark, table_name)
        target.alias("tgt").merge(
            new_df.alias("src"),
            f"tgt.{merge_key} = src.{merge_key}"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        new_df.write.format("delta").saveAsTable(table_name)

upsert_to_silver(silver_df, "silver.orders", "order_id")
```

---

### 2.4 Building a Bronze Layer Ingestion Pipeline

```python
# ─── bronze_orders_ingest.py ────────────────────────────────────────────────
# Pipeline: Ingest raw orders from S3 CSV files into bronze Delta table
# Layer: Bronze (raw, no transformation, append-only)
# Trigger: Daily at 02:00 UTC

from pyspark.sql.functions import current_timestamp, input_file_name, lit
import dbutils

# Configuration (injected via Job parameters or Widgets)
dbutils.widgets.text("source_date", "",   "Source Date (yyyy-MM-dd)")
dbutils.widgets.text("catalog",     "dev","Target Catalog")

source_date  = dbutils.widgets.get("source_date") or "2024-03-26"
catalog      = dbutils.widgets.get("catalog")
source_path  = f"s3://my-bucket/raw/orders/{source_date}/"
target_table = f"{catalog}.bronze.orders"

# ── Step 1: Read ──
raw_df = spark.read \
    .format("csv") \
    .option("header",          "true") \
    .option("inferSchema",     "false") \
    .option("badRecordsPath",  f"/corrupt/{source_date}/") \
    .load(source_path)

# ── Step 2: Add ingestion metadata ──
bronze_df = raw_df \
    .withColumn("_ingested_at",   current_timestamp()) \
    .withColumn("_source_file",   input_file_name()) \
    .withColumn("_source_date",   lit(source_date)) \
    .withColumn("_pipeline_name", lit("bronze_orders_ingest"))

# ── Step 3: Data quality check ──
row_count = bronze_df.count()
assert row_count > 0, f"No records found in {source_path} — aborting"
print(f"[Bronze] Ingested {row_count:,} raw records from {source_path}")

# ── Step 4: Write to Bronze Delta table (append) ──
bronze_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"[Bronze] Successfully written to {target_table}")
```

---

### 2.5 Building a Silver Layer Transformation Pipeline

```python
# ─── silver_orders_transform.py ─────────────────────────────────────────────
# Pipeline: Clean and transform bronze orders → silver orders
# Layer: Silver (validated, typed, deduplicated)

from pyspark.sql.functions import *
from delta.tables import DeltaTable

catalog = dbutils.widgets.get("catalog") or "dev"
bronze_table = f"{catalog}.bronze.orders"
silver_table = f"{catalog}.silver.orders"

# ── Step 1: Read from Bronze ──
bronze_df = spark.read.table(bronze_table)

# ── Step 2: Transform ──
silver_df = (
    bronze_df
    .withColumn("order_id",    trim(col("order_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("product_id",  upper(trim(col("product_id"))))
    .withColumn("quantity",    col("quantity").cast("integer"))
    .withColumn("unit_price",  col("unit_price").cast("double"))
    .withColumn("order_date",  to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("total_amount",col("quantity") * col("unit_price"))
    .withColumn("status",      lower(trim(col("status"))))
    .withColumn("status",
        when(col("status").isin(["shipped", "dispatched"]),  "shipped")
        .when(col("status").isin(["pending", "open"]),        "pending")
        .when(col("status").isin(["cancelled", "canceled"]), "cancelled")
        .otherwise(col("status"))
    )
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("quantity") > 0)
    .filter(col("unit_price") >= 0)
    .dropDuplicates(["order_id"])
    .withColumn("_silver_processed_at", current_timestamp())
    .drop("_ingested_at", "_source_file", "_pipeline_name")   # drop bronze metadata
)

# ── Step 3: Data quality assertions ──
total_in  = bronze_df.count()
total_out = silver_df.count()
reject_pct = (total_in - total_out) / total_in * 100

print(f"[Silver] Input rows:  {total_in:,}")
print(f"[Silver] Output rows: {total_out:,}")
print(f"[Silver] Rejection:   {reject_pct:.2f}%")

assert reject_pct < 5.0, f"Rejection rate {reject_pct:.2f}% exceeds 5% threshold — investigate!"

# ── Step 4: Upsert to Silver ──
if DeltaTable.isDeltaTable(spark, silver_table):
    target = DeltaTable.forName(spark, silver_table)
    target.alias("tgt").merge(
        silver_df.alias("src"),
        "tgt.order_id = src.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"[Silver] MERGE complete → {silver_table}")
else:
    silver_df.write.format("delta").saveAsTable(silver_table)
    print(f"[Silver] TABLE CREATED → {silver_table}")
```

---

### 2.6 Building a Gold Layer Aggregation Pipeline

```python
# ─── gold_orders_aggregate.py ────────────────────────────────────────────────
# Pipeline: Build gold-layer KPI tables for BI reporting
# Layer: Gold (business-ready aggregates)

from pyspark.sql.functions import *

catalog = dbutils.widgets.get("catalog") or "dev"
silver_orders    = f"{catalog}.silver.orders"
silver_customers = f"{catalog}.silver.customers"
silver_products  = f"{catalog}.silver.products"

# ── Step 1: Read Silver tables ──
orders    = spark.read.table(silver_orders)
customers = spark.read.table(silver_customers)
products  = spark.read.table(silver_products)

# ── Step 2: Build Gold — Daily Revenue Summary ──
daily_revenue = (
    orders
    .filter(col("status") == "shipped")
    .groupBy("order_date", "status")
    .agg(
        count("order_id").alias("order_count"),
        sum("total_amount").alias("revenue"),
        avg("total_amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

# ── Step 3: Build Gold — Customer Lifetime Value ──
customer_ltv = (
    orders
    .filter(col("status") == "shipped")
    .join(customers.select("customer_id", "name", "country", "tier"),
          on="customer_id", how="left")
    .groupBy("customer_id", "name", "country", "tier")
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_amount").alias("lifetime_value"),
        min("order_date").alias("first_order_date"),
        max("order_date").alias("last_order_date"),
        avg("total_amount").alias("avg_order_value")
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

# ── Step 4: Build Gold — Product Performance ──
product_performance = (
    orders
    .filter(col("status").isin(["shipped", "pending"]))
    .join(products.select("product_id", "name", "category", "cost_price"),
          on="product_id", how="left")
    .groupBy("product_id", "name", "category")
    .agg(
        sum("quantity").alias("total_units_sold"),
        sum("total_amount").alias("gross_revenue"),
        sum(col("quantity") * col("cost_price")).alias("total_cogs"),
        (sum("total_amount") - sum(col("quantity") * col("cost_price"))).alias("gross_profit")
    )
    .withColumn("margin_pct", round(col("gross_profit") / col("gross_revenue") * 100, 2))
    .withColumn("_gold_processed_at", current_timestamp())
)

# ── Step 5: Write Gold tables (overwrite — full refresh from silver) ──
daily_revenue.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.daily_revenue")

customer_ltv.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.customer_ltv")

product_performance.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.product_performance")

print("[Gold] All KPI tables refreshed successfully.")
```

---

### 2.7 Incremental Processing with Change Data Capture

For large tables where full refresh is too slow, process **only the new/changed data** since the last run.

```python
# ── Incremental Processing Pattern using Delta CDF (Change Data Feed) ──

# Step 1: Enable CDF on the Silver table
spark.sql("""
    ALTER TABLE silver.orders
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# Step 2: Track the last processed version (use a state table or external store)
state_df = spark.sql("SELECT MAX(last_version) FROM state.pipeline_state WHERE table_name = 'silver.orders'")
last_version = state_df.collect()[0][0] or 0

# Step 3: Read only changes since last version
from pyspark.sql.functions import col

changes = spark.read \
    .format("delta") \
    .option("readChangeFeed",    "true") \
    .option("startingVersion",   last_version + 1) \
    .table("silver.orders") \
    .filter(col("_change_type").isin(["insert", "update_postimage"]))   # ignore pre-image rows

current_version = spark.sql("SELECT MAX(version) FROM (DESCRIBE HISTORY silver.orders)").collect()[0][0]

# Step 4: Apply incremental changes to Gold
if changes.count() > 0:
    changes.createOrReplaceTempView("order_changes")
    # ... apply your gold transformations on changes only ...

    # Step 5: Save state
    spark.sql(f"""
        MERGE INTO state.pipeline_state AS tgt
        USING (SELECT 'silver.orders' AS table_name, {current_version} AS last_version) AS src
        ON tgt.table_name = src.table_name
        WHEN MATCHED THEN UPDATE SET tgt.last_version = src.last_version
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"[Incremental] Processed {changes.count():,} changes. State updated to v{current_version}.")
else:
    print("[Incremental] No new changes found.")
```

---

### 2.8 Error Handling and Data Quality Checks

```python
# ── Robust pipeline with error handling and quarantine pattern ──

from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql import DataFrame

def validate_and_split(df: DataFrame, rules: dict) -> tuple:
    """
    Splits a DataFrame into valid rows and quarantined (failed) rows.

    rules: dict mapping rule_name → SQL expression (returns True if valid)
    """
    from functools import reduce
    from pyspark.sql.functions import expr, concat_ws, array, when

    # Identify failed rules for each row
    rule_checks = [
        when(~expr(sql_expr), lit(rule_name)).otherwise(lit(None))
        for rule_name, sql_expr in rules.items()
    ]

    df_with_failures = df.withColumn(
        "_failed_rules",
        concat_ws(", ", *rule_checks)
    )

    valid      = df_with_failures.filter(col("_failed_rules") == "").drop("_failed_rules")
    quarantine = df_with_failures.filter(col("_failed_rules") != "") \
                                 .withColumn("_quarantined_at", current_timestamp())

    return valid, quarantine


# Define your data quality rules
quality_rules = {
    "order_id_not_null":   "order_id IS NOT NULL",
    "positive_quantity":   "quantity > 0",
    "non_negative_price":  "unit_price >= 0",
    "valid_status":        "status IN ('shipped', 'pending', 'cancelled')",
    "valid_date":          "order_date >= '2020-01-01' AND order_date <= current_date()",
}

valid_df, quarantine_df = validate_and_split(bronze_df, quality_rules)

# Write valid records to Silver
valid_df.write.format("delta").mode("append").saveAsTable("silver.orders")

# Write quarantined records for investigation
if quarantine_df.count() > 0:
    quarantine_df.write.format("delta").mode("append").saveAsTable("quarantine.orders")
    print(f"[WARNING] {quarantine_df.count():,} records quarantined — check quarantine.orders")

print(f"[Quality] Valid: {valid_df.count():,} | Quarantined: {quarantine_df.count():,}")
```

---

## 3. Scheduling and Automating Data Pipelines

### 3.1 Databricks Jobs — Overview

A **Databricks Job** is the primary mechanism for scheduling and automating notebooks, Python scripts, JARs, and wheel files. Jobs support:

- **Single-task** or **multi-task** (DAG) execution.
- **Scheduled**, **triggered**, or **manually run** execution.
- **Job clusters** (spun up fresh per run) or **interactive clusters** (shared).
- **Retry policies**, **timeout settings**, and **alerts**.

```
Databricks Workspace
  └── Jobs
        ├── Job: "Daily ETL Pipeline"
        │     ├── Task A: bronze_orders_ingest   (runs first)
        │     ├── Task B: silver_orders_transform (depends on A)
        │     ├── Task C: gold_orders_aggregate   (depends on B)
        │     └── Task D: send_report_email       (depends on C)
        │
        └── Job: "Hourly Streaming Checkpoint"
              └── Task A: restart_streaming_query
```

---

### 3.2 Creating a Job from a Notebook

**Via the Databricks UI:**

1. Navigate to **Workflows** → **Jobs** → **Create Job**.
2. Give the job a name.
3. Under **Task**, choose **Notebook** and select your notebook.
4. Configure the **Cluster** (new job cluster recommended).
5. Under **Schedule**, choose **Scheduled** and set the cron expression.
6. Add **Alerts** for failure/success notifications.

**Via the Databricks CLI (automation):**

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Create a job from a JSON specification
databricks jobs create --json '{
  "name": "Daily ETL Pipeline",
  "tasks": [
    {
      "task_key": "bronze_ingest",
      "notebook_task": {
        "notebook_path": "/Shared/pipelines/bronze_orders_ingest",
        "base_parameters": {
          "catalog": "prod",
          "source_date": "{{job.start_time.iso_date}}"
        }
      },
      "new_cluster": {
        "spark_version":  "14.3.x-scala2.12",
        "node_type_id":   "i3.xlarge",
        "num_workers":    4
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  }
}'
```

---

### 3.3 Job Scheduling Options

| Schedule Type      | Cron Expression  | Description            |
| ------------------ | ---------------- | ---------------------- |
| Every day at 2AM   | `0 0 2 * * ?`    | Daily batch run        |
| Every hour         | `0 0 * * * ?`    | Hourly refresh         |
| Every 15 minutes   | `0 0/15 * * * ?` | Near-real-time         |
| Every Monday 6AM   | `0 0 6 ? * MON`  | Weekly report          |
| First day of month | `0 0 0 1 * ?`    | Monthly reconciliation |

Databricks Jobs use **Quartz cron syntax**: `seconds minutes hours day-of-month month day-of-week year`.

**Trigger-based scheduling** (recommended for event-driven pipelines):

```python
# Trigger a downstream job when an Autoloader stream finishes
# Use the Jobs API to chain jobs
import requests

def trigger_downstream_job(job_id: int, params: dict):
    token = dbutils.secrets.get("databricks-scope", "api-token")
    url   = f"https://<workspace-url>/api/2.1/jobs/run-now"

    response = requests.post(url,
        headers = {"Authorization": f"Bearer {token}"},
        json    = {"job_id": job_id, "notebook_params": params}
    )
    response.raise_for_status()
    return response.json()["run_id"]
```

---

### 3.4 Multi-Task Jobs and Task Dependencies

Multi-task jobs define a **DAG** (Directed Acyclic Graph) of tasks:

```
                    ┌──────────────────┐
                    │  Task A          │
                    │  bronze_ingest   │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
     ┌────────▼──────┐  ┌────▼────┐  ┌─────▼──────┐
     │  Task B       │  │Task C   │  │ Task D     │
     │ silver_orders │  │silver_  │  │ silver_    │
     │ _transform    │  │customers│  │ products   │
     └────────┬──────┘  └────┬────┘  └─────┬──────┘
              │              │              │
              └──────────────▼──────────────┘
                             │
                    ┌────────▼─────────┐
                    │  Task E          │
                    │  gold_aggregate  │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  Task F          │
                    │  notify_success  │
                    └──────────────────┘
```

```python
# Define multi-task job via Python SDK (databricks-sdk)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

job = w.jobs.create(
    name="Full ETL Pipeline",
    tasks=[
        jobs.Task(
            task_key         = "bronze_ingest",
            notebook_task    = jobs.NotebookTask(
                notebook_path    = "/pipelines/bronze_orders_ingest",
                base_parameters  = {"catalog": "prod"}
            ),
            new_cluster      = jobs.ClusterSpec(
                spark_version = "14.3.x-scala2.12",
                node_type_id  = "i3.xlarge",
                num_workers   = 4
            )
        ),
        jobs.Task(
            task_key         = "silver_transform",
            depends_on       = [jobs.TaskDependency(task_key="bronze_ingest")],
            notebook_task    = jobs.NotebookTask(
                notebook_path    = "/pipelines/silver_orders_transform",
                base_parameters  = {"catalog": "prod"}
            ),
            job_cluster_key  = "shared_cluster"  # reuse a cluster defined at job level
        ),
        jobs.Task(
            task_key         = "gold_aggregate",
            depends_on       = [jobs.TaskDependency(task_key="silver_transform")],
            notebook_task    = jobs.NotebookTask(
                notebook_path    = "/pipelines/gold_orders_aggregate",
                base_parameters  = {"catalog": "prod"}
            ),
            job_cluster_key  = "shared_cluster"
        ),
    ]
)
print(f"Created job {job.job_id}")
```

---

### 3.5 Parameterizing Jobs with Widgets

**Databricks Widgets** allow notebooks to accept runtime parameters from Jobs:

```python
# In your notebook — define widgets
dbutils.widgets.text("catalog",         "dev",          "Target Catalog")
dbutils.widgets.text("source_date",     "",             "Source Date (yyyy-MM-dd)")
dbutils.widgets.dropdown("run_mode",    "incremental",  ["full", "incremental"])
dbutils.widgets.combobox("environment", "dev",          ["dev", "staging", "prod"])

# Read widget values
catalog      = dbutils.widgets.get("catalog")
source_date  = dbutils.widgets.get("source_date")
run_mode     = dbutils.widgets.get("run_mode")
environment  = dbutils.widgets.get("environment")

print(f"Running in {environment} mode={run_mode} for date={source_date}")

# When running interactively (notebook), use the widgets UI
# When running as a Job, pass parameters in the Job configuration:
# {
#   "catalog": "prod",
#   "source_date": "2024-03-26",
#   "run_mode": "incremental"
# }

# Clean up widgets at the end
dbutils.widgets.removeAll()
```

---

### 3.6 Job Clusters vs Interactive Clusters

| Aspect           | Job Cluster                             | Interactive Cluster                |
| ---------------- | --------------------------------------- | ---------------------------------- |
| **Lifecycle**    | Created at job start, terminated at end | Always on (or auto-terminating)    |
| **Cost**         | Pay per run only                        | Pay while running (idle waste)     |
| **Isolation**    | Fully isolated per run                  | Shared — other users affect it     |
| **Performance**  | Consistent (fresh environment)          | Variable (depends on current load) |
| **Use Case**     | Production scheduled jobs               | Development, exploration           |
| **Startup time** | ~3–5 minutes cold start                 | Instant (already running)          |

**Recommendation:** Always use **Job Clusters** for production pipelines. Use **interactive clusters** only during development.

---

## 4. Monitoring and Managing Data Pipelines

### 4.1 Job Run Monitoring in the UI

The **Databricks Workflows** UI provides a complete view of job execution:

```
Workflows → Jobs → [Your Job] → Runs Tab

Each run shows:
  ✓ Status:          Succeeded / Failed / Running / Pending
  ✓ Start Time:      2024-03-26 02:01:34
  ✓ Duration:        14m 32s
  ✓ Triggered by:    SCHEDULED
  ✓ Task breakdown:  [bronze: 3m] [silver: 6m] [gold: 4m] [notify: 1m]
  ✓ Spark UI link:   Direct link to the Spark History Server
  ✓ Output logs:     stdout + stderr from each notebook
```

For multi-task jobs, the **Run Graph** view shows the task DAG coloured by status (green/red/grey) with per-task timing.

---

### 4.2 Structured Logging with Log4j and print

```python
import logging

# Set up Python logging
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("ETL.SilverOrders")

# Use logger throughout your pipeline
logger.info(f"Starting silver_orders_transform for catalog={catalog}")
logger.info(f"Reading from {bronze_table}")

try:
    silver_df = transform_silver_orders(bronze_df)
    row_count = silver_df.count()
    logger.info(f"Transformation complete. Output rows: {row_count:,}")
except Exception as e:
    logger.error(f"Transformation failed: {str(e)}", exc_info=True)
    raise

# Use dbutils.notebook.exit() to pass structured output to parent jobs
import json
result = {
    "status":      "success",
    "rows_written": row_count,
    "table":        silver_table,
    "run_date":     source_date
}
dbutils.notebook.exit(json.dumps(result))
```

---

### 4.3 Alerting on Job Failures

**Email alerts (built-in):**
Configure under **Job settings → Notifications**:

- On start, success, or failure.
- Email to one or more addresses.

**Slack / PagerDuty via webhook:**

```python
import requests, json

def send_alert(webhook_url: str, job_name: str, status: str, message: str):
    payload = {
        "text": f":{'white_check_mark' if status == 'success' else 'red_circle'} "
                f"*{job_name}* — `{status.upper()}`\n{message}"
    }
    requests.post(webhook_url, data=json.dumps(payload),
                  headers={"Content-Type": "application/json"})

# In your notebook's final cell
try:
    # run pipeline
    send_alert(SLACK_WEBHOOK, "Daily ETL Pipeline", "success",
               f"Processed {row_count:,} rows in {duration:.1f}s")
except Exception as e:
    send_alert(SLACK_WEBHOOK, "Daily ETL Pipeline", "failure", str(e))
    raise
```

---

### 4.4 Pipeline Metrics with Spark UI

The **Spark UI** is accessible directly from a notebook cell or from a Job run:

```python
# Print Spark UI URL in your notebook for easy access
print(f"Spark UI: {spark.sparkContext.uiWebUrl}")

# Force metrics collection before reading them
df.cache()
df.count()   # triggers actual computation

# Programmatic access to job/stage metrics
sc = spark.sparkContext
status = sc.statusTracker()

for job_id in status.getJobIdsForGroup(None):
    job_info = status.getJobInfo(job_id)
    print(f"Job {job_id}: {job_info.status()}")
    for stage_id in job_info.stageIds():
        stage_info = status.getStageInfo(stage_id)
        if stage_info:
            print(f"  Stage {stage_id}: "
                  f"tasks={stage_info.numActiveTasks()}/{stage_info.numTasks()}")
```

**Key metrics to watch in Spark UI:**

| Metric                 | What to Look For                                  |
| ---------------------- | ------------------------------------------------- |
| **Shuffle Read/Write** | Very high = consider broadcast join or Z-ORDER    |
| **GC Time %**          | > 5% = reduce partition size or increase memory   |
| **Task Skew**          | One task 10× slower than others = data skew       |
| **Stage Duration**     | Unexpectedly slow stages = look for spill to disk |
| **Input Size**         | Matches expected data volume                      |

---

### 4.5 Data Quality Monitoring

```python
# ── Build a data quality report table ──
from pyspark.sql.functions import col, count, sum, isnan, when, current_timestamp, lit

def compute_quality_metrics(df, table_name: str, run_date: str):
    total_rows = df.count()
    metrics    = []

    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull() | isnan(col(col_name))).count()
        metrics.append({
            "table_name":  table_name,
            "column_name": col_name,
            "total_rows":  total_rows,
            "null_count":  null_count,
            "null_pct":    round(null_count / total_rows * 100, 2),
            "run_date":    run_date,
            "checked_at":  str(current_timestamp())
        })

    return spark.createDataFrame(metrics)

quality_report = compute_quality_metrics(silver_df, "silver.orders", source_date)

quality_report.write.format("delta") \
    .mode("append") \
    .saveAsTable("monitoring.data_quality_metrics")

# Alert if any critical column exceeds 1% NULL rate
critical_columns = ["order_id", "customer_id", "order_date"]
for row in quality_report.filter(col("column_name").isin(critical_columns)).collect():
    assert row["null_pct"] < 1.0, \
        f"ALERT: {row['column_name']} has {row['null_pct']}% NULLs in {table_name}"
```

---

### 4.6 Retrying and Recovering Failed Pipelines

```python
# ── Automatic retry with exponential back-off ──
import time
from functools import wraps

def retry(max_attempts=3, backoff_seconds=10):
    """Decorator: retry a function on failure with exponential back-off."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        raise
                    wait = backoff_seconds * (2 ** (attempt - 1))
                    print(f"[Retry] Attempt {attempt} failed: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
        return wrapper
    return decorator

@retry(max_attempts=3, backoff_seconds=15)
def run_transformation():
    silver_df = transform_silver_orders(bronze_df)
    silver_df.write.format("delta").mode("append").saveAsTable("silver.orders")

run_transformation()
```

**Job-level retry (built-in):**
In Databricks Job settings, set:

- `Max retries: 2`
- `Min Retry Interval: 5 minutes`

This handles transient infrastructure failures (e.g., spot instance termination) without any code changes.

---

## 5. Case Study: Implementing a Complete Data Pipeline

### 5.1 Business Scenario

**Company:** RetailCo — an e-commerce retailer  
**Requirement:** Build a daily data pipeline to:

1. Ingest order data from S3 (CSV files dropped by the transactional system nightly).
2. Clean and validate orders, joining with customer and product reference data.
3. Produce daily KPI tables: revenue by region, top products, customer LTV.
4. Schedule to run at 03:00 UTC every day.
5. Alert the data team on failure.

---

### 5.2 Pipeline Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                     RetailCo Daily ETL Pipeline                    │
│                                                                    │
│  S3: raw/orders/        S3: raw/customers/     S3: raw/products/   │
│        │                       │                      │            │
│        ▼                       ▼                      ▼            │
│  ┌─────────────┐        ┌──────────────┐       ┌─────────────┐    │
│  │ TASK A      │        │ TASK B       │       │ TASK C      │    │
│  │ Bronze      │        │ Customer     │       │ Product     │    │
│  │ Orders      │        │ Ref Load     │       │ Ref Load    │    │
│  │ Ingest      │        │              │       │             │    │
│  └──────┬──────┘        └──────┬───────┘       └──────┬──────┘    │
│         │                      │                      │            │
│         └──────────────────────▼──────────────────────┘            │
│                                │                                   │
│                        ┌───────▼──────────┐                        │
│                        │ TASK D           │                        │
│                        │ Silver Orders    │                        │
│                        │ Transform        │                        │
│                        └───────┬──────────┘                        │
│                                │                                   │
│                        ┌───────▼──────────┐                        │
│                        │ TASK E           │                        │
│                        │ Gold KPI         │                        │
│                        │ Aggregation      │                        │
│                        └───────┬──────────┘                        │
│                                │                                   │
│                        ┌───────▼──────────┐                        │
│                        │ TASK F           │                        │
│                        │ Notify Success / │                        │
│                        │ Failure Alert    │                        │
│                        └──────────────────┘                        │
└────────────────────────────────────────────────────────────────────┘
```

---

### 5.3 Implementation: Bronze Layer

```python
# Notebook: /pipelines/retailco/01_bronze_ingest
# Task A: Load raw CSV order files from S3

dbutils.widgets.text("run_date", "", "Run Date (yyyy-MM-dd)")
run_date = dbutils.widgets.get("run_date") or "2024-03-26"

raw_path = f"s3://retailco-data/raw/orders/{run_date}/*.csv"

bronze_df = (
    spark.read
        .format("csv")
        .option("header",         "true")
        .option("inferSchema",    "false")
        .option("badRecordsPath", f"s3://retailco-data/quarantine/bronze/{run_date}/")
        .load(raw_path)
        .withColumn("_run_date",      lit(run_date))
        .withColumn("_ingested_at",   current_timestamp())
        .withColumn("_source_file",   input_file_name())
)

row_count = bronze_df.count()
assert row_count > 0, f"No files found at {raw_path}"

bronze_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("_run_date") \
    .saveAsTable("retailco.bronze.orders")

dbutils.notebook.exit(json.dumps({"rows": row_count, "status": "ok"}))
```

---

### 5.4 Implementation: Silver Layer

```python
# Notebook: /pipelines/retailco/04_silver_transform
# Task D: Transform and validate orders (depends on A, B, C)

dbutils.widgets.text("run_date", "", "Run Date")
run_date = dbutils.widgets.get("run_date")

# Read today's bronze partition
bronze    = spark.read.table("retailco.bronze.orders").filter(f"_run_date = '{run_date}'")
customers = spark.read.table("retailco.silver.customers")   # reference table
products  = spark.read.table("retailco.silver.products")     # reference table

# Transform
silver = (
    bronze
    .withColumn("order_id",    trim(col("order_id")))
    .withColumn("quantity",    col("quantity").cast("integer"))
    .withColumn("unit_price",  col("unit_price").cast("double"))
    .withColumn("order_date",  to_date(col("order_date")))
    .withColumn("total",       col("quantity") * col("unit_price"))
    .filter("order_id IS NOT NULL AND customer_id IS NOT NULL")
    .filter("quantity > 0 AND unit_price >= 0")
    .dropDuplicates(["order_id"])
    # ── Enrich: join with customer region ──
    .join(customers.select("customer_id", "region"), on="customer_id", how="left")
    # ── Enrich: join with product category ──
    .join(products.select("product_id", "category"), on="product_id", how="left")
    .withColumn("_silver_processed_at", current_timestamp())
    .drop("_run_date", "_ingested_at", "_source_file")
)

# Upsert
target = DeltaTable.forName(spark, "retailco.silver.orders")
target.alias("t").merge(silver.alias("s"), "t.order_id = s.order_id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

dbutils.notebook.exit(json.dumps({"rows": silver.count(), "status": "ok"}))
```

---

### 5.5 Implementation: Gold Layer

```python
# Notebook: /pipelines/retailco/05_gold_kpis
# Task E: Aggregate to KPI tables (depends on Task D)

silver = spark.read.table("retailco.silver.orders")

# KPI 1: Revenue by region and day
daily_regional_revenue = (
    silver.filter("status = 'shipped'")
          .groupBy("order_date", "region")
          .agg(
              count("*").alias("orders"),
              sum("total").alias("revenue"),
              avg("total").alias("avg_order_value")
          )
)
daily_regional_revenue.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("retailco.gold.daily_regional_revenue")

# KPI 2: Top 20 products by revenue
top_products = (
    silver.filter("status != 'cancelled'")
          .groupBy("product_id", "category")
          .agg(sum("total").alias("revenue"), sum("quantity").alias("units"))
          .orderBy(col("revenue").desc())
          .limit(20)
)
top_products.write.format("delta").mode("overwrite").saveAsTable("retailco.gold.top_products")

print("[Gold] KPI tables refreshed.")
dbutils.notebook.exit(json.dumps({"status": "ok"}))
```

---

### 5.6 Orchestrating the Pipeline as a Job

```json
{
  "name": "RetailCo Daily ETL",
  "schedule": {
    "quartz_cron_expression": "0 0 3 * * ?",
    "timezone_id": "UTC",
    "pause_status": "UNPAUSED"
  },
  "email_notifications": {
    "on_failure": ["data-team@retailco.com"],
    "on_success": ["data-team@retailco.com"]
  },
  "max_concurrent_runs": 1,
  "job_clusters": [
    {
      "job_cluster_key": "etl_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "i3.2xlarge",
        "num_workers": 8,
        "aws_attributes": { "availability": "SPOT_WITH_FALLBACK" }
      }
    }
  ],
  "tasks": [
    {
      "task_key": "bronze_ingest",
      "notebook_task": {
        "notebook_path": "/pipelines/retailco/01_bronze_ingest",
        "base_parameters": { "run_date": "{{job.start_time.iso_date}}" }
      },
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "silver_transform",
      "depends_on": [{ "task_key": "bronze_ingest" }],
      "notebook_task": {
        "notebook_path": "/pipelines/retailco/04_silver_transform",
        "base_parameters": { "run_date": "{{job.start_time.iso_date}}" }
      },
      "job_cluster_key": "etl_cluster",
      "retry_on_timeout": true,
      "max_retries": 2
    },
    {
      "task_key": "gold_kpis",
      "depends_on": [{ "task_key": "silver_transform" }],
      "notebook_task": {
        "notebook_path": "/pipelines/retailco/05_gold_kpis",
        "base_parameters": { "run_date": "{{job.start_time.iso_date}}" }
      },
      "job_cluster_key": "etl_cluster"
    }
  ]
}
```

---

## 6. Summary

| Topic                      | Key Points                                                            |
| -------------------------- | --------------------------------------------------------------------- |
| **Medallion Architecture** | Bronze (raw) → Silver (clean) → Gold (aggregated) tiers               |
| **ETL Pattern**            | Extract from sources, transform for quality, load to Delta with MERGE |
| **Bronze Layer**           | Append-only raw copy with ingestion metadata                          |
| **Silver Layer**           | Typed, validated, deduplicated, enriched — MERGE for idempotency      |
| **Gold Layer**             | Business KPIs and aggregates — full overwrite on each run             |
| **Databricks Jobs**        | Multi-task DAGs with scheduling, retry, and alerting                  |
| **Job Clusters**           | Use for production; spin up/down per run for cost efficiency          |
| **Widgets**                | Parameterize notebooks for reusable, configurable pipelines           |
| **Error Handling**         | Quarantine invalid records, retry transient failures, alert on breach |
| **Quality Monitoring**     | Track NULL rates, row counts, rejection ratios in a metrics table     |

**What comes next:** The next guide — **Deploy Workloads with Databricks Workflows** — expands on everything covered here by introducing the full **Workflows** feature set: multi-task dependencies, repair runs, conditional branching, and complex orchestration patterns.

---
