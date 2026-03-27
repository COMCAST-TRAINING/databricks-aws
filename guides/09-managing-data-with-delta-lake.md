# Managing Data with Delta Lake

---

## Table of Contents

1. [Overview of Delta Lake and Its Benefits](#1-overview-of-delta-lake-and-its-benefits)
   - 1.1 [What Is Delta Lake?](#11-what-is-delta-lake)
   - 1.2 [The Problem Delta Lake Solves](#12-the-problem-delta-lake-solves)
   - 1.3 [Delta Lake Architecture](#13-delta-lake-architecture)
   - 1.4 [The Delta Transaction Log (`_delta_log`)](#14-the-delta-transaction-log-_delta_log)
   - 1.5 [Key Benefits of Delta Lake](#15-key-benefits-of-delta-lake)
   - 1.6 [Delta Lake vs Parquet vs Hive](#16-delta-lake-vs-parquet-vs-hive)
2. [Creating and Managing Delta Tables](#2-creating-and-managing-delta-tables)
   - 2.1 [Creating a Delta Table from a DataFrame](#21-creating-a-delta-table-from-a-dataframe)
   - 2.2 [Creating a Delta Table with SQL DDL](#22-creating-a-delta-table-with-sql-ddl)
   - 2.3 [Reading Delta Tables](#23-reading-delta-tables)
   - 2.4 [Inserting Data into Delta Tables](#24-inserting-data-into-delta-tables)
   - 2.5 [Updating Rows in Delta Tables](#25-updating-rows-in-delta-tables)
   - 2.6 [Deleting Rows from Delta Tables](#26-deleting-rows-from-delta-tables)
   - 2.7 [Merging Data (Upserts) with MERGE INTO](#27-merging-data-upserts-with-merge-into)
   - 2.8 [Schema Evolution](#28-schema-evolution)
   - 2.9 [Table Properties and Metadata](#29-table-properties-and-metadata)
3. [Implementing ACID Transactions with Delta Lake](#3-implementing-acid-transactions-with-delta-lake)
   - 3.1 [What Are ACID Transactions?](#31-what-are-acid-transactions)
   - 3.2 [Atomicity in Delta Lake](#32-atomicity-in-delta-lake)
   - 3.3 [Consistency in Delta Lake](#33-consistency-in-delta-lake)
   - 3.4 [Isolation in Delta Lake](#34-isolation-in-delta-lake)
   - 3.5 [Durability in Delta Lake](#35-durability-in-delta-lake)
   - 3.6 [Optimistic Concurrency Control](#36-optimistic-concurrency-control)
   - 3.7 [Handling Concurrent Writes](#37-handling-concurrent-writes)
4. [Autoloader in Delta Lake](#4-autoloader-in-delta-lake)
   - 4.1 [What Is Autoloader?](#41-what-is-autoloader)
   - 4.2 [How Autoloader Works Internally](#42-how-autoloader-works-internally)
   - 4.3 [Directory Listing vs File Notification Mode](#43-directory-listing-vs-file-notification-mode)
   - 4.4 [Writing Your First Autoloader Stream](#44-writing-your-first-autoloader-stream)
   - 4.5 [Schema Inference and Evolution with Autoloader](#45-schema-inference-and-evolution-with-autoloader)
   - 4.6 [Autoloader with Checkpointing](#46-autoloader-with-checkpointing)
   - 4.7 [Common Autoloader Patterns](#47-common-autoloader-patterns)
5. [Time Travel and Data Versioning in Delta Lake](#5-time-travel-and-data-versioning-in-delta-lake)
   - 5.1 [How Versioning Works](#51-how-versioning-works)
   - 5.2 [Querying Previous Versions by Version Number](#52-querying-previous-versions-by-version-number)
   - 5.3 [Querying Previous Versions by Timestamp](#53-querying-previous-versions-by-timestamp)
   - 5.4 [Restoring a Table to a Previous Version](#54-restoring-a-table-to-a-previous-version)
   - 5.5 [Viewing Table History](#55-viewing-table-history)
   - 5.6 [Vacuuming Old Data Files](#56-vacuuming-old-data-files)
   - 5.7 [Practical Use Cases for Time Travel](#57-practical-use-cases-for-time-travel)
6. [Delta Lake Optimization](#6-delta-lake-optimization)
   - 6.1 [OPTIMIZE and File Compaction](#61-optimize-and-file-compaction)
   - 6.2 [Z-Ordering for Data Skipping](#62-z-ordering-for-data-skipping)
   - 6.3 [Data Skipping and Statistics](#63-data-skipping-and-statistics)
   - 6.4 [Partitioning Delta Tables](#64-partitioning-delta-tables)
7. [Summary](#7-summary)

---

## 1. Overview of Delta Lake and Its Benefits

### 1.1 What Is Delta Lake?

**Delta Lake** is an open-source storage layer that brings **ACID transactions**, **scalable metadata handling**, and **unified batch/streaming** processing to Apache Spark and other data engines. It sits on top of your existing cloud object storage (AWS S3, Azure Data Lake, GCS) and stores data as **Parquet files** enhanced with a **transaction log**.

Delta Lake was **open-sourced by Databricks in 2019** and is now a Linux Foundation project. It is the foundational storage format of the Databricks Lakehouse Platform.

```
┌──────────────────────────────────────────────────────────────┐
│                    Databricks Lakehouse                       │
│                                                              │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│   │   Spark SQL  │  │  PySpark     │  │   Streaming  │     │
│   │   Queries    │  │  DataFrames  │  │   Jobs       │     │
│   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘     │
│          │                 │                  │              │
│   ┌──────▼─────────────────▼──────────────────▼───────┐     │
│   │                    DELTA LAKE                       │     │
│   │                                                    │     │
│   │  ┌─────────────────┐   ┌──────────────────────┐   │     │
│   │  │  Parquet Files  │   │  _delta_log/         │   │     │
│   │  │  (actual data)  │   │  (transaction log)   │   │     │
│   │  └─────────────────┘   └──────────────────────┘   │     │
│   └────────────────────────────────────────────────────┘     │
│                                                              │
│   ┌────────────────────────────────────────────────────┐     │
│   │           Cloud Object Storage (S3 / ADLS / GCS)   │     │
│   └────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────┘
```

---

### 1.2 The Problem Delta Lake Solves

Before Delta Lake, data lakes suffered from significant reliability and usability problems:

**Problem 1 — No ACID transactions:**

- Multiple writers could corrupt a table by writing partial results simultaneously.
- A failed job left partially written data, making the table unreadable.

**Problem 2 — No schema enforcement:**

- Any program could append data with wrong column types or missing columns, silently corrupting data quality.

**Problem 3 — Difficult data corrections:**

- To delete or update a record in a Parquet data lake, you had to rewrite entire partitions.
- There was no native `UPDATE`, `DELETE`, or `MERGE` support.

**Problem 4 — Slow metadata operations:**

- Listing millions of files to discover what data exists was slow and expensive.
- Partition discovery required scanning all of S3.

**Problem 5 — No data versioning:**

- Once data was overwritten, previous versions were lost.
- Debugging production data issues required complex data recovery procedures.

Delta Lake eliminates all these problems.

---

### 1.3 Delta Lake Architecture

A Delta table is stored as a **directory** on cloud storage containing:

```
my_delta_table/
├── _delta_log/                          ← Transaction log (the brain)
│   ├── 00000000000000000000.json        ← Commit 0: table creation
│   ├── 00000000000000000001.json        ← Commit 1: first insert
│   ├── 00000000000000000002.json        ← Commit 2: update/delete
│   ├── 00000000000000000003.json        ← Commit 3: schema change
│   └── 00000000000000000010.checkpoint.parquet  ← Checkpoint every 10 commits
├── part-00000-abc123.snappy.parquet     ← Data file (active)
├── part-00001-def456.snappy.parquet     ← Data file (active)
└── part-00002-ghi789.snappy.parquet     ← Data file (logically deleted, awaiting VACUUM)
```

---

### 1.4 The Delta Transaction Log (`_delta_log`)

The `_delta_log` is the heart of Delta Lake. Every operation on a Delta table — CREATE, INSERT, UPDATE, DELETE, MERGE, OPTIMIZE — writes a **JSON commit file** that records:

- **`add`** — files added by this transaction.
- **`remove`** — files logically deleted by this transaction.
- **`metaData`** — schema changes, partition information.
- **`commitInfo`** — who made the change, when, and what operation.

```
# Example commit file contents (simplified):
{
  "commitInfo": {
    "timestamp": 1711440000000,
    "operation": "WRITE",
    "operationParameters": {"mode": "Append"},
    "userName": "user@company.com"
  }
}
{
  "add": {
    "path": "part-00000-abc123.snappy.parquet",
    "size": 1048576,
    "stats": "{\"numRecords\":1000,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":1000}}"
  }
}
```

**Checkpointing:** Every 10 commits (configurable), Delta Lake writes a **Parquet checkpoint** file that aggregates all commit history. This prevents reading thousands of JSON files to reconstruct the current table state.

---

### 1.5 Key Benefits of Delta Lake

| Feature                  | Description                                                                      |
| ------------------------ | -------------------------------------------------------------------------------- |
| **ACID Transactions**    | Atomicity, Consistency, Isolation, Durability for all table operations           |
| **Scalable Metadata**    | Transaction log uses Spark to process metadata for tables with billions of files |
| **Schema Enforcement**   | Rejects writes that don't match the table schema                                 |
| **Schema Evolution**     | Allows safe addition of new columns to an existing table                         |
| **Upserts (MERGE)**      | Native `MERGE INTO` for insert/update/delete in one operation                    |
| **Time Travel**          | Query any previous version of a table by version number or timestamp             |
| **Unified Batch/Stream** | Same table can be written by batch jobs and read by streaming queries            |
| **Data Skipping**        | Per-file statistics enable skipping irrelevant files at query time               |
| **Audit History**        | Complete record of every operation performed on the table                        |
| **OPTIMIZE & Z-Order**   | File compaction and multi-dimensional clustering for fast queries                |

---

### 1.6 Delta Lake vs Parquet vs Hive

| Capability              | Plain Parquet |  Hive Tables   | Delta Lake |
| ----------------------- | :-----------: | :------------: | :--------: |
| ACID Transactions       |       ✗       |    Partial     |     ✓      |
| Schema Enforcement      |       ✗       |       ✗        |     ✓      |
| UPDATE / DELETE / MERGE |       ✗       |    Limited     |     ✓      |
| Time Travel             |       ✗       |       ✗        |     ✓      |
| Streaming + Batch       |       ✗       |       ✗        |     ✓      |
| Scalable Metadata       |       ✗       |       ✗        |     ✓      |
| Data Skipping           |       ✗       | Partition only |     ✓      |
| Open Format             |       ✓       |       ✓        |     ✓      |

---

## 2. Creating and Managing Delta Tables

### 2.1 Creating a Delta Table from a DataFrame

The most common way to create a Delta table is to write a DataFrame using the `delta` format:

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.functions import current_date

# Define schema
schema = StructType([
    StructField("customer_id",  StringType(),  nullable=False),
    StructField("name",         StringType(),  nullable=True),
    StructField("email",        StringType(),  nullable=True),
    StructField("country",      StringType(),  nullable=True),
    StructField("total_spend",  DoubleType(),  nullable=True),
    StructField("join_date",    DateType(),    nullable=True),
])

# Sample data
data = [
    ("C001", "Alice Smith",   "alice@email.com",  "USA",    15000.0, "2022-01-15"),
    ("C002", "Bob Jones",     "bob@email.com",    "UK",      8500.0, "2022-03-20"),
    ("C003", "Carol White",   "carol@email.com",  "Canada", 22000.0, "2021-11-05"),
    ("C004", "David Brown",   "david@email.com",  "USA",    11000.0, "2023-02-28"),
    ("C005", "Eve Martinez",  "eve@email.com",    "Spain",   6200.0, "2023-07-10"),
]

df = spark.createDataFrame(data, ["customer_id", "name", "email", "country", "total_spend", "join_date"])

# Write as Delta — creates a managed Delta table
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customers")

# Write as Delta to a specific path (unmanaged/external)
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/delta/customers")
```

**Write modes:**

| Mode              | Behavior                           |
| ----------------- | ---------------------------------- |
| `overwrite`       | Replace all existing data          |
| `append`          | Add rows to existing data          |
| `error` (default) | Raise error if data already exists |
| `ignore`          | Do nothing if data already exists  |

---

### 2.2 Creating a Delta Table with SQL DDL

```sql
-- Create a managed Delta table with DDL
CREATE TABLE IF NOT EXISTS orders (
    order_id      STRING      NOT NULL,
    customer_id   STRING      NOT NULL,
    product_id    STRING,
    quantity      INT,
    unit_price    DOUBLE,
    order_date    DATE,
    status        STRING      DEFAULT 'pending'
)
USING DELTA
COMMENT 'Customer orders table'
PARTITIONED BY (order_date);

-- Create an external Delta table at a specific path
CREATE TABLE IF NOT EXISTS orders_external
USING DELTA
LOCATION 's3://my-bucket/delta/orders';

-- Create a Delta table from an existing table (CTAS — Create Table As Select)
CREATE TABLE orders_2024
USING DELTA
AS SELECT * FROM orders WHERE year(order_date) = 2024;

-- Create a table from a Parquet file
CREATE TABLE legacy_data
USING DELTA
AS SELECT * FROM parquet.`/data/legacy/`;
```

---

### 2.3 Reading Delta Tables

```python
# Read a managed Delta table by name
df = spark.read.table("customers")
df.show()

# Read a Delta table from a path
df = spark.read.format("delta").load("/delta/customers")

# Read with SQL
spark.sql("SELECT * FROM customers WHERE country = 'USA'").show()

# Read with Spark SQL display
display(spark.sql("SELECT country, COUNT(*) AS cnt, SUM(total_spend) AS revenue FROM customers GROUP BY country"))
```

---

### 2.4 Inserting Data into Delta Tables

```python
from delta.tables import DeltaTable

# Method 1: Append a DataFrame
new_customers = spark.createDataFrame([
    ("C006", "Frank Lee",    "frank@email.com",  "USA",   9800.0, "2024-01-20"),
    ("C007", "Grace Kim",    "grace@email.com",  "Korea", 13500.0, "2024-02-14"),
], ["customer_id", "name", "email", "country", "total_spend", "join_date"])

new_customers.write.format("delta").mode("append").saveAsTable("customers")

# Method 2: INSERT using SQL
spark.sql("""
    INSERT INTO customers VALUES
    ('C008', 'Henry Park', 'henry@email.com', 'USA', 7200.0, '2024-03-01')
""")

# Method 3: INSERT OVERWRITE using SQL (replaces all data)
spark.sql("""
    INSERT OVERWRITE customers
    SELECT * FROM staging_customers WHERE is_valid = true
""")
```

---

### 2.5 Updating Rows in Delta Tables

```python
# Update using DeltaTable API (Python)
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "customers")

# Update total_spend for a specific customer
dt.update(
    condition = "customer_id = 'C001'",
    set       = {"total_spend": "total_spend + 500.0"}
)

# Update multiple customers matching a condition
dt.update(
    condition = "country = 'USA'",
    set       = {"total_spend": "total_spend * 1.05"}   # 5% increase for USA customers
)

# Update using SQL
spark.sql("""
    UPDATE customers
    SET total_spend = total_spend + 500.0,
        status      = 'premium'
    WHERE total_spend > 20000
""")
```

---

### 2.6 Deleting Rows from Delta Tables

```python
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "customers")

# Delete a specific customer (Python API)
dt.delete("customer_id = 'C008'")

# Delete customers meeting a condition
dt.delete("total_spend < 1000 AND join_date < '2020-01-01'")

# Delete using SQL
spark.sql("""
    DELETE FROM customers
    WHERE country = 'UK' AND total_spend < 5000
""")
```

**Important:** Delta `DELETE` does not physically remove data immediately. It writes a **remove entry** in the transaction log and marks the data files as logically deleted. The files are physically removed only when you run `VACUUM`.

---

### 2.7 Merging Data (Upserts) with MERGE INTO

`MERGE INTO` is the most powerful Delta Lake operation. It combines INSERT, UPDATE, and DELETE in a single atomic transaction — commonly called an **upsert**.

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Target table — existing customers
target = DeltaTable.forName(spark, "customers")

# Source data — new/updated customers arriving from upstream
source = spark.createDataFrame([
    ("C001", "Alice Smith",    "newalice@email.com", "USA",    16500.0, "2022-01-15"),  # updated email + spend
    ("C009", "Ivan Torres",    "ivan@email.com",     "Mexico", 4300.0,  "2024-04-01"),  # new customer
    ("C010", "Julia Chen",     "julia@email.com",    "China",  19000.0, "2024-04-05"),  # new customer
], ["customer_id", "name", "email", "country", "total_spend", "join_date"])

# Perform MERGE
target.alias("tgt").merge(
    source.alias("src"),
    condition = "tgt.customer_id = src.customer_id"
).whenMatchedUpdate(set={
    "email":       "src.email",
    "total_spend": "src.total_spend"
}).whenNotMatchedInsertAll(
).execute()
```

**MERGE with SQL:**

```sql
MERGE INTO customers AS tgt
USING source_customers AS src
ON tgt.customer_id = src.customer_id

WHEN MATCHED AND src.total_spend > tgt.total_spend THEN
    UPDATE SET
        tgt.email       = src.email,
        tgt.total_spend = src.total_spend

WHEN MATCHED AND src.is_deleted = true THEN
    DELETE

WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, country, total_spend, join_date)
    VALUES (src.customer_id, src.name, src.email, src.country, src.total_spend, src.join_date);
```

**MERGE patterns summary:**

| Clause                       | Trigger                              | Action           |
| ---------------------------- | ------------------------------------ | ---------------- |
| `WHEN MATCHED`               | Key exists in both target and source | UPDATE or DELETE |
| `WHEN NOT MATCHED`           | Key in source but not in target      | INSERT           |
| `WHEN NOT MATCHED BY SOURCE` | Key in target but not in source      | UPDATE or DELETE |

---

### 2.8 Schema Evolution

By default, Delta Lake enforces the schema — new columns in the source cause a write to fail. **Schema evolution** allows safe changes.

```python
# Add new columns to a Delta table using mergeSchema
new_data = spark.createDataFrame([
    ("C011", "Kate Johnson", "kate@email.com", "Australia", 5100.0, "2024-05-01", "Gold"),
], ["customer_id", "name", "email", "country", "total_spend", "join_date", "tier"])

# mergeSchema adds new columns automatically
new_data.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("customers")

# For streaming, enable automatic schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Manually add a column using ALTER TABLE
spark.sql("ALTER TABLE customers ADD COLUMN (loyalty_points INT)")

# Change column comment
spark.sql("ALTER TABLE customers ALTER COLUMN total_spend COMMENT 'Lifetime spend in USD'")

# Drop a column (requires column mapping to be enabled)
spark.sql("ALTER TABLE customers DROP COLUMN loyalty_points")
```

---

### 2.9 Table Properties and Metadata

```python
# View table details
spark.sql("DESCRIBE TABLE customers").show(truncate=False)
spark.sql("DESCRIBE TABLE EXTENDED customers").show(truncate=False)
spark.sql("DESCRIBE DETAIL customers").show(truncate=False)

# Set table properties
spark.sql("""
    ALTER TABLE customers
    SET TBLPROPERTIES (
        'delta.autoOptimize.autoCompact'   = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.logRetentionDuration'       = 'interval 30 days'
    )
""")

# View SHOW CREATE TABLE
spark.sql("SHOW CREATE TABLE customers").show(truncate=False)

# List all tables in current database
spark.sql("SHOW TABLES").show()
```

---

## 3. Implementing ACID Transactions with Delta Lake

### 3.1 What Are ACID Transactions?

**ACID** stands for four properties that guarantee reliable database transactions, even in the presence of failures, concurrent writes, and power outages:

```
A — Atomicity:   A transaction either fully succeeds or fully fails.
                 There is no partial write.

C — Consistency: A transaction always brings the data from one valid
                 state to another valid state, enforcing all rules.

I — Isolation:   Concurrent transactions produce the same result as
                 if they had run sequentially. Writers don't see each
                 other's partial results.

D — Durability:  Once a transaction commits, it survives system failure.
                 Committed data is never lost.
```

---

### 3.2 Atomicity in Delta Lake

Delta Lake achieves **atomicity** through the transaction log. A write operation proceeds in two phases:

```
Phase 1 — Write data files:
  ┌──────────────────────────────────────────────┐
  │   Spark writes N Parquet files to storage    │
  │   part-00000-new.parquet                     │
  │   part-00001-new.parquet                     │
  │   (Files exist but are NOT visible yet)      │
  └──────────────────────────────────────────────┘
                        │
                        ▼ (if job fails here, files are orphaned — no impact)
Phase 2 — Commit to _delta_log (atomic):
  ┌──────────────────────────────────────────────┐
  │   Write a single JSON commit file            │
  │   00000000000000000005.json                  │
  │   { "add": [...new files...] }               │
  │                                              │
  │   ← This write is ATOMIC (single file write) │
  │   Success: data is now visible               │
  │   Failure: no impact, data stays at v4       │
  └──────────────────────────────────────────────┘
```

If the job crashes after writing Parquet files but before writing the commit JSON, the Parquet files are simply orphaned and invisible to readers. A subsequent `VACUUM` cleans them up.

---

### 3.3 Consistency in Delta Lake

Delta Lake enforces consistency through:

1. **Schema Enforcement** — Rejects writes that violate the table schema.

```python
# This will fail — 'total_spend' is DoubleType but we're writing a String
bad_data = spark.createDataFrame([
    ("C012", "Leo", "leo@email.com", "USA", "not_a_number", "2024-06-01")
], ["customer_id", "name", "email", "country", "total_spend", "join_date"])

# Raises AnalysisException: schema mismatch
bad_data.write.format("delta").mode("append").saveAsTable("customers")
```

2. **Constraints** — Delta Lake supports `NOT NULL` and `CHECK` constraints.

```sql
-- Add a CHECK constraint
ALTER TABLE customers ADD CONSTRAINT positive_spend CHECK (total_spend >= 0);

-- Add a NOT NULL constraint
ALTER TABLE customers ALTER COLUMN customer_id SET NOT NULL;

-- View constraints
DESCRIBE DETAIL customers;
```

---

### 3.4 Isolation in Delta Lake

Delta Lake uses **Snapshot Isolation** — each reader sees a consistent snapshot of the table at a specific version, independent of ongoing writes.

```
Timeline:
  t=0  Reader A starts scan of version 5
  t=1  Writer B begins writing version 6
  t=2  Writer B commits version 6
  t=3  Reader A completes scan

Reader A still sees version 5 throughout its scan.
Writer B's data is NOT visible to Reader A.
This is Snapshot Isolation.
```

```python
# Readers always read the latest committed version automatically
df = spark.read.table("customers")   # always consistent snapshot

# You can pin to a specific version for reproducible queries
df_v5 = spark.read.format("delta").option("versionAsOf", 5).table("customers")
```

---

### 3.5 Durability in Delta Lake

**Durability** is guaranteed by cloud object storage. Once a commit JSON file is written to S3/ADLS/GCS, it is:

- **Replicated** across multiple availability zones.
- **Durable** to 99.999999999% (11 nines) — S3 SLA.
- **Never lost** unless explicitly deleted.

Delta Lake additionally supports:

- **Checkpoints** every 10 commits to guard against JSON log corruption.
- **CRC files** for data file integrity validation.

---

### 3.6 Optimistic Concurrency Control

Delta Lake uses **Optimistic Concurrency Control (OCC)** to handle concurrent writes without locking:

```
Concurrency Protocol:
  1. Each writer reads the current table version (e.g., v5).
  2. Each writer computes changes and writes new Parquet files.
  3. Each writer attempts to commit by writing v6.json.
  4. Cloud storage guarantees that only ONE writer succeeds for v6.json.
  5. The writer that lost the race:
       a. Checks if its changes conflict with the winning write.
       b. If NO conflict  → re-reads v6, retries as v7.
       c. If YES conflict → raises ConcurrentModificationException.
```

**What is a conflict?**

- Two writers updating the SAME rows → conflict.
- One writer appending, another updating different rows → no conflict (can auto-resolve).

---

### 3.7 Handling Concurrent Writes

```python
# Enable write serialization for strict correctness (at cost of some concurrency)
spark.conf.set("spark.databricks.delta.commitLock.enabled", "true")

# Idempotent writes with txnAppId and txnVersion
# Prevents duplicate writes if a job is retried
df.write \
    .format("delta") \
    .mode("append") \
    .option("txnAppId",   "my_etl_job") \
    .option("txnVersion", "1001") \
    .saveAsTable("customers")

# Example: handling ConcurrentModificationException with retry logic
import time
from delta.exceptions import ConcurrentModificationException

def update_with_retry(delta_table, max_retries=3):
    for attempt in range(max_retries):
        try:
            delta_table.update(
                condition = "status = 'pending'",
                set       = {"status": "'processing'"}
            )
            return
        except ConcurrentModificationException:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)   # exponential back-off
            else:
                raise
```

---

## 4. Autoloader in Delta Lake

### 4.1 What Is Autoloader?

**Autoloader** (`cloudFiles`) is a Databricks-native structured streaming source that **incrementally and efficiently ingests new files** arriving in cloud storage (S3, ADLS, GCS) into a Delta table.

Before Autoloader, ingestion pipelines had to:

1. List all files in the source directory.
2. Track which files had already been processed (manually, using a state table).
3. Reprocess everything on failure.

Autoloader handles all of this automatically:

```
Cloud Storage (S3/ADLS/GCS)
  ├── raw/orders/2024-01-01/file_001.json   ← already processed
  ├── raw/orders/2024-01-02/file_002.json   ← already processed
  └── raw/orders/2024-01-03/file_003.json   ← NEW — Autoloader picks this up
                    │
                    ▼
             Autoloader Stream
             (cloudFiles source)
                    │
                    ▼
           Delta Table: orders_bronze
```

---

### 4.2 How Autoloader Works Internally

Autoloader maintains state in a **checkpoint directory** that stores:

1. **File tracking state** — which files have been seen (using RocksDB or cloud-native file notification).
2. **Schema** — inferred or provided schema of incoming files.
3. **Stream offsets** — exactly-once processing guarantees.

```
Processing Guarantee: Exactly-Once
  - Even if the cluster crashes mid-batch, Autoloader re-reads from the checkpoint
    and does not re-insert already-committed records.
```

---

### 4.3 Directory Listing vs File Notification Mode

Autoloader supports two discovery modes:

**Directory Listing Mode** (default):

```
│  Autoloader periodically lists the source directory
│  and compares against previously seen files.
│
│  Pros:  Works everywhere, no setup required.
│  Cons:  Slow for directories with millions of files.
│         API rate limits on S3 for large directories.
```

**File Notification Mode** (recommended for production):

```
│  Autoloader integrates with AWS SNS + SQS (or Azure Event Grid)
│  to receive real-time notifications when new files arrive.
│
│  Pros:  Near real-time, scales to billions of files.
│  Cons:  Requires cloud infrastructure setup.
│         (Databricks can auto-provision this)
```

```python
# Directory Listing Mode (default)
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/checkpoints/orders/schema") \
    .load("s3://my-bucket/raw/orders/")

# File Notification Mode
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.useNotifications", "true") \
    .option("cloudFiles.schemaLocation", "/checkpoints/orders/schema") \
    .load("s3://my-bucket/raw/orders/")
```

---

### 4.4 Writing Your First Autoloader Stream

```python
# Complete Autoloader ingestion pipeline
def ingest_orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")              # source format: json, csv, parquet, avro
            .option("cloudFiles.schemaLocation",              # where Autoloader persists inferred schema
                    "/checkpoints/orders/schema")
            .option("cloudFiles.inferColumnTypes", "true")    # infer data types (not just string)
            .option("cloudFiles.maxFilesPerTrigger", "1000")  # process at most 1000 files per batch
            .load("s3://my-bucket/raw/orders/")               # source directory (recursive by default)
            .writeStream
            .format("delta")
            .option("checkpointLocation",                     # tracks stream progress
                    "/checkpoints/orders/bronze")
            .option("mergeSchema", "true")                    # allow schema evolution
            .trigger(availableNow=True)                       # process all available files, then stop
            .toTable("orders_bronze")                         # write to managed Delta table
    )

# Start the stream
query = ingest_orders_bronze()
query.awaitTermination()
```

**Trigger modes:**

| Trigger                               | Behavior                               | Use Case                         |
| ------------------------------------- | -------------------------------------- | -------------------------------- |
| `trigger(availableNow=True)`          | Process all available files, then stop | Scheduled batch-style ingestion  |
| `trigger(processingTime="5 minutes")` | Run micro-batches every 5 minutes      | Continuous low-latency streaming |
| `trigger(once=True)`                  | Run exactly one batch (legacy)         | Deprecated, use `availableNow`   |
| (no trigger)                          | Continuous streaming                   | Real-time ingestion              |

---

### 4.5 Schema Inference and Evolution with Autoloader

```python
# Autoloader infers schema on first run and saves it to schemaLocation
# On subsequent runs, it reuses the saved schema

# Enable schema evolution (new columns in source are added to the table)
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/checkpoints/orders/schema") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")    # or "rescue", "failOnNewColumns"
    .load("s3://my-bucket/raw/orders/") \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/orders/bronze") \
    .option("mergeSchema", "true") \
    .toTable("orders_bronze")
```

**Schema evolution modes:**

| Mode               | Behavior                                                           |
| ------------------ | ------------------------------------------------------------------ |
| `addNewColumns`    | New columns in source are automatically added to the target        |
| `rescue`           | Unrecognized columns are saved in a `_rescued_data` column as JSON |
| `failOnNewColumns` | Stream raises an error if new columns are detected                 |
| `none`             | New columns are silently dropped                                   |

---

### 4.6 Autoloader with Checkpointing

```python
# Autoloader with full production configuration
(
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",         "csv")
        .option("cloudFiles.schemaLocation", "/mnt/checkpoints/sales/schema")
        .option("cloudFiles.inferColumnTypes","true")
        .option("cloudFiles.schemaHints",    "order_id STRING, amount DOUBLE")  # override inferred types
        .option("header",                    "true")                             # CSV header row
        .load("/mnt/raw/sales/")
        .select(
            "*",
            "_metadata.file_path".alias("source_file"),      # add source file metadata
            "_metadata.file_modification_time".alias("ingest_time")
        )
        .writeStream
        .format("delta")
        .option("checkpointLocation", "/mnt/checkpoints/sales/bronze")
        .option("mergeSchema",        "true")
        .partitionBy("order_date")
        .trigger(availableNow=True)
        .toTable("sales_bronze")
        .awaitTermination()
)
```

---

### 4.7 Common Autoloader Patterns

**Pattern 1 — Multi-hop (Bronze/Silver):**

```python
# Bronze: raw ingest with Autoloader
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/checkpoints/events/schema") \
    .load("/raw/events/") \
    .writeStream.format("delta") \
    .option("checkpointLocation", "/checkpoints/events/bronze") \
    .toTable("events_bronze")

# Silver: clean data from Bronze
spark.readStream.table("events_bronze") \
    .filter("event_type IS NOT NULL") \
    .dropDuplicates(["event_id"]) \
    .writeStream.format("delta") \
    .option("checkpointLocation", "/checkpoints/events/silver") \
    .toTable("events_silver")
```

**Pattern 2 — Adding ingestion metadata:**

```python
from pyspark.sql.functions import current_timestamp, input_file_name

spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/checkpoints/logs/schema") \
    .load("/raw/logs/") \
    .withColumn("_ingested_at",   current_timestamp()) \
    .withColumn("_source_file",   col("_metadata.file_path")) \
    .writeStream.format("delta") \
    .option("checkpointLocation", "/checkpoints/logs/bronze") \
    .toTable("logs_bronze")
```

---

## 5. Time Travel and Data Versioning in Delta Lake

### 5.1 How Versioning Works

Every write to a Delta table creates a new **version** (an immutable commit in the transaction log). The version history records exactly what changed, when, and by whom.

```
Version Timeline:
  v0  ──  CREATE TABLE customers (empty)
  v1  ──  INSERT 5 rows
  v2  ──  UPDATE Alice's email
  v3  ──  DELETE Bob (GDPR request)
  v4  ──  INSERT 2 new customers (C006, C007)
  v5  ──  MERGE: update 3, insert 1
  v6  ──  OPTIMIZE (file compaction)
  │
  └──  current version = v6
       but ALL previous versions are accessible
```

---

### 5.2 Querying Previous Versions by Version Number

```python
# Python API — read a specific version
df_v1 = spark.read \
    .format("delta") \
    .option("versionAsOf", 1) \
    .table("customers")
df_v1.show()

# SQL — read a specific version
spark.sql("SELECT * FROM customers VERSION AS OF 1").show()
spark.sql("SELECT * FROM customers@v1").show()   # shorthand syntax

# Compare two versions
v1 = spark.read.format("delta").option("versionAsOf", 1).table("customers")
v5 = spark.read.format("delta").option("versionAsOf", 5).table("customers")

# Find rows that changed between versions
added = v5.subtract(v1)
removed = v1.subtract(v5)

print(f"Rows added between v1 and v5:   {added.count()}")
print(f"Rows removed between v1 and v5: {removed.count()}")
```

---

### 5.3 Querying Previous Versions by Timestamp

```python
# Read the table as it was at a specific timestamp
df_yesterday = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-03-25 00:00:00") \
    .table("customers")

# SQL — timestamp-based time travel
spark.sql("""
    SELECT * FROM customers TIMESTAMP AS OF '2024-03-25T00:00:00.000Z'
""").show()

# Use a Python datetime object
from datetime import datetime, timedelta

yesterday = datetime.now() - timedelta(days=1)
df = spark.read \
    .format("delta") \
    .option("timestampAsOf", yesterday.strftime("%Y-%m-%d %H:%M:%S")) \
    .table("customers")
```

---

### 5.4 Restoring a Table to a Previous Version

```python
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "customers")

# Restore to a specific version
dt.restoreToVersion(3)

# Restore to a specific timestamp
dt.restoreToTimestamp("2024-03-25 00:00:00")

# SQL equivalent
spark.sql("RESTORE TABLE customers TO VERSION AS OF 3")
spark.sql("RESTORE TABLE customers TO TIMESTAMP AS OF '2024-03-25 00:00:00'")
```

**RESTORE creates a new version** (it does not rewrite history). The table history shows the RESTORE operation itself as a new commit, preserving full auditability.

---

### 5.5 Viewing Table History

```python
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "customers")

# Full history
dt.history().show(truncate=False)

# Last N operations
dt.history(10).show(truncate=False)

# SQL equivalent
spark.sql("DESCRIBE HISTORY customers").show(truncate=False)

# History columns:
# version | timestamp | userId | userName | operation | operationParameters | ...
```

**Example history output:**

| version | timestamp           | operation    | operationParameters                   |
| ------- | ------------------- | ------------ | ------------------------------------- |
| 5       | 2024-03-26 10:00:00 | MERGE        | {"predicate": "customer_id"}          |
| 4       | 2024-03-26 09:00:00 | WRITE        | {"mode": "Append"}                    |
| 3       | 2024-03-25 15:00:00 | DELETE       | {"predicate": "country = 'UK'"}       |
| 2       | 2024-03-25 12:00:00 | UPDATE       | {"predicate": "customer_id = 'C001'"} |
| 1       | 2024-03-25 10:00:00 | WRITE        | {"mode": "Append"}                    |
| 0       | 2024-03-25 09:00:00 | CREATE TABLE | {}                                    |

---

### 5.6 Vacuuming Old Data Files

`VACUUM` physically removes data files that are no longer referenced by the current or recent versions of the table.

```python
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "customers")

# Dry run — shows what will be deleted without actually deleting
dt.vacuum(retentionHours=168)   # 168 hours = 7 days (default retention)

# Actually vacuum
dt.vacuum(retentionHours=168)

# SQL equivalent
spark.sql("VACUUM customers RETAIN 168 HOURS")

# Force vacuum with less than 7-day retention (WARNING: breaks time travel)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM customers RETAIN 0 HOURS")   # removes ALL old files
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
```

**Important:** After vacuuming, you cannot time travel to versions older than the retention period. Always ensure retention covers your SLA for data recovery.

---

### 5.7 Practical Use Cases for Time Travel

**Use Case 1 — Audit / Compliance:**

```python
# What were the orders on the last day of last quarter?
spark.sql("""
    SELECT * FROM orders TIMESTAMP AS OF '2023-12-31 23:59:59'
    WHERE status = 'completed'
""").show()
```

**Use Case 2 — Rollback after a bad pipeline run:**

```python
# A pipeline accidentally deleted customer data — restore immediately
spark.sql("DESCRIBE HISTORY customers LIMIT 5").show()
# Identify the last good version, e.g., version 10
spark.sql("RESTORE TABLE customers TO VERSION AS OF 10")
```

**Use Case 3 — Reproduce historical ML training data:**

```python
# Reproduce exactly the feature set used to train model version 7
training_data = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-01-15 00:00:00") \
    .table("feature_store")
```

**Use Case 4 — Incremental data processing:**

```python
# Process only the changes since the last run (Incremental ETL)
last_version = get_last_processed_version()  # from your state store

changes = spark.read \
    .format("delta") \
    .option("startingVersion", last_version + 1) \
    .table("orders")

# Process changes
changes.write.format("delta").mode("append").saveAsTable("orders_processed")
save_last_processed_version(changes.select("_commit_version").max())
```

---

## 6. Delta Lake Optimization

### 6.1 OPTIMIZE and File Compaction

Over time, streaming jobs and frequent small writes create many small Parquet files. `OPTIMIZE` compacts them into fewer, larger files (targeting ~1 GB each) for faster queries.

```python
# Optimize the entire table
spark.sql("OPTIMIZE customers")

# Optimize with Z-ORDER (more powerful — see section 6.2)
spark.sql("OPTIMIZE customers ZORDER BY (country)")

# Optimize a specific partition
spark.sql("OPTIMIZE customers WHERE order_date = '2024-03-25'")

# Enable Auto Optimize (recommended for tables with frequent small writes)
spark.sql("""
    ALTER TABLE customers
    SET TBLPROPERTIES (
        'delta.autoOptimize.autoCompact'   = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")
```

---

### 6.2 Z-Ordering for Data Skipping

**Z-Ordering** is a multi-dimensional clustering technique that co-locates related data in the same files. When combined with Delta's **data skipping**, this dramatically reduces the number of files read for filtered queries.

```python
# Z-Order by frequently filtered columns
spark.sql("OPTIMIZE orders ZORDER BY (customer_id, order_date)")

# Now queries filtering on customer_id or order_date skip most files
spark.sql("""
    SELECT * FROM orders
    WHERE customer_id = 'C001'
    AND order_date BETWEEN '2024-01-01' AND '2024-03-31'
""").show()
# Without Z-ORDER: reads ALL files
# With Z-ORDER: reads only the few files containing C001's orders
```

**Z-ORDER best practices:**

- Use columns that appear most frequently in `WHERE` clauses.
- Limit to 3-4 columns (diminishing returns beyond that).
- Do NOT Z-Order partition columns (they already achieve data skipping through partitioning).

---

### 6.3 Data Skipping and Statistics

Delta Lake automatically collects per-file statistics during writes:

```
For each data file, Delta stores:
  - numRecords: total row count
  - minValues:  minimum value for each column (first 32 columns)
  - maxValues:  maximum value for each column
  - nullCount:  number of NULLs per column

Query: WHERE customer_id = 'C500'
  File A: min=C001, max=C200 → SKIP (C500 is outside range)
  File B: min=C201, max=C400 → SKIP
  File C: min=C401, max=C600 → READ (C500 is within range)
  File D: min=C601, max=C800 → SKIP

Result: Read only 1 out of 4 files — 75% I/O reduction
```

---

### 6.4 Partitioning Delta Tables

Partitioning divides a table's data into subdirectories by column value. Good for:

- Tables larger than 1 TB.
- Columns with low cardinality (e.g., `country`, `year`, `status`).
- Columns almost always in the `WHERE` clause.

```python
# Create a partitioned Delta table
df.write \
    .format("delta") \
    .partitionBy("country", "order_year") \
    .mode("overwrite") \
    .saveAsTable("orders_partitioned")

# Query a partition (Spark reads only the matching subdirectory)
spark.sql("SELECT * FROM orders_partitioned WHERE country = 'USA' AND order_year = 2024").show()

# Show table partitions
spark.sql("SHOW PARTITIONS orders_partitioned").show()
```

**Partitioning anti-patterns to avoid:**

- High-cardinality columns (e.g., `customer_id`, `order_id`) → millions of tiny directories.
- Small tables (< 1 GB) → partitioning overhead exceeds benefit.
- Columns not used in filters → wasted storage layout.

For most tables under 1 TB, **Z-Ordering** is preferred over partitioning.

---

## 7. Summary

| Topic               | Key Points                                                                                 |
| ------------------- | ------------------------------------------------------------------------------------------ |
| **Delta Lake**      | Open-source storage layer adding ACID, versioning, and reliability to data lakes           |
| **Transaction Log** | `_delta_log/` JSON files record every operation; checkpoints every 10 commits              |
| **CRUD Operations** | Full `INSERT`, `UPDATE`, `DELETE`, `MERGE` support on Delta tables                         |
| **ACID**            | Atomicity via two-phase commit; Isolation via snapshot reads; Durability via cloud storage |
| **Autoloader**      | Incremental file ingestion from cloud storage with exactly-once guarantees                 |
| **Time Travel**     | Query any previous table version by version number or timestamp                            |
| **RESTORE**         | Roll back a table to a previous version (creates a new commit)                             |
| **VACUUM**          | Physically removes old data files beyond the retention window                              |
| **OPTIMIZE**        | Compacts small files; Z-ORDER co-locates data for faster skipping                          |

**What comes next:** With a solid foundation in Delta Lake, the next guide covers **Building Data Engineering Pipelines** — assembling the Delta Lake operations you have learned into end-to-end ETL workflows using Databricks notebooks, Jobs, and scheduling.

---
