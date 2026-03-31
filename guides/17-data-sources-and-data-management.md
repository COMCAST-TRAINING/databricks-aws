# Data Sources and Data Management in Databricks

---

## Table of Contents

1. [Introduction to Data Sources in Databricks](#1-introduction-to-data-sources-in-databricks)
   - 1.1 [The Data Source Ecosystem](#11-the-data-source-ecosystem)
   - 1.2 [DataFrameReader and DataFrameWriter Overview](#12-dataframereader-and-dataframewriter-overview)
   - 1.3 [When to Use Which Format](#13-when-to-use-which-format)
2. [DataFrameReader: Reading Data](#2-dataframereader-reading-data)
   - 2.1 [Reading CSV Files](#21-reading-csv-files)
   - 2.2 [Reading JSON Files](#22-reading-json-files)
   - 2.3 [Reading Parquet Files](#23-reading-parquet-files)
   - 2.4 [Reading ORC and Avro Files](#24-reading-orc-and-avro-files)
   - 2.5 [Reading Delta Tables](#25-reading-delta-tables)
   - 2.6 [Reading from JDBC Sources](#26-reading-from-jdbc-sources)
   - 2.7 [Schema Inference vs. Explicit Schema](#27-schema-inference-vs-explicit-schema)
3. [DataFrameWriter: Writing Data to Different Formats](#3-dataframereader-writing-data-to-different-formats)
   - 3.1 [Write Modes](#31-write-modes)
   - 3.2 [Writing CSV and JSON](#32-writing-csv-and-json)
   - 3.3 [Writing Parquet and ORC](#33-writing-parquet-and-orc)
   - 3.4 [Writing Delta Tables](#34-writing-delta-tables)
   - 3.5 [Writing via JDBC](#35-writing-via-jdbc)
   - 3.6 [Partitioning on Write with `partitionBy`](#36-partitioning-on-write-with-partitionby)
   - 3.7 [Bucketing with `bucketBy` and `sortBy`](#37-bucketing-with-bucketby-and-sortby)
4. [Creating DataFrames Manually](#4-creating-dataframes-manually)
   - 4.1 [From a Python List with Schema](#41-from-a-python-list-with-schema)
   - 4.2 [Using `spark.range()`](#42-using-sparkrange)
   - 4.3 [Using `Row` Objects](#43-using-row-objects)
   - 4.4 [From a Pandas DataFrame](#44-from-a-pandas-dataframe)
5. [Schema Management](#5-schema-management)
   - 5.1 [StructType and StructField](#51-structtype-and-structfield)
   - 5.2 [Schema on Read vs. Schema on Write](#52-schema-on-read-vs-schema-on-write)
   - 5.3 [Schema Evolution](#53-schema-evolution)
   - 5.4 [Enforcing Schema with DDL Strings](#54-enforcing-schema-with-ddl-strings)
6. [Working with Nested and Complex Data](#6-working-with-nested-and-complex-data)
   - 6.1 [Nested JSON Structures](#61-nested-json-structures)
   - 6.2 [ArrayType Columns](#62-arraytype-columns)
   - 6.3 [MapType Columns](#63-maptype-columns)
   - 6.4 [`explode`, `flatten`, `from_json`, and `to_json`](#64-explode-flatten-from_json-and-to_json)
7. [Best Practices for Managing Data Sources](#7-best-practices-for-managing-data-sources)
   - 7.1 [File Discovery Patterns](#71-file-discovery-patterns)
   - 7.2 [Handling Corrupt Records](#72-handling-corrupt-records)
   - 7.3 [Optimizing Reads: Predicate Pushdown and Column Pruning](#73-optimizing-reads-predicate-pushdown-and-column-pruning)
   - 7.4 [Separating Raw, Processed, and Curated Zones](#74-separating-raw-processed-and-curated-zones)
   - 7.5 [Compression Codec Selection](#75-compression-codec-selection)
8. [Summary](#8-summary)

---

## 1. Introduction to Data Sources in Databricks

### 1.1 The Data Source Ecosystem

Databricks can read from and write to a rich variety of data sources — from flat files on cloud object storage to relational databases, message queues, and Delta Lake tables managed by Unity Catalog.

```
DATA SOURCE ECOSYSTEM IN DATABRICKS
────────────────────────────────────────────────────────────────────────
                         ┌─────────────────┐
                         │   Apache Spark   │
                         │  DataFrameReader │
                         └────────┬────────┘
                                  │
         ┌─────────────┬──────────┼──────────┬─────────────┐
         ▼             ▼          ▼           ▼             ▼
  ┌────────────┐ ┌──────────┐ ┌──────┐ ┌──────────┐ ┌──────────┐
  │ FILE-BASED │ │  DELTA   │ │ JDBC │ │STREAMING │ │ CUSTOM   │
  │            │ │          │ │      │ │ SOURCES  │ │ (3rd     │
  │ CSV        │ │ Delta    │ │ MySQL│ │          │ │  party)  │
  │ JSON       │ │ Tables   │ │ PG   │ │ Kafka    │ │          │
  │ Parquet    │ │ (Unity   │ │ SQL  │ │ Kinesis  │ │ Redshift │
  │ ORC        │ │ Catalog) │ │ Srv  │ │ Auto-    │ │ BigQuery │
  │ Avro       │ │          │ │ RDS  │ │ loader   │ │ MongoDB  │
  │ Text       │ │          │ │ Snow-│ │          │ │          │
  │ Binary     │ │          │ │ flake│ │          │ │          │
  └────────────┘ └──────────┘ └──────┘ └──────────┘ └──────────┘
         │             │          │           │             │
         └─────────────┴──────────┴───────────┴─────────────┘
                                  │
                         ┌────────▼────────┐
                         │   Apache Spark   │
                         │  DataFrameWriter │
                         └─────────────────┘
```

---

### 1.2 DataFrameReader and DataFrameWriter Overview

The **DataFrameReader** (`spark.read`) and **DataFrameWriter** (`df.write`) share a consistent fluent API pattern.

```python
# ── DataFrameReader fluent API pattern ────────────────────────────
df = (
    spark
    .read
    .format("csv")             # specify format
    .option("header", "true")  # format-specific options
    .option("inferSchema", "true")
    .schema(my_schema)         # optional: explicit schema
    .load("s3://bucket/path/") # path or table name
)

# ── DataFrameWriter fluent API pattern ────────────────────────────
(
    df
    .write
    .format("delta")           # specify format
    .mode("overwrite")         # write mode
    .option("overwriteSchema", "true")
    .partitionBy("year", "month")
    .save("s3://bucket/delta/output/")
)
```

---

### 1.3 When to Use Which Format

| Format         | Best For                                           | Pros                                                     | Cons                                                      |
| -------------- | -------------------------------------------------- | -------------------------------------------------------- | --------------------------------------------------------- |
| **CSV**        | Data exchange, raw ingestion from external systems | Human-readable, universal support                        | No schema, slow (text parsing), no compression by default |
| **JSON**       | Semi-structured / nested data from APIs            | Handles nested structures natively                       | Verbose, larger file sizes, slower than columnar          |
| **Parquet**    | Analytical workloads, intermediate data            | Columnar, high compression, predicate pushdown           | Not human-readable, no ACID transactions                  |
| **ORC**        | Hive-originated workloads                          | Highly optimized for Hive, good compression              | Less common outside Hadoop ecosystem                      |
| **Avro**       | Kafka schema registry, row-level streaming         | Schema embedded, row-oriented, schema evolution          | Not columnar (worse for analytics)                        |
| **Delta Lake** | All production Databricks workloads                | ACID, time travel, schema enforcement, streaming support | Databricks/Delta dependency                               |

> **Recommendation:** Use **Delta Lake** as the primary format for all managed tables in Databricks. Use Parquet for raw zone landing if Delta is not yet available. Avoid CSV/JSON for large-scale production transformations.

---

## 2. DataFrameReader: Reading Data

### 2.1 Reading CSV Files

```python
# ── Minimal CSV read ───────────────────────────────────────────────
df = spark.read.csv("s3://my-bucket/data/customers.csv", header=True, inferSchema=True)

# ── Full-option CSV read ───────────────────────────────────────────
df_customers = (
    spark
    .read
    .format("csv")
    .option("header",          "true")     # first row is column headers
    .option("inferSchema",     "true")     # auto-detect data types
    .option("sep",             ",")        # delimiter (default is comma)
    .option("quote",           '"')        # quote character
    .option("escape",          "\\")       # escape character
    .option("nullValue",       "NULL")     # string to treat as null
    .option("emptyValue",      "")         # empty string behaviour
    .option("multiLine",       "false")    # true if values span multiple lines
    .option("encoding",        "UTF-8")    # file encoding
    .option("dateFormat",      "yyyy-MM-dd")
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    .option("ignoreLeadingWhiteSpace",  "true")
    .option("ignoreTrailingWhiteSpace", "true")
    .load("s3://my-bucket/data/customers/")
)

# ── Read multiple CSV files with a glob pattern ───────────────────
df_multi = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("s3://my-bucket/data/transactions/year=2025/month=*/")
)

# ── Read CSV with an explicit schema (no inferSchema overhead) ─────
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

customer_schema = StructType([
    StructField("customer_id",   StringType(),  nullable=False),
    StructField("first_name",    StringType(),  nullable=True),
    StructField("last_name",     StringType(),  nullable=True),
    StructField("email",         StringType(),  nullable=True),
    StructField("signup_date",   DateType(),    nullable=True),
    StructField("country",       StringType(),  nullable=True),
    StructField("lifetime_value", DoubleType(), nullable=True),
])

df_typed = (
    spark.read
    .format("csv")
    .option("header",     "true")
    .option("dateFormat", "yyyy-MM-dd")
    .schema(customer_schema)
    .load("s3://my-bucket/data/customers/")
)
```

**Useful CSV Read Options Reference:**

| Option                      | Default           | Description                                   |
| --------------------------- | ----------------- | --------------------------------------------- |
| `header`                    | `false`           | Treat first row as column names               |
| `inferSchema`               | `false`           | Auto-detect column types (requires full scan) |
| `sep`                       | `,`               | Column delimiter                              |
| `nullValue`                 | `""`              | String to interpret as null                   |
| `multiLine`                 | `false`           | Allow values spanning multiple lines          |
| `mode`                      | `PERMISSIVE`      | How to handle malformed rows                  |
| `columnNameOfCorruptRecord` | `_corrupt_record` | Column to store malformed rows                |
| `maxColumns`                | `20480`           | Maximum columns allowed                       |

---

### 2.2 Reading JSON Files

```python
# ── Simple JSON (one object per line — JSON Lines format) ──────────
df_orders = (
    spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("s3://my-bucket/data/orders/")
)

# ── Multi-line JSON (single JSON object or array spans multiple lines)
df_config = (
    spark.read
    .format("json")
    .option("multiLine", "true")         # required for pretty-printed JSON
    .option("inferSchema", "true")
    .load("s3://my-bucket/config/app_settings.json")
)

# ── Nested JSON with explicit schema ──────────────────────────────
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType, TimestampType, ArrayType)

order_schema = StructType([
    StructField("order_id",    StringType(),    False),
    StructField("customer_id", StringType(),    True),
    StructField("order_time",  TimestampType(), True),
    StructField("status",      StringType(),    True),
    StructField("total",       DoubleType(),    True),
    StructField("shipping_address", StructType([
        StructField("street",  StringType(), True),
        StructField("city",    StringType(), True),
        StructField("state",   StringType(), True),
        StructField("zip",     StringType(), True),
        StructField("country", StringType(), True),
    ]), True),
    StructField("items", ArrayType(StructType([
        StructField("product_id", StringType(), True),
        StructField("quantity",   IntegerType(), True),
        StructField("unit_price", DoubleType(),  True),
    ])), True),
])

df_nested = (
    spark.read
    .format("json")
    .schema(order_schema)
    .load("s3://my-bucket/data/orders/")
)

# ── Access nested fields ───────────────────────────────────────────
from pyspark.sql import functions as F

df_flat = df_nested.select(
    "order_id",
    "customer_id",
    "order_time",
    F.col("shipping_address.city").alias("ship_city"),
    F.col("shipping_address.country").alias("ship_country"),
    F.col("total"),
    F.size("items").alias("item_count"),
)
```

---

### 2.3 Reading Parquet Files

Parquet is the most performant file format for analytical queries. It is columnar, compressed, and supports predicate pushdown directly in the file reader layer.

```python
# ── Read Parquet ───────────────────────────────────────────────────
df_parquet = spark.read.parquet("s3://my-bucket/data/transactions/")

# ── Read partitioned Parquet directory ────────────────────────────
# Directory structure: /data/transactions/year=2025/month=01/day=15/
df_partitioned = (
    spark.read
    .format("parquet")
    .load("s3://my-bucket/data/transactions/")
)

# ── Partition pruning — Spark only reads relevant partitions ───────
df_jan = df_partitioned.filter(
    (F.col("year") == 2025) & (F.col("month") == 1)
)
# Spark pushes the filter into the reader — only Jan 2025 files are scanned

# ── Merge schemas from multiple Parquet files with different schemas
df_merged = (
    spark.read
    .format("parquet")
    .option("mergeSchema", "true")
    .load("s3://my-bucket/data/events/")
)
```

> **Performance tip:** When reading Parquet with a filter on a partition column, Spark skips all non-matching partitions entirely. Always partition Parquet data by high-cardinality date/region columns used in WHERE clauses.

---

### 2.4 Reading ORC and Avro Files

```python
# ── Read ORC ───────────────────────────────────────────────────────
df_orc = (
    spark.read
    .format("orc")
    .load("s3://my-bucket/data/hive_exports/")
)

# ── Read Avro (requires spark-avro JAR in Databricks Runtime 7.x+) ─
df_avro = (
    spark.read
    .format("avro")
    .load("s3://my-bucket/data/kafka_dumps/")
)

# ── Read Avro with an external schema (Avro schema registry pattern)
avro_schema_str = """
{
  "type": "record",
  "name": "Transaction",
  "fields": [
    {"name": "txn_id",    "type": "string"},
    {"name": "amount",    "type": "double"},
    {"name": "currency",  "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}
"""

df_avro_typed = (
    spark.read
    .format("avro")
    .option("avroSchema", avro_schema_str)
    .load("s3://my-bucket/data/kafka_avro/")
)
```

---

### 2.5 Reading Delta Tables

```python
# ── Read a Delta table by path ─────────────────────────────────────
df_delta_path = spark.read.format("delta").load("s3://my-bucket/delta/orders/")

# ── Read a Delta table by name (Unity Catalog 3-part name) ────────
df_delta_table = spark.read.table("catalog.schema.orders")

# Also works as:
df_delta_sql = spark.sql("SELECT * FROM catalog.schema.orders")

# ── Read a specific version (time travel) ─────────────────────────
df_v5 = (
    spark.read
    .format("delta")
    .option("versionAsOf", "5")
    .load("s3://my-bucket/delta/orders/")
)

# ── Read as of a timestamp ─────────────────────────────────────────
df_yesterday = (
    spark.read
    .format("delta")
    .option("timestampAsOf", "2026-03-29 00:00:00")
    .load("s3://my-bucket/delta/orders/")
)

# ── Read a Delta table with CDF (Change Data Feed) ────────────────
df_changes = (
    spark.read
    .format("delta")
    .option("readChangeFeed",    "true")
    .option("startingVersion",   "10")
    .option("endingVersion",     "20")
    .table("catalog.schema.orders")
)

# CDF adds _change_type, _commit_version, _commit_timestamp columns
df_changes.filter(F.col("_change_type") == "update_postimage").show()
```

---

### 2.6 Reading from JDBC Sources

```python
# ── JDBC credentials from Databricks Secrets ──────────────────────
jdbc_user     = dbutils.secrets.get(scope="rds",  key="username")
jdbc_password = dbutils.secrets.get(scope="rds",  key="password")
jdbc_url      = (
    "jdbc:postgresql://mydb.cluster.us-east-1.rds.amazonaws.com:5432/ecommerce"
)

# ── Basic JDBC read ────────────────────────────────────────────────
df_jdbc = (
    spark.read
    .format("jdbc")
    .option("url",      jdbc_url)
    .option("dbtable",  "public.orders")
    .option("user",     jdbc_user)
    .option("password", jdbc_password)
    .option("driver",   "org.postgresql.Driver")
    .load()
)

# ── JDBC read with a custom SQL query ─────────────────────────────
df_recent = (
    spark.read
    .format("jdbc")
    .option("url",      jdbc_url)
    .option("query",    "SELECT * FROM public.orders WHERE status = 'pending'")
    .option("user",     jdbc_user)
    .option("password", jdbc_password)
    .option("driver",   "org.postgresql.Driver")
    .load()
)

# ── JDBC read with partitioning (parallel reads) ───────────────────
df_parallel = (
    spark.read
    .format("jdbc")
    .option("url",               jdbc_url)
    .option("dbtable",           "public.transactions")
    .option("user",              jdbc_user)
    .option("password",          jdbc_password)
    .option("driver",            "org.postgresql.Driver")
    .option("partitionColumn",   "txn_id_hash")    # numeric or date column
    .option("lowerBound",        "0")
    .option("upperBound",        "1000000")
    .option("numPartitions",     "20")             # parallel JDBC connections
    .option("fetchsize",         "10000")          # rows fetched per trip
    .load()
)
```

> **Security:** Always use Databricks Secrets for JDBC credentials. Never hardcode usernames, passwords, or connection strings directly in notebook code.

---

### 2.7 Schema Inference vs. Explicit Schema

| Approach              | How                                          | Pros                                                              | Cons                                                                            |
| --------------------- | -------------------------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| **Schema inference**  | `inferSchema=true` or default JSON           | No code needed, fast to prototype                                 | Full file scan (slow), may infer wrong types, non-deterministic for mixed types |
| **Explicit schema**   | `.schema(StructType(...))`                   | Fast (skips scan), deterministic, catches type mismatches at load | More code to write                                                              |
| **DDL string schema** | `.schema("id STRING, name STRING, age INT")` | Concise, readable                                                 | Still string-based, less IDE support                                            |

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ── Explicit StructType schema ─────────────────────────────────────
product_schema = StructType([
    StructField("product_id",   StringType(),  nullable=False),
    StructField("product_name", StringType(),  nullable=True),
    StructField("category",     StringType(),  nullable=True),
    StructField("unit_price",   DoubleType(),  nullable=True),
    StructField("stock_qty",    IntegerType(), nullable=True),
    StructField("supplier_id",  StringType(),  nullable=True),
])

df = spark.read.schema(product_schema).json("s3://my-bucket/products/")

# ── DDL string schema (alternative concise syntax) ─────────────────
ddl_schema = "product_id STRING NOT NULL, product_name STRING, unit_price DOUBLE, stock_qty INT"
df_ddl = spark.read.schema(ddl_schema).json("s3://my-bucket/products/")

# ── Print inferred schema for documentation purposes ───────────────
df_inferred = spark.read.option("inferSchema", "true").json("s3://my-bucket/products/")
df_inferred.printSchema()
# Use the printed schema to create an explicit StructType definition
```

---

## 3. DataFrameReader: Writing Data to Different Formats

### 3.1 Write Modes

| Mode              | Behaviour                                      | Use Case                             |
| ----------------- | ---------------------------------------------- | ------------------------------------ |
| `overwrite`       | Replaces existing data (and schema by default) | Full refresh, dimension table reload |
| `append`          | Adds new rows without modifying existing data  | Incremental load, event log          |
| `ignore`          | Silently skips write if data already exists    | Idempotent initial loads             |
| `error` (default) | Raises an exception if data already exists     | Prevent accidental overwrites in dev |

```python
# ── Overwrite ─────────────────────────────────────────────────────
df.write.mode("overwrite").parquet("s3://my-bucket/output/products/")

# ── Append ────────────────────────────────────────────────────────
df.write.mode("append").format("delta").save("s3://my-bucket/delta/events/")

# ── Ignore ────────────────────────────────────────────────────────
df.write.mode("ignore").format("delta").saveAsTable("catalog.staging.new_data")

# ── Error (default — explicit for clarity) ────────────────────────
df.write.mode("error").parquet("s3://my-bucket/output/new_table/")
```

---

### 3.2 Writing CSV and JSON

```python
# ── Write CSV ─────────────────────────────────────────────────────
(
    df
    .write
    .format("csv")
    .mode("overwrite")
    .option("header",          "true")
    .option("sep",             ",")
    .option("nullValue",       "NULL")
    .option("dateFormat",      "yyyy-MM-dd")
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
    .option("compression",     "gzip")       # gz, bz2, lz4, snappy, none
    .save("s3://my-bucket/output/customers_export/")
)

# ── Write JSON ────────────────────────────────────────────────────
(
    df
    .write
    .format("json")
    .mode("overwrite")
    .option("compression",     "gzip")
    .option("dateFormat",      "yyyy-MM-dd")
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
    .save("s3://my-bucket/output/orders_json/")
)

# ── Coalesce to a single file for small exports ───────────────────
(
    df
    .coalesce(1)              # merge to 1 partition before writing
    .write
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save("s3://my-bucket/output/small_report/")
)
```

> **Tip:** Avoid writing large DataFrames as a single file. Use `coalesce(1)` only for small export files (< 1 GB). For large data, let Spark write multiple part files in parallel.

---

### 3.3 Writing Parquet and ORC

```python
# ── Write Parquet with snappy compression (default) ───────────────
(
    df
    .write
    .format("parquet")
    .mode("overwrite")
    .option("compression", "snappy")   # snappy (default), gzip, lz4, zstd, none
    .save("s3://my-bucket/output/transactions_parquet/")
)

# ── Write Parquet optimized for query performance ──────────────────
(
    df
    .repartition(200)       # control number of output part files
    .write
    .format("parquet")
    .mode("overwrite")
    .option("compression", "zstd")     # better ratio than snappy
    .partitionBy("year", "month")
    .save("s3://my-bucket/output/transactions_partitioned/")
)

# ── Write ORC ─────────────────────────────────────────────────────
(
    df
    .write
    .format("orc")
    .mode("overwrite")
    .option("compression", "zlib")
    .save("s3://my-bucket/output/hive_compatible/")
)
```

---

### 3.4 Writing Delta Tables

```python
# ── Write to Delta table by path ───────────────────────────────────
(
    df
    .write
    .format("delta")
    .mode("overwrite")
    .save("s3://my-bucket/delta/products/")
)

# ── Write to a Unity Catalog managed table by name ─────────────────
(
    df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")   # allow schema changes on overwrite
    .saveAsTable("catalog.silver.products")
)

# ── Append to a Delta table (schema must match) ────────────────────
(
    df_new
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("catalog.silver.products")
)

# ── MERGE (upsert) into a Delta table ─────────────────────────────
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "catalog.silver.products")

(
    target.alias("tgt")
    .merge(
        df.alias("src"),
        "tgt.product_id = src.product_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# ── Append with schema evolution enabled ──────────────────────────
(
    df_with_new_columns
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")    # add new columns to target schema
    .saveAsTable("catalog.silver.products")
)
```

---

### 3.5 Writing via JDBC

```python
# ── Write DataFrame to PostgreSQL via JDBC ────────────────────────
(
    df
    .write
    .format("jdbc")
    .option("url",      jdbc_url)
    .option("dbtable",  "public.product_summary")
    .option("user",     jdbc_user)
    .option("password", jdbc_password)
    .option("driver",   "org.postgresql.Driver")
    .option("batchsize", "10000")     # rows per INSERT batch
    .mode("overwrite")
    .save()
)

# ── Append to an existing table ───────────────────────────────────
(
    df_new
    .write
    .format("jdbc")
    .option("url",      jdbc_url)
    .option("dbtable",  "public.daily_events")
    .option("user",     jdbc_user)
    .option("password", jdbc_password)
    .option("driver",   "org.postgresql.Driver")
    .option("batchsize", "5000")
    .mode("append")
    .save()
)
```

> **Performance:** For large writes, increase `batchsize` to `10000–50000`. Consider creating indices on the target table after the write, not before, to avoid costly index updates per row.

---

### 3.6 Partitioning on Write with `partitionBy`

`partitionBy` creates a Hive-style directory partition structure. It splits the output data into subdirectories per partition value, enabling partition pruning on reads.

```python
# ── Write partitioned by date columns ────────────────────────────
from pyspark.sql import functions as F

df_with_date = df.withColumn("year",  F.year("order_date")) \
                 .withColumn("month", F.month("order_date"))

(
    df_with_date
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("year", "month")
    .saveAsTable("catalog.silver.orders")
)

# Output structure:
# s3://my-bucket/delta/orders/
# ├── year=2025/
# │   ├── month=1/  ← part-00000.snappy.parquet
# │   ├── month=2/  ← part-00000.snappy.parquet
# │   └── ...
# └── year=2026/
#     └── month=3/  ← part-00000.snappy.parquet

# ── Reads become fast with partition filter ────────────────────────
# Spark reads ONLY year=2026/month=3/ directory:
df_march = spark.table("catalog.silver.orders").filter(
    (F.col("year") == 2026) & (F.col("month") == 3)
)
```

**Partitioning guidelines:**

| Guideline                                     | Reason                                                             |
| --------------------------------------------- | ------------------------------------------------------------------ |
| Choose low-cardinality columns                | High cardinality creates too many small files (small file problem) |
| Prefer date-based partitioning (year/month)   | Queries almost always filter by date ranges                        |
| Avoid partitioning very small tables (< 1 GB) | Overhead of partition discovery exceeds benefit                    |
| Use at most 2–3 partition levels              | Deeper nesting complicates file management                         |

---

### 3.7 Bucketing with `bucketBy` and `sortBy`

Bucketing pre-sorts and distributes data into a fixed number of buckets based on a hash of the bucket column. Unlike partitioning, bucketing creates a fixed number of files and is ideal for join optimization.

```python
# ── Write a bucketed and sorted table ─────────────────────────────
(
    df_orders
    .write
    .format("delta")
    .mode("overwrite")
    .bucketBy(50, "customer_id")   # 50 buckets, hashed on customer_id
    .sortBy("order_date")           # sort within each bucket
    .saveAsTable("catalog.silver.orders_bucketed")
)

# ── Benefit: joins on customer_id avoid shuffle ────────────────────
# Both tables must be bucketed on the same column with the same bucket count
df_customers_bucketed = spark.table("catalog.silver.customers_bucketed")
df_orders_bucketed    = spark.table("catalog.silver.orders_bucketed")

# Spark performs a bucket merge join — no shuffle exchange
df_joined = df_orders_bucketed.join(df_customers_bucketed, on="customer_id")
df_joined.explain()  # look for "SortMergeJoin" without Exchange nodes
```

---

## 4. Creating DataFrames Manually

### 4.1 From a Python List with Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql import Row

# ── Method 1: List of tuples with explicit schema ──────────────────
data = [
    ("P001", "Laptop Pro",       "Electronics",  1299.99, 45, True),
    ("P002", "Wireless Mouse",   "Accessories",    29.99, 320, True),
    ("P003", "USB-C Hub",        "Accessories",    49.99, 180, True),
    ("P004", "Monitor 27-inch",  "Electronics",   649.99, 22, False),
    ("P005", "Mechanical Keyboard", "Accessories", 89.99, 210, True),
]

product_schema = StructType([
    StructField("product_id",   StringType(),  False),
    StructField("product_name", StringType(),  True),
    StructField("category",     StringType(),  True),
    StructField("unit_price",   DoubleType(),  True),
    StructField("stock_qty",    IntegerType(), True),
    StructField("is_active",    BooleanType(), True),
])

df_products = spark.createDataFrame(data, schema=product_schema)
df_products.show()
df_products.printSchema()

# ── Method 2: List of dicts (schema inferred from keys) ────────────
data_dicts = [
    {"region": "APAC",  "sales": 120_000.0, "year": 2025},
    {"region": "EMEA",  "sales": 340_000.0, "year": 2025},
    {"region": "AMER",  "sales": 510_000.0, "year": 2025},
    {"region": "APAC",  "sales": 145_000.0, "year": 2026},
]

df_sales = spark.createDataFrame(data_dicts)
df_sales.printSchema()
```

---

### 4.2 Using `spark.range()`

`spark.range()` creates a DataFrame with a single `id` column (LongType). It is extremely useful for generating test data, synthetic datasets, and benchmarking.

```python
# ── Simple range ───────────────────────────────────────────────────
df_ids = spark.range(10)          # 0 through 9
df_ids.show()

# ── Range with start, end, step, and partition count ──────────────
df_range = spark.range(
    start=0,
    end=1_000_000,
    step=1,
    numPartitions=20          # controls parallelism
)

# ── Generate a synthetic test table ───────────────────────────────
from pyspark.sql import functions as F
import string

df_synthetic = (
    spark.range(100_000)
    .withColumn("name",        F.concat(F.lit("user_"), F.col("id").cast("string")))
    .withColumn("email",       F.concat(F.lit("user"), F.col("id").cast("string"), F.lit("@example.com")))
    .withColumn("score",       (F.rand(seed=42) * 100).cast("double"))
    .withColumn("is_premium",  (F.rand(seed=7) > 0.8))
    .withColumn("signup_date", F.date_add(F.lit("2020-01-01"), (F.rand(seed=99) * 2000).cast("int")))
)

df_synthetic.show(5)
```

---

### 4.3 Using `Row` Objects

```python
from pyspark.sql import Row

# ── Named Row ─────────────────────────────────────────────────────
Order = Row("order_id", "customer_id", "total", "status")

row1 = Order("ORD-001", "CUST-42", 150.00, "shipped")
row2 = Order("ORD-002", "CUST-17", 299.99, "pending")
row3 = Order("ORD-003", "CUST-88",  45.50, "delivered")

df_rows = spark.createDataFrame([row1, row2, row3])
df_rows.show()

# ── Anonymous Row (positional) ─────────────────────────────────────
rows = [Row(1, "Alice", 35), Row(2, "Bob", 28), Row(3, "Carol", 42)]
df_anon = spark.createDataFrame(rows, ["user_id", "name", "age"])
df_anon.show()

# ── Nested Row structures ──────────────────────────────────────────
Address = Row("street", "city", "country")
Person  = Row("person_id", "name", "address")

people = [
    Person("P001", "Alice",  Address("123 Main St", "New York",  "USA")),
    Person("P002", "Bruno",  Address("456 Oak Ave", "São Paulo", "BRA")),
    Person("P003", "Claire", Address("789 Pine Rd", "London",    "GBR")),
]

df_nested = spark.createDataFrame(people)
df_nested.printSchema()
df_nested.select("person_id", "name", "address.city", "address.country").show()
```

---

### 4.4 From a Pandas DataFrame

```python
import pandas as pd

# ── Create Pandas DataFrame ────────────────────────────────────────
pdf = pd.DataFrame({
    "product_id":    ["P001", "P002", "P003"],
    "product_name":  ["Laptop", "Mouse", "Keyboard"],
    "unit_price":    [1299.99, 29.99, 89.99],
    "launch_date":   pd.to_datetime(["2024-01-15", "2024-03-01", "2024-06-20"]),
})

# ── Convert to Spark DataFrame ────────────────────────────────────
df_from_pandas = spark.createDataFrame(pdf)
df_from_pandas.printSchema()
df_from_pandas.show()

# ── Convert back to Pandas ────────────────────────────────────────
pdf_back = df_from_pandas.toPandas()   # caution: collects all data to driver

# ── Use Arrow-based conversion for better performance ─────────────
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
df_arrow = spark.createDataFrame(pdf)   # uses Apache Arrow for fast serialization

# ── Pandas API on Spark (ps) — most Pandas operations on Spark scale ─
import pyspark.pandas as ps

psdf = ps.from_pandas(pdf)             # Pandas-like API over distributed data
psdf["revenue"] = psdf["unit_price"] * 100
print(psdf.describe())
```

---

## 5. Schema Management

### 5.1 StructType and StructField

```python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, DateType, TimestampType,
    ArrayType, MapType, BinaryType, DecimalType
)

# ── Comprehensive schema definition ───────────────────────────────
transaction_schema = StructType([
    StructField("txn_id",          StringType(),       nullable=False),
    StructField("account_id",      StringType(),       nullable=True),
    StructField("amount",          DecimalType(18,2),  nullable=True),   # precision, scale
    StructField("currency",        StringType(),       nullable=True),
    StructField("txn_time",        TimestampType(),    nullable=True),
    StructField("is_fraud",        BooleanType(),      nullable=True),
    StructField("tags",            ArrayType(StringType()), nullable=True),
    StructField("metadata",        MapType(StringType(), StringType()), nullable=True),
    StructField("raw_payload",     BinaryType(),       nullable=True),
    StructField("merchant", StructType([
        StructField("merchant_id",   StringType(),  True),
        StructField("merchant_name", StringType(),  True),
        StructField("category",      StringType(),  True),
        StructField("country_code",  StringType(),  True),
    ]), nullable=True),
])

print(transaction_schema.simpleString())
# struct<txn_id:string,account_id:string,amount:decimal(18,2),...>

# ── Get schema from an existing DataFrame ─────────────────────────
schema_from_df = df_orders.schema
print(schema_from_df.json())   # serialize to JSON for storage/sharing
```

---

### 5.2 Schema on Read vs. Schema on Write

| Aspect            | Schema on Read                                    | Schema on Write                            |
| ----------------- | ------------------------------------------------- | ------------------------------------------ |
| **When enforced** | At query time                                     | At write time                              |
| **Flexibility**   | High — same data, multiple schema interpretations | Lower — data must match schema             |
| **Performance**   | Slower — type coercion at scan time               | Faster — types already stored correctly    |
| **Reliability**   | Lower — silently accepts bad data                 | Higher — rejects non-conforming data       |
| **Examples**      | CSV with `inferSchema`, JSON                      | Delta tables, Parquet with explicit schema |

```python
# ── Schema on read example — CSV with inferSchema ─────────────────
df_sor = spark.read.option("inferSchema", "true").csv("s3://bucket/raw/")
# Types determined at scan time — may differ across different file batches

# ── Schema on write example — Delta with enforced schema ──────────
# First, create table with enforced schema:
spark.sql("""
    CREATE TABLE IF NOT EXISTS catalog.silver.orders (
        order_id     STRING    NOT NULL,
        customer_id  STRING,
        order_date   DATE,
        total_amount DECIMAL(12,2),
        status       STRING
    )
    USING DELTA
    LOCATION 's3://my-bucket/delta/orders/'
""")

# Writing incompatible data raises AnalysisException:
# df_with_different_types.write.format("delta").mode("append").saveAsTable("catalog.silver.orders")
```

---

### 5.3 Schema Evolution

Delta Lake supports **automatic schema evolution** with controlled options.

```python
# ── Allow new columns to be added automatically ───────────────────
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

(
    df_with_new_column
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")     # adds new columns to existing schema
    .saveAsTable("catalog.silver.orders")
)

# ── Overwrite with full schema replacement ─────────────────────────
(
    df_reshaped
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")  # replaces entire schema
    .saveAsTable("catalog.silver.orders")
)

# ── Check schema history with DESCRIBE HISTORY ────────────────────
%sql
DESCRIBE HISTORY catalog.silver.orders;
-- Shows all operations including schema changes with timestamp and user
```

---

### 5.4 Enforcing Schema with DDL Strings

```python
# ── DDL string for simple schemas ─────────────────────────────────
ddl = """
    order_id    STRING  NOT NULL,
    customer_id STRING,
    order_date  DATE,
    total       DOUBLE,
    status      STRING
"""
df_ddl = spark.read.schema(ddl).json("s3://bucket/orders/")

# ── Convert DDL string to StructType ─────────────────────────────
from pyspark.sql.types import _parse_datatype_string

schema_from_ddl = spark.createDataFrame([], ddl).schema
# or
schema_from_ddl = spark._jvm.org.apache.spark.sql.types.DataType.fromJson(
    spark.createDataFrame([], ddl).schema.json()
)
```

---

## 6. Working with Nested and Complex Data

### 6.1 Nested JSON Structures

```python
from pyspark.sql import functions as F

# Sample nested JSON:
# {"order_id":"O1","customer":{"id":"C1","name":"Alice"},"items":[...]}

df_orders = spark.read.format("json").option("inferSchema","true") \
                .load("s3://bucket/raw_orders/")

# ── Access nested struct fields with dot notation ──────────────────
df_flat = df_orders.select(
    "order_id",
    F.col("customer.id").alias("customer_id"),
    F.col("customer.name").alias("customer_name"),
    F.col("customer.address.city").alias("city"),
    F.col("customer.address.country").alias("country"),
)

# ── Rename or restructure with withColumn ─────────────────────────
df_renamed = (
    df_orders
    .withColumn("cust_id",      F.col("customer.id"))
    .withColumn("cust_name",    F.col("customer.name"))
    .withColumn("ship_city",    F.col("shipping.address.city"))
    .drop("customer", "shipping")
)
```

---

### 6.2 ArrayType Columns

```python
# ── DataFrame with array column ────────────────────────────────────
# items: [{"product_id":"P1","qty":2},{"product_id":"P2","qty":1}]

# ── explode: one row per array element ───────────────────────────
df_exploded = df_orders.select(
    "order_id",
    F.explode("items").alias("item")
).select(
    "order_id",
    F.col("item.product_id"),
    F.col("item.qty"),
    F.col("item.unit_price"),
)

# ── explode_outer: keeps rows with empty/null arrays ───────────────
df_safe = df_orders.select(
    "order_id",
    F.explode_outer("items").alias("item")
)

# ── posexplode: also returns element index ─────────────────────────
df_pos = df_orders.select(
    "order_id",
    F.posexplode("items").alias("item_pos", "item")
)

# ── Array manipulation functions ───────────────────────────────────
df_arrays = df_orders.select(
    "order_id",
    F.size("items").alias("item_count"),
    F.array_contains("tags", "express").alias("is_express"),
    F.array_distinct("tags").alias("unique_tags"),
    F.array_sort("tags").alias("sorted_tags"),
    F.concat("tags", F.array(F.lit("postprocessed"))).alias("all_tags"),
)

# ── collect_list / collect_set — aggregate rows into array ─────────
df_user_orders = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.collect_list("order_id").alias("all_order_ids"),
        F.collect_set("status").alias("distinct_statuses"),
        F.count("*").alias("order_count"),
    )
)
```

---

### 6.3 MapType Columns

```python
# ── Create a map column from two arrays ───────────────────────────
df_with_map = df.select(
    "product_id",
    F.map_from_arrays(
        F.array(F.lit("color"), F.lit("size"), F.lit("material")),
        F.array(F.col("color"), F.col("size"), F.col("material"))
    ).alias("attributes")
)

# ── Access map values ─────────────────────────────────────────────
df_with_map.withColumn("color_value", F.col("attributes")["color"]).show()

# ── Map manipulation ───────────────────────────────────────────────
df_maps = df_with_map.select(
    "product_id",
    F.map_keys("attributes").alias("attribute_keys"),
    F.map_values("attributes").alias("attribute_values"),
    F.map_contains_key("attributes", "color").alias("has_color"),
    F.map_concat("attributes", F.create_map(F.lit("brand"), F.lit("Acme"))).alias("full_attrs"),
)
```

---

### 6.4 `explode`, `flatten`, `from_json`, and `to_json`

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# ── flatten: collapse nested array of arrays ──────────────────────
df_nested_arrays = spark.createDataFrame(
    [([["a","b"],["c","d"]],), ([["e"],["f","g","h"]],)],
    ["nested"]
)
df_flat_arr = df_nested_arrays.withColumn("flat", F.flatten("nested"))
# [["a","b"],["c","d"]] → ["a","b","c","d"]

# ── from_json: parse JSON string column into struct ────────────────
payload_schema = StructType([
    StructField("event_id",   StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("amount",     DoubleType(), True),
])

df_parsed = (
    df_raw_events
    .withColumn("parsed", F.from_json(F.col("payload_string"), payload_schema))
    .select("*", "parsed.*")
    .drop("payload_string", "parsed")
)

# ── to_json: serialize struct/array/map column to JSON string ──────
df_serialized = df_orders.select(
    "order_id",
    F.to_json(F.struct("customer_id", "total", "status")).alias("summary_json"),
    F.to_json(F.col("items")).alias("items_json"),
)

# ── schema_of_json: infer schema of a JSON string sample ──────────
sample_json = '{"id": "X1", "score": 9.2, "tags": ["vip", "repeat"]}'
inferred = spark.range(1).select(
    F.schema_of_json(F.lit(sample_json)).alias("schema")
).collect()[0]["schema"]
print(inferred)
# struct<id:string,score:double,tags:array<string>>
```

---

## 7. Best Practices for Managing Data Sources

### 7.1 File Discovery Patterns

```python
# ── Glob patterns for flexible path matching ───────────────────────
# All CSV files in any month directory under 2025:
df = spark.read.csv("s3://bucket/data/year=2025/month=*/")

# All JSON files under any subdirectory:
df = spark.read.json("s3://bucket/raw/**/*.json")

# Multiple explicit paths:
df = spark.read.format("parquet").load(
    "s3://bucket/data/part-001.parquet",
    "s3://bucket/data/part-002.parquet",
    "s3://bucket/data/part-003.parquet",
)

# List of paths from a Python list:
paths = [f"s3://bucket/data/year=2025/month={m:02d}/" for m in range(1, 13)]
df = spark.read.format("parquet").load(*paths)

# ── Add filename as a column for provenance tracking ──────────────
df_with_src = df.withColumn("source_file", F.input_file_name())
```

---

### 7.2 Handling Corrupt Records

Spark offers three **parse modes** for handling malformed rows.

| Mode                   | Behaviour                                                                   | Use Case                                                  |
| ---------------------- | --------------------------------------------------------------------------- | --------------------------------------------------------- |
| `PERMISSIVE` (default) | Puts corrupt record in `_corrupt_record` column; sets other columns to null | Development, auditing bad rows                            |
| `DROPMALFORMED`        | Silently drops malformed rows                                               | Tolerant pipelines where occasional bad rows are expected |
| `FAILFAST`             | Raises exception on first malformed row                                     | Strict validation pipelines                               |

```python
# ── PERMISSIVE mode — capture bad rows for review ─────────────────
df_permissive = (
    spark.read
    .format("csv")
    .option("header",                   "true")
    .option("mode",                     "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(my_schema)
    .load("s3://bucket/raw/noisy_data/")
)

# Separate good from bad rows:
df_good = df_permissive.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
df_bad  = df_permissive.filter(F.col("_corrupt_record").isNotNull())

# Write bad rows to quarantine for review:
df_bad.write.format("delta").mode("append").saveAsTable("catalog.quarantine.csv_bad_rows")

# ── DROPMALFORMED mode ────────────────────────────────────────────
df_drop = (
    spark.read
    .format("json")
    .option("mode", "DROPMALFORMED")
    .load("s3://bucket/raw/events/")
)

# ── FAILFAST mode — strict validation ─────────────────────────────
df_strict = (
    spark.read
    .format("csv")
    .option("header",     "true")
    .option("mode",       "FAILFAST")
    .schema(strict_schema)
    .load("s3://bucket/validated_feeds/")
)
```

---

### 7.3 Optimizing Reads: Predicate Pushdown and Column Pruning

```python
# ── Column pruning — only read needed columns ─────────────────────
# BAD: reads all 80 columns from Parquet
df_all = spark.read.parquet("s3://bucket/events/")
result = df_all.select("event_id", "user_id", "event_time", "event_type")

# GOOD: specify columns upfront — Parquet readers skip other column chunks
df_pruned = (
    spark.read
    .parquet("s3://bucket/events/")
    .select("event_id", "user_id", "event_time", "event_type")
)

# ── Predicate pushdown — filter at scan time ──────────────────────
# BAD: reads all data, then filter in Spark
df_filtered = spark.read.parquet("s3://bucket/events/").filter("event_type = 'purchase'")

# GOOD: for partitioned data, Spark prunes directories:
df_partition_pruned = (
    spark.read
    .parquet("s3://bucket/events/")
    .filter(
        (F.col("year") == 2026) &
        (F.col("month") == 3) &
        (F.col("event_type") == "purchase")
    )
)

# Verify pushdown is occurring:
df_partition_pruned.explain(extended=True)
# Look for "PushedFilters" in the physical plan

# ── Delta: Z-order improves data skipping ─────────────────────────
%sql
OPTIMIZE catalog.silver.events ZORDER BY (event_type, event_time);
-- After Z-order, filters on event_type skip many files via Delta's min/max stats
```

---

### 7.4 Separating Raw, Processed, and Curated Zones

A well-organized Databricks lakehouse separates data into distinct zones with clear ownership and access rules:

```
DATA ZONE ARCHITECTURE
──────────────────────────────────────────────────────────────────────
 LANDING ZONE     BRONZE ZONE      SILVER ZONE       GOLD ZONE
 (Object Store)   (Raw Delta)      (Cleaned Delta)   (Aggregated Delta)

 s3://bucket/     catalog.bronze   catalog.silver    catalog.gold
  raw/            ─────────────    ──────────────    ────────────
  ├── orders/     No transforms    Type casting      Business KPIs
  ├── events/     No filtering     Deduplication     Joined datasets
  └── products/   Keep all data    Null handling     ML feature tables
                  Source fidelity  Schema enforced   Dashboard-ready

 Policy: No       Policy: Append   Policy: Append/   Policy: Overwrite
         deletion  only, immutable  Upsert allowed    on schedule
```

```python
# ── Bronze: ingest raw files without transformation ────────────────
df_bronze = (
    spark.read
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "s3://bucket/schema/orders/")
    .load("s3://bucket/raw/orders/")
    .withColumn("_ingest_time", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)
df_bronze.write.format("delta").mode("append").saveAsTable("catalog.bronze.orders")

# ── Silver: clean and enforce schema ──────────────────────────────
df_silver = (
    spark.table("catalog.bronze.orders")
    .filter(F.col("order_id").isNotNull())
    .withColumn("order_time", F.to_timestamp("order_time"))
    .withColumn("total",      F.round(F.col("qty") * F.col("unit_price"), 2))
    .dropDuplicates(["order_id"])
)
df_silver.write.format("delta").mode("append").saveAsTable("catalog.silver.orders")

# ── Gold: aggregate for BI/dashboards ─────────────────────────────
df_gold = (
    spark.table("catalog.silver.orders")
    .groupBy("order_date", "region", "product_category")
    .agg(
        F.sum("total").alias("daily_revenue"),
        F.count("*").alias("order_count"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
)
df_gold.write.format("delta").mode("overwrite").saveAsTable("catalog.gold.daily_sales_summary")
```

---

### 7.5 Compression Codec Selection

| Codec    | Compression Ratio | Speed     | Best For                                        |
| -------- | ----------------- | --------- | ----------------------------------------------- |
| `snappy` | Medium            | Very fast | Default Parquet/Delta — balanced                |
| `gzip`   | High              | Slow      | Cold storage, CSV exports                       |
| `zstd`   | High              | Fast      | Modern Parquet — better than gzip at same speed |
| `lz4`    | Low               | Fastest   | Hot path data, temp tables                      |
| `brotli` | Highest           | Slowest   | Long-term archive                               |
| `none`   | None              | N/A       | Text files, small debug data                    |

```python
# ── Set default compression for Delta ─────────────────────────────
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

# ── Override per-write ─────────────────────────────────────────────
df.write.format("parquet").option("compression", "zstd").save("s3://bucket/output/")
df.write.format("csv").option("compression", "gzip").save("s3://bucket/export/")
```

---

## 8. Summary

| Section                           | Key Points                                                                                                                                                                                                                                                                                                                     |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Introduction to Data Sources**  | Databricks supports files (CSV, JSON, Parquet, ORC, Avro), Delta Lake, JDBC, and streaming sources; DataFrameReader and DataFrameWriter share a consistent fluent API; Delta Lake is the recommended format for all production tables                                                                                          |
| **DataFrameReader: Reading Data** | Use explicit schemas for production (skip inferSchema); CSV and JSON support PERMISSIVE/DROPMALFORMED/FAILFAST parse modes; Parquet and Delta support predicate pushdown and column pruning; JDBC reads benefit from `partitionColumn` + `numPartitions` for parallelism; always load JDBC credentials from Databricks Secrets |
| **DataFrameWriter: Writing Data** | Four write modes: overwrite / append / ignore / error; `partitionBy` creates directory-based partition pruning; `bucketBy` + `sortBy` eliminates shuffle in co-located joins; use `mergeSchema` for safe schema evolution on appends                                                                                           |
| **Creating DataFrames Manually**  | `spark.createDataFrame(data, schema)` from list of tuples or dicts; `spark.range()` for test data generation; `Row` objects for named tuples; `spark.createDataFrame(pdf)` for Pandas interop; enable Arrow for fast Pandas ↔ Spark conversion                                                                                 |
| **Schema Management**             | StructType / StructField provides the full type system; explicit schema is always preferred over inference in production; schema on write (Delta) is more reliable than schema on read (CSV/JSON); `mergeSchema` adds new columns, `overwriteSchema` replaces the schema                                                       |
| **Nested and Complex Data**       | Access struct fields with dot notation; `explode` for arrays, `explode_outer` retains nulls; `from_json` parses string columns into structs; `to_json` serializes structs back to strings; `flatten` collapses arrays of arrays                                                                                                |
| **Best Practices**                | Use glob patterns for flexible file discovery; quarantine corrupt records with PERMISSIVE mode + `_corrupt_record` column; always push predicates down to partition columns; separate raw/bronze/silver/gold zones with clear policies; choose `zstd` over `gzip` for modern Parquet workloads                                 |

---
