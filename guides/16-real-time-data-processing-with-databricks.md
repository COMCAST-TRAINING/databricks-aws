# Real-Time Data Processing with Databricks

---

## Table of Contents

1. [Introduction to Real-Time Analytics](#1-introduction-to-real-time-analytics)
   - 1.1 [What Is Real-Time Analytics?](#11-what-is-real-time-analytics)
   - 1.2 [Batch vs. Streaming: Key Differences](#12-batch-vs-streaming-key-differences)
   - 1.3 [Databricks Streaming Architecture](#13-databricks-streaming-architecture)
   - 1.4 [Industry Use Cases](#14-industry-use-cases)
2. [Spark Structured Streaming Fundamentals](#2-spark-structured-streaming-fundamentals)
   - 2.1 [The Unbounded Table Model](#21-the-unbounded-table-model)
   - 2.2 [Triggers](#22-triggers)
   - 2.3 [Output Modes](#23-output-modes)
   - 2.4 [Watermarking for Late Data](#24-watermarking-for-late-data)
   - 2.5 [Checkpointing and Fault Tolerance](#25-checkpointing-and-fault-tolerance)
3. [Implementing Streaming Data Pipelines](#3-implementing-streaming-data-pipelines)
   - 3.1 [Reading from File Sources with Autoloader](#31-reading-from-file-sources-with-autoloader)
   - 3.2 [Streaming Transformations](#32-streaming-transformations)
   - 3.3 [Writing to Delta Lake Sinks](#33-writing-to-delta-lake-sinks)
   - 3.4 [The `foreachBatch` Pattern](#34-the-foreachbatch-pattern)
   - 3.5 [End-to-End Autoloader → Delta Pipeline](#35-end-to-end-autoloader--delta-pipeline)
4. [Working with Apache Kafka and Spark Structured Streaming](#4-working-with-apache-kafka-and-spark-structured-streaming)
   - 4.1 [Kafka Architecture Overview](#41-kafka-architecture-overview)
   - 4.2 [Configuring the Kafka Connector in Databricks](#42-configuring-the-kafka-connector-in-databricks)
   - 4.3 [Reading from Kafka](#43-reading-from-kafka)
   - 4.4 [Deserializing Kafka Messages](#44-deserializing-kafka-messages)
   - 4.5 [Writing to Kafka](#45-writing-to-kafka)
   - 4.6 [End-to-End Kafka → Delta Lake Pipeline](#46-end-to-end-kafka--delta-lake-pipeline)
5. [Stateful Streaming and Windowing](#5-stateful-streaming-and-windowing)
   - 5.1 [Tumbling Windows](#51-tumbling-windows)
   - 5.2 [Sliding Windows](#52-sliding-windows)
   - 5.3 [Session Windows](#53-session-windows)
   - 5.4 [Arbitrary Stateful Processing with flatMapGroupsWithState](#54-arbitrary-stateful-processing-with-flatmapgroupswithstate)
6. [Real-Time Data Processing Use Cases](#6-real-time-data-processing-use-cases)
   - 6.1 [IoT Sensor Stream and Anomaly Detection](#61-iot-sensor-stream-and-anomaly-detection)
   - 6.2 [Clickstream Real-Time Aggregation](#62-clickstream-real-time-aggregation)
   - 6.3 [Change Data Capture with Kafka and Delta Lake](#63-change-data-capture-with-kafka-and-delta-lake)
7. [Monitoring and Debugging Streaming Pipelines](#7-monitoring-and-debugging-streaming-pipelines)
   - 7.1 [StreamingQuery Progress and Status](#71-streamingquery-progress-and-status)
   - 7.2 [Spark UI Streaming Tab](#72-spark-ui-streaming-tab)
   - 7.3 [Common Failure Patterns and Fixes](#73-common-failure-patterns-and-fixes)
8. [Summary](#8-summary)

---

## 1. Introduction to Real-Time Analytics

### 1.1 What Is Real-Time Analytics?

**Real-time analytics** is the discipline of processing and querying data as it arrives, enabling decisions within milliseconds to seconds rather than hours or days. In contrast to traditional batch pipelines that run on a schedule, streaming pipelines process each new event or micro-batch of events immediately.

Databricks unifies batch and streaming workloads on the same platform using **Apache Spark Structured Streaming** — a scalable, fault-tolerant streaming engine built on Spark SQL. Whether you stream from Apache Kafka, AWS Kinesis, cloud object storage, or Delta Lake itself, the programming model stays consistent with the DataFrames API you already know.

---

### 1.2 Batch vs. Streaming: Key Differences

| Dimension          | Batch Processing                          | Stream Processing                                     |
| ------------------ | ----------------------------------------- | ----------------------------------------------------- |
| **Data freshness** | Minutes to hours                          | Milliseconds to seconds                               |
| **Trigger**        | Scheduled (cron, workflow)                | Continuous / micro-batch                              |
| **Latency**        | High — entire dataset loads before output | Low — output produced per event/micro-batch           |
| **Fault recovery** | Re-run the job from start                 | Checkpoint-based exactly-once recovery                |
| **Statefulness**   | All data available in memory at once      | State maintained across micro-batches via state store |
| **Use cases**      | Nightly ETL, reports, model training      | Fraud detection, IoT monitoring, live dashboards      |
| **Spark API**      | `spark.read` / `df.write`                 | `spark.readStream` / `df.writeStream`                 |
| **Cost model**     | Burst compute                             | Always-on or nearly continuous cluster                |

> **Tip:** Structured Streaming uses the same DataFrame/Dataset API as batch. Many transformations (`select`, `filter`, `groupBy`, `join`) work on both batch and stream DataFrames with no code changes.

---

### 1.3 Databricks Streaming Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DATABRICKS STREAMING ARCHITECTURE                 │
│                                                                      │
│    DATA SOURCES         COMPUTE (Structured Streaming)    SINKS     │
│                                                                      │
│  ┌─────────────┐       ┌────────────────────────────┐   ┌────────┐  │
│  │ Apache Kafka │──────▶│  Spark Driver              │──▶│ Delta  │  │
│  └─────────────┘       │  - StreamingQuery manager  │   │ Lake   │  │
│  ┌─────────────┐       │  - Checkpoint state        │   └────────┘  │
│  │  AWS Kinesis │──────▶│                            │   ┌────────┐  │
│  └─────────────┘       │  Spark Executors            │──▶│ Kafka  │  │
│  ┌─────────────┐       │  - Source partitions read  │   │ Topic  │  │
│  │  S3 / ADLS  │──────▶│  - Micro-batch processing  │   └────────┘  │
│  │ (Autoloader)│       │  - State store (RocksDB)   │   ┌────────┐  │
│  └─────────────┘       │  - Shuffle/aggregation     │──▶│ Memory │  │
│  ┌─────────────┐       └────────────────────────────┘   │ Table  │  │
│  │ Delta Table │                  │                      └────────┘  │
│  │  (CDC)      │       ┌──────────▼─────────┐           ┌────────┐  │
│  └─────────────┘       │  Checkpoint Dir    │──────────▶│Foreach │  │
│                        │  (S3 / ADLS)       │           │Batch   │  │
│                        └────────────────────┘           └────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

Each **micro-batch** cycle:

1. Source scans for new data since the last offset
2. Executor tasks process the new records
3. Results are written to the sink
4. Checkpoint is updated atomically — guaranteeing exactly-once delivery end-to-end

---

### 1.4 Industry Use Cases

| Industry               | Streaming Use Case                                 | Typical Latency SLA |
| ---------------------- | -------------------------------------------------- | ------------------- |
| **Financial Services** | Real-time fraud scoring on card transactions       | < 100 ms            |
| **E-Commerce**         | Live inventory updates and recommendation feeds    | < 1 s               |
| **Telecommunications** | Network anomaly detection on switch telemetry      | < 5 s               |
| **Healthcare**         | Patient vitals monitoring from medical IoT devices | < 500 ms            |
| **Logistics**          | Fleet tracking and ETA recalculation               | < 2 s               |
| **Media & Gaming**     | Live leaderboards and engagement analytics         | < 1 s               |
| **Manufacturing**      | Predictive maintenance on sensor streams           | < 10 s              |

---

## 2. Spark Structured Streaming Fundamentals

### 2.1 The Unbounded Table Model

The key mental model of Structured Streaming is the **unbounded table**: new data arriving from a stream is conceptually appended as new rows to an infinite table. Queries are defined once against this table and run continuously or on a trigger.

```
Streaming Source (e.g., Kafka topic)
─────────────────────────────────────
Time  event_time    user_id   action
 t1   2026-01-01    user_42   click
 t2   2026-01-01    user_17   view
 t3   2026-01-01    user_42   purchase
 t4   2026-01-01    user_99   click
 ...  (grows forever)
```

Your query (`groupBy("user_id").count()`) runs incrementally over this growing table. Spark only processes **new rows** each trigger, updating the result state in its state store.

---

### 2.2 Triggers

Triggers control **when** a micro-batch is executed.

| Trigger                           | Code                                   | Behaviour                                                    |
| --------------------------------- | -------------------------------------- | ------------------------------------------------------------ |
| **Default (as fast as possible)** | No trigger specified                   | Next batch starts immediately after previous finishes        |
| **Fixed interval**                | `Trigger.ProcessingTime("30 seconds")` | Batch runs every 30 s; skips if previous batch still running |
| **Once (deprecated)**             | `Trigger.Once()`                       | Runs exactly one micro-batch then stops                      |
| **AvailableNow**                  | `Trigger.AvailableNow()`               | Processes all available data in multiple batches then stops  |
| **Continuous (experimental)**     | `Trigger.Continuous("1 second")`       | Row-level processing; ~1 ms latency; limited operations      |

```python
# ProcessingTime trigger — recommended for production
query = (
    df_stream
    .writeStream
    .format("delta")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/checkpoints/my_stream")
    .start("/delta/output")
)

# AvailableNow — useful for scheduled incremental loads
query = (
    df_stream
    .writeStream
    .format("delta")
    .trigger(availableNow=True)
    .option("checkpointLocation", "/checkpoints/my_stream")
    .start("/delta/output")
)
```

> **Important:** `Trigger.AvailableNow()` is the recommended replacement for `Trigger.Once()` in Databricks Runtime 11.3+. It processes all available data in multiple optimally-sized batches, improving efficiency.

---

### 2.3 Output Modes

Output modes define **which rows** are written to the sink each trigger.

| Mode         | Description                               | Supported Aggregations                       | Typical Sink              |
| ------------ | ----------------------------------------- | -------------------------------------------- | ------------------------- |
| **append**   | Only new rows added since last trigger    | Non-aggregated, or aggregated with watermark | Delta, Kafka, files       |
| **update**   | Only rows that changed since last trigger | Aggregated                                   | Delta, memory table       |
| **complete** | Full result table rewritten every trigger | Aggregated (no watermark required)           | Memory table, small Delta |

```python
from pyspark.sql import functions as F

# ── append mode — no aggregation ──────────────────────────────────
query_append = (
    spark
    .readStream.format("delta").load("/delta/raw_events")
    .filter(F.col("event_type") == "purchase")
    .writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "/checkpoints/purchases")
    .start("/delta/purchases")
)

# ── update mode — streaming aggregation ───────────────────────────
query_update = (
    spark
    .readStream.format("delta").load("/delta/raw_events")
    .groupBy("user_id")
    .count()
    .writeStream
    .outputMode("update")
    .format("memory")
    .queryName("user_event_counts")
    .start()
)
```

---

### 2.4 Watermarking for Late Data

In real-world streams, events arrive **out of order** due to network delays. **Watermarking** tells Spark how long to wait for late-arriving events before closing a time window.

```
Event time:   09:00  09:01  09:02  09:03  09:04  09:05  ...
Watermark:    ───────────────────────────────────────────▶
              Events older than (max_event_time - 10 min)
              are dropped
```

```python
from pyspark.sql import functions as F

# Define watermark of 10 minutes on event_time
df_with_watermark = (
    df_stream
    .withWatermark("event_time", "10 minutes")   # tolerate 10-min late arrivals
    .groupBy(
        F.window("event_time", "5 minutes"),      # 5-minute tumbling window
        "sensor_id"
    )
    .agg(
        F.avg("temperature").alias("avg_temp"),
        F.max("temperature").alias("max_temp"),
        F.count("*").alias("reading_count")
    )
)

query = (
    df_with_watermark
    .writeStream
    .outputMode("append")          # append mode works because watermark is defined
    .format("delta")
    .option("checkpointLocation", "/checkpoints/sensor_agg")
    .start("/delta/sensor_aggregates")
)
```

> **Rule:** `append` output mode with aggregations requires a watermark. Without a watermark, use `complete` or `update` mode.

---

### 2.5 Checkpointing and Fault Tolerance

Checkpointing persists:

- **Source offsets** — exactly where the stream was last read
- **State store snapshots** — for aggregations and stateful operations

```python
# Always specify a checkpoint location — required for production
query = (
    df_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/stream_name/")
    .start("s3://my-bucket/delta/output/")
)
```

**Checkpoint directory structure:**

```
s3://my-bucket/checkpoints/stream_name/
├── commits/
│   ├── 0          ← committed batch IDs
│   ├── 1
│   └── 2
├── offsets/
│   ├── 0          ← source offsets for each batch
│   ├── 1
│   └── 2
└── state/
    └── 0/         ← RocksDB state store snapshots
        ├── 1.zip
        └── 2.zip
```

> **Important:** Never share a checkpoint directory between two streaming queries. Each query must have its own unique checkpoint path.

---

## 3. Implementing Streaming Data Pipelines

### 3.1 Reading from File Sources with Autoloader

**Autoloader** (`cloudFiles`) is the recommended way to incrementally ingest files from cloud storage into Delta Lake. It automatically discovers new files as they land, handles schema inference and evolution, and scales to billions of files without listing the entire directory each run.

```python
# ── Reading CSV files with Autoloader ─────────────────────────────
df_raw = (
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "s3://my-bucket/schema/events/")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("s3://my-bucket/raw/events/")
)

# ── Reading JSON files with Autoloader ────────────────────────────
df_json = (
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "s3://my-bucket/schema/orders/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("s3://my-bucket/raw/orders/")
)

# ── Reading with an explicit schema (no inference overhead) ────────
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

event_schema = StructType([
    StructField("event_id",    StringType(),    nullable=False),
    StructField("event_time",  TimestampType(), nullable=False),
    StructField("user_id",     StringType(),    nullable=True),
    StructField("event_type",  StringType(),    nullable=True),
    StructField("amount",      DoubleType(),    nullable=True),
])

df_typed = (
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "s3://my-bucket/schema/events_v2/")
    .schema(event_schema)
    .load("s3://my-bucket/raw/events/")
)
```

**Autoloader Options Reference:**

| Option                          | Description                                         | Example                          |
| ------------------------------- | --------------------------------------------------- | -------------------------------- |
| `cloudFiles.format`             | File format to parse                                | `csv`, `json`, `parquet`, `avro` |
| `cloudFiles.schemaLocation`     | Where to persist inferred schema                    | S3/ADLS path                     |
| `cloudFiles.inferColumnTypes`   | Infer columns beyond strings                        | `true`                           |
| `cloudFiles.maxFilesPerTrigger` | Cap files processed per batch                       | `1000`                           |
| `cloudFiles.maxBytesPerTrigger` | Cap bytes processed per batch                       | `1g`                             |
| `cloudFiles.useNotifications`   | Use S3/Azure event notifications instead of listing | `true`                           |

---

### 3.2 Streaming Transformations

Transformations on streaming DataFrames work identically to batch DataFrames with a few constraints.

```python
from pyspark.sql import functions as F

# ── Stateless transformations — all supported ──────────────────────
df_clean = (
    df_raw
    .filter(F.col("event_type").isNotNull())
    .filter(F.col("amount") > 0)
    .withColumn("event_date", F.to_date("event_time"))
    .withColumn("hour_of_day", F.hour("event_time"))
    .withColumn(
        "amount_category",
        F.when(F.col("amount") < 10,   F.lit("micro"))
         .when(F.col("amount") < 100,  F.lit("small"))
         .when(F.col("amount") < 1000, F.lit("medium"))
         .otherwise(F.lit("large"))
    )
    .select("event_id", "event_time", "user_id", "event_type",
            "amount", "amount_category", "event_date", "hour_of_day")
)

# ── Stateful aggregation with watermark ───────────────────────────
df_hourly_totals = (
    df_clean
    .withWatermark("event_time", "15 minutes")
    .groupBy(
        F.window("event_time", "1 hour").alias("hour_window"),
        "event_type"
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("event_count"),
        F.countDistinct("user_id").alias("unique_users")
    )
    .withColumn("window_start", F.col("hour_window.start"))
    .withColumn("window_end",   F.col("hour_window.end"))
    .drop("hour_window")
)
```

**Operations supported on streaming DataFrames:**

| Category          | Supported                                               | Not Supported                                        |
| ----------------- | ------------------------------------------------------- | ---------------------------------------------------- |
| **Projections**   | `select`, `filter`, `withColumn`, `drop`                | —                                                    |
| **Aggregations**  | `groupBy`, `agg`, `count`, `sum`                        | Rolling aggregations without watermark               |
| **Joins**         | Stream-static join, stream-stream join (with watermark) | Cartesian join                                       |
| **Sorting**       | Only within `complete` output mode                      | `orderBy` in append/update mode                      |
| **Deduplication** | `dropDuplicates` with watermark                         | `dropDuplicates` without watermark (unbounded state) |
| **UDFs**          | Supported                                               | Pandas UDFs with iterator output (limited)           |

---

### 3.3 Writing to Delta Lake Sinks

Delta Lake is the recommended sink for all Databricks streaming pipelines because it provides ACID transactions, exactly-once semantics, and serves as both a stream sink and a stream source.

```python
# ── Basic Delta sink ───────────────────────────────────────────────
query = (
    df_clean
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/clean_events/")
    .option("mergeSchema", "true")           # handle new columns gracefully
    .trigger(processingTime="1 minute")
    .start("s3://my-bucket/delta/clean_events/")
)

# ── Writing to a Unity Catalog table ──────────────────────────────
query_uc = (
    df_clean
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/clean_events/")
    .trigger(processingTime="1 minute")
    .toTable("catalog.streaming.clean_events")   # Unity Catalog 3-part name
)
```

---

### 3.4 The `foreachBatch` Pattern

`foreachBatch` gives you full control over what happens with each micro-batch DataFrame. It is ideal for complex sinks: JDBC databases, multiple Delta tables, deduplication with MERGE, REST API calls, etc.

```python
from delta.tables import DeltaTable

def upsert_to_delta(batch_df, batch_id):
    """
    Upsert each micro-batch into the Delta target table using MERGE.
    batch_id is used for idempotency.
    """
    # ── Initialize target table if it doesn't exist ───────────────
    if not DeltaTable.isDeltaTable(spark, "/delta/users_current/"):
        batch_df.write.format("delta").save("/delta/users_current/")
        return

    target = DeltaTable.forPath(spark, "/delta/users_current/")

    (
        target.alias("tgt")
        .merge(
            batch_df.alias("src"),
            "tgt.user_id = src.user_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

query = (
    df_stream
    .writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "s3://my-bucket/checkpoints/users/")
    .trigger(processingTime="2 minutes")
    .start()
)
```

---

### 3.5 End-to-End Autoloader → Delta Pipeline

```python
# ═══════════════════════════════════════════════════════════════════
# End-to-End Streaming Ingestion Pipeline
# Source:  s3://my-bucket/raw/orders/   (JSON files)
# Bronze:  delta.bronze.orders          (raw + metadata)
# Silver:  delta.silver.orders          (cleaned + typed)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# ── Step 1: Define schema ──────────────────────────────────────────
order_schema = StructType([
    StructField("order_id",      StringType(),    nullable=False),
    StructField("customer_id",   StringType(),    nullable=True),
    StructField("order_time",    StringType(),    nullable=True),  # raw string
    StructField("product_id",    StringType(),    nullable=True),
    StructField("quantity",      IntegerType(),   nullable=True),
    StructField("unit_price",    DoubleType(),    nullable=True),
    StructField("status",        StringType(),    nullable=True),
])

# ── Step 2: Bronze — raw ingestion via Autoloader ──────────────────
df_bronze = (
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "s3://my-bucket/schema/orders/")
    .schema(order_schema)
    .load("s3://my-bucket/raw/orders/")
    .withColumn("_ingest_time",    F.current_timestamp())
    .withColumn("_source_file",    F.input_file_name())
    .withColumn("_batch_id",       F.lit(None).cast("long"))   # filled by foreachBatch
)

q_bronze = (
    df_bronze
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/bronze_orders/")
    .trigger(processingTime="5 minutes")
    .toTable("catalog.bronze.orders")
)

# ── Step 3: Silver — read from Bronze, clean, type-cast ───────────
df_silver = (
    spark
    .readStream
    .format("delta")
    .table("catalog.bronze.orders")
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") > 0)
    .withColumn("order_time",  F.to_timestamp("order_time", "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("order_total", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("order_date",  F.to_date("order_time"))
    .dropDuplicates(["order_id"])
)

q_silver = (
    df_silver
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/silver_orders/")
    .trigger(processingTime="5 minutes")
    .toTable("catalog.silver.orders")
)

# ── Step 4: Monitor both queries ───────────────────────────────────
import time

for _ in range(3):
    for q in [q_bronze, q_silver]:
        prog = q.lastProgress
        if prog:
            print(f"[{q.name}] batch={prog['batchId']}  "
                  f"input={prog['numInputRows']}  "
                  f"processed={prog['processedRowsPerSecond']:.1f} rows/s")
    time.sleep(30)
```

---

## 4. Working with Apache Kafka and Spark Structured Streaming

### 4.1 Kafka Architecture Overview

**Apache Kafka** is a distributed event streaming platform. It organizes data as **topics** partitioned across **brokers**. Producers write events; consumers read events using tracked **offsets**.

```
APACHE KAFKA CLUSTER
──────────────────────────────────────────────────────────────────
 Zookeeper / KRaft (metadata coordination)

 Broker 0          Broker 1          Broker 2
 ┌──────────┐      ┌──────────┐      ┌──────────┐
 │ topic_A  │      │ topic_A  │      │ topic_A  │
 │  Part 0  │      │  Part 1  │      │  Part 2  │
 │ [0,1,2,] │      │ [0,1,2,] │      │ [0,1,2,] │
 │          │      │          │      │          │
 │ topic_B  │      │ topic_B  │      │ topic_B  │
 │  Part 0  │      │  Part 1  │      │  Part 2  │
 └──────────┘      └──────────┘      └──────────┘

PRODUCERS   ──────▶ write to topic partitions (keyed or round-robin)
CONSUMERS   ──────▶ read from partitions; track offsets per group
DATABRICKS  ──────▶ Spark Structured Streaming consumer group

Each message:  key (bytes) | value (bytes) | partition | offset | timestamp
```

---

### 4.2 Configuring the Kafka Connector in Databricks

The Kafka connector is bundled with Databricks Runtime (DBR 7.x+). For older clusters, attach the Maven library `org.apache.spark:spark-sql-kafka-0-10_2.12:<version>` via the cluster Libraries UI.

> **Security:** Never embed Kafka credentials (username, password, SASL JAAS config) directly in notebook code. Store them in Databricks Secrets and reference via `dbutils.secrets.get()`.

```python
# ── Retrieve secrets from Databricks Secrets ──────────────────────
kafka_username = dbutils.secrets.get(scope="kafka-creds", key="username")
kafka_password = dbutils.secrets.get(scope="kafka-creds", key="password")

# ── SASL_SSL config string (MSK / Confluent Cloud) ─────────────────
sasl_config = (
    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{kafka_username}" password="{kafka_password}";'
)

kafka_options = {
    "kafka.bootstrap.servers":        "b-1.mycluster.kafka.us-east-1.amazonaws.com:9096",
    "kafka.security.protocol":        "SASL_SSL",
    "kafka.sasl.mechanism":           "PLAIN",
    "kafka.sasl.jaas.config":         sasl_config,
    "kafka.ssl.endpoint.identification.algorithm": "https",
}
```

---

### 4.3 Reading from Kafka

```python
# ── Reading a single topic ─────────────────────────────────────────
df_kafka_raw = (
    spark
    .readStream
    .format("kafka")
    .options(**kafka_options)
    .option("subscribe",        "orders_topic")
    .option("startingOffsets",  "latest")      # "earliest" to replay all history
    .option("maxOffsetsPerTrigger", 10_000)    # backpressure — cap rows per batch
    .load()
)

# ── Kafka message schema ───────────────────────────────────────────
# key       binary
# value     binary    ← the actual payload, must be deserialized
# topic     string
# partition int
# offset    long
# timestamp timestamp
# timestampType int

# ── Subscribe to multiple topics ──────────────────────────────────
df_multi = (
    spark
    .readStream
    .format("kafka")
    .options(**kafka_options)
    .option("subscribePattern", "orders_.*")   # regex for topic names
    .option("startingOffsets",  "latest")
    .load()
)
```

**`startingOffsets` values:**

| Value        | Meaning                                                  |
| ------------ | -------------------------------------------------------- |
| `"latest"`   | Start at the most recent message (default for streaming) |
| `"earliest"` | Start from the very first message in the topic           |
| JSON string  | Per-partition offset: `{"topic":{"0":100,"1":200}}`      |

---

### 4.4 Deserializing Kafka Messages

Kafka messages arrive as raw bytes. You must cast to string (for JSON/Avro text) and then parse.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# ── Step 1: Define the JSON payload schema ─────────────────────────
order_payload_schema = StructType([
    StructField("order_id",     StringType(),    nullable=False),
    StructField("customer_id",  StringType(),    nullable=True),
    StructField("product_id",   StringType(),    nullable=True),
    StructField("quantity",     IntegerType(),   nullable=True),
    StructField("unit_price",   DoubleType(),    nullable=True),
    StructField("event_time",   TimestampType(), nullable=True),
    StructField("status",       StringType(),    nullable=True),
])

# ── Step 2: Cast binary key/value to string, then parse JSON ───────
df_parsed = (
    df_kafka_raw
    .select(
        F.col("key").cast("string").alias("kafka_key"),
        F.from_json(
            F.col("value").cast("string"),
            order_payload_schema
        ).alias("payload"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    .select(
        "kafka_key",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "payload.*"    # expand all nested fields from parsed JSON
    )
)

# ── Step 3: Add data quality flags ────────────────────────────────
df_validated = (
    df_parsed
    .withColumn(
        "_is_valid",
        F.col("order_id").isNotNull() &
        F.col("quantity").isNotNull() &
        (F.col("quantity") > 0) &
        F.col("unit_price").isNotNull() &
        (F.col("unit_price") > 0)
    )
)
```

---

### 4.5 Writing to Kafka

```python
# ── Write parsed/enriched data back to a Kafka topic ──────────────
df_to_publish = (
    df_validated
    .filter(F.col("_is_valid") == True)
    .withColumn("order_total", F.col("quantity") * F.col("unit_price"))
    .select(
        F.col("order_id").alias("key"),          # Kafka message key (string/binary)
        F.to_json(F.struct(
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "order_total", "status", "event_time"
        )).alias("value")                         # Kafka message value (JSON string)
    )
)

query_to_kafka = (
    df_to_publish
    .writeStream
    .format("kafka")
    .options(**kafka_options)
    .option("topic",              "orders_enriched")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/kafka_write/")
    .trigger(processingTime="30 seconds")
    .start()
)
```

---

### 4.6 End-to-End Kafka → Delta Lake Pipeline

```python
# ═══════════════════════════════════════════════════════════════════
# End-to-End Pipeline: Kafka → Bronze Delta → Silver Delta
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType, TimestampType)

# ── Schema definition ──────────────────────────────────────────────
transaction_schema = StructType([
    StructField("txn_id",      StringType(),    nullable=False),
    StructField("account_id",  StringType(),    nullable=True),
    StructField("merchant_id", StringType(),    nullable=True),
    StructField("amount",      DoubleType(),    nullable=True),
    StructField("currency",    StringType(),    nullable=True),
    StructField("txn_time",    TimestampType(), nullable=True),
    StructField("card_type",   StringType(),    nullable=True),
    StructField("status",      StringType(),    nullable=True),
])

# ── Kafka secrets from Databricks Secrets ─────────────────────────
kafka_opts = {
    "kafka.bootstrap.servers": dbutils.secrets.get("kafka", "bootstrap_servers"),
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism":    "PLAIN",
    "kafka.sasl.jaas.config":  (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{dbutils.secrets.get("kafka", "username")}" '
        f'password="{dbutils.secrets.get("kafka", "password")}";'
    ),
}

CHECKPOINT_BASE = "s3://my-bucket/checkpoints"
DELTA_BASE      = "s3://my-bucket/delta"

# ── Bronze layer — raw Kafka ingest ───────────────────────────────
df_bronze = (
    spark
    .readStream
    .format("kafka")
    .options(**kafka_opts)
    .option("subscribe",           "financial_transactions")
    .option("startingOffsets",     "latest")
    .option("maxOffsetsPerTrigger", 50_000)
    .load()
    .select(
        F.col("offset").alias("kafka_offset"),
        F.col("partition").alias("kafka_partition"),
        F.col("timestamp").alias("kafka_ingest_time"),
        F.from_json(
            F.col("value").cast("string"),
            transaction_schema
        ).alias("data")
    )
    .select("kafka_offset", "kafka_partition", "kafka_ingest_time", "data.*")
)

q_bronze = (
    df_bronze
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze_transactions/")
    .trigger(processingTime="1 minute")
    .toTable("catalog.bronze.transactions")
)

# ── Silver layer — read bronze, clean, flag anomalies ─────────────
df_silver = (
    spark
    .readStream
    .format("delta")
    .table("catalog.bronze.transactions")
    .filter(F.col("txn_id").isNotNull())
    .filter(F.col("amount") > 0)
    .withColumn(
        "is_high_value",
        F.col("amount") > 10_000
    )
    .withColumn(
        "txn_date",
        F.to_date("txn_time")
    )
    .withColumn(
        "processing_lag_seconds",
        F.unix_timestamp("kafka_ingest_time") - F.unix_timestamp("txn_time")
    )
    .dropDuplicates(["txn_id"])
)

q_silver = (
    df_silver
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver_transactions/")
    .trigger(processingTime="1 minute")
    .toTable("catalog.silver.transactions")
)

print("Streaming pipelines started.")
print(f"Bronze query ID: {q_bronze.id}")
print(f"Silver query ID: {q_silver.id}")
```

---

## 5. Stateful Streaming and Windowing

### 5.1 Tumbling Windows

**Tumbling windows** are fixed-size, non-overlapping time intervals. Each event belongs to exactly one window.

```
Tumbling window (size = 5 min):
─────┬─────┬─────┬─────┬─────┬──────▶ time
     │ W1  │ W2  │ W3  │ W4  │
     00:00  00:05  00:10  00:15  00:20
```

```python
from pyspark.sql import functions as F

df_tumbling = (
    spark
    .readStream
    .format("delta")
    .table("catalog.silver.transactions")
    .withWatermark("txn_time", "10 minutes")
    .groupBy(
        F.window("txn_time", "5 minutes").alias("txn_window"),
        "card_type"
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("txn_count"),
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
    .withColumn("window_start", F.col("txn_window.start"))
    .withColumn("window_end",   F.col("txn_window.end"))
    .drop("txn_window")
)
```

---

### 5.2 Sliding Windows

**Sliding windows** overlap. An event can belong to multiple windows. Defined by **window size** and **slide interval**.

```
Sliding window (size = 10 min, slide = 5 min):
        │◄────── 10 min ──────▶│
        W1: 00:00 – 00:10
             W2: 00:05 – 00:15
                  W3: 00:10 – 00:20
─────┬─────┬─────┬─────┬─────┬──────▶ time
     00:00  00:05  00:10  00:15  00:20
```

```python
df_sliding = (
    spark
    .readStream
    .format("delta")
    .table("catalog.silver.transactions")
    .withWatermark("txn_time", "15 minutes")
    .groupBy(
        F.window("txn_time",
                 windowDuration="10 minutes",
                 slideDuration="5 minutes").alias("txn_window"),
        "merchant_id"
    )
    .agg(
        F.sum("amount").alias("rolling_spend"),
        F.count("*").alias("txn_count"),
    )
    .withColumn("window_start", F.col("txn_window.start"))
    .withColumn("window_end",   F.col("txn_window.end"))
    .drop("txn_window")
)
```

---

### 5.3 Session Windows

**Session windows** group events by activity gap. A new session starts after a period of inactivity. Session windows have **variable length** based on the gap.

```python
# Session window: new window starts when gap > 30 minutes of inactivity
df_sessions = (
    spark
    .readStream
    .format("delta")
    .table("catalog.silver.events")
    .withWatermark("event_time", "1 hour")
    .groupBy(
        F.session_window("event_time", "30 minutes").alias("session"),
        "user_id"
    )
    .agg(
        F.count("*").alias("events_in_session"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("amount")).otherwise(0))
          .alias("session_revenue"),
        F.collect_list("event_type").alias("event_sequence"),
    )
    .withColumn("session_start",    F.col("session.start"))
    .withColumn("session_end",      F.col("session.end"))
    .withColumn(
        "session_duration_mins",
        (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")) / 60
    )
    .drop("session")
)
```

---

### 5.4 Arbitrary Stateful Processing with flatMapGroupsWithState

For complex stateful logic (e.g., a user transaction sequence that spans multiple batches), use `flatMapGroupsWithState`.

```python
from pyspark.sql.streaming.state import GroupStateTimeout
from typing import Iterator, Tuple

# ── State: running total and count per account ─────────────────────
def update_account_state(
    account_id: str,
    new_txns: Iterator,
    state          # GroupState
) -> Iterator[Tuple]:
    """
    Accumulate running total and count per account.
    Emit alert if 24h spend exceeds threshold.
    """
    SPEND_THRESHOLD = 5_000.0

    # ── Load existing state or initialize ─────────────────────────
    if state.exists:
        running_total, txn_count = state.get
    else:
        running_total, txn_count = 0.0, 0

    alerts = []
    for txn in new_txns:
        running_total += float(txn["amount"])
        txn_count     += 1

        if running_total > SPEND_THRESHOLD:
            alerts.append((account_id, running_total, txn_count, txn["txn_time"]))

    state.update((running_total, txn_count))
    state.setTimeoutDuration(86_400_000)   # expire state after 24 h

    return iter(alerts)


from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

output_schema = StructType([
    StructField("account_id",    StringType(),    True),
    StructField("running_total", DoubleType(),    True),
    StructField("txn_count",     IntegerType(),   True),
    StructField("last_txn_time", TimestampType(), True),
])

df_alerts = (
    df_silver
    .withWatermark("txn_time", "1 hour")
    .groupBy("account_id")
    .flatMapGroupsWithState(
        outputMode="append",
        timeoutConf=GroupStateTimeout.EventTimeTimeout,
        func=update_account_state,
        outputStructType=output_schema
    )
)
```

---

## 6. Real-Time Data Processing Use Cases

### 6.1 IoT Sensor Stream and Anomaly Detection

```python
# ═══════════════════════════════════════════════════════════════════
# Use Case: Manufacturing IoT — Temperature Anomaly Detection
# Source: Kafka topic "sensor_readings"
# Alert threshold: temperature > mean + 3 * stddev (per sensor type)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

sensor_schema = StructType([
    StructField("sensor_id",   StringType(),    False),
    StructField("sensor_type", StringType(),    True),
    StructField("temperature", DoubleType(),    True),
    StructField("humidity",    DoubleType(),    True),
    StructField("pressure",    DoubleType(),    True),
    StructField("read_time",   TimestampType(), True),
    StructField("plant_id",    StringType(),    True),
])

# ── Load static reference: threshold per sensor_type ──────────────
df_thresholds = spark.table("catalog.reference.sensor_thresholds")   # batch table

# ── Read sensor stream from Kafka ─────────────────────────────────
df_sensors = (
    spark.readStream.format("kafka")
    .options(**kafka_opts)
    .option("subscribe", "sensor_readings")
    .option("startingOffsets", "latest")
    .load()
    .select(F.from_json(F.col("value").cast("string"), sensor_schema).alias("s"))
    .select("s.*")
)

# ── Stream-static join: enrich with thresholds ────────────────────
df_enriched = df_sensors.join(df_thresholds, on="sensor_type", how="left")

# ── Flag anomalies ────────────────────────────────────────────────
df_anomalies = (
    df_enriched
    .withWatermark("read_time", "5 minutes")
    .withColumn(
        "is_anomaly",
        (F.col("temperature") > F.col("max_normal_temp")) |
        (F.col("temperature") < F.col("min_normal_temp"))
    )
    .filter(F.col("is_anomaly") == True)
)

# ── Write anomalies to Delta for alerting downstream ─────────────
q_anomalies = (
    df_anomalies
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/anomalies/")
    .trigger(processingTime="30 seconds")
    .toTable("catalog.monitoring.sensor_anomalies")
)
```

---

### 6.2 Clickstream Real-Time Aggregation

```python
# ═══════════════════════════════════════════════════════════════════
# Use Case: E-commerce clickstream → live dashboard aggregates
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql import functions as F

df_clicks = (
    spark.readStream.format("delta")
    .table("catalog.bronze.clickstream")
)

# ── 1-minute tumbling window: page views and add-to-cart count ────
df_live_metrics = (
    df_clicks
    .withWatermark("click_time", "2 minutes")
    .groupBy(
        F.window("click_time", "1 minute").alias("win"),
        "page_category",
        "device_type"
    )
    .agg(
        F.count("*").alias("page_views"),
        F.sum(F.when(F.col("event") == "add_to_cart", 1).otherwise(0))
          .alias("add_to_cart_count"),
        F.countDistinct("session_id").alias("active_sessions"),
        F.countDistinct("user_id").alias("unique_users"),
    )
    .withColumn("window_start", F.col("win.start"))
    .withColumn("window_end",   F.col("win.end"))
    .withColumn(
        "cart_conversion_rate",
        F.round(F.col("add_to_cart_count") / F.col("page_views"), 4)
    )
    .drop("win")
)

q_live = (
    df_live_metrics
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/live_metrics/")
    .trigger(processingTime="1 minute")
    .toTable("catalog.gold.live_page_metrics")
)
```

---

### 6.3 Change Data Capture with Kafka and Delta Lake

CDC (Change Data Capture) streams database row changes (INSERT, UPDATE, DELETE) via Kafka in a Debezium-compatible format into Delta Lake using MERGE.

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# Debezium CDC envelope schema (simplified)
cdc_schema = StructType([
    StructField("op",  StringType(), True),   # c=create, u=update, d=delete, r=read
    StructField("ts_ms", LongType(), True),   # event timestamp ms
    StructField("after",  StructType([        # new row values (null for deletes)
        StructField("customer_id",   StringType(), True),
        StructField("name",          StringType(), True),
        StructField("email",         StringType(), True),
        StructField("updated_at",    TimestampType(), True),
    ]), True),
    StructField("before", StructType([        # old row values (null for inserts)
        StructField("customer_id",   StringType(), True),
        StructField("email",         StringType(), True),
    ]), True),
])

df_cdc_raw = (
    spark.readStream.format("kafka")
    .options(**kafka_opts)
    .option("subscribe",       "db.public.customers")   # Debezium topic
    .option("startingOffsets", "earliest")
    .load()
    .select(F.from_json(F.col("value").cast("string"), cdc_schema).alias("cdc"))
    .select("cdc.*")
)

def apply_cdc_merge(batch_df, batch_id):
    """Apply CDC operations as MERGE into the Delta target table."""
    # ── Deduplicate within the batch — keep latest by ts_ms ───────
    from pyspark.sql.window import Window
    w = Window.partitionBy("customer_id").orderBy(F.col("ts_ms").desc())
    batch_deduped = (
        batch_df
        .filter(F.col("op").isin("c", "u", "d", "r"))
        .withColumn("row_num", F.row_number().over(w))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # ── Separate inserts/updates from deletes ─────────────────────
    df_upserts = batch_deduped.filter(F.col("op").isin("c", "u", "r")).select("after.*")
    df_deletes = batch_deduped.filter(F.col("op") == "d").select(F.col("before.customer_id"))

    target = DeltaTable.forName(spark, "catalog.silver.customers")

    # ── Apply upserts ─────────────────────────────────────────────
    if df_upserts.count() > 0:
        (
            target.alias("tgt")
            .merge(df_upserts.alias("src"), "tgt.customer_id = src.customer_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # ── Apply deletes ─────────────────────────────────────────────
    if df_deletes.count() > 0:
        (
            target.alias("tgt")
            .merge(df_deletes.alias("src"), "tgt.customer_id = src.customer_id")
            .whenMatchedDelete()
            .execute()
        )

q_cdc = (
    df_cdc_raw
    .writeStream
    .foreachBatch(apply_cdc_merge)
    .option("checkpointLocation", "s3://my-bucket/checkpoints/cdc_customers/")
    .trigger(processingTime="2 minutes")
    .start()
)
```

---

## 7. Monitoring and Debugging Streaming Pipelines

### 7.1 StreamingQuery Progress and Status

Every active streaming query exposes a `StreamingQuery` object with real-time metrics.

```python
# ── Get query status ───────────────────────────────────────────────
print(query.status)
# {
#   "message": "Processing new data",
#   "isDataAvailable": true,
#   "isTriggerActive": true
# }

# ── Get last batch progress metrics ───────────────────────────────
prog = query.lastProgress
print(f"Batch ID:           {prog['batchId']}")
print(f"Input rows:         {prog['numInputRows']}")
print(f"Rows/sec input:     {prog['inputRowsPerSecond']:.1f}")
print(f"Rows/sec processed: {prog['processedRowsPerSecond']:.1f}")
print(f"Trigger duration:   {prog['triggerExecution']['durationMs']} ms")

# ── Get all recent progress entries ───────────────────────────────
for p in query.recentProgress:
    print(f"Batch {p['batchId']}: "
          f"{p['numInputRows']} rows in "
          f"{p['triggerExecution']['durationMs']} ms")

# ── Wait for query termination or timeout ─────────────────────────
try:
    query.awaitTermination(timeout=300)   # wait up to 5 minutes
except Exception as e:
    print(f"Query failed: {e}")
    query.stop()

# ── List all active streaming queries ─────────────────────────────
for q in spark.streams.active:
    print(f"ID={q.id}  Name={q.name}  Status={q.status['message']}")
```

---

### 7.2 Spark UI Streaming Tab

Navigate to the **Spark UI → Structured Streaming** tab in the Databricks cluster UI to see:

- **Input rate** vs. **processing rate** graphs — lag detection
- **Batch duration** histogram — identify slow batches
- **State operator details** — rows in state, state memory, evictions
- **Source and sink details** — Kafka partition offsets, file counts

> **Tip:** If `inputRowsPerSecond` consistently exceeds `processedRowsPerSecond`, your pipeline is falling behind. Scale up your cluster or increase parallelism.

---

### 7.3 Common Failure Patterns and Fixes

| Failure Pattern           | Symptom                                          | Root Cause                                                       | Fix                                                       |
| ------------------------- | ------------------------------------------------ | ---------------------------------------------------------------- | --------------------------------------------------------- |
| **State store OOM**       | Executor OOM errors                              | Too many distinct keys in state without watermark                | Add watermark; enable RocksDB state store                 |
| **Checkpoint corruption** | `StreamingQueryException: Failed to read offset` | Checkpoint path re-used or deleted                               | Create new checkpoint path; restart query                 |
| **Late data overflows**   | Missing data in aggregates                       | Watermark too short                                              | Increase watermark duration                               |
| **Kafka lag buildup**     | Batch duration increasing                        | Processing slower than ingestion rate                            | Scale cluster; reduce `maxOffsetsPerTrigger`              |
| **Schema mismatch**       | `AnalysisException` on new columns               | Source schema changed                                            | Enable `mergeSchema`; update schema definition            |
| **Trigger missed**        | Batches infrequent                               | Trigger interval too long                                        | Shorten `processingTime`; use `AvailableNow` for catch-up |
| **Duplicate records**     | Duplicates in Delta sink                         | Multiple queries share checkpoint or foreachBatch not idempotent | One checkpoint per query; add `dropDuplicates`            |

```python
# ── Enable RocksDB state store (reduces state memory pressure) ─────
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
)

# ── Gracefully stop a query ────────────────────────────────────────
query.stop()

# ── Restart from checkpoint (recovers exactly where it left off) ───
query_restarted = (
    df_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/my_stream/")  # same path
    .trigger(processingTime="1 minute")
    .start("/delta/output/")
)
```

---

## 8. Summary

| Section                                 | Key Points                                                                                                                                                                                             |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Introduction to Real-Time Analytics** | Streaming processes events as they arrive; Structured Streaming uses the unbounded table model; Databricks unifies batch and streaming on Spark                                                        |
| **Structured Streaming Fundamentals**   | Triggers control micro-batch frequency; output modes (append/update/complete) control what rows are written; watermarks handle late-arriving data; checkpoints enable fault tolerance                  |
| **Implementing Streaming Pipelines**    | Autoloader (`cloudFiles`) is the recommended file source; stateless transforms work identically to batch; Delta Lake is the preferred sink; `foreachBatch` enables complex write logic including MERGE |
| **Kafka Integration**                   | Kafka messages arrive as binary key/value; use `from_json` to deserialize; never hardcode credentials — use Databricks Secrets; `maxOffsetsPerTrigger` provides backpressure                           |
| **Stateful Streaming & Windowing**      | Tumbling windows — non-overlapping fixed intervals; Sliding windows — overlapping; Session windows — gap-based variable length; `flatMapGroupsWithState` for arbitrary stateful logic                  |
| **Real-Time Use Cases**                 | IoT anomaly detection uses stream-static join with threshold table; Clickstream uses 1-minute tumbling windows; CDC uses `foreachBatch` + Delta MERGE to apply inserts/updates/deletes                 |
| **Monitoring & Debugging**              | `query.lastProgress` exposes per-batch metrics; Spark UI Streaming tab shows input vs. processing rate; common failures: state OOM, checkpoint corruption, schema mismatch, Kafka lag                  |

---
