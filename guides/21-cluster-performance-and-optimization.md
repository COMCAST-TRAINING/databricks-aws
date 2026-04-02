# Cluster Performance, Optimization, and Best Practices

---

## Table of Contents

1. [Cluster Performance](#1-cluster-performance)
   - 1.1 [Choosing the Right Cluster Size](#11-choosing-the-right-cluster-size)
   - 1.2 [Auto-Scaling: Configuration and Behaviour](#12-auto-scaling-configuration-and-behaviour)
   - 1.3 [Node Types: Compute-Optimized vs Memory-Optimized](#13-node-types-compute-optimized-vs-memory-optimized)
   - 1.4 [Spot Instances for Cost Efficiency](#14-spot-instances-for-cost-efficiency)
   - 1.5 [Cluster Restart Policy and Memory Leak Prevention](#15-cluster-restart-policy-and-memory-leak-prevention)
   - 1.6 [Job Clusters vs Interactive Clusters](#16-job-clusters-vs-interactive-clusters)
   - 1.7 [Enabling Spark Caching for Improved Speed](#17-enabling-spark-caching-for-improved-speed)
   - 1.8 [Instance Pools for Fast Cluster Start](#18-instance-pools-for-fast-cluster-start)
2. [Performance Optimization](#2-performance-optimization)
   - 2.1 [Columnar File Formats: Parquet and Delta](#21-columnar-file-formats-parquet-and-delta)
   - 2.2 [Partitioning Large Datasets](#22-partitioning-large-datasets)
   - 2.3 [Caching Frequently Accessed Data](#23-caching-frequently-accessed-data)
   - 2.4 [Z-Ordering for High-Cardinality Columns](#24-z-ordering-for-high-cardinality-columns)
   - 2.5 [Delta Lake Compaction with OPTIMIZE](#25-delta-lake-compaction-with-optimize)
   - 2.6 [File Pruning with VACUUM](#26-file-pruning-with-vacuum)
   - 2.7 [Data Skipping and Column Statistics](#27-data-skipping-and-column-statistics)
   - 2.8 [Broadcast Joins for Small Tables](#28-broadcast-joins-for-small-tables)
   - 2.9 [Controlling Shuffle Partitions](#29-controlling-shuffle-partitions)
   - 2.10 [Liquid Clustering (Databricks Runtime 13.3+)](#210-liquid-clustering-databricks-runtime-133)
3. [Best Practices](#3-best-practices)
   - 3.1 [Cluster Configuration Best Practices](#31-cluster-configuration-best-practices)
   - 3.2 [Data Storage and Format Best Practices](#32-data-storage-and-format-best-practices)
   - 3.3 [Query and Code Best Practices](#33-query-and-code-best-practices)
   - 3.4 [Cost Management Best Practices](#34-cost-management-best-practices)
   - 3.5 [Monitoring and Observability Best Practices](#35-monitoring-and-observability-best-practices)
   - 3.6 [Security Best Practices](#36-security-best-practices)
   - 3.7 [Development Workflow Best Practices](#37-development-workflow-best-practices)
4. [Diagnosing Performance Problems](#4-diagnosing-performance-problems)
   - 4.1 [Reading the Spark UI](#41-reading-the-spark-ui)
   - 4.2 [Identifying Data Skew](#42-identifying-data-skew)
   - 4.3 [Identifying Shuffle Bottlenecks](#43-identifying-shuffle-bottlenecks)
   - 4.4 [Common Symptoms and Fixes](#44-common-symptoms-and-fixes)
5. [Summary and Quick-Reference Checklist](#5-summary-and-quick-reference-checklist)

---

## 1. Cluster Performance

### 1.1 Choosing the Right Cluster Size

Selecting the right cluster size is the single most impactful decision for both cost and performance. An under-sized cluster causes spills and OOM errors; an over-sized cluster wastes money.

**Decision framework:**

```
┌─────────────────────────────────────────────────────────────────┐
│                  CLUSTER SIZING DECISION TREE                   │
│                                                                 │
│  What is the primary bottleneck of your workload?               │
│                                                                 │
│  CPU-bound          Memory-bound         IO-bound               │
│  (transforms,       (large joins,        (many small files,     │
│   ML training)      window functions,    shuffle-heavy ETL)     │
│       │              wide DataFrames)          │                │
│       ▼                    │                   ▼                │
│  Compute-optimised         ▼             Storage-optimised      │
│  (c5, c6i on AWS)    Memory-optimised    (i3, i4i on AWS)       │
│                      (r5, r6i on AWS)                           │
│                                                                 │
│  Start small → profile with Spark UI → scale up if needed       │
└─────────────────────────────────────────────────────────────────┘
```

**Sizing rules of thumb:**

| Data Volume    | Recommended Starting Point                  | Notes                                           |
| -------------- | ------------------------------------------- | ----------------------------------------------- |
| < 10 GB        | Single-node cluster (`m5.xlarge`)           | No need for distributed compute                 |
| 10 GB – 100 GB | 2–4 workers (`m5.2xlarge` or `r5.2xlarge`)  | Enable auto-scaling                             |
| 100 GB – 1 TB  | 4–16 workers (`r5.4xlarge` or `m5.4xlarge`) | Memory-optimised if joins or aggregations       |
| > 1 TB         | 16–64+ workers with auto-scaling            | Profile first; horizontal scale before vertical |

**Key cluster configuration fields:**

```python
# Via Databricks SDK (Python)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    ClusterSpec, AutoScale, AwsAttributes, AwsAvailability
)

w = WorkspaceClient()

cluster = w.clusters.create(
    cluster_name       = "data-eng-autoscaling",
    spark_version      = "14.3.x-scala2.12",
    node_type_id       = "r5.2xlarge",          # memory-optimised
    driver_node_type_id= "m5.xlarge",           # smaller driver is fine
    autoscale          = AutoScale(min_workers=2, max_workers=12),
    autotermination_minutes = 30,
    aws_attributes     = AwsAttributes(
        availability             = AwsAvailability.SPOT_WITH_FALLBACK,
        first_on_demand          = 1,           # driver is on-demand
        spot_bid_price_percent   = 100,
    ),
    spark_conf = {
        "spark.sql.shuffle.partitions":         "auto",
        "spark.databricks.delta.preview.enabled":"true",
        "spark.databricks.io.cache.enabled":    "true",
    },
)
print(f"Cluster ID: {cluster.cluster_id}")
```

---

### 1.2 Auto-Scaling: Configuration and Behaviour

**Auto-scaling** lets the cluster dynamically add or remove workers based on workload demand. It eliminates the need to over-provision and reduces idle cost.

```
SCALE-OUT: when pending tasks exceed available executor capacity
  for > 1 minute, Databricks requests additional nodes.

SCALE-IN:  when executor utilisation drops below a threshold
  for > 10 minutes (configurable), idle nodes are terminated.

                  ┌──────────────────────────────────┐
  Min workers ──► │░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│ ◄── Max workers
                  │                                  │
                  │  Spark scheduler pending tasks   │
                  │  ▲▲▲▲▲▲▲▲ → add nodes           │
                  │  ▼▼▼▼▼▼ → remove idle nodes      │
                  └──────────────────────────────────┘
```

**Auto-scaling via the UI:**

1. **Compute** → **Create compute** (or edit an existing cluster)
2. Under **Workers**, toggle **Enable autoscaling**
3. Set **Min workers** and **Max workers**
4. Set **Auto termination** (e.g. 30 minutes) to shut down when fully idle

**Auto-scaling limitations to be aware of:**

| Limitation                                       | Recommendation                                                         |
| ------------------------------------------------ | ---------------------------------------------------------------------- |
| Scale-out takes 2–5 min for new nodes to start   | Use **Instance Pools** (section 1.8) to pre-warm nodes                 |
| Streaming jobs scale out but rarely scale in     | Set a fixed worker count for steady-state streaming jobs               |
| Auto-scaling may thrash on bursty workloads      | Set `spark.dynamicAllocation.schedulerBacklogTimeout` = `2s` (default) |
| Delta OPTIMIZE is not parallelised by auto-scale | Run OPTIMIZE on a dedicated fixed-size cluster                         |

**Enhanced auto-scaling (Databricks proprietary):**

Databricks enhanced auto-scaling uses Spark task scheduling metrics rather than generic CPU/memory thresholds. Enable it explicitly:

```python
spark_conf = {
    "spark.databricks.aggressiveWindowDownS": "600",
    # Enhanced autoscaling is on by default for DBR 6.4+ — no explicit flag needed
}
```

---

### 1.3 Node Types: Compute-Optimized vs Memory-Optimized

```
┌──────────────────────────────────────────────────────────────────┐
│                     AWS NODE TYPE GUIDE                          │
│                                                                  │
│  GENERAL PURPOSE     COMPUTE-OPTIMIZED    MEMORY-OPTIMIZED       │
│  m5 / m6i            c5 / c6i             r5 / r6i               │
│                                                                  │
│  vCPU:Memory          vCPU:Memory          vCPU:Memory           │
│  1 : 4 GB             1 : 2 GB             1 : 8 GB              │
│                                                                  │
│  Use for:             Use for:             Use for:              │
│  • General ETL        • ML training        • Large joins         │
│  • Mixed workloads    • Feature eng.       • Window functions    │
│  • Dev/test           • CPU-bound          • Caching datasets    │
│                         transformations    • Aggregations on     │
│                       • Kafka consumers      wide tables         │
│                                                                  │
│  GPU nodes (p3, g4dn) — for deep learning and LLM workloads      │
└──────────────────────────────────────────────────────────────────┘
```

**Practical comparison on AWS:**

| Instance Family | vCPUs | Memory | Best For                          | DBU Multiplier |
| --------------- | ----- | ------ | --------------------------------- | -------------- |
| `m5.xlarge`     | 4     | 16 GB  | Light ETL, dev clusters           | 0.75×          |
| `m5.2xlarge`    | 8     | 32 GB  | Standard ETL pipelines            | 1.0×           |
| `m5.4xlarge`    | 16    | 64 GB  | Medium-scale batch processing     | 2.0×           |
| `r5.2xlarge`    | 8     | 64 GB  | Join-heavy, aggregation workloads | 1.0×           |
| `r5.4xlarge`    | 16    | 128 GB | Large in-memory analytics         | 2.0×           |
| `c5.2xlarge`    | 8     | 16 GB  | Compute-intensive ML, transforms  | 1.0×           |
| `i3.2xlarge`    | 8     | 61 GB  | NVMe local disk for shuffle       | 1.25×          |

> **Best Practice:** Default to `r5` or `r6i` family for data engineering workloads. The extra memory pays for itself by avoiding disk spills, which are far more expensive in elapsed time than the memory premium.

---

### 1.4 Spot Instances for Cost Efficiency

AWS **Spot Instances** offer up to 90% discount vs On-Demand — at the cost of potential interruption when AWS reclaims capacity. Databricks handles interruptions gracefully via **SPOT_WITH_FALLBACK** mode.

```
┌─────────────────────────────────────────────────────────────────┐
│              SPOT INSTANCE STRATEGY: SPOT_WITH_FALLBACK         │
│                                                                 │
│  Worker nodes ── SPOT (cheap, interruptible)                    │
│                                                                 │
│  If SPOT capacity unavailable → automatically falls back to     │
│  ON_DEMAND for those workers                                    │
│                                                                 │
│  Driver node ── ON_DEMAND (1 node, first_on_demand = 1)         │
│  (Driver must never be interrupted — it owns the SparkContext)  │
└─────────────────────────────────────────────────────────────────┘
```

**Recommended AWS attributes for job clusters:**

```json
"aws_attributes": {
  "availability":            "SPOT_WITH_FALLBACK",
  "zone_id":                 "us-east-1a",
  "first_on_demand":         1,
  "spot_bid_price_percent":  100,
  "ebs_volume_type":         "GENERAL_PURPOSE_SSD",
  "ebs_volume_count":        1,
  "ebs_volume_size":         100
}
```

**When NOT to use Spot:**

| Scenario                                | Recommendation                                  |
| --------------------------------------- | ----------------------------------------------- |
| Real-time streaming (always-on cluster) | ON_DEMAND workers — interruption causes lag     |
| Interactive / shared development        | ON_DEMAND — users shouldn't lose notebook state |
| Jobs with tight SLA (< 1h deadline)     | ON_DEMAND or Instance Pool with reserve nodes   |
| Jobs > 6h                               | Test interruption resilience with checkpointing |

**Multi-AZ spot for better availability:**

```json
"aws_attributes": {
  "availability": "SPOT_WITH_FALLBACK",
  "zone_id": "auto"
}
```

Setting `zone_id` to `auto` lets Databricks pick the AZ with the highest spot availability.

---

### 1.5 Cluster Restart Policy and Memory Leak Prevention

Long-running interactive clusters accumulate state in driver and executor JVM memory over days or weeks — cached objects, broadcast variables, leaked connections, and Python subprocess residue. A regular restart policy flushes these and returns the cluster to a clean state.

**Symptoms of a cluster needing a restart:**

- Slow notebook response times despite low CPU/memory metrics
- `java.lang.OutOfMemoryError: GC overhead limit exceeded` on tasks that previously ran fine
- Gradual increase in driver memory usage over days with no corresponding increase in data volume
- `BrokenPipeError` or stale Kafka connections in streaming jobs

**Configuring a scheduled restart:**

Databricks does not have a built-in "restart on schedule" option, but you can implement it with a Workflow:

```python
# notebook: restart_shared_cluster.py
# Runs as a Databricks Job — e.g. every Sunday at 02:00 UTC

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterState

w = WorkspaceClient()
CLUSTER_ID = dbutils.widgets.get("cluster_id")

# Restart (stop → start)
w.clusters.restart(cluster_id=CLUSTER_ID)

# Wait for the cluster to become RUNNING again
for _ in range(60):
    state = w.clusters.get(cluster_id=CLUSTER_ID).state
    if state == ClusterState.RUNNING:
        print(f"Cluster {CLUSTER_ID} restarted successfully.")
        break
    print(f"State: {state} — waiting 30s...")
    time.sleep(30)
else:
    raise TimeoutError(f"Cluster {CLUSTER_ID} did not come back up within 30 minutes.")
```

**Auto-termination as a lightweight restart mechanism:**

Set `autotermination_minutes` on interactive clusters. When a user is done and closes their notebook, the cluster shuts down. The next user starts a fresh cluster (aided by Instance Pools to keep start times low).

```json
"autotermination_minutes": 60
```

---

### 1.6 Job Clusters vs Interactive Clusters

| Dimension             | Job Cluster                                     | Interactive (All-Purpose) Cluster                   |
| --------------------- | ----------------------------------------------- | --------------------------------------------------- |
| **Lifecycle**         | Created at job start, terminated at job end     | Persistent — started/stopped manually or via policy |
| **Multi-user**        | Single-task isolation — no sharing              | Multiple users can attach and run concurrently      |
| **Cost**              | Lower — no idle time between job runs           | Higher — accrues DBU cost while idle                |
| **Cold-start time**   | 4–8 min without pools; < 60s with Instance Pool | N/A — already running                               |
| **Configuration**     | Defined per-job in the job spec                 | Defined once; reused across sessions                |
| **State persistence** | None — clean environment every run              | Shared global state (cached DFs, imports, widgets)  |
| **Best for**          | Production pipelines, scheduled ETL, CI/CD      | Development, exploration, collaboration             |

**Using job clusters in Workflows:**

```json
{
  "job_clusters": [
    {
      "job_cluster_key": "etl_job_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "r5.2xlarge",
        "num_workers": 4,
        "autotermination_minutes": 60,
        "aws_attributes": {
          "availability": "SPOT_WITH_FALLBACK",
          "first_on_demand": 1
        }
      }
    }
  ]
}
```

> **Best Practice:** Never run production pipelines on interactive clusters. Environmental state accumulated from interactive use can cause intermittent failures that are impossible to reproduce.

---

### 1.7 Enabling Spark Caching for Improved Speed

Databricks provides two complementary caching mechanisms:

```
┌──────────────────────────────────────────────────────────────────┐
│                    DATABRICKS CACHING LAYERS                     │
│                                                                  │
│  1. SPARK IN-MEMORY CACHE  (df.cache() / df.persist())           │
│     • Caches deserialized JVM objects in executor memory         │
│     • Survives multiple actions in the same session              │
│     • Evicted when memory pressure forces it to disk (MEMORY_AND_DISK) │
│     • Cleared on cluster restart                                 │
│                                                                  │
│  2. DATABRICKS IO CACHE  (disk-level SSD cache)                  │
│     • Caches raw Parquet / Delta data blocks on NVMe SSDs        │
│     • Transparent — no code change needed                        │
│     • Survives reattachment and multiple jobs on the same cluster│
│     • Does NOT survive cluster restart                           │
└──────────────────────────────────────────────────────────────────┘
```

**Enable the Databricks IO cache (cluster-level):**

```python
# In spark_conf when creating the cluster
spark_conf = {
    "spark.databricks.io.cache.enabled":          "true",
    "spark.databricks.io.cache.maxDiskUsage":     "200g",   # cap disk usage
    "spark.databricks.io.cache.maxMetaDataCache": "1g",
}
```

> Requires instance types with NVMe SSDs: `i3`, `i3en`, `i4i` family on AWS.

**Spark in-memory cache in notebooks:**

```python
from pyspark.storagelevel import StorageLevel

# Cache a DataFrame used multiple times in a pipeline
customers_df = spark.table("telecom.silver.customers")
customers_df.cache()        # equivalent to MEMORY_AND_DISK
customers_df.count()        # trigger the actual cache materialisation

# Or use persist() to choose the storage level explicitly
customers_df.persist(StorageLevel.MEMORY_AND_DISK_SER)   # serialized — uses less memory

# Always unpersist when done to free memory
customers_df.unpersist()
```

**Caching in SQL:**

```sql
-- Cache a table in the Spark session
CACHE TABLE telecom.silver.customers;

-- Lazy cache (cached on first access, not immediately)
CACHE LAZY TABLE telecom.silver.customers;

-- Free the cache
UNCACHE TABLE telecom.silver.customers;
```

**What to cache and what not to:**

| Cache ✓                                       | Don't cache ✗                                        |
| --------------------------------------------- | ---------------------------------------------------- |
| Dimension tables used in multiple joins       | Huge fact tables > cluster memory capacity           |
| Feature tables read in a training loop        | Tables written to immediately after being read       |
| Lookup DataFrames broadcast-joined many times | Streaming DataFrames (they are inherently unbounded) |
| Results of expensive aggregations reused      | Tables only accessed once in a pipeline              |

---

### 1.8 Instance Pools for Fast Cluster Start

**Instance Pools** maintain a pool of idle, ready-to-use cloud instances. Clusters that draw from a pool start in seconds rather than minutes by skipping the cloud provisioning step.

```
WITHOUT POOL:  Job trigger → Cloud VM request (2-4 min) → Spark start (1 min)
                                              Total: 3–5 min cold start

WITH POOL:     Job trigger → Allocate from pool (< 5 sec) → Spark start (30 sec)
                                              Total: ~35 sec cold start
```

**Creating an Instance Pool via the UI:**

1. **Compute** → **Pools** → **Create pool**
2. Set **Min idle instances** (e.g. 5) — Databricks keeps these warm at all times
3. Set **Max capacity** (e.g. 50)
4. Set **Idle instance auto-termination** (e.g. 15 min after they leave the pool)
5. Choose the instance type (e.g. `r5.2xlarge`)

**Using a pool in a job cluster config:**

```json
{
  "new_cluster": {
    "instance_pool_id": "pool-xxxxxxxxxxxx",
    "num_workers": 4,
    "spark_version": "14.3.x-scala2.12"
  }
}
```

> **Cost note:** Idle instances in a pool accrue EC2 charges but not DBU charges. Set a reasonable `idle_instance_autotermination_minutes` to avoid paying for stale warm instances overnight.

---

## 2. Performance Optimization

### 2.1 Columnar File Formats: Parquet and Delta

Columnar storage stores each column's values contiguously on disk rather than row-by-row. This dramatically reduces IO for analytical queries that read only a subset of columns.

```
ROW-BASED (CSV, JSON, Avro):           COLUMNAR (Parquet, Delta):
─────────────────────────────          ──────────────────────────────
Row 1: id=1, name=Alice, age=30        Column id:   [1, 2, 3, ...]
Row 2: id=2, name=Bob,   age=25        Column name: [Alice, Bob, ...]
Row 3: id=3, name=Carol, age=35        Column age:  [30, 25, 35, ...]

Query: SELECT avg(age) FROM t          Query: SELECT avg(age) FROM t
→ Read ALL bytes of ALL rows           → Read ONLY the age column bytes
```

**Parquet benefits:**

| Feature                   | Benefit                                                             |
| ------------------------- | ------------------------------------------------------------------- |
| Columnar projection       | Read only the columns needed — skip others entirely                 |
| Row group statistics      | `min` / `max` metadata per row group enables predicate push-down    |
| Snappy / Zstd compression | Columnar data compresses 5–10× better than uncompressed row data    |
| Dictionary encoding       | Low-cardinality columns (enums, flags) stored extremely efficiently |

**Delta Lake adds on top of Parquet:**

| Delta Feature      | Benefit                                                         |
| ------------------ | --------------------------------------------------------------- |
| Transaction log    | ACID guarantees; no more partial write corruption               |
| Data skipping      | Per-file `min`/`max` in `_delta_log` — skip whole files         |
| Z-Ordering         | Co-locate related data in fewer files for faster filtered reads |
| Schema enforcement | Reject bad data at write time                                   |
| Time travel        | Query any historical version                                    |
| MERGE / UPDATE     | Efficient upserts without full table rewrites                   |

**Write as Delta explicitly:**

```python
# Always write as Delta in a lakehouse
df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("telecom.silver.customers")
```

---

### 2.2 Partitioning Large Datasets

**Partitioning** physically organises data files into subdirectories by the value of one or more columns. Queries filtering on the partition key skip reading irrelevant partitions entirely — a technique called **partition pruning**.

```
UNPARTITIONED TABLE:                  PARTITIONED BY event_date:
s3://data/events/                     s3://data/events/
  ├── part-0001.parquet   (reads all) │  ├── event_date=2026-01-01/
  ├── part-0002.parquet               │  │    └── part-0001.parquet
  └── part-0003.parquet               │  ├── event_date=2026-01-02/
                                      │  └── event_date=2026-01-03/
                                      │       └── part-0001.parquet

Query: WHERE event_date = '2026-01-02'
→ Reads all 3 files (no pruning)      → Reads ONLY event_date=2026-01-02/
```

**Choosing a partition column:**

| Criterion                 | Good Partition Column               | Poor Partition Column                |
| ------------------------- | ----------------------------------- | ------------------------------------ |
| **Cardinality**           | Low–medium (date, region, status)   | High (user_id, order_id, timestamp)  |
| **Query filter usage**    | Almost always filtered on           | Rarely or never filtered on          |
| **Data distribution**     | Even — similar row counts per value | Skewed — some partitions 100× larger |
| **Target partition size** | 100 MB – 1 GB per partition file    | < 1 MB (too many small files)        |

**Creating a partitioned Delta table:**

```python
# Python — write with partition
df.write \
  .format("delta") \
  .partitionBy("event_date", "region") \
  .mode("overwrite") \
  .saveAsTable("telecom.gold.network_events")
```

```sql
-- SQL — DDL with partition
CREATE TABLE telecom.gold.network_events (
    event_id    BIGINT,
    customer_id STRING,
    event_type  STRING,
    data_mb     DOUBLE,
    event_date  DATE,
    region      STRING
)
USING DELTA
PARTITIONED BY (event_date, region);
```

**Verifying partition pruning in the query plan:**

```python
df = spark.table("telecom.gold.network_events") \
          .filter("event_date = '2026-01-15' AND region = 'APAC'")
df.explain(mode="formatted")
# Look for "PartitionFilters: [...]" in the scan section
```

> **Anti-pattern:** Never partition by a high-cardinality column like `customer_id`. A 10 M-customer table would create 10 M subdirectories — each with a single tiny file. This is the **small files problem** and kills both query and write performance.

---

### 2.3 Caching Frequently Accessed Data

Refer to [Section 1.7](#17-enabling-spark-caching-for-improved-speed) for the full caching reference. Additional patterns for pipelines:

**Pattern: Cache a shared lookup table at the start of a notebook:**

```python
# At the start of a complex multi-join notebook
region_map_df = spark.table("telecom.silver.region_mapping")
product_df    = spark.table("telecom.silver.products")

region_map_df.cache()
product_df.cache()

# Trigger materialisation up-front
region_map_df.count()
product_df.count()

# --- rest of pipeline uses these DataFrames many times ---
```

**Pattern: Checkpoint to break long lineage chains:**

When a DataFrame has hundreds of transformation steps, Spark will recompute the entire chain on failure. `checkpoint()` saves a snapshot to HDFS/S3 and breaks the lineage.

```python
spark.sparkContext.setCheckpointDir("s3://acme-checkpoints/spark/")

# After 50+ chained transformations, checkpoint to cut the lineage
df_checkpoint = df_heavy_transforms.checkpoint()
df_checkpoint.count()   # materialise
```

---

### 2.4 Z-Ordering for High-Cardinality Columns

**Z-Ordering** is a data skipping technique that co-locates rows with similar values of a column into the same Parquet files. Unlike partitioning (which creates subdirectories), Z-Ordering works within existing files and is suitable for **high-to-medium cardinality** columns.

```
WITHOUT Z-ORDER on customer_id:        WITH Z-ORDER BY customer_id:
─────────────────────────────────      ───────────────────────────────────
File 1: customer_ids → mixed           File 1: customer_ids → 1 – 250,000
         (1, 99, 1005, 200500, ...)     File 2: customer_ids → 250,001 – 500,000
File 2: customer_ids → mixed           File 3: customer_ids → 500,001 – 750,000
         (3, 450, 9000, 3000000, ...)

Query: WHERE customer_id = 182000
→ Must read ALL files (no stats match) → Read only File 1 (stats: 1–250,000)
```

**Running Z-ORDER optimization:**

```sql
-- OPTIMIZE + ZORDER in one command (runs during a maintenance window)
OPTIMIZE telecom.gold.network_events
ZORDER BY (customer_id, event_type);
```

```python
# Python equivalent
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "telecom.gold.network_events")
dt.optimize().executeZOrderBy("customer_id", "event_type")
```

**Z-ORDER usage rules:**

| Rule                                                  | Reason                                                       |
| ----------------------------------------------------- | ------------------------------------------------------------ |
| Z-ORDER only works on Delta tables                    | Requires Delta transaction log for file statistics           |
| Limit to 1–4 columns                                  | Effectiveness decreases sharply with more columns            |
| Choose columns frequently used in `WHERE` / `JOIN ON` | Z-ORDER improves read performance only for filtered queries  |
| Do not Z-ORDER the partition column                   | Already handled by partition pruning — no additional benefit |
| Re-run after large writes (e.g. weekly MERGE)         | New files from writes are not automatically Z-Ordered        |

---

### 2.5 Delta Lake Compaction with OPTIMIZE

Delta Lake accumulates many small Parquet files over time — each streaming micro-batch, each incremental `MERGE`, and each `INSERT` writes a new file. Small files degrade read performance because the driver must open thousands of file handles.

**The small files problem:**

```
After 30 days of hourly streaming writes (24 × 30 = 720 micro-batches):
─────────────────────────────────────────────────────────────────────
s3://data/events/_delta_log/      (transaction log)
s3://data/events/
  ├── part-00001.parquet   2.1 MB   ← micro-batch 1
  ├── part-00002.parquet   2.3 MB   ← micro-batch 2
  ...
  └── part-00720.parquet   1.9 MB   ← micro-batch 720

Total: 720 files × 2 MB = ~1.4 GB stored in 720 tiny files
Read a month of data → 720 file opens, metadata reads, and seeks

AFTER OPTIMIZE:
  └── part-00001-compacted.parquet   1.4 GB  (target 1 file per partition)
→ 1 file → minimal overhead
```

**Running OPTIMIZE:**

```sql
-- Compact the entire table
OPTIMIZE telecom.gold.network_events;

-- Compact only a specific partition (faster, targeted)
OPTIMIZE telecom.gold.network_events
WHERE event_date = '2026-04-01';

-- Compact + Z-Order together in one pass
OPTIMIZE telecom.gold.network_events
ZORDER BY (customer_id);
```

**Scheduling OPTIMIZE as part of a maintenance workflow:**

```python
# maintenance_optimize.py — runs daily at 01:00 UTC as a Databricks Job task
from delta.tables import DeltaTable

TABLES_TO_OPTIMIZE = [
    ("telecom.gold.network_events",   ["customer_id"]),
    ("telecom.gold.churn_features",   ["customer_id"]),
    ("telecom.silver.customers",       None),
]

for table_name, zorder_cols in TABLES_TO_OPTIMIZE:
    dt = DeltaTable.forName(spark, table_name)
    if zorder_cols:
        dt.optimize().executeZOrderBy(*zorder_cols)
    else:
        dt.optimize().executeCompaction()
    print(f"OPTIMIZE complete: {table_name}")
```

**Target file size:**

```sql
-- Default target is 1 GB per file (configurable)
ALTER TABLE telecom.gold.network_events
SET TBLPROPERTIES ('delta.targetFileSize' = '134217728');  -- 128 MB
```

---

### 2.6 File Pruning with VACUUM

`VACUUM` removes **old data files no longer referenced** by the Delta transaction log. Without it, replaced files from `UPDATE`, `DELETE`, `MERGE`, and `OPTIMIZE` accumulate indefinitely.

```
BEFORE VACUUM:                        AFTER VACUUM (retentionHours=168):
──────────────────────────────────    ──────────────────────────────────────
_delta_log/  (current + history)      _delta_log/  (unchanged)
part-00001.parquet  ← current v5      part-00001.parquet  ← current v5
part-00001-old.parquet ← pre-merge    [deleted]
part-00002.parquet  ← current v5      part-00002.parquet  ← current v5
part-very-old.parquet ← v1            [deleted — older than 7 days]
```

**Running VACUUM:**

```sql
-- Default retention: 7 days (168 hours)
VACUUM telecom.gold.network_events;

-- Specify retention explicitly (minimum 168h for time travel safety)
VACUUM telecom.gold.network_events RETAIN 168 HOURS;

-- Dry run — preview what would be deleted without deleting
VACUUM telecom.gold.network_events DRY RUN;
```

> **Warning:** Running `VACUUM` with a retention period shorter than 7 days disables time travel to versions within that window. Never set retention below your time-travel requirement.

```python
# Python equivalent with Delta API
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "telecom.gold.network_events")
dt.vacuum(retentionHours=168)
```

---

### 2.7 Data Skipping and Column Statistics

Delta Lake automatically collects **min**, **max**, **null count**, and (for strings) **distinct count** statistics for the first 32 columns of every new file. The query engine uses these to skip entire files that cannot contain matching rows.

```
Delta file statistics per Parquet file:
────────────────────────────────────────
file: part-00001.parquet
  stats:
    customer_id: {min: "C-00001", max: "C-10000", nullCount: 0}
    age:         {min: 18,        max: 72,         nullCount: 12}
    churned:     {min: 0,         max: 1,           nullCount: 0}

Query: WHERE customer_id = 'C-50000'
→ min='C-00001', max='C-10000' → 'C-50000' is outside this range
→ SKIP this file entirely — no IO
```

**Confirming data skipping is working:**

```sql
-- Check the query, look for "numFilesSkipped" in the Spark UI or the scan stats
EXPLAIN EXTENDED
SELECT * FROM telecom.gold.network_events
WHERE customer_id = 'C-50000';
```

**Column statistics are auto-collected on write.** To manually refresh them:

```sql
ANALYZE TABLE telecom.gold.network_events COMPUTE STATISTICS FOR ALL COLUMNS;
```

**Increase the number of columns tracked (default: 32):**

```sql
ALTER TABLE telecom.gold.network_events
SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '50');
```

---

### 2.8 Broadcast Joins for Small Tables

When joining a large table to a small table (e.g. a dimension table), Spark can **broadcast** the small table to every executor, eliminating the expensive shuffle of the large table.

```
SHUFFLE JOIN (default when both tables are large):
  Large table (10 GB)  ──shuffle──► partitioned by join key across all executors
  Small table (50 MB)  ──shuffle──► partitioned by join key across all executors
  Cost: network shuffle of 10+ GB

BROADCAST JOIN (when one table fits in executor memory):
  Large table (10 GB)  ── stays local on each executor
  Small table (50 MB)  ── broadcast to ALL executors (50 MB × N executors)
  Cost: 50 MB × N broadcast (much cheaper)
```

**Automatic broadcast:**

Spark auto-broadcasts tables below `spark.sql.autoBroadcastJoinThreshold` (default: **10 MB**).

```python
# Raise the threshold to 100 MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))
```

**Explicit broadcast hint:**

```python
from pyspark.sql.functions import broadcast

result = (
    large_events_df
    .join(broadcast(region_map_df), on="region_code", how="left")
)
```

```sql
-- SQL broadcast hint
SELECT /*+ BROADCAST(r) */ e.*, r.region_name
FROM   telecom.gold.network_events  e
JOIN   telecom.silver.region_map    r  ON e.region_code = r.code;
```

> **Do not broadcast a table > 500 MB.** The broadcast is replicated to every executor simultaneously, which can overwhelm the driver and network.

---

### 2.9 Controlling Shuffle Partitions

A **shuffle** is caused by operations that redistribute data across executors: `groupBy`, `join`, `orderBy`, `distinct`, `repartition`. The number of output partitions after a shuffle is controlled by `spark.sql.shuffle.partitions`.

**Default: 200 partitions** — a legacy value that is:

- Too high for small datasets → 200 tiny tasks with high scheduling overhead
- Too low for large datasets → 200 over-sized partitions causing executor OOM

**Adaptive Query Execution (AQE):**

Databricks Runtime defaults AQE to **on**, which auto-adjusts shuffle partitions at runtime.

```python
# Verify AQE is enabled (default for DBR 7.0+)
spark.conf.get("spark.sql.adaptive.enabled")   # should be "true"

# AQE will coalesce shuffle partitions dynamically
# You can still set the initial target:
spark.conf.set("spark.sql.shuffle.partitions", "auto")   # DBR 10+ auto mode
```

**Manual tuning formula (when AQE is off or you need predictability):**

$$\text{shuffle partitions} = \frac{\text{total shuffled data (bytes)}}{128 \text{ MB per partition}}$$

```python
# Example: joining two tables totalling 20 GB of shuffled data
# target_partition_size = 128 MB
# shuffle_partitions = 20,000 MB / 128 MB ≈ 156 → round to 200

spark.conf.set("spark.sql.shuffle.partitions", "200")
```

---

### 2.10 Liquid Clustering (Databricks Runtime 13.3+)

**Liquid Clustering** is the next-generation alternative to static partitioning and Z-Ordering for Delta tables, introduced in DBR 13.3. It automatically co-locates data and incrementally re-clusters as new data arrives — eliminating the need to run `OPTIMIZE` with `ZORDER BY` on a schedule.

```
STATIC PARTITIONING + Z-ORDER:              LIQUID CLUSTERING:
────────────────────────────────────────    ─────────────────────────────────────
Partition chosen at table creation time     Clustering columns changeable anytime
Must re-partition to change layout          Automatic incremental clustering
Manual OPTIMIZE runs needed                 Auto-triggered during writes
Poor fit for multi-dimensional queries      Excellent for multi-dimensional queries
```

**Creating a table with Liquid Clustering:**

```sql
CREATE TABLE telecom.gold.network_events_lc (
    event_id    BIGINT,
    customer_id STRING,
    event_type  STRING,
    data_mb     DOUBLE,
    event_date  DATE
)
USING DELTA
CLUSTER BY (customer_id, event_date);   -- replaces PARTITIONED BY + ZORDER
```

**Enabling on an existing table:**

```sql
ALTER TABLE telecom.gold.network_events
CLUSTER BY (customer_id, event_date);
```

**Trigger an explicit clustering pass (like OPTIMIZE):**

```sql
OPTIMIZE telecom.gold.network_events_lc;
-- Liquid clustering is triggered automatically; explicit OPTIMIZE is optional
```

> **When to use Liquid Clustering over Partitioning:** Use Liquid Clustering for most new tables on DBR 13.3+. Use static partitioning only for tables where you always filter by a well-defined, low-cardinality column (e.g. `event_date`) and you need backward compatibility with non-Databricks engines.

---

## 3. Best Practices

### 3.1 Cluster Configuration Best Practices

| #   | Practice                                                             | Why                                                                             |
| --- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| 1   | Use **Job Clusters** for production pipelines                        | Clean environment, no shared-state bugs, exact cost attribution                 |
| 2   | Use **Interactive Clusters** only for dev/test                       | Shared state useful for iteration; too expensive for production always-on       |
| 3   | Set **auto-termination** on all interactive clusters                 | Idle cluster = idle cost; 30–60 min is a safe default                           |
| 4   | Attach all clusters to an **Instance Pool**                          | Reduces cold-start time from 5 min to < 1 min                                   |
| 5   | Put the **driver on On-Demand** (`first_on_demand=1`)                | Driver interruption kills the SparkContext and loses all in-progress work       |
| 6   | Run **workers on Spot** with `SPOT_WITH_FALLBACK`                    | Up to 90% cost saving with automatic On-Demand fallback                         |
| 7   | Use **memory-optimised instances** (`r5`/`r6i`) for data engineering | Prevents disk spills that cost 10–100× in elapsed time vs memory                |
| 8   | Apply a **Cluster Policy** in multi-user workspaces                  | Prevent users from accidentally spinning up oversized or misconfigured clusters |
| 9   | Enable **IO Cache** on Delta-heavy clusters                          | Transparent SSD caching; requires NVMe instance types                           |
| 10  | **Restart shared clusters weekly** (or use auto-terminate + pool)    | Prevent JVM memory leaks from accumulating over days of interactive use         |

---

### 3.2 Data Storage and Format Best Practices

| #   | Practice                                                   | Why                                                                  |
| --- | ---------------------------------------------------------- | -------------------------------------------------------------------- |
| 1   | **Always use Delta format** for tables                     | ACID, data skipping, time travel, schema enforcement                 |
| 2   | **Never write raw CSV or JSON to production tables**       | No schema enforcement, no statistics, catastrophically slow at scale |
| 3   | **Partition by a low-cardinality date/region column**      | Enables partition pruning; 100 MB–1 GB per file is the sweet spot    |
| 4   | **Run `OPTIMIZE` + `ZORDER` on a daily/weekly schedule**   | Combats small-files problem; improves data skipping effectiveness    |
| 5   | **Run `VACUUM` weekly** with `RETAIN 168 HOURS`            | Frees S3 storage consumed by old, replaced files                     |
| 6   | **Avoid over-partitioning** (never partition by `user_id`) | Creates millions of tiny files; kills metadata performance           |
| 7   | **Use Liquid Clustering** on new tables (DBR 13.3+)        | Eliminates manual OPTIMIZE + ZORDER schedule for most workloads      |
| 8   | **Set target file size** to 128 MB – 1 GB per file         | Aligns with Parquet row-group read efficiency and S3 request costs   |
| 9   | **Enable column statistics** for your filter columns       | Required for effective data skipping beyond the first 32 columns     |
| 10  | **Use managed tables** in Unity Catalog                    | Databricks manages lifecycle, permissions, and storage credentials   |

---

### 3.3 Query and Code Best Practices

| #   | Practice                                                 | Why                                                                     |
| --- | -------------------------------------------------------- | ----------------------------------------------------------------------- |
| 1   | **Filter early** (`filter` / `WHERE` before joins)       | Reduces data shuffled in expensive joins                                |
| 2   | **Project early** (`select` only required columns)       | Reduces bytes read from columnar files and shuffled across network      |
| 3   | **Broadcast small dimension tables**                     | Eliminates shuffle of the large fact table                              |
| 4   | **Avoid `SELECT *` in production queries**               | Reads all columns unnecessarily; defeats columnar projection            |
| 5   | **Use `spark.sql.adaptive.enabled = true`** (default)    | AQE auto-optimises joins, coalesces partitions, handles skew at runtime |
| 6   | **Avoid UDFs where built-in Spark functions exist**      | Python UDFs cross the JVM–Python boundary; 10–100× slower than SQL      |
| 7   | **Use vectorised pandas UDFs** when a UDF is unavoidable | Arrow-serialized batch processing is ~10× faster than row-by-row UDFs   |
| 8   | **Avoid `df.collect()` on large DataFrames**             | Moves all data to the driver; causes OOM for > driver-memory datasets   |
| 9   | **Cache DataFrames used multiple times** in the same job | Prevents redundant re-computation and repeated S3 reads                 |
| 10  | **Checkpoint long transformation chains** (> 50 steps)   | Breaks Spark lineage to prevent full recomputation on task failure      |

**Example: filter and project early:**

```python
# BAD — reads all columns, then filters late
result = spark.table("telecom.gold.network_events") \
              .join(customers_df, "customer_id") \
              .filter("region = 'APAC' AND event_date >= '2026-01-01'") \
              .select("customer_id", "data_mb", "event_type")

# GOOD — filter and project BEFORE the join
events_filtered = (
    spark.table("telecom.gold.network_events")
    .filter("region = 'APAC' AND event_date >= '2026-01-01'")
    .select("customer_id", "data_mb", "event_type")         # project early
)

result = events_filtered.join(
    customers_df.select("customer_id", "segment"),           # project small table too
    on="customer_id",
    how="inner"
)
```

**Vectorised pandas UDF example:**

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Vectorised (Arrow-based) — processes a whole column at once
@pandas_udf("double")
def normalise_data_mb(series: pd.Series) -> pd.Series:
    return (series - series.mean()) / series.std()

result = df.withColumn("normalised_data_mb", normalise_data_mb("data_mb"))
```

---

### 3.4 Cost Management Best Practices

| #   | Practice                                           | Impact                                                                  |
| --- | -------------------------------------------------- | ----------------------------------------------------------------------- |
| 1   | Use **Spot Instances for workers**                 | 50–90% compute cost reduction                                           |
| 2   | Enable **auto-termination** on all clusters        | Zero idle DBU cost after workload ends                                  |
| 3   | Use **Job Clusters** instead of interactive        | No pre-warming cost; exact ephemeral billing                            |
| 4   | Apply **Cluster Policies** with `maxWorkers` limit | Prevent accidental oversized clusters from uncontrolled cost spikes     |
| 5   | Use **SQL Warehouses for BI queries**              | Serverless SQL Warehouse has zero idle cost; shares across queries      |
| 6   | Tag all clusters with `team`, `project`, `env`     | Enables cost attribution in the Databricks Cost Monitoring dashboard    |
| 7   | Schedule **OPTIMIZE + VACUUM** after off-peak      | Avoid running maintenance during peak hours on expensive clusters       |
| 8   | Right-size with **auto-scaling**                   | Matches compute to actual demand; eliminates fixed over-provisioning    |
| 9   | Use **Photon** for SQL-heavy workloads             | Native vectorised query engine; can reduce cluster requirements by 2–5× |
| 10  | Review **Job Run History** weekly                  | Identify unexpectedly long-running or repeatedly-failing jobs           |

---

### 3.5 Monitoring and Observability Best Practices

**Key monitoring surfaces in Databricks:**

```
┌──────────────────────────────────────────────────────────────────┐
│                  DATABRICKS MONITORING STACK                     │
│                                                                  │
│  Spark UI          → Task, stage, executor, shuffle metrics      │
│  Ganglia / Metrics → Cluster-level CPU, memory, disk, network    │
│  Job Run History   → Run duration, success/failure rate, trends  │
│  Delta History     → Table-level write audit + operation metrics  │
│  System Tables     → Query history, cluster events (Unity Cat.)  │
│  Databricks Alerts → Email/webhook on job failure or slow runtime│
└──────────────────────────────────────────────────────────────────┘
```

| #   | Practice                                                                    |
| --- | --------------------------------------------------------------------------- |
| 1   | Enable **job email notifications** on failure and on long duration          |
| 2   | Monitor **Spark UI → Stages** after each job run — check for skewed tasks   |
| 3   | Use **`DESCRIBE HISTORY`** to track table writes, OPTIMIZE runs, and VACUUM |
| 4   | Instrument pipelines with **structured logging** (`print(json.dumps(...))`) |
| 5   | Query `system.lakeflow.job_run_timeline` (Unity Catalog) for job analytics  |
| 6   | Set **SLA alerts** via Databricks Workflows (fail if run > N minutes)       |
| 7   | Track **file counts per Delta table** weekly to detect small-file growth    |

**Checking file count for a Delta table:**

```sql
-- Approximate file count (fast — uses Delta log stats)
DESCRIBE DETAIL telecom.gold.network_events;
-- Look for: numFiles column

-- Exact file distribution per partition
SELECT   event_date, COUNT(*) AS file_count
FROM     (
  SELECT   element_at(split(path, '/'), -2) AS event_date
  FROM     telecom.gold.network_events
  -- Note: uses internal Delta table function — use with care in production
)
GROUP BY event_date
ORDER BY file_count DESC;
```

---

### 3.6 Security Best Practices

| #   | Practice                                                               | Why                                                                 |
| --- | ---------------------------------------------------------------------- | ------------------------------------------------------------------- |
| 1   | **Never hard-code credentials** in notebooks or scripts                | Secrets in code → secrets in Git → credentials exposed              |
| 2   | Use **Databricks Secrets** (`dbutils.secrets.get`)                     | Secrets are redacted in logs and never printed                      |
| 3   | Use **Service Principals** for all automation                          | Human PATs tied to employees who may leave; SPs have explicit scope |
| 4   | Set **token expiry** to ≤ 90 days for PATs                             | Limits exposure window if a PAT is inadvertently leaked             |
| 5   | Enable **Unity Catalog** for all data access                           | Centralised RBAC, column masking, row filters, and audit logging    |
| 6   | Grant **minimum required permissions** (least privilege)               | `CAN_READ` only for analysts; `CAN_MANAGE_RUN` only for automation  |
| 7   | Enable **audit logging** to S3 via `system.access.audit`               | Required for compliance (SOC 2, GDPR, HIPAA)                        |
| 8   | Use **IP access lists** to restrict workspace access by CIDR           | Prevent credential use from unexpected IPs                          |
| 9   | Enable **VPC endpoints** (PrivateLink) for workspace traffic           | Eliminate public internet exposure for data plane traffic           |
| 10  | **Rotate secrets** regularly and use AWS Secrets Manager auto-rotation | Reduce exposure from any compromised credential                     |

---

### 3.7 Development Workflow Best Practices

| #   | Practice                                                                                    |
| --- | ------------------------------------------------------------------------------------------- |
| 1   | **Use Databricks Repos** — back all code with Git from day one                              |
| 2   | **One notebook = one task** — avoid monolithic 2000-line notebooks                          |
| 3   | **Use `%run` or `import`** to share utility functions across notebooks                      |
| 4   | **Parameterise notebooks with widgets** — never hard-code paths or dates                    |
| 5   | **Write unit tests** for helper functions in `src/` using `pytest`                          |
| 6   | **Use `dbutils.notebook.run()`** for orchestrated sub-notebook calls                        |
| 7   | **Document data contracts** with table comments and column descriptions                     |
| 8   | **Lint notebooks** with `flake8` / `ruff` in CI before merging                              |
| 9   | **Merge only to `main` via PR** — never push directly to production branches                |
| 10  | **Use environment-specific catalogs** (`dev_catalog`, `prod_catalog`) to isolate workspaces |

**Parameterising a notebook with widgets:**

```python
# At the top of every parameterised notebook
dbutils.widgets.text("run_date",    defaultValue="2026-04-01", label="Processing date")
dbutils.widgets.text("environment", defaultValue="dev",        label="Environment")

RUN_DATE    = dbutils.widgets.get("run_date")
ENVIRONMENT = dbutils.widgets.get("environment")

TARGET_TABLE = f"{ENVIRONMENT}_catalog.gold.churn_features"

print(f"Running for date={RUN_DATE}, env={ENVIRONMENT}, target={TARGET_TABLE}")
```

---

## 4. Diagnosing Performance Problems

### 4.1 Reading the Spark UI

The **Spark UI** (accessible from the cluster's **Spark UI** tab or during a job run) is the primary tool for performance diagnosis.

```
┌──────────────────────────────────────────────────────────────────┐
│                         SPARK UI TABS                            │
│                                                                  │
│  Jobs      → Timeline of all Spark jobs triggered in the session │
│  Stages    → Each stage's tasks: duration, GC, shuffle metrics   │
│  Storage   → Cached RDDs/DataFrames with memory usage            │
│  Executors → Per-executor: tasks, GC time, input/shuffle bytes   │
│  SQL       → Visual DAG of each SQL/DataFrame query with metrics  │
│  Streaming → Micro-batch processing times (for streaming queries) │
└──────────────────────────────────────────────────────────────────┘
```

**Key metrics to look at for each job:**

| Metric                       | Healthy Value             | Warning Sign                                                       |
| ---------------------------- | ------------------------- | ------------------------------------------------------------------ |
| **Task duration spread**     | All tasks ≈ same time     | Some tasks 10× longer → data skew                                  |
| **Shuffle read/write bytes** | Minimal relative to input | High shuffle → look for broadcast opportunity                      |
| **GC time**                  | < 10% of task time        | > 20% → memory pressure, increase executor memory                  |
| **Spill (memory)**           | 0                         | Any spill → OOM risk, increase memory or reduce shuffle partitions |
| **Input bytes vs records**   | Consistent ratio          | Very small records → small files problem                           |

---

### 4.2 Identifying Data Skew

**Data skew** occurs when the distribution of data across partitions is uneven — a few tasks process 100× more data than others, becoming the bottleneck that holds up the entire stage.

**How to spot skew in Spark UI:**

- Go to **Stages** → click the slow stage → look at the **task duration distribution**.
- If the 75th percentile task is 10× slower than the median, skew is the likely cause.
- Check **Shuffle Read Size** per task — skewed partitions show an outlier in bytes.

**Common skew causes:**

| Cause                                       | Example                                   |
| ------------------------------------------- | ----------------------------------------- |
| Join key has null values                    | `LEFT JOIN` where many rows have null key |
| Highly skewed categorical column            | 90% of orders from one customer_id        |
| Hot partition in partitioned table          | Peak-day data 10× larger than typical day |
| Streaming source with imbalanced partitions | One Kafka partition has 5× more messages  |

**Fixing skew — salting technique:**

```python
import random
from pyspark.sql import functions as F

SALT_FACTOR = 10

# Add a random salt to the skewed join key
large_df = large_df.withColumn(
    "salted_key",
    F.concat(F.col("customer_id"), F.lit("_"), (F.rand() * SALT_FACTOR).cast("int").cast("string"))
)

# Explode the small table to match all salt values
from pyspark.sql.functions import array, explode, lit

small_df = small_df.withColumn(
    "salt_range", array([lit(i) for i in range(SALT_FACTOR)])
).withColumn("salt", explode("salt_range")) \
 .withColumn(
    "salted_key",
    F.concat(F.col("customer_id"), F.lit("_"), F.col("salt").cast("string"))
).drop("salt_range", "salt")

result = large_df.join(small_df, on="salted_key", how="inner")
```

**AQE skew join handling (automatic):**

With AQE enabled, Spark detects skewed partitions at runtime and splits them automatically — no code change needed.

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")    # default: true
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", str(256 * 1024 * 1024))
```

---

### 4.3 Identifying Shuffle Bottlenecks

Shuffles are unavoidable for group-by and join operations but can be minimised.

**Signs of a shuffle bottleneck in Spark UI:**

- **Stage**: large "Shuffle Write" and "Shuffle Read" byte counts
- **Executors**: high "Shuffle Read Blocked Time"
- **Timeline**: tasks in the next stage start late (waiting for shuffle data)

**Strategies to reduce shuffle:**

| Strategy                                 | When to Apply                                    |
| ---------------------------------------- | ------------------------------------------------ |
| Broadcast join                           | One side of join < 100 MB                        |
| Pre-partition data by the join key       | Repeated joins on the same key in a pipeline     |
| Filter before join to reduce data volume | Applying predicates after the join               |
| Bucket tables on the join key            | Very frequent joins on the same large table pair |

**Bucketing to pre-partition data for joins:**

```sql
-- Write the table bucketed by customer_id (one-time cost)
CREATE TABLE telecom.gold.events_bucketed
USING DELTA
CLUSTERED BY (customer_id) INTO 256 BUCKETS
AS SELECT * FROM telecom.gold.network_events;

-- Subsequent joins on customer_id skip the shuffle stage
SELECT e.*, c.segment
FROM   telecom.gold.events_bucketed e
JOIN   telecom.silver.customers      c  ON e.customer_id = c.customer_id;
```

---

### 4.4 Common Symptoms and Fixes

| Symptom                               | Root Cause                                  | Fix                                                                |
| ------------------------------------- | ------------------------------------------- | ------------------------------------------------------------------ |
| Job runs correctly once then OOM      | Data volume grew; partition count unchanged | Increase shuffle partitions; add memory-optimised nodes            |
| Tasks spill to disk                   | Not enough executor memory                  | Use `r5`/`r6i` nodes; increase `spark.executor.memory`             |
| First task of a stage infinitely slow | Data skew on join/group key                 | Enable AQE skew join handling; use salting                         |
| Slow queries on a table over time     | Small files accumulation                    | Run `OPTIMIZE` (and `ZORDER`)                                      |
| S3 storage cost keeps growing         | Old Delta files not cleaned up              | Run `VACUUM RETAIN 168 HOURS`                                      |
| Cluster takes 5+ min to start         | No Instance Pool configured                 | Create an Instance Pool and attach job clusters to it              |
| Interactive cluster OOM after weeks   | JVM heap fragmentation / memory leaks       | Restart the cluster; enable auto-termination                       |
| `DataFrame.collect()` hangs           | DataFrame too large for driver memory       | Use `limit()` + `toPandas()` for samples; use Delta table directly |
| `GC overhead exceeded` error          | Too many small objects in executor JVM      | Use `MEMORY_AND_DISK_SER` storage; increase executor memory        |
| Stage takes 3× longer after MERGE     | Optimistic concurrency conflict retries     | Reduce writer concurrency; use partitioned writes                  |

---

## 5. Summary and Quick-Reference Checklist

### Performance Checklist

Use this checklist when reviewing a new pipeline or diagnosing a slow job:

**Cluster Setup:**

- [ ] Job clusters used for all production pipelines (not interactive clusters)
- [ ] Workers on Spot (`SPOT_WITH_FALLBACK`); driver on On-Demand (`first_on_demand=1`)
- [ ] Auto-scaling enabled with appropriate min/max workers
- [ ] Auto-termination set to 30–60 minutes on interactive clusters
- [ ] Instance Pool attached for fast cold starts
- [ ] Memory-optimised node type for data engineering (joins, aggregations)
- [ ] IO Cache enabled (`spark.databricks.io.cache.enabled=true`) if using NVMe instances

**Storage and Format:**

- [ ] All tables stored as Delta (not Parquet, CSV, or JSON)
- [ ] Partitioned by a low-cardinality date or region column
- [ ] `OPTIMIZE` + `ZORDER` scheduled daily or weekly
- [ ] `VACUUM RETAIN 168 HOURS` scheduled weekly
- [ ] Liquid Clustering enabled for new tables on DBR 13.3+
- [ ] Target file size set to 128 MB – 1 GB per file

**Query Optimization:**

- [ ] Filters applied early (before joins and aggregations)
- [ ] Only required columns selected (no `SELECT *` in production)
- [ ] Small tables broadcast-joined (`broadcast()` hint or threshold raised)
- [ ] AQE enabled (`spark.sql.adaptive.enabled=true`)
- [ ] Shuffle partitions set to `auto` or explicitly sized to data volume
- [ ] Frequently used DataFrames cached with `.cache()` and unpersisted after use
- [ ] Python UDFs replaced with native Spark SQL functions where possible

**Observability:**

- [ ] Job email/webhook notification on failure
- [ ] Spark UI checked after first run of a new pipeline
- [ ] No task duration outliers in the Stages view (skew check)
- [ ] Spill metrics are zero in the Executors view
- [ ] Table file count monitored weekly (`DESCRIBE DETAIL`)

---

### Key Configuration Reference

```python
# spark_conf recommended baseline for data engineering clusters
spark_conf = {
    # Adaptive Query Execution
    "spark.sql.adaptive.enabled":                      "true",
    "spark.sql.adaptive.coalescePartitions.enabled":   "true",
    "spark.sql.adaptive.skewJoin.enabled":             "true",
    "spark.sql.shuffle.partitions":                    "auto",

    # Delta
    "spark.databricks.delta.preview.enabled":          "true",
    "spark.databricks.delta.optimizeWrite.enabled":    "true",  # auto-compact on write
    "spark.databricks.delta.autoCompact.enabled":      "true",  # auto-compact small files

    # IO Cache (requires NVMe instance — i3/i4i families)
    "spark.databricks.io.cache.enabled":               "true",
    "spark.databricks.io.cache.maxDiskUsage":          "200g",

    # Broadcast join threshold
    "spark.sql.autoBroadcastJoinThreshold":            str(100 * 1024 * 1024),  # 100 MB
}
```

---

**Further Reading:**

- [Databricks Cluster Configuration Guide](https://docs.databricks.com/clusters/configure.html)
- [Delta Lake Optimization Guide](https://docs.databricks.com/delta/optimizations/index.html)
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
- [Databricks Photon](https://docs.databricks.com/runtime/photon.html)
- [Liquid Clustering](https://docs.databricks.com/delta/clustering.html)
- [Adaptive Query Execution](https://docs.databricks.com/optimizations/aqe.html)
