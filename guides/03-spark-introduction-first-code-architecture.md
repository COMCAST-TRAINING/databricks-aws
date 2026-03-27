# Introduction to Databricks with Apache Spark

---

## Table of Contents

1. [Databricks-Powered Apache Spark](#1-databricks-powered-apache-spark)
   - 1.1 [What Apache Spark Is](#11-what-apache-spark-is)
   - 1.2 [How Databricks Enhances Apache Spark](#12-how-databricks-enhances-apache-spark)
   - 1.3 [Databricks Runtime vs Open-Source Spark](#13-databricks-runtime-vs-open-source-spark)
   - 1.4 [Photon: The Vectorized Query Engine](#14-photon-the-vectorized-query-engine)
2. [Write Your First Apache Spark Code](#2-write-your-first-apache-spark-code)
   - 2.1 [The SparkSession — Your Entry Point](#21-the-sparksession--your-entry-point)
   - 2.2 [Hello World: Creating a Simple DataFrame](#22-hello-world-creating-a-simple-dataframe)
   - 2.3 [Reading a File and Running a Query](#23-reading-a-file-and-running-a-query)
   - 2.4 [Actions vs Transformations](#24-actions-vs-transformations)
   - 2.5 [Checking the Spark UI](#25-checking-the-spark-ui)
3. [Apache Spark Architecture: How Spark Runs on a Cluster](#3-apache-spark-architecture-how-spark-runs-on-a-cluster)
   - 3.1 [High-Level Architecture Overview](#31-high-level-architecture-overview)
   - 3.2 [Cluster Manager](#32-cluster-manager)
   - 3.3 [Driver and Executor Model](#33-driver-and-executor-model)
   - 3.4 [Resilient Distributed Datasets (RDDs)](#34-resilient-distributed-datasets-rdds)
   - 3.5 [DataFrames and Datasets API](#35-dataframes-and-datasets-api)
   - 3.6 [Directed Acyclic Graph (DAG) and Query Planning](#36-directed-acyclic-graph-dag-and-query-planning)
   - 3.7 [Stages, Tasks, and Partitions](#37-stages-tasks-and-partitions)
   - 3.8 [Shuffle Operations](#38-shuffle-operations)
   - 3.9 [Memory Management](#39-memory-management)
4. [Summary](#4-summary)

---

## 1. Databricks-Powered Apache Spark

### 1.1 What Apache Spark Is

**Apache Spark** is an open-source, distributed computing engine designed for large-scale data processing. It was created as a faster, more general-purpose alternative to Hadoop MapReduce.

**Core Properties:**

| Property           | Description                                                 |
| ------------------ | ----------------------------------------------------------- |
| **Distributed**    | Splits data across many nodes and processes in parallel     |
| **In-Memory**      | Keeps intermediate results in RAM, avoiding slow disk I/O   |
| **Fault-Tolerant** | Automatically recovers lost partitions using lineage graphs |
| **Unified Engine** | Handles batch, streaming, SQL, ML, and graph workloads      |
| **Multi-Language** | Supports Python (PySpark), Scala, Java, R, and SQL          |

**What makes Spark fast:**

```
Hadoop MapReduce (old approach)
────────────────────────────────
  Read from disk → Map → Write to disk → Read from disk → Reduce → Write to disk
  (Every step involves expensive HDFS disk I/O)

Apache Spark (modern approach)
────────────────────────────────
  Read from disk → Map → [in memory] → Reduce → Write result
  (Intermediate data stays in RAM — up to 100× faster for iterative jobs)
```

**Spark's Unified Stack:**

```
┌─────────────────────────────────────────────────────────┐
│                   Spark Applications                    │
├──────────────┬──────────────┬────────────┬──────────────┤
│  Spark SQL   │  Structured  │  MLlib     │  GraphX      │
│  DataFrames  │  Streaming   │ (ML/AI)    │  (Graphs)    │
├──────────────┴──────────────┴────────────┴──────────────┤
│              Spark Core (RDD, Scheduling, I/O)          │
├─────────────────────────────────────────────────────────┤
│     Cluster Manager: YARN / Mesos / Kubernetes /        │
│                      Databricks                         │
├─────────────────────────────────────────────────────────┤
│          Storage: HDFS / S3 / ADLS / GCS / Local        │
└─────────────────────────────────────────────────────────┘
```

---

### 1.2 How Databricks Enhances Apache Spark

While Apache Spark is powerful on its own, running it in production requires significant infrastructure management. Databricks eliminates this complexity:

```
Open-Source Apache Spark                Databricks-Managed Spark
────────────────────────                ────────────────────────
• Manual cluster provisioning    →      • One-click cluster creation
• JVM + Python env management    →      • Pre-configured Databricks Runtime
• No built-in notebooks          →      • Collaborative notebooks with cell output
• Manual security configuration  →      • Unity Catalog, RBAC, column masking
• No optimized query engine      →      • Photon vectorized engine (C++ native)
• Manual Spark tuning required   →      • Auto-tuning, auto-scaling
• No Delta Lake by default       →      • Delta Lake built-in (ACID transactions)
• No job scheduling UI           →      • Lakeflow Jobs with DAG visualization
```

**Databricks Runtime (DBR)** is a curated, pre-tuned distribution of Apache Spark that includes:

- **Apache Spark** (latest stable version, backport patches)
- **Delta Lake** (ACID transactions, schema evolution)
- **Photon** (vectorized native execution engine — optional)
- **Optimised connectors** for AWS S3, Azure ADLS, Unity Catalog
- **Pre-installed ML libraries** (MLflow, scikit-learn, TensorFlow, PyTorch)
- **Auto-scaling and auto-termination** capabilities

---

### 1.3 Databricks Runtime vs Open-Source Spark

| Feature                | Open-Source Spark       | Databricks Runtime (DBR)                          |
| ---------------------- | ----------------------- | ------------------------------------------------- |
| **Delta Lake**         | Must install separately | Built-in, fully integrated                        |
| **Photon Engine**      | Not available           | Optional, significant speedup                     |
| **Auto-scaling**       | Manual configuration    | Automatic, cloud-native                           |
| **Cluster management** | Manual (YARN, k8s)      | Fully managed                                     |
| **Security**           | DIY                     | Unity Catalog, row/column-level                   |
| **Observability**      | Basic Spark UI          | Enhanced Spark UI + cluster metrics               |
| **Optimization**       | Manual hints            | Adaptive Query Execution (AQE) enabled by default |
| **Python env**         | Manually managed        | Managed conda/pip + Library UI                    |

---

### 1.4 Photon: The Vectorized Query Engine

**Photon** is Databricks' proprietary query engine written in **C++** that replaces the JVM-based Spark engine for SQL and DataFrame operations.

```
Standard Spark (JVM)                    Photon (C++)
────────────────────                    ────────────
Row-at-a-time processing                Vectorized (batch of rows)
JVM overhead (GC pauses)                No GC — native memory management
Scala/Java bytecode                     CPU-optimised SIMD instructions
~1× baseline                            2-10× faster for SQL workloads
```

Photon is automatically enabled on clusters with `Photon Acceleration` checked. It accelerates:

- SQL queries and DataFrame operations
- Joins, aggregations, sorts
- Delta Lake reads and writes
- I/O scanning of Parquet/Delta files

---

## 2. Write Your First Apache Spark Code

### 2.1 The SparkSession — Your Entry Point

Every Spark application starts with a **`SparkSession`** — the unified entry point to all Spark functionality. In Databricks, it is automatically created and available as the variable `spark`.

```python
# In Databricks — SparkSession is already available as 'spark'
print(spark)
# <pyspark.sql.session.SparkSession object at 0x...>

print(spark.version)
# 3.5.x (or the current Databricks Runtime version)
```

If you are running Spark outside Databricks, you create a SparkSession manually:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstSparkApp") \
    .master("local[*]") \
    .getOrCreate()
```

The `SparkSession` provides access to:

- `spark.read` — Read data from files, databases, etc.
- `spark.sql()` — Run SQL queries
- `spark.catalog` — Manage databases, tables, views
- `spark.conf` — Get/set Spark configuration
- `spark.sparkContext` — Low-level RDD operations

---

### 2.2 Hello World: Creating a Simple DataFrame

```python
# Create a DataFrame from a Python list
data = [
    ("Alice", 30, "Engineering"),
    ("Bob",   25, "Marketing"),
    ("Carol", 35, "Engineering"),
    ("David", 28, "Finance"),
]

columns = ["name", "age", "department"]

df = spark.createDataFrame(data, columns)
df.show()
```

**Output:**

```
+-----+---+-----------+
| name|age| department|
+-----+---+-----------+
|Alice| 30|Engineering|
|  Bob| 25|  Marketing|
|Carol| 35|Engineering|
|David| 28|    Finance|
+-----+---+-----------+
```

**Inspect the DataFrame schema:**

```python
df.printSchema()
```

```
root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- department: string (nullable = true)
```

**Count rows:**

```python
df.count()   # 4
```

---

### 2.3 Reading a File and Running a Query

```python
# Read a CSV file from DBFS or S3
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/databricks-datasets/samples/population-vs-price/data_geo.csv")

# Show first 5 rows
df.show(5)

# Print schema
df.printSchema()

# Run SQL: register as a temporary view, then query
df.createOrReplaceTempView("cities")

result = spark.sql("""
    SELECT City_Name, 2014_Population_estimate
    FROM cities
    WHERE 2014_Population_estimate > 1000000
    ORDER BY 2014_Population_estimate DESC
""")

result.show()
```

**Chaining DataFrame operations:**

```python
from pyspark.sql.functions import col

df.filter(col("2014_Population_estimate") > 1000000) \
  .select("City_Name", "2014_Population_estimate") \
  .orderBy(col("2014_Population_estimate").desc()) \
  .show()
```

Both approaches produce identical results. Spark SQL and the DataFrame API are interchangeable — they compile to the same execution plan.

---

### 2.4 Actions vs Transformations

This is the most important concept for understanding Spark's **lazy evaluation** model.

**Transformations** define _what to do_ but do not execute immediately:

```python
# These lines do NOT trigger any computation
df_filtered    = df.filter(col("age") > 25)        # Transformation
df_selected    = df_filtered.select("name", "age") # Transformation
df_engineered  = df_selected.filter(col("age") < 35) # Transformation
```

**Actions** trigger the actual computation:

```python
# These lines TRIGGER computation (cause Spark to execute the plan)
df_engineered.show()    # Action — prints results
df_engineered.count()   # Action — returns a number
df_engineered.collect() # Action — returns results to driver as a Python list
df_engineered.first()   # Action — returns first row
df_engineered.write.parquet("/tmp/output") # Action — writes to storage
```

**Why lazy evaluation?**

```
Without lazy evaluation:
  Step 1 → Execute → Step 2 → Execute → Step 3 → Execute
  (3 passes over the data)

With lazy evaluation (Spark):
  Step 1 + Step 2 + Step 3 → Optimised Plan → Execute ONCE
  (Catalyst Optimizer rewrites and combines steps — 1 pass)
```

**Transformation Types:**

| Type       | Description                                                                     | Examples                                 |
| ---------- | ------------------------------------------------------------------------------- | ---------------------------------------- |
| **Narrow** | Each output partition depends on at most one input partition — no data movement | `filter`, `select`, `withColumn`, `map`  |
| **Wide**   | Output partitions depend on multiple input partitions — requires a **shuffle**  | `groupBy`, `join`, `distinct`, `orderBy` |

---

### 2.5 Checking the Spark UI

Every Spark job generates a rich UI to monitor execution. In Databricks, click the **Spark UI** link at the top of any notebook cell output after an action runs.

```
Spark UI tabs:
┌─────────────────────────────────────────────────────┐
│  Jobs  │  Stages  │  Storage  │  Environment  │ SQL │
└─────────────────────────────────────────────────────┘

Jobs tab     → List of all actions triggered; click to drill into stages
Stages tab   → Breakdown of each stage; shows task durations, skew
Storage tab  → Cached RDDs / DataFrames and memory usage
SQL tab      → Visual query plan (DAG) for each SQL/DataFrame query
Environment  → Spark configuration, runtime versions, classpath
```

Key metrics to watch:

- **Input size** and **output size** per stage
- **Shuffle read/write** bytes (high values indicate expensive wide transformations)
- **Task skew** — if one task takes much longer than others, data is unevenly distributed
- **GC time** — high GC indicates memory pressure

---

## 3. Apache Spark Architecture: How Spark Runs on a Cluster

### 3.1 High-Level Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      YOUR SPARK APPLICATION                     │
│                    (Notebook / Python script)                   │
└──────────────────────────────┬──────────────────────────────────┘
                               │  SparkContext / SparkSession
┌──────────────────────────────▼──────────────────────────────────┐
│                         DRIVER PROGRAM                          │
│                                                                 │
│  • SparkContext (entry point to Spark)                          │
│  • Converts user code into a logical plan                       │
│  • Catalyst Optimizer → optimises the logical plan              │
│  • Tungsten → generates optimised bytecode                      │
│  • DAG Scheduler → breaks plan into stages                      │
│  • Task Scheduler → assigns tasks to executors                  │
└──────────────┬───────────────────────────────────┬──────────────┘
               │ Request resources                 │ Send tasks
┌──────────────▼──────────────┐       ┌────────────▼──────────────┐
│     CLUSTER MANAGER         │       │       EXECUTOR 1           │
│  (Databricks / YARN /       │◄─────►│  • Runs tasks              │
│   Kubernetes / Standalone)  │       │  • Stores partitions in RAM│
│                             │       │  • Sends results to driver  │
└──────────────┬──────────────┘       └───────────────────────────┘
               │                               ...
               │                      ┌───────────────────────────┐
               └─────────────────────►│       EXECUTOR N           │
                                      │  • Runs tasks              │
                                      │  • Stores partitions in RAM│
                                      └───────────────────────────┘
```

---

### 3.2 Cluster Manager

The **Cluster Manager** is responsible for allocating resources (CPU, RAM) across applications. It acts as a resource broker between the Spark driver and the physical machines.

| Cluster Manager | Use Case                                             |
| --------------- | ---------------------------------------------------- |
| **Databricks**  | Fully managed — preferred for Databricks platform    |
| **YARN**        | Hadoop ecosystems, on-premises clusters              |
| **Kubernetes**  | Containerised environments, cloud-native deployments |
| **Mesos**       | Legacy deployments                                   |
| **Standalone**  | Development and testing on a local machine           |

In Databricks, the cluster manager is fully abstracted. You configure:

- **Worker node type** (EC2 instance type)
- **Number of workers** (fixed or auto-scaling range)
- **Driver node type**
- **Databricks Runtime version**

---

### 3.3 Driver and Executor Model

#### The Driver

The **Driver** is the JVM process running your Spark application. It:

1. Receives the user's Spark code (transformations + actions).
2. Builds a **logical plan** — a description of what needs to be computed.
3. Passes the logical plan to the **Catalyst Optimizer** for optimization.
4. Generates a **physical plan** and breaks it into **stages** and **tasks**.
5. Schedules **tasks** on **executors** via the cluster manager.
6. Collects results when an action (e.g., `.collect()`) returns data to the driver.

```
Driver responsibilities:
  User Code → Logical Plan → Optimised Plan → Physical Plan
            → DAG of Stages → Tasks → Schedule on Executors
            → Collect Results → Return to User
```

> **Important:** The driver stores the entire result of `.collect()` in its memory. Never call `.collect()` on a large DataFrame — this will crash the driver with an out-of-memory error.

#### The Executor

An **Executor** is a JVM process running on a worker node that:

1. Receives **tasks** from the driver.
2. Executes the task on a **partition** of data.
3. Stores results in memory (for caching) or writes to disk/storage.
4. Reports task completion and results back to the driver.

```
Executor internals:
┌─────────────────────────────────────────────────┐
│                   EXECUTOR                      │
│                                                 │
│  Thread 1: Task A on Partition 0                │
│  Thread 2: Task B on Partition 1                │
│  Thread 3: Task C on Partition 2                │
│  ...                                            │
│                                                 │
│  JVM Heap Memory:                               │
│  ┌────────────────┬──────────────────────────┐  │
│  │ Storage Memory │  Execution Memory        │  │
│  │ (RDD cache,    │  (Shuffle buffers,       │  │
│  │  broadcast)    │   sort buffers, join)    │  │
│  └────────────────┴──────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

**Executor configuration:**

```python
# Check current executor settings
spark.conf.get("spark.executor.memory")     # e.g., "4g"
spark.conf.get("spark.executor.cores")      # e.g., "4"

# Number of task slots per executor = spark.executor.cores
# Total parallelism = num_executors × cores_per_executor
```

---

### 3.4 Resilient Distributed Datasets (RDDs)

**RDDs** are the foundational data abstraction in Spark. Although modern Spark code uses DataFrames, understanding RDDs explains how Spark achieves fault-tolerance.

```
An RDD is:
  R - Resilient   → Fault-tolerant; can be recomputed from lineage
  D - Distributed → Data spread across multiple partitions on multiple nodes
  D - Dataset     → A collection of records (any type)
```

**RDD Fault Tolerance — Lineage:**

```
RDD1 (read from S3)
  │ .filter(age > 25)
  ▼
RDD2
  │ .map(name.upper())
  ▼
RDD3   ← If a partition of RDD3 is lost, Spark recomputes it
         by replaying: read from S3 → filter → map
         (No data replication needed — uses the lineage graph)
```

**Creating RDDs:**

```python
# From a Python collection
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
print(rdd.collect())   # [1, 2, 3, 4, 5]

# From a file
rdd = spark.sparkContext.textFile("/path/to/file.txt")

# RDD operations
rdd_doubled = rdd.map(lambda x: x * 2)
rdd_even    = rdd.filter(lambda x: x % 2 == 0)
total       = rdd.reduce(lambda a, b: a + b)
```

> **Recommendation:** In modern Spark, use **DataFrames** or **Spark SQL** instead of RDDs. DataFrames benefit from the Catalyst optimizer and Tungsten memory engine. RDDs bypass these optimizations.

---

### 3.5 DataFrames and Datasets API

**DataFrames** are distributed collections of data organised into named columns — conceptually identical to a table in a relational database or a pandas DataFrame, but distributed across a cluster.

```
DataFrame = RDD[Row] + Schema

┌─────────┬─────┬─────────────┐
│  name   │ age │ department  │  ← Schema (column names + types)
├─────────┼─────┼─────────────┤
│  Alice  │  30 │ Engineering │
│  Bob    │  25 │ Marketing   │  ← Data partitioned across executors
│  Carol  │  35 │ Engineering │
│  David  │  28 │ Finance     │
└─────────┴─────┴─────────────┘
```

**API comparison:**

| API           | Language               | Optimiser      | Type Safety        | Use Case               |
| ------------- | ---------------------- | -------------- | ------------------ | ---------------------- |
| **RDD**       | Python, Scala, Java    | None           | No                 | Low-level custom logic |
| **DataFrame** | Python, Scala, Java, R | Catalyst + AQE | No                 | Most data engineering  |
| **Dataset**   | Scala, Java only       | Catalyst + AQE | Yes (compile-time) | Type-safe Scala apps   |
| **Spark SQL** | SQL                    | Catalyst + AQE | No                 | SQL-first workflows    |

---

### 3.6 Directed Acyclic Graph (DAG) and Query Planning

When you call an action, Spark does not execute your code line by line. Instead, it builds an optimised **execution plan** using the following pipeline:

```
User Code (DataFrame API / SQL)
         │
         ▼
  ┌─────────────────────┐
  │   Unresolved        │
  │   Logical Plan      │  ← Abstract description (column names unresolved)
  └──────────┬──────────┘
             │  Analysis (resolve column names, check schema)
             ▼
  ┌─────────────────────┐
  │   Resolved          │
  │   Logical Plan      │  ← All columns resolved to actual types
  └──────────┬──────────┘
             │  Catalyst Optimizer (apply rules: predicate pushdown,
             │  column pruning, constant folding, join reordering...)
             ▼
  ┌─────────────────────┐
  │   Optimised         │
  │   Logical Plan      │  ← Rewritten, more efficient logical plan
  └──────────┬──────────┘
             │  Physical Planning (choose algorithms: sort-merge vs
             │  broadcast join, hash aggregation vs sort aggregation...)
             ▼
  ┌─────────────────────┐
  │   Physical Plans    │  ← Multiple candidate plans generated
  │   (candidates)      │
  └──────────┬──────────┘
             │  Cost Model (estimate rows, bytes, CPU)
             ▼
  ┌─────────────────────┐
  │   Selected          │
  │   Physical Plan     │  ← Lowest-cost plan chosen
  └──────────┬──────────┘
             │  Code Generation (Tungsten — generates optimised JVM bytecode)
             ▼
         EXECUTE
```

**Catalyst Optimizer Key Rules:**

| Rule                   | Description                                                  |
| ---------------------- | ------------------------------------------------------------ |
| **Predicate Pushdown** | Move `WHERE` filters as early as possible (read less data)   |
| **Column Pruning**     | Only read columns actually referenced in the query           |
| **Constant Folding**   | Pre-compute `1 + 1` at planning time, not execution time     |
| **Join Reordering**    | Reorder joins to process smaller tables first                |
| **Broadcast Join**     | Auto-detect small tables and broadcast them to all executors |

---

### 3.7 Stages, Tasks, and Partitions

**The execution flow from plan to tasks:**

```
Physical Plan
    │
    ▼
 DAG of Stages
(split at wide transformations — shuffle boundaries)
    │
    ├── Stage 1: read + filter + select (narrow transformations)
    │       ├── Task 0  (partition 0)
    │       ├── Task 1  (partition 1)
    │       └── Task N  (partition N)
    │             [shuffle write]
    │
    └── Stage 2: groupBy + aggregate (after shuffle)
            ├── Task 0  (reduce partition 0)
            ├── Task 1  (reduce partition 1)
            └── Task M  (reduce partition M)
```

**Key concepts:**

| Concept       | Definition                                                      |
| ------------- | --------------------------------------------------------------- |
| **Partition** | A chunk of data stored on one executor; the unit of parallelism |
| **Task**      | A unit of work that processes exactly one partition             |
| **Stage**     | A group of tasks that can run without shuffling data            |
| **Job**       | All stages triggered by a single action                         |

**Partitions and parallelism:**

```python
# Check the number of partitions in a DataFrame
df.rdd.getNumPartitions()   # e.g., 8

# Repartition — change the number of partitions
df_repartitioned = df.repartition(16)    # Increases to 16 (causes shuffle)
df_coalesced     = df.coalesce(4)        # Reduces to 4 (no shuffle)

# Default parallelism
spark.conf.get("spark.default.parallelism")         # for RDDs
spark.conf.get("spark.sql.shuffle.partitions")      # for DataFrame shuffles (default: 200)
```

**Best practice:** Set `spark.sql.shuffle.partitions` based on your data size:

```python
# Rule of thumb: ~128 MB per partition after shuffle
spark.conf.set("spark.sql.shuffle.partitions", "32")  # for a small/medium dataset
# Default 200 is often too high for small data → creates overhead
```

---

### 3.8 Shuffle Operations

A **shuffle** is the process of redistributing data across partitions — it is the most expensive operation in Spark because it involves:

1. Writing shuffle data to local disk (spill).
2. Network transfer of data from executors that produced the data to executors that need it.
3. Sorting and merging the incoming data.

```
Before Shuffle (groupBy department)
──────────────────────────────────────
  Partition 0: [Alice/Eng, Bob/Mkt, Carol/Eng]
  Partition 1: [David/Fin, Eve/Eng, Frank/Mkt]

              SHUFFLE (data moves across network)

After Shuffle
──────────────────────────────────────
  Partition 0: [Alice/Eng, Carol/Eng, Eve/Eng]   ← all Engineering
  Partition 1: [Bob/Mkt, Frank/Mkt]               ← all Marketing
  Partition 2: [David/Fin]                         ← all Finance
```

**Operations that cause shuffles (wide transformations):**

```python
df.groupBy("department").count()          # shuffle
df.join(other_df, "id")                   # shuffle (unless broadcast)
df.orderBy("age")                         # shuffle
df.distinct()                             # shuffle
df.repartition(n)                         # shuffle
```

**Minimising shuffle cost:**

- Use `broadcast join` for small lookup tables (< 10 MB by default).
- Avoid unnecessary `orderBy` on large datasets.
- Tune `spark.sql.shuffle.partitions` appropriately.
- Use `cache()` before repeated shuffles on the same base data.

---

### 3.9 Memory Management

Spark Executor memory is divided into regions:

```
┌─────────────────────────────────────────────────────────┐
│                  JVM HEAP (spark.executor.memory)        │
│                                                         │
│  ┌───────────────────────────── spark.memory.fraction ─►│
│  │                                                       │
│  │  ┌──────────────────────┬────────────────────────┐   │
│  │  │  Storage Memory      │  Execution Memory      │   │
│  │  │  (caching RDDs,      │  (shuffle buffers,     │   │
│  │  │   broadcast vars)    │   sort buffers, joins) │   │
│  │  │                      │                        │   │
│  │  │  These two regions borrow from each other     │   │
│  │  └──────────────────────┴────────────────────────┘   │
│  └───────────────────────────────────────────────────── │
│                                                         │
│  Reserved Memory (300 MB) + User Memory (1 - fraction)  │
└─────────────────────────────────────────────────────────┘
```

**Key configuration parameters:**

```python
# Fraction of executor memory used for Spark managed memory (default: 0.6 = 60%)
spark.conf.get("spark.memory.fraction")

# Of the managed memory, fraction for storage vs execution (default: 0.5)
spark.conf.get("spark.memory.storageFraction")

# Maximum size of a table to be auto-broadcast in joins (default: 10 MB)
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
```

**Caching DataFrames:**

```python
# Cache a DataFrame in memory (Storage Memory)
df.cache()          # lazy — actually caches on first action
df.persist()        # same as cache(), uses default storage level MEMORY_AND_DISK

from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK_2)   # 2 replicas

# Free cached data
df.unpersist()
```

**When to cache:**

- A DataFrame is used in multiple actions (e.g., multiple `.show()`, `.count()`, or `.write()`).
- The data is the result of an expensive transformation.
- Do **not** cache if the DataFrame is only used once — it wastes memory.

---

## 4. Summary

| Topic                  | Key Takeaway                                                                                 |
| ---------------------- | -------------------------------------------------------------------------------------------- |
| **Spark vs Hadoop**    | Spark processes data in-memory; 10–100× faster for iterative workloads                       |
| **Databricks Runtime** | Pre-optimised Spark + Delta Lake + Photon; removes operational burden                        |
| **SparkSession**       | The single entry point to all Spark functionality; always available as `spark` in Databricks |
| **Lazy Evaluation**    | Transformations build a logical plan; only Actions trigger execution                         |
| **Driver**             | Orchestrates the entire job; plans execution; collects final results                         |
| **Executor**           | Runs tasks on individual data partitions; stores cached data                                 |
| **Partition**          | The unit of parallelism; one task processes one partition                                    |
| **Stage**              | A group of tasks separated by shuffle boundaries                                             |
| **Shuffle**            | Expensive data redistribution across partitions; minimise when possible                      |
| **Catalyst Optimizer** | Rewrites your logical plan into an optimised physical execution plan                         |
| **Photon**             | Databricks' C++ vectorized engine for 2–10× SQL speedup                                      |

**Continue to:** [Guide 04 — Working with Datasets and Notebooks](04-datasets-notebooks-magic-commands.md)
