# DataFrames and Spark SQL Fundamentals

---

## Table of Contents

1. [Introduction to DataFrames in PySpark](#1-introduction-to-dataframes-in-pyspark)
   - 1.1 [What is a DataFrame?](#11-what-is-a-dataframe)
   - 1.2 [DataFrame vs RDD vs Spark SQL](#12-dataframe-vs-rdd-vs-spark-sql)
   - 1.3 [The `Row` and `Column` Objects](#13-the-row-and-column-objects)
2. [Creating DataFrames from Different Data Sources](#2-creating-dataframes-from-different-data-sources)
   - 2.1 [From Python Lists and Dictionaries](#21-from-python-lists-and-dictionaries)
   - 2.2 [From RDDs](#22-from-rdds)
   - 2.3 [From CSV Files](#23-from-csv-files)
   - 2.4 [From JSON Files](#24-from-json-files)
   - 2.5 [From Parquet Files](#25-from-parquet-files)
   - 2.6 [From Delta Tables](#26-from-delta-tables)
   - 2.7 [From JDBC / Relational Databases](#27-from-jdbc--relational-databases)
3. [DataFrame Operations](#3-dataframe-operations)
   - 3.1 [Filtering Rows](#31-filtering-rows)
   - 3.2 [Selecting Columns](#32-selecting-columns)
   - 3.3 [Aggregating Data](#33-aggregating-data)
   - 3.4 [Sorting Data](#34-sorting-data)
   - 3.5 [Removing Duplicates](#35-removing-duplicates)
   - 3.6 [Sampling Data](#36-sampling-data)
4. [Using Spark SQL for Querying DataFrames](#4-using-spark-sql-for-querying-dataframes)
   - 4.1 [Creating Temporary Views](#41-creating-temporary-views)
   - 4.2 [Running SQL Queries](#42-running-sql-queries)
   - 4.3 [Mixing DataFrame API and SQL](#43-mixing-dataframe-api-and-sql)
5. [Advanced SQL Operations](#5-advanced-sql-operations)
   - 5.1 [Joins](#51-joins)
   - 5.2 [Aggregations and GROUP BY](#52-aggregations-and-group-by)
   - 5.3 [Window Functions](#53-window-functions)
6. [Performance Tuning and Optimisation Techniques](#6-performance-tuning-and-optimisation-techniques)
   - 6.1 [Caching and Persistence](#61-caching-and-persistence)
   - 6.2 [Broadcast Joins](#62-broadcast-joins)
   - 6.3 [Adaptive Query Execution (AQE)](#63-adaptive-query-execution-aqe)
   - 6.4 [Partition Tuning](#64-partition-tuning)
   - 6.5 [Predicate Pushdown and Column Pruning](#65-predicate-pushdown-and-column-pruning)
   - 6.6 [Explain Plans](#66-explain-plans)
7. [Summary](#7-summary)

---

## 1. Introduction to DataFrames in PySpark

### 1.1 What is a DataFrame?

A **DataFrame** in PySpark is a distributed, tabular data structure organised into named columns with an associated **schema** (column names and data types). It is conceptually identical to:

- A **relational database table**
- A **pandas DataFrame** — but distributed across a cluster

```
PySpark DataFrame
─────────────────────────────────────────────────────
Schema (column names + types — stored in the driver)

  name: StringType   age: LongType   dept: StringType
  ──────────────────────────────────────────────────
Partition 0 (Executor 1):
  ("Alice", 30, "Engineering")
  ("Bob",   25, "Marketing")

Partition 1 (Executor 2):
  ("Carol", 35, "Engineering")
  ("David", 28, "Finance")

Partition 2 (Executor 3):
  ("Eve",   31, "Finance")
  ("Frank", 22, "Marketing")
─────────────────────────────────────────────────────
The schema is known to the driver.
The data is distributed across executors.
```

**Key properties:**

- **Immutable** — transformations create new DataFrames; the original is unchanged.
- **Lazy** — transformations are not executed until an action is called.
- **Distributed** — data is split into partitions processed in parallel.
- **Optimised** — the Catalyst Optimizer rewrites your operations for efficiency.

---

### 1.2 DataFrame vs RDD vs Spark SQL

| Feature               | RDD                                      | DataFrame / Dataset                     | Spark SQL               |
| --------------------- | ---------------------------------------- | --------------------------------------- | ----------------------- |
| **Abstraction level** | Low                                      | High                                    | High                    |
| **Schema**            | None                                     | Yes (typed)                             | Yes                     |
| **Optimization**      | None                                     | Catalyst + AQE                          | Catalyst + AQE          |
| **Performance**       | Lower (no optimization)                  | High                                    | High (same plan)        |
| **API**               | Functional (map, filter, reduce)         | Declarative (select, filter, groupBy)   | SQL strings             |
| **Languages**         | Python, Scala, Java, R                   | Python, Scala, Java, R                  | All (via `spark.sql()`) |
| **Type safety**       | No                                       | No (DataFrame) / Yes (Dataset in Scala) | No                      |
| **Best for**          | Custom iterative logic, non-tabular data | Standard data engineering               | SQL-first analysts      |

> **Rule of thumb:** Use **DataFrames** or **Spark SQL** for all standard data engineering. Use RDDs only when you need low-level control (e.g., custom partitioning, non-tabular data structures).

---

### 1.3 The `Row` and `Column` Objects

**`Row`** is the data structure for a single record:

```python
from pyspark.sql import Row

# Create a Row
r = Row(name="Alice", age=30, dept="Engineering")
print(r.name)        # Alice
print(r["age"])      # 30
print(r.asDict())    # {'name': 'Alice', 'age': 30, 'dept': 'Engineering'}
```

**`Column`** represents a column expression — used in transformations:

```python
from pyspark.sql.functions import col

# Three ways to reference a column
df["name"]        # DataFrame subscript notation
df.name           # Attribute notation
col("name")       # col() function — most portable, works outside DataFrame context

# Column expressions
col("salary") * 1.1          # arithmetic
col("age") > 30              # comparison → boolean column
col("name").alias("emp_name") # rename
col("salary").cast("integer") # type cast
```

---

## 2. Creating DataFrames from Different Data Sources

### 2.1 From Python Lists and Dictionaries

**From a list of tuples with a column name list:**

```python
data = [
    ("E001", "Alice",  65000.0, "Engineering"),
    ("E002", "Bob",    48000.0, "Marketing"),
    ("E003", "Carol",  72000.0, "Engineering"),
    ("E004", "David",  55000.0, "Finance"),
]

df = spark.createDataFrame(data, ["emp_id", "name", "salary", "dept"])
df.show()
```

**From a list of `Row` objects:**

```python
from pyspark.sql import Row

rows = [
    Row(emp_id="E001", name="Alice",  salary=65000.0, dept="Engineering"),
    Row(emp_id="E002", name="Bob",    salary=48000.0, dept="Marketing"),
]

df = spark.createDataFrame(rows)
df.show()
```

**From a list of dictionaries:**

```python
data = [
    {"emp_id": "E001", "name": "Alice",  "salary": 65000.0, "dept": "Engineering"},
    {"emp_id": "E002", "name": "Bob",    "salary": 48000.0, "dept": "Marketing"},
]

df = spark.createDataFrame(data)
df.printSchema()
df.show()
```

**From a pandas DataFrame:**

```python
import pandas as pd

pdf = pd.DataFrame({
    "name":   ["Alice", "Bob", "Carol"],
    "salary": [65000, 48000, 72000],
    "dept":   ["Engineering", "Marketing", "Engineering"],
})

# Convert pandas → Spark
df = spark.createDataFrame(pdf)
df.show()
```

---

### 2.2 From RDDs

```python
# Create an RDD first
sc = spark.sparkContext
rdd = sc.parallelize([
    ("E001", "Alice",  65000.0),
    ("E002", "Bob",    48000.0),
])

# Convert RDD → DataFrame with column names
df = rdd.toDF(["emp_id", "name", "salary"])

# Or use createDataFrame
df = spark.createDataFrame(rdd, ["emp_id", "name", "salary"])

# Convert RDD of Row objects → DataFrame (schema inferred from Row fields)
from pyspark.sql import Row
rdd_rows = rdd.map(lambda r: Row(emp_id=r[0], name=r[1], salary=r[2]))
df = spark.createDataFrame(rdd_rows)
```

---

### 2.3 From CSV Files

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("name",   StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("dept",   StringType(), True),
])

df = spark.read \
    .schema(schema) \
    .option("header", True) \
    .csv("/FileStore/datasets/employees.csv")
```

See [Guide 04 — Section 5 & 6](04-datasets-notebooks-magic-commands.md) for full CSV options reference.

---

### 2.4 From JSON Files

Spark can read **single-line JSON** (one JSON object per line — JSONL format) and **multi-line JSON** (pretty-printed JSON files):

```python
# Single-line JSON (default)
# File: {"emp_id":"E001","name":"Alice","salary":65000.0,"dept":"Engineering"}
df = spark.read.json("/FileStore/datasets/employees.json")

# Multi-line JSON (pretty-printed, entire file is one JSON object/array)
df = spark.read \
    .option("multiLine", True) \
    .json("/FileStore/datasets/employees_pretty.json")

# Nested JSON is automatically read into StructType columns
df.printSchema()
# root
#  |-- address: struct (nullable = true)
#  |    |-- city: string (nullable = true)
#  |    |-- country: string (nullable = true)
#  |-- name: string (nullable = true)
```

**Accessing nested fields:**

```python
from pyspark.sql.functions import col

# Access nested struct field
df.select(col("address.city"), col("name")).show()

# Or using dot notation in SQL
spark.sql("SELECT address.city, name FROM emp_json_view").show()
```

---

### 2.5 From Parquet Files

**Parquet** is a columnar binary format — the default interchange format for Spark. It is much faster and more space-efficient than CSV or JSON.

```python
# Read Parquet (schema is embedded in the file — no option needed)
df = spark.read.parquet("/FileStore/datasets/employees.parquet")

# Or equivalently
df = spark.read.format("parquet").load("/FileStore/datasets/employees.parquet")

# Write to Parquet
df.write.mode("overwrite").parquet("/FileStore/output/employees_parquet/")
```

**Parquet vs CSV:**

| Feature            | CSV                       | Parquet                                     |
| ------------------ | ------------------------- | ------------------------------------------- |
| Format             | Text, row-oriented        | Binary, column-oriented                     |
| Schema             | External                  | Embedded                                    |
| Compression        | None or gzip on full file | Per-column compression (snappy, gzip, zstd) |
| Read speed         | Slow (parse every field)  | Fast (skip unused columns)                  |
| Size               | Larger                    | 5–10× smaller typically                     |
| Predicate pushdown | Limited                   | Yes (min/max statistics per row group)      |

---

### 2.6 From Delta Tables

**Delta Lake** is the default table format in Databricks. Reading Delta tables is the most common operation in a Databricks pipeline:

```python
# Read a Delta table from a path
df = spark.read.format("delta").load("/FileStore/delta/employees/")

# Read a registered Delta table by name (requires Unity Catalog or Hive Metastore)
df = spark.table("training.employees")

# Read a specific Delta version (time travel)
df = spark.read.format("delta") \
    .option("versionAsOf", 3) \
    .load("/FileStore/delta/employees/")

# Read as of a specific timestamp
df = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-15") \
    .load("/FileStore/delta/employees/")

# Write to Delta
df.write.format("delta").mode("overwrite").save("/FileStore/delta/employees/")
```

---

### 2.7 From JDBC / Relational Databases

```python
jdbc_url = "jdbc:postgresql://my-rds-host:5432/mydb"

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "public.employees") \
    .option("user", dbutils.secrets.get("my-scope", "pg-user")) \
    .option("password", dbutils.secrets.get("my-scope", "pg-password")) \
    .option("driver", "org.postgresql.Driver") \
    .option("numPartitions", 4) \
    .option("partitionColumn", "emp_id") \
    .option("lowerBound", 1) \
    .option("upperBound", 10000) \
    .load()
```

> **Security:** Always retrieve database credentials from **Databricks Secrets** (`dbutils.secrets.get`). Never hardcode passwords in notebooks — they appear in notebook revision history and are visible to anyone with notebook access.

---

## 3. DataFrame Operations

### 3.1 Filtering Rows

**`filter()` and `where()` are identical** — use whichever reads more naturally:

```python
from pyspark.sql.functions import col

# Filter using column expression
df.filter(col("salary") > 60000).show()

# Filter using SQL string (convenient for complex conditions)
df.filter("salary > 60000").show()

# where() — identical to filter()
df.where(col("dept") == "Engineering").show()

# Multiple conditions with AND (&) and OR (|)
df.filter((col("salary") > 60000) & (col("dept") == "Engineering")).show()
df.filter((col("dept") == "Engineering") | (col("dept") == "Finance")).show()

# SQL IN
from pyspark.sql.functions import col
df.filter(col("dept").isin("Engineering", "Finance")).show()

# NOT IN
df.filter(~col("dept").isin("Marketing")).show()

# Filter on NULL
df.filter(col("salary").isNull()).show()
df.filter(col("salary").isNotNull()).show()

# String pattern matching
df.filter(col("name").like("A%")).show()         # starts with A
df.filter(col("name").startswith("A")).show()    # same
df.filter(col("email").contains("@gmail")).show()
df.filter(col("name").rlike("^[A-C]")).show()    # regex
```

---

### 3.2 Selecting Columns

```python
# Select specific columns
df.select("name", "salary").show()
df.select(col("name"), col("salary")).show()

# Select with expressions (creates derived/computed columns)
from pyspark.sql.functions import col, round

df.select(
    col("name"),
    col("salary"),
    (col("salary") * 1.1).alias("salary_with_raise"),
    round(col("salary") / 12, 2).alias("monthly_salary"),
).show()

# Select all columns
df.select("*").show()

# Drop specific columns
df.drop("emp_id", "hire_date").show()

# Rename a column while selecting
df.select(
    col("emp_id").alias("employee_id"),
    col("name").alias("full_name"),
    "salary",
).show()

# Select using a list variable (useful in loops)
cols_to_select = ["name", "salary", "dept"]
df.select(*cols_to_select).show()
```

---

### 3.3 Aggregating Data

```python
from pyspark.sql.functions import count, sum, avg, min, max, countDistinct

# Aggregate the entire DataFrame (no grouping)
df.agg(
    count("*").alias("total_employees"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    countDistinct("dept").alias("num_departments"),
).show()

# Aggregate by group
df.groupBy("dept").agg(
    count("*").alias("headcount"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary"),
).orderBy("headcount", ascending=False).show()

# Multi-column grouping
df.groupBy("dept", "location").agg(
    count("*").alias("headcount"),
    avg("salary").alias("avg_salary"),
).show()
```

---

### 3.4 Sorting Data

```python
from pyspark.sql.functions import col

# Sort ascending (default)
df.orderBy("salary").show()
df.sort("salary").show()          # sort() is an alias for orderBy()

# Sort descending
df.orderBy(col("salary").desc()).show()
df.orderBy("salary", ascending=False).show()

# Multi-column sort
df.orderBy(
    col("dept").asc(),
    col("salary").desc()
).show()
```

---

### 3.5 Removing Duplicates

```python
# Remove rows where ALL columns are identical
df.distinct().show()

# Remove rows where specific columns are duplicated (keeps first occurrence)
df.dropDuplicates(["emp_id"]).show()
df.dropDuplicates(["name", "dept"]).show()
```

---

### 3.6 Sampling Data

```python
# Random sample — 10% of rows with replacement
sample_df = df.sample(withReplacement=False, fraction=0.1, seed=42)

# Take exactly N rows (not random — just first N rows)
df.limit(100).show()

# Collect first N rows to driver as a Python list
rows = df.take(5)
for row in rows:
    print(row)

# Collect first row
first_row = df.first()
```

---

## 4. Using Spark SQL for Querying DataFrames

### 4.1 Creating Temporary Views

Temporary views allow DataFrames to be queried using SQL syntax. They exist within the current SparkSession.

```python
# Create a session-scoped temporary view (visible only within this SparkSession)
df.createOrReplaceTempView("employees")

# Create a global temporary view (visible across all sessions on the same cluster)
df.createOrReplaceGlobalTempView("employees_global")
# Access with: global_temp.<view_name>
```

| View Type            | Visibility                  | Lifetime                          | Access                                       |
| -------------------- | --------------------------- | --------------------------------- | -------------------------------------------- |
| **Temp View**        | Current SparkSession only   | Until session ends or dropped     | `SELECT * FROM employees`                    |
| **Global Temp View** | All sessions on the cluster | Until cluster restarts or dropped | `SELECT * FROM global_temp.employees_global` |

```python
# Drop a view
spark.catalog.dropTempView("employees")
spark.catalog.dropGlobalTempView("employees_global")

# List all temp views
spark.catalog.listTables()
```

---

### 4.2 Running SQL Queries

```python
# Run a SQL query and return a DataFrame
result = spark.sql("""
    SELECT dept,
           COUNT(*) AS headcount,
           ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    WHERE salary > 40000
    GROUP BY dept
    HAVING headcount > 1
    ORDER BY avg_salary DESC
""")

result.show()

# %sql magic in a notebook
```

```sql
%sql
SELECT dept,
       COUNT(*) AS headcount,
       ROUND(AVG(salary), 2) AS avg_salary
FROM employees
WHERE salary > 40000
GROUP BY dept
HAVING COUNT(*) > 1
ORDER BY avg_salary DESC;
```

---

### 4.3 Mixing DataFrame API and SQL

The DataFrame API and Spark SQL produce identical execution plans — you can freely mix both styles:

```python
# Start with SQL
dept_stats = spark.sql("""
    SELECT dept, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY dept
""")

# Continue with DataFrame API
final = dept_stats \
    .filter(col("avg_salary") > 60000) \
    .orderBy("avg_salary", ascending=False)

final.show()
```

```python
# Or start with DataFrame API, then register for SQL
cleaned_df = df \
    .filter(col("salary").isNotNull()) \
    .withColumn("salary_k", col("salary") / 1000)

cleaned_df.createOrReplaceTempView("cleaned_employees")

# Then query with SQL
spark.sql("SELECT dept, AVG(salary_k) FROM cleaned_employees GROUP BY dept").show()
```

---

## 5. Advanced SQL Operations

### 5.1 Joins

Spark supports all standard SQL join types:

```python
# Sample DataFrames
employees = spark.createDataFrame([
    ("E001", "Alice",  1),
    ("E002", "Bob",    2),
    ("E003", "Carol",  1),
    ("E004", "David",  3),
], ["emp_id", "name", "dept_id"])

departments = spark.createDataFrame([
    (1, "Engineering", "Building A"),
    (2, "Marketing",   "Building B"),
    (4, "HR",          "Building C"),
], ["dept_id", "dept_name", "location"])
```

**Inner Join — only rows with a match in both DataFrames:**

```python
inner_df = employees.join(departments, on="dept_id", how="inner")
inner_df.show()
```

```
+-------+-----+-------+------------+----------+
|dept_id| name| emp_id|   dept_name|  location|
+-------+-----+-------+------------+----------+
|      1|Alice|   E001| Engineering|Building A|
|      1|Carol|   E003| Engineering|Building A|
|      2|  Bob|   E002|   Marketing|Building B|
+-------+-----+-------+------------+----------+
# David (dept_id=3) is excluded — no match in departments
# HR (dept_id=4) is excluded — no match in employees
```

**Left Outer Join — all rows from the left; nulls from right where no match:**

```python
left_df = employees.join(departments, on="dept_id", how="left")
left_df.show()
```

```
+-------+-----+-------+-----------+----------+
|dept_id| name| emp_id|  dept_name|  location|
+-------+-----+-------+-----------+----------+
|      1|Alice|   E001|Engineering|Building A|
|      1|Carol|   E003|Engineering|Building A|
|      2|  Bob|   E002|  Marketing|Building B|
|      3|David|   E004|       null|      null|  ← dept_id=3 had no match
+-------+-----+-------+-----------+----------+
```

**Right Outer Join:**

```python
right_df = employees.join(departments, on="dept_id", how="right")
```

**Full Outer Join:**

```python
full_df = employees.join(departments, on="dept_id", how="full")
```

**Cross Join (Cartesian product — use with care):**

```python
cross_df = employees.crossJoin(departments)
```

**Join on multiple columns:**

```python
df1.join(df2, on=["dept_id", "location"], how="inner")
```

**Join with explicit condition (for non-equal joins or different column names):**

```python
df1.join(df2, df1.dept_id == df2.department_id, how="inner")

# Non-equi join
orders.join(ranges,
    (orders.amount >= ranges.low) & (orders.amount < ranges.high),
    how="inner"
)
```

**SQL Joins:**

```sql
%sql
SELECT e.name, e.emp_id, d.dept_name, d.location
FROM employees e
INNER JOIN departments d ON e.dept_id = d.dept_id;

-- Left join
SELECT e.name, d.dept_name
FROM employees e
LEFT OUTER JOIN departments d ON e.dept_id = d.dept_id;
```

---

### 5.2 Aggregations and GROUP BY

```python
from pyspark.sql.functions import (
    count, sum, avg, min, max, countDistinct,
    stddev, variance, collect_list, collect_set,
    first, last, percentile_approx
)

# Basic aggregation with groupBy
df.groupBy("dept").agg(
    count("*").alias("headcount"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    stddev("salary").alias("salary_stddev"),
    countDistinct("team").alias("num_teams"),
).show()

# Collect values into a list per group
df.groupBy("dept").agg(
    collect_list("name").alias("employee_names"),    # includes duplicates
    collect_set("name").alias("unique_names"),       # removes duplicates
).show(truncate=False)

# Approximate percentile (efficient for large datasets)
df.groupBy("dept").agg(
    percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles")
).show()

# HAVING equivalent: filter after groupBy
df.groupBy("dept") \
  .agg(count("*").alias("headcount")) \
  .filter(col("headcount") >= 2) \
  .show()
```

**SQL equivalent:**

```sql
%sql
SELECT dept,
       COUNT(*)                         AS headcount,
       SUM(salary)                      AS total_salary,
       AVG(salary)                      AS avg_salary,
       STDDEV(salary)                   AS salary_stddev,
       COUNT(DISTINCT team)             AS num_teams
FROM employees
GROUP BY dept
HAVING COUNT(*) >= 2
ORDER BY avg_salary DESC;
```

---

### 5.3 Window Functions

Window functions perform calculations across rows related to the current row, without collapsing rows into a single output (unlike `groupBy`).

```python
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead
from pyspark.sql.functions import sum as spark_sum, avg as spark_avg
from pyspark.sql.window import Window

# Define a window specification
# PARTITION BY dept — restart numbering for each dept
# ORDER BY salary DESC — order rows within each partition
window_spec = Window.partitionBy("dept").orderBy(col("salary").desc())

# ROW_NUMBER: unique sequential number within partition
df.withColumn("row_num", row_number().over(window_spec)).show()

# RANK: same number for ties, leaves gaps (1,1,3...)
df.withColumn("salary_rank", rank().over(window_spec)).show()

# DENSE_RANK: same number for ties, no gaps (1,1,2...)
df.withColumn("salary_rank", dense_rank().over(window_spec)).show()
```

**Running totals and moving averages:**

```python
# cumulative/running total of salary within each dept
cumulative_window = Window.partitionBy("dept") \
    .orderBy("salary") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("cumulative_salary", spark_sum("salary").over(cumulative_window)).show()

# 3-row moving average
rolling_window = Window.partitionBy("dept") \
    .orderBy("hire_date") \
    .rowsBetween(-1, 1)  # 1 row before + current + 1 row after

df.withColumn("rolling_avg", spark_avg("salary").over(rolling_window)).show()
```

**LAG and LEAD — access previous/next row:**

```python
# Compare each employee's salary to the previous employee (same dept)
prev_window = Window.partitionBy("dept").orderBy("salary")

df.withColumn("prev_salary", lag("salary", 1).over(prev_window)) \
  .withColumn("salary_diff", col("salary") - col("prev_salary")) \
  .show()

# LEAD — access the next row's value
df.withColumn("next_salary", lead("salary", 1).over(prev_window)).show()
```

**Finding the top-N records per group using window:**

```python
# Top 2 highest-paid employees per department
window_spec = Window.partitionBy("dept").orderBy(col("salary").desc())

top2 = df \
    .withColumn("rank", dense_rank().over(window_spec)) \
    .filter(col("rank") <= 2) \
    .drop("rank")

top2.show()
```

**SQL Window Functions:**

```sql
%sql
SELECT name, dept, salary,
       ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS row_num,
       RANK()       OVER (PARTITION BY dept ORDER BY salary DESC) AS salary_rank,
       SUM(salary)  OVER (PARTITION BY dept)                      AS dept_total,
       LAG(salary)  OVER (PARTITION BY dept ORDER BY salary)      AS prev_salary
FROM employees;
```

---

## 6. Performance Tuning and Optimisation Techniques

### 6.1 Caching and Persistence

Cache a DataFrame when it is used multiple times in the same job:

```python
# Cache in memory (falls back to disk when memory is full)
df.cache()
# equivalent to:
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)

# Force materialisation (cache is lazy — this triggers it)
df.count()

# Check cache status
print(df.is_cached)   # True

# Release from cache
df.unpersist()
```

**Storage levels:**

| Level             | Description                                             |
| ----------------- | ------------------------------------------------------- |
| `MEMORY_ONLY`     | Store in JVM heap; recompute lost partitions            |
| `MEMORY_AND_DISK` | Spill to disk if memory is full (default for `cache()`) |
| `DISK_ONLY`       | Store only on disk                                      |
| `MEMORY_ONLY_2`   | Two replicas in memory for fault tolerance              |
| `OFF_HEAP`        | Store in off-heap memory (requires configuration)       |

**When to cache:**

```
✓  DataFrame used in 2+ actions (count + write, multiple joins, iterative ML)
✓  Expensive transformation result (complex join + aggregation)
✗  DataFrame used only once (wastes memory)
✗  Very large DataFrame that won't fit in cluster memory
```

---

### 6.2 Broadcast Joins

When one DataFrame is small (< 10 MB by default), you can **broadcast** it to every executor, eliminating the expensive shuffle:

```python
from pyspark.sql.functions import broadcast

# Manual broadcast hint
result = large_df.join(broadcast(small_lookup_df), "dept_id")

# Configure the auto-broadcast threshold (default: 10 MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50 MB

# Disable auto-broadcast (for testing / troubleshooting)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

```
Without broadcast (shuffle join):
  Large table (10M rows)  ─── shuffle ──► Both tables redistributed by join key
  Small table (10k rows)  ─── shuffle ──► Expensive network transfer

With broadcast join:
  Large table (10M rows)  ─── stays in place
  Small table (10k rows)  ─── copied to ALL executors
  Result: each executor joins its local large partition with the full small table
  No shuffle required!
```

---

### 6.3 Adaptive Query Execution (AQE)

**AQE** is enabled by default in Databricks Runtime. It re-optimises the query plan at runtime using actual statistics collected during execution.

```python
# Check if AQE is enabled
spark.conf.get("spark.sql.adaptive.enabled")   # "true" in DBR

# AQE features:
# 1. Dynamically coalesce shuffle partitions
#    (combine small post-shuffle partitions into fewer, larger ones)
spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")  # "true"

# 2. Switch join strategies at runtime
#    (auto-convert sort-merge join to broadcast join if one side turns out small)
spark.conf.get("spark.sql.adaptive.localShuffleReader.enabled")  # "true"

# 3. Optimise skew joins
#    (split oversized partitions to balance work across tasks)
spark.conf.get("spark.sql.adaptive.skewJoin.enabled")  # "true"
```

---

### 6.4 Partition Tuning

```python
# Check current partition count
df.rdd.getNumPartitions()

# Tuning shuffle partitions (key for groupBy, join, distinct performance)
# Rule of thumb: target post-shuffle partitions of ~128 MB each
spark.conf.set("spark.sql.shuffle.partitions", "32")

# Default is 200 — often too high for small/medium data
# For large datasets: roughly total_data_size_in_bytes / (128 * 1024 * 1024)

# Repartition to increase partition count (causes shuffle)
df = df.repartition(16)

# Repartition by a column (all rows with the same key go to the same partition)
df = df.repartition(16, "dept")

# Coalesce to reduce partition count (no shuffle — just merge adjacent partitions)
df = df.coalesce(4)

# Write small files problem — coalesce before writing to reduce output files
df.coalesce(1).write.mode("overwrite").parquet("/output/result/")
```

---

### 6.5 Predicate Pushdown and Column Pruning

Spark's Catalyst Optimizer automatically applies these optimizations, but you can help it:

```python
# ✓ Predicate Pushdown — filter EARLY in the query chain
# Spark will push the filter down to the file reader (reads less data from S3/DBFS)
df = spark.read.parquet("/data/orders/") \
    .filter(col("year") == 2024) \
    .filter(col("status") == "completed")

# For partitioned data, filtering on the partition column is FREE (partition pruning)
# Partition layout: /data/orders/year=2024/month=01/...
df = spark.read.parquet("/data/orders/") \
    .filter(col("year") == 2024)   # reads only year=2024 partition — partition pruning!

# ✓ Column Pruning — select only columns you need (reads fewer bytes from storage)
# Parquet is columnar — unused columns are never read from disk
df = spark.read.parquet("/data/orders/") \
    .select("order_id", "customer_id", "total")  # only 3 columns read from disk
    # vs.  .select("*")  — reads all columns
```

---

### 6.6 Explain Plans

Use `explain()` to see what Spark will actually execute:

```python
# Text explain (simplified)
df.groupBy("dept").agg(avg("salary")).explain()

# Verbose explain showing all stages of the plan
df.groupBy("dept").agg(avg("salary")).explain(True)

# Extended explain (Spark 3.x) — shows all plan types
df.groupBy("dept").agg(avg("salary")).explain(mode="extended")

# Cost-based plan with estimated statistics
df.groupBy("dept").agg(avg("salary")).explain(mode="cost")

# Formatted plan (most human-readable)
df.groupBy("dept").agg(avg("salary")).explain(mode="formatted")
```

**Reading an explain plan — read bottom to top:**

```
== Physical Plan ==
AdaptiveSparkPlan (1)
+- HashAggregate (2)         ← Final aggregation on driver/reducer
   +- Exchange (3)           ← Shuffle (this is a wide transformation)
      +- HashAggregate (4)   ← Partial aggregation on each executor BEFORE shuffle
         +- FileScan (5)     ← Reading from storage (Parquet/Delta/CSV)
```

**What to look for:**

- `BroadcastHashJoin` → broadcast join (good for small table joins)
- `SortMergeJoin` → shuffle-based join (expensive for large tables)
- `Exchange` → shuffle operation (check partition count)
- `Filter` at file scan level → predicate pushdown working
- `Project` at file scan level → column pruning working

---

## 7. Summary

| Topic                  | Key Takeaway                                                                   |
| ---------------------- | ------------------------------------------------------------------------------ |
| **DataFrame**          | Distributed, immutable, schema-aware table; the standard unit of work in Spark |
| **createDataFrame**    | Create from lists, Row objects, dicts, pandas, RDDs, or files                  |
| **filter / where**     | Identical methods; use column expressions or SQL strings                       |
| **select**             | Project specific columns; supports expressions and aliases                     |
| **groupBy + agg**      | Aggregate with `count`, `sum`, `avg`, etc.; use `filter` after for HAVING      |
| **join**               | inner, left, right, full, cross; use `broadcast()` for small lookup tables     |
| **Window Functions**   | `row_number`, `rank`, `lag`, `lead`, running totals — no groupBy collapse      |
| **Temp Views**         | `createOrReplaceTempView` exposes a DataFrame as a SQL-queryable table         |
| **Caching**            | Use `cache()` when a DataFrame is used in 2+ actions                           |
| **AQE**                | Enabled by default; auto-optimises shuffle partitions and join strategies      |
| **explain()**          | Inspect the physical plan; look for BroadcastHashJoin and pushed-down filters  |
| **shuffle.partitions** | Default 200 is too high for small data; tune to ~128 MB per partition          |

**Previous guide:** [Guide 04 — Working with Datasets and Notebooks](04-datasets-notebooks-magic-commands.md)

**Continue to:** [Guide 06 — Transforming Data with Apache Spark](06-transforming-data-with-spark.md)
