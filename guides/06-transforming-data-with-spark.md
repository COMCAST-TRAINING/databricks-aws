# Transforming Data with Apache Spark

---

## Table of Contents

1. [Adding Columns to a DataFrame](#1-adding-columns-to-a-dataframe)
   - 1.1 [withColumn — Adding a New Column](#11-withcolumn--adding-a-new-column)
   - 1.2 [Adding Constant and Computed Columns](#12-adding-constant-and-computed-columns)
   - 1.3 [Conditional Columns with `when` / `otherwise`](#13-conditional-columns-with-when--otherwise)
2. [Renaming Columns of a DataFrame](#2-renaming-columns-of-a-dataframe)
3. [Removing Columns from a DataFrame](#3-removing-columns-from-a-dataframe)
4. [Filtering Rows from a DataFrame](#4-filtering-rows-from-a-dataframe)
5. [Joining Multiple DataFrames](#5-joining-multiple-dataframes)
   - 5.1 [Join Types Reference](#51-join-types-reference)
   - 5.2 [Handling Ambiguous Columns After Join](#52-handling-ambiguous-columns-after-join)
   - 5.3 [Chaining Multiple Joins](#53-chaining-multiple-joins)
6. [Aggregation Operations](#6-aggregation-operations)
   - 6.1 [Count and Count Distinct](#61-count-and-count-distinct)
   - 6.2 [Max, Min, Sum, SumDistinct](#62-max-min-sum-sumdistinct)
   - 6.3 [Average and Mean](#63-average-and-mean)
   - 6.4 [Grouping Data](#64-grouping-data)
   - 6.5 [Grouping with Multiple Aggregations](#65-grouping-with-multiple-aggregations)
7. [Apache Spark Architecture: How Spark Transforms Data Internally](#7-apache-spark-architecture-how-spark-transforms-data-internally)
   - 7.1 [Narrow vs Wide Transformations](#71-narrow-vs-wide-transformations)
   - 7.2 [The Shuffle Process in Detail](#72-the-shuffle-process-in-detail)
   - 7.3 [Pipelining Narrow Transformations](#73-pipelining-narrow-transformations)
   - 7.4 [Stage Boundary Example](#74-stage-boundary-example)
8. [User Defined Functions (UDFs)](#8-user-defined-functions-udfs)
   - 8.1 [Creating a Python UDF](#81-creating-a-python-udf)
   - 8.2 [UDF Performance Limitations](#82-udf-performance-limitations)
   - 8.3 [Pandas UDFs (Vectorized UDFs)](#83-pandas-udfs-vectorized-udfs)
   - 8.4 [SQL UDFs (Permanent UDFs)](#84-sql-udfs-permanent-udfs)
9. [Summary](#9-summary)

---

## 1. Adding Columns to a DataFrame

### 1.1 `withColumn` — Adding a New Column

`withColumn(colName, col_expression)` returns a new DataFrame with an additional column (or replaces an existing column if the name already exists).

```python
from pyspark.sql.functions import col, lit, round, upper, current_date

# Load sample data
df = spark.createDataFrame([
    ("E001", "Alice",  65000.0, "Engineering", "2019-03-15"),
    ("E002", "Bob",    48000.0, "Marketing",   "2021-07-01"),
    ("E003", "Carol",  72000.0, "Engineering", "2017-11-20"),
    ("E004", "David",  55000.0, "Finance",     "2020-04-10"),
    ("E005", "Eve",    82000.0, "Engineering", "2015-08-05"),
], ["emp_id", "name", "salary", "dept", "hire_date"])

df.printSchema()
df.show()
```

```python
# Add a column as a literal (constant value)
df2 = df.withColumn("company", lit("Acme Corp"))

# Add a computed column
df3 = df.withColumn("annual_bonus", col("salary") * 0.15)

# Add a rounded column
df4 = df.withColumn("salary_rounded", round(col("salary"), -3))

# Add a column based on another column
df5 = df.withColumn("name_upper", upper(col("name")))

# Add today's date as a column
df6 = df.withColumn("snapshot_date", current_date())

# Chaining multiple withColumn calls
df_transformed = df \
    .withColumn("annual_bonus",   col("salary") * 0.15) \
    .withColumn("total_comp",     col("salary") + col("annual_bonus")) \
    .withColumn("name_upper",     upper(col("name"))) \
    .withColumn("snapshot_date",  current_date())

df_transformed.show()
```

**Output:**

```
+------+-----+-------+--------+----------+----------+-----------+-----------+-------------+
|emp_id| name| salary|    dept| hire_date|ann_bonus |total_comp |  name_upper|snapshot_date|
+------+-----+-------+--------+----------+----------+-----------+-----------+-------------+
|  E001|Alice|65000.0|Engineer|2019-03-15|  9750.0  |  74750.0  |      ALICE|   2024-01-15|
...
```

> **Note:** `withColumn` creates a new DataFrame every call — it does NOT modify the original DataFrame (DataFrames are immutable). Chain multiple `withColumn` calls rather than writing to intermediate variables to avoid unnecessary plan complexity.

---

### 1.2 Adding Constant and Computed Columns

```python
from pyspark.sql.functions import (
    lit, col, sqrt, abs, log, pow, expr,
    year, month, dayofmonth, datediff, current_date, to_date
)

# lit() — wrap a Python scalar as a Spark column literal
df.withColumn("tax_rate",     lit(0.30)).show()
df.withColumn("is_active",    lit(True)).show()
df.withColumn("version",      lit(1)).show()

# Mathematical operations
df.withColumn("salary_sqrt",  sqrt(col("salary"))).show()
df.withColumn("salary_log",   log(col("salary"))).show()
df.withColumn("salary_sq",    pow(col("salary"), 2)).show()

# Date extraction (requires hire_date to be a DateType)
df2 = df.withColumn("hire_date_parsed", to_date(col("hire_date"), "yyyy-MM-dd")) \
        .withColumn("hire_year",  year(col("hire_date_parsed"))) \
        .withColumn("hire_month", month(col("hire_date_parsed"))) \
        .withColumn("tenure_days", datediff(current_date(), col("hire_date_parsed")))

df2.select("name", "hire_date_parsed", "hire_year", "hire_month", "tenure_days").show()

# expr() — write any SQL expression as a string
df.withColumn("senior",        expr("tenure_days > 1825")).show()   # > 5 years
df.withColumn("salary_band",   expr("CASE WHEN salary < 50000 THEN 'Low' "
                                    "     WHEN salary < 70000 THEN 'Mid' "
                                    "     ELSE 'High' END")).show()
```

---

### 1.3 Conditional Columns with `when` / `otherwise`

`when(condition, value).otherwise(default_value)` is the PySpark equivalent of SQL's `CASE WHEN`:

```python
from pyspark.sql.functions import when, col

# Simple binary condition
df.withColumn(
    "seniority",
    when(col("tenure_days") > 1825, "Senior")
    .otherwise("Junior")
).show()

# Multiple conditions (chain .when() calls)
df.withColumn(
    "salary_band",
    when(col("salary") < 50000, "Low")
    .when(col("salary") < 70000, "Mid")
    .when(col("salary") < 90000, "High")
    .otherwise("Executive")
).show()

# Nested conditions
df.withColumn(
    "performance_bonus",
    when((col("dept") == "Engineering") & (col("salary") > 70000), col("salary") * 0.20)
    .when((col("dept") == "Engineering"), col("salary") * 0.15)
    .when(col("dept") == "Sales", col("salary") * 0.25)   # Sales gets big bonuses
    .otherwise(col("salary") * 0.10)
).show()

# Using when/otherwise without otherwise (returns null when no condition matches)
df.withColumn(
    "flag",
    when(col("dept") == "Engineering", "tech_team")
    # no .otherwise() → returns null for non-Engineering
).show()
```

---

## 2. Renaming Columns of a DataFrame

```python
# Rename a single column
df.withColumnRenamed("emp_id", "employee_id").show()

# Rename multiple columns — chain withColumnRenamed calls
df \
    .withColumnRenamed("emp_id", "employee_id") \
    .withColumnRenamed("dept",   "department") \
    .show()

# Rename using select + alias (more concise for many columns)
df.select(
    col("emp_id").alias("employee_id"),
    col("name").alias("full_name"),
    col("salary").alias("annual_salary"),
    col("dept").alias("department"),
).show()

# Rename all columns using a mapping dictionary
rename_map = {
    "emp_id":     "employee_id",
    "name":       "full_name",
    "salary":     "annual_salary",
    "dept":       "department",
    "hire_date":  "start_date",
}

df_renamed = df
for old_name, new_name in rename_map.items():
    df_renamed = df_renamed.withColumnRenamed(old_name, new_name)

df_renamed.show()

# Rename columns to snake_case programmatically
import re
df_snake = df.toDF(*[re.sub(r'(?<!^)(?=[A-Z])', '_', c).lower() for c in df.columns])
```

---

## 3. Removing Columns from a DataFrame

```python
# Drop a single column
df.drop("hire_date").show()

# Drop multiple columns
df.drop("hire_date", "company", "snapshot_date").show()

# Drop columns using a list
cols_to_drop = ["hire_date", "company"]
df.drop(*cols_to_drop).show()

# Keep specific columns (select is often cleaner than drop for many removals)
keep_cols = ["emp_id", "name", "salary"]
df.select(keep_cols).show()

# Drop columns with a specific prefix (e.g., temp_ columns after joins)
temp_cols = [c for c in df.columns if c.startswith("temp_")]
df.drop(*temp_cols).show()
```

---

## 4. Filtering Rows from a DataFrame

```python
from pyspark.sql.functions import col

# Basic comparison filters
df.filter(col("salary") > 60000).show()
df.where(col("dept") == "Engineering").show()   # where is identical to filter

# Multiple conditions
df.filter((col("salary") > 60000) & (col("dept") == "Engineering")).show()
df.filter((col("dept") == "Engineering") | (col("dept") == "Finance")).show()

# IN / NOT IN
df.filter(col("dept").isin(["Engineering", "Finance"])).show()
df.filter(~col("dept").isin(["HR", "Admin"])).show()

# NULL checks
df.filter(col("salary").isNull()).show()
df.filter(col("salary").isNotNull()).show()

# String filters
df.filter(col("name").startswith("A")).show()
df.filter(col("name").endswith("e")).show()
df.filter(col("name").contains("al")).show()
df.filter(col("name").like("A%")).show()          # SQL LIKE — % = any characters
df.filter(col("emp_id").rlike("^E00[1-3]$")).show()  # Regex

# Between (inclusive)
df.filter(col("salary").between(50000, 70000)).show()

# Using SQL string (shorthand)
df.filter("salary > 60000 AND dept = 'Engineering'").show()

# Filter using Spark SQL
df.createOrReplaceTempView("employees")
spark.sql("SELECT * FROM employees WHERE salary > 60000").show()
```

---

## 5. Joining Multiple DataFrames

### 5.1 Join Types Reference

```python
employees = spark.createDataFrame([
    ("E001", "Alice",  1, 65000.0),
    ("E002", "Bob",    2, 48000.0),
    ("E003", "Carol",  1, 72000.0),
    ("E004", "David",  3, 55000.0),
], ["emp_id", "name", "dept_id", "salary"])

departments = spark.createDataFrame([
    (1, "Engineering", "Building A"),
    (2, "Marketing",   "Building B"),
    (4, "HR",          "Building C"),
], ["dept_id", "dept_name", "location"])

projects = spark.createDataFrame([
    ("E001", "Alpha"),
    ("E001", "Beta"),
    ("E003", "Alpha"),
    ("E005", "Gamma"),     # E005 does not exist in employees
], ["emp_id", "project"])
```

**Inner Join:**

```python
# Returns rows that have a match in BOTH DataFrames
df_inner = employees.join(departments, on="dept_id", how="inner")
df_inner.show()
# Alice, Bob, Carol are included; David (dept_id=3) and HR (dept_id=4) excluded
```

**Left Outer Join:**

```python
# All rows from left; nulls for right columns where no match
df_left = employees.join(departments, on="dept_id", how="left")
# or how="left_outer"
df_left.show()
# David included with null dept_name and location
```

**Right Outer Join:**

```python
df_right = employees.join(departments, on="dept_id", how="right")
# HR included with null emp_id, name, salary
```

**Full Outer Join:**

```python
df_full = employees.join(departments, on="dept_id", how="full")
# or how="full_outer"
# All rows from both sides; nulls where no match
```

**Left Semi Join (EXISTS):**

```python
# Returns only rows from left where a match exists in right (no right columns in result)
df_semi = employees.join(departments, on="dept_id", how="left_semi")
# Returns Alice, Bob, Carol — but NOT department columns
```

**Left Anti Join (NOT EXISTS):**

```python
# Returns only rows from left where NO match exists in right
df_anti = employees.join(departments, on="dept_id", how="left_anti")
# Returns only David (dept_id=3 has no match)
```

**Cross Join:**

```python
# Every row from left × every row from right (Cartesian product)
# 4 employees × 3 departments = 12 rows
df_cross = employees.crossJoin(departments)
```

---

### 5.2 Handling Ambiguous Columns After Join

When both DataFrames have columns with the same name, the result contains duplicate column names, causing errors in subsequent operations:

```python
# Problem: both DataFrames have "dept_id"
df_joined = employees.join(departments, employees.dept_id == departments.dept_id)
# Now df_joined has TWO columns named "dept_id" — causes errors in .select() or .filter()

# Solution 1: Use string-form join key (Spark drops the duplicate key column)
df_joined = employees.join(departments, on="dept_id", how="inner")
# Only ONE dept_id column in result ✓

# Solution 2: Use join condition and then drop the duplicate
df_joined = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    how="inner"
).drop(departments.dept_id)

# Solution 3: Rename columns before joining
departments_renamed = departments \
    .withColumnRenamed("dept_id", "d_dept_id") \
    .withColumnRenamed("dept_name", "department_name")

df_joined = employees.join(
    departments_renamed,
    employees.dept_id == departments_renamed.d_dept_id
).drop("d_dept_id")

# Solution 4: Use alias() on the DataFrames
e = employees.alias("e")
d = departments.alias("d")
df_joined = e.join(d, col("e.dept_id") == col("d.dept_id"), "inner") \
             .select("e.*", col("d.dept_name"), col("d.location"))
```

---

### 5.3 Chaining Multiple Joins

```python
# 3-way join: employees + departments + projects
result = employees \
    .join(departments, on="dept_id", how="left") \
    .join(projects,    on="emp_id",  how="left") \
    .select(
        col("emp_id"),
        col("name"),
        col("dept_name"),
        col("project"),
        col("salary"),
    ) \
    .orderBy("emp_id")

result.show()

# In SQL (equivalent)
employees.createOrReplaceTempView("emp")
departments.createOrReplaceTempView("dept")
projects.createOrReplaceTempView("proj")

spark.sql("""
    SELECT e.emp_id, e.name, d.dept_name, p.project, e.salary
    FROM emp e
    LEFT JOIN dept d  ON e.dept_id = d.dept_id
    LEFT JOIN proj p  ON e.emp_id  = p.emp_id
    ORDER BY e.emp_id
""").show()
```

---

## 6. Aggregation Operations

### 6.1 Count and Count Distinct

```python
from pyspark.sql.functions import count, countDistinct, approx_count_distinct

# Count all rows (including nulls)
df.select(count("*")).show()          # counts all rows
df.count()                             # action — returns integer directly

# Count non-null values in a column
df.select(count("salary")).show()     # skips nulls

# Count distinct values
df.select(countDistinct("dept")).show()

# Approximate count distinct (faster for large datasets, ~3% error by default)
df.select(approx_count_distinct("emp_id", rsd=0.05)).show()

# Count distinct within a group
df.groupBy("dept").agg(
    count("*").alias("total"),
    countDistinct("team_id").alias("unique_teams"),
).show()
```

---

### 6.2 Max, Min, Sum, SumDistinct

```python
from pyspark.sql.functions import max, min, sum, sumDistinct

# Global aggregations
df.select(
    max("salary").alias("max_salary"),
    min("salary").alias("min_salary"),
    sum("salary").alias("total_salary"),
).show()

# sumDistinct — sum of unique values only (less common)
df.select(sumDistinct("dept_id").alias("sum_distinct_dept_ids")).show()

# Per group
df.groupBy("dept").agg(
    max("salary").alias("top_salary"),
    min("salary").alias("entry_salary"),
    sum("salary").alias("total_payroll"),
).show()

# Max/Min of non-numeric columns (returns lexicographic max/min for strings)
df.select(max("name")).show()   # Returns "Eve" (last alphabetically)
df.select(min("name")).show()   # Returns "Alice"
```

---

### 6.3 Average and Mean

`avg` and `mean` are aliases for the same function:

```python
from pyspark.sql.functions import avg, mean

# Global average
df.select(avg("salary").alias("avg_salary")).show()
df.select(mean("salary").alias("mean_salary")).show()   # identical to avg

# Grouped average
df.groupBy("dept").agg(
    avg("salary").alias("avg_salary"),
    mean("salary").alias("mean_salary"),  # same result as avg
).show()
```

---

### 6.4 Grouping Data

```python
from pyspark.sql.functions import count, avg, sum

# Simple groupBy — single column
df.groupBy("dept").count().show()

# GroupBy multiple columns
df.groupBy("dept", "location").count().show()

# GroupBy with ordering
df.groupBy("dept") \
  .count() \
  .orderBy("count", ascending=False) \
  .show()
```

**GroupBy with `rollup` (hierarchical subtotals):**

```python
# rollup adds subtotal rows for each grouping level
df.rollup("dept", "location") \
  .agg(count("*").alias("headcount"), sum("salary").alias("total_salary")) \
  .orderBy("dept", "location") \
  .show()
# Produces: dept+location subtotals, dept subtotals, grand total (null rows = subtotals)
```

**GroupBy with `cube` (all combinations of subtotals):**

```python
# cube adds a row for every subset of the grouping columns
df.cube("dept", "location") \
  .agg(count("*").alias("headcount")) \
  .orderBy("dept", "location") \
  .show()
# Produces: all combinations: dept+location, dept only, location only, grand total
```

**`pivot` — transform unique values in a column into multiple columns:**

```python
# Sales data per product per month
sales = spark.createDataFrame([
    ("Jan", "Widget", 100),
    ("Jan", "Gadget", 200),
    ("Feb", "Widget", 150),
    ("Feb", "Gadget", 180),
    ("Mar", "Widget", 130),
], ["month", "product", "units"])

# Pivot product values to become column headers
sales.groupBy("month") \
     .pivot("product") \
     .agg(sum("units")) \
     .orderBy("month") \
     .show()
```

```
+-----+------+------+
|month|Gadget|Widget|
+-----+------+------+
|  Feb|   180|   150|
|  Jan|   200|   100|
|  Mar|  null|   130|
+-----+------+------+
```

---

### 6.5 Grouping with Multiple Aggregations

```python
from pyspark.sql.functions import (
    count, countDistinct, avg, sum, min, max,
    stddev, collect_list, collect_set,
    percentile_approx, first, last
)

df.groupBy("dept").agg(
    # Counts
    count("*").alias("headcount"),
    countDistinct("team_id").alias("num_teams"),

    # Salary metrics
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_payroll"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    stddev("salary").alias("salary_stddev"),
    percentile_approx("salary", 0.5).alias("median_salary"),

    # Collectting values
    collect_set("name").alias("employee_names"),  # unique names as array
    first("hire_date").alias("earliest_hire"),     # first row's value (non-deterministic)
    last("hire_date").alias("latest_hire"),         # last row's value (non-deterministic)
).show(truncate=False)
```

---

## 7. Apache Spark Architecture: How Spark Transforms Data Internally

### 7.1 Narrow vs Wide Transformations

Every DataFrame transformation is classified as either **narrow** or **wide** based on how data flows between partitions:

```
NARROW Transformation                WIDE Transformation
─────────────────────                ───────────────────────
  Partition 0 → Partition 0            Partition 0 ──┐
  Partition 1 → Partition 1            Partition 1 ──┼──► NEW Partition 0
  Partition 2 → Partition 2            Partition 2 ──┘     (shuffled data)
                                       ...
  Each output partition depends        Each output partition may depend on
  on exactly ONE input partition.      MULTIPLE input partitions.
  NO data movement across nodes.       REQUIRES data movement (shuffle).
```

**Narrow transformations (no shuffle):**

| Operation          | Description                                          |
| ------------------ | ---------------------------------------------------- |
| `filter` / `where` | Remove rows — each partition processed independently |
| `select` / `drop`  | Project columns — no row movement                    |
| `withColumn`       | Add/modify column — no row movement                  |
| `map` (RDD)        | Row-by-row transformation                            |
| `flatMap` (RDD)    | One-to-many transformation                           |
| `union`            | Append two DataFrames (requires same schema)         |
| `coalesce`         | Merge partitions without full shuffle                |

**Wide transformations (cause a shuffle):**

| Operation              | Description                                                   |
| ---------------------- | ------------------------------------------------------------- |
| `groupBy` + `agg`      | Rows with the same key must be on the same partition          |
| `join` (non-broadcast) | Rows with matching keys must meet on the same partition       |
| `orderBy` / `sort`     | Global sort requires all data to be ordered across partitions |
| `distinct`             | Duplicates may be in different partitions — must consolidate  |
| `repartition(n)`       | Force redistribution across N new partitions                  |
| `pivot`                | Must reshuffle data by the pivot column                       |

---

### 7.2 The Shuffle Process in Detail

A **shuffle** happens at **stage boundaries** — it is the most expensive operation in Spark:

```
STAGE 1: Partial Aggregation (Map side)
────────────────────────────────────────
  Each executor reads its partitions and computes PARTIAL aggregations.
  For groupBy("dept").count():

  Executor 1 (Partitions 0-2):
    Engineering → 3,  Marketing → 1

  Executor 2 (Partitions 3-5):
    Engineering → 2,  Finance → 4

  Results written to local disk as "shuffle files".

                    SHUFFLE BOUNDARY
               ↓↓↓  Data moves across network  ↓↓↓

STAGE 2: Final Aggregation (Reduce side)
─────────────────────────────────────────
  All partial Engineering counts sent to Reducer 0:  3 + 2 = 5
  All partial Marketing counts sent to Reducer 1:    1
  All partial Finance counts sent to Reducer 2:      4
```

**What happens during a shuffle:**

1. **Map side**: Each executor writes its shuffle output files partitioned by the target partition key (hash of the join/group key).
2. **Shuffle service**: Files are advertised to the cluster; the Databricks External Shuffle Service manages file access.
3. **Reduce side**: Each executor fetches its assigned shuffle partitions from all other executors.
4. **Sort and merge**: Incoming data is sorted or merged for the final aggregation/join.

**Cost of a shuffle:**

- Network I/O (data movement between nodes)
- Disk I/O (shuffle writes to local SSD, read back during fetch)
- CPU (sorting and merging)

---

### 7.3 Pipelining Narrow Transformations

Spark **pipelines** multiple narrow transformations into a single stage — they are applied together in one pass over the data, without any intermediate materialisation:

```python
# These three narrow transformations...
df.filter(col("salary") > 40000) \
  .withColumn("dept_upper", upper(col("dept"))) \
  .select("emp_id", "name", "dept_upper", "salary")

# ...are compiled into ONE task that does:
#  Read partition → filter → compute upper → project columns → output
#
# No intermediate DataFrame is materialised in memory or on disk.
# This is called "whole-stage code generation" (Tungsten).
```

```
Without pipelining (hypothetical):                With pipelining (actual Spark):
──────────────────────────────────               ──────────────────────────────────
  Pass 1: Read + filter → write temp             Pass 1: Read + filter + upper + project
  Pass 2: Read temp + compute upper → write             → direct to output
  Pass 3: Read temp + project → output
  (3 passes, 2 intermediate materializations)    (1 pass, 0 intermediate materializations)
```

---

### 7.4 Stage Boundary Example

```python
# Example pipeline
orders = spark.read.parquet("/data/orders/")           # Stage 1 starts
customers = spark.read.parquet("/data/customers/")     # Stage 1 starts

# Narrow: filter (still Stage 1)
filtered_orders = orders.filter(col("status") == "completed")

# Narrow: select (still Stage 1)
selected_orders = filtered_orders.select("order_id", "customer_id", "total")

# WIDE: join — Stage 1 ends here, Stage 2 begins
joined = selected_orders.join(customers, on="customer_id", how="inner")

# Narrow: withColumn (still Stage 2)
with_tax = joined.withColumn("tax", col("total") * 0.08)

# WIDE: groupBy — Stage 2 ends, Stage 3 begins
final = with_tax.groupBy("state").agg(
    count("*").alias("orders"),
    sum("total").alias("revenue"),
)

# Action triggers execution of ALL stages
final.show()
```

```
Job → Stage 1: read orders + filter + select  (narrow, parallelised)
              + read customers                (narrow, parallelised)
              [SHUFFLE — join]
   → Stage 2: join result + withColumn        (narrow, post-join)
              [SHUFFLE — groupBy]
   → Stage 3: final aggregation per state
```

Visualise this in the Spark UI (SQL tab) after running the code.

---

## 8. User Defined Functions (UDFs)

### 8.1 Creating a Python UDF

When built-in Spark functions are not sufficient, you can define custom Python logic as a **UDF**:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# Step 1: Define a regular Python function
def classify_salary(salary):
    if salary is None:
        return "Unknown"
    elif salary < 50000:
        return "Low"
    elif salary < 75000:
        return "Mid"
    else:
        return "High"

# Step 2: Register it as a UDF
classify_salary_udf = udf(classify_salary, StringType())

# Step 3: Use it in a transformation
df.withColumn("salary_band", classify_salary_udf(col("salary"))).show()
```

**Using the decorator syntax (more concise):**

```python
@udf(returnType=StringType())
def classify_salary(salary):
    if salary is None:
        return "Unknown"
    elif salary < 50000:
        return "Low"
    elif salary < 75000:
        return "Mid"
    else:
        return "High"

df.withColumn("salary_band", classify_salary(col("salary"))).show()
```

**UDFs with multiple arguments:**

```python
@udf(returnType=StringType())
def format_name(first, last):
    if first is None or last is None:
        return None
    return f"{last.upper()}, {first.title()}"

df.withColumn("formatted_name", format_name(col("first_name"), col("last_name"))).show()
```

---

### 8.2 UDF Performance Limitations

Python UDFs have a significant performance penalty compared to native Spark functions:

```
Native Spark function (col() / built-in)
  ─────────────────────────────────────────
  • Runs inside the JVM (Scala/Java code)
  • Tungsten whole-stage code generation
  • Vectorized execution
  • No serialization overhead

Python UDF
  ─────────────────────────────────────────
  • Spark serializes each Row to Python pickle format
  • Row sent to Python interpreter (separate process)
  • Python function executes
  • Result pickled back to JVM
  • For every row — massive per-row serialization tax
  • Cannot be optimised by Catalyst (black box)
```

**Performance comparison:**

| Function type              | Relative Speed             | Catalyst Optimization            |
| -------------------------- | -------------------------- | -------------------------------- |
| Built-in Spark functions   | 1× (baseline — fastest)    | Yes                              |
| SQL UDFs (via Spark SQL)   | ~1×                        | Yes (for SQL UDFs in Databricks) |
| Pandas UDF (vectorized)    | ~2–5× slower than built-in | Partial                          |
| Python UDF (row-at-a-time) | ~10–100× slower            | No (black box)                   |

**Rule:** Always try to implement logic using **built-in Spark functions** before resorting to UDFs. Check `pyspark.sql.functions` — there are over 300 built-in functions.

---

### 8.3 Pandas UDFs (Vectorized UDFs)

**Pandas UDFs** (also called vectorized UDFs) operate on **batches of rows** as pandas Series or DataFrames, dramatically reducing the serialization overhead of row-at-a-time UDFs.

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, DoubleType

# Scalar Pandas UDF — receives a pandas Series, returns a pandas Series
@pandas_udf(StringType())
def classify_salary_vectorized(salary_series: pd.Series) -> pd.Series:
    def classify(s):
        if pd.isna(s):
            return "Unknown"
        elif s < 50000:
            return "Low"
        elif s < 75000:
            return "Mid"
        else:
            return "High"
    return salary_series.apply(classify)

df.withColumn("salary_band", classify_salary_vectorized(col("salary"))).show()
```

**Pandas UDF for complex string operations:**

```python
import re

@pandas_udf(StringType())
def extract_domain(email_series: pd.Series) -> pd.Series:
    return email_series.str.extract(r'@(.+)$', expand=False)

df.withColumn("email_domain", extract_domain(col("email"))).show()
```

**Group Map Pandas UDF — apply a pandas operation per group:**

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

output_schema = StructType([
    StructField("emp_id",       StringType(), True),
    StructField("name",         StringType(), True),
    StructField("salary",       DoubleType(), True),
    StructField("dept",         StringType(), True),
    StructField("salary_rank",  DoubleType(), True),
])

@pandas_udf(output_schema)
def rank_within_dept(dept_df: pd.DataFrame) -> pd.DataFrame:
    dept_df["salary_rank"] = dept_df["salary"].rank(ascending=False)
    return dept_df

result = df.groupBy("dept").applyInPandas(rank_within_dept, schema=output_schema)
result.show()
```

---

### 8.4 SQL UDFs (Permanent UDFs)

SQL UDFs are stored in the **Unity Catalog** or **Hive Metastore** and are accessible across all notebooks and sessions:

```python
# Register a UDF for use in Spark SQL
spark.udf.register("classify_salary", classify_salary, StringType())

# Now use it in any SQL query
spark.sql("""
    SELECT name, salary, classify_salary(salary) AS salary_band
    FROM employees
""").show()
```

**Create a permanent SQL UDF (Unity Catalog):**

```sql
%sql
CREATE OR REPLACE FUNCTION training.classify_salary(salary DOUBLE)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Classifies salary into Low/Mid/High bands'
RETURN CASE
    WHEN salary IS NULL     THEN 'Unknown'
    WHEN salary < 50000     THEN 'Low'
    WHEN salary < 75000     THEN 'Mid'
    ELSE                         'High'
END;

-- Use it
SELECT name, salary, training.classify_salary(salary) AS salary_band
FROM employees;
```

**Permanent Python UDF (stored in Unity Catalog):**

```sql
%sql
CREATE OR REPLACE FUNCTION training.reverse_string(s STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
def reverse_string(s):
    if s is None:
        return None
    return s[::-1]
$$;

-- Use it
SELECT name, training.reverse_string(name) AS reversed_name FROM employees;
```

---

## 9. Summary

| Operation                 | Method                                                   | Key Notes                                        |
| ------------------------- | -------------------------------------------------------- | ------------------------------------------------ |
| **Add column**            | `withColumn("col_name", expression)`                     | Returns new DataFrame; chains multiple calls     |
| **Conditional column**    | `when(cond, val).when(...).otherwise(default)`           | SQL CASE WHEN equivalent                         |
| **Rename column**         | `withColumnRenamed(old, new)`                            | Or use `select(col.alias(new))`                  |
| **Remove column**         | `drop("col1", "col2")`                                   | Or use `select(keep_columns)`                    |
| **Filter rows**           | `filter(condition)` or `where(condition)`                | Identical methods; supports SQL strings          |
| **Join**                  | `df1.join(df2, on, how)`                                 | Use string key to avoid duplicate columns        |
| **count**                 | `count("col")` skips nulls; `count("*")` counts all rows |                                                  |
| **countDistinct**         | `countDistinct("col")`                                   | Exact; use `approx_count_distinct` for scale     |
| **sum / sumDistinct**     | `sum("col")` / `sumDistinct("col")`                      | sumDistinct sums only unique values              |
| **avg / mean**            | Identical — both compute arithmetic mean                 |                                                  |
| **groupBy**               | `df.groupBy("col").agg(...)`                             | Use `rollup` / `cube` for subtotals              |
| **pivot**                 | `groupBy(...).pivot("col").agg(...)`                     | Converts unique values to columns                |
| **Narrow transformation** | No shuffle; runs in same stage                           | filter, select, withColumn, union, coalesce      |
| **Wide transformation**   | Causes shuffle; stage boundary                           | groupBy, join, orderBy, distinct, repartition    |
| **Python UDF**            | `@udf(returnType)`                                       | Slow — 10-100× overhead; avoid if possible       |
| **Pandas UDF**            | `@pandas_udf(returnType)`                                | Vectorized — much faster than row-at-a-time UDF  |
| **SQL UDF**               | `spark.udf.register(...)`                                | Accessible in SQL; permanent via CREATE FUNCTION |

**Previous guide:** [Guide 05 — DataFrames and Spark SQL Fundamentals](05-dataframes-spark-sql-fundamentals.md)

**Continue to:** [Guide 07 — Exploring Different Data Types in Spark](07-data-types-in-spark.md)
