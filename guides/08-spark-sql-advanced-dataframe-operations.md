# Spark SQL and Advanced DataFrame Operations

---

## Table of Contents

1. [Running SQL on a DataFrame: TempView and GlobalView](#1-running-sql-on-a-dataframe-tempview-and-globalview)
   - 1.1 [Creating and Using Temporary Views](#11-creating-and-using-temporary-views)
   - 1.2 [Global Temporary Views](#12-global-temporary-views)
   - 1.3 [Temp View vs Global View vs Delta Table](#13-temp-view-vs-global-view-vs-delta-table)
2. [Managing Databases: List, Create, Delete, Select](#2-managing-databases-list-create-delete-select)
3. [Managing Tables: Unmanaged and Managed Tables](#3-managing-tables-unmanaged-and-managed-tables)
   - 3.1 [Managed Tables](#31-managed-tables)
   - 3.2 [Unmanaged (External) Tables](#32-unmanaged-external-tables)
   - 3.3 [Managed vs Unmanaged — Key Differences](#33-managed-vs-unmanaged--key-differences)
   - 3.4 [Table Management Commands](#34-table-management-commands)
4. [SQL Fundamentals](#4-sql-fundamentals)
   - 4.1 [SELECT Clause](#41-select-clause)
   - 4.2 [WHERE Clause](#42-where-clause)
   - 4.3 [Handling NULLs](#43-handling-nulls)
   - 4.4 [Aggregations](#44-aggregations)
   - 4.5 [GROUP BY](#45-group-by)
   - 4.6 [HAVING](#46-having)
   - 4.7 [ORDER BY](#47-order-by)
5. [SQL Joins](#5-sql-joins)
   - 5.1 [Inner Join](#51-inner-join)
   - 5.2 [Left Outer Join](#52-left-outer-join)
   - 5.3 [Right Outer Join](#53-right-outer-join)
   - 5.4 [Full Outer, Semi, Anti Joins](#54-full-outer-semi-anti-joins)
6. [SQL Fundamentals: Predicates, Operators, and CASE Expressions](#6-sql-fundamentals-predicates-operators-and-case-expressions)
   - 6.1 [Comparison Operators](#61-comparison-operators)
   - 6.2 [Logical Operators](#62-logical-operators)
   - 6.3 [Arithmetic Operators](#63-arithmetic-operators)
   - 6.4 [String Operators and Functions](#64-string-operators-and-functions)
   - 6.5 [Predicates: IN, BETWEEN, LIKE, IS NULL](#65-predicates-in-between-like-is-null)
   - 6.6 [CASE Expressions](#66-case-expressions)
   - 6.7 [Subqueries](#67-subqueries)
7. [Summary](#7-summary)

---

## 1. Running SQL on a DataFrame: TempView and GlobalView

### 1.1 Creating and Using Temporary Views

A **Temporary View** registers a DataFrame as a named SQL table scoped to the current SparkSession. Once registered, it can be queried with `spark.sql()` exactly like a permanent table.

```python
# Load data
employees = spark.createDataFrame([
    ("E001", "Alice",   65000.0, "Engineering", "New York"),
    ("E002", "Bob",     48000.0, "Marketing",   "Chicago"),
    ("E003", "Carol",   72000.0, "Engineering", "New York"),
    ("E004", "David",   55000.0, "Finance",     "Boston"),
    ("E005", "Eve",     82000.0, "Engineering", "Seattle"),
    ("E006", "Frank",   41000.0, "Marketing",   "Chicago"),
    ("E007", "Grace",   91000.0, "Finance",     "New York"),
], ["emp_id", "name", "salary", "dept", "city"])

# Register as a temporary view
employees.createOrReplaceTempView("employees")

# Now query it with SQL
result = spark.sql("SELECT * FROM employees WHERE salary > 60000")
result.show()
```

**View management:**

```python
# Create view (fails if already exists)
employees.createTempView("employees")

# Create or replace (safe to run repeatedly — idempotent)
employees.createOrReplaceTempView("employees")

# Drop a view
spark.catalog.dropTempView("employees")

# Check if a view exists
spark.catalog.tableExists("employees")    # True / False

# List all registered views and tables in the current database
spark.catalog.listTables()
for table in spark.catalog.listTables():
    print(f"{table.name:20} | isTemp: {table.isTemporary}")
```

---

### 1.2 Global Temporary Views

A **Global Temporary View** is accessible across all SparkSessions on the same cluster. It is stored in the special `global_temp` database.

```python
# Create a global temporary view
employees.createOrReplaceGlobalTempView("employees_global")

# Access via global_temp.<view_name>
spark.sql("SELECT * FROM global_temp.employees_global WHERE dept = 'Finance'").show()

# In a different SparkSession (same cluster)
new_session = spark.newSession()
new_session.sql("SELECT COUNT(*) FROM global_temp.employees_global").show()

# Drop
spark.catalog.dropGlobalTempView("employees_global")
```

---

### 1.3 Temp View vs Global View vs Delta Table

| Feature              | Temp View                 | Global Temp View                  | Delta Table                |
| -------------------- | ------------------------- | --------------------------------- | -------------------------- |
| **Scope**            | Current SparkSession      | All sessions on cluster           | Permanent (stored on disk) |
| **Lifetime**         | Until session ends        | Until cluster restarts            | Until explicitly dropped   |
| **Data stored**      | No — pointer to DataFrame | No — pointer to DataFrame         | Yes — on DBFS/S3           |
| **Access syntax**    | `FROM view_name`          | `FROM global_temp.view_name`      | `FROM db.table_name`       |
| **Survives restart** | No                        | No                                | Yes                        |
| **Suitable for**     | Intermediate query steps  | Cross-session sharing (ephemeral) | Production tables          |

---

## 2. Managing Databases: List, Create, Delete, Select

In Databricks, **databases** (also called **schemas**) are namespaces that contain tables, views, and functions. They map to directories in the configured metastore.

```sql
%sql

-- List all databases
SHOW DATABASES;
SHOW SCHEMAS;           -- same as SHOW DATABASES

-- Show the currently active database
SELECT current_database();

-- Create a database
CREATE DATABASE IF NOT EXISTS training;

-- Create a database with a specific location (external)
CREATE DATABASE IF NOT EXISTS training_ext
LOCATION 's3a://my-bucket/databases/training_ext/';

-- Add a comment/description
CREATE DATABASE IF NOT EXISTS training
COMMENT 'Database for training exercises';

-- Switch to a database (all subsequent queries use this database)
USE training;

-- Show all tables in the current database
SHOW TABLES;

-- Show tables in a specific database
SHOW TABLES IN training;

-- Describe a database
DESCRIBE DATABASE training;
DESCRIBE DATABASE EXTENDED training;   -- includes location info

-- Drop a database (must be empty)
DROP DATABASE IF EXISTS training;

-- Drop a database and all its tables (CASCADE)
DROP DATABASE IF EXISTS training CASCADE;
```

**Python API equivalents:**

```python
# List all databases
spark.catalog.listDatabases()
for db in spark.catalog.listDatabases():
    print(db.name, db.locationUri)

# Create a database
spark.sql("CREATE DATABASE IF NOT EXISTS training")

# Set the active database
spark.catalog.setCurrentDatabase("training")
print(spark.catalog.currentDatabase())   # "training"

# List tables in a database
spark.catalog.listTables("training")
```

---

## 3. Managing Tables: Unmanaged and Managed Tables

### 3.1 Managed Tables

A **managed table** (also called an **internal table**) is fully controlled by Spark/Databrick's metastore:

- **Metadata** (schema, location) stored in the metastore.
- **Data** stored in the default warehouse location (`dbfs:/user/hive/warehouse/<db>.db/<table>/`).
- **Dropping the table drops both metadata AND data.**

```sql
%sql

-- Create a managed Delta table (CREATE TABLE)
CREATE TABLE IF NOT EXISTS training.employees (
    emp_id   STRING NOT NULL,
    name     STRING,
    salary   DOUBLE,
    dept     STRING,
    city     STRING
)
USING DELTA
COMMENT 'Employee master table';

-- Create a managed table from a query
CREATE TABLE IF NOT EXISTS training.high_earners
USING DELTA
AS SELECT * FROM training.employees WHERE salary > 70000;

-- Insert data
INSERT INTO training.employees VALUES
    ('E001', 'Alice',  65000.0, 'Engineering', 'New York'),
    ('E002', 'Bob',    48000.0, 'Marketing',   'Chicago');

-- Insert from another table
INSERT INTO training.employees
SELECT * FROM training.staging_employees;
```

```python
# Python: write a DataFrame as a managed Delta table
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("training.employees")

# Or using writeTo (preferred in newer Databricks)
df.writeTo("training.employees") \
    .using("delta") \
    .createOrReplace()
```

---

### 3.2 Unmanaged (External) Tables

An **unmanaged** (external) table stores data at a **user-specified location**:

- **Metadata** stored in the metastore.
- **Data** stored at an external path (S3, ADLS, DBFS).
- **Dropping the table drops only metadata — data is preserved.**

```sql
%sql

-- Create an external Delta table
CREATE TABLE IF NOT EXISTS training.orders_external (
    order_id    STRING,
    customer_id STRING,
    total       DOUBLE,
    order_date  DATE
)
USING DELTA
LOCATION 's3a://my-bucket/tables/orders/';

-- Create an external table pointing to CSV files (no conversion — files stay as CSV)
CREATE TABLE IF NOT EXISTS training.orders_csv (
    order_id    STRING,
    customer_id STRING,
    total       STRING,
    order_date  STRING
)
USING CSV
OPTIONS (
    path        's3a://my-bucket/raw/orders/',
    header      'true',
    delimiter   ','
);

-- Create an external Parquet table
CREATE TABLE IF NOT EXISTS training.orders_parquet
USING PARQUET
LOCATION 's3a://my-bucket/parquet/orders/';
```

```python
# Python: write DataFrame as an unmanaged external Delta table
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "s3a://my-bucket/tables/employees/") \
    .saveAsTable("training.employees_external")
```

---

### 3.3 Managed vs Unmanaged — Key Differences

```
MANAGED TABLE                          UNMANAGED (EXTERNAL) TABLE
─────────────────────────────────────  ──────────────────────────────────────────
Data location: default warehouse path  Data location: user-specified path
Data on DROP:  ✗ DELETED               Data on DROP:  ✓ PRESERVED (only metadata removed)
Best for:      Development, temp data  Best for:      Production, shared data, S3-backed data
Location:      Metastore-managed       Location:      Specified via LOCATION clause
```

```sql
-- Check whether a table is managed or external
DESCRIBE EXTENDED training.employees;
-- Look for "Type = MANAGED" or "Type = EXTERNAL" in the output
```

---

### 3.4 Table Management Commands

```sql
%sql

-- List tables
SHOW TABLES IN training;

-- Describe table schema
DESCRIBE training.employees;

-- Describe full table details (location, format, statistics)
DESCRIBE EXTENDED training.employees;
DESCRIBE DETAIL training.employees;    -- Delta-only: shows Delta metadata

-- View table properties
SHOW TBLPROPERTIES training.employees;

-- Rename a table
ALTER TABLE training.employees RENAME TO training.employees_v2;

-- Add a column
ALTER TABLE training.employees ADD COLUMN (performance_score INT);

-- Change a column comment
ALTER TABLE training.employees
ALTER COLUMN salary COMMENT 'Annual gross salary in USD';

-- Drop a table (managed: deletes data; external: deletes only metadata)
DROP TABLE IF EXISTS training.temp_staging;

-- Truncate a table (remove all rows, keep schema)
TRUNCATE TABLE training.staging;

-- Show the CREATE TABLE statement for an existing table
SHOW CREATE TABLE training.employees;

-- Repair table (sync metastore with actual files — useful for external tables)
MSCK REPAIR TABLE training.orders_parquet;
-- For Delta tables:
ALTER TABLE training.employees_external REPAIR;
```

**Delta-specific table operations:**

```sql
%sql

-- View Delta transaction history (time travel)
DESCRIBE HISTORY training.employees;

-- Vacuum old Delta files (default: 7 days retention)
VACUUM training.employees;
VACUUM training.employees RETAIN 168 HOURS;  -- 7 days

-- Optimize Delta table (compact small files + ZORDERing)
OPTIMIZE training.employees;
OPTIMIZE training.employees ZORDER BY (dept, city);
```

---

## 4. SQL Fundamentals

### 4.1 SELECT Clause

```sql
%sql

-- Select all columns
SELECT * FROM training.employees;

-- Select specific columns
SELECT emp_id, name, salary FROM training.employees;

-- Column aliases
SELECT
    emp_id                          AS employee_id,
    name                            AS full_name,
    salary                          AS annual_salary,
    ROUND(salary / 12, 2)          AS monthly_salary,
    UPPER(name)                     AS name_upper,
    CONCAT(name, ' (', dept, ')') AS name_with_dept
FROM training.employees;

-- Arithmetic in SELECT
SELECT
    name,
    salary,
    salary * 0.15                  AS bonus,
    salary + (salary * 0.15)       AS total_compensation
FROM training.employees;

-- SELECT DISTINCT
SELECT DISTINCT dept FROM training.employees;
SELECT DISTINCT dept, city FROM training.employees;

-- Limit results
SELECT * FROM training.employees LIMIT 5;

-- SELECT with subquery result
SELECT *, salary - avg_salary AS salary_vs_avg
FROM training.employees
CROSS JOIN (SELECT AVG(salary) AS avg_salary FROM training.employees);
```

---

### 4.2 WHERE Clause

```sql
%sql

-- Basic comparison
SELECT * FROM training.employees WHERE salary > 60000;

-- Equality
SELECT * FROM training.employees WHERE dept = 'Engineering';

-- Compound conditions
SELECT * FROM training.employees
WHERE salary > 60000
  AND dept = 'Engineering';

SELECT * FROM training.employees
WHERE dept = 'Engineering'
   OR dept = 'Finance';

-- NOT
SELECT * FROM training.employees
WHERE NOT dept = 'Marketing';
```

---

### 4.3 Handling NULLs

NULL values in SQL require special handling — standard comparison operators (`=`, `!=`, `<`) do **not** work with NULLs:

```sql
%sql

-- Test for NULL (NOT: WHERE salary = NULL — this never matches)
SELECT * FROM training.employees WHERE salary IS NULL;
SELECT * FROM training.employees WHERE salary IS NOT NULL;

-- NULL-safe equality (treats NULL = NULL as true)
SELECT * FROM training.employees WHERE salary <=> NULL;   -- Spark SQL only

-- Replace NULLs in SELECT
SELECT
    name,
    COALESCE(salary, 0)             AS salary,        -- first non-null value
    IFNULL(manager_id, 'No Manager') AS manager,      -- if null → default
    NULLIF(bonus, 0)                AS bonus_or_null,  -- return null if equal to 0
    NVL(city, 'Unknown')            AS city            -- same as IFNULL
FROM training.employees;

-- Filter out rows where any key column is null
SELECT *
FROM training.employees
WHERE emp_id   IS NOT NULL
  AND name     IS NOT NULL
  AND salary   IS NOT NULL;
```

**Python DataFrame equivalents:**

```python
from pyspark.sql.functions import col, coalesce, lit, isnan, when

# Filter nulls
df.filter(col("salary").isNull()).show()
df.filter(col("salary").isNotNull()).show()

# Replace nulls
df.fillna({"salary": 0, "city": "Unknown"}).show()
df.na.fill({"salary": 0, "city": "Unknown"}).show()   # same as fillna

# coalesce — first non-null from a list of columns
df.withColumn("salary", coalesce(col("salary"), col("base_pay"), lit(0.0))).show()

# Drop rows with any null values
df.dropna().show()                           # drop if ANY column is null
df.dropna(subset=["emp_id", "salary"]).show() # drop only if these columns are null
df.dropna(thresh=3).show()                   # drop if fewer than 3 non-null values
```

---

### 4.4 Aggregations

```sql
%sql

-- Built-in aggregate functions
SELECT
    COUNT(*)                        AS total_rows,
    COUNT(salary)                   AS non_null_salary_rows,
    COUNT(DISTINCT dept)            AS num_departments,
    SUM(salary)                     AS total_payroll,
    AVG(salary)                     AS avg_salary,
    MEAN(salary)                    AS mean_salary,      -- alias for AVG
    MIN(salary)                     AS min_salary,
    MAX(salary)                     AS max_salary,
    ROUND(STDDEV(salary), 2)       AS salary_stddev,
    ROUND(VARIANCE(salary), 2)     AS salary_variance,
    PERCENTILE(salary, 0.5)        AS median_salary,
    PERCENTILE_APPROX(salary, 0.9) AS p90_salary
FROM training.employees;

-- Conditional aggregation (COUNT with filter)
SELECT
    COUNT(CASE WHEN dept = 'Engineering' THEN 1 END) AS eng_count,
    COUNT(CASE WHEN salary > 70000      THEN 1 END) AS high_earner_count,
    SUM(  CASE WHEN dept = 'Sales'      THEN salary ELSE 0 END) AS sales_payroll
FROM training.employees;

-- FILTER clause (Spark SQL 3.x+)
SELECT
    COUNT(*) FILTER (WHERE dept = 'Engineering') AS eng_count,
    AVG(salary) FILTER (WHERE dept = 'Engineering') AS eng_avg_salary
FROM training.employees;
```

---

### 4.5 GROUP BY

```sql
%sql

-- Simple GROUP BY
SELECT dept, COUNT(*) AS headcount
FROM training.employees
GROUP BY dept;

-- GROUP BY multiple columns
SELECT dept, city, COUNT(*) AS headcount, AVG(salary) AS avg_salary
FROM training.employees
GROUP BY dept, city
ORDER BY dept, city;

-- GROUP BY with rollup (adds subtotals)
SELECT
    COALESCE(dept, 'TOTAL') AS dept,
    COALESCE(city, 'ALL CITIES') AS city,
    COUNT(*) AS headcount,
    SUM(salary) AS total_salary
FROM training.employees
GROUP BY ROLLUP (dept, city)
ORDER BY dept, city;

-- GROUP BY with cube (all subgroup combinations)
SELECT
    dept, city,
    COUNT(*) AS headcount
FROM training.employees
GROUP BY CUBE (dept, city)
ORDER BY dept NULLS LAST, city NULLS LAST;

-- GROUPING SETS (explicit combinations)
SELECT dept, city, COUNT(*) AS headcount
FROM training.employees
GROUP BY GROUPING SETS (
    (dept, city),     -- by dept+city
    (dept),           -- by dept only
    ()                -- grand total
);

-- GROUPING() function — identify which row is a subtotal
SELECT
    CASE WHEN GROUPING(dept) = 1 THEN 'ALL' ELSE dept END AS dept,
    COUNT(*) AS headcount
FROM training.employees
GROUP BY ROLLUP (dept);
```

---

### 4.6 HAVING

`HAVING` filters **after** `GROUP BY` (operates on aggregated results). `WHERE` filters **before** grouping (operates on individual rows).

```sql
%sql

-- HAVING: filter groups where headcount >= 2
SELECT dept, COUNT(*) AS headcount, AVG(salary) AS avg_salary
FROM training.employees
GROUP BY dept
HAVING COUNT(*) >= 2
ORDER BY headcount DESC;

-- Combining WHERE and HAVING
SELECT dept, COUNT(*) AS headcount, AVG(salary) AS avg_salary
FROM training.employees
WHERE city != 'Chicago'       -- filter rows BEFORE grouping
GROUP BY dept
HAVING AVG(salary) > 60000    -- filter groups AFTER grouping
ORDER BY avg_salary DESC;

-- Complex HAVING
SELECT dept, MIN(salary) AS min_sal, MAX(salary) AS max_sal
FROM training.employees
GROUP BY dept
HAVING MAX(salary) - MIN(salary) > 20000;  -- depts with large salary spread
```

```
Execution order matters:
  FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT
```

---

### 4.7 ORDER BY

```sql
%sql

-- Sort ascending (default)
SELECT * FROM training.employees ORDER BY salary;
SELECT * FROM training.employees ORDER BY salary ASC;

-- Sort descending
SELECT * FROM training.employees ORDER BY salary DESC;

-- Multi-column sort
SELECT * FROM training.employees
ORDER BY dept ASC, salary DESC;

-- Sort with NULLs placement
SELECT * FROM training.employees
ORDER BY manager_id ASC NULLS LAST;   -- nulls go to the end

SELECT * FROM training.employees
ORDER BY manager_id ASC NULLS FIRST;  -- nulls go to the beginning

-- Sort by column alias
SELECT name, salary * 0.15 AS bonus
FROM training.employees
ORDER BY bonus DESC;

-- Sort by column position (1-based — valid but discouraged)
SELECT name, dept, salary
FROM training.employees
ORDER BY 3 DESC;   -- sort by 3rd column (salary)
```

---

## 5. SQL Joins

### 5.1 Inner Join

Returns only rows that have matching values in **both** tables:

```sql
%sql

-- INNER JOIN (default join type)
SELECT e.emp_id, e.name, e.salary, d.dept_name, d.location
FROM training.employees e
INNER JOIN training.departments d ON e.dept_id = d.dept_id;

-- JOIN (same as INNER JOIN)
SELECT e.emp_id, e.name, d.dept_name
FROM training.employees e
JOIN training.departments d ON e.dept_id = d.dept_id;

-- Natural join (joins on all columns with the same name — use carefully)
SELECT * FROM training.employees
NATURAL JOIN training.departments;

-- Join on multiple columns
SELECT * FROM training.orders o
JOIN training.order_details od
    ON o.order_id = od.order_id
   AND o.customer_id = od.customer_id;

-- Self join (join a table to itself)
SELECT e1.name AS employee, e2.name AS manager
FROM training.employees e1
JOIN training.employees e2 ON e1.manager_id = e2.emp_id;
```

---

### 5.2 Left Outer Join

Returns **all rows from the left table** and matched rows from the right; NULLs for unmatched right columns:

```sql
%sql

SELECT e.emp_id, e.name, e.salary,
       d.dept_name,                     -- will be NULL if no match
       d.location                       -- will be NULL if no match
FROM training.employees e
LEFT OUTER JOIN training.departments d ON e.dept_id = d.dept_id;

-- LEFT JOIN is shorthand for LEFT OUTER JOIN
SELECT e.name, d.dept_name
FROM training.employees e
LEFT JOIN training.departments d ON e.dept_id = d.dept_id;

-- Find employees WITHOUT a matching department (anti-pattern using left join)
SELECT e.name
FROM training.employees e
LEFT JOIN training.departments d ON e.dept_id = d.dept_id
WHERE d.dept_id IS NULL;    -- only unmatched rows from left table
```

---

### 5.3 Right Outer Join

Returns **all rows from the right table** and matched rows from the left; NULLs for unmatched left columns:

```sql
%sql

SELECT e.name,                          -- will be NULL if no match
       d.dept_name, d.location
FROM training.employees e
RIGHT OUTER JOIN training.departments d ON e.dept_id = d.dept_id;

-- RIGHT JOIN is shorthand
SELECT e.name, d.dept_name
FROM training.employees e
RIGHT JOIN training.departments d ON e.dept_id = d.dept_id;
```

> **Tip:** A `RIGHT JOIN t2` is equivalent to writing `LEFT JOIN` with the table order reversed. Most developers use `LEFT JOIN` consistently and swap table order for readability.

---

### 5.4 Full Outer, Semi, Anti Joins

**Full Outer Join:**

```sql
%sql

-- All rows from BOTH sides; NULLs where no match
SELECT e.name, d.dept_name
FROM training.employees e
FULL OUTER JOIN training.departments d ON e.dept_id = d.dept_id;
```

**Left Semi Join (filter using EXISTS):**

```sql
%sql

-- Only rows from left where a match exists (no right columns returned)
SELECT e.*
FROM training.employees e
LEFT SEMI JOIN training.departments d ON e.dept_id = d.dept_id;

-- Equivalent using EXISTS
SELECT * FROM training.employees e
WHERE EXISTS (
    SELECT 1 FROM training.departments d WHERE d.dept_id = e.dept_id
);
```

**Left Anti Join (filter using NOT EXISTS):**

```sql
%sql

-- Only rows from left where NO match exists
SELECT e.*
FROM training.employees e
LEFT ANTI JOIN training.departments d ON e.dept_id = d.dept_id;

-- Equivalent using NOT EXISTS
SELECT * FROM training.employees e
WHERE NOT EXISTS (
    SELECT 1 FROM training.departments d WHERE d.dept_id = e.dept_id
);
```

**Cross Join:**

```sql
%sql

-- Cartesian product — all combinations
SELECT e.name, d.dept_name
FROM training.employees e
CROSS JOIN training.departments d;
-- Result: 7 employees × 3 departments = 21 rows
```

---

## 6. SQL Fundamentals: Predicates, Operators, and CASE Expressions

### 6.1 Comparison Operators

```sql
%sql

-- Standard comparisons
SELECT * FROM training.employees WHERE salary = 65000;
SELECT * FROM training.employees WHERE salary != 65000;
SELECT * FROM training.employees WHERE salary <> 65000;   -- same as !=
SELECT * FROM training.employees WHERE salary > 60000;
SELECT * FROM training.employees WHERE salary >= 60000;
SELECT * FROM training.employees WHERE salary < 50000;
SELECT * FROM training.employees WHERE salary <= 50000;

-- NULL-safe equal (returns true when both sides are NULL)
SELECT * FROM training.employees WHERE manager_id <=> NULL;
```

---

### 6.2 Logical Operators

```sql
%sql

-- AND — both conditions must be true
SELECT * FROM training.employees
WHERE salary > 60000 AND dept = 'Engineering';

-- OR — at least one condition must be true
SELECT * FROM training.employees
WHERE dept = 'Engineering' OR dept = 'Finance';

-- NOT — negate a condition
SELECT * FROM training.employees WHERE NOT dept = 'Marketing';

-- Combining AND/OR — use parentheses for clarity
SELECT * FROM training.employees
WHERE (dept = 'Engineering' OR dept = 'Finance')
  AND salary > 60000;
```

---

### 6.3 Arithmetic Operators

```sql
%sql

SELECT
    name,
    salary,
    salary + 5000            AS salary_adjusted,
    salary * 1.10            AS with_10pct_raise,
    salary - 3000            AS after_deduction,
    salary / 12              AS monthly,
    salary % 10000           AS remainder,        -- modulo
    POWER(salary, 0.5)       AS salary_sqrt,
    ABS(salary - 60000)      AS deviation_from_60k,
    ROUND(salary / 12, 2)    AS monthly_rounded,
    CEIL(salary / 1000.0)    AS thousands_rounded_up,
    FLOOR(salary / 1000.0)   AS thousands_rounded_down
FROM training.employees;
```

---

### 6.4 String Operators and Functions

```sql
%sql

SELECT
    name,
    UPPER(name)                          AS name_upper,
    LOWER(name)                          AS name_lower,
    INITCAP(name)                        AS name_title,
    LENGTH(name)                         AS name_length,
    TRIM(name)                           AS name_trimmed,
    LTRIM(name)                          AS name_ltrimmed,
    RTRIM(name)                          AS name_rtrimmed,
    TRIM(LEADING 'A' FROM name)         AS trim_leading_a,

    -- Concatenation
    CONCAT(name, ' (', dept, ')')       AS name_with_dept,
    name || ' - ' || dept               AS name_dept_concat,  -- || operator
    CONCAT_WS(' | ', name, dept, city)  AS pipe_separated,   -- with separator

    -- Substring
    SUBSTR(name, 1, 3)                  AS first_3_chars,
    SUBSTRING(name, 2)                  AS from_2nd_char,

    -- Find position
    INSTR(name, 'a')                    AS pos_of_a,
    LOCATE('a', name)                   AS pos_of_a_alt,

    -- Padding
    LPAD(emp_id, 8, '0')               AS padded_id,
    RPAD(dept, 15, '.')                AS padded_dept,

    -- Replace
    REPLACE(name, 'a', '@')            AS name_replaced,
    REGEXP_REPLACE(name, '[aeiou]', '*') AS vowels_masked,

    -- Extract using regex
    REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain
FROM training.employees;
```

---

### 6.5 Predicates: IN, BETWEEN, LIKE, IS NULL

```sql
%sql

-- IN — match any value in a list
SELECT * FROM training.employees
WHERE dept IN ('Engineering', 'Finance', 'Data');

-- NOT IN — match none of the values (caution: returns 0 rows if list contains NULL)
SELECT * FROM training.employees
WHERE dept NOT IN ('Marketing', 'HR');

-- BETWEEN — inclusive range
SELECT * FROM training.employees WHERE salary BETWEEN 50000 AND 75000;
SELECT * FROM training.employees WHERE hire_date BETWEEN '2020-01-01' AND '2022-12-31';

-- NOT BETWEEN
SELECT * FROM training.employees WHERE salary NOT BETWEEN 50000 AND 75000;

-- LIKE — pattern matching (% = any sequence, _ = single character)
SELECT * FROM training.employees WHERE name LIKE 'A%';      -- starts with A
SELECT * FROM training.employees WHERE name LIKE '%e';      -- ends with e
SELECT * FROM training.employees WHERE name LIKE '%al%';    -- contains "al"
SELECT * FROM training.employees WHERE emp_id LIKE 'E00_'; -- E001, E002, E009...

-- ILIKE — case-insensitive LIKE (Spark SQL)
SELECT * FROM training.employees WHERE name ILIKE 'alice';

-- RLIKE / REGEXP — regular expression matching
SELECT * FROM training.employees WHERE name RLIKE '^[A-C]';   -- starts A, B, or C
SELECT * FROM training.employees WHERE emp_id REGEXP '^E0[0-9]{2}$';

-- IS NULL / IS NOT NULL
SELECT * FROM training.employees WHERE manager_id IS NULL;
SELECT * FROM training.employees WHERE salary IS NOT NULL;

-- EXISTS — correlated subquery test
SELECT * FROM training.employees e
WHERE EXISTS (
    SELECT 1 FROM training.projects p WHERE p.emp_id = e.emp_id
);
```

---

### 6.6 CASE Expressions

`CASE` is the most flexible conditional construct in SQL — equivalent to `if/elif/else` or `switch/case`:

**Searched CASE (full condition per branch):**

```sql
%sql

SELECT
    name, salary,
    CASE
        WHEN salary < 40000               THEN 'Entry'
        WHEN salary BETWEEN 40000 AND 59999 THEN 'Mid'
        WHEN salary BETWEEN 60000 AND 79999 THEN 'Senior'
        WHEN salary >= 80000              THEN 'Principal'
        ELSE 'Unknown'
    END AS salary_band
FROM training.employees;
```

**Simple CASE (equality check on one column):**

```sql
%sql

SELECT
    name, dept,
    CASE dept
        WHEN 'Engineering' THEN 'Tech'
        WHEN 'Finance'     THEN 'Business'
        WHEN 'Marketing'   THEN 'Business'
        ELSE                    'Other'
    END AS dept_group
FROM training.employees;
```

**CASE in aggregation (conditional aggregation):**

```sql
%sql

SELECT
    city,
    COUNT(CASE WHEN dept = 'Engineering' THEN 1 END) AS eng_headcount,
    COUNT(CASE WHEN dept = 'Marketing'   THEN 1 END) AS mkt_headcount,
    COUNT(CASE WHEN salary > 70000       THEN 1 END) AS high_earners,
    SUM(  CASE WHEN dept = 'Engineering' THEN salary ELSE 0 END) AS eng_payroll
FROM training.employees
GROUP BY city;
```

**CASE in ORDER BY (custom sort order):**

```sql
%sql

SELECT name, dept
FROM training.employees
ORDER BY
    CASE dept
        WHEN 'Engineering' THEN 1
        WHEN 'Finance'     THEN 2
        WHEN 'Marketing'   THEN 3
        ELSE                    99
    END,
    name ASC;
```

---

### 6.7 Subqueries

```sql
%sql

-- Scalar subquery — returns a single value
SELECT name, salary,
       salary - (SELECT AVG(salary) FROM training.employees) AS deviation_from_avg
FROM training.employees;

-- Subquery in FROM (derived table / inline view)
SELECT dept, avg_salary
FROM (
    SELECT dept, AVG(salary) AS avg_salary
    FROM training.employees
    GROUP BY dept
) dept_avgs
WHERE avg_salary > 60000;

-- Correlated subquery — references the outer query
SELECT name, salary, dept
FROM training.employees e1
WHERE salary > (
    SELECT AVG(salary)
    FROM training.employees e2
    WHERE e2.dept = e1.dept   -- avg salary of the SAME dept
);

-- EXISTS subquery
SELECT name FROM training.employees e
WHERE EXISTS (
    SELECT 1 FROM training.projects p WHERE p.emp_id = e.emp_id
);

-- IN with subquery
SELECT name FROM training.employees
WHERE dept IN (
    SELECT dept FROM training.departments WHERE budget > 1000000
);

-- WITH clause (Common Table Expression — CTE)
WITH dept_stats AS (
    SELECT dept,
           AVG(salary)  AS avg_salary,
           COUNT(*)     AS headcount
    FROM training.employees
    GROUP BY dept
),
high_value_depts AS (
    SELECT dept FROM dept_stats
    WHERE avg_salary > 60000 AND headcount >= 2
)
SELECT e.name, e.salary, e.dept
FROM training.employees e
JOIN high_value_depts h ON e.dept = h.dept
ORDER BY e.dept, e.salary DESC;
```

---

## 7. Summary

| Topic                   | Key Takeaway                                                                           |
| ----------------------- | -------------------------------------------------------------------------------------- |
| **TempView**            | Session-scoped SQL alias for a DataFrame; use `createOrReplaceTempView`                |
| **GlobalTempView**      | Cluster-scoped; access via `global_temp.<name>`; cleared on restart                    |
| **Database**            | Namespace for tables/views; `CREATE DATABASE`, `USE`, `DROP DATABASE CASCADE`          |
| **Managed table**       | Data owned by Spark; `DROP TABLE` deletes data                                         |
| **External table**      | Data at user path; `DROP TABLE` preserves data                                         |
| **SELECT**              | Project, transform, and alias columns; supports arithmetic and string functions        |
| **WHERE**               | Filter rows before grouping; `IS NULL` / `IS NOT NULL` for null checks                 |
| **NULL handling**       | Use `IS NULL`, `COALESCE`, `IFNULL`; never `= NULL`                                    |
| **GROUP BY**            | Aggregate by one or more columns; `ROLLUP`/`CUBE` add subtotals                        |
| **HAVING**              | Filter groups after `GROUP BY`; equivalent to DataFrame `.filter()` after `.groupBy()` |
| **ORDER BY**            | Sort with `ASC`/`DESC`; control null placement with `NULLS FIRST/LAST`                 |
| **INNER JOIN**          | Matching rows only; most restrictive                                                   |
| **LEFT JOIN**           | All left rows; nulls for unmatched right                                               |
| **RIGHT JOIN**          | All right rows; nulls for unmatched left                                               |
| **SEMI / ANTI**         | Filter by existence/non-existence; no columns from right table                         |
| **IN / BETWEEN / LIKE** | Standard predicates; RLIKE for regex                                                   |
| **CASE**                | Conditional column values; powerful for conditional aggregation                        |
| **CTE (`WITH`)**        | Named subquery blocks; improve readability for complex queries                         |

**Previous guide:** [Guide 07 — Exploring Different Data Types in Spark](07-data-types-in-spark.md)
