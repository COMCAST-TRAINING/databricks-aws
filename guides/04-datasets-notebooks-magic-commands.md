# Working with Datasets and Notebooks in Databricks

---

## Table of Contents

1. [Magic Commands](#1-magic-commands)
   - 1.1 [What Are Magic Commands?](#11-what-are-magic-commands)
   - 1.2 [Language Magic Commands](#12-language-magic-commands)
   - 1.3 [The `%run` Command](#13-the-run-command)
   - 1.4 [The `%fs` Command](#14-the-fs-command)
   - 1.5 [The `%sh` Command](#15-the-sh-command)
   - 1.6 [The `%md` Command](#16-the-md-command)
   - 1.7 [The `%pip` and `%conda` Commands](#17-the-pip-and-conda-commands)
   - 1.8 [The `%sql` Command](#18-the-sql-command)
2. [Installing the Dataset](#2-installing-the-dataset)
   - 2.1 [Databricks Datasets (Built-In)](#21-databricks-datasets-built-in)
   - 2.2 [Uploading Files to DBFS](#22-uploading-files-to-dbfs)
   - 2.3 [Mounting an S3 Bucket](#23-mounting-an-s3-bucket)
   - 2.4 [Accessing S3 Directly with IAM Roles](#24-accessing-s3-directly-with-iam-roles)
3. [Overview of the Dataset](#3-overview-of-the-dataset)
   - 3.1 [Exploring the Dataset Structure](#31-exploring-the-dataset-structure)
   - 3.2 [Profiling Dataset Contents](#32-profiling-dataset-contents)
4. [Installing the Notebooks](#4-installing-the-notebooks)
   - 4.1 [Importing a Notebook from a URL](#41-importing-a-notebook-from-a-url)
   - 4.2 [Importing a DBC Archive](#42-importing-a-dbc-archive)
   - 4.3 [Importing from a Git Repository (Databricks Repos)](#43-importing-from-a-git-repository-databricks-repos)
5. [Creating a DataFrame from a CSV File](#5-creating-a-dataframe-from-a-csv-file)
   - 5.1 [Basic CSV Read](#51-basic-csv-read)
   - 5.2 [Schema Inference vs Explicit Schema](#52-schema-inference-vs-explicit-schema)
6. [Configuring Options to Read a CSV File](#6-configuring-options-to-read-a-csv-file)
   - 6.1 [Common Read Options](#61-common-read-options)
   - 6.2 [Handling Malformed Records](#62-handling-malformed-records)
   - 6.3 [Reading Multiple CSV Files](#63-reading-multiple-csv-files)
   - 6.4 [Writing DataFrames to CSV](#64-writing-dataframes-to-csv)
7. [Summary](#7-summary)

---

## 1. Magic Commands

### 1.1 What Are Magic Commands?

**Magic commands** are special directives that change the behaviour of a single notebook cell. They begin with a `%` symbol and must appear as the **first line** of a cell.

Unlike regular code, magic commands are interpreted by the Databricks notebook runtime rather than the underlying language kernel. This allows you to mix languages, run shell commands, mount filesystems, and install packages — all within a single notebook.

```
%<magic>     ← Applies to the entire cell
%%<magic>    ← Not used in Databricks (that is IPython/Jupyter syntax)
```

**Full list of supported magic commands:**

| Command    | Purpose                                         |
| ---------- | ----------------------------------------------- |
| `%python`  | Execute the cell as Python                      |
| `%scala`   | Execute the cell as Scala                       |
| `%sql`     | Execute the cell as SQL                         |
| `%r`       | Execute the cell as R                           |
| `%run`     | Run another notebook as a script                |
| `%fs`      | Interact with the Databricks File System (DBFS) |
| `%sh`      | Run shell (bash) commands on the driver node    |
| `%md`      | Render the cell as Markdown                     |
| `%pip`     | Install Python packages                         |
| `%conda`   | Manage conda environments                       |
| `%lsmagic` | List all available magic commands               |

---

### 1.2 Language Magic Commands

Databricks notebooks have a **default language** (set at creation time — Python, Scala, SQL, or R). Use language magic commands to override this on a per-cell basis.

**Example: A Python-default notebook using multiple languages**

```python
# Cell 1 (default Python)
df = spark.range(5)
df.show()
```

```scala
// Cell 2 — run as Scala
%scala
val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3))
rdd.collect().foreach(println)
```

```sql
-- Cell 3 — run as SQL
%sql
SELECT current_date() AS today, current_timestamp() AS now;
```

```r
# Cell 4 — run as R
%r
library(SparkR)
head(as.data.frame(sql("SELECT 1 + 1 AS result")))
```

**Variable sharing between languages:**

> Variables defined in one language cell **are not** directly accessible in another language cell — each language has its own runtime context. To share data between languages, write to a **temporary view** (SQL-accessible from any language) or to **DBFS/Delta**.

```python
# Python cell: create a DataFrame
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
df.createOrReplaceTempView("shared_view")
```

```sql
-- SQL cell: access the same data via the temp view
%sql
SELECT * FROM shared_view;
```

---

### 1.3 The `%run` Command

`%run` executes another notebook **in the same execution context**. Variables, functions, and DataFrames defined in the target notebook become available in the calling notebook.

```python
# Run a setup/utility notebook
%run ./includes/setup

# Run with parameters (using widgets)
%run ./includes/setup $env="production" $data_path="/mnt/data"
```

**Common use cases:**

- Centralise utility functions in a shared notebook and `%run` it from multiple notebooks.
- Split large notebooks into smaller logical units.
- Run setup/teardown notebooks for lab exercises.

```
Directory structure example:
  includes/
    setup.py       ← defines common variables and functions
    schemas.py     ← defines reusable schema definitions
  analysis/
    01_explore.py  ← %run ../includes/setup at the top
    02_model.py    ← %run ../includes/setup at the top
```

> **Important:** `%run` is not the same as a library import — it executes the entire notebook. Use it for setup scripts, not for importing reusable modules (use `%pip install` + `import` for that).

---

### 1.4 The `%fs` Command

`%fs` is a shortcut for `dbutils.fs` and provides a filesystem interface to **DBFS (Databricks File System)**. It maps DBFS commands to familiar Unix-style operations.

```bash
# List files at a path
%fs ls /databricks-datasets/

# List your personal DBFS location
%fs ls /FileStore/

# Display file contents (for small text files)
%fs head /databricks-datasets/README.md

# Copy a file
%fs cp /FileStore/source.csv /FileStore/destination.csv

# Move / rename a file
%fs mv /FileStore/old_name.csv /FileStore/new_name.csv

# Delete a file
%fs rm /FileStore/temp_file.csv

# Delete a directory recursively
%fs rm -r /FileStore/old_data/

# Create a directory
%fs mkdirs /FileStore/my_project/data/
```

**DBFS path conventions:**

| Path                    | Description                                               |
| ----------------------- | --------------------------------------------------------- |
| `/databricks-datasets/` | Built-in sample datasets provided by Databricks           |
| `/FileStore/`           | Files uploaded via the UI; accessible as static web links |
| `/mnt/<mount-name>/`    | Externally mounted storage (e.g., S3, ADLS)               |
| `/user/hive/warehouse/` | Default location for managed Delta/Hive tables            |
| `dbfs:/`                | Explicit DBFS URI prefix (same as `/`)                    |

**Using `dbutils.fs` in code (equivalent to `%fs`):**

```python
# List files programmatically
files = dbutils.fs.ls("/databricks-datasets/")
for f in files:
    print(f.name, f.size, f.path)

# Check if a path exists
try:
    dbutils.fs.ls("/FileStore/my_file.csv")
    print("File exists")
except Exception:
    print("File does not exist")
```

---

### 1.5 The `%sh` Command

`%sh` runs **shell commands** using bash on the **driver node only** — not on executor nodes.

```bash
# Check Python version
%sh python3 --version

# List files in the local driver filesystem
%sh ls -la /tmp/

# Check available disk space
%sh df -h

# Download a file from the internet to the driver's local filesystem
%sh wget -q https://example.com/data.zip -O /tmp/data.zip

# After downloading, copy to DBFS so all workers can access it
```

```python
# Copy from driver local filesystem to DBFS
dbutils.fs.cp("file:///tmp/data.csv", "/FileStore/data.csv")
```

> **Note:** Files written by `%sh` to `/tmp/` exist only on the driver. To make files available to all executor nodes, copy them to DBFS using `dbutils.fs.cp`.

---

### 1.6 The `%md` Command

`%md` renders the cell as **Markdown** — useful for documentation, section headers, and explanatory text within notebooks.

```markdown
%md

# Section 1: Data Loading

This notebook demonstrates how to load and transform customer data.

## Prerequisites

- Cluster is running Databricks Runtime 13.x or higher
- Dataset is available at `/FileStore/datasets/customers.csv`

## Steps

1. Load the CSV file into a DataFrame
2. Clean and validate the data
3. Write the cleaned data to Delta format

> **Note:** Ensure the cluster is attached before running any cells.
```

**Markdown formatting supported:**

| Syntax                    | Output                    |
| ------------------------- | ------------------------- |
| `# H1`, `## H2`, `### H3` | Headers                   |
| `**bold**`, `*italic*`    | Bold, Italic              |
| `` `code` ``              | Inline code               |
| `- item` or `1. item`     | Unordered / ordered lists |
| `[text](url)`             | Hyperlink                 |
| `![alt](url)`             | Image                     |
| `\| col1 \| col2 \|`      | Markdown table            |
| `> text`                  | Blockquote                |

---

### 1.7 The `%pip` and `%conda` Commands

`%pip` installs Python packages from **PyPI** into the cluster's Python environment.

```python
# Install a single package
%pip install pandas==2.1.0

# Install multiple packages
%pip install scikit-learn xgboost lightgbm

# Install from a requirements file
%pip install -r /dbfs/FileStore/requirements.txt

# Upgrade an existing package
%pip install --upgrade matplotlib

# Install a private package from a Git repo
%pip install git+https://github.com/myorg/mypackage.git
```

> **Note:** `%pip install` automatically **restarts the Python interpreter** after installation. Any variables defined before the `%pip` cell are lost. Put `%pip` cells at the very beginning of the notebook.

`%conda` manages packages in the notebook's conda environment:

```python
# Install a conda package
%conda install -c conda-forge pyarrow

# List installed packages
%conda list
```

**Best practice for reproducibility:** Pin exact versions in `%pip` installs, or better — use a **Cluster Library** (configured in the cluster settings UI) to install packages before the cluster starts. This avoids the restart penalty.

---

### 1.8 The `%sql` Command

Run a SQL query directly in a Python notebook cell:

```sql
%sql
-- Create a database
CREATE DATABASE IF NOT EXISTS training;

-- Use the database
USE training;

-- Show all tables
SHOW TABLES;

-- Run an ad-hoc query
SELECT department, COUNT(*) AS headcount
FROM employees
GROUP BY department
ORDER BY headcount DESC;
```

`%sql` cells in Python notebooks return a **display result** automatically — no explicit `display()` call needed. The results appear as an interactive table that can be converted to a chart using the notebook's chart builder.

---

## 2. Installing the Dataset

### 2.1 Databricks Datasets (Built-In)

Databricks includes a set of curated sample datasets available at `/databricks-datasets/`. These are pre-loaded into DBFS and require no setup.

```python
# List all available sample datasets
display(dbutils.fs.ls("/databricks-datasets/"))
```

Common datasets:

| Path                                      | Description                           |
| ----------------------------------------- | ------------------------------------- |
| `/databricks-datasets/samples/`           | Small general-purpose samples         |
| `/databricks-datasets/flights/`           | US flight delay data                  |
| `/databricks-datasets/nyctaxi/`           | New York City taxi trip records       |
| `/databricks-datasets/wine-quality/`      | Wine quality CSV (classification)     |
| `/databricks-datasets/adult/`             | Adult income census data              |
| `/databricks-datasets/learning-spark-v2/` | Datasets from the Learning Spark book |

```python
# Explore a dataset
display(dbutils.fs.ls("/databricks-datasets/samples/"))

# Quickly peek at a CSV file
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/databricks-datasets/samples/population-vs-price/data_geo.csv")

display(df)
```

---

### 2.2 Uploading Files to DBFS

Small files (< 2 GB) can be uploaded directly from the Databricks UI:

```
Databricks Workspace → Catalogue (sidebar) → click "+" next to any table
or
Workspace → Data → Add Data → Upload File → drag-and-drop your CSV
```

The file lands in `/FileStore/tables/` by default, or you can specify a path.

**Uploading via Databricks CLI:**

```bash
# Install the CLI
pip install databricks-cli

# Configure authentication
databricks configure --token
# Prompt: Host: https://<your-workspace>.azuredatabricks.net
#         Token: <your-personal-access-token>

# Upload a file
databricks fs cp ./local_data.csv dbfs:/FileStore/datasets/local_data.csv

# Upload a directory
databricks fs cp --recursive ./data/ dbfs:/FileStore/datasets/
```

---

### 2.3 Mounting an S3 Bucket

**Mounting** creates a persistent shorthand path (`/mnt/<name>`) that maps to an external S3 bucket. Once mounted, all Databricks users and notebooks can reference the bucket using the `/mnt/` path.

```python
# Mount an S3 bucket using IAM role (recommended)
dbutils.fs.mount(
    source      = "s3a://my-training-bucket/datasets/",
    mount_point = "/mnt/training-data",
    extra_configs = {
        "fs.s3a.aws.credentials.provider":
            "com.amazonaws.auth.InstanceProfileCredentialsProvider"
    }
)

# Verify the mount
display(dbutils.fs.ls("/mnt/training-data/"))

# Unmount when no longer needed
dbutils.fs.unmount("/mnt/training-data")
```

> **Security note:** Use IAM Instance Profiles (not access keys) for S3 authentication. Hardcoding AWS keys in notebooks is a security risk. See Guide 02 for EC2 instance profile setup.

**List all mounts:**

```python
display(dbutils.fs.mounts())
```

---

### 2.4 Accessing S3 Directly with IAM Roles

When the cluster has an IAM Instance Profile attached (configured in cluster settings), you can access S3 directly without mounting:

```python
# Read directly from S3 using s3a:// protocol
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("s3a://my-training-bucket/datasets/customers.csv")

# Write directly to S3
df.write \
    .mode("overwrite") \
    .parquet("s3a://my-training-bucket/output/customers_parquet/")
```

**Direct access vs mount — when to use which:**

|                   | Mount (`/mnt/`)                                   | Direct (`s3a://`)                    |
| ----------------- | ------------------------------------------------- | ------------------------------------ |
| **Persistence**   | Persists across cluster restarts                  | Path written inline in code          |
| **Portability**   | Code uses short paths; easy to move between env   | Bucket names hardcoded               |
| **Performance**   | Same (mount is just a path alias)                 | Same                                 |
| **Unity Catalog** | Prefer External Location over mounts for new code | Use Unity Catalog External Locations |

---

## 3. Overview of the Dataset

### 3.1 Exploring the Dataset Structure

Before processing a dataset, always perform an exploratory inspection:

```python
# Step 1: Read the CSV
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/FileStore/datasets/customers.csv")

# Step 2: How many rows and columns?
print(f"Rows: {df.count()}")
print(f"Columns: {len(df.columns)}")
print(f"Columns: {df.columns}")

# Step 3: View the schema
df.printSchema()

# Step 4: Show a sample
df.show(10, truncate=False)

# Step 5: Summary statistics for numeric columns
df.describe().show()
```

**`describe()` output example:**

```
+-------+------------------+------------------+
|summary|               age|            salary|
+-------+------------------+------------------+
|  count|              1000|              1000|
|   mean|            35.142|          62453.30|
| stddev|            12.403|          18920.21|
|    min|                18|           21000.0|
|    max|                68|          150000.0|
+-------+------------------+------------------+
```

---

### 3.2 Profiling Dataset Contents

```python
from pyspark.sql.functions import col, count, when, isnan, isnull

# Count NULL values per column
null_counts = df.select([
    count(when(isnull(c) | isnan(c), c)).alias(c)
    for c in df.columns
])
null_counts.show()

# Distinct values per column (for categorical columns)
for c in ["department", "gender", "country"]:
    print(f"{c}: {df.select(c).distinct().count()} distinct values")

# Distribution of a categorical column
df.groupBy("department") \
  .count() \
  .orderBy("count", ascending=False) \
  .show()
```

**Using `display()` for interactive exploration:**

```python
# display() renders an interactive table with built-in chart builder
display(df)

# display() also renders matplotlib and other plots inline
import matplotlib.pyplot as plt
pdf = df.select("age").toPandas()
plt.hist(pdf["age"], bins=20)
plt.title("Age Distribution")
display(plt.gcf())
```

---

## 4. Installing the Notebooks

### 4.1 Importing a Notebook from a URL

```
1. In the Databricks workspace sidebar, click "Workspace"
2. Navigate to the folder where you want to import the notebook
3. Click the down arrow (▼) next to the folder name
4. Select "Import"
5. In the dialog, choose "URL"
6. Paste the raw URL to the notebook file (.ipynb or .py or .dbc)
7. Click "Import"
```

Databricks supports importing:

- `.ipynb` — Jupyter notebooks
- `.py` — Python source files (treated as notebooks with `# COMMAND ----------` separators)
- `.scala` — Scala source files
- `.sql` — SQL files
- `.dbc` — Databricks Archive format (see below)
- `.html` — Exported Databricks notebooks

---

### 4.2 Importing a DBC Archive

A **DBC (Databricks Archive)** is a ZIP file containing one or more notebooks — the native Databricks export format.

**Import via UI:**

```
Workspace → (select target folder) → Import → "File" → upload the .dbc file
```

**Import via CLI:**

```bash
# Import a DBC file into a workspace path
databricks workspace import \
  --language PYTHON \
  --format DBC \
  /Workspace/Users/myuser/imported_lab \
  ./DataEngineering.dbc
```

**Import via REST API:**

```bash
curl -X POST \
  "https://<workspace-url>/api/2.0/workspace/import" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "path": "/Users/myuser@example.com/lab_notebook",
    "format": "DBC",
    "overwrite": true,
    "content": "<base64-encoded-dbc-content>"
  }'
```

---

### 4.3 Importing from a Git Repository (Databricks Repos)

**Databricks Repos** provides native Git integration — clone, pull, push, and branch directly from the Databricks UI.

**Setup:**

```
1. Workspace sidebar → "Repos"
2. Click "Add Repo"
3. Paste the Git repository URL
4. Optionally set a branch
5. Click "Create Repo"
```

**Supported Git providers:**

- GitHub (public and private)
- GitLab
- Bitbucket
- Azure DevOps

**Working with Repos:**

```
• Pull latest changes: click the branch name → Pull
• Switch branches:     click the branch name → Checkout
• Create a branch:     click the branch name → Create Branch
• Commit changes:      click the branch name → Commit & Push
```

**Cloning via CLI:**

```bash
databricks repos create \
  --url https://github.com/myorg/training-notebooks.git \
  --provider gitHub \
  --path /Repos/myuser@example.com/training-notebooks
```

---

## 5. Creating a DataFrame from a CSV File

### 5.1 Basic CSV Read

The most common entry point for Spark data engineering is reading a **CSV file** into a DataFrame:

```python
# Minimal form
df = spark.read.csv("/FileStore/datasets/employees.csv")
df.show(5)
```

Output (no header option — columns named `_c0`, `_c1`, etc.):

```
+------+-----+--------+------+
|   _c0|  _c1|     _c2|   _c3|
+------+-----+--------+------+
| empId| name|  salary|  dept|
+------+-----+--------+------+
|  E001|Alice|65000.00|  Eng |
...
```

**Better form with header and schema inference:**

```python
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/FileStore/datasets/employees.csv")

df.printSchema()
df.show(5)
```

```
root
 |-- empId: string (nullable = true)
 |-- name: string (nullable = true)
 |-- salary: double (nullable = true)
 |-- dept: string (nullable = true)
```

---

### 5.2 Schema Inference vs Explicit Schema

**Schema inference** reads a sample of the data to guess column types. This is convenient but:

- Slower — requires a full extra scan of the file.
- Can infer wrong types (e.g., a numeric ID column inferred as `integer` when it should stay `string`).
- Non-deterministic for very mixed data.

**Explicit schema** — always preferred for production:

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

schema = StructType([
    StructField("empId",    StringType(),  nullable=False),
    StructField("name",     StringType(),  nullable=True),
    StructField("salary",   DoubleType(),  nullable=True),
    StructField("dept",     StringType(),  nullable=True),
    StructField("hireDate", DateType(),    nullable=True),
])

df = spark.read \
    .schema(schema) \
    .option("header", True) \
    .option("dateFormat", "yyyy-MM-dd") \
    .csv("/FileStore/datasets/employees.csv")

df.printSchema()
```

**DDL-style schema string (simpler syntax):**

```python
# Equivalent to the StructType above
schema = "empId STRING, name STRING, salary DOUBLE, dept STRING, hireDate DATE"

df = spark.read \
    .schema(schema) \
    .option("header", True) \
    .csv("/FileStore/datasets/employees.csv")
```

---

## 6. Configuring Options to Read a CSV File

### 6.1 Common Read Options

The CSV reader supports many options to handle real-world messy files:

```python
df = spark.read \
    .format("csv") \
    .option("header", True)           \  # First row is column names
    .option("inferSchema", True)      \  # Guess column data types
    .option("sep", ",")               \  # Column delimiter (default: comma)
    .option("quote", '"')             \  # Quote character (default: double-quote)
    .option("escape", "\\")          \  # Escape character inside quoted fields
    .option("nullValue", "NULL")      \  # String treated as null
    .option("nanValue", "NaN")        \  # String treated as NaN for floats
    .option("emptyValue", "")         \  # String representing empty value
    .option("dateFormat", "MM/dd/yyyy") \  # Format for date columns
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .option("multiLine", False)       \  # Multi-line quoted fields (default: False)
    .option("encoding", "UTF-8")      \  # File encoding (default: UTF-8)
    .option("ignoreLeadingWhiteSpace", True) \
    .option("ignoreTrailingWhiteSpace", True) \
    .load("/FileStore/datasets/employees.csv")
```

**Quick reference table:**

| Option                     | Type    | Default                            | Description                              |
| -------------------------- | ------- | ---------------------------------- | ---------------------------------------- |
| `header`                   | Boolean | `false`                            | Use first row as column names            |
| `inferSchema`              | Boolean | `false`                            | Infer column data types                  |
| `sep` / `delimiter`        | String  | `,`                                | Column separator character               |
| `quote`                    | String  | `"`                                | Quote character for string fields        |
| `escape`                   | String  | `\`                                | Escape character                         |
| `nullValue`                | String  | `""`                               | Value to interpret as null               |
| `nanValue`                 | String  | `NaN`                              | Value to interpret as NaN                |
| `dateFormat`               | String  | `yyyy-MM-dd`                       | Pattern for date parsing                 |
| `timestampFormat`          | String  | `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]` | Timestamp pattern                        |
| `multiLine`                | Boolean | `false`                            | Allow fields spanning multiple lines     |
| `encoding`                 | String  | `UTF-8`                            | File character encoding                  |
| `ignoreLeadingWhiteSpace`  | Boolean | `false`                            | Trim leading spaces from values          |
| `ignoreTrailingWhiteSpace` | Boolean | `false`                            | Trim trailing spaces from values         |
| `comment`                  | String  | `""`                               | Skip lines beginning with this character |
| `maxColumns`               | Integer | `20480`                            | Max number of columns                    |
| `maxCharsPerColumn`        | Integer | `-1`                               | Max characters per column value          |

---

### 6.2 Handling Malformed Records

Real-world CSV files often contain dirty or unexpected data. Spark's `mode` option controls what happens when a record cannot be parsed:

```python
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("mode", "PERMISSIVE")   \  # default: fill bad fields with null
    .csv("/FileStore/datasets/messy_data.csv")
```

**Mode options:**

| Mode                   | Behaviour                                                                                  |
| ---------------------- | ------------------------------------------------------------------------------------------ |
| `PERMISSIVE` (default) | Places corrupt records in a special `_corrupt_record` column; fills other fields with null |
| `DROPMALFORMED`        | Silently drops any malformed records                                                       |
| `FAILFAST`             | Throws an exception immediately on the first bad record                                    |

**Capturing corrupt records for investigation:**

```python
from pyspark.sql.types import StructType, StructField, StringType

# Include _corrupt_record column in the schema
schema = StructType([
    StructField("id",              StringType(), True),
    StructField("name",            StringType(), True),
    StructField("salary",          StringType(), True),
    StructField("_corrupt_record", StringType(), True),  # catch-all
])

df = spark.read \
    .schema(schema) \
    .option("header", True) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv("/FileStore/datasets/messy_data.csv")

# Separate good and bad records
good_df = df.filter(df._corrupt_record.isNull())
bad_df  = df.filter(df._corrupt_record.isNotNull())

print(f"Good records: {good_df.count()}")
print(f"Bad records:  {bad_df.count()}")
bad_df.show(truncate=False)
```

---

### 6.3 Reading Multiple CSV Files

Spark can read multiple files in a single call — it processes them in parallel:

```python
# Read all CSV files in a directory
df = spark.read \
    .option("header", True) \
    .csv("/FileStore/datasets/monthly_sales/")

# Read specific files with glob patterns
df = spark.read \
    .option("header", True) \
    .csv("/FileStore/datasets/sales_2024_*.csv")

# Read from a list of paths
df = spark.read \
    .option("header", True) \
    .csv([
        "/FileStore/datasets/sales_jan.csv",
        "/FileStore/datasets/sales_feb.csv",
        "/FileStore/datasets/sales_mar.csv",
    ])

# Add a column to track which file each record came from
from pyspark.sql.functions import input_file_name
df = df.withColumn("source_file", input_file_name())
```

---

### 6.4 Writing DataFrames to CSV

```python
# Write a single CSV file (output is a directory with one part file)
df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", True) \
  .csv("/FileStore/output/employees_out/")

# Write with all options
df.write \
    .format("csv") \
    .mode("overwrite")       \  # overwrite | append | ignore | error (default)
    .option("header", True)  \
    .option("sep", ",")      \
    .option("compression", "gzip") \
    .option("dateFormat", "yyyy-MM-dd") \
    .save("/FileStore/output/employees_compressed/")
```

**Write modes:**

| Mode                      | Behaviour                                       |
| ------------------------- | ----------------------------------------------- |
| `overwrite`               | Delete and replace existing data at the path    |
| `append`                  | Add new data to existing data                   |
| `ignore`                  | Do nothing if data already exists at the path   |
| `error` / `errorIfExists` | (Default) Throw an error if data already exists |

> **Best practice:** For production pipelines, prefer writing to **Delta format** instead of CSV. Delta provides ACID transactions, schema enforcement, and time-travel. See Guide 05 for details.

---

## 7. Summary

| Topic                 | Key Takeaway                                                                              |
| --------------------- | ----------------------------------------------------------------------------------------- |
| **Magic Commands**    | Override the cell language or run special operations; must be on the first line of a cell |
| `%run`                | Executes another notebook in the current context — variables and functions are shared     |
| `%fs`                 | DBFS file operations (list, copy, delete); equivalent to `dbutils.fs`                     |
| `%sh`                 | Shell commands on driver node; use `dbutils.fs.cp` to copy results to DBFS                |
| `%pip`                | Install Python packages; place at start of notebook before other cells                    |
| **DBFS**              | Virtual distributed filesystem; `/databricks-datasets/` has built-in samples              |
| **S3 Access**         | Use IAM Instance Profiles (not hardcoded keys); access via direct path or mount           |
| **Dataset Profiling** | Always inspect `.printSchema()`, `.describe()`, null counts before processing             |
| **CSV Reader**        | Use explicit schema for production; use `mode` option to handle bad records               |
| **inferSchema**       | Convenient for exploration; avoid in production pipelines (slow + unreliable)             |
| **Write modes**       | `overwrite` replaces; `append` adds; `error` (default) fails if exists                    |

**Previous guide:** [Guide 03 — Introduction to Databricks with Apache Spark](03-spark-introduction-first-code-architecture.md)

**Continue to:** [Guide 05 — DataFrames and Spark SQL Fundamentals](05-dataframes-spark-sql-fundamentals.md)
