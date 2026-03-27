# Exploring Different Data Types in Spark

---

## Table of Contents

1. [Specifying the Schema of a DataFrame with StructType](#1-specifying-the-schema-of-a-dataframe-with-structtype)
   - 1.1 [StructType and StructField](#11-structtype-and-structfield)
   - 1.2 [DDL-Style Schema Strings](#12-ddl-style-schema-strings)
   - 1.3 [Reading the Schema from an Existing Table](#13-reading-the-schema-from-an-existing-table)
2. [Converting Literals to Spark Types: The `lit` Function](#2-converting-literals-to-spark-types-the-lit-function)
3. [Working with Booleans](#3-working-with-booleans)
   - 3.1 [Boolean Operations and Filtering](#31-boolean-operations-and-filtering)
   - 3.2 [Boolean Aggregations](#32-boolean-aggregations)
4. [Working with Numbers](#4-working-with-numbers)
   - 4.1 [Numeric Types in Spark](#41-numeric-types-in-spark)
   - 4.2 [Arithmetic and Math Functions](#42-arithmetic-and-math-functions)
   - 4.3 [Rounding and Precision](#43-rounding-and-precision)
   - 4.4 [Type Casting](#44-type-casting)
5. [Working with Strings](#5-working-with-strings)
   - 5.1 [String Functions](#51-string-functions)
   - 5.2 [Regular Expressions](#52-regular-expressions)
   - 5.3 [String Splitting and Parsing](#53-string-splitting-and-parsing)
6. [Working with Dates and Timestamps](#6-working-with-dates-and-timestamps)
   - 6.1 [Date and Timestamp Types](#61-date-and-timestamp-types)
   - 6.2 [Creating and Parsing Dates](#62-creating-and-parsing-dates)
   - 6.3 [Date Arithmetic](#63-date-arithmetic)
   - 6.4 [Date Formatting and Extraction](#64-date-formatting-and-extraction)
   - 6.5 [Timezones](#65-timezones)
7. [Handling Complex Types](#7-handling-complex-types)
   - 7.1 [Structs](#71-structs)
   - 7.2 [Arrays](#72-arrays)
   - 7.3 [Maps](#73-maps)
8. [Handling NULL Values](#8-handling-null-values)
   - 8.1 [Dropping NULL Values](#81-dropping-null-values)
   - 8.2 [Replacing NULL Values](#82-replacing-null-values)
9. [Summary](#9-summary)

---

## 1. Specifying the Schema of a DataFrame with StructType

### 1.1 StructType and StructField

A Spark **schema** is defined by a `StructType` — a collection of `StructField` objects, where each field describes one column:

```python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, FloatType,
    DecimalType, BooleanType, DateType, TimestampType,
    ArrayType, MapType
)

# Define a schema explicitly
schema = StructType([
    StructField("emp_id",     StringType(),       nullable=False),
    StructField("name",       StringType(),       nullable=True),
    StructField("age",        IntegerType(),      nullable=True),
    StructField("salary",     DoubleType(),       nullable=True),
    StructField("is_active",  BooleanType(),      nullable=True),
    StructField("hire_date",  DateType(),         nullable=True),
    StructField("last_login", TimestampType(),    nullable=True),
])

# Use the schema when reading
df = spark.read \
    .schema(schema) \
    .option("header", True) \
    .csv("/FileStore/datasets/employees.csv")

df.printSchema()
```

**All available Spark SQL data types:**

| PySpark Type         | SQL Type      | Python Equivalent | Notes                                            |
| -------------------- | ------------- | ----------------- | ------------------------------------------------ |
| `ByteType()`         | TINYINT       | int               | 1-byte signed integer (-128 to 127)              |
| `ShortType()`        | SMALLINT      | int               | 2-byte signed integer                            |
| `IntegerType()`      | INT           | int               | 4-byte signed integer                            |
| `LongType()`         | BIGINT        | int               | 8-byte signed integer (default for Python int)   |
| `FloatType()`        | FLOAT         | float             | 4-byte floating point                            |
| `DoubleType()`       | DOUBLE        | float             | 8-byte floating point (default for Python float) |
| `DecimalType(p,s)`   | DECIMAL(p,s)  | Decimal           | Exact fixed-point; p=precision, s=scale          |
| `StringType()`       | STRING        | str               | Unicode string                                   |
| `BinaryType()`       | BINARY        | bytes             | Raw bytes                                        |
| `BooleanType()`      | BOOLEAN       | bool              | True / False                                     |
| `DateType()`         | DATE          | datetime.date     | Year-month-day only                              |
| `TimestampType()`    | TIMESTAMP     | datetime.datetime | Date + time + microseconds                       |
| `TimestampNTZType()` | TIMESTAMP_NTZ | datetime.datetime | Timestamp without timezone                       |
| `ArrayType(T)`       | ARRAY\<T\>    | list              | Ordered collection of type T                     |
| `MapType(K, V)`      | MAP\<K,V\>    | dict              | Key-value pairs                                  |
| `StructType([...])`  | STRUCT\<...\> | nested object     | Nested record                                    |

---

### 1.2 DDL-Style Schema Strings

For simple schemas, DDL strings are more compact:

```python
# DDL string — equivalent to the StructType above
schema_ddl = """
    emp_id     STRING    NOT NULL,
    name       STRING,
    age        INT,
    salary     DOUBLE,
    is_active  BOOLEAN,
    hire_date  DATE,
    last_login TIMESTAMP
"""

df = spark.read \
    .schema(schema_ddl) \
    .option("header", True) \
    .csv("/FileStore/datasets/employees.csv")

# Convert StructType to DDL string
print(df.schema.simpleString())
# struct<emp_id:string,name:string,age:int,salary:double,...>

print(df.schema.json())    # JSON representation of the schema
```

---

### 1.3 Reading the Schema from an Existing Table

```python
# Get the schema from an existing DataFrame
existing_schema = spark.table("training.employees").schema
print(existing_schema)

# Reuse the schema for a new read (ensures consistent typing)
df_new = spark.read \
    .schema(existing_schema) \
    .option("header", True) \
    .csv("/FileStore/datasets/new_employees.csv")

# Inspect column types programmatically
for field in df.schema.fields:
    print(f"{field.name:20} | {str(field.dataType):20} | nullable: {field.nullable}")
```

---

## 2. Converting Literals to Spark Types: The `lit` Function

`lit()` wraps a Python scalar value into a **Spark Column** of the appropriate Spark type. It is essential when you need to combine a Python value with a Spark column in an expression.

```python
from pyspark.sql.functions import lit, col

# Add a constant column
df.withColumn("tax_rate",     lit(0.30)).show()       # DoubleType
df.withColumn("version",      lit(1)).show()           # IntegerType
df.withColumn("is_active",    lit(True)).show()        # BooleanType
df.withColumn("label",        lit("production")).show() # StringType
df.withColumn("null_col",     lit(None).cast("string")).show()  # NULL string

# lit() is needed when mixing Python scalars with column arithmetic
df.withColumn("salary_with_bonus", col("salary") + lit(5000)).show()
# vs — this also works because Spark auto-lifts scalars in column expressions:
df.withColumn("salary_with_bonus", col("salary") + 5000).show()

# lit() with explicit type
from pyspark.sql.types import DecimalType
df.withColumn("rate", lit(0.0825).cast(DecimalType(5, 4))).show()
```

**Type mapping: Python → Spark `lit()` types:**

| Python value             | Spark type produced by `lit()`    |
| ------------------------ | --------------------------------- |
| `42` (int)               | `LongType`                        |
| `3.14` (float)           | `DoubleType`                      |
| `"hello"` (str)          | `StringType`                      |
| `True` / `False`         | `BooleanType`                     |
| `None`                   | `NullType` (cast to desired type) |
| `datetime.date(...)`     | `DateType`                        |
| `datetime.datetime(...)` | `TimestampType`                   |
| `[1, 2, 3]` (list)       | `ArrayType(LongType)`             |
| `{"a": 1}` (dict)        | `MapType(StringType, LongType)`   |

---

## 3. Working with Booleans

### 3.1 Boolean Operations and Filtering

```python
from pyspark.sql.functions import col, when

df = spark.createDataFrame([
    ("E001", "Alice",  65000.0, True,  "Engineering"),
    ("E002", "Bob",    48000.0, False, "Marketing"),
    ("E003", "Carol",  72000.0, True,  "Engineering"),
    ("E004", "David",  55000.0, True,  "Finance"),
    ("E005", "Eve",    82000.0, False, "Engineering"),
], ["emp_id", "name", "salary", "is_active", "dept"])

# Filter by boolean column
df.filter(col("is_active") == True).show()
df.filter(col("is_active")).show()          # shorthand for == True
df.filter(~col("is_active")).show()         # NOT — equivalent to == False

# Boolean expressions (result: boolean column)
df.withColumn("is_high_earner",   col("salary") > 70000).show()
df.withColumn("is_senior_eng",    (col("dept") == "Engineering") & (col("salary") > 70000)).show()
df.withColumn("is_low_or_new",    (col("salary") < 50000) | (~col("is_active"))).show()

# AND (&), OR (|), NOT (~) — use parentheses around each condition
active_engineers = df.filter(
    col("is_active") & (col("dept") == "Engineering")
)

# Convert boolean to int (True → 1, False → 0)
df.withColumn("active_int", col("is_active").cast("integer")).show()
```

```sql
%sql

-- Boolean WHERE conditions
SELECT * FROM employees WHERE is_active = TRUE;
SELECT * FROM employees WHERE is_active;              -- same as = TRUE
SELECT * FROM employees WHERE NOT is_active;
SELECT * FROM employees WHERE is_active AND dept = 'Engineering';
SELECT name, is_active, is_active AND (salary > 60000) AS active_high_earner
FROM employees;
```

---

### 3.2 Boolean Aggregations

```python
from pyspark.sql.functions import sum as spark_sum, count, col

# Count where a boolean column is True
df.agg(
    spark_sum(col("is_active").cast("integer")).alias("active_count"),
    count("*").alias("total"),
).show()

# Percentage active per department
df.groupBy("dept").agg(
    count("*").alias("total"),
    spark_sum(col("is_active").cast("integer")).alias("active_count"),
).withColumn(
    "active_pct",
    (col("active_count") / col("total") * 100).cast("decimal(5,2)")
).show()
```

---

## 4. Working with Numbers

### 4.1 Numeric Types in Spark

```python
from pyspark.sql.types import IntegerType, LongType, DoubleType, FloatType, DecimalType
from pyspark.sql.functions import col

df = spark.createDataFrame([
    (1,  "Alice",  65000.123456789, 3.14),
    (2,  "Bob",    48000.987654321, 2.71),
], ["id", "name", "salary", "score"])

# Cast between numeric types
df.withColumn("salary_int",     col("salary").cast(IntegerType())).show()
df.withColumn("salary_long",    col("salary").cast("long")).show()
df.withColumn("salary_float",   col("salary").cast(FloatType())).show()
df.withColumn("salary_decimal", col("salary").cast(DecimalType(12, 2))).show()

# DecimalType(precision, scale)
# precision = total significant digits
# scale = digits after decimal point
# DecimalType(10, 2) can store up to 99999999.99
```

**Choosing the right numeric type:**

| Type          | Range              | Precision          | Use Case                        |
| ------------- | ------------------ | ------------------ | ------------------------------- |
| `ByteType`    | -128 to 127        | Exact              | Flags, status codes             |
| `ShortType`   | -32768 to 32767    | Exact              | Small integers                  |
| `IntegerType` | ~-2.1B to 2.1B     | Exact              | General integers, row counts    |
| `LongType`    | ~-9.2E18 to 9.2E18 | Exact              | Large integers, Unix timestamps |
| `FloatType`   | ~1.4E-45 to 3.4E38 | ~7 decimal digits  | Approximate float (less common) |
| `DoubleType`  | ~5E-324 to 1.8E308 | ~16 decimal digits | General floating point          |
| `DecimalType` | User-defined       | Exact              | Financial data, prices, rates   |

---

### 4.2 Arithmetic and Math Functions

```python
from pyspark.sql.functions import (
    abs, ceil, floor, round, bround,
    sqrt, cbrt, exp, log, log2, log10, pow,
    sin, cos, tan, asin, acos, atan, atan2,
    greatest, least, signum,
    factorial, rand, randn
)

df.select(
    col("salary"),
    abs(col("salary") - 60000).alias("deviation"),
    sqrt(col("salary")).alias("salary_sqrt"),
    pow(col("salary"), 2).alias("salary_squared"),
    log(col("salary")).alias("salary_ln"),          # natural log
    log2(col("salary")).alias("salary_log2"),
    log10(col("salary")).alias("salary_log10"),
    exp(col("score")).alias("e_to_score"),
    ceil(col("score")).alias("score_ceil"),
    floor(col("score")).alias("score_floor"),
    round(col("score"), 1).alias("score_round1"),
    bround(col("score"), 1).alias("score_bround1"),  # banker's rounding
    signum(col("salary") - 60000).alias("above_60k"), # -1, 0, or 1
).show()

# greatest / least — element-wise max/min across columns
spark.createDataFrame([(1, 5, 3), (6, 2, 4)], ["a", "b", "c"]) \
    .withColumn("max_val", greatest("a", "b", "c")) \
    .withColumn("min_val", least("a", "b", "c")) \
    .show()

# Random numbers
df.withColumn("rand_uniform",  rand(seed=42)).show()   # uniform [0, 1)
df.withColumn("rand_normal",   randn(seed=42)).show()  # standard normal N(0,1)
```

---

### 4.3 Rounding and Precision

```python
from pyspark.sql.functions import round, bround, format_number, col
from pyspark.sql.types import DecimalType

# round(col, scale) — rounds to 'scale' decimal places (half-up)
df.withColumn("salary_2dp", round(col("salary"), 2)).show()
df.withColumn("salary_0dp", round(col("salary"), 0)).show()
df.withColumn("salary_neg", round(col("salary"), -3)).show()  # round to nearest 1000

# bround — banker's rounding (rounds 0.5 to even — avoids systematic bias)
# Example: bround(2.5, 0) = 2; bround(3.5, 0) = 4
df.withColumn("bankers_round", bround(col("salary"), 0)).show()

# format_number — for display/output (returns STRING type)
df.withColumn("formatted_salary", format_number(col("salary"), 2)).show()
# '65,000.12' (adds thousands separators)

# Cast to DecimalType for exact arithmetic
df.withColumn("exact_salary",
    col("salary").cast(DecimalType(12, 2))
).show()
```

---

### 4.4 Type Casting

```python
from pyspark.sql.functions import col

# Cast using .cast()
df.withColumn("age_string",  col("age").cast("string")).show()
df.withColumn("salary_int",  col("salary").cast("integer")).show()
df.withColumn("score_float", col("score").cast("float")).show()

# Cast using type objects
from pyspark.sql.types import DoubleType, StringType
df.withColumn("salary_dbl", col("salary").cast(DoubleType())).show()

# Cast string → date / timestamp
from pyspark.sql.functions import to_date, to_timestamp
df.withColumn("date_col",  to_date(col("date_string"), "yyyy-MM-dd")).show()
df.withColumn("ts_col",    to_timestamp(col("ts_string"), "yyyy-MM-dd HH:mm:ss")).show()

# trycast — returns NULL instead of error on bad values (Spark 3.x+)
df.withColumn("safe_int", col("input").cast("integer"))
# invalid strings become null automatically

# Check result of cast
df.select(
    col("salary_string"),
    col("salary_string").cast("double").alias("salary_double"),
).show()
```

---

## 5. Working with Strings

### 5.1 String Functions

```python
from pyspark.sql.functions import (
    upper, lower, initcap, length, trim, ltrim, rtrim,
    lpad, rpad, concat, concat_ws,
    substr, substring, locate, instr,
    replace, translate, repeat, reverse, overlay,
    split, regexp_extract, regexp_replace,
    col, lit
)

df.select(
    col("name"),

    # Case
    upper(col("name")).alias("name_upper"),
    lower(col("name")).alias("name_lower"),
    initcap(col("name")).alias("name_title"),   # first letter of each word capitalised

    # Whitespace
    trim(col("name")).alias("name_trim"),
    ltrim(col("name")).alias("name_ltrim"),
    rtrim(col("name")).alias("name_rtrim"),

    # Length
    length(col("name")).alias("name_len"),

    # Padding
    lpad(col("emp_id"), 8, "0").alias("padded_id"),     # left-pad with "0" to width 8
    rpad(col("dept"),   15, ".").alias("padded_dept"),  # right-pad with "." to width 15

    # Concatenation
    concat(col("name"), lit(" - "), col("dept")).alias("name_dept"),
    concat_ws(" | ", col("name"), col("dept"), col("city")).alias("piped"),

    # Substrings
    substr(col("name"), 1, 3).alias("first_3"),            # 1-indexed
    substring(col("name"), 2, 4).alias("substr_2_4"),      # start=2, length=4

    # Locate / Find
    locate("a", col("name")).alias("pos_of_a"),            # 0 if not found
    instr(col("name"), "a").alias("instr_a"),              # 0 if not found

    # Replace
    replace(col("name"), "a", "@").alias("at_replaced"),
    translate(col("name"), "aeiou", "*****").alias("vowels_replaced"),  # char-by-char

    # Repeat / Reverse
    repeat(col("dept"), 2).alias("dept_doubled"),
    reverse(col("name")).alias("name_reversed"),
).show(truncate=False)
```

---

### 5.2 Regular Expressions

```python
from pyspark.sql.functions import regexp_extract, regexp_replace, rlike, col

# regexp_extract(col, pattern, group_index)
df.withColumn("first_word",
    regexp_extract(col("full_name"), r"^(\w+)", 1)
).show()

df.withColumn("email_domain",
    regexp_extract(col("email"), r"@(.+)$", 1)
).show()

df.withColumn("area_code",
    regexp_extract(col("phone"), r"^\((\d{3})\)", 1)
).show()

# Multiple groups
df.withColumn("year",
    regexp_extract(col("date_str"), r"(\d{4})-(\d{2})-(\d{2})", 1)  # group 1: year
).show()

# regexp_replace(col, pattern, replacement)
df.withColumn("clean_phone",
    regexp_replace(col("phone"), r"[\s\-\(\)]", "")   # remove spaces, dashes, parens
).show()

df.withColumn("no_html",
    regexp_replace(col("description"), r"<[^>]+>", "")  # strip HTML tags
).show()

# Filter using rlike
df.filter(col("name").rlike(r"^[A-C]")).show()           # starts with A, B, or C
df.filter(col("email").rlike(r"@gmail\.com$")).show()    # Gmail addresses
```

```sql
%sql
-- SQL regex functions
SELECT name, REGEXP_EXTRACT(email, '@(.+)$', 1) AS domain FROM employees;
SELECT name, REGEXP_REPLACE(phone, '[^0-9]', '') AS digits_only FROM employees;
SELECT * FROM employees WHERE name RLIKE '^[A-C]';
```

---

### 5.3 String Splitting and Parsing

```python
from pyspark.sql.functions import split, explode, col, concat_ws

# split(col, pattern) → ArrayType(StringType)
df.withColumn("name_parts", split(col("full_name"), " ")).show()

# Access elements of the split array
df.withColumn("first_name", split(col("full_name"), " ").getItem(0)) \
  .withColumn("last_name",  split(col("full_name"), " ").getItem(1)) \
  .show()

# Explode an array into rows (one row per array element)
tags_df = spark.createDataFrame([
    ("E001", "python,spark,delta"),
    ("E002", "sql,power-bi"),
], ["emp_id", "tags_str"])

tags_df \
    .withColumn("tag_array", split(col("tags_str"), ",")) \
    .withColumn("tag",       explode(col("tag_array"))) \
    .select("emp_id", "tag") \
    .show()
```

---

## 6. Working with Dates and Timestamps

### 6.1 Date and Timestamp Types

| Type               | SQL Type      | Description                                                   |
| ------------------ | ------------- | ------------------------------------------------------------- |
| `DateType`         | DATE          | Year, month, day only — no time component                     |
| `TimestampType`    | TIMESTAMP     | Date + time + microseconds; timezone-aware (session timezone) |
| `TimestampNTZType` | TIMESTAMP_NTZ | Date + time; no timezone (Spark 3.4+)                         |

```python
import datetime
from pyspark.sql.functions import current_date, current_timestamp, lit

# Create a DataFrame with date and timestamp values
df = spark.createDataFrame([
    ("E001", datetime.date(2023, 1, 15),
             datetime.datetime(2023, 1, 15, 9, 30, 0)),
], ["emp_id", "hire_date", "last_login"])

df.printSchema()
# root
#  |-- emp_id: string (nullable = true)
#  |-- hire_date: date (nullable = true)
#  |-- last_login: timestamp (nullable = true)

# Add current date and timestamp
df.withColumn("today", current_date()) \
  .withColumn("now",   current_timestamp()) \
  .show()
```

---

### 6.2 Creating and Parsing Dates

```python
from pyspark.sql.functions import to_date, to_timestamp, col, lit

# Parse strings to dates
df.withColumn("hire_date",
    to_date(col("hire_date_str"), "yyyy-MM-dd")
).show()

# Multiple format patterns
df.withColumn("hire_date", to_date(col("raw"), "MM/dd/yyyy")).show()
df.withColumn("hire_date", to_date(col("raw"), "dd-MMM-yyyy")).show()  # 15-Jan-2023
df.withColumn("hire_date", to_date(col("raw"), "yyyyMMdd")).show()     # 20230115

# Parse strings to timestamps
df.withColumn("login_ts",
    to_timestamp(col("login_str"), "yyyy-MM-dd HH:mm:ss")
).show()

df.withColumn("login_ts",
    to_timestamp(col("login_str"), "MM/dd/yyyy hh:mm a")  # 01/15/2023 09:30 AM
).show()

# Create date from components
from pyspark.sql.functions import make_date, make_timestamp
df.withColumn("constructed_date",
    make_date(col("year_col"), col("month_col"), col("day_col"))
).show()

# Unix timestamp ↔ Timestamp
from pyspark.sql.functions import from_unixtime, unix_timestamp
df.withColumn("from_unix", from_unixtime(col("unix_ts"))).show()
df.withColumn("to_unix",   unix_timestamp(col("event_ts"))).show()
```

**Date format patterns (Java SimpleDateFormat):**

| Pattern | Meaning               | Example |
| ------- | --------------------- | ------- |
| `yyyy`  | 4-digit year          | 2024    |
| `yy`    | 2-digit year          | 24      |
| `MM`    | 2-digit month         | 01      |
| `MMM`   | Month abbreviation    | Jan     |
| `MMMM`  | Full month name       | January |
| `dd`    | Day of month          | 05      |
| `HH`    | Hour (24-hour)        | 14      |
| `hh`    | Hour (12-hour)        | 02      |
| `mm`    | Minutes               | 30      |
| `ss`    | Seconds               | 45      |
| `SSS`   | Milliseconds          | 123     |
| `a`     | AM/PM                 | AM      |
| `z`     | Timezone abbreviation | UTC     |
| `Z`     | Timezone offset       | +0530   |

---

### 6.3 Date Arithmetic

```python
from pyspark.sql.functions import (
    datediff, date_add, date_sub,
    months_between, add_months,
    date_trunc, next_day,
    col, current_date
)

df.select(
    col("hire_date"),

    # Difference between two dates (in days)
    datediff(current_date(), col("hire_date")).alias("tenure_days"),

    # Add / subtract days
    date_add(col("hire_date"), 90).alias("probation_end"),
    date_sub(col("hire_date"), 30).alias("notice_start"),

    # Months between (decimal)
    months_between(current_date(), col("hire_date")).alias("tenure_months"),

    # Add months
    add_months(col("hire_date"), 6).alias("6_months_after_hire"),

    # Truncate to period
    date_trunc("year",    col("hire_date")).alias("year_start"),
    date_trunc("month",   col("hire_date")).alias("month_start"),
    date_trunc("week",    col("hire_date")).alias("week_start"),
    date_trunc("quarter", col("hire_date")).alias("quarter_start"),

    # Next specific day of week
    next_day(col("hire_date"), "Monday").alias("next_monday"),
).show()
```

---

### 6.4 Date Formatting and Extraction

```python
from pyspark.sql.functions import (
    date_format, year, month, dayofmonth,
    dayofweek, dayofyear, weekofyear,
    hour, minute, second, quarter,
    last_day, col
)

df.select(
    col("hire_date"),

    # Format to string
    date_format(col("hire_date"), "dd/MM/yyyy").alias("uk_format"),
    date_format(col("hire_date"), "MMMM d, yyyy").alias("long_format"),
    date_format(col("hire_date"), "yyyy-'Q'Q").alias("year_quarter"),  # 2023-Q1

    # Extract components (return IntegerType)
    year(col("hire_date")).alias("hire_year"),
    month(col("hire_date")).alias("hire_month"),
    quarter(col("hire_date")).alias("hire_quarter"),
    dayofmonth(col("hire_date")).alias("hire_day"),
    dayofweek(col("hire_date")).alias("hire_dow"),    # 1=Sunday, 7=Saturday
    dayofyear(col("hire_date")).alias("hire_doy"),
    weekofyear(col("hire_date")).alias("hire_week"),
    last_day(col("hire_date")).alias("month_end"),

    # Extract from timestamp
    hour(col("last_login")).alias("login_hour"),
    minute(col("last_login")).alias("login_minute"),
    second(col("last_login")).alias("login_second"),
).show()
```

```sql
%sql

-- SQL date functions
SELECT
    hire_date,
    DATE_FORMAT(hire_date, 'dd/MM/yyyy')    AS uk_format,
    YEAR(hire_date)                          AS hire_year,
    MONTH(hire_date)                         AS hire_month,
    DAY(hire_date)                           AS hire_day,
    QUARTER(hire_date)                       AS hire_quarter,
    DATEDIFF(CURRENT_DATE(), hire_date)      AS tenure_days,
    DATE_ADD(hire_date, 90)                  AS probation_end,
    DATE_TRUNC('MONTH', hire_date)           AS month_start,
    LAST_DAY(hire_date)                      AS month_end
FROM training.employees;
```

---

### 6.5 Timezones

```python
from pyspark.sql.functions import (
    to_utc_timestamp, from_utc_timestamp, convert_timezone, col
)

# Convert a local timestamp to UTC
df.withColumn("hire_ts_utc",
    to_utc_timestamp(col("hire_ts_local"), "America/New_York")
).show()

# Convert UTC to a local timezone
df.withColumn("hire_ts_ist",
    from_utc_timestamp(col("hire_ts_utc"), "Asia/Kolkata")
).show()

# Check Spark session timezone
spark.conf.get("spark.sql.session.timeZone")

# Set session timezone
spark.conf.set("spark.sql.session.timeZone", "UTC")
```

---

## 7. Handling Complex Types

### 7.1 Structs

A **Struct** is a nested record — a column that contains sub-columns (like a JSON object embedded in a row):

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import struct, col

# Create a DataFrame where 'address' is a struct column
schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("name",   StringType(), True),
    StructField("address", StructType([
        StructField("street",  StringType(), True),
        StructField("city",    StringType(), True),
        StructField("country", StringType(), True),
    ]), True),
])

data = [("E001", "Alice", ("123 Main St", "New York",  "USA")),
        ("E002", "Bob",   ("45 King St",  "Chicago",   "USA"))]

df = spark.createDataFrame(data, schema)
df.printSchema()

# Access nested struct fields using dot notation
df.select(col("name"), col("address.city"), col("address.country")).show()

# Create a struct column from existing columns
df2 = spark.createDataFrame([
    ("E001", "Alice", "123 Main St", "New York", "USA"),
    ("E002", "Bob",   "45 King St",  "Chicago",  "USA"),
], ["emp_id", "name", "street", "city", "country"])

df2.withColumn("address", struct(
    col("street"),
    col("city"),
    col("country")
)).drop("street", "city", "country").show()

# Modify a nested field — create a new struct with the updated field
from pyspark.sql.functions import struct
df.withColumn("address",
    struct(
        col("address.street"),
        col("address.city"),
        lit("United States").alias("country")  # override country
    )
).show()
```

---

### 7.2 Arrays

An **Array** column holds an ordered list of elements of the same type:

```python
from pyspark.sql.types import ArrayType, StringType, IntegerType
from pyspark.sql.functions import (
    array, array_contains, array_distinct, array_union, array_intersect,
    array_except, array_sort, array_min, array_max, array_size,
    array_remove, array_position, array_zip, arrays_zip,
    flatten, explode, explode_outer, posexplode,
    collect_list, collect_set, slice, col, lit
)

# Create DataFrame with array columns
df = spark.createDataFrame([
    ("E001", "Alice",  ["Python", "Spark", "SQL"],    [95, 88, 92]),
    ("E002", "Bob",    ["SQL", "Power BI"],             [78, 85]),
    ("E003", "Carol",  ["Python", "Java", "Spark", "SQL"], [90, 82, 95, 88]),
    ("E004", "David",  None,                            []),
], ["emp_id", "name", "skills", "scores"])

df.printSchema()
# root
#  |-- skills: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- scores: array (nullable = true)
#  |    |-- element: integer (containsNull = true)

# Create array column from individual values
df2.withColumn("key_skills", array(lit("Python"), lit("SQL"), lit("Spark"))).show()

# Array inspection
df.withColumn("skill_count",    array_size(col("skills"))).show()
df.withColumn("has_python",     array_contains(col("skills"), "Python")).show()
df.withColumn("python_pos",     array_position(col("skills"), "Python")).show()  # 1-based, 0 = not found
df.withColumn("max_score",      array_max(col("scores"))).show()
df.withColumn("min_score",      array_min(col("scores"))).show()

# Array transformations
df.withColumn("skills_sorted",  array_sort(col("skills"))).show()
df.withColumn("unique_skills",  array_distinct(col("skills"))).show()
df.withColumn("removed",        array_remove(col("skills"), "SQL")).show()
df.withColumn("first_2",        slice(col("skills"), 1, 2)).show()  # start=1, length=2

# Set-like operations
from pyspark.sql.functions import lit
other = [lit("Python"), lit("Scala")]
df.withColumn("skills_union",    array_union(col("skills"), array(*other))).show()
df.withColumn("skills_intersect",array_intersect(col("skills"), array(*other))).show()
df.withColumn("skills_except",   array_except(col("skills"), array(*other))).show()
```

**Exploding arrays to rows:**

```python
# explode — one row per array element (drops rows where array is null or empty)
df.select("emp_id", "name", explode(col("skills")).alias("skill")).show()

# explode_outer — like explode but keeps rows with null/empty arrays (produces null)
df.select("emp_id", "name", explode_outer(col("skills")).alias("skill")).show()

# posexplode — returns position (index) AND element
df.select("emp_id", "name",
    posexplode(col("skills")).alias("pos", "skill")
).show()
```

**Collecting values into arrays:**

```python
# Reverse of explode: aggregate rows into an array
df_flat = spark.createDataFrame([
    ("E001", "Python"), ("E001", "Spark"),
    ("E002", "SQL"),    ("E002", "Power BI"),
], ["emp_id", "skill"])

# collect_list — includes duplicates
df_flat.groupBy("emp_id").agg(
    collect_list("skill").alias("skills"),
    collect_set("skill").alias("skills_unique"),  # removes duplicates
).show(truncate=False)
```

---

### 7.3 Maps

A **Map** column holds key-value pairs where all keys have the same type and all values have the same type:

```python
from pyspark.sql.types import MapType, StringType, IntegerType
from pyspark.sql.functions import (
    create_map, map_keys, map_values, map_contains_key,
    element_at, map_from_entries, map_from_arrays,
    explode, col, lit
)

# Create a DataFrame with a map column
df = spark.createDataFrame([
    ("E001", {"python": 90, "spark": 88, "sql": 92}),
    ("E002", {"sql": 78, "excel": 85}),
    ("E003", {"python": 95, "java": 82, "spark": 88}),
], ["emp_id", "skill_scores"])

df.printSchema()
# root
#  |-- skill_scores: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: integer (valueContainsNull = true)

# Access map values
df.withColumn("python_score",
    col("skill_scores")["python"]                # returns null if key missing
).show()

df.withColumn("python_score",
    element_at(col("skill_scores"), "python")    # same as above; more explicit
).show()

# Map inspection
df.withColumn("keys",   map_keys(col("skill_scores"))).show()
df.withColumn("values", map_values(col("skill_scores"))).show()
df.withColumn("has_python",
    map_contains_key(col("skill_scores"), "python")
).show()

# Create a map from individual columns
df2 = spark.createDataFrame([
    ("E001", "python", 90),
    ("E001", "spark",  88),
    ("E002", "sql",    78),
], ["emp_id", "skill", "score"])

# Aggregate key-value pairs into a map
from pyspark.sql.functions import map_from_entries, struct, collect_list
df2.groupBy("emp_id") \
   .agg(
       map_from_entries(
           collect_list(struct(col("skill"), col("score")))
       ).alias("skill_scores")
   ).show(truncate=False)

# Create map from two arrays
from pyspark.sql.functions import array, map_from_arrays
df.withColumn("demo_map",
    map_from_arrays(
        array(lit("key1"), lit("key2")),
        array(lit(100), lit(200))
    )
).show()

# Explode a map to rows (key + value per row)
df.select("emp_id",
    explode(col("skill_scores")).alias("skill", "score")
).show()
```

---

## 8. Handling NULL Values

### 8.1 Dropping NULL Values

```python
# Drop rows where ANY column is null
df.dropna().show()
df.na.drop().show()          # same using the DataFrameNaFunctions accessor

# Drop rows where ALL columns are null
df.dropna(how="all").show()
df.na.drop(how="all").show()

# Drop rows where specific columns have nulls
df.dropna(subset=["emp_id", "salary"]).show()
df.na.drop(subset=["emp_id", "salary"]).show()

# Drop rows that have fewer than 'thresh' non-null values
df.dropna(thresh=3).show()   # keep only rows with at least 3 non-null values
```

---

### 8.2 Replacing NULL Values

**`fillna()` / `na.fill()` — replace NULLs with a specified value:**

```python
# Fill all nulls with a single value (applies only to compatible columns)
df.fillna(0).show()         # fills numeric columns with 0
df.fillna("Unknown").show()  # fills string columns with "Unknown"

# Fill using a dictionary mapping column name → fill value
df.fillna({
    "salary":   0.0,
    "dept":     "Unknown",
    "manager":  "No Manager",
    "score":    0,
}).show()

# Using na.fill (identical)
df.na.fill({
    "salary": 0.0,
    "dept":   "Unknown",
}).show()
```

**`replace()` / `na.replace()` — replace specific values (including NULLs):**

```python
# Replace specific values in a column
df.replace("Unknown", "N/A", subset=["dept"]).show()

# Replace in all columns
df.replace("", "null_string").show()

# Replace multiple values
df.replace({"Engineering": "Tech", "Marketing": "Mktg"}, subset=["dept"]).show()
```

**Using `coalesce()` for conditional null handling:**

```python
from pyspark.sql.functions import coalesce, col, lit

# Return the first non-null value from a list of columns
df.withColumn("effective_salary",
    coalesce(col("adjusted_salary"), col("base_salary"), lit(0.0))
).show()

# Replace null with a computed default
df.withColumn("bonus",
    coalesce(col("annual_bonus"), col("salary") * lit(0.05))  # use 5% if bonus is null
).show()

# nullif — return null if two values are equal (prevents division by zero)
from pyspark.sql.functions import nullif
df.withColumn("rate",
    col("amount") / nullif(col("total"), lit(0))
).show()
```

**SQL NULL handling:**

```sql
%sql

-- Display
SELECT
    name,
    COALESCE(salary, 0)             AS salary,
    IFNULL(manager_id, 'Self')      AS manager,
    NULLIF(bonus, 0)                AS bonus_or_null,
    NVL2(manager_id, 'Has Manager', 'Top Level') AS mgr_status
FROM employees;

-- Drop rows with NULLs
SELECT * FROM employees WHERE emp_id IS NOT NULL AND salary IS NOT NULL;

-- Fill NULLs in aggregation (NULL is excluded from aggregation by default)
SELECT dept,
    AVG(COALESCE(salary, 0)) AS avg_salary_with_zeros,
    AVG(salary)              AS avg_salary_excludes_nulls
FROM employees
GROUP BY dept;
```

---

## 9. Summary

| Topic                 | Key Takeaway                                                                                 |
| --------------------- | -------------------------------------------------------------------------------------------- |
| **StructType**        | Explicit schema prevents inference errors; use `StructField` per column                      |
| **DDL schema string** | Compact alternative to StructType: `"col1 STRING, col2 INT"`                                 |
| **`lit()`**           | Wraps Python scalars into Spark Column type; required in expressions                         |
| **Boolean**           | Filter with `col("bool_col")` (no `== True`); use `&`, `\|`, `~` for logic                   |
| **Numeric types**     | Use `DecimalType` for financial data; `DoubleType` for general float                         |
| **Math functions**    | `round`, `sqrt`, `abs`, `pow`, `log`, `ceil`, `floor` — all in `pyspark.sql.functions`       |
| **String functions**  | `upper`, `lower`, `trim`, `concat`, `substr`, `replace`, `regexp_extract`                    |
| **Regex**             | `regexp_extract(col, pattern, group)` to extract; `regexp_replace` to replace                |
| **DateType**          | Date only (no time); parse with `to_date(col, "format")`                                     |
| **TimestampType**     | Date + time; parse with `to_timestamp(col, "format")`                                        |
| **Date arithmetic**   | `datediff`, `date_add`, `months_between`, `add_months`, `date_trunc`                         |
| **Struct**            | Nested record; access with `col("parent.child")`                                             |
| **Array**             | Ordered list; `explode` to rows; `collect_list` to aggregate; `array_contains`, `array_size` |
| **Map**               | Key-value pairs; access with `col["key"]` or `element_at`; `explode` to rows                 |
| **dropna**            | Remove rows with nulls; control with `how`, `thresh`, `subset`                               |
| **fillna**            | Replace nulls with defaults; use dict for per-column values                                  |
| **coalesce**          | Return first non-null from multiple columns; best for cascading defaults                     |

---

**Previous guide:** [Guide 06 — Transforming Data with Apache Spark](06-transforming-data-with-spark.md)

**Continue to:** [Guide 08 — Spark SQL and Advanced DataFrame Operations](08-spark-sql-advanced-dataframe-operations.md)

---

## Guide Series Overview

| #      | Guide                                                                                                                | Topics                                                                 |
| ------ | -------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| 01     | [Databricks: Introduction, History, Architecture & Core Concepts](01-databricks-intro-architecture-core-concepts.md) | Platform overview, Lakehouse, control/data plane                       |
| 02     | [Setting Up a Databricks Cluster on AWS](02-databricks-cluster-setup-aws.md)                                         | IAM, VPC, S3, cluster creation                                         |
| 03     | [Introduction to Databricks with Apache Spark](03-spark-introduction-first-code-architecture.md)                     | Spark overview, first code, cluster architecture                       |
| 04     | [Working with Datasets and Notebooks](04-datasets-notebooks-magic-commands.md)                                       | Magic commands, DBFS, CSV reading                                      |
| 05     | [DataFrames and Spark SQL Fundamentals](05-dataframes-spark-sql-fundamentals.md)                                     | DataFrame creation, filtering, aggregation, joins, window functions    |
| 06     | [Transforming Data with Apache Spark](06-transforming-data-with-spark.md)                                            | withColumn, filter, join, aggregations, UDFs, internal architecture    |
| **07** | **[Exploring Different Data Types in Spark](07-data-types-in-spark.md)**                                             | **StructType, booleans, numbers, strings, dates, arrays, maps, nulls** |
| 08     | [Spark SQL and Advanced DataFrame Operations](08-spark-sql-advanced-dataframe-operations.md)                         | TempViews, tables, databases, SQL syntax, joins, predicates, CASE      |
