# Databricks: Introduction, History, Architecture & Core Concepts

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Background & History](#2-background--history)
3. [The Databricks Lakehouse Platform](#3-the-databricks-lakehouse-platform)
4. [Architecture Overview](#4-architecture-overview)
   - 4.1 [Control Plane vs Data Plane](#41-control-plane-vs-data-plane)
   - 4.2 [Cluster Architecture](#42-cluster-architecture)
   - 4.3 [Storage Layer — Delta Lake](#43-storage-layer--delta-lake)
5. [Core Concepts](#5-core-concepts)
   - 5.1 [Workspaces](#51-workspaces)
   - 5.2 [Clusters](#52-clusters)
   - 5.3 [Notebooks](#53-notebooks)
   - 5.4 [Jobs & Workflows](#54-jobs--workflows)
   - 5.5 [Delta Lake](#55-delta-lake)
   - 5.6 [Delta Live Tables (DLT)](#56-delta-live-tables-dlt)
   - 5.7 [Unity Catalog](#57-unity-catalog)
   - 5.8 [MLflow](#58-mlflow)
   - 5.9 [Databricks SQL](#59-databricks-sql)
   - 5.10 [Databricks Runtime](#510-databricks-runtime)
6. [Databricks on AWS](#6-databricks-on-aws)
7. [Key Differentiators](#7-key-differentiators)
8. [Summary](#8-summary)

---

## 1. Introduction

**Databricks** is a unified, cloud-native data intelligence platform that brings together **data engineering**, **data science**, **machine learning**, and **analytics** into a single collaborative environment. It is built on top of **Apache Spark** and extends it with managed infrastructure, an optimized runtime, and a rich set of higher-level abstractions.

At its core, Databricks enables organisations to:

- Ingest, transform, and store large-scale data efficiently.
- Build and deploy machine learning models at scale.
- Run interactive analytics and BI queries on data lakes.
- Govern and secure data assets across the enterprise.

Databricks is available natively on **AWS**, **Azure**, and **Google Cloud Platform (GCP)**, and follows a SaaS (Software-as-a-Service) delivery model where Databricks manages the platform while customer data stays in their own cloud account.

---

## 2. Background & History

### 2.1 Origins — Apache Spark

The story of Databricks begins at the **AMPLab at UC Berkeley** (Algorithms, Machines, and People Lab). In **2009**, researchers led by **Matei Zaharia** began working on a new distributed computing engine to overcome the limitations of **Hadoop MapReduce**, which was:

- Disk-bound (every intermediate result written to HDFS)
- Slow for iterative algorithms (e.g., machine learning)
- Complex to program

The result was **Apache Spark**, first published in 2010. Spark introduced **Resilient Distributed Datasets (RDDs)** — an in-memory, fault-tolerant distributed data abstraction — delivering up to **100× faster** performance than Hadoop for iterative workloads.

Spark was donated to the **Apache Software Foundation** in **2013** and became a top-level project, rapidly becoming the most active open-source project in big data.

### 2.2 Founding of Databricks (2013)

In **2013**, Matei Zaharia and co-founders **Ali Ghodsi**, **Andy Konwinski**, **Patrick Wendell**, **Reynold Xin**, **Scott Shenker**, and **Ion Stoica** — all from the original Spark team — founded **Databricks** with a mission to:

> *"Make data + AI simple and accessible for everyone."*

The company was founded to commercialise Spark and build a managed cloud platform around it, removing the operational burden of running distributed systems.

### 2.3 Key Milestones

| Year | Milestone |
|------|-----------|
| 2009 | Apache Spark research begins at AMPLab, UC Berkeley |
| 2010 | Apache Spark first paper published |
| 2013 | Apache Spark donated to Apache Foundation; Databricks founded |
| 2015 | Databricks Cloud launched (managed Spark platform) |
| 2017 | Structured Streaming generally available in Spark 2.0 |
| 2019 | **Delta Lake** open-sourced — ACID transactions for data lakes |
| 2020 | **MLflow 1.0** released; **Databricks SQL** (formerly SQL Analytics) launched |
| 2021 | **Lakehouse** architecture coined; **Delta Live Tables** announced |
| 2021 | **Unity Catalog** introduced for unified data governance |
| 2022 | Databricks acquires **MosaicML** (AI training) |
| 2023 | **Dolly** open-source LLM released; **Databricks AI** / GenAI features accelerated |
| 2024 | **Unity Catalog open-sourced**; valuation exceeds $43 billion |
| 2025 | Databricks acquires **Neon** (serverless Postgres) for $1 billion |

### 2.4 Funding & Scale

Databricks has raised over **$3.5 billion** in funding and is one of the most valuable private technology companies globally. It serves over **10,000 customers** worldwide, including enterprises across financial services, healthcare, retail, media, and the public sector.

---

## 3. The Databricks Lakehouse Platform

### 3.1 The Data Warehouse Problem

Traditional **data warehouses** (e.g., Redshift, Snowflake, Teradata) are optimised for structured SQL analytics but:

- Are expensive for storing large volumes of raw data.
- Cannot natively handle unstructured data (images, text, video).
- Struggle with ML workloads that require raw feature data.

### 3.2 The Data Lake Problem

**Data lakes** (raw files on S3/ADLS/GCS) offer cheap, schema-flexible storage but:

- Lack ACID transactions — partial writes corrupt data.
- Have no data quality enforcement.
- Are difficult to manage at scale (no schema evolution, no versioning).
- Suffer from "data swamp" syndrome.

### 3.3 The Lakehouse Architecture

Databricks coined the term **"Lakehouse"** to describe an architecture that combines the best of both worlds:

```
┌─────────────────────────────────────────────────────────┐
│                    LAKEHOUSE                            │
│                                                         │
│  ┌─────────────────┐    ┌─────────────────────────┐    │
│  │   Data Lake     │    │    Data Warehouse        │    │
│  │                 │    │                          │    │
│  │ • Low cost      │ +  │ • ACID transactions      │    │
│  │ • All data types│    │ • Schema enforcement     │    │
│  │ • Open formats  │    │ • BI & SQL performance   │    │
│  │ • Scalability   │    │ • Data quality           │    │
│  └─────────────────┘    └─────────────────────────┘    │
│                                                         │
│  Enabled by: Delta Lake (open format on object storage) │
└─────────────────────────────────────────────────────────┘
```

The Lakehouse enables:
- One copy of data serving all workloads (ETL, BI, ML, streaming).
- ACID-compliant operations on open file formats (Parquet + Delta).
- Separation of storage and compute.

---

## 4. Architecture Overview

### 4.1 Control Plane vs Data Plane

Databricks uses a **two-plane architecture** that separates platform management from customer data:

```
┌───────────────────────────────────────────────────────────────┐
│                    DATABRICKS CONTROL PLANE                   │
│                  (Managed by Databricks)                      │
│                                                               │
│  • Web Application (UI)                                       │
│  • REST API Gateway                                           │
│  • Cluster Manager                                            │
│  • Job Scheduler                                              │
│  • Notebook Service                                           │
│  • Unity Catalog Metastore (metadata only)                    │
│  • CI/CD Pipelines (Repos)                                    │
└─────────────────────────┬─────────────────────────────────────┘
                          │  Secure HTTPS / VPC Peering
┌─────────────────────────▼─────────────────────────────────────┐
│                     CUSTOMER DATA PLANE                       │
│              (Lives in the Customer's AWS Account)            │
│                                                               │
│  ┌────────────────────────┐   ┌───────────────────────────┐  │
│  │    Compute (EC2)       │   │    Storage (S3)            │  │
│  │                        │   │                            │  │
│  │  • Driver Node         │   │  • Delta Tables            │  │
│  │  • Worker Nodes        │   │  • Raw Files (CSV, JSON,   │  │
│  │  • Spark Executors     │   │    Parquet, etc.)          │  │
│  │  • Photon Engine       │   │  • MLflow Artifacts        │  │
│  └────────────────────────┘   └───────────────────────────┘  │
│                                                               │
│  • VPC / Subnets                                              │
│  • IAM Roles & Policies                                       │
│  • Security Groups                                            │
└───────────────────────────────────────────────────────────────┘
```

**Key insight:** Customer data **never leaves** the customer's cloud account. The control plane only stores metadata and orchestration information.

### 4.2 Cluster Architecture

A Databricks **cluster** is a set of cloud VM instances that run Apache Spark.

```
┌─────────────────────────────────────────────────┐
│                  SPARK CLUSTER                  │
│                                                 │
│  ┌─────────────┐                                │
│  │ Driver Node │  ← SparkContext, DAG Scheduler │
│  │  (Master)   │    Query Planning, Results     │
│  └──────┬──────┘                                │
│         │ Task Distribution                     │
│  ┌──────▼───────────────────────────────────┐   │
│  │              Worker Nodes                │   │
│  │  ┌──────────┐  ┌──────────┐  ┌────────┐ │   │
│  │  │Executor 1│  │Executor 2│  │Exec. N │ │   │
│  │  │          │  │          │  │        │ │   │
│  │  │ Tasks    │  │ Tasks    │  │ Tasks  │ │   │
│  │  │ Cache    │  │ Cache    │  │ Cache  │ │   │
│  │  └──────────┘  └──────────┘  └────────┘ │   │
│  └──────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
```

#### Cluster Types

| Type | Description | Use Case |
|------|-------------|----------|
| **All-Purpose Cluster** | Long-running, interactive | Notebooks, exploration, development |
| **Job Cluster** | Ephemeral, created per job run | Production pipelines, automated jobs |
| **SQL Warehouse** | Optimised for SQL queries | BI tools, Databricks SQL, dashboards |

#### Cluster Modes

| Mode | Description |
|------|-------------|
| **Standard** | Single-user, full Spark |
| **High Concurrency** | Multi-user, fine-grained resource sharing |
| **Single Node** | Driver-only, no workers — for small data / local testing |

### 4.3 Storage Layer — Delta Lake

All persistent data in Databricks is typically stored using **Delta Lake format** on cloud object storage (e.g., Amazon S3).

```
S3 Bucket
├── delta_table/
│   ├── _delta_log/              ← Transaction log (JSON + Parquet)
│   │   ├── 00000000000000000000.json
│   │   ├── 00000000000000000001.json
│   │   └── ...checkpoint files
│   ├── part-00000-<uuid>.snappy.parquet
│   ├── part-00001-<uuid>.snappy.parquet
│   └── ...
```

Delta Lake provides:
- **ACID transactions** via the `_delta_log`
- **Time Travel** — query historical versions
- **Schema enforcement & evolution**
- **Audit history** of all operations

---

## 5. Core Concepts

### 5.1 Workspaces

A **Databricks Workspace** is the primary organisational unit — a collaborative environment where data engineers, scientists, and analysts work together.

Each workspace contains:
- Notebooks (interactive code)
- Folders & files
- Clusters
- Jobs & pipelines
- SQL Warehouses
- Data catalog (Unity Catalog)
- Repos (Git integration)
- Secrets (credential management)

Workspaces are mapped to a specific cloud region and correspond to a Databricks account.

### 5.2 Clusters

Clusters are the **compute** backbone of Databricks. They provide the Spark engine and runtime environment.

**Cluster Lifecycle:**
```
PENDING → RUNNING → TERMINATING → TERMINATED
           ↑                          |
           └──── Auto-restart ────────┘
```

**Auto-scaling:** Clusters can automatically scale the number of worker nodes up and down based on workload demand, minimising cost.

**Auto-termination:** Idle clusters automatically terminate after a configurable period (default: 120 minutes) to prevent runaway costs.

**Cluster Policies:** Administrators can define policies that restrict which cluster configurations users can choose, enforcing governance and cost controls.

### 5.3 Notebooks

Databricks **Notebooks** are browser-based interactive documents that combine:
- **Code cells** (Python, Scala, SQL, R, or Bash)
- **Markdown cells** (documentation)
- **Visualisations** (built-in charts)

Key notebook features:

| Feature | Description |
|---------|-------------|
| **Multi-language** | Mix Python, SQL, Scala in a single notebook using `%python`, `%sql`, `%scala` magic |
| **Real-time collaboration** | Multiple users can co-edit simultaneously (like Google Docs) |
| **Widgets** | Parameterise notebooks with input widgets |
| **Versioning** | Built-in revision history; Git integration via Repos |
| **Run as Job** | Notebooks can be scheduled as automated jobs |
| **%run** | Include and execute another notebook inline |
| **dbutils** | Databricks utility library for file system, secrets, widgets |

### 5.4 Jobs & Workflows

Databricks **Jobs** provide a way to run notebooks, Python scripts, JARs, and Delta Live Table pipelines on a schedule or programmatically.

**Workflows** (Multi-Task Jobs) allow you to define a **Directed Acyclic Graph (DAG)** of tasks:

```
Task A (Ingest)
    ↓
Task B (Clean)    Task C (Validate)
    ↓                   ↓
         Task D (Aggregate)
                ↓
         Task E (Report)
```

Each task can:
- Use a different cluster or compute
- Execute notebooks, Python scripts, JARs, Spark Submit, or DLT pipelines
- Be triggered on success, failure, or completion of previous tasks
- Be retried on failure

**Triggers:**
- **Scheduled** — cron-based scheduling
- **File Arrival** — trigger on new data landing in a path
- **Continuous** — always-on pipeline execution
- **Manual** — on-demand

### 5.5 Delta Lake

**Delta Lake** is the foundational open-source storage layer of the Databricks Lakehouse. It adds a **transaction log** on top of Parquet files in object storage.

#### Key Features

**1. ACID Transactions**
```sql
-- Atomic multi-row update — either fully succeeds or fully fails
UPDATE customers
SET loyalty_tier = 'Gold'
WHERE total_spend > 10000;
```

**2. Time Travel**
```sql
-- Query data as of 7 days ago
SELECT * FROM sales TIMESTAMP AS OF date_sub(current_timestamp(), 7);

-- Query a specific version
SELECT * FROM sales VERSION AS OF 5;
```

**3. Schema Enforcement & Evolution**
```python
# Schema enforcement — rejects bad data automatically
df.write.format("delta").mode("append").save("/delta/sales")

# Schema evolution — allows new columns to be added
df.write.format("delta").mode("append") \
  .option("mergeSchema", "true").save("/delta/sales")
```

**4. MERGE (Upsert)**
```sql
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

**5. OPTIMIZE & Z-ORDER**
```sql
-- Compact small files and co-locate related data
OPTIMIZE sales ZORDER BY (customer_id, order_date);
```

**6. VACUUM**
```sql
-- Remove old files no longer needed (retain 7 days by default)
VACUUM sales RETAIN 168 HOURS;
```

#### Delta Lake Table Format

```
Delta Table
├── Parquet data files    ← Columnar data storage
└── _delta_log/          ← Ordered transaction log
    ├── 000...000.json   ← Commit 0: CREATE TABLE
    ├── 000...001.json   ← Commit 1: INSERT
    ├── 000...002.json   ← Commit 2: UPDATE
    └── 000...010.checkpoint.parquet  ← Compact snapshot every 10 commits
```

### 5.6 Delta Live Tables (DLT)

**Delta Live Tables** is a declarative ETL framework that manages pipeline orchestration, data quality enforcement, and dependency resolution automatically.

Instead of writing imperative code with explicit ordering, DLT uses **dataset declarations**:

```python
import dlt
from pyspark.sql.functions import *

# Bronze — Raw ingestion
@dlt.table(comment="Raw orders from S3")
def raw_orders():
    return (spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format", "json")
                 .load("/mnt/raw/orders"))

# Silver — Cleaned & validated
@dlt.table(comment="Validated orders")
@dlt.expect_or_drop("valid_amount", "order_amount > 0")
@dlt.expect("valid_customer", "customer_id IS NOT NULL")
def cleaned_orders():
    return (dlt.read_stream("raw_orders")
               .select("order_id", "customer_id",
                       col("amount").alias("order_amount"),
                       to_timestamp("order_ts").alias("order_time")))

# Gold — Aggregated business metric
@dlt.table(comment="Daily revenue by customer")
def daily_revenue():
    return (dlt.read("cleaned_orders")
               .groupBy("customer_id", date_trunc("day", "order_time"))
               .agg(sum("order_amount").alias("daily_revenue")))
```

**DLT Pipeline Layers (Medallion Architecture):**

```
Raw Source Data
      ↓
┌──────────────────────────────────────────────┐
│  BRONZE (Raw)                                │
│  • Exact copy of source                      │
│  • Schema-on-read                            │
│  • Append-only                               │
└──────────────────────┬───────────────────────┘
                       ↓
┌──────────────────────────────────────────────┐
│  SILVER (Cleansed)                           │
│  • Validated & filtered                      │
│  • Deduplicated                              │
│  • Conformed data types                      │
│  • Business rules applied                   │
└──────────────────────┬───────────────────────┘
                       ↓
┌──────────────────────────────────────────────┐
│  GOLD (Aggregated)                           │
│  • Business-level aggregates                 │
│  • Optimised for BI / ML                     │
│  • Dimension & fact tables                   │
└──────────────────────────────────────────────┘
```

**DLT Data Quality Expectations:**

| Expectation | Behaviour on Failure |
|-------------|----------------------|
| `@dlt.expect` | Record metric, keep row |
| `@dlt.expect_or_drop` | Drop the failing row |
| `@dlt.expect_or_fail` | Halt the pipeline |

### 5.7 Unity Catalog

**Unity Catalog** is Databricks' unified governance solution that provides fine-grained access control, data lineage, and discovery across all workspaces and data assets.

#### Three-Level Namespace

```
Catalog
  └── Schema (Database)
        └── Table / View / Volume / Function / Model
```

```sql
-- Fully qualified reference
SELECT * FROM my_catalog.sales_schema.orders;

-- Create a table in Unity Catalog
CREATE TABLE analytics.finance.monthly_revenue
AS SELECT ...;
```

#### Key Capabilities

| Capability | Description |
|------------|-------------|
| **Centralised ACLs** | Manage permissions for all workspaces in one place |
| **Column-level security** | Mask or restrict access to sensitive columns |
| **Row-level security** | Filter rows based on user identity |
| **Data lineage** | Track where data came from, how it was transformed |
| **Audit logs** | Full audit trail of all data access |
| **Data discovery** | Search and browse all data assets |
| **Delta Sharing** | Securely share data with external parties |
| **Volumes** | Govern unstructured file storage in Unity Catalog |

#### Unity Catalog Architecture

```
Account Level
├── Metastore (one per region)
│   ├── Catalog A (Production)
│   │   ├── Schema: sales
│   │   ├── Schema: finance
│   │   └── Schema: marketing
│   ├── Catalog B (Development)
│   └── Catalog C (External — e.g., Hive Metastore Federation)
│
├── Workspace 1 (attached to Metastore)
└── Workspace 2 (attached to Metastore)
```

### 5.8 MLflow

**MLflow** is an open-source platform (created by Databricks) for the **full ML lifecycle**. It is natively integrated into Databricks.

#### MLflow Components

**1. Tracking**
Logs parameters, metrics, and artifacts for every ML experiment run.

```python
import mlflow
import mlflow.sklearn

with mlflow.start_run():
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 5)
    
    model = RandomForestClassifier(n_estimators=100, max_depth=5)
    model.fit(X_train, y_train)
    
    accuracy = model.score(X_test, y_test)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.sklearn.log_model(model, "model")
```

**2. Model Registry**
A centralised store for sharing and managing model versions with lifecycle stages.

```
Model Versions
  ↓
None → Staging → Production → Archived
```

**3. Projects**
Package ML code in a reproducible, reusable format with a `MLproject` file.

**4. Model Serving**
Deploy registered models as REST API endpoints (Databricks Model Serving).

#### MLflow + Feature Store

Databricks **Feature Store** allows teams to define, store, and reuse ML features:
- Centralised feature definitions
- Point-in-time correct feature lookups (avoiding training/serving skew)
- Automatic lineage tracking

### 5.9 Databricks SQL

**Databricks SQL** (DBSQL) is a serverless SQL layer optimised for BI and analytics workloads, built on top of the Lakehouse.

#### Key Components

| Component | Description |
|-----------|-------------|
| **SQL Warehouses** | Compute clusters optimised for SQL (uses Photon engine) |
| **SQL Editor** | Browser-based SQL IDE with autocomplete and query history |
| **Dashboards** | Visualise query results with charts and dashboards |
| **Alerts** | Notify when query results meet conditions |
| **Query History** | Full history with performance insights |

#### SQL Warehouse Types

| Type | Description |
|------|-------------|
| **Classic** | Long-running warehouse, pre-warmed |
| **Pro** | Adds query federation, materialised views |
| **Serverless** | Fully managed by Databricks — instant startup, per-second billing |

Databricks SQL is compatible with most BI tools via **JDBC/ODBC** connectors:
- Tableau
- Power BI
- Looker
- Qlik
- dbt (data build tool)

### 5.10 Databricks Runtime

The **Databricks Runtime (DBR)** is a pre-configured, optimised distribution of Apache Spark that runs on every cluster. It includes:

- **Apache Spark** (optimised fork)
- **Delta Lake** libraries
- **Python** (Conda environment with popular ML libraries)
- **Java / Scala** runtime
- **R** runtime (optional)
- Pre-installed libraries (Pandas, NumPy, Scikit-Learn, TensorFlow, PyTorch, etc.)

#### Runtime Variants

| Runtime | Optimised For |
|---------|---------------|
| **Databricks Runtime (Standard)** | General use — Spark + Python + Scala + R |
| **Databricks Runtime ML** | Machine learning — adds GPU support, MLflow, popular ML frameworks |
| **Photon Runtime** | SQL/BI workloads — C++ native vectorised engine for up to 4× speedup |
| **Databricks Runtime for Genomics** | Bioinformatics workloads |

#### Photon Engine

**Photon** is Databricks' native vectorised query engine written in C++. It:
- Replaces Spark's JVM-based execution for SQL operations
- Uses **SIMD** (Single Instruction, Multiple Data) CPU instructions
- Delivers significant speedups for SQL scans, joins, aggregations, and sorts
- Is transparent — no code changes required, same APIs

---

## 6. Databricks on AWS

When deploying Databricks on **Amazon Web Services**, the architecture maps as follows:

### 6.1 AWS Resource Mapping

| Databricks Concept | AWS Resource |
|--------------------|--------------|
| Workspace | VPC + S3 bucket + IAM roles |
| Cluster nodes | EC2 instances (driver + workers) |
| Storage | Amazon S3 (Delta tables, notebooks, logs) |
| Security | IAM roles, Security Groups, VPC, KMS |
| Networking | VPC, Subnets, NAT Gateway, VPC Peering |
| Secrets backend (optional) | AWS Secrets Manager / Hashicorp Vault |
| Logging | AWS CloudWatch / S3 |

### 6.2 Deployment Modes

| Mode | Description |
|------|-------------|
| **E2 Architecture (Classic)** | Customer VPC; EC2 + S3 in customer account |
| **Serverless** | Compute managed by Databricks in Databricks' account |

### 6.3 Instance Pools

**Instance Pools** maintain a pre-warmed pool of idle EC2 instances to reduce cluster startup time from minutes to seconds.

```
Instance Pool (keeps N instances warm)
    ↓
Cluster start (allocates from pool)
    ↓ (seconds, not minutes)
RUNNING state
```

### 6.4 Security Considerations on AWS

- **VPC Peering / PrivateLink** — keep all traffic within AWS backbone (no public internet).
- **Customer-Managed Keys (CMK)** — encrypt data in S3 and EBS volumes with your own KMS keys.
- **IAM Instance Profiles** — grant EC2 nodes S3 access without storing credentials.
- **Secrets Management** — use `dbutils.secrets` backed by Databricks Secret Scopes (backed by AWS Secrets Manager).
- **IP Access Lists** — restrict workspace access to corporate IP ranges.

---

## 7. Key Differentiators

### Why Databricks over Alternatives?

| Area | Databricks | Traditional DW (Redshift/Snowflake) | Open-Source Hadoop |
|------|------------|--------------------------------------|--------------------|
| Data types | Structured + unstructured + semi-structured | Primarily structured | All types (complex setup) |
| ML/AI support | Native (MLflow, Feature Store, Model Serving) | Limited | Manual integration |
| Streaming | Native (Structured Streaming, DLT) | Limited / add-ons | Requires Flink/Kafka Streams |
| Open formats | Yes (Delta, Parquet, Iceberg, Hudi) | Proprietary | Yes |
| Governance | Unity Catalog (unified) | Separate per tool | Manual |
| Cost model | Compute + DBUs (pay per use) | Storage + compute bundles | Infrastructure overhead |
| Collaboration | Real-time notebook co-authoring | Limited | None built-in |

### Databricks Unit (DBU)

A **Databricks Unit (DBU)** is the unit of processing capability per hour, used for billing. Different features and runtime types consume DBUs at different rates:

- Standard interactive clusters: ~0.07–1.0 DBU/hr per node
- Photon-enabled clusters: higher DBU rate, but faster execution
- Serverless SQL: per-second DBU billing

DBU costs vary by cloud provider, region, and instance type.

---

## 8. Summary

Databricks began as a commercial wrapper around Apache Spark and has evolved into a comprehensive **Data Intelligence Platform** that unifies the entire data and AI lifecycle. Its key innovations—Delta Lake, the Lakehouse architecture, Unity Catalog, and MLflow—address long-standing industry challenges around data reliability, governance, and ML operationalisation.

### Platform at a Glance

```
┌─────────────────────────────────────────────────────────────────────┐
│                  DATABRICKS DATA INTELLIGENCE PLATFORM              │
├───────────────┬──────────────────┬──────────────┬───────────────────┤
│ DATA          │ DATA SCIENCE &   │ BI &         │ GOVERNANCE        │
│ ENGINEERING   │ MACHINE LEARNING │ ANALYTICS    │                   │
│               │                  │              │                   │
│ • DLT         │ • MLflow         │ • DBSQL      │ • Unity Catalog   │
│ • Workflows   │ • Feature Store  │ • Dashboards │ • Lineage         │
│ • Auto Loader │ • Model Serving  │ • Photon     │ • Audit Logs      │
│ • Spark ETL   │ • Notebooks      │ • BI Connectors│ • Delta Sharing │
├───────────────┴──────────────────┴──────────────┴───────────────────┤
│                        DELTA LAKE (Storage)                         │
│          ACID Transactions • Time Travel • Schema Evolution         │
├─────────────────────────────────────────────────────────────────────┤
│              Cloud Object Storage (S3 / ADLS / GCS)                 │
└─────────────────────────────────────────────────────────────────────┘
```

### Core Takeaways

1. **Lakehouse = Data Lake + Data Warehouse** — one open platform for all workloads.
2. **Delta Lake** is the foundation — reliable, versioned, ACID-compliant storage.
3. **Apache Spark** provides the distributed compute engine.
4. **Unity Catalog** centralises governance across all workspaces.
5. **MLflow** standardises the end-to-end machine learning lifecycle.
6. **Databricks SQL + Photon** deliver data warehouse-class SQL performance on the Lakehouse.
7. The **two-plane model** ensures customer data stays in the customer's cloud account.

---

*Last updated: March 2026 | Databricks Platform Version: DBR 14.x / Unity Catalog GA*
