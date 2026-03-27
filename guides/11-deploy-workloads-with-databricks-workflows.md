# Deploy Workloads with Databricks Workflows

---

## Table of Contents

1. [Introduction to Databricks Workflows](#1-introduction-to-databricks-workflows)
   - 1.1 [What Are Databricks Workflows?](#11-what-are-databricks-workflows)
   - 1.2 [Workflows vs Classic Jobs](#12-workflows-vs-classic-jobs)
   - 1.3 [Workflows Architecture](#13-workflows-architecture)
   - 1.4 [Task Types Supported](#14-task-types-supported)
2. [Building and Managing Workflows in Databricks](#2-building-and-managing-workflows-in-databricks)
   - 2.1 [Creating a Workflow via the UI](#21-creating-a-workflow-via-the-ui)
   - 2.2 [Defining Tasks and Dependencies](#22-defining-tasks-and-dependencies)
   - 2.3 [Conditional Task Execution](#23-conditional-task-execution)
   - 2.4 [Using Task Values to Pass Data Between Tasks](#24-using-task-values-to-pass-data-between-tasks)
   - 2.5 [For-Each Task — Dynamic Parallel Execution](#25-for-each-task--dynamic-parallel-execution)
   - 2.6 [Managing Workflows with the Databricks SDK](#26-managing-workflows-with-the-databricks-sdk)
   - 2.7 [Managing Workflows with the CLI and REST API](#27-managing-workflows-with-the-cli-and-rest-api)
3. [Orchestrating Data Pipelines with Databricks Workflows](#3-orchestrating-data-pipelines-with-databricks-workflows)
   - 3.1 [Orchestrating Notebook Pipelines](#31-orchestrating-notebook-pipelines)
   - 3.2 [Orchestrating Python Script Pipelines](#32-orchestrating-python-script-pipelines)
   - 3.3 [Orchestrating Delta Live Tables Pipelines](#33-orchestrating-delta-live-tables-pipelines)
   - 3.4 [Orchestrating dbt Projects](#34-orchestrating-dbt-projects)
   - 3.5 [Cross-Workspace and External System Triggers](#35-cross-workspace-and-external-system-triggers)
   - 3.6 [Cluster Configuration Best Practices](#36-cluster-configuration-best-practices)
4. [Monitoring and Troubleshooting Workflows](#4-monitoring-and-troubleshooting-workflows)
   - 4.1 [Workflow Run Dashboard](#41-workflow-run-dashboard)
   - 4.2 [Task-Level Logs and Outputs](#42-task-level-logs-and-outputs)
   - 4.3 [Repair Runs — Restarting from Failed Task](#43-repair-runs--restarting-from-failed-task)
   - 4.4 [Notifications and Alerting](#44-notifications-and-alerting)
   - 4.5 [Job Run History and Audit Logs](#45-job-run-history-and-audit-logs)
   - 4.6 [Common Failure Patterns and Fixes](#46-common-failure-patterns-and-fixes)
5. [Case Study: End-to-End Workflow Implementation](#5-case-study-end-to-end-workflow-implementation)
   - 5.1 [Business Scenario](#51-business-scenario)
   - 5.2 [Workflow Design](#52-workflow-design)
   - 5.3 [Implementation: Ingest Tasks](#53-implementation-ingest-tasks)
   - 5.4 [Implementation: Transform Task with Task Values](#54-implementation-transform-task-with-task-values)
   - 5.5 [Implementation: Conditional Routing](#55-implementation-conditional-routing)
   - 5.6 [Implementation: Notification Task](#56-implementation-notification-task)
   - 5.7 [Full Workflow JSON Definition](#57-full-workflow-json-definition)
6. [Summary](#6-summary)

---

## 1. Introduction to Databricks Workflows

### 1.1 What Are Databricks Workflows?

**Databricks Workflows** is the fully managed, native orchestration service built into the Databricks platform. It enables you to define, schedule, execute, monitor, and debug multi-step data and AI workloads — without requiring any external orchestration tooling.

```
External Orchestrators (Airflow, Prefect, etc.)
  ├── Call Databricks APIs externally
  ├── No access to Spark UI / job internals
  └── Separate infrastructure to maintain

Databricks Workflows (built-in)
  ├── Native integration — runs inside the platform
  ├── Full Spark UI, logs, and metrics per task
  ├── Automatic cluster lifecycle management
  ├── Repair runs (retry from failure point)
  └── Task Values, For-Each, conditional routing
```

Workflows covers two primary use cases:

- **Scheduled batch jobs** — Daily ETL, weekly reporting, monthly reconciliation.
- **Triggered pipelines** — Event-driven execution via API, file arrival, or downstream dependency.

---

### 1.2 Workflows vs Classic Jobs

Databricks Workflows (introduced in 2022) supersedes the legacy Jobs UI with major additions:

| Feature                         | Classic Jobs | Databricks Workflows |
| ------------------------------- | :----------: | :------------------: |
| Multi-task DAG                  |      ✗       |          ✓           |
| Task type: Notebook             |      ✓       |          ✓           |
| Task type: Python script        |      ✓       |          ✓           |
| Task type: DLT pipeline         |      ✗       |          ✓           |
| Task type: dbt                  |      ✗       |          ✓           |
| Task type: SQL                  |      ✗       |          ✓           |
| Task Values (inter-task comms)  |      ✗       |          ✓           |
| For-Each Task                   |      ✗       |          ✓           |
| Conditional task branching      |      ✗       |          ✓           |
| Repair Run (retry from failure) |      ✗       |          ✓           |
| Job Cluster pool reuse          |      ✗       |          ✓           |
| Webhook triggers                |      ✗       |          ✓           |

---

### 1.3 Workflows Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Databricks Control Plane                   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                 Workflow Engine                      │   │
│  │                                                     │   │
│  │  Scheduler ──▶ DAG Executor ──▶ Cluster Manager    │   │
│  │                    │                    │            │   │
│  │                    ▼                    ▼            │   │
│  │            Task Queue           Job Cluster API     │   │
│  │            (priorities,         (spin up / reuse /  │   │
│  │             ordering)            terminate)         │   │
│  └───────────────────────────────────────────────────--┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Monitoring & History Store              │   │
│  │  Run logs │ Spark metrics │ Task outputs │ Alerts   │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            │ API calls
┌─────────────────────────────────────────────────────────────┐
│                  Customer Data Plane (AWS)                  │
│                                                             │
│  EC2 Job Cluster ─ runs Spark tasks ─ reads/writes S3      │
└─────────────────────────────────────────────────────────────┘
```

---

### 1.4 Task Types Supported

| Task Type                        | Use Case                                           |
| -------------------------------- | -------------------------------------------------- |
| **Notebook**                     | PySpark, SQL, or Scala notebooks in your Workspace |
| **Python Script**                | Standalone `.py` files in DBFS, Repos, or Volumes  |
| **Python Wheel**                 | Packaged Python library with a CLI entry point     |
| **JAR**                          | Compiled Scala/Java Spark applications             |
| **Spark Submit**                 | Raw `spark-submit` invocation                      |
| **SQL**                          | SQL statement or Databricks SQL file               |
| **Delta Live Tables (Pipeline)** | Trigger a DLT pipeline run                         |
| **dbt**                          | Run a dbt project against Databricks SQL           |
| **Run Job Task**                 | Trigger another Databricks Job as a sub-workflow   |
| **For-Each Task**                | Run a task repeatedly over a collection of inputs  |

---

## 2. Building and Managing Workflows in Databricks

### 2.1 Creating a Workflow via the UI

**Step-by-step workflow creation:**

1. In the left sidebar, click **Workflows** → **Jobs** → **Create Job**.
2. Enter a job name (e.g., `RetailCo Daily ETL`).
3. **Add Task:**
   - Choose task type (Notebook, Python Script, etc.).
   - Select the notebook/script path.
   - Choose cluster (New Job Cluster recommended for production).
4. **Add more tasks** and draw dependencies by selecting a task as "Depends on".
5. **Configure schedule** (under the Schedule tab):
   - Quartz cron expression (e.g., `0 0 3 * * ?` = daily 3AM UTC).
   - Timezone.
6. **Configure notifications** (email or webhook on start/success/failure).
7. **Save** and **Run Now** to validate.

---

### 2.2 Defining Tasks and Dependencies

A Workflow DAG is defined by a list of tasks where each task optionally lists the tasks it `depends_on`. Databricks executes tasks in topological order — tasks that can run in parallel do so automatically.

```python
# Example: 4-task DAG
#
#  ingest_bronze ──┬──▶ transform_silver ──▶ build_gold ──▶ notify
#                  │
#  load_reference ─┘

tasks = [
    {
        "task_key":       "ingest_bronze",
        "notebook_task":  {"notebook_path": "/pipelines/01_bronze"}
        # no depends_on → runs first
    },
    {
        "task_key":       "load_reference",
        "notebook_task":  {"notebook_path": "/pipelines/02_reference"}
        # also runs first (parallel with ingest_bronze)
    },
    {
        "task_key":       "transform_silver",
        "depends_on":     [
            {"task_key": "ingest_bronze"},
            {"task_key": "load_reference"}
        ],
        "notebook_task":  {"notebook_path": "/pipelines/03_silver"}
        # runs ONLY after both ingest_bronze AND load_reference succeed
    },
    {
        "task_key":       "build_gold",
        "depends_on":     [{"task_key": "transform_silver"}],
        "notebook_task":  {"notebook_path": "/pipelines/04_gold"}
    },
    {
        "task_key":       "notify",
        "depends_on":     [{"task_key": "build_gold"}],
        "notebook_task":  {"notebook_path": "/pipelines/05_notify"}
    }
]
```

**Dependency outcomes:**

| Depends On Outcome | Default Behavior    | Can Override?      |
| ------------------ | ------------------- | ------------------ |
| Upstream succeeded | Task runs           | N/A                |
| Upstream failed    | Task is **skipped** | Yes — use `run_if` |
| Upstream skipped   | Task is **skipped** | Yes — use `run_if` |

---

### 2.3 Conditional Task Execution

The `run_if` parameter controls whether a task executes based on the outcomes of its dependencies.

```json
// Run task only if ALL dependencies succeeded (default)
{
  "task_key": "build_gold",
  "depends_on": [{"task_key": "transform_silver"}],
  "run_if": "ALL_SUCCESS"
}

// Run task even if some dependencies failed (e.g., cleanup / alerting)
{
  "task_key": "notify_on_failure",
  "depends_on": [{"task_key": "build_gold"}],
  "run_if": "AT_LEAST_ONE_FAILED"
}

// Run task regardless of outcome (always-run cleanup)
{
  "task_key": "cleanup",
  "depends_on": [{"task_key": "build_gold"}],
  "run_if": "ALL_DONE"
}
```

**`run_if` options:**

| Option                 | Meaning                                                |
| ---------------------- | ------------------------------------------------------ |
| `ALL_SUCCESS`          | All upstream tasks succeeded (default)                 |
| `AT_LEAST_ONE_SUCCESS` | At least one upstream succeeded                        |
| `NONE_FAILED`          | No upstream tasks failed (succeeded or skipped are OK) |
| `ALL_DONE`             | All upstream finished (any outcome)                    |
| `AT_LEAST_ONE_FAILED`  | At least one upstream failed                           |
| `ALL_FAILED`           | All upstream tasks failed                              |

**Example — Branching on success/failure:**

```
                    ┌──────────────────┐
                    │  transform_silver │
                    └──────┬───────────┘
                    ↙ SUCCESS      ↘ FAILURE
     ┌────────────────┐    ┌──────────────────┐
     │   build_gold   │    │  alert_on_failure │
     │  (ALL_SUCCESS) │    │  (AT_LEAST_ONE_  │
     └────────────────┘    │   FAILED)         │
                           └──────────────────┘
```

---

### 2.4 Using Task Values to Pass Data Between Tasks

**Task Values** allow a task to publish a key-value pair that downstream tasks can read. This enables dynamic, data-driven workflows.

```python
# ──── Task A: bronze_ingest ────────────────────────────────────────────────
# After ingestion, publish the row count for downstream tasks to use

row_count = bronze_df.count()
source_version = spark.sql("SELECT MAX(version) FROM (DESCRIBE HISTORY bronze.orders)") \
                      .collect()[0][0]

# Publish task values
dbutils.jobs.taskValues.set(key="row_count",       value=int(row_count))
dbutils.jobs.taskValues.set(key="source_version",  value=int(source_version))
dbutils.jobs.taskValues.set(key="run_status",      value="success")

print(f"[Bronze] Published: row_count={row_count}, source_version={source_version}")
```

```python
# ──── Task B: silver_transform ─────────────────────────────────────────────
# Read values published by bronze_ingest

upstream_rows    = dbutils.jobs.taskValues.get(
    taskKey  = "bronze_ingest",
    key      = "row_count",
    default  = 0,
    debugValue = 100   # value used during notebook dev (not in a job run)
)
source_version = dbutils.jobs.taskValues.get(
    taskKey    = "bronze_ingest",
    key        = "source_version",
    default    = 0
)

print(f"[Silver] Processing {upstream_rows:,} rows from bronze version {source_version}")

# Use the task value in your transformation logic
if upstream_rows == 0:
    dbutils.notebook.exit("SKIPPED — no upstream data")

# run transformation...
silver_df.write.format("delta").mode("append").saveAsTable("silver.orders")

# Publish downstream value
dbutils.jobs.taskValues.set(key="silver_rows", value=silver_df.count())
```

---

### 2.5 For-Each Task — Dynamic Parallel Execution

The **For-Each Task** runs a child task repeatedly in parallel across a dynamic list of inputs. This replaces ad-hoc fan-out patterns.

```python
# ──── Task: generate_date_list ─────────────────────────────────────────────
# Publish a list of dates to process in parallel

import json
from datetime import date, timedelta

# Generate list of the last 7 days needing backfill
dates = [(date.today() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
dbutils.jobs.taskValues.set(key="backfill_dates", value=json.dumps(dates))
# Published: ["2024-03-26", "2024-03-25", "2024-03-24", ...]
```

**In the Workflow JSON:**

```json
{
  "task_key": "parallel_backfill",
  "for_each_task": {
    "inputs": "{{tasks.generate_date_list.values.backfill_dates}}",
    "concurrency": 4,
    "task": {
      "task_key": "backfill_single_date",
      "notebook_task": {
        "notebook_path": "/pipelines/01_bronze_ingest",
        "base_parameters": {
          "run_date": "{{input}}"
        }
      },
      "job_cluster_key": "etl_cluster"
    }
  },
  "depends_on": [{ "task_key": "generate_date_list" }]
}
```

The `{{input}}` placeholder receives each item from the `inputs` array. With `concurrency: 4`, up to 4 notebook runs execute simultaneously.

---

### 2.6 Managing Workflows with the Databricks SDK

The **Databricks SDK for Python** (`databricks-sdk`) is the recommended way to programmatically create and manage Workflows.

```python
# Install: pip install databricks-sdk
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, JobCluster, ClusterSpec, CronSchedule,
    TaskDependency, JobEmailNotifications, AutoScale
)

w = WorkspaceClient()  # auto-configures from env vars DATABRICKS_HOST + DATABRICKS_TOKEN

# ── Create a Workflow ──
job = w.jobs.create(
    name = "RetailCo Production ETL",
    schedule = CronSchedule(
        quartz_cron_expression = "0 0 3 * * ?",
        timezone_id            = "UTC",
        pause_status           = "UNPAUSED"
    ),
    email_notifications = JobEmailNotifications(
        on_failure = ["data-alerts@retailco.com"],
        on_success = ["data-team@retailco.com"]
    ),
    max_concurrent_runs = 1,
    job_clusters = [
        JobCluster(
            job_cluster_key = "etl_cluster",
            new_cluster     = ClusterSpec(
                spark_version  = "14.3.x-scala2.12",
                node_type_id   = "i3.2xlarge",
                autoscale      = AutoScale(min_workers=4, max_workers=12)
            )
        )
    ],
    tasks = [
        Task(
            task_key      = "bronze_ingest",
            job_cluster_key = "etl_cluster",
            notebook_task = NotebookTask(
                notebook_path    = "/pipelines/01_bronze_ingest",
                base_parameters  = {"catalog": "prod"}
            )
        ),
        Task(
            task_key        = "silver_transform",
            depends_on      = [TaskDependency(task_key="bronze_ingest")],
            job_cluster_key = "etl_cluster",
            notebook_task   = NotebookTask(
                notebook_path    = "/pipelines/03_silver_transform",
                base_parameters  = {"catalog": "prod"}
            ),
            max_retries         = 2,
            min_retry_interval_millis = 300_000
        )
    ]
)
print(f"Created job: {job.job_id}")

# ── Trigger a run now ──
run = w.jobs.run_now(job_id=job.job_id, notebook_params={"catalog": "prod"})
print(f"Run ID: {run.run_id}")

# ── List recent runs ──
for run in w.jobs.list_runs(job_id=job.job_id, limit=5):
    print(f"Run {run.run_id}: {run.state.life_cycle_state} | {run.state.result_state}")

# ── Update an existing job ──
w.jobs.update(
    job_id = job.job_id,
    new_settings = {"max_concurrent_runs": 2}
)

# ── Delete a job ──
# w.jobs.delete(job_id=job.job_id)  # irreversible — confirm before running
```

---

### 2.7 Managing Workflows with the CLI and REST API

**Databricks CLI (v0.200+):**

```bash
# Install
pip install databricks-cli

# Authenticate (store profile)
databricks configure --profile prod

# List all jobs
databricks jobs list --profile prod

# Export a job definition
databricks jobs get --job-id 12345 --profile prod > job_definition.json

# Import / create from JSON
databricks jobs create --json @job_definition.json --profile prod

# Trigger a run
databricks jobs run-now --job-id 12345 --profile prod

# Get run status
databricks runs get --run-id 67890 --profile prod

# Cancel a run
databricks runs cancel --run-id 67890 --profile prod
```

**REST API (curl examples):**

```bash
HOST="https://adb-xxxx.azuredatabricks.net"
TOKEN="<your-pat-token>"

# List jobs
curl -s -X GET "$HOST/api/2.1/jobs/list" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# Trigger run
curl -s -X POST "$HOST/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": 12345,
    "notebook_params": {"catalog": "prod", "run_date": "2024-03-26"}
  }'

# Check run status
curl -s -X GET "$HOST/api/2.1/runs/get?run_id=67890" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

---

## 3. Orchestrating Data Pipelines with Databricks Workflows

### 3.1 Orchestrating Notebook Pipelines

Notebooks are the most common task type. The key integration points are:

```python
# ── Calling another notebook from within a notebook (DBUtils Notebook API) ──
# Useful for breaking a large notebook into smaller reusable units

# Run a sub-notebook and capture its output
result = dbutils.notebook.run(
    path    = "/pipelines/shared/data_quality_check",
    timeout = 600,   # seconds
    arguments = {
        "table_name": "silver.orders",
        "min_rows":   "1000"
    }
)
print(f"Sub-notebook result: {result}")

# If sub-notebook called dbutils.notebook.exit("ok"), result == "ok"
if result != "ok":
    raise Exception(f"Data quality check failed: {result}")
```

**Notebooks in Workflows vs `dbutils.notebook.run`:**

| Aspect             |     Workflow Task     |   `dbutils.notebook.run`    |
| ------------------ | :-------------------: | :-------------------------: |
| Parallel execution |     ✓ (automatic)     |         ✓ (manual)          |
| Retries            | ✓ (configured in job) |           Manual            |
| Monitoring         |   Full Workflow UI    |           Limited           |
| Cluster            |  Shared or separate   |     Always same cluster     |
| Recommended for    |      Production       | Development / utility calls |

---

### 3.2 Orchestrating Python Script Pipelines

For production-grade pipelines, **Python script tasks** (`.py` files) are cleaner than notebooks because they:

- Support version control naturally.
- Allow unit testing.
- Enable modular package structure.

```python
# pipeline/main.py — entry point run as a Python Script task
import sys
import logging
from pyspark.sql import SparkSession
from pipeline.bronze import run_bronze
from pipeline.silver import run_silver

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("ETL")

def main():
    spark = SparkSession.builder.appName("RetailCo ETL").getOrCreate()

    run_date = sys.argv[1] if len(sys.argv) > 1 else "2024-03-26"
    log.info(f"Starting ETL for {run_date}")

    bronze_count = run_bronze(spark, run_date)
    log.info(f"Bronze complete: {bronze_count:,} rows")

    silver_count = run_silver(spark, run_date)
    log.info(f"Silver complete: {silver_count:,} rows")

    log.info("ETL complete.")

if __name__ == "__main__":
    main()
```

```json
// Workflow task definition for Python script
{
  "task_key": "daily_etl",
  "python_wheel_task": {
    "package_name": "retailco_pipeline",
    "entry_point": "main",
    "parameters": ["2024-03-26"]
  },
  "libraries": [
    { "whl": "dbfs:/Shared/wheels/retailco_pipeline-1.0.0-py3-none-any.whl" }
  ],
  "job_cluster_key": "etl_cluster"
}
```

---

### 3.3 Orchestrating Delta Live Tables Pipelines

A DLT pipeline can be orchestrated as a task within a Workflow. This is the recommended pattern for production ingest-and-transform workloads.

```json
// DLT Pipeline task in a Workflow
{
  "task_key": "run_dlt_pipeline",
  "pipeline_task": {
    "pipeline_id": "abc123-def456-ghi789", // ID of your DLT pipeline
    "full_refresh": false // true = reprocess from scratch
  }
}
```

**Typical pattern — DLT + Reporting Job:**

```
DLT Task (Bronze → Silver via declarative tables)
      │
      ▼
Notebook Task (Gold aggregations on top of Silver)
      │
      ▼
SQL Task (Refresh Databricks SQL dashboard)
      │
      ▼
Notebook Task (Send email report)
```

---

### 3.4 Orchestrating dbt Projects

Databricks Workflows has native dbt support — it runs dbt models directly against Databricks SQL without any external runner.

```json
// dbt task in a Workflow
{
  "task_key": "run_dbt_models",
  "dbt_task": {
    "project_directory": "/Repos/data-team/dbt-project",
    "commands": [
      "dbt deps",
      "dbt run --select tag:daily",
      "dbt test --select tag:daily"
    ],
    "schema": "dbt_prod",
    "warehouse_id": "abc123def456"
  },
  "libraries": [{ "pypi": { "package": "dbt-databricks>=1.6.0" } }]
}
```

---

### 3.5 Cross-Workspace and External System Triggers

**Triggering a workflow from an external system (CI/CD, Airflow, etc.):**

```python
# Python — trigger a Databricks job from an external system
import requests

def trigger_databricks_job(host: str, token: str, job_id: int, params: dict) -> int:
    url = f"{host}/api/2.1/jobs/run-now"
    response = requests.post(
        url,
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json    = {"job_id": job_id, "notebook_params": params},
        timeout = 30
    )
    response.raise_for_status()
    return response.json()["run_id"]

def wait_for_run(host: str, token: str, run_id: int, poll_interval: int = 30) -> str:
    import time
    url = f"{host}/api/2.1/runs/get"
    while True:
        resp = requests.get(url,
            headers = {"Authorization": f"Bearer {token}"},
            params  = {"run_id": run_id}
        )
        resp.raise_for_status()
        state = resp.json()["state"]
        life_cycle = state["life_cycle_state"]
        if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return state.get("result_state", "UNKNOWN")
        time.sleep(poll_interval)
```

**Airflow DatabricksRunNowOperator:**

```python
# In your Airflow DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

run_etl = DatabricksRunNowOperator(
    task_id         = "run_retailco_etl",
    job_id          = 12345,
    notebook_params = {"catalog": "prod", "run_date": "{{ ds }}"},
    databricks_conn_id = "databricks_prod",
    dag             = dag
)
```

---

### 3.6 Cluster Configuration Best Practices

```python
# ── Recommended Job Cluster Configuration ──

# 1. Always use versioned Databricks Runtime
"spark_version": "14.3.x-scala2.12"    # LTS version preferred

# 2. Use SPOT instances with on-demand fallback (cost saving ~60-70%)
"aws_attributes": {
    "availability":                 "SPOT_WITH_FALLBACK",
    "spot_bid_price_percent":       100,
    "first_on_demand":              1    # first node always on-demand (driver)
}

# 3. Autoscaling for variable workloads
"autoscale": {
    "min_workers": 2,
    "max_workers": 20
}

# 4. Auto-termination (catches job cluster leaks)
"autotermination_minutes": 30

# 5. Cluster-level Spark config
"spark_conf": {
    "spark.sql.adaptive.enabled":            "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled":  "true"
}

# 6. Init scripts for custom dependencies
"init_scripts": [
    {"dbfs": {"destination": "dbfs:/init-scripts/install-deps.sh"}}
]
```

---

## 4. Monitoring and Troubleshooting Workflows

### 4.1 Workflow Run Dashboard

The **Workflow Runs** dashboard provides a real-time and historical view of all job executions.

```
Workflows → Jobs → [Job Name] → View Runs

Run View shows:
  ┌──────────────────────────────────────────────────────────────┐
  │  Run #45  │  2024-03-26 03:01:12  │  Duration: 18m 44s     │
  │  Status:  SUCCEEDED                                          │
  │                                                              │
  │  Task Graph:                                                 │
  │                        ┌──────────────┐                     │
  │                        │ bronze_ingest│ ✓ 3m 12s            │
  │                        └──────┬───────┘                     │
  │                               │                             │
  │               ┌───────────────▼──────────────┐              │
  │               │     silver_transform          │ ✓ 9m 05s    │
  │               └───────────────┬──────────────┘              │
  │                               │                             │
  │                        ┌──────▼───────┐                     │
  │                        │  gold_kpis   │ ✓ 5m 22s            │
  │                        └──────┬───────┘                     │
  │                               │                             │
  │                        ┌──────▼───────┐                     │
  │                        │   notify     │ ✓ 1m 05s            │
  │                        └──────────────┘                     │
  └──────────────────────────────────────────────────────────────┘
```

Click any task node to view:

- Spark UI for that specific task run.
- stdout / stderr logs.
- Task output / exit value.
- Timing breakdown.

---

### 4.2 Task-Level Logs and Outputs

```python
# Best practices for log visibility in Workflows

import logging
import sys

# Use Python logging (shows in task stderr)
logging.basicConfig(
    stream = sys.stdout,
    level  = logging.INFO,
    format = "[%(asctime)s] %(levelname)-8s %(name)s – %(message)s"
)
log = logging.getLogger("silver_transform")

log.info("Task started")
log.info(f"Reading from catalog={catalog}")
log.warning(f"High NULL rate ({null_pct:.1f}%) in column order_date")
log.error(f"Failed to connect to JDBC source: {str(e)}")

# Use structured print for key metrics
# These appear in the Workflow task output panel
print(f"METRIC:rows_read={bronze_df.count()}")
print(f"METRIC:rows_written={silver_df.count()}")
print(f"METRIC:rejection_pct={rejection_pct:.2f}")
print(f"METRIC:duration_seconds={duration:.1f}")
```

**Viewing logs from the REST API:**

```python
# Retrieve run output programmatically
import requests

def get_run_output(host: str, token: str, run_id: int) -> dict:
    url = f"{host}/api/2.1/runs/get-output"
    resp = requests.get(
        url,
        headers = {"Authorization": f"Bearer {token}"},
        params  = {"run_id": run_id}
    )
    resp.raise_for_status()
    return resp.json()

output = get_run_output(HOST, TOKEN, run_id=67890)
print(output["notebook_output"]["result"])     # dbutils.notebook.exit() value
print(output["logs"])                           # stdout
print(output["error"])                          # error message if failed
```

---

### 4.3 Repair Runs — Restarting from Failed Task

**Repair Run** is one of the most powerful Workflows features. When a multi-task job fails midway, you can re-run only the failed tasks (and downstream tasks) without re-running the successful tasks.

```
Job fails at task "silver_transform" (tasks ran: bronze ✓, silver ✗):

Without Repair Run:   Re-run entire job
  → bronze_ingest runs again (wasted compute, possible duplicates)

With Repair Run:      Resume from "silver_transform"
  → bronze_ingest result is preserved and reused
  → Only silver_transform and gold_kpis re-run
  → Fewer resources, faster recovery
```

**Via UI:**

1. Go to the failed run.
2. Click **Repair Run** button.
3. Select which failed tasks to include in the repair.
4. Optionally update task parameters.
5. Click **Repair** — only the selected tasks and their downstream tasks run.

**Via API:**

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Repair a run
repair = w.jobs.repair_run(
    run_id          = 67890,
    rerun_tasks     = ["silver_transform", "gold_kpis"],  # tasks to re-run
    notebook_params = {"catalog": "prod"}
)
print(f"Repair run ID: {repair.repair_id}")
```

---

### 4.4 Notifications and Alerting

**Built-in email notifications:**

```json
{
  "email_notifications": {
    "on_start": [],
    "on_success": ["data-team@company.com"],
    "on_failure": ["data-alerts@company.com", "oncall@company.com"],
    "on_duration_warning_threshold_exceeded": ["perf-alerts@company.com"],
    "no_alert_for_skipped_runs": true
  },
  "notification_settings": {
    "no_alert_for_skipped_runs": true,
    "no_alert_for_canceled_runs": false
  },
  "run_as": { "user_name": "svc-databricks@company.com" },
  "timeout_seconds": 7200, // 2 hours — fail if exceeded
  "health": {
    "rules": [
      {
        "metric": "RUN_DURATION_SECONDS",
        "op": "GREATER_THAN",
        "value": 3600 // warn if run exceeds 60 min
      }
    ]
  }
}
```

**Webhook notifications (Slack, PagerDuty, etc.):**

```json
{
  "webhook_notifications": {
    "on_failure": [
      { "id": "webhook-abc123" } // Pre-configured webhook in Databricks System Settings
    ]
  }
}
```

---

### 4.5 Job Run History and Audit Logs

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# List all runs for a specific job (last 25)
for run in w.jobs.list_runs(job_id=12345, limit=25):
    duration_s = (run.end_time - run.start_time) / 1000 if run.end_time else None
    print(f"Run {run.run_id:8d} | "
          f"{run.state.result_state:12} | "
          f"{run.start_time} | "
          f"Duration: {duration_s:.0f}s" if duration_s else "")

# List ALL runs across all jobs (for a dashboard)
from datetime import datetime, timedelta

cutoff_ms = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)
all_runs = list(w.jobs.list_runs(
    completed_only     = True,
    start_time_from    = cutoff_ms
))

# Compute success rate
total    = len(all_runs)
success  = sum(1 for r in all_runs if r.state.result_state == "SUCCESS")
print(f"7-day success rate: {success}/{total} = {success/total*100:.1f}%")
```

**Audit logs (Databricks Audit Log via AWS CloudTrail):**
Databricks automatically emits audit events for:

- Job runs (create, trigger, cancel, repair).
- Cluster lifecycle (start, terminate, resize).
- Data access (table reads, writes).

These events appear in your AWS CloudTrail or Azure Diagnostic Logs within 15 minutes.

---

### 4.6 Common Failure Patterns and Fixes

| Failure Pattern                             | Root Cause                                       | Fix                                                                  |
| ------------------------------------------- | ------------------------------------------------ | -------------------------------------------------------------------- |
| `AnalysisException: Table not found`        | Dependency task didn't create the table          | Check upstream task succeeded; add `DeltaTable.isDeltaTable()` guard |
| `java.lang.OutOfMemoryError`                | Executor memory exhausted                        | Reduce partition size; increase `num_workers`; check for data skew   |
| `ConcurrentModificationException`           | Two jobs writing same Delta table simultaneously | Set `max_concurrent_runs: 1`; use MERGE instead of overwrite         |
| `ClusterSpotEviction: Spot node terminated` | AWS reclaimed spot instance                      | Use `SPOT_WITH_FALLBACK`; set `max_retries: 2` on affected tasks     |
| `Timeout: task exceeded X seconds`          | Slow query / data skew / missing Z-ORDER         | OPTIMIZE + ZORDER the source table; check Spark UI for skew          |
| `FileNotFoundException: s3://...`           | Source file not yet available                    | Add a wait/retry loop; use Autoloader instead of batch read          |
| `SchemaEvolutionException`                  | New column in source, schema mismatch            | Add `.option("mergeSchema", "true")` or enable autoMerge             |
| `NullPointerException` in transform         | Bad data passing NULL to non-nullable function   | Add `.filter(col("x").isNotNull())` before affected transform        |

**Debugging workflow:**

```python
# 1. Enable Spark event logging for deeper analysis
spark.conf.set("spark.eventLog.enabled", "true")
spark.conf.set("spark.eventLog.dir", "dbfs:/event-logs/")

# 2. Print execution plan before running expensive operations
silver_df.explain(mode="formatted")   # shows the physical plan

# 3. Identify data skew
from pyspark.sql.functions import spark_partition_id, count

silver_df.groupBy(spark_partition_id()).agg(count("*").alias("rows")).orderBy("rows").show()
# If one partition has >> rows than others → skew detected

# 4. Check for shuffle spill (a sign of OOM risk)
# Look for "spill" in the Stages tab of Spark UI
```

---

## 5. Case Study: End-to-End Workflow Implementation

### 5.1 Business Scenario

**Company:** FinanceStream — a financial data provider  
**Requirement:**

- Ingest three data sources in parallel: transactions, account data, forex rates.
- Join and transform data if all three ingestions succeed.
- If any ingestion fails, send an alert but still process what succeeded.
- Run daily aggregations and publish to a reporting database.
- Repair failed stages daily without re-ingesting clean data.

---

### 5.2 Workflow Design

```
                ┌───────────────────┐
                │  ingest_transactions  │  (Task A)
                │  ingest_accounts      │  (Task B)  ─── parallel
                │  ingest_forex_rates   │  (Task C)
                └──────────┬────────────┘
                           │ ALL_SUCCESS
                           ▼
                ┌──────────────────────┐
                │  transform_silver    │  (Task D)
                └──────────┬───────────┘
                           │
              ┌────────────┴──────────────┐
    ALL_SUCCESS │                AT_LEAST_ONE_FAILED │
              ▼                           ▼
   ┌──────────────────┐       ┌──────────────────────────┐
   │  build_gold_kpis │       │  notify_partial_failure  │
   │  (Task E)        │       │  (Task F)                │
   └──────────┬───────┘       └──────────────────────────┘
              │ ALL_DONE (runs regardless of E result)
              ▼
    ┌─────────────────────┐
    │  send_daily_report  │
    │  (Task G)           │
    └─────────────────────┘
```

---

### 5.3 Implementation: Ingest Tasks

```python
# ── Task A: ingest_transactions ─────────────────────────────────────────────
# Notebook: /financestream/ingest/01_transactions

from pyspark.sql.functions import current_timestamp, input_file_name, lit

run_date = dbutils.widgets.get("run_date")
source   = f"s3://fs-raw/transactions/{run_date}/"

df = (
    spark.read.format("json")
        .option("multiLine", "true")
        .load(source)
        .withColumn("_ingested_at",   current_timestamp())
        .withColumn("_source",        lit("transactions"))
        .withColumn("_run_date",      lit(run_date))
)

row_count = df.count()
df.write.format("delta").mode("append").partitionBy("_run_date") \
    .saveAsTable("financestream.bronze.transactions")

dbutils.jobs.taskValues.set("row_count",  row_count)
dbutils.jobs.taskValues.set("run_status", "success")
print(f"[A] Ingested {row_count:,} transactions")
```

```python
# ── Task C: ingest_forex_rates ───────────────────────────────────────────────
# Forex rates are from a REST API endpoint

import requests, json
from pyspark.sql import Row

run_date = dbutils.widgets.get("run_date")
api_url  = f"https://api.forexdata.io/rates?date={run_date}"
token    = dbutils.secrets.get("forex-scope", "api-key")

response = requests.get(api_url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
response.raise_for_status()

rates = response.json()["rates"]   # {"USD": 1.0, "EUR": 0.92, "GBP": 0.79, ...}

rows = [Row(base="USD", target_ccy=ccy, rate=float(rate), rate_date=run_date)
        for ccy, rate in rates.items()]

df = spark.createDataFrame(rows) \
          .withColumn("_ingested_at", current_timestamp())

df.write.format("delta").mode("overwrite").saveAsTable("financestream.bronze.forex_rates")

dbutils.jobs.taskValues.set("currencies_loaded", len(rows))
print(f"[C] Loaded {len(rows)} forex rates")
```

---

### 5.4 Implementation: Transform Task with Task Values

```python
# ── Task D: transform_silver ─────────────────────────────────────────────────
# Uses task values from upstream ingest tasks

run_date = dbutils.widgets.get("run_date")

txn_rows   = dbutils.jobs.taskValues.get("ingest_transactions", "row_count", default=0)
forex_ccys = dbutils.jobs.taskValues.get("ingest_forex_rates",  "currencies_loaded", default=0)

print(f"[D] Upstream stats: txn_rows={txn_rows:,}, forex_currencies={forex_ccys}")

# Read silver inputs
bronze_txns  = spark.read.table("financestream.bronze.transactions") \
                         .filter(f"_run_date = '{run_date}'")
forex_rates  = spark.read.table("financestream.bronze.forex_rates") \
                         .filter(f"rate_date = '{run_date}'")
accounts     = spark.read.table("financestream.bronze.accounts")

# Transform: join, convert currencies, validate
from pyspark.sql.functions import col, to_date, when, coalesce, lit, round

silver = (
    bronze_txns
    .join(forex_rates.select("target_ccy", "rate"),
          bronze_txns.currency == forex_rates.target_ccy, "left")
    .join(accounts.select("account_id", "account_type", "region"),
          on="account_id", how="left")
    .withColumn("amount_usd",
        when(col("currency") == "USD", col("amount"))
        .otherwise(round(col("amount") / col("rate"), 4)))
    .withColumn("txn_date",     to_date(col("txn_timestamp")))
    .filter("txn_id IS NOT NULL AND amount > 0")
    .dropDuplicates(["txn_id"])
    .drop("currency", "rate", "target_ccy", "_run_date", "_ingested_at", "_source")
    .withColumn("_silver_at", current_timestamp())
)

silver_count = silver.count()
DeltaTable.forName(spark, "financestream.silver.transactions") \
    .alias("t").merge(silver.alias("s"), "t.txn_id = s.txn_id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

dbutils.jobs.taskValues.set("silver_rows", silver_count)
print(f"[D] Silver transform complete: {silver_count:,} rows")
```

---

### 5.5 Implementation: Conditional Routing

```python
# ── Task F: notify_partial_failure ──────────────────────────────────────────
# run_if: AT_LEAST_ONE_FAILED (on Tasks A, B, C)
# This notebook runs when at least one ingest task failed

import requests, json

failed_tasks = []
for task_key in ["ingest_transactions", "ingest_accounts", "ingest_forex_rates"]:
    try:
        status = dbutils.jobs.taskValues.get(task_key, "run_status", default="failed")
        if status != "success":
            failed_tasks.append(task_key)
    except Exception:
        failed_tasks.append(task_key)

message = f":warning: FinanceStream partial failure on {run_date}\nFailed tasks: {', '.join(failed_tasks)}"

slack_webhook = dbutils.secrets.get("alerting-scope", "slack-webhook")
requests.post(slack_webhook, json={"text": message})
print(f"[F] Alert sent for: {failed_tasks}")
```

---

### 5.6 Implementation: Notification Task

```python
# ── Task G: send_daily_report ───────────────────────────────────────────────
# run_if: ALL_DONE (runs regardless of E result)

from pyspark.sql.functions import sum, count

run_date = dbutils.widgets.get("run_date")

# Collect summary stats
gold_summary = spark.sql(f"""
    SELECT
        region,
        COUNT(*)         AS transaction_count,
        SUM(amount_usd)  AS total_volume_usd
    FROM financestream.silver.transactions
    WHERE txn_date = '{run_date}'
    GROUP BY region
    ORDER BY total_volume_usd DESC
""")

# Format as a simple HTML table for email
rows_html = "".join([
    f"<tr><td>{r.region}</td><td>{r.transaction_count:,}</td><td>${r.total_volume_usd:,.0f}</td></tr>"
    for r in gold_summary.collect()
])

html_body = f"""
<h2>FinanceStream Daily Report — {run_date}</h2>
<table border='1'>
  <tr><th>Region</th><th>Transactions</th><th>Volume (USD)</th></tr>
  {rows_html}
</table>
"""

# Send via SMTP or Databricks email API
# ... (omitted for brevity)
print(f"[G] Daily report sent for {run_date}")
dbutils.notebook.exit("REPORT_SENT")
```

---

### 5.7 Full Workflow JSON Definition

```json
{
  "name": "FinanceStream Daily Pipeline",
  "schedule": {
    "quartz_cron_expression": "0 0 4 * * ?",
    "timezone_id": "UTC"
  },
  "max_concurrent_runs": 1,
  "email_notifications": {
    "on_failure": ["engineering@financestream.com"]
  },
  "job_clusters": [
    {
      "job_cluster_key": "etl_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 6,
        "aws_attributes": { "availability": "SPOT_WITH_FALLBACK" }
      }
    }
  ],
  "tasks": [
    {
      "task_key": "ingest_transactions",
      "notebook_task": {
        "notebook_path": "/financestream/ingest/01_transactions",
        "base_parameters": { "run_date": "{{job.start_time.iso_date}}" }
      },
      "job_cluster_key": "etl_cluster",
      "max_retries": 2
    },
    {
      "task_key": "ingest_accounts",
      "notebook_task": {
        "notebook_path": "/financestream/ingest/02_accounts",
        "base_parameters": { "run_date": "{{job.start_time.iso_date}}" }
      },
      "job_cluster_key": "etl_cluster",
      "max_retries": 1
    },
    {
      "task_key": "ingest_forex_rates",
      "notebook_task": {
        "notebook_path": "/financestream/ingest/03_forex",
        "base_parameters": { "run_date": "{{job.start_time.iso_date}}" }
      },
      "job_cluster_key": "etl_cluster",
      "max_retries": 3
    },
    {
      "task_key": "transform_silver",
      "depends_on": [
        { "task_key": "ingest_transactions" },
        { "task_key": "ingest_accounts" },
        { "task_key": "ingest_forex_rates" }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/financestream/transform/04_silver",
        "base_parameters": { "run_date": "{{job.start_time.iso_date}}" }
      },
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "build_gold_kpis",
      "depends_on": [{ "task_key": "transform_silver" }],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/financestream/gold/05_kpis",
        "base_parameters": { "run_date": "{{job.start_time.iso_date}}" }
      },
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "notify_partial_failure",
      "depends_on": [
        { "task_key": "ingest_transactions" },
        { "task_key": "ingest_accounts" },
        { "task_key": "ingest_forex_rates" }
      ],
      "run_if": "AT_LEAST_ONE_FAILED",
      "notebook_task": {
        "notebook_path": "/financestream/ops/notify_failure",
        "base_parameters": { "run_date": "{{job.start_time.iso_date}}" }
      },
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "send_daily_report",
      "depends_on": [{ "task_key": "build_gold_kpis" }],
      "run_if": "ALL_DONE",
      "notebook_task": {
        "notebook_path": "/financestream/ops/daily_report",
        "base_parameters": { "run_date": "{{job.start_time.iso_date}}" }
      },
      "job_cluster_key": "etl_cluster"
    }
  ]
}
```

---

## 6. Summary

| Topic              | Key Points                                                                      |
| ------------------ | ------------------------------------------------------------------------------- |
| **Workflows**      | Built-in orchestration for Databricks — multi-task DAGs with full monitoring    |
| **Task Types**     | Notebook, Python script, Wheel, DLT, dbt, SQL, For-Each, Run Job                |
| **Dependencies**   | Tasks execute in topological order; parallel tasks run simultaneously           |
| **`run_if`**       | Control conditional execution: ALL_SUCCESS, AT_LEAST_ONE_FAILED, ALL_DONE, etc. |
| **Task Values**    | Publish and consume key-value data between tasks in the same run                |
| **For-Each Task**  | Dynamic parallel execution over a collection of inputs                          |
| **Repair Run**     | Resume a failed multi-task job from the failure point — saves compute           |
| **Job Clusters**   | Always use for production; SPOT_WITH_FALLBACK for cost savings                  |
| **Monitoring**     | Run dashboard with task-level logs, Spark UI per task, REST API access          |
| **Databricks SDK** | `WorkspaceClient` for programmatic job create/update/trigger/list               |

**What comes next:** The next guide — **Build Data Pipelines with Delta Live Tables** — introduces Databricks's **declarative pipeline framework** (DLT/Spark Declarative Pipelines), which automates much of the operational burden you have manually built in this and the previous guide.

---
