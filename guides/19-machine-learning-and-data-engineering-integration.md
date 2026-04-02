# Machine Learning and Data Engineering Integration

---

## Table of Contents

1. [Introduction to Machine Learning in Databricks](#1-introduction-to-machine-learning-in-databricks)
   - 1.1 [What Is Databricks ML?](#11-what-is-databricks-ml)
   - 1.2 [ML vs. Traditional Data Engineering](#12-ml-vs-traditional-data-engineering)
   - 1.3 [Databricks ML Architecture](#13-databricks-ml-architecture)
   - 1.4 [ML Runtimes and Cluster Configuration](#14-ml-runtimes-and-cluster-configuration)
   - 1.5 [Key ML Tools on Databricks](#15-key-ml-tools-on-databricks)
2. [Building and Deploying Simple ML Models](#2-building-and-deploying-simple-ml-models)
   - 2.1 [Preparing a Feature Dataset with Spark](#21-preparing-a-feature-dataset-with-spark)
   - 2.2 [Training a Model with scikit-learn](#22-training-a-model-with-scikit-learn)
   - 2.3 [Training a Model with Spark MLlib](#23-training-a-model-with-spark-mllib)
   - 2.4 [Evaluating Model Performance](#24-evaluating-model-performance)
   - 2.5 [Registering and Serving a Model](#25-registering-and-serving-a-model)
   - 2.6 [Batch Inference with a Registered Model](#26-batch-inference-with-a-registered-model)
3. [Integrating Data Engineering with ML Workflows](#3-integrating-data-engineering-with-ml-workflows)
   - 3.1 [The Feature Engineering Pipeline Pattern](#31-the-feature-engineering-pipeline-pattern)
   - 3.2 [Delta Lake as the ML Feature Store Foundation](#32-delta-lake-as-the-ml-feature-store-foundation)
   - 3.3 [Databricks Feature Store](#33-databricks-feature-store)
   - 3.4 [Retraining Triggers and Data Freshness](#34-retraining-triggers-and-data-freshness)
   - 3.5 [Data Validation Before Training](#35-data-validation-before-training)
   - 3.6 [Online vs. Offline Feature Serving](#36-online-vs-offline-feature-serving)
4. [Using MLflow for Experiment Tracking and Model Management](#4-using-mlflow-for-experiment-tracking-and-model-management)
   - 4.1 [MLflow Overview and Components](#41-mlflow-overview-and-components)
   - 4.2 [Tracking Experiments with mlflow.log\_\*](#42-tracking-experiments-with-mlflowlog__)
   - 4.3 [Auto-Logging with mlflow.autolog](#43-auto-logging-with-mlflowautolog)
   - 4.4 [The MLflow Model Registry](#44-the-mlflow-model-registry)
   - 4.5 [Model Lifecycle: Staging → Production → Archived](#45-model-lifecycle-staging--production--archived)
   - 4.6 [Comparing Experiments in the UI](#46-comparing-experiments-in-the-ui)
   - 4.7 [Model Signatures and Input Examples](#47-model-signatures-and-input-examples)
   - 4.8 [MLflow Projects for Reproducibility](#48-mlflow-projects-for-reproducibility)
5. [Case Study: End-to-End Data Processing and ML Pipeline](#5-case-study-end-to-end-data-processing-and-ml-pipeline)
   - 5.1 [Business Scenario — Telecom Churn Prediction](#51-business-scenario--telecom-churn-prediction)
   - 5.2 [Architecture Overview](#52-architecture-overview)
   - 5.3 [Stage 1: Data Ingestion to Bronze Layer](#53-stage-1-data-ingestion-to-bronze-layer)
   - 5.4 [Stage 2: Data Cleaning to Silver Layer](#54-stage-2-data-cleaning-to-silver-layer)
   - 5.5 [Stage 3: Feature Engineering to Gold Layer](#55-stage-3-feature-engineering-to-gold-layer)
   - 5.6 [Stage 4: Model Training with MLflow](#56-stage-4-model-training-with-mlflow)
   - 5.7 [Stage 5: Model Registration and Promotion](#57-stage-5-model-registration-and-promotion)
   - 5.8 [Stage 6: Batch Scoring and Results](#58-stage-6-batch-scoring-and-results)
   - 5.9 [Orchestrating the Pipeline as a Workflow](#59-orchestrating-the-pipeline-as-a-workflow)
6. [Summary](#6-summary)

---

## 1. Introduction to Machine Learning in Databricks

### 1.1 What Is Databricks ML?

**Databricks ML** is the unified machine learning platform built directly into the Databricks Lakehouse. It removes the common barrier between data engineering (moving and transforming data) and data science (building models on that data) by providing both capabilities on a single, collaborative platform.

```
┌──────────────────────────────────────────────────────────────────┐
│                     DATABRICKS LAKEHOUSE PLATFORM                │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                    Delta Lake Storage                       │  │
│  │   Bronze (Raw)  → Silver (Cleaned)  → Gold (Features)      │  │
│  └────────────────────────────────────────────────────────────┘  │
│           │                    │                  │               │
│  ┌────────▼──────┐   ┌─────────▼──────┐  ┌───────▼──────────┐  │
│  │  Data          │   │   Feature      │   │  Model Training  │  │
│  │  Engineering   │   │   Engineering  │   │  & Experiment    │  │
│  │  (Spark/ETL)   │   │   (Feat Store) │   │  Tracking        │  │
│  └───────────────┘   └────────────────┘   └──────────────────┘  │
│                                                     │             │
│  ┌──────────────────────────────────────────────────▼──────────┐ │
│  │                   MLflow Model Registry                      │ │
│  │            Staging → Production → Archived                   │ │
│  └────────────────────────────────────────────────────────────-┘ │
│                                │                                  │
│  ┌────────────────────────────▼───────────────────────────────┐  │
│  │              Model Serving & Inference (Batch / Real-Time)   │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

Databricks ML provides:

- **Collaborative notebooks** with built-in Python, R, and Scala kernels for ML work
- **MLflow** for end-to-end experiment tracking, model registry, and deployment
- **Databricks Feature Store** for creating, sharing, and serving reusable features
- **AutoML** for rapidly prototyping baseline models
- **Model Serving** for real-time REST endpoint deployment

---

### 1.2 ML vs. Traditional Data Engineering

| Dimension               | Data Engineering                 | Machine Learning                              |
| ----------------------- | -------------------------------- | --------------------------------------------- |
| **Goal**                | Move and transform data reliably | Extract patterns and predictions from data    |
| **Artefacts**           | Tables, pipelines, Jobs          | Models, experiments, feature vectors          |
| **Iteration**           | Low — pipeline runs predictably  | High — hyperparameter tuning, re-training     |
| **Failure mode**        | Data quality, job failure        | Model drift, distribution shift               |
| **Versioning**          | Code + schema versions           | Code + data + model + hyperparameter versions |
| **Deployment**          | Scheduled job, DLT pipeline      | Batch scoring job or real-time REST endpoint  |
| **Key Databricks APIs** | Spark, Delta, Autoloader, DLT    | MLflow, Feature Store, Model Serving          |

---

### 1.3 Databricks ML Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   END-TO-END ML ARCHITECTURE                    │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │          DATA LAYER (Delta Lake)                           │ │
│  │  raw_events → cleaned_events → features → predictions      │ │
│  └───────────────────────────────────────────────────────────┘ │
│       │ Spark ETL           │ Feature Eng.      │ Scoring       │
│  ┌────▼──────┐   ┌──────────▼──────┐   ┌───────▼───────────┐  │
│  │Data Eng.  │   │Feature Store    │   │ Batch / Streaming │  │
│  │Notebooks  │   │(offline table)  │   │ Inference Job     │  │
│  └───────────┘   └─────────────────┘   └───────────────────┘  │
│                                                   ▲             │
│  ┌────────────────────────────────────────────────┤             │
│  │              ML LAYER (MLflow)                  │             │
│  │  Experiments → Runs → Models → Registry        │             │
│  └─────────────────────────────────────────────────             │
│                        │                                         │
│  ┌─────────────────────▼───────────────────────────────────┐   │
│  │              SERVING LAYER                               │   │
│  │  Model Serving Endpoint  │  Spark UDF  │  SQL Function  │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

### 1.4 ML Runtimes and Cluster Configuration

Databricks provides specialised **ML Runtime** images that pre-install popular ML libraries.

**Creating an ML cluster:**

1. Navigate to **Compute** → **Create compute**
2. Under **Databricks Runtime Version**, select e.g. `14.3 LTS ML (includes Apache Spark 3.5.0, GPU)` for GPU nodes or `14.3 LTS ML` for CPU
3. Select an appropriate instance type (e.g. `m5.xlarge` on AWS for CPU, `g4dn.xlarge` for GPU)

**Pre-installed ML libraries in the ML Runtime:**

| Library            | Version (approx.) | Use                           |
| ------------------ | ----------------- | ----------------------------- |
| MLflow             | ≥ 2.x             | Experiment tracking, registry |
| scikit-learn       | ≥ 1.x             | Classical ML                  |
| XGBoost            | ≥ 2.x             | Gradient boosting             |
| LightGBM           | ≥ 4.x             | Fast gradient boosting        |
| PyTorch            | ≥ 2.x             | Deep learning                 |
| TensorFlow / Keras | ≥ 2.x             | Deep learning                 |
| Hugging Face       | ≥ 4.x             | NLP / LLMs                    |
| pandas             | ≥ 2.x             | In-memory data manipulation   |
| numpy / scipy      | latest            | Numeric computing             |

> **Best Practice:** Always select an ML Runtime cluster for ML workloads. You can still use it for ETL — the additional libraries add no overhead to pure Spark jobs.

---

### 1.5 Key ML Tools on Databricks

| Tool                         | Purpose                                                                 |
| ---------------------------- | ----------------------------------------------------------------------- |
| **MLflow Tracking**          | Log parameters, metrics, and artefacts during training runs             |
| **MLflow Registry**          | Version, stage, and deploy models                                       |
| **Databricks AutoML**        | Auto-generate baseline notebooks + MLflow experiments                   |
| **Databricks Feature Store** | Create, share, and serve reusable feature tables                        |
| **Model Serving**            | Deploy models as scalable REST endpoints via a managed serverless layer |
| **Spark MLlib**              | Distributed ML for very large datasets that don't fit in driver memory  |
| **pandas on Spark**          | Run pandas-like code on distributed data (`pyspark.pandas`)             |

---

## 2. Building and Deploying Simple ML Models

### 2.1 Preparing a Feature Dataset with Spark

Data engineers own the pipeline that produces clean, feature-ready tables. Before any model training, the data must be pre-processed and persisted in Delta Lake.

```python
# Notebook: 01_feature_preparation.py
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Load silver-layer customer table
customers_df = spark.table("telecom.silver.customers")
usage_df     = spark.table("telecom.silver.service_usage")

# Aggregate usage features per customer
usage_features = (
    usage_df
    .groupBy("customer_id")
    .agg(
        F.avg("data_gb").alias("avg_monthly_data_gb"),
        F.sum("voice_minutes").alias("total_voice_minutes"),
        F.countDistinct("month").alias("active_months"),
        F.max("data_gb").alias("peak_data_gb"),
        F.stddev("data_gb").alias("stddev_data_gb"),
    )
)

# Join demographic and usage features
feature_df = (
    customers_df
    .join(usage_features, on="customer_id", how="left")
    .select(
        "customer_id",
        "age",
        "tenure_months",
        "contract_type",     # categorical
        "monthly_charges",
        "avg_monthly_data_gb",
        "total_voice_minutes",
        "active_months",
        "peak_data_gb",
        F.coalesce(F.col("stddev_data_gb"), F.lit(0.0)).alias("stddev_data_gb"),
        "churned",           # target label  (1 = churned, 0 = retained)
    )
    .na.fill(0, subset=["avg_monthly_data_gb", "total_voice_minutes"])
)

# Persist as Gold feature table
(
    feature_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("telecom.gold.churn_features")
)

print(f"Feature table written: {feature_df.count()} rows")
```

---

### 2.2 Training a Model with scikit-learn

For datasets that fit in driver memory (typically < 100 M rows with a moderate number of features), collecting to the driver and using **scikit-learn** is the quickest path to a trained model.

```python
# Notebook: 02_train_sklearn.py
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score, classification_report
import mlflow
import mlflow.sklearn

# Collect feature table to the driver
pdf = spark.table("telecom.gold.churn_features").toPandas()

# Encode categorical features
le = LabelEncoder()
pdf["contract_type_enc"] = le.fit_transform(pdf["contract_type"])

FEATURES = [
    "age", "tenure_months", "contract_type_enc",
    "monthly_charges", "avg_monthly_data_gb",
    "total_voice_minutes", "active_months",
    "peak_data_gb", "stddev_data_gb",
]
TARGET = "churned"

X = pdf[FEATURES]
y = pdf[TARGET]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Train
params = {"n_estimators": 200, "max_depth": 5, "learning_rate": 0.05}
model  = GradientBoostingClassifier(**params, random_state=42)
model.fit(X_train, y_train)

# Evaluate
y_prob = model.predict_proba(X_test)[:, 1]
auc    = roc_auc_score(y_test, y_prob)
print(f"ROC-AUC: {auc:.4f}")
print(classification_report(y_test, model.predict(X_test)))
```

---

### 2.3 Training a Model with Spark MLlib

For datasets too large for driver memory, use **Spark MLlib** which distributes training across the cluster.

```python
# Notebook: 02b_train_mllib.py
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, StandardScaler
)
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import mlflow
import mlflow.spark

feature_df = spark.table("telecom.gold.churn_features")

# Encode categorical columns
indexer = StringIndexer(
    inputCol="contract_type",
    outputCol="contract_type_idx",
    handleInvalid="keep"
)

NUMERIC_FEATURES = [
    "age", "tenure_months", "monthly_charges",
    "avg_monthly_data_gb", "total_voice_minutes",
    "active_months", "peak_data_gb", "stddev_data_gb",
]

assembler = VectorAssembler(
    inputCols=NUMERIC_FEATURES + ["contract_type_idx"],
    outputCol="raw_features"
)

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True, withStd=True
)

gbt = GBTClassifier(
    labelCol="churned",
    featuresCol="features",
    maxIter=50,
    maxDepth=5,
)

pipeline = Pipeline(stages=[indexer, assembler, scaler, gbt])

# Train / test split
train_df, test_df = feature_df.randomSplit([0.8, 0.2], seed=42)

model = pipeline.fit(train_df)

# Evaluate
evaluator = BinaryClassificationEvaluator(
    labelCol="churned",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc = evaluator.evaluate(model.transform(test_df))
print(f"MLlib GBT ROC-AUC: {auc:.4f}")
```

---

### 2.4 Evaluating Model Performance

Key evaluation metrics for classification models:

| Metric        | Formula                         | When to Use                          |
| ------------- | ------------------------------- | ------------------------------------ |
| **Accuracy**  | (TP + TN) / Total               | Balanced classes                     |
| **Precision** | TP / (TP + FP)                  | Cost of false positives is high      |
| **Recall**    | TP / (TP + FN)                  | Cost of missing a positive is high   |
| **F1-Score**  | 2 × (Prec × Rec) / (Prec + Rec) | Unbalanced classes                   |
| **ROC-AUC**   | Area under the ROC curve        | Overall model discriminative ability |
| **Log Loss**  | −(1/N) Σ [y log(ŷ)]             | When calibrated probabilities matter |

```python
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score,
    f1_score, roc_auc_score, log_loss
)

y_pred = model.predict(X_test)
y_prob = model.predict_proba(X_test)[:, 1]

metrics = {
    "accuracy":  accuracy_score(y_test, y_pred),
    "precision": precision_score(y_test, y_pred),
    "recall":    recall_score(y_test, y_pred),
    "f1":        f1_score(y_test, y_pred),
    "roc_auc":   roc_auc_score(y_test, y_prob),
    "log_loss":  log_loss(y_test, y_prob),
}
for k, v in metrics.items():
    print(f"  {k}: {v:.4f}")
```

---

### 2.5 Registering and Serving a Model

After training, models are logged to MLflow and registered for deployment.

```python
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

mlflow.set_experiment("/experiments/churn_prediction")

with mlflow.start_run(run_name="gbm_v2") as run:
    # Log parameters
    mlflow.log_params(params)
    # Log metrics
    mlflow.log_metrics(metrics)
    # Log model with signature
    signature = infer_signature(X_train, model.predict(X_train))
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        signature=signature,
        input_example=X_train.head(5),
        registered_model_name="churn_prediction_gbm",
    )
    run_id = run.info.run_id

print(f"Run ID: {run_id}")
```

**Register via the MLflow API:**

```python
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Get the latest version of the registered model
latest = client.get_latest_versions("churn_prediction_gbm", stages=["None"])
version = latest[0].version

# Transition to Staging for validation
client.transition_model_version_stage(
    name="churn_prediction_gbm",
    version=version,
    stage="Staging",
    archive_existing_versions=False,
)
print(f"Model version {version} moved to Staging")
```

---

### 2.6 Batch Inference with a Registered Model

```python
# Notebook: 03_batch_scoring.py
import mlflow.pyfunc

# Load the production model
model_uri = "models:/churn_prediction_gbm/Production"
loaded_model = mlflow.pyfunc.load_model(model_uri)

# Load scoring data from Delta table
score_df = spark.table("telecom.gold.churn_features")
score_pdf = score_df.toPandas()

# Run predictions
score_pdf["churn_probability"] = loaded_model.predict(score_pdf[FEATURES])

# Write predictions back to Delta Lake
predictions_df = spark.createDataFrame(
    score_pdf[["customer_id", "churn_probability"]]
)

(
    predictions_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("telecom.gold.churn_predictions")
)

print("Batch scoring complete.")
```

**For very large datasets — use Spark UDF:**

```python
import mlflow.pyfunc
from pyspark.sql.functions import struct

# Load model as a Spark UDF
predict_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri="models:/churn_prediction_gbm/Production",
    result_type="double",
)

score_df = spark.table("telecom.gold.churn_features")

predictions = score_df.withColumn(
    "churn_probability",
    predict_udf(struct(*FEATURES))
)

predictions.select("customer_id", "churn_probability").write \
    .format("delta").mode("overwrite") \
    .saveAsTable("telecom.gold.churn_predictions")
```

---

## 3. Integrating Data Engineering with ML Workflows

### 3.1 The Feature Engineering Pipeline Pattern

The ML lifecycle introduces a **third layer** above Silver in the Medallion Architecture — the **Features layer** — which bridges data engineering outputs and ML model inputs.

```
┌──────────────────────────────────────────────────────────────────┐
│              EXTENDED MEDALLION ARCHITECTURE FOR ML              │
│                                                                  │
│  S3 Raw Data                                                     │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────┐                                                 │
│  │   BRONZE    │  Raw ingestion — AutoLoader / COPY INTO         │
│  │  (raw_*)    │  Schema-on-read, immutable append               │
│  └──────┬──────┘                                                 │
│         │ Clean + Validate                                        │
│         ▼                                                         │
│  ┌─────────────┐                                                 │
│  │   SILVER    │  Cleaned, typed, deduped                        │
│  │  (cleaned_*)│  Business rules applied                         │
│  └──────┬──────┘                                                 │
│         │ Aggregate + Enrich                                      │
│         ▼                                                         │
│  ┌─────────────┐                                                 │
│  │    GOLD     │  Business-level aggregates for analytics        │
│  │  (kpi_*)    │  and ML feature tables                          │
│  └──────┬──────┘                                                 │
│         │ Feature Join + Normalise                               │
│         ▼                                                         │
│  ┌──────────────────┐                                            │
│  │  FEATURE STORE   │  Point-in-time correct feature tables      │
│  │  (features_*)    │  Reusable across many models               │
│  └──────┬───────────┘                                            │
│         │                                                         │
│         ▼                                                         │
│  ┌───────────────┐   ┌───────────────┐                          │
│  │  ML Training  │   │ Batch Scoring │                          │
│  └───────────────┘   └───────────────┘                          │
└──────────────────────────────────────────────────────────────────┘
```

---

### 3.2 Delta Lake as the ML Feature Store Foundation

Delta Lake is an ideal backing store for ML features because:

- **Time-travel** enables **point-in-time** feature retrieval — preventing training/serving skew by ensuring models trained on historical features use the same version of data as during inference.
- **Schema enforcement** prevents downstream models from receiving malformed feature vectors.
- **ACID transactions** ensure partial writes don't corrupt the feature table.
- **`OPTIMIZE` and Z-ORDER`** compaction on the feature join key (e.g. `customer_id`) dramatically speeds up feature lookups.

```sql
-- Optimise the feature table for fast customer_id lookups
OPTIMIZE telecom.gold.churn_features
ZORDER BY (customer_id);

-- Inspect table history for point-in-time reads
DESCRIBE HISTORY telecom.gold.churn_features;

-- Read the feature table as of a specific commit (point-in-time)
SELECT *
FROM   telecom.gold.churn_features VERSION AS OF 42
WHERE  customer_id = 'C-10042';
```

---

### 3.3 Databricks Feature Store

The **Databricks Feature Store** extends Delta Lake with ML-specific capabilities:

- **Feature lineage** — tracks which models depend on which feature tables.
- **Online/offline serving** — publish features to a low-latency online store for real-time inference.
- **Point-in-time joins** — automatically performs correct time-based joins during training.

```python
from databricks.feature_store import FeatureStoreClient, FeatureLookup
import databricks.feature_store as feature_store

fs = FeatureStoreClient()

# Create the feature table in the Feature Store (backed by Delta)
fs.create_table(
    name="telecom.feature_store.customer_usage_features",
    primary_keys=["customer_id"],
    timestamp_keys=["snapshot_date"],   # for point-in-time joins
    description="Per-customer monthly usage features for churn model",
    schema=feature_df.schema,
)

# Write features to the store
fs.write_table(
    name="telecom.feature_store.customer_usage_features",
    df=feature_df,
    mode="merge",
)

# Training: automatically join features from the store
feature_lookups = [
    FeatureLookup(
        table_name="telecom.feature_store.customer_usage_features",
        feature_names=[
            "avg_monthly_data_gb", "total_voice_minutes",
            "active_months", "peak_data_gb", "stddev_data_gb",
        ],
        lookup_key=["customer_id"],
        timestamp_lookup_key="snapshot_date",
    )
]

# Create a training set with automatic feature join
training_set = fs.create_training_set(
    df=labels_df,                # contains customer_id, snapshot_date, churned
    feature_lookups=feature_lookups,
    label="churned",
    exclude_columns=["customer_id", "snapshot_date"],
)

training_df = training_set.load_df()
```

---

### 3.4 Retraining Triggers and Data Freshness

ML models degrade when the statistical properties of production data shift away from training data (**data drift** / **concept drift**). A robust DE+ML integration layer automates retraining when drift is detected.

**Common retraining triggers:**

| Trigger Type          | Description                                                          | Implementation                                  |
| --------------------- | -------------------------------------------------------------------- | ----------------------------------------------- |
| **Schedule-based**    | Retrain on a fixed cadence (weekly/monthly)                          | Databricks Workflow with a CRON trigger         |
| **Data-volume-based** | Retrain when N new records have accumulated                          | DLT pipeline expectation + conditional job run  |
| **Drift-based**       | Retrain when feature distribution or model accuracy drops            | Evidently / custom drift check notebook in Jobs |
| **Event-based**       | Retrain after a major business event (product launch, market change) | Webhook trigger via Databricks REST API         |

```python
# Example: schedule-based retraining notebook
# Runs as a Databricks Job task on the 1st of each month

import mlflow
from mlflow.tracking import MlflowClient

# Fetch last production model metrics
client = MlflowClient()
prod_models = client.get_latest_versions("churn_prediction_gbm", stages=["Production"])

if prod_models:
    last_auc = float(
        client.get_metric_history(prod_models[0].run_id, "roc_auc")[-1].value
    )
    print(f"Current production ROC-AUC: {last_auc:.4f}")
    if last_auc < 0.75:
        print("Model performance below threshold — triggering retraining!")
        # Trigger retraining logic or fail the job for alerting
        raise ValueError("Model drift detected — retraining required.")
```

---

### 3.5 Data Validation Before Training

Always validate data quality before training to avoid "garbage in, garbage out" models.

```python
from pyspark.sql import functions as F

def validate_feature_table(df, min_rows: int = 1000) -> None:
    """Raise an error if the feature table fails quality checks."""
    total = df.count()

    # Row count check
    assert total >= min_rows, (
        f"Feature table has only {total} rows — expected at least {min_rows}."
    )

    # Null check on critical features
    null_counts = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in FEATURES]
    ).collect()[0].asDict()

    null_violations = {k: v for k, v in null_counts.items() if v > 0}
    if null_violations:
        raise ValueError(f"Null values found in features: {null_violations}")

    # Target distribution check (avoid severe imbalance without a warning)
    churn_rate = df.selectExpr("avg(churned)").collect()[0][0]
    if churn_rate < 0.01 or churn_rate > 0.99:
        print(f"WARNING: Severe class imbalance — churn rate = {churn_rate:.3f}")

    print(f"Feature table validation passed — {total:,} rows.")

feature_df = spark.table("telecom.gold.churn_features")
validate_feature_table(feature_df)
```

---

### 3.6 Online vs. Offline Feature Serving

| Dimension          | Offline (Batch) Serving                     | Online (Real-Time) Serving                       |
| ------------------ | ------------------------------------------- | ------------------------------------------------ |
| **Latency**        | Minutes to hours                            | < 100 ms                                         |
| **Storage**        | Delta Lake (S3)                             | DynamoDB, Redis, Cosmos DB, or Databricks Online |
| **Use case**       | Nightly batch scoring, model training       | Real-time recommendation, fraud scoring          |
| **Consistency**    | Point-in-time correct via Delta time travel | Near-real-time with a short propagation lag      |
| **Infrastructure** | Spark cluster / SQL Warehouse               | Managed online store (Databricks Feature Store)  |

```python
# Publish a feature table to the online store
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store.online_store_spec import AmazonDynamoDBSpec

fs = FeatureStoreClient()

online_store = AmazonDynamoDBSpec(
    region="us-east-1",
    table_name="churn_features_online",
    read_secret_prefix="churn-model/dynamodb-read",   # Databricks secret scope
    write_secret_prefix="churn-model/dynamodb-write",
)

fs.publish_table(
    name="telecom.feature_store.customer_usage_features",
    online_store=online_store,
)
```

---

## 4. Using MLflow for Experiment Tracking and Model Management

### 4.1 MLflow Overview and Components

**MLflow** is an open-source platform for the complete machine learning lifecycle. Databricks hosts a managed, enterprise-grade version natively integrated with Unity Catalog.

```
┌────────────────────────────────────────────────────────────────┐
│                     MLFLOW COMPONENTS                          │
│                                                                │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌─────────┐ │
│  │  Tracking  │  │  Projects  │  │   Models   │  │ Registry│ │
│  │            │  │            │  │            │  │         │ │
│  │ Log params │  │ Reproducib.│  │ Package +  │  │ Version │ │
│  │ Log metrics│  │ ML code in │  │ standardise│  │ Stage   │ │
│  │ Log artefacts  │ conda/venv │  │ any flavour│  │ Deploy  │ │
│  └────────────┘  └────────────┘  └────────────┘  └─────────┘ │
│                                                                │
│  Everything stored in:  /databricks/mlflow-tracking  (managed)│
└────────────────────────────────────────────────────────────────┘
```

| Component    | Purpose                                                                     |
| ------------ | --------------------------------------------------------------------------- |
| **Tracking** | Log and query experiments: parameters, metrics, tags, artefacts             |
| **Projects** | Package ML code in a reusable, reproducible format (MLproject file)         |
| **Models**   | Standard model packaging format with `predict()` interface (pyfunc flavour) |
| **Registry** | Centralised model store with versioning, lifecycle stages, and annotations  |

---

### 4.2 Tracking Experiments with mlflow.log\_\*

```python
import mlflow

# Set the experiment (creates it if it doesn't exist)
mlflow.set_experiment("/experiments/churn_prediction")

with mlflow.start_run(run_name="xgboost_baseline"):
    # --- Hyperparameters ---
    params = {
        "n_estimators":  300,
        "max_depth":     6,
        "learning_rate": 0.03,
        "subsample":     0.8,
        "colsample_bytree": 0.8,
    }
    mlflow.log_params(params)

    # --- Training (abbreviated) ---
    from xgboost import XGBClassifier
    model = XGBClassifier(**params, use_label_encoder=False, eval_metric="logloss")
    model.fit(X_train, y_train,
              eval_set=[(X_test, y_test)],
              verbose=False)

    # --- Metrics ---
    y_prob = model.predict_proba(X_test)[:, 1]
    mlflow.log_metric("roc_auc",   roc_auc_score(y_test, y_prob))
    mlflow.log_metric("log_loss",  log_loss(y_test, y_prob))
    mlflow.log_metric("f1_score",  f1_score(y_test, model.predict(X_test)))

    # --- Artefacts ---
    import matplotlib.pyplot as plt
    from sklearn.metrics import RocCurveDisplay
    fig, ax = plt.subplots()
    RocCurveDisplay.from_predictions(y_test, y_prob, ax=ax)
    fig.savefig("/tmp/roc_curve.png")
    mlflow.log_artifact("/tmp/roc_curve.png", artifact_path="plots")

    # --- Tags ---
    mlflow.set_tag("team", "data_science")
    mlflow.set_tag("feature_version", "v3")

    # --- Log model ---
    mlflow.xgboost.log_model(model, artifact_path="model")
```

---

### 4.3 Auto-Logging with mlflow.autolog

MLflow auto-logging eliminates the need to manually call `log_params`, `log_metrics`, and `log_model` for supported frameworks.

```python
import mlflow

# Enable auto-logging for all supported frameworks in this session
mlflow.autolog()

# OR enable for specific frameworks
mlflow.sklearn.autolog()
mlflow.xgboost.autolog()
mlflow.lightgbm.autolog()
mlflow.spark.autolog()

with mlflow.start_run():
    from sklearn.ensemble import RandomForestClassifier
    model = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42)
    model.fit(X_train, y_train)
    # ↑ All parameters, metrics, and the model are logged automatically
```

**What auto-logging captures:**

| Framework    | Parameters            | Metrics                    | Artefacts                  |
| ------------ | --------------------- | -------------------------- | -------------------------- |
| scikit-learn | All `__init__` params | Training scores            | Feature importances, model |
| XGBoost      | All params            | Eval metrics per iteration | Model                      |
| LightGBM     | All params            | Eval metrics per iteration | Model                      |
| Spark MLlib  | Pipeline params       | Training metrics           | Spark model                |
| PyTorch/TF   | Training config       | Loss per epoch             | Checkpoint, model          |

---

### 4.4 The MLflow Model Registry

The **Model Registry** provides a centralised hub to manage the lifecycle of every model version.

```
┌─────────────────────────────────────────────────────────────────┐
│                    MLflow Model Registry                        │
│                                                                 │
│  Model: churn_prediction_gbm                                    │
│                                                                 │
│  ┌──────────┬─────────────┬────────────────┬──────────────┐    │
│  │ Version  │   Stage     │   Run ID       │  Created     │    │
│  ├──────────┼─────────────┼────────────────┼──────────────┤    │
│  │    1     │  Archived   │  abc123...     │  2026-01-10  │    │
│  │    2     │  Staging    │  def456...     │  2026-02-01  │    │
│  │    3     │  Production │  ghi789...     │  2026-03-01  │    │
│  └──────────┴─────────────┴────────────────┴──────────────┘    │
│                                                                 │
│  Lifecycle stages:  None → Staging → Production → Archived      │
└─────────────────────────────────────────────────────────────────┘
```

```python
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Register a new model version from a completed run
result = mlflow.register_model(
    model_uri=f"runs:/{run_id}/model",
    name="churn_prediction_gbm",
)
print(f"Registered version: {result.version}")

# Add a description to the version
client.update_model_version(
    name="churn_prediction_gbm",
    version=result.version,
    description="XGBoost v3 trained on 24 months of usage data. AUC=0.89",
)
```

---

### 4.5 Model Lifecycle: Staging → Production → Archived

```python
from mlflow.tracking import MlflowClient

client = MlflowClient()

def promote_model(name: str, version: str) -> None:
    """Run validation then promote model from Staging to Production."""

    # Load model and run a quick smoke test
    model_uri  = f"models:/{name}/{version}"
    test_model = mlflow.pyfunc.load_model(model_uri)
    sample_input = X_test.head(10)
    preds = test_model.predict(sample_input)
    assert len(preds) == 10, "Smoke test failed — prediction count mismatch"

    # Archive the current production version
    prod_versions = client.get_latest_versions(name, stages=["Production"])
    for pv in prod_versions:
        client.transition_model_version_stage(
            name=name,
            version=pv.version,
            stage="Archived",
        )

    # Promote the validated version
    client.transition_model_version_stage(
        name=name,
        version=version,
        stage="Production",
    )
    print(f"Model {name} v{version} is now in Production.")

promote_model("churn_prediction_gbm", result.version)
```

---

### 4.6 Comparing Experiments in the UI

1. Open **Experiments** in the left sidebar.
2. Select your experiment (e.g. `/experiments/churn_prediction`).
3. Check multiple run rows → click **Compare**.
4. Use the **Parallel Coordinates** plot to visualise the relationship between hyperparameters and metrics.
5. Use **Metric Charts** tab to plot `roc_auc` over iterations.

**Programmatic comparison:**

```python
from mlflow.tracking import MlflowClient
import pandas as pd

client = MlflowClient()

runs = client.search_runs(
    experiment_ids=["<your-experiment-id>"],
    order_by=["metrics.roc_auc DESC"],
    max_results=10,
)

comparison = pd.DataFrame([
    {
        "run_id":        r.info.run_id,
        "run_name":      r.data.tags.get("mlflow.runName"),
        "roc_auc":       r.data.metrics.get("roc_auc"),
        "log_loss":      r.data.metrics.get("log_loss"),
        "n_estimators":  r.data.params.get("n_estimators"),
        "max_depth":     r.data.params.get("max_depth"),
    }
    for r in runs
])
display(comparison.sort_values("roc_auc", ascending=False))
```

---

### 4.7 Model Signatures and Input Examples

A **model signature** defines the expected schema of model inputs and outputs, enabling MLflow to validate inputs at serving time and preventing runtime errors.

```python
from mlflow.models.signature import infer_signature, ModelSignature
from mlflow.types.schema import Schema, ColSpec

# Infer signature automatically from training data
signature = infer_signature(X_train, model.predict(X_train))
print(signature)
# inputs:  ['age': long, 'tenure_months': long, ...]
# outputs: ['churned': long]

# Log model with signature and a sample input
with mlflow.start_run():
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        signature=signature,
        input_example=X_train.iloc[:3],         # stored as JSON for documentation
        registered_model_name="churn_prediction_gbm",
    )
```

---

### 4.8 MLflow Projects for Reproducibility

An **MLflow Project** packages your training code so it can be run anywhere (locally, on Databricks, in a Docker container) with identical results.

**`MLproject` file (place at the root of your project directory):**

```yaml
name: churn_prediction

conda_env: conda.yaml

entry_points:
  main:
    parameters:
      n_estimators: {type: int, default: 200}
      max_depth:    {type: int, default: 5}
      learning_rate:{type: float, default: 0.05}
    command: "python train.py --n_estimators {n_estimators}
                               --max_depth {max_depth}
                               --learning_rate {learning_rate}"
```

**Run the project on Databricks:**

```python
import mlflow

mlflow.projects.run(
    uri="https://github.com/your-org/churn-model",
    backend="databricks",
    backend_config={
        "spark_version":  "14.3.x-ml-scala2.12",
        "node_type_id":   "m5.xlarge",
        "num_workers":    2,
        "new_cluster":    True,
    },
    parameters={
        "n_estimators":  300,
        "max_depth":     6,
        "learning_rate": 0.03,
    },
)
```

---

## 5. Case Study: End-to-End Data Processing and ML Pipeline

### 5.1 Business Scenario — Telecom Churn Prediction

**Acme Telecom** has 2 million mobile subscribers. The business analytics team estimates that each churned customer costs $240 in acquisition replacement cost. The goal is to build an end-to-end pipeline that:

1. Ingests raw customer and usage data daily from S3.
2. Cleans and standardises the data in Delta tables.
3. Computes per-customer features.
4. Trains a churn prediction model tracked in MLflow.
5. Promotes the best model to production.
6. Scores all active customers daily and surfaces high-risk accounts for the retention team.

**Target SLA:** Daily batch scoring completed by 06:00 UTC, predictions available in the `churn_predictions` table for consumption by the CRM dashboard.

---

### 5.2 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     ACME TELECOM ML PIPELINE                        │
│                                                                     │
│  S3: s3://acme-raw/                                                 │
│  ├── customers/YYYY/MM/DD/customers_*.parquet                       │
│  └── usage/YYYY/MM/DD/usage_*.json                                  │
│                │                                                    │
│     ┌──────────▼──────────────────────────────────────────┐        │
│     │           Task 1: Bronze Ingestion                   │        │
│     │   AutoLoader → telecom.bronze.raw_customers          │        │
│     │   AutoLoader → telecom.bronze.raw_usage              │        │
│     └──────────┬──────────────────────────────────────────┘        │
│                │                                                    │
│     ┌──────────▼──────────────────────────────────────────┐        │
│     │           Task 2: Silver Cleansing                   │        │
│     │   Clean + Validate → telecom.silver.customers        │        │
│     │   Clean + Validate → telecom.silver.usage            │        │
│     └──────────┬──────────────────────────────────────────┘        │
│                │                                                    │
│     ┌──────────▼──────────────────────────────────────────┐        │
│     │           Task 3: Feature Engineering                │        │
│     │   Join + Aggregate → telecom.gold.churn_features     │        │
│     └──────────┬──────────────────────────────────────────┘        │
│                │                     │                              │
│     ┌──────────▼─────────┐  ┌────────▼──────────────────────┐     │
│     │  Task 4: Training   │  │  Task 5: Batch Scoring         │     │
│     │  (weekly, Mondays)  │  │  (daily, all active customers) │     │
│     │  MLflow Experiment  │  │  → telecom.gold.churn_preds    │     │
│     └─────────────────────┘  └────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 5.3 Stage 1: Data Ingestion to Bronze Layer

```python
# task_01_bronze_ingestion.py

def ingest_bronze_customers():
    """Use AutoLoader to incrementally ingest customer parquet files."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "s3://acme-checkpoints/customers/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://acme-raw/customers/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .writeStream
        .format("delta")
        .option("checkpointLocation", "s3://acme-checkpoints/customers/ckpt")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)       # process all pending files, then stop
        .toTable("telecom.bronze.raw_customers")
    )

def ingest_bronze_usage():
    """Use AutoLoader to incrementally ingest usage JSON files."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "s3://acme-checkpoints/usage/schema")
        .load("s3://acme-raw/usage/")
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream
        .format("delta")
        .option("checkpointLocation", "s3://acme-checkpoints/usage/ckpt")
        .trigger(availableNow=True)
        .toTable("telecom.bronze.raw_usage")
    )

q1 = ingest_bronze_customers()
q2 = ingest_bronze_usage()
q1.awaitTermination()
q2.awaitTermination()
```

---

### 5.4 Stage 2: Data Cleaning to Silver Layer

```python
# task_02_silver_cleansing.py
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

raw_customers = spark.table("telecom.bronze.raw_customers")

silver_customers = (
    raw_customers
    # Remove duplicates keeping the latest record
    .withColumn("_rank", F.row_number().over(
        Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
    ))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
    # Type coercions
    .withColumn("age",             F.col("age").cast(IntegerType()))
    .withColumn("tenure_months",   F.col("tenure_months").cast(IntegerType()))
    .withColumn("monthly_charges", F.col("monthly_charges").cast(DoubleType()))
    # Normalise nulls
    .na.fill({"contract_type": "Month-to-month"})
    # Drop internal columns
    .drop("_source_file")
    # Add audit column
    .withColumn("_processed_at", F.current_timestamp())
)

(
    silver_customers
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("telecom.silver.customers")
)
print(f"Silver customers: {silver_customers.count():,} rows")
```

---

### 5.5 Stage 3: Feature Engineering to Gold Layer

```python
# task_03_feature_engineering.py
from pyspark.sql import functions as F
from pyspark.sql import Window

customers = spark.table("telecom.silver.customers")
usage     = spark.table("telecom.silver.usage")

# Rolling 3-month feature window
w3m = (
    usage
    .filter(F.col("month") >= F.add_months(F.current_date(), -3))
    .groupBy("customer_id")
    .agg(
        F.avg("data_gb").alias("avg_3m_data_gb"),
        F.stddev("data_gb").alias("std_3m_data_gb"),
        F.sum("voice_minutes").alias("sum_3m_voice_min"),
        F.count("*").alias("active_months_3m"),
    )
)

# All-time totals
all_time = (
    usage
    .groupBy("customer_id")
    .agg(
        F.sum("data_gb").alias("total_data_gb"),
        F.avg("data_gb").alias("avg_data_gb_all"),
        F.max("data_gb").alias("peak_data_gb"),
    )
)

features = (
    customers
    .join(w3m,      on="customer_id", how="left")
    .join(all_time, on="customer_id", how="left")
    .na.fill(0)
    .withColumn("snapshot_date", F.current_date())
)

(
    features
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("telecom.gold.churn_features")
)
print(f"Feature table: {features.count():,} rows")
```

---

### 5.6 Stage 4: Model Training with MLflow

```python
# task_04_model_training.py  (runs weekly on Mondays)
import mlflow
import mlflow.sklearn
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.metrics import roc_auc_score
import numpy as np

mlflow.set_experiment("/experiments/churn_prediction_prod")

pdf = spark.table("telecom.gold.churn_features").toPandas()

FEATURES = [
    "age", "tenure_months", "monthly_charges",
    "avg_3m_data_gb", "std_3m_data_gb", "sum_3m_voice_min",
    "active_months_3m", "total_data_gb", "peak_data_gb",
]
X = pdf[FEATURES].fillna(0)
y = pdf["churned"]

# Hyperparameter grid search with cross-validation
param_grid = [
    {"n_estimators": 100, "max_depth": 4, "learning_rate": 0.1},
    {"n_estimators": 200, "max_depth": 5, "learning_rate": 0.05},
    {"n_estimators": 300, "max_depth": 6, "learning_rate": 0.03},
]

best_auc, best_params, best_model = 0, None, None

for params in param_grid:
    with mlflow.start_run(run_name=f"gbm_ne{params['n_estimators']}_d{params['max_depth']}"):
        mlflow.log_params(params)
        model = GradientBoostingClassifier(**params, random_state=42)

        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        scores = cross_val_score(model, X, y, cv=cv, scoring="roc_auc", n_jobs=-1)

        model.fit(X, y)
        mean_auc = scores.mean()
        mlflow.log_metric("cv_roc_auc_mean", mean_auc)
        mlflow.log_metric("cv_roc_auc_std",  scores.std())
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            signature=infer_signature(X, model.predict(X)),
            registered_model_name="churn_prediction_gbm",
        )

        if mean_auc > best_auc:
            best_auc    = mean_auc
            best_params = params
            best_model  = model

print(f"Best CV AUC: {best_auc:.4f}  |  Params: {best_params}")
```

---

### 5.7 Stage 5: Model Registration and Promotion

```python
# task_05_model_promotion.py
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature

client = MlflowClient()
MODEL_NAME = "churn_prediction_gbm"
THRESHOLD  = 0.80        # minimum acceptable ROC-AUC

# Get the latest version in None (just registered)
candidates = client.get_latest_versions(MODEL_NAME, stages=["None"])
if not candidates:
    raise ValueError("No candidate model found in None stage.")

latest = max(candidates, key=lambda v: int(v.version))
run    = client.get_run(latest.run_id)
auc    = run.data.metrics.get("cv_roc_auc_mean", 0)

print(f"Candidate: version={latest.version}, CV AUC={auc:.4f}")

if auc < THRESHOLD:
    raise ValueError(
        f"Model AUC {auc:.4f} is below threshold {THRESHOLD}. Not promoting."
    )

# Move to Staging
client.transition_model_version_stage(
    name=MODEL_NAME, version=latest.version, stage="Staging"
)

# Run smoke test
loaded = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/Staging")
test_preds = loaded.predict(X[:50])
assert len(test_preds) == 50

# Archive existing Production, move candidate to Production
for pv in client.get_latest_versions(MODEL_NAME, stages=["Production"]):
    client.transition_model_version_stage(
        name=MODEL_NAME, version=pv.version, stage="Archived"
    )

client.transition_model_version_stage(
    name=MODEL_NAME, version=latest.version, stage="Production"
)
print(f"Version {latest.version} is now in Production.")
```

---

### 5.8 Stage 6: Batch Scoring and Results

```python
# task_06_batch_scoring.py
import mlflow.pyfunc
from pyspark.sql.functions import struct, current_timestamp

model_uri = "models:/churn_prediction_gbm/Production"
predict_udf = mlflow.pyfunc.spark_udf(
    spark, model_uri=model_uri, result_type="double"
)

score_df = spark.table("telecom.gold.churn_features")

predictions = (
    score_df
    .withColumn("churn_probability", predict_udf(struct(*FEATURES)))
    .withColumn("risk_tier", F.when(F.col("churn_probability") >= 0.75, "HIGH")
                              .when(F.col("churn_probability") >= 0.40, "MEDIUM")
                              .otherwise("LOW"))
    .withColumn("scored_at", current_timestamp())
    .select("customer_id", "churn_probability", "risk_tier", "scored_at")
)

(
    predictions
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("telecom.gold.churn_predictions")
)

# Print daily summary
summary = predictions.groupBy("risk_tier").count().orderBy("risk_tier")
display(summary)
```

---

### 5.9 Orchestrating the Pipeline as a Workflow

Define the full pipeline as a **Databricks Workflow** with task dependencies:

```json
{
  "name": "acme_churn_pipeline",
  "schedule": {
    "quartz_cron_expression": "0 0 4 * * ?",
    "timezone_id": "UTC"
  },
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "notebook_task": {
        "notebook_path": "/pipelines/task_01_bronze_ingestion"
      },
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "silver_cleansing",
      "depends_on": [{ "task_key": "bronze_ingestion" }],
      "notebook_task": {
        "notebook_path": "/pipelines/task_02_silver_cleansing"
      },
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "feature_engineering",
      "depends_on": [{ "task_key": "silver_cleansing" }],
      "notebook_task": {
        "notebook_path": "/pipelines/task_03_feature_engineering"
      },
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "batch_scoring",
      "depends_on": [{ "task_key": "feature_engineering" }],
      "notebook_task": { "notebook_path": "/pipelines/task_06_batch_scoring" },
      "job_cluster_key": "etl_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "etl_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-ml-scala2.12",
        "node_type_id": "m5.2xlarge",
        "num_workers": 4
      }
    }
  ]
}
```

> **Note:** Model training runs weekly as a separate workflow (same structure, with a `0 0 2 * * MON` schedule) to decouple the daily scoring cadence from the weekly retraining cadence.

---

## 6. Summary

This guide covered the complete integration of data engineering and machine learning on Databricks:

| Topic                      | Key Takeaway                                                                                      |
| -------------------------- | ------------------------------------------------------------------------------------------------- |
| **Databricks ML Platform** | A unified lakehouse that eliminates the DE/DS tool boundary via shared Delta Lake + MLflow        |
| **ML Runtimes**            | Use `x.y LTS ML` clusters to get pre-installed scikit-learn, XGBoost, PyTorch, etc.               |
| **scikit-learn vs MLlib**  | Use scikit-learn for driver-memory-sized datasets; use MLlib for distributed training             |
| **Feature Engineering**    | Store features in Delta Lake Gold layer; use the Feature Store for reusable, point-in-time joins  |
| **MLflow Tracking**        | Log every experiment with `mlflow.start_run()` — or use `mlflow.autolog()` for zero extra code    |
| **Model Registry**         | Register → Stage → Promote → Archive: enforce a lifecycle with programmatic validation gates      |
| **Batch Inference**        | Use `mlflow.pyfunc.spark_udf` for scalable scoring directly on Spark DataFrames                   |
| **End-to-End Pipeline**    | Chain Bronze → Silver → Gold → Training → Scoring as a Databricks Workflow with task dependencies |

**Further Reading:**

- [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
- [Databricks Feature Store Guide](https://docs.databricks.com/en/machine-learning/feature-store/index.html)
- [Databricks Model Serving](https://docs.databricks.com/en/machine-learning/model-serving/index.html)
- [Databricks AutoML](https://docs.databricks.com/en/machine-learning/automl/index.html)
