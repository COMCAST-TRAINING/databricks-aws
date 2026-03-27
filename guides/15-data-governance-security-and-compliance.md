# Data Governance, Security, and Compliance in Databricks

---

## Table of Contents

1. [Data Governance Best Practices in Databricks](#1-data-governance-best-practices-in-databricks)
   - 1.1 [What Is Data Governance in a Lakehouse?](#11-what-is-data-governance-in-a-lakehouse)
   - 1.2 [The Governance Framework: People, Process, Technology](#12-the-governance-framework-people-process-technology)
   - 1.3 [Data Ownership and Stewardship Model](#13-data-ownership-and-stewardship-model)
   - 1.4 [Data Catalog Hygiene — Comments, Tags, and Documentation](#14-data-catalog-hygiene--comments-tags-and-documentation)
   - 1.5 [Data Quality as a Governance Component](#15-data-quality-as-a-governance-component)
2. [Managing Access and Permissions (RBAC)](#2-managing-access-and-permissions-rbac)
   - 2.1 [RBAC Design Principles for Databricks](#21-rbac-design-principles-for-databricks)
   - 2.2 [Role Design Patterns Across the Platform](#22-role-design-patterns-across-the-platform)
   - 2.3 [Dynamic Permission Patterns Using is_member()](#23-dynamic-permission-patterns-using-is_member)
   - 2.4 [Managing Permissions at Scale with Groups](#24-managing-permissions-at-scale-with-groups)
   - 2.5 [Workspace-Level Access Controls](#25-workspace-level-access-controls)
   - 2.6 [Network-Level Access Controls](#26-network-level-access-controls)
3. [Data Encryption and Security Best Practices](#3-data-encryption-and-security-best-practices)
   - 3.1 [Encryption at Rest](#31-encryption-at-rest)
   - 3.2 [Encryption in Transit](#32-encryption-in-transit)
   - 3.3 [Customer-Managed Keys (BYOK)](#33-customer-managed-keys-byok)
   - 3.4 [Secrets Management with Databricks Secret Scopes](#34-secrets-management-with-databricks-secret-scopes)
   - 3.5 [Network Isolation: VPC, Private Link, and IP Allowlisting](#35-network-isolation-vpc-private-link-and-ip-allowlisting)
   - 3.6 [Securing Notebooks and Code](#36-securing-notebooks-and-code)
4. [Implementing Data Lineage and Auditing](#4-implementing-data-lineage-and-auditing)
   - 4.1 [End-to-End Lineage Architecture](#41-end-to-end-lineage-architecture)
   - 4.2 [Querying Lineage Programmatically](#42-querying-lineage-programmatically)
   - 4.3 [Building a Data Access Audit Dashboard](#43-building-a-data-access-audit-dashboard)
   - 4.4 [Alerting on Suspicious Access Patterns](#44-alerting-on-suspicious-access-patterns)
   - 4.5 [Retaining Audit Logs for Compliance](#45-retaining-audit-logs-for-compliance)
5. [Compliance Considerations — GDPR and HIPAA](#5-compliance-considerations--gdpr-and-hipaa)
   - 5.1 [GDPR Overview and Key Obligations](#51-gdpr-overview-and-key-obligations)
   - 5.2 [Implementing GDPR Compliance in Databricks](#52-implementing-gdpr-compliance-in-databricks)
   - 5.3 [Right to Erasure — Deleting Personal Data](#53-right-to-erasure--deleting-personal-data)
   - 5.4 [HIPAA Overview and Key Obligations](#54-hipaa-overview-and-key-obligations)
   - 5.5 [Implementing HIPAA Controls in Databricks](#55-implementing-hipaa-controls-in-databricks)
   - 5.6 [Compliance Checklist for Databricks on AWS](#56-compliance-checklist-for-databricks-on-aws)
6. [Summary](#6-summary)

---

## 1. Data Governance Best Practices in Databricks

### 1.1 What Is Data Governance in a Lakehouse?

**Data governance** is the system of policies, standards, and processes that ensures data is **accurate, consistent, discoverable, secure, and used appropriately**. In a Databricks Lakehouse context, governance spans:

- **Access control** — who can read or write which data.
- **Data quality** — ensuring data meets defined standards before it is consumed.
- **Data lineage** — tracking how data flows from source to consumption.
- **Auditability** — recording every access and modification event.
- **Compliance** — meeting legal and regulatory data obligations (GDPR, HIPAA, SOC 2, etc.).
- **Discoverability** — enabling teams to find and understand data assets.

```
┌──────────────────────────────────────────────────────────────┐
│              Data Governance Pillars                         │
│                                                              │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐  │
│  │  Access   │ │  Quality  │ │  Lineage  │ │  Audit    │  │
│  │  Control  │ │Enforcement│ │ Tracking  │ │  Logging  │  │
│  │ (RBAC /   │ │(DLT expec-│ │(Unity Cat │ │(system.   │  │
│  │  UC GRANT)│ │ tations)  │ │ auto-log) │ │  access)  │  │
│  └───────────┘ └───────────┘ └───────────┘ └───────────┘  │
│                                                              │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              Unity Catalog (enforcement layer)         │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌───────────────────────────────────────────────────────┐  │
│  │           Delta Lake (storage + ACID foundation)       │  │
│  └───────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

---

### 1.2 The Governance Framework: People, Process, Technology

Effective governance requires all three dimensions:

**People:**

- Appoint a **Data Platform Lead** or **Data Governance Officer** with authority to enforce standards.
- Define **data domain owners** responsible for specific catalogs or schemas.
- Establish a **data governance committee** that meets regularly to review policies and incidents.

**Process:**

- **Data onboarding checklist:** Before publishing a new table, must it have an owner, description, quality constraints, and appropriate access grants?
- **Access request process:** How do users request access to a table? What approval workflow is required?
- **Incident response:** What happens when unauthorised access or a data breach is detected?
- **Periodic access reviews:** Quarterly review of group memberships and permission grants.

**Technology:**

- Unity Catalog for permissions, lineage, and audit logging.
- DLT expectations for data quality enforcement.
- Databricks secret scopes for secrets management.
- Terraform/CI-CD for permission-as-code.
- SQL Warehouse alerts for anomaly detection.

---

### 1.3 Data Ownership and Stewardship Model

Every securable object in Unity Catalog has an **owner** — the principal who controls permissions on that object. Design ownership thoughtfully:

```sql
-- Assign catalog ownership to a team group (recommended):
ALTER CATALOG retail_prod OWNER TO `data_platform_admins`;

-- Assign schema ownership to the domain team:
ALTER SCHEMA retail_prod.raw OWNER TO `data_engineers`;

-- Assign table ownership to the team that produces it:
ALTER TABLE retail_prod.cleaned.orders OWNER TO `orders_pipeline_team`;
```

**Ownership responsibilities for a table owner:**

- Maintain the table schema and column documentation.
- Approve or deny access requests for the table.
- Ensure data quality constraints are in place.
- Review the table's audit log monthly.
- Handle Right-to-Erasure requests for PII tables.

---

### 1.4 Data Catalog Hygiene — Comments, Tags, and Documentation

A well-documented catalog is essential for discovery and compliance. Invest in catalog metadata:

```sql
-- Add table description:
COMMENT ON TABLE retail_prod.cleaned.orders IS
  'Cleaned and validated customer orders. Updated hourly via the orders_etl pipeline.
   PII fields: customer_id is a surrogate key — no PII. Fully GDPR-compliant.
   Owner: orders_pipeline_team | Refresh: hourly | SLA: data available by :15 each hour';

-- Add column descriptions:
ALTER TABLE retail_prod.cleaned.orders
  ALTER COLUMN order_id    COMMENT 'Unique order identifier (bigint surrogate key)';
ALTER TABLE retail_prod.cleaned.orders
  ALTER COLUMN customer_id COMMENT 'Foreign key to customers table. Not PII — surrogate key only.';
ALTER TABLE retail_prod.cleaned.orders
  ALTER COLUMN amount      COMMENT 'Order total in USD, rounded to 2 decimal places.';

-- Apply classification tags:
ALTER TABLE retail_prod.cleaned.orders
  SET TAGS (
    'domain'            = 'retail',
    'data_layer'        = 'silver',
    'refresh_frequency' = 'hourly',
    'data_owner'        = 'orders_pipeline_team',
    'pii_present'       = 'false'
  );
```

**Documentation standard — require these fields for every published table:**

| Field                 | Required | Example                                |
| --------------------- | :------: | -------------------------------------- |
| Table comment         |    ✓     | "Cleaned orders, updated hourly"       |
| Owner tag             |    ✓     | `owner = orders_pipeline_team`         |
| PII tag               |    ✓     | `pii_present = false`                  |
| Data layer tag        |    ✓     | `data_layer = silver`                  |
| Refresh frequency tag |    ✓     | `refresh_frequency = hourly`           |
| Column comments (key) |    ✓     | On primary key, foreign keys, PII cols |

---

### 1.5 Data Quality as a Governance Component

Data quality is enforced at ingestion via **DLT Expectations** (see Guide 12). At the governance level, monitor quality metrics as part of your governance dashboard:

```sql
-- Query DLT expectation failure metrics (stored in the event log):
SELECT
    timestamp,
    details:flow_progress.metrics.num_output_rows        AS rows_written,
    details:flow_progress.data_quality.dropped_records   AS records_dropped,
    details:flow_progress.data_quality.expectations[0].name AS expectation_name,
    details:flow_progress.data_quality.expectations[0].failed_records AS failures
FROM delta.`/pipelines/<pipeline-id>/system/events`
WHERE event_type = 'flow_progress'
  AND timestamp >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAYS)
ORDER BY timestamp DESC;

-- Business-level quality check via SQL Warehouse alert:
SELECT
    COUNT(*) AS orders_missing_customer_id
FROM retail_prod.cleaned.orders
WHERE customer_id IS NULL
  AND order_date = CURRENT_DATE();
-- Alert: trigger when orders_missing_customer_id > 0
```

---

## 2. Managing Access and Permissions (RBAC)

### 2.1 RBAC Design Principles for Databricks

**Role-Based Access Control (RBAC)** in Databricks spans multiple layers — Unity Catalog data permissions, workspace access, cluster policies, and network controls. Apply these principles consistently:

1. **Default Deny** — No access unless explicitly granted. Users start with zero permissions.
2. **Least Privilege** — Grant only the minimum permissions needed for the user's job.
3. **Group-Based** — Assign permissions to groups, not individuals.
4. **Separation of Duties** — Data engineers should not have admin access to production data they process; an approval step is required.
5. **Time-Bound Access** — For temporary use cases (a contractor, an incident investigation), grant access only for the required duration.
6. **Auditability** — Every permission change is logged. Permissions are managed in code (Terraform).

---

### 2.2 Role Design Patterns Across the Platform

A complete RBAC model covers three layers:

```
Layer 1: Account/Workspace Level (Databricks admin controls)
  ├── Account Admin           — manages users, workspaces, metatstores
  ├── Workspace Admin         — manages clusters, policies, user access within workspace
  ├── Cluster Creator         — can create all-purpose clusters
  └── SQL Warehouse Manager   — can create/manage SQL Warehouses

Layer 2: Unity Catalog Level (data access)
  ├── Metastore Admin         — full control of Unity Catalog
  ├── Catalog Owner           — controls their catalog and delegates to schema owners
  ├── Schema Owner            — controls their schema and table access
  ├── Data Engineer Role      — write access to raw/cleaned schemas, read reporting
  ├── Data Analyst Role       — read-only reporting schema access
  └── Service Principal Role  — automated pipeline write access to specific schemas

Layer 3: Network/Infrastructure Level (AWS controls)
  ├── VPC / Security Groups   — controls which IP ranges can reach Databricks
  ├── S3 Bucket Policies      — controls which IAM roles can access which S3 paths
  └── IAM Roles               — controls what Databricks clusters can do on AWS
```

---

### 2.3 Dynamic Permission Patterns Using is_member()

`is_member()` is a Unity Catalog built-in function that returns `true` if the current query user belongs to the specified group. Use it to build **dynamic, group-aware** row filters and column masks without hardcoding user lists:

```sql
-- ── Universal row filter: privileged groups see all, others see their region ──

CREATE FUNCTION retail_prod.security.row_filter_by_region(row_region STRING)
  RETURNS BOOLEAN
  RETURN
    is_account_admin()
    OR is_member('global_data_viewers')
    OR is_member('data_platform_admins')
    OR row_region = (
        SELECT region
        FROM system.iam.current_user_attributes    -- hypothetical: from IdP attributes
    );

-- ── Column mask with tiered visibility ────────────────────────────────────────

CREATE FUNCTION retail_prod.security.mask_salary(salary DECIMAL(12,2))
  RETURNS DECIMAL(12,2)
  RETURN
    CASE
      WHEN is_member('hr_full_access')     THEN salary           -- full value
      WHEN is_member('hr_managers')        THEN ROUND(salary, -3) -- rounded to nearest $1000
      WHEN is_member('finance_reporting')  THEN NULL             -- hidden entirely
      ELSE NULL
    END;
```

---

### 2.4 Managing Permissions at Scale with Groups

For organisations with many teams and tables, permission management becomes complex. Use a **tiered group model**:

```
Account-Level Groups (synced from IdP):
  company_employees
  ├── data_platform_team
  │    ├── data_engineers
  │    └── data_platform_admins
  ├── analytics
  │    ├── bi_developers
  │    └── data_analysts
  ├── science
  │    └── data_scientists
  └── business
       ├── finance_analysts
       ├── marketing_analysts
       └── operations_analysts

Permission assignment strategy:
  • data_platform_admins  → metastore admin, all catalog admins
  • data_engineers        → write to raw/cleaned catalogs; read reporting
  • bi_developers         → read reporting + selected cleaned views
  • data_analysts         → read reporting only
  • data_scientists       → read cleaned + reporting; write in dev catalog
  • finance_analysts      → read finance_prod.reporting only
```

**Maintain group memberships in your IdP (not in Databricks),** so revocation is handled centrally when someone leaves the organisation.

---

### 2.5 Workspace-Level Access Controls

Beyond Unity Catalog data permissions, control what users can do inside the workspace:

**Cluster policies** — restrict what cluster configurations users can create:

```json
{
  "cluster_type": {
    "type": "fixed",
    "value": "job"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": ["m5.large", "m5.xlarge", "m5.2xlarge"]
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 10,
    "maxValue": 120,
    "defaultValue": 30
  },
  "custom_tags.team": {
    "type": "fixed",
    "value": "analytics"
  }
}
```

**Entitlements** — workspace-level permissions:

| Entitlement                  | What It Allows                                    |
| ---------------------------- | ------------------------------------------------- |
| `workspace-access`           | Log in to the workspace                           |
| `allow-cluster-create`       | Create all-purpose interactive clusters           |
| `allow-instance-pool-create` | Create instance pools                             |
| `databricks-sql-access`      | Access the SQL interface (SQL Editor, Dashboards) |

```python
# Grant SQL access to the analysts group via SDK:
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.groups.patch(
    id="<group-id>",
    operations=[{
        "op": "add",
        "path": "entitlements",
        "value": [{"value": "databricks-sql-access"}]
    }]
)
```

---

### 2.6 Network-Level Access Controls

**IP Access Lists** — restrict workspace access to known IP ranges (corporate network, VPN):

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.settings import ListType

w = WorkspaceClient()

# Allow only corporate VPN range:
w.ip_access_lists.create(
    label="corporate-vpn",
    list_type=ListType.ALLOW,
    ip_addresses=["203.0.113.0/24", "198.51.100.0/24"]  # replace with real CIDR ranges
)
```

**VPC configuration** — deploy Databricks in a customer-managed VPC for full network control:

```
Customer AWS Account
├── VPC (10.0.0.0/16)
│   ├── Private Subnet A (10.0.1.0/24)  ← Databricks worker nodes
│   ├── Private Subnet B (10.0.2.0/24)  ← Databricks worker nodes (AZ-B)
│   └── Public Subnet   (10.0.3.0/24)   ← NAT Gateway
│
├── Security Groups
│   ├── databricks-worker-sg            ← inter-worker traffic only
│   └── databricks-endpoint-sg          ← HTTPS from allowed IP ranges
│
└── VPC Endpoints (AWS PrivateLink)
    ├── S3 VPC Endpoint                 ← S3 traffic stays in VPC
    ├── KMS VPC Endpoint                ← KMS calls stay in VPC
    └── Databricks Control Plane PE     ← no public internet traversal
```

---

## 3. Data Encryption and Security Best Practices

### 3.1 Encryption at Rest

Databricks uses encryption at rest for all data and metadata:

| Component                      | Default Encryption                  | Enhanced Option                      |
| ------------------------------ | ----------------------------------- | ------------------------------------ |
| **S3 data files (Delta Lake)** | SSE-S3 (AES-256, AWS-managed)       | SSE-KMS with customer-managed CMK    |
| **DBFS root (EBS volumes)**    | AES-256 (Databricks-managed)        | Customer-managed keys via Databricks |
| **Unity Catalog metadata**     | AES-256 (Databricks cloud internal) | Not customer-configurable            |
| **Secret scopes**              | Databricks-managed encryption       | AWS Secrets Manager backend (CMK)    |
| **Audit logs (system tables)** | Databricks-managed                  | Export to S3 with your KMS key       |

**Enable SSE-KMS on S3 buckets:**

```json
// S3 bucket policy: enforce SSE-KMS on all uploads
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyUnencryptedObjectUploads",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::company-data-lake/*",
      "Condition": {
        "StringNotEquals": {
          "s3:x-amz-server-side-encryption": "aws:kms"
        }
      }
    }
  ]
}
```

---

### 3.2 Encryption in Transit

All communication in Databricks is encrypted in transit:

| Connection                                 | Protocol                          |
| ------------------------------------------ | --------------------------------- |
| Browser/client → Databricks control plane  | TLS 1.2+, HTTPS                   |
| JDBC/ODBC → SQL Warehouse                  | TLS 1.2+                          |
| Worker nodes ↔ Driver node (intra-cluster) | TLS 1.2+ (optional, configurable) |
| Worker nodes → S3                          | HTTPS via S3 SDK                  |
| Worker nodes → Unity Catalog               | HTTPS                             |
| Databricks CLI / SDK → REST API            | HTTPS                             |

**Enable intra-cluster encryption** for sensitive workloads:

```python
# Cluster configuration to enable inter-node TLS:
cluster_config = {
    "spark_conf": {
        "spark.databricks.io.encryption.enabled": "true"
    }
}
# Note: ~5-10% performance overhead; enable for PII/PHI processing clusters only
```

---

### 3.3 Customer-Managed Keys (BYOK)

**Bring Your Own Key (BYOK)** allows you to use your own AWS KMS key to encrypt Databricks-managed storage (DBFS root, notebook files, job results). This ensures that Databricks cannot access your data without your key.

```
Without BYOK:                           With BYOK:
  Databricks manages DEK/KEK             Your AWS KMS CMK wraps the DEK
  You cannot revoke Databricks' access   Revoke IAM access to KMS → data inaccessible
  Databricks key rotation                You control rotation schedule
```

**Configure via Account Console:**

1. Go to **Account Console** → **Settings** → **Encryption**.
2. Select the workspace.
3. Provide the **AWS KMS Key ARN**.
4. Grant Databricks the `kms:GenerateDataKey`, `kms:Decrypt`, `kms:DescribeKey` permissions on the CMK.

---

### 3.4 Secrets Management with Databricks Secret Scopes

**Never hardcode credentials, API keys, or passwords in notebooks or code.** Use Databricks Secret Scopes instead.

**Secret Scope types:**

| Type                    | Storage Backend                     | Recommended For                      |
| ----------------------- | ----------------------------------- | ------------------------------------ |
| **Databricks-backed**   | Databricks internal encrypted store | Simple secrets, easy setup           |
| **AWS Secrets Manager** | Your AWS Secrets Manager            | Enterprise; CMK encryption; rotation |

**Create a Databricks-backed secret scope:**

```bash
# Using Databricks CLI:
databricks secrets create-scope --scope production-secrets

# Store a secret:
databricks secrets put-secret --scope production-secrets --key db-password

# Grant read access to a group:
databricks secrets put-acl --scope production-secrets --principal analysts --permission READ
```

**Use secrets in notebooks (always use dbutils.secrets, never print):**

```python
# CORRECT — value never appears in output:
jdbc_password = dbutils.secrets.get(scope="production-secrets", key="db-password")

# CORRECT — interpolate into connection string without printing:
jdbc_url = (
    f"jdbc:postgresql://db.company.com:5432/retail"
    f"?user=etl_user&password={jdbc_password}"
)
df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "public.orders")
    .load())

# WRONG — exposes secret in output:
print(dbutils.secrets.get(scope="production-secrets", key="db-password"))
# Databricks replaces the value with [REDACTED] in output, but avoid this pattern entirely.
```

**Create an AWS Secrets Manager-backed scope:**

```bash
databricks secrets create-scope \
  --scope production-secrets \
  --scope-backend-type AWS_SECRETS_MANAGER \
  --resource-id "arn:aws:secretsmanager:us-east-1:<account-id>:secret:databricks/prod"
```

---

### 3.5 Network Isolation: VPC, Private Link, and IP Allowlisting

**AWS PrivateLink** eliminates public internet traffic between Databricks components and your data:

```
Without PrivateLink:
  Worker Node → Internet → Databricks Control Plane (REST API)
  Worker Node → Internet → S3

With PrivateLink:
  Worker Node → VPC Endpoint → Databricks Control Plane (private)
  Worker Node → VPC Endpoint → S3 (private)

Benefits:
  • Data never leaves AWS network backbone
  • No public IP required for worker nodes
  • Satisfies network isolation requirements for HIPAA/PCI DSS
```

**Enable Private Link in Databricks workspace settings (required fields):**

- VPC ID and subnet IDs.
- Workspace relay endpoint (private DNS).
- Databricks back-end private endpoint (`com.databricks.<region>.backend`).
- Databricks front-end private endpoint (`com.databricks.<region>.frontend`).

---

### 3.6 Securing Notebooks and Code

**Notebook access controls:**

```
Default: all workspace users can see all notebooks in the workspace (legacy behaviour)

Recommended: enable workspace object ACLs
  → Settings → Workspace Admin → Advanced → "Enable Workspace Object ACLs"

With ACLs enabled:
  • Private notebooks: only owner can see
  • Can share with specific users or groups (Can View / Can Edit / Can Run / Is Owner)
  • Service principals run notebooks without exposing credentials
```

**Git integration for code security:**

- Store all production notebooks/scripts in Git (Databricks Repos feature).
- Use **branch protection** on `main`/`prod` — require code review before merge.
- Scan notebooks and code for **hardcoded secrets** using tools like `truffleHog` or `gitleaks` in your CI pipeline.
- Use `.gitignore` to exclude files containing credentials.

```bash
# Pre-commit hook to detect secrets before git commit:
pip install detect-secrets
detect-secrets scan > .secrets.baseline
echo ".secrets.baseline" >> .gitignore

# In CI pipeline:
detect-secrets audit .secrets.baseline --only-allowlisted
```

---

## 4. Implementing Data Lineage and Auditing

### 4.1 End-to-End Lineage Architecture

Lineage in a Databricks lakehouse flows through multiple tools. Unity Catalog captures the SQL layer automatically, but a complete lineage picture requires connecting additional points:

```
Source System (CRM, ERP, S3)
         │
         │  [Autoloader / Kafka / Partner Connector]
         ▼
Bronze Delta Table  ────────────────────────────────────────┐
         │  CTAS / INSERT INTO / DLT streaming table        │
         ▼                                                  │
Silver Delta Table  ────────────────────────────────────────┤
         │  CTAS / MERGE / DLT materialized view           │  Unity Catalog
         ▼                                                  │  tracks all SQL
Gold Delta Table                                            │  lineage here
         │  SQL Warehouse queries / Dashboard               │
         ▼                                                  │
BI Tool / Report  ──────────────────────────────────────────┘
```

Unity Catalog stores lineage in `system.access.table_lineage` and `system.access.column_lineage`.

---

### 4.2 Querying Lineage Programmatically

```sql
-- ── What tables were used to create gold.revenue_summary? ───────────────────
SELECT DISTINCT
    source_table_full_name,
    target_table_full_name,
    event_time
FROM system.access.table_lineage
WHERE target_table_full_name = 'retail_prod.reporting.revenue_summary'
ORDER BY event_time DESC;

-- ── What tables read from the customers table (downstream impact)? ───────────
SELECT DISTINCT
    source_table_full_name,
    target_table_full_name,
    created_by         AS pipeline_user
FROM system.access.table_lineage
WHERE source_table_full_name = 'retail_prod.cleaned.customers'
ORDER BY event_time DESC;

-- ── Full upstream dependency graph (recursive CTE): ─────────────────────────
WITH RECURSIVE lineage AS (
    -- Base case: the table we start from
    SELECT
        source_table_full_name,
        target_table_full_name,
        1 AS depth
    FROM system.access.table_lineage
    WHERE target_table_full_name = 'retail_prod.reporting.revenue_summary'

    UNION ALL

    -- Recursive case: go one level further upstream
    SELECT
        tl.source_table_full_name,
        tl.target_table_full_name,
        l.depth + 1
    FROM system.access.table_lineage tl
    INNER JOIN lineage l ON tl.target_table_full_name = l.source_table_full_name
    WHERE l.depth < 10   -- safety limit
)
SELECT DISTINCT source_table_full_name, target_table_full_name, depth
FROM lineage
ORDER BY depth, source_table_full_name;
```

---

### 4.3 Building a Data Access Audit Dashboard

Build a **governance dashboard** in Databricks SQL connected to system tables:

```sql
-- Widget 1: Daily query volume by user group
SELECT
    DATE_TRUNC('day', event_time)            AS query_date,
    user_identity.email                       AS user,
    COUNT(*)                                  AS query_count
FROM system.access.audit
WHERE event_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAYS)
  AND action_name = 'commandSubmit'
GROUP BY query_date, user
ORDER BY query_date DESC, query_count DESC;

-- Widget 2: Most accessed tables (data popularity)
SELECT
    request_params.table_full_name           AS table_name,
    COUNT(*)                                  AS access_count,
    COUNT(DISTINCT user_identity.email)       AS distinct_users,
    MAX(event_time)                           AS last_accessed
FROM system.access.audit
WHERE event_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAYS)
  AND action_name = 'commandSubmit'
  AND request_params.table_full_name IS NOT NULL
GROUP BY table_name
ORDER BY access_count DESC
LIMIT 20;

-- Widget 3: Permission changes over last 90 days
SELECT
    DATE_TRUNC('day', event_time)            AS change_date,
    user_identity.email                       AS changed_by,
    action_name,
    request_params.securable_full_name        AS object_affected,
    request_params.changes                    AS permission_changes
FROM system.access.audit
WHERE event_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAYS)
  AND action_name IN ('updatePermissions', 'grantPermissions', 'revokePermissions')
ORDER BY change_date DESC;

-- Widget 4: Failed access attempts (potential security incidents)
SELECT
    DATE_TRUNC('hour', event_time)           AS hour,
    user_identity.email                       AS user,
    COUNT(*)                                  AS failed_attempts,
    COLLECT_SET(request_params.table_full_name) AS tables_attempted
FROM system.access.audit
WHERE event_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOURS)
  AND response.status_code IN (403, 401)
GROUP BY hour, user
HAVING COUNT(*) >= 5    -- flag users with 5+ failures in an hour
ORDER BY failed_attempts DESC;
```

---

### 4.4 Alerting on Suspicious Access Patterns

Configure **SQL Warehouse Alerts** (see Guide 13) to detect anomalies automatically:

```sql
-- Alert 1: Bulk data export (possible exfiltration)
-- Trigger when: rows_returned > 1,000,000 by a single non-pipeline user
SELECT
    user_name,
    COUNT(*)     AS query_count,
    SUM(rows_produced) AS total_rows
FROM system.query.history
WHERE start_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOURS)
  AND user_name NOT LIKE '%service_principal%'
  AND user_name NOT LIKE '%pipeline%'
GROUP BY user_name
HAVING SUM(rows_produced) > 1000000;

-- Alert 2: Access outside business hours (11pm - 5am UTC)
SELECT COUNT(*) AS after_hours_queries
FROM system.access.audit
WHERE event_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOURS)
  AND HOUR(event_time) BETWEEN 23 AND 5
  AND action_name = 'commandSubmit'
  AND user_identity.email NOT LIKE '%service%';

-- Alert 3: New grants to sensitive table
SELECT COUNT(*) AS new_grants
FROM system.access.audit
WHERE event_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOURS)
  AND action_name IN ('grantPermissions', 'updatePermissions')
  AND request_params.securable_full_name LIKE '%customers%';
```

---

### 4.5 Retaining Audit Logs for Compliance

Unity Catalog system tables retain audit logs for **1 year by default**. For compliance frameworks like HIPAA (6 years) or certain GDPR interpretations, you must export and archive logs to longer-term storage:

```python
# Export audit logs to immutable S3 storage (run daily via Databricks Workflow):

from datetime import date, timedelta

yesterday = date.today() - timedelta(days=1)

audit_df = spark.sql(f"""
    SELECT *
    FROM system.access.audit
    WHERE event_time >= '{yesterday}' AND event_time < '{date.today()}'
""")

# Write to external S3 with KMS encryption, partitioned by date:
(audit_df.write
    .format("delta")
    .mode("append")
    .option("path", "s3://company-compliance-archive/audit-logs/")
    .partitionBy("event_date")
    .save())
```

**S3 archive configuration for compliance:**

```json
// S3 bucket policy for archival bucket:
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Deny",
      "Principal": "*",
      "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
      "Resource": "arn:aws:s3:::company-compliance-archive/*"
    }
  ]
}
// Also enable S3 Object Lock in Compliance Mode for WORM (Write Once Read Many)
```

---

## 5. Compliance Considerations — GDPR and HIPAA

### 5.1 GDPR Overview and Key Obligations

The **General Data Protection Regulation (GDPR)** is the EU data protection law. Key obligations relevant to a data lakehouse:

| GDPR Principle                  | Meaning                                                   | Databricks Implementation                           |
| ------------------------------- | --------------------------------------------------------- | --------------------------------------------------- |
| **Lawfulness**                  | Must have legal basis to process personal data            | Document legal basis per data source in catalog     |
| **Purpose Limitation**          | Collect data only for specified, explicit purposes        | Tag data with `processing_purpose` in Unity Catalog |
| **Data Minimisation**           | Collect only what is necessary                            | Apply column masks or views that exclude unused PII |
| **Accuracy**                    | Data must be accurate and up-to-date                      | DLT expectations, data quality checks               |
| **Storage Limitation**          | Don't keep data longer than necessary                     | Delta Lake TTL policies, VACUUM, scheduled deletes  |
| **Integrity & Confidentiality** | Appropriate security                                      | Encryption, RBAC, secret scopes, network controls   |
| **Right to Access**             | Individuals can request their data                        | Search-by-subject-id queries, documented procedures |
| **Right to Erasure**            | "Right to be forgotten" — delete personal data on request | Delta Lake `DELETE` + GDPR deletion procedure       |

---

### 5.2 Implementing GDPR Compliance in Databricks

**Step 1 — Discover and classify PII:**

```sql
-- Find all tables with PII flag:
SELECT table_catalog, table_schema, table_name
FROM system.information_schema.table_tags
WHERE tag_name = 'pii_present' AND tag_value = 'true'
ORDER BY table_catalog, table_schema, table_name;

-- Find all PII columns across the platform:
SELECT table_catalog, table_schema, table_name, column_name, tag_value AS pii_type
FROM system.information_schema.column_tags
WHERE tag_name = 'pii_type'
ORDER BY table_catalog, table_schema, table_name, column_name;
```

**Step 2 — Apply data minimisation (column masks or non-PII views):**

```sql
-- Create a GDPR-safe view that excludes direct identifiers:
CREATE VIEW retail_prod.reporting.orders_gdpr_safe AS
SELECT
    SHA2(o.customer_id::STRING, 256) AS hashed_customer_id,  -- pseudonymised
    o.order_date,
    o.region,
    o.amount,
    o.status
FROM retail_prod.cleaned.orders o;

GRANT SELECT ON VIEW retail_prod.reporting.orders_gdpr_safe TO `data_analysts`;
```

**Step 3 — Implement data retention policy:**

```python
# Delete personal data older than the retention period (e.g., 3 years):
from datetime import date, timedelta

retention_cutoff = date.today() - timedelta(days=3 * 365)

spark.sql(f"""
    DELETE FROM retail_prod.cleaned.customers
    WHERE last_activity_date < '{retention_cutoff}'
      AND customer_status = 'inactive'
""")

# After deletion, run VACUUM to physically remove deleted files:
spark.sql("""
    VACUUM retail_prod.cleaned.customers RETAIN 0 HOURS
""")
# WARNING: RETAIN 0 HOURS disables safety check — use only for erasure compliance
# Standard recommendation: VACUUM RETAIN 168 HOURS (7 days) for time travel support
```

---

### 5.3 Right to Erasure — Deleting Personal Data

GDPR Article 17 requires deletion of an individual's personal data upon request ("Right to be Forgotten"). In a Delta Lake lakehouse, this requires careful handling because historical versions retain deleted data.

**GDPR delete procedure:**

```python
# Step 1: Delete the record from all tables containing the subject's PII
subject_id = 12345   # customer ID from erasure request

tables_with_pii = [
    "retail_prod.cleaned.customers",
    "retail_prod.cleaned.orders",
    "retail_prod.raw.raw_events",
]

for table in tables_with_pii:
    spark.sql(f"""
        DELETE FROM {table}
        WHERE customer_id = {subject_id}
    """)
    print(f"Deleted customer {subject_id} from {table}")

# Step 2: Confirm deletion
for table in tables_with_pii:
    count = spark.sql(f"""
        SELECT COUNT(*) AS remaining
        FROM {table}
        WHERE customer_id = {subject_id}
    """).collect()[0]["remaining"]
    assert count == 0, f"Records still present in {table}!"

# Step 3: VACUUM to physically remove files containing deleted records
# Note: This eliminates time-travel access to the deleted data
for table in tables_with_pii:
    spark.sql(f"VACUUM {table} RETAIN 0 HOURS")

# Step 4: Log the erasure event for compliance record:
spark.sql(f"""
    INSERT INTO retail_prod.compliance.erasure_log
    VALUES (
        {subject_id},
        current_timestamp(),
        current_user(),
        'GDPR Article 17 erasure request - completed'
    )
""")
```

> **Important:** Before running `VACUUM RETAIN 0 HOURS`, confirm no active streaming readers depend on old versions. This operation is irreversible.

---

### 5.4 HIPAA Overview and Key Obligations

The **Health Insurance Portability and Accountability Act (HIPAA)** applies to healthcare data in the US. Key security rules for data platforms:

| HIPAA Rule                | Requirement                                           | Databricks Control                                     |
| ------------------------- | ----------------------------------------------------- | ------------------------------------------------------ |
| **Access Controls**       | Unique user IDs; automatic logoff; emergency access   | Unity Catalog RBAC; workspace session timeout          |
| **Audit Controls**        | Record and examine activity in systems containing PHI | `system.access.audit` + long-term S3 archival          |
| **Integrity Controls**    | PHI not improperly altered or destroyed               | Delta Lake ACID; S3 versioning; object lock            |
| **Transmission Security** | Protect PHI in transit                                | TLS 1.2+; PrivateLink; VPC endpoints                   |
| **Encryption**            | Encryption recommended (addressable standard)         | SSE-KMS CMK; intra-cluster TLS; secret scopes          |
| **Physical Safeguards**   | Limit physical access to systems                      | AWS data centers (ISO 27001; SOC 2); no on-prem needed |
| **BAA**                   | Business Associate Agreement with cloud providers     | Databricks HIPAA BAA available; AWS HIPAA BAA          |

---

### 5.5 Implementing HIPAA Controls in Databricks

**PHI data handling checklist:**

```sql
-- 1. Tag PHI tables and columns:
ALTER TABLE health_prod.clinical.patient_records
  SET TAGS (
    'phi_present'  = 'true',
    'hipaa_class'  = 'phi',
    'data_owner'   = 'clinical_data_team',
    'retention_years' = '6'
  );

ALTER TABLE health_prod.clinical.patient_records
  ALTER COLUMN ssn   SET TAGS ('phi_type' = 'ssn');
ALTER TABLE health_prod.clinical.patient_records
  ALTER COLUMN dob   SET TAGS ('phi_type' = 'date_of_birth');
ALTER TABLE health_prod.clinical.patient_records
  ALTER COLUMN diagnosis SET TAGS ('phi_type' = 'diagnosis');
```

**Cluster configuration for HIPAA workloads:**

```python
# Cluster spark config for HIPAA-compliant cluster:
hipaa_cluster_config = {
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "m5.2xlarge",
    "spark_conf": {
        # Enable intra-cluster encryption:
        "spark.databricks.io.encryption.enabled": "true",
        # Prevent credentials in Spark UI (mitigates accidental exposure):
        "spark.ui.enabled": "false",
        # Prevent reading unencrypted external data:
        "spark.hadoop.fs.s3a.server-side-encryption-algorithm": "aws:kms",
    },
    "custom_tags": {
        "hipaa_workload": "true",
        "environment": "production"
    }
}
```

**Data de-identification for analytics (HIPAA Safe Harbor Method):**

```sql
-- De-identify data for analytics use (removes 18 PHI identifiers):
CREATE TABLE health_prod.analytics.deidentified_encounters AS
SELECT
    -- Remove direct identifiers:
    SHA2(patient_id::STRING || 'salt_value', 256) AS patient_token,  -- pseudonymised
    -- Generalise quasi-identifiers:
    FLOOR(age / 10) * 10                           AS age_decade,     -- age range not exact age
    LEFT(postal_code, 3)                           AS postal_prefix,  -- region not exact zip
    DATE_TRUNC('year', admission_date)             AS admission_year, -- year not exact date
    -- Retain non-identifying clinical data:
    diagnosis_code,
    procedure_code,
    los_days
FROM health_prod.clinical.patient_encounters
WHERE admission_date >= '2020-01-01';
```

---

### 5.6 Compliance Checklist for Databricks on AWS

Use this checklist to assess your environment's compliance readiness:

**Identity & Access Management**

- [ ] Unity Catalog deployed with metastore assigned to all workspaces
- [ ] All permissions granted to groups, not individual users
- [ ] Service principals used for all automated pipelines
- [ ] Least-privilege model documented and implemented
- [ ] Workspace admin access limited to operations team only
- [ ] Periodic access review schedule in place (quarterly)

**Data Protection**

- [ ] All S3 buckets: public access blocked, default encryption enabled (SSE-KMS)
- [ ] Customer-managed KMS keys configured for sensitive workloads
- [ ] Databricks secret scopes used — no hardcoded credentials anywhere
- [ ] Intra-cluster TLS enabled for PHI/PII processing clusters
- [ ] AWS PrivateLink configured (HIPAA and high-security environments)

**Network Security**

- [ ] Workspace deployed in customer-managed VPC
- [ ] IP Access Lists configured for workspace login
- [ ] S3 VPC endpoints configured — S3 traffic stays off public internet
- [ ] Security groups restrict inter-node traffic appropriately

**Monitoring & Auditing**

- [ ] Unity Catalog `system.access.audit` table is accessible
- [ ] Audit logs exported to long-retention S3 archive (WORM if HIPAA)
- [ ] Data access monitoring dashboard deployed in SQL Warehouse
- [ ] Alerts configured for: failed access, bulk export, after-hours access, permission changes
- [ ] Incident response runbook documented

**GDPR Specific**

- [ ] PII tables and columns tagged (`pii_present`, `pii_type`)
- [ ] Legal basis for processing documented per data source
- [ ] Right-to-Erasure procedure documented and tested
- [ ] Data retention policies implemented with scheduled deletion jobs
- [ ] Data minimisation applied — column masks on unneeded PII for analysts

**HIPAA Specific**

- [ ] Databricks HIPAA BAA signed
- [ ] AWS HIPAA BAA signed
- [ ] PHI tables tagged (`phi_present`, `hipaa_class`)
- [ ] PHI clusters run with intra-cluster encryption enabled
- [ ] 6-year audit log retention in place
- [ ] De-identification procedure for analytics datasets documented
- [ ] Workforce training documentation in place

---

## 6. Summary

Data governance, security, and compliance in Databricks is a multi-layered discipline spanning platform configuration, data controls, access management, and regulatory obligations.

| Domain          | Key Capabilities                                                                      |
| --------------- | ------------------------------------------------------------------------------------- |
| **Governance**  | Unity Catalog ownership model; data classification tags; quality enforcement via DLT  |
| **RBAC**        | Group-based grants; least-privilege; row filters; column masks; dynamic `is_member()` |
| **Encryption**  | SSE-KMS at rest; TLS in transit; BYOK; secret scopes for credentials                  |
| **Network**     | Customer VPC; AWS PrivateLink; IP Access Lists; S3 VPC endpoints                      |
| **Lineage**     | Automatic table/column lineage in `system.access.table_lineage` and `column_lineage`  |
| **Auditing**    | `system.access.audit` for all events; export to S3 for long-term retention            |
| **GDPR**        | PII tagging; column masks; Right to Erasure via `DELETE` + `VACUUM`; retention jobs   |
| **HIPAA**       | PHI classification; encrypted clusters; PrivateLink; de-identification; 6-yr logs     |
| **Operational** | Governance dashboard; anomaly alerts; Terraform for permissions-as-code               |

---

### Cross-Guide Reference

| Topic                              | Primary Guide                                                                                              |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| SQL Warehouse setup and cost       | [Guide 13: Using SQL Warehouse in Databricks](13-using-sql-warehouse-in-databricks.md)                     |
| Unity Catalog setup and RBAC       | [Guide 14: Managing Data Access with Unity Catalog](14-managing-data-access-with-unity-catalog.md)         |
| DLT data quality expectations      | [Guide 12: Build Data Pipelines with Delta Live Tables](12-build-data-pipelines-with-delta-live-tables.md) |
| Delta Lake ACID and VACUUM         | [Guide 09: Managing Data with Delta Lake](09-managing-data-with-delta-lake.md)                             |
| Workflows for scheduled governance | [Guide 11: Deploy Workloads with Databricks Workflows](11-deploy-workloads-with-databricks-workflows.md)   |

---
