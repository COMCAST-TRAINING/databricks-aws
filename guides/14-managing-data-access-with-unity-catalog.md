# Managing Data Access with Unity Catalog

---

## Table of Contents

1. [Introduction to Unity Catalog in Databricks](#1-introduction-to-unity-catalog-in-databricks)
   - 1.1 [What Is Unity Catalog?](#11-what-is-unity-catalog)
   - 1.2 [The Problem Unity Catalog Solves](#12-the-problem-unity-catalog-solves)
   - 1.3 [Unity Catalog Architecture](#13-unity-catalog-architecture)
   - 1.4 [Three-Level Namespace: Catalog → Schema → Table](#14-three-level-namespace-catalog--schema--table)
   - 1.5 [Key Capabilities Overview](#15-key-capabilities-overview)
2. [Setting Up Unity Catalog for Data Governance](#2-setting-up-unity-catalog-for-data-governance)
   - 2.1 [Unity Catalog Prerequisites on AWS](#21-unity-catalog-prerequisites-on-aws)
   - 2.2 [Creating a Unity Catalog Metastore](#22-creating-a-unity-catalog-metastore)
   - 2.3 [Assigning Workspaces to a Metastore](#23-assigning-workspaces-to-a-metastore)
   - 2.4 [Creating Catalogs and Schemas](#24-creating-catalogs-and-schemas)
   - 2.5 [Creating External Locations](#25-creating-external-locations)
   - 2.6 [Creating Managed vs External Tables](#26-creating-managed-vs-external-tables)
   - 2.7 [Upgrading Legacy Hive Metastore Tables](#27-upgrading-legacy-hive-metastore-tables)
3. [Managing Access and Permissions (RBAC) with Unity Catalog](#3-managing-access-and-permissions-rbac-with-unity-catalog)
   - 3.1 [Principals: Users, Groups, and Service Principals](#31-principals-users-groups-and-service-principals)
   - 3.2 [Privilege Hierarchy and Inheritance](#32-privilege-hierarchy-and-inheritance)
   - 3.3 [Securable Objects and Available Privileges](#33-securable-objects-and-available-privileges)
   - 3.4 [Granting and Revoking Privileges](#34-granting-and-revoking-privileges)
   - 3.5 [Row-Level Security with Row Filters](#35-row-level-security-with-row-filters)
   - 3.6 [Column-Level Security with Column Masks](#36-column-level-security-with-column-masks)
   - 3.7 [Service Principal and Automated Workload Access](#37-service-principal-and-automated-workload-access)
4. [Data Lineage and Auditing with Unity Catalog](#4-data-lineage-and-auditing-with-unity-catalog)
   - 4.1 [Automatic Data Lineage Tracking](#41-automatic-data-lineage-tracking)
   - 4.2 [Viewing Lineage in the Catalog UI](#42-viewing-lineage-in-the-catalog-ui)
   - 4.3 [Column-Level Lineage](#43-column-level-lineage)
   - 4.4 [Audit Logging with System Tables](#44-audit-logging-with-system-tables)
   - 4.5 [Querying Audit Logs: Who Accessed What?](#45-querying-audit-logs-who-accessed-what)
   - 4.6 [Data Classification and Tagging](#46-data-classification-and-tagging)
5. [Best Practices for Data Governance with Unity Catalog](#5-best-practices-for-data-governance-with-unity-catalog)
   - 5.1 [Catalog Design Patterns](#51-catalog-design-patterns)
   - 5.2 [Group-Based Access Model](#52-group-based-access-model)
   - 5.3 [Least-Privilege Principle in Practice](#53-least-privilege-principle-in-practice)
   - 5.4 [Naming Conventions for Catalogs and Schemas](#54-naming-conventions-for-catalogs-and-schemas)
   - 5.5 [Managing Sensitive Data](#55-managing-sensitive-data)
   - 5.6 [Infrastructure as Code: Managing Unity Catalog with Terraform](#56-infrastructure-as-code-managing-unity-catalog-with-terraform)
6. [Summary](#6-summary)

---

## 1. Introduction to Unity Catalog in Databricks

### 1.1 What Is Unity Catalog?

**Unity Catalog** is Databricks' unified governance solution for data and AI assets. It provides a single, centralized **metastore** that manages:

- **Data discovery** — a searchable catalog of all tables, views, volumes, and models across all workspaces.
- **Access control** — fine-grained permissions at every level (catalog, schema, table, column, row).
- **Data lineage** — automatic tracking of how data flows from source to consumption.
- **Auditing** — immutable logs of every access and modification event.
- **Data sharing** — Delta Sharing protocol for sharing data with external parties.

Unity Catalog is account-level — one metastore governs **all workspaces** in an account, eliminating the per-workspace Hive metastore silos that existed before.

```
┌─────────────────────────────────────────────────────────────┐
│                  Databricks Account                         │
│                                                             │
│   ┌───────────────────────────────────────────────────┐    │
│   │              Unity Catalog Metastore               │    │
│   │                                                   │    │
│   │  Catalogs  │  Permissions  │  Lineage  │  Audit  │    │
│   └─────────────────────────────────────────────────--┘    │
│               │               │               │             │
│        ┌──────▼──┐    ┌───────▼──┐    ┌──────▼──┐         │
│        │Workspace│    │Workspace │    │Workspace│         │
│        │    A    │    │    B     │    │    C    │         │
│        │(Dev)    │    │(Staging) │    │(Prod)   │         │
│        └─────────┘    └──────────┘    └─────────┘         │
└─────────────────────────────────────────────────────────────┘
```

---

### 1.2 The Problem Unity Catalog Solves

Before Unity Catalog, Databricks workspaces each had their own isolated **Hive Metastore**, creating these problems:

**Problem 1 — Fragmented governance:**

- Each workspace had its own `default` database with no cross-workspace visibility.
- Permissions granted in Workspace A were invisible from Workspace B.
- Auditing required collecting logs from every workspace separately.

**Problem 2 — No fine-grained access control:**

- Table-level grants existed but row/column-level security required custom workarounds (views with filters).
- No centralized masking or dynamic data obfuscation.

**Problem 3 — No automatic lineage:**

- Data provenance was tracked manually (if at all) in external documentation.
- Debug "where did this data come from?" required reading code, not the platform.

**Problem 4 — Data silos across teams:**

- Teams couldn't easily discover what tables other teams had published without documentation.
- No standard for sharing data across business units or with external partners.

Unity Catalog solves all four problems through a single account-level control plane.

---

### 1.3 Unity Catalog Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                   Unity Catalog Metastore                    │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                  Data Catalog                        │    │
│  │   Catalog A             Catalog B           system  │    │
│  │     └─ Schema 1           └─ Schema 1        └─ ...│    │
│  │          └─ Table/View         └─ Table/View        │    │
│  │          └─ Volume             └─ Volume            │    │
│  │          └─ Function           └─ Function          │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │  Permission  │  │   Lineage    │  │   Audit Logs     │  │
│  │  Store       │  │   Graph      │  │   system.access  │  │
│  │  (GRANT/DENY)│  │  (auto-track)│  │   .audit         │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              Storage Credentials                      │   │
│  │  External Locations (S3 paths)  │  Storage Creds     │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
                            │
              Enforced at query time by:
                            │
               ┌────────────┼─────────────┐
               ▼            ▼             ▼
         SQL Warehouse  All-Purpose   Job Cluster
```

---

### 1.4 Three-Level Namespace: Catalog → Schema → Table

Unity Catalog organises all data assets in a **three-level hierarchy**:

```
catalog_name
  └── schema_name  (also called "database")
       ├── table_name
       ├── view_name
       ├── materialized_view_name
       ├── function_name
       └── volume_name  (for files, not tables)
```

**Reference a table fully:**

```sql
SELECT * FROM catalog_name.schema_name.table_name;
```

**Example organisation for a retail company:**

```
retail_prod              ← production data catalog
  ├── raw                ← bronze layer
  │    ├── raw_orders
  │    └── raw_customers
  ├── cleaned            ← silver layer
  │    ├── orders
  │    └── customers
  └── reporting          ← gold layer
       ├── revenue_summary
       └── customer_segments

retail_dev               ← development sandbox catalog
  └── ...

shared_data              ← catalog for cross-team sharing
  └── ...
```

---

### 1.5 Key Capabilities Overview

| Capability              | Description                                                  |
| ----------------------- | ------------------------------------------------------------ |
| **Unified Metastore**   | One catalog for all workspaces in the account                |
| **Fine-Grained ACL**    | Column masking and row filters on Delta tables               |
| **Auto Lineage**        | Table- and column-level lineage tracked automatically        |
| **Audit Logging**       | All read/write/admin events in `system.access.audit`         |
| **Data Discovery**      | Search tables by name, tag, or comment across all workspaces |
| **External Locations**  | Managed access to S3 paths — no raw credentials in code      |
| **Delta Sharing**       | Open-standard protocol for sharing data externally           |
| **AI Asset Governance** | Registers and governs ML models and features alongside data  |

---

## 2. Setting Up Unity Catalog for Data Governance

### 2.1 Unity Catalog Prerequisites on AWS

Before creating a Unity Catalog metastore, ensure the following AWS resources are in place:

```
Required AWS Resources:
─────────────────────
1. S3 Bucket (for managed table storage)
   • Dedicated bucket: s3://company-uc-metastore-root/
   • Block all public access
   • Default encryption: SSE-S3 or SSE-KMS

2. IAM Role (for Unity Catalog to access S3)
   • Role name: databricks-unity-catalog-role
   • Trust policy: allows Databricks service account (Unity Catalog ARN)
   • Permissions policy: s3:GetObject, s3:PutObject, s3:DeleteObject,
     s3:ListBucket on the metastore root bucket and any external locations

3. Cross-Account IAM Role
   • Already exists if Databricks workspace is deployed
   • Must be updated to allow sts:AssumeRole for the UC role above
```

**IAM role trust policy for Unity Catalog:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::414351767826:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<databricks-account-id>"
        }
      }
    }
  ]
}
```

---

### 2.2 Creating a Unity Catalog Metastore

A **metastore** is the top-level Unity Catalog container. Each region should have one metastore.

**Via Account Console UI:**

1. Log in to [accounts.cloud.databricks.com](https://accounts.cloud.databricks.com).
2. Click **Catalog** → **Create Metastore**.
3. Fill in:
   - **Name:** `company-uc-us-east-1`
   - **Region:** `us-east-1` (must match your workspace region)
   - **S3 Location:** `s3://company-uc-metastore-root/`
   - **IAM Role ARN:** `arn:aws:iam::<account-id>:role/databricks-unity-catalog-role`
4. Click **Create**.

**Via Terraform:**

```hcl
resource "databricks_metastore" "main" {
  name          = "company-uc-us-east-1"
  region        = "us-east-1"
  storage_root  = "s3://company-uc-metastore-root/"
  owner         = "data_platform_team"
  force_destroy = false
}

resource "databricks_metastore_data_access" "main" {
  metastore_id = databricks_metastore.main.id
  name         = "uc-root-access"
  aws_iam_role {
    role_arn = "arn:aws:iam::<account-id>:role/databricks-unity-catalog-role"
  }
  is_default = true
}
```

---

### 2.3 Assigning Workspaces to a Metastore

Each Databricks workspace must be **assigned** to the metastore to gain Unity Catalog access.

```python
# Using Databricks SDK (Python) — run in Account Console notebook or admin script
from databricks.sdk import AccountClient

a = AccountClient()

a.metastores.assign(
    workspace_id=1234567890,           # from Account Console → Workspaces
    metastore_id="abc1234-...",        # from Account Console → Catalog → your metastore
    default_catalog_name="main"        # which catalog users land in by default
)
```

After assignment, all users in the workspace can browse Unity Catalog objects subject to their permissions.

---

### 2.4 Creating Catalogs and Schemas

```sql
-- Create a production catalog:
CREATE CATALOG IF NOT EXISTS retail_prod
  COMMENT 'Production data for the Retail business unit';

-- Grant ownership to a team group:
ALTER CATALOG retail_prod OWNER TO `data_platform_team`;

-- Create schemas (layers) within the catalog:
CREATE SCHEMA IF NOT EXISTS retail_prod.raw
  COMMENT 'Bronze layer: raw ingested data';

CREATE SCHEMA IF NOT EXISTS retail_prod.cleaned
  COMMENT 'Silver layer: validated and standardised data';

CREATE SCHEMA IF NOT EXISTS retail_prod.reporting
  COMMENT 'Gold layer: business-ready aggregates for BI/reporting';

-- View all catalogs visible to current user:
SHOW CATALOGS;

-- View all schemas in a catalog:
SHOW SCHEMAS IN retail_prod;
```

---

### 2.5 Creating External Locations

**External Locations** allow Unity Catalog to register S3 paths as trusted locations for external tables, without embedding raw S3 credentials in notebooks.

```sql
-- Step 1: Create a storage credential (links to IAM role):
CREATE STORAGE CREDENTIAL retail_s3_cred
  WITH AWS IAM ROLE
  ROLE_ARN = 'arn:aws:iam::<account-id>:role/databricks-external-data-role';

-- Step 2: Register the external location:
CREATE EXTERNAL LOCATION retail_data_lake
  URL 's3://company-retail-data/'
  WITH (STORAGE CREDENTIAL retail_s3_cred)
  COMMENT 'External location for all retail raw data on S3';

-- Validate the location is accessible:
VALIDATE STORAGE LOCATION 'retail_data_lake';

-- Grant access to specific teams:
GRANT CREATE EXTERNAL TABLE, READ FILES ON EXTERNAL LOCATION retail_data_lake
  TO `data_engineers`;
```

---

### 2.6 Creating Managed vs External Tables

**Managed tables:** Unity Catalog manages the data lifecycle (data stored under the metastore root S3 path). When you `DROP TABLE`, data is deleted.

```sql
-- Managed table — data stored in UC-managed S3 location:
CREATE TABLE retail_prod.cleaned.orders (
    order_id     BIGINT        NOT NULL,
    customer_id  BIGINT        NOT NULL,
    order_date   DATE          NOT NULL,
    status       STRING,
    amount       DECIMAL(12,2)
)
USING DELTA
COMMENT 'Cleaned orders table — silver layer';
```

**External tables:** Data lives at a path you own. `DROP TABLE` only removes the metadata; data stays on S3.

```sql
-- External table — data lives at your S3 location:
CREATE TABLE retail_prod.raw.raw_events
  USING DELTA
  LOCATION 's3://company-retail-data/raw/events/'
  COMMENT 'Raw clickstream events — external table';
```

| Aspect            | Managed Table             | External Table                         |
| ----------------- | ------------------------- | -------------------------------------- |
| **Data Location** | UC metastore root S3 path | Your specified S3 path                 |
| **DROP TABLE**    | Deletes metadata AND data | Deletes metadata only                  |
| **Governance**    | Fully managed by UC       | Managed by UC, data lifecycle is yours |
| **Use Case**      | Standard lakehouse tables | Data shared with other systems / tools |

---

### 2.7 Upgrading Legacy Hive Metastore Tables

If you have tables in the legacy per-workspace Hive metastore (`hive_metastore.default.my_table`), upgrade them to Unity Catalog:

```sql
-- Sync a single table from Hive metastore to Unity Catalog:
CREATE TABLE retail_prod.cleaned.orders
  AS SELECT * FROM hive_metastore.default.orders;

-- Alternatively, use the SYNC command (for external tables pointing to same S3 path):
CREATE TABLE retail_prod.raw.legacy_data
  USING DELTA
  LOCATION 's3://company-data/legacy/'
  COMMENT 'Migrated from hive_metastore';
```

Databricks also provides the **HMS to UC Migration Tool** in the Account Console for bulk migration with lineage preservation.

---

## 3. Managing Access and Permissions (RBAC) with Unity Catalog

### 3.1 Principals: Users, Groups, and Service Principals

Unity Catalog uses **identity federation** — principals are managed in your identity provider (Azure AD, Okta, etc.) and synced into Databricks via **SCIM provisioning**.

```
Identity Provider (Azure AD / Okta)
    │   SCIM sync (automatic, near real-time)
    ▼
Databricks Account
  ├── Users               ← individual human identities
  ├── Groups              ← collections of users (nested groups supported)
  └── Service Principals  ← machine/application identities (API tokens / OAuth)
         │
         ▼
  Unity Catalog GRANT statements reference these principals
```

**Best practice:** Grant permissions to **groups**, not individual users. When an employee leaves, removing them from the group immediately revokes all their data access.

```sql
-- BAD: granting to individual user (operational burden)
GRANT SELECT ON TABLE retail_prod.reporting.revenue_summary TO `alice@company.com`;

-- GOOD: granting to a group (scalable)
GRANT SELECT ON TABLE retail_prod.reporting.revenue_summary TO `bi_analysts`;
```

---

### 3.2 Privilege Hierarchy and Inheritance

Unity Catalog has a strict hierarchy: privileges at a higher level **do not automatically cascade** down — you must grant `USE` privileges down the chain, then the data-access privilege on the target object.

```
To SELECT from retail_prod.cleaned.orders, a user needs:

GRANT USE CATALOG ON CATALOG retail_prod     ← permission to enter the catalog
GRANT USE SCHEMA  ON SCHEMA retail_prod.cleaned  ← permission to enter the schema
GRANT SELECT      ON TABLE retail_prod.cleaned.orders  ← permission to read the table

All three must be present. Missing any one = "TABLE_NOT_FOUND" or "PERMISSION_DENIED"
```

**Granting at a higher level covers USE implicitly for child objects when combined:**

```sql
-- This grants USE CATALOG + USE SCHEMA + SELECT on all tables in the schema:
GRANT SELECT ON SCHEMA retail_prod.reporting TO `bi_analysts`;
-- Note: still need USE CATALOG on retail_prod separately!
GRANT USE CATALOG ON CATALOG retail_prod TO `bi_analysts`;
```

---

### 3.3 Securable Objects and Available Privileges

| Securable Object       | Available Privileges                                                          |
| ---------------------- | ----------------------------------------------------------------------------- |
| **Metastore**          | CREATE CATALOG, CREATE CONNECTION, MANAGE ALLOWLIST, SET SHARE PERMISSION     |
| **Catalog**            | USE CATALOG, CREATE SCHEMA, CREATE CONNECTION                                 |
| **Schema**             | USE SCHEMA, CREATE TABLE, CREATE VIEW, CREATE FUNCTION, CREATE VOLUME, MODIFY |
| **Table/View**         | SELECT, MODIFY, ALL PRIVILEGES                                                |
| **Volume**             | READ VOLUME, WRITE VOLUME, ALL PRIVILEGES                                     |
| **Function**           | EXECUTE                                                                       |
| **External Location**  | CREATE EXTERNAL TABLE, READ FILES, WRITE FILES, ALL PRIVILEGES                |
| **Storage Credential** | CREATE EXTERNAL LOCATION, READ FILES, WRITE FILES                             |

---

### 3.4 Granting and Revoking Privileges

```sql
-- ─── CATALOG LEVEL ───────────────────────────────────────────────────────────

-- Allow data engineers to create schemas in the dev catalog:
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG retail_dev TO `data_engineers`;

-- ─── SCHEMA LEVEL ────────────────────────────────────────────────────────────

-- Allow analysts to read all tables in reporting:
GRANT USE CATALOG ON CATALOG retail_prod TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA retail_prod.reporting TO `analysts`;

-- Allow a pipeline service principal to write to the cleaned schema:
GRANT USE CATALOG ON CATALOG retail_prod TO `etl_service_principal`;
GRANT USE SCHEMA, MODIFY, CREATE TABLE ON SCHEMA retail_prod.cleaned
  TO `etl_service_principal`;

-- ─── TABLE LEVEL ─────────────────────────────────────────────────────────────

-- Grant SELECT on one specific table:
GRANT SELECT ON TABLE retail_prod.cleaned.orders TO `external_partner_group`;

-- Grant full control of a table to an owner:
GRANT ALL PRIVILEGES ON TABLE retail_prod.cleaned.orders TO `table_owner_group`;

-- ─── REVOKE ──────────────────────────────────────────────────────────────────

REVOKE SELECT ON TABLE retail_prod.cleaned.orders FROM `external_partner_group`;

-- ─── INSPECT ─────────────────────────────────────────────────────────────────

-- What privileges does a group have on a catalog?
SHOW GRANTS TO `analysts` ON CATALOG retail_prod;

-- Who has access to this table?
SHOW GRANTS ON TABLE retail_prod.cleaned.orders;

-- What can the current user access?
SHOW GRANTS;
```

---

### 3.5 Row-Level Security with Row Filters

Row filters restrict the rows a query returns based on the user's identity or group membership. The filter is **transparent to the user** — they simply query the table normally and receive only the rows they are authorised to see.

```sql
-- ── Step 1: Create the filter function ──────────────────────────────────────

CREATE FUNCTION retail_prod.security.filter_orders_by_region(order_region STRING)
  RETURNS BOOLEAN
  RETURN
    is_member('global_viewers')          -- global viewers see all rows
    OR order_region = current_region()   -- others see only their region
    OR is_account_admin();               -- account admins see all rows

-- ── Step 2: Apply the filter to the table ───────────────────────────────────

ALTER TABLE retail_prod.cleaned.orders
  SET ROW FILTER retail_prod.security.filter_orders_by_region ON (region);

-- ── Result ────────────────────────────────────────────────────────────────────
-- APAC analyst runs:
SELECT COUNT(*) FROM retail_prod.cleaned.orders;
-- Returns only rows where region = 'APAC' (e.g., 45,000 rows)

-- Global viewer runs same query:
-- Returns all rows (e.g., 200,000 rows)

-- ── Remove the filter ────────────────────────────────────────────────────────
ALTER TABLE retail_prod.cleaned.orders DROP ROW FILTER;
```

---

### 3.6 Column-Level Security with Column Masks

Column masks transform the value of a column when returned to unauthorised users, enabling **data sharing without full exposure** of sensitive fields (PII, financial data, etc.).

```sql
-- ── Step 1: Create mask function for credit card numbers ────────────────────

CREATE FUNCTION retail_prod.security.mask_credit_card(card_number STRING)
  RETURNS STRING
  RETURN
    CASE
      WHEN is_member('payment_team') THEN card_number            -- full access
      WHEN is_member('fraud_analysts') THEN
        CONCAT('****-****-****-', RIGHT(card_number, 4))          -- last 4 visible
      ELSE '****-****-****-****'                                  -- fully masked
    END;

-- ── Step 2: Apply to column ──────────────────────────────────────────────────

ALTER TABLE retail_prod.cleaned.transactions
  ALTER COLUMN credit_card_number
  SET MASK retail_prod.security.mask_credit_card;

-- ── Step 3: Create mask for email addresses ──────────────────────────────────

CREATE FUNCTION retail_prod.security.mask_email(email STRING)
  RETURNS STRING
  RETURN
    CASE
      WHEN is_member('crm_team') THEN email
      ELSE CONCAT(LEFT(email, 2), '***@***.com')
    END;

ALTER TABLE retail_prod.cleaned.customers
  ALTER COLUMN email
  SET MASK retail_prod.security.mask_email;

-- ── Remove a column mask ─────────────────────────────────────────────────────
ALTER TABLE retail_prod.cleaned.customers
  ALTER COLUMN email UNSET MASK;
```

---

### 3.7 Service Principal and Automated Workload Access

For automated pipelines, ETL jobs, and applications, use **service principals** rather than individual user credentials:

```python
# Step 1: Create service principal in Account Console → Service Principals
# Step 2: Generate an OAuth M2M token (recommended) or personal access token
# Step 3: Grant Unity Catalog permissions to the service principal

# SQL grants to service principal:
# GRANT USE CATALOG ON CATALOG retail_prod TO `etl-pipeline-sp`;
# GRANT USE SCHEMA, MODIFY ON SCHEMA retail_prod.cleaned TO `etl-pipeline-sp`;

# Step 4: Authenticate pipeline using OAuth token:
from databricks.sdk import WorkspaceClient
from databricks.sdk.oauth import OAuthM2MCredentials

credentials = OAuthM2MCredentials(
    host="https://<workspace>.cloud.databricks.com",
    client_id="<service-principal-client-id>",
    client_secret=os.environ["SP_CLIENT_SECRET"]   # from secrets manager
)

client = WorkspaceClient(credentials_provider=credentials)
```

---

## 4. Data Lineage and Auditing with Unity Catalog

### 4.1 Automatic Data Lineage Tracking

Unity Catalog automatically captures **runtime lineage** — no annotation, no external tools, no configuration required. Lineage is captured whenever data moves between tables through:

| Operation                   | Lineage Captured |
| --------------------------- | :--------------: |
| `INSERT INTO ... SELECT`    |        ✓         |
| `CREATE TABLE AS SELECT`    |        ✓         |
| `MERGE INTO`                |        ✓         |
| `CREATE VIEW AS SELECT`     |        ✓         |
| DLT pipeline execution      |        ✓         |
| SQL Warehouse queries       |        ✓         |
| Notebook Spark SQL          |        ✓         |
| `spark.sql()` PySpark       |        ✓         |
| Manual file upload (no SQL) |        ✗         |

---

### 4.2 Viewing Lineage in the Catalog UI

1. In the left sidebar, click **Catalog** (the book icon).
2. Browse to a table: `retail_prod` → `reporting` → `revenue_summary`.
3. Click the **Lineage** tab.
4. Toggle between **Upstream** (where data came from) and **Downstream** (where data goes to).
5. Click any node to navigate to that table's lineage.

```
Upstream Lineage of retail_prod.reporting.revenue_summary:
─────────────────────────────────────────────────────────
retail_prod.raw.raw_orders        (bronze)
        │
        ▼
retail_prod.cleaned.orders        (silver)
        │
        ├──▶ retail_prod.reporting.revenue_summary  (gold — THIS TABLE)
        │
        └──▶ retail_prod.reporting.customer_segments
```

---

### 4.3 Column-Level Lineage

Unity Catalog also tracks **which source columns contributed to which target columns**, enabling impact analysis when a source column changes.

**Viewing column lineage:**

1. Navigate to a table → **Lineage** tab.
2. Click on a specific column (e.g., `total_revenue`).
3. The UI shows exactly which upstream columns were used to compute it.

```sql
-- This query creates column-level lineage:
INSERT INTO retail_prod.reporting.revenue_summary
SELECT
    region,                            -- LINEAGE: from cleaned.orders.region
    SUM(amount) AS total_revenue,      -- LINEAGE: from cleaned.orders.amount
    COUNT(*)    AS order_count,        -- LINEAGE: from cleaned.orders.order_id
    CURRENT_DATE() AS report_date      -- LINEAGE: computed (no upstream column)
FROM retail_prod.cleaned.orders
GROUP BY region;
```

**Impact analysis use case:**

> "We're renaming `cleaned.orders.amount` to `cleaned.orders.order_amount`. What tables will break?"
> → Unity Catalog lineage graph shows all downstream tables and columns that reference `amount`.

---

### 4.4 Audit Logging with System Tables

Unity Catalog writes **all access events** to a set of read-only **system tables** under `system.access`:

| System Table                   | Contents                                                  |
| ------------------------------ | --------------------------------------------------------- |
| `system.access.audit`          | All audit events (query, login, permission changes, etc.) |
| `system.access.column_lineage` | Column-level lineage records                              |
| `system.access.table_lineage`  | Table-level lineage records                               |
| `system.billing.usage`         | DBU consumption by cluster/warehouse                      |
| `system.query.history`         | SQL query history for SQL Warehouses                      |
| `system.compute.clusters`      | Cluster creation and termination events                   |

These tables are queryable via SQL and are **append-only** — no one can alter or delete audit records.

---

### 4.5 Querying Audit Logs: Who Accessed What?

```sql
-- ── Who accessed the customer PII table in the last 30 days? ─────────────────
SELECT
    event_time,
    user_identity.email         AS user,
    action_name,
    request_params.table_full_name AS table_accessed
FROM system.access.audit
WHERE event_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAYS)
  AND request_params.table_full_name = 'retail_prod.cleaned.customers'
  AND action_name                    = 'commandSubmit'
ORDER BY event_time DESC;

-- ── Failed permission checks (potential unauthorized access attempts): ────────
SELECT
    event_time,
    user_identity.email AS user,
    action_name,
    response.error_message,
    request_params
FROM system.access.audit
WHERE event_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAYS)
  AND response.status_code IN (403, 401)
ORDER BY event_time DESC;

-- ── Permission grants and revokes (who changed what access): ─────────────────
SELECT
    event_time,
    user_identity.email AS changed_by,
    action_name,
    request_params.securable_full_name,
    request_params.changes
FROM system.access.audit
WHERE event_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAYS)
  AND action_name IN ('updatePermissions', 'grantPermissions', 'revokePermissions')
ORDER BY event_time DESC;

-- ── Top 10 most-queried tables this month: ───────────────────────────────────
SELECT
    request_params.table_full_name,
    COUNT(*) AS query_count,
    COUNT(DISTINCT user_identity.email) AS distinct_users
FROM system.access.audit
WHERE event_time >= DATE_TRUNC('month', CURRENT_TIMESTAMP())
  AND action_name = 'commandSubmit'
  AND request_params.table_full_name IS NOT NULL
GROUP BY request_params.table_full_name
ORDER BY query_count DESC
LIMIT 10;
```

---

### 4.6 Data Classification and Tagging

Unity Catalog supports **tags** on catalogs, schemas, tables, and columns to enable data classification:

```sql
-- Tag a table as containing PII:
ALTER TABLE retail_prod.cleaned.customers
  SET TAGS ('pii' = 'true', 'data_classification' = 'confidential', 'owner' = 'crm_team');

-- Tag a specific column as sensitive:
ALTER TABLE retail_prod.cleaned.customers
  ALTER COLUMN email
  SET TAGS ('pii_type' = 'email', 'gdpr_relevant' = 'true');

-- Search for all PII tables:
SELECT table_catalog, table_schema, table_name, tag_name, tag_value
FROM system.information_schema.table_tags
WHERE tag_name = 'pii' AND tag_value = 'true';

-- Search for GDPR-relevant columns across warehouse:
SELECT table_catalog, table_schema, table_name, column_name, tag_value
FROM system.information_schema.column_tags
WHERE tag_name = 'gdpr_relevant';
```

---

## 5. Best Practices for Data Governance with Unity Catalog

### 5.1 Catalog Design Patterns

**Pattern A — Environment-based catalogs (recommended for most teams):**

```
retail_prod      ← production: all data engineers and analysts read here
retail_staging   ← pre-production testing
retail_dev       ← individual developer sandboxes
shared_data      ← cross-team reference data (exchange rates, geo data)
```

**Pattern B — Domain-based catalogs (recommended for large organisations):**

```
finance_prod     ← owned by Finance team
marketing_prod   ← owned by Marketing team
operations_prod  ← owned by Operations team
shared           ← cross-domain shared assets
```

**Pattern C — Hybrid (environment + domain):**

```
finance_prod / finance_dev
marketing_prod / marketing_dev
...
```

> **Recommendation:** Start with Pattern A (environment-based). Move to Pattern B only when you have clear domain ownership and >5 teams with distinct data domains.

---

### 5.2 Group-Based Access Model

Design a **role matrix** before implementing grants. Common roles for a data platform:

| Role Group              | Catalog Access                 | Schema Access                   | Table Access           |
| ----------------------- | ------------------------------ | ------------------------------- | ---------------------- |
| `data_platform_admins`  | ALL PRIVILEGES on all catalogs | ALL PRIVILEGES on all schemas   | ALL PRIVILEGES         |
| `data_engineers`        | USE on prod; ALL on dev        | CREATE TABLE, MODIFY on cleaned | MODIFY + SELECT        |
| `data_analysts`         | USE on prod                    | USE on reporting                | SELECT only            |
| `bi_developers`         | USE on prod                    | USE on reporting, cleaned       | SELECT only            |
| `data_scientists`       | USE on prod; ALL on dev        | SELECT on cleaned + reporting   | SELECT + CREATE in dev |
| `etl_service_principal` | USE on prod                    | MODIFY on cleaned; USE on raw   | MODIFY + SELECT        |

---

### 5.3 Least-Privilege Principle in Practice

```sql
-- ── WRONG: Over-broad permissions ────────────────────────────────────────────

-- Don't do this — grants way more than analyst needs:
GRANT ALL PRIVILEGES ON CATALOG retail_prod TO `analysts`;

-- ── RIGHT: Minimal necessary permissions ─────────────────────────────────────

-- Analysts should only read from the reporting layer:
GRANT USE CATALOG ON CATALOG retail_prod TO `analysts`;
GRANT USE SCHEMA  ON SCHEMA retail_prod.reporting TO `analysts`;
GRANT SELECT      ON SCHEMA retail_prod.reporting TO `analysts`;
-- analysts cannot see raw or cleaned schemas at all

-- ── WRONG: Granting to individual users ──────────────────────────────────────
GRANT SELECT ON TABLE retail_prod.reporting.revenue_summary TO `alice@company.com`;

-- ── RIGHT: Granting to a group ────────────────────────────────────────────────
GRANT SELECT ON TABLE retail_prod.reporting.revenue_summary TO `bi_analysts`;
-- Then add alice to the bi_analysts group in your IdP
```

---

### 5.4 Naming Conventions for Catalogs and Schemas

Use consistent, predictable naming to make governance easier:

```
Catalogs:
  {domain}_{environment}
  Examples: retail_prod, finance_dev, shared_prod

Schemas:
  {layer} or {domain_layer}
  Examples: raw, cleaned, reporting, ml_features, reference_data

Tables:
  {entity}_{modifier}     (snake_case, descriptive)
  Examples: orders_daily, customer_segments_v2, product_inventory

Views:
  vw_{entity}_{purpose}
  Examples: vw_orders_last_30_days, vw_customer_pii_masked

Functions (masks, filters):
  {purpose}_{target}
  Examples: mask_email, filter_orders_by_region
```

---

### 5.5 Managing Sensitive Data

**Classify and locate PII early** using tags, then apply protection systematically:

```sql
-- 1. Tag PII columns during table creation:
CREATE TABLE retail_prod.cleaned.customers (
    customer_id BIGINT  NOT NULL,
    name        STRING  NOT NULL,
    email       STRING  NOT NULL,    -- PII
    phone       STRING,              -- PII
    address     STRING,              -- PII
    region      STRING  NOT NULL
)
USING DELTA
TBLPROPERTIES ('pii_present' = 'true');

-- After creation, tag individual columns:
ALTER TABLE retail_prod.cleaned.customers ALTER COLUMN email
  SET TAGS ('pii_type' = 'email', 'gdpr' = 'true');

-- 2. Apply column masks to all PII columns:
ALTER TABLE retail_prod.cleaned.customers ALTER COLUMN email
  SET MASK retail_prod.security.mask_email;

ALTER TABLE retail_prod.cleaned.customers ALTER COLUMN phone
  SET MASK retail_prod.security.mask_phone;

-- 3. Create a safe view for teams that don't need PII at all:
CREATE VIEW retail_prod.reporting.customers_anonymised AS
SELECT
    customer_id,
    region,
    DATE_TRUNC('month', created_date) AS cohort_month
FROM retail_prod.cleaned.customers;

GRANT SELECT ON VIEW retail_prod.reporting.customers_anonymised TO `analytics_team`;
```

---

### 5.6 Infrastructure as Code: Managing Unity Catalog with Terraform

For repeatable, auditable governance configuration, manage Unity Catalog with Terraform:

```hcl
# catalog.tf — Production catalog and schemas

resource "databricks_catalog" "retail_prod" {
  name    = "retail_prod"
  comment = "Production data for the Retail business unit"
  owner   = "data_platform_admins"
}

resource "databricks_schema" "raw" {
  catalog_name = databricks_catalog.retail_prod.name
  name         = "raw"
  comment      = "Bronze layer: raw ingested data"
  owner        = "data_engineers"
}

resource "databricks_schema" "reporting" {
  catalog_name = databricks_catalog.retail_prod.name
  name         = "reporting"
  comment      = "Gold layer: business-ready aggregates"
  owner        = "data_platform_admins"
}

# permissions.tf — Access grants

resource "databricks_grants" "catalog_analysts" {
  catalog = databricks_catalog.retail_prod.name
  grant {
    principal  = "analysts"
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "schema_reporting_analysts" {
  schema = "${databricks_catalog.retail_prod.name}.${databricks_schema.reporting.name}"
  grant {
    principal  = "analysts"
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}
```

Store Terraform state and plan in a CI/CD pipeline (GitHub Actions, GitLab CI) to enforce **peer review** before any permission changes take effect.

---

## 6. Summary

Unity Catalog is the governance backbone of the Databricks Lakehouse. It brings centralised, fine-grained, auditable access control to all workspaces in an organisation.

| Concept                   | Key Takeaway                                                                   |
| ------------------------- | ------------------------------------------------------------------------------ |
| **Three-Level Namespace** | `catalog.schema.table` — organises all data assets hierarchically              |
| **Metastore**             | One per region, governs all workspaces — replaces per-workspace Hive metastore |
| **External Locations**    | Governed access to S3 paths — no raw credentials in code                       |
| **Privilege Hierarchy**   | Must grant `USE CATALOG` + `USE SCHEMA` + data privilege for access to work    |
| **Groups Over Users**     | Always grant to groups — easier lifecycle management when people join/leave    |
| **Row Filters**           | Transparent row-level access restriction via SQL functions                     |
| **Column Masks**          | Obfuscate sensitive columns for unauthorised users (PII, financial data)       |
| **Automatic Lineage**     | Table and column lineage captured on every query — no manual tracking needed   |
| **Audit Logs**            | `system.access.audit` — immutable record of all access and permission events   |
| **Tagging**               | Tag tables and columns for data classification (PII, GDPR, etc.)               |
| **Terraform**             | Manage catalogs, schemas, and grants as infrastructure as code                 |

**What's next:** Guide 15 covers **Data Governance, Security, and Compliance** — broadening the governance picture to encryption, RBAC patterns, GDPR/HIPAA compliance, and security best practices across the entire Databricks platform.

---
