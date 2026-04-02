# CI/CD in Databricks

---

## Table of Contents

1. [Version Control: Integrating Notebooks with Git](#1-version-control-integrating-notebooks-with-git)
   - 1.1 [Why Version Control for Notebooks?](#11-why-version-control-for-notebooks)
   - 1.2 [Databricks Repos: Git-Backed Folders](#12-databricks-repos-git-backed-folders)
   - 1.3 [Connecting a Remote Repository](#13-connecting-a-remote-repository)
   - 1.4 [Branching, Committing, and Pull Requests](#14-branching-committing-and-pull-requests)
   - 1.5 [Repository Structure Best Practices](#15-repository-structure-best-practices)
   - 1.6 [`.gitignore` for Databricks Projects](#16-gitignore-for-databricks-projects)
2. [Databricks REST API and Repos](#2-databricks-rest-api-and-repos)
   - 2.1 [REST API Overview and Authentication](#21-rest-api-overview-and-authentication)
   - 2.2 [Personal Access Tokens vs. Service Principals](#22-personal-access-tokens-vs-service-principals)
   - 2.3 [Managing Repos via the API](#23-managing-repos-via-the-api)
   - 2.4 [Triggering Jobs via the REST API](#24-triggering-jobs-via-the-rest-api)
   - 2.5 [Common REST API Endpoints Reference](#25-common-rest-api-endpoints-reference)
   - 2.6 [Using the Databricks CLI](#26-using-the-databricks-cli)
3. [Calling Databricks Jobs from AWS Lambda Using a Service Principal](#3-calling-databricks-jobs-from-aws-lambda-using-a-service-principal)
   - 3.1 [What Is a Service Principal?](#31-what-is-a-service-principal)
   - 3.2 [Creating a Service Principal in Databricks](#32-creating-a-service-principal-in-databricks)
   - 3.3 [Granting the Service Principal Job Permissions](#33-granting-the-service-principal-job-permissions)
   - 3.4 [Storing Credentials Securely in AWS Secrets Manager](#34-storing-credentials-securely-in-aws-secrets-manager)
   - 3.5 [Writing the AWS Lambda Function](#35-writing-the-aws-lambda-function)
   - 3.6 [Triggering a Job Run and Polling for Completion](#36-triggering-a-job-run-and-polling-for-completion)
   - 3.7 [Setting Up the Lambda IAM Role](#37-setting-up-the-lambda-iam-role)
   - 3.8 [End-to-End Architecture Diagram](#38-end-to-end-architecture-diagram)
4. [Infrastructure as Code with Terraform](#4-infrastructure-as-code-with-terraform)
   - 4.1 [Why Terraform for Databricks?](#41-why-terraform-for-databricks)
   - 4.2 [Setting Up the Databricks Terraform Provider](#42-setting-up-the-databricks-terraform-provider)
   - 4.3 [Managing Clusters with Terraform](#43-managing-clusters-with-terraform)
   - 4.4 [Managing Notebooks and Repos with Terraform](#44-managing-notebooks-and-repos-with-terraform)
   - 4.5 [Managing Jobs with Terraform](#45-managing-jobs-with-terraform)
   - 4.6 [Managing Secrets with Terraform](#46-managing-secrets-with-terraform)
   - 4.7 [Managing Unity Catalog with Terraform](#47-managing-unity-catalog-with-terraform)
   - 4.8 [CI/CD Pipeline with Terraform and GitHub Actions](#48-cicd-pipeline-with-terraform-and-github-actions)
   - 4.9 [Workspace Bootstrap: Full Example](#49-workspace-bootstrap-full-example)
5. [Summary](#5-summary)

---

## 1. Version Control: Integrating Notebooks with Git

### 1.1 Why Version Control for Notebooks?

Without version control, Databricks notebooks exist only in the workspace file system — invisible to your team's existing code review process, unreproducible across environments, and impossible to diff or revert after a breaking change.

**Problems version control solves:**

| Problem                                     | Git Solution                                   |
| ------------------------------------------- | ---------------------------------------------- |
| "Who changed this notebook and why?"        | `git log` and commit messages                  |
| "The pipeline broke after yesterday's edit" | `git diff` + `git revert`                      |
| "I need a copy in dev, staging, prod"       | Branches mapped to workspace environments      |
| "I want a peer review before merging"       | Pull Request on GitHub / Azure DevOps / GitLab |
| "I need to run a specific past version"     | `git checkout <tag>` + repo update API         |

---

### 1.2 Databricks Repos: Git-Backed Folders

**Databricks Repos** is a native Git integration that replaces static workspace folders with Git-backed folder trees. A Repo is a checked-out clone of a remote repository stored inside the Databricks workspace.

```
┌──────────────────────────────────────────────────────────────────┐
│                  DATABRICKS REPOS ARCHITECTURE                   │
│                                                                  │
│  Remote Repository (GitHub / GitLab / Azure DevOps / Bitbucket) │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  main branch                                             │   │
│  │    ├── notebooks/                                         │   │
│  │    ├── src/                                               │   │
│  │    └── tests/                                             │   │
│  └───────────────────────┬──────────────────────────────────┘   │
│                           │  Git sync  (pull / push / checkout)  │
│  ┌────────────────────────▼────────────────────────────────┐    │
│  │  Databricks Workspace                                    │    │
│  │                                                          │    │
│  │  /Repos/                                                  │    │
│  │    ├── dev/   → feature/my-feature branch               │    │
│  │    ├── staging/ → release/2026-Q2 branch                 │    │
│  │    └── prod/  → main branch                              │    │
│  └──────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

**Key behaviours:**

- Notebooks in a Repo can be opened, edited, and run exactly like workspace notebooks.
- Git operations (commit, pull, push, branch switch) are available directly in the Databricks UI via the **Repos** sidebar or programmatically via the REST API.
- Repos support `.py`, `.ipynb`, `.sql`, and `.scala` notebook formats.

---

### 1.3 Connecting a Remote Repository

**Step-by-step — GitHub example:**

1. **Generate a GitHub Personal Access Token (PAT):**
   - GitHub → Settings → Developer Settings → Personal Access Tokens → Fine-grained tokens
   - Grant **Contents: Read/Write** on the target repository

2. **Add a Git credential in Databricks:**
   - Databricks workspace → **Settings** → **Linked accounts** → **Add a Git credential**
   - Provider: `GitHub`
   - Token: paste the PAT

3. **Create a Repo:**
   - Left sidebar → **Repos** → **Add Repo**
   - URL: `https://github.com/your-org/databricks-pipelines`
   - Databricks clones the repo into `/Repos/<your-username>/databricks-pipelines`

4. **Assign the Repo to a user or group:**
   - Use the workspace admin panel or `PATCH /api/2.0/repos/{id}` to reassign ownership.

> **Security note:** Never hard-code PATs in notebooks. Use Databricks Secrets or environment variables. Rotate tokens on a 90-day schedule.

---

### 1.4 Branching, Committing, and Pull Requests

**Typical branch strategy for a Databricks project:**

```
main  (protected — CI must pass, PR required)
  ├── develop     (integration branch)
  │     ├── feature/ingest-autoloader   ← developer branch
  │     ├── feature/add-churn-model     ← developer branch
  │     └── fix/silver-null-handling    ← hotfix branch
  └── release/2026-Q2  (staging pre-release)
```

**In the UI — making a commit:**

1. Open any notebook inside the Repo folder.
2. Click the branch name in the top-right corner of the notebook header.
3. **Create branch** → name it `feature/my-change`.
4. Make edits in the notebook.
5. Click **Commit & Push** → add a commit message → **Commit**.

**Programmatically — update a repo to a specific branch:**

```bash
# Using the Databricks CLI
databricks repos update \
  --repo-id 123456789 \
  --branch feature/my-change
```

```python
# Using the REST API directly (Python requests)
import requests

DATABRICKS_HOST  = "https://<workspace-id>.azuredatabricks.net"
DATABRICKS_TOKEN = dbutils.secrets.get("cicd", "databricks-pat")

requests.patch(
    f"{DATABRICKS_HOST}/api/2.0/repos/123456789",
    headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
    json={"branch": "feature/my-change"},
).raise_for_status()
```

---

### 1.5 Repository Structure Best Practices

A well-structured Databricks repository makes CI/CD automation straightforward:

```
databricks-pipelines/
├── .github/
│   └── workflows/
│       ├── ci.yml         # Run tests on every PR
│       └── deploy.yml     # Deploy to staging/prod on merge
├── notebooks/
│   ├── bronze/
│   │   ├── ingest_customers.py
│   │   └── ingest_usage.py
│   ├── silver/
│   │   └── cleanse_customers.py
│   └── gold/
│       └── feature_engineering.py
├── src/
│   └── utils/
│       ├── delta_helpers.py
│       └── validation.py
├── tests/
│   ├── unit/
│   │   ├── test_delta_helpers.py
│   │   └── test_validation.py
│   └── integration/
│       └── test_pipeline_e2e.py
├── infrastructure/
│   └── terraform/
│       ├── main.tf
│       ├── variables.tf
│       └── jobs.tf
├── requirements.txt
├── setup.py         # Makes src/ importable as a package
└── README.md
```

---

### 1.6 `.gitignore` for Databricks Projects

```gitignore
# Databricks-generated files
*.dbc
.databricks/
*/__pycache__/
*.pyc
*.pyo

# Environment
.env
.venv/
venv/

# Test artefacts
.pytest_cache/
htmlcov/
.coverage

# Terraform state (never commit state files)
*.tfstate
*.tfstate.backup
.terraform/

# IDE
.idea/
.vscode/

# macOS
.DS_Store
```

---

## 2. Databricks REST API and Repos

### 2.1 REST API Overview and Authentication

The **Databricks REST API 2.0 / 2.1** provides programmatic access to virtually every platform capability — clusters, jobs, repos, secrets, notebooks, SQL warehouses, and more.

**Base URL format:**

```
https://<databricks-workspace-id>.<cloud-region>.azuredatabricks.net/api/2.0/
```

For AWS:

```
https://<workspace-id>.cloud.databricks.com/api/2.0/
```

**Authentication options:**

| Method                    | Best For                      | How to Use                                      |
| ------------------------- | ----------------------------- | ----------------------------------------------- |
| **Personal Access Token** | Interactive dev / testing     | `Authorization: Bearer <token>` header          |
| **Service Principal**     | CI/CD pipelines, automation   | OAuth 2.0 M2M token (`/oidc/v1/token` endpoint) |
| **OAuth (User)**          | User-facing integrations, CLI | `databricks auth login`                         |

---

### 2.2 Personal Access Tokens vs. Service Principals

| Dimension             | Personal Access Token (PAT)              | Service Principal                               |
| --------------------- | ---------------------------------------- | ----------------------------------------------- |
| **Identity**          | Tied to a specific user account          | Non-human identity (an app/service)             |
| **Expiry**            | Configurable (1 – 730 days)              | OAuth token expires in 1h; refreshable          |
| **Use in automation** | Not recommended — risky if user leaves   | Recommended for all CI/CD and production use    |
| **Permissions**       | Inherits user's permissions              | Explicitly granted minimum required permissions |
| **Audit logs**        | Actions appear under the user's identity | Actions appear under service principal identity |
| **Rotation**          | Manual                                   | Automatic via OAuth token refresh               |

**Creating a PAT (for testing only):**

1. Databricks workspace → top-right icon → **Settings** → **Developer** → **Access tokens** → **Generate new token**
2. Set a short lifetime (e.g. 30 days for testing).
3. Copy and store in a secrets manager immediately — it is only shown once.

---

### 2.3 Managing Repos via the API

**List all repos:**

```bash
curl -X GET \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  "https://$DATABRICKS_HOST/api/2.0/repos"
```

**Create a new repo (clone a remote repository):**

```bash
curl -X POST \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "url":  "https://github.com/your-org/databricks-pipelines.git",
    "provider": "gitHub",
    "path": "/Repos/deploy/databricks-pipelines"
  }' \
  "https://$DATABRICKS_HOST/api/2.0/repos"
```

**Update a repo to a specific branch or tag (used in CI/CD deploy step):**

```bash
# Switch to the main branch before deploying
curl -X PATCH \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"branch": "main"}' \
  "https://$DATABRICKS_HOST/api/2.0/repos/$REPO_ID"

# Or pin to a specific Git tag (for production immutability)
curl -X PATCH \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"tag": "v1.4.2"}' \
  "https://$DATABRICKS_HOST/api/2.0/repos/$REPO_ID"
```

---

### 2.4 Triggering Jobs via the REST API

**Run a job now (one-off triggered run):**

```bash
curl -X POST \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": 98765,
    "notebook_params": {
      "env":         "production",
      "run_date":    "2026-04-02",
      "target_table":"telecom.gold.churn_predictions"
    }
  }' \
  "https://$DATABRICKS_HOST/api/2.1/jobs/run-now"
```

**Response includes `run_id` for polling:**

```json
{ "run_id": 1234567 }
```

**Poll run status:**

```bash
curl -X GET \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  "https://$DATABRICKS_HOST/api/2.1/jobs/runs/get?run_id=1234567"
```

**Key status fields in the response:**

```json
{
  "run_id": 1234567,
  "state": {
    "life_cycle_state": "RUNNING",
    "result_state": null,
    "state_message": "In run"
  }
}
```

`life_cycle_state` values: `PENDING` → `RUNNING` → `TERMINATING` → `TERMINATED`
`result_state` values (once terminated): `SUCCESS`, `FAILED`, `TIMEDOUT`, `CANCELED`

---

### 2.5 Common REST API Endpoints Reference

| Category      | Method | Endpoint                          | Description                  |
| ------------- | ------ | --------------------------------- | ---------------------------- |
| **Clusters**  | GET    | `/api/2.0/clusters/list`          | List all clusters            |
|               | POST   | `/api/2.0/clusters/start`         | Start a terminated cluster   |
|               | POST   | `/api/2.0/clusters/delete`        | Permanently delete a cluster |
| **Jobs**      | GET    | `/api/2.1/jobs/list`              | List all jobs                |
|               | POST   | `/api/2.1/jobs/create`            | Create a new job             |
|               | POST   | `/api/2.1/jobs/run-now`           | Trigger an immediate job run |
|               | GET    | `/api/2.1/jobs/runs/get?run_id=X` | Get run status and metadata  |
|               | POST   | `/api/2.1/jobs/runs/cancel`       | Cancel a running job         |
| **Repos**     | GET    | `/api/2.0/repos`                  | List repos in workspace      |
|               | POST   | `/api/2.0/repos`                  | Create (clone) a repo        |
|               | PATCH  | `/api/2.0/repos/{repo_id}`        | Update branch / tag          |
|               | DELETE | `/api/2.0/repos/{repo_id}`        | Delete a repo                |
| **Secrets**   | POST   | `/api/2.0/secrets/scopes/create`  | Create a secret scope        |
|               | POST   | `/api/2.0/secrets/put`            | Create or update a secret    |
|               | GET    | `/api/2.0/secrets/list?scope=X`   | List secret keys in a scope  |
| **Notebooks** | POST   | `/api/2.0/workspace/import`       | Upload / import a notebook   |
|               | GET    | `/api/2.0/workspace/export`       | Export a notebook            |
| **SQL**       | GET    | `/api/2.0/sql/warehouses`         | List SQL warehouses          |
|               | POST   | `/api/2.0/sql/statements`         | Execute a SQL statement      |

---

### 2.6 Using the Databricks CLI

The **Databricks CLI** wraps the REST API with a developer-friendly command-line interface.

**Install and configure:**

```bash
# Install via pip (v0.200+ — the new Rust-based CLI)
pip install databricks-cli

# Authenticate (opens browser for OAuth or accepts a PAT)
databricks configure --token
# Prompt: Databricks Host: https://<workspace>.cloud.databricks.com
# Prompt: Token: <your-pat>

# Or use environment variables (preferred for CI pipelines)
export DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-pat-or-sp-token>"
```

**Common CLI commands:**

```bash
# List jobs
databricks jobs list

# Trigger a job run
databricks jobs run-now --job-id 98765

# Get run output
databricks runs get --run-id 1234567

# Upload a notebook
databricks workspace import \
  --language PYTHON \
  --overwrite \
  ./notebooks/silver/cleanse_customers.py \
  /Repos/prod/databricks-pipelines/notebooks/silver/cleanse_customers

# Update a repo
databricks repos update \
  --repo-id $REPO_ID \
  --branch main

# Manage secrets
databricks secrets create-scope --scope cicd
databricks secrets put --scope cicd --key databricks-pat
```

---

## 3. Calling Databricks Jobs from AWS Lambda Using a Service Principal

### 3.1 What Is a Service Principal?

A **Service Principal** is a non-human identity registered in Databricks (backed by the account identity provider — either Databricks accounts, Microsoft Entra ID, or AWS IAM Identity Center). It acts as the identity for automated systems — CI/CD pipelines, AWS Lambda functions, cron jobs — so that no human user's credentials are embedded in automation.

```
┌──────────────────────────────────────────────────────────────────┐
│                   SERVICE PRINCIPAL FLOW                         │
│                                                                  │
│  Databricks Account                                              │
│  ┌──────────────────────┐                                        │
│  │  Service Principal   │                                        │
│  │  • Client ID         │◄──── Created once in account admin    │
│  │  • Client Secret     │      Stored in AWS Secrets Manager     │
│  └──────────┬───────────┘                                        │
│             │  OAuth 2.0 token exchange                          │
│             ▼                                                     │
│  ┌──────────────────────┐                                        │
│  │  Access Token (1h)   │◄──── POST /oidc/v1/token              │
│  │  Bearer eyJhbG...    │                                        │
│  └──────────┬───────────┘                                        │
│             │  REST API calls                                     │
│             ▼                                                     │
│  ┌──────────────────────┐                                        │
│  │  Databricks Jobs API │                                        │
│  │  POST /jobs/run-now  │                                        │
│  └──────────────────────┘                                        │
└──────────────────────────────────────────────────────────────────┘
```

---

### 3.2 Creating a Service Principal in Databricks

1. **Account Admin panel:** `https://accounts.cloud.databricks.com`
2. Navigate to **User management** → **Service principals** → **Add service principal**
3. Enter a name (e.g. `lambda-pipeline-trigger`) → **Add**
4. Open the created service principal → **Generate secret**
5. Copy the **Client ID** and **Client Secret** — store immediately in AWS Secrets Manager (shown only once)

**Assign the service principal to your workspace:**

1. Databricks workspace → **Settings** → **Identity and access** → **Service principals**
2. **Add service principal** → select `lambda-pipeline-trigger`
3. Grant **Can use** or higher role

---

### 3.3 Granting the Service Principal Job Permissions

The service principal needs explicit permission on each job it must trigger.

**Via the Databricks UI:**

1. Jobs page → select the target job → **Permissions**
2. Add the service principal → set role to **Can Manage Run**

**Via the Permissions API:**

```bash
curl -X PUT \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "access_control_list": [
      {
        "service_principal_name": "<client-id>",
        "permission_level": "CAN_MANAGE_RUN"
      }
    ]
  }' \
  "https://$DATABRICKS_HOST/api/2.0/permissions/jobs/$JOB_ID"
```

---

### 3.4 Storing Credentials Securely in AWS Secrets Manager

Never hard-code credentials in Lambda code. Store the service principal's Client ID and Client Secret in **AWS Secrets Manager**.

```bash
# Create a secret with two fields
aws secretsmanager create-secret \
  --name "databricks/lambda-pipeline-trigger" \
  --description "Databricks Service Principal credentials for Lambda" \
  --secret-string '{
    "client_id":     "8a2f1c3d-...",
    "client_secret": "doBxxxxxxxxxxxxxxxxxxxxxxx",
    "databricks_host": "https://dbc-12345.cloud.databricks.com",
    "job_id": "98765"
  }' \
  --region us-east-1
```

Enable **automatic rotation** for the secret in the AWS console to rotate the service principal secret on a schedule.

---

### 3.5 Writing the AWS Lambda Function

```python
# lambda_function.py
"""
AWS Lambda function that retrieves Databricks credentials from
AWS Secrets Manager and triggers a Databricks Job run via the REST API.
"""

import json
import os
import time
import boto3
import urllib.request
import urllib.parse
import urllib.error


def get_secret(secret_name: str) -> dict:
    """Retrieve a secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=os.environ["AWS_REGION"])
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_databricks_token(host: str, client_id: str, client_secret: str) -> str:
    """
    Exchange service principal credentials for a short-lived OAuth access token
    using the Databricks OIDC M2M token endpoint.
    """
    token_url  = f"{host}/oidc/v1/token"
    body       = urllib.parse.urlencode({
        "grant_type":    "client_credentials",
        "scope":         "all-apis",
    }).encode()

    import base64
    credentials = base64.b64encode(
        f"{client_id}:{client_secret}".encode()
    ).decode()

    req = urllib.request.Request(
        token_url,
        data=body,
        headers={
            "Content-Type":  "application/x-www-form-urlencoded",
            "Authorization": f"Basic {credentials}",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["access_token"]


def trigger_job(host: str, token: str, job_id: int, params: dict) -> int:
    """Trigger a Databricks job run and return the run_id."""
    url  = f"{host}/api/2.1/jobs/run-now"
    body = json.dumps({
        "job_id":           job_id,
        "notebook_params":  params,
    }).encode()

    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["run_id"]


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point."""
    secret_name = os.environ.get(
        "DATABRICKS_SECRET_NAME", "databricks/lambda-pipeline-trigger"
    )
    creds = get_secret(secret_name)

    host          = creds["databricks_host"]
    client_id     = creds["client_id"]
    client_secret = creds["client_secret"]
    job_id        = int(creds["job_id"])

    # Obtain an OAuth access token
    token = get_databricks_token(host, client_id, client_secret)

    # Pass through any run parameters from the event payload
    notebook_params = event.get("notebook_params", {})

    # Trigger the job
    run_id = trigger_job(host, token, job_id, notebook_params)

    print(f"Databricks job {job_id} triggered — run_id={run_id}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "job_id": job_id,
            "run_id": run_id,
        }),
    }
```

---

### 3.6 Triggering a Job Run and Polling for Completion

For pipelines that need the Lambda to wait until the Databricks job finishes (synchronous pattern), use polling with a configurable timeout.

```python
def poll_run_completion(
    host: str,
    token: str,
    run_id: int,
    poll_interval_secs: int = 30,
    timeout_secs: int = 3600,
) -> str:
    """
    Poll a Databricks run until it reaches a terminal state.
    Returns the result_state: SUCCESS | FAILED | TIMEDOUT | CANCELED.
    """
    url     = f"{host}/api/2.1/jobs/runs/get?run_id={run_id}"
    headers = {"Authorization": f"Bearer {token}"}
    elapsed = 0

    while elapsed < timeout_secs:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=30) as resp:
            data  = json.loads(resp.read())
            state = data["state"]
            lcs   = state["life_cycle_state"]
            msg   = state.get("state_message", "")

        print(f"[{elapsed}s] Run {run_id}: {lcs} — {msg}")

        if lcs == "TERMINATED":
            result = state.get("result_state", "UNKNOWN")
            if result != "SUCCESS":
                raise RuntimeError(
                    f"Databricks run {run_id} ended with result_state={result}: {msg}"
                )
            return result

        if lcs in ("SKIPPED", "INTERNAL_ERROR"):
            raise RuntimeError(
                f"Databricks run {run_id} failed unexpectedly: {lcs} — {msg}"
            )

        time.sleep(poll_interval_secs)
        elapsed += poll_interval_secs

    raise TimeoutError(
        f"Databricks run {run_id} did not complete within {timeout_secs}s."
    )


# In lambda_handler, after trigger_job():
# result = poll_run_completion(host, token, run_id)
# print(f"Run completed with: {result}")
```

> **Note:** AWS Lambda has a maximum execution timeout of **15 minutes**. For jobs longer than 15 minutes, use the **fire-and-forget** pattern (just trigger the run and return the `run_id`), and monitor completion via a separate Step Function or CloudWatch alarm.

---

### 3.7 Setting Up the Lambda IAM Role

The Lambda execution role must have permission to read the secret from AWS Secrets Manager — and nothing else (least privilege principle).

**IAM policy to attach to the Lambda role:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowReadDatabricksSecret",
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": "arn:aws:secretsmanager:us-east-1:123456789012:secret:databricks/lambda-pipeline-trigger-*"
    }
  ]
}
```

> **Best Practice:** Lock the `Resource` ARN to the specific secret — never use `*`. This prevents the Lambda from reading any other secrets even if it were compromised.

---

### 3.8 End-to-End Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│               AWS LAMBDA → DATABRICKS JOB TRIGGER FLOW               │
│                                                                      │
│  ┌──────────────┐                                                    │
│  │  Event Source │                                                   │
│  │  S3 PUT event │──── triggers ────────────────────────────────┐   │
│  │  EventBridge  │                                              │   │
│  │  API Gateway  │                                              │   │
│  └──────────────┘                                              │   │
│                                                                ▼   │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                     AWS Lambda                                │  │
│  │  1. Read secret (client_id, client_secret) from              │  │
│  │     AWS Secrets Manager                                       │  │
│  │  2. POST /oidc/v1/token → receive OAuth access_token         │  │
│  │  3. POST /api/2.1/jobs/run-now → receive run_id              │  │
│  │  4. (optional) Poll /api/2.1/jobs/runs/get until TERMINATED  │  │
│  └──────────┬───────────────────────────────────────────────────┘  │
│             │                    │                                  │
│             ▼                    ▼                                  │
│  ┌──────────────────┐  ┌──────────────────────────────────────┐   │
│  │ AWS Secrets Mgr  │  │       Databricks Workspace            │   │
│  │  client_id       │  │                                       │   │
│  │  client_secret   │  │  Service Principal authenticated      │   │
│  │  databricks_host │  │  Job ID 98765 triggered               │   │
│  │  job_id          │  │  Notebook runs on Job Cluster         │   │
│  └──────────────────┘  └──────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 4. Infrastructure as Code with Terraform

### 4.1 Why Terraform for Databricks?

Without IaC, Databricks workspace setup — clusters, jobs, repos, permissions, secrets, Unity Catalog objects — is performed manually through the UI, making it:

- **Unreproducible** — the dev workspace configuration drifts from production over time.
- **Undocumented** — no record of who created what or why.
- **Unversioned** — destructive changes can't be rolled back.
- **Error-prone** — manual click-through misses steps under pressure.

**Terraform with the [Databricks provider](https://registry.terraform.io/providers/databricks/databricks)** solves all four problems:

```
┌──────────────────────────────────────────────────────────────────┐
│                  TERRAFORM FOR DATABRICKS                        │
│                                                                  │
│  main.tf  ──► Terraform Plan ──► Terraform Apply                │
│                                                                  │
│  Manages:                                                        │
│  • Workspace provisioning (AWS — via aws provider)               │
│  • Clusters (interactive, job clusters)                          │
│  • Jobs and Workflows                                            │
│  • Repos (Git integrations)                                      │
│  • Secret scopes and secrets                                     │
│  • Unity Catalog (catalogs, schemas, grants)                     │
│  • SQL Warehouses                                                │
│  • Permissions (clusters, jobs, notebooks, repos)                │
└──────────────────────────────────────────────────────────────────┘
```

---

### 4.2 Setting Up the Databricks Terraform Provider

**`versions.tf`:**

```hcl
terraform {
  required_version = ">= 1.5"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.38"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Store Terraform state in S3 with DynamoDB locking
  backend "s3" {
    bucket         = "acme-terraform-state"
    key            = "databricks/workspace/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-state-lock"
    encrypt        = true
  }
}
```

**`provider.tf`:**

```hcl
provider "databricks" {
  host  = var.databricks_host
  # Authenticate via service principal (recommended for CI)
  client_id     = var.databricks_client_id
  client_secret = var.databricks_client_secret
}

provider "aws" {
  region = var.aws_region
}
```

**`variables.tf`:**

```hcl
variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
}

variable "databricks_client_id" {
  description = "Service principal client ID"
  type        = string
  sensitive   = true
}

variable "databricks_client_secret" {
  description = "Service principal client secret"
  type        = string
  sensitive   = true
}

variable "aws_region" {
  description = "AWS region for supporting infrastructure"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment: dev | staging | prod"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod"
  }
}
```

---

### 4.3 Managing Clusters with Terraform

**`clusters.tf`:**

```hcl
data "databricks_spark_version" "ml_lts" {
  long_term_support = true
  ml                = true
}

data "databricks_node_type" "standard" {
  local_disk = false
  min_memory_gb = 16
  min_cores     = 4
}

resource "databricks_cluster" "shared_autoscaling" {
  cluster_name            = "shared-${var.environment}-autoscaling"
  spark_version           = data.databricks_spark_version.ml_lts.id
  node_type_id            = data.databricks_node_type.standard.id
  autotermination_minutes = 30

  autoscale {
    min_workers = 1
    max_workers = 8
  }

  spark_conf = {
    "spark.databricks.delta.preview.enabled" = "true"
    "spark.sql.shuffle.partitions"           = "auto"
  }

  spark_env_vars = {
    "ENVIRONMENT" = var.environment
  }

  aws_attributes {
    availability           = "SPOT_WITH_FALLBACK"
    zone_id                = "us-east-1a"
    first_on_demand        = 1
    spot_bid_price_percent = 100
  }
}

output "shared_cluster_id" {
  value = databricks_cluster.shared_autoscaling.id
}
```

---

### 4.4 Managing Notebooks and Repos with Terraform

**`repos.tf`:**

```hcl
resource "databricks_git_credential" "github" {
  git_provider          = "gitHub"
  git_username          = var.github_username
  personal_access_token = var.github_pat
}

resource "databricks_repo" "pipelines" {
  url    = "https://github.com/acme-org/databricks-pipelines.git"
  path   = "/Repos/${var.environment}/databricks-pipelines"
  branch = var.environment == "prod" ? "main" : var.environment

  depends_on = [databricks_git_credential.github]
}

# Grant a group access to the Repo folder
resource "databricks_permissions" "repo_usage" {
  repo_id = databricks_repo.pipelines.id

  access_control {
    group_name       = "data-engineers"
    permission_level = "CAN_EDIT"
  }

  access_control {
    group_name       = "data-analysts"
    permission_level = "CAN_READ"
  }
}
```

---

### 4.5 Managing Jobs with Terraform

**`jobs.tf`:**

```hcl
resource "databricks_job" "churn_pipeline" {
  name = "acme-churn-pipeline-${var.environment}"

  schedule {
    quartz_cron_expression = "0 0 4 * * ?"
    timezone_id            = "UTC"
    pause_status           = var.environment == "prod" ? "UNPAUSED" : "PAUSED"
  }

  job_cluster {
    job_cluster_key = "etl_cluster"

    new_cluster {
      spark_version           = data.databricks_spark_version.ml_lts.id
      node_type_id            = data.databricks_node_type.standard.id
      autotermination_minutes = 60
      num_workers             = 4

      aws_attributes {
        availability = "SPOT_WITH_FALLBACK"
        zone_id      = "us-east-1a"
      }
    }
  }

  task {
    task_key = "bronze_ingestion"

    notebook_task {
      notebook_path = "${databricks_repo.pipelines.path}/notebooks/bronze/ingest_customers"
      base_parameters = {
        environment = var.environment
      }
    }

    job_cluster_key = "etl_cluster"
  }

  task {
    task_key = "silver_cleansing"

    depends_on {
      task_key = "bronze_ingestion"
    }

    notebook_task {
      notebook_path = "${databricks_repo.pipelines.path}/notebooks/silver/cleanse_customers"
    }

    job_cluster_key = "etl_cluster"
  }

  task {
    task_key = "feature_engineering"

    depends_on {
      task_key = "silver_cleansing"
    }

    notebook_task {
      notebook_path = "${databricks_repo.pipelines.path}/notebooks/gold/feature_engineering"
    }

    job_cluster_key = "etl_cluster"
  }

  task {
    task_key = "batch_scoring"

    depends_on {
      task_key = "feature_engineering"
    }

    notebook_task {
      notebook_path = "${databricks_repo.pipelines.path}/notebooks/gold/batch_scoring"
    }

    job_cluster_key = "etl_cluster"
  }

  email_notifications {
    on_failure = ["data-platform-alerts@acme.com"]
  }

  tags = {
    environment = var.environment
    team        = "data-engineering"
    managed-by  = "terraform"
  }
}
```

---

### 4.6 Managing Secrets with Terraform

```hcl
# secrets.tf

# Create a secret scope backed by Databricks internal storage
resource "databricks_secret_scope" "pipeline" {
  name = "pipeline-${var.environment}"
}

# Store the Databricks service principal token for use inside notebooks
resource "databricks_secret" "sp_token" {
  key          = "sp-token"
  string_value = var.databricks_sp_token   # passed in via TF_VAR or CI secret
  scope        = databricks_secret_scope.pipeline.name
}

# Store an AWS key for S3 access (prefer IAM instance profiles; use secrets as fallback)
resource "databricks_secret" "aws_access_key" {
  key          = "aws-access-key-id"
  string_value = var.aws_access_key_id
  scope        = databricks_secret_scope.pipeline.name
}

# Grant read access to the secret scope for a group
resource "databricks_secret_acl" "engineers_read" {
  scope      = databricks_secret_scope.pipeline.name
  principal  = "data-engineers"
  permission = "READ"
}
```

**Accessing secrets in a notebook:**

```python
# In a Databricks notebook — dbutils reads secrets at runtime
sp_token = dbutils.secrets.get(scope="pipeline-prod", key="sp-token")
# Never print secret values; use them only in API calls or client configs
```

---

### 4.7 Managing Unity Catalog with Terraform

```hcl
# unity_catalog.tf

resource "databricks_catalog" "telecom_prod" {
  name    = "telecom_prod"
  comment = "Production data for Acme Telecom — managed by Terraform"

  properties = {
    purpose = "production"
    team    = "data-engineering"
  }
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.telecom_prod.name
  name         = "bronze"
  comment      = "Raw ingestion layer"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.telecom_prod.name
  name         = "silver"
  comment      = "Cleansed data layer"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.telecom_prod.name
  name         = "gold"
  comment      = "Feature and aggregation layer"
}

# Grants
resource "databricks_grants" "bronze_usage" {
  schema = "${databricks_catalog.telecom_prod.name}.${databricks_schema.bronze.name}"

  grant {
    principal  = "data-engineers"
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY"]
  }
}

resource "databricks_grants" "gold_read" {
  schema = "${databricks_catalog.telecom_prod.name}.${databricks_schema.gold.name}"

  grant {
    principal  = "data-analysts"
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}
```

---

### 4.8 CI/CD Pipeline with Terraform and GitHub Actions

A complete CI/CD workflow automates testing, Terraform plan and apply, and repo refreshes on every merge to main.

**`.github/workflows/ci.yml` — Pull Request validation:**

```yaml
name: CI — Test and Terraform Plan

on:
  pull_request:
    branches: [main, develop]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r requirements.txt pytest pytest-cov

      - name: Run unit tests
        run: pytest tests/unit/ --cov=src --cov-report=xml

  terraform-plan:
    runs-on: ubuntu-latest
    needs: unit-tests
    steps:
      - uses: actions/checkout@v4

      - name: Set up Terraform
        uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.7.0"

      - name: Terraform Init
        working-directory: infrastructure/terraform
        run: terraform init
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}

      - name: Terraform Validate
        working-directory: infrastructure/terraform
        run: terraform validate

      - name: Terraform Plan
        working-directory: infrastructure/terraform
        run: |
          terraform plan \
            -var="environment=staging" \
            -var="databricks_host=${{ secrets.DATABRICKS_STAGING_HOST }}" \
            -var="databricks_client_id=${{ secrets.DATABRICKS_CLIENT_ID }}" \
            -var="databricks_client_secret=${{ secrets.DATABRICKS_CLIENT_SECRET }}" \
            -out=tfplan.bin
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
```

**`.github/workflows/deploy.yml` — Deploy on merge to main:**

```yaml
name: Deploy — Terraform Apply and Repo Refresh

on:
  push:
    branches: [main]

jobs:
  deploy-staging:
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - uses: actions/checkout@v4

      - name: Set up Terraform
        uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.7.0"

      - name: Terraform Apply (staging)
        working-directory: infrastructure/terraform
        run: |
          terraform init
          terraform apply -auto-approve \
            -var="environment=staging" \
            -var="databricks_host=${{ secrets.DATABRICKS_STAGING_HOST }}" \
            -var="databricks_client_id=${{ secrets.DATABRICKS_CLIENT_ID }}" \
            -var="databricks_client_secret=${{ secrets.DATABRICKS_CLIENT_SECRET }}"
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}

      - name: Refresh Databricks Repo to latest main
        run: |
          # Obtain OAuth token for the service principal
          TOKEN=$(curl -s -X POST \
            "${{ secrets.DATABRICKS_STAGING_HOST }}/oidc/v1/token" \
            -H "Content-Type: application/x-www-form-urlencoded" \
            -u "${{ secrets.DATABRICKS_CLIENT_ID }}:${{ secrets.DATABRICKS_CLIENT_SECRET }}" \
            -d "grant_type=client_credentials&scope=all-apis" \
            | jq -r .access_token)

          # Update the staging Repo to the latest main branch commit
          curl -s -X PATCH \
            "${{ secrets.DATABRICKS_STAGING_HOST }}/api/2.0/repos/${{ secrets.STAGING_REPO_ID }}" \
            -H "Authorization: Bearer $TOKEN" \
            -H "Content-Type: application/json" \
            -d '{"branch": "main"}'

  integration-tests:
    runs-on: ubuntu-latest
    needs: deploy-staging
    steps:
      - uses: actions/checkout@v4

      - name: Install test dependencies
        run: pip install -r requirements.txt pytest

      - name: Run integration tests against staging
        run: pytest tests/integration/ -v
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_STAGING_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_STAGING_PAT }}

  deploy-production:
    runs-on: ubuntu-latest
    needs: integration-tests
    environment: production # requires a manual approval gate in GitHub
    steps:
      - uses: actions/checkout@v4

      - name: Set up Terraform
        uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.7.0"

      - name: Terraform Apply (production)
        working-directory: infrastructure/terraform
        run: |
          terraform init
          terraform apply -auto-approve \
            -var="environment=prod" \
            -var="databricks_host=${{ secrets.DATABRICKS_PROD_HOST }}" \
            -var="databricks_client_id=${{ secrets.DATABRICKS_CLIENT_ID }}" \
            -var="databricks_client_secret=${{ secrets.DATABRICKS_CLIENT_SECRET }}"
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
```

---

### 4.9 Workspace Bootstrap: Full Example

A complete `main.tf` that bootstraps a new Databricks workspace from scratch:

```hcl
# main.tf — Databricks workspace bootstrap

locals {
  tags = {
    environment = var.environment
    team        = "data-platform"
    managed_by  = "terraform"
  }
}

# --- Groups ---
resource "databricks_group" "data_engineers" {
  display_name = "data-engineers"
}

resource "databricks_group" "data_analysts" {
  display_name = "data-analysts"
}

# --- Cluster policy (cost guardrails) ---
resource "databricks_cluster_policy" "engineering" {
  name = "engineering-policy-${var.environment}"

  definition = jsonencode({
    "autotermination_minutes" = {
      "type"  = "range"
      "minValue" = 10
      "maxValue" = 120
      "defaultValue" = 30
    }
    "num_workers" = {
      "type"     = "range"
      "maxValue" = 16
    }
  })
}

# --- Secret scope ---
resource "databricks_secret_scope" "platform" {
  name = "platform-${var.environment}"
}

# --- SQL Warehouse ---
resource "databricks_sql_endpoint" "analytics" {
  name             = "analytics-${var.environment}"
  cluster_size     = "Small"
  max_num_clusters = 3
  auto_stop_mins   = 30

  tags {
    custom_tags {
      key   = "environment"
      value = var.environment
    }
  }
}

# --- Repo ---
resource "databricks_repo" "main_pipelines" {
  url    = var.repo_url
  path   = "/Repos/${var.environment}/main-pipelines"
  branch = var.environment == "prod" ? "main" : var.environment
}

# --- Output ---
output "workspace_url" {
  value = var.databricks_host
}

output "repo_path" {
  value = databricks_repo.main_pipelines.path
}

output "sql_warehouse_id" {
  value = databricks_sql_endpoint.analytics.id
}
```

---

## 5. Summary

This guide covered all aspects of CI/CD practices in Databricks:

| Topic                       | Key Takeaway                                                                                                    |
| --------------------------- | --------------------------------------------------------------------------------------------------------------- |
| **Git Integration / Repos** | Use Databricks Repos to back workspace folders with a remote Git repository; branch per environment             |
| **Repository structure**    | Separate `notebooks/`, `src/`, `tests/`, and `infrastructure/` directories for clean CI/CD                      |
| **REST API**                | Every Databricks action is available via REST 2.0/2.1 — clusters, jobs, repos, secrets, permissions             |
| **Authentication**          | Use Service Principals with OAuth M2M tokens for automation; PATs only for interactive development              |
| **Service Principals**      | Non-human identities for pipelines; grant only `CAN_MANAGE_RUN` — nothing more                                  |
| **AWS Lambda + Databricks** | Retrieve credentials from Secrets Manager → exchange for OAuth token → call `/jobs/run-now`                     |
| **Least-Privilege IAM**     | Lambda role must only have `secretsmanager:GetSecretValue` on the specific secret ARN                           |
| **Terraform**               | Manage clusters, jobs, repos, secrets, Unity Catalog, and permissions as version-controlled Terraform HCL       |
| **Terraform state**         | Store state in S3 with DynamoDB locking; never commit `.tfstate` files to Git                                   |
| **GitHub Actions**          | CI: run unit tests + `terraform plan` on PRs; CD: `terraform apply` + repo refresh + integration tests on merge |
| **Environment promotion**   | Gate production deploys with a GitHub Actions manual approval environment (`environment: production`)           |

**Further Reading:**

- [Databricks REST API Reference](https://docs.databricks.com/api/workspace/introduction)
- [Databricks Terraform Provider](https://registry.terraform.io/providers/databricks/databricks/latest/docs)
- [Databricks CLI Documentation](https://docs.databricks.com/dev-tools/cli/index.html)
- [Databricks Repos Documentation](https://docs.databricks.com/repos/index.html)
- [GitHub Actions for Databricks](https://github.com/databricks/run-notebook)
