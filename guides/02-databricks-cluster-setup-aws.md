# Setting Up & Provisioning a Databricks Cluster on AWS

## Step-by-Step Guide

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [AWS Account Preparation](#2-aws-account-preparation)
   - 2.1 [IAM Roles & Policies](#21-iam-roles--policies)
   - 2.2 [VPC & Networking Setup](#22-vpc--networking-setup)
   - 2.3 [S3 Bucket for Workspace Storage](#23-s3-bucket-for-workspace-storage)
3. [Create a Databricks Account](#3-create-a-databricks-account)
4. [Deploy a Databricks Workspace on AWS](#4-deploy-a-databricks-workspace-on-aws)
   - 4.1 [Workspace Deployment via Databricks Account Console](#41-workspace-deployment-via-databricks-account-console)
   - 4.2 [Custom VPC Deployment (Recommended for Production)](#42-custom-vpc-deployment-recommended-for-production)
5. [Configure Workspace Settings](#5-configure-workspace-settings)
6. [Create Your First Cluster](#6-create-your-first-cluster)
   - 6.1 [Cluster Creation via UI](#61-cluster-creation-via-ui)
   - 6.2 [Cluster Creation via REST API](#62-cluster-creation-via-rest-api)
   - 6.3 [Cluster Creation via Terraform](#63-cluster-creation-via-terraform)
7. [Instance Pools (Optional but Recommended)](#7-instance-pools-optional-but-recommended)
8. [Cluster Policies](#8-cluster-policies)
9. [Connecting to the Cluster](#9-connecting-to-the-cluster)
10. [Cost Optimisation Strategies](#10-cost-optimisation-strategies)
11. [Monitoring & Troubleshooting](#11-monitoring--troubleshooting)
12. [Security Hardening Checklist](#12-security-hardening-checklist)

---

## 1. Prerequisites

Before you begin, ensure you have the following:

### AWS Prerequisites

| Requirement            | Details                                                                                 |
| ---------------------- | --------------------------------------------------------------------------------------- |
| **AWS Account**        | Active AWS account with billing enabled                                                 |
| **IAM permissions**    | Ability to create IAM roles, policies, VPCs, S3 buckets                                 |
| **AWS Region**         | Choose a supported Databricks region (e.g., `us-east-1`, `us-west-2`, `ap-southeast-1`) |
| **EC2 service limits** | Sufficient EC2 vCPU quota for your intended cluster sizes                               |
| **AWS CLI**            | Installed and configured (`aws configure`) — optional but recommended                   |

### Databricks Prerequisites

| Requirement            | Details                                                                    |
| ---------------------- | -------------------------------------------------------------------------- |
| **Databricks account** | Sign up at [databricks.com](https://databricks.com) (free trial available) |
| **Account Admin**      | Account-level admin rights during initial setup                            |
| **Subscription plan**  | Premium plan recommended for production (required for Unity Catalog)       |

### Tools (Optional but Recommended)

```bash
# Verify AWS CLI is installed
aws --version
# aws-cli/2.15.x Python/3.x...

# Install Databricks CLI
pip install databricks-cli
# or
brew install databricks

# Verify Databricks CLI
databricks --version
```

---

## 2. AWS Account Preparation

### 2.1 IAM Roles & Policies

Databricks requires two IAM roles in your AWS account:

#### Role 1: Cross-Account IAM Role (for Databricks Control Plane)

This role allows the Databricks control plane to manage EC2 and VPC resources in your account.

**Step 1 — Create the IAM policy:**

Save the following as `databricks-cross-account-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "NonResourceBasedPermissions",
      "Effect": "Allow",
      "Action": [
        "ec2:CancelSpotInstanceRequests",
        "ec2:DescribeAvailabilityZones",
        "ec2:DescribeIamInstanceProfileAssociations",
        "ec2:DescribeInstanceStatus",
        "ec2:DescribeInstances",
        "ec2:DescribeInternetGateways",
        "ec2:DescribeNatGateways",
        "ec2:DescribeNetworkAcls",
        "ec2:DescribePrefixLists",
        "ec2:DescribeReservedInstancesOfferings",
        "ec2:DescribeRouteTables",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeSpotInstanceRequests",
        "ec2:DescribeSpotPriceHistory",
        "ec2:DescribeSubnets",
        "ec2:DescribeVpcAttribute",
        "ec2:DescribeVpcs",
        "ec2:CreateTags",
        "ec2:DeleteTags",
        "ec2:RequestSpotInstances",
        "ec2:RunInstances",
        "ec2:TerminateInstances",
        "ec2:AttachVolume",
        "ec2:CreateVolume",
        "ec2:DeleteVolume",
        "ec2:DescribeVolumes",
        "ec2:DetachVolume"
      ],
      "Resource": "*"
    },
    {
      "Sid": "AllowPassRoleForInstanceProfile",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::<YOUR_ACCOUNT_ID>:role/<EC2_INSTANCE_PROFILE_ROLE>"
    }
  ]
}
```

```bash
# Create the policy
aws iam create-policy \
  --policy-name DatabricksCrossAccountPolicy \
  --policy-document file://databricks-cross-account-policy.json
```

**Step 2 — Create the cross-account IAM role with trust relationship:**

Save as `databricks-trust-policy.json` (replace `<DATABRICKS_ACCOUNT_ID>` — found in Databricks account console):

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
          "sts:ExternalId": "<YOUR_DATABRICKS_ACCOUNT_ID>"
        }
      }
    }
  ]
}
```

> **Note:** `414351767826` is the Databricks AWS account ID (fixed value — same for all Databricks AWS deployments).

```bash
# Create the cross-account role
aws iam create-role \
  --role-name DatabricksCrossAccountRole \
  --assume-role-policy-document file://databricks-trust-policy.json

# Attach the policy to the role
aws iam attach-role-policy \
  --role-name DatabricksCrossAccountRole \
  --policy-arn arn:aws:iam::<YOUR_ACCOUNT_ID>:policy/DatabricksCrossAccountPolicy
```

#### Role 2: EC2 Instance Profile (for Data Plane Nodes)

This role is attached to each EC2 node and allows it to access S3 and other AWS services.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::<YOUR_DATABRICKS_S3_BUCKET>",
        "arn:aws:s3:::<YOUR_DATABRICKS_S3_BUCKET>/*",
        "arn:aws:s3:::<YOUR_DATA_BUCKET>",
        "arn:aws:s3:::<YOUR_DATA_BUCKET>/*"
      ]
    }
  ]
}
```

```bash
# Create instance profile policy
aws iam create-policy \
  --policy-name DatabricksEC2S3Policy \
  --policy-document file://databricks-ec2-s3-policy.json

# Create the IAM role for EC2 (trust EC2 service)
aws iam create-role \
  --role-name DatabricksEC2InstanceProfileRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "ec2.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach the policy
aws iam attach-role-policy \
  --role-name DatabricksEC2InstanceProfileRole \
  --policy-arn arn:aws:iam::<YOUR_ACCOUNT_ID>:policy/DatabricksEC2S3Policy

# Create instance profile and add the role to it
aws iam create-instance-profile \
  --instance-profile-name DatabricksEC2InstanceProfile

aws iam add-role-to-instance-profile \
  --instance-profile-name DatabricksEC2InstanceProfile \
  --role-name DatabricksEC2InstanceProfileRole
```

---

### 2.2 VPC & Networking Setup

> **For production environments**, always use a custom VPC. Avoid the default Databricks VPC (which uses public subnets).

#### Architecture

```
AWS Region: us-east-1
│
└── VPC: 10.0.0.0/16
    ├── Private Subnet A (10.0.1.0/24) — AZ: us-east-1a  [Cluster nodes]
    ├── Private Subnet B (10.0.2.0/24) — AZ: us-east-1b  [Cluster nodes]
    ├── Public Subnet  C (10.0.3.0/24) — AZ: us-east-1a  [NAT Gateway]
    │
    ├── Internet Gateway  ── Public Subnet C
    ├── NAT Gateway       ── Private Subnets (outbound internet)
    └── VPC Endpoints     ── S3, STS, Kinesis (private traffic)
```

#### Step-by-Step VPC Creation

**Step 1 — Create the VPC:**

```bash
# Create VPC
VPC_ID=$(aws ec2 create-vpc \
  --cidr-block 10.0.0.0/16 \
  --tag-specifications 'ResourceType=vpc,Tags=[{Key=Name,Value=databricks-vpc}]' \
  --query 'Vpc.VpcId' --output text)

echo "VPC ID: $VPC_ID"

# Enable DNS hostnames (required by Databricks)
aws ec2 modify-vpc-attribute --vpc-id $VPC_ID --enable-dns-hostnames
aws ec2 modify-vpc-attribute --vpc-id $VPC_ID --enable-dns-support
```

**Step 2 — Create subnets:**

```bash
# Private Subnet A
PRIVATE_SUBNET_A=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --cidr-block 10.0.1.0/24 \
  --availability-zone us-east-1a \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=databricks-private-a}]' \
  --query 'Subnet.SubnetId' --output text)

# Private Subnet B
PRIVATE_SUBNET_B=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --cidr-block 10.0.2.0/24 \
  --availability-zone us-east-1b \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=databricks-private-b}]' \
  --query 'Subnet.SubnetId' --output text)

# Public Subnet (for NAT Gateway)
PUBLIC_SUBNET=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --cidr-block 10.0.3.0/24 \
  --availability-zone us-east-1a \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=databricks-public}]' \
  --query 'Subnet.SubnetId' --output text)
```

**Step 3 — Internet Gateway & NAT Gateway:**

```bash
# Create and attach Internet Gateway
IGW_ID=$(aws ec2 create-internet-gateway \
  --tag-specifications 'ResourceType=internet-gateway,Tags=[{Key=Name,Value=databricks-igw}]' \
  --query 'InternetGateway.InternetGatewayId' --output text)

aws ec2 attach-internet-gateway --internet-gateway-id $IGW_ID --vpc-id $VPC_ID

# Allocate an Elastic IP for NAT Gateway
EIP_ALLOC=$(aws ec2 allocate-address --domain vpc --query 'AllocationId' --output text)

# Create NAT Gateway in public subnet
NAT_GW_ID=$(aws ec2 create-nat-gateway \
  --subnet-id $PUBLIC_SUBNET \
  --allocation-id $EIP_ALLOC \
  --tag-specifications 'ResourceType=natgateway,Tags=[{Key=Name,Value=databricks-nat}]' \
  --query 'NatGateway.NatGatewayId' --output text)

echo "Waiting for NAT Gateway to be available..."
aws ec2 wait nat-gateway-available --nat-gateway-ids $NAT_GW_ID
```

**Step 4 — Route Tables:**

```bash
# Public route table: route all traffic through IGW
PUBLIC_RT=$(aws ec2 create-route-table --vpc-id $VPC_ID \
  --query 'RouteTable.RouteTableId' --output text)
aws ec2 create-route --route-table-id $PUBLIC_RT \
  --destination-cidr-block 0.0.0.0/0 --gateway-id $IGW_ID
aws ec2 associate-route-table --route-table-id $PUBLIC_RT --subnet-id $PUBLIC_SUBNET

# Private route table: route all traffic through NAT Gateway
PRIVATE_RT=$(aws ec2 create-route-table --vpc-id $VPC_ID \
  --query 'RouteTable.RouteTableId' --output text)
aws ec2 create-route --route-table-id $PRIVATE_RT \
  --destination-cidr-block 0.0.0.0/0 --nat-gateway-id $NAT_GW_ID
aws ec2 associate-route-table --route-table-id $PRIVATE_RT --subnet-id $PRIVATE_SUBNET_A
aws ec2 associate-route-table --route-table-id $PRIVATE_RT --subnet-id $PRIVATE_SUBNET_B
```

**Step 5 — Security Groups:**

Databricks requires two security groups — one for the cluster nodes and one for the workspace.

```bash
# Cluster security group
CLUSTER_SG=$(aws ec2 create-security-group \
  --group-name databricks-cluster-sg \
  --description "Databricks cluster nodes security group" \
  --vpc-id $VPC_ID \
  --query 'GroupId' --output text)

# Allow all traffic within the security group (required for Spark inter-node)
aws ec2 authorize-security-group-ingress \
  --group-id $CLUSTER_SG \
  --protocol all \
  --source-group $CLUSTER_SG

# Allow all outbound traffic (nodes need internet for packages)
aws ec2 authorize-security-group-egress \
  --group-id $CLUSTER_SG \
  --protocol all \
  --cidr 0.0.0.0/0
```

**Step 6 — VPC Endpoints (recommended for private traffic):**

```bash
# S3 Gateway Endpoint — free, keeps S3 traffic private
aws ec2 create-vpc-endpoint \
  --vpc-id $VPC_ID \
  --service-name com.amazonaws.us-east-1.s3 \
  --route-table-ids $PRIVATE_RT

# STS Interface Endpoint
aws ec2 create-vpc-endpoint \
  --vpc-id $VPC_ID \
  --service-name com.amazonaws.us-east-1.sts \
  --vpc-endpoint-type Interface \
  --subnet-ids $PRIVATE_SUBNET_A $PRIVATE_SUBNET_B \
  --security-group-ids $CLUSTER_SG
```

---

### 2.3 S3 Bucket for Workspace Storage

Databricks uses an S3 bucket to store notebooks, cluster logs, job outputs, and workspace data.

```bash
# Create the root S3 bucket (replace with a globally unique name)
BUCKET_NAME="databricks-workspace-<YOUR_ACCOUNT_ID>-us-east-1"

aws s3api create-bucket \
  --bucket $BUCKET_NAME \
  --region us-east-1

# Block all public access (critical for security)
aws s3api put-public-access-block \
  --bucket $BUCKET_NAME \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Enable versioning (recommended)
aws s3api put-bucket-versioning \
  --bucket $BUCKET_NAME \
  --versioning-configuration Status=Enabled

# Enable server-side encryption
aws s3api put-bucket-encryption \
  --bucket $BUCKET_NAME \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "aws:kms"
      }
    }]
  }'
```

---

## 3. Create a Databricks Account

1. Navigate to **[https://accounts.cloud.databricks.com](https://accounts.cloud.databricks.com)** (AWS).
2. Click **"Try Databricks"** or log in with an existing account.
3. Select **Amazon Web Services** as the cloud provider.
4. Complete the registration form and verify your email.
5. After logging in, you land on the **Databricks Account Console**.

> **Account Console vs Workspace:** The Account Console manages billing, workspaces, and Unity Catalog metastores. Each individual workspace is accessed via a separate URL.

#### Note Your Account ID

In the Account Console → bottom-left corner → click your profile → copy the **Account ID** (UUID format). You will need this when configuring the cross-account IAM trust policy.

---

## 4. Deploy a Databricks Workspace on AWS

### 4.1 Workspace Deployment via Databricks Account Console

**Step 1 — Open the Account Console**

Log in at `https://accounts.cloud.databricks.com` → click **"Workspaces"** → **"Create workspace"**.

**Step 2 — Configure workspace basics**

| Field                   | Value                                              |
| ----------------------- | -------------------------------------------------- |
| Workspace name          | `my-databricks-workspace` (alphanumeric + hyphens) |
| AWS Region              | `us-east-1` (or your target region)                |
| AWS account credentials | Cross-account IAM Role ARN created in Step 2.1     |

**Step 3 — Storage configuration**

| Field           | Value                                              |
| --------------- | -------------------------------------------------- |
| S3 root bucket  | `s3://databricks-workspace-<account-id>-us-east-1` |
| Default catalog | `hive_metastore` (or Unity Catalog if configured)  |

**Step 4 — Network configuration (choose one)**

- **Default (Databricks-managed VPC):** Easiest, Databricks creates a VPC in your account. Suitable for development.
- **Customer-managed VPC:** Select the VPC, private subnets, and security group you created above. Required for production.

**Step 5 — Review and create**

Click **"Create"**. Workspace provisioning takes approximately **5–15 minutes**.

**Step 6 — Access your workspace**

Once provisioned, click **"Open"** in the Account Console. The workspace URL follows the pattern:

```
https://<workspace-id>.cloud.databricks.com
```

---

### 4.2 Custom VPC Deployment (Recommended for Production)

When you select **"Customer-managed VPC"**, provide:

| Field                | Value                                              |
| -------------------- | -------------------------------------------------- |
| VPC ID               | `vpc-xxxxxxxxxxxxxxxxx`                            |
| Subnet IDs           | `subnet-aaa` (private A), `subnet-bbb` (private B) |
| Security Group ID    | `sg-xxxxxxxxxxxxxxxx` (cluster SG)                 |
| Backend private link | Optional — for fully private connectivity          |

#### Subnet Sizing Guidelines

Databricks allocates **2 private IP addresses per EC2 node** (one for the node, one for the Spark container). Each subnet must have enough free IP addresses for your maximum expected cluster size.

```
Required IPs = (max workers + 1 driver) × 2

Example: 10 workers + 1 driver = 11 nodes × 2 = 22 IPs needed
/24 subnet (254 usable IPs) handles ~127 nodes — recommended minimum
```

---

## 5. Configure Workspace Settings

Once your workspace is running, perform these initial configurations before creating clusters.

### 5.1 Add Instance Profile (EC2 → S3 access)

1. In the workspace UI, go to **Settings** → **Identity and access** → **Instance profiles**.
2. Click **"Add instance profile"**.
3. Enter the ARN of the instance profile you created:
   ```
   arn:aws:iam::<YOUR_ACCOUNT_ID>:instance-profile/DatabricksEC2InstanceProfile
   ```
4. Check **"Skip validation"** only if IAM propagation hasn't completed yet, otherwise allow it to validate.
5. Click **"Add"**.

### 5.2 Workspace Admins

1. Go to **Settings** → **Identity and access** → **Admins**.
2. Add additional workspace admins as needed.
3. Assign users to appropriate groups (admins, data engineers, data scientists, analysts).

### 5.3 IP Access Lists (Production Security)

Restrict workspace access to known IP ranges:

1. Go to **Settings** → **Security** → **IP access list**.
2. Click **"Enable IP access list"**.
3. Add your corporate IP ranges (CIDR format):
   ```
   203.0.113.0/24      # Corporate office
   10.0.0.0/8          # Internal VPN
   ```

### 5.4 Personal Access Tokens

Generate a token for CLI/API access:

1. Click your username (top-right) → **User settings** → **Developer** → **Access tokens**.
2. Click **"Generate new token"**.
3. Set a description and expiry (90 days recommended for production).
4. **Copy the token immediately** — it will not be shown again.
5. Store it securely (e.g., in AWS Secrets Manager).

Configure the Databricks CLI:

```bash
databricks configure --token
# Databricks Host: https://<workspace-id>.cloud.databricks.com
# Token: <paste your token>

# Verify connection
databricks workspace list /
```

---

## 6. Create Your First Cluster

### 6.1 Cluster Creation via UI

**Step 1 — Navigate to Compute**

In the left sidebar, click **"Compute"** → **"Create compute"**.

**Step 2 — Configure cluster basics**

| Setting                | Recommended Value                                 |
| ---------------------- | ------------------------------------------------- |
| **Cluster name**       | `dev-general-cluster`                             |
| **Policy**             | Unrestricted (dev) or select a Policy (prod)      |
| **Cluster mode**       | Single node (solo dev) / Standard (team)          |
| **Databricks Runtime** | `14.3 LTS` (Long Term Support — stable)           |
| **Use Photon**         | ✅ Enabled (for SQL-heavy workloads)              |
| **Node type**          | `i3.xlarge` (4 vCPU, 30.5 GB RAM) for general use |

**Step 3 — Configure auto-scaling**

| Setting                    | Value                                           |
| -------------------------- | ----------------------------------------------- |
| **Enable autoscaling**     | ✅ Enabled                                      |
| **Min workers**            | `2`                                             |
| **Max workers**            | `8`                                             |
| **On-demand / Spot split** | 1 on-demand driver + Spot workers (cost saving) |

> **Tip:** Always keep the driver node on On-Demand to avoid spot interruption killing your session.

**Step 4 — Advanced configuration**

Click **"Advanced options"** and configure:

**Auto-termination:**

```
Terminate after: 60 minutes of inactivity
```

**Instance profile:**

```
Select: DatabricksEC2InstanceProfile
```

**Spark configuration** (optional tuning):

```
spark.sql.adaptive.enabled true
spark.databricks.delta.optimizeWrite.enabled true
spark.databricks.delta.autoCompact.enabled true
```

**Environment variables** (for secrets):

```
# Do NOT hardcode secrets here — use dbutils.secrets instead
```

**Logging:**

```
Destination: S3
S3 location: s3://databricks-workspace-<account-id>-us-east-1/cluster-logs
```

**Step 5 — Create the cluster**

Click **"Create compute"**. The cluster transitions through:

```
PENDING → RUNNING
```

Initial startup takes **3–7 minutes** (EC2 provisioning + Spark initialisation). With an Instance Pool, this reduces to **30–60 seconds**.

---

### 6.2 Cluster Creation via REST API

After setting up your personal access token, you can create clusters programmatically.

```bash
# Set your workspace variables
WORKSPACE_URL="https://<workspace-id>.cloud.databricks.com"
TOKEN="<your-personal-access-token>"

# Create cluster
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "$WORKSPACE_URL/api/2.0/clusters/create" \
  -d '{
    "cluster_name": "dev-general-cluster",
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2,
    "autoscale": {
      "min_workers": 2,
      "max_workers": 8
    },
    "aws_attributes": {
      "instance_profile_arn": "arn:aws:iam::<ACCOUNT_ID>:instance-profile/DatabricksEC2InstanceProfile",
      "availability": "SPOT_WITH_FALLBACK",
      "first_on_demand": 1,
      "spot_bid_price_percent": 100
    },
    "spark_conf": {
      "spark.sql.adaptive.enabled": "true",
      "spark.databricks.delta.optimizeWrite.enabled": "true"
    },
    "autotermination_minutes": 60,
    "enable_elastic_disk": true,
    "runtime_engine": "PHOTON"
  }'
```

**Response:**

```json
{
  "cluster_id": "1234-567890-abc12345"
}
```

**Check cluster state:**

```bash
# Get cluster status
curl -X GET \
  -H "Authorization: Bearer $TOKEN" \
  "$WORKSPACE_URL/api/2.0/clusters/get?cluster_id=1234-567890-abc12345"
```

**Using Databricks CLI:**

```bash
# List available Spark versions
databricks clusters spark-versions

# List available node types
databricks clusters list-node-types | jq '.node_types[] | {node_type_id, num_cores, memory_mb}'

# Create cluster from JSON file
databricks clusters create --json-file cluster-config.json
```

---

### 6.3 Cluster Creation via Terraform

For infrastructure-as-code deployments:

**`main.tf`:**

```hcl
terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }
}

provider "databricks" {
  host  = var.workspace_url
  token = var.databricks_token
}

# Fetch the latest LTS runtime
data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

# Fetch the smallest available node type for dev
data "databricks_node_type" "general" {
  local_disk  = true
  min_cores   = 4
  gb_per_core = 8
}

# Create the cluster
resource "databricks_cluster" "dev_cluster" {
  cluster_name            = "dev-general-cluster"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = data.databricks_node_type.general.id
  autotermination_minutes = 60

  autoscale {
    min_workers = 2
    max_workers = 8
  }

  aws_attributes {
    instance_profile_arn  = var.instance_profile_arn
    availability          = "SPOT_WITH_FALLBACK"
    first_on_demand       = 1
    spot_bid_price_percent = 100
  }

  spark_conf = {
    "spark.sql.adaptive.enabled"                     = "true"
    "spark.databricks.delta.optimizeWrite.enabled"   = "true"
    "spark.databricks.delta.autoCompact.enabled"     = "true"
  }

  library {
    pypi {
      package = "pandas==2.1.0"
    }
  }

  tags = {
    Environment = "development"
    Team        = "data-engineering"
    CostCenter  = "DE-001"
  }
}
```

**`variables.tf`:**

```hcl
variable "workspace_url" {
  description = "Databricks workspace URL"
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token"
  type        = string
  sensitive   = true
}

variable "instance_profile_arn" {
  description = "EC2 instance profile ARN for S3 access"
  type        = string
}
```

**`terraform.tfvars`** (do not commit to git):

```hcl
workspace_url        = "https://<workspace-id>.cloud.databricks.com"
databricks_token     = "dapi..."
instance_profile_arn = "arn:aws:iam::123456789:instance-profile/DatabricksEC2InstanceProfile"
```

**Deploy:**

```bash
terraform init
terraform plan
terraform apply
```

---

## 7. Instance Pools (Optional but Recommended)

Instance Pools maintain a set of pre-warmed EC2 instances, drastically reducing cluster startup time.

### 7.1 Create an Instance Pool via UI

1. In the left sidebar, click **"Compute"** → **"Pools"** → **"Create pool"**.
2. Configure:

| Setting                            | Value                   |
| ---------------------------------- | ----------------------- |
| **Pool name**                      | `general-pool-i3xlarge` |
| **Instance type**                  | `i3.xlarge`             |
| **Min idle instances**             | `2` (always warm)       |
| **Max capacity**                   | `20`                    |
| **Idle instance auto-termination** | `60 minutes`            |

### 7.2 Create a Pool via API

```bash
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "$WORKSPACE_URL/api/2.0/instance-pools/create" \
  -d '{
    "instance_pool_name": "general-pool-i3xlarge",
    "node_type_id": "i3.xlarge",
    "min_idle_instances": 2,
    "max_capacity": 20,
    "idle_instance_autotermination_minutes": 60,
    "aws_attributes": {
      "availability": "SPOT_WITH_FALLBACK",
      "spot_bid_price_percent": 100
    },
    "preloaded_spark_versions": ["14.3.x-scala2.12"]
  }'
```

### 7.3 Attach a Cluster to a Pool

When creating a cluster, set the `instance_pool_id`:

```json
{
  "cluster_name": "pool-backed-cluster",
  "spark_version": "14.3.x-scala2.12",
  "instance_pool_id": "<pool-id>",
  "num_workers": 4,
  "autotermination_minutes": 30
}
```

---

## 8. Cluster Policies

Cluster Policies allow workspace admins to define allowed configurations, enforce cost controls, and simplify the cluster creation form for users.

### 8.1 Create a Cluster Policy

Go to **Compute** → **Policies** → **Create policy**.

Example policy JSON (restricts max workers and enforces auto-termination):

```json
{
  "node_type_id": {
    "type": "allowlist",
    "values": ["i3.xlarge", "i3.2xlarge", "m5.xlarge"],
    "defaultValue": "i3.xlarge"
  },
  "autoscale.max_workers": {
    "type": "range",
    "maxValue": 10,
    "defaultValue": 4
  },
  "autotermination_minutes": {
    "type": "fixed",
    "value": 60,
    "hidden": false
  },
  "spark_version": {
    "type": "regex",
    "pattern": "14\\.[0-9]+\\.x-scala2\\.12",
    "defaultValue": "14.3.x-scala2.12"
  },
  "aws_attributes.instance_profile_arn": {
    "type": "fixed",
    "value": "arn:aws:iam::<ACCOUNT_ID>:instance-profile/DatabricksEC2InstanceProfile",
    "hidden": true
  }
}
```

### 8.2 Assign Policy to Users

1. In the policy, click **"Permissions"**.
2. Add groups (e.g., `data-engineers`) with **"Can use"** permission.
3. Users in the group see only the simplified, policy-constrained creation form.

---

## 9. Connecting to the Cluster

### 9.1 Attach a Notebook

1. Open or create a notebook.
2. At the top of the notebook, click the cluster dropdown.
3. Select your running cluster.
4. The notebook status shows **"Connected"** with a green dot.

### 9.2 Connect via JDBC/ODBC (BI Tools)

Get connection details:

1. Go to **Compute** → select your cluster → **"Advanced options"** → **"JDBC/ODBC"** tab.
2. Note the **Server Hostname**, **Port**, and **HTTP Path**.

**JDBC URL format:**

```
jdbc:spark://<server-hostname>:443/default;transportMode=http;
ssl=1;httpPath=<http-path>;AuthMech=3;UID=token;PWD=<personal-access-token>
```

**Python connection (PySpark via JDBC):**

```python
df = spark.read.format("jdbc") \
    .option("url", "jdbc:spark://<hostname>:443/default") \
    .option("httpPath", "<http-path>") \
    .option("user", "token") \
    .option("password", "<token>") \
    .option("dbtable", "my_schema.my_table") \
    .load()
```

### 9.3 Connect via Databricks Connect (Local IDE)

Run Spark code locally (in VS Code / PyCharm) against a remote Databricks cluster:

```bash
# Install
pip install databricks-connect==14.3.*

# Configure
databricks-connect configure
# Host: https://<workspace-id>.cloud.databricks.com
# Token: <your-token>
# Cluster ID: <cluster-id>
# Port: 15001
```

```python
# Test the connection
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()
df = spark.range(10)
df.show()
```

---

## 10. Cost Optimisation Strategies

| Strategy             | Description                                                    | Estimated Saving          |
| -------------------- | -------------------------------------------------------------- | ------------------------- |
| **Spot instances**   | Use AWS Spot for worker nodes (`SPOT_WITH_FALLBACK`)           | 60–80% on workers         |
| **Auto-termination** | Set idle timeout (60 min for dev, 30 min for job clusters)     | Prevents zombie clusters  |
| **Autoscaling**      | Let Spark scale workers based on workload                      | 20–40% vs fixed size      |
| **Instance Pools**   | Pre-warm instances to avoid waste from slow starts             | Reduces dev friction      |
| **Job clusters**     | Use ephemeral clusters for production jobs (not all-purpose)   | Eliminates idle cost      |
| **Cluster policies** | Enforce max worker limits per team                             | Governance on spend       |
| **SQL Serverless**   | Pay-per-second for SQL workloads — no idle cost                | High saving for bursty BI |
| **Photon**           | Fewer nodes needed for same throughput                         | 2–4× query speedup        |
| **Right-sizing**     | Profile jobs with Spark UI, downsize over-provisioned clusters | 10–30% saving             |
| **Tagging**          | Tag clusters with team/project, use AWS Cost Explorer          | Better cost visibility    |

### Spot Instance Configuration

```json
"aws_attributes": {
  "availability": "SPOT_WITH_FALLBACK",
  "first_on_demand": 1,
  "spot_bid_price_percent": 100
}
```

- `SPOT_WITH_FALLBACK`: Use Spot, fall back to On-Demand if Spot unavailable.
- `first_on_demand: 1`: Keep the driver node as On-Demand for stability.
- `spot_bid_price_percent: 100`: Bid up to On-Demand price (recommended — avoids frequent interruptions).

---

## 11. Monitoring & Troubleshooting

### 11.1 Spark UI

Each running cluster exposes a Spark UI for real-time job monitoring:

1. In **Compute** → select cluster → click **"Spark UI"**.
2. Key tabs:
   - **Jobs** — see all running/completed Spark jobs
   - **Stages** — drill into task-level execution
   - **Executors** — memory/CPU usage per worker node
   - **Storage** — cached RDDs/DataFrames
   - **SQL** — query execution plans

### 11.2 Cluster Event Log

**Compute** → select cluster → **"Event log"** tab shows:

- Cluster state transitions (PENDING → RUNNING → TERMINATING)
- Autoscaling events (nodes added/removed)
- Error messages if cluster fails to start

### 11.3 Driver Logs

**Compute** → select cluster → **"Driver logs"** tab:

- `stdout`: Print statements from your code
- `stderr`: Error stack traces, Spark warnings
- `log4j-active.log`: Full Spark internal logs

### 11.4 Common Issues & Fixes

| Issue                                | Likely Cause                                             | Fix                                                              |
| ------------------------------------ | -------------------------------------------------------- | ---------------------------------------------------------------- |
| Cluster stuck in PENDING             | EC2 capacity shortage, wrong subnet                      | Check event log; try different AZ or instance type               |
| `403 Forbidden` on S3                | Instance profile not attached or missing IAM permissions | Verify instance profile ARN in cluster config                    |
| OutOfMemoryError                     | Too little memory per executor                           | Increase node size, reduce partition count, enable disk spilling |
| Spot interruption                    | AWS reclaimed Spot instances                             | Use `SPOT_WITH_FALLBACK`; checkpointing for long jobs            |
| Slow cluster startup                 | No instance pool                                         | Create an instance pool for dev clusters                         |
| `AnalysisException: Table not found` | Wrong catalog/schema context                             | Check Unity Catalog namespace; run `SHOW TABLES`                 |
| Workers not joining                  | Security group misconfiguration                          | Ensure intra-SG traffic is allowed on all ports                  |

### 11.5 CloudWatch Metrics

AWS CloudWatch automatically collects EC2 metrics for cluster nodes. Useful metrics:

```bash
# View CPU utilisation for a cluster instance
aws cloudwatch get-metric-statistics \
  --namespace AWS/EC2 \
  --metric-name CPUUtilization \
  --dimensions Name=InstanceId,Value=<instance-id> \
  --start-time 2026-03-18T00:00:00Z \
  --end-time 2026-03-18T23:59:59Z \
  --period 300 \
  --statistics Average
```

---

## 12. Security Hardening Checklist

Use this checklist before moving any workspace to production:

### Network Security

- [ ] Use custom VPC (not Databricks-managed)
- [ ] Cluster nodes in **private subnets only** — no public IP
- [ ] NAT Gateway or VPC PrivateLink for outbound traffic
- [ ] VPC Endpoints for S3 and STS (avoid public internet for data)
- [ ] Security group: intra-cluster traffic only; no inbound from 0.0.0.0/0
- [ ] IP Access List enabled on the workspace

### IAM & Identity

- [ ] Cross-account role uses ExternalId condition in trust policy
- [ ] EC2 instance profile uses least-privilege S3 access
- [ ] No hardcoded AWS credentials anywhere in notebooks or config
- [ ] Personal Access Tokens rotated regularly (set expiry)
- [ ] Single Sign-On (SSO) configured via SAML/OIDC (Azure AD, Okta, etc.)

### Data Security

- [ ] S3 bucket with public access blocked
- [ ] S3 bucket encrypted with KMS (customer-managed key preferred)
- [ ] EBS volumes encrypted (enable `enable_elastic_disk` + disk encryption)
- [ ] Cluster logs written to encrypted S3 location
- [ ] Unity Catalog enabled for column/row-level security

### Secrets Management

- [ ] All credentials stored in Databricks Secret Scopes
- [ ] Secret Scopes backed by AWS Secrets Manager

```python
# CORRECT — use secret scopes
password = dbutils.secrets.get(scope="aws-scope", key="db-password")

# WRONG — never hardcode credentials
password = "my-secret-password"  # ❌ Never do this
```

### Governance

- [ ] Cluster policies defined and assigned — no unrestricted clusters in prod
- [ ] Auto-termination enforced on all clusters
- [ ] Resource tagging enforced (team, project, environment)
- [ ] Audit logs enabled (Unity Catalog audit, AWS CloudTrail)
- [ ] Unity Catalog RBAC configured for data access control

---

## Quick Reference — Cluster Configuration Summary

```
┌────────────────────────────────────────────────────────────────────┐
│              CLUSTER CONFIGURATION QUICK REFERENCE                 │
├─────────────────────┬──────────────────────┬───────────────────────┤
│ Use Case            │ Recommended Config   │ Notes                 │
├─────────────────────┼──────────────────────┼───────────────────────┤
│ Dev / Exploration   │ Single Node or       │ Auto-terminate 60 min │
│                     │ 2–4 workers          │ DBR LTS, Spot OK      │
├─────────────────────┼──────────────────────┼───────────────────────┤
│ ETL / Data Eng.     │ Job Cluster          │ Ephemeral per job run │
│                     │ 4–16 workers         │ Spot + On-Demand mix  │
├─────────────────────┼──────────────────────┼───────────────────────┤
│ ML Training         │ DBR ML Runtime       │ GPU nodes for DL      │
│                     │ 4–32 workers         │ p3.2xlarge / g4dn     │
├─────────────────────┼──────────────────────┼───────────────────────┤
│ SQL Analytics / BI  │ SQL Warehouse        │ Serverless for bursty │
│                     │ (Photon enabled)     │ Pro for federation    │
├─────────────────────┼──────────────────────┼───────────────────────┤
│ Streaming           │ All-Purpose Cluster  │ Continuous mode; DLT  │
│                     │ 2–8 workers          │ for declarative ETL   │
└─────────────────────┴──────────────────────┴───────────────────────┘
```

---

_Last updated: March 2026 | Databricks Runtime: 14.3 LTS | AWS Provider: us-east-1_
