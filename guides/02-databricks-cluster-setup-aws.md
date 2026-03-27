# Setting Up & Provisioning a Databricks Cluster on AWS

## Step-by-Step Guide

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [AWS Account Preparation — Manual](#2-aws-account-preparation--manual)
   - 2.1 [IAM Roles & Policies](#21-iam-roles--policies)
   - 2.2 [VPC & Networking Setup](#22-vpc--networking-setup)
   - 2.3 [S3 Bucket for Workspace Storage](#23-s3-bucket-for-workspace-storage)
3. [Create / Link Your Databricks Account](#3-create--link-your-databricks-account)
   - 3.1 [Direct Sign-Up](#31-direct-sign-up)
   - 3.2 [Launch from AWS Marketplace (Recommended)](#32-launch-from-aws-marketplace-recommended)
4. [Deploy a Databricks Workspace on AWS](#4-deploy-a-databricks-workspace-on-aws)
   - 4.1 [Fully Automated via AWS Marketplace (Recommended)](#41-fully-automated-via-aws-marketplace-recommended)
   - 4.2 [Databricks-Managed VPC, Manual AWS Prep](#42-databricks-managed-vpc-manual-aws-prep)
   - 4.3 [Customer-Managed VPC via Account Console](#43-customer-managed-vpc-via-account-console)
   - 4.4 [Databricks-Managed Provisioning via Account API](#44-databricks-managed-provisioning-via-account-api)
   - 4.5 [Full-Stack Terraform (AWS + Databricks Providers)](#45-full-stack-terraform-aws--databricks-providers)
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

## Deployment Model Overview

This guide covers **three provisioning models** for deploying Databricks on AWS, differing in how much AWS infrastructure you manage yourself.

|                               | **Model A — Fully Automated** ⭐ Recommended             | **Model B — Databricks-Managed VPC**                 | **Model C — Customer-Managed**                       |
| ----------------------------- | -------------------------------------------------------- | ---------------------------------------------------- | ---------------------------------------------------- |
| **Who creates IAM roles**     | Databricks (CloudFormation, one-click)                   | You (AWS CLI / Console — Section 2.1)                | You (AWS CLI / Console — Section 2.1)                |
| **Who creates S3 bucket**     | Databricks (auto-created)                                | You (AWS CLI / Console — Section 2.3)                | You (AWS CLI / Console — Section 2.3)                |
| **Who creates VPC**           | Databricks (auto-provisioned)                            | Databricks (auto-provisioned)                        | You (AWS CLI / Console — Section 2.2)                |
| **AWS account prep required** | ❌ None — just an AWS account linked via Marketplace     | ✅ Sections 2.1 + 2.3                                | ✅ Sections 2.1 + 2.2 + 2.3                          |
| **Network control**           | Standard Databricks VPC                                  | Standard Databricks VPC                              | Full — custom CIDRs, peering, PrivateLink            |
| **Setup effort**              | Minimal — subscribe on Marketplace, click through wizard | Medium — create IAM + S3, then workspace UI          | High — create IAM + VPC + S3, then workspace UI      |
| **VPC peering / PrivateLink** | Requires migrating to customer-managed VPC later         | Requires migrating to customer-managed VPC later     | Configurable from the start                          |
| **Best suited for**           | Quick-start, learning, dev/test, standard production     | Production when you already manage IAM/S3 separately | Production with strict network segmentation policies |
| **Provisioning method**       | **Section 4.1 (UI) ← PRIMARY**                           | Section 4.2 (UI), 4.4 (API), or 4.5 (Terraform)      | Section 4.3 (UI) or 4.5 (Terraform)                  |

### Step Comparison

```
MODEL A — Fully Automated (AWS Marketplace)    ⭐ RECOMMENDED
────────────────────────────────────────────────────────────────────────
  STEP                    PERFORMED BY                  GUIDE SECTION
  ──────────────────────────────────────────────────────────────────────
  Subscribe to Databricks  You (AWS Marketplace)        Section 3.2
  Link AWS account         Databricks (Marketplace SSO) Section 3.2  ← automatic
  Cross-account IAM role   Databricks (CloudFormation)  Section 4.1  ← automatic
  EC2 Instance Profile     Databricks (CloudFormation)  Section 4.1  ← automatic
  S3 Root Bucket           Databricks (auto-created)    Section 4.1  ← automatic
  VPC + Networking         Databricks (automatic)       Section 4.1  ← automatic
  Workspace                You (Account Console UI)     Section 4.1
  Cluster                  You (Databricks UI/API)      Section 6

MODEL B — Databricks-Managed VPC, Manual AWS Prep
────────────────────────────────────────────────────────────────────────
  STEP                    PERFORMED BY                  GUIDE SECTION
  ──────────────────────────────────────────────────────────────────────
  Cross-account IAM role  You (AWS CLI / Console)       Section 2.1
  EC2 Instance Profile    You (AWS CLI / Console)       Section 2.1
  S3 Root Bucket          You (AWS CLI / Console)       Section 2.3
  VPC + Networking        Databricks (automatic)        ← auto, nothing to do
  Workspace (UI)          You (Account Console UI)      Section 4.2
  Workspace (API)         You (Account API)             Section 4.4  (automation)
  Workspace (IaC)         You (Terraform)               Section 4.5  (IaC)
  Cluster                 You (Databricks UI/API)       Section 6

MODEL C — Customer-Managed Infrastructure
────────────────────────────────────────────────────────────────────────
  STEP                    PERFORMED BY                  GUIDE SECTION
  ──────────────────────────────────────────────────────────────────────
  Cross-account IAM role  You (AWS CLI / Console)       Section 2.1
  EC2 Instance Profile    You (AWS CLI / Console)       Section 2.1
  VPC + Subnets           You (AWS CLI / Console)       Section 2.2  ← Model C only
  NAT GW + Routes         You (AWS CLI / Console)       Section 2.2  ← Model C only
  Security Groups         You (AWS CLI / Console)       Section 2.2  ← Model C only
  S3 Root Bucket          You (AWS CLI / Console)       Section 2.3
  Workspace (UI)          You (Account Console UI)      Section 4.3
  Workspace (IaC)         You (Terraform)               Section 4.5  (IaC)
  Cluster                 You (Databricks UI/API)       Section 6
```

> **Choosing a model:** For most users, **start with Model A (Section 4.1)** — subscribe on AWS Marketplace, and Databricks automatically provisions IAM roles, S3 storage, VPC, and the workspace with minimal manual steps. Use Model B when you already manage IAM/S3 separately in your organisation. Use Model C only when you need VPC peering, AWS PrivateLink, custom CIDR ranges, or must comply with strict network segmentation policies.

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

## 2. AWS Account Preparation — Manual

> **This section is required for Models B and C only.** If you are using the fully automated AWS Marketplace path (Model A — Section 4.1), Databricks provisions the cross-account IAM role, EC2 instance profile, S3 bucket, and VPC automatically via CloudFormation — skip directly to [Section 3.2](#32-launch-from-aws-marketplace-recommended).

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

## 3. Create / Link Your Databricks Account

### 3.1 Direct Sign-Up

If you are not using AWS Marketplace, create a Databricks account directly:

1. Navigate to **[https://accounts.cloud.databricks.com](https://accounts.cloud.databricks.com)** (AWS).
2. Click **"Try Databricks"** or log in with an existing account.
3. Select **Amazon Web Services** as the cloud provider.
4. Complete the registration form and verify your email.
5. After logging in, you land on the **Databricks Account Console**.

> **Account Console vs Workspace:** The Account Console manages billing, workspaces, and Unity Catalog metastores. Each individual workspace is accessed via a separate URL.

#### Note Your Account ID

In the Account Console → bottom-left corner → click your profile → copy the **Account ID** (UUID format). You will need this when configuring the cross-account IAM trust policy (Model B/C).

---

### 3.2 Launch from AWS Marketplace (Recommended)

Launching Databricks from **AWS Marketplace** is the prerequisite for the **fully automated Model A flow (Section 4.1)**. It establishes a trusted billing and identity link between your AWS account and Databricks, enabling Databricks to provision AWS resources on your behalf via CloudFormation.

> Models B and C work with either a Marketplace-linked account or a directly signed-up account. The Marketplace path is not mandatory for those models.

#### Step-by-Step: Subscribe and Link AWS with Databricks

**Step 1 — Find Databricks on AWS Marketplace**

1. Sign in to the [AWS Management Console](https://console.aws.amazon.com) with your AWS account (you need permissions to subscribe to Marketplace products and manage IAM).
2. Navigate to **AWS Marketplace** → search for **"Databricks Data Intelligence Platform"**.
3. Select the listing published by **Databricks, Inc.**

**Step 2 — Subscribe**

1. Click **"View purchase options"** → **"Subscribe"**.
2. Accept the terms and conditions, then click **"Subscribe"** to confirm.
3. Once the subscription is active, click **"Set Up Your Account"** (the button that appears after a successful subscription).

> You are redirected to `accounts.cloud.databricks.com`. The AWS account that performed the Marketplace subscription is now contractually and technically linked to your Databricks account.

**Step 3 — Create or log in to your Databricks account**

On the Databricks sign-up page (redirected from Marketplace):

| Scenario                    | Action                                                                                   |
| --------------------------- | ---------------------------------------------------------------------------------------- |
| New to Databricks           | Fill in the registration form, verify your email, and land on the Account Console        |
| Existing Databricks account | Log in — the Marketplace subscription is automatically attached to your existing account |

**Step 4 — Confirm the AWS account link**

In the Databricks **Account Console**:

1. Click **"Cloud Resources"** (or **"Settings"** → **"Cloud resources"**) in the left sidebar.
2. You should see your AWS Account ID listed under **"AWS account"** with status **"Linked"**.

> This link grants Databricks the ability to launch CloudFormation stacks in your AWS account during workspace provisioning (Step 4.1). No manual IAM role creation is needed.

**Step 5 — Note your Databricks Account ID**

In the Account Console → bottom-left corner → click your profile → copy the **Account ID** (UUID format). This is referenced in the API and Terraform flows (Sections 4.4 and 4.5).

---

**What the Marketplace link enables:**

| Capability                            | Without Marketplace        | With Marketplace            |
| ------------------------------------- | -------------------------- | --------------------------- |
| Auto-provision cross-account IAM role | ❌ Manual (Section 2.1)    | ✅ CloudFormation one-click |
| Auto-create S3 root bucket            | ❌ Manual (Section 2.3)    | ✅ Databricks creates it    |
| Auto-provision VPC & networking       | ✅ (both paths)            | ✅ (both paths)             |
| Consolidated AWS billing for DBUs     | ❌ Invoice from Databricks | ✅ Charged via AWS bill     |

---

## 4. Deploy a Databricks Workspace on AWS

### 4.1 Fully Automated via AWS Marketplace (Recommended)

> **Pre-requisite:** Complete Section 3.2 (AWS Marketplace subscription and account linking) before following these steps. No manual AWS account preparation (Section 2) is required — Databricks provisions the cross-account IAM role, EC2 instance profile, S3 root bucket, and VPC automatically.

In this flow, you trigger a one-click **AWS CloudFormation Quick-Create stack** directly from the Databricks workspace creation wizard. CloudFormation creates the cross-account IAM role and EC2 instance profile in your AWS account. Databricks then auto-creates the S3 root bucket and auto-provisions the VPC and all networking.

#### What Gets Provisioned Automatically

| AWS Resource               | Provisioned By                  | Method                                          |
| -------------------------- | ------------------------------- | ----------------------------------------------- |
| **Cross-account IAM role** | Databricks (via CloudFormation) | One-click stack launch from the wizard          |
| **EC2 instance profile**   | Databricks (via CloudFormation) | Same CloudFormation stack                       |
| **S3 root bucket**         | Databricks                      | Auto-created with encryption and bucket policy  |
| **VPC**                    | Databricks                      | `/16` CIDR, Databricks-assigned range           |
| **Private Subnet A**       | Databricks                      | `/17` in AZ 1 — cluster worker nodes            |
| **Private Subnet B**       | Databricks                      | `/17` in AZ 2 — cluster worker nodes            |
| **NAT Gateway + IGW**      | Databricks                      | Outbound internet access for cluster nodes      |
| **Security Groups**        | Databricks                      | Intra-cluster (Spark inter-node) + egress rules |
| **Route Tables**           | Databricks                      | Public and private routing                      |

All resources appear in your AWS account (EC2/VPC/IAM/S3 consoles) and count against your AWS service limits, but their lifecycle is managed by Databricks.

---

#### Step-by-Step: Create a Workspace via Fully Automated Path

**Step 1 — Sign in to the Account Console**

Navigate to [https://accounts.cloud.databricks.com](https://accounts.cloud.databricks.com) and sign in (your account is already linked to AWS from Section 3.2).

**Step 2 — Open the workspace creation wizard**

In the left sidebar, click **"Workspaces"** → click **"Create workspace"** (top-right).

**Step 3 — Enter workspace basics**

| Field              | What to enter                                                        |
| ------------------ | -------------------------------------------------------------------- |
| **Workspace name** | Unique name, e.g. `my-databricks-workspace` (alphanumeric + hyphens) |
| **AWS Region**     | Region where you want resources created, e.g. `us-east-1`            |

**Step 4 — Configure AWS credentials via CloudFormation (auto-creation)**

Under **"Credential configuration"**, click **"Add credential configuration"**:

| Field                 | What to enter                   |
| --------------------- | ------------------------------- |
| **Credential name**   | e.g. `auto-cross-account-creds` |
| **IAM role creation** | Select **"Create new role"**    |

Databricks displays a **"Launch CloudFormation"** button. Click it:

1. A new browser tab opens the AWS CloudFormation **Quick-Create Stack** page, pre-populated with all required parameters (Databricks Account ID is automatically embedded in the stack template).
2. Scroll to the bottom, check **"I acknowledge that AWS CloudFormation might create IAM resources"**, then click **"Create stack"**.
3. Wait for the stack status to show **`CREATE_COMPLETE`** (approximately 1–2 minutes).
4. Copy the **IAM Role ARN** from the CloudFormation **Outputs** tab.
5. Return to the Databricks browser tab, paste the Role ARN, and click **"Add"**.

> CloudFormation creates: the `DatabricksCrossAccountRole` IAM role (with trust policy locked to your Databricks account ID using `sts:ExternalId`) and the `DatabricksEC2InstanceProfile` — both with least-privilege policies.

**Step 5 — Configure storage (auto-creation)**

Under **"Storage configuration"**, click **"Add storage configuration"**:

| Field                          | What to enter                     |
| ------------------------------ | --------------------------------- |
| **Storage configuration name** | e.g. `auto-root-storage`          |
| **S3 bucket**                  | Select **"Create new S3 bucket"** |

Databricks automatically creates a bucket named `databricks-workspace-<account-id>-<region>` with:

- All public access blocked
- Server-side encryption (SSE-S3 or SSE-KMS)
- Bucket policy granting the Databricks control plane the required access

No manual S3 steps required.

**Step 6 — Choose network configuration → "Databricks-managed"**

Under **"Network configuration"**, the default option is:

```
● Databricks-managed VPC  ← default, leave this selected
  Databricks automatically creates the VPC, subnets,
  NAT Gateway, and security groups in your AWS account.
```

Leave this selected. No VPC inputs are required.

**Step 7 — (Optional) Enable Unity Catalog**

If your account has a Unity Catalog metastore assigned to the same region, it is automatically attached. Otherwise toggle **"Enable Unity Catalog"** and select or create a metastore.

**Step 8 — Review and create**

| Setting                  | Expected value                       |
| ------------------------ | ------------------------------------ |
| Workspace name           | e.g. `my-databricks-workspace`       |
| AWS Region               | e.g. `us-east-1`                     |
| Credential configuration | Auto-created CloudFormation role ARN |
| Storage configuration    | Auto-created S3 bucket               |
| Network                  | **Databricks-managed**               |

Click **"Save"** / **"Create"**.

**Step 9 — Monitor provisioning**

```
PROVISIONING  →  RUNNING
```

Typically **5–15 minutes**. Databricks:

1. Assumes the CloudFormation-created cross-account role
2. Creates the VPC, subnets, NAT gateway, IGW, security groups, and route tables
3. Validates the S3 bucket policy
4. Starts the workspace control plane

If provisioning fails, click the workspace name → **"Error details"** for the specific AWS error.

**Step 10 — Access the workspace**

Once status shows **RUNNING**, click **"Open"**:

```
https://<deployment-name>.cloud.databricks.com
```

---

**Post-provisioning checklist:**

- [ ] Verify the EC2 Instance Profile appears in Settings → Identity and access → Instance profiles (auto-registered)
- [ ] Add workspace admins (Section 5.2)
- [ ] Enable IP Access Lists if required (Section 5.3)
- [ ] Generate a Personal Access Token for CLI/API use (Section 5.4)
- [ ] Create your first cluster (Section 6)

---

### 4.2 Databricks-Managed VPC, Manual AWS Prep

> **Pre-requisite:** Complete Sections 2.1 (IAM roles) and 2.3 (S3 bucket) before following these steps. This path is for teams that manage IAM and S3 separately (e.g. via a central cloud platform team). **Section 2.2 (VPC & Networking) is not required** — Databricks provisions the VPC automatically.

#### What Databricks Provisions Automatically in Your AWS Account

When you do not supply a custom VPC, Databricks creates all of the following resources inside **your** AWS account during workspace provisioning. They are visible in your EC2/VPC console and count against your AWS service limits.

| AWS Resource         | Details                                                                       |
| -------------------- | ----------------------------------------------------------------------------- |
| **VPC**              | `/16` CIDR, Databricks-assigned address range                                 |
| **Private Subnet A** | `/17` in Availability Zone 1 — cluster worker nodes                           |
| **Private Subnet B** | `/17` in Availability Zone 2 — cluster worker nodes                           |
| **Public Subnet**    | Small subnet for NAT Gateway                                                  |
| **Internet Gateway** | Created and attached to the VPC                                               |
| **NAT Gateway**      | One NAT Gateway in the public subnet — workers use this for outbound internet |
| **Route Tables**     | Public (IGW) and private (NAT) routing configured automatically               |
| **Security Group**   | Intra-cluster Spark inter-node traffic + workspace egress rules               |

> **Limitations of Databricks-managed VPC:** You cannot configure custom CIDR ranges, VPC peering, AWS Transit Gateway, or PrivateLink on a Databricks-managed VPC. If you need any of these, use Section 4.2 (custom VPC) instead.

---

#### Step-by-Step: Create a Workspace with Databricks-Managed VPC

**Step 1 — Sign in to the Account Console**

Navigate to [https://accounts.cloud.databricks.com](https://accounts.cloud.databricks.com) and sign in with your Databricks account credentials.

> The **Account Console** is separate from any workspace. It manages billing, workspace lifecycle, Unity Catalog metastores, and user federation.

**Step 2 — Open the workspace creation wizard**

In the left sidebar, click **"Workspaces"**, then click **"Create workspace"** (top-right button).

**Step 3 — Enter workspace basics**

| Field              | What to enter                                                                       |
| ------------------ | ----------------------------------------------------------------------------------- |
| **Workspace name** | Any unique name, e.g. `my-databricks-workspace` (alphanumeric + hyphens, no spaces) |
| **AWS Region**     | The AWS region matching your S3 bucket and IAM role, e.g. `us-east-1`               |

**Step 4 — Link your AWS account credentials**

| Field                        | What to enter                                                                             |
| ---------------------------- | ----------------------------------------------------------------------------------------- |
| **Credential configuration** | Click **"Add credential configuration"** (or select existing)                             |
| **Credential name**          | e.g. `aws-cross-account-creds`                                                            |
| **IAM role ARN**             | `arn:aws:iam::<YOUR_ACCOUNT_ID>:role/DatabricksCrossAccountRole` (created in Section 2.1) |

Databricks uses this role to call AWS APIs (EC2, VPC, etc.) in your account.

**Step 5 — Configure root storage**

| Field                          | What to enter                                                               |
| ------------------------------ | --------------------------------------------------------------------------- |
| **Storage configuration**      | Click **"Add storage configuration"** (or select existing)                  |
| **Storage configuration name** | e.g. `aws-root-storage`                                                     |
| **S3 bucket name**             | `databricks-workspace-<YOUR_ACCOUNT_ID>-us-east-1` (created in Section 2.3) |

**Step 6 — Choose network configuration → select "Databricks-managed"**

On the **Network** configuration screen, you will see two options:

```
○  Databricks-managed VPC
   Databricks automatically creates and manages the VPC,
   subnets, and networking in your AWS account.
   Recommended for most users.

○  Customer-managed VPC
   Bring your own VPC (custom CIDR, peering, PrivateLink).
   See Section 4.2.
```

**Select "Databricks-managed VPC"** (this is the default). No further network inputs are required.

**Step 7 — (Optional) Enable Unity Catalog**

If your account has a Unity Catalog metastore assigned to the same region, it is automatically attached. Otherwise:

1. Toggle **"Unity Catalog"** → **"Enable Unity Catalog"** if prompted.
2. Select or create a metastore for the region.

> Unity Catalog is strongly recommended for all new workspaces — it provides centralised governance, row/column-level security, and data lineage.

**Step 8 — Review and create**

Review all settings on the summary page:

| Setting                  | Expected Value                                    |
| ------------------------ | ------------------------------------------------- |
| Workspace name           | e.g. `my-databricks-workspace`                    |
| AWS Region               | e.g. `us-east-1`                                  |
| Credential configuration | `DatabricksCrossAccountRole` ARN                  |
| Storage configuration    | Your S3 bucket name                               |
| Network                  | **Databricks-managed** ← confirm this is selected |

Click **"Save"** / **"Create"**. Provisioning begins immediately.

**Step 9 — Monitor provisioning progress**

The workspace card on the Workspaces page shows a status badge:

```
PROVISIONING  →  RUNNING
```

Provisioning typically takes **5–15 minutes**. During this time Databricks:

1. Assumes your cross-account IAM role
2. Creates the VPC, subnets, NAT gateway, IGW, security groups, and route tables in your AWS account
3. Configures the S3 root bucket policy
4. Starts the workspace control plane components

If provisioning fails, click the workspace name → **"Error details"** for the specific AWS error (usually an IAM permission or S3 bucket policy issue).

**Step 10 — Access the workspace**

Once the status shows **RUNNING**, click **"Open"**. The workspace URL follows the pattern:

```
https://<deployment-name>.cloud.databricks.com
```

Bookmark this URL — it is unique to your workspace.

---

**Post-provisioning checklist:**

- [ ] Register the EC2 Instance Profile in workspace Settings → Identity and access → Instance profiles (Section 5.1)
- [ ] Add workspace admins (Section 5.2)
- [ ] Enable IP Access Lists if required (Section 5.3)
- [ ] Generate a Personal Access Token for CLI/API use (Section 5.4)
- [ ] Create your first cluster (Section 6)

---

### 4.3 Customer-Managed VPC via Account Console

> Use this section when you need full control over your VPC — custom CIDR ranges, VPC peering, AWS Transit Gateway, AWS PrivateLink, or organisation-mandated network segmentation. You must complete **all of Section 2 (AWS Account Preparation)** before following these steps.

The workspace creation wizard is identical to Section 4.2 up through Steps 1–5. The difference is in **Step 6 (Network)**: instead of selecting "Databricks-managed", you select **"Customer-managed VPC"** and supply the resources created in Section 2.2.

**Step 6 — Choose network configuration → select "Customer-managed VPC"**

Select **"Customer-managed VPC"** in the Network screen, then provide:

| Field                    | Value                                                                         |
| ------------------------ | ----------------------------------------------------------------------------- |
| **VPC ID**               | `vpc-xxxxxxxxxxxxxxxxx` (from Section 2.2, Step 1)                            |
| **Subnet IDs**           | `subnet-aaa` (private A), `subnet-bbb` (private B) (from Section 2.2, Step 2) |
| **Security Group ID**    | `sg-xxxxxxxxxxxxxxxx` (cluster SG from Section 2.2, Step 5)                   |
| **Backend private link** | Optional — enable only if you have set up AWS PrivateLink for Databricks      |

> Databricks validates that the VPC, subnets, and security group meet the requirements (DNS enabled, sufficient IPs, intra-SG traffic allowed) before letting you proceed. If validation fails, review the error and check your Section 2.2 configuration.

**Steps 7–10** are identical to Section 4.2 (Unity Catalog, review, monitor provisioning, access workspace).

---

#### Subnet Sizing Guidelines

Databricks allocates **2 private IP addresses per EC2 node** (one for the node, one for the Spark container). Each subnet must have enough free IP addresses for your maximum expected cluster size.

```
Required IPs = (max workers + 1 driver) × 2

Example: 10 workers + 1 driver = 11 nodes × 2 = 22 IPs needed
/24 subnet (254 usable IPs) handles ~127 nodes — recommended minimum
```

---

### 4.4 Databricks-Managed Provisioning via Account API

In this flow, **Section 2.2 (VPC & Networking) is skipped entirely**. Only the cross-account IAM role (Section 2.1 — Role 1), the EC2 instance profile (Section 2.1 — Role 2), and the S3 bucket (Section 2.3) are required upfront. When no custom VPC is referenced during workspace creation, Databricks automatically provisions all networking within your AWS account.

#### What Databricks provisions automatically

| Resource             | Details                                             |
| -------------------- | --------------------------------------------------- |
| **VPC**              | `/16` CIDR, Databricks-assigned range               |
| **Private Subnet A** | `/17` in AZ 1 — cluster nodes                       |
| **Private Subnet B** | `/17` in AZ 2 — cluster nodes                       |
| **NAT Gateway**      | Created in an auto-managed public subnet            |
| **Internet Gateway** | Created and attached by Databricks                  |
| **Security Groups**  | Intra-cluster (Spark inter-node) + workspace SG     |
| **Route Tables**     | Public and private routing configured automatically |

All of the above resources are created inside **your** AWS account but are managed by Databricks. They appear in your EC2 / VPC console and count against your AWS service limits.

#### Step-by-Step via Databricks Account REST API

The **Databricks Account API** (also called the MWS API — Multi-Workspace Storage) exposes REST endpoints for credentials, storage, and workspace lifecycle. Authentication uses OAuth 2.0 (recommended for automation) or username/password basic auth (acceptable for manual testing).

**Step 1 — Obtain an OAuth access token**

```bash
ACCOUNT_ID="<your-databricks-account-uuid>"   # from Account Console → profile
ACCOUNT_URL="https://accounts.cloud.databricks.com"
CLIENT_ID="<service-principal-client-id>"
CLIENT_SECRET="<service-principal-client-secret>"

# Obtain an M2M OAuth token via the service principal
TOKEN=$(curl -sX POST \
  "$ACCOUNT_URL/oidc/accounts/$ACCOUNT_ID/v1/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=all-apis" \
  | jq -r '.access_token')
```

> **Alternative for testing:** Replace `Bearer $TOKEN` with `Basic $(echo -n "email@example.com:password" | base64)` — not recommended for production scripts.

**Step 2 — Register the cross-account IAM credentials**

```bash
CREDENTIALS_ID=$(curl -sX POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "$ACCOUNT_URL/api/2.0/accounts/$ACCOUNT_ID/credentials" \
  -d '{
    "credentials_name": "aws-cross-account-creds",
    "aws_credentials": {
      "sts_role": {
        "role_arn": "arn:aws:iam::<YOUR_ACCOUNT_ID>:role/DatabricksCrossAccountRole"
      }
    }
  }' | jq -r '.credentials_id')

echo "Credentials ID: $CREDENTIALS_ID"
```

**Step 3 — Register the S3 root storage configuration**

```bash
STORAGE_CONFIG_ID=$(curl -sX POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "$ACCOUNT_URL/api/2.0/accounts/$ACCOUNT_ID/storage-configurations" \
  -d '{
    "storage_configuration_name": "aws-root-storage",
    "root_bucket_info": {
      "bucket_name": "databricks-workspace-<YOUR_ACCOUNT_ID>-us-east-1"
    }
  }' | jq -r '.storage_configuration_id')

echo "Storage Config ID: $STORAGE_CONFIG_ID"
```

**Step 4 — Create the workspace (Databricks provisions the VPC automatically)**

```bash
WORKSPACE_ID=$(curl -sX POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "$ACCOUNT_URL/api/2.0/accounts/$ACCOUNT_ID/workspaces" \
  -d "{
    \"workspace_name\": \"my-databricks-workspace\",
    \"aws_region\": \"us-east-1\",
    \"credentials_id\": \"$CREDENTIALS_ID\",
    \"storage_configuration_id\": \"$STORAGE_CONFIG_ID\"
  }" | jq -r '.workspace_id')

echo "Workspace ID: $WORKSPACE_ID"
```

> **Key:** No `network_id` field is specified. This instructs Databricks to auto-provision the VPC in your account.
> To use a pre-existing custom VPC instead, register it first with `POST /api/2.0/accounts/{id}/networks` and then pass `"network_id": "<mws-network-id>"` in the workspace request — this is the Model C path described in Section 4.3.

**Step 5 — Poll until the workspace is ready**

```bash
while true; do
  RESPONSE=$(curl -sX GET \
    -H "Authorization: Bearer $TOKEN" \
    "$ACCOUNT_URL/api/2.0/accounts/$ACCOUNT_ID/workspaces/$WORKSPACE_ID")
  STATUS=$(echo "$RESPONSE" | jq -r '.workspace_status')
  echo "$(date '+%H:%M:%S') — Status: $STATUS"
  if [[ "$STATUS" == "RUNNING" ]]; then
    DEPLOY=$(echo "$RESPONSE" | jq -r '.deployment_name')
    echo "Workspace ready: https://${DEPLOY}.cloud.databricks.com"
    break
  elif [[ "$STATUS" == "FAILED" ]]; then
    echo "Provisioning failed:"
    echo "$RESPONSE" | jq '.workspace_status_message'
    break
  fi
  sleep 30
done
```

---

### 4.5 Full-Stack Terraform (AWS + Databricks Providers)

This approach provisions **all** AWS resources and the Databricks workspace in a single `terraform apply`. The `hashicorp/aws` provider creates the IAM role and S3 bucket; the `databricks/databricks` provider (in MWS / Account-level mode) registers them with Databricks and creates the workspace. Databricks manages the VPC automatically — no `databricks_mws_networks` resource is needed unless you want a custom VPC.

**Directory structure:**

```
workspace-infra/
├── providers.tf
├── variables.tf
├── workspace-aws.tf            # AWS: IAM role + S3 bucket
├── workspace-databricks.tf    # Databricks MWS: credentials, storage, workspace
├── outputs.tf
└── terraform.tfvars            # ← never commit to git
```

**`providers.tf`:**

```hcl
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# MWS (Account-level) provider — targets accounts.cloud.databricks.com
provider "databricks" {
  alias         = "mws"
  host          = "https://accounts.cloud.databricks.com"
  account_id    = var.databricks_account_id
  client_id     = var.databricks_client_id      # service principal OAuth
  client_secret = var.databricks_client_secret
}
```

**`workspace-aws.tf`:**

```hcl
# ── Cross-Account IAM Role ─────────────────────────────────────────────────────

data "aws_iam_policy_document" "databricks_cross_account_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::414351767826:root"] # Databricks control plane
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_account_id]
    }
  }
}

resource "aws_iam_role" "cross_account" {
  name               = "databricks-cross-account-role"
  assume_role_policy = data.aws_iam_policy_document.databricks_cross_account_trust.json
  tags               = { ManagedBy = "Terraform", Purpose = "DatabricksCrossAccount" }
}

resource "aws_iam_role_policy" "cross_account" {
  name = "databricks-cross-account-policy"
  role = aws_iam_role.cross_account.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ec2:CancelSpotInstanceRequests", "ec2:DescribeAvailabilityZones",
        "ec2:DescribeInstances",          "ec2:DescribeRouteTables",
        "ec2:DescribeSecurityGroups",     "ec2:DescribeSubnets",
        "ec2:DescribeVpcs",               "ec2:CreateTags", "ec2:DeleteTags",
        "ec2:RunInstances",               "ec2:TerminateInstances",
        "ec2:RequestSpotInstances",       "ec2:AttachVolume",
        "ec2:CreateVolume",               "ec2:DeleteVolume",
        "iam:PassRole"
      ]
      Resource = "*"
    }]
  })
}

# ── S3 Root Storage Bucket ─────────────────────────────────────────────────────

resource "aws_s3_bucket" "root_storage" {
  bucket = "databricks-workspace-${var.databricks_account_id}-${var.aws_region}"
  tags   = { ManagedBy = "Terraform", Purpose = "DatabricksRootStorage" }
}

resource "aws_s3_bucket_public_access_block" "root_storage" {
  bucket                  = aws_s3_bucket.root_storage.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "root_storage" {
  bucket = aws_s3_bucket.root_storage.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
  }
}

resource "aws_s3_bucket_policy" "root_storage" {
  bucket     = aws_s3_bucket.root_storage.id
  depends_on = [aws_s3_bucket_public_access_block.root_storage]
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DatabricksRootBucketAccess"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::414351767826:root" }
      Action    = [
        "s3:GetObject", "s3:GetObjectVersion", "s3:PutObject",
        "s3:DeleteObject", "s3:ListBucket",     "s3:GetBucketLocation"
      ]
      Resource = [
        aws_s3_bucket.root_storage.arn,
        "${aws_s3_bucket.root_storage.arn}/*"
      ]
    }]
  })
}
```

**`workspace-databricks.tf`:**

```hcl
# ── Databricks MWS Resources (Account-level) ──────────────────────────────────

# Register the cross-account IAM role as Databricks credentials
resource "databricks_mws_credentials" "this" {
  provider         = databricks.mws
  account_id       = var.databricks_account_id
  credentials_name = "${var.workspace_name}-credentials"
  role_arn         = aws_iam_role.cross_account.arn
}

# Register the S3 bucket as workspace root storage
resource "databricks_mws_storage_configurations" "this" {
  provider                   = databricks.mws
  account_id                 = var.databricks_account_id
  storage_configuration_name = "${var.workspace_name}-storage"
  bucket_name                = aws_s3_bucket.root_storage.bucket
  depends_on                 = [aws_s3_bucket_policy.root_storage]
}

# Create the workspace — Databricks manages VPC automatically (no network_id)
resource "databricks_mws_workspaces" "this" {
  provider       = databricks.mws
  account_id     = var.databricks_account_id
  workspace_name = var.workspace_name
  aws_region     = var.aws_region

  credentials_id           = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id

  # ── To switch to a custom VPC (Model A), add a databricks_mws_networks resource ──
  # that references the VPC resources from Section 2.2, then set:
  # network_id = databricks_mws_networks.this.network_id
}
```

**`variables.tf`:**

```hcl
variable "aws_region" {
  description = "AWS region for Databricks workspace"
  type        = string
  default     = "us-east-1"
}

variable "databricks_account_id" {
  description = "Databricks account UUID (from Account Console → profile)"
  type        = string
}

variable "databricks_client_id" {
  description = "Databricks service principal client ID (OAuth M2M)"
  type        = string
  sensitive   = true
}

variable "databricks_client_secret" {
  description = "Databricks service principal client secret (OAuth M2M)"
  type        = string
  sensitive   = true
}

variable "workspace_name" {
  description = "Name for the Databricks workspace"
  type        = string
}
```

**`outputs.tf`:**

```hcl
output "workspace_url" {
  description = "Databricks workspace URL"
  value       = "https://${databricks_mws_workspaces.this.deployment_name}.cloud.databricks.com"
}

output "workspace_id" {
  description = "Databricks workspace ID"
  value       = databricks_mws_workspaces.this.workspace_id
}

output "s3_root_bucket" {
  description = "S3 root storage bucket name"
  value       = aws_s3_bucket.root_storage.bucket
}

output "cross_account_role_arn" {
  description = "Cross-account IAM role ARN"
  value       = aws_iam_role.cross_account.arn
}
```

**`terraform.tfvars`** (do not commit to git):

```hcl
aws_region               = "us-east-1"
databricks_account_id    = "<your-databricks-account-uuid>"
databricks_client_id     = "<service-principal-client-id>"
databricks_client_secret = "<service-principal-secret>"
workspace_name           = "my-databricks-workspace"
```

**Deploy:**

```bash
cd workspace-infra
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

**Expected output:**

```
Apply complete! Resources: 8 added, 0 changed, 0 destroyed.

Outputs:
workspace_url          = "https://my-databricks-workspace-abc123.cloud.databricks.com"
workspace_id           = "123456789"
s3_root_bucket         = "databricks-workspace-<account-id>-us-east-1"
cross_account_role_arn = "arn:aws:iam::123456789012:role/databricks-cross-account-role"
```

> **Extending to a custom VPC (Model C):** Add the VPC Terraform resources from Section 2.2 into `workspace-aws.tf`, then add a `databricks_mws_networks` resource to `workspace-databricks.tf` referencing those VPC IDs, and set `network_id` in `databricks_mws_workspaces`. This gives you both the IaC discipline of this section and the full network control of Model C.

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
| **Databricks Runtime** | `17.3 LTS` (Long Term Support — stable)           |
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
    "spark_version": "17.3.x-scala2.12",
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
    "preloaded_spark_versions": ["17.3.x-scala2.12"]
  }'
```

### 7.3 Attach a Cluster to a Pool

When creating a cluster, set the `instance_pool_id`:

```json
{
  "cluster_name": "pool-backed-cluster",
  "spark_version": "17.3.x-scala2.12",
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
    "pattern": "17\\.[0-9]+\\.x-scala2\\.12",
    "defaultValue": "17.3.x-scala2.12"
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
pip install databricks-connect==17.3.*

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

_Last updated: March 2026 | Databricks Runtime: 17.3 LTS | AWS Provider: us-east-1_
