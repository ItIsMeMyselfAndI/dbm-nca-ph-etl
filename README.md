# DBM NCA Data Pipeline

A serverless ETL (Extract, Transform, Load) pipeline designed to automate the scraping, processing, and storage of Notice of Cash Allocation (NCA) documents from the Philippine Department of Budget and Management (DBM).

This project focuses exclusively on the **ingestion layer**: it autonomously monitors the DBM website and populates a **Supabase** database.

## 📖 Table of Contents


- [Key Features](#key-features)
- [Architecture](#architecture)
  - [Data Flow Breakdown](#data-flow-breakdown)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Environment Variables](#environment-variables)
- [Database Setup](#database-setup)
- [How to Run](#how-to-run)
  - [A. Locally](#a-locally)
  - [B. AWS Deployment (Manual)](#b-aws-deployment)
    - [1. Infrastructure Setup (One-Time)](#1-infrastructure-setup-one-time)
    - [2. Packaging & Updating Code](#2-packaging--updating-code)



---

## 🚀 Key Features

* **Serverless Architecture:** Fully serverless execution using **AWS Lambda** to minimize idle costs and scale automatically.
* **Optimized Parallelism (Batching):** Implements a smart batching strategy. Instead of processing pages individually, the orchestrator groups pages into logical batches (e.g., Pages 1-10). This significantly reduces **S3 `GetObject` costs** and Lambda overhead while maintaining high throughput.
* **Resilient Queuing:** Uses **two stages of AWS SQS** (Release Queue & Batch Queue) to decouple scraping, orchestration, and extraction.
* **Adaptive Table Parsing:** Dynamically handles **changing column layouts** within the PDF files using `pdfplumber` and `pandas`.


## 🛠️ Tech Stack


### Core Logic

![Python](https://img.shields.io/badge/python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/numpy-%23013243.svg?style=for-the-badge&logo=numpy&logoColor=white)
![Pydantic](https://img.shields.io/badge/Pydantic-E92063?style=for-the-badge&logo=pydantic&logoColor=white)

* **Language:** Python 3.14+
* **Data Processing:** Pandas, NumPy, pdfplumber

### Infrastructure (AWS)

![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![AWS Lambda](https://img.shields.io/badge/Lambda-FF9900?style=for-the-badge&logo=aws-lambda&logoColor=white)
![AWS S3](https://img.shields.io/badge/S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)
![AWS SQS](https://img.shields.io/badge/SQS-FF4F8B?style=for-the-badge&logo=amazonsqs&logoColor=white)
![AWS CloudWatch](https://img.shields.io/badge/CloudWatch-%23FF4F8B.svg?style=for-the-badge&logo=amazon-cloudwatch&logoColor=white)

* **Compute:** AWS Lambda (3 Functions: Scraper, Orchestrator, Worker)
* **Storage:** AWS S3 (Raw Data Lake)
* **Messaging:** AWS SQS (Standard Queues)
* **Monitoring:** AWS CloudWatch (Logs & Metrics)

### Database

![Supabase](https://img.shields.io/badge/Supabase-3ECF8E?style=for-the-badge&logo=supabase&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)

* **Primary DB:** Supabase (PostgreSQL)




## 🏗️ Architecture

The pipeline follows a **Fan-Out / Worker** pattern with batched processing to optimize resource usage:

![Pipeline Architecture Diagram](./dbm-nca-ph-pipeline.png)

### Data Flow Breakdown

1. **Ingestion (Lambda A):**
* Triggered by a scheduled event (Cron) or *manually*.
* Scrapes the DBM website for new NCA releases.
* Uploads the raw PDF to **S3** and **Database**.
* Pushes a message containing the `release` and metadata to **SQS A**.


```python
class Release(BaseModel):
    id: str
    title: str
    url: str
    filename: str
    year: int
    page_count: int = 0
    file_meta_created_at: Optional[str] = None
    file_meta_modified_at: Optional[str] = None

```


2. **Orchestration (Lambda B):**
* Triggered by **SQS A**.
* Downloads the PDF from S3 to determine the total page count.
* Groups pages into batches (e.g., 1-10, 11-20, etc.) based on a configurable batch size.
* **Fan-Out:** Pushes a message for each *batch*.


```python
class ReleaseBatch(BaseModel):
    batch_num: int
    release: Release
    start_page_num: int
    end_page_num: int

```


3. **Extraction (Lambda C):**
* Triggered by **SQS B** (Queue Batch Size: 1 message per invocation).
* Downloads the PDF **once** per batch.
* Iterates through the specific range of pages (e.g., 1-10) defined in the message.
* Extracts, cleans, and consolidates data using `pandas`.
* Inserts the structured rows into **Supabase**.




## 📂 Project Structure

```bash
.
├── handlers/                               # AWS Lambda Entry Points & Deployment
│   ├── deploy.sh                           # Automated deployment script
│   ├── scraper.py                          # Handler for Lambda A
│   ├── orchestrator.py                     # Handler for Lambda B
│   ├── worker.py                           # Handler for Lambda C
│   ├── dbmScraper_requirements.txt
│   ├── dbmOrchestrator_requirements.txt
│   └── dbmWorker_requirements.txt
│
├── src/                                    # Application Core (Framework Independent)
│   ├── core/                               # Inner Layer (Business Rules)
│   │   ├── entities/                       # Data models
│   │   ├── use_cases/                      # Application logic
│   │   └── interfaces/                     # Abstract base classes (Ports)
│   │
│   ├── infrastructure/                     # Outer Layer (External Systems/Frameworks)
│   │   ├── adapters/                       # Implementations of interfaces
│   │   ├── config.py                       # Environment variable management
│   │   └── constants.py                    # App-wide constants
│   │
│   ├── main.py                             # Local execution entry point (for dev/debugging without AWS)
│   └── initialize_aws.py                   # Script to set up AWS resources (S3, SQS, Lambda)
│
├── requirements.txt                        # Main project dependencies
└── supabase_schema.sql                     # Database initialization script

```

## 📦 Installation

Follow these steps to set up the project locally.

1. **Clone the repository:**
```bash
git clone https://github.com/ItIsMeMyselfAndI/dbm-nca-ph-data-pipeline.git
cd dbm-nca-ph-data-pipeline
```


2. **Create a virtual environment:**
```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```


3. **Install dependencies:**
```bash
pip install -r requirements.txt
```



## 🗄️ Database Setup

The pipeline uses a relational schema with three core tables: `release` (PDF metadata), `record`, and `allocation`.

1. Log in to your **Supabase Dashboard**.
2. Navigate to the **SQL Editor**.
3. Open `supabase_schema.sql` (located in the root) or copy the schema below:

```sql
-- 1. Releases: Metadata for the source PDF file
CREATE TABLE public.release (
  id text PRIMARY KEY,
  title text,
  filename text,
  url text,
  year int,
  page_count int,
  file_meta_created_at text NOT NULL,
  file_meta_modified_at text NOT NULL,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

-- 2. Records: The high-level NCA document details
CREATE TABLE public.record (
  id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nca_number text NOT NULL UNIQUE,
  nca_type text,
  department text,
  released_date text,
  purpose text,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  release_id text NOT NULL REFERENCES public.release(id) ON DELETE CASCADE
);

-- 3. Allocations: Specific budget line items (Operating Units & Amounts)
CREATE TABLE public.allocation (
  id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  operating_unit text NOT NULL,
  agency text NOT NULL,
  amount double precision NOT NULL,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  nca_number text NOT NULL REFERENCES public.record(nca_number) ON DELETE CASCADE
);

```

4. Click **Run** to initialize the tables and indices.




## 🏃 How to Run



### A. Locally

To test the pipeline logic on your machine without deploying to Lambda:

1. Activate your virtual environment if not already done.
```bash
source venv/bin/activate
```

2. **Run Main Script:**
Execute the local entry point.
```bash
python -m src.main
```



### B. AWS Deployment
#### 1. Install AWS CLI
Make sure you have the AWS CLI installed and configured on your machine.
```bash
pip install awscli
```

#### 2. Configure AWS CLI

Set up your AWS profile with the necessary permissions.
```bash
aws configure
```

#### 3. Environment Setup

Create a `.env` file in the root directory:

```bash
# Supabase Configuration
SUPABASE_URL=<SUPABASE_PROJECT_URL>
SUPABASE_ANON_KEY=<SUPABASE_ANON_KEY>

# AWS Configuration (Required for Local Dev)
# Note: Remove AWS_REGION when deploying to Lambda as it is reserved
AWS_REGION=<REGION>

# AWS Resources
AWS_S3_BUCKET_NAME=dbm-nca-ph-release-files
AWS_SQS_RELEASE_QUEUE_URL=https://sqs.<REGION>.amazonaws.com/<ACCOUNT_ID>/dbm-nca-ph-release-queue
AWS_SQS_RELEASE_BATCH_QUEUE_URL=https://sqs.<REGION>.amazonaws.com/<ACCOUNT_ID>/dbm-nca-ph-release-batch-queue
```

> [!NOTE]
> Remember to replace placeholders with their actual values.
> - **SUPABSE_PROJECT_URL**
> - **SUPABASE_ANON_KEY**
> - **REGION**
> - **ACCOUNT_ID**

#### 4. Infrastructure Setup (One-Time)

Before deploying the codes, run the script to set up the necessary AWS resources.

```bash
python -m src.initialize_aws
```

##### The script will create the following resources:

1. **S3 Bucket A**
* *Name:* `dbm-nca-ph-release-files`
* *Description:* A bucket for storing raw PDF files of NCA releases. The scraper uploads the PDFs here, and the orchestrator/worker functions read from this bucket to process the data.
* *Note:* Name is similar to `AWS_S3_BUCKET_NAME` in the environment variables.

2. **S3 Bucket B**
* *Name:* `dbm-nca-ph-lambda-deployment`
* *Description:* A bucket for Lambda deployment packages (optional, can also use direct upload).

2. **SQS Queue A**
* *Name:* `dbm-nca-ph-release-queue`
* *Description:* A standard queue for **release messages**. Each message contains metadata about a new NCA release and triggers the orchestration process.
* *Note*: URL is the same as `AWS_SQS_RELEASE_QUEUE_URL` in the environment variables.

3. **SQS Queue B**
* *Name:* `dbm-nca-ph-release-batch-queue`
* *Description:* A standard queue for **batch messages**. Each message contains information about a specific batch of pages to process, triggered by the orchestrator and consumed by the worker.
* *Note*: URL is the same as `AWS_SQS_RELEASE_BATCH_QUEUE_URL` in the environment variables.

4. **Lambda A (Scraper)**
* *Name:* `dbmScraper`
* *Description:* A function that scrapes the DBM website for new NCA releases, uploads PDFs to S3, insert entries to Supabase, and pushes metadata messages to **SQS A**.
* *Runtime:* Python 3.14

5. **Lambda B (Orchestrator)**
* *Name:* `dbmOrchestrator`
* *Description:* A function that listens to **SQS A**, downloads the PDF from S3 to determine page count, creates batches of pages, and pushes batch messages to **SQS B**.
* *Runtime:* Python 3.14
* *Trigger:* **SQS Queue A**

6. **Lambda C (Worker):**
* *Name:* `dbmWorker`
* *Description:* A function that listens to **SQS B**, downloads the PDF for the specified batch, extracts and processes the data, and inserts structured records into Supabase.
* *Runtime:* Python 3.14
* *Trigger:* **SQS Queue B**


#### 5. Deploying Updates (Automated)

Navigate to the `handlers/` directory and use the deployment script:

```bash
# Usage: ./deploy.sh <SOURCE_FILE> <FUNCTION_NAME>

./deploy.sh scraper.py dbmScraper
./deploy.sh orchestrator.py dbmOrchestrator
./deploy.sh worker.py dbmWorker
```

> [!IMPORTANT]
> **Naming Convention:** The script strictly requires requirements files to be named `<FUNCTION_NAME>_requirements.txt`. Ensure your handler names match your AWS Lambda function names to avoid deployment conflicts.

> [!WARNING]
> This has only been tested on Unix-based systems (Linux/Mac). Windows users may need to modify the script or deploy manually via the AWS Console.

---
