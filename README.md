# CiteConnect: AI-Powered Research Paper Recommendation System

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.7.1-orange.svg)](https://airflow.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen.svg)]()

> **An MLOps-driven data pipeline that collects, processes, and versions academic research papers with comprehensive metadata extraction and automated testing.**

---

##  Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Running the Pipeline](#running-the-pipeline)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Documentation

Comprehensive documentation is available in the [`documentation/`](https://github.com/DhikshaMathanagopal/CiteConnect/tree/main/documentation) directory:

- **[Error Audit & Quality Assurance Plan](https://github.com/DhikshaMathanagopal/CiteConnect/blob/main/documentation/Error%20Audit%20%26%20Quality%20Assurance%20Plan.md)** - Detailed error scenarios, resolution strategies, and QA monitoring framework
- **[User Research Analysis Report](https://github.com/DhikshaMathanagopal/CiteConnect/blob/main/documentation/User%20Research%20Analysis%20Report.md)** - User research findings and UX insights
- **[Pipeline Testing Documentation](https://github.com/DhikshaMathanagopal/CiteConnect/blob/main/documentation/test_pipelineDoc.md)** - Detailed pipeline architecture and testing documentation
- **[Preprocessing & Embedding](https://github.com/DhikshaMathanagopal/CiteConnect/blob/main/documentation/Embedding%20&%20Preprocessing.md)** - Detailed explanation of preprocessing and embedding technique

---
##  Overview

**CiteConnect** is a production-ready data ingestion and processing pipeline designed to collect academic research papers from multiple sources with comprehensive metadata extraction. Built as part of Northeastern University's IE7305 MLOps course, the system demonstrates end-to-end pipeline orchestration with Apache Airflow, automated testing, and data versioning.

### Key Objectives

- **Comprehensive Data Collection**: Multi-source ingestion from Semantic Scholar, arXiv, and CORE APIs
- **Robust Content Extraction**: 4-tier fallback strategy achieving 60-70% full introduction extraction
- **Production-Ready Quality**: 85%+ test coverage with 164 unit tests and 12 integration tests
- **Automated Orchestration**: Apache Airflow DAGs with email notifications and error handling
- **Data Versioning**: DVC integration for reproducible data lineage

---

## ✨ Features

###  Multi-Source Data Ingestion

- **Semantic Scholar API Integration**: Primary source for academic papers
- **arXiv HTML Extraction**: Highest quality full-text content
- **PDF Processing**: GROBID and regex-based extraction
- **Rate Limiting**: Intelligent backoff to avoid API blocks

###  Comprehensive Metadata Collection

**30+ metadata fields per paper**:
- **Identifiers**: Paper ID, DOI, arXiv ID, PubMed ID
- **Content**: Title, abstract, introduction (when available)
- **Authors**: Names, IDs, affiliations
- **Citations**: Citation count, influential citations, references
- **Venue**: Journal/conference, publication type
- **Topics**: Fields of study, AI-tagged categories
- **Access**: Open access status, PDF URLs
- **Quality Metrics**: Extraction method, content quality rating

###  Data Processing Pipeline

- **Schema Validation**: Automated data quality checks
- **Text Preprocessing**: Cleaning, normalization
- **Quality Scoring**: Content quality ratings (high/medium/low)
- **Cloud Storage**: Google Cloud Storage integration

###  Comprehensive Testing

- **164 Unit Tests**: Individual component validation with mocking
- **12 Integration Tests**: End-to-end pipeline testing
- **85%+ Code Coverage**: Production-ready quality assurance
- **Automated CI**: pytest integration in Airflow DAG

###  Data Versioning

- **DVC Integration**: Version control for datasets
- **Run Tracking**: JSON logs with metadata for each pipeline run

---

##  Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│               Apache Airflow Orchestration                  │
│                                                             │
│  Validate → Ingest → Process → Embed → Version              │
└─────────────────────────────────────────────────────────────┘
         ↓          ↓          ↓         ↓         ↓
┌─────────────────────────────────────────────────────────────┐
│                    Storage Layer                            │
│                                                             │
│  GCS (raw/processed) | Local (embeddings) | DVC (versions)  │
└─────────────────────────────────────────────────────────────┘
         ↑
┌─────────────────────────────────────────────────────────────┐
│              Data Sources                                   │
│                                                             │
│  Semantic Scholar | arXiv | CORE                            │
└─────────────────────────────────────────────────────────────┘

```
### Pipeline Flow (5 Stages)

```
[1] VALIDATION
    • Environment & API keys check
    • GCS access validation
    • 164 unit tests execution
    
              ↓

[2] DATA INGESTION
    • Semantic Scholar API query
    • 4-tier content extraction:
      1. ArXiv HTML (best quality)
      2. GROBID PDF (ML-based)
      3. Regex PDF (fallback)
      4. Abstract+TLDR (guaranteed)
    • 30+ metadata fields per paper
    • Save to GCS as Parquet
    
              ↓

[3] DATA PROCESSING
    • Schema validation
    • Text cleaning & normalization
    • Quality filtering
    • Feature engineering
    • Upload to GCS
    
              ↓

[4] EMBEDDING GENERATION
    • Load papers from GCS
    • Sentence-Transformers (384-dim)
    • Batch processing
    • Save embeddings_db.pkl
    
              ↓

[5] DATA VERSIONING
    • DVC tracking of embeddings
    • Generate run_summary.json
    • Git commit with metrics
    • Push to DVC remote (GCS)
    • Email notification

```
---

##  Prerequisites

### System Requirements

- **OS**: macOS, Linux, or Windows with WSL2
- **RAM**: 8GB minimum (16GB recommended)
- **Disk**: 20GB free space
- **Docker**: Version 20.10+ with Docker Compose v2+
- **Git**: For version control

### Software Dependencies

- **Python**: 3.9 or 3.10 (required)
- **Docker Desktop**: Latest version
- **GCP Account**: For cloud storage (free tier available)

### API Keys (Recommended)

1. **Semantic Scholar API Key** (Optional but recommended)
   - Request at: https://www.semanticscholar.org/product/api
   - Without key: 1 request per 5 seconds
   - With key: 1 request per 1 second

2. **Google Cloud Platform** (Required)
   - Create project at [console.cloud.google.com](https://console.cloud.google.com)
   - Enable Cloud Storage API
   - Create service account with Storage Admin role

---

##  Installation & Setup

### Step 1: Clone the Repository

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/CiteConnect.git
cd CiteConnect

# Verify you're on the main branch
git branch
```

### Step 2: Set Up Python Environment

```bash
# Create virtual environment
python3.9 -m venv .venv

# Activate virtual environment
# On macOS/Linux:
source .venv/bin/activate

# On Windows:
.venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip

# Install project dependencies
pip install -r requirements.txt

# Install development/testing dependencies
pip install -r requirements-dev.txt

```

### Step 3: Configure Environment Variables

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your configuration
nano .env  # or use your preferred editor
```

**Required `.env` Configuration:**

```bash
# ============================================
# API KEYS
# ============================================
# Semantic Scholar API (optional but recommended)
SEMANTIC_SCHOLAR_KEY=your_api_key_here

# Unpaywall API (requires email for rate limiting)
UNPAYWALL_EMAIL=your_email@example.com

# CORE API (optional)
CORE_API_KEY=your_core_api_key

# ============================================
# GOOGLE CLOUD CONFIGURATION
# ============================================
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/gcp-credentials.json
GCS_BUCKET_NAME=citeconnect-test-bucket
GCS_PROJECT_ID=your-gcp-project-id

# ============================================
# AIRFLOW CONFIGURATION
# ============================================
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_NAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

# Email notifications
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password

# ============================================
# EMBEDDING CONFIGURATION
# ============================================
EMBEDDING_PROVIDER=weaviate
SENTENCE_TRANSFORMERS_MODEL=all-MiniLM-L6-v2
LOCAL_EMBEDDINGS_PATH=working_data/embeddings_db.pkl
EMBEDDING_BATCH_SIZE=32
CHUNK_SIZE=512
CHUNK_OVERLAP=50

# ============================================
# WEAVIATE CONFIGURATION (if using)
# ============================================
WEAVIATE_URL=http://localhost:8080
WEAVIATE_API_KEY=
WEAVIATE_COLLECTION=PaperChunks

# ============================================
# OPENAI CONFIGURATION (fallback provider)
# ============================================
OPENAI_API_KEY=sk-your-api-key-here
OPENAI_EMBEDDING_MODEL=text-embedding-3-small
EMBEDDING_DIM=768
```

### Step 4: Set Up Google Cloud Storage

```bash
# Authenticate with GCP
gcloud auth login

# Set your project
gcloud config set project YOUR_PROJECT_ID

# Create GCS bucket
gsutil mb -p YOUR_PROJECT_ID -l us-central1 gs://citeconnect-test-bucket

# Create service account
gcloud iam service-accounts create citeconnect-sa \
    --display-name="CiteConnect Service Account"

# Grant Storage Admin role to service account
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:citeconnect-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Download service account credentials
gcloud iam service-accounts keys create ./config/gcp-credentials.json \
    --iam-account=citeconnect-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com

# Grant your personal email access for testing
gsutil iam ch user:YOUR_EMAIL:objectAdmin gs://citeconnect-test-bucket

# Verify bucket access
gsutil ls gs://citeconnect-test-bucket
```

### Step 5: Start Docker Services

```bash
# Remove version field from docker-compose.yaml if present
# Open docker-compose.yaml and delete the line containing: version: '3.8'

# Build and start all services
docker compose up --build

# Or run in detached mode (background)
docker compose up -d --build

# Wait for services to initialize (2-3 minutes)
# Check logs to monitor startup
docker compose logs -f airflow-init
```

**Services Started:**
- **Airflow Webserver**: http://localhost:8080 (admin/admin)
- **Airflow Scheduler**: Background service

### Step 6: Verify Installation

```bash
# Check that all services are running
docker compose ps

# Run setup validation tests
python -m pytest tests/unit/test_setup.py -v

# Verify Airflow DAGs are loaded
docker compose exec airflow-webserver airflow dags list

# Check for the test_citeconnect DAG
docker compose exec airflow-webserver airflow dags list | grep test_citeconnect

# Verify GCS connection
python scripts/health_check.py
```

**Expected Output:**
```
✅ All services running
✅ Setup tests passed
✅ DAG 'test_citeconnect' loaded successfully
✅ GCS bucket accessible
```

---

##  Running the Pipeline

### Option 1: Via Airflow UI (Recommended)

1. **Open Airflow Web Interface**
   - Navigate to: http://localhost:8080
   - Login: `admin` / `admin`

2. **Locate the DAG**
   - Find `test_citeconnect` in the DAG list
   - Toggle the switch to **enable** the DAG

3. **Trigger Pipeline Run**
   - Click the **▶️ Play** button on the right
   - Confirm trigger in the popup

4. **Monitor Execution**
   - Click on the DAG name to view details
   - Switch to **Graph View** to see task progress
   - Tasks turn **green** when successful, **red** on failure

5. **View Logs**
   - Click on any task box
   - Select **Log** to view execution details

### Option 2: Via Command Line

```bash
# Trigger the DAG manually
docker compose exec airflow-webserver airflow dags trigger test_citeconnect

# Check DAG run status
docker compose exec airflow-webserver airflow dags list-runs -d test_citeconnect

# View specific task logs (replace DATE with actual date)
docker compose exec airflow-webserver airflow tasks logs \
    test_citeconnect check_env_variables DATE

# Monitor real-time execution
docker compose logs -f airflow-scheduler
```

### Option 3: Standalone Execution (Without Airflow)

```bash
# Activate virtual environment
source .venv/bin/activate

# Run data collection directly
python src/DataPipeline/Ingestion/main.py \
    --search-terms "machine learning" "deep learning" \
    --limit 10 \
    --output data/raw

# Check collected data
ls -lh data/raw/

# Run preprocessing
python src/DataPipeline/preprocessing/preprocess.py \
    --input data/raw \
    --output data/processed

# Generate embeddings
python src/services/embedding_service.py \
    --domain healthcare \
    --batch-size 10 \
    --max-papers 20
```

### Pipeline Execution Steps

When you run the pipeline, it executes the following tasks in sequence:

1. **check_env_variables** (~5 seconds)
   - Validates all API keys and environment variables

2. **check_gcs_connection** (~10 seconds)
   - Tests GCS bucket access and lists files

3. **test_api_connection** (~2-3 minutes)
   - Runs 164 unit tests with pytest
   - Reports test results and coverage

4. **test_paper_collection** (~2-5 minutes)
   - Searches for papers on "large language models"
   - Collects 5 papers with full metadata
   - Uploads to GCS raw/ folder

5. **preprocess_papers** (~1-2 minutes)
   - Validates and cleans collected data
   - Uploads processed data to GCS

6. **embed_stored_data** (~3-5 minutes)
   - Generates vector embeddings
   - Saves to local embeddings_db.pkl

7. **version_embeddings_dvc** (~1-2 minutes)
   - Tracks embeddings with DVC
   - Creates run_summary.json
   - Commits to Git and pushes to DVC remote

8. **send_success_notification** (~5 seconds)
   - Sends email with pipeline results

**Total Pipeline Duration: ~3-5 minutes**

### Email Notifications

After successful completion, you'll receive an HTML email with:
- Papers processed count
- Embeddings created count
- Final data size
- Pipeline parameters
- Git commit message
- Task completion status

---

##  Project Structure

```
CiteConnect/
├── README.md                              # Project documentation
├── requirements.txt                       # Production dependencies
├── requirements-test.txt                  # Testing dependencies
├── setup.py                               # Package configuration
├── pytest.ini                             # Test configuration
├── docker-compose.yaml                    # Docker services
├── .env.example                           # Environment template
├── .gitignore                             # Git ignore rules
├── dags/
│   └── test_citeconnect.py                # Airflow DAG orchestration
│
├── src/
│   ├── citeconnect.egg-info/              # Package metadata
│   │
│   ├── DataPipeline/
│   │   ├── embeddings/
│   │   │   ├── __init__.py                # Module initialization
│   │   │   ├── config.py                  # Embedding configuration
│   │   │   ├── embed_generator.py         # Generate embeddings
│   │   │   ├── local_embedder.py          # Local model inference
│   │   │   ├── openai_embedder.py         # OpenAI API wrapper
│   │   │   ├── README.md                  # Embeddings documentation
│   │   │   └── vector_store.py            # Vector storage management
│   │   │
│   │   ├── Ingestion/
│   │   │   ├── __init__.py                # Module initialization
│   │   │   ├── batch_ingestion.py         # Batch paper collection
│   │   │   ├── content_extractor.py       # 4-tier extraction
│   │   │   ├── gcs_uploader.py            # Cloud storage upload
│   │   │   ├── main.py                    # Ingestion entry point
│   │   │   ├── metadata_utils.py          # Metadata processing
│   │   │   ├── processor.py               # Data processing logic
│   │   │   └── semantic_scholar_client.py # API client
│   │   │
│   │   ├── metrics/
│   │   │   └── stats.json                 # Pipeline statistics
│   │   │
│   │   ├── preprocessing/
│   │   │   ├── __init__.py                # Module initialization
│   │   │   ├── chunker.py                 # Text chunking
│   │   │   ├── metadata_enricher.py       # Enrich metadata
│   │   │   ├── README.md                  # Preprocessing docs
│   │   │   └── text_cleaner.py            # Text normalization
│   │   │
│   │   ├── Processing/
│   │   │   ├── __init__.py                # Module initialization
│   │   │   └── gcs_read.py                # Read from GCS
│   │   │
│   │   ├── utils/
│   │   │   ├── __init__.py                # Module initialization
│   │   │   ├── constants.py               # Global constants
│   │   │   ├── logging_config.py          # Logging setup
│   │   │   └── storage_helpers.py         # Storage utilities
│   │   │
│   │   └── Validation/
│   │       ├── __init__.py                # Module initialization
│   │       └── analyse_data.ipynb         # Data analysis notebook
│   │
│   └── ModelPipeline/                     # Future ML models
│
├── tests/
│   ├── conftest.py                        # Shared test fixtures
│   │
│   ├── Unit/
│   │   ├── test_semantic_scholar_client.py # API client tests
│   │   ├── test_content_extractor.py      # Extraction tests
│   │   ├── test_metadata_utils.py         # Metadata tests
│   │   ├── test_processor.py              # Processing tests
│   │   ├── test_gcs_uploader.py           # Upload tests
│   │   └── test_setup.py                  # Setup validation
│   │
│   ├── integration/
│   │   └── test_end_to_end_pipeline.py    # E2E pipeline tests
│   │
│   └── fixtures/
│       ├── sample_papers.json             # Test data
│       └── mock_responses.py              # Mock API responses
│
├── services/
│   └── embedding_service.py               # Embedding orchestration
│
├── config/
│   ├── gcp-credentials.json               # GCP service account
│   └── api_keys.env                       # API secrets
│
├── scripts/
│   ├── generate_fernet_key.py             # Generate Airflow key
│   ├── health_check.py                    # System health check
│   └── setup_environment.sh               # Environment setup
│
├── data/                                  # Local data storage
│   ├── raw/                               # Original papers
│   ├── processed/                         # Cleaned data
│   └── embeddings/                        # Vector embeddings
│
├── working_data/                          # DVC tracked data
│   ├── embeddings_db.pkl                  # Embedding database
│   ├── embeddings_db.pkl.dvc              # DVC metadata
│   └── run_summary.json                   # Pipeline run logs
│
├── logs/                                  # Application logs
│   └── pipeline.log                       # Execution logs
│
└── .dvc/                                  # DVC configuration
    ├── config                             # Remote storage config
    └── .gitignore                         # DVC ignore rules

```
---

##  Troubleshooting

### Docker Issues

#### Services Not Starting

**Problem**: Docker Compose fails to start services

**Solution**:
```bash
# Check Docker is running
docker ps

# Restart Docker Desktop completely
# Then try again
docker compose down
docker compose up --build

# Check for port conflicts
lsof -i :8080  # Airflow webserver
lsof -i :5432  # PostgreSQL
```

#### Build Failures with I/O Error

**Problem**: `failed to solve: Internal: write... input/output error`

**Solution**:
```bash
# Stop all containers
docker compose down

# Clean Docker system
docker system prune -a --volumes

# Restart Docker Desktop
# Increase Docker resources:
# Docker Desktop → Settings → Resources
# Memory: 8GB, Disk: 64GB

# Rebuild
docker compose up --build
```

#### Version Warning

**Problem**: `the attribute 'version' is obsolete`

**Solution**:
```bash
# Edit docker-compose.yaml and remove the line:
# version: '3.8'

# Or use sed (Linux/Mac):
sed -i '/^version:/d' docker-compose.yaml
```

### Airflow Issues

#### Webserver Not Accessible

**Problem**: Cannot access http://localhost:8080

**Solution**:
```bash
# Check if webserver is running
docker compose ps

# View webserver logs
docker compose logs airflow-webserver

# Restart webserver
docker compose restart airflow-webserver

# If still failing, rebuild
docker compose down
docker compose up -d --build
```

#### DAG Not Appearing

**Problem**: `test_citeconnect` DAG not visible in UI

**Solution**:
```bash
# Check for import errors
docker compose exec airflow-webserver airflow dags list-import-errors

# Validate DAG syntax
python -m py_compile dags/test_citeconnect.py

# Check DAG file is mounted
docker compose exec airflow-webserver ls /opt/airflow/dags/

# Force DAG refresh
docker compose restart airflow-scheduler

# Wait 30 seconds and refresh UI
```

#### Task Failures

**Problem**: Tasks failing with import errors

**Solution**:
```bash
# Check if src is in Python path
docker compose exec airflow-webserver python -c "import sys; print(sys.path)"

# Verify package installation
docker compose exec airflow-webserver pip list | grep cite

# Reinstall in container
docker compose exec airflow-webserver pip install -e /opt/airflow

# Check task logs for details
docker compose exec airflow-webserver airflow tasks logs test_citeconnect TASK_NAME DATE
```

### GCS Connection Issues

#### Authentication Errors

**Problem**: `403 Forbidden` or authentication failures

**Solution**:
```bash
# Verify credentials file exists and is valid
ls -l config/gcp-credentials.json
cat config/gcp-credentials.json | python -m json.tool

# Test authentication locally
python -c "from google.cloud import storage; storage.Client(); print('Success!')"

# Check environment variable in container
docker compose exec airflow-webserver env | grep GOOGLE_APPLICATION_CREDENTIALS

# Verify credentials are mounted
docker compose exec airflow-webserver ls -l /opt/airflow/config/gcp-credentials.json
```

#### Bucket Access Denied

**Problem**: `AccessDeniedException` when accessing bucket

**Solution**:
```bash
# Verify bucket exists
gsutil ls gs://citeconnect-test-bucket

# Check your permissions
gsutil iam get gs://citeconnect-test-bucket

# Grant yourself access
gsutil iam ch user:YOUR_EMAIL:objectAdmin gs://citeconnect-test-bucket

# Verify service account has access
gsutil iam ch serviceAccount:SERVICE_ACCOUNT:objectAdmin gs://citeconnect-test-bucket

# Test upload
echo "test" > /tmp/test.txt
gsutil cp /tmp/test.txt gs://citeconnect-test-bucket/test.txt
```

#### Cannot Get IAM Policy

**Problem**: `does not have storage.buckets.getIamPolicy access`

**Solution**:
```bash
# Option 1: Use GCP Console (easiest)
# Go to console.cloud.google.com → Storage → Bucket → Permissions
# Add your email with "Storage Object Admin" role

# Option 2: Grant yourself Storage Admin at project level
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="user:YOUR_EMAIL" \
  --role="roles/storage.admin"

# Wait 60 seconds for propagation, then retry
```

### API Issues

#### Rate Limiting (429 Errors)

**Problem**: `429 Too Many Requests` from Semantic Scholar

**Solution**:
```bash
# Set API key in .env file
SEMANTIC_SCHOLAR_KEY=your_actual_api_key_here

# Restart containers to pick up new env var
docker compose restart

# Reduce batch size temporarily
# Edit search_terms in dags/test_citeconnect.py
# Change limit from 5 to 2

# Verify API key is set in container
docker compose exec airflow-webserver env | grep SEMANTIC_SCHOLAR
```

#### Connection Timeouts

**Problem**: API requests timing out

**Solution**:
```bash
# Check internet connectivity
ping api.semanticscholar.org

# Increase timeout in code (if needed)
# Edit src/DataPipeline/Ingestion/semantic_scholar_client.py
# Increase timeout parameter

# Check if behind firewall/proxy
# Set proxy environment variables if needed
```

### Test Failures

#### Import Errors in Tests

**Problem**: `ModuleNotFoundError: No module named 'src'`

**Solution**:
```bash
# Install package in editable mode
pip install -e .

# Verify installation
pip show citeconnect

# Check PYTHONPATH
echo $PYTHONPATH

# Add manually if needed
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Run tests again
pytest
```

#### Fixture Not Found

**Problem**: `fixture 'some_fixture' not found`

**Solution**:
```bash
# Verify conftest.py exists
ls tests/conftest.py

# Run from project root (not from tests/ directory)
cd /path/to/CiteConnect
pytest

# Check fixture is defined in conftest.py
grep "def some_fixture" tests/conftest.py
```

#### Tests Pass Locally but Fail in Airflow

**Problem**: Tests pass when run manually but fail in DAG

**Solution**:
```bash
# Check working directory in Airflow task
# Add to test_api_connection function:
print(f"Current directory: {os.getcwd()}")

# Ensure pytest is installed in container
docker compose exec airflow-webserver pip list | grep pytest

# Run pytest from same directory as Airflow
docker compose exec airflow-webserver bash
cd /opt/airflow
pytest tests/Unit/ -v
```

### DVC Issues

#### DVC Push Fails

**Problem**: `dvc push` fails with authentication error

**Solution**:
```bash
# Verify DVC remote is configured
dvc remote list -v

# Test GCS access with gsutil
gsutil ls gs://citeconnect-test-bucket/dvc-cache/

# Reconfigure credentials
dvc remote modify storage credentialpath ./config/gcp-credentials.json

# Try push again
dvc push -v  # Verbose output for debugging
```

#### Git Safe Directory Error

**Problem**: `fatal: detected dubious ownership in repository`

**Solution**:
```bash
# Add to safe directories (already handled in DAG)
git config --global --add safe.directory /opt/airflow

# Verify configuration
git config --global --list | grep safe.directory
```

### Memory/Performance Issues

#### Out of Memory Errors

**Problem**: Container runs out of memory

**Solution**:
```bash
# Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory: 8GB+

# Reduce batch size in pipeline
# Edit dags/test_citeconnect.py
# Change batch_size=5 to batch_size=2

# Monitor memory usage
docker stats
```

#### Slow Performance

**Problem**: Pipeline runs very slowly

**Solution**:
```bash
# Check if API key is set (5x speed improvement)
docker compose exec airflow-webserver env | grep SEMANTIC_SCHOLAR_KEY

# Reduce paper limit for faster testing
# Edit dags/test_citeconnect.py
# Change limit=5 to limit=2

# Check system resources
docker stats
top  # or htop on Linux

# Disable unnecessary services
# Comment out unused services in docker-compose.yaml
```

---

##  License

This project is licensed under the MIT License. See the LICENSE file for details.

---


##  Project Status

**Current Phase**: Data Pipeline Development (Phase 1 Complete)

**Completed**:
- ✅ Multi-source data ingestion
- ✅ 4-tier content extraction
- ✅ Comprehensive metadata collection
- ✅ Data preprocessing and validation
- ✅ Embedding generation
- ✅ DVC integration for data versioning
- ✅ Airflow orchestration with 8 tasks
- ✅ 176 automated tests (85%+ coverage)
- ✅ Email notifications
- ✅ GCS cloud storage integration

**Planned** (Phase 2):
- 🔄 Citation graph construction (Neo4j)
- 🔄 Vector database optimization
- 🔄 Performance tuning
- 📋 Recommendation algorithm development
- 📋 User interface (web application)
- 📋 Personalized ranking system
- 📋 A/B testing framework
- 📋 Production deployment to GCP

---
**Last Updated**: November 2024  
**Version**: 1.0.0  
**Status**: ✅ Production Ready (Phase 1 Complete)
