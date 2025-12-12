# Environment Variables for CiteConnect Data Pipeline

## Required Variables for Google Cloud Deployment

### Existing Variables (already in docker-compose.yaml)
```bash
# Google Cloud
GCS_BUCKET_NAME=your-bucket-name
GCS_PROJECT_ID=your-project-id

# API Keys
SEMANTIC_SCHOLAR_API_KEY=your-api-key
SEMANTIC_SCHOLAR_API_KEYS=key1,key2,key3  # For rotation

# Email
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
```

### NEW Variables Required for Updated DAG

#### Data Collection Parameters
```bash
SEARCH_TERMS=machine learning,neural networks,computer vision
PAPERS_PER_TERM=100
MAX_REFERENCES_PER_PAPER=50
COLLECTION_DOMAIN=AI
COLLECTION_SUBDOMAINS=machine learning,neural networks,computer vision
PROCESSING_MAX_WORKERS=10
```

#### Bias Analysis & Mitigation
```bash
BIAS_MITIGATION_MAX_FIELDS=5
```

#### Supabase Database Connection
Choose ONE of these methods:

**Method 1: Full DATABASE_URL (Recommended)**
```bash
DATABASE_URL=postgresql://user:password@host:5432/database
```

**Method 2: Individual Variables**
```bash
SUPABASE_DB_HOST=your-project-ref.supabase.co
SUPABASE_DB_PORT=5432
SUPABASE_DB_NAME=postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD=your-password
```

#### Alert Configuration
```bash
ALERT_EMAIL=your-email@gmail.com  # For anomaly alerts (defaults to SMTP_USER)
```

## Quick Setup for .env file

Add these to your `.env` file:

```bash
# Data Collection
SEARCH_TERMS=machine learning,neural networks,computer vision
PAPERS_PER_TERM=100
MAX_REFERENCES_PER_PAPER=50
COLLECTION_DOMAIN=AI

# Bias Analysis
BIAS_MITIGATION_MAX_FIELDS=5

# Supabase (choose one method)
DATABASE_URL=postgresql://user:password@host:5432/database
# OR
SUPABASE_DB_HOST=your-project-ref.supabase.co
SUPABASE_DB_PASSWORD=your-password

# Alerts
ALERT_EMAIL=your-email@gmail.com
```

## Variable Priority in DAG

The DAG uses a 3-tier priority system:

1. **DAG Parameters** (highest priority - set in Airflow UI)
2. **Environment Variables** (from .env file)
3. **Default Values** (fallback)

## Google Cloud Deployment

When deploying to Google Cloud Engine/Composer:

1. **Set environment variables** in your deployment configuration
2. **Upload GCS credentials** to `configs/credentials/gcs-key.json`
3. **Configure Supabase connection** using DATABASE_URL
4. **Set paper collection limits** via environment variables

All new features (bias analysis, Supabase upload, anomaly detection) will work automatically once these variables are configured.
