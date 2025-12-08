# Deploying CiteConnect to Google Cloud Composer

Complete guide for deploying your Airflow pipeline to Google Cloud Composer.

---

## 📋 **Prerequisites**

1. **GCP Project with Billing Enabled**
   ```bash
   # Set your project
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Enable Required APIs**
   ```bash
   gcloud services enable composer.googleapis.com
   gcloud services enable storage-component.googleapis.com
   ```

3. **Install Google Cloud SDK** (if not already installed)
   ```bash
   # Check if installed
   gcloud --version
   ```

4. **Set Up Service Account** (if not already done)
   ```bash
   # Create service account for Composer
   gcloud iam service-accounts create composer-sa \
       --display-name="Composer Service Account"
   
   # Grant necessary roles
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
       --member="serviceAccount:composer-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
       --role="roles/composer.worker"
   
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
       --member="serviceAccount:composer-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
       --role="roles/storage.admin"
   ```

---

## 🚀 **Step 1: Create Cloud Composer Environment**

### **Option A: Using gcloud CLI (Recommended)**

```bash
# Create Composer environment
gcloud composer environments create citeconnect-composer \
    --location us-central1 \
    --python-version 3 \
    --airflow-version composer-2.9.2-airflow-2.9.2 \
    --node-count 3 \
    --machine-type n1-standard-1 \
    --disk-size 30GB \
    --service-account composer-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com \
    --environment-size small
```

**Note:** This will take **20-30 minutes** to create. You can monitor progress in the GCP Console.

### **Option B: Using GCP Console**

1. Go to [Cloud Composer](https://console.cloud.google.com/composer)
2. Click **"Create Environment"**
3. Fill in:
   - **Name:** `citeconnect-composer`
   - **Location:** `us-central1` (or your preferred region)
   - **Airflow version:** `composer-2.9.2-airflow-2.9.2`
   - **Python version:** `3`
   - **Machine type:** `n1-standard-1`
   - **Node count:** `3`
   - **Disk size:** `30GB`
4. Click **"Create"**

---

## 📦 **Step 2: Prepare Dependencies**

### **2.1 Create requirements.txt for Composer**

Create `requirements-composer.txt` (PyPI packages only - no local packages):

```txt
# Core dependencies
requests>=2.31.0
pandas>=2.1.0
numpy>=1.25.0
beautifulsoup4>=4.12.2
lxml>=4.9.3
tqdm>=4.66.1
python-dotenv>=1.0.0

# PDF and document parsing
pymupdf>=1.23.0
pdfminer.six>=20221105

# Data formats and I/O
pyarrow>=14.0.1
fastparquet>=2023.8.0

# Google Cloud
google-cloud-storage>=2.10.0

# Embeddings & Vector Store
weaviate-client>=4.4.0
sentence-transformers>=2.2.0
torch>=2.0.0

# Optional: OpenAI fallback
openai>=1.0.0

# Concurrency and async utils
aiohttp>=3.9.0
tenacity>=8.2.3
urllib3>=2.0.7
idna>=3.4

# Headless browser fallback
playwright>=1.47.0

# Data ingestion
grobid-client-python

# Core Testing Framework
pytest==7.4.3
pytest-cov==4.1.0
pytest-mock==3.12.0
pytest-timeout==2.2.0
pytest-xdist==3.5.0

# Data Quality & Validation
great-expectations==0.18.8
pandera==0.17.2

# Mocking & Fixtures
responses==0.24.1
faker==20.1.0
freezegun==1.4.0

# Code Quality
pylint==3.0.3
black==23.12.1
flake8==7.0.0
mypy==1.7.1

# Optional but useful
pytest-html==4.1.1
pytest-json-report==1.5.0

# DVC for data versioning
dvc[gs]==3.32.0

# Async PostgreSQL driver
asyncpg>=0.29.0

# Data Bias Analysis
matplotlib>=3.8.0
seaborn>=0.12.2
fairlearn>=0.13.0
scikit-learn>=1.3.0
```

### **2.2 Upload Requirements to Composer**

```bash
# Get the GCS bucket name for your Composer environment
COMPOSER_BUCKET=$(gcloud composer environments describe citeconnect-composer \
    --location us-central1 \
    --format="value(config.dagGcsPrefix)")

# Extract bucket name (format: us-central1-citeconnect-composer-xxxxx-bucket)
BUCKET_NAME=$(echo $COMPOSER_BUCKET | cut -d'/' -f3)

# Upload requirements.txt
gsutil cp requirements-composer.txt gs://$BUCKET_NAME/requirements.txt

# Install requirements in Composer environment
gcloud composer environments update citeconnect-composer \
    --location us-central1 \
    --update-pypi-packages-from-file requirements-composer.txt
```

**Note:** Installing PyPI packages can take **10-15 minutes**.

---

## 📁 **Step 3: Prepare and Upload DAGs**

### **3.1 Update DAG Paths for Composer**

Composer uses GCS for DAG storage. You need to update paths in your DAG:

**Key Changes Needed:**
1. Update `sys.path.insert(0, '/opt/airflow')` to work with Composer
2. Update file paths to use GCS or `/home/airflow/gcs` (Composer's mounted GCS)
3. Ensure imports work correctly

### **3.2 Create Deployment Script**

Create `scripts/deploy_to_composer.sh`:

```bash
#!/bin/bash
# Deploy CiteConnect to Cloud Composer

set -e

PROJECT_ID="YOUR_PROJECT_ID"
ENVIRONMENT_NAME="citeconnect-composer"
LOCATION="us-central1"

echo "🚀 Deploying CiteConnect to Cloud Composer..."

# Get Composer bucket
COMPOSER_BUCKET=$(gcloud composer environments describe $ENVIRONMENT_NAME \
    --location $LOCATION \
    --format="value(config.dagGcsPrefix)")

BUCKET_NAME=$(echo $COMPOSER_BUCKET | cut -d'/' -f3)
DAGS_FOLDER=$(echo $COMPOSER_BUCKET | cut -d'/' -f4-)

echo "📦 Composer Bucket: gs://$BUCKET_NAME"
echo "📁 DAGs Folder: $DAGS_FOLDER"

# Upload DAGs
echo "📤 Uploading DAGs..."
gsutil -m cp -r dags/* gs://$BUCKET_NAME/$DAGS_FOLDER/

# Upload source code (needed for imports)
echo "📤 Uploading source code..."
gsutil -m cp -r src/ gs://$BUCKET_NAME/$DAGS_FOLDER/
gsutil -m cp -r services/ gs://$BUCKET_NAME/$DAGS_FOLDER/
gsutil -m cp -r utils/ gs://$BUCKET_NAME/$DAGS_FOLDER/
gsutil -m cp -r databias/ gs://$BUCKET_NAME/$DAGS_FOLDER/
gsutil -m cp -r tests/ gs://$BUCKET_NAME/$DAGS_FOLDER/
gsutil -m cp -r configs/ gs://$BUCKET_NAME/$DAGS_FOLDER/

# Upload requirements if changed
if [ -f requirements-composer.txt ]; then
    echo "📤 Uploading requirements..."
    gsutil cp requirements-composer.txt gs://$BUCKET_NAME/requirements.txt
fi

echo "✅ Deployment complete!"
echo "🌐 Access Airflow UI:"
gcloud composer environments describe $ENVIRONMENT_NAME \
    --location $LOCATION \
    --format="value(config.airflowUri)"
```

Make it executable:
```bash
chmod +x scripts/deploy_to_composer.sh
```

### **3.3 Update DAG for Composer Compatibility**

Update `dags/test_dag.py` to work with Composer:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import
from airflow.operators.bash import BashOperator      # Updated import
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import sys
import json
import os
import subprocess

# Composer-compatible path setup
# Composer mounts GCS at /home/airflow/gcs
COMPOSER_HOME = '/home/airflow/gcs'
if os.path.exists(COMPOSER_HOME):
    # Running in Composer
    PROJECT_ROOT = '/home/airflow/gcs/dags'
    sys.path.insert(0, PROJECT_ROOT)
    sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))
    sys.path.insert(0, os.path.join(PROJECT_ROOT, 'services'))
    sys.path.insert(0, os.path.join(PROJECT_ROOT, 'utils'))
else:
    # Running locally
    PROJECT_ROOT = '/opt/airflow'
    sys.path.insert(0, PROJECT_ROOT)

EMAIL_TO = ['anushasrini2001@gmail.com']

# ... rest of your DAG code ...
```

---

## 🔧 **Step 4: Configure Environment Variables**

### **4.1 Set Environment Variables in Composer**

```bash
gcloud composer environments update citeconnect-composer \
    --location us-central1 \
    --update-env-variables \
        SEMANTIC_SCHOLAR_API_KEY=your_key_here,\
        SEMANTIC_SCHOLAR_API_KEYS=key1,key2,key3,\
        GCS_BUCKET_NAME=citeconnect-test-bucket,\
        GCS_PROJECT_ID=your-project-id,\
        SMTP_USER=your_email@gmail.com,\
        SMTP_PASSWORD=your_app_password,\
        PAPERS_PER_TERM=100,\
        MAX_REFERENCES_PER_PAPER=0,\
        COLLECTION_DOMAIN=general,\
        PROCESSING_MAX_WORKERS=10,\
        LOCAL_EMBEDDINGS_PATH=/home/airflow/gcs/data/embeddings_db.pkl
```

### **4.2 Set Airflow Configuration Overrides**

```bash
gcloud composer environments update citeconnect-composer \
    --location us-central1 \
    --update-airflow-configs \
        core-dags_are_paused_at_creation=False,\
        core-load_examples=False,\
        smtp-smtp_host=smtp.gmail.com,\
        smtp-smtp_starttls=True,\
        smtp-smtp_ssl=False,\
        smtp-smtp_port=587,\
        smtp-smtp_user=your_email@gmail.com,\
        smtp-smtp_password=your_app_password,\
        smtp-smtp_mail_from=your_email@gmail.com
```

---

## 🔐 **Step 5: Set Up GCS Credentials**

Composer uses the environment's service account automatically. However, if you need to use a specific service account key:

### **Option A: Use Default Service Account (Recommended)**

Composer automatically uses its service account. Just ensure it has the necessary permissions:

```bash
# Grant Storage Admin to Composer's service account
COMPOSER_SA=$(gcloud composer environments describe citeconnect-composer \
    --location us-central1 \
    --format="value(config.nodeConfig.serviceAccount)")

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:$COMPOSER_SA" \
    --role="roles/storage.admin"
```

### **Option B: Upload Service Account Key**

If you need a specific service account key:

```bash
# Upload credentials file to Composer's GCS bucket
gsutil cp configs/credentials/gcs-key.json \
    gs://$BUCKET_NAME/configs/credentials/gcs-key.json
```

Then update your code to read from GCS:
```python
from google.cloud import storage

def get_gcs_credentials():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob('configs/credentials/gcs-key.json')
    blob.download_to_filename('/tmp/gcs-key.json')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/gcs-key.json'
```

---

## 🚀 **Step 6: Deploy Everything**

Run the deployment script:

```bash
# Update PROJECT_ID in the script first
./scripts/deploy_to_composer.sh
```

Or manually:

```bash
# Get Composer bucket
COMPOSER_BUCKET=$(gcloud composer environments describe citeconnect-composer \
    --location us-central1 \
    --format="value(config.dagGcsPrefix)")

BUCKET_NAME=$(echo $COMPOSER_BUCKET | cut -d'/' -f3)
DAGS_FOLDER=$(echo $COMPOSER_BUCKET | cut -d'/' -f4-)

# Upload everything
gsutil -m cp -r dags/* gs://$BUCKET_NAME/$DAGS_FOLDER/
gsutil -m cp -r src/ gs://$BUCKET_NAME/$DAGS_FOLDER/
gsutil -m cp -r services/ gs://$BUCKET_NAME/$DAGS_FOLDER/
gsutil -m cp -r utils/ gs://$BUCKET_NAME/$DAGS_FOLDER/
gsutil -m cp -r databias/ gs://$BUCKET_NAME/$DAGS_FOLDER/
gsutil -m cp -r tests/ gs://$BUCKET_NAME/$DAGS_FOLDER/
gsutil -m cp -r configs/ gs://$BUCKET_NAME/$DAGS_FOLDER/
```

---

## ✅ **Step 7: Verify Deployment**

### **7.1 Get Airflow UI URL**

```bash
gcloud composer environments describe citeconnect-composer \
    --location us-central1 \
    --format="value(config.airflowUri)"
```

### **7.2 Check DAGs**

```bash
# List DAGs
gcloud composer environments run citeconnect-composer \
    --location us-central1 \
    dags list

# Check for import errors
gcloud composer environments run citeconnect-composer \
    --location us-central1 \
    dags list-import-errors
```

### **7.3 Access Airflow UI**

1. Go to the URL from step 7.1
2. Login with your GCP account
3. Verify `test_citeconnect` DAG appears
4. Check for any import errors (red indicators)

---

## 🔄 **Step 8: Update Code for Composer**

### **8.1 Fix File Paths**

Update paths in your DAG tasks to use Composer's GCS mount:

```python
# Instead of:
data_path = "/tmp/test_data/processed"

# Use:
if os.path.exists('/home/airflow/gcs'):
    # Composer environment
    data_path = "/home/airflow/gcs/data/processed"
else:
    # Local environment
    data_path = "/tmp/test_data/processed"
```

### **8.2 Update Git/DVC Paths**

For Git and DVC operations in Composer:

```python
def version_embeddings_with_dvc(**context):
    # Composer uses /home/airflow/gcs for mounted GCS
    if os.path.exists('/home/airflow/gcs'):
        project_root = "/home/airflow/gcs/dags"
    else:
        project_root = "/opt/airflow"
    
    # ... rest of your code ...
```

### **8.3 Handle Working Directory**

```python
def test_paper_collection(**context):
    # Set working directory
    if os.path.exists('/home/airflow/gcs'):
        os.chdir('/home/airflow/gcs/dags')
    else:
        os.chdir('/opt/airflow')
    
    # ... rest of your code ...
```

---

## 📊 **Step 9: Monitor and Troubleshoot**

### **9.1 View Logs**

```bash
# View task logs
gcloud composer environments run citeconnect-composer \
    --location us-central1 \
    tasks logs test_citeconnect TASK_NAME DATE

# View scheduler logs
gcloud logging read "resource.type=cloud_composer_environment" \
    --limit 50
```

### **9.2 Common Issues**

**Issue 1: Import Errors**
- **Solution:** Ensure all source code is uploaded to GCS
- **Check:** `gcloud composer environments run ... dags list-import-errors`

**Issue 2: Missing Dependencies**
- **Solution:** Update PyPI packages in Composer environment
- **Command:** `gcloud composer environments update ... --update-pypi-packages-from-file`

**Issue 3: File Not Found**
- **Solution:** Use `/home/airflow/gcs` paths instead of `/opt/airflow`
- **Check:** Verify files are uploaded to GCS bucket

**Issue 4: Permission Errors**
- **Solution:** Grant necessary IAM roles to Composer service account
- **Check:** Service account permissions in IAM console

---

## 🔄 **Step 10: Continuous Deployment**

### **10.1 Create CI/CD Pipeline**

Create `.github/workflows/deploy-composer.yml`:

```yaml
name: Deploy to Cloud Composer

on:
  push:
    branches: [ main ]
    paths:
      - 'dags/**'
      - 'src/**'
      - 'requirements-composer.txt'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - uses: google-github-actions/setup-gcloud@v1
        with:
          service_account_key: ${{ secrets.GCP_SA_KEY }}
          project_id: ${{ secrets.GCP_PROJECT_ID }}
      
      - name: Deploy to Composer
        run: |
          ./scripts/deploy_to_composer.sh
```

### **10.2 Manual Deployment Script**

Update `scripts/deploy_to_composer.sh` with your project details and run it whenever you make changes.

---

## 💰 **Cost Estimation**

### **Monthly Costs (Approximate)**

- **Composer Environment:** ~$72/month (base)
- **Compute (3 nodes, n1-standard-1):** ~$150/month
- **Storage:** ~$5/month
- **Network:** ~$10/month

**Total: ~$237/month**

### **Cost Optimization**

1. **Use smaller machine types** for development
2. **Reduce node count** when not in use
3. **Delete environment** when not needed
4. **Use preemptible nodes** (if available)

---

## 📝 **Quick Reference Commands**

```bash
# Get Composer bucket
gcloud composer environments describe citeconnect-composer \
    --location us-central1 \
    --format="value(config.dagGcsPrefix)"

# Get Airflow UI URL
gcloud composer environments describe citeconnect-composer \
    --location us-central1 \
    --format="value(config.airflowUri)"

# List DAGs
gcloud composer environments run citeconnect-composer \
    --location us-central1 \
    dags list

# Trigger DAG
gcloud composer environments run citeconnect-composer \
    --location us-central1 \
    dags trigger test_citeconnect

# Update environment variables
gcloud composer environments update citeconnect-composer \
    --location us-central1 \
    --update-env-variables KEY=value

# View logs
gcloud logging read "resource.type=cloud_composer_environment" --limit 50
```

---

## ✅ **Deployment Checklist**

- [ ] GCP project with billing enabled
- [ ] Composer API enabled
- [ ] Composer environment created
- [ ] Requirements uploaded and installed
- [ ] DAGs uploaded to GCS
- [ ] Source code uploaded to GCS
- [ ] Environment variables configured
- [ ] Airflow configs set
- [ ] Service account permissions granted
- [ ] DAGs visible in Airflow UI
- [ ] No import errors
- [ ] Test DAG run successful

---

## 🆘 **Need Help?**

1. **Check Composer logs** in GCP Console
2. **Verify DAG import errors** using CLI
3. **Check service account permissions**
4. **Review file paths** in your code
5. **Test locally first** before deploying

---

**Next Steps:** After deployment, test your pipeline end-to-end and monitor for any issues!

