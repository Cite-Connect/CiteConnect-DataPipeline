"""
bias_analysis_connect_gcs.py
-----------------------------------
Connects to GCS, loads one or all .parquet files from citeconnect-test-bucket/raw/,
and prepares data for bias analysis.
"""

import os
import pandas as pd
from google.cloud import storage
from io import BytesIO

# ============================================================
# 1. Authenticate with GCS (replace this path with your JSON key)
# ============================================================
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/dhikshamathan/Downloads/gcs-key.json"

BUCKET_NAME = "citeconnect-test-bucket"
FOLDER = "raw/"

# ============================================================
# 2. Connect to the bucket
# ============================================================
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

print(f"Connected to project: {client.project}")
print(f"Using bucket: {BUCKET_NAME}/{FOLDER}")

# ============================================================
# 3. List all parquet files
# ============================================================
blobs = list(bucket.list_blobs(prefix=FOLDER))
parquet_files = [b for b in blobs if b.name.endswith(".parquet")]
print(f"Found {len(parquet_files)} parquet files in {FOLDER}\n")

# ============================================================
# 4. Choose a sample file (for testing bias on one)
# ============================================================
# You can manually change which file you want here:
SAMPLE_FILE = "raw/ai_in_healthcare_raw.parquet"

print(f"Loading file: {SAMPLE_FILE}")
blob = bucket.blob(SAMPLE_FILE)
data = blob.download_as_bytes()
df = pd.read_parquet(BytesIO(data))

print(f"Loaded {len(df)} records from {SAMPLE_FILE}")
print("\nColumns available:")
print(df.columns.tolist())
print("\nSample rows:")
print(df.head(3))
