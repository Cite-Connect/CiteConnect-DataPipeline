"""
Inspect one sample dataset from CiteConnect GCS to explore possible biases
"""
import os
import pandas as pd
from google.cloud import storage
from io import BytesIO

# --- Step 1: Authenticate ---
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/dhikshamathan/Downloads/gcs-key.json"

# --- Step 2: Define bucket and file ---
BUCKET_NAME = "citeconnect-test-bucket"
SAMPLE_PATH = "raw/Clinical_NLP_medical_text_mining_1761266396.parquet"  # 👈 pick any file shown in your bucket

# --- Step 3: Download and load file ---
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)
blob = bucket.blob(SAMPLE_PATH)

print(f"📦 Downloading {SAMPLE_PATH} from {BUCKET_NAME}...")
data = blob.download_as_bytes()

df = pd.read_parquet(BytesIO(data))
print(f"✅ Loaded {len(df)} records and {len(df.columns)} columns.\n")

# --- Step 4: Show schema and sample ---
print("📋 Columns available:\n", df.columns.tolist(), "\n")
print("🔍 Sample rows:\n", df.head(3), "\n")

# --- Step 5: Describe numeric & categorical fields ---
print("📊 Numeric overview:\n", df.describe(include='number'), "\n")
print("🧮 Categorical overview:\n", df.describe(include='object'), "\n")
