"""
bias_analysis_connect_gcs.py
-----------------------------------
Connects to GCS, loads ALL .parquet files from citeconnect-test-bucket/raw/,
cleans them, merges them into one DataFrame, and saves locally for bias analysis.
"""

import os
import pandas as pd
from google.cloud import storage
from io import BytesIO
import json
import numpy as np

# ============================================================
# 1. Authenticate with GCS (replace with your JSON key path)
# ============================================================
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/dhikshamathan/Downloads/gcs-key.json"

BUCKET_NAME = "citeconnect-test-bucket"
FOLDER = "raw/"

# ============================================================
# 2. Connect to the bucket
# ============================================================
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

print(f"✅ Connected to project: {client.project}")
print(f"📦 Using bucket: {BUCKET_NAME}/{FOLDER}")

# ============================================================
# 3. List all parquet files
# ============================================================
blobs = list(bucket.list_blobs(prefix=FOLDER))
parquet_files = [b for b in blobs if b.name.endswith(".parquet")]

print(f"📁 Found {len(parquet_files)} parquet files in {FOLDER}\n")

# ============================================================
# 4. Load & Combine all parquet files
# ============================================================
dfs = []
for b in parquet_files:
    print(f"⬇️ Loading {b.name} ...")
    try:
        data = b.download_as_bytes()
        temp_df = pd.read_parquet(BytesIO(data))
        temp_df["source_file"] = b.name
        dfs.append(temp_df)
    except Exception as e:
        print(f"⚠️ Failed to load {b.name}: {e}")

# Merge all
df_all = pd.concat(dfs, ignore_index=True)
print(f"\n✅ Combined {len(df_all)} rows from {len(dfs)} files")

# ============================================================
# 5. Clean up any dict/list/ndarray columns before saving
# ============================================================
def safe_serialize(val):
    """Convert dicts, lists, or arrays into JSON-safe strings"""
    if isinstance(val, (dict, list, np.ndarray)):
        try:
            return json.dumps(val, default=str)
        except Exception:
            return str(val)
    return val

for col in df_all.columns:
    if df_all[col].dtype == 'object':
        df_all[col] = df_all[col].apply(safe_serialize)

# ============================================================
# 6. Save merged data locally
# ============================================================
os.makedirs("data", exist_ok=True)
save_path = "data/combined_gcs_data.parquet"
df_all.to_parquet(save_path, index=False)

print(f"💾 Saved merged file → {save_path}")

# ============================================================
# 7. Display summary
# ============================================================
print("\n🧾 Columns:", df_all.columns.tolist())
print("\n📊 Data Sample:")
print(df_all.head(5))
print("\n✅ Ready for bias analysis → Run: python databias/analyze_bias.py")
