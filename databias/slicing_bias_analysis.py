"""
databias/slicing_bias_analysis.py
--------------------------------
Performs data slicing on CiteConnect dataset using Fairlearn + Pandas.
Detects subgroup bias by year, field, and content quality.
"""

import pandas as pd
import numpy as np
from fairlearn.metrics import MetricFrame
import matplotlib.pyplot as plt
import seaborn as sns
import json
import os
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.storage_helpers import upload_to_gcs

# ======================================================
# 1. Load data
# ======================================================
df = pd.read_parquet("data/combined_gcs_data.parquet")
print(f"✅ Loaded {len(df)} papers for slicing analysis")

# Clean numeric + categorical columns
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df["citationCount"] = pd.to_numeric(df["citationCount"], errors="coerce").fillna(0)
df["intro_length"] = pd.to_numeric(df["intro_length"], errors="coerce").fillna(0)
df["fieldsOfStudy"] = df["fieldsOfStudy"].astype(str)
df["content_quality"] = df["content_quality"].astype(str)

os.makedirs("databias/slices", exist_ok=True)

# ======================================================
# 2. Group-wise slicing summaries
# ======================================================
slices = {
    "year": df.groupby("year")["citationCount"].mean().to_dict(),
    "fieldsOfStudy": df.groupby("fieldsOfStudy")["citationCount"].mean().to_dict(),
    "content_quality": df.groupby("content_quality")["intro_length"].mean().to_dict(),
}

with open("databias/slices/slice_summary.json", "w") as f:
    json.dump(slices, f, indent=2)
print("📊 Saved slice summaries → databias/slices/slice_summary.json")

# ======================================================
# 3. Fairlearn MetricFrame – group fairness view
# ======================================================
# Clean numeric target
df["citationCount"] = pd.to_numeric(df["citationCount"], errors="coerce").fillna(0)
df["fieldsOfStudy"] = df["fieldsOfStudy"].astype(str)

# MetricFrame requires numeric arrays
y_true = df["citationCount"].to_numpy().astype(float)
y_pred = df["citationCount"].to_numpy().astype(float)
sensitive = df["fieldsOfStudy"].to_numpy()

metric_frame = MetricFrame(
    metrics={"mean_citations": lambda y_true, y_pred: np.mean(y_true)},
    y_true=y_true,
    y_pred=y_pred,
    sensitive_features=sensitive
)

fairness_df = metric_frame.by_group.sort_values("mean_citations", ascending=False)
print("\n🎓 Avg citation count by field:")
print(fairness_df.head(10))

plt.figure(figsize=(10,6))
sns.barplot(
    x=fairness_df.head(10)["mean_citations"].values,
    y=fairness_df.head(10).index,
    palette="crest"
)
plt.title("Average Citation Count by Field")
plt.xlabel("Mean Citations")
plt.ylabel("Field of Study")
plt.tight_layout()
plt.savefig("databias/slices/field_slicing_bias.png")
plt.close()


# ======================================================
# 4. Simple mitigation: re-sample low-representation fields
# ======================================================
field_counts = df["fieldsOfStudy"].value_counts()
median_count = field_counts.median()
underrep = field_counts[field_counts < median_count].index

df_balanced = pd.concat([
    df[df["fieldsOfStudy"].isin(underrep)].sample(frac=2, replace=True, random_state=42),
    df[~df["fieldsOfStudy"].isin(underrep)]
])

print(f"\n⚖️ After mitigation: {len(df_balanced)} samples (was {len(df)})")
df_balanced.to_parquet("data/combined_gcs_data_balanced.parquet", index=False)
print("💾 Saved mitigated dataset → data/combined_gcs_data_balanced.parquet")

# ======================================================
# 5. Compute Fairness Disparity Metrics
# ======================================================
max_group = fairness_df["mean_citations"].max()
min_group = fairness_df["mean_citations"].min()
disparity_ratio = max_group / (min_group + 1e-6)
disparity_diff = max_group - min_group

bias_report = {
    "max_group_mean": float(max_group),
    "min_group_mean": float(min_group),
    "disparity_ratio": float(disparity_ratio),
    "disparity_difference": float(disparity_diff)
}

with open("databias/slices/fairness_disparity.json", "w") as f:
    json.dump(bias_report, f, indent=2)

print("\n📈 Fairness disparity metrics saved → databias/slices/fairness_disparity.json")
print(json.dumps(bias_report, indent=2))

# ======================================================
# 6. Upload mitigation results to GCS
# ======================================================
BUCKET_NAME = "citeconnect-test-bucket"
GCS_PREFIX = "bias_mitigation/"

print("\n📤 Uploading mitigation results to GCS...")

# Upload mitigated dataset
mitigated_dataset = "data/combined_gcs_data_balanced.parquet"
if os.path.exists(mitigated_dataset):
    dataset_blob_name = f"{GCS_PREFIX}combined_gcs_data_balanced.parquet"
    if upload_to_gcs(mitigated_dataset, BUCKET_NAME, dataset_blob_name):
        print(f"✅ Uploaded mitigated dataset → gs://{BUCKET_NAME}/{dataset_blob_name}")

# Upload slice summary JSON
slice_summary = "databias/slices/slice_summary.json"
if os.path.exists(slice_summary):
    slice_blob_name = f"{GCS_PREFIX}slice_summary.json"
    if upload_to_gcs(slice_summary, BUCKET_NAME, slice_blob_name):
        print(f"✅ Uploaded slice summary → gs://{BUCKET_NAME}/{slice_blob_name}")

# Upload fairness disparity JSON
fairness_file = "databias/slices/fairness_disparity.json"
if os.path.exists(fairness_file):
    fairness_blob_name = f"{GCS_PREFIX}fairness_disparity.json"
    if upload_to_gcs(fairness_file, BUCKET_NAME, fairness_blob_name):
        print(f"✅ Uploaded fairness metrics → gs://{BUCKET_NAME}/{fairness_blob_name}")

# Upload field slicing bias plot
field_plot = "databias/slices/field_slicing_bias.png"
if os.path.exists(field_plot):
    plot_blob_name = f"{GCS_PREFIX}plots/field_slicing_bias.png"
    if upload_to_gcs(field_plot, BUCKET_NAME, plot_blob_name):
        print(f"✅ Uploaded field slicing plot → gs://{BUCKET_NAME}/{plot_blob_name}")

print("\n✅ Bias mitigation complete! Results uploaded to GCS.")
