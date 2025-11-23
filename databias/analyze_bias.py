import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
import ast
import os
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.storage_helpers import upload_to_gcs

# ============================================================
# 1. Load the combined or sample dataset
# ============================================================
DATA_PATH = "data/combined_gcs_data.parquet"  # or local single parquet
df = pd.read_parquet(DATA_PATH)

print(f"Loaded dataset with {len(df)} rows and {len(df.columns)} columns")
print("Columns:", df.columns.tolist())

# ============================================================
# 2. Basic Cleaning & Normalization
# ============================================================
# Convert JSON strings or lists into Python lists for analysis
def parse_json_column(x):
    if pd.isna(x):
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            try:
                return ast.literal_eval(x)
            except Exception:
                return [x]
    return [x]

if "fieldsOfStudy" in df.columns:
    df["fieldsOfStudy"] = df["fieldsOfStudy"].apply(parse_json_column)

# Replace missing numeric values
for col in ["year", "citationCount", "intro_length"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# ============================================================
# 3. TEMPORAL BIAS — Distribution over years
# ============================================================
if "year" in df.columns:
    plt.figure(figsize=(8,4))
    sns.histplot(df["year"], bins=20, color="steelblue", kde=True)
    plt.title("Temporal Bias: Distribution of Papers by Year")
    plt.xlabel("Publication Year")
    plt.ylabel("Paper Count")
    os.makedirs("databias/plots", exist_ok=True)
    plt.savefig("databias/plots/temporal_bias.png", bbox_inches="tight")
    plt.close()

    year_counts = df["year"].value_counts().sort_index()
    print("\nTemporal Bias Summary:")
    print(year_counts.tail(10))
else:
    print("\n No 'year' column found for temporal bias analysis.")

# ============================================================
# 4. FIELD BIAS — Dominance of certain fields of study
# ============================================================
if "fieldsOfStudy" in df.columns:
    all_fields = df["fieldsOfStudy"].explode().value_counts()
    print("\n🎓 Field Bias Summary (Top 10 Fields):")
    print(all_fields.head(10))

    plt.figure(figsize=(8,4))
    sns.barplot(x=all_fields.head(10).values, y=all_fields.head(10).index, palette="crest")
    plt.title("🎓 Field Bias: Top 10 Most Common Research Domains")
    plt.xlabel("Paper Count")
    plt.ylabel("Field of Study")
    plt.savefig("databias/plots/field_bias.png", bbox_inches="tight")
    plt.close()
else:
    print("\nNo 'fieldsOfStudy' column found for field bias analysis.")

# ============================================================
# 5. CITATION BIAS — Detect popularity skew
# ============================================================
if "citationCount" in df.columns:
    avg_cite = df["citationCount"].mean()
    median_cite = df["citationCount"].median()
    top10_mean = df["citationCount"].nlargest(int(0.1 * len(df))).mean()

    print("\n Citation Bias Summary:")
    print(f"Average citations: {avg_cite:.2f}")
    print(f"Median citations: {median_cite:.2f}")
    print(f"Top 10% avg citations: {top10_mean:.2f}")
    print(f"Skew ratio (Top10% / Median): {top10_mean / (median_cite + 1):.2f}")

    plt.figure(figsize=(8,4))
    sns.histplot(df["citationCount"], bins=30, color="tomato", log_scale=True)
    plt.title(" Citation Bias: Citation Count Distribution (log scale)")
    plt.xlabel("Citation Count (log)")
    plt.ylabel("Paper Count")
    plt.savefig("databias/plots/citation_bias.png", bbox_inches="tight")
    plt.close()
else:
    print("\n No 'citationCount' column found for citation bias analysis.")

# ============================================================
# 6. ABSTRACT / QUALITY BIAS — By intro length and content quality
# ============================================================
if "intro_length" in df.columns and "content_quality" in df.columns:
    quality_order = ["low", "medium", "high"]
    plt.figure(figsize=(7,4))
    sns.boxplot(x="content_quality", y="intro_length", data=df, order=quality_order, palette="coolwarm")
    plt.title(" Abstract/Quality Bias: Intro Length by Content Quality")
    plt.xlabel("Content Quality")
    plt.ylabel("Introduction Length")
    plt.savefig("databias/plots/quality_bias.png", bbox_inches="tight")
    plt.close()

    print("\n Abstract / Quality Bias Summary:")
    print(df.groupby("content_quality")["intro_length"].describe())
else:
    print("\n Missing 'intro_length' or 'content_quality' columns for quality bias analysis.")

# ============================================================
# 7. Save summary statistics
# ============================================================
summary_path = "databias/bias_summary.json"
summary = {
    "temporal_bias": df["year"].describe().to_dict() if "year" in df else None,
    "field_bias": df["fieldsOfStudy"].explode().value_counts().head(10).to_dict() if "fieldsOfStudy" in df else None,
    "citation_bias": {
        "average": float(avg_cite) if "citationCount" in df else None,
        "median": float(median_cite) if "citationCount" in df else None,
        "top10_mean": float(top10_mean) if "citationCount" in df else None
    } if "citationCount" in df else None,
}
with open(summary_path, "w") as f:
    json.dump(summary, f, indent=2)

print(f"\n Saved bias summary → {summary_path}")
print("Plots saved in databias/plots/")

# ============================================================
# 8. Upload results to GCS
# ============================================================
BUCKET_NAME = "citeconnect-test-bucket"
GCS_PREFIX = "bias_analysis/"

print("\n Uploading analysis results to GCS...")

# Upload summary JSON
summary_blob_name = f"{GCS_PREFIX}bias_summary.json"
if upload_to_gcs(summary_path, BUCKET_NAME, summary_blob_name):
    print(f"Uploaded bias summary → gs://{BUCKET_NAME}/{summary_blob_name}")

# Upload plots
plot_files = [
    "databias/plots/temporal_bias.png",
    "databias/plots/field_bias.png",
    "databias/plots/citation_bias.png",
    "databias/plots/quality_bias.png"
]

for plot_file in plot_files:
    if os.path.exists(plot_file):
        plot_name = os.path.basename(plot_file)
        plot_blob_name = f"{GCS_PREFIX}plots/{plot_name}"
        if upload_to_gcs(plot_file, BUCKET_NAME, plot_blob_name):
            print(f"Uploaded {plot_name} → gs://{BUCKET_NAME}/{plot_blob_name}")

print("\nBias analysis complete! Results uploaded to GCS.")
