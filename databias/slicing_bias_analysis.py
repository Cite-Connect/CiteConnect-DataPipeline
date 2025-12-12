"""
databias/slicing_bias_analysis.py
--------------------------------
Performs bias slicing and fairness analysis on CiteConnect papers.
Now fully cloud-ready: loads from GCS, saves mitigated data and bias metrics
back to GCS, and triggers an email alert if thresholds are exceeded.
"""

import os
import pandas as pd
import numpy as np
from fairlearn.metrics import MetricFrame
import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import storage
from io import BytesIO
import json
import ast
import smtplib
from email.mime.text import MIMEText
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------
# 1️⃣ GCS Configuration (Auto-detect environment)
# ------------------------------------------------------
BUCKET_NAME = "citeconnect-test-bucket"
# Load from processed_v2 only (final processed files with complete data)
# raw_v2 is excluded as it contains only basic metadata without content extraction
SOURCE_FOLDERS = ["processed_v2/"]
OUTPUT_PREFIX = "databias_v2/"  # Changed from bias_outputs/ to databias_v2/
LOCAL_OUTPUT_DIR = "/opt/airflow/databias_v2/slices"  # Local cache for inter-task communication

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)
print(f"✅ Connected to project: {client.project}")

# ------------------------------------------------------
# 2️⃣ Helper to Save to GCS (primary) and Local (cache)
# ------------------------------------------------------
def save_to_gcs_and_local(dest_blob, data_dict=None, local_path=None):
    """
    Save to GCS first (primary storage), then save locally for inter-task communication.
    
    Args:
        dest_blob: Destination path in GCS (relative to OUTPUT_PREFIX)
        data_dict: Dictionary to save as JSON (optional)
        local_path: Local file path to upload (optional, if data_dict not provided)
    """
    gcs_path = OUTPUT_PREFIX + dest_blob
    blob = bucket.blob(gcs_path)
    
    if data_dict is not None:
        # Save dict as JSON to GCS
        blob.upload_from_string(json.dumps(data_dict, indent=2), content_type='application/json')
        print(f"📤 Uploaded to GCS: gs://{BUCKET_NAME}/{gcs_path}")
        
        # Also save locally for inter-task communication
        local_full_path = os.path.join(LOCAL_OUTPUT_DIR, dest_blob.split('/')[-1])
        os.makedirs(os.path.dirname(local_full_path), exist_ok=True)
        with open(local_full_path, 'w') as f:
            json.dump(data_dict, f, indent=2)
        print(f"💾 Cached locally: {local_full_path}")
        
    elif local_path and os.path.exists(local_path):
        # Upload existing local file to GCS
        blob.upload_from_filename(local_path)
        print(f"📤 Uploaded {local_path} → gs://{BUCKET_NAME}/{gcs_path}")
    else:
        raise ValueError("Must provide either data_dict or valid local_path")

# ------------------------------------------------------
# 3️⃣ Helper to Parse JSON/List Columns
# ------------------------------------------------------
def parse_json_column(x):
    """Convert JSON strings or lists into Python lists for proper analysis"""
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

# ------------------------------------------------------
# 4️⃣ Load data from processed pipeline stage
# ------------------------------------------------------
print(f"📥 Loading parquet files from processed pipeline stage:")
print(f"   • gs://{BUCKET_NAME}/processed_v2/ (final processed files)")
print()

all_dfs = []
total_files = 0

for folder in SOURCE_FOLDERS:
    print(f"📂 Scanning {folder}...")
    
    # List all parquet files in this folder
    blobs = list(bucket.list_blobs(prefix=folder))
    parquet_files = [b for b in blobs if b.name.endswith('.parquet')]
    
    if not parquet_files:
        print(f"  ⚠️  No parquet files found in {folder}")
        continue
    
    print(f"  Found {len(parquet_files)} parquet files")
    
    # Load files from this folder
    for i, blob in enumerate(parquet_files, 1):
        filename = blob.name.split('/')[-1]
        size_mb = blob.size / (1024 * 1024)
        print(f"    [{i}/{len(parquet_files)}] {filename} ({size_mb:.1f} MB)...", end="")
        
        try:
            data = blob.download_as_bytes()
            temp_df = pd.read_parquet(BytesIO(data))
            temp_df['source_folder'] = folder  # Track which folder it came from
            temp_df['source_file'] = blob.name
            all_dfs.append(temp_df)
            print(f" ✅ {len(temp_df)} papers")
            total_files += 1
        except Exception as e:
            print(f" ❌ Failed: {e}")
    
    print()

if not all_dfs:
    print("❌ No data loaded successfully from any folder")
    exit(1)

# Combine all dataframes
print("🔗 Combining all datasets...")
df = pd.concat(all_dfs, ignore_index=True)
print(f"✅ Loaded {len(df):,} papers total from {total_files} files across {len(SOURCE_FOLDERS)} folders")

# Deduplicate by paperId (including new mitigation data)
print(f"\n🔄 Deduplicating papers by paperId...")
initial_count = len(df)
df = df.drop_duplicates(subset='paperId', keep='first')
duplicates_removed = initial_count - len(df)
print(f"✅ Removed {duplicates_removed:,} duplicate papers")
print(f"   Final unique papers: {len(df):,}")

print(f"\n   Breakdown by folder (after deduplication):")
for folder in SOURCE_FOLDERS:
    count = len(df[df['source_folder'] == folder])
    if count > 0:
        print(f"   • {folder:20s} {count:,} papers")
print()

# Clean numeric & categorical fields
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df["citationCount"] = pd.to_numeric(df["citationCount"], errors="coerce").fillna(0)
df["intro_length"] = pd.to_numeric(df["intro_length"], errors="coerce").fillna(0)

# Parse fieldsOfStudy as a list (handles both strings and actual lists) - for reference only
df["fieldsOfStudy"] = df["fieldsOfStudy"].apply(parse_json_column)

df["content_quality"] = df["content_quality"].astype(str)

# Ensure local output directory exists
os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

# ------------------------------------------------------
# 5️⃣ Domain-Level Analysis (User-Defined Domains)
# ------------------------------------------------------
# Filter papers with domain information
df_with_domains = df[df['domain'].notna() & (df['domain'] != '')]

if len(df_with_domains) == 0:
    print("❌ ERROR: No papers with domain information found!")
    print("   Papers need 'domain' field")
    print("   Available columns:", df.columns.tolist())
    exit(1)

print(f"✅ Analyzing {len(df_with_domains)} papers across domains")
if len(df) - len(df_with_domains) > 0:
    print(f"   (Excluded {len(df) - len(df_with_domains)} papers without domain tags)")

# Extract subdomain for supplementary analysis
def get_subdomain(row):
    """Extract subdomain from sub_domains array or search_term"""
    if pd.notna(row.get('sub_domains')) and row.get('sub_domains'):
        if isinstance(row['sub_domains'], list) and len(row['sub_domains']) > 0:
            return row['sub_domains'][0]
        elif isinstance(row['sub_domains'], str):
            try:
                subdomains = ast.literal_eval(row['sub_domains'])
                if isinstance(subdomains, list) and len(subdomains) > 0:
                    return subdomains[0]
            except:
                pass
    return row.get('search_term', '')

df_with_domains['subdomain'] = df_with_domains.apply(get_subdomain, axis=1)

# ------------------------------------------------------
# 6️⃣ Slice Summaries (Domain-Based)
# ------------------------------------------------------
slices = {
    "year": df_with_domains.groupby("year")["citationCount"].mean().to_dict(),
    "domain": df_with_domains.groupby("domain")["citationCount"].mean().to_dict(),
    "subdomain": df_with_domains.groupby("subdomain")["citationCount"].mean().to_dict(),
    "content_quality": df_with_domains.groupby("content_quality")["intro_length"].mean().to_dict(),
}
save_to_gcs_and_local("slices/slice_summary.json", data_dict=slices)

# ------------------------------------------------------
# 7️⃣ Fairlearn MetricFrame (Domain-Level Analysis)
# ------------------------------------------------------
y_true = df_with_domains["citationCount"].to_numpy()
y_pred = df_with_domains["citationCount"].to_numpy()
sensitive = df_with_domains["domain"].to_numpy()

metric_frame = MetricFrame(
    metrics={"mean_citations": lambda y_true, y_pred: np.mean(y_true)},
    y_true=y_true,
    y_pred=y_pred,
    sensitive_features=sensitive
)
fairness_df = metric_frame.by_group.sort_values("mean_citations", ascending=False)
print("\n📊 Avg citation count by domain:")
print(fairness_df)

# Also calculate subdomain-level stats for reference
subdomain_stats = df_with_domains.groupby('subdomain')['citationCount'].agg(['mean', 'count']).sort_values('mean', ascending=False)
print("\n📊 Avg citation count by subdomain (for reference):")
print(subdomain_stats.head(20))

plt.figure(figsize=(10,6))
colors = sns.color_palette("RdYlGn_r", len(fairness_df))
sns.barplot(
    x=fairness_df["mean_citations"].values,
    y=fairness_df.index,
    palette=colors
)
plt.title("Average Citation Count by Domain")
plt.xlabel("Mean Citations")
plt.ylabel("Domain")
plt.tight_layout()

# Save plot locally first, then upload to GCS
local_plot_path = os.path.join(LOCAL_OUTPUT_DIR, "domain_slicing_bias.png")
plt.savefig(local_plot_path, bbox_inches="tight")
save_to_gcs_and_local("slices/domain_slicing_bias.png", local_path=local_plot_path)

# ------------------------------------------------------
# 8️⃣ Collection-Level Mitigation Recommendations (Subdomain-Based)
# ------------------------------------------------------
# Domain-Subdomain mapping (user's configuration)
SUBDOMAINS_BY_DOMAIN = {
    "AI": [
        "machine learning",
        "Neural Networks",
        "computer vision",
        "natural language processing",
        "Graph Neural Networks",
    ],
    "Healthcare": [
        "Disease Prediction",
        "Drug Discovery",
        "Electronic Health Records",
        "Medical Imaging",
        "Genomics",
    ],
    "Quantum": [
        "Quantum Algorithms",
        "Quantum Error Correction",
        "Quantum Machine Learning",
        "Quantum Simulation",
        "Quantum Cryptograph security"
    ],
    "Finance": [
        "Algorithmic Trading",
        "Credit Risk Scoring",
        "Fraud Detection",
        "Portfolio Optimization",
        "Blockchain Cryptocurrency",
    ],
}

# Create reverse mapping: subdomain -> domain
SUBDOMAIN_TO_DOMAIN = {}
for domain, subdomains in SUBDOMAINS_BY_DOMAIN.items():
    for subdomain in subdomains:
        SUBDOMAIN_TO_DOMAIN[subdomain.lower().strip()] = domain

# ------------------------------------------------------
# 8️⃣ Identify Underrepresented Domains
# ------------------------------------------------------
# Calculate domain-level statistics
domain_counts = df_with_domains.groupby("domain").size()
median_domain_count = domain_counts.median()
mean_domain_count = domain_counts.mean()

print(f"\n📊 Domain Statistics:")
print(f"   Total domains: {len(domain_counts)}")
print(f"   Median papers per domain: {median_domain_count:.0f}")
print(f"   Mean papers per domain: {mean_domain_count:.0f}")
print(f"\n   Papers by domain:")
for domain, count in domain_counts.sort_values(ascending=False).items():
    status = "✅" if count >= median_domain_count else "⚠️"
    print(f"   {status} {domain}: {count} papers")

# Identify underrepresented domains (below median)
underrep_domains = domain_counts[domain_counts < median_domain_count].to_dict()

print(f"\n⚠️ Underrepresented Domains: {len(underrep_domains)}")
for domain, count in sorted(underrep_domains.items(), key=lambda x: x[1]):
    papers_needed = int(median_domain_count - count)
    print(f"   • {domain}: {count} papers (need {papers_needed} more)")

# ------------------------------------------------------
# 9️⃣ Generate Collection Recommendations (Domain-Based)
# ------------------------------------------------------
# For each underrepresented domain, list its subdomains as search terms
collection_recommendations = []

for domain, current_count in sorted(underrep_domains.items(), key=lambda x: x[1]):
    papers_needed = int(median_domain_count - current_count)
    
    # Get subdomains for this domain
    available_subdomains = SUBDOMAINS_BY_DOMAIN.get(domain, [])
    
    if not available_subdomains:
        print(f"⚠️ Warning: No subdomains defined for domain '{domain}'")
        continue
    
    # Distribute papers needed across available subdomains
    papers_per_subdomain = max(10, papers_needed // len(available_subdomains))
    
    collection_recommendations.append({
        "domain": domain,
        "current_count": int(current_count),
        "target_count": int(median_domain_count),
        "papers_needed": papers_needed,
        "available_subdomains": available_subdomains,  # List of subdomains to try
        "papers_per_subdomain": papers_per_subdomain,  # Initial estimate
        "priority": "high" if current_count < median_domain_count * 0.5 else "medium",
        "mitigation_strategy": "iterative",  # Use subdomains one by one until target reached
        "mitigation_config": {
            "COLLECTION_DOMAIN": domain,
            "AVAILABLE_SEARCH_TERMS": available_subdomains,  # Mitigation will iterate through these
            "TARGET_PAPERS": papers_needed
        }
    })

print(f"\n📋 Collection Recommendations Generated: {len(collection_recommendations)}")
for rec in collection_recommendations:
    print(f"   • {rec['domain']}: Need {rec['papers_needed']} papers")
    print(f"     Available subdomains to try: {', '.join(rec['available_subdomains'][:3])}...")

# Save collection recommendations to GCS and local cache
recommendations_data = {
    "analysis_timestamp": pd.Timestamp.now().isoformat(),
    "analysis_type": "domain_based",
    "total_underrepresented_domains": len(underrep_domains),
    "total_recommendations": len(collection_recommendations),
    "median_domain_count": int(median_domain_count),
    "mean_domain_count": int(mean_domain_count),
    "domain_subdomain_mapping": SUBDOMAINS_BY_DOMAIN,
    "recommendations": collection_recommendations,
    "total_papers_needed": sum(rec['papers_needed'] for rec in collection_recommendations)
}

save_to_gcs_and_local("slices/collection_recommendations.json", data_dict=recommendations_data)

print(f"\n⚖️ Collection-Level Mitigation Recommendations (Domain-Based):")
print(f"   Underrepresented domains: {len(underrep_domains)}")
print(f"   Median paper count per domain: {int(median_domain_count)}")
print(f"   Total papers needed: {recommendations_data['total_papers_needed']}")
print(f"\n   Top 5 domains needing more papers:")
for rec in collection_recommendations[:5]:
    print(f"   • {rec['domain']:20s} - Need {rec['papers_needed']:3d} more papers (currently: {rec['current_count']})")
    print(f"     Available subdomains ({len(rec['available_subdomains'])}): {', '.join(rec['available_subdomains'][:3])}...")

# ------------------------------------------------------
# 9️⃣ Fairness Disparity Check
# ------------------------------------------------------
max_mean = fairness_df["mean_citations"].max()
min_mean = fairness_df["mean_citations"].min()
disparity_ratio = (max_mean / (min_mean + 1e-6)) if min_mean > 0 else np.inf
disparity_diff = max_mean - min_mean

fairness_stats = {
    "max_group_mean": float(max_mean),
    "min_group_mean": float(min_mean),
    "disparity_ratio": float(disparity_ratio),
    "disparity_difference": float(disparity_diff),
    "analysis_note": "Computed on exploded field data - each paper contributes to all its fields",
    "analysis_timestamp": pd.Timestamp.now().isoformat()
}
save_to_gcs_and_local("slices/fairness_disparity.json", data_dict=fairness_stats)

print("📈 Fairness disparity metrics saved → GCS and local cache")
print(f"   Max group mean: {max_mean:.2f} | Min group mean: {min_mean:.2f}")
print(f"   Disparity ratio: {disparity_ratio:.2f}x | Difference: {disparity_diff:.2f}")

# ------------------------------------------------------
# 🔟 Bias Alert Check (handled by DAG)
# ------------------------------------------------------
THRESHOLD = 50.0  # Alert threshold for disparity difference
print(f"\n🔔 Bias Alert Threshold: {THRESHOLD}")
if disparity_diff > THRESHOLD:
    print(f"   ⚠️  THRESHOLD EXCEEDED! Disparity difference ({disparity_diff:.2f}) > {THRESHOLD}")
    print(f"   → Email alert will be sent by DAG task")
    print(f"   → Collection recommendations generated for mitigation")
else:
    print(f"   ✅ Bias within acceptable limits (difference: {disparity_diff:.2f} ≤ {THRESHOLD})")

print(f"\n✅ Bias analysis complete!")
print(f"   📦 Primary storage: gs://{BUCKET_NAME}/{OUTPUT_PREFIX}slices/")
print(f"   💾 Local cache: {LOCAL_OUTPUT_DIR}/")

# ------------------------------------------------------
# 🎨 Generate Dashboard Visualizations
# ------------------------------------------------------
try:
    from databias.visualization_generator import generate_all_visualizations
    
    logger.info("\n🎨 Generating dashboard visualizations...")
    plots = generate_all_visualizations(df_with_domains, fairness_df, fairness_stats)
    
    print(f"\n📊 Dashboard Visualizations Generated:")
    for plot_name, gcs_path in plots.items():
        print(f"   • {plot_name}: {gcs_path}")
        
except Exception as e:
    print(f"⚠️ Warning: Could not generate visualizations: {e}")
    print("   Bias analysis completed, but plots may be missing")

print(f"\n🎉 All analysis and visualizations complete!")