"""
databias/visualization_generator.py
-----------------------------------
Generates comprehensive visualization plots for dashboard analysis.
All plots are saved to GCS for later use in dashboards.
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
from datetime import datetime
from pathlib import Path
from google.cloud import storage
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10

# GCS Configuration
BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'citeconnect-test-bucket')
OUTPUT_PREFIX = "databias_v2/visualizations/"
LOCAL_OUTPUT_DIR = "/opt/airflow/databias_v2/visualizations"


def setup_gcs():
    """Initialize GCS client"""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    return client, bucket


def save_plot_to_gcs(fig, filename, bucket):
    """Save matplotlib figure to GCS and local cache"""
    # Save locally
    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)
    local_path = os.path.join(LOCAL_OUTPUT_DIR, filename)
    fig.savefig(local_path, bbox_inches='tight', dpi=150)
    logger.info(f"💾 Saved plot locally: {local_path}")
    
    # Upload to GCS
    gcs_path = OUTPUT_PREFIX + filename
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    logger.info(f"📤 Uploaded plot to GCS: gs://{BUCKET_NAME}/{gcs_path}")
    
    plt.close(fig)
    return f"gs://{BUCKET_NAME}/{gcs_path}"


def plot_collection_overview(df, bucket):
    """
    Overview of papers collected per subdomain.
    Bar chart showing paper counts by subdomain.
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Plot 1: Papers per subdomain
    subdomain_counts = df['subdomain'].value_counts().head(20)
    colors = sns.color_palette("viridis", len(subdomain_counts))
    
    ax1.barh(range(len(subdomain_counts)), subdomain_counts.values, color=colors)
    ax1.set_yticks(range(len(subdomain_counts)))
    ax1.set_yticklabels(subdomain_counts.index)
    ax1.set_xlabel('Number of Papers')
    ax1.set_title('Papers Collected per Subdomain (Top 20)')
    ax1.invert_yaxis()
    
    # Add value labels
    for i, v in enumerate(subdomain_counts.values):
        ax1.text(v + 5, i, str(v), va='center')
    
    # Plot 2: Papers per domain
    if 'domain' in df.columns:
        domain_counts = df['domain'].value_counts()
        ax2.pie(domain_counts.values, labels=domain_counts.index, autopct='%1.1f%%',
                colors=sns.color_palette("Set2", len(domain_counts)))
        ax2.set_title('Paper Distribution by Domain')
    
    plt.tight_layout()
    return save_plot_to_gcs(fig, "collection_overview.png", bucket)


def plot_citation_analysis(df, bucket):
    """
    Citation statistics across subdomains.
    Box plot and violin plot showing citation distribution.
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # Get top 10 subdomains by count
    top_subdomains = df['subdomain'].value_counts().head(10).index
    df_top = df[df['subdomain'].isin(top_subdomains)]
    
    # Plot 1: Box plot of citations
    df_top.boxplot(column='citationCount', by='subdomain', ax=ax1)
    ax1.set_xlabel('Subdomain')
    ax1.set_ylabel('Citation Count')
    ax1.set_title('Citation Distribution by Subdomain (Top 10)')
    plt.sca(ax1)
    plt.xticks(rotation=45, ha='right')
    
    # Plot 2: Violin plot
    sns.violinplot(data=df_top, x='subdomain', y='citationCount', ax=ax2, palette='muted')
    ax2.set_xlabel('Subdomain')
    ax2.set_ylabel('Citation Count')
    ax2.set_title('Citation Density by Subdomain (Top 10)')
    ax2.tick_params(axis='x', rotation=45)
    plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    return save_plot_to_gcs(fig, "citation_analysis.png", bucket)


def plot_quality_metrics(df, bucket):
    """
    Content extraction quality metrics.
    Shows extraction success rates and quality distribution.
    """
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Plot 1: Extraction method breakdown
    if 'extraction_method' in df.columns:
        method_counts = df['extraction_method'].value_counts()
        ax1.pie(method_counts.values, labels=method_counts.index, autopct='%1.1f%%',
                colors=sns.color_palette("pastel", len(method_counts)))
        ax1.set_title('Content Extraction Methods Used')
    
    # Plot 2: Content quality distribution
    if 'content_quality' in df.columns:
        quality_counts = df['content_quality'].value_counts()
        colors = {'high': 'green', 'medium': 'orange', 'low': 'red'}
        bar_colors = [colors.get(q, 'gray') for q in quality_counts.index]
        ax2.bar(range(len(quality_counts)), quality_counts.values, color=bar_colors)
        ax2.set_xticks(range(len(quality_counts)))
        ax2.set_xticklabels(quality_counts.index)
        ax2.set_ylabel('Number of Papers')
        ax2.set_title('Content Quality Distribution')
        
        # Add percentage labels
        total = quality_counts.sum()
        for i, v in enumerate(quality_counts.values):
            ax2.text(i, v + 5, f'{v}\n({v/total*100:.1f}%)', ha='center')
    
    # Plot 3: Success rate by subdomain (top 10)
    if 'has_intro' in df.columns:
        top_subdomains = df['subdomain'].value_counts().head(10).index
        df_top = df[df['subdomain'].isin(top_subdomains)]
        
        success_rates = df_top.groupby('subdomain')['has_intro'].agg(['sum', 'count'])
        success_rates['rate'] = (success_rates['sum'] / success_rates['count'] * 100)
        success_rates = success_rates.sort_values('rate', ascending=True)
        
        colors_rate = ['green' if r >= 80 else 'orange' if r >= 50 else 'red' 
                       for r in success_rates['rate']]
        
        ax3.barh(range(len(success_rates)), success_rates['rate'], color=colors_rate)
        ax3.set_yticks(range(len(success_rates)))
        ax3.set_yticklabels(success_rates.index)
        ax3.set_xlabel('Success Rate (%)')
        ax3.set_title('Content Extraction Success Rate by Subdomain (Top 10)')
        ax3.axvline(x=80, color='green', linestyle='--', alpha=0.3, label='Target: 80%')
        ax3.legend()
        
        # Add value labels
        for i, v in enumerate(success_rates['rate']):
            ax3.text(v + 1, i, f'{v:.1f}%', va='center')
    
    # Plot 4: Intro length distribution
    if 'intro_length' in df.columns:
        df_with_intro = df[df['intro_length'] > 0]
        ax4.hist(df_with_intro['intro_length'], bins=50, color='skyblue', edgecolor='black', alpha=0.7)
        ax4.set_xlabel('Introduction Length (characters)')
        ax4.set_ylabel('Number of Papers')
        ax4.set_title('Introduction Length Distribution')
        ax4.axvline(df_with_intro['intro_length'].median(), color='red', 
                   linestyle='--', label=f'Median: {int(df_with_intro["intro_length"].median())}')
        ax4.legend()
    
    plt.tight_layout()
    return save_plot_to_gcs(fig, "quality_metrics.png", bucket)


def plot_temporal_analysis(df, bucket):
    """
    Temporal analysis - papers over time, citations vs year.
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Plot 1: Papers published per year
    if 'year' in df.columns:
        df_with_year = df[df['year'].notna() & (df['year'] > 1990) & (df['year'] < 2030)]
        year_counts = df_with_year['year'].value_counts().sort_index()
        
        ax1.plot(year_counts.index, year_counts.values, marker='o', linewidth=2, markersize=4)
        ax1.fill_between(year_counts.index, year_counts.values, alpha=0.3)
        ax1.set_xlabel('Publication Year')
        ax1.set_ylabel('Number of Papers')
        ax1.set_title('Papers Published per Year')
        ax1.grid(True, alpha=0.3)
    
    # Plot 2: Average citations vs publication year
    if 'year' in df.columns and 'citationCount' in df.columns:
        df_citations = df_with_year.groupby('year')['citationCount'].agg(['mean', 'median'])
        
        ax2.plot(df_citations.index, df_citations['mean'], marker='o', label='Mean Citations', linewidth=2)
        ax2.plot(df_citations.index, df_citations['median'], marker='s', label='Median Citations', linewidth=2)
        ax2.set_xlabel('Publication Year')
        ax2.set_ylabel('Citation Count')
        ax2.set_title('Citation Trends Over Time')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return save_plot_to_gcs(fig, "temporal_analysis.png", bucket)


def plot_bias_metrics(fairness_df, disparity_stats, bucket):
    """
    Bias and fairness metrics visualization.
    Shows disparity across subdomains.
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Plot 1: Citation disparity across subdomains
    top_subdomains = fairness_df.head(15)
    colors = sns.color_palette("RdYlGn_r", len(top_subdomains))
    
    bars = ax1.barh(range(len(top_subdomains)), top_subdomains['mean_citations'].values, color=colors)
    ax1.set_yticks(range(len(top_subdomains)))
    ax1.set_yticklabels(top_subdomains.index)
    ax1.set_xlabel('Mean Citations')
    ax1.set_title('Citation Disparity Across Subdomains (Top 15)')
    ax1.invert_yaxis()
    
    # Add median line
    median_citations = top_subdomains['mean_citations'].median()
    ax1.axvline(median_citations, color='blue', linestyle='--', alpha=0.5, label=f'Median: {median_citations:.1f}')
    ax1.legend()
    
    # Add value labels
    for i, v in enumerate(top_subdomains['mean_citations'].values):
        ax1.text(v + 5, i, f'{v:.1f}', va='center')
    
    # Plot 2: Disparity metrics gauge
    disparity_ratio = disparity_stats.get('disparity_ratio', 0)
    disparity_diff = disparity_stats.get('disparity_difference', 0)
    
    # Create a summary text box
    summary_text = f"""
    DISPARITY METRICS
    
    Ratio: {disparity_ratio:.2f}x
    Difference: {disparity_diff:.1f} citations
    
    Max Group: {disparity_stats.get('max_group_mean', 0):.1f}
    Min Group: {disparity_stats.get('min_group_mean', 0):.1f}
    
    Status: {'⚠️ THRESHOLD EXCEEDED' if disparity_diff > 50 else '✅ ACCEPTABLE'}
    """
    
    ax2.text(0.5, 0.5, summary_text, transform=ax2.transAxes,
            fontsize=14, verticalalignment='center', horizontalalignment='center',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
            family='monospace')
    ax2.axis('off')
    ax2.set_title('Bias Disparity Summary')
    
    plt.tight_layout()
    return save_plot_to_gcs(fig, "bias_metrics.png", bucket)


def plot_disparity_trends(bucket):
    """
    Historical disparity trends (if multiple runs exist).
    Shows how bias changes over time/runs.
    """
    # Try to load historical data
    history_file = os.path.join(LOCAL_OUTPUT_DIR, "disparity_history.json")
    
    history = []
    if os.path.exists(history_file):
        with open(history_file, 'r') as f:
            history = json.load(f)
    
    if len(history) < 2:
        logger.info("⚠️ Not enough historical data for trend analysis (need 2+ runs)")
        return None
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # Convert to DataFrame
    df_history = pd.DataFrame(history)
    df_history['timestamp'] = pd.to_datetime(df_history['timestamp'])
    
    # Plot 1: Disparity ratio over time
    ax1.plot(df_history['timestamp'], df_history['disparity_ratio'], 
             marker='o', linewidth=2, markersize=8, color='coral')
    ax1.fill_between(df_history['timestamp'], df_history['disparity_ratio'], alpha=0.3, color='coral')
    ax1.set_ylabel('Disparity Ratio')
    ax1.set_title('Bias Disparity Ratio Trend Over Time')
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # Plot 2: Total papers and underrepresented subdomains
    ax2_twin = ax2.twinx()
    
    ax2.plot(df_history['timestamp'], df_history['total_papers'], 
             marker='s', linewidth=2, color='blue', label='Total Papers')
    ax2_twin.plot(df_history['timestamp'], df_history['underrep_subdomains'], 
                  marker='^', linewidth=2, color='red', label='Underrep Subdomains')
    
    ax2.set_xlabel('Timestamp')
    ax2.set_ylabel('Total Papers', color='blue')
    ax2_twin.set_ylabel('Underrepresented Subdomains', color='red')
    ax2.set_title('Collection Progress Over Time')
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(True, alpha=0.3)
    
    # Combine legends
    lines1, labels1 = ax2.get_legend_handles_labels()
    lines2, labels2 = ax2_twin.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    plt.tight_layout()
    return save_plot_to_gcs(fig, "disparity_trends.png", bucket)


def plot_mitigation_effectiveness(bucket):
    """
    Compare bias metrics before and after mitigation.
    Shows effectiveness of mitigation efforts.
    """
    # Load mitigation report if exists
    mitigation_report_path = os.path.join(LOCAL_OUTPUT_DIR.replace('visualizations', 'slices'), 
                                         "mitigation_report.json")
    
    if not os.path.exists(mitigation_report_path):
        logger.info("⚠️ No mitigation report found, skipping effectiveness plot")
        return None
    
    with open(mitigation_report_path, 'r') as f:
        mitigation_report = json.load(f)
    
    if mitigation_report.get('status') != 'success':
        logger.info("⚠️ Mitigation was not successful, skipping effectiveness plot")
        return None
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Create summary
    papers_collected = mitigation_report.get('papers_collected', 0)
    search_terms = mitigation_report.get('search_terms_used', [])
    
    summary_text = f"""
    MITIGATION EFFECTIVENESS
    
    Papers Collected: {papers_collected}
    Subdomains Targeted: {len(search_terms)}
    
    Targeted Subdomains:
    {chr(10).join(['  • ' + term for term in search_terms[:10]])}
    
    Status: ✅ Mitigation Completed
    """
    
    ax.text(0.5, 0.5, summary_text, transform=ax.transAxes,
           fontsize=12, verticalalignment='center', horizontalalignment='center',
           bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.5),
           family='monospace')
    ax.axis('off')
    ax.set_title('Mitigation Collection Summary', fontsize=16, fontweight='bold')
    
    plt.tight_layout()
    return save_plot_to_gcs(fig, "mitigation_effectiveness.png", bucket)


def plot_subdomain_heatmap(df, bucket):
    """
    Heatmap showing subdomain vs domain relationships with citation counts.
    """
    if 'domain' not in df.columns or 'subdomain' not in df.columns:
        logger.info("⚠️ Missing domain/subdomain columns for heatmap")
        return None
    
    # Create pivot table: subdomain vs domain with mean citations
    pivot = df.pivot_table(
        values='citationCount',
        index='subdomain',
        columns='domain',
        aggfunc='mean',
        fill_value=0
    )
    
    # Take top 15 subdomains by total citations
    top_subdomains = df.groupby('subdomain')['citationCount'].sum().nlargest(15).index
    pivot_filtered = pivot.loc[pivot.index.isin(top_subdomains)]
    
    fig, ax = plt.subplots(figsize=(12, 10))
    sns.heatmap(pivot_filtered, annot=True, fmt='.1f', cmap='YlOrRd', 
                ax=ax, cbar_kws={'label': 'Mean Citations'})
    ax.set_title('Mean Citations: Subdomain vs Domain (Top 15 Subdomains)')
    ax.set_xlabel('Domain')
    ax.set_ylabel('Subdomain')
    
    plt.tight_layout()
    return save_plot_to_gcs(fig, "subdomain_domain_heatmap.png", bucket)


def update_disparity_history(disparity_stats, total_papers, underrep_count):
    """
    Append current run's disparity metrics to historical tracking.
    """
    history_file = os.path.join(LOCAL_OUTPUT_DIR, "disparity_history.json")
    
    history = []
    if os.path.exists(history_file):
        try:
            with open(history_file, 'r') as f:
                history = json.load(f)
        except:
            history = []
    
    # Add current run
    history.append({
        'timestamp': datetime.now().isoformat(),
        'disparity_ratio': disparity_stats.get('disparity_ratio', 0),
        'disparity_difference': disparity_stats.get('disparity_difference', 0),
        'total_papers': total_papers,
        'underrep_subdomains': underrep_count
    })
    
    # Save updated history
    os.makedirs(os.path.dirname(history_file), exist_ok=True)
    with open(history_file, 'w') as f:
        json.dump(history, f, indent=2)
    
    # Upload to GCS
    try:
        client, bucket = setup_gcs()
        blob = bucket.blob(OUTPUT_PREFIX + "disparity_history.json")
        blob.upload_from_filename(history_file)
        logger.info(f"📤 Updated disparity history in GCS")
    except Exception as e:
        logger.warning(f"⚠️ Could not upload history to GCS: {e}")


def generate_all_visualizations(df, fairness_df, disparity_stats):
    """
    Generate all visualization plots for dashboard.
    
    Args:
        df: Full dataset DataFrame
        fairness_df: Fairness metrics by subdomain
        disparity_stats: Disparity statistics dictionary
        
    Returns:
        Dictionary with GCS paths to all generated plots
    """
    logger.info("\n" + "="*60)
    logger.info("GENERATING DASHBOARD VISUALIZATIONS")
    logger.info("="*60)
    
    client, bucket = setup_gcs()
    
    plots = {}
    
    try:
        # Generate all plots
        logger.info("\n📊 Generating collection overview...")
        plots['collection_overview'] = plot_collection_overview(df, bucket)
        
        logger.info("\n📈 Generating citation analysis...")
        plots['citation_analysis'] = plot_citation_analysis(df, bucket)
        
        logger.info("\n✨ Generating quality metrics...")
        plots['quality_metrics'] = plot_quality_metrics(df, bucket)
        
        logger.info("\n📅 Generating temporal analysis...")
        plots['temporal_analysis'] = plot_temporal_analysis(df, bucket)
        
        logger.info("\n⚖️ Generating bias metrics...")
        plots['bias_metrics'] = plot_bias_metrics(fairness_df, disparity_stats, bucket)
        
        logger.info("\n🗺️ Generating subdomain heatmap...")
        plots['subdomain_heatmap'] = plot_subdomain_heatmap(df, bucket)
        
        logger.info("\n📉 Generating disparity trends...")
        trends_plot = plot_disparity_trends(bucket)
        if trends_plot:
            plots['disparity_trends'] = trends_plot
        
        logger.info("\n💊 Generating mitigation effectiveness...")
        mitigation_plot = plot_mitigation_effectiveness(bucket)
        if mitigation_plot:
            plots['mitigation_effectiveness'] = mitigation_plot
        
        # Update historical tracking
        underrep_count = len([v for v in fairness_df['mean_citations'].values 
                             if v < fairness_df['mean_citations'].median()])
        update_disparity_history(disparity_stats, len(df), underrep_count)
        
        # Save plot manifest
        manifest = {
            'timestamp': datetime.now().isoformat(),
            'total_plots': len(plots),
            'plots': plots,
            'stats': {
                'total_papers': len(df),
                'total_subdomains': len(df['subdomain'].unique()),
                'disparity_ratio': disparity_stats.get('disparity_ratio', 0),
                'disparity_difference': disparity_stats.get('disparity_difference', 0)
            }
        }
        
        manifest_path = os.path.join(LOCAL_OUTPUT_DIR, "plot_manifest.json")
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        # Upload manifest to GCS
        blob = bucket.blob(OUTPUT_PREFIX + "plot_manifest.json")
        blob.upload_from_filename(manifest_path)
        
        logger.info("\n" + "="*60)
        logger.info(f"✅ Generated {len(plots)} visualization plots")
        logger.info(f"📦 All plots saved to: gs://{BUCKET_NAME}/{OUTPUT_PREFIX}")
        logger.info("="*60)
        
        return plots
        
    except Exception as e:
        logger.error(f"❌ Error generating visualizations: {e}")
        return {}


# Main entry point
if __name__ == "__main__":
    # Load data and generate visualizations
    from databias.slicing_bias_analysis import load_and_analyze_data
    
    df, fairness_df, disparity_stats = load_and_analyze_data()
    plots = generate_all_visualizations(df, fairness_df, disparity_stats)
    
    print(f"\n✅ Generated {len(plots)} plots for dashboard")

