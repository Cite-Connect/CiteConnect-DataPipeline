"""
Inspect GCS parquet data to debug why papers are being skipped.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.gcs_reader import GCSReader
import pandas as pd

def inspect_data():
    """Inspect GCS parquet data."""
    
    print("\n" + "="*60)
    print("GCS DATA INSPECTION")
    print("="*60 + "\n")
    
    # Read from GCS
    reader = GCSReader(bucket_name="citeconnect-test-bucket", project_id="strange-calling-476017-r5")
    df = reader.read_all_from_domain("healthcare", custom_prefix="raw/", flat_structure=True)
    
    print(f"Total records: {len(df)}\n")
    
    # Check first 10 papers
    sample = df.head(10)
    
    print("=" * 60)
    print("SAMPLE PAPER ANALYSIS (First 10 Papers)")
    print("=" * 60 + "\n")
    
    for i, (idx, paper) in enumerate(sample.iterrows(), 1):
        print(f"\n📄 Paper {i}: {paper.get('paperId', 'N/A')}")
        print(f"   Title: {paper.get('title', 'N/A')[:60]}...")
        print(f"   Has intro field: {'introduction' in paper and pd.notna(paper['introduction'])}")
        print(f"   has_intro flag: {paper.get('has_intro', 'N/A')}")
        
        intro = paper.get('introduction')
        if pd.notna(intro) and intro:
            print(f"   Introduction length: {len(str(intro))} chars")
            print(f"   First 100 chars: {str(intro)[:100]}...")
        else:
            print(f"   Introduction: MISSING or NULL")
        
        print(f"   Extraction method: {paper.get('extraction_method', 'N/A')}")
        print(f"   Content quality: {paper.get('content_quality', 'N/A')}")
        print(f"   Status: {paper.get('status', 'N/A')}")
    
    # Summary statistics
    print("\n" + "="*60)
    print("DATASET STATISTICS")
    print("="*60)
    print(f"Total papers: {len(df)}")
    print(f"Papers with has_intro=True: {df['has_intro'].sum() if 'has_intro' in df else 'N/A'}")
    print(f"Papers with introduction text: {df['introduction'].notna().sum() if 'introduction' in df else 'N/A'}")
    print(f"\nExtraction methods:")
    if 'extraction_method' in df:
        print(df['extraction_method'].value_counts())
    
    print(f"\nContent quality:")
    if 'content_quality' in df:
        print(df['content_quality'].value_counts())
    
    print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    inspect_data()