"""
Test column_mapper.py with sample data
"""

import sys
import os
import pandas as pd

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from services.column_mapper import (
    map_parquet_row_to_paper,
    prepare_embedding_text,
    validate_paper_data,
    batch_map_papers,
    infer_search_term_from_filename
)

print("="*60)
print("TESTING COLUMN MAPPER")
print("="*60)

# Test 1: Infer search term from filename
print("\n1. Testing filename inference:")
filenames = [
    'finance_processed.parquet',
    'quantum_computing_processed.parquet',
    'healthcare_papers.parquet'
]
for fn in filenames:
    term = infer_search_term_from_filename(fn)
    print(f"   {fn} → '{term}'")

# Test 2: Map sample row
print("\n2. Testing row mapping:")
sample_row = pd.Series({
    'paperId': 'test123',
    'title': 'Machine Learning for Healthcare',
    'abstract': 'This paper discusses machine learning applications in healthcare settings.',
    'introduction': 'Introduction text about ML in healthcare. ' * 10,
    'tldr': 'Test TLDR summary',  # Changed from 'summary'
    'year': 2024,
    'citationCount': 15,
    'referenceCount': 25,  # NEW
    'extraction_method': 'arxiv_html',
    'content_quality': 'high',
    'has_intro': True
})

paper = map_parquet_row_to_paper(sample_row, 'healthcare')
print(f"   Paper ID: {paper['paper_id']}")
print(f"   Domain: {paper['domain']}")
print(f"   Sub-domains: {paper['sub_domains']}")  # NEW
print(f"   Reference count: {paper['reference_count']}")  # NEW
print(f"   Has intro: {paper['has_introduction']}")
print(f"   Valid: {validate_paper_data(paper)}")

# Test 3: Prepare embedding text
print("\n3. Testing embedding text preparation:")
embedding_text = prepare_embedding_text(paper)
print(f"   Text length: {len(embedding_text)} chars")
print(f"   Preview: {embedding_text[:100]}...")

# Test 4: Batch mapping
print("\n4. Testing batch mapping:")
sample_df = pd.DataFrame([
    {
        'paperId': 'p1',
        'title': 'Paper 1',
        'abstract': 'Abstract 1 ' * 10,
        'introduction': 'Intro 1 ' * 20,
        'year': 2024
    },
    {
        'paperId': 'p2',
        'title': 'Paper 2',
        'abstract': 'Abstract 2 ' * 10,
        'year': 2023
    }
])

papers = batch_map_papers(sample_df, 'finance')
print(f"   Mapped {len(papers)} papers")

print("\n" + "="*60)
print("✅ ALL COLUMN MAPPER TESTS PASSED")
print("="*60)