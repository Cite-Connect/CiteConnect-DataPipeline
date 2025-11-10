import logging
import re
import time
import pandas as pd
from pathlib import Path
from .semantic_scholar_client import search_semantic_scholar
from .processor import process_papers
from .gcs_uploader import upload_to_gcs
import os

GCS_BUCKET = os.getenv('GCS_BUCKET_NAME')


def save_results(data, search_term, output_dir):
    print(f"Saving results for: {search_term}")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^\w\s-]", "", search_term).replace(" ", "_")
    filename = f"{output_dir}/{safe_name}_{int(time.time())}.parquet"

    pd.DataFrame(data).to_parquet(filename, index=False)
    logging.info(f"Saved: {filename}")

    upload_to_gcs(filename, GCS_BUCKET, f"raw/{safe_name}.parquet")

# def ingest_papers(search_terms, limit=10, output_dir="data/raw"):
#     setup_logging()
#     for term in search_terms:
#         logging.info(f"Starting: {term}")
#         papers = search_semantic_scholar(term, limit)
#         if not papers:
#             logging.warning(f"No results for: {term}")
#             continue

#         processed = process_papers(papers, term)
#         save_results(processed, term, output_dir)

# def collect_papers(search_terms, limit=10, output_dir="data/raw"):
#     collected_files = []
    
#     for term in search_terms:
#         logging.info(f"Collecting: {term}")
        
#         # Get papers from API
#         papers = search_semantic_scholar(term, limit)
#         if not papers:
#             logging.warning(f"No results for: {term}")
#             continue
        
#         # Save raw papers
#         Path(output_dir).mkdir(parents=True, exist_ok=True)
#         safe_name = re.sub(r"[^\w\s-]", "", term).replace(" ", "_")
#         filename = f"{output_dir}/raw_{safe_name}_{int(time.time())}.parquet"
        
#         pd.DataFrame(papers).to_parquet(filename, index=False)
#         logging.info(f"Saved: {filename} ({len(papers)} papers)")

#         save_results(papers, term, output_dir)
    
#     return collected_files


# def extract_and_process_papers(collection_results, output_dir="data/processed"):
#     processed_files = []

#     for result in collection_results:
#         raw_file_path = result['file_path']
        
#         logging.info(f"Processing...")
        
#         # Load raw papers
#         raw_papers_df = pd.read_parquet(raw_file_path)
#         raw_papers = raw_papers_df.to_dict('records')
        
#         # Call your existing process_papers function
#         processed = process_papers(raw_papers, search_term)
        
#         if not processed:
#             continue
        
#         # Save processed papers
#         Path(output_dir).mkdir(parents=True, exist_ok=True)
#         safe_name = re.sub(r"[^\w\s-]", "", search_term).replace(" ", "_")
#         processed_filename = f"{output_dir}/processed_{safe_name}_{int(time.time())}.parquet"
        
#         pd.DataFrame(processed).to_parquet(processed_filename, index=False)
#         logging.info(f"Processed: {processed_filename} ({len(processed)} papers)")
    
#     return processed_files

def collect_papers_only(search_terms, limit=10, output_dir="data/raw"):
    collection_results = []
    
    for term in search_terms:
        logging.info(f"Starting collection: {term}")
        papers = search_semantic_scholar(term, limit)
        
        if not papers:
            logging.warning(f"No results for: {term}")
            continue
        
        # Save raw papers (both local and GCS)
        local_file, gcs_path = save_raw_results(papers, term, output_dir)
        
        collection_results.append({
            'search_term': term,
            'local_file': local_file,
            'gcs_path': gcs_path,
            'paper_count': len(papers)
        })
        
        logging.info(f"Collected {len(papers)} papers for: {term}")
    
    return collection_results

# Function 2: Processing only (saves processed data to GCS) 
def process_collected_papers(collection_results, output_dir="data/processed"):
    processing_results = []
    
    for result in collection_results:
        term = result['search_term']
        local_file = result['local_file']
        
        logging.info(f"Starting processing: {term}")
        
        # Load raw papers from local file
        papers = load_raw_papers(local_file)
        
        # Process papers (your existing logic)
        processed = process_papers(papers, term)
        
        # Save processed results (both local and GCS)
        processed_local, processed_gcs = save_processed_results(processed, term, output_dir)
        
        processing_results.append({
            'search_term': term,
            'raw_local': local_file,
            'raw_gcs': result['gcs_path'],
            'processed_local': processed_local,
            'processed_gcs': processed_gcs,
            'processed_count': len(processed)
        })
        
        logging.info(f"Processed {len(processed)} papers for: {term}")
    
    return processing_results

# Updated save functions for raw data
def save_raw_results(data, search_term, output_dir):
    print(f"Saving raw results for: {search_term}")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^\w\s-]", "", search_term).replace(" ", "_")
    
    # Local filename
    local_filename = f"{output_dir}/raw_{safe_name}_{int(time.time())}.parquet"
    
    # Save locally
    pd.DataFrame(data).to_parquet(local_filename, index=False)
    logging.info(f"Saved locally: {local_filename}")
    
    # Upload to GCS in raw folder
    gcs_path = f"raw/{safe_name}_raw.parquet"
    upload_to_gcs(local_filename, GCS_BUCKET, gcs_path)
    
    return local_filename, gcs_path

# Updated save function for processed data  
def save_processed_results(data, search_term, output_dir):
    print(f"Saving processed results for: {search_term}")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^\w\s-]", "", search_term).replace(" ", "_")
    
    # Local filename
    local_filename = f"{output_dir}/processed_{safe_name}_{int(time.time())}.parquet"
    
    # Save locally
    pd.DataFrame(data).to_parquet(local_filename, index=False)
    logging.info(f"Saved locally: {local_filename}")
    
    # Upload to GCS in processed folder
    gcs_path = f"processed/{safe_name}_processed.parquet"
    upload_to_gcs(local_filename, GCS_BUCKET, gcs_path)
    
    return local_filename, gcs_path


def load_raw_papers(file_path):
    """Load raw papers from local parquet file"""
    import pandas as pd
    df = pd.read_parquet(file_path)
    return df.to_dict('records')

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("terms", nargs="+", help="Search terms")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--output", default="data/raw")

    args = parser.parse_args()
    # collect_papers(args.terms, args.limit, args.output)
