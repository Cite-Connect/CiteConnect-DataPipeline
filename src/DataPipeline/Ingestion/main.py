import logging
import re
import time
import pandas as pd
import asyncio
import aiohttp
from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from .semantic_scholar_client import search_semantic_scholar, search_semantic_scholar_async, get_papers_by_ids_batch_async
from .processor import process_papers
from .gcs_uploader import upload_to_gcs, upload_to_gcs_async
from .api_key_manager import APIKeyManager
import os

GCS_BUCKET = os.getenv('GCS_BUCKET_NAME')

# Initialize API key manager (shared across all collection tasks)
api_key_manager = APIKeyManager()


async def extract_reference_paper_ids_from_api(
    session: aiohttp.ClientSession,
    paper_ids: List[str],
    api_key: Optional[str] = None,
    max_references_per_paper: int = None
) -> List[str]:
    """
    Extract unique paperIds from references of seed papers using the /references endpoint
    
    Args:
        session: aiohttp ClientSession
        paper_ids: List of seed paper IDs
        api_key: Optional API key
        max_references_per_paper: Limit references per paper (None = all)
    
    Returns:
        List of unique reference paperIds
    """
    from .semantic_scholar_client import get_papers_references_batch_async
    
    if not paper_ids:
        return []
    
    # Fetch references for all papers in parallel
    references_dict = await get_papers_references_batch_async(
        session,
        paper_ids,
        api_key=api_key,
        max_references_per_paper=max_references_per_paper,
        max_concurrent=10
    )
    
    # Collect all unique reference IDs
    reference_ids = set()
    total_refs = 0
    
    for paper_id, ref_ids in references_dict.items():
        total_refs += len(ref_ids)
        reference_ids.update(ref_ids)
    
    unique_ids = list(reference_ids)
    
    # Log statistics
    if max_references_per_paper:
        logging.info(f"📚 Extracted {len(unique_ids)} unique reference paperIds from {len(paper_ids)} seed papers")
        logging.info(f"   📊 Limit: {max_references_per_paper} refs/paper → {total_refs} total refs → {len(unique_ids)} unique (after deduplication)")
        logging.info(f"   💡 Note: Multiple seed papers may reference the same paper, so unique count is lower")
    else:
        logging.info(f"📚 Extracted {len(unique_ids)} unique reference paperIds from {len(paper_ids)} seed papers")
        logging.info(f"   📊 {total_refs} total refs → {len(unique_ids)} unique (after deduplication)")
    
    return unique_ids


async def collect_referenced_papers(
    session: aiohttp.ClientSession,
    seed_paper_ids: List[str],
    api_key: Optional[str] = None,
    max_references_per_paper: int = None,
    max_concurrent_fetches: int = 10
) -> List[Dict]:
    """
    Collect all papers referenced by seed papers using the /references endpoint
    
    Args:
        session: aiohttp ClientSession
        seed_paper_ids: List of seed paper IDs
        api_key: Optional API key (will rotate if multiple keys available)
        max_references_per_paper: Max references to collect per seed paper
        max_concurrent_fetches: Max concurrent API calls
    
    Returns:
        List of referenced papers
    """
    from .api_key_manager import APIKeyManager
    from .semantic_scholar_client import get_papers_by_ids_batch_async
    
    # Extract unique reference paperIds using the /references endpoint
    reference_ids = await extract_reference_paper_ids_from_api(
        session,
        seed_paper_ids,
        api_key=api_key,
        max_references_per_paper=max_references_per_paper
    )
    
    if not reference_ids:
        logging.info("📚 No references found in seed papers")
        return []
    
    # Adaptive concurrency: reduce if only 1 API key available
    key_manager = APIKeyManager()
    num_keys = key_manager.get_key_count()
    
    # Adjust concurrency based on available keys
    if num_keys == 1:
        # Single key: be more conservative (2-3 concurrent requests)
        adaptive_concurrent = min(3, max_concurrent_fetches)
        logging.info(f"📚 Fetching {len(reference_ids)} referenced papers...")
        logging.info(f"   ⚠️  Only 1 API key available - using {adaptive_concurrent} concurrent requests (reduced from {max_concurrent_fetches})")
        logging.info(f"   💡 This will take longer but avoid rate limits")
    elif num_keys == 2:
        # 2 keys: moderate concurrency
        adaptive_concurrent = min(5, max_concurrent_fetches)
        logging.info(f"📚 Fetching {len(reference_ids)} referenced papers...")
        logging.info(f"   Using {adaptive_concurrent} concurrent requests with {num_keys} API keys")
    else:
        # 3+ keys: can use higher concurrency
        adaptive_concurrent = max_concurrent_fetches
        logging.info(f"📚 Fetching {len(reference_ids)} referenced papers...")
        logging.info(f"   Using {adaptive_concurrent} concurrent requests with {num_keys} API keys (key rotation enabled)")
    
    # Use API key rotation for reference fetching (distribute load across keys)
    # Fetch referenced papers in parallel with key rotation
    referenced_papers = await get_papers_by_ids_batch_async(
        session,
        reference_ids,
        api_key=api_key,  # Will rotate if api_key_manager has multiple keys
        max_concurrent=adaptive_concurrent  # Use adaptive concurrency
    )
    
    logging.info(f"✅ Successfully fetched {len(referenced_papers)}/{len(reference_ids)} referenced papers")
    
    return referenced_papers


def save_results(data, search_term, output_dir):
    print(f"Saving results for: {search_term}")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^\w\s-]", "", search_term).replace(" ", "_")
    filename = f"{output_dir}/{safe_name}_{int(time.time())}.parquet"

    pd.DataFrame(data).to_parquet(filename, index=False)
    logging.info(f"Saved: {filename}")

    upload_to_gcs(filename, GCS_BUCKET, f"raw/{safe_name}.parquet")


def collect_single_term(term, limit, output_dir, api_key):
    """
    Collect papers for a single search term (used in parallel execution)
    
    Args:
        term: Search term
        limit: Number of papers to collect
        output_dir: Output directory
        api_key: API key to use for this request
    
    Returns:
        Dictionary with collection results or None if failed
    """
    try:
        key_display = api_key[:8] + "..." if api_key and len(api_key) > 8 else ("none" if not api_key else api_key)
        logging.info(f"[{term}] Starting collection with API key {key_display}...")
        papers = search_semantic_scholar(term, limit, api_key=api_key)
        
        if not papers:
            logging.warning(f"[{term}] No results found")
            return None
        
        # Save raw papers (both local and GCS) - this is already parallelized per term
        local_file, gcs_path = save_raw_results(papers, term, output_dir)
        
        result = {
            'search_term': term,
            'local_file': local_file,
            'gcs_path': gcs_path,
            'paper_count': len(papers)
        }
        
        logging.info(f"[{term}] ✅ Collected {len(papers)} papers")
        return result
    except Exception as e:
        logging.error(f"[{term}] ❌ Collection failed: {e}")
        return None


def collect_papers_only(search_terms, limit=10, output_dir="data/raw", max_workers=None):
    """
    Parallelized paper collection with API key rotation
    
    Args:
        search_terms: List of search terms
        limit: Papers per term
        output_dir: Output directory
        max_workers: Max parallel workers (default: number of API keys or len(search_terms))
    
    Returns:
        List of collection result dictionaries
    """
    collection_results = []
    
    # Determine number of workers
    num_keys = api_key_manager.get_key_count()
    if max_workers is None:
        # Use number of API keys if available, otherwise use number of terms (up to reasonable limit)
        max_workers = min(num_keys if num_keys > 0 else 1, len(search_terms), 10)
    
    logging.info(f"🚀 Starting parallel collection: {len(search_terms)} terms, {max_workers} workers, {num_keys} API key(s)")
    
    # Use ThreadPoolExecutor for parallel collection
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks with API key rotation
        future_to_term = {
            executor.submit(collect_single_term, term, limit, output_dir, api_key_manager.get_key()): term
            for term in search_terms
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_term):
            term = future_to_term[future]
            try:
                result = future.result()
                if result:
                    collection_results.append(result)
            except Exception as e:
                logging.error(f"[{term}] Task failed with exception: {e}")
    
    logging.info(f"✅ Parallel collection complete: {len(collection_results)}/{len(search_terms)} terms succeeded")
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
    
    # Upload to GCS in raw folder (use safe_name for cleaner paths)
    gcs_path = f"raw/{safe_name}_{int(time.time())}.parquet"
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


async def collect_and_process_pipeline_async(
    search_terms,
    limit=10,
    raw_output_dir="data/raw",
    processed_output_dir="data/processed",
    max_concurrent_collections=None
):
    """
    Async pipeline with overlapping collection and preprocessing.
    
    Pattern: As soon as papers are collected for term N, start preprocessing term N
    while simultaneously collecting papers for term N+1.
    
    Args:
        search_terms: List of search terms
        limit: Papers per term
        raw_output_dir: Directory for raw data
        processed_output_dir: Directory for processed data
        max_concurrent_collections: Max concurrent API calls (default: number of API keys)
    
    Returns:
        Dictionary with both collection and processing results
    """
    collection_results = []
    processing_results = []
    
    num_keys = api_key_manager.get_key_count()
    if max_concurrent_collections is None:
        max_concurrent_collections = min(num_keys if num_keys > 0 else 1, len(search_terms), 10)
    
    # Calculate preprocessing executor size: enough to handle multiple search terms in parallel
    # Each search term uses 5 workers internally, so we need at least len(search_terms) workers
    # Add some buffer for overlapping operations
    preprocessing_executor_size = max(len(search_terms) * 2, 10)
    
    logging.info(f"🚀 Starting async pipeline: {len(search_terms)} terms, {max_concurrent_collections} concurrent collections")
    logging.info(f"📊 Pipeline pattern: Collection and preprocessing will overlap")
    logging.info(f"⚙️ Preprocessing: {len(search_terms)} terms in parallel, {preprocessing_executor_size} total workers")
    logging.info(f"⏱️  Staggered start: {3}s delay between each search term to avoid rate limits")
    
    # Create dedicated ThreadPoolExecutor for preprocessing multiple search terms
    preprocessing_executor = ThreadPoolExecutor(max_workers=preprocessing_executor_size)
    
    try:
        # Create aiohttp session for async requests
        connector = aiohttp.TCPConnector(limit=max_concurrent_collections * 2)
        timeout = aiohttp.ClientTimeout(total=60)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Semaphore to limit concurrent collections
            collection_semaphore = asyncio.Semaphore(max_concurrent_collections)
            
            # Add staggered start to avoid overwhelming API
            async def collect_term(term, api_key, delay=0):
                """Collect papers for a single term, including references"""
                # Stagger requests to avoid rate limits
                if delay > 0:
                    logging.info(f"[{term}] ⏳ Waiting {delay}s before starting (staggered start)...")
                    await asyncio.sleep(delay)
                
                async with collection_semaphore:
                    try:
                        key_display = api_key[:8] + "..." if api_key and len(api_key) > 8 else ("none" if not api_key else api_key)
                        logging.info(f"[{term}] 🔍 Starting collection (API key: {key_display})")
                        
                        # Warn about potentially problematic search terms
                        if len(term.strip()) <= 3:
                            logging.warning(f"⚠️  Search term '{term}' is very short. Consider using a more specific term.")
                            logging.warning(f"   Example: 'LLM' → 'Large Language Models' or 'LLMs'")
                        
                        # Async API call for seed papers
                        seed_papers = await search_semantic_scholar_async(session, term, limit, api_key=api_key)
                        
                        if not seed_papers:
                            logging.warning(f"[{term}] No results found")
                            logging.warning(f"   💡 Suggestions:")
                            logging.warning(f"      - Try a more specific or longer search term")
                            logging.warning(f"      - Check if the term is spelled correctly")
                            logging.warning(f"      - For acronyms, try the full term (e.g., 'LLM' → 'Large Language Models')")
                            return None
                        
                        logging.info(f"[{term}] ✅ Collected {len(seed_papers)} seed papers")
                        
                        # Extract paper IDs from seed papers
                        seed_paper_ids = [p.get("paperId") for p in seed_papers if p.get("paperId")]
                        
                        # Collect referenced papers using the /references endpoint
                        referenced_papers = []
                        max_refs_per_paper = int(os.getenv('MAX_REFERENCES_PER_PAPER', '0'))  # 0 = collect all, >0 = limit per paper
                        if max_refs_per_paper == 0:
                            logging.info(f"[{term}] 📚 Collecting ALL references from {len(seed_paper_ids)} seed papers using /references endpoint...")
                        else:
                            logging.info(f"[{term}] 📚 Collecting references (max {max_refs_per_paper} per seed paper) from {len(seed_paper_ids)} seed papers...")
                        
                        # Always collect references (0 = all, >0 = limited)
                        references_dict = {}  # Map paper_id -> list of reference IDs
                        if seed_paper_ids:
                            try:
                                # Get references for each seed paper (to add to metadata)
                                from .semantic_scholar_client import get_papers_references_batch_async
                                references_dict = await get_papers_references_batch_async(
                                    session,
                                    seed_paper_ids,
                                    api_key=api_key,
                                    max_references_per_paper=max_refs_per_paper if max_refs_per_paper > 0 else None,
                                    max_concurrent=10
                                )
                                
                                # Add references_id to each seed paper
                                for paper in seed_papers:
                                    paper_id = paper.get("paperId")
                                    if paper_id and paper_id in references_dict:
                                        paper["references_id"] = references_dict[paper_id]
                                    else:
                                        paper["references_id"] = []
                                
                                # Extract unique reference IDs from all seed papers
                                all_reference_ids = set()
                                for ref_ids in references_dict.values():
                                    all_reference_ids.update(ref_ids)
                                
                                # Now collect the actual referenced papers using the unique IDs
                                if all_reference_ids:
                                    from .semantic_scholar_client import get_papers_by_ids_batch_async
                                    from .api_key_manager import APIKeyManager
                                    
                                    key_manager = APIKeyManager()
                                    num_keys = key_manager.get_key_count()
                                    adaptive_concurrent = min(3, 10) if num_keys == 1 else min(5, 10) if num_keys == 2 else 10
                                    
                                    referenced_papers = await get_papers_by_ids_batch_async(
                                        session,
                                        list(all_reference_ids),
                                        api_key=api_key,
                                        max_concurrent=adaptive_concurrent
                                    )
                                    logging.info(f"[{term}] ✅ Collected {len(referenced_papers)}/{len(all_reference_ids)} referenced papers")
                                else:
                                    referenced_papers = []
                                    logging.info(f"[{term}] ✅ No referenced papers to collect")
                                
                                # Ensure we have a list (not None)
                                if referenced_papers is None:
                                    logging.warning(f"[{term}] ⚠️  collect_referenced_papers returned None, using empty list")
                                    referenced_papers = []
                            except Exception as e:
                                logging.error(f"[{term}] ❌ Error collecting references: {e}")
                                logging.warning(f"[{term}] ⚠️  Continuing with seed papers only (no references)")
                                referenced_papers = []
                                # Still add empty references_id to seed papers
                                for paper in seed_papers:
                                    paper["references_id"] = []
                        else:
                            logging.warning(f"[{term}] ⚠️  No paperIds found in seed papers, skipping reference collection")
                            referenced_papers = []
                            # Add empty references_id to seed papers
                            for paper in seed_papers:
                                paper["references_id"] = []
                        
                        # Combine seed papers and referenced papers
                        all_papers = seed_papers + referenced_papers
                        
                        # Save raw papers locally
                        safe_name = re.sub(r"[^\w\s-]", "", term).replace(" ", "_")
                        local_filename = f"{raw_output_dir}/raw_{safe_name}_{int(time.time())}.parquet"
                        Path(raw_output_dir).mkdir(parents=True, exist_ok=True)
                        pd.DataFrame(all_papers).to_parquet(local_filename, index=False)
                        logging.info(f"[{term}] 💾 Saved locally: {local_filename}")
                        
                        # Async GCS upload to raw_v2/ folder
                        gcs_path = f"raw_v2/{safe_name}_v2_{int(time.time())}.parquet"
                        await upload_to_gcs_async(local_filename, GCS_BUCKET, gcs_path)
                        
                        result = {
                            'search_term': term,
                            'local_file': local_filename,
                            'gcs_path': gcs_path,
                            'paper_count': len(all_papers),
                            'seed_paper_count': len(seed_papers),
                            'referenced_paper_count': len(referenced_papers),
                            'papers': all_papers  # Keep papers in memory for immediate processing
                        }
                        
                        logging.info(f"[{term}] ✅ Total collected: {len(seed_papers)} seed + {len(referenced_papers)} referenced = {len(all_papers)} papers")
                        return result
                    except Exception as e:
                        logging.error(f"[{term}] ❌ Collection failed: {e}")
                        return None
            
            async def process_term(collection_result):
                """Process papers for a single term (runs in dedicated preprocessing executor)"""
                if not collection_result:
                    return None
                
                term = collection_result['search_term']
                papers = collection_result.get('papers', [])
                
                # Validate papers is a list
                if not isinstance(papers, list):
                    logging.error(f"[{term}] ❌ Invalid papers data: expected list, got {type(papers)}")
                    return None
                
                if not papers:
                    logging.warning(f"[{term}] ⚠️  No papers to process")
                    return None
                
                try:
                    logging.info(f"[{term}] 🔧 Starting preprocessing for {len(papers)} papers...")
                    
                    # Run CPU-bound processing in dedicated preprocessing executor
                    # This allows multiple search terms to be processed in parallel
                    loop = asyncio.get_event_loop()
                    processed = await loop.run_in_executor(
                        preprocessing_executor,  # Use dedicated executor instead of None
                        process_papers,
                        papers,
                        term
                    )
                    
                    # Validate processed result
                    if processed is None:
                        logging.error(f"[{term}] ❌ process_papers returned None")
                        return None
                    
                    if not isinstance(processed, list):
                        logging.error(f"[{term}] ❌ process_papers returned invalid type: {type(processed)}")
                        return None
                    
                    # Save processed results locally
                    safe_name = re.sub(r"[^\w\s-]", "", term).replace(" ", "_")
                    processed_filename = f"{processed_output_dir}/processed_{safe_name}_{int(time.time())}.parquet"
                    Path(processed_output_dir).mkdir(parents=True, exist_ok=True)
                    pd.DataFrame(processed).to_parquet(processed_filename, index=False)
                    logging.info(f"[{term}] 💾 Saved processed locally: {processed_filename}")
                    
                    # Async GCS upload to processed_v2/ folder
                    processed_gcs_path = f"processed_v2/{safe_name}_v2_processed.parquet"
                    await upload_to_gcs_async(processed_filename, GCS_BUCKET, processed_gcs_path)
                    
                    result = {
                        'search_term': term,
                        'raw_local': collection_result['local_file'],
                        'raw_gcs': collection_result['gcs_path'],
                        'processed_local': processed_filename,
                        'processed_gcs': processed_gcs_path,
                        'processed_count': len(processed)
                    }
                    
                    logging.info(f"[{term}] ✅ Processed {len(processed)} papers")
                    return result
                except Exception as e:
                    logging.error(f"[{term}] ❌ Processing failed: {e}")
                    return None
            
            # Pipeline: Start collecting all terms with staggered delays to avoid rate limits
            collection_tasks = []
            stagger_delay = 3  # 3 seconds between each search term start
            for i, term in enumerate(search_terms):
                api_key = api_key_manager.get_key()
                delay = i * stagger_delay  # Stagger: 0s, 3s, 6s, etc.
                task = collect_term(term, api_key, delay=delay)
                collection_tasks.append(task)
            
            # Process results as they complete (pipeline pattern)
            processing_tasks = []
            for collection_task in asyncio.as_completed(collection_tasks):
                collection_result = await collection_task
                if collection_result:
                    collection_results.append(collection_result)
                    # Start processing immediately (overlapping with next collection)
                    processing_task = process_term(collection_result)
                    processing_tasks.append(processing_task)
            
            # Wait for all processing to complete
            for processing_task in asyncio.as_completed(processing_tasks):
                processing_result = await processing_task
                if processing_result:
                    processing_results.append(processing_result)
    finally:
        # Clean up the preprocessing executor
        preprocessing_executor.shutdown(wait=True)
    
    logging.info(f"✅ Pipeline complete: {len(collection_results)}/{len(search_terms)} collected, {len(processing_results)} processed")
    
    return {
        'collection_results': collection_results,
        'processing_results': processing_results
    }


def collect_and_process_pipeline(
    search_terms,
    limit=10,
    raw_output_dir="data/raw",
    processed_output_dir="data/processed",
    use_async=True
):
    """
    Wrapper function that can use async pipeline or fall back to sync
    
    Args:
        search_terms: List of search terms
        limit: Papers per term
        raw_output_dir: Directory for raw data
        processed_output_dir: Directory for processed data
        use_async: Whether to use async pipeline (default: True)
    
    Returns:
        Dictionary with both collection and processing results
    """
    if use_async:
        # Run async pipeline
        return asyncio.run(collect_and_process_pipeline_async(
            search_terms,
            limit,
            raw_output_dir,
            processed_output_dir
        ))
    else:
        # Fall back to sequential processing
        collection_results = collect_papers_only(search_terms, limit, raw_output_dir)
        processing_results = process_collected_papers(collection_results, processed_output_dir)
        return {
            'collection_results': collection_results,
            'processing_results': processing_results
        }

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("terms", nargs="+", help="Search terms")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--output", default="data/raw")

    args = parser.parse_args()
    # collect_papers(args.terms, args.limit, args.output)
