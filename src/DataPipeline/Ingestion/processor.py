import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .content_extractor import ContentExtractor
from .metadata_utils import extract_metadata


def _process_single_paper(paper, search_term, index, total):
    """Process a single paper (used by ThreadPoolExecutor)"""
    extractor = ContentExtractor()  # Create extractor per thread for thread safety
        title = paper.get("title", "Unknown")
    logging.info(f"\nPaper {index}/{total}: {title[:60]}")

        record = extract_metadata(paper, search_term)
        content, method, quality = extractor.extract_content(paper)

        if content:
            record.update({
                "introduction": content,
                "extraction_method": method,
                "content_quality": quality,
                "has_intro": True,
                "intro_length": len(content),
                "status": f"success_{method}"
            })
        else:
            record["fail_reason"] = "extraction_failed"

    return record


def process_papers(papers, search_term, debug=False, max_workers=5):
    """
    Process papers in parallel using ThreadPoolExecutor.
    
    Args:
        papers: List of paper dictionaries
        search_term: Search term for metadata
        debug: Debug flag (unused, kept for compatibility)
        max_workers: Number of parallel workers (default: 5)
    
    Returns:
        List of processed paper records
    """
    if not papers:
        return []
    
    results = []
    total = len(papers)
    
    # Process papers in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_index = {
            executor.submit(_process_single_paper, paper, search_term, i+1, total): i
            for i, paper in enumerate(papers)
        }
        
        # Collect results as they complete (maintain order)
        results_dict = {}
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                results_dict[index] = future.result()
            except Exception as e:
                logging.error(f"Error processing paper {index+1}: {e}")
                # Create a failed record
                paper = papers[index]
                record = extract_metadata(paper, search_term)
                record["fail_reason"] = f"processing_error: {str(e)}"
                results_dict[index] = record
        
        # Reconstruct results in original order
        results = [results_dict[i] for i in range(total)]

    return results
