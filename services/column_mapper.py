"""
Column mapping utilities for CiteConnect.
Maps parquet file columns to PostgreSQL table columns.
Updated for Schema v2.0 with new domain strategy and citation arrays.
"""

import logging
from typing import Dict, Any, Optional, List
import pandas as pd
from datetime import datetime
import re
import os

logger = logging.getLogger(__name__)


# Search term to PRIMARY domain mapping (only 3 domains)
SEARCH_TERM_TO_DOMAIN = {
    'finance': 'fintech',
    'quantum computing': 'quantum_computing',
    'healthcare': 'healthcare',
    # Other search terms don't map to primary domain (will be sub_domains only)
}

# All search terms become sub-domains
SEARCH_TERM_TO_SUBDOMAIN = {
    'finance': 'finance',
    'quantum computing': 'quantum_computing',
    'healthcare': 'healthcare',
    'llm': 'llm',
    'computer vision': 'computer_vision',
    'machine learning': 'machine_learning',
    'transformers': 'transformers',
}


def sanitize_text(text: Any) -> Optional[str]:
    """
    Sanitize text for PostgreSQL insertion.
    Removes null by 2 and other problematic characters.
    
    Args:
        text: Input text (can be str, None, or other types)
    
    Returns:
        Cleaned text or None
    """
    if text is None or pd.isna(text):
        return None
    
    text_str = str(text)
    
    # Remove null bytes (0x00) - PostgreSQL doesn't allow these
    text_str = text_str.replace('\x00', '')
    
    # Remove other control characters
    text_str = re.sub(r'[\x01-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', text_str)
    
    # Strip whitespace
    text_str = text_str.strip()
    
    # Return None if empty
    if not text_str:
        return None
    
    return text_str
# Function to sanitize lists using the provided sanitize_text function
def sanitize_and_filter_id_list(raw_list: Any) -> list[str]:
    if not isinstance(raw_list, list):
        # If the data isn't a list (e.g., it's None or NaN), return an empty list
        return []
        
    cleaned_ids = []
    for item in raw_list:
        # Use sanitize_text on each individual ID
        cleaned_id = sanitize_text(item)
        if cleaned_id:
            # Only append IDs that are not None and not empty strings after cleaning
            cleaned_ids.append(cleaned_id)
            
    return cleaned_ids


def map_parquet_row_to_paper(row: pd.Series, search_term: Optional[str] = None) -> Dict[str, Any]:
    """
    Map a parquet row to PostgreSQL papers table format (Schema v2.0).
    
    Args:
        row: pandas Series from parquet file
        search_term: Search term used to collect this paper
    
    Returns:
        Dictionary ready for PostgreSQL insertion
    """
    # Extract paper_id
    paper_id = row.get('paperId') or row.get('paper_id')
    if not paper_id:
        raise ValueError(f"No paper_id found in row")
    
    # Determine PRIMARY domain from search term (only 3 allowed)
    domain = None
    if search_term:
        domain = SEARCH_TERM_TO_DOMAIN.get(search_term.lower())
    
    # Determine sub_domain from search term (all search terms become sub-domains)
    sub_domains = []
    if search_term:
        sub_domain = SEARCH_TERM_TO_SUBDOMAIN.get(search_term.lower())
        if sub_domain:
            sub_domains = [sub_domain]
    
    # Map authors
    authors = row.get('authors', [])
    
    if authors is None:
        authors = []
    elif isinstance(authors, str):
        import json
        try:
            authors = json.loads(authors)
        except:
            authors = [authors]
    elif isinstance(authors, (list, tuple)):
        authors = list(authors)
    else:
        try:
            if pd.isna(authors):
                authors = []
            else:
                authors = [str(authors)]
        except (ValueError, TypeError):
            authors = list(authors) if hasattr(authors, '__iter__') else [str(authors)]
    
    if not isinstance(authors, list):
        authors = [str(authors)]
    authors = [str(a).strip() for a in authors if a and str(a).strip()]
    
    # Map title with sanitization
    title = sanitize_text(row.get('title', ''))
    if not title or len(title) < 10:
        raise ValueError(f"Paper {paper_id} has invalid title")
    
    # Map introduction
    introduction = sanitize_text(
        row.get('introduction') or row.get('intro')
    )
    
    # Map abstract
    abstract = sanitize_text(row.get('abstract'))
    
    # Map tldr (was 'summary' in old schema)
    tldr = sanitize_text(row.get('tldr') or row.get('summary'))
    
    # Map venue
    venue = sanitize_text(row.get('venue'))
    
    # Map year
    year = row.get('year')
    if year is not None and not pd.isna(year):
        try:
            year = int(year)
        except (ValueError, TypeError):
            year = None
    else:
        year = None
    
    # Map citation count
    citation_count = row.get('citationCount', 0)
    if citation_count is not None and not pd.isna(citation_count):
        try:
            citation_count = int(citation_count)
        except (ValueError, TypeError):
            citation_count = 0
    else:
        citation_count = 0
    
    # Map extraction method
    extraction_method = row.get('extraction_method')
    if extraction_method is None or pd.isna(extraction_method):
        extraction_method = 'unknown'
    
    # Map content quality
    content_quality = row.get('content_quality')
    if content_quality is None or pd.isna(content_quality):
        content_quality = 'medium'
    
    # Map has_introduction
    has_intro = row.get('has_intro', False)
    if isinstance(has_intro, str):
        has_intro = has_intro.lower() in ('true', '1', 'yes')
    elif not isinstance(has_intro, bool):
        has_intro = introduction is not None and len(str(introduction)) > 100
    
    # Map intro_length
    intro_length = row.get('intro_length', 0)
    if intro_length is None or pd.isna(intro_length):
        intro_length = 0
    
    if intro_length == 0 and introduction:
        intro_length = len(str(introduction))
    else:
        try:
            intro_length = int(intro_length)
        except (ValueError, TypeError):
            intro_length = 0
    
    # NEW: reference_ids and citation_ids (empty for now, teammate will populate)
    '''    reference_ids = []
    citation_ids = []
    '''
    # Extracting and cleaning the IDs
    raw_reference_ids = row.get('reference_ids')
    raw_citation_ids = row.get('citation_ids')

    reference_ids = sanitize_and_filter_id_list(raw_reference_ids)
    citation_ids = sanitize_and_filter_id_list(raw_citation_ids)
        
    # NEW: Calculate reference_count from parquet if available
    reference_count = row.get('referenceCount', 0)
    if reference_count is None or pd.isna(reference_count):
        reference_count = 0
    else:
        try:
            reference_count = int(reference_count)
        except (ValueError, TypeError):
            reference_count = 0
    
    # Build paper dictionary (NEW SCHEMA)
    paper = {
        'paper_id': str(paper_id),
        'title': title,
        'abstract': abstract,
        'introduction': introduction,
        'tldr': tldr,  # Changed from 'summary'
        'authors': authors,
        'year': year,
        'venue': venue,
        'citation_count': citation_count,
        'domain': domain,  # Only 3 primary domains or NULL
        'domain_confidence': 0.0,  # TODO: Implement domain classification
        'sub_domains': sub_domains,  # NEW: Array of sub-domains
        'reference_ids': reference_ids,  # NEW: Empty for now
        'citation_ids': citation_ids,  # NEW: Empty for now
        'reference_count': reference_count,  # NEW: From parquet
        'quality_score': 0.0,  # TODO: Calculate quality score
        'extraction_method': extraction_method,
        'content_quality': content_quality,
        'has_introduction': has_intro,
        'intro_length': intro_length,
        'gcs_pdf_path': None,  # Not storing PDFs yet
        'ingested_at': datetime.utcnow()
    }
    
    return paper


def prepare_embedding_text(paper: Dict[str, Any]) -> str:
    """
    Prepare text for embedding generation.
    
    Priority:
    1. title + abstract + introduction (best)
    2. title + abstract (good)
    3. title only (fallback)
    """
    parts = []
    
    if paper.get('title'):
        title = sanitize_text(paper['title'])
        if title:
            parts.append(title)
    
    if paper.get('abstract'):
        abstract = sanitize_text(paper['abstract'])
        if abstract and len(abstract) > 50:
            parts.append(abstract)
    
    if paper.get('introduction'):
        intro = sanitize_text(paper['introduction'])
        if intro and len(intro) > 100:
            if len(intro) > 3000:
                intro = intro[:3000]
            parts.append(intro)
    
    combined_text = " ".join(parts)
    combined_text = sanitize_text(combined_text) or ""
    
    if len(combined_text) < 50:
        logger.warning(f"Very short text for paper {paper.get('paper_id')}: {len(combined_text)} chars")
    
    return combined_text


def validate_paper_data(paper: Dict[str, Any]) -> bool:
    """
    Validate that paper has minimum required data.
    """
    if not paper.get('paper_id'):
        logger.warning("Paper missing paper_id")
        return False
    
    if not paper.get('title') or len(str(paper['title'])) < 10:
        logger.warning(f"Paper {paper['paper_id']} has invalid title")
        return False
    
    has_abstract = paper.get('abstract') and len(str(paper['abstract'])) > 50
    has_intro = paper.get('introduction') and len(str(paper['introduction'])) > 100
    
    if not has_abstract and not has_intro:
        logger.warning(
            f"Paper {paper['paper_id']} has neither substantial abstract nor introduction"
        )
        return False
    
    return True


def compute_text_hash(text: str) -> str:
    """Compute SHA-256 hash of text."""
    import hashlib
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def infer_search_term_from_filename(filename: str) -> Optional[str]:
    """
    Infer search term from parquet filename.
    """
    filename = os.path.basename(filename) if '/' in filename else filename
    filename = filename.lower().replace('.parquet', '')
    filename = filename.replace('_processed', '').replace('_papers', '')
    
    if 'finance' in filename:
        return 'finance'
    elif 'quantum' in filename:
        return 'quantum computing'
    elif 'healthcare' in filename or 'health' in filename:
        return 'healthcare'
    elif 'llm' in filename:
        return 'llm'
    elif 'computer' in filename and 'vision' in filename:
        return 'computer vision'
    elif 'machine' in filename and 'learning' in filename:
        return 'machine learning'
    elif 'transformer' in filename:
        return 'transformers'
    else:
        parts = filename.split('_')
        if parts and parts[0]:
            return parts[0].replace('_', ' ')
    
    return None


def batch_map_papers(
    df: pd.DataFrame,
    search_term: Optional[str] = None,
    filename: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Map entire parquet DataFrame to list of paper dictionaries.
    """
    if not search_term and filename:
        search_term = infer_search_term_from_filename(filename)
        if search_term:
            logger.info(f"Inferred search term from filename: '{search_term}'")
    
    # Check if search_term column exists in dataframe
    if 'search_term' in df.columns and not search_term:
        logger.info("Using search_term column from parquet data")
    
    papers = []
    skipped = 0
    
    for idx, row in df.iterrows():
        try:
            # Use row's search_term if available, otherwise use provided
            row_search_term = row.get('search_term') if 'search_term' in df.columns else search_term
            
            paper = map_parquet_row_to_paper(row, row_search_term)
            
            if validate_paper_data(paper):
                papers.append(paper)
            else:
                skipped += 1
                logger.debug(f"Skipped invalid paper at row {idx}")
        
        except Exception as e:
            skipped += 1
            logger.error(f"Failed to map row {idx}: {e}")
    
    logger.info(
        f"Mapped {len(papers)} papers from DataFrame "
        f"({skipped} skipped due to validation failures)"
    )
    
    return papers