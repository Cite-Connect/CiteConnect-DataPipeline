"""
Column mapping utilities for CiteConnect.
Maps parquet file columns to PostgreSQL table columns.
"""

import logging
from typing import Dict, Any, Optional, List
import pandas as pd
from datetime import datetime
import os

logger = logging.getLogger(__name__)


# Parquet column → PostgreSQL column mapping
PARQUET_TO_POSTGRES_MAPPING = {
    'paperId': 'paper_id',
    'title': 'title',
    'abstract': 'abstract',
    'introduction': 'introduction',
    'tldr': 'summary',  # Map tldr to summary column
    'authors': 'authors',
    'year': 'year',
    'venue': 'venue',
    'citationCount': 'citation_count',
    'fieldsOfStudy': 'fields_of_study',
    'extraction_method': 'extraction_method',
    'content_quality': 'content_quality',
    'has_intro': 'has_introduction',
    'intro_length': 'intro_length',
}

# Search term to domain mapping
SEARCH_TERM_TO_DOMAIN = {
    'finance': 'fintech',
    'quantum computing': 'quantum_computing',
    'healthcare': 'healthcare',
}


def map_parquet_row_to_paper(row: pd.Series, search_term: Optional[str] = None) -> Dict[str, Any]:
    """
    Map a parquet row to PostgreSQL papers table format.
    
    Args:
        row: pandas Series from parquet file
        search_term: Search term used to collect this paper (for domain mapping)
    
    Returns:
        Dictionary ready for PostgreSQL insertion
    """
    # Determine domain from search term
    domain = None
    if search_term:
        domain = SEARCH_TERM_TO_DOMAIN.get(search_term.lower(), None)
    
    # Extract paper_id (handle both 'paperId' and 'paper_id')
    paper_id = row.get('paperId') or row.get('paper_id')
    if not paper_id:
        raise ValueError(f"No paper_id found in row: {row.to_dict()}")
    
    # Map authors (assume stored as list or string)# Map authors (handle list or string)
    authors = row.get('authors', [])

    # Handle different types of author data
    if authors is None:
        authors = []
    elif isinstance(authors, str):
        # If it's a string representation of a list, try to parse
        import json
        try:
            authors = json.loads(authors)
        except:
            authors = [authors]  # Single author as string
    elif isinstance(authors, (list, tuple)):
        # Already a list/tuple, keep as is
        authors = list(authors)
    else:
        # Try to check if it's a pandas NA or numpy array
        try:
            if pd.isna(authors):
                authors = []
            else:
                authors = [str(authors)]
        except (ValueError, TypeError):
            # It's probably already a list or array
            authors = list(authors) if hasattr(authors, '__iter__') else [str(authors)]

    # Ensure authors is a list of strings
    if not isinstance(authors, list):
        authors = [str(authors)]

    # Clean up empty strings
    authors = [str(a).strip() for a in authors if a and str(a).strip()]    
    # Map introduction (check multiple possible column names)
    introduction = (
        row.get('introduction') or 
        row.get('intro') or 
        None
    )
    if pd.isna(introduction):
        introduction = None
    
    # Map abstract
    abstract = row.get('abstract')
    if pd.isna(abstract):
        abstract = None
    
    # Map summary/tldr
    summary = row.get('tldr') or row.get('summary')
    if pd.isna(summary):
        summary = None
    
    # Map year
    year = row.get('year')
    if pd.isna(year):
        year = None
    else:
        try:
            year = int(year)
        except (ValueError, TypeError):
            year = None
    
    # Map citation count
    citation_count = row.get('citationCount', 0)
    if pd.isna(citation_count):
        citation_count = 0
    else:
        try:
            citation_count = int(citation_count)
        except (ValueError, TypeError):
            citation_count = 0
    
    # Map venue
    venue = row.get('venue')
    if pd.isna(venue):
        venue = None
    
    # Map extraction method
    extraction_method = row.get('extraction_method')
    if pd.isna(extraction_method):
        extraction_method = 'unknown'
    
    # Map content quality
    content_quality = row.get('content_quality')
    if pd.isna(content_quality):
        content_quality = 'medium'  # Default
    
    # Map has_introduction
    has_intro = row.get('has_intro', False)
    if pd.isna(has_intro):
        # Infer from introduction presence
        has_intro = introduction is not None and len(str(introduction)) > 100
    elif isinstance(has_intro, str):
        has_intro = has_intro.lower() in ('true', '1', 'yes')
    
    # Map intro_length
    intro_length = row.get('intro_length', 0)
    if pd.isna(intro_length) or intro_length == 0:
        # Calculate from introduction if available
        if introduction:
            intro_length = len(str(introduction))
        else:
            intro_length = 0
    else:
        try:
            intro_length = int(intro_length)
        except (ValueError, TypeError):
            intro_length = 0
    
    # Build paper dictionary
    paper = {
        'paper_id': str(paper_id),
        'title': str(row.get('title', '')),
        'abstract': abstract,
        'introduction': introduction,
        'summary': summary,
        'authors': authors,
        'year': year,
        'venue': venue,
        'citation_count': citation_count,
        'domain': domain,
        'gcs_pdf_path': None,  # Not storing PDFs yet
        'extraction_method': extraction_method,
        'content_quality': content_quality,
        'has_introduction': has_intro,
        'intro_length': intro_length,
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
    
    Args:
        paper: Paper dictionary
    
    Returns:
        Combined text for embedding
    """
    parts = []
    
    # Always include title
    if paper.get('title'):
        parts.append(str(paper['title']))
    
    # Include abstract if available
    if paper.get('abstract'):
        abstract = str(paper['abstract'])
        if len(abstract) > 50:  # Ensure it's substantial
            parts.append(abstract)
    
    # Include introduction if available and substantial
    if paper.get('introduction'):
        intro = str(paper['introduction'])
        if len(intro) > 100:  # Only if substantial
            # Truncate very long introductions to avoid token limits
            if len(intro) > 3000:
                intro = intro[:3000]
            parts.append(intro)
    
    # Combine with space separator
    combined_text = " ".join(parts)
    
    # Ensure we have at least some text
    if len(combined_text) < 50:
        logger.warning(f"Very short text for paper {paper.get('paper_id')}: {len(combined_text)} chars")
    
    return combined_text


def infer_search_term_from_filename(filename: str) -> Optional[str]:
    """
    Infer search term from parquet filename.
    
    Examples:
        'finance_processed.parquet' → 'finance'
        'quantum_computing_processed.parquet' → 'quantum computing'
        'healthcare_papers.parquet' → 'healthcare'
    
    Args:
        filename: Parquet filename
    
    Returns:
        Inferred search term or None
    """
    filename = filename.lower().replace('.parquet', '')
    filename = filename.replace('_processed', '').replace('_papers', '')
    
    # Handle common patterns
    if 'finance' in filename:
        return 'finance'
    elif 'quantum' in filename:
        return 'quantum computing'
    elif 'healthcare' in filename or 'health' in filename:
        return 'healthcare'
    else:
        # Try to extract the part before first underscore
        parts = filename.split('_')
        if parts:
            search_term = parts[0].replace('_', ' ')
            return search_term
    
    return None


def validate_paper_data(paper: Dict[str, Any]) -> bool:
    """
    Validate that paper has minimum required data.
    
    Args:
        paper: Paper dictionary
    
    Returns:
        True if valid, False otherwise
    """
    # Must have paper_id
    if not paper.get('paper_id'):
        logger.warning("Paper missing paper_id")
        return False
    
    # Must have title
    if not paper.get('title') or len(str(paper['title'])) < 10:
        logger.warning(f"Paper {paper['paper_id']} has invalid title")
        return False
    
    # Must have at least abstract OR introduction
    has_abstract = paper.get('abstract') and len(str(paper['abstract'])) > 50
    has_intro = paper.get('introduction') and len(str(paper['introduction'])) > 100
    
    if not has_abstract and not has_intro:
        logger.warning(
            f"Paper {paper['paper_id']} has neither substantial abstract nor introduction"
        )
        return False
    
    return True


def compute_text_hash(text: str) -> str:
    """
    Compute SHA-256 hash of text.
    Used to detect when embedding needs regeneration.
    
    Args:
        text: Text to hash
    
    Returns:
        Hex digest of SHA-256 hash
    """
    import hashlib
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def batch_map_papers(
    df: pd.DataFrame,
    search_term: Optional[str] = None,
    filename: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Map entire parquet DataFrame to list of paper dictionaries.
    
    Args:
        df: Parquet DataFrame
        search_term: Search term for domain mapping
        filename: Parquet filename (used to infer search term if not provided)
    
    Returns:
        List of paper dictionaries
    """
    # Try to infer search term from filename if not provided
    if not search_term and filename:
        search_term = infer_search_term_from_filename(filename)
        if search_term:
            logger.info(f"Inferred search term from filename: '{search_term}'")
    
    papers = []
    skipped = 0
    
    for idx, row in df.iterrows():
        try:
            paper = map_parquet_row_to_paper(row, search_term)
            
            # Validate
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