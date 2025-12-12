import json
import os
from datetime import datetime
from typing import List

def extract_metadata(paper, search_term):
    """Build structured metadata dictionary for each paper."""

    def safe_get(data, *keys, default=None):
        for key in keys:
            data = data.get(key) if isinstance(data, dict) else None
            if data is None:
                return default
        return data
    
    fields_of_study = paper.get("fieldsOfStudy") or []
    if hasattr(fields_of_study, 'tolist'):  # Check if it's a NumPy array
        fields_of_study = fields_of_study.tolist()
    elif not isinstance(fields_of_study, list):
        fields_of_study = []
    
    # Extract references as TEXT[] array (paper IDs only) - PostgreSQL compatible
    references = []
    refs_data = paper.get("references") or []  # Handle None values
    if isinstance(refs_data, list):
        for ref in refs_data:
            if isinstance(ref, dict) and ref.get("paperId"):
                references.append(ref["paperId"])
            elif isinstance(ref, str):
                references.append(ref)
    
    # Extract citations as TEXT[] array (paper IDs only) - PostgreSQL compatible
    citations = []
    cites_data = paper.get("citations") or []  # Handle None values
    if isinstance(cites_data, list):
        for cit in cites_data:
            if isinstance(cit, dict) and cit.get("paperId"):
                citations.append(cit["paperId"])
            elif isinstance(cit, str):
                citations.append(cit)
    
    # Get domain from environment (use directly, no mapping)
    domain = os.getenv('COLLECTION_DOMAIN', 'general')
    
    # Subdomain = search_term (1:1 mapping as per user requirement)
    # User collects papers with search_term matching the subdomain name
    sub_domains = [search_term.strip()]
    
    # Handle authors safely (can be None or not a list)
    authors_data = paper.get("authors") or []
    authors_str = ", ".join(a.get("name", "") for a in authors_data) if isinstance(authors_data, list) else ""

    # Get references_id if it was added to the paper (for seed papers)
    references_id = paper.get("references_id", [])
    if not isinstance(references_id, list):
        references_id = []
    
    return {
        "search_term": search_term,
        "paperId": paper.get("paperId"),
        "externalIds": json.dumps(paper.get("externalIds", {})),
        "title": paper.get("title"),
        "abstract": paper.get("abstract"),
        "year": paper.get("year"),
        "publicationDate": paper.get("publicationDate"),
        "authors": authors_str,
        "citationCount": paper.get("citationCount", 0),
        "referenceCount": len(references),  # Use actual count from extracted refs
        "references": references,  # TEXT[] array for PostgreSQL
        "references_id": references_id,  # Paper IDs of references in seed paper (from /references endpoint)
        "citations": citations,    # TEXT[] array for PostgreSQL
        "fieldsOfStudy": json.dumps(fields_of_study),
        "domain": domain,  # ✅ Primary domain from COLLECTION_DOMAIN
        "sub_domains": sub_domains,  # ✅ Sub-domains array (search_term)
        "pdf_url": safe_get(paper, "openAccessPdf", "url"),
        "tldr": safe_get(paper, "tldr", "text"),
        "introduction": None,
        "extraction_method": None,
        "content_quality": None,
        "has_intro": False,
        "intro_length": 0,
        "status": "pending",
        "scraped_at": datetime.utcnow().isoformat() + "Z",
    }
