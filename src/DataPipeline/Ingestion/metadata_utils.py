import json
from datetime import datetime

def extract_metadata(paper, search_term):
    """Build structured metadata dictionary for each paper."""

    def safe_get(data, *keys, default=None):
        for key in keys:
            data = data.get(key) if isinstance(data, dict) else None
            if data is None:
                return default
        return data
    
    fields_of_study = paper.get("fieldsOfStudy", [])
    if hasattr(fields_of_study, 'tolist'):  # Check if it's a NumPy array
        fields_of_study = fields_of_study.tolist()

    return {
        "search_term": search_term,
        "paperId": paper.get("paperId"),
        "externalIds": json.dumps(paper.get("externalIds", {})),
        "title": paper.get("title"),
        "abstract": paper.get("abstract"),
        "year": paper.get("year"),
        "publicationDate": paper.get("publicationDate"),
        "authors": ", ".join(a.get("name", "") for a in (paper.get("authors", []) if isinstance(paper.get("authors"), list) else [])),
        "citationCount": paper.get("citationCount", 0),
        "referenceCount": paper.get("referenceCount", 0),
        "fieldsOfStudy": json.dumps(fields_of_study),
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
