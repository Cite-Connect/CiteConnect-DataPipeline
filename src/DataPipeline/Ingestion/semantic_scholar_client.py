from typing import List, Dict
import requests, time, logging
from src.DataPipeline.utils.constants import HEADERS


def search_semantic_scholar(query: str, limit: int = 10, retries: int = 3) -> List[Dict]:
    fields = [
    "paperId", "externalIds", "title", "abstract", "year", "publicationDate",
    "venue", "publicationVenue", "publicationTypes", "authors",
    "citationCount", "influentialCitationCount", "referenceCount",
    "citations", "references", "fieldsOfStudy", "s2FieldsOfStudy",
    "isOpenAccess", "openAccessPdf", "tldr"
]

    url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={query}&limit={limit}&fields={','.join(fields)}"

    for attempt in range(retries):
        logging.info(f"Query: {query} attempt {attempt+1}")
        try:
            r = requests.get(url, headers=HEADERS, timeout=40)
            if r.status_code == 200:
                return r.json().get("data", [])
        except Exception as e:
            logging.error(f"Search failed: {e}")
        time.sleep(3)
    return []
