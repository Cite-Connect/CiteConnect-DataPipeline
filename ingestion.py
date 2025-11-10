import os
import time
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import re
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from utils.storage_helpers import upload_to_gcs  # ✅ Upload helper

# ================================================================
# GROBID + PDF Libraries
# ================================================================
try:
    from grobid_client.grobid_client import GrobidClient
    GROBID_AVAILABLE = True
except ImportError:
    GROBID_AVAILABLE = False
    logging.warning("⚠️ GROBID not installed. Install: pip install grobid-client-python")

try:
    import fitz  # PyMuPDF
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    logging.warning("⚠️ PyMuPDF not installed. Install: pip install pymupdf")

# ================================================================
# LOGGING CONFIG
# ================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ================================================================
# ENV VARIABLES
# ================================================================
SEM_KEY = os.getenv("SEMANTIC_SCHOLAR_KEY")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept": "application/json, text/html",
}
if SEM_KEY:
    HEADERS["x-api-key"] = SEM_KEY

# ================================================================
# SEMANTIC SCHOLAR SEARCH
# ================================================================
def search_semantic_scholar(query: str, limit: int = 10, retries: int = 3) -> List[Dict]:
    """Fetch papers from Semantic Scholar API."""
    fields = [
        "paperId", "externalIds", "title", "abstract", "year", "publicationDate",
        "venue", "publicationVenue", "publicationTypes", "authors",
        "citationCount", "influentialCitationCount", "referenceCount",
        "citations", "references", "fieldsOfStudy", "s2FieldsOfStudy",
        "isOpenAccess", "openAccessPdf", "tldr"
    ]
    url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={query}&limit={limit}&fields={','.join(fields)}"

    for attempt in range(retries):
        wait_time = 1.5 if SEM_KEY else 5.0
        if attempt > 0:
            wait_time *= 2
        logging.info(f"[API] Query='{query}' Attempt={attempt+1} (waiting {wait_time:.1f}s to respect rate limits)")
        time.sleep(wait_time)
        try:
            r = requests.get(url, headers=HEADERS, timeout=40)
            if r.status_code == 200:
                return r.json().get("data", [])
            elif r.status_code == 429:
                logging.warning("[API] ⚠️ Rate limit hit! Backing off...")
                time.sleep(wait_time * 2)
            else:
                logging.warning(f"[API] Error {r.status_code}: {r.text[:200]}")
        except Exception as e:
            logging.error(f"[API] Request failed: {e}")
            time.sleep(2)
    return []

# ================================================================
# CONTENT EXTRACTION
# ================================================================
class ContentExtractor:
    """Implements 4-tier fallback: ArXiv → GROBID → Regex → Abstract+TLDR"""
    def __init__(self):
        self.grobid_client = None
        if GROBID_AVAILABLE:
            try:
                if requests.get("http://localhost:8070/api/isalive", timeout=2).status_code == 200:
                    self.grobid_client = GrobidClient(config_path=None)
                    logging.info("✅ GROBID server detected and ready")
            except Exception:
                logging.warning("⚠️ GROBID not reachable — fallback mode only.")

    def scrape_arxiv_html(self, arxiv_id: str) -> Optional[str]:
        """Strategy 1: Extract intro from ArXiv HTML page."""
        try:
            url = f"https://arxiv.org/html/{arxiv_id}"
            time.sleep(2)
            response = requests.get(url, headers=HEADERS, timeout=20)
            if response.status_code != 200:
                return None
            soup = BeautifulSoup(response.text, "html.parser")
            intro = ""
            for section in soup.find_all("section"):
                if "introduction" in section.get_text(strip=True).lower():
                    intro = "\n".join(p.get_text(strip=True) for p in section.find_all("p"))
                    break
            if len(intro) > 200:
                logging.info(f"✅ ArXiv HTML extraction successful ({len(intro)} chars)")
                return intro
        except Exception as e:
            logging.debug(f"ArXiv HTML failed: {e}")
        return None

    def extract_with_grobid(self, pdf_url: str) -> Optional[str]:
        """Strategy 2: Extract using GROBID structured parser."""
        if not (self.grobid_client and GROBID_AVAILABLE):
            return None
        try:
            response = requests.get(pdf_url, headers=HEADERS, timeout=30)
            if response.status_code != 200:
                return None
            temp_path = f"/tmp/paper_{int(time.time())}.pdf"
            with open(temp_path, "wb") as f:
                f.write(response.content)
            xml = self.grobid_client.process_pdf("processFulltextDocument", temp_path)
            os.remove(temp_path)
            soup = BeautifulSoup(xml, "xml")
            intro_div = soup.find("div", {"type": "introduction"})
            if intro_div:
                text = "\n".join(p.get_text(strip=True) for p in intro_div.find_all("p"))
                if len(text) > 200:
                    logging.info(f"✅ GROBID extraction successful ({len(text)} chars)")
                    return text
        except Exception as e:
            logging.debug(f"GROBID failed: {e}")
        return None

    def extract_with_regex(self, pdf_url: str) -> Optional[str]:
        """Strategy 3: Regex-based PDF parsing."""
        if not PDF_SUPPORT:
            return None
        try:
            response = requests.get(pdf_url, headers=HEADERS, timeout=30)
            if response.status_code != 200:
                return None
            doc = fitz.open(stream=response.content, filetype="pdf")
            text = ""
            for page in doc[:7]:
                text += page.get_text()
            doc.close()
            match = re.search(r"(?:^|\n)Introduction[\s\S]{200,8000}?(?=\n\s*(?:2\.|Method|Materials|Results|Discussion|References))", text, re.IGNORECASE)
            if match:
                intro = match.group(0).strip()
                if len(intro) > 200:
                    logging.info(f"✅ Regex extraction successful ({len(intro)} chars)")
                    return intro
        except Exception as e:
            logging.debug(f"Regex failed: {e}")
        return None

    def extract_content(self, paper: Dict) -> Tuple[Optional[str], str, str]:
        """Multi-tier fallback extractor."""
        external_ids = paper.get("externalIds", {})
        if isinstance(external_ids, dict) and "ArXiv" in external_ids:
            intro = self.scrape_arxiv_html(external_ids["ArXiv"])
            if intro:
                return intro, "arxiv_html", "high"

        pdf_url = paper.get("openAccessPdf", {}).get("url")
        if pdf_url and GROBID_AVAILABLE:
            intro = self.extract_with_grobid(pdf_url)
            if intro:
                return intro, "grobid_pdf", "high"

        if pdf_url:
            intro = self.extract_with_regex(pdf_url)
            if intro:
                return intro, "regex_pdf", "medium"

        abstract = paper.get("abstract", "")
        tldr = paper.get("tldr", {}).get("text", "") if isinstance(paper.get("tldr"), dict) else ""
        if abstract:
            combined = f"{abstract}\n\nSummary: {tldr}" if tldr else abstract
            return combined, "abstract_tldr", "low"

        return None, "failed", "none"

# ================================================================
# METADATA EXTRACTION (FULL 30 FIELDS)
# ================================================================
def extract_metadata(paper: Dict, search_term: str) -> Dict:
    """Extract and normalize all metadata fields."""
    def safe(d, *keys, default=None):
        for k in keys:
            if isinstance(d, dict):
                d = d.get(k)
            else:
                return default
        return d

    citations = [c.get("paperId") for c in paper.get("citations", [])[:50] if c.get("paperId")]
    refs = [r.get("paperId") for r in paper.get("references", [])[:50] if r.get("paperId")]

    return {
        "search_term": search_term,
        "paperId": paper.get("paperId"),
        "externalIds": json.dumps(paper.get("externalIds", {})),
        "title": paper.get("title"),
        "abstract": paper.get("abstract"),
        "year": paper.get("year"),
        "publicationDate": paper.get("publicationDate"),
        "venue": paper.get("venue"),
        "publicationTypes": json.dumps(paper.get("publicationTypes", [])),
        "publicationVenue": json.dumps(paper.get("publicationVenue", {})),
        "authors": ", ".join(a.get("name", "") for a in paper.get("authors", [])),
        "authorIds": json.dumps([a.get("authorId") for a in paper.get("authors", [])]),
        "citationCount": paper.get("citationCount", 0),
        "influentialCitationCount": paper.get("influentialCitationCount", 0),
        "referenceCount": paper.get("referenceCount", 0),
        "citations": json.dumps(citations),
        "references": json.dumps(refs),
        "fieldsOfStudy": json.dumps(paper.get("fieldsOfStudy", [])),
        "s2FieldsOfStudy": json.dumps(paper.get("s2FieldsOfStudy", [])),
        "isOpenAccess": paper.get("isOpenAccess", False),
        "pdf_url": safe(paper, "openAccessPdf", "url"),
        "tldr": safe(paper, "tldr", "text"),
        "extraction_method": None,
        "content_quality": None,
        "has_intro": False,
        "intro_length": 0,
        "status": "pending",
        "scraped_at": datetime.utcnow().isoformat() + "Z",
    }

# ================================================================
# PIPELINE LOGIC
# ================================================================
def process_papers(papers: List[Dict], search_term: str) -> List[Dict]:
    extractor = ContentExtractor()
    results = []
    for i, paper in enumerate(papers, 1):
        logging.info(f"📄 Processing {i}/{len(papers)}: {paper.get('title', 'Unknown')[:80]}")
        record = extract_metadata(paper, search_term)
        content, method, quality = extractor.extract_content(paper)
        if content:
            record["introduction"] = content
            record["extraction_method"] = method
            record["content_quality"] = quality
            record["has_intro"] = method in ["arxiv_html", "grobid_pdf", "regex_pdf"]
            record["intro_length"] = len(content)
            record["status"] = f"success_{method}"
        else:
            record["status"] = "failed"
        results.append(record)
    return results

def save_to_parquet(data: List[Dict], search_term: str, output_dir: str = "data"):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    safe_term = re.sub(r"[^\w\s-]", "", search_term).strip().replace(" ", "_")
    filename = f"{output_dir}/{safe_term}_{int(time.time())}.parquet"
    df = pd.DataFrame(data)
    df.to_parquet(filename, index=False)
    logging.info(f"✅ Saved {len(data)} records to {filename}")

    try:
        bucket = "citeconnect-processed-parquet"
        domain = Path(output_dir).name
        dest = f"{domain}/{os.path.basename(filename)}"
        if upload_to_gcs(filename, bucket, dest):
            logging.info(f"📤 Uploaded → gs://{bucket}/{dest}")
    except Exception as e:
        logging.error(f"❌ GCS upload failed: {e}")
    return filename

# ================================================================
# MAIN INGESTION
# ================================================================
def ingest_papers(search_terms: List[str], limit: int = 10, output_dir: str = "data"):
    for term in search_terms:
        logging.info(f"\n{'='*60}\n🚀 Starting ingestion for: {term}\n{'='*60}")
        papers = search_semantic_scholar(term, limit)
        if not papers:
            logging.warning(f"No papers found for '{term}'")
            continue
        processed = process_papers(papers, term)
        save_to_parquet(processed, term, output_dir)
        logging.info(f"✅ Completed ingestion for: {term}\n")

# ================================================================
# ENTRY POINT
# ================================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CiteConnect Hybrid Research Paper Ingestion Pipeline")
    parser.add_argument("search_terms", nargs="+", help="Search terms (space-separated)")
    parser.add_argument("--limit", type=int, default=10, help="Number of papers per term")
    parser.add_argument("--output", default="data/healthcare", help="Output directory")
    args = parser.parse_args()
    ingest_papers(args.search_terms, args.limit, args.output)
