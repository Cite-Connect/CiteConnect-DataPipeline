import time
import logging
import requests
import re
from bs4 import BeautifulSoup
from typing import Optional, Tuple, Dict

try:
    from grobid_client.grobid_client import GrobidClient
    GROBID_AVAILABLE = True
except ImportError:
    GROBID_AVAILABLE = False
    logging.warning("GROBID not installed")

try:
    import fitz  # PyMuPDF
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    logging.warning("PyMuPDF not installed")

from src.DataPipeline.utils.constants import HEADERS


class ContentExtractor:
    """
    Extraction sequence:
    1. ArXiv HTML (cleanest source)
    2. GROBID structured PDF parsing
    3. Regex-based PDF extraction
    4. Abstract + TLDR fallback
    """

    def __init__(self):
        self.grobid_client = None

        if GROBID_AVAILABLE:
            try:
                alive = requests.get("http://localhost:8070/api/isalive", timeout=2)
                if alive.status_code == 200:
                    self.grobid_client = GrobidClient(config_path=None)
                    logging.info("GROBID server connected")
            except Exception:
                logging.warning("GROBID connection failed")

    def scrape_arxiv_html(self, arxiv_id: str) -> Optional[str]:
        """Extract introduction from ArXiv HTML if available."""
        try:
            time.sleep(2)
            url = f"https://arxiv.org/html/{arxiv_id}"
            response = requests.get(url, headers=HEADERS, timeout=20)
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, "html.parser")

            intro_section = soup.find('section', {'id': 'S1'})
            if not intro_section:
                for section in soup.find_all("section"):
                    if "introduction" in section.get_text(strip=True).lower()[:50]:
                        intro_section = section
                        break

            if not intro_section:
                return None

            paragraphs = intro_section.find_all("p")
            text = "\n\n".join(p.get_text(strip=True) for p in paragraphs)

            if len(text) > 200:
                logging.info(f"ArXiv HTML extracted: {len(text)} characters")
                return text

        except Exception as e:
            logging.debug(f"ArXiv extraction error: {e}")

        return None

    def extract_with_grobid(self, pdf_url: str) -> Optional[str]:
        """Use GROBID to extract structured introduction."""
        if not (self.grobid_client and GROBID_AVAILABLE):
            return None

        temp_path = None
        try:
            time.sleep(2)
            response = requests.get(pdf_url, headers=HEADERS, timeout=60)
            if response.status_code != 200:
                return None

            temp_path = f"/tmp/paper_{int(time.time())}.pdf"
            with open(temp_path, "wb") as f:
                f.write(response.content)

            xml = self.grobid_client.process_pdf("processFulltextDocument", temp_path)

            import os
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

            if not xml:
                return None

            soup = BeautifulSoup(xml, "xml")
            intro_div = soup.find("div", {"type": "introduction"})
            if not intro_div:
                return None

            text = "\n\n".join(p.get_text(strip=True) for p in intro_div.find_all("p"))

            if len(text) > 200:
                logging.info(f"GROBID extracted: {len(text)} characters")
                return text

        except requests.exceptions.Timeout:
            logging.warning("GROBID PDF download timeout")
        except Exception as e:
            logging.debug(f"GROBID error: {e}")

        return None

    def extract_with_regex(self, pdf_url: str) -> Optional[str]:
        """Fallback PDF extraction using plain text from first 7 pages."""
        if not PDF_SUPPORT:
            return None

        try:
            time.sleep(2)
            response = requests.get(pdf_url, headers=HEADERS, timeout=60)
            if response.status_code != 200:
                return None

            doc = fitz.open(stream=response.content, filetype="pdf")
            text = ""
            for page in range(min(7, len(doc))):
                text += doc[page].get_text()
            doc.close()

            text = re.sub(r'\n\s*\n', '\n', text)

            intro_start = None
            patterns_start = [
                r'(?:^|\n)1\.\s*Introduction',
                r'(?:^|\n)Introduction',
                r'(?:^|\n)INTRODUCTION',
            ]
            for p in patterns_start:
                m = re.search(p, text, re.IGNORECASE)
                if m:
                    intro_start = m.end()
                    break

            if not intro_start:
                return None

            remaining = text[intro_start:]
            patterns_end = [
                r'\n\s*2\.\s*(Method|Background|Related|Result)',
                r'\n\s*References',
                r'\n\s*Discussion',
            ]
            end_pos = None
            for p in patterns_end:
                m = re.search(p, remaining, re.IGNORECASE)
                if m:
                    end_pos = m.start()
                    break

            intro = remaining[:end_pos] if end_pos else remaining[:10000]
            intro = re.sub(r'\s+', ' ', intro).strip()

            if 200 < len(intro) < 15000:
                logging.info(f"Regex extracted: {len(intro)} characters")
                return intro

        except Exception as e:
            logging.debug(f"Regex extraction error: {e}")

        return None

    def fallback(self, paper):
        """Last resort: use abstract and optional TLDR."""
        abstract = paper.get("abstract")
        tldr = paper.get("tldr", {}).get("text") if isinstance(paper.get("tldr"), dict) else None

        if abstract and len(abstract) > 50:
            return (
                abstract + (f"\n\nSummary: {tldr}" if tldr else ""),
                "abstract_tldr",
                "low"
            )

        if tldr and len(tldr) > 50:
            return (tldr, "tldr_only", "low")

        return None, "failed", "none"

    def extract_content(self, paper: Dict) -> Tuple[Optional[str], str, str]:
        """Run extraction strategies in priority order."""

        ext_ids = paper.get("externalIds", {})
        if isinstance(ext_ids, dict) and "ArXiv" in ext_ids:
            intro = self.scrape_arxiv_html(ext_ids["ArXiv"])
            if intro:
                return intro, "arxiv_html", "high"

        pdf_dict = paper.get("openAccessPdf")
        pdf_url = pdf_dict.get("url") if isinstance(pdf_dict, dict) else None
        if pdf_url:
            intro = self.extract_with_grobid(pdf_url)
            if intro:
                return intro, "grobid_pdf", "high"

        if pdf_url:
            intro = self.extract_with_regex(pdf_url)
            if intro:
                return intro, "regex_pdf", "medium"

        return self.fallback(paper)
