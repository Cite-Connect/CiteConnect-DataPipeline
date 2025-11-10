import time
import logging
from .content_extractor import ContentExtractor
from .metadata_utils import extract_metadata


def process_papers(papers, search_term, debug=False):
    extractor = ContentExtractor()
    results = []

    for i, paper in enumerate(papers, 1):
        title = paper.get("title", "Unknown")
        logging.info(f"\nPaper {i}/{len(papers)}: {title[:60]}")

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

        results.append(record)
        time.sleep(1)

    return results
