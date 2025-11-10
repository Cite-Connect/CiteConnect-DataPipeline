"""
Metadata enrichment for research papers.
Extracts keywords, calculates importance scores.
"""

import logging
from typing import Dict, List, Optional
from collections import Counter
import re

logger = logging.getLogger(__name__)


class MetadataEnricher:
    """Enriches paper metadata with derived fields."""

    def __init__(self, top_k_keywords: int = 10):
        """
        Initialize enricher.

        Args:
            top_k_keywords: Number of top keywords to extract (default 10)
        """
        self.top_k_keywords = top_k_keywords
        # Common English stopwords
        self.stopwords = set([
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
            'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
            'should', 'may', 'might', 'must', 'can', 'this', 'that', 'these', 'those'
        ])

    def enrich(self, paper_metadata: Dict) -> Dict:
        """
        Enrich paper metadata with additional fields.

        Args:
            paper_metadata: Original paper metadata dict

        Returns:
            Enriched metadata with new fields
        """
        enriched = paper_metadata.copy()

        # Extract keywords from title + abstract
        text_for_keywords = f"{paper_metadata.get('title', '')} {paper_metadata.get('abstract', '')}"
        enriched['keywords'] = self.extract_keywords(text_for_keywords)

        # Calculate importance score
        enriched['importance_score'] = self.calculate_importance_score(paper_metadata)

        # Determine research domain
        enriched['research_domain'] = self.classify_domain(paper_metadata)

        # Extract year decade
        year = paper_metadata.get('year')
        enriched['decade'] = (year // 10 * 10) if year else None

        logger.debug(f"Enriched metadata for paper {paper_metadata.get('paperId', 'unknown')}")
        return enriched

    def extract_keywords(self, text: str, use_tfidf: bool = False) -> List[str]:
        """
        Extract keywords using frequency-based method.

        Args:
            text: Text to extract keywords from
            use_tfidf: Use TF-IDF (not implemented, placeholder)

        Returns:
            List of top keywords
        """
        if not text:
            return []

        # Tokenize and clean
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
        
        # Remove stopwords
        filtered_words = [w for w in words if w not in self.stopwords]

        # Count frequencies
        word_counts = Counter(filtered_words)
        
        # Return top K
        top_keywords = [word for word, _ in word_counts.most_common(self.top_k_keywords)]
        
        logger.debug(f"Extracted {len(top_keywords)} keywords from text")
        return top_keywords

    def calculate_importance_score(self, metadata: Dict) -> float:
        """
        Calculate paper importance score based on citations and other factors.

        Formula: log(citations + 1) * recency_factor * influence_factor

        Args:
            metadata: Paper metadata

        Returns:
            Importance score (0-100 scale)
        """
        import math
        from datetime import datetime

        citation_count = metadata.get('citationCount', 0)
        influential_citations = metadata.get('influentialCitationCount', 0)
        year = metadata.get('year')
        
        # Citation score (log scale)
        citation_score = math.log10(citation_count + 1)

        # Recency factor (newer papers get boost)
        current_year = datetime.now().year
        if year:
            years_old = current_year - year
            recency_factor = max(0.5, 1 - (years_old / 50))  # Decay over 50 years
        else:
            recency_factor = 0.5

        # Influence factor
        influence_ratio = influential_citations / max(citation_count, 1)
        influence_factor = 1 + (influence_ratio * 0.5)  # Up to 1.5x boost

        # Combined score (normalized to 0-100)
        raw_score = citation_score * recency_factor * influence_factor
        normalized_score = min(100, raw_score * 10)  # Scale to 0-100

        return round(normalized_score, 2)

    def classify_domain(self, metadata: Dict) -> str:
        """
        Classify paper into research domain based on fields of study.

        Args:
            metadata: Paper metadata

        Returns:
            Domain classification (e.g., 'Computer Science', 'Medicine')
        """
        import json

        fields_of_study = metadata.get('fieldsOfStudy', [])
        if isinstance(fields_of_study, str):
            try:
                fields_of_study = json.loads(fields_of_study)
            except:
                fields_of_study = []

        if not fields_of_study:
            return 'General'

        # Priority mapping
        domain_keywords = {
            'Computer Science': ['computer science', 'machine learning', 'artificial intelligence'],
            'Medicine': ['medicine', 'clinical', 'healthcare', 'medical'],
            'Physics': ['physics', 'quantum'],
            'Biology': ['biology', 'genomics', 'bioinformatics'],
            'Mathematics': ['mathematics', 'statistics'],
            'Engineering': ['engineering'],
        }

        # Check for matches
        fields_lower = [f.lower() for f in fields_of_study if isinstance(f, str)]
        
        for domain, keywords in domain_keywords.items():
            if any(kw in field for field in fields_lower for kw in keywords):
                return domain

        return fields_of_study[0] if fields_of_study else 'General'