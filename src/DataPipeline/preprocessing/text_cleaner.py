"""
Text cleaning utilities for research papers.
Removes headers, footers, references, and normalizes text.
"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class TextCleaner:
    """Cleans and normalizes research paper text."""

    # Patterns to remove
    HEADER_FOOTER_PATTERN = r"(?:Page\s+\d+|^\d+\s*$|\b\d{4}\b.*?Conference)"
    REFERENCE_SECTION_PATTERN = r"(?:^|\n)\s*(?:References|Bibliography|REFERENCES)\s*\n[\s\S]*$"
    EMAIL_PATTERN = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    URL_PATTERN = r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
    CITATION_PATTERN = r"\[\d+(?:,\s*\d+)*\]|\(\w+\s+et\s+al\.,?\s+\d{4}\)"

    def __init__(self, remove_citations: bool = False, remove_urls: bool = True):
        """
        Initialize text cleaner.

        Args:
            remove_citations: Whether to remove in-text citations like [1,2,3]
            remove_urls: Whether to remove URLs
        """
        self.remove_citations = remove_citations
        self.remove_urls = remove_urls

    def clean(self, text: str) -> str:
        """
        Clean and normalize text.

        Args:
            text: Raw text from paper

        Returns:
            Cleaned text ready for chunking
        """
        if not text or not isinstance(text, str):
            logger.warning("Empty or invalid text provided to cleaner")
            return ""

        original_length = len(text)
        
        # Remove reference section (usually at end)
        text = re.sub(self.REFERENCE_SECTION_PATTERN, "", text, flags=re.IGNORECASE | re.MULTILINE)

        # Remove headers and footers
        text = re.sub(self.HEADER_FOOTER_PATTERN, "", text, flags=re.MULTILINE)

        # Remove emails
        text = re.sub(self.EMAIL_PATTERN, "", text)

        # Remove URLs if enabled
        if self.remove_urls:
            text = re.sub(self.URL_PATTERN, "", text)

        # Remove citations if enabled
        if self.remove_citations:
            text = re.sub(self.CITATION_PATTERN, "", text)

        # Normalize whitespace
        text = self._normalize_whitespace(text)

        # Remove common artifacts
        text = self._remove_artifacts(text)

        cleaned_length = len(text)
        logger.debug(
            f"Text cleaned: {original_length} → {cleaned_length} chars "
            f"({100 * (1 - cleaned_length / original_length):.1f}% removed)"
        )

        return text.strip()

    def _normalize_whitespace(self, text: str) -> str:
        """Normalize excessive whitespace and newlines."""
        # Replace multiple spaces with single space
        text = re.sub(r" +", " ", text)
        # Replace multiple newlines with double newline
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        # Remove spaces before punctuation
        text = re.sub(r"\s+([.,;:!?])", r"\1", text)
        return text

    def _remove_artifacts(self, text: str) -> str:
        """Remove common PDF extraction artifacts."""
        # Remove hyphenation at line breaks
        text = re.sub(r"(\w+)-\s*\n\s*(\w+)", r"\1\2", text)
        # Remove isolated single characters (often OCR errors)
        text = re.sub(r"\b[a-zA-Z]\b(?!\.|[A-Z])", "", text)
        # Remove excessive special characters
        text = re.sub(r"[^\w\s.,;:!?()\[\]{}'\"@#$%&*+=/<>-]", "", text)
        return text

    def clean_batch(self, texts: list[str]) -> list[str]:
        """
        Clean multiple texts in batch.

        Args:
            texts: List of raw texts

        Returns:
            List of cleaned texts
        """
        logger.info(f"Cleaning batch of {len(texts)} texts")
        return [self.clean(text) for text in texts]