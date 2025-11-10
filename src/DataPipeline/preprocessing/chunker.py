"""
Document chunking with semantic coherence.
Splits documents into overlapping chunks suitable for embeddings.
"""

import re
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Chunk:
    """Represents a text chunk with metadata."""
    chunk_id: str
    paper_id: str
    text: str
    position: int
    token_count: int
    start_char: int
    end_char: int


class DocumentChunker:
    """Chunks documents with semantic overlap."""

    def __init__(
        self,
        chunk_size: int = 512,
        overlap: int = 50,
        min_chunk_size: int = 100
    ):
        """
        Initialize chunker.

        Args:
            chunk_size: Target chunk size in tokens (default 512)
            overlap: Overlap between chunks in tokens (default 50)
            min_chunk_size: Minimum viable chunk size (default 100)
        """
        if overlap >= chunk_size:
            raise ValueError(f"Overlap ({overlap}) must be less than chunk_size ({chunk_size})")

        self.chunk_size = chunk_size
        self.overlap = overlap
        self.min_chunk_size = min_chunk_size
        logger.info(
            f"Chunker initialized: size={chunk_size}, overlap={overlap}, min={min_chunk_size}"
        )

    def chunk_document(
        self,
        text: str,
        paper_id: str,
        preserve_sentences: bool = True
    ) -> List[Chunk]:
        """
        Split document into overlapping chunks.

        Args:
            text: Cleaned text to chunk
            paper_id: Unique paper identifier
            preserve_sentences: Don't break mid-sentence (default True)

        Returns:
            List of Chunk objects
        """
        if not text or len(text) < self.min_chunk_size:
            logger.warning(f"Text too short to chunk for paper {paper_id}: {len(text)} chars")
            return []

        # Split into sentences for semantic preservation
        sentences = self._split_sentences(text) if preserve_sentences else [text]
        
        chunks = []
        current_chunk = []
        current_tokens = 0
        char_position = 0
        position = 0

        for sentence in sentences:
            sentence_tokens = self._estimate_tokens(sentence)

            # If single sentence exceeds chunk size, split it forcefully
            if sentence_tokens > self.chunk_size:
                if current_chunk:
                    chunks.append(self._create_chunk(
                        current_chunk, paper_id, position, char_position
                    ))
                    position += 1
                    current_chunk = []
                    current_tokens = 0

                # Split long sentence
                sub_chunks = self._split_long_sentence(sentence, paper_id, position)
                chunks.extend(sub_chunks)
                position += len(sub_chunks)
                char_position += len(sentence)
                continue

            # Check if adding sentence exceeds chunk size
            if current_tokens + sentence_tokens > self.chunk_size and current_chunk:
                # Save current chunk
                chunk = self._create_chunk(current_chunk, paper_id, position, char_position)
                chunks.append(chunk)
                position += 1

                # Start new chunk with overlap
                overlap_text = self._get_overlap_text(current_chunk, self.overlap)
                current_chunk = [overlap_text] if overlap_text else []
                current_tokens = self._estimate_tokens(overlap_text) if overlap_text else 0

            current_chunk.append(sentence)
            current_tokens += sentence_tokens
            char_position += len(sentence)

        # Add final chunk
        if current_chunk:
            chunks.append(self._create_chunk(current_chunk, paper_id, position, char_position))

        logger.info(f"Created {len(chunks)} chunks for paper {paper_id}")
        return chunks

    def _split_sentences(self, text: str) -> List[str]:
        """Split text into sentences using regex."""
        # Split on period, exclamation, question mark followed by space/newline
        sentences = re.split(r'(?<=[.!?])\s+', text)
        return [s.strip() for s in sentences if s.strip()]

    def _estimate_tokens(self, text: str) -> int:
        """
        Estimate token count (rough approximation).
        Rule of thumb: ~4 chars per token for English text.
        """
        return max(1, len(text) // 4)

    def _create_chunk(
        self,
        sentences: List[str],
        paper_id: str,
        position: int,
        start_char: int
    ) -> Chunk:
        """Create Chunk object from sentences."""
        text = " ".join(sentences)
        return Chunk(
            chunk_id=f"{paper_id}_chunk_{position}",
            paper_id=paper_id,
            text=text,
            position=position,
            token_count=self._estimate_tokens(text),
            start_char=start_char,
            end_char=start_char + len(text)
        )

    def _get_overlap_text(self, sentences: List[str], overlap_tokens: int) -> str:
        """Get last N tokens from sentences for overlap."""
        combined = " ".join(sentences)
        tokens_estimate = self._estimate_tokens(combined)
        
        if tokens_estimate <= overlap_tokens:
            return combined

        # Calculate approximate character count for overlap
        overlap_chars = overlap_tokens * 4
        return combined[-overlap_chars:] if len(combined) > overlap_chars else combined

    def _split_long_sentence(
        self,
        sentence: str,
        paper_id: str,
        start_position: int
    ) -> List[Chunk]:
        """Force-split very long sentences."""
        chunks = []
        words = sentence.split()
        current_words = []
        current_tokens = 0

        for word in words:
            word_tokens = self._estimate_tokens(word)
            if current_tokens + word_tokens > self.chunk_size:
                if current_words:
                    text = " ".join(current_words)
                    chunks.append(Chunk(
                        chunk_id=f"{paper_id}_chunk_{start_position + len(chunks)}",
                        paper_id=paper_id,
                        text=text,
                        position=start_position + len(chunks),
                        token_count=self._estimate_tokens(text),
                        start_char=0,
                        end_char=len(text)
                    ))
                current_words = [word]
                current_tokens = word_tokens
            else:
                current_words.append(word)
                current_tokens += word_tokens

        if current_words:
            text = " ".join(current_words)
            chunks.append(Chunk(
                chunk_id=f"{paper_id}_chunk_{start_position + len(chunks)}",
                paper_id=paper_id,
                text=text,
                position=start_position + len(chunks),
                token_count=self._estimate_tokens(text),
                start_char=0,
                end_char=len(text)
            ))

        return chunks