"""
Main embedding generation logic with provider abstraction.
Supports Local (sentence-transformers), Weaviate, and OpenAI.
"""

import logging
from typing import List, Dict, Optional
from dataclasses import dataclass
import time

from .config import config

logger = logging.getLogger(__name__)


@dataclass
class EmbeddingResult:
    """Result of embedding generation."""
    chunk_id: str
    embedding: List[float]
    success: bool
    error: Optional[str] = None


class EmbeddingGenerator:
    """Generates embeddings using configured provider."""

    def __init__(self, provider: Optional[str] = None):
        """
        Initialize embedding generator.

        Args:
            provider: Override config provider ('local', 'weaviate', or 'openai')
        """
        self.provider = provider or config.provider
        logger.info(f"Initialized EmbeddingGenerator with provider: {self.provider}")

        if self.provider == 'local':
            from .local_embedder import LocalEmbedder
            self.local_embedder = LocalEmbedder()
            logger.info(f"✅ Using local sentence-transformers (model: {config.local_model})")
            
        elif self.provider == 'weaviate':
            from .vector_store import WeaviateStore
            self.vector_store = WeaviateStore()
            logger.info("✅ Using Weaviate's built-in vectorization")
            
        elif self.provider == 'openai':
            self._init_openai()
            logger.info(f"✅ Using OpenAI model: {config.openai_model}")
            
        else:
            raise ValueError(
                f"Unknown provider: {self.provider}. "
                f"Valid options: 'local', 'weaviate', 'openai'"
            )

    def _init_openai(self):
        """Initialize OpenAI client (lazy import)."""
        try:
            from openai import OpenAI
            self.openai_client = OpenAI(api_key=config.openai_api_key)
        except ImportError:
            logger.error("OpenAI library not installed. Run: pip install openai")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {e}")
            raise

    def embed_chunks(
        self,
        chunks: List[Dict],
        show_progress: bool = True
    ) -> List[EmbeddingResult]:
        """
        Generate embeddings for list of chunks.

        Args:
            chunks: List of chunk dicts with 'chunk_id', 'text', 'paper_id'
            show_progress: Log progress during processing

        Returns:
            List of EmbeddingResult objects
        """
        if not chunks:
            logger.warning("No chunks provided for embedding")
            return []

        logger.info(f"Generating embeddings for {len(chunks)} chunks using provider: {self.provider}")
        start_time = time.time()

        if self.provider == 'local':
            results = self._embed_with_local(chunks, show_progress)
        elif self.provider == 'weaviate':
            results = self._embed_with_weaviate(chunks, show_progress)
        else:
            results = self._embed_with_openai(chunks, show_progress)

        elapsed = time.time() - start_time
        success_count = sum(1 for r in results if r.success)
        logger.info(
            f"Embedding complete: {success_count}/{len(chunks)} successful "
            f"in {elapsed:.2f}s ({len(chunks)/elapsed:.1f} chunks/s)"
        )

        return results

    def _embed_with_local(
        self,
        chunks: List[Dict],
        show_progress: bool
    ) -> List[EmbeddingResult]:
        """
        Generate embeddings using local sentence-transformers.

        Args:
            chunks: List of chunk dicts
            show_progress: Show progress bar

        Returns:
            List of EmbeddingResult objects
        """
        try:
            # Generate embeddings (auto-saves internally)
            results = self.local_embedder.embed_chunks(chunks, show_progress=show_progress)
            
            # Convert to EmbeddingResult format
            return [
                EmbeddingResult(
                    chunk_id=r['chunk_id'],
                    embedding=[],  # Stored internally in local_embedder
                    success=r['success'],
                    error=None if r['success'] else "Embedding failed"
                )
                for r in results
            ]

        except Exception as e:
            logger.error(f"Local embedding batch failed: {e}")
            return [
                EmbeddingResult(
                    chunk_id=chunk.get('chunk_id', 'unknown'),
                    embedding=[],
                    success=False,
                    error=str(e)
                )
                for chunk in chunks
            ]

    def _embed_with_weaviate(
        self,
        chunks: List[Dict],
        show_progress: bool
    ) -> List[EmbeddingResult]:
        """
        Store chunks in Weaviate (vectorization happens automatically).

        Args:
            chunks: List of chunk dicts
            show_progress: Log progress

        Returns:
            List of EmbeddingResult objects
        """
        results = []
        batch_size = config.batch_size

        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]
            
            if show_progress:
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(chunks)-1)//batch_size + 1}")

            try:
                # Weaviate handles vectorization internally
                success = self.vector_store.add_chunks(batch)
                
                for chunk in batch:
                    results.append(EmbeddingResult(
                        chunk_id=chunk['chunk_id'],
                        embedding=[],  # Managed by Weaviate internally
                        success=success,
                        error=None if success else "Batch insertion failed"
                    ))

            except Exception as e:
                logger.error(f"Batch embedding failed: {e}")
                for chunk in batch:
                    results.append(EmbeddingResult(
                        chunk_id=chunk['chunk_id'],
                        embedding=[],
                        success=False,
                        error=str(e)
                    ))

            time.sleep(0.1)  # Rate limiting

        return results

    def _embed_with_openai(
        self,
        chunks: List[Dict],
        show_progress: bool
    ) -> List[EmbeddingResult]:
        """
        Generate embeddings using OpenAI API.

        Args:
            chunks: List of chunk dicts
            show_progress: Log progress

        Returns:
            List of EmbeddingResult objects
        """
        results = []
        batch_size = min(config.batch_size, 2048)  # OpenAI limit

        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]
            
            if show_progress:
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(chunks)-1)//batch_size + 1}")

            texts = [chunk['text'] for chunk in batch]

            try:
                response = self.openai_client.embeddings.create(
                    input=texts,
                    model=config.openai_model
                )

                for chunk, embedding_obj in zip(batch, response.data):
                    results.append(EmbeddingResult(
                        chunk_id=chunk['chunk_id'],
                        embedding=embedding_obj.embedding,
                        success=True,
                        error=None
                    ))

            except Exception as e:
                logger.error(f"OpenAI batch embedding failed: {e}")
                for chunk in batch:
                    results.append(EmbeddingResult(
                        chunk_id=chunk['chunk_id'],
                        embedding=[],
                        success=False,
                        error=str(e)
                    ))

            time.sleep(1)  # OpenAI rate limiting

        return results

    def embed_single(self, text: str) -> Optional[List[float]]:
        """
        Generate embedding for single text (query use case).

        Args:
            text: Text to embed

        Returns:
            Embedding vector or None on failure
        """
        if self.provider == 'local':
            try:
                embedding = self.local_embedder.model.encode([text], convert_to_numpy=True)[0]
                return embedding.tolist()
            except Exception as e:
                logger.error(f"Single embedding generation failed: {e}")
                return None
                
        elif self.provider == 'weaviate':
            # For queries, we use Weaviate's nearText search directly
            logger.warning("Use vector_store.search() for query embeddings with Weaviate")
            return None
            
        else:  # openai
            try:
                response = self.openai_client.embeddings.create(
                    input=[text],
                    model=config.openai_model
                )
                return response.data[0].embedding
            except Exception as e:
                logger.error(f"Single embedding generation failed: {e}")
                return None

    def search(self, query: str, limit: int = 10) -> List[Dict]:
        """
        Search for similar chunks.

        Args:
            query: Search query text
            limit: Number of results to return

        Returns:
            List of matching chunks with metadata
        """
        if self.provider == 'local':
            return self.local_embedder.search(query, limit)
        elif self.provider == 'weaviate':
            return self.vector_store.search(query, limit)
        else:
            logger.error("Search not supported for OpenAI provider (use local or weaviate)")
            return []