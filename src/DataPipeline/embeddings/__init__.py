"""
CiteConnect Embeddings Module
Handles embedding generation and vector storage.
Supports Weaviate (cloud/Docker) and local sentence-transformers.
"""

from .config import EmbeddingConfig, config
from .embed_generator import EmbeddingGenerator, EmbeddingResult
from .local_embedder import LocalEmbedder

# Conditional imports (only load if needed)
try:
    from .vector_store import WeaviateStore
    __all__ = ["EmbeddingConfig", "config", "EmbeddingGenerator", "EmbeddingResult", "LocalEmbedder", "WeaviateStore"]
except ImportError:
    # Weaviate not available, only expose local embedder
    __all__ = ["EmbeddingConfig", "config", "EmbeddingGenerator", "EmbeddingResult", "LocalEmbedder"]