"""
Local sentence-transformers embedder (no Docker required).
Generates embeddings and stores them locally for search.
"""

import logging
from typing import List, Dict, Optional
from sentence_transformers import SentenceTransformer
import numpy as np
import pickle
import os

from .config import config

logger = logging.getLogger(__name__)


class LocalEmbedder:
    """Local embedding generator using sentence-transformers."""

    def __init__(self, model_name: Optional[str] = None, save_path: Optional[str] = None):
        """
        Initialize local embedder.

        Args:
            model_name: Sentence-transformers model (default: from config)
            save_path: Path to save/load embeddings (default: from config)
        """
        model_name = model_name or config.local_model
        save_path = save_path or config.local_db_path
        
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        logger.info(f"Loading sentence-transformers model: {model_name}...")
        self.model = SentenceTransformer(model_name)
        self.save_path = save_path
        self.chunks_db = []
        self.embeddings = None
        
        dimension = self.model.get_sentence_embedding_dimension()
        logger.info(f"✅ Model loaded successfully (dimension: {dimension})")
        
        # Try to load existing embeddings if available
        if os.path.exists(save_path):
            self.load(save_path)

    def embed_chunks(self, chunks: List[Dict], show_progress: bool = False) -> List[Dict]:
        """
        Generate embeddings for chunks.

        Args:
            chunks: List of chunk dicts with 'chunk_id' and 'text' fields
            show_progress: Show progress bar during encoding

        Returns:
            List of result dicts with 'chunk_id' and 'success' fields
        """
        if not chunks:
            logger.warning("No chunks provided for embedding")
            return []

        texts = [chunk['text'] for chunk in chunks]
        
        logger.info(f"Generating embeddings for {len(texts)} chunks...")
        
        try:
            # Generate embeddings
            embeddings = self.model.encode(
                texts, 
                show_progress_bar=show_progress,
                convert_to_numpy=True
            )
            
            # Store in memory
            self.chunks_db.extend(chunks)
            
            if self.embeddings is None:
                self.embeddings = embeddings
            else:
                self.embeddings = np.vstack([self.embeddings, embeddings])
            
            # Auto-save after each batch
            self.save(self.save_path)
            
            logger.info(f"✅ Generated {len(embeddings)} embeddings (total: {len(self.chunks_db)})")
            
            return [{"chunk_id": c["chunk_id"], "success": True} for c in chunks]

        except Exception as e:
            logger.error(f"❌ Embedding generation failed: {e}")
            return [{"chunk_id": c.get("chunk_id", "unknown"), "success": False} for c in chunks]

    def search(self, query: str, limit: int = 10) -> List[Dict]:
        """
        Search using cosine similarity.

        Args:
            query: Search query text
            limit: Number of results to return

        Returns:
            List of matching chunks with metadata and similarity scores
        """
        if self.embeddings is None or len(self.chunks_db) == 0:
            logger.warning("No embeddings available for search")
            return []
        
        logger.info(f"Searching for: '{query[:50]}...' (limit={limit})")
        
        try:
            # Generate query embedding
            query_embedding = self.model.encode([query], convert_to_numpy=True)[0]
            
            # Calculate cosine similarity
            similarities = np.dot(self.embeddings, query_embedding) / (
                np.linalg.norm(self.embeddings, axis=1) * np.linalg.norm(query_embedding)
            )
            
            # Get top results
            top_indices = np.argsort(similarities)[::-1][:limit]
            
            results = []
            for idx in top_indices:
                chunk = self.chunks_db[idx]
                similarity = float(similarities[idx])
                results.append({
                    "chunk_id": chunk.get("chunk_id"),
                    "paper_id": chunk.get("paper_id"),
                    "text": chunk.get("text"),
                    "paper_title": chunk.get("paper_title", ""),
                    "paper_year": chunk.get("paper_year", 0),
                    "citation_count": chunk.get("citation_count", 0),
                    "similarity": similarity,
                    "distance": 1 - similarity,
                    "position": chunk.get("position", 0),
                })
            
            logger.info(f"✅ Search returned {len(results)} results")
            return results

        except Exception as e:
            logger.error(f"❌ Search failed: {e}")
            return []

    def save(self, filepath: Optional[str] = None):
        """
        Save embeddings database to disk.

        Args:
            filepath: Path to save file (default: self.save_path)
        """
        filepath = filepath or self.save_path
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        try:
            with open(filepath, 'wb') as f:
                pickle.dump({
                    'chunks': self.chunks_db,
                    'embeddings': self.embeddings
                }, f)
            
            logger.info(f"💾 Saved {len(self.chunks_db)} chunks to {filepath}")
        except Exception as e:
            logger.error(f"❌ Failed to save embeddings: {e}")

    def load(self, filepath: Optional[str] = None):
        """
        Load embeddings database from disk.

        Args:
            filepath: Path to load from (default: self.save_path)
        """
        filepath = filepath or self.save_path
        
        if not os.path.exists(filepath):
            logger.info(f"No existing embeddings found at {filepath} (will create new)")
            return
        
        try:
            with open(filepath, 'rb') as f:
                data = pickle.load(f)
                self.chunks_db = data['chunks']
                self.embeddings = data['embeddings']
            
            logger.info(f"📂 Loaded {len(self.chunks_db)} chunks from {filepath}")
        except Exception as e:
            logger.error(f"❌ Failed to load embeddings: {e}")
            self.chunks_db = []
            self.embeddings = None

    def get_stats(self) -> Dict:
        """Get statistics about stored embeddings."""
        return {
            "total_chunks": len(self.chunks_db),
            "embedding_dimension": self.embeddings.shape[1] if self.embeddings is not None else 0,
            "unique_papers": len(set(c.get("paper_id") for c in self.chunks_db)),
            "save_path": self.save_path,
        }