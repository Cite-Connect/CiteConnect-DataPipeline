"""
Weaviate vector store client.
Handles collection management and search operations.
"""

import logging
from typing import List, Dict, Optional, Any
import weaviate
from weaviate.classes.config import Configure, Property, DataType
from weaviate.classes.query import MetadataQuery

from .config import config

logger = logging.getLogger(__name__)


class WeaviateStore:
    """Weaviate vector database client."""

    def __init__(self):
        """Initialize Weaviate connection."""
        try:
            self.client = weaviate.connect_to_local(
                host=config.weaviate_url.replace('http://', '').replace('https://', ''),
                headers={'X-Weaviate-Api-Key': config.weaviate_api_key} if config.weaviate_api_key else None
            )
            logger.info(f"Connected to Weaviate at {config.weaviate_url}")
            self._setup_collection()
        except Exception as e:
            logger.error(f"Failed to connect to Weaviate: {e}")
            raise

    def _setup_collection(self):
        """Create collection if it doesn't exist."""
        try:
            if self.client.collections.exists(config.collection_name):
                logger.info(f"Collection '{config.collection_name}' already exists")
                self.collection = self.client.collections.get(config.collection_name)
            else:
                logger.info(f"Creating collection '{config.collection_name}'")
                self.collection = self.client.collections.create(
                    name=config.collection_name,
                    vectorizer_config=Configure.Vectorizer.text2vec_transformers(),
                    properties=[
                        Property(name="chunk_id", data_type=DataType.TEXT),
                        Property(name="paper_id", data_type=DataType.TEXT),
                        Property(name="text", data_type=DataType.TEXT),
                        Property(name="position", data_type=DataType.INT),
                        Property(name="token_count", data_type=DataType.INT),
                        Property(name="paper_title", data_type=DataType.TEXT),
                        Property(name="paper_year", data_type=DataType.INT),
                        Property(name="citation_count", data_type=DataType.INT),
                    ]
                )
                logger.info("Collection created successfully")
        except Exception as e:
            logger.error(f"Collection setup failed: {e}")
            raise

    def add_chunks(self, chunks: List[Dict]) -> bool:
        """
        Add chunks to Weaviate with automatic vectorization.

        Args:
            chunks: List of chunk dicts with required fields

        Returns:
            True if successful
        """
        try:
            with self.collection.batch.dynamic() as batch:
                for chunk in chunks:
                    batch.add_object(
                        properties={
                            "chunk_id": chunk.get("chunk_id"),
                            "paper_id": chunk.get("paper_id"),
                            "text": chunk.get("text"),
                            "position": chunk.get("position", 0),
                            "token_count": chunk.get("token_count", 0),
                            "paper_title": chunk.get("paper_title", ""),
                            "paper_year": chunk.get("paper_year", 0),
                            "citation_count": chunk.get("citation_count", 0),
                        }
                    )

            logger.info(f"Added {len(chunks)} chunks to Weaviate")
            return True

        except Exception as e:
            logger.error(f"Failed to add chunks to Weaviate: {e}")
            return False

    def search(
        self,
        query: str,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict]:
        """
        Semantic search using Weaviate's nearText.

        Args:
            query: Search query text
            limit: Number of results to return
            filters: Optional metadata filters (e.g., {'paper_year': 2023})

        Returns:
            List of matching chunks with metadata
        """
        try:
            response = self.collection.query.near_text(
                query=query,
                limit=limit,
                return_metadata=MetadataQuery(distance=True),
                # Add filters if provided
                # where=filters if filters else None
            )

            results = []
            for obj in response.objects:
                results.append({
                    "chunk_id": obj.properties.get("chunk_id"),
                    "paper_id": obj.properties.get("paper_id"),
                    "text": obj.properties.get("text"),
                    "paper_title": obj.properties.get("paper_title"),
                    "distance": obj.metadata.distance,
                    "position": obj.properties.get("position"),
                })

            logger.info(f"Search returned {len(results)} results for query: '{query[:50]}...'")
            return results

        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

    def delete_collection(self):
        """Delete the entire collection (use with caution)."""
        try:
            self.client.collections.delete(config.collection_name)
            logger.warning(f"Deleted collection: {config.collection_name}")
        except Exception as e:
            logger.error(f"Failed to delete collection: {e}")

    def close(self):
        """Close Weaviate connection."""
        self.client.close()
        logger.info("Weaviate connection closed")