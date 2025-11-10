"""
Configuration for embedding generation.
Loads settings from environment variables with proper defaults.
Supports local, Weaviate, and OpenAI providers.
"""

import os
from dataclasses import dataclass
from typing import Optional, Literal
import logging

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not required, will use os.getenv defaults

logger = logging.getLogger(__name__)


@dataclass
class EmbeddingConfig:
    """Configuration for embedding system."""
    
    # ============================================
    # PROVIDER SELECTION
    # ============================================
    # Options: 'local' (no Docker/cloud), 'weaviate' (Docker/cloud), or 'openai' (API)
    provider: str = os.getenv('EMBEDDING_PROVIDER', 'local')
    
    # ============================================
    # LOCAL PROVIDER SETTINGS
    # ============================================
    # Sentence-transformers model name
    local_model: str = os.getenv('SENTENCE_TRANSFORMERS_MODEL', 'all-MiniLM-L6-v2')
    # Path to save/load local embeddings database
    local_db_path: str = os.getenv('LOCAL_EMBEDDINGS_PATH', '/opt/airflow/working_data/embeddings_db.pkl')
    
    # ============================================
    # WEAVIATE SETTINGS
    # ============================================
    weaviate_url: str = os.getenv('WEAVIATE_URL', 'http://localhost:8080')
    weaviate_api_key: Optional[str] = os.getenv('WEAVIATE_API_KEY')
    collection_name: str = os.getenv('WEAVIATE_COLLECTION', 'PaperChunks')
    
    # ============================================
    # OPENAI SETTINGS
    # ============================================
    openai_api_key: Optional[str] = os.getenv('OPENAI_API_KEY')
    openai_model: str = os.getenv('OPENAI_EMBEDDING_MODEL', 'text-embedding-3-small')
    
    # ============================================
    # GENERAL SETTINGS
    # ============================================
    # Embedding dimensions (auto-detected from model)
    embedding_dim: int = int(os.getenv('EMBEDDING_DIM', '384'))
    
    # Batch processing size
    batch_size: int = int(os.getenv('EMBEDDING_BATCH_SIZE', '100'))
    
    # Chunking parameters
    chunk_size: int = int(os.getenv('CHUNK_SIZE', '512'))
    chunk_overlap: int = int(os.getenv('CHUNK_OVERLAP', '50'))

    def validate(self) -> bool:
        """
        Validate configuration based on selected provider.
        
        Returns:
            True if valid
            
        Raises:
            ValueError: If configuration is invalid
        """
        # Normalize provider string
        self.provider = self.provider.lower().strip()
        
        # Check provider is valid
        valid_providers = ['local', 'weaviate', 'openai']
        if self.provider not in valid_providers:
            raise ValueError(
                f"Invalid provider: '{self.provider}'. "
                f"Must be one of: {', '.join(valid_providers)}"
            )
        
        # Provider-specific validation
        if self.provider == 'weaviate':
            if not self.weaviate_url:
                raise ValueError("WEAVIATE_URL is required when provider='weaviate'")
            logger.info(f"Weaviate configuration: {self.weaviate_url}")
            
        elif self.provider == 'openai':
            if not self.openai_api_key:
                raise ValueError("OPENAI_API_KEY is required when provider='openai'")
            logger.info(f"OpenAI configuration: model={self.openai_model}")
            
        elif self.provider == 'local':
            logger.info(f"Local configuration: model={self.local_model}, save_path={self.local_db_path}")
        
        # Validate chunking parameters
        if self.chunk_overlap >= self.chunk_size:
            raise ValueError(
                f"CHUNK_OVERLAP ({self.chunk_overlap}) must be less than "
                f"CHUNK_SIZE ({self.chunk_size})"
            )
        
        return True

    def __post_init__(self):
        """Post-initialization validation."""
        try:
            self.validate()
            logger.info(f"✅ Configuration validated successfully (provider={self.provider})")
        except ValueError as e:
            logger.error(f"❌ Configuration validation failed: {e}")
            # Fallback to safe defaults
            logger.warning("⚠️ Falling back to provider='local' with default settings")
            self.provider = 'local'
            self.local_model = 'all-MiniLM-L6-v2'
            self.local_db_path = 'embeddings_db.pkl'


# ============================================
# GLOBAL CONFIG INSTANCE
# ============================================
config = EmbeddingConfig()

# Log final configuration
logger.info(f"Embedding configuration loaded: provider={config.provider}")