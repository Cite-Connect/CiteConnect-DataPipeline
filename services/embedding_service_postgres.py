"""
PostgreSQL Embedding Service for CiteConnect.
Processes papers from GCS and generates dual embeddings (mini-LM + SPECTER).
"""

import logging
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import os
import sys

# Add utils to path for GCS reader
if '/opt/airflow' not in sys.path:
    sys.path.insert(0, '/opt/airflow')

from services.postgres_utils import (
    PostgreSQLConnectionPool,
    batch_insert_papers,
    batch_insert_embeddings,
    check_database_health
)
from services.column_mapper import (
    batch_map_papers,
    prepare_embedding_text,
    compute_text_hash,
    infer_search_term_from_filename
)

logger = logging.getLogger(__name__)


class PostgreSQLEmbeddingService:
    """
    Service for generating embeddings and storing in PostgreSQL.
    Supports dual embedding generation (mini-LM + SPECTER).
    """
    
    def __init__(
        self,
        gcs_bucket: str,
        gcs_prefix: str,
        db_config: Dict[str, Any],
        gcs_project_id: Optional[str] = None
    ):
        """
        Initialize embedding service.
        
        Args:
            gcs_bucket: GCS bucket name
            gcs_prefix: GCS prefix (e.g., 'processed/')
            db_config: PostgreSQL connection config
            gcs_project_id: GCP project ID
        """
        logger.info("="*60)
        logger.info("Initializing PostgreSQL Embedding Service")
        logger.info("="*60)
        
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix
        self.gcs_project_id = gcs_project_id
        
        # Initialize PostgreSQL connection pool
        self.db_pool = PostgreSQLConnectionPool(**db_config)
        
        # Initialize GCS reader
        try:
            from utils.gcs_reader import GCSReader
            self.gcs_reader = GCSReader(
                bucket_name=gcs_bucket,
                project_id=gcs_project_id
            )
            logger.info(f"✅ GCS Reader initialized: {gcs_bucket}/{gcs_prefix}")
        except Exception as e:
            logger.error(f"Failed to initialize GCS reader: {e}")
            raise
        
        # Embedding models (loaded lazily)
        self._minilm_model = None
        self._specter_model = None
        
        logger.info("✅ PostgreSQL Embedding Service initialized")
    
    def _load_minilm_model(self) -> SentenceTransformer:
        """Load mini-LM model (lazy loading)."""
        if self._minilm_model is None:
            logger.info("Loading mini-LM model (sentence-transformers/all-MiniLM-L6-v2)...")
            try:
                self._minilm_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
                dim = self._minilm_model.get_sentence_embedding_dimension()
                logger.info(f"✅ mini-LM model loaded (dimension: {dim})")
            except Exception as e:
                logger.error(f"Failed to load mini-LM model: {e}")
                raise
        return self._minilm_model
    
    def _load_specter_model(self) -> SentenceTransformer:
        """Load SPECTER model (lazy loading)."""
        if self._specter_model is None:
            logger.info("Loading SPECTER model (allenai/specter)...")
            try:
                self._specter_model = SentenceTransformer('allenai/specter')
                dim = self._specter_model.get_sentence_embedding_dimension()
                logger.info(f"✅ SPECTER model loaded (dimension: {dim})")
            except Exception as e:
                logger.error(f"Failed to load SPECTER model: {e}")
                raise
        return self._specter_model
    
    async def process_and_store_dual_embeddings(
        self,
        batch_size: int = 32,
        max_papers: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Main processing pipeline:
        1. Read papers from GCS
        2. Insert into PostgreSQL
        3. Generate mini-LM embeddings
        4. Generate SPECTER embeddings
        5. Export parquet for DVC
        
        Args:
            batch_size: Papers per batch
            max_papers: Maximum papers to process (None = all)
        
        Returns:
            Statistics dictionary (compatible with DVC tracking)
        """
        logger.info("\n" + "="*60)
        logger.info("STARTING DUAL EMBEDDING PIPELINE")
        logger.info("="*60)
        
        stats = {
            'total_papers': 0,
            'processed_papers': 0,
            'skipped_papers': 0,
            'embedded_chunks': 0,  # For DVC compatibility (actually embeddings count)
            'minilm_embeddings': 0,
            'specter_embeddings': 0,
            'params': {
                'batch_size': batch_size,
                'max_papers': max_papers,
                'gcs_bucket': self.gcs_bucket,
                'gcs_prefix': self.gcs_prefix
            }
        }
        
        try:
            # Step 1: Create database connection pool
            await self.db_pool.create_pool()
            
            # Step 2: Check database health
            logger.info("\n--- Health Check ---")
            is_healthy = await check_database_health(self.db_pool)
            if not is_healthy:
                raise Exception("Database health check failed")
            
            # Step 3: Load papers from GCS
            logger.info("\n--- Loading Papers from GCS ---")
            papers = await self._load_papers_from_gcs(max_papers)
            stats['total_papers'] = len(papers)
            
            if not papers:
                logger.warning("No papers loaded from GCS")
                return stats
            
            # Step 4: Insert papers into PostgreSQL
            logger.info("\n--- Inserting Papers into PostgreSQL ---")
            inserted_count = await self._insert_papers(papers, batch_size)
            stats['processed_papers'] = inserted_count
            
            # Step 5: Generate and store mini-LM embeddings
            logger.info("\n--- Generating mini-LM Embeddings ---")
            minilm_count = await self._generate_minilm_embeddings(papers, batch_size)
            stats['minilm_embeddings'] = minilm_count
            stats['embedded_chunks'] += minilm_count  # For DVC compatibility
            
            # Step 6: Generate and store SPECTER embeddings
            logger.info("\n--- Generating SPECTER Embeddings ---")
            specter_count = await self._generate_specter_embeddings(papers, batch_size)
            stats['specter_embeddings'] = specter_count
            stats['embedded_chunks'] += specter_count
            
            # Close pool
            await self.db_pool.close_pool()
            
            logger.info("\n" + "="*60)
            logger.info("PIPELINE COMPLETE")
            logger.info("="*60)
            logger.info(f"Total papers: {stats['total_papers']}")
            logger.info(f"Processed: {stats['processed_papers']}")
            logger.info(f"mini-LM embeddings: {stats['minilm_embeddings']}")
            logger.info(f"SPECTER embeddings: {stats['specter_embeddings']}")
            logger.info("="*60)
            
            return stats
        
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
    
    async def _load_papers_from_gcs(self, max_papers: Optional[int]) -> List[Dict[str, Any]]:
        """Load and parse papers from GCS parquet files."""
        logger.info(f"Reading parquet files from gs://{self.gcs_bucket}/{self.gcs_prefix}")
        
        try:
            # Read all parquet files from GCS prefix
            df_all = self.gcs_reader.read_all_from_domain(
                domain="",  # Empty means read all
                custom_prefix=self.gcs_prefix,
                flat_structure=True
            )
            
            if df_all.empty:
                logger.warning("No data found in GCS")
                return []
            
            logger.info(f"Loaded {len(df_all)} papers from GCS")
            
            # Limit if requested
            if max_papers and len(df_all) > max_papers:
                logger.info(f"Limiting to {max_papers} papers")
                df_all = df_all.head(max_papers)
            
            # Map to paper dictionaries
            # Try to infer search term from GCS path if possible
            papers = batch_map_papers(df_all, search_term=None, filename=self.gcs_prefix)
            
            logger.info(f"Successfully mapped {len(papers)} papers")
            return papers
        
        except Exception as e:
            logger.error(f"Failed to load papers from GCS: {e}", exc_info=True)
            raise
    
    async def _insert_papers(
        self,
        papers: List[Dict[str, Any]],
        batch_size: int
    ) -> int:
        """Insert papers into PostgreSQL papers table."""
        logger.info(f"Inserting {len(papers)} papers into PostgreSQL...")
        
        try:
            inserted_count = await batch_insert_papers(
                self.db_pool,
                papers,
                batch_size=batch_size
            )
            
            logger.info(f"✅ Inserted {inserted_count} papers")
            return inserted_count
        
        except Exception as e:
            logger.error(f"Failed to insert papers: {e}", exc_info=True)
            raise
    
    async def _generate_minilm_embeddings(
        self,
        papers: List[Dict[str, Any]],
        batch_size: int
    ) -> int:
        """Generate and store mini-LM embeddings."""
        logger.info("Generating mini-LM embeddings (384-dim)...")
        
        # Load model
        model = self._load_minilm_model()
        
        embeddings_data = []
        
        for i in range(0, len(papers), batch_size):
            batch = papers[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(papers) - 1) // batch_size + 1
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} papers)")
            
            # Prepare texts
            texts = []
            paper_ids = []
            text_info = []
            
            for paper in batch:
                text = prepare_embedding_text(paper)
                if len(text) > 50:  # Ensure substantial text
                    texts.append(text)
                    paper_ids.append(paper['paper_id'])
                    text_info.append({
                        'paper_id': paper['paper_id'],
                        'text': text,
                        'text_length': len(text),
                        'has_introduction': paper.get('has_introduction', False)
                    })
            
            if not texts:
                logger.warning(f"Batch {batch_num}: No valid texts to embed")
                continue
            
            # Generate embeddings
            try:
                embeddings = model.encode(
                    texts,
                    batch_size=8,
                    show_progress_bar=False,
                    convert_to_numpy=True
                )
                
                # Prepare for database insertion
                for idx, (paper_id, info) in enumerate(zip(paper_ids, text_info)):
                    embedding_record = {
                        'paper_id': paper_id,
                        'embedding': embeddings[idx].tolist(),  # Convert numpy to list
                        'model_name': 'sentence-transformers/all-MiniLM-L6-v2',
                        'model_version': 'v1.0',
                        'embedding_source': f"title + abstract + introduction ({info['text_length']} chars)",
                        'source_hash': compute_text_hash(info['text']),
                        'text_length': info['text_length'],
                        'has_introduction': info['has_introduction'],
                        'created_at': datetime.utcnow()
                    }
                    embeddings_data.append(embedding_record)
                
                logger.info(f"Generated {len(embeddings)} mini-LM embeddings")
            
            except Exception as e:
                logger.error(f"Failed to generate embeddings for batch {batch_num}: {e}")
        
        # Insert embeddings into database
        if embeddings_data:
            logger.info(f"Storing {len(embeddings_data)} mini-LM embeddings in PostgreSQL...")
            inserted_count = await batch_insert_embeddings(
                self.db_pool,
                'paper_embeddings_minilm',
                embeddings_data,
                batch_size=batch_size
            )
            logger.info(f"✅ Stored {inserted_count} mini-LM embeddings")
            return inserted_count
        
        return 0
    
    async def _generate_specter_embeddings(
        self,
        papers: List[Dict[str, Any]],
        batch_size: int
    ) -> int:
        """Generate and store SPECTER embeddings."""
        logger.info("Generating SPECTER embeddings (768-dim)...")
        
        # Load model
        model = self._load_specter_model()
        
        embeddings_data = []
        
        for i in range(0, len(papers), batch_size):
            batch = papers[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(papers) - 1) // batch_size + 1
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} papers)")
            
            # Prepare texts (same as mini-LM)
            texts = []
            paper_ids = []
            text_info = []
            
            for paper in batch:
                text = prepare_embedding_text(paper)
                if len(text) > 50:
                    texts.append(text)
                    paper_ids.append(paper['paper_id'])
                    text_info.append({
                        'paper_id': paper['paper_id'],
                        'text': text,
                        'text_length': len(text),
                        'has_introduction': paper.get('has_introduction', False)
                    })
            
            if not texts:
                logger.warning(f"Batch {batch_num}: No valid texts to embed")
                continue
            
            # Generate embeddings
            try:
                embeddings = model.encode(
                    texts,
                    batch_size=4,
                    show_progress_bar=False,
                    convert_to_numpy=True
                )
                
                # Prepare for database insertion
                for idx, (paper_id, info) in enumerate(zip(paper_ids, text_info)):
                    embedding_record = {
                        'paper_id': paper_id,
                        'embedding': embeddings[idx].tolist(),
                        'model_name': 'allenai/specter',
                        'model_version': 'v1.0',
                        'embedding_source': f"title + abstract + introduction ({info['text_length']} chars)",
                        'source_hash': compute_text_hash(info['text']),
                        'text_length': info['text_length'],
                        'has_introduction': info['has_introduction'],
                        'created_at': datetime.utcnow()
                    }
                    embeddings_data.append(embedding_record)
                
                logger.info(f"Generated {len(embeddings)} SPECTER embeddings")
            
            except Exception as e:
                logger.error(f"Failed to generate embeddings for batch {batch_num}: {e}")
        
        # Insert embeddings into database
        if embeddings_data:
            logger.info(f"Storing {len(embeddings_data)} SPECTER embeddings in PostgreSQL...")
            inserted_count = await batch_insert_embeddings(
                self.db_pool,
                'paper_embeddings_specter',
                embeddings_data,
                batch_size=batch_size
            )
            logger.info(f"✅ Stored {inserted_count} SPECTER embeddings")
            return inserted_count
        
        return 0
    
    def export_embeddings_to_parquet(
        self,
        output_dir: str = 'working_data'
    ) -> Dict[str, str]:
        """
        Export embeddings to parquet files for DVC tracking.
        
        Args:
            output_dir: Output directory
        
        Returns:
            Dictionary with file paths
        """
        logger.info("\n--- Exporting Embeddings to Parquet ---")
        
        os.makedirs(output_dir, exist_ok=True)
        
        files = {}
        
        # This would query PostgreSQL and export to parquet
        # For now, we'll create placeholder files that DVC can track
        # In the actual implementation, you'd query the embeddings and save them
        
        logger.info(f"✅ Embeddings available in PostgreSQL tables")
        logger.info(f"   - paper_embeddings_minilm (384-dim)")
        logger.info(f"   - paper_embeddings_specter (768-dim)")
        logger.info(f"\nNote: DVC can track the database state via run_summary.json")
        
        return files