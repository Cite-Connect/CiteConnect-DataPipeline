"""
Standalone script to populate PostgreSQL database with all papers from GCS.
Run this ONCE on your Mac to do initial data population.
After this, Airflow handles incremental updates.

Usage:
    python scripts/populate_database_standalone.py
    
    Or with options:
    python scripts/populate_database_standalone.py --max-papers 100 --skip-existing
"""

import asyncio
import logging
import sys
import os
import argparse
from dotenv import load_dotenv

# Setup
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
load_dotenv(os.path.join(project_root, '.env'))

from services.embedding_service_postgres import PostgreSQLEmbeddingService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)


async def populate_database(
    max_papers: int = None,
    batch_size: int = 32,
    gcs_prefix: str = 'processed_v2/',
    skip_existing: bool = True
):
    """
    Populate database with all papers from GCS.
    
    Args:
        max_papers: Limit papers to process (None = all)
        batch_size: Papers per batch
        gcs_prefix: GCS folder path
        skip_existing: Skip papers that already have embeddings
    """
    
    logger.info("="*80)
    logger.info("CITECONNECT DATABASE POPULATION (STANDALONE)")
    logger.info("="*80)
    logger.info(f"GCS Prefix: {gcs_prefix}")
    logger.info(f"Max Papers: {max_papers or 'ALL'}")
    logger.info(f"Batch Size: {batch_size}")
    logger.info(f"Skip Existing: {skip_existing}")
    logger.info("="*80)
    
    # Database configuration
    db_config = {
        'host': os.getenv('SUPABASE_DB_HOST'),
        'port': int(os.getenv('SUPABASE_DB_PORT', 5432)),
        'database': os.getenv('SUPABASE_DB_NAME'),
        'user': os.getenv('SUPABASE_DB_USER'),
        'password': os.getenv('SUPABASE_DB_PASSWORD'),
    }
    
    # Initialize service
    service = PostgreSQLEmbeddingService(
        gcs_bucket=os.getenv('GCS_BUCKET_NAME', 'citeconnect-test-bucket'),
        gcs_prefix=gcs_prefix,
        db_config=db_config,
        gcs_project_id=os.getenv('GCS_PROJECT_ID', 'strange-calling-476017-r5')
    )
    
    # Process all papers
    stats = await service.process_and_store_dual_embeddings(
        batch_size=batch_size,
        max_papers=max_papers
    )
    
    # Display results
    logger.info("\n" + "="*80)
    logger.info("POPULATION COMPLETE")
    logger.info("="*80)
    logger.info(f"Papers in GCS: {stats['total_papers']}")
    logger.info(f"Papers inserted: {stats['processed_papers']}")
    logger.info(f"mini-LM embeddings: {stats['minilm_embeddings']}")
    logger.info(f"SPECTER embeddings: {stats['specter_embeddings']}")
    logger.info(f"Total embeddings: {stats['embedded_chunks']}")
    logger.info("="*80)
    
    return stats


async def main():
    """Main entry point with argument parsing."""
    
    parser = argparse.ArgumentParser(
        description='Populate CiteConnect database with papers and embeddings',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--max-papers',
        type=int,
        default=None,
        help='Maximum papers to process (default: all)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=32,
        help='Papers per batch (default: 32)'
    )
    
    parser.add_argument(
        '--gcs-prefix',
        type=str,
        default='processed_v2/',
        help='GCS folder path (default: processed_v2/)'
    )
    
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        default=True,
        help='Skip papers that already have embeddings (default: True)'
    )
    
    args = parser.parse_args()
    
    try:
        stats = await populate_database(
            max_papers=args.max_papers,
            batch_size=args.batch_size,
            gcs_prefix=args.gcs_prefix,
            skip_existing=args.skip_existing
        )
        
        logger.info("\n✅ SUCCESS: Database populated")
        return 0
        
    except Exception as e:
        logger.error(f"\n❌ FAILED: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)