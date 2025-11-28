"""
Test PostgreSQL Embedding Service with sample data.
"""

import asyncio
import logging
import sys
import os

# Setup paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Load environment
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(project_root, '.env'))
except ImportError:
    pass

from services.embedding_service_postgres import PostgreSQLEmbeddingService

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)


async def test_service():
    """Test the embedding service with minimal configuration."""
    
    logger.info("="*80)
    logger.info("TESTING POSTGRESQL EMBEDDING SERVICE")
    logger.info("="*80)
    
    # Database configuration
    db_config = {
        'host': os.getenv('SUPABASE_DB_HOST'),
        'port': int(os.getenv('SUPABASE_DB_PORT', 5432)),
        'database': os.getenv('SUPABASE_DB_NAME'),
        'user': os.getenv('SUPABASE_DB_USER'),
        'password': os.getenv('SUPABASE_DB_PASSWORD'),
    }
    
    if not db_config['password']:
        logger.error("❌ SUPABASE_DB_PASSWORD not set")
        return
    
    try:
        # Initialize service
        service = PostgreSQLEmbeddingService(
            gcs_bucket=os.getenv('GCS_BUCKET_NAME', 'citeconnect-test-bucket'),
            gcs_prefix=os.getenv('GCS_PREFIX', 'processed_v2/'),
            db_config=db_config,
            gcs_project_id=os.getenv('GCS_PROJECT_ID', 'strange-calling-476017-r5')
        )
        
        # Run pipeline with max 5 papers for testing
        logger.info("\n🚀 Running pipeline with max_papers=5 for testing...")
        stats = await service.process_and_store_dual_embeddings(
            batch_size=5,
            max_papers=5
        )
        
        # Display results
        logger.info("\n" + "="*80)
        logger.info("TEST RESULTS")
        logger.info("="*80)
        logger.info(f"Total papers: {stats['total_papers']}")
        logger.info(f"Processed: {stats['processed_papers']}")
        logger.info(f"mini-LM embeddings: {stats['minilm_embeddings']}")
        logger.info(f"SPECTER embeddings: {stats['specter_embeddings']}")
        logger.info("="*80)
        
        if stats['minilm_embeddings'] > 0 and stats['specter_embeddings'] > 0:
            logger.info("\n🎉 TEST PASSED: Embeddings generated successfully!")
            return True
        else:
            logger.error("\n❌ TEST FAILED: No embeddings generated")
            return False
    
    except Exception as e:
        logger.error(f"\n❌ TEST FAILED: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = asyncio.run(test_service())
    sys.exit(0 if success else 1)