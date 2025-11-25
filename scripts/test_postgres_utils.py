"""
Test script for postgres_utils.py
Tests connection pool, health check, and basic operations.
"""

import asyncio
import logging
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
print(f"Project root: {project_root}")

try:
    from dotenv import load_dotenv
    env_path = os.path.join(project_root, '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✅ Loaded environment from: {env_path}")
    else:
        print(f"⚠️  No .env file found at {env_path}")
        print(f"   Using system environment variables")
except ImportError:
    print(f"⚠️  python-dotenv not installed, using system environment variables")
    print(f"   Install with: pip install python-dotenv")

from services.postgres_utils import (
    PostgreSQLConnectionPool,
    check_database_health,
    batch_insert_papers
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def test_connection_pool():
    """Test 1: Connection pool creation and basic query."""
    logger.info("\n" + "="*60)
    logger.info("TEST 1: Connection Pool Creation")
    logger.info("="*60)
    
    # Get credentials from environment
    db_config = {
        'host': os.getenv('SUPABASE_DB_HOST', 'db.wvvogncqrqzfbfztkwfo.supabase.co'),
        'port': int(os.getenv('SUPABASE_DB_PORT', 5432)),
        'database': os.getenv('SUPABASE_DB_NAME', 'postgres'),
        'user': os.getenv('SUPABASE_DB_USER', 'postgres'),
        'password': os.getenv('SUPABASE_DB_PASSWORD'),
    }
    
    if not db_config['password']:
        logger.error("❌ SUPABASE_DB_PASSWORD not set in environment")
        return False
    
    try:
        # Create pool
        pool = PostgreSQLConnectionPool(**db_config)
        await pool.create_pool()
        
        # Test query
        async with pool.acquire() as conn:
            result = await conn.fetchval('SELECT 1')
            logger.info(f"✅ Test query result: {result}")
            
            # Check PostgreSQL version
            version = await conn.fetchval('SELECT version()')
            logger.info(f"✅ PostgreSQL version: {version[:50]}...")
        
        # Close pool
        await pool.close_pool()
        logger.info("✅ Test 1 PASSED: Connection pool works")
        return True
        
    except Exception as e:
        logger.error(f"❌ Test 1 FAILED: {e}", exc_info=True)
        return False


async def test_database_health():
    """Test 2: Database health check."""
    logger.info("\n" + "="*60)
    logger.info("TEST 2: Database Health Check")
    logger.info("="*60)
    
    db_config = {
        'host': os.getenv('SUPABASE_DB_HOST', 'db.wvvogncqrqzfbfztkwfo.supabase.co'),
        'port': int(os.getenv('SUPABASE_DB_PORT', 5432)),
        'database': os.getenv('SUPABASE_DB_NAME', 'postgres'),
        'user': os.getenv('SUPABASE_DB_USER', 'postgres'),
        'password': os.getenv('SUPABASE_DB_PASSWORD'),
    }
    
    try:
        pool = PostgreSQLConnectionPool(**db_config)
        await pool.create_pool()
        
        # Run health check
        is_healthy = await check_database_health(pool)
        
        if is_healthy:
            logger.info("✅ Test 2 PASSED: Database is healthy")
            
            # Show table info
            async with pool.acquire() as conn:
                # Count rows in each table
                papers_count = await conn.fetchval('SELECT COUNT(*) FROM papers')
                minilm_count = await conn.fetchval('SELECT COUNT(*) FROM paper_embeddings_minilm')
                specter_count = await conn.fetchval('SELECT COUNT(*) FROM paper_embeddings_specter')
                
                logger.info(f"📊 Current row counts:")
                logger.info(f"   papers: {papers_count}")
                logger.info(f"   paper_embeddings_minilm: {minilm_count}")
                logger.info(f"   paper_embeddings_specter: {specter_count}")
        else:
            logger.error("❌ Test 2 FAILED: Database health check failed")
            return False
        
        await pool.close_pool()
        return True
        
    except Exception as e:
        logger.error(f"❌ Test 2 FAILED: {e}", exc_info=True)
        return False


async def test_batch_insert():
    """Test 3: Batch insert test papers."""
    logger.info("\n" + "="*60)
    logger.info("TEST 3: Batch Insert Papers")
    logger.info("="*60)
    
    db_config = {
        'host': os.getenv('SUPABASE_DB_HOST', 'db.wvvogncqrqzfbfztkwfo.supabase.co'),
        'port': int(os.getenv('SUPABASE_DB_PORT', 5432)),
        'database': os.getenv('SUPABASE_DB_NAME', 'postgres'),
        'user': os.getenv('SUPABASE_DB_USER', 'postgres'),
        'password': os.getenv('SUPABASE_DB_PASSWORD'),
    }
    
    try:
        pool = PostgreSQLConnectionPool(**db_config)
        await pool.create_pool()
        
        # Create test papers
        from datetime import datetime
        test_papers = [
            {
                'paper_id': 'test:001',
                'title': 'Test Paper 1: Machine Learning for Healthcare',
                'abstract': 'This is a test abstract about machine learning applications in healthcare.',
                'introduction': 'Introduction text for test paper 1. This discusses ML methods.',
                'summary': 'Test summary 1',
                'authors': ['Test Author 1', 'Test Author 2'],
                'year': 2024,
                'venue': 'Test Conference 2024',
                'citation_count': 10,
                'domain': 'healthcare',
                'gcs_pdf_path': 'gs://test-bucket/test001.pdf',
                'extraction_method': 'arxiv_html',
                'content_quality': 'high',
                'has_introduction': True,
                'intro_length': 250,
                'ingested_at': datetime.utcnow()
            },
            {
                'paper_id': 'test:002',
                'title': 'Test Paper 2: Deep Learning for Finance',
                'abstract': 'This is a test abstract about deep learning in financial markets.',
                'introduction': 'Introduction for test paper 2 about finance.',
                'summary': 'Test summary 2',
                'authors': ['Test Author 3'],
                'year': 2023,
                'venue': 'Test Journal 2023',
                'citation_count': 5,
                'domain': 'fintech',
                'gcs_pdf_path': 'gs://test-bucket/test002.pdf',
                'extraction_method': 'grobid_pdf',
                'content_quality': 'medium',
                'has_introduction': True,
                'intro_length': 180,
                'ingested_at': datetime.utcnow()
            }
        ]
        
        # Insert test papers
        logger.info(f"Inserting {len(test_papers)} test papers...")
        inserted_count = await batch_insert_papers(pool, test_papers, batch_size=10)
        
        if inserted_count == len(test_papers):
            logger.info(f"✅ Successfully inserted {inserted_count} test papers")
            
            # Verify insertion
            async with pool.acquire() as conn:
                result = await conn.fetch(
                    "SELECT paper_id, title, domain FROM papers WHERE paper_id LIKE 'test:%' ORDER BY paper_id"
                )
                
                logger.info(f"📋 Inserted papers:")
                for row in result:
                    logger.info(f"   {row['paper_id']}: {row['title']} [{row['domain']}]")
            
            logger.info("✅ Test 3 PASSED: Batch insert works")
            success = True
        else:
            logger.error(f"❌ Expected {len(test_papers)} insertions, got {inserted_count}")
            success = False
        
        await pool.close_pool()
        return success
        
    except Exception as e:
        logger.error(f"❌ Test 3 FAILED: {e}", exc_info=True)
        return False


async def cleanup_test_data():
    """Cleanup: Remove test papers."""
    logger.info("\n" + "="*60)
    logger.info("CLEANUP: Removing Test Data")
    logger.info("="*60)
    
    db_config = {
        'host': os.getenv('SUPABASE_DB_HOST', 'db.wvvogncqrqzfbfztkwfo.supabase.co'),
        'port': int(os.getenv('SUPABASE_DB_PORT', 5432)),
        'database': os.getenv('SUPABASE_DB_NAME', 'postgres'),
        'user': os.getenv('SUPABASE_DB_USER', 'postgres'),
        'password': os.getenv('SUPABASE_DB_PASSWORD'),
    }
    
    try:
        pool = PostgreSQLConnectionPool(**db_config)
        await pool.create_pool()
        
        async with pool.acquire() as conn:
            result = await conn.execute("DELETE FROM papers WHERE paper_id LIKE 'test:%'")
            logger.info(f"✅ Cleanup complete: {result}")
        
        await pool.close_pool()
        
    except Exception as e:
        logger.error(f"⚠️ Cleanup failed: {e}")


async def main():
    """Run all tests."""
    logger.info("\n" + "="*80)
    logger.info("POSTGRES_UTILS.PY TEST SUITE")
    logger.info("="*80)
    
    # Check environment variables
    required_vars = ['SUPABASE_DB_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"❌ Missing environment variables: {missing_vars}")
        logger.error("Please set: export SUPABASE_DB_PASSWORD='your_password'")
        return
    
    results = []
    
    # Test 1: Connection Pool
    results.append(("Connection Pool", await test_connection_pool()))
    
    # Test 2: Health Check
    results.append(("Health Check", await test_database_health()))
    
    # Test 3: Batch Insert
    results.append(("Batch Insert", await test_batch_insert()))
    
    # Cleanup
    await cleanup_test_data()
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("TEST SUMMARY")
    logger.info("="*80)
    
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"{test_name:.<50} {status}")
    
    total_passed = sum(1 for _, passed in results if passed)
    logger.info(f"\nTotal: {total_passed}/{len(results)} tests passed")
    logger.info("="*80 + "\n")
    
    if total_passed == len(results):
        logger.info("🎉 ALL TESTS PASSED! Ready to proceed to Step 2.")
        return 0
    else:
        logger.error("❌ SOME TESTS FAILED. Please fix issues before proceeding.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)