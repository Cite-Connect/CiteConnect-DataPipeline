"""
PostgreSQL utility functions for CiteConnect.
Handles connection pooling, batch inserts, and async database operations.
"""

import logging
import asyncpg
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
import time

logger = logging.getLogger(__name__)


class PostgreSQLConnectionPool:
    """Manages PostgreSQL connection pool with retry logic."""
    
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_size: int = 5,
        max_size: int = 20
    ):
        """
        Initialize connection pool configuration.
        
        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            user: Username
            password: Password
            min_size: Minimum pool connections
            max_size: Maximum pool connections
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.min_size = min_size
        self.max_size = max_size
        self._pool = None
        
        logger.info(
            f"PostgreSQL connection pool configured: "
            f"{user}@{host}:{port}/{database} "
            f"(pool size: {min_size}-{max_size})"
        )
    
    async def create_pool(self):
        """Create connection pool with retry logic."""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Creating connection pool (attempt {attempt + 1}/{max_retries})...")
                
                self._pool = await asyncpg.create_pool(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    min_size=self.min_size,
                    max_size=self.max_size,
                    command_timeout=60,  # 60 second timeout for queries
                    server_settings={
                        'application_name': 'citeconnect_embedding_service'
                    }
                )
                
                # Test connection
                async with self._pool.acquire() as conn:
                    await conn.fetchval('SELECT 1')
                
                logger.info("✅ Connection pool created successfully")
                return self._pool
                
            except Exception as e:
                logger.error(f"Connection pool creation failed (attempt {attempt + 1}): {e}")
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error("❌ Failed to create connection pool after all retries")
                    raise
    
    async def close_pool(self):
        """Close connection pool gracefully."""
        if self._pool:
            await self._pool.close()
            logger.info("Connection pool closed")
    
    @asynccontextmanager
    async def acquire(self):
        """Context manager for acquiring connection from pool."""
        if not self._pool:
            await self.create_pool()
        
        async with self._pool.acquire() as conn:
            yield conn


async def batch_insert_papers(
    pool: PostgreSQLConnectionPool,
    papers: List[Dict[str, Any]],
    batch_size: int = 100
) -> int:
    """
    Batch insert papers into PostgreSQL with ON CONFLICT handling.
    
    Args:
        pool: Connection pool
        papers: List of paper dictionaries
        batch_size: Papers per batch
    
    Returns:
        Number of papers inserted/updated
    """
    if not papers:
        return 0
    
    inserted_count = 0
    
    for i in range(0, len(papers), batch_size):
        batch = papers[i:i + batch_size]
        
        try:
            async with pool.acquire() as conn:
                # Prepare batch insert with ON CONFLICT DO UPDATE
                query = """
                INSERT INTO papers (
                    paper_id, title, abstract, introduction, summary,
                    authors, year, venue, citation_count,
                    domain, gcs_pdf_path,
                    extraction_method, content_quality, has_introduction, intro_length,
                    ingested_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $16)
                ON CONFLICT (paper_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    abstract = EXCLUDED.abstract,
                    introduction = EXCLUDED.introduction,
                    summary = EXCLUDED.summary,
                    authors = EXCLUDED.authors,
                    year = EXCLUDED.year,
                    venue = EXCLUDED.venue,
                    citation_count = EXCLUDED.citation_count,
                    domain = EXCLUDED.domain,
                    extraction_method = EXCLUDED.extraction_method,
                    content_quality = EXCLUDED.content_quality,
                    has_introduction = EXCLUDED.has_introduction,
                    intro_length = EXCLUDED.intro_length,
                    updated_at = EXCLUDED.updated_at
                """
                
                # Execute batch
                await conn.executemany(query, [
                    (
                        paper['paper_id'],
                        paper['title'],
                        paper['abstract'],
                        paper['introduction'],
                        paper['summary'],
                        paper['authors'],
                        paper['year'],
                        paper['venue'],
                        paper['citation_count'],
                        paper['domain'],
                        paper['gcs_pdf_path'],
                        paper['extraction_method'],
                        paper['content_quality'],
                        paper['has_introduction'],
                        paper['intro_length'],
                        paper['ingested_at']
                    )
                    for paper in batch
                ])
                
                inserted_count += len(batch)
                logger.info(f"Inserted batch of {len(batch)} papers (total: {inserted_count}/{len(papers)})")
        
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            raise
    
    return inserted_count


async def batch_insert_embeddings(
    pool: PostgreSQLConnectionPool,
    table_name: str,
    embeddings: List[Dict[str, Any]],
    batch_size: int = 100
) -> int:
    """
    Batch insert embeddings into mini-LM or SPECTER table.
    
    Args:
        pool: Connection pool
        table_name: 'paper_embeddings_minilm' or 'paper_embeddings_specter'
        embeddings: List of embedding dictionaries
        batch_size: Embeddings per batch
    
    Returns:
        Number of embeddings inserted/updated
    """
    if not embeddings:
        return 0
    
    inserted_count = 0
    
    for i in range(0, len(embeddings), batch_size):
        batch = embeddings[i:i + batch_size]
        
        try:
            async with pool.acquire() as conn:
                query = f"""
                INSERT INTO {table_name} (
                    paper_id, embedding, model_name, model_version,
                    embedding_source, source_hash,
                    text_length, has_introduction,
                    created_at, updated_at
                ) VALUES ($1, $2::vector, $3, $4, $5, $6, $7, $8, $9, $9)
                ON CONFLICT (paper_id) DO UPDATE SET
                    embedding = EXCLUDED.embedding,
                    embedding_source = EXCLUDED.embedding_source,
                    source_hash = EXCLUDED.source_hash,
                    text_length = EXCLUDED.text_length,
                    has_introduction = EXCLUDED.has_introduction,
                    updated_at = EXCLUDED.updated_at
                """
                
                # Convert embedding lists to PostgreSQL vector format strings
                batch_data = []
                for emb in batch:
                    # Convert list to PostgreSQL array string format
                    embedding_list = emb['embedding']
                    if isinstance(embedding_list, list):
                        # Format: '[0.1, 0.2, 0.3, ...]'
                        embedding_str = '[' + ','.join(str(x) for x in embedding_list) + ']'
                    else:
                        # Already a string
                        embedding_str = str(embedding_list)
                    
                    batch_data.append((
                        emb['paper_id'],
                        embedding_str,  # ✅ String format for vector
                        emb['model_name'],
                        emb['model_version'],
                        emb['embedding_source'],
                        emb['source_hash'],
                        emb['text_length'],
                        emb['has_introduction'],
                        emb['created_at']
                    ))
                
                await conn.executemany(query, batch_data)
                
                inserted_count += len(batch)
                logger.info(f"Inserted batch of {len(batch)} embeddings into {table_name} (total: {inserted_count}/{len(embeddings)})")
        
        except Exception as e:
            logger.error(f"Batch embedding insert failed: {e}")
            raise
    
    return inserted_count


async def check_database_health(pool: PostgreSQLConnectionPool) -> bool:
    """
    Check database connectivity and table existence.
    
    Returns:
        True if healthy, False otherwise
    """
    try:
        async with pool.acquire() as conn:
            # Check connection
            result = await conn.fetchval('SELECT 1')
            if result != 1:
                return False
            
            # Check tables exist
            tables = await conn.fetch("""
                SELECT table_name FROM information_schema.tables
                WHERE table_name IN ('papers', 'paper_embeddings_minilm', 'paper_embeddings_specter')
            """)
            
            if len(tables) != 3:
                logger.error(f"Missing tables. Found: {[t['table_name'] for t in tables]}")
                return False
            
            logger.info("✅ Database health check passed")
            return True
    
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False