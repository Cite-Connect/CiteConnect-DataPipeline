"""
Upload papers from GCS to Supabase (PostgreSQL) - Papers Only, No Embeddings
Designed for Airflow DAG task integration
"""

import asyncio
import asyncpg
import pandas as pd
import logging
import os
import json
from google.cloud import storage
from typing import Dict, List, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SupabasePaperUploader:
    """Upload papers from GCS to Supabase papers table (no embeddings)"""
    
    def __init__(
        self,
        gcs_bucket: str,
        gcs_prefix: str = 'processed_v2/',
        db_config: Dict = None
    ):
        """
        Initialize uploader
        
        Args:
            gcs_bucket: GCS bucket name
            gcs_prefix: Folder path in GCS (default: processed_v2/)
            db_config: Database configuration dictionary
        """
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix
        self.db_config = db_config or self._get_db_config_from_env()
        
        logger.info(f"📦 GCS Bucket: {self.gcs_bucket}")
        logger.info(f"📁 GCS Prefix: {self.gcs_prefix}")
        logger.info(f"🗄️  Database: {self.db_config['host']}")
    
    def _get_db_config_from_env(self) -> Dict:
        """Load database config from environment variables"""
        
        # Option 1: Use DATABASE_URL if available (standard format)
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            # Parse DATABASE_URL: postgresql://user:password@host:port/database
            # Supports both standard and pooler formats
            from urllib.parse import urlparse
            
            try:
                parsed = urlparse(database_url)
                if parsed.scheme in ['postgresql', 'postgres']:
                    return {
                        'host': parsed.hostname,
                        'port': parsed.port or 5432,
                        'database': parsed.path.lstrip('/').split('?')[0],  # Remove query params
                        'user': parsed.username,
                        'password': parsed.password,
                    }
            except Exception as e:
                logger.warning(f"⚠️ Could not parse DATABASE_URL: {e}")
                logger.info("Falling back to individual environment variables")
        
        # Option 2: Fall back to individual variables
        return {
            'host': os.getenv('SUPABASE_DB_HOST'),
            'port': int(os.getenv('SUPABASE_DB_PORT', 5432)),
            'database': os.getenv('SUPABASE_DB_NAME', 'postgres'),
            'user': os.getenv('SUPABASE_DB_USER', 'postgres'),
            'password': os.getenv('SUPABASE_DB_PASSWORD'),
        }
    
    async def get_existing_paper_ids(self, conn) -> set:
        """Get set of paper IDs already in database"""
        try:
            rows = await conn.fetch("SELECT paper_id FROM papers")
            existing_ids = {row['paper_id'] for row in rows}
            logger.info(f"📊 Found {len(existing_ids)} existing papers in database")
            return existing_ids
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch existing paper IDs: {e}")
            return set()
    
    def load_papers_from_gcs(self) -> List[pd.DataFrame]:
        """Load all parquet files from GCS"""
        try:
            client = storage.Client()
            bucket = client.bucket(self.gcs_bucket)
            
            # List all parquet files in the prefix
            blobs = list(bucket.list_blobs(prefix=self.gcs_prefix))
            parquet_blobs = [b for b in blobs if b.name.endswith('.parquet')]
            
            logger.info(f"📦 Found {len(parquet_blobs)} parquet files in gs://{self.gcs_bucket}/{self.gcs_prefix}")
            
            dataframes = []
            for blob in parquet_blobs:
                try:
                    # Download to memory
                    content = blob.download_as_bytes()
                    
                    # Read parquet
                    import io
                    df = pd.read_parquet(io.BytesIO(content))
                    
                    logger.info(f"  ✅ Loaded {len(df)} papers from {blob.name}")
                    dataframes.append(df)
                    
                except Exception as e:
                    logger.error(f"  ❌ Failed to load {blob.name}: {e}")
                    continue
            
            return dataframes
            
        except Exception as e:
            logger.error(f"❌ Failed to load from GCS: {e}")
            raise
    
    def prepare_paper_for_insert(self, paper_row: pd.Series) -> Dict:
        """
        Convert DataFrame row to database-ready dictionary
        
        Maps paper fields to Supabase papers table schema
        """
        # Safe get function that handles pandas arrays
        def safe_get(key, default=None):
            """Safely get value from pandas Series"""
            try:
                val = paper_row.get(key, default)
                # Handle pandas NA/None
                if val is None:
                    return default
                if pd.isna(val) if not isinstance(val, (list, dict)) else False:
                    return default
                return val
            except (ValueError, TypeError):
                return default

        # Clean text to remove null bytes and other problematic characters
        def clean_text(text):
            """Clean text by removing null bytes and other problematic characters"""
            if text is None:
                return None
            if isinstance(text, str):
                # Remove null bytes and other control characters (except newlines/tabs)
                cleaned = text.replace('\x00', '')  # Remove null bytes
                # Remove other problematic control characters but keep newlines and tabs
                cleaned = ''.join(c for c in cleaned if ord(c) >= 32 or c in '\n\t')
                return cleaned if cleaned else None
            return text
        
        # Convert arrays/lists to PostgreSQL ARRAY format (not JSON!)
        def to_array(val):
            """Convert to PostgreSQL array (keep as Python list)"""
            if val is None:
                return None
            if isinstance(val, str):
                try:
                    # Parse JSON string to list
                    parsed = json.loads(val)
                    if isinstance(parsed, list):
                        return parsed
                except:
                    return None
            if isinstance(val, list):
                return val
            return None
        
        # Convert timestamp string to datetime (timezone-naive for PostgreSQL TIMESTAMP column)
        def to_datetime(val):
            """Convert ISO timestamp string to timezone-naive datetime object"""
            if val is None:
                return None
            if isinstance(val, str):
                try:
                    # Parse ISO format: '2025-11-29T20:10:53.173227Z'
                    # Remove timezone to make it naive (PostgreSQL TIMESTAMP requires naive)
                    dt = datetime.fromisoformat(val.replace('Z', '+00:00'))
                    return dt.replace(tzinfo=None)  # Make timezone-naive
                except:
                    try:
                        # Fallback: parse without timezone
                        return datetime.fromisoformat(val.rstrip('Z'))
                    except:
                        return None
            if isinstance(val, datetime):
                # Ensure it's timezone-naive
                return val.replace(tzinfo=None) if val.tzinfo else val
            return None
        
        # Map DataFrame columns to Supabase schema
        # Match the exact column names from your schema
        paper_data = {
            'paper_id': str(safe_get('paperId', '')),
            'title': clean_text(str(safe_get('title', ''))[:1000]),  # Clean and truncate
            'authors': to_array(safe_get('authors')),  # ARRAY type
            'year': int(safe_get('year')) if pd.notna(safe_get('year')) else None,
            'venue': clean_text(safe_get('venue')),  # Clean text field
            'citation_count': int(safe_get('citationCount', 0)) if pd.notna(safe_get('citationCount')) else 0,
            'abstract': clean_text(safe_get('abstract')),  # Clean text field
            'introduction': clean_text(safe_get('introduction')),  # Clean text field
            'tldr': clean_text(safe_get('tldr')),  # Clean text field
            'domain': clean_text(safe_get('domain', 'general')),  # Clean text field
            'domain_confidence': None,  # We don't have this yet
            'sub_domains': to_array(safe_get('sub_domains')),  # ARRAY type
            'reference_ids': to_array(safe_get('references_id')),  # ARRAY type (note: references_id → reference_ids)
            'citation_ids': None,  # We don't have this
            'reference_count': int(safe_get('referenceCount', 0)) if pd.notna(safe_get('referenceCount')) else 0,
            'quality_score': None,  # We don't have this yet
            'extraction_method': clean_text(safe_get('extraction_method')),  # Clean text field
            'content_quality': clean_text(safe_get('content_quality')),  # Clean text field
            'has_introduction': bool(safe_get('has_intro', False)),  # has_intro → has_introduction
            'intro_length': int(safe_get('intro_length', 0)) if pd.notna(safe_get('intro_length')) else 0,
            'gcs_pdf_path': clean_text(safe_get('pdf_url')),  # Clean text field
            'ingested_at': to_datetime(safe_get('scraped_at')),  # Convert string to datetime
        }
        
        # Remove None keys to use database defaults
        paper_data = {k: v for k, v in paper_data.items() if v is not None}
        
        return paper_data
    
    async def upload_papers_batch(
        self,
        conn,
        papers: List[Dict],
        existing_ids: set
    ) -> Dict:
        """
        Upload a batch of papers to database
        
        Returns:
            Statistics dictionary
        """
        stats = {
            'attempted': len(papers),
            'inserted': 0,
            'skipped': 0,
            'failed': 0
        }
        
        for paper in papers:
            paper_id = paper.get('paper_id')
            
            # Skip if already exists
            if paper_id in existing_ids:
                stats['skipped'] += 1
                continue
            
            try:
                # Build INSERT query
                columns = list(paper.keys())
                placeholders = [f'${i+1}' for i in range(len(columns))]
                values = [paper[col] for col in columns]
                
                query = f"""
                    INSERT INTO papers ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                    ON CONFLICT (paper_id) DO NOTHING
                """
                
                await conn.execute(query, *values)
                stats['inserted'] += 1
                existing_ids.add(paper_id)  # Update set
                
            except Exception as e:
                logger.error(f"❌ Failed to insert paper {paper_id}: {e}")
                stats['failed'] += 1
                continue
        
        return stats
    
    async def upload_all_papers(
        self,
        batch_size: int = 100,
        max_papers: Optional[int] = None
    ) -> Dict:
        """
        Main function: Load papers from GCS and upload to Supabase
        
        Args:
            batch_size: Papers per batch
            max_papers: Limit total papers (None = all)
            
        Returns:
            Statistics dictionary
        """
        logger.info("="*80)
        logger.info("🚀 STARTING PAPER UPLOAD TO SUPABASE")
        logger.info("="*80)
        
        # Load papers from GCS
        logger.info("\n📥 Loading papers from GCS...")
        dataframes = self.load_papers_from_gcs()
        
        if not dataframes:
            logger.warning("⚠️ No papers found in GCS")
            return {'status': 'no_data', 'total_papers': 0}
        
        # Combine all DataFrames
        df_all = pd.concat(dataframes, ignore_index=True)
        
        # Deduplicate by paperId
        original_count = len(df_all)
        df_all = df_all.drop_duplicates(subset='paperId', keep='first')
        dedupe_count = original_count - len(df_all)
        
        if dedupe_count > 0:
            logger.info(f"🔄 Removed {dedupe_count} duplicate papers")
        
        logger.info(f"📊 Total unique papers in GCS: {len(df_all)}")
        
        # Apply max_papers limit if specified
        if max_papers and len(df_all) > max_papers:
            df_all = df_all.head(max_papers)
            logger.info(f"⚠️ Limited to first {max_papers} papers")
        
        # Connect to database
        logger.info("\n🔌 Connecting to Supabase...")
        conn = None
        # Disable prepared statements for connection pooler (pgbouncer) compatibility
        conn = await asyncpg.connect(**self.db_config, statement_cache_size=0)
        
        try:
            # Get existing papers
            existing_ids = await self.get_existing_paper_ids(conn)
            
            # Prepare papers for upload
            logger.info("\n🔄 Preparing papers for upload...")
            papers_to_upload = []
            
            for _, row in df_all.iterrows():
                try:
                    paper_data = self.prepare_paper_for_insert(row)
                    papers_to_upload.append(paper_data)
                except Exception as e:
                    logger.error(f"❌ Failed to prepare paper: {e}")
                    continue
            
            logger.info(f"✅ Prepared {len(papers_to_upload)} papers")
            
            # Upload in batches
            logger.info(f"\n📤 Uploading in batches of {batch_size}...")
            
            total_stats = {
                'attempted': 0,
                'inserted': 0,
                'skipped': 0,
                'failed': 0
            }
            
            for i in range(0, len(papers_to_upload), batch_size):
                batch = papers_to_upload[i:i+batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(papers_to_upload) + batch_size - 1) // batch_size
                
                logger.info(f"  Batch {batch_num}/{total_batches} ({len(batch)} papers)...")
                
                batch_stats = await self.upload_papers_batch(conn, batch, existing_ids)
                
                # Aggregate stats
                for key in total_stats:
                    total_stats[key] += batch_stats[key]
                
                logger.info(f"    ✅ Inserted: {batch_stats['inserted']}, "
                          f"Skipped: {batch_stats['skipped']}, "
                          f"Failed: {batch_stats['failed']}")
            
            # Final results
            logger.info("\n" + "="*80)
            logger.info("✅ UPLOAD COMPLETE")
            logger.info("="*80)
            logger.info(f"📊 Papers in GCS: {len(df_all)}")
            logger.info(f"✅ Papers inserted: {total_stats['inserted']}")
            logger.info(f"⏭️  Papers skipped (already exist): {total_stats['skipped']}")
            logger.info(f"❌ Papers failed: {total_stats['failed']}")
            logger.info("="*80)
            
            return {
                'status': 'success',
                'gcs_papers': len(df_all),
                'inserted': total_stats['inserted'],
                'skipped': total_stats['skipped'],
                'failed': total_stats['failed'],
                'timestamp': datetime.now().isoformat()
            }
            
        finally:
            try:
                if conn:
                    await conn.close()
                    logger.info("🔌 Database connection closed")
            except Exception as e:
                logger.warning(f"⚠️ Error closing connection: {e}")


# ========================================
# DAG TASK FUNCTION
# ========================================

async def upload_papers_to_supabase_async(
    max_papers: Optional[int] = None,
    batch_size: int = 100
) -> Dict:
    """
    Async function for DAG task
    
    Args:
        max_papers: Limit papers to upload (None = all)
        batch_size: Papers per batch
        
    Returns:
        Upload statistics
    """
    uploader = SupabasePaperUploader(
        gcs_bucket=os.getenv('GCS_BUCKET_NAME', 'citeconnect-test-bucket'),
        gcs_prefix='processed_v2/'
    )
    
    return await uploader.upload_all_papers(
        batch_size=batch_size,
        max_papers=max_papers
    )


def upload_papers_to_supabase(**context):
    """
    Synchronous wrapper for Airflow DAG task

    Usage in DAG:
        upload_task = PythonOperator(
            task_id='upload_to_supabase',
            python_callable=upload_papers_to_supabase,
            dag=dag
        )
    """
    # Get config from DAG params if available
    dag_run = context.get('dag_run')
    max_papers = None
    batch_size = 100

    if dag_run and dag_run.conf:
        max_papers = dag_run.conf.get('MAX_PAPERS_UPLOAD')
        batch_size = dag_run.conf.get('UPLOAD_BATCH_SIZE', 100)

    try:
        # This creates a fresh event loop, runs your async function, then closes it
        result = asyncio.run(
            upload_papers_to_supabase_async(
                max_papers=max_papers,
                batch_size=batch_size,
            )
        )

        logger.info("✅ Upload task completed successfully")
        return result

    except Exception as e:
        logger.error(f"❌ Upload task failed: {e}")
        import traceback
        traceback.print_exc()
        raise


# ========================================
# STANDALONE EXECUTION
# ========================================

async def main():
    """Standalone execution for testing"""
    import argparse
    from dotenv import load_dotenv
    
    load_dotenv()
    
    parser = argparse.ArgumentParser(description='Upload papers from GCS to Supabase')
    parser.add_argument('--max-papers', type=int, default=None, help='Max papers to upload')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size')
    
    args = parser.parse_args()
    
    try:
        result = await upload_papers_to_supabase_async(
            max_papers=args.max_papers,
            batch_size=args.batch_size
        )
        
        print(f"\n✅ SUCCESS: {result}")
        return 0
        
    except Exception as e:
        print(f"\n❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)

