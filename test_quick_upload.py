"""
Quick test - uploads 10 papers from a single GCS file
Much faster than loading all 48 files
"""

import asyncio
import asyncpg
import pandas as pd
import os
import json
from dotenv import load_dotenv
from urllib.parse import urlparse
from google.cloud import storage
import io

load_dotenv()

async def quick_test():
    import sys
    
    print("="*80, flush=True)
    print("🚀 QUICK SUPABASE UPLOAD TEST (1 FILE, 10 PAPERS)", flush=True)
    print("="*80, flush=True)
    
    # Parse DATABASE_URL
    database_url = os.getenv('DATABASE_URL')
    print(f"\n📋 DATABASE_URL: {database_url[:50] if database_url else 'NOT SET'}...", flush=True)
    
    parsed = urlparse(database_url)
    db_config = {
        'host': parsed.hostname,
        'port': parsed.port or 5432,
        'database': parsed.path.lstrip('/').split('?')[0],
        'user': parsed.username,
        'password': parsed.password,
    }
    
    print(f"\n🔌 Connecting to Supabase...", flush=True)
    print(f"   Host: {db_config['host']}", flush=True)
    print(f"   Port: {db_config['port']}", flush=True)
    print(f"   Database: {db_config['database']}", flush=True)
    print(f"   User: {db_config['user']}", flush=True)
    
    sys.stdout.flush()
    conn = await asyncpg.connect(**db_config, statement_cache_size=0)
    print(f"✅ Connected!", flush=True)
    
    try:
        # Check existing papers
        print(f"   Fetching existing paper count...", flush=True)
        existing_count = await conn.fetchval("SELECT COUNT(*) FROM papers")
        print(f"✅ Current papers in database: {existing_count}", flush=True)
        
        # Load just ONE parquet file from GCS
        print(f"\n📥 Loading ONE parquet file from GCS...", flush=True)
        print(f"   Initializing GCS client...", flush=True)
        client = storage.Client()
        bucket = client.bucket(os.getenv('GCS_BUCKET_NAME'))
        
        # Get first parquet file
        print(f"   Listing parquet files in processed_v2/...", flush=True)
        blobs = list(bucket.list_blobs(prefix='processed_v2/'))
        parquet_blobs = [b for b in blobs if b.name.endswith('.parquet')]
        print(f"   Found {len(parquet_blobs)} parquet files", flush=True)
        
        if not parquet_blobs:
            print("❌ No parquet files found in GCS")
            return 1
        
        # Load first file only
        first_blob = parquet_blobs[0]
        print(f"   Loading: {first_blob.name}")
        
        content = first_blob.download_as_bytes()
        df = pd.read_parquet(io.BytesIO(content))
        
        print(f"✅ Loaded {len(df)} papers from file")
        
        # Take first 10 papers
        df_test = df.head(10)
        print(f"📊 Using first 10 papers for test")
        
        # Get existing paper IDs
        rows = await conn.fetch("SELECT paper_id FROM papers")
        existing_ids = {row['paper_id'] for row in rows}
        
        # Prepare and upload papers
        print(f"\n📤 Uploading 10 papers...")
        
        inserted = 0
        skipped = 0
        failed = 0
        
        for _, row in df_test.iterrows():
            paper_id = str(row.get('paperId', ''))
            
            # Skip if exists
            if paper_id in existing_ids:
                skipped += 1
                print(f"   ⏭️  Skipped: {paper_id[:20]}... (already exists)")
                continue
            
            try:
                # Prepare data matching YOUR schema
                def to_array(val):
                    if val is None or pd.isna(val):
                        return None
                    if isinstance(val, str):
                        try:
                            parsed = json.loads(val)
                            return parsed if isinstance(parsed, list) else None
                        except:
                            return None
                    return val if isinstance(val, list) else None
                
                def safe_get(key, default=None):
                    try:
                        val = row.get(key, default)
                        if pd.isna(val) if not isinstance(val, (list, dict)) else False:
                            return default
                        return val
                    except:
                        return default

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
                
                def to_datetime(val):
                    """Convert ISO timestamp string to timezone-naive datetime object"""
                    from datetime import datetime
                    if val is None or pd.isna(val):
                        return None
                    if isinstance(val, str):
                        try:
                            # Parse and remove timezone (PostgreSQL TIMESTAMP requires naive)
                            dt = datetime.fromisoformat(val.replace('Z', '+00:00'))
                            return dt.replace(tzinfo=None)  # Make timezone-naive
                        except:
                            try:
                                return datetime.fromisoformat(val.rstrip('Z'))
                            except:
                                return None
                    if isinstance(val, datetime):
                        return val.replace(tzinfo=None) if val.tzinfo else val
                    return None
                
                paper_data = {
                    'paper_id': paper_id,
                    'title': clean_text(str(safe_get('title', ''))[:1000]),  # Clean text
                    'authors': to_array(safe_get('authors')),
                    'year': int(safe_get('year')) if pd.notna(safe_get('year')) else None,
                    'citation_count': int(safe_get('citationCount', 0)),
                    'abstract': clean_text(safe_get('abstract')),  # Clean text
                    'introduction': clean_text(safe_get('introduction')),  # Clean text
                    'tldr': clean_text(safe_get('tldr')),  # Clean text
                    'domain': clean_text(safe_get('domain', 'general')),  # Clean text
                    'sub_domains': to_array(safe_get('sub_domains')),
                    'reference_ids': to_array(safe_get('references_id')),
                    'reference_count': int(safe_get('referenceCount', 0)),
                    'extraction_method': clean_text(safe_get('extraction_method')),  # Clean text
                    'content_quality': clean_text(safe_get('content_quality')),  # Clean text
                    'has_introduction': bool(safe_get('has_intro', False)),
                    'intro_length': int(safe_get('intro_length', 0)),
                    'gcs_pdf_path': clean_text(safe_get('pdf_url')),  # Clean text
                    'ingested_at': to_datetime(safe_get('scraped_at')),  # Convert string to datetime
                }
                
                # Remove None values
                paper_data = {k: v for k, v in paper_data.items() if v is not None}
                
                # Insert
                columns = list(paper_data.keys())
                placeholders = [f'${i+1}' for i in range(len(columns))]
                values = [paper_data[col] for col in columns]
                
                query = f"""
                    INSERT INTO papers ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                    ON CONFLICT (paper_id) DO NOTHING
                """
                
                await conn.execute(query, *values)
                inserted += 1
                existing_ids.add(paper_id)
                print(f"   ✅ Inserted: {paper_id[:20]}... ({row.get('title', '')[:40]}...)")
                
            except Exception as e:
                failed += 1
                print(f"   ❌ Failed: {paper_id[:20]}... - {e}")
        
        # Results
        print(f"\n📊 Test Results:")
        print(f"   Inserted: {inserted}")
        print(f"   Skipped: {skipped}")
        print(f"   Failed: {failed}")
        
        # Verify count increased
        new_count = await conn.fetchval("SELECT COUNT(*) FROM papers")
        print(f"\n📈 Database count:")
        print(f"   Before: {existing_count}")
        print(f"   After: {new_count}")
        print(f"   Increase: +{new_count - existing_count}")
        
        if inserted > 0:
            print(f"\n✅ SUCCESS! Uploaded {inserted} papers to Supabase")
        elif skipped > 0:
            print(f"\n✅ SUCCESS! All papers already exist (deduplication working)")
        
        return 0
        
    finally:
        await conn.close()
        print(f"\n🔌 Connection closed")

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(quick_test())
        exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        exit(1)

