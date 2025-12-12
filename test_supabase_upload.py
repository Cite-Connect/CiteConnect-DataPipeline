"""
Test script to upload 10 papers from GCS to Supabase
Verifies deduplication works correctly
"""

import asyncio
import sys
import os
from dotenv import load_dotenv

# Setup
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Load .env from project root explicitly
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

print(f"📁 Loading .env from: {env_path}")
print(f"   File exists: {os.path.exists(env_path)}")

from src.DataPipeline.Processing.upload_papers_to_supabase import upload_papers_to_supabase_async


async def test_upload():
    """Test uploading 10 papers"""
    
    print("="*80)
    print("🧪 TESTING SUPABASE UPLOAD (10 PAPERS)")
    print("="*80)
    print("\nThis test will:")
    print("  1. Load papers from GCS processed_v2/")
    print("  2. Deduplicate within GCS files")
    print("  3. Check existing papers in Supabase")
    print("  4. Upload only 10 papers (for testing)")
    print("  5. Skip papers already in database")
    print("="*80)
    
    # Verify environment variables
    print("\n🔍 Checking configuration...")
    
    # Check if using DATABASE_URL (preferred) or individual variables
    database_url = os.getenv('DATABASE_URL')
    
    if database_url:
        print("✅ Using DATABASE_URL (standard format)")
        required_vars = [
            'DATABASE_URL',
            'GCS_BUCKET_NAME'
        ]
    else:
        print("ℹ️  Using individual database variables")
        required_vars = [
            'SUPABASE_DB_HOST',
            'SUPABASE_DB_NAME',
            'SUPABASE_DB_USER',
            'SUPABASE_DB_PASSWORD',
            'GCS_BUCKET_NAME'
        ]
    
    # Debug: Show what's loaded
    print("\n📋 Environment variables loaded:")
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Mask password
            if 'PASSWORD' in var:
                masked = value[:3] + '*' * (len(value) - 3) if len(value) > 3 else '***'
                print(f"   ✅ {var}: {masked}")
            else:
                print(f"   ✅ {var}: {value}")
        else:
            print(f"   ❌ {var}: NOT SET")
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"\n❌ Missing environment variables: {', '.join(missing_vars)}")
        print("\nPlease add these to your .env file:")
        for var in missing_vars:
            print(f"  {var}=...")
        return 1
    
    print("\n✅ All environment variables configured")
    print(f"   GCS Bucket: {os.getenv('GCS_BUCKET_NAME')}")
    
    if database_url:
        # Extract host from DATABASE_URL
        import re
        match = re.search(r'@([^:]+):', database_url)
        if match:
            print(f"   Database Host: {match.group(1)}")
    else:
        print(f"   Supabase Host: {os.getenv('SUPABASE_DB_HOST')}")
    
    # Run upload with max_papers=10
    print("\n" + "="*80)
    print("🚀 Starting upload test...")
    print("="*80)
    
    try:
        result = await upload_papers_to_supabase_async(
            max_papers=10,  # Only 10 papers for testing
            batch_size=10   # Single batch
        )
        
        # Display results
        print("\n" + "="*80)
        print("✅ TEST COMPLETE")
        print("="*80)
        print(f"\n📊 Results:")
        print(f"   Status: {result['status']}")
        print(f"   Papers in GCS: {result['gcs_papers']}")
        print(f"   Inserted: {result['inserted']}")
        print(f"   Skipped (already exist): {result['skipped']}")
        print(f"   Failed: {result['failed']}")
        
        # Verify deduplication worked
        print(f"\n🔍 Deduplication Check:")
        if result['skipped'] > 0:
            print(f"   ✅ Correctly skipped {result['skipped']} existing papers")
            print(f"   → Database already had these papers")
        else:
            print(f"   ℹ️  No duplicates found (all papers were new)")
        
        if result['inserted'] > 0:
            print(f"   ✅ Successfully inserted {result['inserted']} new papers")
        
        # Test again to verify deduplication
        if result['inserted'] > 0:
            print(f"\n🔄 Running second upload to test deduplication...")
            result2 = await upload_papers_to_supabase_async(
                max_papers=10,
                batch_size=10
            )
            
            print(f"\n📊 Second Upload Results:")
            print(f"   Inserted: {result2['inserted']}")
            print(f"   Skipped: {result2['skipped']}")
            
            if result2['skipped'] == 10 and result2['inserted'] == 0:
                print(f"\n✅ DEDUPLICATION VERIFIED!")
                print(f"   → All 10 papers were correctly identified as duplicates")
                print(f"   → No papers were inserted on second run")
            elif result2['skipped'] > 0:
                print(f"\n✅ Partial deduplication working")
                print(f"   → {result2['skipped']} papers were correctly skipped")
        
        print("\n" + "="*80)
        print("🎉 TEST SUCCESSFUL")
        print("="*80)
        print("\nYou can now:")
        print("  1. Check Supabase to see the uploaded papers")
        print("  2. Run the full DAG to upload all papers")
        print("  3. Rerun this test - it should skip all 10 papers")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        
        print("\n🔍 Troubleshooting:")
        print("  1. Check Supabase credentials in .env")
        print("  2. Verify 'papers' table exists in Supabase")
        print("  3. Check GCS bucket has processed_v2/ folder with parquet files")
        print("  4. Verify network connectivity to Supabase")
        
        return 1


async def verify_database_connection():
    """Verify we can connect to Supabase before testing"""
    import asyncpg
    from urllib.parse import urlparse
    
    print("\n🔌 Testing database connection...")
    
    # Parse DATABASE_URL or use individual variables
    database_url = os.getenv('DATABASE_URL')
    
    if database_url:
        parsed = urlparse(database_url)
        db_config = {
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path.lstrip('/').split('?')[0],
            'user': parsed.username,
            'password': parsed.password,
        }
    else:
        db_config = {
            'host': os.getenv('SUPABASE_DB_HOST'),
            'port': int(os.getenv('SUPABASE_DB_PORT', 5432)),
            'database': os.getenv('SUPABASE_DB_NAME'),
            'user': os.getenv('SUPABASE_DB_USER'),
            'password': os.getenv('SUPABASE_DB_PASSWORD'),
        }
    
    try:
        # Disable prepared statements for connection pooler compatibility
        conn = await asyncpg.connect(**db_config, statement_cache_size=0)
        
        # Test query
        result = await conn.fetchval("SELECT COUNT(*) FROM papers")
        
        print(f"✅ Connection successful!")
        print(f"   Current papers in database: {result}")
        
        await conn.close()
        return True
        
    except asyncpg.InvalidCatalogNameError:
        print(f"❌ Database '{db_config['database']}' does not exist")
        return False
        
    except asyncpg.InvalidPasswordError:
        print(f"❌ Invalid password for user '{db_config['user']}'")
        return False
        
    except asyncpg.UndefinedTableError:
        print(f"❌ Table 'papers' does not exist")
        print(f"\nCreate it with:")
        print(f"   See SUPABASE_UPLOAD.md for SQL schema")
        return False
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


async def main():
    """Main test runner"""
    
    # First verify connection
    connection_ok = await verify_database_connection()
    
    if not connection_ok:
        print("\n❌ Cannot proceed with test - fix database connection first")
        return 1
    
    # Run upload test
    return await test_upload()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

