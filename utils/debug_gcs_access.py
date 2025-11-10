"""
Debug GCS Access
Tests GCS authentication, permissions, and bucket access.
"""

import logging
from google.cloud import storage
from google.auth import default

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s"
)

def diagnose_gcs_access():
    """Run comprehensive GCS diagnostics."""
    
    print("\n" + "="*60)
    print("GCS ACCESS DIAGNOSTICS")
    print("="*60 + "\n")
    
    # 1. Check authentication
    print("1️⃣ Checking authentication...")
    try:
        credentials, project_id = default()
        print(f"   ✅ Authenticated")
        print(f"   📧 Account: {credentials.service_account_email if hasattr(credentials, 'service_account_email') else 'User account'}")
        print(f"   🏗️  Project ID: {project_id}")
    except Exception as e:
        print(f"   ❌ Authentication failed: {e}")
        return
    
    # 2. Test bucket access
    print("\n2️⃣ Testing bucket access...")
    bucket_name = "citeconnect-test-bucket"
    
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # Try to get bucket metadata
        bucket.reload()
        print(f"   ✅ Bucket exists: {bucket_name}")
        print(f"   📍 Location: {bucket.location}")
        print(f"   🏷️  Storage class: {bucket.storage_class}")
        
    except Exception as e:
        print(f"   ❌ Bucket access failed: {e}")
        return
    
    # 3. List blobs
    print("\n3️⃣ Listing files in raw/...")
    try:
        blobs = list(bucket.list_blobs(prefix='raw/', max_results=10))
        print(f"   ✅ Found {len(blobs)} files (showing first 10)")
        for blob in blobs:
            print(f"      - {blob.name} ({blob.size / 1024:.1f} KB)")
    except Exception as e:
        print(f"   ❌ List failed: {e}")
        return
    
    # 4. Test download permissions
    print("\n4️⃣ Testing download permissions...")
    if blobs:
        test_blob = blobs[0]
        print(f"   Testing: {test_blob.name}")
        
        try:
            # Check if blob exists
            exists = test_blob.exists()
            print(f"   ✅ Blob exists: {exists}")
            
            # Try to download first 1KB
            content = test_blob.download_as_bytes(start=0, end=1024)
            print(f"   ✅ Download successful! (Downloaded {len(content)} bytes)")
            
        except Exception as e:
            print(f"   ❌ Download failed: {e}")
            
            # Check specific error
            if "403" in str(e):
                print("\n   🚨 403 ERROR DIAGNOSIS:")
                print("   - Billing account is disabled/closed")
                print("   - Project owner needs to enable billing")
                print("   - OR you need different permissions")
                
                # Try to get IAM permissions
                try:
                    iam_policy = bucket.get_iam_policy(requested_policy_version=3)
                    print("\n   📋 Your permissions:")
                    for binding in iam_policy.bindings:
                        print(f"      - {binding['role']}")
                except:
                    print("   ⚠️  Cannot check IAM permissions")
    
    print("\n" + "="*60)
    print("DIAGNOSTICS COMPLETE")
    print("="*60 + "\n")


if __name__ == "__main__":
    diagnose_gcs_access()