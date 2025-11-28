#!/usr/bin/env python3
"""
Diagnostic script to test Semantic Scholar API keys
Tests each key individually to identify rate limit issues
"""
import os
import sys
import time
import requests
from typing import List, Dict, Optional

def test_api_key(api_key: str, test_query: str = "machine learning", limit: int = 5) -> Dict:
    """Test a single API key with a simple query"""
    url = f"https://api.semanticscholar.org/graph/v1/paper/search"
    params = {
        "query": test_query,
        "limit": limit,
        "fields": "paperId,title"
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "x-api-key": api_key
    }
    
    key_display = api_key[:8] + "..." if len(api_key) > 8 else api_key
    
    try:
        print(f"\n🔑 Testing API key: {key_display}")
        print(f"   Query: '{test_query}' (limit: {limit})")
        
        start_time = time.time()
        response = requests.get(url, params=params, headers=headers, timeout=10)
        elapsed = time.time() - start_time
        
        result = {
            "key": key_display,
            "status_code": response.status_code,
            "elapsed": elapsed,
            "success": False,
            "error": None,
            "paper_count": 0
        }
        
        if response.status_code == 200:
            data = response.json()
            papers = data.get("data", [])
            result["success"] = True
            result["paper_count"] = len(papers)
            print(f"   ✅ SUCCESS: Got {len(papers)} papers in {elapsed:.2f}s")
            if papers:
                print(f"   📄 Sample: {papers[0].get('title', 'N/A')[:60]}...")
        elif response.status_code == 429:
            result["error"] = "Rate limit exceeded"
            print(f"   ⚠️  RATE LIMIT (429): Key is rate-limited")
            print(f"   💡 Wait 5-10 minutes before using this key again")
        elif response.status_code == 403:
            result["error"] = "Forbidden"
            text = response.text[:200]
            print(f"   ❌ FORBIDDEN (403): Key may be invalid or restricted")
            print(f"   Error: {text}")
        elif response.status_code == 400:
            result["error"] = "Bad request"
            text = response.text[:200]
            print(f"   ⚠️  BAD REQUEST (400): {text}")
        else:
            result["error"] = f"HTTP {response.status_code}"
            text = response.text[:200]
            print(f"   ❌ ERROR ({response.status_code}): {text}")
        
        return result
        
    except requests.exceptions.Timeout:
        result = {
            "key": key_display,
            "status_code": None,
            "elapsed": None,
            "success": False,
            "error": "Timeout",
            "paper_count": 0
        }
        print(f"   ❌ TIMEOUT: Request took too long")
        return result
    except Exception as e:
        result = {
            "key": key_display,
            "status_code": None,
            "elapsed": None,
            "success": False,
            "error": str(e),
            "paper_count": 0
        }
        print(f"   ❌ EXCEPTION: {e}")
        return result


def test_all_keys():
    """Test all API keys from environment"""
    print("=" * 70)
    print("🔍 Semantic Scholar API Key Diagnostic Test")
    print("=" * 70)
    
    # Get API keys from environment
    keys_env = os.getenv('SEMANTIC_SCHOLAR_API_KEYS', '')
    if not keys_env:
        single_key = os.getenv('SEMANTIC_SCHOLAR_API_KEY', '')
        if single_key:
            api_keys = [single_key]
        else:
            print("❌ No API keys found in environment variables")
            print("   Set SEMANTIC_SCHOLAR_API_KEYS or SEMANTIC_SCHOLAR_API_KEY")
            return
    else:
        api_keys = [key.strip() for key in keys_env.split(',') if key.strip()]
    
    print(f"\n📊 Found {len(api_keys)} API key(s) to test")
    
    # Test each key with a simple query
    results = []
    test_query = "machine learning"
    
    for i, key in enumerate(api_keys):
        if i > 0:
            print(f"\n⏳ Waiting 2 seconds before testing next key...")
            time.sleep(2)
        
        result = test_api_key(key, test_query=test_query, limit=5)
        results.append(result)
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 TEST SUMMARY")
    print("=" * 70)
    
    working_keys = [r for r in results if r["success"]]
    rate_limited = [r for r in results if r.get("status_code") == 429]
    forbidden = [r for r in results if r.get("status_code") == 403]
    other_errors = [r for r in results if not r["success"] and r.get("status_code") not in [429, 403]]
    
    print(f"\n✅ Working keys: {len(working_keys)}/{len(api_keys)}")
    for r in working_keys:
        print(f"   - {r['key']}: {r['paper_count']} papers in {r['elapsed']:.2f}s")
    
    if rate_limited:
        print(f"\n⚠️  Rate-limited keys: {len(rate_limited)}/{len(api_keys)}")
        for r in rate_limited:
            print(f"   - {r['key']}: Wait 5-10 minutes before using")
    
    if forbidden:
        print(f"\n❌ Forbidden/Invalid keys: {len(forbidden)}/{len(api_keys)}")
        for r in forbidden:
            print(f"   - {r['key']}: May be invalid or have restricted permissions")
    
    if other_errors:
        print(f"\n⚠️  Other errors: {len(other_errors)}/{len(api_keys)}")
        for r in other_errors:
            print(f"   - {r['key']}: {r.get('error', 'Unknown error')}")
    
    # Test problematic search terms
    if working_keys:
        print("\n" + "=" * 70)
        print("🔍 Testing problematic search terms")
        print("=" * 70)
        
        problematic_terms = [
            "transformer architecture",
            "transformers",
            "LLM",
            "computer vision",
            "natural language processing"
        ]
        
        test_key = api_keys[0]  # Use first key
        key_display = test_key[:8] + "..." if len(test_key) > 8 else test_key
        
        for term in problematic_terms:
            print(f"\n🔍 Testing: '{term}'")
            result = test_api_key(test_key, test_query=term, limit=5)
            time.sleep(2)  # Wait between tests
    
    print("\n" + "=" * 70)
    print("✅ Diagnostic complete")
    print("=" * 70)


if __name__ == "__main__":
    test_all_keys()


