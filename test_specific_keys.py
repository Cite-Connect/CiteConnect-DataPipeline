#!/usr/bin/env python3
"""
Test specific API keys
"""
import requests
import time

api_keys = [
    "pzMMKRLk0P15w2sNebQIn5LAM6Je8qcK1fG6nysB",
    "6280Re5cgJaBh7RY4IYNw2YTkKQPCpZk5kdFrHxR",
    "KKRcI2dWRc1eK4UTLaZGu6uDHYtL7CZt8nyLvfhk",
    "SrMLtI3fWg7zZHceTNx6a8Ht0uJs8fGa2k4eYaJM"
]

print("=" * 70)
print("🔍 Testing 4 API Keys")
print("=" * 70)

results = {
    "working": [],
    "rate_limited": [],
    "forbidden": [],
    "other_errors": []
}

for i, key in enumerate(api_keys):
    key_display = key[:8] + "..."
    print(f"\n🔑 Testing API key {i+1}/4: {key_display}")
    
    url = "https://api.semanticscholar.org/graph/v1/paper/search"
    params = {"query": "machine learning", "limit": 5, "fields": "paperId,title"}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "x-api-key": key
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            papers = data.get("data", [])
            print(f"   ✅ WORKING: Got {len(papers)} papers")
            results["working"].append(key_display)
        elif response.status_code == 429:
            print(f"   ⚠️  RATE LIMITED (429)")
            results["rate_limited"].append(key_display)
        elif response.status_code == 403:
            print(f"   ❌ FORBIDDEN (403): Key may be invalid")
            results["forbidden"].append(key_display)
        else:
            print(f"   ❌ ERROR ({response.status_code}): {response.text[:100]}")
            results["other_errors"].append((key_display, response.status_code))
    except Exception as e:
        print(f"   ❌ EXCEPTION: {e}")
        results["other_errors"].append((key_display, str(e)))
    
    if i < len(api_keys) - 1:
        time.sleep(2)  # Wait between tests

print("\n" + "=" * 70)
print("📊 SUMMARY")
print("=" * 70)
print(f"\n✅ Working keys: {len(results['working'])}/4")
for key in results['working']:
    print(f"   - {key}")

print(f"\n⚠️  Rate-limited keys: {len(results['rate_limited'])}/4")
for key in results['rate_limited']:
    print(f"   - {key} (wait 5-10 minutes)")

print(f"\n❌ Forbidden keys: {len(results['forbidden'])}/4")
for key in results['forbidden']:
    print(f"   - {key} (may be invalid)")

if results['other_errors']:
    print(f"\n⚠️  Other errors: {len(results['other_errors'])}/4")
    for key, error in results['other_errors']:
        print(f"   - {key}: {error}")

print("\n" + "=" * 70)


