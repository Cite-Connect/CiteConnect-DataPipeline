from typing import List, Dict, Optional
import requests, time, logging
import aiohttp
import asyncio
from src.DataPipeline.utils.constants import HEADERS


def search_semantic_scholar(query: str, limit: int = 10, retries: int = 3, api_key: Optional[str] = None) -> List[Dict]:
    """
    Search Semantic Scholar API for papers
    
    Args:
        query: Search query string
        limit: Maximum number of papers to return
        retries: Number of retry attempts
        api_key: Optional API key (if None, uses default from constants)
    
    Returns:
        List of paper dictionaries
    """
    import re
    
    fields = [
        "paperId", "externalIds", "title", "abstract", "year", "publicationDate",
        "venue", "publicationVenue", "publicationTypes", "authors",
        "citationCount", "influentialCitationCount", "referenceCount",
        "citations", "references", "fieldsOfStudy", "s2FieldsOfStudy",
        "isOpenAccess", "openAccessPdf", "tldr"
    ]

    current_limit = limit
    
    # Use provided API key or fall back to default headers
    headers = HEADERS.copy() if not api_key else {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/html",
        "x-api-key": api_key
    }

    for attempt in range(retries):
        key_display = api_key[:8] + "..." if api_key and len(api_key) > 8 else ("none" if not api_key else api_key)
        logging.info(f"Query: {query} attempt {attempt+1} (API key: {key_display}, limit: {current_limit})")
        
        url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={query}&limit={current_limit}&fields={','.join(fields)}"
        
        try:
            r = requests.get(url, headers=headers, timeout=40)
            if r.status_code == 200:
                return r.json().get("data", [])
            elif r.status_code == 429:
                logging.warning(f"Rate limit hit for key {key_display}, backing off...")
                time.sleep(5)
            elif r.status_code == 400:
                # Parse error message for suggested limit
                error_text = r.text[:200]
                logging.warning(f"API error 400: {error_text}")
                
                # Try to extract suggested limit from error message
                limit_match = re.search(r'limit=(\d+)', error_text)
                if limit_match:
                    suggested_limit = int(limit_match.group(1))
                    if suggested_limit < current_limit:
                        logging.info(f"📉 API suggests reducing limit from {current_limit} to {suggested_limit}")
                        current_limit = suggested_limit
                        # Continue to next iteration with new limit
                        continue
                else:
                    logging.error(f"400 Bad Request for query '{query}'. Error: {error_text}")
            elif r.status_code == 403:
                logging.error(f"❌ 403 Forbidden for query '{query}' with API key {key_display}")
                logging.error(f"   This may indicate: invalid API key, restricted query, or API key permissions issue")
                logging.error(f"   Error: {r.text[:200]}")
                # Don't retry 403 - it's likely a permanent issue
                return []
            else:
                logging.warning(f"API error {r.status_code}: {r.text[:200]}")
        except Exception as e:
            logging.error(f"Search failed: {e}")
        
        # Wait time: 1.5s with API key, 5s without
        # Check both the parameter and HEADERS for API key
        has_api_key = api_key is not None or (HEADERS.get("x-api-key") is not None)
        wait_time = 1.5 if has_api_key else 5.0
        time.sleep(wait_time)
    return []


async def search_semantic_scholar_async(
    session: aiohttp.ClientSession,
    query: str,
    limit: int = 10,
    retries: int = 3,
    api_key: Optional[str] = None
) -> List[Dict]:
    """
    Async version of search_semantic_scholar using aiohttp
    
    Args:
        session: aiohttp ClientSession
        query: Search query string
        limit: Maximum number of papers to return
        retries: Number of retry attempts
        api_key: Optional API key
    
    Returns:
        List of paper dictionaries
    """
    import re
    
    fields = [
        "paperId", "externalIds", "title", "abstract", "year", "publicationDate",
        "venue", "publicationVenue", "publicationTypes", "authors",
        "citationCount", "influentialCitationCount", "referenceCount",
        "citations", "references", "fieldsOfStudy", "s2FieldsOfStudy",
        "isOpenAccess", "openAccessPdf", "tldr"
    ]

    current_limit = limit
    url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={query}&limit={current_limit}&fields={','.join(fields)}"
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/html"
    }
    if api_key:
        headers["x-api-key"] = api_key

    for attempt in range(retries):
        key_display = api_key[:8] + "..." if api_key and len(api_key) > 8 else ("none" if not api_key else api_key)
        logging.info(f"[ASYNC] Query: {query} attempt {attempt+1} (API key: {key_display}, limit: {current_limit})")
        
        try:
            # Update URL with current limit
            url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={query}&limit={current_limit}&fields={','.join(fields)}"
            
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=40)) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("data", [])
                elif response.status == 429:
                    logging.warning(f"Rate limit hit for key {key_display}, backing off...")
                    await asyncio.sleep(5)
                elif response.status == 400:
                    # Parse error message for suggested limit
                    text = await response.text()
                    logging.warning(f"API error 400: {text[:200]}")
                    
                    # Try to extract suggested limit from error message
                    # Format: "Response would exceed maximum size. Please retry with the query parameter: limit=83."
                    limit_match = re.search(r'limit=(\d+)', text)
                    if limit_match:
                        suggested_limit = int(limit_match.group(1))
                        if suggested_limit < current_limit:
                            logging.info(f"📉 API suggests reducing limit from {current_limit} to {suggested_limit}")
                            current_limit = suggested_limit
                            # Continue to next iteration with new limit
                            continue
                    else:
                        # 400 error without limit suggestion - might be invalid query
                        logging.error(f"400 Bad Request for query '{query}'. Error: {text[:200]}")
                        if "Forbidden" in text or "forbidden" in text.lower():
                            logging.error(f"❌ Query '{query}' may be forbidden or invalid. Try a different search term.")
                elif response.status == 403:
                    text = await response.text()
                    logging.error(f"❌ 403 Forbidden for query '{query}' with API key {key_display}")
                    logging.error(f"   This may indicate: invalid API key, restricted query, or API key permissions issue")
                    logging.error(f"   Error: {text[:200]}")
                    # Don't retry 403 - it's likely a permanent issue
                    return []
                else:
                    text = await response.text()
                    logging.warning(f"API error {response.status}: {text[:200]}")
        except asyncio.TimeoutError:
            logging.error(f"Request timeout for {query}")
        except Exception as e:
            logging.error(f"Search failed: {e}")
        
        wait_time = 1.5 if api_key else 5.0
        await asyncio.sleep(wait_time)
    
    return []
