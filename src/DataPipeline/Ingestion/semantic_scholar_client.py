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
                    # Exponential backoff for rate limits: 10s, 20s, 40s
                    backoff_time = min(10 * (2 ** attempt), 60)  # Max 60 seconds
                    logging.warning(f"Rate limit hit for key {key_display}, backing off for {backoff_time}s...")
                    await asyncio.sleep(backoff_time)
                    # Continue to retry
                    continue
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
                elif response.status == 504:
                    # Gateway timeout - server is overloaded, wait longer and retry
                    backoff_time = min(15 * (attempt + 1), 60)  # 15s, 30s, 45s, max 60s
                    logging.warning(f"⚠️  Gateway timeout (504) for query '{query}', backing off for {backoff_time}s...")
                    await asyncio.sleep(backoff_time)
                    continue  # Retry
                else:
                    text = await response.text()
                    logging.warning(f"API error {response.status}: {text[:200]}")
        except asyncio.TimeoutError:
            logging.error(f"Request timeout for {query}")
        except Exception as e:
            logging.error(f"Search failed: {e}")
        
        # Wait between retries (longer wait if we hit rate limits)
        # If we just hit a rate limit, we already waited in the 429 handler
        # Otherwise, use normal wait time
        if attempt < retries - 1:  # Don't wait after last attempt
            wait_time = 1.5 if api_key else 5.0
            await asyncio.sleep(wait_time)
    
    logging.warning(f"⚠️ All retries exhausted for query '{query}'. Returning empty results.")
    return []


async def get_paper_by_id_async(
    session: aiohttp.ClientSession,
    paper_id: str,
    retries: int = 3,
    api_key: Optional[str] = None
) -> Optional[Dict]:
    """
    Fetch a single paper by paperId from Semantic Scholar API
    
    Args:
        session: aiohttp ClientSession
        paper_id: Semantic Scholar paperId
        retries: Number of retry attempts
        api_key: Optional API key
    
    Returns:
        Paper dictionary or None if not found
    """
    fields = [
        "paperId", "externalIds", "title", "abstract", "year", "publicationDate",
        "venue", "publicationVenue", "publicationTypes", "authors",
        "citationCount", "influentialCitationCount", "referenceCount",
        "citations", "references", "fieldsOfStudy", "s2FieldsOfStudy",
        "isOpenAccess", "openAccessPdf", "tldr"
    ]
    
    url = f"https://api.semanticscholar.org/graph/v1/paper/{paper_id}?fields={','.join(fields)}"
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/html"
    }
    if api_key:
        headers["x-api-key"] = api_key
    
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=40)) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 404:
                    logging.warning(f"Paper {paper_id} not found")
                    return None
                elif response.status == 429:
                    # Exponential backoff for rate limits: 10s, 20s, 40s
                    backoff_time = min(10 * (2 ** attempt), 60)  # Max 60 seconds
                    logging.warning(f"Rate limit hit for paper {paper_id}, backing off for {backoff_time}s...")
                    await asyncio.sleep(backoff_time)
                    # Continue to retry
                    continue
                elif response.status == 403:
                    logging.error(f"❌ 403 Forbidden for paper {paper_id}")
                    return None
                else:
                    text = await response.text()
                    logging.warning(f"API error {response.status} for paper {paper_id}: {text[:200]}")
        except asyncio.TimeoutError:
            logging.error(f"Request timeout for paper {paper_id}")
        except Exception as e:
            logging.error(f"Failed to fetch paper {paper_id}: {e}")
        
        # Wait between retries (longer wait if we hit rate limits)
        # If we just hit a rate limit, we already waited in the 429 handler
        # Otherwise, use normal wait time
        if attempt < retries - 1:  # Don't wait after last attempt
            # Longer wait for single key scenarios to avoid rate limits
            wait_time = 2.0 if api_key else 5.0  # Increased from 1.5s to 2.0s
            await asyncio.sleep(wait_time)
    
    logging.warning(f"⚠️  All retries exhausted for paper {paper_id}. Skipping.")
    return None


async def get_papers_by_ids_batch_async(
    session: aiohttp.ClientSession,
    paper_ids: List[str],
    api_key: Optional[str] = None,
    max_concurrent: int = 10
) -> List[Dict]:
    """
    Fetch multiple papers by paperIds in parallel with API key rotation and adaptive rate limiting
    
    Args:
        session: aiohttp ClientSession
        paper_ids: List of paperIds to fetch
        api_key: Optional API key (if None, will use api_key_manager for rotation)
        max_concurrent: Maximum concurrent requests
    
    Returns:
        List of paper dictionaries (only successfully fetched papers)
    """
    from .api_key_manager import APIKeyManager
    
    # Use API key manager for rotation if no specific key provided
    key_manager = APIKeyManager()
    num_keys = key_manager.get_key_count()
    use_key_rotation = api_key is None and num_keys > 1
    
    # Adaptive delay: longer wait between requests if single key
    base_delay = 0.5 if num_keys > 1 else 1.5  # 0.5s for multiple keys, 1.5s for single key
    
    semaphore = asyncio.Semaphore(max_concurrent)
    results = []
    fetched_count = 0
    rate_limit_count = 0
    total = len(paper_ids)
    
    async def fetch_with_semaphore(paper_id, index):
        nonlocal fetched_count, rate_limit_count
        async with semaphore:
            # Rotate API key if multiple keys available
            current_key = key_manager.get_key() if use_key_rotation else api_key
            
            # Add small delay between requests to avoid overwhelming API
            if index > 0 and index % max_concurrent == 0:
                await asyncio.sleep(base_delay)
            
            paper = await get_paper_by_id_async(session, paper_id, api_key=current_key)
            if paper:
                results.append(paper)
                fetched_count += 1
                # Log progress every 100 papers
                if fetched_count % 100 == 0:
                    logging.info(f"📚 Progress: {fetched_count}/{total} referenced papers fetched ({100*fetched_count/total:.1f}%)")
            return paper
    
    tasks = [fetch_with_semaphore(pid, i) for i, pid in enumerate(paper_ids)]
    await asyncio.gather(*tasks, return_exceptions=True)
    
    if rate_limit_count > 0:
        logging.warning(f"⚠️  Hit rate limits {rate_limit_count} times during fetch. Consider reducing MAX_REFERENCES_PER_PAPER or adding more API keys.")
    
    return results


async def get_paper_references_async(
    session: aiohttp.ClientSession,
    paper_id: str,
    api_key: Optional[str] = None,
    max_references: Optional[int] = None,
    retries: int = 3
) -> List[str]:
    """
    Fetch references for a paper using the dedicated /references endpoint
    
    Args:
        session: aiohttp ClientSession
        paper_id: Semantic Scholar paperId
        api_key: Optional API key
        max_references: Maximum number of references to return (None = all)
        retries: Number of retry attempts
    
    Returns:
        List of paperIds of referenced papers (from citedPaper.paperId)
    """
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/html"
    }
    if api_key:
        headers["x-api-key"] = api_key
    
    reference_ids = []
    offset = 0
    limit = 100  # API default limit per page
    has_more = True
    
    while has_more:
        url = f"https://api.semanticscholar.org/graph/v1/paper/{paper_id}/references?offset={offset}&limit={limit}"
        
        for attempt in range(retries):
            try:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=40)) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data is None:
                            logging.warning(f"Empty response for paper {paper_id} references")
                            has_more = False
                            break
                        
                        references = data.get("data", [])
                        if not isinstance(references, list):
                            references = []
                        
                        # Extract paperIds from citedPaper objects, respecting max_references limit
                        for ref in references:
                            # Stop if we've reached the limit
                            if max_references is not None and len(reference_ids) >= max_references:
                                break
                            
                            cited_paper = ref.get("citedPaper", {})
                            if cited_paper and cited_paper.get("paperId"):
                                reference_ids.append(cited_paper["paperId"])
                        
                        # Check if we've reached the limit or if there are more references
                        if max_references is not None and len(reference_ids) >= max_references:
                            # We've collected enough, stop fetching
                            has_more = False
                            break
                        
                        # Check if there are more references to fetch
                        next_offset = data.get("next")
                        if next_offset is not None:
                            offset = next_offset
                        else:
                            has_more = False
                            break
                        
                        break  # Success, exit retry loop
                        
                    elif response.status == 404:
                        logging.warning(f"Paper {paper_id} not found or has no references")
                        has_more = False
                        break
                    elif response.status == 429:
                        backoff_time = min(10 * (2 ** attempt), 60)
                        logging.warning(f"Rate limit hit for paper {paper_id} references, backing off for {backoff_time}s...")
                        await asyncio.sleep(backoff_time)
                        continue
                    elif response.status == 403:
                        logging.warning(f"403 Forbidden for paper {paper_id} references")
                        has_more = False
                        break
                    else:
                        logging.warning(f"API error {response.status} for paper {paper_id} references: {await response.text()[:200]}")
                        if attempt < retries - 1:
                            await asyncio.sleep(2 ** attempt)
                        else:
                            has_more = False
                            break
                            
            except Exception as e:
                logging.error(f"Error fetching references for paper {paper_id}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    has_more = False
                    break
    
    return reference_ids


async def get_papers_references_batch_async(
    session: aiohttp.ClientSession,
    paper_ids: List[str],
    api_key: Optional[str] = None,
    max_references_per_paper: Optional[int] = None,
    max_concurrent: int = 10
) -> Dict[str, List[str]]:
    """
    Fetch references for multiple papers in parallel
    
    Args:
        session: aiohttp ClientSession
        paper_ids: List of paperIds to fetch references for
        api_key: Optional API key (will rotate if multiple keys available)
        max_references_per_paper: Max references per paper (None = all)
        max_concurrent: Maximum concurrent requests
    
    Returns:
        Dictionary mapping paperId -> list of referenced paperIds
    """
    from .api_key_manager import APIKeyManager
    
    key_manager = APIKeyManager()
    num_keys = key_manager.get_key_count()
    use_key_rotation = api_key is None and num_keys > 1
    
    # Adaptive delay: longer wait between requests if single key
    base_delay = 0.5 if num_keys > 1 else 1.5
    
    semaphore = asyncio.Semaphore(max_concurrent)
    results = {}
    
    async def fetch_references_with_semaphore(paper_id, index):
        async with semaphore:
            # Rotate API key if multiple keys available
            current_key = key_manager.get_key() if use_key_rotation else api_key
            
            # Add small delay between requests
            if index > 0 and index % max_concurrent == 0:
                await asyncio.sleep(base_delay)
            
            reference_ids = await get_paper_references_async(
                session,
                paper_id,
                api_key=current_key,
                max_references=max_references_per_paper
            )
            results[paper_id] = reference_ids
            return reference_ids
    
    tasks = [fetch_references_with_semaphore(pid, i) for i, pid in enumerate(paper_ids)]
    await asyncio.gather(*tasks, return_exceptions=True)
    
    return results
