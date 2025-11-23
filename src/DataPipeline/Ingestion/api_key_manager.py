"""
API Key Manager for Semantic Scholar API
Handles rotation between multiple API keys for rate limit management
"""
import os
import threading
import logging
from typing import List, Optional
from itertools import cycle

class APIKeyManager:
    """Manages multiple API keys with round-robin rotation"""
    
    def __init__(self, api_keys: Optional[List[str]] = None):
        """
        Initialize API key manager
        
        Args:
            api_keys: List of API keys. If None, reads from SEMANTIC_SCHOLAR_API_KEYS env var
        """
        if api_keys is None:
            # Read from environment variable (comma-separated)
            keys_env = os.getenv('SEMANTIC_SCHOLAR_API_KEYS', '')
            if keys_env:
                api_keys = [key.strip() for key in keys_env.split(',') if key.strip()]
            else:
                # Fallback to single key
                single_key = os.getenv('SEMANTIC_SCHOLAR_API_KEY', '')
                api_keys = [single_key] if single_key else []
        
        self.api_keys = api_keys
        self._lock = threading.Lock()
        self._key_cycle = cycle(api_keys) if api_keys else None
        self._key_index = 0
        
        if not self.api_keys:
            logging.warning("⚠️ No API keys configured. Using unauthenticated requests (slower rate limits)")
        else:
            logging.info(f"✅ Initialized API key manager with {len(self.api_keys)} key(s)")
    
    def get_key(self) -> Optional[str]:
        """Get the next API key in rotation (thread-safe)"""
        if not self.api_keys:
            return None
        
        with self._lock:
            key = next(self._key_cycle)
            self._key_index = (self._key_index + 1) % len(self.api_keys)
            return key
    
    def get_headers(self) -> dict:
        """Get headers with current API key"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Accept": "application/json, text/html"
        }
        
        api_key = self.get_key()
        if api_key:
            headers["x-api-key"] = api_key
        
        return headers
    
    def get_key_count(self) -> int:
        """Get number of available API keys"""
        return len(self.api_keys)

