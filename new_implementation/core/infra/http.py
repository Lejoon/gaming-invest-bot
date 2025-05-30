"""
HTTP client infrastructure with retry logic and error handling.
"""

import asyncio
import logging
from typing import Any, Dict, Optional

import aiohttp


logger = logging.getLogger(__name__)


class HttpClient:
    """Async HTTP client with retry logic and error handling."""
    
    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: float = 30.0,
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
    ):
        self._external_session = session
        self._timeout = timeout
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._own_session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session."""
        if self._external_session:
            return self._external_session
        
        if not self._own_session:
            self._own_session = aiohttp.ClientSession()
        return self._own_session

    async def close(self) -> None:
        """Close the session if we own it."""
        if self._own_session:
            await self._own_session.close()
            self._own_session = None

    async def get_text(self, url: str, **kwargs) -> str:
        """GET request returning text content with retry logic."""
        session = await self._get_session()
        
        for attempt in range(self._max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=self._timeout)
                async with session.get(url, timeout=timeout, **kwargs) as response:
                    response.raise_for_status()
                    return await response.text()
            except Exception as e:
                if attempt == self._max_retries - 1:
                    logger.error(f"Failed to GET {url} after {self._max_retries} attempts: {e}")
                    raise
                
                delay = min(self._base_delay * (2 ** attempt), self._max_delay)
                logger.warning(f"GET {url} failed (attempt {attempt + 1}), retrying in {delay}s: {e}")
                await asyncio.sleep(delay)

    async def get_bytes(self, url: str, **kwargs) -> bytes:
        """GET request returning binary content with retry logic."""
        session = await self._get_session()
        
        for attempt in range(self._max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=self._timeout * 2)  # Longer timeout for binary
                async with session.get(url, timeout=timeout, **kwargs) as response:
                    response.raise_for_status()
                    return await response.read()
            except Exception as e:
                if attempt == self._max_retries - 1:
                    logger.error(f"Failed to GET {url} after {self._max_retries} attempts: {e}")
                    raise
                
                delay = min(self._base_delay * (2 ** attempt), self._max_delay)
                logger.warning(f"GET {url} failed (attempt {attempt + 1}), retrying in {delay}s: {e}")
                await asyncio.sleep(delay)

    async def post_json(self, url: str, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """POST JSON request with retry logic."""
        session = await self._get_session()
        
        for attempt in range(self._max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=self._timeout)
                async with session.post(url, json=data, timeout=timeout, **kwargs) as response:
                    response.raise_for_status()
                    return await response.json()
            except Exception as e:
                if attempt == self._max_retries - 1:
                    logger.error(f"Failed to POST {url} after {self._max_retries} attempts: {e}")
                    raise
                
                delay = min(self._base_delay * (2 ** attempt), self._max_delay)
                logger.warning(f"POST {url} failed (attempt {attempt + 1}), retrying in {delay}s: {e}")
                await asyncio.sleep(delay)
