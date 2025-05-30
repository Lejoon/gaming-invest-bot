"""
FI Short Interest Fetcher - Downloads ODS files from Finansinspektionen.
"""

import asyncio
import logging
from datetime import datetime
from typing import AsyncIterator, Optional

import aiohttp
from bs4 import BeautifulSoup

from core.interfaces import Fetcher
from core.models import RawItem
from core.infra.http import HttpClient


logger = logging.getLogger(__name__)


class FiFetcher(Fetcher):
    """Fetches short interest data from Finansinspektionen."""
    
    name = "FiFetcher"
    
    URL_TS = "https://www.fi.se/sv/vara-register/blankningsregistret/"
    URL_AGG = "https://www.fi.se/sv/vara-register/blankningsregistret/GetBlankningsregisterAggregat/"
    URL_ACT = "https://www.fi.se/sv/vara-register/blankningsregistret/GetAktuellFile/"
    SLEEP = 15 * 60  # 15 minutes

    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self._http_client = HttpClient(session=session)
        self._last_seen: Optional[str] = None

    async def fetch(self) -> AsyncIterator[RawItem]:
        """Fetch FI short interest data."""
        try:
            while True:
                ts = await self._poll_timestamp()
                if ts and ts != "0001-01-01 00:00" and ts != self._last_seen:
                    logger.info(f"New timestamp detected: {ts}")
                    
                    # Download both files
                    agg_bytes = await self._http_client.get_bytes(self.URL_AGG)
                    act_bytes = await self._http_client.get_bytes(self.URL_ACT)

                    fetched_at = datetime.utcnow()
                    self._last_seen = ts

                    yield RawItem(source="fi.short.agg", payload=agg_bytes, fetched_at=fetched_at)
                    yield RawItem(source="fi.short.act", payload=act_bytes, fetched_at=fetched_at)
                
                await asyncio.sleep(self.SLEEP)
        finally:
            await self._http_client.close()

    async def _poll_timestamp(self) -> Optional[str]:
        """Poll the FI website for the last update timestamp."""
        try:
            html = await self._http_client.get_text(self.URL_TS)
            soup = BeautifulSoup(html, "html.parser")
            
            # Look for the timestamp text
            tag = soup.find("p", string=lambda t: "Listan uppdaterades:" in t if t else False)
            if not tag:
                logger.warning("Could not find timestamp on FI website")
                return None
            
            timestamp = tag.text.split(": ")[1].strip()
            logger.debug(f"Found timestamp: {timestamp}")
            return timestamp
            
        except Exception as e:
            logger.error(f"Failed to poll timestamp: {e}")
            return None
