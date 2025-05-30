"""
Core interfaces for the scraper platform.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator, List

from .models import RawItem, ParsedItem


class Fetcher(ABC):
    """Abstract base class for data fetchers."""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name for this fetcher."""
        pass

    @abstractmethod
    async def fetch(self) -> AsyncIterator[RawItem]:
        """Fetch raw data items."""
        pass


class Parser(ABC):
    """Abstract base class for data parsers."""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name for this parser."""
        pass

    @abstractmethod
    async def parse(self, item: RawItem) -> List[ParsedItem]:
        """Parse a raw item into structured data."""
        pass


class Sink(ABC):
    """Abstract base class for data sinks."""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name for this sink."""
        pass

    @abstractmethod
    async def handle(self, item: ParsedItem) -> None:
        """Handle a parsed item."""
        pass
