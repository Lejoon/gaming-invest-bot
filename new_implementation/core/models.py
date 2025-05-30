"""
Core data models for the scraper platform.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from pydantic import BaseModel, Field


class RawItem(BaseModel):
    """Raw data fetched from a source."""
    source: str
    payload: bytes
    fetched_at: datetime = Field(default_factory=datetime.utcnow)


class ParsedItem(BaseModel):
    """Parsed and structured data."""
    topic: str
    content: Dict[str, Any]
    discovered_at: datetime = Field(default_factory=datetime.utcnow)


class Event(BaseModel):
    """System events for logging and monitoring."""
    level: str  # INFO, WARNING, ERROR, etc.
    message: str
    source: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)
