"""
FI Short Interest Plugin - Entry point registration.
"""

from .fetcher import FiFetcher
from .parser import FiAggParser, FiActParser

__all__ = ["FiFetcher", "FiAggParser", "FiActParser"]
