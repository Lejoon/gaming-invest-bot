"""
Database sink for persisting data to SQLite.
"""

import logging
from typing import Dict, List

from ..core.interfaces import Sink
from ..core.models import ParsedItem
from ..core.infra.db import Database


logger = logging.getLogger(__name__)


class DatabaseSink(Sink):
    """Sink that persists data to SQLite database."""
    
    name = "DatabaseSink"
    
    _TABLE_MAP = {
        "fi.short.aggregate": {
            "table": "short_positions",
            "pk": ["lei"],
            "cols": [
                "lei",
                "company_name",
                "position_percent",
                "latest_position_date",
                "timestamp",
            ],
        },
        "fi.short.aggregate.diff": {  # same table / cols
            "table": "short_positions",
            "pk": ["lei"],
            "cols": [
                "lei",
                "company_name",
                "position_percent",
                "latest_position_date",
                "timestamp",
            ],
        },
        "fi.short.positions": {
            "table": "position_holders",
            "pk": ["entity_name", "issuer_name", "isin"],
            "cols": [
                "entity_name",
                "issuer_name",
                "isin",
                "position_percent",
                "position_date",
                "timestamp",
                "comment",
            ],
        },
        "fi.short.positions.diff": {  # ditto
            "table": "position_holders",
            "pk": ["entity_name", "issuer_name", "isin"],
            "cols": [
                "entity_name",
                "issuer_name",
                "isin",
                "position_percent",
                "position_date",
                "timestamp",
                "comment",
            ],
        },
    }

    def __init__(self, db_path: str = "scraper.db"):
        self.db = Database(db_path)

    async def handle(self, item: ParsedItem) -> None:
        """Handle a parsed item by persisting to database."""
        if item.topic not in self._TABLE_MAP:
            logger.debug(f"No table mapping for topic: {item.topic}")
            return
        
        config = self._TABLE_MAP[item.topic]
        table = config["table"]
        pk_columns = config["pk"]
        columns = config["cols"]
        
        # Extract data for the configured columns
        data = {}
        for col in columns:
            value = item.content.get(col)
            if value is not None:
                data[col] = value
        
        if not data:
            logger.warning(f"No data to insert for topic: {item.topic}")
            return
        
        try:
            await self.db.upsert(table, data, pk_columns)
            logger.debug(f"Upserted data to {table}: {list(data.keys())}")
        except Exception as e:
            logger.error(f"Failed to upsert data to {table}: {e}")

    async def close(self) -> None:
        """Close the database connection."""
        await self.db.close()
